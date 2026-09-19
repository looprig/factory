package hostlink

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	centrifuge "github.com/centrifugal/centrifuge"
	centrifugego "github.com/centrifugal/centrifuge-go"
	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// TestReconnectDoesNotWaitForAnRPCInFlight uses the real Centrifuge transport
// to pin the liveness that a lock held across client.RPC once broke:
//
//  1. a delivery is in flight, waiting on a slow Host reply;
//  2. the Host moves the transport to Connecting; and
//  3. the transport must still run its OnConnecting callback and schedule the
//     reconnect, while the in-flight call settles on its own.
//
// centrifuge-go runs OnConnecting synchronously inside moveToConnecting and
// only then schedules the reconnect (client.go:592-617), so any lock a call
// holds across the RPC and a callback waits for is a deadlock of the link.
// call holds none now. Read what this case does and does not kill: it is a
// liveness pin, and a lock re-added across the RPC AND taken in onConnecting
// survives it, because moveToConnecting fails every pending request
// (clearConnectedState, client.go:540) BEFORE it runs the callback, so the
// in-flight call returns and releases such a lock in time. The mutant that
// does turn red is the head-of-line case below; what this one adds is that
// the caller is released and the lost reply is not reported as success. The
// slow reply guarantees the RPC is in flight at the disconnect, so the case
// does not depend on a scheduler race.
func TestReconnectDoesNotWaitForAnRPCInFlight(t *testing.T) {
	t.Parallel()

	host := newLivenessHost(t)
	host.setBeforeReply(func(string) { time.Sleep(500 * time.Millisecond) })
	link := dialLiveness(t, host, 10*time.Millisecond)

	serverClient := <-host.connected
	inFlight := make(chan error, 1)
	go func() {
		inFlight <- link.DeliverCommand(context.Background(), "tenant-a", "s-1", sessionwire.HostLinkCommandDelivery{CommandID: "cmd-1"})
	}()
	waitUntilLiveness(t, "the delivery to reach the Host", func() bool { return host.calls() == 1 })
	serverClient.Disconnect(centrifuge.Disconnect{Code: 4000, Reason: "test reconnect"})

	select {
	case <-host.reconnected:
	case <-time.After(2 * time.Second):
		t.Fatal("reconnect did not reach the Host while an RPC was in flight")
	}
	select {
	case err := <-inFlight:
		// The reply was lost with the connection; what matters is that the
		// caller was released rather than parked behind the reconnect.
		if err == nil {
			t.Fatal("a delivery whose connection dropped before the reply reported success")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the in-flight delivery did not settle after the reconnect")
	}
}

// TestASlowReplyForOneSessionDoesNotBlockAnotherSessionsCall is the
// head-of-line case: every session on a Host shares one link, and a mutex held
// across client.RPC made a 100ms-deadline delivery wait 702ms behind another
// session's slow reply, because a caller deadline cannot preempt a mutex wait.
// RPCs on a link are concurrent; only the local admission snapshot is locked.
func TestASlowReplyForOneSessionDoesNotBlockAnotherSessionsCall(t *testing.T) {
	t.Parallel()

	host := newLivenessHost(t)
	slow := sessionwire.HostLinkChannel("tenant-a", "slow")
	host.setBeforeReply(func(method string) {
		if method == slow {
			time.Sleep(800 * time.Millisecond)
		}
	})
	link := dialLiveness(t, host, 100*time.Millisecond)

	slowDone := make(chan error, 1)
	go func() {
		slowDone <- link.DeliverCommand(context.Background(), "tenant-a", "slow", sessionwire.HostLinkCommandDelivery{CommandID: "cmd-slow"})
	}()
	waitUntilLiveness(t, "the slow delivery to reach the Host", func() bool { return host.calls() == 1 })

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := link.DeliverCommand(ctx, "tenant-a", "fast", sessionwire.HostLinkCommandDelivery{CommandID: "cmd-fast"})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("the 100ms-deadline delivery succeeded while the Host was still holding the slow reply")
	}
	if elapsed > 300*time.Millisecond {
		t.Fatalf("a 100ms-deadline delivery for another session waited %v behind a slow reply", elapsed)
	}
	select {
	case <-slowDone:
	case <-time.After(3 * time.Second):
		t.Fatal("the slow delivery never settled")
	}
}

// TestABindInTheConnectingWindowIsRefusedAsReconnectingNotUnsupported pins two
// things about a reserved call admitted after OnConnecting ran and before the
// new reply arrived. It is refused LOCALLY rather than queued against the old
// connection's capability set -- a stale bind delivered to a restarted Host
// that no longer advertises it is the case this guards -- and the refusal
// names a transient, ErrLinkReconnecting, not a capability fact: a caller that
// read "host did not advertise bind" during a 250ms reconnect would remake a
// placement decision over a Host that supports it perfectly well.
func TestABindInTheConnectingWindowIsRefusedAsReconnectingNotUnsupported(t *testing.T) {
	t.Parallel()

	host := newLivenessHost(t)
	link := dialLiveness(t, host, 300*time.Millisecond)
	serverClient := <-host.connected

	host.setNegotiation(`{"version":1}`)
	serverClient.Disconnect(centrifuge.Disconnect{Code: 4000, Reason: "test capability change"})
	waitForState(t, link.client, centrifugego.StateConnecting)
	// OnConnecting runs synchronously inside moveToConnecting, microseconds
	// after the state flips; 20ms is far inside the 300ms reconnect delay.
	time.Sleep(20 * time.Millisecond)
	if got := link.client.State(); got != centrifugego.StateConnecting {
		t.Fatalf("left the Connecting window early: %s", got)
	}

	err := link.Bind(context.Background(), livenessBindRequest(host.target.Host))
	if !errors.Is(err, ErrLinkReconnecting) {
		t.Fatalf("Bind in the Connecting window = %v, want ErrLinkReconnecting", err)
	}
	var unsupported *UnsupportedMethodError
	if errors.As(err, &unsupported) || errors.Is(err, ErrUnsupportedMethod) {
		t.Fatalf("Bind in the Connecting window reported a transient as a capability fact: %v", err)
	}

	select {
	case <-host.reconnected:
	case <-time.After(3 * time.Second):
		t.Fatal("reconnect did not complete")
	}
	time.Sleep(100 * time.Millisecond)
	if got := host.calls(); got != 0 {
		t.Fatalf("Host received %d RPCs, want 0: the stale bind was queued and delivered", got)
	}
	// Once the reply has landed the answer is the capability fact again.
	err = link.Bind(context.Background(), livenessBindRequest(host.target.Host))
	if !errors.As(err, &unsupported) {
		t.Fatalf("Bind after a capability-less reconnect = %v, want *UnsupportedMethodError", err)
	}
	if errors.Is(err, ErrLinkReconnecting) {
		t.Fatalf("Bind after the reconnect settled still reports ErrLinkReconnecting: %v", err)
	}
}

// TestReconnectStressNeverWedgesACaller drives four Bind loops through sixty
// reconnects into a Host that alternately advertises and withdraws bind. Two
// properties: no bind RPC ever reaches a connection whose reply advertised
// nothing (the generation registry), and every caller returns -- centrifuge-go
// v0.12.0 can run an RPC's completion callback twice when a send fails while
// clearConnectedState is failing the same request (client.go:373 blocks on a
// capacity-1 channel), and with a lock held across the RPC that wedge froze
// every later call on the link. It is confined now: client.RPC runs on its own
// goroutine and the caller selects against its context, so a wedge costs one
// leaked goroutine rather than a caller.
//
// Under the race detector it is opt-in (HOSTLINK_STRESS_UNDER_RACE=1): sixty
// reconnects with four senders trip centrifuge-go v0.12.0's own data race
// (client.go:2187 reads c.transport without c.mu) on nearly every run, and a
// third-party race report would turn every -race run of this package red
// without saying anything about this module. It still runs in every non-race
// run, which is where the wedge was reproduced.
func TestReconnectStressNeverWedgesACaller(t *testing.T) {
	t.Parallel()
	if raceDetectorEnabled && os.Getenv("HOSTLINK_STRESS_UNDER_RACE") == "" {
		t.Skip("trips centrifuge-go v0.12.0's data race at client.go:2187 under -race; set HOSTLINK_STRESS_UNDER_RACE=1 to run it anyway")
	}

	host := newLivenessHost(t)
	var stale atomic.Int32
	host.setOnRPC(func(advertised bool, method string) {
		if !advertised && method == sessionwire.HostLinkMethodBind {
			stale.Add(1)
		}
	})
	link := dialLiveness(t, host, 5*time.Millisecond)

	const rounds = 60
	var admitted, refused, reconnecting, canceled, other atomic.Int32
	for round := 0; round < rounds; round++ {
		serverClient := <-host.connected
		host.setNegotiation("")
		if round%2 == 0 {
			host.setNegotiation(`{"version":1}`) // the NEXT connection will not advertise bind
		}
		stop := make(chan struct{})
		var wg sync.WaitGroup
		for w := 0; w < 4; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					err := link.Bind(context.Background(), livenessBindRequest(host.target.Host))
					var unsupported *UnsupportedMethodError
					switch {
					case err == nil:
						admitted.Add(1)
					case errors.As(err, &unsupported):
						refused.Add(1)
					case errors.Is(err, ErrLinkReconnecting):
						reconnecting.Add(1)
					case errors.Is(err, context.Canceled):
						canceled.Add(1)
					default:
						other.Add(1)
					}
				}
			}()
		}
		time.Sleep(time.Duration(round%3) * time.Millisecond)
		wantConnects := host.connects.Load() + 1
		serverClient.Disconnect(centrifuge.Disconnect{Code: 4000, Reason: "stress"})
		deadline := time.Now().Add(3 * time.Second)
		for (host.connects.Load() < wantConnects || link.client.State() != centrifugego.StateConnected) && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		close(stop)
		waited := make(chan struct{})
		go func() { wg.Wait(); close(waited) }()
		select {
		case <-waited:
		case <-time.After(10 * time.Second):
			t.Fatalf("round %d: WEDGED -- Bind workers did not return in 10s", round)
		}
		if host.connects.Load() < wantConnects || link.client.State() != centrifugego.StateConnected {
			t.Fatalf("round %d: not reconnected (connects=%d want>=%d state=%s)", round, host.connects.Load(), wantConnects, link.client.State())
		}
	}
	<-host.connected
	t.Logf("rounds=%d admitted=%d refused=%d reconnecting=%d canceled=%d other=%d stale=%d",
		rounds, admitted.Load(), refused.Load(), reconnecting.Load(), canceled.Load(), other.Load(), stale.Load())
	if stale.Load() != 0 {
		t.Fatalf("%d bind RPCs reached a connection that did not advertise bind", stale.Load())
	}
}

func TestQueuedBoundBindCannotCrossIntoANewerCapabilityGeneration(t *testing.T) {
	t.Parallel()

	host := newLivenessHost(t)
	dialer, err := NewCentrifugeDialer(DialerConfig{
		Credential: livenessCredential("service-token"),
		Version:    "factory-test-build",
		Limits: Limits{
			MaxLinks:     1,
			DialTimeout:  5 * time.Second,
			IdleTimeout:  10 * time.Second,
			ReconnectMin: 100 * time.Millisecond,
			ReconnectMax: 100 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("NewCentrifugeDialer: %v", err)
	}
	linkValue, err := dialer.Dial(context.Background(), host.target, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	link := linkValue.(*centrifugeLink)
	t.Cleanup(func() { _ = link.Close(context.Background()) })
	serverClient := <-host.connected
	link.mu.Lock()
	oldGeneration := link.generation
	link.mu.Unlock()

	// Every wait below is bounded and the gate is always released, because a
	// fixture that cannot fail hangs instead: an earlier version received on
	// entered unbounded and left the gate closed on its Fatal paths, so a Bind
	// that returned before consulting its context parked the package, and a
	// Bind parked inside the gate pinned Cleanup behind it.
	parent, cancelParent := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelParent()
	caller := newDoneGate(parent)
	defer caller.Release()
	bindDone := make(chan error, 1)
	go func() {
		bindDone <- link.Bind(caller, livenessBindRequest(host.target.Host))
	}()
	select {
	case <-caller.entered:
	case err := <-bindDone:
		t.Fatalf("Bind returned %v before consulting its context", err)
	case <-time.After(2 * time.Second):
		t.Fatal("Bind did not consult its context within 2s")
	}

	// The bind has passed its capability snapshot but has not registered its
	// generation binding yet. Reconnect now changes the Host to a
	// capability-less generation. What this pins is the re-check after
	// context.WithCancel: the registry has been cancelled by the time the gate
	// opens, and bind must observe that rather than enqueue the stale RPC. It
	// does NOT pin that the registry's cancellation is synchronous -- the
	// former context.AfterFunc bridge passes this case too, because the
	// reconnect delay is milliseconds and the asynchronous cancel lands in
	// microseconds. TestRPCGenerationCancelIsSynchronous pins that at the unit.
	host.setNegotiation(`{"version":1}`)
	serverClient.Disconnect(centrifuge.Disconnect{Code: 4000, Reason: "test capability change"})
	waitForState(t, link.client, centrifugego.StateConnecting)
	waitForGenerationCanceled(t, oldGeneration)
	caller.Release()

	select {
	case err = <-bindDone:
	case <-time.After(2 * time.Second):
		t.Fatal("queued Bind did not settle before reconnect")
	}
	select {
	case <-host.reconnected:
	case <-time.After(2 * time.Second):
		t.Fatal("reconnect did not complete")
	}
	t.Logf("queued Bind result: %v; Host RPCs: %d", err, host.calls())
	if err == nil {
		t.Fatal("Bind succeeded after its capability generation was replaced")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("queued Bind error = %v, want generation cancellation", err)
	}
	if got := host.calls(); got != 0 {
		t.Fatalf("Host received %d stale bind RPCs after capability removal, want 0", got)
	}
}

func TestRPCGenerationBindingPreservesCallerContext(t *testing.T) {
	t.Parallel()

	type contextKey struct{}
	wantValue := "caller-value"
	wantDeadline := time.Now().Add(time.Hour)
	parent, cancelParent := context.WithDeadline(
		context.WithValue(context.Background(), contextKey{}, wantValue),
		wantDeadline,
	)
	defer cancelParent()

	generation := newRPCGeneration()
	bound, release, err := bindGenerationContext(parent, generation)
	if err != nil {
		t.Fatalf("bindGenerationContext: %v", err)
	}
	defer release()
	if got := bound.Value(contextKey{}); got != wantValue {
		t.Fatalf("bound context value = %v, want %q", got, wantValue)
	}
	if got, ok := bound.Deadline(); !ok || !got.Equal(wantDeadline) {
		t.Fatalf("bound context deadline = %v, %t; want %v, true", got, ok, wantDeadline)
	}

	cancelParent()
	select {
	case <-bound.Done():
		if !errors.Is(bound.Err(), context.Canceled) {
			t.Fatalf("bound context error = %v, want context.Canceled", bound.Err())
		}
	case <-time.After(time.Second):
		t.Fatal("caller cancellation did not reach the bound context")
	}
}

// TestRPCGenerationCancelIsSynchronous pins the registry's one mechanical
// difference from a context.AfterFunc bridge: when cancel returns, every bound
// context is already done. AfterFunc runs its function on a new goroutine, so
// the same select would find the child still live. This is the whole of what
// "synchronous" is proven to mean; whether the transport could ever emit a
// stale RPC in that goroutine-wide window is reasoned, not measured.
func TestRPCGenerationCancelIsSynchronous(t *testing.T) {
	t.Parallel()

	generation := newRPCGeneration()
	bound, release, err := bindGenerationContext(context.Background(), generation)
	if err != nil {
		t.Fatalf("bindGenerationContext: %v", err)
	}
	defer release()

	generation.cancel()
	select {
	case <-bound.Done():
	default:
		t.Fatal("cancel returned before the bound context was done")
	}
	if !errors.Is(bound.Err(), context.Canceled) {
		t.Fatalf("bound context error = %v, want context.Canceled", bound.Err())
	}
	if _, _, err := generation.bind(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("bind after cancel = %v, want context.Canceled", err)
	}
}

type livenessCredential string

func (c livenessCredential) ServiceToken(context.Context) (string, error) { return string(c), nil }

type livenessHost struct {
	target      Target
	connected   chan *centrifuge.Client
	reconnected chan struct{}
	connects    atomic.Int32
	rpcCalls    atomic.Int32
	subscribes  atomic.Int32
	closeOnce   sync.Once
	mu          sync.Mutex
	negotiation string
	// beforeReply, when set, runs on the Host's RPC goroutine before the reply.
	beforeReply func(method string)
	// onRPC, when set, sees every RPC with whether the connection it arrived
	// on advertised bind.
	onRPC  func(advertised bool, method string)
	node   *centrifuge.Node
	server *httptest.Server
}

// livenessAdvertisedKey carries "did this connection's reply advertise bind"
// from OnConnecting to the connection's RPC handler.
type livenessAdvertisedKey struct{}

func newLivenessHost(t *testing.T) *livenessHost {
	t.Helper()
	node, err := centrifuge.New(centrifuge.Config{
		Name:     "host-standin",
		LogLevel: centrifuge.LogLevelNone,
	})
	if err != nil {
		t.Fatalf("centrifuge.New: %v", err)
	}
	host := &livenessHost{
		target:      Target{Host: sessionwire.HostID("host-1")},
		connected:   make(chan *centrifuge.Client, 4),
		reconnected: make(chan struct{}),
		node:        node,
	}
	node.OnConnecting(func(_ context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		request, err := sessionwire.DecodeHostLinkConnectRequest(e.Data)
		if err != nil {
			return centrifuge.ConnectReply{}, centrifuge.DisconnectInappropriateProtocol
		}
		selection, err := sessionwire.NegotiateVersion(request)
		if err != nil {
			return centrifuge.ConnectReply{}, centrifuge.DisconnectInappropriateProtocol
		}
		host.mu.Lock()
		rawNegotiation := host.negotiation
		host.mu.Unlock()
		var body []byte
		switch rawNegotiation {
		case "":
			selection = selection.WithHostLinkMethods(sessionwire.HostLinkMethodBind, sessionwire.HostLinkMethodUnbind)
			body, err = sessionwire.EncodeHostLinkConnectReply(selection)
			if err != nil {
				return centrifuge.ConnectReply{}, centrifuge.DisconnectServerError
			}
		default:
			body = []byte(rawNegotiation)
		}
		if host.connects.Add(1) == 2 {
			close(host.reconnected)
		}
		return centrifuge.ConnectReply{
			Context:     context.WithValue(context.Background(), livenessAdvertisedKey{}, rawNegotiation == ""),
			Credentials: &centrifuge.Credentials{UserID: "factory"},
			Data:        body,
		}, nil
	})
	node.OnConnect(func(client *centrifuge.Client) {
		select {
		case host.connected <- client:
		default:
		}
		advertised, _ := client.Context().Value(livenessAdvertisedKey{}).(bool)
		client.OnRPC(func(e centrifuge.RPCEvent, cb centrifuge.RPCCallback) {
			host.rpcCalls.Add(1)
			host.mu.Lock()
			beforeReply, onRPC := host.beforeReply, host.onRPC
			host.mu.Unlock()
			if onRPC != nil {
				onRPC(advertised, e.Method)
			}
			if beforeReply != nil {
				beforeReply(e.Method)
			}
			// Empty RPC data is the legitimate success shape.
			cb(centrifuge.RPCReply{}, nil)
		})
		client.OnSubscribe(func(_ centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			host.subscribes.Add(1)
			cb(centrifuge.SubscribeReply{}, nil)
		})
	})
	if err := node.Run(); err != nil {
		t.Fatalf("node.Run: %v", err)
	}
	host.server = httptest.NewServer(RequireJSONSubprotocol(centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{
		CheckOrigin: func(*http.Request) bool { return true },
		Compression: false,
	})))
	host.target.Endpoint = sessionwire.InternalEndpoint("ws" + strings.TrimPrefix(host.server.URL, "http"))
	t.Cleanup(func() {
		host.closeOnce.Do(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = node.Shutdown(ctx)
			host.server.Close()
		})
	})
	return host
}

func (h *livenessHost) calls() int { return int(h.rpcCalls.Load()) }

func (h *livenessHost) setBeforeReply(hook func(method string)) {
	h.mu.Lock()
	h.beforeReply = hook
	h.mu.Unlock()
}

func (h *livenessHost) setOnRPC(hook func(advertised bool, method string)) {
	h.mu.Lock()
	h.onRPC = hook
	h.mu.Unlock()
}

// dialLiveness dials the stand-in with a fixed reconnect delay and closes the
// link at cleanup.
func dialLiveness(t *testing.T, host *livenessHost, reconnect time.Duration) *centrifugeLink {
	t.Helper()
	dialer, err := NewCentrifugeDialer(DialerConfig{
		Credential: livenessCredential("service-token"),
		Version:    "factory-test-build",
		Limits: Limits{
			MaxLinks:     1,
			DialTimeout:  5 * time.Second,
			IdleTimeout:  10 * time.Second,
			ReconnectMin: reconnect,
			ReconnectMax: reconnect,
		},
	})
	if err != nil {
		t.Fatalf("NewCentrifugeDialer: %v", err)
	}
	linkValue, err := dialer.Dial(context.Background(), host.target, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	link := linkValue.(*centrifugeLink)
	t.Cleanup(func() { _ = link.Close(context.Background()) })
	return link
}

func waitUntilLiveness(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func waitForState(t *testing.T, client *centrifugego.Client, want centrifugego.State) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if client.State() == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("client State = %s, want %s", client.State(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

func waitForGenerationCanceled(t *testing.T, generation *rpcGeneration) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		generation.mu.Lock()
		canceled := generation.canceled
		generation.mu.Unlock()
		if canceled {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the prior RPC generation was not canceled")
		}
		time.Sleep(time.Millisecond)
	}
}

func (h *livenessHost) setNegotiation(raw string) {
	h.mu.Lock()
	h.negotiation = raw
	h.mu.Unlock()
}

func livenessBindRequest(host sessionwire.HostID) sessionwire.HostLinkBindRequest {
	return sessionwire.HostLinkBindRequest{
		Version:                sessionwire.CurrentWireVersion,
		TenantID:               sessionwire.TenantID("tenant-a"),
		SessionID:              sessionwire.SessionID("s-1"),
		HostID:                 host,
		HostGeneration:         7,
		LeaseEpoch:             3,
		RuntimeCompatibilityID: "runtime-1",
		IdempotencyKey:         "bind-s-1",
	}
}

// doneGate is a context whose Done blocks until released, which holds a call
// still at the exact point it first consults its context.
//
// It leans on an implementation detail of context.WithCancel: propagateCancel
// calls parent.Done() SYNCHRONOUSLY when the parent is not one of the standard
// library's own context types (go1.26.8 context.go, propagateCancel). If a
// future Go release stopped doing that, entered would never close -- and the
// bounded receive in the test that uses it would then FAIL rather than turn
// the case into a silent no-op, which is the reason that receive is bounded.
type doneGate struct {
	context.Context
	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once
	releaseOnce sync.Once
}

func newDoneGate(parent context.Context) *doneGate {
	return &doneGate{
		Context: parent,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (g *doneGate) Done() <-chan struct{} {
	g.enteredOnce.Do(func() { close(g.entered) })
	<-g.release
	return g.Context.Done()
}

// Release opens the gate; it is idempotent so a deferred release after an
// explicit one is safe.
func (g *doneGate) Release() {
	g.releaseOnce.Do(func() { close(g.release) })
}

// TestAnAttachInTheConnectingWindowIsRefusedAsReconnectingNotUnsupported is
// the bind case's rule for attach (B5 spec gate G3). Between a dropped
// connection and the next reply there is no capability set, so an attach is
// refused with the transient ErrLinkReconnecting -- which placement treats as
// unreachable -- and never as a Host that "does not advertise attach", which
// placement would log and exclude as a capability fact.
func TestAnAttachInTheConnectingWindowIsRefusedAsReconnectingNotUnsupported(t *testing.T) {
	t.Parallel()

	host := newLivenessHost(t)
	link := dialLiveness(t, host, 300*time.Millisecond)
	serverClient := <-host.connected

	host.setNegotiation(`{"version":1}`)
	serverClient.Disconnect(centrifuge.Disconnect{Code: 4000, Reason: "test capability change"})
	waitForState(t, link.client, centrifugego.StateConnecting)
	time.Sleep(20 * time.Millisecond)
	if got := link.client.State(); got != centrifugego.StateConnecting {
		t.Fatalf("left the Connecting window early: %s", got)
	}

	_, err := link.Attach(context.Background(), sessionwire.HostLinkAttachRequest{
		Version: sessionwire.CurrentWireVersion, TenantID: "tenant-a", SessionID: "s-1",
		HostID: host.target.Host, HostGeneration: 7, AgentID: "agent-1", RuntimeCompatibilityID: "runtime-1",
		Mode: sessionwire.HostLinkAttachModeCreate, ActorID: "factory-service", IdempotencyKey: "attach-s-1",
	})
	if !errors.Is(err, ErrLinkReconnecting) {
		t.Fatalf("Attach in the Connecting window = %v, want ErrLinkReconnecting", err)
	}
	var unsupported *UnsupportedMethodError
	if errors.As(err, &unsupported) || errors.Is(err, ErrUnsupportedMethod) {
		t.Fatalf("Attach in the Connecting window reported a transient as a capability fact: %v", err)
	}
}

// TestASubscribeInTheConnectingWindowIsRefusedAsTransientAndNeverSent: a
// subscribe issued between a dropped connection and the next reply would be
// QUEUED by the transport and sent on the new connection before anything was
// re-bound there. It is refused locally with ErrLinkReconnecting, and the Host
// receives no subscribe once the link is back.
//
// The window is found by the client's own state, not by polling an RPC: an
// RPC racing the close trips centrifuge-go's known data race.
func TestASubscribeInTheConnectingWindowIsRefusedAsTransientAndNeverSent(t *testing.T) {
	t.Parallel()

	host := newLivenessHost(t)
	link := dialLiveness(t, host, 300*time.Millisecond)
	serverClient := <-host.connected

	serverClient.Disconnect(centrifuge.Disconnect{Code: 4000, Reason: "test reconnect"})
	waitForState(t, link.client, centrifugego.StateConnecting)
	time.Sleep(20 * time.Millisecond)
	if got := link.client.State(); got != centrifugego.StateConnecting {
		t.Fatalf("left the Connecting window early: %s", got)
	}
	err := link.Subscribe(context.Background(), "tenant-a", "s-1", nopSink{})
	if !errors.Is(err, ErrLinkReconnecting) {
		t.Fatalf("Subscribe in the Connecting window = %v, want ErrLinkReconnecting", err)
	}
	select {
	case <-host.reconnected:
	case <-time.After(3 * time.Second):
		t.Fatal("reconnect did not complete")
	}
	time.Sleep(200 * time.Millisecond)
	if got := host.subscribes.Load(); got != 0 {
		t.Fatalf("the Host received %d subscribes: the refused one was queued and sent", got)
	}
}

type nopSink struct{}

func (nopSink) Subscribed()        {}
func (nopSink) Publication([]byte) {}
func (nopSink) Ended()             {}
func (nopSink) Restored()          {}
