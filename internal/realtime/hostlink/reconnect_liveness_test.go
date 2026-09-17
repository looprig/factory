package hostlink

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	centrifuge "github.com/centrifugal/centrifuge"
	centrifugego "github.com/centrifugal/centrifuge-go"
	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// TestReconnectDoesNotWaitForAnRPCAdmissionLock uses the real Centrifuge
// transport to pin the ordering that used to deadlock:
//
//  1. a caller owns the local RPC serialization lock;
//  2. the Host moves the transport to Connecting;
//  3. another RPC enters the transport's connect-future queue; and
//  4. the transport must still run its OnConnecting callback and schedule the
//     reconnect.
//
// The client callback is synchronous and runs before centrifuge-go schedules
// the reconnect. Holding the lock across client.RPC therefore made the
// callback wait for an RPC that was itself waiting for the callback's future.
// This test holds the same lock and drives the real client RPC at the exact
// Connecting barrier, so it does not depend on a scheduler race.
func TestReconnectDoesNotWaitForAnRPCAdmissionLock(t *testing.T) {
	t.Parallel()

	host := newLivenessHost(t)
	dialer, err := NewCentrifugeDialer(DialerConfig{
		Credential: livenessCredential("service-token"),
		Version:    "factory-test-build",
		Limits: Limits{
			MaxLinks:     1,
			DialTimeout:  5 * time.Second,
			IdleTimeout:  10 * time.Second,
			ReconnectMin: 10 * time.Millisecond,
			ReconnectMax: 20 * time.Millisecond,
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
	link.rpcMu.Lock()
	defer link.rpcMu.Unlock()
	serverClient.Disconnect(centrifuge.Disconnect{Code: 4000, Reason: "test reconnect"})

	waitForState(t, link.client, centrifugego.StateConnecting)

	// With the lock held, this RPC is guaranteed to enter centrifuge-go's
	// connect-future queue after the transport has entered Connecting. It is
	// deliberately unbounded: liveness must not depend on a caller timeout.
	rpcDone := make(chan error, 1)
	go func() {
		_, err := link.client.RPC(context.Background(), "hostlink.v1.test", nil)
		rpcDone <- err
	}()

	select {
	case <-host.reconnected:
	case <-time.After(2 * time.Second):
		t.Fatal("reconnect did not reach the Host while an RPC admission lock was held")
	}
	select {
	case err := <-rpcDone:
		if err != nil {
			t.Fatalf("queued RPC after reconnect: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("queued RPC did not settle after reconnect")
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

	caller := newDoneGate(context.Background())
	bindDone := make(chan error, 1)
	go func() {
		bindDone <- link.Bind(caller, livenessBindRequest(host.target.Host))
	}()
	<-caller.entered

	// The bind has passed its capability snapshot but has not registered its
	// generation binding yet. Reconnect now changes the Host to a
	// capability-less generation. The old context.AfterFunc bridge could then
	// let Bind enter centrifuge-go's connect-future queue before its callback
	// ran; the generation registry instead observes cancellation before it can
	// enqueue that stale RPC.
	host.setNegotiation(`{"version":1}`)
	serverClient.Disconnect(centrifuge.Disconnect{Code: 4000, Reason: "test capability change"})
	waitForState(t, link.client, centrifugego.StateConnecting)
	waitForGenerationCanceled(t, oldGeneration)
	close(caller.release)

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

type livenessCredential string

func (c livenessCredential) ServiceToken(context.Context) (string, error) { return string(c), nil }

type livenessHost struct {
	target      Target
	connected   chan *centrifuge.Client
	reconnected chan struct{}
	connects    atomic.Int32
	rpcCalls    atomic.Int32
	closeOnce   sync.Once
	mu          sync.Mutex
	negotiation string
	node        *centrifuge.Node
	server      *httptest.Server
}

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
			Credentials: &centrifuge.Credentials{UserID: "factory"},
			Data:        body,
		}, nil
	})
	node.OnConnect(func(client *centrifuge.Client) {
		select {
		case host.connected <- client:
		default:
		}
		client.OnRPC(func(_ centrifuge.RPCEvent, cb centrifuge.RPCCallback) {
			// Empty RPC data is the legitimate success shape.
			host.rpcCalls.Add(1)
			cb(centrifuge.RPCReply{}, nil)
		})
	})
	if err := node.Run(); err != nil {
		t.Fatalf("node.Run: %v", err)
	}
	host.server = httptest.NewServer(centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{
		CheckOrigin: func(*http.Request) bool { return true },
		Compression: false,
	}))
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

type doneGate struct {
	context.Context
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newDoneGate(parent context.Context) *doneGate {
	return &doneGate{
		Context: parent,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (g *doneGate) Done() <-chan struct{} {
	g.once.Do(func() { close(g.entered) })
	<-g.release
	return g.Context.Done()
}
