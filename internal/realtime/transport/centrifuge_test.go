package transport_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	centrifuge "github.com/centrifugal/centrifuge"
	centrifugego "github.com/centrifugal/centrifuge-go"
)

// Everything below runs against a real embedded Centrifuge node over a real
// loopback WebSocket, driven by the real Go client. There is no fake transport
// anywhere in this file: a fake would answer whatever it was written to answer,
// which is exactly the question a qualification task exists to avoid begging.

// The credential the embedded server accepts. Exactly one value is accepted,
// so "the connect callback authenticated" stays distinguishable from "the
// connect callback ran and let everything through".
const (
	goodToken = "transport-spike-token"
	badToken  = "not-the-token"

	// serverVersion is echoed to a client on connect. It is an arbitrary
	// absolute literal and deliberately not the module version, so a client
	// that reported the SDK's own version instead of the server's would fail.
	serverVersion = "factory-transport-0"

	clientName        = "looprig-factory"
	clientVersionSent = "transport-spike"
)

// waitFor is how long a case waits for a transport event. It is generous
// because this suite does not control the machine's load; nothing here asserts
// that anything is FAST.
const waitFor = 30 * time.Second

// serverOptions is the whole surface a case may vary. Everything absent is off:
// see the package doc for why "off" is asserted rather than assumed.
type serverOptions struct {
	// authorizeSubscribe decides a subscribe. nil allows every channel.
	authorizeSubscribe func(channel string) error
	// rpc answers an RPC. nil registers NO RPC handler.
	rpc func(method string, data []byte) ([]byte, error)
	// allowPublish registers a publish handler. Left false, client
	// publication has no handler at all and the server refuses it.
	allowPublish bool
	// queueMaxSize bounds one connection's outbound queue in BYTES.
	queueMaxSize int
	// writeTimeout bounds one websocket write.
	writeTimeout time.Duration
	// pingInterval and pongTimeout drive the server's application-level ping.
	pingInterval time.Duration
	pongTimeout  time.Duration
	// onDisconnect observes why the server let a connection go.
	onDisconnect func(centrifuge.DisconnectEvent)
	// onSubscribed observes what the server agreed to.
	onSubscribed func(centrifuge.SubscribeEvent)
	// compression sets WebsocketConfig.Compression. It exists so that the
	// handshake case can assert the REFUSAL against a positive control in
	// which the same probe observes an acceptance; without the control the
	// refusal would be indistinguishable from a probe that never offered.
	compression bool
	// onConnect hands a case the SERVER's side of a connection. It is an
	// option rather than a second node.OnConnect call because that method is a
	// setter: registering a second handler would silently discard the first
	// and take every per-client handler above with it.
	onConnect func(*centrifuge.Client)
}

// spikeServer is one embedded Centrifuge node behind one loopback HTTP server.
type spikeServer struct {
	node *centrifuge.Node
	url  string
	// addr is the loopback host:port the handler is mounted on, for the one
	// case that speaks the WebSocket handshake itself instead of through a
	// client library.
	addr string
}

// newServer starts an embedded node. Every disabled feature is disabled HERE,
// in one place, so a case cannot quietly re-enable one.
func newServer(t *testing.T, opts serverOptions) *spikeServer {
	t.Helper()

	node, err := centrifuge.New(centrifuge.Config{
		Version:  serverVersion,
		Name:     "factory-transport-spike",
		LogLevel: centrifuge.LogLevelNone,
		// No GetBroker and no GetPresenceManager: the node keeps its default
		// in-process memory broker. Nothing here reaches Redis or NATS, and a
		// standalone Centrifugo is never started.
		ClientQueueMaxSize: opts.queueMaxSize,
	})
	if err != nil {
		t.Fatalf("centrifuge.New: %v", err)
	}

	node.OnConnecting(func(_ context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		if e.Token != goodToken {
			// Returning a Disconnect rather than an Error is the terminal
			// answer: this credential will not become valid by retrying.
			return centrifuge.ConnectReply{}, centrifuge.DisconnectInvalidToken
		}
		reply := centrifuge.ConnectReply{
			Credentials: &centrifuge.Credentials{UserID: "user-" + e.Name},
			// The client's own name and version are echoed back so a case can
			// assert the server RECEIVED them rather than inferring it from a
			// successful connect.
			Data: []byte(fmt.Sprintf(`{"client_name":%q,"client_version":%q}`, e.Name, e.Version)),
		}
		if opts.pingInterval > 0 {
			reply.PingPongConfig = &centrifuge.PingPongConfig{
				PingInterval: opts.pingInterval,
				PongTimeout:  opts.pongTimeout,
			}
		}
		return reply, nil
	})

	node.OnConnect(func(client *centrifuge.Client) {
		client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			if opts.authorizeSubscribe != nil {
				if err := opts.authorizeSubscribe(e.Channel); err != nil {
					cb(centrifuge.SubscribeReply{}, err)
					return
				}
			}
			if opts.onSubscribed != nil {
				opts.onSubscribed(e)
			}
			// The zero SubscribeOptions is the configuration this workspace
			// wants: no EmitPresence, no EmitJoinLeave, no PushJoinLeave, no
			// EnablePositioning and no EnableRecovery. Naming the zero value
			// explicitly is the point -- it is a decision, not an omission.
			cb(centrifuge.SubscribeReply{Options: centrifuge.SubscribeOptions{}}, nil)
		})
		if opts.rpc != nil {
			client.OnRPC(func(e centrifuge.RPCEvent, cb centrifuge.RPCCallback) {
				data, err := opts.rpc(e.Method, e.Data)
				if err != nil {
					cb(centrifuge.RPCReply{}, err)
					return
				}
				cb(centrifuge.RPCReply{Data: data}, nil)
			})
		}
		if opts.allowPublish {
			client.OnPublish(func(_ centrifuge.PublishEvent, cb centrifuge.PublishCallback) {
				cb(centrifuge.PublishReply{}, nil)
			})
		}
		if opts.onDisconnect != nil {
			client.OnDisconnect(opts.onDisconnect)
		}
		if opts.onConnect != nil {
			opts.onConnect(client)
		}
	})

	if err := node.Run(); err != nil {
		t.Fatalf("node.Run: %v", err)
	}

	handler := centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{
		// Loopback only; the test's own origin is not a deployment decision.
		CheckOrigin: func(*http.Request) bool { return true },
		// Compression is OFF by default. It is a decision: permessage-deflate
		// is a per-connection memory and CPU cost at the 1,000-5,000
		// connection scale this transport is sized for.
		Compression:    opts.compression,
		WriteTimeout:   opts.writeTimeout,
		PingPongConfig: centrifuge.PingPongConfig{PingInterval: opts.pingInterval, PongTimeout: opts.pongTimeout},
	})
	httpServer := httptest.NewServer(handler)
	t.Cleanup(func() {
		// Bounded Shutdown first: an unbounded Close waits for the outstanding
		// request that a wedged read loop is still holding, so this order is
		// the difference between a failing case and a ten-minute package
		// timeout. TestEveryBoundedShutdownPrecedesAnUnboundedClose censuses it.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = node.Shutdown(ctx)
		httpServer.Close()
	})

	return &spikeServer{
		node: node,
		url:  "ws" + strings.TrimPrefix(httpServer.URL, "http"),
		addr: strings.TrimPrefix(httpServer.URL, "http://"),
	}
}

// events collects what a client observed, so an assertion is made against a
// recorded transition rather than against a sleep.
type events struct {
	connected    chan centrifugego.ConnectedEvent
	disconnected chan centrifugego.DisconnectedEvent
	errs         chan error
}

func newEvents() *events {
	return &events{
		connected:    make(chan centrifugego.ConnectedEvent, 8),
		disconnected: make(chan centrifugego.DisconnectedEvent, 8),
		errs:         make(chan error, 32),
	}
}

// dial builds a JSON client against the embedded server and connects it.
func dial(t *testing.T, server *spikeServer, token string) (*centrifugego.Client, *events) {
	t.Helper()

	return dialAs(t, server, token, clientName, clientVersionSent)
}

// dialAs is dial with the client identity the connect exchange carries made
// explicit, for the one case that must vary it.
func dialAs(t *testing.T, server *spikeServer, token, name, version string) (*centrifugego.Client, *events) {
	t.Helper()

	client := centrifugego.NewJsonClient(server.url, centrifugego.Config{
		Token:   token,
		Name:    name,
		Version: version,
	})
	observed := newEvents()
	client.OnConnected(func(e centrifugego.ConnectedEvent) { send(observed.connected, e) })
	client.OnDisconnected(func(e centrifugego.DisconnectedEvent) { send(observed.disconnected, e) })
	client.OnError(func(e centrifugego.ErrorEvent) { send(observed.errs, e.Error) })
	t.Cleanup(client.Close)

	if err := client.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	return client, observed
}

// send never blocks a transport callback. A callback that blocked would change
// the very behaviour these cases measure.
func send[T any](ch chan T, value T) {
	select {
	case ch <- value:
	default:
	}
}

// await returns the next value or fails naming what it waited for.
func await[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()

	select {
	case value := <-ch:
		return value
	case <-time.After(waitFor):
		var zero T
		t.Fatalf("no %s arrived within %v", what, waitFor)
		return zero
	}
}

// codeOf reports a Centrifuge protocol error code, or 0 if err is not one.
func codeOf(err error) uint32 {
	var protocolErr *centrifugego.Error
	if errors.As(err, &protocolErr) {
		return protocolErr.Code
	}
	return 0
}

// ---------------------------------------------------------------------------
// Step 2 -- version negotiation and the authentication callback.
// ---------------------------------------------------------------------------

// TestTheServerReceivesTheClientNameAndVersionAndAnswersWithItsOwn is version
// negotiation in both directions over one real connect exchange.
//
// Two clients present DIFFERENT identities, and each must see its own reflected
// back. One client alone could not tell "the server received what this client
// sent" from "the server echoed a constant that happens to match", which is
// exactly the shape of test this workspace has been burned by before.
func TestTheServerReceivesTheClientNameAndVersionAndAnswersWithItsOwn(t *testing.T) {
	t.Parallel()

	server := newServer(t, serverOptions{})
	// Absolute literals, both different from the package defaults and from
	// each other.
	identities := []struct{ name, version string }{
		{name: "looprig-factory", version: "transport-spike"},
		{name: "some-other-client", version: "9.9.9-rc1"},
	}
	for _, identity := range identities {
		t.Run(identity.name, func(t *testing.T) {
			t.Parallel()

			_, observed := dialAs(t, server, goodToken, identity.name, identity.version)
			connected := await(t, observed.connected, "connected event")

			if connected.Version != serverVersion {
				t.Errorf("server version reported to the client = %q, want %q", connected.Version, serverVersion)
			}
			if connected.ClientID == "" {
				t.Error("the server assigned no client identifier")
			}
			// The connect reply carries back what the SERVER saw, which is the
			// only way to assert this client's name and version crossed the
			// wire rather than being remembered locally or hard-coded.
			body := string(connected.Data)
			for _, want := range []string{
				`"client_name":"` + identity.name + `"`,
				`"client_version":"` + identity.version + `"`,
			} {
				if !strings.Contains(body, want) {
					t.Errorf("connect reply %q does not contain %q", body, want)
				}
			}
		})
	}
}

// TestAnUnacceptedCredentialIsRefusedTerminally is the authentication callback.
// It matters that the refusal is TERMINAL: a client that treated it as a
// transient disconnect would reconnect forever against a credential that will
// never become valid.
func TestAnUnacceptedCredentialIsRefusedTerminally(t *testing.T) {
	t.Parallel()

	server := newServer(t, serverOptions{})
	_, observed := dial(t, server, badToken)

	disconnected := await(t, observed.disconnected, "disconnected event")
	if disconnected.Code != centrifuge.DisconnectInvalidToken.Code {
		t.Errorf("disconnect code = %d, want %d (invalid token)", disconnected.Code, centrifuge.DisconnectInvalidToken.Code)
	}
	if disconnected.Reason == "" {
		t.Error("the refusal carried no reason")
	}
	select {
	case connected := <-observed.connected:
		t.Errorf("a refused credential still connected: %+v", connected)
	default:
	}
}

// ---------------------------------------------------------------------------
// Step 2 -- JSON RPC correlation.
// ---------------------------------------------------------------------------

// TestConcurrentRPCsAreCorrelatedToTheirOwnReplies is the property Factory's
// command RPCs depend on and the one a multiplexed protocol can get wrong: many
// requests are in flight on ONE connection at once, and each caller must
// receive its own answer.
//
// The server deliberately answers out of order -- an even-numbered request
// sleeps -- so a transport that paired replies by arrival order rather than by
// request identity would fail rather than pass by luck.
func TestConcurrentRPCsAreCorrelatedToTheirOwnReplies(t *testing.T) {
	t.Parallel()

	const calls = 24
	server := newServer(t, serverOptions{
		rpc: func(method string, data []byte) ([]byte, error) {
			if method != "echo" {
				return nil, centrifuge.ErrorMethodNotFound
			}
			if len(data)%2 == 0 {
				time.Sleep(5 * time.Millisecond)
			}
			return data, nil
		},
	})
	client, observed := dial(t, server, goodToken)
	await(t, observed.connected, "connected event")

	var wg sync.WaitGroup
	mismatched := make(chan string, calls)
	for i := range calls {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), waitFor)
			defer cancel()
			// Each payload is distinct and its own length varies, which is
			// what makes the out-of-order answer above reachable. It is a
			// JSON string because the JSON protocol carries RPC data as JSON:
			// raw bytes would fail to encode in the reply and cost the whole
			// connection, which this case would then be measuring instead.
			payload := `"` + strings.Repeat("x", i+1) + `"`
			result, err := client.RPC(ctx, "echo", []byte(payload))
			if err != nil {
				mismatched <- fmt.Sprintf("RPC %d = %v", i, err)
				return
			}
			if string(result.Data) != payload {
				mismatched <- fmt.Sprintf("RPC %d received %q, want its own payload %q", i, result.Data, payload)
			}
		}()
	}
	wg.Wait()
	close(mismatched)
	for failure := range mismatched {
		t.Error(failure)
	}
}

// TestAnUnknownRPCMethodIsAnErrorAndNotADisconnect keeps a caller's mistake
// from costing every other multiplexed caller on the same connection.
func TestAnUnknownRPCMethodIsAnErrorAndNotADisconnect(t *testing.T) {
	t.Parallel()

	server := newServer(t, serverOptions{
		rpc: func(method string, data []byte) ([]byte, error) {
			if method != "echo" {
				return nil, centrifuge.ErrorMethodNotFound
			}
			return data, nil
		},
	})
	client, observed := dial(t, server, goodToken)
	await(t, observed.connected, "connected event")

	ctx, cancel := context.WithTimeout(context.Background(), waitFor)
	defer cancel()
	if _, err := client.RPC(ctx, "no-such-method", []byte(`{}`)); err == nil {
		t.Fatal("an unknown RPC method was answered without an error")
	} else if got := codeOf(err); got != centrifuge.ErrorMethodNotFound.Code {
		t.Errorf("error code = %d (%v), want %d", got, err, centrifuge.ErrorMethodNotFound.Code)
	}

	// The connection survived, and still answers.
	result, err := client.RPC(ctx, "echo", []byte(`"still here"`))
	if err != nil {
		t.Fatalf("the connection did not survive a failed RPC: %v", err)
	}
	if string(result.Data) != `"still here"` {
		t.Errorf("echo returned %q", result.Data)
	}
	select {
	case d := <-observed.disconnected:
		t.Errorf("a failed RPC disconnected the connection: %+v", d)
	default:
	}
}

// ---------------------------------------------------------------------------
// Step 2 -- multiplexed subscriptions, and a denial's blast radius.
// ---------------------------------------------------------------------------

// subscribe creates and subscribes a channel, returning its publication and
// error channels.
func subscribe(t *testing.T, client *centrifugego.Client, channel string) (<-chan []byte, <-chan error, <-chan centrifugego.UnsubscribedEvent) {
	t.Helper()

	sub, err := client.NewSubscription(channel)
	if err != nil {
		t.Fatalf("NewSubscription(%q): %v", channel, err)
	}
	publications := make(chan []byte, 64)
	errs := make(chan error, 8)
	unsubscribed := make(chan centrifugego.UnsubscribedEvent, 8)
	sub.OnPublication(func(e centrifugego.PublicationEvent) { send(publications, e.Data) })
	sub.OnError(func(e centrifugego.SubscriptionErrorEvent) { send(errs, e.Error) })
	sub.OnUnsubscribed(func(e centrifugego.UnsubscribedEvent) { send(unsubscribed, e) })
	if err := sub.Subscribe(); err != nil {
		t.Fatalf("Subscribe(%q): %v", channel, err)
	}
	return publications, errs, unsubscribed
}

// TestOneConnectionCarriesManySubscriptionsIndependently is the property that
// makes one HostLink per Host, and one browser link per app, possible at all.
func TestOneConnectionCarriesManySubscriptionsIndependently(t *testing.T) {
	t.Parallel()

	server := newServer(t, serverOptions{})
	client, observed := dial(t, server, goodToken)
	await(t, observed.connected, "connected event")

	channels := []string{"session:tenant-a:s1", "session:tenant-a:s2", "session:tenant-a:s3"}
	delivered := map[string]<-chan []byte{}
	for _, channel := range channels {
		publications, _, _ := subscribe(t, client, channel)
		delivered[channel] = publications
	}
	// A subscribe reply is not observed here; publishing until each channel
	// answers is what proves the subscription is live, and it costs what the
	// machine costs rather than asserting a duration.
	for _, channel := range channels {
		awaitPublication(t, server, channel, delivered[channel], []byte(`{"for":"`+channel+`"}`))
	}
	// Nothing leaked across subscriptions: each channel received its own
	// payload and no other channel's.
	for _, channel := range channels {
		select {
		case extra := <-delivered[channel]:
			if !strings.Contains(string(extra), channel) {
				t.Errorf("%s received another channel's publication %q", channel, extra)
			}
		default:
		}
	}
}

// awaitPublication publishes repeatedly until the subscription delivers, which
// removes the race between "subscribed" and "published" without sleeping.
func awaitPublication(t *testing.T, server *spikeServer, channel string, publications <-chan []byte, payload []byte) {
	t.Helper()

	deadline := time.Now().Add(waitFor)
	for {
		if _, err := server.node.Publish(channel, payload); err != nil {
			t.Fatalf("Publish(%q): %v", channel, err)
		}
		select {
		case got := <-publications:
			if string(got) != string(payload) {
				t.Errorf("%s delivered %q, want %q", channel, got, payload)
			}
			return
		case <-time.After(20 * time.Millisecond):
			if time.Now().After(deadline) {
				t.Fatalf("%s delivered no publication within %v", channel, waitFor)
			}
		}
	}
}

// TestARefusedSubscriptionDoesNotCostTheConnection is the authorization blast
// radius, measured rather than assumed. Factory authorizes every subscribe; a
// denial that killed the whole link would take a browser's other sessions down
// with the one it was not allowed to see.
func TestARefusedSubscriptionDoesNotCostTheConnection(t *testing.T) {
	t.Parallel()

	server := newServer(t, serverOptions{
		authorizeSubscribe: func(channel string) error {
			if channel == "session:tenant-b:forbidden" {
				return centrifuge.ErrorPermissionDenied
			}
			return nil
		},
	})
	client, observed := dial(t, server, goodToken)
	await(t, observed.connected, "connected event")

	allowed, _, _ := subscribe(t, client, "session:tenant-a:allowed")
	_, refusedErrs, _ := subscribe(t, client, "session:tenant-b:forbidden")

	err := await(t, refusedErrs, "subscription error")
	if got := codeOf(err); got != centrifuge.ErrorPermissionDenied.Code {
		t.Errorf("refusal code = %d (%v), want %d", got, err, centrifuge.ErrorPermissionDenied.Code)
	}
	select {
	case d := <-observed.disconnected:
		t.Fatalf("a refused subscription disconnected the whole connection: %+v", d)
	default:
	}
	// The permitted subscription on the SAME connection still works.
	awaitPublication(t, server, "session:tenant-a:allowed", allowed, []byte(`{"still":"live"}`))
}

// ---------------------------------------------------------------------------
// Step 3 -- the slow-subscriber blast radius, measured.
// ---------------------------------------------------------------------------

// stalledConsumer runs one slow-consumer scenario to its ending and returns the
// CONNECTION-level disconnect the server recorded.
//
// It also asserts the blast radius, which is the same whichever bound fires and
// is the finding step 3 asks for: the outbound queue and the write deadline are
// both per CONNECTION, so a consumer that cannot keep up costs every
// subscription multiplexed onto its link. Selective reset of one subscription
// does not happen and must not be assumed -- which is why A7.3's per-binding
// repair cannot be delegated to the transport.
func stalledConsumer(t *testing.T, queueMaxSize int, writeTimeout time.Duration) centrifuge.DisconnectEvent {
	t.Helper()

	disconnects := make(chan centrifuge.DisconnectEvent, 8)
	server := newServer(t, serverOptions{
		queueMaxSize: queueMaxSize,
		writeTimeout: writeTimeout,
		onDisconnect: func(e centrifuge.DisconnectEvent) { send(disconnects, e) },
	})
	client, observed := dial(t, server, goodToken)
	await(t, observed.connected, "connected event")

	blocked := make(chan struct{})
	var release sync.Once
	unblock := func() { release.Do(func() { close(blocked) }) }
	defer unblock()

	sub, err := client.NewSubscription("session:tenant-a:slow")
	if err != nil {
		t.Fatalf("NewSubscription: %v", err)
	}
	// This callback BLOCKS, which is what a slow consumer IS.
	sub.OnPublication(func(centrifugego.PublicationEvent) { <-blocked })
	if err := sub.Subscribe(); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// A second subscription on the same connection, whose fate is the
	// measurement: if the transport reset only the slow subscription, this one
	// would stay subscribed.
	//
	// What is observed is the SUBSCRIBING transition rather than an
	// unsubscribe, and the difference is itself part of the finding: the Go
	// client does not report a lost connection as an unsubscribe, it moves
	// every subscription back to subscribing and reconnects. A consumer that
	// waited for an unsubscribed event to learn its subscription had been
	// interrupted would wait forever.
	healthySub, err := client.NewSubscription("session:tenant-a:healthy")
	if err != nil {
		t.Fatalf("NewSubscription: %v", err)
	}
	other := make(chan []byte, 8)
	interrupted := make(chan centrifugego.SubscribingEvent, 8)
	healthySubscribed := make(chan struct{}, 1)
	healthySub.OnPublication(func(e centrifugego.PublicationEvent) { send(other, e.Data) })
	healthySub.OnSubscribed(func(centrifugego.SubscribedEvent) { send(healthySubscribed, struct{}{}) })
	healthySub.OnSubscribing(func(e centrifugego.SubscribingEvent) { send(interrupted, e) })
	if err := healthySub.Subscribe(); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	await(t, healthySubscribed, "healthy subscription")
	awaitPublication(t, server, "session:tenant-a:healthy", other, []byte(`{"healthy":true}`))
	// Drain the subscribing transition that reaching "subscribed" produced, so
	// what is awaited below is the INTERRUPTION and not the initial subscribe.
	select {
	case <-interrupted:
	default:
	}

	payload := []byte(`"` + strings.Repeat("p", 4096) + `"`)
	deadline := time.Now().Add(waitFor)
	for {
		if _, err := server.node.Publish("session:tenant-a:slow", payload); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		select {
		case event := <-disconnects:
			// The stalled consumer is released only NOW. Until it is, the
			// client's own event loop is stuck inside it and cannot report
			// anything -- which is itself part of the blast radius: a slow
			// consumer does not merely lose its connection, it stops the
			// client from observing that it lost it.
			unblock()
			select {
			case <-interrupted:
			case <-time.After(waitFor):
				t.Error("the healthy subscription outlived the connection its slow sibling closed")
			}
			return event
		case <-time.After(10 * time.Millisecond):
			if time.Now().After(deadline) {
				t.Fatalf("a stalled consumer did not end the connection within %v", waitFor)
			}
		}
	}
}

// TestAStalledConsumerOverflowsItsConnectionQueue is the QUEUE bound, measured
// with a write deadline far too long to be the cause. Splitting the two bounds
// apart is not tidiness: with both tight, either one ending the connection
// satisfies a case that accepts either code, and raising the queue by four
// thousandfold would leave that case passing on the write deadline's back.
func TestAStalledConsumerOverflowsItsConnectionQueue(t *testing.T) {
	t.Parallel()

	// 4 KiB of queue, and a write deadline the case could not reach.
	event := stalledConsumer(t, 1<<12, 30*time.Second)
	if event.Code != centrifuge.DisconnectSlow.Code {
		t.Errorf("a stalled consumer with a 4 KiB queue ended with code %d (%s), want %d (slow)",
			event.Code, event.Reason, centrifuge.DisconnectSlow.Code)
	}
}

// TestAStalledConsumerTripsTheConnectionWriteDeadline is the other bound, with
// a queue 16 MiB deep so the queue cannot be what fired.
func TestAStalledConsumerTripsTheConnectionWriteDeadline(t *testing.T) {
	t.Parallel()

	event := stalledConsumer(t, 1<<24, 50*time.Millisecond)
	if event.Code != centrifuge.DisconnectWriteError.Code {
		t.Errorf("a stalled consumer behind a 50ms write deadline ended with code %d (%s), want %d (write error)",
			event.Code, event.Reason, centrifuge.DisconnectWriteError.Code)
	}
}

// ---------------------------------------------------------------------------
// Step 4 -- the features that must be off.
// ---------------------------------------------------------------------------

// TestClientPublicationHistoryAndPresenceAreAllRefused asserts the disabled
// half of the configuration, because "we did not enable it" is not a property a
// later configuration change can violate but "the server refuses it" is.
func TestClientPublicationHistoryAndPresenceAreAllRefused(t *testing.T) {
	t.Parallel()

	server := newServer(t, serverOptions{})
	client, observed := dial(t, server, goodToken)
	await(t, observed.connected, "connected event")

	const channel = "session:tenant-a:s1"
	sub, err := client.NewSubscription(channel)
	if err != nil {
		t.Fatalf("NewSubscription: %v", err)
	}
	subscribed := make(chan struct{}, 1)
	sub.OnSubscribed(func(centrifugego.SubscribedEvent) { send(subscribed, struct{}{}) })
	if err := sub.Subscribe(); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	await(t, subscribed, "subscribed event")

	ctx, cancel := context.WithTimeout(context.Background(), waitFor)
	defer cancel()

	refusals := []struct {
		name string
		call func() error
	}{
		{name: "client publication", call: func() error {
			_, err := client.Publish(ctx, channel, []byte(`{"from":"client"}`))
			return err
		}},
		{name: "history", call: func() error {
			_, err := sub.History(ctx)
			return err
		}},
		{name: "presence", call: func() error {
			_, err := sub.Presence(ctx)
			return err
		}},
		{name: "presence stats", call: func() error {
			_, err := sub.PresenceStats(ctx)
			return err
		}},
	}
	// The four refusals above are all CLIENT-side: they hold that a browser
	// cannot ask. They say nothing about whether the subscription is emitting
	// presence into the node, which is a separate switch (SubscribeOptions.
	// EmitPresence) and which no client call can observe. So it is asserted
	// from the server, where it is visible.
	stats, err := server.node.PresenceStats(channel)
	if err != nil {
		t.Fatalf("node.PresenceStats: %v", err)
	}
	if stats.NumClients != 0 || stats.NumUsers != 0 {
		t.Errorf("the channel has presence: %d clients, %d users; presence is deliberately off",
			stats.NumClients, stats.NumUsers)
	}

	for _, refusal := range refusals {
		t.Run(refusal.name, func(t *testing.T) {
			err := refusal.call()
			if err == nil {
				t.Fatalf("%s was permitted; it must be refused", refusal.name)
			}
			if got := codeOf(err); got != centrifuge.ErrorNotAvailable.Code {
				t.Errorf("%s = %v (code %d), want code %d (not available)",
					refusal.name, err, got, centrifuge.ErrorNotAvailable.Code)
			}
		})
	}
}

// TestASubscriptionIsNeitherPositionedNorRecoverable holds the recovery
// decision at the wire. Sessions are durable in SessionStore and a client
// resumes from a SessionStore cursor; a transport that also offered a stream
// position would be a second, weaker answer to the same question.
func TestASubscriptionIsNeitherPositionedNorRecoverable(t *testing.T) {
	t.Parallel()

	server := newServer(t, serverOptions{})
	client, observed := dial(t, server, goodToken)
	await(t, observed.connected, "connected event")

	sub, err := client.NewSubscription("session:tenant-a:s1")
	if err != nil {
		t.Fatalf("NewSubscription: %v", err)
	}
	subscribed := make(chan centrifugego.SubscribedEvent, 1)
	sub.OnSubscribed(func(e centrifugego.SubscribedEvent) { send(subscribed, e) })
	if err := sub.Subscribe(); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	event := await(t, subscribed, "subscribed event")
	if event.Positioned {
		t.Error("the subscription is positioned; positioning is deliberately off")
	}
	if event.Recoverable {
		t.Error("the subscription is recoverable; Centrifuge history is not Looprig's session cursor")
	}
	if event.StreamPosition != nil {
		t.Errorf("the server returned a stream position %+v; there is no history stream to be at", event.StreamPosition)
	}
}

// ---------------------------------------------------------------------------
// Step 2 -- ping/pong and close reasons.
// ---------------------------------------------------------------------------

// TestServerPingsHoldAnIdleConnectionOpen is the liveness half. An interval far
// shorter than the case's own patience means many pings are exchanged over an
// otherwise silent connection; what is asserted is that the connection is still
// there afterwards and was never ended for a missing pong.
func TestServerPingsHoldAnIdleConnectionOpen(t *testing.T) {
	t.Parallel()

	disconnects := make(chan centrifuge.DisconnectEvent, 8)
	server := newServer(t, serverOptions{
		// One second, and NOT less. See the case below: the connect reply
		// carries the ping interval in whole seconds, so a sub-second interval
		// is not expressible on the wire at all.
		pingInterval: 1 * time.Second,
		pongTimeout:  500 * time.Millisecond,
		onDisconnect: func(e centrifuge.DisconnectEvent) { send(disconnects, e) },
	})
	client, observed := dial(t, server, goodToken)
	await(t, observed.connected, "connected event")

	// Several ping intervals of silence. The connection is then required to
	// still ANSWER, which is a stronger claim than "no disconnect was seen".
	time.Sleep(4 * time.Second)

	select {
	case event := <-disconnects:
		t.Fatalf("an idle connection was ended with code %d (%s)", event.Code, event.Reason)
	default:
	}
	if state := client.State(); state != centrifugego.StateConnected {
		t.Fatalf("client state after an idle period = %v, want connected", state)
	}
	sub, _, _ := subscribe(t, client, "session:tenant-a:idle")
	awaitPublication(t, server, "session:tenant-a:idle", sub, []byte(`{"after":"idle"}`))
}

// TestASubSecondPingIntervalIsNotExpressibleOnTheWire is the trap the case
// above is written around, recorded rather than merely avoided.
//
// The connect reply's ping field is an integer number of SECONDS. Configure the
// server with 500ms and the field is zero, the client is told nothing, it never
// pongs, and the server closes a perfectly healthy connection with "no pong"
// every pong timeout -- a reconnect loop that looks like a network fault. It is
// asserted here so ClientLinkLimits.PingInterval keeps a floor of one second
// with a reason attached, rather than acquiring a sub-second default in some
// later tuning pass.
func TestASubSecondPingIntervalIsNotExpressibleOnTheWire(t *testing.T) {
	t.Parallel()

	disconnects := make(chan centrifuge.DisconnectEvent, 8)
	server := newServer(t, serverOptions{
		pingInterval: 500 * time.Millisecond,
		pongTimeout:  400 * time.Millisecond,
		onDisconnect: func(e centrifuge.DisconnectEvent) { send(disconnects, e) },
	})
	_, observed := dial(t, server, goodToken)
	await(t, observed.connected, "connected event")

	event := await(t, disconnects, "server-side disconnect")
	if event.Code != centrifuge.DisconnectNoPong.Code {
		t.Fatalf("a sub-second ping interval ended the connection with code %d (%s), want %d (no pong)",
			event.Code, event.Reason, centrifuge.DisconnectNoPong.Code)
	}
}

// TestAServerInitiatedCloseCarriesItsCodeAndReason is what lets Factory tell a
// client WHY a link ended. Without it every ending is indistinguishable from a
// dropped network.
func TestAServerInitiatedCloseCarriesItsCodeAndReason(t *testing.T) {
	t.Parallel()

	// A TERMINAL application close code and its reason, both absolute
	// literals, and deliberately not one of Centrifuge's own: a client that
	// reported a built-in code would not match.
	//
	// The range matters and is the thing this case pinned down. A custom code
	// in 4000-4499 is a RECONNECTING advice, and the Go client does not report
	// one through OnDisconnected at all -- it reports it through OnConnecting
	// and reconnects. Only 4500-4999 is terminal. A ClientLink that wanted to
	// tell a browser why its link ended, and picked 4123, would be telling it
	// nothing and would be reconnected to immediately.
	const (
		terminalCode   uint32 = 4712
		terminalReason        = "factory: session binding released"

		reconnectingCode uint32 = 4123
	)

	t.Run("a terminal code reaches the client", func(t *testing.T) {
		t.Parallel()

		connected := make(chan *centrifuge.Client, 1)
		server := newServer(t, serverOptions{
			onConnect: func(client *centrifuge.Client) { send(connected, client) },
		})
		_, observed := dial(t, server, goodToken)
		await(t, observed.connected, "connected event")

		serverSide := await(t, connected, "server-side client")
		serverSide.Disconnect(centrifuge.Disconnect{Code: terminalCode, Reason: terminalReason})

		disconnected := await(t, observed.disconnected, "disconnected event")
		if disconnected.Code != terminalCode {
			t.Errorf("close code = %d, want %d", disconnected.Code, terminalCode)
		}
		if disconnected.Reason != terminalReason {
			t.Errorf("close reason = %q, want %q", disconnected.Reason, terminalReason)
		}
	})

	t.Run("a reconnecting code is not an ending", func(t *testing.T) {
		t.Parallel()

		connected := make(chan *centrifuge.Client, 4)
		server := newServer(t, serverOptions{
			onConnect: func(client *centrifuge.Client) { send(connected, client) },
		})
		client, observed := dial(t, server, goodToken)
		await(t, observed.connected, "connected event")

		serverSide := await(t, connected, "server-side client")
		serverSide.Disconnect(centrifuge.Disconnect{Code: reconnectingCode, Reason: "factory: replica draining"})

		// The client comes BACK rather than reporting an ending: the second
		// connected event is the whole assertion, and the server sees a second
		// connection.
		await(t, connected, "second server-side client")
		await(t, observed.connected, "second connected event")
		if state := client.State(); state != centrifugego.StateConnected {
			t.Errorf("client state = %v after a reconnecting close, want connected", state)
		}
	})
}
