package hostlink_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	centrifuge "github.com/centrifugal/centrifuge"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/hostlink"
)

// These cases drive the REAL transport against an embedded Centrifuge node
// standing in for a Host. Nothing here is inferred from a signature: every
// claim about the handshake, the RPCs and the pushes is measured over a live
// WebSocket connection, because the one blocking defect this lane has already
// produced was a compatibility claim filled in from go.mod rather than run.
//
// The node is a STAND-IN, not the Host. It implements the Factory half's
// counterpart -- the method names, the reply shape and the push envelope --
// and that mirror uses Core's connect and RPC framing; the {type, data} push
// envelope remains Factory's half. A case here proves Factory speaks what it
// documents, not that the real Host agrees.

// waitFor is deliberately generous. This box runs under a load that makes wall
// times meaningless, so a timeout here is evidence of nothing except that the
// event did not arrive; every case that can name a VALUE asserts on the value.
const waitFor = 20 * time.Second

const (
	serviceToken = "service-token-alpha"
	buildVersion = "factory-test-build"
)

func TestADialPresentsItsServiceTokenAndOffersThisBuildsWireVersion(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{token: ""})

	// Two dials with DIFFERENT credentials. One alone could not tell "the Host
	// received what this replica sent" from "the Host recorded a constant".
	for _, token := range []string{"service-token-alpha", "service-token-beta"} {
		link, err := dialHost(t, host, dialerFor(t, token))
		if err != nil {
			t.Fatalf("Dial with %q: %v", token, err)
		}
		t.Cleanup(func() { _ = link.Close(context.Background()) })
	}

	connects := host.connects()
	if len(connects) != 2 {
		t.Fatalf("the Host saw %d handshakes, want 2", len(connects))
	}
	for index, want := range []string{"service-token-alpha", "service-token-beta"} {
		if got := connects[index].token; got != want {
			t.Errorf("handshake %d presented token %q, want %q", index, got, want)
		}
		if got := connects[index].name; got != hostlink.ClientName {
			t.Errorf("handshake %d named itself %q, want %q", index, got, hostlink.ClientName)
		}
		if got := connects[index].version; got != buildVersion {
			t.Errorf("handshake %d reported build %q, want %q", index, got, buildVersion)
		}
		if got, want := connects[index].offered, []sessionwire.WireVersion{sessionwire.CurrentWireVersion}; !slices.Equal(got, want) {
			t.Errorf("handshake %d offered %v, want %v", index, got, want)
		}
	}
}

func TestADialIsRefusedWhenTheHostSelectsAnotherWireVersion(t *testing.T) {
	t.Parallel()

	for name, opts := range map[string]hostOptions{
		"a version this build does not speak": {rawNegotiation: `{"version":2}`},
		"no selection at all":                 {rawNegotiation: "-"},
		"an unreadable selection":             {rawNegotiation: `{"version":"one"}`},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			host := newHostServer(t, opts)
			link, err := dialHost(t, host, dialerFor(t, serviceToken))
			if link != nil {
				t.Cleanup(func() { _ = link.Close(context.Background()) })
			}
			if !errors.Is(err, hostlink.ErrUnsupportedProtocol) {
				t.Fatalf("Dial = %v, want ErrUnsupportedProtocol", err)
			}
		})
	}
}

// TestADialIsRefusedWhenTheHostRejectsTheCredential asserts on the CODE the
// Host closed with rather than on a deadline, so a slow machine cannot make
// this case pass for the wrong reason.
func TestADialIsRefusedWhenTheHostRejectsTheCredential(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{token: "some-other-token"})

	_, err := dialHost(t, host, dialerFor(t, serviceToken))
	if err == nil {
		t.Fatal("Dial accepted a credential the Host refused")
	}
	// The code is read from a STRUCTURED field, not from the message: a
	// refusal whose text happened to contain the number would pass a Contains
	// check for the wrong reason.
	var closed *hostlink.HostDisconnect
	if !errors.As(err, &closed) {
		t.Fatalf("Dial = %v, want a *HostDisconnect", err)
	}
	// 3500 is DisconnectInvalidToken (centrifuge@v0.38.0/disconnect.go:122),
	// in the terminal band 3500-3999, so the client does not reconnect and
	// reports it through OnDisconnected. The literal was measured, not
	// recalled: it was first written as 3501, which is DisconnectBadRequest
	// (disconnect.go:127 -- its own declaration, not :122).
	if closed.Code != 3500 {
		t.Errorf("HostDisconnect.Code = %d, want 3500", closed.Code)
	}
	if closed.Host != hostOne {
		t.Errorf("HostDisconnect.Host = %q, want %q", closed.Host, hostOne)
	}
	if !errors.Is(err, hostlink.ErrDialFailed) {
		t.Errorf("Dial = %v, want it in the ErrDialFailed class", err)
	}
}

// TestADialIsRefusedWhenTheCredentialCannotBeObtained keeps a credential
// outage from costing a connection attempt.
func TestADialIsRefusedWhenTheCredentialCannotBeObtained(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{})
	cause := errors.New("vault is unreachable")
	dialer, err := hostlink.NewCentrifugeDialer(hostlink.DialerConfig{
		Credential: credentialFunc(func(context.Context) (string, error) { return "", cause }),
		Version:    buildVersion,
	})
	if err != nil {
		t.Fatalf("NewCentrifugeDialer: %v", err)
	}

	if _, err := dialHost(t, host, dialer); !errors.Is(err, cause) {
		t.Fatalf("Dial = %v, want the credential failure", err)
	}
	if got := len(host.connects()); got != 0 {
		t.Errorf("the Host saw %d handshakes, want 0", got)
	}
}

func TestEachControlRecordReachesTheHostAsItsOwnMethodAndBody(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{})
	link := mustDial(t, host)

	bind := bindRequest(hostOne, "s-1")
	if err := link.Bind(context.Background(), bind); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := link.DeliverCommand(context.Background(), tenant, "s-1", sessionwire.HostLinkCommandDelivery{CommandID: "cmd-abc"}); err != nil {
		t.Fatalf("DeliverCommand: %v", err)
	}
	unbind := unbindRequest(hostOne, "s-1")
	if err := link.Unbind(context.Background(), unbind); err != nil {
		t.Fatalf("Unbind: %v", err)
	}

	calls := host.calls()
	if len(calls) != 3 {
		t.Fatalf("the Host received %d rpcs, want 3", len(calls))
	}
	// The wanted methods are ABSOLUTE LITERALS, not the constants. Comparing
	// what the Host recorded against sessionwire.HostLinkMethodBind pins
	// nothing: a rename moves both sides together and this case stays green
	// while Factory stops speaking the protocol the Host implements.
	// TestTheWireVocabularyIsPinnedToItsLiterals states the same strings once
	// more, deliberately. The command's method is the SESSION CHANNEL --
	// "hostlink.v1." + base64url("tenant-a") + "." + base64url("s-1"), spelled
	// out -- because Core defines no command method: a Host resolves every
	// unreserved method as a channel, and "hostlink.command" was exactly the
	// string it refused.
	if got, want := []string{calls[0].method, calls[1].method, calls[2].method},
		[]string{"hostlink.bind", "hostlink.v1.dGVuYW50LWE.cy0x", "hostlink.unbind"}; !slices.Equal(got, want) {
		t.Fatalf("the Host received methods %v, want %v", got, want)
	}

	// The BODIES are decoded by Core's own strict decoder on the Host side, so
	// a record that arrived with a missing or renamed member would fail here
	// rather than being read as a zero.
	var gotBind sessionwire.HostLinkBindRequest
	if err := json.Unmarshal(calls[0].data, &gotBind); err != nil {
		t.Fatalf("the bind body did not decode: %v", err)
	}
	if gotBind != bind {
		t.Errorf("the Host received bind %+v, want %+v", gotBind, bind)
	}
	var gotCommand sessionwire.HostLinkCommandDelivery
	if err := json.Unmarshal(calls[1].data, &gotCommand); err != nil {
		t.Fatalf("the command body did not decode: %v", err)
	}
	if gotCommand.CommandID != "cmd-abc" {
		t.Errorf("the Host received command %q, want %q", gotCommand.CommandID, "cmd-abc")
	}
	var gotUnbind sessionwire.HostLinkUnbindRequest
	if err := json.Unmarshal(calls[2].data, &gotUnbind); err != nil {
		t.Fatalf("the unbind body did not decode: %v", err)
	}
	if gotUnbind != unbind {
		t.Errorf("the Host received unbind %+v, want %+v", gotUnbind, unbind)
	}
}

// TestARecordCoreWouldNotMarshalNeverReachesTheWire keeps a malformed control
// record from arriving at a Host as something it will misread.
func TestARecordCoreWouldNotMarshalNeverReachesTheWire(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{})
	link := mustDial(t, host)

	invalid := bindRequest(hostOne, "s-1")
	invalid.RuntimeCompatibilityID = ""
	if err := link.Bind(context.Background(), invalid); err == nil {
		t.Fatal("Bind sent a record Core refuses to marshal")
	}
	if got := len(host.calls()); got != 0 {
		t.Errorf("the Host received %d rpcs, want 0", got)
	}
}

// TestATypedHostRefusalArrivesWithItsCodeAndDetail is the reply shape the
// whole undelivered/refused distinction rests on.
func TestATypedHostRefusalArrivesWithItsCodeAndDetail(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{
		rpc: func(string, []byte) ([]byte, error) {
			return json.Marshal(sessionwire.HostLinkError{
				Code:              sessionwire.HostLinkErrorEpochMismatch,
				CurrentLeaseEpoch: 11,
			})
		},
	})
	link := mustDial(t, host)

	err := link.DeliverCommand(context.Background(), tenant, "s-1", sessionwire.HostLinkCommandDelivery{CommandID: "cmd-abc"})
	var refusal *hostlink.HostRefusal
	if !errors.As(err, &refusal) {
		t.Fatalf("DeliverCommand = %v, want a *HostRefusal", err)
	}
	// Structured fields, not the message: a refusal whose text happened to
	// contain "epoch_mismatch" would pass a Contains check for the wrong
	// reason.
	if refusal.Code != sessionwire.HostLinkErrorEpochMismatch {
		t.Errorf("HostRefusal.Code = %q, want %q", refusal.Code, sessionwire.HostLinkErrorEpochMismatch)
	}
	if refusal.CurrentLeaseEpoch != 11 {
		t.Errorf("HostRefusal.CurrentLeaseEpoch = %d, want 11", refusal.CurrentLeaseEpoch)
	}
}

// TestAProtocolErrorIsNotAHostRefusal is the other half. A Host that answers
// with a transport-level error has told us nothing about the command, so
// reporting it as a refusal would terminate a command the Host may never have
// seen.
func TestAProtocolErrorIsNotAHostRefusal(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{
		rpc: func(string, []byte) ([]byte, error) {
			return nil, centrifuge.ErrorInternal
		},
	})
	link := mustDial(t, host)

	err := link.DeliverCommand(context.Background(), tenant, "s-1", sessionwire.HostLinkCommandDelivery{CommandID: "cmd-abc"})
	if err == nil {
		t.Fatal("DeliverCommand hid a protocol error")
	}
	var refusal *hostlink.HostRefusal
	if errors.As(err, &refusal) {
		t.Errorf("a protocol error was reported as a Host refusal: %v", err)
	}
}

func TestCapacityAndRegistryPushesReachTheObserverDecoded(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{})
	observer := &recordingObserver{}
	mustDialWithObserver(t, host, observer)

	host.push(t, hostlink.PushTypeCapacity, capacityReport(hostOne))
	host.push(t, hostlink.PushTypeRegistry, registryObservation(hostOne, "s-7"))

	waitUntil(t, "the capacity report", func() bool { return len(observer.capacityHosts()) == 1 })
	waitUntil(t, "the registry observation", func() bool { return len(observer.registrySessions()) == 1 })

	if got := observer.capacityHosts()[0]; got != hostOne {
		t.Errorf("observed capacity for %q, want %q", got, hostOne)
	}
	if got := observer.registrySessions()[0]; got != "s-7" {
		t.Errorf("observed registry for %q, want %q", got, "s-7")
	}
}

// TestAnUnreadablePushIsDroppedAndTheLinkKeepsWorking. A capacity report is an
// advertisement; it is not worth the sessions multiplexed over the connection,
// and a Host that added a push type must not take every replica down.
func TestAnUnreadablePushIsDroppedAndTheLinkKeepsWorking(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{})
	observer := &recordingObserver{}
	link := mustDialWithObserver(t, host, observer)

	// Not "not JSON at all": the embedded server VALIDATES a push body and
	// Client.Send refuses invalid JSON outright, which was measured here rather
	// than assumed. So every case below is well-formed JSON that this build
	// still cannot read.
	host.pushRaw(t, []byte(`[]`))
	host.pushRaw(t, []byte(`{"type":123}`))
	host.pushRaw(t, []byte(`{"type":"host.capacity","data":{"host_id":"missing-everything-else"}}`))
	host.pushRaw(t, []byte(`{"type":"host.something.new","data":{}}`))
	// A good one AFTER the bad ones: it is what proves the link survived rather
	// than merely that nothing was observed.
	host.push(t, hostlink.PushTypeCapacity, capacityReport(hostOne))

	waitUntil(t, "the good capacity report", func() bool { return len(observer.capacityHosts()) == 1 })
	if got := len(observer.registrySessions()); got != 0 {
		t.Errorf("observed %d registry observations, want 0", got)
	}
	if err := link.Bind(context.Background(), bindRequest(hostOne, "s-1")); err != nil {
		t.Errorf("the link stopped working after an unreadable push: %v", err)
	}
}

// TestCloseEndsTheConnectionAtTheHost counts EVENTS, not populations.
//
// A5.1 established that a population check reads the wrong number when the
// client reconnects; the Host's disconnect count is monotonic and cannot be
// restored by anything the client does.
func TestCloseEndsTheConnectionAtTheHost(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{})
	link := mustDial(t, host)

	if err := link.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	waitUntil(t, "the Host's disconnect", func() bool { return host.disconnects() == 1 })

	// Closing twice must not fail: the pool can reap and shut down at once.
	if err := link.Close(context.Background()); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

// TestTheLinkReconnectsAfterAReconnectBandCloseAndKeepsWorking measures the
// transport's reconnect against a real close.
//
// 4000 is in the reconnect band (4000-4499), so centrifuge-go moves to
// connecting rather than reporting a disconnect. The verdict is taken from the
// Host's own handshake COUNT and from a control record landing afterwards, not
// from the client's state.
func TestTheLinkReconnectsAfterAReconnectBandCloseAndKeepsWorking(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{})
	link := mustDial(t, host)

	host.disconnectEveryone(centrifuge.Disconnect{Code: 4000, Reason: "test reconnect"})
	waitUntil(t, "a second handshake", func() bool { return len(host.connects()) >= 2 })

	// The version is re-negotiated on every connect, and the link must be
	// usable again afterwards. The bind is retried rather than attempted once,
	// because the Host records the handshake before the client has finished
	// it; a single attempt would be measuring that race, not the reconnect.
	waitUntil(t, "a bind to land on the reconnected link", func() bool {
		return link.Bind(context.Background(), bindRequest(hostOne, "s-1")) == nil
	})
	calls := host.calls()
	if len(calls) == 0 {
		t.Fatal("the Host received no rpc after the reconnect")
	}
	for index, call := range calls {
		if call.method != sessionwire.HostLinkMethodBind {
			t.Errorf("rpc %d after the reconnect was %q, want %q", index, call.method, sessionwire.HostLinkMethodBind)
		}
	}
}

// TestAReconnectIntoAHostSpeakingAnotherVersionStopsTheLink. A Host restarted
// at a version this build does not speak must not be reconnected to forever
// while every control record fails to decode.
func TestAReconnectIntoAHostSpeakingAnotherVersionStopsTheLink(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{})
	link := mustDial(t, host)

	host.setNegotiation(`{"version":2}`)
	host.disconnectEveryone(centrifuge.Disconnect{Code: 4000, Reason: "test reconnect"})
	waitUntil(t, "a second handshake", func() bool { return len(host.connects()) >= 2 })

	waitUntil(t, "the link to stop answering", func() bool {
		return errors.Is(link.Bind(context.Background(), bindRequest(hostOne, "s-1")), hostlink.ErrUnsupportedProtocol)
	})
}

// TestOneRealConnectionCarriesEverySessionOnAHost is runbook A7.1's headline,
// measured at the Host rather than at the pool's own counter.
func TestOneRealConnectionCarriesEverySessionOnAHost(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{})
	pool := poolOver(t, dialerFor(t, serviceToken))

	for _, session := range []sessionwire.SessionID{"s-1", "s-2", "s-3"} {
		if err := pool.Bind(context.Background(), host.target(), bindRequest(host.id, session)); err != nil {
			t.Fatalf("Bind(%s): %v", session, err)
		}
	}

	if got := len(host.connects()); got != 1 {
		t.Errorf("the Host saw %d handshakes for three sessions, want 1", got)
	}
	if got := len(host.calls()); got != 3 {
		t.Errorf("the Host received %d bind rpcs, want 3", got)
	}
	if got := pool.Links(); got != 1 {
		t.Errorf("Links() = %d, want 1", got)
	}
}

// TestTwoReplicasConnectToOneHostOverRealSockets is runbook A7.1 step 2 with
// the transport in the picture: two independent pools, no broker, no leader.
func TestTwoReplicasConnectToOneHostOverRealSockets(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{})
	replicaA := poolOver(t, dialerFor(t, "service-token-replica-a"))
	replicaB := poolOver(t, dialerFor(t, "service-token-replica-b"))

	if err := replicaA.Bind(context.Background(), host.target(), bindRequest(host.id, "s-1")); err != nil {
		t.Fatalf("replica A Bind: %v", err)
	}
	if err := replicaB.Bind(context.Background(), host.target(), bindRequest(host.id, "s-2")); err != nil {
		t.Fatalf("replica B Bind: %v", err)
	}

	connects := host.connects()
	if len(connects) != 2 {
		t.Fatalf("the Host saw %d handshakes, want 2", len(connects))
	}
	if connects[0].token == connects[1].token {
		t.Errorf("both handshakes presented %q; the replicas were not independent", connects[0].token)
	}

	// Closing one replica leaves the other's connection and route alone.
	if err := replicaA.Close(context.Background()); err != nil {
		t.Fatalf("replica A Close: %v", err)
	}
	waitUntil(t, "replica A's disconnect", func() bool { return host.disconnects() == 1 })
	if err := replicaB.DeliverCommand(context.Background(), tenant, "s-2", sessionwire.HostLinkCommandDelivery{CommandID: "cmd-b"}); err != nil {
		t.Errorf("replica B stopped working when replica A closed: %v", err)
	}
}

func TestNewCentrifugeDialerRejectsAnIncompleteComposition(t *testing.T) {
	t.Parallel()

	for name, cfg := range map[string]hostlink.DialerConfig{
		"no credential": {Version: buildVersion},
		"no version":    {Credential: staticCredential(serviceToken)},
		"invalid limits": {
			Credential: staticCredential(serviceToken),
			Version:    buildVersion,
			Limits: hostlink.Limits{
				MaxLinks: 1, DialTimeout: time.Minute, IdleTimeout: time.Second,
				ReconnectMin: time.Second, ReconnectMax: time.Second,
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if _, err := hostlink.NewCentrifugeDialer(cfg); !errors.Is(err, hostlink.ErrInvalidConfig) {
				t.Fatalf("NewCentrifugeDialer(%s) = %v, want ErrInvalidConfig", name, err)
			}
		})
	}
}

// TestTheRealDialerRefusesAnEndpointCoreRejects is the same guard the pool
// applies, at the transport: a dialer used directly must not open a socket to
// an address Core would not accept as a routing observation.
func TestTheRealDialerRefusesAnEndpointCoreRejects(t *testing.T) {
	t.Parallel()

	dialer := dialerFor(t, serviceToken)
	for name, endpoint := range map[string]sessionwire.InternalEndpoint{
		"empty":            "",
		"not websocket":    "https://host-1.internal/hostlink",
		"carries a secret": "wss://user:password@host-1.internal/hostlink",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if _, err := dialer.Dial(context.Background(), hostlink.Target{Host: hostOne, Endpoint: endpoint}, nil); err == nil {
				t.Fatalf("Dial accepted %q", endpoint)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// The Host stand-in
// ---------------------------------------------------------------------------

type connectRecord struct {
	token   string
	name    string
	version string
	offered []sessionwire.WireVersion
	data    []byte
	reply   []byte
}

type rpcCall struct {
	method string
	data   []byte
}

type hostOptions struct {
	// token, when set, is the only credential the Host accepts.
	token string
	// methods are the reserved HostLink methods advertised in a bare reply.
	// A nil slice takes the ordinary stand-in capabilities; an explicitly empty
	// non-nil slice models a Host that advertises no reserved methods.
	methods []string
	// rawNegotiation replaces the connect reply body. "-" sends none.
	rawNegotiation string
	// rpc answers every control RPC. A nil value acknowledges.
	rpc func(method string, data []byte) ([]byte, error)
}

type hostServer struct {
	id   sessionwire.HostID
	url  string
	node *centrifuge.Node

	mu             sync.Mutex
	connectRecords []connectRecord
	rpcCalls       []rpcCall
	disconnected   int
	clients        map[string]*centrifuge.Client
	negotiation    string
	methods        []string
}

func newHostServer(t *testing.T, opts hostOptions) *hostServer {
	t.Helper()

	node, err := centrifuge.New(centrifuge.Config{
		Name:     "host-stand-in",
		LogLevel: centrifuge.LogLevelNone,
	})
	if err != nil {
		t.Fatalf("centrifuge.New: %v", err)
	}
	methods := opts.methods
	if methods == nil {
		methods = []string{sessionwire.HostLinkMethodBind, sessionwire.HostLinkMethodUnbind}
	}
	host := &hostServer{
		id:          hostOne,
		node:        node,
		clients:     map[string]*centrifuge.Client{},
		negotiation: opts.rawNegotiation,
		methods:     append([]string(nil), methods...),
	}

	node.OnConnecting(func(_ context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		record := connectRecord{
			token: e.Token, name: e.Name, version: e.Version,
			data: append([]byte(nil), e.Data...),
		}
		host.mu.Lock()
		rawNegotiation := host.negotiation
		methods := append([]string(nil), host.methods...)
		host.mu.Unlock()

		request, err := sessionwire.DecodeHostLinkConnectRequest(e.Data)
		record.offered = append([]sessionwire.WireVersion(nil), request.SupportedVersions...)
		if err != nil {
			host.mu.Lock()
			host.connectRecords = append(host.connectRecords, record)
			host.mu.Unlock()
			return centrifuge.ConnectReply{}, centrifuge.DisconnectInappropriateProtocol
		}

		if opts.token != "" && e.Token != opts.token {
			host.mu.Lock()
			host.connectRecords = append(host.connectRecords, record)
			host.mu.Unlock()
			return centrifuge.ConnectReply{}, centrifuge.DisconnectInvalidToken
		}

		reply := centrifuge.ConnectReply{Credentials: &centrifuge.Credentials{UserID: "factory"}}
		switch rawNegotiation {
		case "-":
			// No selection at all.
		case "":
			selection, err := sessionwire.NegotiateVersion(request)
			if err != nil {
				host.mu.Lock()
				host.connectRecords = append(host.connectRecords, record)
				host.mu.Unlock()
				return centrifuge.ConnectReply{}, centrifuge.DisconnectInappropriateProtocol
			}
			selection = selection.WithHostLinkMethods(methods...)
			body, err := sessionwire.EncodeHostLinkConnectReply(selection)
			if err != nil {
				host.mu.Lock()
				host.connectRecords = append(host.connectRecords, record)
				host.mu.Unlock()
				return centrifuge.ConnectReply{}, centrifuge.DisconnectServerError
			}
			reply.Data = body
		default:
			reply.Data = []byte(rawNegotiation)
		}
		record.reply = append([]byte(nil), reply.Data...)
		host.mu.Lock()
		host.connectRecords = append(host.connectRecords, record)
		host.mu.Unlock()
		return reply, nil
	})

	node.OnConnect(func(client *centrifuge.Client) {
		host.mu.Lock()
		host.clients[client.ID()] = client
		host.mu.Unlock()

		client.OnRPC(func(e centrifuge.RPCEvent, cb centrifuge.RPCCallback) {
			host.mu.Lock()
			host.rpcCalls = append(host.rpcCalls, rpcCall{method: e.Method, data: append([]byte(nil), e.Data...)})
			host.mu.Unlock()
			if opts.rpc != nil {
				data, err := opts.rpc(e.Method, e.Data)
				cb(centrifuge.RPCReply{Data: data}, err)
				return
			}
			// An empty body is the legitimate success shape. A non-empty body
			// must be a bare Core HostLinkError; returning {} here would make
			// the stand-in bless a malformed reply.
			cb(centrifuge.RPCReply{}, nil)
		})
		client.OnDisconnect(func(centrifuge.DisconnectEvent) {
			host.mu.Lock()
			host.disconnected++
			delete(host.clients, client.ID())
			host.mu.Unlock()
		})
	})

	if err := node.Run(); err != nil {
		t.Fatalf("node.Run: %v", err)
	}
	handler := centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{
		CheckOrigin: func(*http.Request) bool { return true },
		Compression: false,
	})
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		// Bounded Shutdown first: an unbounded Close waits for the outstanding
		// request that a wedged read loop is still holding, so this order is
		// the difference between a failing case and a ten-minute package
		// timeout. TestEveryBoundedShutdownPrecedesAnUnboundedClose censuses it.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = node.Shutdown(ctx)
		server.Close()
	})
	host.url = "ws" + strings.TrimPrefix(server.URL, "http")
	return host
}

func (h *hostServer) target() hostlink.Target {
	return hostlink.Target{Host: h.id, Endpoint: sessionwire.InternalEndpoint(h.url)}
}

func (h *hostServer) connects() []connectRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	cloned := make([]connectRecord, len(h.connectRecords))
	for index, record := range h.connectRecords {
		cloned[index] = record
		cloned[index].offered = append([]sessionwire.WireVersion(nil), record.offered...)
		cloned[index].data = append([]byte(nil), record.data...)
		cloned[index].reply = append([]byte(nil), record.reply...)
	}
	return cloned
}

func (h *hostServer) calls() []rpcCall {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]rpcCall(nil), h.rpcCalls...)
}

func (h *hostServer) disconnects() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.disconnected
}

func (h *hostServer) setNegotiation(raw string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.negotiation = raw
}

func (h *hostServer) disconnectEveryone(d centrifuge.Disconnect) {
	h.mu.Lock()
	clients := make([]*centrifuge.Client, 0, len(h.clients))
	for _, client := range h.clients {
		clients = append(clients, client)
	}
	h.mu.Unlock()
	for _, client := range clients {
		client.Disconnect(d)
	}
}

func (h *hostServer) push(t *testing.T, kind string, record any) {
	t.Helper()

	body, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal %s: %v", kind, err)
	}
	envelope, err := json.Marshal(map[string]any{"type": kind, "data": json.RawMessage(body)})
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	h.pushRaw(t, envelope)
}

func (h *hostServer) pushRaw(t *testing.T, data []byte) {
	t.Helper()

	deadline := time.Now().Add(waitFor)
	for {
		h.mu.Lock()
		clients := make([]*centrifuge.Client, 0, len(h.clients))
		for _, client := range h.clients {
			clients = append(clients, client)
		}
		h.mu.Unlock()
		if len(clients) > 0 {
			for _, client := range clients {
				if err := client.Send(data); err != nil {
					t.Fatalf("Send: %v", err)
				}
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("no connected client to push to")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestTheWireVocabularyIsPinnedToItsLiterals holds the framing this package
// sends to absolute strings.
//
// The method names are Core's since v0.8.0, and the push discriminators are
// still Factory's half of the protocol; both are pinned here as literals. Every
// other case in this file compares what the stand-in recorded against the
// constant it was sent with, which pins nothing at all: renaming a constant
// renames both sides and the whole suite stays green. Writing the strings out
// is what makes a rename a deliberate act rather than a silent one, for the
// same reason limits_test.go pins the ping boundary as a literal rather than
// through the constant it guards.
//
// The command row is the one that closed B6. It pins the delivery method to
// Core's HostLinkChannel AND to the spelled-out channel for one known pair, so
// a Factory that went back to a command method of its own, or a Core whose
// helper changed its encoding, both fail here rather than at the first Host.
func TestTheWireVocabularyIsPinnedToItsLiterals(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		got  string
		want string
	}{
		{"HostLinkMethodBind", sessionwire.HostLinkMethodBind, "hostlink.bind"},
		{"HostLinkMethodUnbind", sessionwire.HostLinkMethodUnbind, "hostlink.unbind"},
		{"HostLinkMethodAttach", sessionwire.HostLinkMethodAttach, "hostlink.attach"},
		{"HostLinkChannelPrefix", sessionwire.HostLinkChannelPrefix, "hostlink.v1."},
		{"HostLinkChannel(tenant-a, s-1)", sessionwire.HostLinkChannel("tenant-a", "s-1"), "hostlink.v1.dGVuYW50LWE.cy0x"},
		{"PushTypeCapacity", hostlink.PushTypeCapacity, "host.capacity"},
		{"PushTypeRegistry", hostlink.PushTypeRegistry, "host.registry"},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %q, want %q -- this string is one half of a protocol "+
				"whose Host half mirrors it; changing it is a cross-repo change", tc.name, tc.got, tc.want)
		}
	}
}

// TestACommandDeliveryIsSentOnTheSessionsChannelAsItsMethod is B6 measured
// over a real socket, per session and per tenant.
//
// The method the stand-in records is compared against Core's helper for the
// SAME pair, and two deliveries differing only in tenant must reach the Host
// as two different methods -- a session id is unique only within a tenant, so
// a channel derived from the session alone would let one tenant's binding
// answer for another's. The sibling literal in
// TestEachControlRecordReachesTheHostAsItsOwnMethodAndBody pins the encoding;
// this pins the derivation.
func TestACommandDeliveryIsSentOnTheSessionsChannelAsItsMethod(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{})
	link := mustDial(t, host)

	deliveries := []struct {
		tenant  sessionwire.TenantID
		session sessionwire.SessionID
	}{
		{"tenant-a", "s-1"},
		{"tenant-b", "s-1"},
		{"tenant-a", "s.2"},
	}
	for _, d := range deliveries {
		if err := link.DeliverCommand(context.Background(), d.tenant, d.session, sessionwire.HostLinkCommandDelivery{CommandID: "cmd-1"}); err != nil {
			t.Fatalf("DeliverCommand(%s, %s): %v", d.tenant, d.session, err)
		}
	}
	calls := host.calls()
	if len(calls) != len(deliveries) {
		t.Fatalf("the Host received %d rpcs, want %d", len(calls), len(deliveries))
	}
	seen := map[string]bool{}
	for i, d := range deliveries {
		want := sessionwire.HostLinkChannel(d.tenant, d.session)
		if calls[i].method != want {
			t.Errorf("delivery %d was sent as method %q, want HostLinkChannel = %q", i, calls[i].method, want)
		}
		if !strings.HasPrefix(calls[i].method, "hostlink.v1.") {
			t.Errorf("delivery %d method %q does not carry the channel prefix", i, calls[i].method)
		}
		if seen[calls[i].method] {
			t.Errorf("delivery %d reused method %q for a different (tenant, session)", i, calls[i].method)
		}
		seen[calls[i].method] = true
	}
}

// TestATerminalDisconnectOnALiveLinkStopsItAnswering is the other half of the
// terminal-link decision, which was otherwise pinned only on the version path.
//
// centrifuge-go reports 3500-3999 and 4500-4999 through OnDisconnected and does
// not reconnect from them (centrifuge-go@v0.12.0/client.go:989). A link that
// kept answering afterwards would report every control record as an undelivered
// command, which is a retry loop against a Host that has already given its
// final answer. TestADialIsRefusedWhenTheHostRejectsTheCredential cannot cover
// this: there the refusal arrives at DIAL time, Dial fails and no link is ever
// returned, so a LIVE link never carries the terminal state.
func TestATerminalDisconnectOnALiveLinkStopsItAnswering(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{})
	link := mustDial(t, host)
	// The link must be established and working first, otherwise this is the
	// dial-time case again under another name.
	if err := link.Bind(context.Background(), bindRequest(hostOne, "s-1")); err != nil {
		t.Fatalf("Bind before the disconnect: %v", err)
	}

	// The library's own constant, so the band is the library's rather than a
	// number recalled here. DisconnectInvalidToken is 3500
	// (centrifuge@v0.38.0/disconnect.go:122).
	host.disconnectEveryone(centrifuge.DisconnectInvalidToken)

	// The wait is written out rather than delegated to waitUntil so that a
	// failure names a VALUE: a link that answered would be reporting some other
	// error, and "no HostDisconnect within 20s" alone would read as this box's
	// load. The last error actually seen is carried into the message.
	var closed *hostlink.HostDisconnect
	var last error
	for deadline := time.Now().Add(waitFor); ; {
		last = link.Bind(context.Background(), bindRequest(hostOne, "s-2"))
		if errors.As(last, &closed) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the link kept answering after a terminal disconnect: last Bind error = %v, want a *HostDisconnect", last)
		}
		time.Sleep(10 * time.Millisecond)
	}
	// The verdict is taken from values, not from the wait.
	if closed.Code != 3500 {
		t.Errorf("HostDisconnect.Code = %d, want 3500", closed.Code)
	}
	if closed.Host != hostOne {
		t.Errorf("HostDisconnect.Host = %q, want %q", closed.Host, hostOne)
	}
	// Every method is refused, not just the one that discovered the state.
	err := link.DeliverCommand(context.Background(), tenant, "s-1", sessionwire.HostLinkCommandDelivery{CommandID: "cmd-abc"})
	if !errors.As(err, &closed) {
		t.Errorf("DeliverCommand after a terminal disconnect = %v, want the HostDisconnect", err)
	}
	// And nothing reached the Host afterwards: a link that answered would have
	// tried, which is the retry loop this decision exists to prevent.
	for index, call := range host.calls() {
		if call.method != "hostlink.bind" {
			t.Errorf("rpc %d after the terminal disconnect was %q, want no rpc but the first bind", index, call.method)
		}
	}
	if got := len(host.calls()); got != 1 {
		t.Errorf("the Host received %d rpcs, want 1 -- only the bind that preceded the disconnect", got)
	}
}

// ---------------------------------------------------------------------------
// Transport fixtures
// ---------------------------------------------------------------------------

type credentialFunc func(context.Context) (string, error)

func (f credentialFunc) ServiceToken(ctx context.Context) (string, error) { return f(ctx) }

func staticCredential(token string) hostlink.Credential {
	return credentialFunc(func(context.Context) (string, error) { return token, nil })
}

func dialerFor(t *testing.T, token string) hostlink.Dialer {
	t.Helper()

	dialer, err := hostlink.NewCentrifugeDialer(hostlink.DialerConfig{
		Credential: staticCredential(token),
		Version:    buildVersion,
	})
	if err != nil {
		t.Fatalf("NewCentrifugeDialer: %v", err)
	}
	return dialer
}

func dialHost(t *testing.T, host *hostServer, dialer hostlink.Dialer) (hostlink.Link, error) {
	t.Helper()

	return dialer.Dial(context.Background(), host.target(), &recordingObserver{})
}

func mustDial(t *testing.T, host *hostServer) hostlink.Link {
	t.Helper()

	return mustDialWithObserver(t, host, &recordingObserver{})
}

func mustDialWithObserver(t *testing.T, host *hostServer, observer hostlink.Observer) hostlink.Link {
	t.Helper()

	link, err := dialerFor(t, serviceToken).Dial(context.Background(), host.target(), observer)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = link.Close(context.Background()) })
	return link
}

func poolOver(t *testing.T, dialer hostlink.Dialer) *hostlink.Pool {
	t.Helper()

	pool, err := hostlink.NewPool(hostlink.Config{Dialer: dialer, Observer: &recordingObserver{}})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close(context.Background()) })
	return pool
}

// waitUntil polls a condition. It reports a DEADLINE, which under this box's
// load is only evidence that the event did not arrive -- every case that can
// name a value asserts on the value separately.
func waitUntil(t *testing.T, what string, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(waitFor)
	for {
		if condition() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not arrive within %v", what, waitFor)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
