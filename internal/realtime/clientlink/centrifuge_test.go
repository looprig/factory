package clientlink_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	centrifugego "github.com/centrifugal/centrifuge-go"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	"github.com/looprig/factory/internal/command"
	internalidentity "github.com/looprig/factory/internal/identity"
	"github.com/looprig/factory/internal/realtime/clientlink"
	"github.com/looprig/sessionstore"
)

// Everything below drives the REAL handler over a real loopback WebSocket with
// the real Go client, against Factory's own authenticator and Factory's own
// authorizer. The only fake is the credential Verifier, which is the seam a
// deployment supplies and the one thing this module genuinely does not own.
//
// What is deliberately NOT faked is the transport. A5.1 qualified it; a fake
// here would answer whatever it was written to answer, and every finding this
// task inherits is about what the transport actually does.

const (
	// buildVersion is what the handler reports to a connecting client. It is an
	// absolute literal and deliberately not the module version, so a handler
	// that echoed the SDK's own version instead of Factory's would fail.
	buildVersion = "factory-clientlink-0"

	tenantA = sessionwire.TenantID("tenant-a")
	tenantB = sessionwire.TenantID("tenant-b")
)

// waitFor is how long a case waits for a transport event. It is generous
// because this suite does not control the machine's load; nothing here asserts
// that anything is FAST.
const waitFor = 30 * time.Second

// ---------------------------------------------------------------------------
// The fixture.
// ---------------------------------------------------------------------------

// verifier is the deployment seam. It answers from a table of tokens, so
// "authenticated" stays distinguishable from "let everything through": exactly
// one value per case is accepted and everything else is refused.
type verifier struct {
	mu sync.Mutex
	// claims maps a token to what it asserts.
	claims map[string]identity.Claims
	// unavailable, when set, is returned for EVERY credential. It is the
	// verifier-outage case, and it returns an error that does NOT wrap
	// ErrUnauthenticated, which is exactly how a deployment reports one.
	unavailable error
	// calls counts verifications, so a case can assert one did not happen.
	calls int
}

func (v *verifier) VerifyCredential(_ context.Context, credential identity.Credential) (identity.Claims, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.calls++
	if v.unavailable != nil {
		return identity.Claims{}, v.unavailable
	}
	claims, ok := v.claims[credential.Value()]
	if !ok {
		return identity.Claims{}, fmt.Errorf("%w: no such credential", identity.ErrUnauthenticated)
	}
	return claims, nil
}

func (v *verifier) verifications() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.calls
}

// recordingAuthorizer wraps Factory's real authorizer and records what it was
// asked, so a case can assert a decision was CONSULTED rather than inferring it
// from an outcome that a handler skipping the call would also produce.
type recordingAuthorizer struct {
	inner internalidentity.Authorizer

	mu        sync.Mutex
	subscribe []subscribeCall
	control   []controlCall
	// denyKind, when set, refuses exactly one command kind.
	denyKind sessionstore.CommandKind
}

type subscribeCall struct {
	tenant  sessionwire.TenantID
	channel string
}

type controlCall struct {
	tenant  sessionwire.TenantID
	session sessionwire.SessionID
	kind    sessionstore.CommandKind
}

func (a *recordingAuthorizer) AuthorizeSubscribe(ctx context.Context, principal identity.Principal, channel string) error {
	a.mu.Lock()
	a.subscribe = append(a.subscribe, subscribeCall{tenant: principal.Tenant(), channel: channel})
	a.mu.Unlock()
	return a.inner.AuthorizeSubscribe(ctx, principal, channel)
}

func (a *recordingAuthorizer) AuthorizeControl(ctx context.Context, principal identity.Principal, session sessionwire.SessionID, kind sessionstore.CommandKind) error {
	a.mu.Lock()
	a.control = append(a.control, controlCall{tenant: principal.Tenant(), session: session, kind: kind})
	deny := a.denyKind
	a.mu.Unlock()
	if deny != "" && kind == deny {
		return fmt.Errorf("%w: refused for this case", internalidentity.ErrUnauthorized)
	}
	return a.inner.AuthorizeControl(ctx, principal, session, kind)
}

func (a *recordingAuthorizer) subscribeCalls() []subscribeCall {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]subscribeCall(nil), a.subscribe...)
}

func (a *recordingAuthorizer) controlCalls() []controlCall {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]controlCall(nil), a.control...)
}

// fixture is one running handler behind one loopback HTTP server.
type fixture struct {
	handler    *clientlink.Handler
	url        string
	verifier   *verifier
	authorizer *recordingAuthorizer
	admitter   *recordingAdmitter
}

// testLimits is the ClientLink configuration a case starts from. It is written
// out rather than taken from factory.DefaultClientLinkLimits, which this
// package cannot import without a cycle and which a test should not track
// anyway: a fixture built from the production default pins nothing.
func testLimits() clientlink.Limits {
	return clientlink.Limits{
		MaxConnections:           64,
		MaxChannelsPerConnection: 256,
		PerConnectionQueueBytes:  1 << 20,
		WriteTimeout:             5 * time.Second,
		PingInterval:             25 * time.Second,
		PongTimeout:              10 * time.Second,
		CommandTimeout:           30 * time.Second,
	}
}

// tokens is the credential table every case starts from: one live actor per
// tenant, one already expired, and nothing else.
func tokens() map[string]identity.Claims {
	future := time.Now().Add(time.Hour)
	return map[string]identity.Claims{
		"token-a":       {Tenant: tenantA, Subject: "user-a", Kind: identity.KindActor, ExpiresAt: future},
		"token-b":       {Tenant: tenantB, Subject: "user-b", Kind: identity.KindActor, ExpiresAt: future},
		"token-expired": {Tenant: tenantA, Subject: "user-a", Kind: identity.KindActor, ExpiresAt: time.Now().Add(-time.Hour)},
	}
}

func newFixture(t *testing.T, limits clientlink.Limits) *fixture {
	t.Helper()

	v := &verifier{claims: tokens()}
	authenticator, err := internalidentity.NewAuthenticator(internalidentity.Config{Verifier: v})
	if err != nil {
		t.Fatalf("NewAuthenticator: %v", err)
	}
	authorizer := &recordingAuthorizer{}
	admitter := &recordingAdmitter{created: true}
	handler, err := clientlink.NewHandler(clientlink.Config{
		Authenticator: authenticator,
		Authorizer:    authorizer,
		Admitter:      admitter,
		Limits:        limits,
		Version:       buildVersion,
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = handler.Shutdown(ctx)
	})
	return &fixture{
		handler:    handler,
		url:        "ws" + strings.TrimPrefix(server.URL, "http"),
		verifier:   v,
		authorizer: authorizer,
		admitter:   admitter,
	}
}

// events collects what a client observed, so an assertion is made against a
// recorded transition rather than against a sleep.
type events struct {
	connected chan centrifugego.ConnectedEvent
	// disconnected receives only TERMINAL closes, and connecting receives the
	// rest. That split is the client's, not this suite's:
	// centrifuge-go@v0.12.0/transport_websocket.go:29 computes
	//
	//	reconnect := code < 3500 || code >= 5000 || (code >= 4000 && code < 4500)
	//
	// so a close in the reconnect band never reaches OnDisconnected at all. A
	// case that watched only OnDisconnected would therefore time out on every
	// retryable close and read as a hang rather than as a classification, which
	// is exactly the trap the inherited close-code finding describes.
	disconnected chan centrifugego.DisconnectedEvent
	connecting   chan centrifugego.ConnectingEvent
}

func send[T any](ch chan T, value T) {
	select {
	case ch <- value:
	default:
	}
}

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

// dial connects a JSON client presenting a token and a protocol version.
func dial(t *testing.T, f *fixture, token, protocol string) (*centrifugego.Client, *events) {
	t.Helper()

	data, err := json.Marshal(map[string]string{"protocol_version": protocol})
	if err != nil {
		t.Fatalf("marshal connect data: %v", err)
	}
	client := centrifugego.NewJsonClient(f.url, centrifugego.Config{Token: token, Data: data})
	observed := &events{
		connected:    make(chan centrifugego.ConnectedEvent, 32),
		disconnected: make(chan centrifugego.DisconnectedEvent, 32),
		connecting:   make(chan centrifugego.ConnectingEvent, 32),
	}
	client.OnConnected(func(e centrifugego.ConnectedEvent) { send(observed.connected, e) })
	client.OnDisconnected(func(e centrifugego.DisconnectedEvent) { send(observed.disconnected, e) })
	client.OnConnecting(func(e centrifugego.ConnectingEvent) { send(observed.connecting, e) })
	t.Cleanup(client.Close)
	if err := client.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	return client, observed
}

// dialSupported is dial at the protocol version this build speaks.
func dialSupported(t *testing.T, f *fixture, token string) (*centrifugego.Client, *events) {
	t.Helper()

	return dial(t, f, token, clientlink.ProtocolVersion)
}

// connectingDrops filters OnConnecting down to the transitions that mean a
// LINK WAS LOST.
//
// OnConnecting also fires for the client's own initial connect, with code 0
// ("connect called"), and that event is not a drop. Draining once was measured
// insufficient: the callback runs on the client's own goroutine and the code-0
// event can be delivered after the connected event, so a case that drained and
// then watched would fail on the very transition that produced the connection
// it is watching.
func connectingDrops(observed *events) <-chan centrifugego.ConnectingEvent {
	drops := make(chan centrifugego.ConnectingEvent, 1)
	go func() {
		for event := range observed.connecting {
			if event.Code != 0 {
				drops <- event
				return
			}
		}
	}()
	return drops
}

// awaitReconnectCode waits for a RECONNECT-band close carrying code.
//
// It loops rather than reading once, because OnConnecting also fires for the
// client's own initial connect and for each retry, so the wanted transition is
// not the first event. A failure names every code that did arrive, so a wrong
// classification is diagnosed rather than merely reported as a timeout.
func awaitReconnectCode(t *testing.T, observed *events, want uint32) {
	t.Helper()

	var seen []uint32
	deadline := time.After(waitFor)
	for {
		select {
		case event := <-observed.connecting:
			if event.Code == want {
				return
			}
			seen = append(seen, event.Code)
		case event := <-observed.disconnected:
			t.Fatalf("the link was closed TERMINALLY with code %d (%s), want a reconnect-band %d; connecting codes seen: %v",
				event.Code, event.Reason, want, seen)
		case <-deadline:
			t.Fatalf("no reconnect-band close with code %d arrived within %v; codes seen: %v", want, waitFor, seen)
			return
		}
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

// subscribe subscribes and returns the error the server answered, or nil.
func subscribe(t *testing.T, client *centrifugego.Client, channel string) error {
	t.Helper()

	sub, err := client.NewSubscription(channel)
	if err != nil {
		t.Fatalf("NewSubscription(%q): %v", channel, err)
	}
	subscribed := make(chan struct{}, 1)
	failed := make(chan error, 4)
	sub.OnSubscribed(func(centrifugego.SubscribedEvent) { send(subscribed, struct{}{}) })
	sub.OnError(func(e centrifugego.SubscriptionErrorEvent) { send(failed, e.Error) })
	if err := sub.Subscribe(); err != nil {
		t.Fatalf("Subscribe(%q): %v", channel, err)
	}
	select {
	case <-subscribed:
		return nil
	case err := <-failed:
		return err
	case <-time.After(waitFor):
		t.Fatalf("subscribing to %q neither succeeded nor failed within %v", channel, waitFor)
		return nil
	}
}

func sessionChannel(tenant sessionwire.TenantID, session string) string {
	return fmt.Sprintf("session:%s:%s", tenant, session)
}

// ---------------------------------------------------------------------------
// Step 1 -- the handshake.
// ---------------------------------------------------------------------------

// TestAnAuthenticatedHandshakeReportsTheBuildAndTheProtocol is version
// negotiation in both directions over one real connect exchange.
func TestAnAuthenticatedHandshakeReportsTheBuildAndTheProtocol(t *testing.T) {
	t.Parallel()

	f := newFixture(t, testLimits())
	_, observed := dialSupported(t, f, "token-a")
	connected := await(t, observed.connected, "connected event")

	if connected.Version != buildVersion {
		t.Errorf("server version reported to the client = %q, want %q", connected.Version, buildVersion)
	}
	if connected.ClientID == "" {
		t.Error("the server assigned no client identifier")
	}
	var reply struct {
		ProtocolVersion string `json:"protocol_version"`
		FactoryVersion  string `json:"factory_version"`
	}
	if err := json.Unmarshal(connected.Data, &reply); err != nil {
		t.Fatalf("connect reply %q is not JSON: %v", connected.Data, err)
	}
	if reply.ProtocolVersion != clientlink.ProtocolVersion {
		t.Errorf("connect reply protocol_version = %q, want %q", reply.ProtocolVersion, clientlink.ProtocolVersion)
	}
	if reply.FactoryVersion != buildVersion {
		t.Errorf("connect reply factory_version = %q, want %q", reply.FactoryVersion, buildVersion)
	}
	if f.verifier.verifications() != 1 {
		t.Errorf("the verifier was consulted %d times, want once", f.verifier.verifications())
	}
}

// TestEveryHandshakeRefusalIsClassifiedForTheBrowser holds the three answers
// apart, because they differ in what the browser must DO.
//
// Each row asserts the exact close code rather than "some refusal". The
// distinction that matters most is the last one: a verifier that cannot be
// reached must NOT be answered with an invalid-token close, because that signs
// every live user out for an outage that is nobody's fault. A row asserting
// only "the connection was refused" would pass on exactly that bug.
func TestEveryHandshakeRefusalIsClassifiedForTheBrowser(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name        string
		token       string
		protocol    string
		unavailable error
		wantCode    uint32
		// wantTerminal says whether the close instructs the browser to STOP.
		// It is the assertion, not a detail of how the assertion is observed:
		// a browser told to give up and a browser told to come back are
		// different instructions to the same code.
		//
		// A reconnect-band row asserts only that the close was NOT terminal,
		// and it has to, because of a limitation measured here rather than
		// assumed: centrifuge-go@v0.12.0/client.go:511 is moveToConnecting and
		// its guard at :519-527 returns early when the client is ALREADY in
		// StateConnecting,
		// which it is for the whole of an initial connect. So a reconnect-band
		// close at connect time reaches no client callback carrying its code
		// at all. The code itself is pinned by
		// TestConnectRefusalClassifiesEachFailure, which drives the classifier
		// directly; what this row establishes is the half that matters to a
		// user, which is that the credential was not thrown away.
		wantTerminal bool
	}{
		// 3500, DisconnectInvalidToken. Terminal: retrying changes nothing.
		{name: "an unknown credential", token: "not-a-token", protocol: clientlink.ProtocolVersion, wantCode: 3500, wantTerminal: true},
		// 3005, DisconnectExpired. It is BELOW 3500 and therefore in the
		// reconnect band: the browser refreshes its credential and comes back,
		// rather than sending the user to login. That is exactly the
		// difference from the row above, and mapping expiry onto 3500 would
		// fail here.
		{name: "an expired credential", token: "token-expired", protocol: clientlink.ProtocolVersion, wantCode: 3005},
		// 3506, DisconnectInappropriateProtocol. Terminal: this build does not
		// speak what the client speaks.
		{name: "a protocol version this build does not speak", token: "token-a", protocol: "999", wantCode: 3506, wantTerminal: true},
		// The same, for a client that named none at all. An empty version must
		// not be read as "whatever the server speaks".
		{name: "no protocol version at all", token: "token-a", protocol: "", wantCode: 3506, wantTerminal: true},
		// 3004, DisconnectServerError, in the RECONNECT band: an outage of the
		// credential service must not log a live user out.
		{name: "a verifier that cannot answer", token: "token-a", protocol: clientlink.ProtocolVersion,
			unavailable: errors.New("the credential service is unreachable"), wantCode: 3004},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t, testLimits())
			if tt.unavailable != nil {
				f.verifier.mu.Lock()
				f.verifier.unavailable = tt.unavailable
				f.verifier.mu.Unlock()
			}
			_, observed := dial(t, f, tt.token, tt.protocol)
			if !tt.wantTerminal {
				// The window is short because the terminal close, when it
				// happens, arrives with the failed connect and not later: the
				// three terminal rows above all report inside a second.
				select {
				case event := <-observed.disconnected:
					t.Fatalf("the browser was told to STOP with code %d (%s); %d is in the reconnect band",
						event.Code, event.Reason, tt.wantCode)
				case <-observed.connected:
					t.Fatal("the handshake succeeded")
				case <-time.After(5 * time.Second):
				}
				return
			}
			event := await(t, observed.disconnected, "terminal disconnected event")
			if event.Code != tt.wantCode {
				t.Errorf("close code = %d (%s), want %d", event.Code, event.Reason, tt.wantCode)
			}
		})
	}
}

// TestAnUnsupportedProtocolIsRefusedBeforeTheCredentialIsVerified is the ORDER,
// which the codes alone cannot show.
//
// It matters for a reason a comment can state and a test cannot infer: a
// deployed browser bundle that has gone stale reconnects forever, and putting
// its whole arrival rate onto the credential verifier is a self-inflicted load
// spike on the one dependency every other request also needs.
func TestAnUnsupportedProtocolIsRefusedBeforeTheCredentialIsVerified(t *testing.T) {
	t.Parallel()

	f := newFixture(t, testLimits())
	_, observed := dial(t, f, "token-a", "999")
	await(t, observed.disconnected, "disconnected event")

	if got := f.verifier.verifications(); got != 0 {
		t.Errorf("the verifier was consulted %d times for an unsupported protocol, want 0", got)
	}
}

// TestProtobufFramingIsRefusedBeforeTheUpgrade covers every spelling this
// transport version accepts, with a positive control.
//
// The control is not decoration. Without it, a handler that refused EVERY
// request would pass all three refusals, and this surface's entire job is to
// accept the fourth.
func TestProtobufFramingIsRefusedBeforeTheUpgrade(t *testing.T) {
	t.Parallel()

	f := newFixture(t, testLimits())
	base := "http" + strings.TrimPrefix(f.url, "ws")

	for _, tt := range []struct {
		name       string
		target     string
		protocols  string
		wantStatus int
	}{
		{name: "format query", target: base + "?format=protobuf", wantStatus: http.StatusBadRequest},
		{name: "cf_protocol query", target: base + "?cf_protocol=protobuf", wantStatus: http.StatusBadRequest},
		{name: "subprotocol offer", target: base, protocols: "centrifuge-json, centrifuge-protobuf", wantStatus: http.StatusBadRequest},
		// The control, and it is the whole reason the three rows above mean
		// anything: the SAME handshake without a protobuf selector is
		// UPGRADED. Without it, a handler that refused every request would
		// satisfy all three refusals while serving nobody.
		{name: "an otherwise identical handshake is upgraded", target: base, wantStatus: http.StatusSwitchingProtocols},
		// A subprotocol offer that merely CONTAINS the forbidden token as a
		// prefix is not the forbidden token. Matching by prefix would refuse a
		// name that has nothing to do with Protobuf framing.
		{name: "a lookalike subprotocol is not the protobuf token", target: base, protocols: "centrifuge-protobuf-experimental", wantStatus: http.StatusSwitchingProtocols},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, tt.target, nil)
			if err != nil {
				t.Fatalf("NewRequest: %v", err)
			}
			req.Header.Set("Upgrade", "websocket")
			req.Header.Set("Connection", "Upgrade")
			req.Header.Set("Sec-WebSocket-Version", "13")
			req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
			if tt.protocols != "" {
				req.Header.Set("Sec-WebSocket-Protocol", tt.protocols)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("Do: %v", err)
			}
			defer func() { _ = resp.Body.Close() }()

			// A refusal is separated by its BODY as well as its status: a
			// handshake this handler declined and a handshake the websocket
			// library declined are both 400, and only the message says which.
			wantUpgrade := tt.wantStatus == http.StatusSwitchingProtocols
			if !wantUpgrade {
				body := make([]byte, 256)
				n, _ := resp.Body.Read(body)
				if !strings.Contains(string(body[:n]), "JSON protocol only") {
					t.Errorf("the request was refused with %q, not by this handler's framing refusal", body[:n])
				}
			}
			if resp.StatusCode != tt.wantStatus {
				t.Errorf("status = %d, want %d", resp.StatusCode, tt.wantStatus)
			}
			if upgraded := resp.Header.Get("Sec-WebSocket-Accept") != ""; upgraded != wantUpgrade {
				t.Errorf("upgraded = %v, want %v", upgraded, wantUpgrade)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Step 2 -- the principal in the connection context, and per-operation
// authorization.
// ---------------------------------------------------------------------------

// TestASuccessfulHandshakeAuthorizesNoChannel is step 2's rule stated as an
// outcome: the link is authenticated, and the subscribe is still refused.
func TestASuccessfulHandshakeAuthorizesNoChannel(t *testing.T) {
	t.Parallel()

	f := newFixture(t, testLimits())
	client, observed := dialSupported(t, f, "token-a")
	await(t, observed.connected, "connected event")

	for _, tt := range []struct {
		name    string
		channel string
	}{
		{name: "another tenant's session", channel: sessionChannel(tenantB, "session-1")},
		{name: "a channel that is not a session channel", channel: "arbitrary"},
		{name: "a session channel with an empty tenant", channel: "session::session-1"},
		{name: "a session channel with too many segments", channel: "session:tenant-a:session-1:extra"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := subscribe(t, client, tt.channel)
			if err == nil {
				t.Fatalf("subscribing to %q succeeded, want a refusal", tt.channel)
			}
			// 103 is ErrorPermissionDenied. The code is asserted rather than
			// the message because a message can contain anything and still be
			// the wrong answer.
			if got := codeOf(err); got != 103 {
				t.Errorf("subscribing to %q failed with code %d (%v), want 103 (permission denied)", tt.channel, got, err)
			}
		})
	}

	// The same link may still subscribe to its OWN tenant, which is what makes
	// the refusals above about the channel rather than about the link.
	if err := subscribe(t, client, sessionChannel(tenantA, "session-1")); err != nil {
		t.Fatalf("subscribing to this principal's own session failed: %v", err)
	}
}

// TestTheConnectionContextCarriesTheHandshakePrincipal drives TWO tenants
// against ONE handler at once.
//
// One connection alone could not separate "the principal came from this
// connection's handshake" from "the handler holds one principal", which is
// exactly the shape of defect a per-process cache or a shared field produces.
// The authorizer's record is compared as well as the outcome, so a handler that
// reached the right answer without consulting the principal it stored would
// still fail.
func TestTheConnectionContextCarriesTheHandshakePrincipal(t *testing.T) {
	t.Parallel()

	f := newFixture(t, testLimits())
	clientA, observedA := dialSupported(t, f, "token-a")
	await(t, observedA.connected, "connected event for tenant-a")
	clientB, observedB := dialSupported(t, f, "token-b")
	await(t, observedB.connected, "connected event for tenant-b")

	if err := subscribe(t, clientA, sessionChannel(tenantA, "s1")); err != nil {
		t.Errorf("tenant-a could not subscribe to its own session: %v", err)
	}
	if err := subscribe(t, clientB, sessionChannel(tenantB, "s2")); err != nil {
		t.Errorf("tenant-b could not subscribe to its own session: %v", err)
	}
	if err := subscribe(t, clientA, sessionChannel(tenantB, "s2")); err == nil {
		t.Error("tenant-a subscribed to tenant-b's session")
	}
	if err := subscribe(t, clientB, sessionChannel(tenantA, "s1")); err == nil {
		t.Error("tenant-b subscribed to tenant-a's session")
	}

	// The outcomes above are consistent with a handler that never consulted a
	// principal and compared the channel's tenant to itself. What rules that
	// out is the RECORD: each of the four decisions must carry the tenant of
	// the connection the subscribe arrived on, and the two channels are each
	// asked about by both principals, so the pairs are distinguishing.
	channelA := sessionChannel(tenantA, "s1")
	channelB := sessionChannel(tenantB, "s2")
	want := map[subscribeCall]bool{
		{tenant: tenantA, channel: channelA}: false,
		{tenant: tenantB, channel: channelB}: false,
		{tenant: tenantA, channel: channelB}: false,
		{tenant: tenantB, channel: channelA}: false,
	}
	calls := f.authorizer.subscribeCalls()
	for _, call := range calls {
		if _, expected := want[call]; !expected {
			t.Errorf("the authorizer was asked about %q for tenant %q, which no connection should have produced", call.channel, call.tenant)
			continue
		}
		want[call] = true
	}
	for call, seen := range want {
		if !seen {
			t.Errorf("the authorizer was never asked about %q for tenant %q", call.channel, call.tenant)
		}
	}
	if len(calls) != len(want) {
		t.Errorf("the authorizer was consulted %d times, want %d", len(calls), len(want))
	}
}

// TestOneLinkCarriesManySessionSubscriptions is step 1's multiplexing claim,
// and it is sized to cross the transport's own silent default.
//
// centrifuge@v0.38.0/node.go:135-136 sets ClientChannelLimit to 128 when the
// configuration leaves it zero, so a handler that did not pass
// MaxChannelsPerConnection would fail here at the 129th channel and nowhere
// else. The count is an absolute literal above that default for exactly that
// reason.
func TestOneLinkCarriesManySessionSubscriptions(t *testing.T) {
	t.Parallel()

	limits := testLimits()
	limits.MaxChannelsPerConnection = 200
	f := newFixture(t, limits)
	client, observed := dialSupported(t, f, "token-a")
	await(t, observed.connected, "connected event")

	for i := range 200 {
		channel := sessionChannel(tenantA, fmt.Sprintf("session-%03d", i))
		if err := subscribe(t, client, channel); err != nil {
			t.Fatalf("subscription %d of 200 on one link failed: %v", i+1, err)
		}
	}

	// And the ceiling is a ceiling: the 201st is refused, so the number is
	// established as Factory's rather than as "more than 128".
	err := subscribe(t, client, sessionChannel(tenantA, "session-over"))
	if err == nil {
		t.Fatal("the 201st subscription succeeded against a 200-channel ceiling")
	}
	// 106 is ErrorLimitExceeded.
	if got := codeOf(err); got != 106 {
		t.Errorf("the 201st subscription failed with code %d (%v), want 106 (limit exceeded)", got, err)
	}
}

// ---------------------------------------------------------------------------
// Step 3 -- publication is disabled; commands are typed RPCs.
// ---------------------------------------------------------------------------

// TestAClientMayNotPublish is the half of step 3 that is a refusal.
//
// The subscription is established FIRST, so the refusal cannot be explained by
// the client having no business on the channel: it is subscribed, it is
// authorized, and publishing is still not something a client may do.
func TestAClientMayNotPublish(t *testing.T) {
	t.Parallel()

	f := newFixture(t, testLimits())
	client, observed := dialSupported(t, f, "token-a")
	await(t, observed.connected, "connected event")

	channel := sessionChannel(tenantA, "session-1")
	if err := subscribe(t, client, channel); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), waitFor)
	defer cancel()
	_, err := client.Publish(ctx, channel, []byte(`{"forged":true}`))
	if err == nil {
		t.Fatal("a client published to a channel it was subscribed to")
	}
	// 108 is ErrorNotAvailable: the library's answer when no publish handler is
	// registered. Registering one is what enables client publication, so the
	// absence of the handler IS the refusal.
	if got := codeOf(err); got != 108 {
		t.Errorf("publish failed with code %d (%v), want 108 (not available)", got, err)
	}
}

// TestEveryCommandRPCIsAuthorizedUnderItsOwnKind is the other half.
//
// Each method must reach the authorizer with the command kind the REST route
// admits under, and the session it named. The table restates the mapping with
// ABSOLUTE kinds rather than calling CommandKindFor, because a table built from
// the function under test asserts only that the function equals itself.
func TestEveryCommandRPCIsAuthorizedUnderItsOwnKind(t *testing.T) {
	t.Parallel()

	f := newFixture(t, testLimits())
	client, observed := dialSupported(t, f, "token-a")
	await(t, observed.connected, "connected event")

	ctx, cancel := context.WithTimeout(context.Background(), waitFor)
	defer cancel()

	for _, tt := range []struct {
		method clientlink.Method
		kind   sessionstore.CommandKind
	}{
		{clientlink.MethodSessionCreate, "create"},
		{clientlink.MethodSessionInput, "input"},
		{clientlink.MethodSessionInterrupt, "interrupt"},
		{clientlink.MethodSessionRestore, "restore"},
		{clientlink.MethodGateRespond, "gate_response"},
	} {
		session := sessionwire.SessionID("session-" + string(tt.kind))
		id := sessionwire.CommandID("cmd-" + string(tt.kind))
		f.admitter.mu.Lock()
		f.admitter.entry = acceptedEntry(session, id)
		f.admitter.mu.Unlock()
		result, err := client.RPC(ctx, string(tt.method), commandBody(tt.method, session, id))
		if err != nil {
			t.Fatalf("%s = %v, want the admitted record", tt.method, err)
		}
		if got := statusOf(t, result.Data).CommandID; got != id {
			t.Errorf("%s was answered for command %q, want %q", tt.method, got, id)
		}

		var seen *controlCall
		for _, call := range f.authorizer.controlCalls() {
			if call.session == session {
				seen = &call
			}
		}
		if seen == nil {
			t.Errorf("%s did not reach the authorizer for session %q", tt.method, session)
			continue
		}
		if seen.kind != tt.kind {
			t.Errorf("%s was authorized under kind %q, want %q", tt.method, seen.kind, tt.kind)
		}
		if seen.tenant != tenantA {
			t.Errorf("%s was authorized for tenant %q, want %q", tt.method, seen.tenant, tenantA)
		}
	}
}

// TestARefusedCommandRPCIsDenied separates "the authorizer was consulted" from
// "its answer was obeyed". Without it, a handler that called the authorizer and
// discarded the error would pass the case above.
func TestARefusedCommandRPCIsDenied(t *testing.T) {
	t.Parallel()

	f := newFixture(t, testLimits())
	f.authorizer.mu.Lock()
	f.authorizer.denyKind = command.KindInterrupt
	f.authorizer.mu.Unlock()

	client, observed := dialSupported(t, f, "token-a")
	await(t, observed.connected, "connected event")
	ctx, cancel := context.WithTimeout(context.Background(), waitFor)
	defer cancel()

	f.admitter.mu.Lock()
	f.admitter.entry = acceptedEntry("session-1", "cmd-1")
	f.admitter.mu.Unlock()

	_, err := client.RPC(ctx, string(clientlink.MethodSessionInterrupt),
		commandBody(clientlink.MethodSessionInterrupt, "session-1", "cmd-1"))
	if got := codeOf(err); got != 103 {
		t.Errorf("a denied interrupt failed with code %d (%v), want 103 (permission denied)", got, err)
	}
	if calls := f.admitter.recorded(); len(calls) != 0 {
		t.Errorf("a denied interrupt reached admission %d times", len(calls))
	}
	// The sibling method, denied by nothing, must still be admitted: otherwise
	// "denied" would be indistinguishable from "every RPC is refused".
	result, err := client.RPC(ctx, string(clientlink.MethodSessionInput),
		commandBody(clientlink.MethodSessionInput, "session-1", "cmd-1"))
	if err != nil {
		t.Fatalf("an allowed input failed with %v, want the admitted record", err)
	}
	if got := statusOf(t, result.Data).CommandID; got != "cmd-1" {
		t.Errorf("the reply named command %q, want %q", got, "cmd-1")
	}
}

// TestAnUnknownRPCMethodIsRefusedWithoutAnAuthorizationDecision holds the order
// the engine documents: there is no command kind to ask about, so asking would
// be a question whose answer means nothing.
func TestAnUnknownRPCMethodIsRefusedWithoutAnAuthorizationDecision(t *testing.T) {
	t.Parallel()

	f := newFixture(t, testLimits())
	client, observed := dialSupported(t, f, "token-a")
	await(t, observed.connected, "connected event")
	ctx, cancel := context.WithTimeout(context.Background(), waitFor)
	defer cancel()

	_, err := client.RPC(ctx, "session.delete-everything", []byte(`{"session_id":"session-1"}`))
	// 104 is ErrorMethodNotFound.
	if got := codeOf(err); got != 104 {
		t.Errorf("an unknown method failed with code %d (%v), want 104 (method not found)", got, err)
	}
	if calls := f.authorizer.controlCalls(); len(calls) != 0 {
		t.Errorf("the authorizer was consulted %d times about an unknown method, want 0: %+v", len(calls), calls)
	}
}

// ---------------------------------------------------------------------------
// Step 1 -- ping/pong, cancellation, capacity, and reconnect to another
// replica.
// ---------------------------------------------------------------------------

// TestAnIdleLinkSurvivesSeveralPingPeriods is the inherited ping finding
// measured at Factory's own edge.
//
// The cadence is deliberately short and the pong deadline shorter, so an idle
// connection crosses several complete ping/pong cycles inside the case. If the
// reply failed to carry a usable cadence -- which is what a sub-second interval
// would cause, since it crosses the wire as zero and the client is then never
// asked to pong -- the server would close this HEALTHY connection with
// DisconnectNoPong (3012). The assertion is therefore not "nothing happened":
// it is that a specific close did not happen while the mechanism that produces
// it was running as fast as the limits allow.
func TestAnIdleLinkSurvivesSeveralPingPeriods(t *testing.T) {
	t.Parallel()

	limits := testLimits()
	limits.PingInterval = time.Second
	limits.PongTimeout = 500 * time.Millisecond
	limits.WriteTimeout = 400 * time.Millisecond
	f := newFixture(t, limits)

	client, observed := dialSupported(t, f, "token-a")
	await(t, observed.connected, "connected event")

	// Four ping periods plus a pong deadline. Nothing is sent on this link in
	// the meantime, which is what makes it idle.
	// BOTH callbacks are watched. DisconnectNoPong is 3012, which is in the
	// reconnect band and therefore never reaches OnDisconnected: a case that
	// watched only the terminal callback could not see the very failure it
	// exists to rule out.
	select {
	case event := <-observed.disconnected:
		t.Fatalf("an idle link was closed terminally with code %d (%s)", event.Code, event.Reason)
	case event := <-connectingDrops(observed):
		t.Fatalf("an idle link was dropped with code %d (%s); 3012 is DisconnectNoPong", event.Code, event.Reason)
	case <-time.After(4*limits.PingInterval + limits.PongTimeout):
	}

	// And it is still usable, which separates "not closed" from "closed and the
	// client has not noticed".
	if err := subscribe(t, client, sessionChannel(tenantA, "session-1")); err != nil {
		t.Errorf("an idle link could not subscribe afterwards: %v", err)
	}
}

// TestShutdownClosesEveryLink is cancellation: a replica going away must take
// its connections with it rather than leaving them to time out.
func TestShutdownClosesEveryLink(t *testing.T) {
	t.Parallel()

	f := newFixture(t, testLimits())
	_, observed := dialSupported(t, f, "token-a")
	await(t, observed.connected, "connected event")
	if got := f.handler.Connections(); got != 1 {
		t.Fatalf("Connections() = %d after one client connected, want 1", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), waitFor)
	defer cancel()
	if err := f.handler.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	// 3001 is DisconnectShutdown, in the RECONNECT band: the browser should go
	// to another replica, not to the login page. Being in that band is why it
	// arrives through OnConnecting and never through OnDisconnected.
	awaitReconnectCode(t, observed, 3001)
	if got := f.handler.Connections(); got != 0 {
		t.Errorf("Connections() = %d after shutdown, want 0", got)
	}
}

// TestAReplicaRefusesConnectionsBeyondItsCeiling is MaxConnections, measured.
//
// The ceiling is 1 so the case is cheap, and the FIRST connection is asserted
// to succeed, so "the ceiling was enforced" cannot be satisfied by a handler
// that refuses everything.
func TestAReplicaRefusesConnectionsBeyondItsCeiling(t *testing.T) {
	t.Parallel()

	limits := testLimits()
	limits.MaxConnections = 1
	f := newFixture(t, limits)

	_, first := dialSupported(t, f, "token-a")
	await(t, first.connected, "connected event for the first client")

	_, second := dialSupported(t, f, "token-b")
	event := await(t, second.disconnected, "disconnected event for the second client")
	// 3504 is DisconnectConnectionLimit.
	if event.Code != 3504 {
		t.Errorf("the second client was closed with code %d (%s), want 3504 (connection limit)", event.Code, event.Reason)
	}
	// The refusal happened before the credential was verified: the first
	// connection is the only verification.
	if got := f.verifier.verifications(); got != 1 {
		t.Errorf("the verifier was consulted %d times, want 1 (the refused client must not reach it)", got)
	}
}

// TestABrowserReconnectsToAnotherReplicaWithTheSameToken is the stateless
// reconnect A5.1 left to this task.
//
// A5.1 proved reconnect only at the TRANSPORT level, to the same server. What
// makes reconnecting to a DIFFERENT Factory work is that nothing about the
// connection is held in the replica: the two handlers below are independently
// constructed, share no memory, and are given their own authenticator over the
// same deployment Verifier -- which is the whole of what a deployment shares.
// The same token that authenticated on one authorizes the same channel on the
// other, with no handoff between them.
func TestABrowserReconnectsToAnotherReplicaWithTheSameToken(t *testing.T) {
	t.Parallel()

	first := newFixture(t, testLimits())
	second := newFixture(t, testLimits())
	if first.handler == second.handler {
		t.Fatal("the two replicas are the same handler")
	}

	client, observed := dialSupported(t, first, "token-a")
	await(t, observed.connected, "connected event on the first replica")
	channel := sessionChannel(tenantA, "session-1")
	if err := subscribe(t, client, channel); err != nil {
		t.Fatalf("subscribe on the first replica: %v", err)
	}

	// The first replica goes away, exactly as a rolling deployment would take
	// it away.
	ctx, cancel := context.WithTimeout(context.Background(), waitFor)
	defer cancel()
	if err := first.handler.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown of the first replica: %v", err)
	}
	awaitReconnectCode(t, observed, 3001)

	// The browser presents what it already holds to the second replica.
	moved, movedObserved := dialSupported(t, second, "token-a")
	connected := await(t, movedObserved.connected, "connected event on the second replica")
	if connected.ClientID == "" {
		t.Error("the second replica assigned no client identifier")
	}
	if err := subscribe(t, moved, channel); err != nil {
		t.Errorf("subscribe on the second replica: %v", err)
	}

	// The second replica reached its own decision rather than inheriting one:
	// it verified the credential itself and asked its own authorizer.
	if got := second.verifier.verifications(); got != 1 {
		t.Errorf("the second replica verified the credential %d times, want 1", got)
	}
	if calls := second.authorizer.subscribeCalls(); len(calls) != 1 || calls[0].channel != channel {
		t.Errorf("the second replica's authorizer saw %+v, want one call for %q", calls, channel)
	}
}
