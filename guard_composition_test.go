package factory_test

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	factoryidentity "github.com/looprig/factory/identity"
	"github.com/looprig/factory/internal/httpapi"
	internalidentity "github.com/looprig/factory/internal/identity"
	"github.com/looprig/factory/internal/realtime/clientlink"
	"github.com/looprig/sessionstore"
)

// This file is the composition the origin guard and the authorizer meet in.
//
// It exists because the claim it holds is not a claim about either of them
// separately: "a successful WebSocket handshake does not replace
// per-subscription and per-RPC authorization" is FALSE OF A COMPONENT and true
// only of the composition, so it is asserted where both decisions have a
// reader. The guard alone can only report that it admitted a handshake, and the
// authorizer alone knows nothing about handshakes.
//
// What is real here and what is not, stated rather than implied: the guard, the
// authenticator, the authorizer and the operation context are production code.
// In the first three cases the LINK is not -- the object below carries the
// handshake's outcome and nothing else, and calls the production authorizer
// through the interface clientlink declares for itself -- and the limit of that
// form is that it cannot prove an engine consults the authorizer; it proves
// that the handshake hands a subscription nothing that would let it skip one.
//
// TestTheGuardDecidesOriginBeforeTheClientLinkUpgrade, added after A6.1, does
// use the real clientlink.Handler. A6.1 had deferred that composition to A9.1
// on the belief that it needed production wiring; it does not, because
// httpapi.NewGuard, Guard.Wrap and clientlink.NewHandler are all exported and
// the chain is three lines. Composition seams are not the same thing as
// composition CODE, and a claim provable at the test level should not wait for
// the wiring that will later state it in production.

const composedOrigin = "https://app.example.com"

type composedClock struct{ now time.Time }

func (c composedClock) Now() time.Time { return c.now }

// composedVerifier accepts one credential value and issues the claims of one
// tenant, so the principal a handshake ends up holding is a real one derived by
// the real authenticator.
type composedVerifier struct {
	tenant  sessionwire.TenantID
	subject string
	expiry  time.Time
}

func (v composedVerifier) VerifyCredential(_ context.Context, credential internalidentity.Credential) (internalidentity.Claims, error) {
	if credential.Value() != "browser-session-credential" {
		return internalidentity.Claims{}, factoryidentity.ErrUnauthenticated
	}
	return internalidentity.Claims{Tenant: v.tenant, Subject: v.subject, Kind: factoryidentity.KindActor, ExpiresAt: v.expiry}, nil
}

// establishedLink is what a completed handshake produces: the principal the
// connection was authenticated as, and nothing else. It holds no grant.
type establishedLink struct {
	principal  factoryidentity.Principal
	authorizer clientlink.Authorizer
}

// Subscribe is the per-subscription decision. It consults the authorizer on
// every call; there is no handshake result for it to consult instead.
func (l *establishedLink) Subscribe(ctx context.Context, channel string) error {
	return l.authorizer.AuthorizeSubscribe(ctx, l.principal, channel)
}

// Call is the per-RPC decision.
func (l *establishedLink) Call(ctx context.Context, session sessionwire.SessionID, kind sessionstore.CommandKind) error {
	return l.authorizer.AuthorizeControl(ctx, l.principal, session, kind)
}

// TestASuccessfulHandshakeDoesNotAuthorizeASubscription is the whole point of
// the file. The handshake really succeeds -- 101, through the real guard, with
// a real principal -- and the very next subscription on that same connection is
// still refused for a channel outside the principal's tenant.
func TestASuccessfulHandshakeDoesNotAuthorizeASubscription(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	authenticator, err := internalidentity.NewAuthenticator(internalidentity.Config{
		Verifier: composedVerifier{tenant: "tenant-a", subject: "subject-a", expiry: now.Add(time.Hour)},
		Clock:    composedClock{now: now},
	})
	if err != nil {
		t.Fatalf("NewAuthenticator: %v", err)
	}
	guard, err := httpapi.NewGuard(httpapi.GuardConfig{
		CSRF: factoryidentity.CSRFConfig{
			SharedKey:      []byte("0123456789abcdef0123456789abcdef"),
			TokenTTL:       time.Hour,
			TrustedOrigins: []string{composedOrigin},
		},
		Credentials: authenticator,
		Clock:       composedClock{now: now},
	})
	if err != nil {
		t.Fatalf("NewGuard: %v", err)
	}

	var link *establishedLink
	// The authentication middleware is outside the guard, because a token is
	// bound to a principal and there is no principal before it has run.
	handler := authenticateThen(t, authenticator, guard.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		operation, ok := internalidentity.OperationContextFrom(r.Context())
		if !ok {
			t.Error("the handshake reached the upgrade with no operation context")
			w.WriteHeader(http.StatusForbidden)
			return
		}
		link = &establishedLink{principal: operation.Principal, authorizer: internalidentity.Authorizer{}}
		w.Header().Set("Upgrade", "websocket")
		w.Header().Set("Connection", "Upgrade")
		w.WriteHeader(http.StatusSwitchingProtocols)
	})))

	handshake := httptest.NewRequest(http.MethodGet, composedOrigin+"/v1/link", nil)
	handshake.Header.Set("Origin", composedOrigin)
	handshake.Header.Set("Connection", "Upgrade")
	handshake.Header.Set("Upgrade", "websocket")
	handshake.AddCookie(&http.Cookie{Name: internalidentity.DefaultCookieName, Value: "browser-session-credential"})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, handshake)

	// The premise. Without it every assertion below is vacuous, so it is a
	// Fatal rather than an Error.
	if recorder.Code != http.StatusSwitchingProtocols {
		t.Fatalf("the handshake was answered %d (%s); this test asserts what a SUCCESSFUL handshake does not grant", recorder.Code, recorder.Body)
	}
	if link == nil {
		t.Fatal("the handshake succeeded but established no link")
	}
	if link.principal.Tenant() != "tenant-a" {
		t.Fatalf("the established link holds tenant %q, want tenant-a", link.principal.Tenant())
	}

	ctx := t.Context()
	if err := link.Subscribe(ctx, "session:tenant-b:session-b"); !errors.Is(err, internalidentity.ErrUnauthorized) {
		t.Errorf("Subscribe to another tenant's channel over an established link = %v, want ErrUnauthorized", err)
	}

	// And the connection is not broken: its OWN tenant's channel is still
	// granted, so the refusal above is the tenant boundary and not a link that
	// authorizes nothing.
	if err := link.Subscribe(ctx, "session:tenant-a:session-a"); err != nil {
		t.Errorf("Subscribe to the principal's own channel = %v, want nil", err)
	}

	// The RPC seam is asserted for what it actually decides, which is narrower
	// than the subscription seam and deliberately so. AuthorizeControl does not
	// inspect the SessionID -- a session identifier is tenant-local and opaque,
	// and looking one up before authorizing would itself cross the boundary and
	// disclose existence -- so "session-b" is granted here. What confines a
	// command to its tenant is the handler building its SessionStore request
	// with the principal's tenant, which is A2's obligation and is enforced by
	// nothing today. What this composition can and does show is that the
	// decision is taken PER CALL from the link's principal: a link established
	// for a principal with no tenant is refused the identical call.
	if err := link.Call(ctx, "session-a", sessionstore.CommandKind("input")); err != nil {
		t.Errorf("an RPC within the principal's own tenant = %v, want nil", err)
	}
	if err := link.Call(ctx, "session-b", sessionstore.CommandKind("input")); err != nil {
		t.Errorf("AuthorizeControl inspected the session identifier: %v", err)
	}
	unauthenticated := &establishedLink{authorizer: internalidentity.Authorizer{}}
	if err := unauthenticated.Call(ctx, "session-a", sessionstore.CommandKind("input")); !errors.Is(err, internalidentity.ErrUnauthorized) {
		t.Errorf("an RPC over a link holding no principal = %v, want ErrUnauthorized", err)
	}
}

// TestTheGuardAdmitsAHandshakeItCannotAuthorize is the other half, and it is
// what stops the test above from being satisfied by a guard that rejects
// cross-tenant handshakes itself. The guard has no channel and no session to
// decide about: it admits the same handshake for a principal whose every
// subscription the authorizer then refuses.
func TestTheGuardAdmitsAHandshakeItCannotAuthorize(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	authenticator, err := internalidentity.NewAuthenticator(internalidentity.Config{
		Verifier: composedVerifier{tenant: "tenant-z", subject: "subject-z", expiry: now.Add(time.Hour)},
		Clock:    composedClock{now: now},
	})
	if err != nil {
		t.Fatalf("NewAuthenticator: %v", err)
	}
	guard, err := httpapi.NewGuard(httpapi.GuardConfig{
		CSRF: factoryidentity.CSRFConfig{
			SharedKey:      []byte("0123456789abcdef0123456789abcdef"),
			TokenTTL:       time.Hour,
			TrustedOrigins: []string{composedOrigin},
		},
		Credentials: authenticator,
		Clock:       composedClock{now: now},
	})
	if err != nil {
		t.Fatalf("NewGuard: %v", err)
	}

	handshake := httptest.NewRequest(http.MethodGet, composedOrigin+"/v1/link", nil)
	handshake.Header.Set("Origin", composedOrigin)
	handshake.Header.Set("Upgrade", "websocket")
	handshake.AddCookie(&http.Cookie{Name: internalidentity.DefaultCookieName, Value: "browser-session-credential"})
	principal, err := authenticator.AuthenticateRequest(t.Context(), handshake)
	if err != nil {
		t.Fatalf("AuthenticateRequest: %v", err)
	}
	handshake = handshake.WithContext(authenticator.NewOperationContext(t.Context(), handshake, principal))

	if reason, allowed := guard.Check(handshake); !allowed {
		t.Fatalf("the guard rejected the handshake with %q", reason)
	}
	link := &establishedLink{principal: principal, authorizer: internalidentity.Authorizer{}}
	for _, channel := range []string{
		"session:tenant-a:session-a",
		"session:tenant-b:session-b",
		"session:tenant-z:session-a",
	} {
		err := link.Subscribe(t.Context(), channel)
		want := channel == "session:tenant-z:session-a"
		if (err == nil) != want {
			t.Errorf("Subscribe(%q) = %v, want granted=%v", channel, err, want)
		}
	}
}

// authenticateThen is the authentication middleware the guard is mounted
// inside. It is the composition's own, not production code: the real one is
// httpapi.Router's, added by A2.1, and this is the minimum that gives the guard
// a principal to bind a token to. It is kept rather than replaced by the router
// because this file's claim is about the guard and the authorizer meeting, and
// routing through the whole public surface would put a third component between
// them.
func authenticateThen(t *testing.T, authenticator *internalidentity.Authenticator, next http.Handler) http.Handler {
	t.Helper()

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, err := authenticator.AuthenticateRequest(r.Context(), r)
		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r.WithContext(authenticator.NewOperationContext(r.Context(), r, principal)))
	})
}

// TestMountingTheGuardOutsideAuthenticationFailsLoudly pins the direction of
// the ordering mistake. The guard has to run after authentication, and the
// consequence of getting it wrong must be that writes stop working -- not that
// the CSRF check is silently skipped.
func TestMountingTheGuardOutsideAuthenticationFailsLoudly(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	authenticator, err := internalidentity.NewAuthenticator(internalidentity.Config{
		Verifier: composedVerifier{tenant: "tenant-a", subject: "subject-a", expiry: now.Add(time.Hour)},
		Clock:    composedClock{now: now},
	})
	if err != nil {
		t.Fatalf("NewAuthenticator: %v", err)
	}
	guard, err := httpapi.NewGuard(httpapi.GuardConfig{
		CSRF: factoryidentity.CSRFConfig{
			SharedKey:      []byte("0123456789abcdef0123456789abcdef"),
			TokenTTL:       time.Hour,
			TrustedOrigins: []string{composedOrigin},
		},
		Credentials: authenticator,
		Clock:       composedClock{now: now},
	})
	if err != nil {
		t.Fatalf("NewGuard: %v", err)
	}

	reached := false
	// The WRONG order: the guard outside, authentication inside.
	wrong := guard.Wrap(authenticateThen(t, authenticator, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached = true
		w.WriteHeader(http.StatusNoContent)
	})))

	r := httptest.NewRequest(http.MethodPost, composedOrigin+"/v1/sessions", nil)
	r.Header.Set("Origin", composedOrigin)
	r.AddCookie(&http.Cookie{Name: internalidentity.DefaultCookieName, Value: "browser-session-credential"})
	principal, err := authenticator.AuthenticateRequest(t.Context(), r)
	if err != nil {
		t.Fatalf("AuthenticateRequest: %v", err)
	}
	token, _, err := guard.IssueToken(principal)
	if err != nil {
		t.Fatalf("IssueToken: %v", err)
	}
	r.Header.Set(httpapi.CSRFHeaderName, token)

	recorder := httptest.NewRecorder()
	wrong.ServeHTTP(recorder, r)
	if reached {
		t.Error("a state-changing request reached the handler with the guard mounted outside authentication")
	}
	if recorder.Code != http.StatusForbidden {
		t.Errorf("status = %d, want 403: the ordering mistake must present as a rejection", recorder.Code)
	}
	if body := recorder.Body.String(); !strings.Contains(body, string(httpapi.ReasonUnauthenticated)) {
		t.Errorf("rejection body %q does not name %q", body, httpapi.ReasonUnauthenticated)
	}
}

// TestTheGuardDecidesOriginBeforeTheClientLinkUpgrade is the composition A6.1
// deferred to A9.1 and did not have to.
//
// `clientlink`'s WebsocketConfig.CheckOrigin admits everything on purpose, and
// node_test.go asserts that deliberate hole. What it cannot show is the half
// that makes the hole safe: that something else decided origin FIRST. That is
// not a claim about either component -- the guard knows nothing about
// websockets beyond the upgrade headers, and the link cannot see what ran
// before it -- so it is asserted here, over the REAL guard wrapped around the
// REAL clientlink.Handler, with no production wiring required. `httpapi.NewGuard`
// and `Guard.Wrap` are exported, `clientlink.NewHandler` returns an
// http.Handler, and that is the whole composition.
//
// The handshake is written by hand over a socket rather than dialled with a
// client library, for two reasons: the answer being measured is the STATUS LINE
// (101 against 403), and `gorilla/websocket` is an INDIRECT dependency of this
// module which a test importing it would make direct.
//
// Three rows, and the first is the control that makes the other two mean
// something. Without it, a chain that refused every handshake would pass.
func TestTheGuardDecidesOriginBeforeTheClientLinkUpgrade(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	authenticator, err := internalidentity.NewAuthenticator(internalidentity.Config{
		Verifier: composedVerifier{tenant: "tenant-a", subject: "subject-a", expiry: now.Add(time.Hour)},
		Clock:    composedClock{now: now},
	})
	if err != nil {
		t.Fatalf("NewAuthenticator: %v", err)
	}
	link, err := clientlink.NewHandler(clientlink.Config{
		// The production Authenticator already IS clientlink's seam: it declares
		// AuthenticateLink over the same Verifier, which is what makes a link
		// credential and a bearer credential the same material.
		Authenticator: authenticator,
		Authorizer:    internalidentity.Authorizer{},
		// The durable plane fails closed. This case is about the ORDER the
		// origin guard and the link decide in, and no handshake it drives ever
		// gets as far as a command; an admitter that answered would let a case
		// pass on a fabricated acceptance.
		Admitter: refusingAdmitter{},
		// The demand plane fails closed for the reason the admitter does: no
		// handshake this case drives reaches a subscription, and a demand
		// manager that answered would let a case pass on a binding nothing
		// took.
		Demand: refusingDemand{},
		Clock:  unusedClock{},
		Limits: clientlink.Limits{
			MaxConnections:           16,
			MaxChannelsPerConnection: 32,
			PerConnectionQueueBytes:  1 << 20,
			WriteTimeout:             5 * time.Second,
			PingInterval:             25 * time.Second,
			PongTimeout:              10 * time.Second,
			CommandTimeout:           30 * time.Second,
			DemandReleaseDebounce:    30 * time.Second,
			DemandTimeout:            30 * time.Second,
		},
		Version: "v-composed",
	})
	if err != nil {
		t.Fatalf("clientlink.NewHandler: %v", err)
	}

	// reached counts requests that got PAST the guard. A status code alone
	// cannot separate "the guard refused" from "the link refused", and the
	// ordering is the claim.
	var reached atomic.Int64
	counted := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached.Add(1)
		link.ServeHTTP(w, r)
	})

	// The trusted origin cannot be known until the listener has a port, and the
	// guard's host rule compares against exactly that authority, so the server
	// is started around an indirection and the chain is installed once the
	// address exists. Nothing serves a request in between.
	var composed http.Handler
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		composed.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = link.Shutdown(ctx)
	})

	guard, err := httpapi.NewGuard(httpapi.GuardConfig{
		CSRF: factoryidentity.CSRFConfig{
			SharedKey:      []byte("0123456789abcdef0123456789abcdef"),
			TokenTTL:       time.Hour,
			TrustedOrigins: []string{server.URL},
		},
		Credentials: authenticator,
		Clock:       composedClock{now: now},
	})
	if err != nil {
		t.Fatalf("NewGuard: %v", err)
	}
	composed = authenticateThen(t, authenticator, guard.Wrap(counted))

	for _, tt := range []struct {
		name string
		// origin is sent only when present.
		origin     string
		sendOrigin bool
		wantStatus int
		// wantReason is the guard's stable code, empty when the handshake is
		// expected to be upgraded.
		wantReason string
		wantPassed bool
	}{
		{
			name:       "the deployment's own origin is upgraded",
			origin:     server.URL,
			sendOrigin: true,
			wantStatus: http.StatusSwitchingProtocols,
			wantPassed: true,
		},
		{
			name:       "another site's origin never reaches the link",
			origin:     "https://evil.example.com",
			sendOrigin: true,
			wantStatus: http.StatusForbidden,
			wantReason: string(httpapi.ReasonOriginNotTrusted),
		},
		{
			name:       "an upgrade with an ambient credential and no origin never reaches the link",
			wantStatus: http.StatusForbidden,
			wantReason: string(httpapi.ReasonUpgradeOriginMissing),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			before := reached.Load()
			status, body := rawHandshake(t, server.Listener.Addr().String(), tt.origin, tt.sendOrigin)
			if status != tt.wantStatus {
				t.Errorf("the handshake was answered %d, want %d (body %q)", status, tt.wantStatus, body)
			}
			if tt.wantReason != "" && !strings.Contains(body, tt.wantReason) {
				t.Errorf("the refusal body %q does not name %q, so it is not the guard's decision", body, tt.wantReason)
			}
			if passed := reached.Load() > before; passed != tt.wantPassed {
				t.Errorf("the request reached the clientlink handler = %v, want %v", passed, tt.wantPassed)
			}
		})
	}
}

// rawHandshake writes one WebSocket handshake and returns its status and body.
//
// Written by hand because the measurement IS the status line, and because
// importing a websocket client here would promote gorilla/websocket from an
// indirect requirement of this module to a direct one.
func rawHandshake(t *testing.T, address, origin string, sendOrigin bool) (int, string) {
	t.Helper()

	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatalf("dial %s: %v", address, err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
		t.Fatalf("SetDeadline: %v", err)
	}

	request := "GET /v1/realtime HTTP/1.1\r\n" +
		"Host: " + address + "\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Version: 13\r\n" +
		"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n" +
		"Cookie: " + internalidentity.DefaultCookieName + "=browser-session-credential\r\n"
	if sendOrigin {
		request += "Origin: " + origin + "\r\n"
	}
	request += "\r\n"
	if _, err := io.WriteString(conn, request); err != nil {
		t.Fatalf("write handshake: %v", err)
	}

	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read handshake response: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read handshake body: %v", err)
	}
	return response.StatusCode, string(body)
}

// unusedClock is the debounce clock for a composition that never establishes a
// subscription. It schedules nothing, so a release this case somehow reached
// would never run and the demand fake's own refusal would be reported instead.
type unusedClock struct{}

func (unusedClock) AfterFunc(time.Duration, func()) func() bool {
	return func() bool { return false }
}

// refusingDemand is the delivery-demand plane for a composition case that
// tracks nothing.
type refusingDemand struct{}

var errNoDemandHere = errors.New("this composition tracks no delivery demand")

func (refusingDemand) Acquire(context.Context, sessionwire.TenantID, sessionwire.SessionID) error {
	return errNoDemandHere
}

func (refusingDemand) Release(context.Context, sessionwire.TenantID, sessionwire.SessionID) error {
	return errNoDemandHere
}

// refusingAdmitter is the durable command plane for a composition case that
// admits nothing.
type refusingAdmitter struct{}

var errNoAdmissionHere = errors.New("this composition admits no command")

func (refusingAdmitter) AdmitCreate(context.Context, factoryidentity.Principal, sessionwire.CreateRequest) (sessionstore.DispositionInboxEntry, bool, error) {
	return sessionstore.DispositionInboxEntry{}, false, errNoAdmissionHere
}

func (refusingAdmitter) AdmitInput(context.Context, factoryidentity.Principal, sessionwire.InputRequest) (sessionstore.InboxEntry, bool, error) {
	return sessionstore.InboxEntry{}, false, errNoAdmissionHere
}

func (refusingAdmitter) AdmitInterrupt(context.Context, factoryidentity.Principal, sessionwire.InterruptRequest) (sessionstore.InboxEntry, bool, error) {
	return sessionstore.InboxEntry{}, false, errNoAdmissionHere
}

func (refusingAdmitter) AdmitRestore(context.Context, factoryidentity.Principal, sessionwire.RestoreRequest) (sessionstore.InboxEntry, bool, error) {
	return sessionstore.InboxEntry{}, false, errNoAdmissionHere
}

func (refusingAdmitter) AdmitGateResponse(context.Context, factoryidentity.Principal, sessionwire.GateResponseRequest) (sessionstore.InboxEntry, bool, error) {
	return sessionstore.InboxEntry{}, false, errNoAdmissionHere
}
