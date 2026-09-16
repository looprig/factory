package clientlink_test

import (
	"net/http"
	"testing"

	"github.com/looprig/factory/identity"
	internalidentity "github.com/looprig/factory/internal/identity"
	"github.com/looprig/factory/internal/realtime/clientlink"
)

// routerLike is the composed router's authentication chain reduced to what
// the handshake can observe: the upgrade is authenticated by the SAME
// Authenticator the handler holds, a refusal is answered before the handler is
// entered, and a success records the principal and the credential SOURCE on
// the request context exactly as internal/httpapi's authenticate middleware
// does (NewOperationContext derives the source from the request; nothing here
// states it).
func routerLike(authenticator *internalidentity.Authenticator, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, err := authenticator.AuthenticateRequest(r.Context(), r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r.WithContext(authenticator.NewOperationContext(r.Context(), r, principal)))
	})
}

func cookieHeader(token string) http.Header {
	return http.Header{"Cookie": {internalidentity.DefaultCookieName + "=" + token}}
}

func bearerHeader(token string) http.Header {
	return http.Header{"Authorization": {"Bearer " + token}}
}

// TestACookieAuthenticatedUpgradeNeedsNoConnectToken is B7: the embedded WUI
// holds a session cookie and sends no connect token, and before this the
// handshake refused the empty credential before any verifier ran
// (internal/identity/http.go, authenticate's first check), so REST worked and
// realtime never did.
//
// The rows are the whole decision table, and each is asserted absolutely:
// connected, or refused with the exact terminal code 3500
// (centrifuge.DisconnectInvalidToken, the unauthenticated close). "Bearer path
// unchanged" is two rows -- a bearer header on the upgrade is NOT reused, and
// a connect token is verified as before whether or not a cookie rode the
// upgrade -- because a rule that reused any authenticated upgrade would pass
// the first and a rule that let the cookie override a token would pass the
// second.
func TestACookieAuthenticatedUpgradeNeedsNoConnectToken(t *testing.T) {
	t.Parallel()

	for _, row := range []struct {
		name          string
		header        http.Header
		token         string
		wantConnected bool
		// wantTenant is the tenant the connection must act as, checked by
		// which session channel it may subscribe to.
		wantTenant identity.Principal
		// wantVerifications is how many times the verifier ran across the
		// upgrade and the handshake together.
		wantVerifications int
	}{
		{name: "cookie on the upgrade, no connect token: the upgrade's principal is reused",
			header: cookieHeader("token-a"), token: "", wantConnected: true, wantTenant: principalA(t), wantVerifications: 1},
		{name: "cookie on the upgrade AND a connect token: the token decides, as before",
			header: cookieHeader("token-a"), token: "token-b", wantConnected: true, wantTenant: principalB(t), wantVerifications: 2},
		{name: "bearer header on the upgrade, no connect token: not reused, refused",
			header: bearerHeader("token-a"), token: "", wantConnected: false, wantVerifications: 1},
		{name: "bearer header on the upgrade and a connect token: the token decides",
			header: bearerHeader("token-a"), token: "token-b", wantConnected: true, wantTenant: principalB(t), wantVerifications: 2},
	} {
		t.Run(row.name, func(t *testing.T) {
			t.Parallel()

			f := newFixtureBehind(t, testLimits(), routerLike)
			client, observed := dialWithHeader(t, f, row.token, clientlink.ProtocolVersion, row.header)
			if !row.wantConnected {
				closed := await(t, observed.disconnected, "terminal disconnect")
				if closed.Code != 3500 {
					t.Fatalf("refused with code %d, want exactly 3500 (DisconnectInvalidToken)", closed.Code)
				}
				if got := f.verifier.verifications(); got != row.wantVerifications {
					t.Errorf("the verifier ran %d times, want %d", got, row.wantVerifications)
				}
				return
			}
			await(t, observed.connected, "connected event")
			own := sessionChannel(row.wantTenant.Tenant(), "s1")
			if err := subscribe(t, client, own); err != nil {
				t.Errorf("the connection could not subscribe to its own tenant's session: %v", err)
			}
			other := tenantB
			if row.wantTenant.Tenant() == tenantB {
				other = tenantA
			}
			if err := subscribe(t, client, sessionChannel(other, "s2")); err == nil {
				t.Errorf("the connection subscribed to %s's session while acting as %s", other, row.wantTenant.Tenant())
			}
			// The RECORD, not only the outcome: the authorizer must have been
			// asked about the own channel under exactly the wanted tenant.
			found := false
			for _, call := range f.authorizer.subscribeCalls() {
				if call.channel == own && call.tenant == row.wantTenant.Tenant() {
					found = true
				}
			}
			if !found {
				t.Errorf("the authorizer was never asked about %q for tenant %q", own, row.wantTenant.Tenant())
			}
			if got := f.verifier.verifications(); got != row.wantVerifications {
				t.Errorf("the verifier ran %d times, want %d -- a reused upgrade costs no second verification", got, row.wantVerifications)
			}
		})
	}
}

// TestAnEmptyTokenIsStillRefusedWithoutACookieUpgrade holds the refusal that
// B7's reuse must not have widened: no cookie and no token is exactly what it
// was, and a bare handler with no router in front of it -- an upgrade nobody
// authenticated -- refuses the same way.
func TestAnEmptyTokenIsStillRefusedWithoutACookieUpgrade(t *testing.T) {
	t.Parallel()

	for _, row := range []struct {
		name string
		f    func(*testing.T) *fixture
	}{
		{"behind the router, no cookie", func(t *testing.T) *fixture { return newFixtureBehind(t, testLimits(), routerLikeOptional) }},
		{"bare handler, no router at all", func(t *testing.T) *fixture { return newFixture(t, testLimits()) }},
	} {
		t.Run(row.name, func(t *testing.T) {
			t.Parallel()

			f := row.f(t)
			_, observed := dialWithHeader(t, f, "", clientlink.ProtocolVersion, nil)
			closed := await(t, observed.disconnected, "terminal disconnect")
			if closed.Code != 3500 {
				t.Fatalf("refused with code %d, want exactly 3500", closed.Code)
			}
		})
	}
}

// routerLikeOptional is routerLike for a request carrying no credential: the
// handler is entered with NO operation context rather than refused, so the
// case above measures the handshake's own refusal and not the middleware's.
func routerLikeOptional(authenticator *internalidentity.Authenticator, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, present := authenticator.CredentialSource(r); !present {
			next.ServeHTTP(w, r)
			return
		}
		routerLike(authenticator, next).ServeHTTP(w, r)
	})
}

// TestAnUpgradeContextWithNoPrincipalIsNotReused drives the third clause of
// the rule: a context that names the cookie source but carries an
// unconstructed principal is the shape of a context no authenticated edge
// built, and it is refused rather than admitted as an empty tenant.
func TestAnUpgradeContextWithNoPrincipalIsNotReused(t *testing.T) {
	t.Parallel()

	f := newFixtureBehind(t, testLimits(), func(_ *internalidentity.Authenticator, next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := internalidentity.NewOperationContextWithSource(r.Context(), r, identity.Principal{}, internalidentity.SourceCookie)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	})
	_, observed := dialWithHeader(t, f, "", clientlink.ProtocolVersion, nil)
	closed := await(t, observed.disconnected, "terminal disconnect")
	if closed.Code != 3500 {
		t.Fatalf("refused with code %d, want exactly 3500", closed.Code)
	}
}

// TestAnUpgradePrincipalIsNeverReusedOnAnotherConnection is the property that
// makes the reuse safe: the principal lives in the connection's own context,
// which the transport derives from that connection's upgrade request, so a
// second connection to the same handler that presents nothing gets nothing --
// while the first is live, and while it is still subscribing.
func TestAnUpgradePrincipalIsNeverReusedOnAnotherConnection(t *testing.T) {
	t.Parallel()

	f := newFixtureBehind(t, testLimits(), routerLikeOptional)
	clientA, observedA := dialWithHeader(t, f, "", clientlink.ProtocolVersion, cookieHeader("token-a"))
	await(t, observedA.connected, "connected event for the cookie connection")

	_, observedB := dialWithHeader(t, f, "", clientlink.ProtocolVersion, nil)
	closed := await(t, observedB.disconnected, "terminal disconnect for the bare connection")
	if closed.Code != 3500 {
		t.Fatalf("the bare connection was refused with %d, want exactly 3500", closed.Code)
	}
	if err := subscribe(t, clientA, sessionChannel(tenantA, "s1")); err != nil {
		t.Errorf("the cookie connection stopped working when the bare one was refused: %v", err)
	}
	if got := f.handler.Connections(); got != 1 {
		t.Errorf("the handler holds %d connections, want exactly the cookie one", got)
	}
}

func principalA(t *testing.T) identity.Principal {
	t.Helper()
	p, err := identity.NewPrincipal(tenantA, "user-a", identity.KindActor)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func principalB(t *testing.T) identity.Principal {
	t.Helper()
	p, err := identity.NewPrincipal(tenantB, "user-b", identity.KindActor)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
