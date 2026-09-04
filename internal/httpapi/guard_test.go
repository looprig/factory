package httpapi_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	"github.com/looprig/factory/internal/httpapi"
	internalidentity "github.com/looprig/factory/internal/identity"
)

// ---------------------------------------------------------------------------
// Fixture.
// ---------------------------------------------------------------------------

const (
	trustedOrigin = "https://app.example.com"
	devOrigin     = "http://localhost:5173"
	sharedKeyText = "0123456789abcdef0123456789abcdef"
)

// refusingVerifier fails the test if it is called. The guard decides which
// credential a request presents and never decides whether it is valid, so a
// guard that reached a verifier would be doing authentication's work with
// authentication's failure modes.
type refusingVerifier struct{ t *testing.T }

func (v refusingVerifier) VerifyCredential(context.Context, internalidentity.Credential) (internalidentity.Claims, error) {
	v.t.Error("the guard called the credential verifier; it decides origins and tokens, not credentials")
	return internalidentity.Claims{}, errors.New("unreachable")
}

type stubClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *stubClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *stubClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type fixture struct {
	guard         *httpapi.Guard
	authenticator *internalidentity.Authenticator
	clock         *stubClock
	principal     identity.Principal
	other         identity.Principal
	config        identity.CSRFConfig
}

func guardConfig() identity.CSRFConfig {
	return identity.CSRFConfig{
		SharedKey:      []byte(sharedKeyText),
		TokenTTL:       time.Hour,
		TrustedOrigins: []string{trustedOrigin, devOrigin},
	}
}

func newFixture(t *testing.T, configure ...func(*identity.CSRFConfig)) *fixture {
	t.Helper()

	cfg := guardConfig()
	for _, apply := range configure {
		apply(&cfg)
	}
	authenticator, err := internalidentity.NewAuthenticator(internalidentity.Config{Verifier: refusingVerifier{t: t}})
	if err != nil {
		t.Fatalf("NewAuthenticator: %v", err)
	}
	clock := &stubClock{now: time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)}
	guard, err := httpapi.NewGuard(httpapi.GuardConfig{CSRF: cfg, Credentials: authenticator, Clock: clock})
	if err != nil {
		t.Fatalf("NewGuard: %v", err)
	}
	return &fixture{
		guard:         guard,
		authenticator: authenticator,
		clock:         clock,
		principal:     newPrincipal(t, "tenant-a", "subject-a"),
		other:         newPrincipal(t, "tenant-b", "subject-b"),
		config:        cfg,
	}
}

func newPrincipal(t *testing.T, tenant sessionwire.TenantID, subject string) identity.Principal {
	t.Helper()

	principal, err := identity.NewPrincipal(tenant, subject, identity.KindActor)
	if err != nil {
		t.Fatalf("NewPrincipal(%q, %q): %v", tenant, subject, err)
	}
	return principal
}

func (f *fixture) token(t *testing.T, principal identity.Principal) string {
	t.Helper()

	token, _, err := f.guard.IssueToken(principal)
	if err != nil {
		t.Fatalf("IssueToken: %v", err)
	}
	return token
}

// stateChangingRequest is the base every rule row mutates by exactly one
// dimension, so a rejection is attributable to that dimension and not to the
// base. It is the shape of a real control call: a same-origin JSON POST from
// the SPA, authenticated by the browser session cookie, echoing a token it
// fetched from the token endpoint.
func (f *fixture) stateChangingRequest(t *testing.T) *http.Request {
	t.Helper()

	r := httptest.NewRequest(http.MethodPost, trustedOrigin+"/v1/sessions/session-a/input", strings.NewReader(`{"text":"hello"}`))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Origin", trustedOrigin)
	r.AddCookie(&http.Cookie{Name: internalidentity.DefaultCookieName, Value: "browser-session-credential"})
	r.Header.Set(httpapi.CSRFHeaderName, f.token(t, f.principal))
	return f.authenticated(r, f.principal)
}

// authenticated returns r carrying the operation context an authenticated edge
// would have built for it, with the credential source DERIVED from the request
// rather than stated here.
func (f *fixture) authenticated(r *http.Request, principal identity.Principal) *http.Request {
	return r.WithContext(f.authenticator.NewOperationContext(r.Context(), r, principal))
}

// upgradeRequest is the WebSocket handshake base.
func (f *fixture) upgradeRequest(t *testing.T) *http.Request {
	t.Helper()

	r := httptest.NewRequest(http.MethodGet, trustedOrigin+"/v1/link", nil)
	r.Header.Set("Origin", trustedOrigin)
	r.Header.Set("Connection", "Upgrade")
	r.Header.Set("Upgrade", "websocket")
	r.AddCookie(&http.Cookie{Name: internalidentity.DefaultCookieName, Value: "browser-session-credential"})
	return f.authenticated(r, f.principal)
}

// safeRequest is the SPA navigation and safe-read base: a top-level GET with no
// Origin header, which is exactly what a browser sends when a user types the
// address or follows a link.
func (f *fixture) safeRequest(t *testing.T) *http.Request {
	t.Helper()

	r := httptest.NewRequest(http.MethodGet, trustedOrigin+"/", nil)
	r.AddCookie(&http.Cookie{Name: internalidentity.DefaultCookieName, Value: "browser-session-credential"})
	return f.authenticated(r, f.principal)
}

// ---------------------------------------------------------------------------
// The rule table.
// ---------------------------------------------------------------------------

type guardRow struct {
	name string
	// base builds the request this row mutates. A nil base is the
	// state-changing REST base.
	base func(*fixture, *testing.T) *http.Request
	// mutate applies exactly one change.
	mutate func(*fixture, *testing.T, *http.Request) *http.Request
	// want is the reason this row's single change must produce, or "" when the
	// row must still be allowed.
	want httpapi.Reason
}

func guardRows() []guardRow {
	return []guardRow{
		{name: "the unmutated state-changing base is allowed"},
		{
			name: "a safe read with an ambient credential, no Origin and no token is allowed",
			base: (*fixture).safeRequest,
		},
		{
			name: "a WebSocket handshake from a trusted origin is allowed",
			base: (*fixture).upgradeRequest,
		},
		{
			name: "a rebound host",
			mutate: func(_ *fixture, _ *testing.T, r *http.Request) *http.Request {
				r.Host = "rebound.attacker.test"
				return r
			},
			want: httpapi.ReasonHostNotTrusted,
		},
		{
			name: "a host that is not an authority at all",
			mutate: func(_ *fixture, _ *testing.T, r *http.Request) *http.Request {
				r.Host = "app.example.com/evil"
				return r
			},
			want: httpapi.ReasonHostNotTrusted,
		},
		{
			name: "the trusted host on another port is still the trusted host",
			mutate: func(_ *fixture, _ *testing.T, r *http.Request) *http.Request {
				r.Host = "app.example.com:9443"
				return r
			},
		},
		{
			name: "the trusted host in another case is still the trusted host",
			mutate: func(_ *fixture, _ *testing.T, r *http.Request) *http.Request {
				r.Host = "APP.Example.COM"
				return r
			},
		},
		{
			name: "a cross-site Origin",
			mutate: func(_ *fixture, _ *testing.T, r *http.Request) *http.Request {
				r.Header.Set("Origin", "https://attacker.test")
				return r
			},
			want: httpapi.ReasonOriginNotTrusted,
		},
		{
			name: "an Origin that only looks like the trusted one",
			mutate: func(_ *fixture, _ *testing.T, r *http.Request) *http.Request {
				r.Header.Set("Origin", "https://app.example.com.attacker.test")
				return r
			},
			want: httpapi.ReasonOriginNotTrusted,
		},
		{
			name: "the trusted origin over the wrong scheme",
			mutate: func(_ *fixture, _ *testing.T, r *http.Request) *http.Request {
				r.Header.Set("Origin", "http://app.example.com")
				return r
			},
			want: httpapi.ReasonOriginNotTrusted,
		},
		{
			name: "the null Origin a sandboxed document sends",
			mutate: func(_ *fixture, _ *testing.T, r *http.Request) *http.Request {
				r.Header.Set("Origin", "null")
				return r
			},
			want: httpapi.ReasonOriginNotTrusted,
		},
		{
			name: "two Origin headers, so no one origin is the request's",
			mutate: func(_ *fixture, _ *testing.T, r *http.Request) *http.Request {
				r.Header.Add("Origin", "https://attacker.test")
				return r
			},
			want: httpapi.ReasonOriginNotTrusted,
		},
		{
			name: "the trusted Origin written with its default port",
			mutate: func(_ *fixture, _ *testing.T, r *http.Request) *http.Request {
				r.Header.Set("Origin", "https://app.example.com:443")
				return r
			},
		},
		{
			name: "the trusted Origin written in upper case",
			mutate: func(_ *fixture, _ *testing.T, r *http.Request) *http.Request {
				r.Header.Set("Origin", "HTTPS://APP.Example.COM")
				return r
			},
		},
		{
			name: "a state-changing request with no Origin at all",
			mutate: func(_ *fixture, _ *testing.T, r *http.Request) *http.Request {
				r.Header.Del("Origin")
				return r
			},
			want: httpapi.ReasonOriginMissing,
		},
		{
			name: "a WebSocket handshake with no Origin at all",
			base: (*fixture).upgradeRequest,
			mutate: func(_ *fixture, _ *testing.T, r *http.Request) *http.Request {
				r.Header.Del("Origin")
				return r
			},
			want: httpapi.ReasonUpgradeOriginMissing,
		},
		{
			name: "a WebSocket handshake declaring the upgrade in a token list",
			base: (*fixture).upgradeRequest,
			mutate: func(_ *fixture, _ *testing.T, r *http.Request) *http.Request {
				r.Header.Set("Upgrade", "HTTP/2.0, WebSocket")
				r.Header.Del("Origin")
				return r
			},
			want: httpapi.ReasonUpgradeOriginMissing,
		},
		{
			name: "a state-changing request the edge did not authenticate",
			mutate: func(_ *fixture, _ *testing.T, r *http.Request) *http.Request {
				return r.WithContext(context.Background())
			},
			want: httpapi.ReasonUnauthenticated,
		},
		{
			name: "a state-changing request with no token",
			mutate: func(_ *fixture, _ *testing.T, r *http.Request) *http.Request {
				r.Header.Del(httpapi.CSRFHeaderName)
				return r
			},
			want: httpapi.ReasonCSRFMissing,
		},
		{
			name: "a token that is not this deployment's",
			mutate: func(_ *fixture, _ *testing.T, r *http.Request) *http.Request {
				r.Header.Set(httpapi.CSRFHeaderName, base64.RawURLEncoding.EncodeToString(make([]byte, 56)))
				return r
			},
			want: httpapi.ReasonCSRFInvalid,
		},
		{
			name: "a token that is not even base64",
			mutate: func(_ *fixture, _ *testing.T, r *http.Request) *http.Request {
				r.Header.Set(httpapi.CSRFHeaderName, "not a token")
				return r
			},
			want: httpapi.ReasonCSRFInvalid,
		},
		{
			name: "a token issued to another principal",
			mutate: func(f *fixture, t *testing.T, r *http.Request) *http.Request {
				r.Header.Set(httpapi.CSRFHeaderName, f.token(t, f.other))
				return r
			},
			want: httpapi.ReasonCSRFInvalid,
		},
		{
			name: "a token past its expiry",
			mutate: func(f *fixture, _ *testing.T, r *http.Request) *http.Request {
				f.clock.advance(f.config.TokenTTL + time.Second)
				return r
			},
			want: httpapi.ReasonCSRFExpired,
		},
		{
			name: "a bearer credential, which a cross-site page cannot cause to be attached",
			mutate: func(f *fixture, _ *testing.T, r *http.Request) *http.Request {
				r.Header.Del("Cookie")
				r.Header.Del(httpapi.CSRFHeaderName)
				r.Header.Del("Origin")
				r.Header.Set("Authorization", "Bearer application-held-token")
				return f.authenticated(r, f.principal)
			},
		},
		{
			name: "a WebSocket handshake with a bearer credential needs no Origin",
			base: (*fixture).upgradeRequest,
			mutate: func(f *fixture, _ *testing.T, r *http.Request) *http.Request {
				r.Header.Del("Cookie")
				r.Header.Del("Origin")
				r.Header.Set("Authorization", "Bearer application-held-token")
				return f.authenticated(r, f.principal)
			},
		},
		{
			name: "a WebSocket handshake with no credential still needs an Origin",
			base: (*fixture).upgradeRequest,
			mutate: func(f *fixture, _ *testing.T, r *http.Request) *http.Request {
				r.Header.Del("Cookie")
				r.Header.Del("Origin")
				return f.authenticated(r, f.principal)
			},
			want: httpapi.ReasonUpgradeOriginMissing,
		},
		{
			name: "a bearer credential does not excuse a cross-site Origin",
			mutate: func(f *fixture, _ *testing.T, r *http.Request) *http.Request {
				r.Header.Del("Cookie")
				r.Header.Set("Authorization", "Bearer application-held-token")
				r.Header.Set("Origin", "https://attacker.test")
				return f.authenticated(r, f.principal)
			},
			want: httpapi.ReasonOriginNotTrusted,
		},
		{
			name: "a bearer credential does not excuse a rebound host",
			mutate: func(f *fixture, _ *testing.T, r *http.Request) *http.Request {
				r.Header.Del("Cookie")
				r.Header.Set("Authorization", "Bearer application-held-token")
				r.Host = "rebound.attacker.test"
				return f.authenticated(r, f.principal)
			},
			want: httpapi.ReasonHostNotTrusted,
		},
		{
			name: "no credential at all, which is not the bearer exemption",
			mutate: func(f *fixture, _ *testing.T, r *http.Request) *http.Request {
				r.Header.Del("Cookie")
				r.Header.Del(httpapi.CSRFHeaderName)
				return f.authenticated(r, f.principal)
			},
			want: httpapi.ReasonCSRFMissing,
		},
		{
			name: "a malformed Authorization header, whose source is also empty",
			mutate: func(f *fixture, _ *testing.T, r *http.Request) *http.Request {
				r.Header.Del("Cookie")
				r.Header.Del(httpapi.CSRFHeaderName)
				r.Header.Set("Authorization", "Bearer")
				return f.authenticated(r, f.principal)
			},
			want: httpapi.ReasonCSRFMissing,
		},
		{
			name: "a nil request",
			mutate: func(_ *fixture, _ *testing.T, _ *http.Request) *http.Request {
				return nil
			},
			want: httpapi.ReasonNoRequest,
		},
	}
}

// TestGuardRulesAreEachSoleOnTheirOwnPath drives every rule from ONE accepted
// base, changing exactly one dimension per row.
//
// That construction is what the reasons are for. Every rejection here answers
// 403, so a table asserting only the status would pass with all nine checks
// collapsed into one; asserting the reason, from a base that satisfies every
// other rule, is what establishes that the named rule is the sole one deciding
// that path.
func TestGuardRulesAreEachSoleOnTheirOwnPath(t *testing.T) {
	t.Parallel()

	for _, row := range guardRows() {
		t.Run(row.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			base := (*fixture).stateChangingRequest
			if row.base != nil {
				base = row.base
			}
			r := base(f, t)
			if row.mutate != nil {
				r = row.mutate(f, t, r)
			}
			reason, allowed := f.guard.Check(r)
			if row.want == "" {
				if !allowed {
					t.Fatalf("Check() = (%q, false), want allowed", reason)
				}
				if reason != "" {
					t.Errorf("Check() allowed the request but reported reason %q", reason)
				}
				return
			}
			if allowed {
				t.Fatalf("Check() allowed the request, want reason %q", row.want)
			}
			if reason != row.want {
				t.Errorf("Check() = %q, want %q", reason, row.want)
			}
		})
	}
}

// TestEveryGuardReasonIsReachedByTheRuleTable is the anti-vacuity assertion for
// the table above: a reason nothing produces is a rule nothing exercises, and a
// reason with no client-facing message is a code nobody can act on.
func TestEveryGuardReasonIsReachedByTheRuleTable(t *testing.T) {
	t.Parallel()

	reached := map[httpapi.Reason]bool{}
	for _, row := range guardRows() {
		if row.want != "" {
			reached[row.want] = true
		}
	}
	for _, reason := range []httpapi.Reason{
		httpapi.ReasonNoRequest,
		httpapi.ReasonHostNotTrusted,
		httpapi.ReasonOriginNotTrusted,
		httpapi.ReasonUpgradeOriginMissing,
		httpapi.ReasonOriginMissing,
		httpapi.ReasonUnauthenticated,
		httpapi.ReasonCSRFMissing,
		httpapi.ReasonCSRFInvalid,
		httpapi.ReasonCSRFExpired,
	} {
		if !reached[reason] {
			t.Errorf("no row of guardRows produces reason %q", reason)
		}
	}
	allowedRows := 0
	for _, row := range guardRows() {
		if row.want == "" {
			allowedRows++
		}
	}
	if allowedRows == 0 {
		t.Fatal("no row of guardRows is allowed, so every rejection could be produced by a guard that rejects everything")
	}
}

// ---------------------------------------------------------------------------
// The six surfaces.
// ---------------------------------------------------------------------------

// TestSafeMethodsNeedNoToken covers the safe-read surface across every method
// RFC 9110 makes safe, with an ambient credential and no token.
func TestSafeMethodsNeedNoToken(t *testing.T) {
	t.Parallel()

	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodOptions} {
		t.Run(method, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			r := httptest.NewRequest(method, trustedOrigin+"/v1/sessions", nil)
			r.Header.Set("Origin", trustedOrigin)
			r.AddCookie(&http.Cookie{Name: internalidentity.DefaultCookieName, Value: "browser-session-credential"})
			if reason, allowed := f.guard.Check(f.authenticated(r, f.principal)); !allowed {
				t.Errorf("Check() rejected a safe %s with %q", method, reason)
			}
		})
	}
}

// TestStateChangingMethodsRequireAToken covers the state-changing REST surface.
// The unknown method is the point of the list: stateChanging is a safe-method
// allowlist, so a method nobody has heard of is guarded rather than exempt.
func TestStateChangingMethodsRequireAToken(t *testing.T) {
	t.Parallel()

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, "PURGE"} {
		t.Run(method, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			r := httptest.NewRequest(method, trustedOrigin+"/v1/sessions/session-a", nil)
			r.Header.Set("Origin", trustedOrigin)
			r.AddCookie(&http.Cookie{Name: internalidentity.DefaultCookieName, Value: "browser-session-credential"})
			r = f.authenticated(r, f.principal)
			if reason, allowed := f.guard.Check(r); allowed || reason != httpapi.ReasonCSRFMissing {
				t.Errorf("Check() = (%q, %v) for %s with no token, want (%q, false)", reason, allowed, method, httpapi.ReasonCSRFMissing)
			}
			r.Header.Set(httpapi.CSRFHeaderName, f.token(t, f.principal))
			if reason, allowed := f.guard.Check(r); !allowed {
				t.Errorf("Check() rejected %s carrying a valid token with %q", method, reason)
			}
		})
	}
}

// TestAJSONAPIRouteIsGuardedByItsMethodAndNotItsContentType pins that the API
// JSON surface is guarded by the same rules as any other route. A guard keyed
// on Content-Type would be defeated by the simple-request content types a
// cross-site form can send without a preflight, which is why this asserts the
// rejection is identical across all three of them.
func TestAJSONAPIRouteIsGuardedByItsMethodAndNotItsContentType(t *testing.T) {
	t.Parallel()

	for _, contentType := range []string{
		"application/json",
		"text/plain;charset=UTF-8",
		"application/x-www-form-urlencoded",
		"multipart/form-data; boundary=x",
		"",
	} {
		t.Run(contentType, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			r := httptest.NewRequest(http.MethodPost, trustedOrigin+"/v1/sessions", strings.NewReader("{}"))
			if contentType != "" {
				r.Header.Set("Content-Type", contentType)
			}
			r.Header.Set("Origin", "https://attacker.test")
			r.AddCookie(&http.Cookie{Name: internalidentity.DefaultCookieName, Value: "browser-session-credential"})
			reason, allowed := f.guard.Check(f.authenticated(r, f.principal))
			if allowed || reason != httpapi.ReasonOriginNotTrusted {
				t.Errorf("Check() = (%q, %v) for a cross-site %q POST, want (%q, false)", reason, allowed, contentType, httpapi.ReasonOriginNotTrusted)
			}
		})
	}
}

// TestSPANavigationIsNotACrossSiteRequest covers the SPA navigation surface
// from both sides: the navigation a browser performs when a user opens the app,
// which carries no Origin header and must work, and the cross-site form
// submission that also arrives as a top-level navigation and must not.
func TestSPANavigationIsNotACrossSiteRequest(t *testing.T) {
	t.Parallel()

	f := newFixture(t)

	for _, path := range []string{"/", "/index.html", "/sessions/session-a", "/assets/app.js"} {
		r := httptest.NewRequest(http.MethodGet, trustedOrigin+path, nil)
		r.AddCookie(&http.Cookie{Name: internalidentity.DefaultCookieName, Value: "browser-session-credential"})
		if reason, allowed := f.guard.Check(f.authenticated(r, f.principal)); !allowed {
			t.Errorf("Check() rejected the navigation to %q with %q", path, reason)
		}
	}

	form := httptest.NewRequest(http.MethodPost, trustedOrigin+"/v1/sessions/session-a/input", strings.NewReader("text=hello"))
	form.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	form.Header.Set("Origin", "https://attacker.test")
	form.AddCookie(&http.Cookie{Name: internalidentity.DefaultCookieName, Value: "browser-session-credential"})
	if reason, allowed := f.guard.Check(f.authenticated(form, f.principal)); allowed || reason != httpapi.ReasonOriginNotTrusted {
		t.Errorf("Check() = (%q, %v) for a cross-site form submission, want (%q, false)", reason, allowed, httpapi.ReasonOriginNotTrusted)
	}
}

// TestForwardedHostIsReadOnlyUnderItsTrustOption drives the option in BOTH
// directions with the same request, which is what makes it a tested option
// rather than a value every call site passes identically.
//
// The request is the shape a TLS-terminating proxy produces: the Host the
// Factory process itself sees is the internal address it was dialled at, and
// the browser's host survives only in X-Forwarded-Host.
func TestForwardedHostIsReadOnlyUnderItsTrustOption(t *testing.T) {
	t.Parallel()

	build := func(f *fixture, t *testing.T, host, forwarded string) *http.Request {
		t.Helper()

		r := httptest.NewRequest(http.MethodPost, "http://internal/v1/sessions", strings.NewReader("{}"))
		r.Host = host
		if forwarded != "" {
			r.Header.Set("X-Forwarded-Host", forwarded)
		}
		r.Header.Set("Origin", trustedOrigin)
		r.AddCookie(&http.Cookie{Name: internalidentity.DefaultCookieName, Value: "browser-session-credential"})
		r.Header.Set(httpapi.CSRFHeaderName, f.token(t, f.principal))
		return f.authenticated(r, f.principal)
	}

	trusting := newFixture(t, func(c *identity.CSRFConfig) { c.TrustForwardedHeaders = true })
	untrusting := newFixture(t)

	// A proxied request the deployment really receives.
	if reason, allowed := trusting.guard.Check(build(trusting, t, "10.0.0.5:8080", "app.example.com")); !allowed {
		t.Errorf("with the trust option on, a proxied request was rejected with %q", reason)
	}
	if reason, allowed := untrusting.guard.Check(build(untrusting, t, "10.0.0.5:8080", "app.example.com")); allowed || reason != httpapi.ReasonHostNotTrusted {
		t.Errorf("with the trust option off, Check() = (%q, %v), want (%q, false): the header must not be read",
			reason, allowed, httpapi.ReasonHostNotTrusted)
	}

	// The same header, forged by whoever can reach the listener directly.
	if reason, allowed := untrusting.guard.Check(build(untrusting, t, "rebound.attacker.test", "app.example.com")); allowed || reason != httpapi.ReasonHostNotTrusted {
		t.Errorf("with the trust option off, a forged X-Forwarded-Host changed the answer: Check() = (%q, %v)", reason, allowed)
	}
	if reason, allowed := trusting.guard.Check(build(trusting, t, "app.example.com", "rebound.attacker.test")); allowed || reason != httpapi.ReasonHostNotTrusted {
		t.Errorf("with the trust option on, the forwarded host must REPLACE the received one: Check() = (%q, %v)", reason, allowed)
	}

	// A proxy chain and a duplicated header are both refused rather than
	// resolved: nothing may choose between two answers to one question.
	for _, forwarded := range []string{"app.example.com, internal", "app.example.com,internal"} {
		if reason, allowed := trusting.guard.Check(build(trusting, t, "10.0.0.5:8080", forwarded)); allowed || reason != httpapi.ReasonHostNotTrusted {
			t.Errorf("a forwarded chain %q was accepted: Check() = (%q, %v)", forwarded, reason, allowed)
		}
	}
	doubled := build(trusting, t, "10.0.0.5:8080", "app.example.com")
	doubled.Header.Add("X-Forwarded-Host", "app.example.com")
	if reason, allowed := trusting.guard.Check(doubled); allowed || reason != httpapi.ReasonHostNotTrusted {
		t.Errorf("two X-Forwarded-Host headers were accepted: Check() = (%q, %v)", reason, allowed)
	}

	// And with the option on but no header, the received Host still decides, so
	// a direct request to a proxied deployment is not silently unguarded.
	if reason, allowed := trusting.guard.Check(build(trusting, t, "app.example.com", "")); !allowed {
		t.Errorf("with the trust option on and no forwarded header, the received Host was not used: %q", reason)
	}
}

// ---------------------------------------------------------------------------
// Tokens.
// ---------------------------------------------------------------------------

// TestTokensAreStatelessAcrossReplicas is the Step 4 property, and it is proved
// by construction rather than asserted: the token is verified by a SECOND Guard
// that never issued it and shares nothing with the first but the configured
// key. A per-process token set cannot pass this.
func TestTokensAreStatelessAcrossReplicas(t *testing.T) {
	t.Parallel()

	first := newFixture(t)
	second := newFixture(t)
	if first.guard == second.guard {
		t.Fatal("the two replicas are the same Guard, so this test proves nothing")
	}

	r := second.stateChangingRequest(t)
	r.Header.Set(httpapi.CSRFHeaderName, first.token(t, first.principal))
	if reason, allowed := second.guard.Check(r); !allowed {
		t.Errorf("a token issued by one replica was rejected by another with %q", reason)
	}

	// A replica configured with a DIFFERENT key must reject it, or the check
	// above would pass for a guard that verifies nothing.
	foreign := newFixture(t, func(c *identity.CSRFConfig) { c.SharedKey = []byte("ffffffffffffffffffffffffffffffff") })
	foreignRequest := foreign.stateChangingRequest(t)
	foreignRequest.Header.Set(httpapi.CSRFHeaderName, first.token(t, first.principal))
	if reason, allowed := foreign.guard.Check(foreignRequest); allowed || reason != httpapi.ReasonCSRFInvalid {
		t.Errorf("a replica with another key accepted the token: Check() = (%q, %v)", reason, allowed)
	}
}

// TestATokenIsBoundToItsPrincipal is the property that makes a stateless token
// safe. Without it, an attacker holding any account of their own mints a token
// with their own credentials and embeds the literal string in the attacking
// page, and the victim's browser posts it beside the victim's cookie.
//
// The two principals below are chosen so that a delimiter-separated encoding of
// (tenant, subject) would authenticate identical bytes for both. Under the
// length-prefixed encoding they differ, and this test is what reads that
// difference.
func TestATokenIsBoundToItsPrincipal(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	first := newPrincipal(t, "a", "b\x00c")
	second := newPrincipal(t, "a\x00b", "c")

	firstToken, _, err := f.guard.IssueToken(first)
	if err != nil {
		t.Fatalf("IssueToken: %v", err)
	}

	build := func(principal identity.Principal, token string) *http.Request {
		r := httptest.NewRequest(http.MethodPost, trustedOrigin+"/v1/sessions", strings.NewReader("{}"))
		r.Header.Set("Origin", trustedOrigin)
		r.AddCookie(&http.Cookie{Name: internalidentity.DefaultCookieName, Value: "browser-session-credential"})
		r.Header.Set(httpapi.CSRFHeaderName, token)
		return f.authenticated(r, principal)
	}

	if reason, allowed := f.guard.Check(build(first, firstToken)); !allowed {
		t.Fatalf("the principal the token was issued to was rejected with %q", reason)
	}
	if reason, allowed := f.guard.Check(build(second, firstToken)); allowed || reason != httpapi.ReasonCSRFInvalid {
		t.Errorf("a token issued to (%q, %q) verified for (%q, %q): Check() = (%q, %v)",
			first.Tenant(), first.Subject(), second.Tenant(), second.Subject(), reason, allowed)
	}

	// And the ordinary case, so the row above is not passing on the exotic
	// subject alone.
	if reason, allowed := f.guard.Check(build(f.other, f.token(t, f.principal))); allowed || reason != httpapi.ReasonCSRFInvalid {
		t.Errorf("a token issued to another tenant verified: Check() = (%q, %v)", reason, allowed)
	}
}

// TestTokenExpiryIsDecidedAgainstTheInjectedClock pins which instant owns the
// boundary and that the expiry is read from the token rather than from when it
// was seen.
func TestTokenExpiryIsDecidedAgainstTheInjectedClock(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		advance time.Duration
		want    httpapi.Reason
	}{
		{name: "well inside the TTL", advance: time.Minute},
		{name: "one nanosecond before the expiry", advance: time.Hour - time.Nanosecond},
		{name: "at the expiry", advance: time.Hour, want: httpapi.ReasonCSRFExpired},
		{name: "past the expiry", advance: 2 * time.Hour, want: httpapi.ReasonCSRFExpired},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			r := f.stateChangingRequest(t)
			f.clock.advance(tt.advance)
			reason, allowed := f.guard.Check(r)
			if tt.want == "" {
				if !allowed {
					t.Errorf("Check() = (%q, false) after %v, want allowed", reason, tt.advance)
				}
				return
			}
			if allowed || reason != tt.want {
				t.Errorf("Check() = (%q, %v) after %v, want (%q, false)", reason, allowed, tt.advance, tt.want)
			}
		})
	}
}

// TestAForgedExpiryIsNotReadBeforeTheMAC pins the order inside verifyToken. A
// caller who edits the expiry of a token they hold must be answered "invalid",
// not "expired" and not "accepted": the timestamp is authenticated, so it is
// never the thing that decides.
func TestAForgedExpiryIsNotReadBeforeTheMAC(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	r := f.stateChangingRequest(t)
	raw, err := base64.RawURLEncoding.DecodeString(r.Header.Get(httpapi.CSRFHeaderName))
	if err != nil {
		t.Fatalf("decode the issued token: %v", err)
	}
	// Byte 16 is the top of the big-endian millisecond expiry; setting it moves
	// the expiry far into the future.
	forged := append([]byte(nil), raw...)
	forged[16] = 0x7f
	r.Header.Set(httpapi.CSRFHeaderName, base64.RawURLEncoding.EncodeToString(forged))
	if reason, allowed := f.guard.Check(r); allowed || reason != httpapi.ReasonCSRFInvalid {
		t.Errorf("Check() = (%q, %v) for a token with a rewritten expiry, want (%q, false)", reason, allowed, httpapi.ReasonCSRFInvalid)
	}

	// Every single-byte edit anywhere in the token is refused, which is what
	// makes the row above a property of the MAC rather than of that one byte.
	for i := range raw {
		flipped := append([]byte(nil), raw...)
		flipped[i] ^= 0x01
		r.Header.Set(httpapi.CSRFHeaderName, base64.RawURLEncoding.EncodeToString(flipped))
		if reason, allowed := f.guard.Check(r); allowed || reason != httpapi.ReasonCSRFInvalid {
			t.Fatalf("flipping bit 0 of byte %d gave Check() = (%q, %v)", i, reason, allowed)
		}
	}
}

// TestIssuedTokensAreDistinct pins the per-call nonce: a token that depended
// only on the principal and the clock would repeat for two page loads in the
// same millisecond, and one leaked token would then be every token.
func TestIssuedTokensAreDistinct(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	seen := map[string]bool{}
	for range 32 {
		token := f.token(t, f.principal)
		if seen[token] {
			t.Fatalf("IssueToken returned %q twice with the clock unchanged", token)
		}
		seen[token] = true
	}
}

func TestIssueTokenRefusesAnUnconstructedPrincipal(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	if token, _, err := f.guard.IssueToken(identity.Principal{}); err == nil {
		t.Errorf("IssueToken(zero Principal) = %q, want an error", token)
	}
}

// ---------------------------------------------------------------------------
// Composition: NewGuard, Wrap and TokenHandler.
// ---------------------------------------------------------------------------

func TestNewGuardValidatesItsConfiguration(t *testing.T) {
	t.Parallel()

	authenticator, err := internalidentity.NewAuthenticator(internalidentity.Config{Verifier: refusingVerifier{t: t}})
	if err != nil {
		t.Fatalf("NewAuthenticator: %v", err)
	}
	clock := &stubClock{now: time.Now()}

	tests := []struct {
		name   string
		mutate func(*httpapi.GuardConfig)
	}{
		{name: "no credential source", mutate: func(c *httpapi.GuardConfig) { c.Credentials = nil }},
		{name: "no clock", mutate: func(c *httpapi.GuardConfig) { c.Clock = nil }},
		{name: "no shared key", mutate: func(c *httpapi.GuardConfig) { c.CSRF.SharedKey = nil }},
		{name: "no trusted origins", mutate: func(c *httpapi.GuardConfig) { c.CSRF.TrustedOrigins = nil }},
		{name: "an origin that could never match", mutate: func(c *httpapi.GuardConfig) {
			c.CSRF.TrustedOrigins = []string{"https://app.example.com."}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := httpapi.GuardConfig{CSRF: guardConfig(), Credentials: authenticator, Clock: clock}
			tt.mutate(&cfg)
			guard, err := httpapi.NewGuard(cfg)
			if err == nil {
				t.Fatal("NewGuard accepted the configuration")
			}
			if !errors.Is(err, httpapi.ErrInvalidGuardConfig) {
				t.Errorf("error %v does not wrap ErrInvalidGuardConfig", err)
			}
			if guard != nil {
				t.Error("NewGuard returned a Guard beside its error")
			}
		})
	}
}

// TestWrapAnswersARejectionItselfAndNeverCallsNext pins that a rejected request
// does not reach the handler, and that the reason reaches the client as a
// machine-readable code rather than as an undifferentiated 403.
func TestWrapAnswersARejectionItselfAndNeverCallsNext(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	called := false
	handler := f.guard.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))

	rejected := f.stateChangingRequest(t)
	rejected.Header.Set("Origin", "https://attacker.test")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, rejected)

	if called {
		t.Error("the wrapped handler ran for a rejected request")
	}
	if recorder.Code != http.StatusForbidden {
		t.Errorf("status = %d, want %d", recorder.Code, http.StatusForbidden)
	}
	if got := recorder.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}
	var body map[string]string
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode the rejection body %q: %v", recorder.Body, err)
	}
	if body["code"] != string(httpapi.ReasonOriginNotTrusted) {
		t.Errorf("code = %q, want %q", body["code"], httpapi.ReasonOriginNotTrusted)
	}
	if body["message"] == "" {
		t.Error("the rejection carries no message")
	}
	if strings.Contains(recorder.Body.String(), "attacker.test") {
		t.Errorf("the rejection body echoes the caller's origin: %q", recorder.Body)
	}

	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, f.stateChangingRequest(t))
	if !called || recorder.Code != http.StatusNoContent {
		t.Errorf("an accepted request did not reach the handler: called=%v status=%d", called, recorder.Code)
	}
}

// TestTokenHandlerIssuesToTheAuthenticatedPrincipalOnly covers the delivery
// mechanism: the token endpoint is a safe read, so a client holding no token
// can reach it through the guard, and what it issues is bound to whoever asked.
func TestTokenHandlerIssuesToTheAuthenticatedPrincipalOnly(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	handler := f.guard.Wrap(f.guard.TokenHandler())

	r := httptest.NewRequest(http.MethodGet, trustedOrigin+"/v1/csrf-token", nil)
	r.Header.Set("Origin", trustedOrigin)
	r.AddCookie(&http.Cookie{Name: internalidentity.DefaultCookieName, Value: "browser-session-credential"})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, f.authenticated(r, f.principal))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d (%s), want 200", recorder.Code, recorder.Body)
	}
	if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store: a token is a credential", got)
	}
	if got := recorder.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Errorf("X-Content-Type-Options = %q, want nosniff", got)
	}
	var body struct {
		CSRFToken string `json:"csrf_token"`
		ExpiresAt string `json:"expires_at"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode %q: %v", recorder.Body, err)
	}
	if body.CSRFToken == "" || body.ExpiresAt == "" {
		t.Fatalf("token response = %+v, want both members", body)
	}

	post := f.stateChangingRequest(t)
	post.Header.Set(httpapi.CSRFHeaderName, body.CSRFToken)
	if reason, allowed := f.guard.Check(post); !allowed {
		t.Errorf("the issued token was rejected on the next request with %q", reason)
	}
	other := f.stateChangingRequest(t)
	other.Header.Set(httpapi.CSRFHeaderName, body.CSRFToken)
	other = f.authenticated(other, f.other)
	if reason, allowed := f.guard.Check(other); allowed || reason != httpapi.ReasonCSRFInvalid {
		t.Errorf("the issued token verified for another principal: Check() = (%q, %v)", reason, allowed)
	}

	// An unauthenticated caller is answered, not issued to.
	unauthenticated := httptest.NewRequest(http.MethodGet, trustedOrigin+"/v1/csrf-token", nil)
	unauthenticated.Header.Set("Origin", trustedOrigin)
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, unauthenticated)
	if recorder.Code != http.StatusForbidden {
		t.Errorf("status = %d for an unauthenticated token request, want 403", recorder.Code)
	}
	if strings.Contains(recorder.Body.String(), "csrf_token") {
		t.Errorf("a token was issued to an unauthenticated caller: %q", recorder.Body)
	}
}

// TestAClockBeforeTheUnixEpochIssuesNothingAndValidatesNothing covers the two
// branches the millisecond encoding needs, and both fail closed.
//
// An expiry before the epoch has no representation in the token's unsigned
// millisecond field, so it is refused at issue rather than wrapped into an
// enormous value that would be a token nothing expires. On the verifying side
// the token may have been issued by a correctly-clocked replica, so the
// question is not whether the token is good but whether THIS replica can say:
// it cannot, and answers expired.
func TestAClockBeforeTheUnixEpochIssuesNothingAndValidatesNothing(t *testing.T) {
	t.Parallel()

	const backToTheTwenties = -100 * 365 * 24 * time.Hour

	broken := newFixture(t)
	correct := newFixture(t)
	request := correct.stateChangingRequest(t)

	broken.clock.advance(backToTheTwenties)
	if broken.clock.Now().Unix() >= 0 {
		t.Fatalf("the broken clock reads %s, which is not before the Unix epoch", broken.clock.Now())
	}

	if token, _, err := broken.guard.IssueToken(broken.principal); err == nil {
		t.Errorf("IssueToken with a pre-epoch clock = %q, want an error", token)
	}

	// The same request the correctly-clocked replica accepts.
	if reason, allowed := correct.guard.Check(request); !allowed {
		t.Fatalf("the correctly-clocked replica rejected its own request with %q", reason)
	}
	if reason, allowed := broken.guard.Check(request); allowed || reason != httpapi.ReasonCSRFExpired {
		t.Errorf("Check() = (%q, %v) on a pre-epoch replica, want (%q, false)", reason, allowed, httpapi.ReasonCSRFExpired)
	}
}
