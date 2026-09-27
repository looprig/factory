package httpapi

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	factoryidentity "github.com/looprig/factory/identity"
	internalidentity "github.com/looprig/factory/internal/identity"
)

// These requests exercise the router's real authentication, guard, token
// endpoint, and control route. They transfer WUI's browser security cases to
// Factory's principal-bound, stateless token contract.
func browserMigrationRequest(method, target string) *http.Request {
	var body io.Reader
	if method == http.MethodPost {
		body = strings.NewReader(`{}`)
	}
	r := httptest.NewRequest(method, fixtureOrigin+target, body)
	r.Header.Set("Origin", fixtureOrigin)
	r.AddCookie(&http.Cookie{Name: internalidentity.DefaultCookieName, Value: fixtureBearer})
	if body != nil {
		r.Header.Set("Content-Type", "application/json")
	}
	return r
}

func browserMigrationToken(t *testing.T, f *fixture) string {
	t.Helper()
	response := f.serve(browserMigrationRequest(http.MethodGet, "/v1/csrf-token"))
	if response.Code != http.StatusOK {
		t.Fatalf("token endpoint = %d (%s), want 200", response.Code, response.Body)
	}
	if got := response.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("token Cache-Control = %q, want no-store", got)
	}
	if got := response.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Errorf("token X-Content-Type-Options = %q, want nosniff", got)
	}
	var body struct {
		Token string `json:"csrf_token"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode token response: %v", err)
	}
	if body.Token == "" {
		t.Fatal("token endpoint returned an empty token")
	}
	return body.Token
}

func browserMigrationControl(token string) *http.Request {
	r := browserMigrationRequest(http.MethodPost, "/v1/sessions")
	if token != "" {
		r.Header.Set("X-CSRF-Token", token)
	}
	return r
}

func assertBrowserMigrationRejection(t *testing.T, response *httptest.ResponseRecorder, reason Reason) {
	t.Helper()
	if response.Code != http.StatusForbidden {
		t.Fatalf("status = %d (%s), want 403 for %q", response.Code, response.Body, reason)
	}
	if got := decodeEnvelope(t, response).Error.Code; got != sessionwire.ErrorCode(reason) {
		t.Errorf("rejection code = %q, want %q", got, reason)
	}
}

func TestBrowserMigrationIssuerAndControlsShareGuard(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	token := browserMigrationToken(t, f)

	assertBrowserMigrationRejection(t, f.serve(browserMigrationControl("")), ReasonCSRFMissing)
	accepted := f.serve(browserMigrationControl(token))
	if accepted.Code == http.StatusForbidden {
		t.Fatalf("the token issued by this router was rejected by its control guard: %s", accepted.Body)
	}
	if accepted.Code != http.StatusServiceUnavailable {
		t.Fatalf("guarded control = %d (%s), want 503 from the fixture's absent admission service", accepted.Code, accepted.Body)
	}
}

func TestBrowserMigrationHostAndOriginRejectOnIssuerAndControls(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	token := browserMigrationToken(t, f)
	cases := []struct {
		name   string
		mutate func(*http.Request)
		want   Reason
	}{
		{"foreign Host", func(r *http.Request) { r.Host = "attacker.test" }, ReasonHostNotTrusted},
		{"lookalike Host", func(r *http.Request) { r.Host = "app.example.com.attacker.test" }, ReasonHostNotTrusted},
		{"empty Host", func(r *http.Request) { r.Host = "" }, ReasonHostNotTrusted},
		{"foreign Origin", func(r *http.Request) { r.Header.Set("Origin", "https://attacker.test") }, ReasonOriginNotTrusted},
		{"lookalike Origin", func(r *http.Request) { r.Header.Set("Origin", "https://app.example.com.attacker.test") }, ReasonOriginNotTrusted},
		{"different port", func(r *http.Request) { r.Header.Set("Origin", "https://app.example.com:9443") }, ReasonOriginNotTrusted},
		{"different scheme", func(r *http.Request) { r.Header.Set("Origin", "http://app.example.com") }, ReasonOriginNotTrusted},
		{"null Origin", func(r *http.Request) { r.Header.Set("Origin", "null") }, ReasonOriginNotTrusted},
		{"userinfo Origin", func(r *http.Request) { r.Header.Set("Origin", "https://attacker.test@app.example.com") }, ReasonOriginNotTrusted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, endpoint := range []struct {
				name    string
				request *http.Request
			}{
				{"issuer", browserMigrationRequest(http.MethodGet, "/v1/csrf-token")},
				{"control", browserMigrationControl(token)},
			} {
				t.Run(endpoint.name, func(t *testing.T) {
					tc.mutate(endpoint.request)
					assertBrowserMigrationRejection(t, f.serve(endpoint.request), tc.want)
				})
			}
		})
	}

	// A top-level same-site navigation can omit Origin. The token still needs
	// authentication and the trusted Host, but no CSRF token of its own.
	r := browserMigrationRequest(http.MethodGet, "/v1/csrf-token")
	r.Header.Del("Origin")
	if got := f.serve(r); got.Code != http.StatusOK {
		t.Errorf("safe GET without Origin = %d (%s), want 200", got.Code, got.Body)
	}
}

func TestBrowserMigrationAmbientControlRequiresOriginAndValidToken(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	token := browserMigrationToken(t, f)

	withoutOrigin := browserMigrationControl(token)
	withoutOrigin.Header.Del("Origin")
	assertBrowserMigrationRejection(t, f.serve(withoutOrigin), ReasonOriginMissing)

	for _, candidate := range []string{"not-a-real-token", token[:len(token)-1], token + "x"} {
		assertBrowserMigrationRejection(t, f.serve(browserMigrationControl(candidate)), ReasonCSRFInvalid)
	}
}

func TestBrowserMigrationAmbientWebSocketRequiresOrigin(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	r := browserMigrationRequest(http.MethodGet, "/v1/realtime")
	r.Header.Set("Connection", "Upgrade")
	r.Header.Set("Upgrade", "websocket")
	r.Header.Del("Origin")
	assertBrowserMigrationRejection(t, f.serve(r), ReasonUpgradeOriginMissing)

	r.Header.Set("Origin", fixtureOrigin)
	if got := f.serve(r); got.Code != http.StatusServiceUnavailable {
		t.Errorf("trusted WebSocket upgrade = %d (%s), want 503 from the fixture's absent realtime service", got.Code, got.Body)
	}
}

func TestBrowserMigrationTokensArePrincipalBoundAndReplicaSafe(t *testing.T) {
	t.Parallel()
	issuer := newFixture(t)
	token := browserMigrationToken(t, issuer)

	otherPrincipal := newFixture(t, withVerifierTenant(otherTenant))
	assertBrowserMigrationRejection(t, otherPrincipal.serve(browserMigrationControl(token)), ReasonCSRFInvalid)

	replica := newFixture(t)
	if replica.guard == issuer.guard {
		t.Fatal("replica shares the issuer's guard instance")
	}
	if got := replica.serve(browserMigrationControl(token)); got.Code != http.StatusServiceUnavailable {
		t.Errorf("replica-issued token = %d (%s), want 503 after passing the guard", got.Code, got.Body)
	}

	// The same token is unusable on a deployment with a different shared key.
	foreignKey := newFixture(t, func(cfg *RouterConfig, f *fixture) {
		guard, err := NewGuard(GuardConfig{
			CSRF: factoryidentity.CSRFConfig{
				SharedKey:      []byte(strings.Repeat("f", 32)),
				TokenTTL:       time.Hour,
				TrustedOrigins: []string{fixtureOrigin},
			},
			Credentials: f.authn,
			Clock:       f.clock,
		})
		if err != nil {
			t.Fatalf("foreign-key guard: %v", err)
		}
		cfg.Guard = guard
	})
	assertBrowserMigrationRejection(t, foreignKey.serve(browserMigrationControl(token)), ReasonCSRFInvalid)
}

func TestBrowserMigrationExpiredTokenIsRejected(t *testing.T) {
	t.Parallel()
	issuer := newFixture(t)
	token := browserMigrationToken(t, issuer)

	later := newFixture(t, func(cfg *RouterConfig, f *fixture) {
		guard, err := NewGuard(GuardConfig{
			CSRF: factoryidentity.CSRFConfig{
				SharedKey:      []byte("0123456789abcdef0123456789abcdef"),
				TokenTTL:       time.Hour,
				TrustedOrigins: []string{fixtureOrigin},
			},
			Credentials: f.authn,
			Clock:       fixedClock{now: f.clock.now.Add(time.Hour)},
		})
		if err != nil {
			t.Fatalf("later guard: %v", err)
		}
		cfg.Guard = guard
	})
	assertBrowserMigrationRejection(t, later.serve(browserMigrationControl(token)), ReasonCSRFExpired)
}
