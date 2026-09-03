package identity_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory"
	factoryidentity "github.com/looprig/factory/identity"
	"github.com/looprig/factory/internal/httpapi"
	"github.com/looprig/factory/internal/identity"
	"github.com/looprig/factory/internal/realtime/clientlink"
)

// testNow is the instant every clock in this file reports. Expiry is decided
// against the injected clock, never against time.Now, so nothing here is
// sensitive to how long the suite takes to run.
var testNow = time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

type verifierFunc func(context.Context, identity.Credential) (identity.Claims, error)

func (f verifierFunc) VerifyCredential(ctx context.Context, c identity.Credential) (identity.Claims, error) {
	return f(ctx, c)
}

// constantVerifier verifies every credential to the same claims. Tests that
// care which credential arrived use recordingVerifier instead.
func constantVerifier(claims identity.Claims) identity.Verifier {
	return verifierFunc(func(context.Context, identity.Credential) (identity.Claims, error) {
		return claims, nil
	})
}

type recordingVerifier struct {
	claims identity.Claims
	err    error
	seen   []identity.Credential
}

func (v *recordingVerifier) VerifyCredential(_ context.Context, c identity.Credential) (identity.Claims, error) {
	v.seen = append(v.seen, c)
	return v.claims, v.err
}

func validClaims() identity.Claims {
	return identity.Claims{
		Tenant:    "tenant-1",
		Subject:   "user-1",
		Kind:      factoryidentity.KindActor,
		ExpiresAt: testNow.Add(time.Hour),
	}
}

func newTestAuthenticator(t *testing.T, cfg identity.Config) *identity.Authenticator {
	t.Helper()
	if cfg.Clock == nil {
		cfg.Clock = fixedClock{now: testNow}
	}
	a, err := identity.NewAuthenticator(cfg)
	if err != nil {
		t.Fatalf("NewAuthenticator: %v", err)
	}
	return a
}

func bearerRequest(token string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/v1/sessions", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	return r
}

func cookieRequest(name, value string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/v1/sessions", nil)
	r.AddCookie(&http.Cookie{Name: name, Value: value})
	return r
}

func randomNonce(t *testing.T) string {
	t.Helper()
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		t.Fatalf("read random nonce: %v", err)
	}
	return hex.EncodeToString(raw)
}

// ---------------------------------------------------------------------------
// Composition.
// ---------------------------------------------------------------------------

func TestNewAuthenticatorValidatesItsConfiguration(t *testing.T) {
	t.Parallel()

	valid := identity.Config{Verifier: constantVerifier(validClaims()), Clock: fixedClock{now: testNow}}
	tests := []struct {
		name    string
		mutate  func(*identity.Config)
		wantErr string
	}{
		{name: "valid", mutate: func(*identity.Config) {}},
		{name: "no verifier", mutate: func(c *identity.Config) { c.Verifier = nil }, wantErr: "Verifier"},
		{name: "no clock defaults", mutate: func(c *identity.Config) { c.Clock = nil }},
		{name: "cookie name defaults", mutate: func(c *identity.Config) { c.CookieName = "" }},
		{name: "cookie name with a space", mutate: func(c *identity.Config) { c.CookieName = "factory session" }, wantErr: "CookieName"},
		{name: "cookie name with a separator", mutate: func(c *identity.Config) { c.CookieName = "factory;session" }, wantErr: "CookieName"},
		{name: "default tenant too long", mutate: func(c *identity.Config) {
			c.DefaultTenant = sessionwire.TenantID(strings.Repeat("t", sessionwire.MaxIDBytes+1))
		}, wantErr: "DefaultTenant"},
		{name: "default tenant absent is allowed", mutate: func(c *identity.Config) { c.DefaultTenant = "" }},
		{name: "default tenant present is allowed", mutate: func(c *identity.Config) { c.DefaultTenant = "local" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := valid
			tt.mutate(&cfg)
			got, err := identity.NewAuthenticator(cfg)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("NewAuthenticator = %v, want no error", err)
				}
				if got == nil {
					t.Fatal("NewAuthenticator returned no authenticator and no error")
				}
				return
			}
			if err == nil {
				t.Fatalf("NewAuthenticator = %v, want an error naming %q", got, tt.wantErr)
			}
			if got != nil {
				t.Errorf("NewAuthenticator returned an authenticator alongside its error %v", err)
			}
			if !errors.Is(err, identity.ErrInvalidConfig) {
				t.Errorf("error %v does not wrap ErrInvalidConfig", err)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error %q does not name %q", err, tt.wantErr)
			}
		})
	}
}

// TestAuthenticatorSatisfiesTheSeamsItIsBuiltFor is what makes this type the
// thing composition can supply. It is asserted against the two narrow consumer
// interfaces AND the public union, because an implementation satisfying the
// narrow pair but not the union cannot be passed to WithAuthenticator.
func TestAuthenticatorSatisfiesTheSeamsItIsBuiltFor(t *testing.T) {
	t.Parallel()

	a := newTestAuthenticator(t, identity.Config{Verifier: constantVerifier(validClaims())})
	var _ httpapi.Authenticator = a
	var _ clientlink.Authenticator = a
	var _ factory.Authenticator = a
}

// ---------------------------------------------------------------------------
// Request credentials.
// ---------------------------------------------------------------------------

func TestAuthenticateRequestAcceptsAVerifiedCredential(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		request func() *http.Request
		source  identity.Source
	}{
		{name: "bearer", request: func() *http.Request { return bearerRequest("token-1") }, source: identity.SourceBearer},
		{name: "lowercase scheme", request: func() *http.Request {
			r := httptest.NewRequest(http.MethodGet, "/v1/sessions", nil)
			r.Header.Set("Authorization", "bearer token-1")
			return r
		}, source: identity.SourceBearer},
		// RFC 9110 spells the separator 1*SP, so repeated spaces are legal and
		// are trimmed. Without this row the TrimLeft is unread.
		{name: "repeated spaces after the scheme", request: func() *http.Request {
			r := httptest.NewRequest(http.MethodGet, "/v1/sessions", nil)
			r.Header.Set("Authorization", "Bearer   token-1")
			return r
		}, source: identity.SourceBearer},
		{name: "cookie", request: func() *http.Request {
			return cookieRequest(identity.DefaultCookieName, "token-1")
		}, source: identity.SourceCookie},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			verifier := &recordingVerifier{claims: validClaims()}
			a := newTestAuthenticator(t, identity.Config{Verifier: verifier})
			got, err := a.AuthenticateRequest(context.Background(), tt.request())
			if err != nil {
				t.Fatalf("AuthenticateRequest = %v, want a principal", err)
			}
			want, err := factoryidentity.NewPrincipal("tenant-1", "user-1", factoryidentity.KindActor)
			if err != nil {
				t.Fatalf("NewPrincipal: %v", err)
			}
			if got != want {
				t.Errorf("AuthenticateRequest = %+v, want %+v", got, want)
			}
			if len(verifier.seen) != 1 {
				t.Fatalf("verifier saw %d credentials, want exactly 1", len(verifier.seen))
			}
			if verifier.seen[0].Source() != tt.source {
				t.Errorf("credential source = %q, want %q", verifier.seen[0].Source(), tt.source)
			}
			if verifier.seen[0].Value() != "token-1" {
				t.Errorf("credential value = %q, want the presented token", verifier.seen[0].Value())
			}
		})
	}
}

func TestAuthenticateRequestRejections(t *testing.T) {
	t.Parallel()

	expired := validClaims()
	expired.ExpiresAt = testNow.Add(-time.Nanosecond)
	atExpiry := validClaims()
	atExpiry.ExpiresAt = testNow
	noExpiry := validClaims()
	noExpiry.ExpiresAt = time.Time{}
	noSubject := validClaims()
	noSubject.Subject = ""
	badKind := validClaims()
	badKind.Kind = factoryidentity.Kind("admin")
	serviceNoTenant := validClaims()
	serviceNoTenant.Kind = factoryidentity.KindService
	serviceNoTenant.Tenant = ""
	actorNoTenant := validClaims()
	actorNoTenant.Tenant = ""
	longTenant := validClaims()
	longTenant.Tenant = sessionwire.TenantID(strings.Repeat("t", sessionwire.MaxIDBytes+1))

	tests := []struct {
		name          string
		defaultTenant sessionwire.TenantID
		claims        identity.Claims
		verifyErr     error
		request       func() *http.Request
		wantErr       string
		wantExpired   bool
		wantUnavail   bool
	}{
		{name: "no credential", claims: validClaims(), request: func() *http.Request {
			return httptest.NewRequest(http.MethodGet, "/v1/sessions", nil)
		}, wantErr: "neither"},
		{name: "wrong cookie name", claims: validClaims(), request: func() *http.Request {
			return cookieRequest("other_cookie", "token-1")
		}, wantErr: "neither"},
		{name: "empty cookie value", claims: validClaims(), request: func() *http.Request {
			return cookieRequest(identity.DefaultCookieName, "")
		}, wantErr: "empty"},
		{name: "basic scheme", claims: validClaims(), request: func() *http.Request {
			r := httptest.NewRequest(http.MethodGet, "/v1/sessions", nil)
			r.Header.Set("Authorization", "Basic dXNlcjpwYXNz")
			return r
		}, wantErr: "bearer"},
		{name: "scheme only", claims: validClaims(), request: func() *http.Request {
			r := httptest.NewRequest(http.MethodGet, "/v1/sessions", nil)
			r.Header.Set("Authorization", "Bearer")
			return r
		}, wantErr: "bearer"},
		{name: "scheme and spaces", claims: validClaims(), request: func() *http.Request {
			r := httptest.NewRequest(http.MethodGet, "/v1/sessions", nil)
			r.Header.Set("Authorization", "Bearer    ")
			return r
		}, wantErr: "bearer"},
		{name: "token with an interior space", claims: validClaims(), request: func() *http.Request {
			r := httptest.NewRequest(http.MethodGet, "/v1/sessions", nil)
			r.Header.Set("Authorization", "Bearer token one")
			return r
		}, wantErr: "bearer"},
		// The tab has its own row: dropping the space from ContainsAny is
		// killed by the case above, dropping the tab is killed by nothing else.
		{name: "token with an interior tab", claims: validClaims(), request: func() *http.Request {
			r := httptest.NewRequest(http.MethodGet, "/v1/sessions", nil)
			r.Header.Set("Authorization", "Bearer token\tone")
			return r
		}, wantErr: "bearer"},
		{name: "glued scheme", claims: validClaims(), request: func() *http.Request {
			r := httptest.NewRequest(http.MethodGet, "/v1/sessions", nil)
			r.Header.Set("Authorization", "Bearertoken-1")
			return r
		}, wantErr: "bearer"},
		{name: "verifier rejected", claims: validClaims(), verifyErr: fmt.Errorf("%w: bad signature", factoryidentity.ErrUnauthenticated),
			request: func() *http.Request { return bearerRequest("token-1") }, wantErr: "not verified"},
		{name: "verifier unavailable", claims: validClaims(), verifyErr: errors.New("dial tcp: connection refused"),
			request: func() *http.Request { return bearerRequest("token-1") }, wantErr: "connection refused", wantUnavail: true},
		{name: "expired", claims: expired, request: func() *http.Request { return bearerRequest("token-1") },
			wantErr: "expired", wantExpired: true},
		{name: "expiring exactly now", claims: atExpiry, request: func() *http.Request { return bearerRequest("token-1") },
			wantErr: "expired", wantExpired: true},
		{name: "no expiry", claims: noExpiry, request: func() *http.Request { return bearerRequest("token-1") }, wantErr: "expiry"},
		{name: "no subject", claims: noSubject, request: func() *http.Request { return bearerRequest("token-1") }, wantErr: "subject"},
		{name: "unknown kind", claims: badKind, request: func() *http.Request { return bearerRequest("token-1") }, wantErr: "kind"},
		{name: "service without a tenant", defaultTenant: "local", claims: serviceNoTenant,
			request: func() *http.Request { return bearerRequest("token-1") }, wantErr: "service"},
		{name: "actor without a tenant and no default", claims: actorNoTenant,
			request: func() *http.Request { return bearerRequest("token-1") }, wantErr: "no tenant"},
		{name: "tenant too long", claims: longTenant, request: func() *http.Request { return bearerRequest("token-1") }, wantErr: "tenant"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			a := newTestAuthenticator(t, identity.Config{
				Verifier: verifierFunc(func(context.Context, identity.Credential) (identity.Claims, error) {
					return tt.claims, tt.verifyErr
				}),
				DefaultTenant: tt.defaultTenant,
			})
			got, err := a.AuthenticateRequest(context.Background(), tt.request())
			if err == nil {
				t.Fatalf("AuthenticateRequest = %+v, want an error naming %q", got, tt.wantErr)
			}
			if got != (factoryidentity.Principal{}) {
				t.Errorf("AuthenticateRequest returned %+v alongside its error, want the zero Principal", got)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error %q does not name %q", err, tt.wantErr)
			}
			if tt.wantUnavail {
				if !errors.Is(err, identity.ErrVerifierUnavailable) {
					t.Errorf("error %v does not wrap ErrVerifierUnavailable", err)
				}
				// A verifier that could not answer has not said the caller is
				// unauthenticated. Reporting it as one logs every user out of
				// a running deployment the moment the credential service
				// blinks, which is the opposite of the correct response.
				if errors.Is(err, factoryidentity.ErrUnauthenticated) {
					t.Errorf("error %v reports an unavailable verifier as an unauthenticated caller", err)
				}
				return
			}
			if !errors.Is(err, factoryidentity.ErrUnauthenticated) {
				t.Errorf("error %v does not wrap ErrUnauthenticated", err)
			}
			if got, want := errors.Is(err, factoryidentity.ErrCredentialExpired), tt.wantExpired; got != want {
				t.Errorf("errors.Is(%v, ErrCredentialExpired) = %t, want %t", err, got, want)
			}
		})
	}
}

// TestAMalformedAuthorizationHeaderIsNotDowngradedToTheCookie holds the
// precedence rule. Falling through to the cookie would let a caller who can set
// a header choose which of two credentials is verified, and would make a
// mistyped header silently authenticate as somebody else's cookie.
func TestAMalformedAuthorizationHeaderIsNotDowngradedToTheCookie(t *testing.T) {
	t.Parallel()

	verifier := &recordingVerifier{claims: validClaims()}
	a := newTestAuthenticator(t, identity.Config{Verifier: verifier})
	r := cookieRequest(identity.DefaultCookieName, "cookie-token")
	r.Header.Set("Authorization", "Basic dXNlcjpwYXNz")
	if _, err := a.AuthenticateRequest(context.Background(), r); err == nil {
		t.Fatal("AuthenticateRequest accepted a request whose Authorization header is not a bearer credential")
	}
	if len(verifier.seen) != 0 {
		t.Errorf("the verifier saw %d credentials, want none: the cookie must not be reached", len(verifier.seen))
	}
}

// TestAPresentButEmptyAuthorizationHeaderIsNotAnAbsentOne is the header case
// that made presented's own comment untrue. A client library that always sets
// the header, and leaves it empty when it holds no token, is ordinary; reading
// presence as "non-empty" gave exactly that client the fall-through to whatever
// cookie the browser sent.
func TestAPresentButEmptyAuthorizationHeaderIsNotAnAbsentOne(t *testing.T) {
	t.Parallel()

	verifier := &recordingVerifier{claims: validClaims()}
	a := newTestAuthenticator(t, identity.Config{Verifier: verifier})
	r := cookieRequest(identity.DefaultCookieName, "cookie-token")
	r.Header.Set("Authorization", "")
	if _, err := a.AuthenticateRequest(context.Background(), r); err == nil {
		t.Fatal("AuthenticateRequest accepted a request whose Authorization header is present and empty")
	} else if !errors.Is(err, factoryidentity.ErrUnauthenticated) {
		t.Errorf("error %v does not wrap ErrUnauthenticated", err)
	}
	if len(verifier.seen) != 0 {
		t.Errorf("the verifier saw %d credentials, want none: the cookie must not be reached", len(verifier.seen))
	}
	if _, ok := a.CredentialSource(r); ok {
		t.Error("CredentialSource reports a credential for a request whose header is present and empty")
	}
}

// TestTwoAuthorizationHeadersAreRefused holds the same rule one step further
// out: nothing in Factory may CHOOSE between two presented credentials.
func TestTwoAuthorizationHeadersAreRefused(t *testing.T) {
	t.Parallel()

	verifier := &recordingVerifier{claims: validClaims()}
	a := newTestAuthenticator(t, identity.Config{Verifier: verifier})
	r := httptest.NewRequest(http.MethodGet, "/v1/sessions", nil)
	r.Header.Add("Authorization", "Bearer token-1")
	r.Header.Add("Authorization", "Bearer token-2")
	_, err := a.AuthenticateRequest(context.Background(), r)
	if err == nil {
		t.Fatal("AuthenticateRequest accepted a request carrying two Authorization headers")
	}
	if !strings.Contains(err.Error(), "unambiguous") {
		t.Errorf("error %q does not say why two headers are refused", err)
	}
	if len(verifier.seen) != 0 {
		t.Errorf("the verifier saw %d credentials, want none", len(verifier.seen))
	}
}

// TestAnEmptyCredentialIsRejectedBeforeVerification is not only about the
// answer. It is what makes redactSecrets' empty-secret guard unreachable: the
// value handed to the verifier, and therefore the value scrubbed out of the
// verifier's error, is never the empty string.
func TestAnEmptyCredentialIsRejectedBeforeVerification(t *testing.T) {
	t.Parallel()

	verifier := &recordingVerifier{claims: validClaims()}
	a := newTestAuthenticator(t, identity.Config{Verifier: verifier})
	if _, err := a.AuthenticateRequest(context.Background(), cookieRequest(identity.DefaultCookieName, "")); err == nil {
		t.Fatal("AuthenticateRequest accepted an empty cookie value")
	}
	if _, err := a.AuthenticateLink(context.Background(), ""); err == nil {
		t.Fatal("AuthenticateLink accepted an empty token")
	}
	if len(verifier.seen) != 0 {
		t.Errorf("the verifier saw %d credentials, want none", len(verifier.seen))
	}
}

func TestAuthenticateRequestRejectsANilRequest(t *testing.T) {
	t.Parallel()

	a := newTestAuthenticator(t, identity.Config{Verifier: constantVerifier(validClaims())})
	if _, err := a.AuthenticateRequest(context.Background(), nil); err == nil {
		t.Fatal("AuthenticateRequest(nil) = nil error")
	} else if !errors.Is(err, factoryidentity.ErrUnauthenticated) {
		t.Errorf("error %v does not wrap ErrUnauthenticated", err)
	}
}

// TestCredentialSourceAgreesWithWhatAuthenticationUsed is the anti-duplication
// assertion behind exporting it. A1.3's CSRF guard applies to an ambient
// credential and not to a bearer one, so it needs this answer; if it re-derived
// "is there an Authorization header" beside the derivation, the two could
// disagree, and the disagreement would present as CSRF being skipped rather
// than as an error. The two are therefore driven over one request set and
// required to match.
func TestCredentialSourceAgreesWithWhatAuthenticationUsed(t *testing.T) {
	t.Parallel()

	requests := map[string]struct {
		request func() *http.Request
		want    identity.Source
		present bool
	}{
		"bearer": {request: func() *http.Request { return bearerRequest("token-1") }, want: identity.SourceBearer, present: true},
		"cookie": {request: func() *http.Request { return cookieRequest(identity.DefaultCookieName, "token-1") }, want: identity.SourceCookie, present: true},
		"header wins": {request: func() *http.Request {
			r := cookieRequest(identity.DefaultCookieName, "c")
			r.Header.Set("Authorization", "Bearer token-1")
			return r
		}, want: identity.SourceBearer, present: true},
		"none": {request: func() *http.Request { return httptest.NewRequest(http.MethodGet, "/v1/sessions", nil) }},
		"malformed header": {request: func() *http.Request {
			r := cookieRequest(identity.DefaultCookieName, "c")
			r.Header.Set("Authorization", "Basic x")
			return r
		}},
		"unrelated cookie": {request: func() *http.Request { return cookieRequest("other", "token-1") }},
	}
	for _, name := range slices.Sorted(maps.Keys(requests)) {
		tt := requests[name]
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			verifier := &recordingVerifier{claims: validClaims()}
			a := newTestAuthenticator(t, identity.Config{Verifier: verifier})
			r := tt.request()
			got, ok := a.CredentialSource(r)
			if ok != tt.present || got != tt.want {
				t.Errorf("CredentialSource = %q, %t, want %q, %t", got, ok, tt.want, tt.present)
			}
			// The cross-check: whatever authentication actually verified must
			// be what CredentialSource reported, for the same request.
			_, err := a.AuthenticateRequest(context.Background(), tt.request())
			if !tt.present {
				if err == nil {
					t.Error("AuthenticateRequest succeeded for a request CredentialSource reports no credential for")
				}
				if len(verifier.seen) != 0 {
					t.Errorf("the verifier saw %+v for a request with no credential", verifier.seen)
				}
				return
			}
			if err != nil {
				t.Fatalf("AuthenticateRequest: %v", err)
			}
			if len(verifier.seen) != 1 {
				t.Fatalf("the verifier saw %d credentials, want 1", len(verifier.seen))
			}
			if verifier.seen[0].Source() != got {
				t.Errorf("authentication used the %q credential while CredentialSource reported %q",
					verifier.seen[0].Source(), got)
			}
		})
	}
	if _, ok := newTestAuthenticator(t, identity.Config{Verifier: constantVerifier(validClaims())}).CredentialSource(nil); ok {
		t.Error("CredentialSource(nil) reports a credential")
	}
}

// ---------------------------------------------------------------------------
// Tenancy.
// ---------------------------------------------------------------------------

// TestTenantComesOnlyFromVerifiedClaims sweeps every carrier a tenant could
// arrive on and asserts none of them moves the answer. The carriers are
// enumerated rather than sampled because the claim is "for all of them", and
// the set a request offers is small enough to name.
func TestTenantComesOnlyFromVerifiedClaims(t *testing.T) {
	t.Parallel()

	const hostile = "tenant-evil"
	carriers := map[string]func(*http.Request){
		"query":      func(r *http.Request) { r.URL.RawQuery = "tenant=" + hostile },
		"path":       func(r *http.Request) { r.URL.Path = "/v1/tenants/" + hostile + "/sessions" },
		"path value": func(r *http.Request) { r.SetPathValue("tenant", hostile) },
		"body": func(r *http.Request) {
			*r = *httptest.NewRequest(http.MethodPost, "/v1/sessions", strings.NewReader(`{"tenant":"`+hostile+`"}`))
		},
		"header":      func(r *http.Request) { r.Header.Set("X-Tenant-Id", hostile) },
		"cookie":      func(r *http.Request) { r.AddCookie(&http.Cookie{Name: "tenant", Value: hostile}) },
		"host":        func(r *http.Request) { r.Host = hostile + ".example.com" },
		"request uri": func(r *http.Request) { r.RequestURI = "/v1/tenants/" + hostile + "/sessions" },
	}
	claimed := []struct {
		name          string
		claims        identity.Claims
		defaultTenant sessionwire.TenantID
		want          sessionwire.TenantID
	}{
		{name: "from the claims", claims: validClaims(), want: "tenant-1"},
		// The claim wins over the configured default, and that precedence
		// needs its own case: with no default configured, a derivation that
		// preferred configuration would still answer from the claim.
		{name: "from the claims over a configured default", claims: validClaims(), defaultTenant: "local", want: "tenant-1"},
		{name: "from the configured default", claims: func() identity.Claims {
			c := validClaims()
			c.Tenant = ""
			return c
		}(), defaultTenant: "local", want: "local"},
	}
	for _, tc := range claimed {
		for _, carrier := range slices.Sorted(maps.Keys(carriers)) {
			t.Run(tc.name+"/"+carrier, func(t *testing.T) {
				t.Parallel()

				a := newTestAuthenticator(t, identity.Config{
					Verifier:      constantVerifier(tc.claims),
					DefaultTenant: tc.defaultTenant,
				})
				r := bearerRequest("token-1")
				carriers[carrier](r)
				if r.Header.Get("Authorization") == "" {
					r.Header.Set("Authorization", "Bearer token-1")
				}
				got, err := a.AuthenticateRequest(context.Background(), r)
				if err != nil {
					t.Fatalf("AuthenticateRequest: %v", err)
				}
				if got.Tenant() != tc.want {
					t.Errorf("tenant = %q, want %q: the %s carrier moved the answer", got.Tenant(), tc.want, carrier)
				}
			})
		}
	}
}

// TestAuthenticationDoesNotConsumeTheRequestBody is the other half of the body
// carrier: a derivation that read the body to check it for a tenant would leave
// the handler an empty one, so the carrier assertion above could pass while the
// request was destroyed.
func TestAuthenticationDoesNotConsumeTheRequestBody(t *testing.T) {
	t.Parallel()

	const body = `{"prompt":"hello"}`
	a := newTestAuthenticator(t, identity.Config{Verifier: constantVerifier(validClaims())})
	r := httptest.NewRequest(http.MethodPost, "/v1/sessions", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer token-1")
	if _, err := a.AuthenticateRequest(context.Background(), r); err != nil {
		t.Fatalf("AuthenticateRequest: %v", err)
	}
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read body after authentication: %v", err)
	}
	if got := string(raw); got != body {
		t.Errorf("body after authentication = %q, want %q", got, body)
	}
}

// TestTheDefaultTenantAppliesToAnActorAndNotToAService is the actor/service
// distinction where it changes an answer rather than a label. A service
// credential is minted by deployment configuration and can name its own scope;
// letting it fall back to the local default would give an unscoped service
// credential authority over whichever tenant the deployment happens to default.
func TestTheDefaultTenantAppliesToAnActorAndNotToAService(t *testing.T) {
	t.Parallel()

	actor := validClaims()
	actor.Tenant = ""
	service := actor
	service.Kind = factoryidentity.KindService

	a := newTestAuthenticator(t, identity.Config{Verifier: constantVerifier(actor), DefaultTenant: "local"})
	got, err := a.AuthenticateRequest(context.Background(), bearerRequest("token-1"))
	if err != nil {
		t.Fatalf("actor: %v", err)
	}
	if got.Tenant() != "local" || got.IsService() {
		t.Errorf("actor principal = %+v, want the default tenant and a non-service kind", got)
	}

	b := newTestAuthenticator(t, identity.Config{Verifier: constantVerifier(service), DefaultTenant: "local"})
	if got, err := b.AuthenticateRequest(context.Background(), bearerRequest("token-1")); err == nil {
		t.Errorf("service principal = %+v, want a rejection: a service credential must name its tenant", got)
	}
}

func TestAServiceCredentialProducesAServicePrincipal(t *testing.T) {
	t.Parallel()

	claims := validClaims()
	claims.Kind = factoryidentity.KindService
	claims.Subject = "factory-reconciler"
	a := newTestAuthenticator(t, identity.Config{Verifier: constantVerifier(claims)})
	got, err := a.AuthenticateRequest(context.Background(), bearerRequest("token-1"))
	if err != nil {
		t.Fatalf("AuthenticateRequest: %v", err)
	}
	if !got.IsService() || got.Kind() != factoryidentity.KindService {
		t.Errorf("principal = %+v, want a service identity", got)
	}
}

// ---------------------------------------------------------------------------
// Expiry, logout and reconnect.
// ---------------------------------------------------------------------------

// TestExpiryIsDecidedAgainstTheInjectedClock drives the boundary from both
// sides. The instant of expiry itself is expired: a credential is accepted only
// while now is strictly before it.
func TestExpiryIsDecidedAgainstTheInjectedClock(t *testing.T) {
	t.Parallel()

	expiry := testNow.Add(time.Hour)
	claims := validClaims()
	claims.ExpiresAt = expiry
	tests := []struct {
		name       string
		now        time.Time
		wantExpiry bool
	}{
		{name: "long before", now: expiry.Add(-time.Hour)},
		{name: "one nanosecond before", now: expiry.Add(-time.Nanosecond)},
		{name: "at the instant", now: expiry, wantExpiry: true},
		{name: "one nanosecond after", now: expiry.Add(time.Nanosecond), wantExpiry: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			a := newTestAuthenticator(t, identity.Config{
				Verifier: constantVerifier(claims),
				Clock:    fixedClock{now: tt.now},
			})
			_, err := a.AuthenticateRequest(context.Background(), bearerRequest("token-1"))
			if got := errors.Is(err, factoryidentity.ErrCredentialExpired); got != tt.wantExpiry {
				t.Errorf("expired = %t (err %v), want %t", got, err, tt.wantExpiry)
			}
		})
	}
}

// TestLogoutAndReconnectAreDecidedByTheSharedVerifier is the multi-replica
// requirement made observable. Two Authenticators are built from one Config --
// they stand for two Factory replicas -- and never exchange anything. A
// credential accepted by one is accepted by the other, and a logout recorded in
// the shared verifier is observed by both, including the replica that never saw
// the original login.
func TestLogoutAndReconnectAreDecidedByTheSharedVerifier(t *testing.T) {
	t.Parallel()

	revoked := map[string]bool{}
	shared := verifierFunc(func(_ context.Context, c identity.Credential) (identity.Claims, error) {
		if revoked[c.Value()] {
			return identity.Claims{}, fmt.Errorf("%w: the session was ended", factoryidentity.ErrUnauthenticated)
		}
		return validClaims(), nil
	})
	cfg := identity.Config{Verifier: shared, Clock: fixedClock{now: testNow}}
	replicaA := newTestAuthenticator(t, cfg)
	replicaB := newTestAuthenticator(t, cfg)

	first, err := replicaA.AuthenticateRequest(context.Background(), bearerRequest("token-1"))
	if err != nil {
		t.Fatalf("replica A: %v", err)
	}
	// The browser's WebSocket reconnects and lands on the other replica.
	second, err := replicaB.AuthenticateLink(context.Background(), "token-1")
	if err != nil {
		t.Fatalf("replica B reconnect: %v", err)
	}
	if first != second {
		t.Errorf("replica B derived %+v, want the %+v replica A derived", second, first)
	}

	revoked["token-1"] = true
	for name, a := range map[string]*identity.Authenticator{"A": replicaA, "B": replicaB} {
		if _, err := a.AuthenticateRequest(context.Background(), bearerRequest("token-1")); !errors.Is(err, factoryidentity.ErrUnauthenticated) {
			t.Errorf("replica %s after logout: err = %v, want an unauthenticated rejection", name, err)
		}
	}
}

// TestAuthenticatorDeclaresNoPerProcessState is the structural half of the same
// requirement, and it is the half a behavioural test cannot supply: a
// per-process cache is INVISIBLE to the two-replica test above whenever the
// cache happens to miss, which is every case that test drives.
//
// What it establishes is bounded and worth stating: the Authenticator declares
// no field that can hold accumulated state. State reachable THROUGH an injected
// seam is the deployer's, and is what the shared-verifier requirement and
// WithCSRF's mandatory shared key govern instead.
func TestAuthenticatorDeclaresNoPerProcessState(t *testing.T) {
	t.Parallel()

	if found := mutableStateIn(reflect.TypeOf(identity.Authenticator{})); len(found) > 0 {
		t.Errorf("Authenticator declares per-process state: %s", strings.Join(found, ", "))
	}
}

type probeWithAMap struct {
	verifier identity.Verifier
	sessions map[string]string
}

type probeWithASlice struct {
	seen []string
}

type probeWithAPointer struct {
	cache *probeWithAMap
}

type probeWithANestedMap struct {
	inner struct {
		sessions map[string]string
	}
}

type probeWithAChannel struct {
	events chan string
}

type cleanProbe struct {
	verifier identity.Verifier
	tenant   sessionwire.TenantID
	name     string
}

// The probe fields above exist to be REFLECTED over rather than read, so
// nothing else in this file mentions them. Naming them here keeps staticcheck's
// unused-field check on for the fields that are not probes.
var _ = []any{
	probeWithAMap{}.sessions, probeWithAMap{}.verifier,
	probeWithASlice{}.seen,
	probeWithAPointer{}.cache,
	probeWithANestedMap{}.inner.sessions,
	probeWithAChannel{}.events,
	cleanProbe{}.verifier, cleanProbe{}.tenant, cleanProbe{}.name,
}

// TestMutableStateInFindsTheShapesTheAssertionLooksFor drives the detector, so
// the assertion above cannot pass by finding nothing. Each probe hides the
// state one layer deeper than the last.
func TestMutableStateInFindsTheShapesTheAssertionLooksFor(t *testing.T) {
	t.Parallel()

	for _, probe := range []any{probeWithAMap{}, probeWithASlice{}, probeWithAPointer{}, probeWithANestedMap{}, probeWithAChannel{}} {
		typ := reflect.TypeOf(probe)
		if found := mutableStateIn(typ); len(found) == 0 {
			t.Errorf("mutableStateIn(%s) found nothing, so the assertion it backs proves nothing", typ)
		}
	}
	if found := mutableStateIn(reflect.TypeOf(cleanProbe{})); len(found) > 0 {
		t.Errorf("mutableStateIn(cleanProbe) = %v, want nothing: the detector rejects a stateless shape", found)
	}
	if found := mutableStateIn(reflect.TypeOf(struct{}{})); len(found) == 0 {
		t.Error("mutableStateIn on a fieldless struct found nothing, so a type could pass by having no fields")
	}
}

// mutableStateIn reports every field of typ that can hold state accumulated by
// one process. An interface or a string cannot: an interface is an injected
// seam, and a string is configuration. Everything else -- map, slice, channel,
// pointer, function -- either holds state or can close over it.
func mutableStateIn(typ reflect.Type) []string {
	if typ.Kind() != reflect.Struct {
		return []string{typ.String() + " is not a struct"}
	}
	if typ.NumField() == 0 {
		return []string{typ.String() + " has no fields, so examining it proves nothing"}
	}
	var found []string
	for i := range typ.NumField() {
		field := typ.Field(i)
		switch field.Type.Kind() {
		case reflect.Interface, reflect.String:
		case reflect.Struct:
			for _, nested := range mutableStateIn(field.Type) {
				found = append(found, field.Name+"."+nested)
			}
		default:
			found = append(found, fmt.Sprintf("%s is %s (%s)", field.Name, field.Type, field.Type.Kind()))
		}
	}
	return found
}

// ---------------------------------------------------------------------------
// ClientLink credentials.
// ---------------------------------------------------------------------------

func TestAuthenticateLink(t *testing.T) {
	t.Parallel()

	expired := validClaims()
	expired.ExpiresAt = testNow.Add(-time.Second)
	tests := []struct {
		name    string
		token   string
		claims  identity.Claims
		wantErr string
	}{
		{name: "verified", token: "token-1", claims: validClaims()},
		{name: "empty", token: "", claims: validClaims(), wantErr: "empty"},
		{name: "expired", token: "token-1", claims: expired, wantErr: "expired"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			verifier := &recordingVerifier{claims: tt.claims}
			a := newTestAuthenticator(t, identity.Config{Verifier: verifier})
			got, err := a.AuthenticateLink(context.Background(), tt.token)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("AuthenticateLink = %v, want a principal", err)
				}
				if got.Tenant() != "tenant-1" {
					t.Errorf("tenant = %q, want %q", got.Tenant(), "tenant-1")
				}
				if len(verifier.seen) != 1 || verifier.seen[0].Source() != identity.SourceLink {
					t.Errorf("verifier saw %+v, want one credential with source %q", verifier.seen, identity.SourceLink)
				}
				return
			}
			if err == nil {
				t.Fatalf("AuthenticateLink = %+v, want an error naming %q", got, tt.wantErr)
			}
			if got != (factoryidentity.Principal{}) {
				t.Errorf("AuthenticateLink returned %+v alongside its error", got)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error %q does not name %q", err, tt.wantErr)
			}
			if !errors.Is(err, factoryidentity.ErrUnauthenticated) {
				t.Errorf("error %v does not wrap ErrUnauthenticated", err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Operation contexts.
// ---------------------------------------------------------------------------

// TestOperationContextCarriesOnlyApprovedFields enumerates the allowlist. The
// requirement is "copy only approved principal and trace fields", which is a
// claim about a set, so the set is named here and compared -- adding a field
// that holds a header, a cookie or a raw token fails until it is approved in
// this list and in review.
func TestOperationContextCarriesOnlyApprovedFields(t *testing.T) {
	t.Parallel()

	approved := []string{"CredentialSource", "Principal", "TraceID"}
	typ := reflect.TypeOf(identity.OperationContext{})
	var got []string
	for i := range typ.NumField() {
		got = append(got, typ.Field(i).Name)
	}
	slices.Sort(got)
	if !slices.Equal(got, approved) {
		t.Errorf("OperationContext fields = %v, want exactly %v", got, approved)
	}
	if found := mutableStateIn(typ); len(found) > 0 {
		t.Errorf("OperationContext holds shared state: %s", strings.Join(found, ", "))
	}
	if !typ.Comparable() {
		t.Errorf("%s is not comparable", typ)
	}
}

func TestOperationContextRoundTrips(t *testing.T) {
	t.Parallel()

	principal, err := factoryidentity.NewPrincipal("tenant-1", "user-1", factoryidentity.KindActor)
	if err != nil {
		t.Fatalf("NewPrincipal: %v", err)
	}
	r := bearerRequest("token-1")
	r.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")

	if _, ok := identity.OperationContextFrom(context.Background()); ok {
		t.Error("a bare context reports an operation context")
	}
	ctx := identity.NewOperationContext(context.Background(), r, principal, identity.SourceBearer)
	got, ok := identity.OperationContextFrom(ctx)
	if !ok {
		t.Fatal("the derived context reports no operation context")
	}
	want := identity.OperationContext{
		Principal:        principal,
		CredentialSource: identity.SourceBearer,
		TraceID:          "4bf92f3577b34da6a3ce929d0e0e4736",
	}
	if got != want {
		t.Errorf("OperationContextFrom = %+v, want %+v", got, want)
	}

	// A nil request is not a shape any edge produces today, and the guard is
	// still driven: the alternative to answering "" is a panic inside
	// authentication, which is the worst place in the server to discover a
	// caller passed nil.
	if got, ok := identity.OperationContextFrom(identity.NewOperationContext(context.Background(), nil, principal, identity.SourceBearer)); !ok || got.TraceID != "" {
		t.Errorf("NewOperationContext with no request carried %+v (ok %t), want an empty trace", got, ok)
	}

	link := identity.NewLinkOperationContext(context.Background(), principal)
	want = identity.OperationContext{Principal: principal, CredentialSource: identity.SourceLink}
	if got, ok := identity.OperationContextFrom(link); !ok || got != want {
		t.Errorf("NewLinkOperationContext carried %+v (ok %t), want %+v", got, ok, want)
	}
}

// TestTraceIDIsCopiedOnlyFromAWellFormedTraceparent is what keeps the trace
// field from becoming an unvalidated copy of an attacker-controlled header. The
// W3C grammar is fixed, so the acceptance rule is exact rather than defensive.
func TestTraceIDIsCopiedOnlyFromAWellFormedTraceparent(t *testing.T) {
	t.Parallel()

	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	const parentID = "00f067aa0ba902b7"
	tests := []struct {
		name   string
		header string
		want   string
	}{
		{name: "absent"},
		{name: "well formed", header: "00-" + traceID + "-" + parentID + "-01", want: traceID},
		{name: "sampled flag off", header: "00-" + traceID + "-" + parentID + "-00", want: traceID},
		{name: "later version with extra fields", header: "01-" + traceID + "-" + parentID + "-01-extra", want: traceID},
		{name: "forbidden version", header: "ff-" + traceID + "-" + parentID + "-01"},
		{name: "zero trace id", header: "00-" + strings.Repeat("0", 32) + "-" + parentID + "-01"},
		{name: "zero parent id", header: "00-" + traceID + "-" + strings.Repeat("0", 16) + "-01"},
		{name: "short trace id", header: "00-" + traceID[:31] + "-" + parentID + "-01"},
		{name: "long trace id", header: "00-" + traceID + "0-" + parentID + "-01"},
		{name: "uppercase trace id", header: "00-" + strings.ToUpper(traceID) + "-" + parentID + "-01"},
		{name: "non hex trace id", header: "00-" + traceID[:31] + "z-" + parentID + "-01"},
		{name: "too few fields", header: "00-" + traceID + "-" + parentID},
		{name: "not a trace parent at all", header: "please log me"},
		// The version field. Without these two rows the version length and hex
		// checks are unread: the "ff" row alone is killed by the == "ff" test.
		{name: "one digit version", header: "0-" + traceID + "-" + parentID + "-01"},
		{name: "uppercase version", header: "AB-" + traceID + "-" + parentID + "-01"},
		// The flags field. A mutant that stops reading flags -- or aliases them
		// to the version, which is "00" and valid -- adopts a trace-id from a
		// header W3C says to discard.
		{name: "non hex flags", header: "00-" + traceID + "-" + parentID + "-zz"},
		{name: "one digit flags", header: "00-" + traceID + "-" + parentID + "-0"},
		// The length bound. Factory does not own the http.Server, so nothing
		// guarantees MaxHeaderBytes; a later version may legally carry extra
		// fields, which is the shape that gets long.
		{name: "oversized later version", header: "01-" + traceID + "-" + parentID + "-01-" + strings.Repeat("a", 256)},
		{name: "later version just inside the bound", header: "01-" + traceID + "-" + parentID + "-01-" + strings.Repeat("a", 256-56), want: traceID},
		{name: "version 1 with a trailing dash", header: "01-" + traceID + "-" + parentID + "-01-"},
		{name: "version 0 with extra fields", header: "00-" + traceID + "-" + parentID + "-01-extra"},
	}
	principal, err := factoryidentity.NewPrincipal("tenant-1", "user-1", factoryidentity.KindActor)
	if err != nil {
		t.Fatalf("NewPrincipal: %v", err)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := bearerRequest("token-1")
			if tt.header != "" {
				r.Header.Set("traceparent", tt.header)
			}
			got, ok := identity.OperationContextFrom(identity.NewOperationContext(context.Background(), r, principal, identity.SourceBearer))
			if !ok {
				t.Fatal("no operation context")
			}
			if got.TraceID != tt.want {
				t.Errorf("TraceID = %q, want %q", got.TraceID, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// No auth material in errors, logs, or contexts.
// ---------------------------------------------------------------------------

// TestNoAuthMaterialReachesErrorsLogsOrContexts drives every outcome this
// package can produce with a credential that is a fresh random value, so its
// appearance anywhere can only be a leak -- the pattern pgstore uses for a DSN.
// A fixed token would be indistinguishable from ordinary message text.
//
// It cannot run in parallel: it replaces the process-wide default slog logger,
// which is where a stray log line would land.
func TestNoAuthMaterialReachesErrorsLogsOrContexts(t *testing.T) {
	nonce := randomNonce(t)
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	expired := validClaims()
	expired.ExpiresAt = testNow.Add(-time.Hour)
	badKind := validClaims()
	badKind.Kind = factoryidentity.Kind("admin")

	// A verifier that embeds the credential in its own error is not a
	// hypothetical: an upstream HTTP client reporting a failed introspection
	// call commonly prints the request it made.
	leaky := verifierFunc(func(_ context.Context, c identity.Credential) (identity.Claims, error) {
		return identity.Claims{}, fmt.Errorf("introspection POST /token?value=%s failed", c.Value())
	})
	leakyUnauthenticated := verifierFunc(func(_ context.Context, c identity.Credential) (identity.Claims, error) {
		return identity.Claims{}, fmt.Errorf("%w: token %q is not in the store", factoryidentity.ErrUnauthenticated, c.Value())
	})

	cases := map[string]struct {
		verifier identity.Verifier
		run      func(*identity.Authenticator) (factoryidentity.Principal, error)
	}{
		"accepted bearer": {verifier: constantVerifier(validClaims()), run: func(a *identity.Authenticator) (factoryidentity.Principal, error) {
			return a.AuthenticateRequest(context.Background(), bearerRequest(nonce))
		}},
		"accepted cookie": {verifier: constantVerifier(validClaims()), run: func(a *identity.Authenticator) (factoryidentity.Principal, error) {
			return a.AuthenticateRequest(context.Background(), cookieRequest(identity.DefaultCookieName, nonce))
		}},
		"accepted link": {verifier: constantVerifier(validClaims()), run: func(a *identity.Authenticator) (factoryidentity.Principal, error) {
			return a.AuthenticateLink(context.Background(), nonce)
		}},
		"malformed header": {verifier: constantVerifier(validClaims()), run: func(a *identity.Authenticator) (factoryidentity.Principal, error) {
			r := httptest.NewRequest(http.MethodGet, "/v1/sessions", nil)
			r.Header.Set("Authorization", "Basic "+nonce)
			return a.AuthenticateRequest(context.Background(), r)
		}},
		"glued scheme": {verifier: constantVerifier(validClaims()), run: func(a *identity.Authenticator) (factoryidentity.Principal, error) {
			r := httptest.NewRequest(http.MethodGet, "/v1/sessions", nil)
			r.Header.Set("Authorization", "Bearer"+nonce)
			return a.AuthenticateRequest(context.Background(), r)
		}},
		"spaced token": {verifier: constantVerifier(validClaims()), run: func(a *identity.Authenticator) (factoryidentity.Principal, error) {
			r := httptest.NewRequest(http.MethodGet, "/v1/sessions", nil)
			r.Header.Set("Authorization", "Bearer "+nonce+" "+nonce)
			return a.AuthenticateRequest(context.Background(), r)
		}},
		"wrong cookie name": {verifier: constantVerifier(validClaims()), run: func(a *identity.Authenticator) (factoryidentity.Principal, error) {
			return a.AuthenticateRequest(context.Background(), cookieRequest("other", nonce))
		}},
		"expired": {verifier: constantVerifier(expired), run: func(a *identity.Authenticator) (factoryidentity.Principal, error) {
			return a.AuthenticateRequest(context.Background(), bearerRequest(nonce))
		}},
		"unknown kind": {verifier: constantVerifier(badKind), run: func(a *identity.Authenticator) (factoryidentity.Principal, error) {
			return a.AuthenticateRequest(context.Background(), bearerRequest(nonce))
		}},
		"verifier unavailable and leaky": {verifier: leaky, run: func(a *identity.Authenticator) (factoryidentity.Principal, error) {
			return a.AuthenticateRequest(context.Background(), bearerRequest(nonce))
		}},
		"verifier rejected and leaky": {verifier: leakyUnauthenticated, run: func(a *identity.Authenticator) (factoryidentity.Principal, error) {
			return a.AuthenticateRequest(context.Background(), bearerRequest(nonce))
		}},
		"leaky link": {verifier: leaky, run: func(a *identity.Authenticator) (factoryidentity.Principal, error) {
			return a.AuthenticateLink(context.Background(), nonce)
		}},
	}
	for _, name := range slices.Sorted(maps.Keys(cases)) {
		tc := cases[name]
		a := newTestAuthenticator(t, identity.Config{Verifier: tc.verifier})
		principal, err := tc.run(a)
		rendered := map[string]string{
			"principal %#v": fmt.Sprintf("%#v", principal),
			"principal %+v": fmt.Sprintf("%+v", principal),
		}
		if err != nil {
			for _, verb := range []string{"%v", "%s", "%q", "%#v", "%+v"} {
				rendered["error "+verb] = fmt.Sprintf(verb, err)
			}
			for unwrapped := errors.Unwrap(err); unwrapped != nil; unwrapped = errors.Unwrap(unwrapped) {
				rendered["unwrapped error"] += unwrapped.Error()
			}
		}
		octx := identity.NewOperationContext(context.Background(), bearerRequest(nonce), principal, identity.SourceBearer)
		got, _ := identity.OperationContextFrom(octx)
		rendered["operation context %#v"] = fmt.Sprintf("%#v", got)
		rendered["operation context %+v"] = fmt.Sprintf("%+v", got)
		for what, text := range rendered {
			if strings.Contains(text, nonce) {
				t.Errorf("%s: the %s discloses the credential: %s", name, what, text)
			}
		}
	}
	if strings.Contains(logs.String(), nonce) {
		t.Errorf("the credential reached the default logger: %s", logs.String())
	}
	if logs.Len() != 0 {
		t.Logf("authentication wrote %d bytes to the default logger", logs.Len())
	}
}

// TestScrubbingCoversOnlyAVerbatimCredential states the boundary of the
// redaction mechanism as a measured property rather than as a caveat, because
// the mechanism is string equality and string equality cannot recognise a
// derivative it was not given.
//
// The negative half is deliberately an assertion and not a comment: a verifier
// that logs the first sixteen characters of the token puts sixty-four bits of
// it into an error the caller will log, and whoever writes that verifier should
// find this test rather than discover it in production. Removing the leak means
// discarding the verifier's message entirely, which costs the operator the
// difference between "bad signature" and "issuer unreachable"; this package
// keeps the message and names the price.
func TestScrubbingCoversOnlyAVerbatimCredential(t *testing.T) {
	t.Parallel()

	nonce := randomNonce(t)
	verbatim := newTestAuthenticator(t, identity.Config{
		Verifier: verifierFunc(func(_ context.Context, c identity.Credential) (identity.Claims, error) {
			return identity.Claims{}, fmt.Errorf("introspection failed for %s", c.Value())
		}),
	})
	_, err := verbatim.AuthenticateRequest(context.Background(), bearerRequest(nonce))
	if err == nil {
		t.Fatal("the verifier error was not reported")
	}
	if strings.Contains(err.Error(), nonce) {
		t.Errorf("a verbatim credential survived scrubbing: %v", err)
	}
	if !strings.Contains(err.Error(), "REDACTED") {
		t.Errorf("error %q does not show that something was removed", err)
	}

	// The other side of the same boundary. This is the DOCUMENTED limit, not a
	// defect being tolerated silently: if a future change makes the derivative
	// disappear too, this assertion is what says so and must be revisited.
	prefix := nonce[:16]
	derived := newTestAuthenticator(t, identity.Config{
		Verifier: verifierFunc(func(_ context.Context, c identity.Credential) (identity.Claims, error) {
			return identity.Claims{}, fmt.Errorf("introspection failed for token prefix %s", c.Value()[:16])
		}),
	})
	_, err = derived.AuthenticateRequest(context.Background(), bearerRequest(nonce))
	if err == nil {
		t.Fatal("the verifier error was not reported")
	}
	if !strings.Contains(err.Error(), prefix) {
		t.Errorf("error %q no longer carries the verifier's derivative of the credential; "+
			"the scrubbing limit this test pins has changed and the comments naming it are now wrong", err)
	}
}

// TestCredentialRedactsUnderEveryRenderingAConsumerCanReach is the mechanism
// half of the same requirement, and it is the half that covers code this task
// does not own: a Credential is handed to a deployer's Verifier, and the most
// likely way a token reaches a log is that somebody formats the value they were
// given.
//
// The four methods are not four independent mechanisms, and this test asserts
// only what deleting each one changes. Deleting GoString or MarshalJSON is
// caught by %#v and by json; deleting String is caught by every remaining verb.
// Deleting LogValue is caught by NEITHER -- slog.TextHandler's KindAny path
// falls back to fmt and reaches String -- so LogValue is held by the interface
// assertion and the direct call below instead. Counting mechanisms means
// disabling each and watching the outcome change; a fallback path will
// otherwise carry one silently and the count will be wrong.
func TestCredentialRedactsUnderEveryRenderingAConsumerCanReach(t *testing.T) {
	t.Parallel()

	nonce := randomNonce(t)
	credential := identity.NewCredential(identity.SourceBearer, nonce)
	// The %s verb is exercised deliberately: whether a Stringer is reached is
	// the property under test, so the lint that says to call String() directly
	// would remove the assertion rather than simplify it.
	//lint:ignore S1025 the verb is the subject of this test
	viaS := fmt.Sprintf("%s", credential)
	rendered := map[string]string{
		"%v":         fmt.Sprintf("%v", credential),
		"%+v":        fmt.Sprintf("%+v", credential),
		"%#v":        fmt.Sprintf("%#v", credential),
		"%s":         viaS,
		"%q":         fmt.Sprintf("%q", credential),
		"%v pointer": fmt.Sprintf("%v", &credential),
	}
	encoded, err := json.Marshal(credential)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	rendered["json"] = string(encoded)

	var logs bytes.Buffer
	slog.New(slog.NewTextHandler(&logs, nil)).Info("verifying", "credential", credential)
	rendered["slog"] = logs.String()

	// The assertion is a RUNTIME one. A compile-time `var _ slog.LogValuer`
	// would report a deleted method as a build failure, which is not an
	// assertion kill and would leave this claim resting on the compiler.
	if valuer, ok := any(credential).(slog.LogValuer); ok {
		rendered["LogValue()"] = valuer.LogValue().String()
	} else {
		t.Error("Credential does not implement slog.LogValuer, so a handler that reflects over the value rather than formatting it sees the fields")
	}

	for verb, text := range rendered {
		if strings.Contains(text, nonce) {
			t.Errorf("%s renders the credential value: %s", verb, text)
		}
		if !strings.Contains(text, string(identity.SourceBearer)) {
			t.Errorf("%s = %q, which does not name the credential source, so the redaction is not diagnosable", verb, text)
		}
	}
	if got := credential.Value(); got != nonce {
		t.Errorf("Value() = %q, want the credential: a verifier cannot verify what it cannot read", got)
	}
}

// ---------------------------------------------------------------------------
// The derivation reads no tenant carrier.
// ---------------------------------------------------------------------------

// tenantCarrierSelectors are the members of *http.Request through which a
// tenant could arrive from the caller. TestTenantComesOnlyFromVerifiedClaims
// drives the behaviour; this list is what makes the claim STRUCTURAL, because a
// behavioural sweep can only cover the carriers somebody thought of.
var tenantCarrierSelectors = []string{
	"URL", "RequestURI", "Body", "GetBody", "Form", "PostForm", "MultipartForm",
	"FormValue", "PostFormValue", "ParseForm", "ParseMultipartForm", "FormFile",
	"PathValue", "Query", "Host", "Referer",
}

// TestTheDerivationNamesNoTenantCarrier scans EVERY production file of this
// package, enumerated from the directory rather than named.
//
// A version of this pinned to "http.go" passed unchanged when a second file
// containing r.PathValue("tenant") and r.URL.Query().Get("tenant") was added
// beside it. Nothing else in the module would have reported that: the walks in
// import_boundary_test.go examine imports, not selectors. A1.2 and A1.3 both
// add files here, so the file set has to come from the directory or the
// structural half of the tenancy claim stops covering the package on the next
// commit.
func TestTheDerivationNamesNoTenantCarrier(t *testing.T) {
	t.Parallel()

	files := productionFiles(t)
	if len(files) == 0 {
		t.Fatal("no production files were found, so this scan proves nothing")
	}
	total := 0
	for _, name := range files {
		source, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		found, selectors, err := forbiddenSelectors(source)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		total += selectors
		if len(found) > 0 {
			t.Errorf("%s names tenant carriers %v; a tenant may come only from verified claims", name, found)
		}
	}
	if total == 0 {
		t.Fatal("the production files contain no selector expressions, so this scan proves nothing")
	}
}

// productionFiles enumerates this package's compiled files. It is deliberately
// a directory read rather than a list: a list is what let a new file escape.
func productionFiles(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read the package directory: %v", err)
	}
	var files []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		files = append(files, name)
	}
	slices.Sort(files)
	return files
}

// TestTheDerivationScanCoversEveryProductionFile is the tripwire for the defect
// above: the enumeration must actually reach the files, and a file added
// tomorrow must be in the set without anyone editing this test.
func TestTheDerivationScanCoversEveryProductionFile(t *testing.T) {
	t.Parallel()

	files := productionFiles(t)
	if !slices.Contains(files, "http.go") {
		t.Fatalf("productionFiles = %v, which does not include http.go", files)
	}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			t.Errorf("productionFiles returned the test file %s", name)
		}
	}
}

func TestForbiddenSelectorsFindsEveryCarrierItNames(t *testing.T) {
	t.Parallel()

	if len(tenantCarrierSelectors) == 0 {
		t.Fatal("no carriers are named, so the scan above proves nothing")
	}
	for _, carrier := range tenantCarrierSelectors {
		source := "package p\nfunc f(r *http.Request) { _ = r." + carrier + " }\n"
		found, _, err := forbiddenSelectors([]byte(source))
		if err != nil {
			t.Fatalf("parse probe for %s: %v", carrier, err)
		}
		if !slices.Contains(found, carrier) {
			t.Errorf("forbiddenSelectors did not report r.%s", carrier)
		}
	}
	clean := "package p\nfunc f(r *http.Request) { _ = r.Header.Get(\"Authorization\"); _, _ = r.Cookie(\"c\") }\n"
	found, selectors, err := forbiddenSelectors([]byte(clean))
	if err != nil {
		t.Fatalf("parse clean probe: %v", err)
	}
	if len(found) > 0 {
		t.Errorf("forbiddenSelectors reported %v for a file reading only the Authorization header and a cookie", found)
	}
	if selectors == 0 {
		t.Error("the clean probe counted no selectors, so a zero count would not distinguish an unparsed file")
	}
}

// forbiddenSelectors reports every tenant carrier named as a selector in
// source, and how many selectors it examined. It works over the PARSED file
// rather than its text, for the reason import_boundary_test.go states: a
// substring scan cannot tell code from a comment and is defeated by a rename.
func forbiddenSelectors(source []byte) (found []string, selectors int, err error) {
	file, err := parser.ParseFile(token.NewFileSet(), "src.go", source, parser.SkipObjectResolution)
	if err != nil {
		return nil, 0, err
	}
	ast.Inspect(file, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		selectors++
		if slices.Contains(tenantCarrierSelectors, selector.Sel.Name) && !slices.Contains(found, selector.Sel.Name) {
			found = append(found, selector.Sel.Name)
		}
		return true
	})
	slices.Sort(found)
	return found, selectors, nil
}
