package factory_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory"
	"github.com/looprig/factory/identity"
	"github.com/looprig/factory/internal/httpapi"
	internalidentity "github.com/looprig/factory/internal/identity"
	"github.com/looprig/sessionstore"
)

// This file drives the COMPOSITION, not the router.
//
// internal/httpapi already has its own tests for the API/SPA split and for the
// JSON error envelope. What no test in that package can hold is that Factory's
// public Server WIRES the two together the one way that is safe: the injected
// user interface reaches the router as its SPA fallback, INSIDE the router's
// own path split, rather than being mounted above or beside it. A composition
// that served the UI first, or that fell back to the UI on a router 404, would
// leave every httpapi test passing and answer /v1/unknown with index.html.
//
// So every assertion below is driven through factory.Server.Handler with a real
// authenticator, a real guard and the real router behind it. The only fakes are
// the durable seams a deployer supplies.

const trustedBase = "https://app.example.com"

// apiRequest is an authenticated request carrying a BEARER credential, which
// the origin guard exempts from its ambient-credential rules, so these cases
// exercise routing rather than CSRF.
func apiRequest(t *testing.T, method, target string) *http.Request {
	t.Helper()

	r := httptest.NewRequest(method, trustedBase+target, nil)
	r.Header.Set("Authorization", "Bearer "+factory.FakeCredentialValue)
	return r
}

func serveHandler(t *testing.T, h http.Handler, r *http.Request) *httptest.ResponseRecorder {
	t.Helper()

	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, r)
	return recorder
}

// decodeError requires a JSON error envelope and returns its code. It fails
// rather than tolerating an HTML body, which is the exact failure this file
// exists to catch.
func decodeError(t *testing.T, recorder *httptest.ResponseRecorder) sessionwire.ErrorCode {
	t.Helper()

	if got := recorder.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json; body was %q", got, recorder.Body.String())
	}
	var envelope sessionwire.ErrorEnvelope
	if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("the body is not a JSON error envelope (%v): %q", err, recorder.Body.String())
	}
	return envelope.Error.Code
}

// TestAnUnknownAPIRouteIsAJSON404AndNotTheSPA is runbook step 3.
//
// The UI here is a REAL fallback that answers every path, which is what a
// single-page application does. That is the point: with such a UI mounted, a
// composition that consulted the UI first -- or that used it as the answer to a
// router miss -- would serve index.html with status 200 for /v1/unknown, and a
// browser would show the application shell where the API owed an error.
func TestAnUnknownAPIRouteIsAJSON404AndNotTheSPA(t *testing.T) {
	t.Parallel()

	server, err := factory.New(append(factory.RequiredOptions(), factory.WithUIHandler(spaFallback()))...)
	if err != nil {
		t.Fatalf("New() = %v", err)
	}
	recorder := serveHandler(t, server.Handler(), apiRequest(t, http.MethodGet, "/v1/unknown"))

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("GET /v1/unknown = %d, want 404; body %q", recorder.Code, recorder.Body)
	}
	if strings.Contains(recorder.Body.String(), spaMarker) {
		t.Fatalf("GET /v1/unknown served the SPA shell: %q", recorder.Body)
	}
	if code := decodeError(t, recorder); code != httpapi.ErrorCodeRouteNotFound {
		t.Errorf("error code = %q, want %q", code, httpapi.ErrorCodeRouteNotFound)
	}
}

// TestAnUnauthenticatedAPIRouteIsStillJSON covers the other half of precedence.
// The API chain rejects before the mux is reached, so the status differs; what
// must not differ is that the answer is the API's rather than the SPA's.
func TestAnUnauthenticatedAPIRouteIsStillJSON(t *testing.T) {
	t.Parallel()

	server, err := factory.New(append(factory.RequiredOptions(), factory.WithUIHandler(spaFallback()))...)
	if err != nil {
		t.Fatalf("New() = %v", err)
	}
	r := httptest.NewRequest(http.MethodGet, trustedBase+"/v1/bootstrap", nil)
	recorder := serveHandler(t, server.Handler(), r)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated GET /v1/bootstrap = %d, want 401; body %q", recorder.Code, recorder.Body)
	}
	if strings.Contains(recorder.Body.String(), spaMarker) {
		t.Fatalf("an unauthenticated API request served the SPA shell: %q", recorder.Body)
	}
	decodeError(t, recorder)
}

// TestTheComposedHandlerServesAnAuthenticatedBootstrap is the positive half:
// the composition really authenticates, through the credential verifier a
// deployer supplies, and the tenant that reaches the response is the one the
// verified claims named. Without this, every assertion above could be satisfied
// by a Server that answered 404 to everything.
func TestTheComposedHandlerServesAnAuthenticatedBootstrap(t *testing.T) {
	t.Parallel()

	server, err := factory.New(factory.RequiredOptions()...)
	if err != nil {
		t.Fatalf("New() = %v", err)
	}
	recorder := serveHandler(t, server.Handler(), apiRequest(t, http.MethodGet, "/v1/bootstrap"))

	if recorder.Code != http.StatusOK {
		t.Fatalf("GET /v1/bootstrap = %d, want 200; body %q", recorder.Code, recorder.Body)
	}
	var body struct {
		Tenant string `json:"tenant_id"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("the bootstrap body is not JSON (%v): %q", err, recorder.Body)
	}
	if body.Tenant != factory.FakeTenant {
		t.Errorf("bootstrap tenant = %q, want %q", body.Tenant, factory.FakeTenant)
	}
}

// TestTheInjectedUIServesEverythingOutsideTheAPISegment is the SPA half of the
// split, driven through the composition rather than the router.
func TestTheInjectedUIServesEverythingOutsideTheAPISegment(t *testing.T) {
	t.Parallel()

	server, err := factory.New(append(factory.RequiredOptions(), factory.WithUIFS(uiBundle()))...)
	if err != nil {
		t.Fatalf("New() = %v", err)
	}
	handler := server.Handler()
	if body := serve(t, handler, "/app.js"); body != "console.log(1)" {
		t.Errorf("GET /app.js through the composed handler = %q", body)
	}
	if body := serve(t, handler, "/"); body != "<!doctype html>" {
		t.Errorf("GET / through the composed handler = %q", body)
	}
}

// TestAnAlternateStaticUIIsTheOneServed is runbook step 2's alternate-static-UI
// case. A second, different bundle must be the one a second Server serves, so
// the mounted UI cannot be a constant hidden behind the option.
func TestAnAlternateStaticUIIsTheOneServed(t *testing.T) {
	t.Parallel()

	alternate := alternateBundle()
	server, err := factory.New(append(factory.RequiredOptions(), factory.WithUIFS(alternate))...)
	if err != nil {
		t.Fatalf("New() = %v", err)
	}
	handler := server.Handler()
	if body := serve(t, handler, "/"); body != "<!doctype html><!--alternate-->" {
		t.Errorf("the alternate bundle's index was not served: %q", body)
	}
	if body := serve(t, handler, "/alternate.js"); body != "console.log(2)" {
		t.Errorf("the alternate bundle's asset was not served: %q", body)
	}
	// The alternate UI changes nothing about the API surface.
	recorder := serveHandler(t, handler, apiRequest(t, http.MethodGet, "/v1/unknown"))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("GET /v1/unknown with the alternate UI = %d, want 404", recorder.Code)
	}
	if code := decodeError(t, recorder); code != httpapi.ErrorCodeRouteNotFound {
		t.Errorf("error code = %q, want %q", code, httpapi.ErrorCodeRouteNotFound)
	}
}

// TestANoUICompositionAnswersEveryPathAsTheAPI is runbook step 2's no-UI case.
// With no UI mounted there is no fallback at all, so a non-API path is the
// router's own JSON route failure rather than a 200 or a net/http plain-text
// 404.
func TestANoUICompositionAnswersEveryPathAsTheAPI(t *testing.T) {
	t.Parallel()

	server, err := factory.New(factory.RequiredOptions()...)
	if err != nil {
		t.Fatalf("New() = %v", err)
	}
	if _, mounted := server.UI(); mounted {
		t.Fatal("UI() reported a UI in a composition that supplied none")
	}
	recorder := serveHandler(t, server.Handler(), httptest.NewRequest(http.MethodGet, trustedBase+"/app.js", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("GET /app.js with no UI = %d, want 404; body %q", recorder.Code, recorder.Body)
	}
	if code := decodeError(t, recorder); code != httpapi.ErrorCodeRouteNotFound {
		t.Errorf("error code = %q, want %q", code, httpapi.ErrorCodeRouteNotFound)
	}
}

const spaMarker = "<!--single-page-application-shell-->"

// spaFallback is a UI that answers 200 to EVERY path, which is what a
// single-page application's history fallback does. A UI that 404ed on unknown
// paths would let a wrongly ordered composition pass.
func spaFallback() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<!doctype html>" + spaMarker))
	})
}

// alternateBundle is a SECOND static bundle, different from uiBundle in both
// its index and its asset name.
func alternateBundle() fstest.MapFS {
	return fstest.MapFS{
		"index.html":   &fstest.MapFile{Data: []byte("<!doctype html><!--alternate-->")},
		"alternate.js": &fstest.MapFile{Data: []byte("console.log(2)")},
	}
}

// The three cases below each hold ONE composed seam that no response above
// reads. Without them a composition that built the authenticator, the guard and
// the router from constants instead of from the deployer's options would serve
// every case above correctly.

// TestTheComposedUUIDSourceMintsTheRequestID holds WithUUIDSource reaching the
// router.
func TestTheComposedUUIDSourceMintsTheRequestID(t *testing.T) {
	t.Parallel()

	server, err := factory.New(append(factory.RequiredOptions(), factory.WithUUIDSource(factory.FakeUUIDs{}))...)
	if err != nil {
		t.Fatalf("New() = %v", err)
	}
	recorder := serveHandler(t, server.Handler(), apiRequest(t, http.MethodGet, "/v1/bootstrap"))
	if got := recorder.Header().Get("X-Request-Id"); got != factory.FakeUUID {
		t.Errorf("X-Request-Id = %q, want the composed source's %q", got, factory.FakeUUID)
	}
}

// TestTheComposedCSRFConfigurationDecidesTrustedOrigins holds WithCSRF reaching
// the guard. An origin the composition does not trust is refused, and the
// refusal names the rule rather than being any 403.
func TestTheComposedCSRFConfigurationDecidesTrustedOrigins(t *testing.T) {
	t.Parallel()

	server, err := factory.New(factory.RequiredOptions()...)
	if err != nil {
		t.Fatalf("New() = %v", err)
	}
	r := apiRequest(t, http.MethodGet, "/v1/bootstrap")
	r.Header.Set("Origin", "https://not-this-deployment.example.com")
	recorder := serveHandler(t, server.Handler(), r)

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("a request from an untrusted origin = %d, want 403; body %q", recorder.Code, recorder.Body)
	}
	if !strings.Contains(recorder.Body.String(), string(httpapi.ReasonOriginNotTrusted)) {
		t.Errorf("the rejection does not name %q: %q", httpapi.ReasonOriginNotTrusted, recorder.Body)
	}
}

// TestTheComposedClockDecidesCredentialExpiry holds WithClock reaching the
// authenticator. The credential the verifier issues expires an hour from now,
// so a composition reading a clock two hours ahead must refuse it; a
// composition that let internal/identity default to the system clock would
// accept it.
func TestTheComposedClockDecidesCredentialExpiry(t *testing.T) {
	t.Parallel()

	server, err := factory.New(append(factory.RequiredOptions(), factory.WithClock(aheadClock{by: 2 * time.Hour}))...)
	if err != nil {
		t.Fatalf("New() = %v", err)
	}
	recorder := serveHandler(t, server.Handler(), apiRequest(t, http.MethodGet, "/v1/bootstrap"))
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("an expired credential = %d, want 401; body %q", recorder.Code, recorder.Body)
	}
	decodeError(t, recorder)
}

// aheadClock reads a fixed offset ahead of the wall clock, so it stays ahead of
// a credential the verifier dates relative to time.Now.
type aheadClock struct{ by time.Duration }

func (c aheadClock) Now() time.Time { return time.Now().Add(c.by) }

func (aheadClock) AfterFunc(time.Duration, func()) func() bool { return func() bool { return false } }

// recordingReader is the composed durable read plane, and it RECORDS. A test
// that only checked a status could not tell "the composition queried the reader
// a deployer supplied" from "the composition queried some other reader and got
// the same shape of answer".
type recordingReader struct {
	factory.SessionReader
	queried *atomic.Bool
	err     error
}

func (r recordingReader) ListSessions(context.Context, sessionstore.ListSessionsRequest) (sessionstore.SessionPage, error) {
	r.queried.Store(true)
	return sessionstore.SessionPage{}, r.err
}

// refusingAuthorizer refuses the session list and nothing else.
type refusingAuthorizer struct {
	factory.Authorizer
	err error
}

func (a refusingAuthorizer) AuthorizeSessionList(context.Context, identity.Principal) error {
	return a.err
}

// TestTheComposedSessionReaderIsTheOneQueried holds WithSessionReader reaching
// the router, by the reader's own record rather than by the response shape.
func TestTheComposedSessionReaderIsTheOneQueried(t *testing.T) {
	t.Parallel()

	var queried atomic.Bool
	reader := recordingReader{queried: &queried, err: errors.New("this reader was asked")}
	server, err := factory.New(append(factory.RequiredOptionsExcept("WithSessionReader"),
		factory.WithSessionReader(reader))...)
	if err != nil {
		t.Fatalf("New() = %v", err)
	}
	recorder := serveHandler(t, server.Handler(), apiRequest(t, http.MethodGet, "/v1/sessions"))

	if !queried.Load() {
		t.Fatal("GET /v1/sessions did not reach the composed session reader")
	}
	if recorder.Code == http.StatusOK {
		t.Errorf("a failing reader was answered 200: %q", recorder.Body)
	}
	decodeError(t, recorder)
}

// TestTheComposedAuthorizerDecidesTheSessionList holds WithAuthorizer reaching
// the router, and holds the ORDER: a refused request must not have reached the
// durable read plane at all.
func TestTheComposedAuthorizerDecidesTheSessionList(t *testing.T) {
	t.Parallel()

	var queried atomic.Bool
	server, err := factory.New(append(
		factory.RequiredOptionsExcept("WithSessionReader", "WithAuthorizer"),
		factory.WithSessionReader(recordingReader{queried: &queried}),
		factory.WithAuthorizer(refusingAuthorizer{err: internalidentity.ErrUnauthorized}),
	)...)
	if err != nil {
		t.Fatalf("New() = %v", err)
	}
	recorder := serveHandler(t, server.Handler(), apiRequest(t, http.MethodGet, "/v1/sessions"))

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("a refused session list = %d, want 403; body %q", recorder.Code, recorder.Body)
	}
	if queried.Load() {
		t.Error("a refused session list still reached the durable read plane")
	}
	decodeError(t, recorder)
}
