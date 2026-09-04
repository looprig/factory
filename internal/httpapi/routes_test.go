package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	factoryidentity "github.com/looprig/factory/identity"
	internalidentity "github.com/looprig/factory/internal/identity"
	"github.com/looprig/sessionstore"
)

// ---------------------------------------------------------------------------
// The fixture.
// ---------------------------------------------------------------------------

const (
	fixtureOrigin  = "https://app.example.com"
	fixtureHost    = "app.example.com"
	fixtureTenant  = sessionwire.TenantID("tenant-a")
	otherTenant    = sessionwire.TenantID("tenant-b")
	fixtureSubject = "subject-a"
	fixtureSession = sessionwire.SessionID("session-a")
	fixtureBearer  = "bearer-credential"
	spaMarker      = "<!doctype html><title>the single page application</title>"
)

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

// fixtureVerifier issues one tenant's claims for one credential value, so the
// principal the router ends up holding is derived by the real authenticator
// from a real credential rather than assembled by the test.
type fixtureVerifier struct {
	tenant sessionwire.TenantID
	expiry time.Time
	fail   error
}

func (v fixtureVerifier) VerifyCredential(_ context.Context, credential internalidentity.Credential) (internalidentity.Claims, error) {
	if v.fail != nil {
		return internalidentity.Claims{}, v.fail
	}
	if credential.Value() != fixtureBearer {
		return internalidentity.Claims{}, factoryidentity.ErrUnauthenticated
	}
	return internalidentity.Claims{
		Tenant:    v.tenant,
		Subject:   fixtureSubject,
		Kind:      factoryidentity.KindActor,
		ExpiresAt: v.expiry,
	}, nil
}

// storedSession is one session the fake store holds, keyed by BOTH identifiers.
// Keying by the pair is what makes the store able to disagree with a query
// scoped to the wrong tenant; a store keyed by SessionID alone would answer a
// cross-tenant read successfully and no test here could tell.
type storedSession struct {
	tenant  sessionwire.TenantID
	session sessionwire.SessionID
}

type fakeReader struct {
	mu       sync.Mutex
	sessions map[storedSession]bool
	// requests records every catalog request the router built, so a test can
	// assert the tenant it was scoped by rather than only the answer.
	requests []sessionstore.GetCatalogEntryRequest
	// deadlines records whether each call's context carried one.
	deadlines []bool
	// block, when set, holds the call until the context ends and returns the
	// context's error, which is how the request deadline acquires a reader.
	block bool
	// fail, when set, replaces the answer for every call.
	fail error
}

func newFakeReader(sessions ...storedSession) *fakeReader {
	held := make(map[storedSession]bool, len(sessions))
	for _, s := range sessions {
		held[s] = true
	}
	return &fakeReader{sessions: held}
}

func (f *fakeReader) GetCatalogEntry(ctx context.Context, req sessionstore.GetCatalogEntryRequest) (sessionstore.CatalogEntry, error) {
	f.mu.Lock()
	f.requests = append(f.requests, req)
	_, hasDeadline := ctx.Deadline()
	f.deadlines = append(f.deadlines, hasDeadline)
	block, fail, held := f.block, f.fail, f.sessions[storedSession{tenant: req.TenantID, session: req.SessionID}]
	f.mu.Unlock()

	if block {
		<-ctx.Done()
		return sessionstore.CatalogEntry{}, ctx.Err()
	}
	if fail != nil {
		return sessionstore.CatalogEntry{}, fail
	}
	if !held {
		return sessionstore.CatalogEntry{}, &sessionstore.CatalogError{Code: sessionstore.CatalogErrorNotFound, Field: "record"}
	}
	return sessionstore.CatalogEntry{Record: sessionstore.CatalogRecord{
		TenantID:  req.TenantID,
		SessionID: req.SessionID,
	}}, nil
}

func (f *fakeReader) ListSessions(context.Context, sessionstore.ListSessionsRequest) (sessionstore.SessionPage, error) {
	return sessionstore.SessionPage{}, errors.New("unused by A2.1")
}

func (f *fakeReader) ReadPublicJournal(context.Context, sessionstore.ReadPublicJournalRequest) (sessionwire.JournalPage, error) {
	return sessionwire.JournalPage{}, errors.New("unused by A2.1")
}

func (f *fakeReader) ReadGates(context.Context, sessionstore.ReadGatesRequest) (sessionwire.GatePage, error) {
	return sessionwire.GatePage{}, errors.New("unused by A2.1")
}

func (f *fakeReader) GetObject(context.Context, sessionstore.GetObjectRequest) (io.ReadCloser, error) {
	return nil, errors.New("unused by A2.1")
}

func (f *fakeReader) snapshot() ([]sessionstore.GetCatalogEntryRequest, []bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.requests), slices.Clone(f.deadlines)
}

// countingIDs hands out predictable identifiers so a test can tell a minted one
// from a header the caller supplied.
type countingIDs struct {
	mu   sync.Mutex
	next int
	fail error
}

func (c *countingIDs) NewUUID() (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fail != nil {
		return "", c.fail
	}
	c.next++
	return fmt.Sprintf("00000000-0000-4000-8000-%012d", c.next), nil
}

type fixture struct {
	router  *Router
	reads   *fakeReader
	ids     *countingIDs
	guard   *Guard
	clock   fixedClock
	authn   *internalidentity.Authenticator
	limits  RouteLimits
	verify  *fixtureVerifier
	uiCalls *int
}

type fixtureOption func(*RouterConfig, *fixture)

func withUI() fixtureOption {
	return func(cfg *RouterConfig, f *fixture) {
		calls := 0
		f.uiCalls = &calls
		cfg.UI = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			calls++
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = io.WriteString(w, spaMarker)
		})
	}
}

func withLimits(limits RouteLimits) fixtureOption {
	return func(cfg *RouterConfig, f *fixture) {
		cfg.Limits = limits
		f.limits = limits
	}
}

func withSessions(sessions ...storedSession) fixtureOption {
	return func(cfg *RouterConfig, f *fixture) {
		f.reads = newFakeReader(sessions...)
		cfg.Reads = f.reads
	}
}

func withVerifierFailure(err error) fixtureOption {
	return func(_ *RouterConfig, f *fixture) { f.verify.fail = err }
}

func withAuthorizer(authorizer Authorizer) fixtureOption {
	return func(cfg *RouterConfig, _ *fixture) { cfg.Authorizer = authorizer }
}

// recordingAuthorizer wraps the production authorizer and records which
// decision each route asked for. It DELEGATES rather than answering, so a
// sweep over it exercises the real decision as well as observing the call.
type recordingAuthorizer struct {
	mu    sync.Mutex
	inner internalidentity.Authorizer
	calls []authorizationCall
	deny  bool
}

type authorizationCall struct {
	operation string
	session   sessionwire.SessionID
	command   sessionstore.CommandKind
}

func (a *recordingAuthorizer) record(call authorizationCall) error {
	a.mu.Lock()
	a.calls = append(a.calls, call)
	deny := a.deny
	a.mu.Unlock()
	if deny {
		return internalidentity.ErrUnauthorized
	}
	return nil
}

func (a *recordingAuthorizer) AuthorizeSessionList(ctx context.Context, principal factoryidentity.Principal) error {
	if err := a.inner.AuthorizeSessionList(ctx, principal); err != nil {
		return err
	}
	return a.record(authorizationCall{operation: "list"})
}

func (a *recordingAuthorizer) AuthorizeSessionRead(ctx context.Context, principal factoryidentity.Principal, session sessionwire.SessionID) error {
	if err := a.inner.AuthorizeSessionRead(ctx, principal, session); err != nil {
		return err
	}
	return a.record(authorizationCall{operation: "read", session: session})
}

func (a *recordingAuthorizer) AuthorizeObjectRead(ctx context.Context, principal factoryidentity.Principal, session sessionwire.SessionID, object sessionwire.ObjectReference) error {
	if err := a.inner.AuthorizeObjectRead(ctx, principal, session, object); err != nil {
		return err
	}
	return a.record(authorizationCall{operation: "object", session: session})
}

func (a *recordingAuthorizer) AuthorizeControl(ctx context.Context, principal factoryidentity.Principal, session sessionwire.SessionID, kind sessionstore.CommandKind) error {
	if err := a.inner.AuthorizeControl(ctx, principal, session, kind); err != nil {
		return err
	}
	return a.record(authorizationCall{operation: "control", session: session, command: kind})
}

func (a *recordingAuthorizer) snapshot() []authorizationCall {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.calls)
}

func newFixture(t *testing.T, options ...fixtureOption) *fixture {
	t.Helper()

	clock := fixedClock{now: time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)}
	verifier := &fixtureVerifier{tenant: fixtureTenant, expiry: clock.now.Add(time.Hour)}
	authenticator, err := internalidentity.NewAuthenticator(internalidentity.Config{
		Verifier: verifier,
		Clock:    clock,
	})
	if err != nil {
		t.Fatalf("NewAuthenticator: %v", err)
	}
	guard, err := NewGuard(GuardConfig{
		CSRF: factoryidentity.CSRFConfig{
			SharedKey:      []byte("0123456789abcdef0123456789abcdef"),
			TokenTTL:       time.Hour,
			TrustedOrigins: []string{fixtureOrigin},
		},
		Credentials: authenticator,
		Clock:       clock,
	})
	if err != nil {
		t.Fatalf("NewGuard: %v", err)
	}

	f := &fixture{
		reads:  newFakeReader(storedSession{tenant: fixtureTenant, session: fixtureSession}),
		ids:    &countingIDs{},
		guard:  guard,
		clock:  clock,
		authn:  authenticator,
		verify: verifier,
		limits: DefaultRouteLimits(),
	}
	cfg := RouterConfig{
		Credentials: authenticator,
		Authorizer:  internalidentity.Authorizer{},
		Reads:       f.reads,
		Guard:       guard,
		IDs:         f.ids,
		Limits:      f.limits,
	}
	for _, option := range options {
		option(&cfg, f)
	}
	cfg.Reads = f.reads
	router, err := NewRouter(cfg)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	f.router = router
	return f
}

// request builds an authenticated request carrying a BEARER credential. A
// bearer is not ambient, so the CSRF rules exempt it and a control test can
// exercise the cookie path deliberately rather than every test paying for it.
func request(method, target string, body io.Reader) *http.Request {
	r := httptest.NewRequest(method, fixtureOrigin+target, body)
	r.Host = fixtureHost
	r.Header.Set("Authorization", "Bearer "+fixtureBearer)
	if body != nil {
		r.Header.Set("Content-Type", "application/json")
	}
	return r
}

func (f *fixture) serve(r *http.Request) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	f.router.ServeHTTP(recorder, r)
	return recorder
}

func (f *fixture) get(target string) *httptest.ResponseRecorder {
	return f.serve(request(http.MethodGet, target, nil))
}

func decodeEnvelope(t *testing.T, recorder *httptest.ResponseRecorder) sessionwire.ErrorEnvelope {
	t.Helper()

	if got := recorder.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json; body was %q", got, recorder.Body)
	}
	var envelope sessionwire.ErrorEnvelope
	if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("body %q is not a Core ErrorEnvelope: %v", recorder.Body, err)
	}
	return envelope
}

// ---------------------------------------------------------------------------
// The route table.
// ---------------------------------------------------------------------------

// TestTheRouteTableServesEveryPathTheSpecNames pins the public surface against
// section 8.1's list rather than against itself.
func TestTheRouteTableServesEveryPathTheSpecNames(t *testing.T) {
	t.Parallel()

	want := map[string][]string{
		"/v1/agents":                       {http.MethodGet},
		"/v1/capabilities":                 {http.MethodGet},
		"/v1/sessions":                     {http.MethodGet, http.MethodPost},
		"/v1/sessions/{sid}/status":        {http.MethodGet},
		"/v1/sessions/{sid}/journal":       {http.MethodGet},
		"/v1/sessions/{sid}/gates":         {http.MethodGet},
		"/v1/sessions/{sid}/input":         {http.MethodPost},
		"/v1/sessions/{sid}/interrupt":     {http.MethodPost},
		"/v1/sessions/{sid}/restore":       {http.MethodPost},
		"/v1/sessions/{sid}/gates/{gid}":   {http.MethodPost},
		"/v1/sessions/{sid}/objects/{oid}": {http.MethodGet},
		"/v1/realtime":                     {http.MethodGet},
		"/v1/csrf-token":                   {http.MethodGet},
	}
	got := map[string][]string{}
	for _, route := range routeTable() {
		got[route.pattern] = route.methods
	}
	if !maps.EqualFunc(want, got, slices.Equal) {
		t.Errorf("the route table serves\n  %v\nthe specification names\n  %v", got, want)
	}
}

// TestEveryRouteRefusesEveryMethodItDoesNotDeclare sweeps the whole method
// space against every route, so a route that quietly accepts DELETE is found
// without anybody choosing to test DELETE on it.
func TestEveryRouteRefusesEveryMethodItDoesNotDeclare(t *testing.T) {
	t.Parallel()

	methods := []string{
		http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut,
		http.MethodPatch, http.MethodDelete, http.MethodOptions, http.MethodTrace,
	}
	f := newFixture(t)
	checked := 0
	for _, route := range routeTable() {
		target := concreteTarget(route.pattern)
		for _, method := range methods {
			if slices.Contains(route.methods, method) {
				continue
			}
			checked++
			var body io.Reader
			if method == http.MethodPost || method == http.MethodPut || method == http.MethodPatch {
				body = strings.NewReader(`{}`)
			}
			recorder := f.serve(request(method, target, body))
			if recorder.Code != http.StatusMethodNotAllowed {
				t.Errorf("%s %s = %d, want 405", method, target, recorder.Code)
				continue
			}
			if got := recorder.Header().Get("Allow"); got != strings.Join(route.methods, ", ") {
				t.Errorf("%s %s: Allow = %q, want %q", method, target, got, strings.Join(route.methods, ", "))
			}
			if code := decodeEnvelope(t, recorder).Error.Code; code != ErrorCodeMethodNotAllowed {
				t.Errorf("%s %s: code = %q", method, target, code)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no refused method was exercised, so this sweep proves nothing")
	}
}

// TestNoCORSPreflightIsAnswered is the method sweep's most consequential row,
// stated on its own because the reason is not "OPTIONS is unused".
//
// CSRFHeaderName's whole defence is that a cross-site page cannot set a custom
// header without a preflight THIS SERVER approves. Answering OPTIONS is the
// first half of approving one, so the refusal is derived from that obstacle
// rather than chosen. It must also carry no Access-Control- header, which is
// the condition guard.go's comment records.
func TestNoCORSPreflightIsAnswered(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	r := request(http.MethodOptions, "/v1/sessions", nil)
	r.Header.Set("Origin", fixtureOrigin)
	r.Header.Set("Access-Control-Request-Method", http.MethodPost)
	r.Header.Set("Access-Control-Request-Headers", CSRFHeaderName)
	recorder := f.serve(r)

	if recorder.Code != http.StatusMethodNotAllowed {
		t.Errorf("a preflight was answered %d, want 405", recorder.Code)
	}
	for name := range recorder.Header() {
		if strings.HasPrefix(http.CanonicalHeaderKey(name), "Access-Control-") {
			t.Errorf("the response carries %s, which would begin approving the preflight %s exists to prevent", name, CSRFHeaderName)
		}
	}
}

// TestTheUnimplementedRoutesAreExactlyTheOnesLaterTasksOwn gives the 501
// answers a reader. Every route whose body a later runbook task fills in must
// say so in the table AND answer 501 today; every route this task implements
// must have no owner recorded and must not answer 501.
func TestTheUnimplementedRoutesAreExactlyTheOnesLaterTasksOwn(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	implemented := 0
	pending := 0
	for _, route := range routeTable() {
		target := concreteTarget(route.pattern)
		var body io.Reader
		if route.body == bodyJSON {
			body = strings.NewReader(`{}`)
		}
		recorder := f.serve(request(route.methods[0], target, body))
		if route.owner == "" {
			implemented++
			if recorder.Code == http.StatusNotImplemented {
				t.Errorf("%s claims no later owner but answers 501", route.pattern)
			}
			continue
		}
		pending++
		if recorder.Code != http.StatusNotImplemented {
			t.Errorf("%s is owned by %s but answered %d, want 501", route.pattern, route.owner, recorder.Code)
			continue
		}
		if code := decodeEnvelope(t, recorder).Error.Code; code != ErrorCodeNotImplemented {
			t.Errorf("%s: code = %q, want %q", route.pattern, code, ErrorCodeNotImplemented)
		}
	}
	if implemented == 0 {
		t.Fatal("no route is implemented at this task, so the 501 split is vacuous")
	}
	if pending == 0 {
		t.Fatal("no route is pending, so the owner column has no reader")
	}
}

// TestTheCSRFTokenRouteIssuesATokenTheGuardAccepts is the one route this task
// implements, driven end to end: the token the route serves must be the token
// the guard admits on the very next state-changing request, on the AMBIENT
// credential path where CSRF applies.
func TestTheCSRFTokenRouteIssuesATokenTheGuardAccepts(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	cookieRequest := func(method, target string, body io.Reader) *http.Request {
		r := httptest.NewRequest(method, fixtureOrigin+target, body)
		r.Host = fixtureHost
		r.Header.Set("Origin", fixtureOrigin)
		r.AddCookie(&http.Cookie{Name: internalidentity.DefaultCookieName, Value: fixtureBearer})
		if body != nil {
			r.Header.Set("Content-Type", "application/json")
		}
		return r
	}

	recorder := f.serve(cookieRequest(http.MethodGet, "/v1/csrf-token", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET /v1/csrf-token = %d (%s), want 200", recorder.Code, recorder.Body)
	}
	var issued struct {
		CSRFToken string `json:"csrf_token"`
		ExpiresAt string `json:"expires_at"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &issued); err != nil {
		t.Fatalf("decoding the issued token: %v", err)
	}
	if issued.CSRFToken == "" {
		t.Fatal("the route served an empty token")
	}

	without := f.serve(cookieRequest(http.MethodPost, "/v1/sessions", strings.NewReader(`{}`)))
	if without.Code != http.StatusForbidden {
		t.Errorf("a cookie POST with no token = %d, want 403", without.Code)
	}

	with := cookieRequest(http.MethodPost, "/v1/sessions", strings.NewReader(`{}`))
	with.Header.Set(CSRFHeaderName, issued.CSRFToken)
	if got := f.serve(with); got.Code == http.StatusForbidden {
		t.Errorf("the token this route issued was refused by the guard: %s", got.Body)
	}
}

// concreteTarget turns a route pattern into a request path by substituting a
// fixture value for each wildcard. It is derived from the pattern rather than
// listed beside it, so a route added with a new wildcard cannot silently be
// requested at a literal path containing braces.
func concreteTarget(pattern string) string {
	segments := strings.Split(pattern, "/")
	for i, segment := range segments {
		if !strings.HasPrefix(segment, "{") || !strings.HasSuffix(segment, "}") {
			continue
		}
		switch segment {
		case "{sid}":
			segments[i] = string(fixtureSession)
		default:
			segments[i] = "wildcard-value"
		}
	}
	return strings.Join(segments, "/")
}

func TestConcreteTargetSubstitutesEveryWildcard(t *testing.T) {
	t.Parallel()

	for _, route := range routeTable() {
		target := concreteTarget(route.pattern)
		if strings.ContainsAny(target, "{}") {
			t.Errorf("concreteTarget(%q) = %q, which still holds a wildcard", route.pattern, target)
		}
	}
	if got := concreteTarget("/v1/sessions/{sid}/gates/{gid}"); got != "/v1/sessions/session-a/gates/wildcard-value" {
		t.Errorf("concreteTarget substituted %q", got)
	}
}

// ---------------------------------------------------------------------------
// No API failure falls through to the SPA.
// ---------------------------------------------------------------------------

// TestNoAPIFailureFallsThroughToTheSPA is step 2's headline requirement, driven
// over every failure this router can produce with a SPA actually mounted. The
// SPA is a real handler serving real HTML, so a fallthrough would be visible as
// its marker rather than merely as a missing header.
func TestNoAPIFailureFallsThroughToTheSPA(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		status  int
		code    sessionwire.ErrorCode
		build   func(*fixture) *http.Request
		options []fixtureOption
	}{
		{
			name: "unauthenticated", status: http.StatusUnauthorized, code: ErrorCodeUnauthenticated,
			build: func(*fixture) *http.Request {
				r := httptest.NewRequest(http.MethodGet, fixtureOrigin+"/v1/agents", nil)
				r.Host = fixtureHost
				return r
			},
		},
		{
			name: "verifier unavailable", status: http.StatusServiceUnavailable, code: ErrorCodeUnavailable,
			options: []fixtureOption{withVerifierFailure(errors.New("the credential service is down"))},
			build:   func(*fixture) *http.Request { return request(http.MethodGet, "/v1/agents", nil) },
		},
		{
			name: "unknown API route", status: http.StatusNotFound, code: ErrorCodeRouteNotFound,
			build: func(*fixture) *http.Request { return request(http.MethodGet, "/v1/nothing-here", nil) },
		},
		{
			name: "a session path that names no route", status: http.StatusNotFound, code: ErrorCodeRouteNotFound,
			build: func(*fixture) *http.Request { return request(http.MethodGet, "/v1/sessions/session-a", nil) },
		},
		{
			name: "method not allowed", status: http.StatusMethodNotAllowed, code: ErrorCodeMethodNotAllowed,
			build: func(*fixture) *http.Request { return request(http.MethodDelete, "/v1/agents", nil) },
		},
		{
			name: "session in another tenant", status: http.StatusNotFound, code: sessionwire.ErrorCodeSessionNotFound,
			options: []fixtureOption{withSessions(storedSession{tenant: otherTenant, session: fixtureSession})},
			build: func(*fixture) *http.Request {
				return request(http.MethodGet, "/v1/sessions/"+string(fixtureSession)+"/status", nil)
			},
		},
		{
			name: "unsupported media type", status: http.StatusUnsupportedMediaType, code: ErrorCodeUnsupportedMediaType,
			build: func(*fixture) *http.Request {
				r := request(http.MethodPost, "/v1/sessions", strings.NewReader("name=value"))
				r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				return r
			},
		},
		{
			name: "payload too large", status: http.StatusRequestEntityTooLarge, code: ErrorCodePayloadTooLarge,
			options: []fixtureOption{withLimits(RouteLimits{MaxRequestBytes: 8, RequestTimeout: time.Second})},
			build: func(*fixture) *http.Request {
				return request(http.MethodPost, "/v1/sessions", strings.NewReader(strings.Repeat("x", 64)))
			},
		},
		{
			name: "an empty body on a bodied route", status: http.StatusBadRequest, code: sessionwire.ErrorCodeInvalidRequest,
			build: func(*fixture) *http.Request {
				return request(http.MethodPost, "/v1/sessions", strings.NewReader(""))
			},
		},
		{
			name: "an internal store fault", status: http.StatusInternalServerError, code: ErrorCodeInternal,
			build: func(f *fixture) *http.Request {
				f.reads.fail = errors.New("the backend is confused")
				return request(http.MethodGet, "/v1/sessions/"+string(fixtureSession)+"/status", nil)
			},
		},
		{
			name: "a rejected origin", status: http.StatusForbidden, code: sessionwire.ErrorCode(ReasonOriginNotTrusted),
			build: func(*fixture) *http.Request {
				r := request(http.MethodGet, "/v1/agents", nil)
				r.Header.Set("Origin", "https://attacker.test")
				return r
			},
		},
		{
			name: "not implemented", status: http.StatusNotImplemented, code: ErrorCodeNotImplemented,
			build: func(*fixture) *http.Request { return request(http.MethodGet, "/v1/agents", nil) },
		},
	}
	for _, row := range cases {
		t.Run(row.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t, append(slices.Clone(row.options), withUI())...)
			recorder := f.serve(row.build(f))

			if recorder.Code != row.status {
				t.Errorf("status = %d, want %d; body %q", recorder.Code, row.status, recorder.Body)
			}
			if strings.Contains(recorder.Body.String(), spaMarker) {
				t.Fatalf("the API failure fell through to the SPA: %q", recorder.Body)
			}
			if *f.uiCalls != 0 {
				t.Errorf("the SPA handler was reached %d times by an API request", *f.uiCalls)
			}
			if code := decodeEnvelope(t, recorder).Error.Code; code != row.code {
				t.Errorf("code = %q, want %q", code, row.code)
			}
		})
	}
	if len(cases) == 0 {
		t.Fatal("no failure was exercised")
	}
}

// TestTheSPAIsServedOnlyOutsideTheAPI is the other direction of the same claim:
// mounting a UI must not make it the answer to anything under /v1, and must
// still make it the answer everywhere else. Without this, a router that never
// reached the SPA at all would pass the sweep above.
func TestTheSPAIsServedOnlyOutsideTheAPI(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withUI())
	for _, target := range []string{"/", "/sessions/session-a", "/v1x/agents", "/assets/app.js"} {
		recorder := f.serve(httptest.NewRequest(http.MethodGet, fixtureOrigin+target, nil))
		if !strings.Contains(recorder.Body.String(), spaMarker) {
			t.Errorf("GET %s did not reach the SPA: %d %q", target, recorder.Code, recorder.Body)
		}
	}
	if *f.uiCalls != 4 {
		t.Errorf("the SPA was reached %d times, want 4", *f.uiCalls)
	}
}

// TestWithoutAUIAnUnknownPathIsStillJSON keeps the no-UI composition -- which
// ui_test.go establishes is a supported one, not a degraded one -- from
// answering with net/http's plain-text 404.
func TestWithoutAUIAnUnknownPathIsStillJSON(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	recorder := f.serve(httptest.NewRequest(http.MethodGet, fixtureOrigin+"/anything", nil))
	if recorder.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", recorder.Code)
	}
	if code := decodeEnvelope(t, recorder).Error.Code; code != ErrorCodeRouteNotFound {
		t.Errorf("code = %q", code)
	}
}

// TestAnUncleanAPIPathIsRefusedRatherThanRedirected closes the one way an API
// request can leave the API without a handler deciding to let it.
//
// http.ServeMux CLEANS a path and answers 301 to the cleaned one. A request to
// /v1/../assets/app.js would therefore be redirected out of /v1 and into the
// SPA, with a Location header and an HTML body, which is a fallthrough by
// another name. The router refuses an unclean API path outright.
func TestAnUncleanAPIPathIsRefusedRatherThanRedirected(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withUI())
	for _, target := range []string{
		"/v1/sessions/../agents",
		"/v1/../assets/app.js",
		"/v1//agents",
		"/v1/agents/",
		// Percent-encoded dot segments: the mux cleans the ESCAPED path, so
		// these are the constructions that establish the decoded check covers
		// the escaped one rather than merely coinciding with it.
		"/v1/%2e%2e/assets/app.js",
		"/v1/sessions/%2e%2e/agents",
		"/v1/%2F/agents",
		// The membership test reads the path as requested AND as cleaned. This
		// one is API only under the second reading, so without that disjunct it
		// would be handed to the SPA and then routed by the mux as /v1/agents.
		"//v1/agents",
	} {
		r := httptest.NewRequest(http.MethodGet, fixtureOrigin+target, nil)
		r.Host = fixtureHost
		r.Header.Set("Authorization", "Bearer "+fixtureBearer)
		recorder := f.serve(r)
		if recorder.Code/100 == 3 {
			t.Errorf("GET %s = %d with Location %q; an API path must not be redirected",
				target, recorder.Code, recorder.Header().Get("Location"))
		}
		if strings.Contains(recorder.Body.String(), spaMarker) {
			t.Errorf("GET %s reached the SPA", target)
		}
		if recorder.Code != http.StatusNotFound {
			t.Errorf("GET %s = %d, want 404", target, recorder.Code)
		}
	}
}

// ---------------------------------------------------------------------------
// Identity, headers and request identifiers.
// ---------------------------------------------------------------------------

// TestAnUnauthenticatedRequestCannotEnumerateRoutes is the ordering claim the
// guard's doc makes at the level the router composes it: authentication runs
// before routing, so an anonymous caller gets the same 401 for a route that
// exists and one that does not.
func TestAnUnauthenticatedRequestCannotEnumerateRoutes(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	var bodies []string
	for _, target := range []string{"/v1/agents", "/v1/nothing-here"} {
		r := httptest.NewRequest(http.MethodGet, fixtureOrigin+target, nil)
		r.Host = fixtureHost
		recorder := f.serve(r)
		if recorder.Code != http.StatusUnauthorized {
			t.Fatalf("GET %s = %d, want 401", target, recorder.Code)
		}
		if got := recorder.Header().Get("WWW-Authenticate"); !strings.HasPrefix(got, "Bearer") {
			t.Errorf("GET %s: WWW-Authenticate = %q, want a Bearer challenge", target, got)
		}
		bodies = append(bodies, recorder.Body.String())
	}
	if bodies[0] != bodies[1] {
		t.Errorf("an existing and a nonexistent route answered differently while unauthenticated:\n  %q\n  %q", bodies[0], bodies[1])
	}
}

// TestSecurityHeadersAreOnEveryAPIResponse sweeps the success answer and a
// failure answer, because a header set only on the error path is a header the
// responses that carry private data do not have.
func TestSecurityHeadersAreOnEveryAPIResponse(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	want := map[string]string{
		"X-Content-Type-Options":  "nosniff",
		"Cache-Control":           "no-store",
		"Referrer-Policy":         "no-referrer",
		"X-Frame-Options":         "DENY",
		"Content-Security-Policy": "default-src 'none'; frame-ancestors 'none'",
	}
	for _, target := range []string{"/v1/csrf-token", "/v1/agents", "/v1/nothing-here"} {
		recorder := f.get(target)
		for name, value := range want {
			if got := recorder.Header().Get(name); got != value {
				t.Errorf("GET %s: %s = %q, want %q", target, name, got, value)
			}
		}
	}
}

// TestSecurityHeadersAreSetByTheMiddlewareItself is the unit the end-to-end
// sweep above cannot separate.
//
// Every response the router produces TODAY is written by writeAPIError or by
// the guard's token writer, and both set nosniff and no-store themselves, so
// deleting either from the middleware changes nothing a route can observe --
// measured: that mutation survives the whole suite. It will stop being harmless
// the moment A2.2 writes a success body of its own. The middleware is the
// durable mechanism, so it is asserted as one: an inner handler that sets no
// headers at all must still be wrapped in the full set.
func TestSecurityHeadersAreSetByTheMiddlewareItself(t *testing.T) {
	t.Parallel()

	bare := securityHeaders(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	recorder := httptest.NewRecorder()
	bare.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/anything", nil))

	for name, value := range map[string]string{
		"X-Content-Type-Options":  "nosniff",
		"Cache-Control":           "no-store",
		"Referrer-Policy":         "no-referrer",
		"X-Frame-Options":         "DENY",
		"Content-Security-Policy": "default-src 'none'; frame-ancestors 'none'",
	} {
		if got := recorder.Header().Get(name); got != value {
			t.Errorf("%s = %q, want %q", name, got, value)
		}
	}
}

// TestEveryAPIResponseCarriesAMintedRequestID holds two properties at once: the
// identifier exists, and it is MINTED rather than echoed. A router that copied
// the caller's X-Request-Id would put arbitrary caller-controlled text into
// every log line that later correlates on it.
func TestEveryAPIResponseCarriesAMintedRequestID(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	const forged = "forged-by-the-caller\r\nX-Injected: yes"
	seen := map[string]bool{}
	for range 3 {
		r := request(http.MethodGet, "/v1/agents", nil)
		r.Header.Set(RequestIDHeader, forged)
		recorder := f.serve(r)
		got := recorder.Header().Get(RequestIDHeader)
		if got == "" {
			t.Fatal("no request identifier was stamped")
		}
		if strings.Contains(got, "forged") {
			t.Fatalf("the request identifier %q came from the caller", got)
		}
		if seen[got] {
			t.Errorf("request identifier %q was reused", got)
		}
		seen[got] = true
	}
	if len(seen) != 3 {
		t.Errorf("three requests produced %d identifiers", len(seen))
	}
}

// TestAnIdentifierSourceFailureDoesNotFailTheRequest keeps a correlation aid
// from becoming an availability dependency.
func TestAnIdentifierSourceFailureDoesNotFailTheRequest(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.ids.fail = errors.New("no entropy")
	recorder := f.get("/v1/csrf-token")
	if recorder.Code != http.StatusOK {
		t.Errorf("status = %d, want 200: a missing request identifier must not fail the request", recorder.Code)
	}
	if got := recorder.Header().Get(RequestIDHeader); got != "" {
		t.Errorf("%s = %q, want it absent rather than a fabricated value", RequestIDHeader, got)
	}
}

// ---------------------------------------------------------------------------
// Panic recovery.
// ---------------------------------------------------------------------------

// TestAPanicBecomesAJSONInternalError drives a real panic through the real
// chain by making the store panic, which is the shape a production fault takes:
// a dependency, not the router.
func TestAPanicBecomesAJSONInternalError(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withUI())
	f.reads.fail = panicError{}
	recorder := f.get("/v1/sessions/" + string(fixtureSession) + "/status")

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body %q", recorder.Code, recorder.Body)
	}
	if strings.Contains(recorder.Body.String(), spaMarker) {
		t.Fatal("a panic fell through to the SPA")
	}
	envelope := decodeEnvelope(t, recorder)
	if envelope.Error.Code != ErrorCodeInternal {
		t.Errorf("code = %q", envelope.Error.Code)
	}
	if strings.Contains(envelope.Error.Message, "deliberate") {
		t.Errorf("the panic value reached the response body: %q", envelope.Error.Message)
	}
}

// panicError is an error whose use panics, so the fake store can fail the way a
// real dependency does without the fake having a panic switch of its own.
type panicError struct{}

func (panicError) Error() string { panic("deliberate panic from a dependency") }

// TestARecoveredPanicAfterAPartialWriteDoesNotAppendAnEnvelope is the branch a
// recovery middleware usually gets wrong. Once bytes are on the wire the status
// is already sent, so writing a 500 envelope produces a body that is neither
// the handler's nor an error -- a client parsing it sees corruption. The
// recovery must re-panic with http.ErrAbortHandler, which net/http answers by
// closing the connection.
func TestARecoveredPanicAfterAPartialWriteDoesNotAppendAnEnvelope(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	recorder := httptest.NewRecorder()
	partial := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"partial":`)
		panic("after the write")
	})

	writer := &recordingWriter{ResponseWriter: recorder}
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		f.router.recoverInto(writer, partial).ServeHTTP(writer, request(http.MethodGet, "/v1/agents", nil))
	}()

	if !errors.Is(recovered.(error), http.ErrAbortHandler) {
		t.Fatalf("recovered %v, want http.ErrAbortHandler so the connection is closed", recovered)
	}
	if body := recorder.Body.String(); body != `{"partial":` {
		t.Errorf("the recovery appended to a partial body: %q", body)
	}
}

// ---------------------------------------------------------------------------
// Bodies and deadlines.
// ---------------------------------------------------------------------------

// TestABodiedRouteAcceptsAJSONContentTypeWithAParameter keeps the media-type
// check from being string equality, which would reject the header every browser
// and every JSON client actually sends.
func TestABodiedRouteAcceptsAJSONContentTypeWithAParameter(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	for _, contentType := range []string{
		"application/json",
		"application/json; charset=utf-8",
		"application/json;charset=UTF-8",
		"APPLICATION/JSON",
	} {
		r := request(http.MethodPost, "/v1/sessions", strings.NewReader(`{}`))
		r.Header.Set("Content-Type", contentType)
		if got := f.serve(r); got.Code == http.StatusUnsupportedMediaType {
			t.Errorf("Content-Type %q was refused", contentType)
		}
	}
	for _, contentType := range []string{
		"", "text/plain", "application/json-patch+json", "multipart/form-data; boundary=x",
		"application/json; charset=iso-8859-1",
	} {
		r := request(http.MethodPost, "/v1/sessions", strings.NewReader(`{}`))
		r.Header.Set("Content-Type", contentType)
		if got := f.serve(r); got.Code != http.StatusUnsupportedMediaType {
			t.Errorf("Content-Type %q answered %d, want 415", contentType, got.Code)
		}
	}
}

// TestTheBodyCeilingIsAppliedWhileReading is the difference between bounding a
// body and trusting a header. A caller that declares a small Content-Length and
// sends more must still be cut off at the ceiling.
func TestTheBodyCeilingIsAppliedWhileReading(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withLimits(RouteLimits{MaxRequestBytes: 16, RequestTimeout: time.Second}))
	r := request(http.MethodPost, "/v1/sessions", strings.NewReader(strings.Repeat("x", 512)))
	r.ContentLength = 4
	recorder := f.serve(r)
	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 413; a lying Content-Length must not raise the ceiling", recorder.Code)
	}
	if code := decodeEnvelope(t, recorder).Error.Code; code != ErrorCodePayloadTooLarge {
		t.Errorf("code = %q", code)
	}
	// The bound is inclusive, and BOTH sides of it are driven. A test that
	// only refused an obviously oversized body cannot tell a ceiling of n from
	// a ceiling of n+1, so the pair is what pins the value rather than the
	// presence of a check.
	atCeiling := request(http.MethodPost, "/v1/sessions", strings.NewReader(strings.Repeat("x", 16)))
	if got := f.serve(atCeiling); got.Code == http.StatusRequestEntityTooLarge {
		t.Error("a body of exactly 16 bytes was refused by a ceiling of 16")
	}
	overByOne := request(http.MethodPost, "/v1/sessions", strings.NewReader(strings.Repeat("x", 17)))
	if got := f.serve(overByOne); got.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("a body of 17 bytes answered %d under a ceiling of 16, want 413", got.Code)
	}
}

// TestARequestDeadlineBoundsHandlerWork gives the deadline a reader that is
// production code: the store receives the context the router built, and a store
// that never answers must end as a timeout rather than as a hung request.
func TestARequestDeadlineBoundsHandlerWork(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withLimits(RouteLimits{MaxRequestBytes: 1 << 20, RequestTimeout: 20 * time.Millisecond}))
	f.reads.block = true
	recorder := f.get("/v1/sessions/" + string(fixtureSession) + "/status")

	if recorder.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504; body %q", recorder.Code, recorder.Body)
	}
	envelope := decodeEnvelope(t, recorder)
	if envelope.Error.Code != ErrorCodeTimeout {
		t.Errorf("code = %q", envelope.Error.Code)
	}
	if !envelope.Error.Retryable {
		t.Error("a deadline is retryable and the envelope says it is not")
	}
	_, deadlines := f.reads.snapshot()
	if len(deadlines) != 1 || !deadlines[0] {
		t.Errorf("the store saw deadlines %v, want exactly one call carrying one", deadlines)
	}
}

// TestAStreamingRouteCarriesNoHandlerDeadline is the other half. A deadline on
// a route that streams an object or holds a WebSocket open would cut the
// response off in the middle, so the exemption is behaviour rather than
// preference -- and it is read through the same store call.
func TestAStreamingRouteCarriesNoHandlerDeadline(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withLimits(RouteLimits{MaxRequestBytes: 1 << 20, RequestTimeout: 20 * time.Millisecond}))
	recorder := f.get("/v1/sessions/" + string(fixtureSession) + "/objects/object-a")
	if recorder.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501; body %q", recorder.Code, recorder.Body)
	}
	_, deadlines := f.reads.snapshot()
	if len(deadlines) != 1 {
		t.Fatalf("the store was called %d times, want 1", len(deadlines))
	}
	if deadlines[0] {
		t.Error("the object route imposed a handler deadline, which would truncate a stream")
	}
}

// ---------------------------------------------------------------------------
// The tenant boundary: A1.2's carry-forward, discharged.
// ---------------------------------------------------------------------------

// TestACrossTenantSessionIsIndistinguishableFromOneThatDoesNotExist is the
// obligation A1.2 could only satisfy vacuously.
//
// It compares the WHOLE response -- status, every header but the per-request
// identifier, and the body bytes -- rather than the error code, because an
// identical code is the usual mask for a difference somewhere else in the
// answer. The two requests differ only in which tenant the store holds the
// session for; everything the caller sends is identical.
func TestACrossTenantSessionIsIndistinguishableFromOneThatDoesNotExist(t *testing.T) {
	t.Parallel()

	target := "/v1/sessions/" + string(fixtureSession) + "/status"
	crossTenant := newFixture(t, withSessions(storedSession{tenant: otherTenant, session: fixtureSession}))
	absent := newFixture(t, withSessions())

	first := crossTenant.get(target)
	second := absent.get(target)

	if first.Code != http.StatusNotFound {
		t.Fatalf("the cross-tenant read answered %d, want 404", first.Code)
	}
	if diff := responseDifference(first, second); diff != "" {
		t.Errorf("a session in another tenant is distinguishable from one that does not exist: %s", diff)
	}
	// The answer names no identifier from the request or from the store. Byte
	// identity alone would still permit a message that echoed the session --
	// identical in both responses, and a disclosure in both.
	for _, secret := range []string{string(fixtureSession), string(fixtureTenant), string(otherTenant)} {
		if strings.Contains(first.Body.String(), secret) {
			t.Errorf("the 404 body %q names %q", first.Body, secret)
		}
	}
	// The store really was asked, and asked with the READER's tenant. Without
	// this a router that answered 404 without consulting the store at all would
	// satisfy the comparison above.
	requests, _ := crossTenant.reads.snapshot()
	if len(requests) != 1 {
		t.Fatalf("the store was called %d times, want 1", len(requests))
	}
	if requests[0].TenantID != fixtureTenant {
		t.Errorf("the store was asked for tenant %q, want the principal's %q", requests[0].TenantID, fixtureTenant)
	}
}

// TestASessionInThePrincipalsOwnTenantIsReached is the control for the test
// above. A handler that always answered 404 would make cross-tenant and absent
// indistinguishable in the strongest possible way and be useless.
func TestASessionInThePrincipalsOwnTenantIsReached(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withSessions(storedSession{tenant: fixtureTenant, session: fixtureSession}))
	recorder := f.get("/v1/sessions/" + string(fixtureSession) + "/status")
	if recorder.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501: the session resolved and the body is A2.3's; got %q", recorder.Code, recorder.Body)
	}
}

// TestTheStoreIsAlwaysAskedForThePrincipalsOwnTenant sweeps every session route
// and every method, so the scoping is a property of the ROUTE SET rather than
// of the one route the indistinguishability test happens to drive.
func TestTheStoreIsAlwaysAskedForThePrincipalsOwnTenant(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	driven := 0
	for _, route := range routeTable() {
		if !route.session {
			continue
		}
		driven++
		var body io.Reader
		if route.body == bodyJSON {
			body = strings.NewReader(`{}`)
		}
		f.serve(request(route.methods[0], concreteTarget(route.pattern), body))
	}
	if driven == 0 {
		t.Fatal("no session-scoped route was driven, so this sweep proves nothing")
	}
	requests, _ := f.reads.snapshot()
	if len(requests) != driven {
		t.Fatalf("%d session routes produced %d catalog reads", driven, len(requests))
	}
	for _, req := range requests {
		if req.TenantID != fixtureTenant {
			t.Errorf("a catalog read was scoped to %q, want the principal's %q", req.TenantID, fixtureTenant)
		}
		if req.SessionID != fixtureSession {
			t.Errorf("a catalog read named session %q, want %q", req.SessionID, fixtureSession)
		}
	}
}

// TestAnInvalidSessionIdentifierNeverReachesTheStore keeps a malformed path
// value out of the durable plane. Validity is a pure function of the string, so
// answering it apart from absence discloses nothing.
func TestAnInvalidSessionIdentifierNeverReachesTheStore(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	target := "/v1/sessions/" + strings.Repeat("s", sessionwire.MaxIDBytes+1) + "/status"
	recorder := f.serve(request(http.MethodGet, target, nil))
	if recorder.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", recorder.Code)
	}
	if code := decodeEnvelope(t, recorder).Error.Code; code != sessionwire.ErrorCodeInvalidRequest {
		t.Errorf("code = %q", code)
	}
	if requests, _ := f.reads.snapshot(); len(requests) != 0 {
		t.Errorf("an invalid session identifier reached the store as %+v", requests)
	}
}

// responseDifference reports the first way two responses differ, ignoring the
// per-request identifier, which is random by construction.
func responseDifference(first, second *httptest.ResponseRecorder) string {
	if first.Code != second.Code {
		return fmt.Sprintf("status %d against %d", first.Code, second.Code)
	}
	if first.Body.String() != second.Body.String() {
		return fmt.Sprintf("body %q against %q", first.Body, second.Body)
	}
	names := map[string]bool{}
	for name := range first.Header() {
		names[name] = true
	}
	for name := range second.Header() {
		names[name] = true
	}
	for name := range names {
		if http.CanonicalHeaderKey(name) == RequestIDHeader {
			continue
		}
		if a, b := first.Header().Get(name), second.Header().Get(name); a != b {
			return fmt.Sprintf("header %s %q against %q", name, a, b)
		}
	}
	return ""
}

func TestResponseDifferenceSeesEachAxisItIgnoresNone(t *testing.T) {
	t.Parallel()

	base := func() *httptest.ResponseRecorder {
		r := httptest.NewRecorder()
		r.Code = http.StatusNotFound
		r.Header().Set("Content-Type", "application/json")
		r.Header().Set(RequestIDHeader, "one")
		_, _ = r.Body.WriteString(`{"a":1}`)
		return r
	}
	if diff := responseDifference(base(), base()); diff != "" {
		t.Fatalf("two identical responses differed: %s", diff)
	}
	onlyID := base()
	onlyID.Header().Set(RequestIDHeader, "two")
	if diff := responseDifference(base(), onlyID); diff != "" {
		t.Errorf("the per-request identifier was treated as a difference: %s", diff)
	}
	for name, mutate := range map[string]func(*httptest.ResponseRecorder){
		"status": func(r *httptest.ResponseRecorder) { r.Code = http.StatusOK },
		"body":   func(r *httptest.ResponseRecorder) { r.Body.Reset(); _, _ = r.Body.WriteString(`{"a":2}`) },
		"header": func(r *httptest.ResponseRecorder) { r.Header().Set("Cache-Control", "no-store") },
	} {
		changed := base()
		mutate(changed)
		if responseDifference(base(), changed) == "" {
			t.Errorf("a %s difference was not reported", name)
		}
	}
}

// ---------------------------------------------------------------------------
// The structural half: a tenant reaches SessionStore only through scope.
// ---------------------------------------------------------------------------

// TestNoProductionFileBuildsAStoreRequestOutsideTheScope is the structural
// equivalent A1.2's carry-forward asked for.
//
// The behavioural sweep above can only cover the routes that exist today. This
// scans EVERY production file of the package, enumerated from the directory
// rather than named, and requires that a SessionStore request is constructed
// only inside a method on scope -- the one type that gets its tenant from an
// authenticated principal.
//
// What it establishes and what it does not, stated rather than implied: it
// establishes that a tenant reaches a SessionStore request only through scope.
// It does NOT establish that the principal a scope was built from is the
// authenticated one; nothing structural can, because both are values of the
// same type. That half is behavioural, and its reader is
// TestTheStoreIsAlwaysAskedForThePrincipalsOwnTenant, which compares the tenant
// the store received against the one the VERIFIER issued.
func TestNoProductionFileBuildsAStoreRequestOutsideTheScope(t *testing.T) {
	t.Parallel()

	files := productionSources(t)
	if len(files) == 0 {
		t.Fatal("no production files were found, so this scan proves nothing")
	}
	literals, inScope := 0, 0
	for name, source := range files {
		report, err := scanTenantScope(name, source, allScopeRules)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		literals += report.storeLiterals
		inScope += report.scopedLiterals
		for _, violation := range report.violations {
			t.Errorf("%s: %s", name, violation)
		}
	}
	if literals == 0 {
		t.Fatal("the production files construct no SessionStore request, so this scan proves nothing")
	}
	if inScope == 0 {
		t.Fatal("no SessionStore request is constructed inside a scope method, so the exemption has no reader")
	}
}

// TestTheScopeScanFindsEveryConstructionItForbids proves the guard in the
// direction that matters: each evasion must be reported, and reverting ONLY the
// rule that reports it must let the same source through. A rule with no
// construct of its own is a rule the scan does not need.
func TestTheScopeScanFindsEveryConstructionItForbids(t *testing.T) {
	t.Parallel()

	for name, probe := range map[string]struct {
		source string
		rule   scopeRule
	}{
		"a composite literal outside scope": {
			// The literal names no TenantID member, so ruleTenantField cannot
			// be what reports it: reverting ruleStoreLiteral must let it pass.
			source: "package httpapi\nfunc handle(s sessionwire.SessionID) any {\n\treturn sessionstore.GetCatalogEntryRequest{SessionID: s}\n}\n",
			rule:   ruleStoreLiteral,
		},
		"a keyed tenant field on some other type": {
			source: "package httpapi\ntype query struct{ TenantID sessionwire.TenantID }\nfunc handle(t sessionwire.TenantID) any {\n\treturn query{TenantID: t}\n}\n",
			rule:   ruleTenantField,
		},
		"an assignment to a tenant field": {
			source: "package httpapi\nfunc handle(t sessionwire.TenantID) any {\n\tvar req sessionstore.GetCatalogEntryRequest\n\treq.TenantID = t\n\treturn req\n}\n",
			rule:   ruleTenantAssign,
		},
	} {
		report, err := scanTenantScope(name+".go", []byte(probe.source), allScopeRules)
		if err != nil {
			t.Fatalf("%s: parse: %v", name, err)
		}
		if len(report.violations) == 0 {
			t.Errorf("%s: the scan reported nothing", name)
		}
		// Reverting exactly the rule under test must let the same source pass,
		// which is what shows the report came from that rule and not from
		// another one that happens to match the same construct.
		reverted, err := scanTenantScope(name+".go", []byte(probe.source), allScopeRules&^probe.rule)
		if err != nil {
			t.Fatalf("%s: parse without %v: %v", name, probe.rule, err)
		}
		if len(reverted.violations) != 0 {
			t.Errorf("%s: with %v reverted the scan still reported %v; the construct is caught by another rule, so this one is untested",
				name, probe.rule, reverted.violations)
		}
	}
}

// TestTheScopeScanAcceptsTheConstructionItExistsGuardsFor is the negative
// direction: the exempt shape must really pass, or the guard would be satisfied
// by a package that builds no requests at all.
func TestTheScopeScanAcceptsTheConstructionItExistsGuardsFor(t *testing.T) {
	t.Parallel()

	const exempt = "package httpapi\n" +
		"type scope struct{ principal identity.Principal }\n" +
		"func (s scope) catalogEntry(session sessionwire.SessionID) sessionstore.GetCatalogEntryRequest {\n" +
		"\treturn sessionstore.GetCatalogEntryRequest{TenantID: s.principal.Tenant(), SessionID: session}\n}\n"
	report, err := scanTenantScope("exempt.go", []byte(exempt), allScopeRules)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(report.violations) != 0 {
		t.Errorf("the exempt construction was reported: %v", report.violations)
	}
	if report.storeLiterals != 1 || report.scopedLiterals != 1 {
		t.Errorf("counted %d store literals and %d inside scope, want 1 and 1", report.storeLiterals, report.scopedLiterals)
	}
	// The exemption is the RECEIVER, not the file: the same construction in a
	// method on another type must still be reported.
	elsewhere := strings.Replace(exempt, "(s scope)", "(s handler)", 1)
	other, err := scanTenantScope("elsewhere.go", []byte(elsewhere), allScopeRules)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(other.violations) == 0 {
		t.Error("a method on another type built a store request and was not reported")
	}
}

// TestTheScopeScanCoversEveryProductionFile is the tripwire for the defect that
// cost internal/identity a round: a guard naming its own subject cannot fail
// for a subject that did not exist when it was written.
func TestTheScopeScanCoversEveryProductionFile(t *testing.T) {
	t.Parallel()

	files := productionSources(t)
	for _, name := range []string{"routes.go", "errors.go", "guard.go", "deps.go"} {
		if _, ok := files[name]; !ok {
			t.Errorf("productionSources omits %s; it enumerated %v", name, slices.Sorted(maps.Keys(files)))
		}
	}
	for name := range files {
		if strings.HasSuffix(name, "_test.go") {
			t.Errorf("productionSources returned the test file %s", name)
		}
	}
}

// productionSources reads this package's compiled files. It is a directory read
// rather than a list, for the reason internal/identity's is.
func productionSources(t *testing.T) map[string][]byte {
	t.Helper()

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read the package directory: %v", err)
	}
	sources := map[string][]byte{}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		source, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		sources[name] = source
	}
	return sources
}

// scopeRule names one construction the scan refuses. They are a bitmask so a
// test can revert exactly one and observe that the construct it names is the
// only thing that rule reports.
type scopeRule uint

const (
	// ruleStoreLiteral refuses a sessionstore composite literal outside scope.
	ruleStoreLiteral scopeRule = 1 << iota
	// ruleTenantField refuses a keyed TenantID member outside scope, which is
	// the evasion of writing the request through a local alias type.
	ruleTenantField
	// ruleTenantAssign refuses an assignment to a .TenantID selector outside
	// scope, which is the evasion of building a zero request and filling it in.
	ruleTenantAssign

	allScopeRules = ruleStoreLiteral | ruleTenantField | ruleTenantAssign
)

func (r scopeRule) String() string {
	switch r {
	case ruleStoreLiteral:
		return "ruleStoreLiteral"
	case ruleTenantField:
		return "ruleTenantField"
	case ruleTenantAssign:
		return "ruleTenantAssign"
	default:
		return fmt.Sprintf("scopeRule(%d)", uint(r))
	}
}

type scopeReport struct {
	violations     []string
	storeLiterals  int
	scopedLiterals int
}

// scanTenantScope parses one file and reports every construction that could put
// a tenant into a SessionStore request without going through scope.
//
// It works on the PARSED file rather than on source text, for the reason
// import_boundary_test.go's rules do: a substring ban is defeated by a line
// break, a rename or an alias, and cannot tell code from a comment.
func scanTenantScope(name string, source []byte, enabled scopeRule) (scopeReport, error) {
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, name, source, parser.SkipObjectResolution)
	if err != nil {
		return scopeReport{}, err
	}
	var report scopeReport
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		exempt := isScopeMethod(function)
		ast.Inspect(function, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.CompositeLit:
				if isStoreType(typed.Type) {
					report.storeLiterals++
					if exempt {
						report.scopedLiterals++
					} else if enabled&ruleStoreLiteral != 0 {
						report.violations = append(report.violations,
							fmt.Sprintf("%s builds a SessionStore request outside a scope method", position(fileSet, typed.Pos())))
					}
				}
				if exempt || enabled&ruleTenantField == 0 {
					return true
				}
				for _, element := range typed.Elts {
					pair, ok := element.(*ast.KeyValueExpr)
					if !ok {
						continue
					}
					if key, ok := pair.Key.(*ast.Ident); ok && key.Name == "TenantID" {
						report.violations = append(report.violations,
							fmt.Sprintf("%s writes a TenantID member outside a scope method", position(fileSet, pair.Pos())))
					}
				}
			case *ast.AssignStmt:
				if exempt || enabled&ruleTenantAssign == 0 {
					return true
				}
				for _, target := range typed.Lhs {
					if selector, ok := target.(*ast.SelectorExpr); ok && selector.Sel.Name == "TenantID" {
						report.violations = append(report.violations,
							fmt.Sprintf("%s assigns a TenantID field outside a scope method", position(fileSet, selector.Pos())))
					}
				}
			}
			return true
		})
	}
	return report, nil
}

func position(fileSet *token.FileSet, pos token.Pos) string {
	return fileSet.Position(pos).String()
}

// isScopeMethod reports whether declaration is a method on scope, by value or
// by pointer.
func isScopeMethod(declaration *ast.FuncDecl) bool {
	if declaration.Recv == nil || len(declaration.Recv.List) != 1 {
		return false
	}
	receiver := declaration.Recv.List[0].Type
	if star, ok := receiver.(*ast.StarExpr); ok {
		receiver = star.X
	}
	identifier, ok := receiver.(*ast.Ident)
	return ok && identifier.Name == "scope"
}

// isStoreType reports whether the composite literal names a type from the
// sessionstore package.
func isStoreType(expression ast.Expr) bool {
	selector, ok := expression.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := selector.X.(*ast.Ident)
	return ok && pkg.Name == "sessionstore"
}

// ---------------------------------------------------------------------------
// Composition.
// ---------------------------------------------------------------------------

func TestNewRouterRefusesAnIncompleteComposition(t *testing.T) {
	t.Parallel()

	complete := func() RouterConfig {
		authenticator, err := internalidentity.NewAuthenticator(internalidentity.Config{Verifier: &fixtureVerifier{}})
		if err != nil {
			t.Fatalf("NewAuthenticator: %v", err)
		}
		guard, err := NewGuard(GuardConfig{
			CSRF: factoryidentity.CSRFConfig{
				SharedKey:      []byte("0123456789abcdef0123456789abcdef"),
				TokenTTL:       time.Hour,
				TrustedOrigins: []string{fixtureOrigin},
			},
			Credentials: authenticator,
			Clock:       fixedClock{},
		})
		if err != nil {
			t.Fatalf("NewGuard: %v", err)
		}
		return RouterConfig{
			Credentials: authenticator,
			Authorizer:  internalidentity.Authorizer{},
			Reads:       newFakeReader(),
			Guard:       guard,
			IDs:         &countingIDs{},
		}
	}
	if _, err := NewRouter(complete()); err != nil {
		t.Fatalf("a complete composition was refused: %v", err)
	}
	for name, remove := range map[string]func(*RouterConfig){
		"Credentials": func(c *RouterConfig) { c.Credentials = nil },
		"Authorizer":  func(c *RouterConfig) { c.Authorizer = nil },
		"Reads":       func(c *RouterConfig) { c.Reads = nil },
		"Guard":       func(c *RouterConfig) { c.Guard = nil },
		"IDs":         func(c *RouterConfig) { c.IDs = nil },
	} {
		cfg := complete()
		remove(&cfg)
		router, err := NewRouter(cfg)
		if !errors.Is(err, ErrInvalidRouterConfig) {
			t.Errorf("omitting %s = %v, want ErrInvalidRouterConfig", name, err)
		}
		if router != nil {
			t.Errorf("omitting %s returned a router as well as an error", name)
		}
	}
	for name, limits := range map[string]RouteLimits{
		"a zero body ceiling": {MaxRequestBytes: 0, RequestTimeout: time.Second},
		"a negative ceiling":  {MaxRequestBytes: -1, RequestTimeout: time.Second},
		"a zero timeout":      {MaxRequestBytes: 1, RequestTimeout: 0},
		"a negative timeout":  {MaxRequestBytes: 1, RequestTimeout: -time.Second},
	} {
		cfg := complete()
		cfg.Limits = limits
		if _, err := NewRouter(cfg); !errors.Is(err, ErrInvalidRouterConfig) {
			t.Errorf("%s = %v, want ErrInvalidRouterConfig", name, err)
		}
	}
}

// TestOmittedLimitsTakeTheDefaults keeps the zero RouteLimits a request for the
// defaults rather than a refusal, so a composition that names no limits still
// bounds its bodies.
func TestOmittedLimitsTakeTheDefaults(t *testing.T) {
	t.Parallel()

	if err := DefaultRouteLimits().Validate(); err != nil {
		t.Fatalf("the defaults do not validate: %v", err)
	}
	f := newFixture(t)
	if f.router.limits != DefaultRouteLimits() {
		t.Errorf("limits = %+v, want %+v", f.router.limits, DefaultRouteLimits())
	}
}

// ---------------------------------------------------------------------------
// Authorization.
// ---------------------------------------------------------------------------

// TestEveryRouteAsksForTheDecisionItsRuleNames gives the route table's auth and
// command columns a reader.
//
// Without it, `auth` and `command` are values nothing consults: a route
// downgraded from authControl to authSessionRead, or one whose command kind was
// copied from the route above it, would answer identically and survive. The
// sweep is over the WHOLE table, so a route added with the wrong rule is caught
// without anybody choosing to test that route.
func TestEveryRouteAsksForTheDecisionItsRuleNames(t *testing.T) {
	t.Parallel()

	for _, entry := range routeTable() {
		t.Run(entry.pattern, func(t *testing.T) {
			t.Parallel()

			authorizer := &recordingAuthorizer{}
			f := newFixture(t, withAuthorizer(authorizer))
			var body io.Reader
			if entry.body == bodyJSON {
				body = strings.NewReader(`{}`)
			}
			method := entry.methods[len(entry.methods)-1]
			f.serve(request(method, concreteTarget(entry.pattern), body))

			var want []authorizationCall
			switch entry.auth {
			case authAuthenticated:
			case authSessionList:
				want = []authorizationCall{{operation: "list"}}
			case authSessionRead:
				want = []authorizationCall{{operation: "read", session: fixtureSession}}
			case authControl:
				want = []authorizationCall{{operation: "control", session: fixtureSession, command: entry.command}}
			}
			if got := authorizer.snapshot(); !slices.Equal(got, want) {
				t.Errorf("%s %s asked for %+v, want %+v", method, entry.pattern, got, want)
			}
		})
	}
}

// TestEveryRouteRuleIsExercisedByTheTable floors the sweep above on the RULE
// set rather than on the route set: a rule nothing declares is a branch of
// Router.authorize no test drives.
func TestEveryRouteRuleIsExercisedByTheTable(t *testing.T) {
	t.Parallel()

	declared := map[authRule]bool{}
	commands := map[sessionstore.CommandKind]bool{}
	for _, entry := range routeTable() {
		declared[entry.auth] = true
		if entry.auth == authControl {
			commands[entry.command] = true
		}
	}
	for _, rule := range []authRule{authAuthenticated, authSessionList, authSessionRead, authControl} {
		if !declared[rule] {
			t.Errorf("no route declares rule %d, so Router.authorize has a branch nothing drives", rule)
		}
	}
	for _, command := range []sessionstore.CommandKind{commandInput, commandInterrupt, commandRestore, commandGateResponse} {
		if !commands[command] {
			t.Errorf("no route authorizes under command kind %q", command)
		}
	}
	// commandCreate belongs to POST /v1/sessions, whose rule is the tenant
	// list because the session it would create does not exist yet. It is
	// declared for A3.1 and deliberately has no route reader at this task.
	if commandCreate == "" {
		t.Error("commandCreate is empty")
	}
	// Both body rules must occur, or one of them is a constant nothing selects
	// and a route that lost its body requirement would be indistinguishable
	// from one that never had one.
	bodies := map[bodyRule]bool{}
	for _, entry := range routeTable() {
		bodies[entry.body] = true
	}
	if !bodies[bodyNone] || !bodies[bodyJSON] {
		t.Errorf("the table declares body rules %v, want both bodyNone and bodyJSON", bodies)
	}
}

// TestADenialIsAnsweredBeforeTheStoreIsTouched is the ordering the whole tenant
// boundary rests on: a caller who may not read this tenant must not be able to
// make Factory consult durable state on their behalf.
func TestADenialIsAnsweredBeforeTheStoreIsTouched(t *testing.T) {
	t.Parallel()

	authorizer := &recordingAuthorizer{deny: true}
	f := newFixture(t, withAuthorizer(authorizer))
	recorder := f.get("/v1/sessions/" + string(fixtureSession) + "/status")

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403; body %q", recorder.Code, recorder.Body)
	}
	if code := decodeEnvelope(t, recorder).Error.Code; code != ErrorCodeNotAuthorized {
		t.Errorf("code = %q, want %q", code, ErrorCodeNotAuthorized)
	}
	if requests, _ := f.reads.snapshot(); len(requests) != 0 {
		t.Errorf("a denied read reached the store as %+v", requests)
	}
}

// TestADenialIsAnsweredBeforeTheBodyIsRead is the other half of the same
// ordering. A caller who may not act on this tenant must not be able to make
// Factory read a megabyte on their behalf.
func TestADenialIsAnsweredBeforeTheBodyIsRead(t *testing.T) {
	t.Parallel()

	authorizer := &recordingAuthorizer{deny: true}
	f := newFixture(t, withAuthorizer(authorizer))
	body := &countingReader{remaining: 1 << 20}
	r := request(http.MethodPost, "/v1/sessions/"+string(fixtureSession)+"/input", body)
	if got := f.serve(r); got.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", got.Code)
	}
	if body.read != 0 {
		t.Errorf("a denied request had %d body bytes read", body.read)
	}
}

// countingReader reports how much of a body was consumed.
type countingReader struct {
	remaining int
	read      int
}

func (c *countingReader) Read(p []byte) (int, error) {
	if c.remaining == 0 {
		return 0, io.EOF
	}
	n := min(len(p), c.remaining)
	for i := range n {
		p[i] = 'x'
	}
	c.remaining -= n
	c.read += n
	return n, nil
}

// ---------------------------------------------------------------------------
// The body rule is keyed to the method, not to the route.
// ---------------------------------------------------------------------------

// TestASafeMethodOnABodiedRouteNeedsNoBody is what keeps GET /v1/sessions --
// the request every client makes first -- from answering 415 because the route
// it shares with create declares a body.
//
// It is also the reader for the linkage to guard.go's stateChanging: replacing
// that predicate with a constant true makes this fail, and replacing it with a
// constant false makes the empty-body and media-type rows of the failure sweep
// fail.
func TestASafeMethodOnABodiedRouteNeedsNoBody(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	bodied := 0
	for _, entry := range routeTable() {
		if entry.body != bodyJSON || !slices.Contains(entry.methods, http.MethodGet) {
			continue
		}
		bodied++
		r := httptest.NewRequest(http.MethodGet, fixtureOrigin+concreteTarget(entry.pattern), nil)
		r.Host = fixtureHost
		r.Header.Set("Authorization", "Bearer "+fixtureBearer)
		recorder := f.serve(r)
		if recorder.Code == http.StatusUnsupportedMediaType || recorder.Code == http.StatusBadRequest {
			t.Errorf("GET %s = %d; a safe method on a bodied route must not require a body", entry.pattern, recorder.Code)
		}
	}
	if bodied == 0 {
		t.Fatal("no route serves both a safe method and a bodied one, so this claim has no subject")
	}
}

// TestTheVersionSegmentIsMatchedWhole is the request-level half of the module's
// standing rule that containment compares whole segments.
func TestTheVersionSegmentIsMatchedWhole(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withUI())
	for path, wantAPI := range map[string]bool{
		"/v1":          true,
		"/v1/agents":   true,
		"/v1x":         false,
		"/v1x/agents":  false,
		"/av1/agents":  false,
		"/":            false,
		"/v2/agents":   false,
		"/api/v1/x":    false,
		"/v1-old/tick": false,
	} {
		r := httptest.NewRequest(http.MethodGet, fixtureOrigin+path, nil)
		r.Host = fixtureHost
		recorder := f.serve(r)
		reachedSPA := strings.Contains(recorder.Body.String(), spaMarker)
		if reachedSPA == wantAPI {
			t.Errorf("GET %s reached the SPA = %v, want it treated as an API path = %v", path, reachedSPA, wantAPI)
		}
	}
}
