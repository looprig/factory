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
	// records is the catalog record each held session projects. A session
	// registered without one gets the minimal record below, which is what the
	// routing tests want; the read tests register a whole one.
	records map[storedSession]sessionstore.CatalogRecord
	// journals is each session's durable journal, public and private records
	// alike. See fakeJournalRecord: the private ones are what "private bytes
	// never leave SessionStore" is asserted against, so they are HELD here and
	// the read below is what refuses to publish them.
	journals map[storedSession][]fakeJournalRecord
	// journalRequests and gateRequests record what the router asked for, so a
	// test can assert the position, the bound and the CALL COUNT rather than
	// only the answer.
	journalRequests []sessionstore.ReadPublicJournalRequest
	gateRequests    []sessionstore.ReadGatesRequest
	// absent produces the error the store answers a session it holds no
	// binding for with. It is a FUNCTION because the answer is layout
	// dependent -- see storeLayout -- and the layout is the axis the
	// "no such session" answer must be identical along.
	absent func() error
	// leak makes the fake publish private material through the public reads.
	// Nothing in production can turn it on: it is the control that shows the
	// private-material assertions can observe a failure at all.
	leak bool
	// requests records every catalog request the router built, so a test can
	// assert the tenant it was scoped by rather than only the answer.
	requests []sessionstore.GetCatalogEntryRequest
	// listRequests records every tenant page request, for the same reason.
	listRequests []sessionstore.ListSessionsRequest
	// pages is the page this fake answers a tenant list with, per tenant. A
	// tenant with no entry gets the zero page, which is the empty answer.
	pages map[sessionwire.TenantID]sessionstore.SessionPage
	// deadlines records whether each call's context carried one.
	deadlines []bool
	// block, when set, holds the call until the context ends and returns the
	// context's error, which is how the request deadline acquires a reader.
	block bool
	// fail, when set, replaces the answer for every call.
	fail error
	// journalFail and gateFail replace the answer for ONE kind of read.
	//
	// They exist because fail is global and resolveSession's catalog read comes
	// FIRST, so with fail alone no test can reach the failure branch of any
	// durable read a request makes after that one: the gate or journal read.
	// That is not a missing convenience -- it is the exact
	// structural hole the A2.1 absence defect lived in, and two mutations of
	// serveSessionGates's error branch survived the whole suite because of it.
	//
	// There is deliberately no catalogFail. A lever failing the catalog read
	// would be fail with extra steps, since the catalog read is the first one
	// and fail already stops the request there; it existed, was never assigned
	// by any test, and its presence made the comment above claim three levers
	// where two do the work.
	journalFail error
	gateFail    error
	// nextRecords is the record GetCatalogEntry answers with from its SECOND
	// call onward. A handler that re-read the catalog rather than using the
	// record the chain resolved renders THIS one, which is what makes the
	// difference between carrying and re-reading observable at all.
	nextRecords map[storedSession]sessionstore.CatalogRecord
	// catalogReads counts calls, so nextRecords can be applied from the second.
	catalogReads int
	// panics, when set, makes the call panic the way a faulty dependency
	// does. It is a real panic inside the handler goroutine, which is the only
	// thing Router's recovery can be driven by.
	panics any
}

func newFakeReader(sessions ...storedSession) *fakeReader {
	held := make(map[storedSession]bool, len(sessions))
	for _, s := range sessions {
		held[s] = true
	}
	return &fakeReader{
		sessions:    held,
		records:     map[storedSession]sessionstore.CatalogRecord{},
		nextRecords: map[storedSession]sessionstore.CatalogRecord{},
		journals:    map[storedSession][]fakeJournalRecord{},
		pages:       map[sessionwire.TenantID]sessionstore.SessionPage{},
		absent:      layoutMultiTenant.absence,
	}
}

func (f *fakeReader) GetCatalogEntry(ctx context.Context, req sessionstore.GetCatalogEntryRequest) (sessionstore.CatalogEntry, error) {
	key := storedSession{tenant: req.TenantID, session: req.SessionID}
	f.mu.Lock()
	f.requests = append(f.requests, req)
	f.catalogReads++
	reads := f.catalogReads
	_, hasDeadline := ctx.Deadline()
	f.deadlines = append(f.deadlines, hasDeadline)
	block, fail, panics, held := f.block, f.fail, f.panics, f.sessions[key]
	record, hasRecord := f.records[key]
	if successor, ok := f.nextRecords[key]; ok && reads > 1 {
		record, hasRecord = successor, true
	}
	f.mu.Unlock()

	if panics != nil {
		panic(panics)
	}
	if block {
		<-ctx.Done()
		return sessionstore.CatalogEntry{}, ctx.Err()
	}
	if fail != nil {
		return sessionstore.CatalogEntry{}, fail
	}
	if !held {
		return sessionstore.CatalogEntry{}, f.absence()
	}
	if !hasRecord {
		record = sessionstore.CatalogRecord{TenantID: req.TenantID, SessionID: req.SessionID}
	}
	return sessionstore.CatalogEntry{Record: record, Revision: 3}, nil
}

// absence is the store's "there is no such session" error under the layout this
// fake is standing in for.
func (f *fakeReader) absence() error {
	f.mu.Lock()
	produce := f.absent
	f.mu.Unlock()
	return produce()
}

// ListSessions answers the tenant's page from the fake's own catalogue.
//
// It records every request, so a test can assert the tenant the router scoped
// by, the cursor it forwarded and the limit it clamped -- and can assert HOW
// MANY times the store was asked, which is what "the bounded tenant page,
// once" means at this layer.
func (f *fakeReader) ListSessions(ctx context.Context, req sessionstore.ListSessionsRequest) (sessionstore.SessionPage, error) {
	f.mu.Lock()
	f.listRequests = append(f.listRequests, req)
	block, fail, panics := f.block, f.fail, f.panics
	page := f.pages[req.TenantID]
	f.mu.Unlock()

	// The store's limit rule, restated in reads_test.go and applied here for
	// the reason it is applied in fakeDirectory: a fake looser than the module
	// it stands in for is what lets a ceiling be raised past what the store
	// accepts with every test still green. Store.pageLimit refuses this before
	// it looks at anything else, so this does too.
	if refusePageLimit(req.Limit) {
		return sessionstore.SessionPage{}, &sessionstore.CatalogError{
			Code: sessionstore.CatalogErrorInvalid, Field: "limit",
		}
	}
	if panics != nil {
		panic(panics)
	}
	if block {
		<-ctx.Done()
		return sessionstore.SessionPage{}, ctx.Err()
	}
	if fail != nil {
		return sessionstore.SessionPage{}, fail
	}
	return page, nil
}

func (f *fakeReader) GetObjectMetadata(context.Context, sessionstore.GetObjectMetadataRequest) (sessionwire.ObjectMetadata, error) {
	return sessionwire.ObjectMetadata{}, errors.New("object metadata unavailable")
}
func (f *fakeReader) GetObject(context.Context, sessionstore.GetObjectRequest) (io.ReadCloser, error) {
	return nil, errors.New("unused by A2.1")
}

func (f *fakeReader) snapshot() ([]sessionstore.GetCatalogEntryRequest, []bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.requests), slices.Clone(f.deadlines)
}

func (f *fakeReader) listSnapshot() []sessionstore.ListSessionsRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.listRequests)
}

// countingIDs hands out predictable identifiers so a test can tell a minted one
// from a header the caller supplied.
type countingIDs struct {
	mu    sync.Mutex
	next  int
	fail  error
	empty bool
}

func (c *countingIDs) NewUUID() (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fail != nil {
		return "", c.fail
	}
	if c.empty {
		return "", nil
	}
	c.next++
	return fmt.Sprintf("00000000-0000-4000-8000-%012d", c.next), nil
}

type fixture struct {
	router  *Router
	reads   *fakeReader
	targets *fakeDirectory
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

// withVerifierTenant issues the fixture credential for a different tenant, so a
// test drives two principals through the SAME router shape rather than
// assembling a principal itself.
func withVerifierTenant(tenant sessionwire.TenantID) fixtureOption {
	return func(_ *RouterConfig, f *fixture) { f.verify.tenant = tenant }
}

func withVerifierFailure(err error) fixtureOption {
	return func(_ *RouterConfig, f *fixture) { f.verify.fail = err }
}

// withRawVerifier replaces the fixture's verifier entirely, for a test that
// needs one with behaviour rather than one with a canned answer. It rebuilds
// the authenticator and the guard around it so the composition stays the real
// one.
func withRawVerifier(verifier internalidentity.Verifier) fixtureOption {
	return func(cfg *RouterConfig, f *fixture) {
		authenticator, err := internalidentity.NewAuthenticator(internalidentity.Config{
			Verifier: verifier,
			Clock:    f.clock,
		})
		if err != nil {
			panic(err)
		}
		cfg.Credentials = authenticator
		f.authn = authenticator
		guard, err := NewGuard(GuardConfig{
			CSRF: factoryidentity.CSRFConfig{
				SharedKey:      []byte("0123456789abcdef0123456789abcdef"),
				TokenTTL:       time.Hour,
				TrustedOrigins: []string{fixtureOrigin},
			},
			Credentials: authenticator,
			Clock:       f.clock,
		})
		if err != nil {
			panic(err)
		}
		cfg.Guard = guard
		f.guard = guard
	}
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
		reads:   newFakeReader(storedSession{tenant: fixtureTenant, session: fixtureSession}),
		targets: newFakeDirectory(),
		ids:     &countingIDs{},
		guard:   guard,
		clock:   clock,
		authn:   authenticator,
		verify:  verifier,
		limits:  DefaultRouteLimits(),
	}
	cfg := RouterConfig{
		Credentials: authenticator,
		Authorizer:  internalidentity.Authorizer{},
		Reads:       f.reads,
		Directory:   f.targets,
		Guard:       guard,
		IDs:         f.ids,
		Limits:      f.limits,
	}
	for _, option := range options {
		option(&cfg, f)
	}
	cfg.Reads = f.reads
	cfg.Directory = f.targets
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

	// HEAD accompanies GET everywhere. RFC 9110 section 9.1 makes GET and HEAD
	// the two methods a general-purpose server MUST support and every other
	// method optional -- the same sentence that makes refusing OPTIONS
	// conformant, so it cannot be read in one direction only.
	read := []string{http.MethodGet, http.MethodHead}
	want := map[string][]string{
		"/v1/bootstrap":                             read,
		"/v1/agents":                                read,
		"/v1/capabilities":                          read,
		"/v1/sessions":                              {http.MethodGet, http.MethodHead, http.MethodPost},
		"/v1/sessions/{sid}/status":                 read,
		"/v1/sessions/{sid}/journal":                read,
		"/v1/sessions/{sid}/gates":                  read,
		"/v1/sessions/{sid}/input":                  {http.MethodPost},
		"/v1/sessions/{sid}/interrupt":              {http.MethodPost},
		"/v1/sessions/{sid}/restore":                {http.MethodPost},
		"/v1/sessions/{sid}/gates/{gid}":            {http.MethodPost},
		"/v1/sessions/{sid}/objects/{oid}":          read,
		"/v1/sessions/{sid}/objects/{oid}/metadata": read,
		"/v1/realtime":                              read,
		"/v1/csrf-token":                            read,
	}
	got := map[string][]string{}
	for _, route := range routeTable() {
		got[route.pattern] = route.methods()
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
			if slices.Contains(route.methods(), method) {
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
			if got := recorder.Header().Get("Allow"); got != strings.Join(route.methods(), ", ") {
				t.Errorf("%s %s: Allow = %q, want %q", method, target, got, strings.Join(route.methods(), ", "))
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

// TestTheUnimplementedMethodsAreExactlyTheOnesLaterTasksOwn gives the 501
// answers a reader. Every METHOD whose body a later runbook task fills in must
// say so in the table AND answer 501 today; every method this build implements
// must have no owner recorded and must not answer 501.
//
// It is per method rather than per route because /v1/sessions is now both: a
// tenant list this task serves and a create A3.1 owns. A route-level split
// could only call that route implemented -- letting the create's 501 go
// unmeasured -- or pending, failing on the list it does serve.
func TestTheUnimplementedMethodsAreExactlyTheOnesLaterTasksOwn(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withDepartment(pooledTemplate), withAdvertised(pooledTemplate.Key))
	implemented := 0
	pending := 0
	for _, route := range routeTable() {
		for _, rule := range route.rules {
			key := rule.method + " " + route.pattern
			recorder := f.driveRule(route, rule)
			if rule.owner == "" {
				implemented++
				if recorder.Code == http.StatusNotImplemented {
					t.Errorf("%s claims no later owner but answers 501", key)
				}
				if rule.handle == nil {
					t.Errorf("%s claims no later owner and declares no handler", key)
				}
				continue
			}
			pending++
			if rule.handle != nil {
				t.Errorf("%s is owned by %s and also declares a handler", key, rule.owner)
			}
			if recorder.Code != http.StatusNotImplemented {
				t.Errorf("%s is owned by %s but answered %d, want 501", key, rule.owner, recorder.Code)
				continue
			}
			if code := decodeEnvelope(t, recorder).Error.Code; code != ErrorCodeNotImplemented {
				t.Errorf("%s: code = %q, want %q", key, code, ErrorCodeNotImplemented)
			}
		}
	}
	if implemented == 0 {
		t.Fatal("no method is implemented at this task, so the 501 split is vacuous")
	}
	if pending == 0 {
		t.Fatal("no method is pending, so the owner column has no reader")
	}
}

// TestARouteMayBePartlyImplemented is the anti-vacuity check for the split
// above: if every route were wholly implemented or wholly pending, moving the
// owner onto the method would have bought nothing and a route-level column
// would still be correct.
func TestARouteMayBePartlyImplemented(t *testing.T) {
	t.Parallel()

	mixed := 0
	for _, route := range routeTable() {
		owned, served := 0, 0
		for _, rule := range route.rules {
			if rule.owner == "" {
				served++
				continue
			}
			owned++
		}
		if owned > 0 && served > 0 {
			mixed++
		}
	}
	if mixed == 0 {
		t.Fatal("no route serves an implemented method beside a pending one, so a per-route owner would still be sufficient")
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

// bodiedMethod reports a method on this route that carries a JSON body, or
// false when the route serves none.
func bodiedMethod(entry route) (string, bool) {
	for _, rule := range entry.rules {
		if rule.body == bodyJSON {
			return rule.method, true
		}
	}
	return "", false
}

// driveOnce sends one request exercising a route: its bodied method if it has
// one, otherwise its first method.
func (f *fixture) driveOnce(entry route) *httptest.ResponseRecorder {
	if method, ok := bodiedMethod(entry); ok {
		return f.serve(request(method, concreteTarget(entry.pattern), strings.NewReader(`{}`)))
	}
	return f.serve(request(entry.methods()[0], concreteTarget(entry.pattern), nil))
}

// driveRule sends one request exercising exactly ONE method of a route.
//
// It exists because owner and handle moved onto methodRule: /v1/sessions serves
// a list this task implements and a create A3.1 owns, so a probe that drove
// "the route" would answer for whichever method it happened to pick and would
// report the other's readiness as that one's.
func (f *fixture) driveRule(entry route, rule methodRule) *httptest.ResponseRecorder {
	var body io.Reader
	if rule.body == bodyJSON {
		body = strings.NewReader(`{}`)
	}
	return f.serve(request(rule.method, concreteTarget(entry.pattern), body))
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

// aPendingReadTarget is a concrete path for some GET a later runbook task still
// owns. It panics when there is none, because at that point every case built on
// it is testing nothing and the caller must be told rather than passed an empty
// string.
func aPendingReadTarget() string {
	for _, route := range routeTable() {
		for _, rule := range route.rules {
			if rule.method == http.MethodGet && rule.owner != "" {
				return concreteTarget(route.pattern)
			}
		}
	}
	panic("no route serves a pending GET, so the not-implemented case has no subject")
}

func TestAPendingReadTargetIsReallyPending(t *testing.T) {
	t.Parallel()

	target := aPendingReadTarget()
	f := newFixture(t)
	if recorder := f.get(target); recorder.Code != http.StatusNotImplemented {
		t.Fatalf("%s answered %d, want 501", target, recorder.Code)
	}
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
			// A route a LATER task owns, DERIVED from the table rather than
			// named. It was /v1/agents until A2.2 served one and the session
			// status until A2.3 did; a case pinned to a route that becomes
			// implemented does not fail, it silently stops testing the
			// condition it names. Deriving it means the case moves itself, and
			// the day nothing is pending it fails loudly instead.
			build: func(*fixture) *http.Request {
				return request(http.MethodGet, aPendingReadTarget(), nil)
			},
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
		// A trailing slash IS unclean here, unlike in net/http's own
		// cleanPath: this router registers no "/tree/" pattern, so a trailing
		// slash on an API path can only name a route that does not exist.
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

// TestAnUncleanAPIPathIsRefusedBeforeAuthentication pins where the refusal
// sits, which is what makes the trailing-slash rule observable.
//
// The refusal happens in the router's own dispatch, ABOVE the API chain, so no
// credential is verified and no route is consulted for a path that cannot name
// one. An unauthenticated caller therefore gets 404 for an unclean path and 401
// for a clean one, and the answer depends only on the shape of the path they
// sent, which they already know.
//
// It is also the reader for cleanRequestPath NOT restoring a trailing slash the
// way net/http's cleanPath does: with the restoration, /v1/agents/ is clean, so
// it reaches authentication and this row answers 401 instead.
func TestAnUncleanAPIPathIsRefusedBeforeAuthentication(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	anonymous := func(target string) int {
		r := httptest.NewRequest(http.MethodGet, fixtureOrigin+target, nil)
		r.Host = fixtureHost
		return f.serve(r).Code
	}
	for _, target := range []string{"/v1/agents/", "/v1//agents", "/v1/../assets/app.js", "/v1/sessions/../agents"} {
		if got := anonymous(target); got != http.StatusNotFound {
			t.Errorf("anonymous GET %s = %d, want 404: an unclean path must be refused above authentication", target, got)
		}
	}
	// The control: a CLEAN path that names no route still reaches
	// authentication, so it answers 401. Without this row a router that
	// answered 404 to everything anonymous would pass.
	if got := anonymous("/v1/nothing-here"); got != http.StatusUnauthorized {
		t.Errorf("anonymous GET /v1/nothing-here = %d, want 401", got)
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

// The security header table, spelled once.
//
// It was spelled three times -- in the API sweep, in the SPA test and in the
// middleware unit -- and the union of the two halves was asserted by a floor
// reading len(neutral)+len(apiOnly) != 5. That floor's real subject was
// CARDINALITY, not membership: it fires on a coordinated four-site deletion,
// but a header ADDED to setNeutralSecurityHeaders alone left every copy and the
// floor itself green, because nothing compared the production functions against
// a list. Both halves are now one table, every reader derives from it, and the
// middleware unit compares the functions' EMITTED KEY SETS against it exactly,
// so an addition in production with no entry here is reported by name.
var (
	// neutralHeaders go on every response the router mounts, the SPA included:
	// a document's own policy has nothing to say about MIME sniffing, referrer
	// leakage or framing.
	neutralHeaders = map[string]string{
		"X-Content-Type-Options": "nosniff",
		"Referrer-Policy":        "no-referrer",
		"X-Frame-Options":        "DENY",
	}
	// apiOnlyHeaders go only on responses the router writes itself. No policy
	// this package could write suits a document it has never seen, and no-store
	// on a content-hashed asset re-downloads the whole bundle every load.
	apiOnlyHeaders = map[string]string{
		"Cache-Control":           "no-store",
		"Content-Security-Policy": "default-src 'none'; frame-ancestors 'none'",
	}
)

// apiResponseHeaders is the union: what an API response carries. It is built
// from the two halves rather than listed, so "the two sets together are what an
// API response carries" holds by construction instead of by a count.
func apiResponseHeaders() map[string]string {
	all := make(map[string]string, len(neutralHeaders)+len(apiOnlyHeaders))
	maps.Copy(all, neutralHeaders)
	maps.Copy(all, apiOnlyHeaders)
	return all
}

// requiredSecurityHeaders is an INDEPENDENT restatement: each header a public
// authenticated surface must carry, and the threat it answers.
//
// It exists because the table above cannot floor itself. Deleting a header from
// setNeutralSecurityHeaders and from neutralHeaders together leaves every
// derived reader agreeing about a smaller world -- measured: that coordinated
// deletion survives the whole suite without this. The cardinality floor it
// replaces did catch that case, and caught it as "want 5"; this catches it by
// NAME, and makes deleting a header cost the deletion of a written threat
// rather than the decrement of a number.
//
// It holds no values, so the table above stays the single authority for what
// each header is set TO. This says only which must exist, and why.
func requiredSecurityHeaders() map[string]string {
	return map[string]string{
		"X-Content-Type-Options": "a browser that MIME-sniffs a served asset can be made to execute it",
		"Referrer-Policy": "the console URL carries a session identifier, and without this it travels " +
			"in the Referer of every outbound navigation and subresource",
		"X-Frame-Options": "the console approves gates and interrupts sessions, so framing it is a " +
			"clickjacking attack on those controls",
		"Cache-Control": "an API response is a caller's private session data, which a shared cache " +
			"must not serve to whoever asks next",
		"Content-Security-Policy": "a JSON error rendered inside an attacker's frame is still a document",
	}
}

// TestTheHeaderTableCarriesEveryHeaderTheThreatsRequire is that floor.
func TestTheHeaderTableCarriesEveryHeaderTheThreatsRequire(t *testing.T) {
	t.Parallel()

	required := requiredSecurityHeaders()
	if len(required) == 0 {
		t.Fatal("no header is required, so this floor is vacuous")
	}
	carried := apiResponseHeaders()
	for name, threat := range required {
		if threat == "" {
			t.Errorf("%s is required with no threat written down", name)
		}
		if _, ok := carried[name]; !ok {
			t.Errorf("no API response carries %s, which is required because %s", name, threat)
		}
	}
	// The other direction: a header in the table with no threat behind it is a
	// header nobody can say why we send.
	for name := range carried {
		if _, ok := required[name]; !ok {
			t.Errorf("the table declares %s, which requiredSecurityHeaders does not justify", name)
		}
	}
}

// headerNames is the key set of a header table, for an exact comparison against
// what a production function emits.
func headerNames(table map[string]string) []string {
	return slices.Sorted(maps.Keys(table))
}

// emittedHeaderNames is the key set a recorder actually carries.
func emittedHeaderNames(recorder *httptest.ResponseRecorder) []string {
	return slices.Sorted(maps.Keys(recorder.Header()))
}

// TestSecurityHeadersAreOnEveryAPIResponse sweeps the success answer and a
// failure answer, because a header set only on the error path is a header the
// responses that carry private data do not have.
//
// # What A2.2 added, and what it did and did not buy
//
// Until this task every response the router produced was a FAILURE, so the
// sweep could not tell a header the middleware sets from one writeAPIError
// sets: deleting nosniff or no-store from the middleware changed nothing any
// route could observe. A2.2 serves three 200s and a 304, and they are swept
// here with a floor requiring a success among them, so a header missing from
// the responses that actually carry data is now reported.
//
// It separates ALL FIVE, and the 304 leg is what makes that true, so that leg
// is load-bearing rather than a thoroughness flourish.
//
// Three of the five -- Referrer-Policy, X-Frame-Options and
// Content-Security-Policy -- are set only by the middleware, so any leg here
// separates them. The other two do not follow from the 200 and 404 legs at all:
// nosniff and Cache-Control are set by writeJSONBytes as well, deliberately,
// because it is the single write path for every JSON body this package produces
// and a response must not acquire a different header set by being written
// somewhere else. On a 200 or a 404 the second writer supplies them, so
// deleting either from the middleware alone changes nothing those legs can see.
//
// A 304 is written by NEITHER writeAPIError nor writeJSONBytes -- it is
// WriteHeader and nothing else -- so on that one response the middleware is the
// sole source of every header in the table. Measured, each mutation sole and
// compiling: deleting nosniff from setNeutralSecurityHeaders fails here on the
// 304, deleting Cache-Control from apiSecurityHeaders fails here, and deleting
// Referrer-Policy fails here.
//
// TestSecurityHeadersAreSetByTheMiddlewareItself is not made redundant by that.
// It asserts each function's EXACT emitted key set, which catches a header
// added to production with no entry in the table, and it is the reader for the
// SPA and non-API paths, which no leg of this sweep reaches.
func TestSecurityHeadersAreOnEveryAPIResponse(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withDepartment(dedicatedTemplate))
	want := apiResponseHeaders()
	if len(want) == 0 {
		t.Fatal("the header table is empty, so this sweep proves nothing")
	}
	// The last four are answered by ServeHTTP's own dispatch, BEFORE the API
	// chain. The headers were once set inside that chain, so every one of these
	// returned 404 with Referrer-Policy, X-Frame-Options and
	// Content-Security-Policy empty -- an unclean path is exactly the request
	// an attacker chooses, so the gap was on the path that mattered most.
	targets := []string{
		"/v1/csrf-token", "/v1/agents", "/v1/capabilities", "/v1/sessions", "/v1/nothing-here",
		"/v1/../assets/app.js", "/v1//agents", "/v1/agents/", "/assets/app.js",
	}
	statuses := map[int]bool{}
	for _, target := range targets {
		recorder := f.get(target)
		statuses[recorder.Code] = true
		for name, value := range want {
			if got := recorder.Header().Get(name); got != value {
				t.Errorf("GET %s (%d): %s = %q, want %q", target, recorder.Code, name, got, value)
			}
		}
	}
	// A 304 is written by neither writeAPIError nor writeJSONBytes, so it is
	// the response on which the middleware is the ONLY source of every header
	// in the table.
	conditional := request(http.MethodGet, "/v1/agents", nil)
	conditional.Header.Set("If-None-Match", "*")
	notModified := f.serve(conditional)
	statuses[notModified.Code] = true
	for name, value := range want {
		if got := notModified.Header().Get(name); got != value {
			t.Errorf("a 304 carries %s = %q, want %q", name, got, value)
		}
	}
	// The floor: a sweep of failures alone is what this test used to be, and
	// it could not see a header missing from a body that carries data.
	if !statuses[http.StatusOK] {
		t.Error("no swept response was a 200, so this sweep still cannot see a header missing from a success")
	}
	if !statuses[http.StatusNotModified] {
		t.Error("no swept response was a 304, so no response here is written by the middleware alone")
	}
	if len(statuses) < 3 {
		t.Errorf("the sweep saw only the statuses %v", slices.Sorted(maps.Keys(statuses)))
	}
}

// TestTheSPAIsGivenTheNeutralHeadersAndNotTheAPIOnlyOnes is the other side of
// the same header decision, and it fails in BOTH directions.
//
// The earlier version asserted only that Content-Security-Policy was absent. It
// could therefore fail one way, and the way it could not fail was the one that
// mattered: the SPA was served with NO security headers at all -- no nosniff,
// no Referrer-Policy, no X-Frame-Options -- while a comment justified the
// omission with an argument about CSP alone. One header's reason had been
// applied to five, and a test that pins only the true half is what let that
// stand.
//
// So both halves are asserted here. The neutral headers must be PRESENT,
// because a document's own policy has nothing to say about MIME sniffing,
// referrer leakage or framing. The API-only ones must be ABSENT, each for its
// own reason: default-src 'none' would forbid the bundle its own scripts, and
// no-store on a content-hashed asset re-downloads the whole bundle every load.
func TestTheSPAIsGivenTheNeutralHeadersAndNotTheAPIOnlyOnes(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withUI())
	for _, target := range []string{"/assets/app.js", "/index.html", "/sessions/session-a"} {
		recorder := f.serve(httptest.NewRequest(http.MethodGet, fixtureOrigin+target, nil))
		if !strings.Contains(recorder.Body.String(), spaMarker) {
			t.Fatalf("GET %s did not reach the SPA: %d %q", target, recorder.Code, recorder.Body)
		}
		for name, value := range neutralHeaders {
			if got := recorder.Header().Get(name); got != value {
				t.Errorf("GET %s: the SPA was served %s = %q, want %q; this header cannot conflict with any document",
					target, name, got, value)
			}
		}
		for name := range apiOnlyHeaders {
			if got := recorder.Header().Get(name); got != "" {
				t.Errorf("GET %s: the SPA was served the API-only %s = %q", target, name, got)
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
// the moment A2.2 writes a success body of its own. The mechanisms are the
// durable thing, so they are asserted as ones: a writer nothing has touched
// must come out of them carrying the full set, split exactly as the two
// functions declare it.
func TestSecurityHeadersAreSetByTheMiddlewareItself(t *testing.T) {
	t.Parallel()

	// Each function's EMITTED key set is compared against its half of the
	// table, exactly. Checking only that the listed headers are present would
	// leave a header added to production with no entry here unreported, which
	// is what the old cardinality floor could not see; an exact comparison
	// reports it, and reports it by name.
	bare := httptest.NewRecorder()
	setNeutralSecurityHeaders(bare.Header())
	if got, want := emittedHeaderNames(bare), headerNames(neutralHeaders); !slices.Equal(got, want) {
		t.Errorf("setNeutralSecurityHeaders emits %v, the table declares %v", got, want)
	}
	for name, value := range neutralHeaders {
		if got := bare.Header().Get(name); got != value {
			t.Errorf("setNeutralSecurityHeaders: %s = %q, want %q", name, got, value)
		}
	}

	recorder := httptest.NewRecorder()
	apiSecurityHeaders(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/anything", nil))
	if got, want := emittedHeaderNames(recorder), headerNames(apiOnlyHeaders); !slices.Equal(got, want) {
		t.Errorf("apiSecurityHeaders emits %v, the table declares %v; one header, one place", got, want)
	}
	for name, value := range apiOnlyHeaders {
		if got := recorder.Header().Get(name); got != value {
			t.Errorf("apiSecurityHeaders: %s = %q, want %q", name, got, value)
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
//
// Both ways a source can decline are driven: an error, and an EMPTY identifier
// returned with no error. They are separate cases because an empty header value
// is not the same thing as an absent one -- Header().Set writes the field, and
// a caller quoting "" in a support request has quoted something. UUIDSource
// promises no non-empty result, so the value is checked and not only the error.
func TestAnIdentifierSourceFailureDoesNotFailTheRequest(t *testing.T) {
	t.Parallel()

	for name, decline := range map[string]func(*countingIDs){
		"an error":                      func(c *countingIDs) { c.fail = errors.New("no entropy") },
		"an empty identifier, no error": func(c *countingIDs) { c.empty = true },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			decline(f.ids)
			recorder := f.get("/v1/csrf-token")
			if recorder.Code != http.StatusOK {
				t.Errorf("status = %d, want 200: a missing request identifier must not fail the request", recorder.Code)
			}
			if _, present := recorder.Header()[RequestIDHeader]; present {
				t.Errorf("%s is present as %q, want the field absent rather than empty",
					RequestIDHeader, recorder.Header().Get(RequestIDHeader))
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Panic recovery.
// ---------------------------------------------------------------------------

// TestAPanicBecomesAJSONInternalError drives a real panic through the real
// chain by making the store panic, which is the shape a production fault takes:
// a dependency, not the router.
//
// The first version of this test did not panic at all. It set an error whose
// Error method panicked, which never ran -- errors.Is and errors.As compare and
// unwrap without formatting -- so the request took the ordinary unknown-error
// path, answered 500 for that reason, and the deferred recovery in ServeHTTP
// was deletable with the suite still green. The fake now panics inside
// GetCatalogEntry, and TestDeletingTheRecoveryLosesTheEnvelope below is the
// half that shows what is lost without it.
func TestAPanicBecomesAJSONInternalError(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withUI())
	f.reads.panics = "deliberate panic from a dependency"
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
	// The store really panicked. Without this the test would pass against a
	// router that answered 500 for some other reason, which is exactly how the
	// first version of it passed while asserting nothing about recovery.
	if requests, _ := f.reads.snapshot(); len(requests) != 1 {
		t.Fatalf("the store recorded %d calls, so the panic did not come from where this test says", len(requests))
	}
}

// TestDeletingTheRecoveryLosesTheEnvelope drives the same panic WITHOUT the
// router's recovery, and asserts the thing that would then be true: no status,
// no body, no content type. net/http's own recovery closes the connection and
// writes nothing, so a panicking A2.2 handler would falsify both "every public
// failure is one Core ErrorEnvelope" and "an API failure never falls through to
// something a browser renders".
func TestDeletingTheRecoveryLosesTheEnvelope(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.reads.panics = "deliberate panic from a dependency"
	recorder := httptest.NewRecorder()
	recorder.Code = 0

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		// The router's own chain, minus the deferred recoverPanic that
		// ServeHTTP installs.
		f.router.own.ServeHTTP(&recordingWriter{ResponseWriter: recorder},
			request(http.MethodGet, "/v1/sessions/"+string(fixtureSession)+"/status", nil))
	}()

	if recovered == nil {
		t.Fatal("the panic did not escape the chain, so this test is not measuring what the recovery catches")
	}
	if recorder.Code != 0 {
		t.Errorf("a status %d was written without the recovery", recorder.Code)
	}
	if recorder.Body.Len() != 0 {
		t.Errorf("a body %q was written without the recovery", recorder.Body)
	}
}

// TestARecoveredPanicAfterAPartialWriteDoesNotAppendAnEnvelope is the branch a
// recovery middleware usually gets wrong. Once bytes are on the wire the status
// is already sent, so writing a 500 envelope produces a body that is neither
// the handler's nor an error -- a client parsing it sees corruption. The
// recovery must re-panic with http.ErrAbortHandler, which net/http answers by
// closing the connection.
func TestARecoveredPanicAfterAPartialWriteDoesNotAppendAnEnvelope(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	writer := &recordingWriter{ResponseWriter: recorder}
	partial := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"partial":`)
		panic("after the write")
	})

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		func() {
			defer recoverPanic(writer)
			partial.ServeHTTP(writer, request(http.MethodGet, "/v1/agents", nil))
		}()
	}()

	// The assertion is guarded rather than written as a bare type assertion.
	// recovered is nil exactly when the mechanism under test has been removed,
	// and an unguarded recovered.(error) would then kill this test by PANIC --
	// which is not an assertion kill and would be reported as a false success
	// by any mutation run.
	if recovered == nil {
		t.Fatal("nothing re-panicked, so the recovery answered after a partial write")
	}
	err, ok := recovered.(error)
	if !ok || !errors.Is(err, http.ErrAbortHandler) {
		t.Fatalf("recovered %v (%T), want http.ErrAbortHandler so the connection is closed", recovered, recovered)
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

// Object pages verify before emission, so their durable work has a deadline.
func TestTheObjectRouteCarriesAVerificationDeadline(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withLimits(RouteLimits{MaxRequestBytes: 1 << 20, RequestTimeout: 20 * time.Millisecond}))
	recorder := f.get("/v1/sessions/" + string(fixtureSession) + "/objects/object-a")
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body %q", recorder.Code, recorder.Body)
	}
	_, deadlines := f.reads.snapshot()
	if len(deadlines) != 1 {
		t.Fatalf("the store was called %d times, want 1", len(deadlines))
	}
	if !deadlines[0] {
		t.Error("the object route did not bound whole-object verification")
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

	f := newFixture(t, withSessions(),
		withRecord(fixtureTenant, fixtureSession, coldRecord(sessionwire.SessionStateIdle, sessionwire.SessionResidencyCold)))
	recorder := f.get("/v1/sessions/" + string(fixtureSession) + "/status")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: the session resolved and the status is served; got %q", recorder.Code, recorder.Body)
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
		f.driveOnce(route)
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
	for _, name := range []string{"routes.go", "reads.go", "sessions.go", "errors.go", "guard.go", "deps.go"} {
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
			Directory:   newFakeDirectory(),
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
		"Directory":   func(c *RouterConfig) { c.Directory = nil },
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

// TestEveryMethodAsksForTheDecisionItsRuleNames gives the table's auth and
// command columns a reader, per METHOD rather than per route.
//
// Per method is the whole point. When the columns sat on the route, GET and
// POST on /v1/sessions shared one rule and the sweep drove one of them, so a
// create authorized as a list was invisible. Now every (route, method) pair is
// driven and every one must ask for exactly the decision it declares -- a route
// downgraded from authControl to authSessionRead, or one whose command kind was
// copied from the route above it, is reported without anybody choosing to test
// that pair.
func TestEveryMethodAsksForTheDecisionItsRuleNames(t *testing.T) {
	t.Parallel()

	driven := 0
	for _, entry := range routeTable() {
		for _, rule := range entry.rules {
			driven++
			t.Run(rule.method+" "+entry.pattern, func(t *testing.T) {
				t.Parallel()

				authorizer := &recordingAuthorizer{}
				f := newFixture(t, withAuthorizer(authorizer))
				var body io.Reader
				if rule.body == bodyJSON {
					body = strings.NewReader(`{}`)
				}
				f.serve(request(rule.method, concreteTarget(entry.pattern), body))

				// A route that names no {sid} passes the zero SessionID,
				// which is what AuthorizeControl's blank parameter makes
				// harmless -- and is why POST /v1/sessions can be a control
				// decision at all.
				var session sessionwire.SessionID
				if entry.session {
					session = fixtureSession
				}
				var want []authorizationCall
				switch rule.auth {
				case authAuthenticated:
				case authSessionList:
					want = []authorizationCall{{operation: "list"}}
				case authSessionRead:
					want = []authorizationCall{{operation: "read", session: session}}
				case authObjectRead:
					want = []authorizationCall{{operation: "object", session: session}}
				case authControl:
					want = []authorizationCall{{operation: "control", session: session, command: rule.command}}
				}
				if got := authorizer.snapshot(); !slices.Equal(got, want) {
					t.Errorf("%s %s asked for %+v, want %+v", rule.method, entry.pattern, got, want)
				}
			})
		}
	}
	if driven == 0 {
		t.Fatal("the table declares no method rules, so this sweep proves nothing")
	}
}

// expectation is an INDEPENDENT restatement of what one (method, route) must
// declare, written from the specification and from the shape of the operation
// rather than from routeTable.
//
// It exists because three of the table's columns had no observer of their own.
// Measured before it: dropping body: bodyJSON from /v1/sessions/{sid}/input,
// and dropping streams from /v1/realtime, each left the entire suite green --
// the media-type and ceiling tests all drove /v1/sessions, and the streams flag
// was read only through the objects route. A column nothing restates is a
// column the table is its own authority for.
type expectation struct {
	auth    authRule
	command sessionstore.CommandKind
	body    bodyRule
	session bool
	streams bool
	// implemented is true for a route this build serves a handler for. It is
	// the same fact as an empty owner, stated from the other side.
	implemented bool
}

// expectedRoutes is that restatement, keyed "METHOD /path".
//
// Reading it as a reviewer: a state-changing operation is authControl under its
// own command kind and carries a JSON body; a durable read within a tenant is
// authSessionRead and resolves its session; the tenant's own list is
// authSessionList; a route describing the deployment rather than a tenant's
// data is authAuthenticated; and streaming is declared only where cutting the
// response at a deadline would truncate it.
func expectedRoutes() map[string]expectation {
	read := func(auth authRule, session bool) expectation {
		return expectation{auth: auth, session: session, implemented: true}
	}
	control := func(command sessionstore.CommandKind, session bool) expectation {
		return expectation{auth: authControl, command: command, body: bodyJSON, session: session}
	}
	routes := map[string]expectation{
		// The caller's authenticated tenant identity, with no caller-selected
		// resource and no durable read.
		"GET /v1/bootstrap": expectation{auth: authAuthenticated, implemented: true},
		// Deployment-wide descriptions: no tenant data, so authentication is
		// the whole decision, and this build serves both. They are the SAME
		// aggregate under two paths -- /v1/capabilities is the migration
		// spelling section 8.1 keeps -- so they must not differ in any column.
		"GET /v1/agents":       expectation{auth: authAuthenticated, implemented: true},
		"GET /v1/capabilities": expectation{auth: authAuthenticated, implemented: true},
		// A WebSocket is held open for the life of the connection, so a
		// handler deadline would close it on a timer.
		"GET /v1/realtime": expectation{auth: authAuthenticated, streams: true},
		// The caller's own token, served by this build.
		"GET /v1/csrf-token": expectation{auth: authAuthenticated, implemented: true},
		// The tenant's own list, and the create that adds to it. The create is
		// a state-changing command and is authorized as one: AuthorizeControl
		// never reads the SessionID, so a session that does not exist yet is no
		// reason to fall back to the list rule.
		"GET /v1/sessions":  expectation{auth: authSessionList, implemented: true},
		"POST /v1/sessions": control(commandCreate, false),
		// Durable reads within one session, all three served by this build.
		// They are replay-free projections of durable state, so each remains
		// answerable while every Host is stopped.
		"GET /v1/sessions/{sid}/status":  read(authSessionRead, true),
		"GET /v1/sessions/{sid}/journal": read(authSessionRead, true),
		"GET /v1/sessions/{sid}/gates":   read(authSessionRead, true),
		// Object decisions precede catalog resolution and trusted reference
		// policy precedes metadata or bytes. Verification has a deadline.
		"GET /v1/sessions/{sid}/objects/{oid}": expectation{
			auth: authObjectRead, session: true, implemented: true,
		},
		"GET /v1/sessions/{sid}/objects/{oid}/metadata": read(authObjectRead, true),
		// State-changing commands on an existing session.
		"POST /v1/sessions/{sid}/input":       control(commandInput, true),
		"POST /v1/sessions/{sid}/interrupt":   control(commandInterrupt, true),
		"POST /v1/sessions/{sid}/restore":     control(commandRestore, true),
		"POST /v1/sessions/{sid}/gates/{gid}": control(commandGateResponse, true),
	}
	// HEAD is GET's rule exactly. RFC 9110 section 9.1 makes GET and HEAD the
	// two methods a general-purpose server MUST support, which is the same
	// sentence that makes refusing OPTIONS conformant.
	for key, want := range routes {
		method, path, _ := strings.Cut(key, " ")
		if method == http.MethodGet {
			routes[http.MethodHead+" "+path] = want
		}
	}
	return routes
}

// TestEveryRouteDeclaresWhatItsShapeRequires compares the table against that
// restatement in BOTH directions: every declared method rule must be expected,
// and every expectation must be declared.
func TestEveryRouteDeclaresWhatItsShapeRequires(t *testing.T) {
	t.Parallel()

	want := expectedRoutes()
	if len(want) == 0 {
		t.Fatal("nothing is expected, so this comparison is vacuous")
	}
	seen := map[string]bool{}
	for _, entry := range routeTable() {
		for _, rule := range entry.rules {
			key := rule.method + " " + entry.pattern
			seen[key] = true
			expected, ok := want[key]
			if !ok {
				t.Errorf("the table serves %s, which the restatement does not expect", key)
				continue
			}
			got := expectation{
				auth: rule.auth, command: rule.command, body: rule.body,
				session: entry.session, streams: entry.streams,
				implemented: rule.owner == "",
			}
			if got != expected {
				t.Errorf("%s declares %+v, the restatement requires %+v", key, got, expected)
			}
		}
	}
	for key := range want {
		if !seen[key] {
			t.Errorf("the restatement expects %s, which the table does not serve", key)
		}
	}
}

// TestTheRestatementIsIndependentOfTheTable is the anti-vacuity check for the
// comparison above: a restatement derived FROM the table could not disagree
// with it. Each perturbation below must be reported.
func TestTheRestatementIsIndependentOfTheTable(t *testing.T) {
	t.Parallel()

	base := expectedRoutes()
	for name, key := range map[string]string{
		"a control route downgraded to a read": "POST /v1/sessions/{sid}/input",
		"the create downgraded to a list":      "POST /v1/sessions",
	} {
		want := base[key]
		if want.auth != authControl {
			t.Fatalf("%s: %s is not expected to be a control route, so this probe is wrong", name, key)
		}
	}
	if base["GET /v1/realtime"].streams != true {
		t.Error("the restatement does not require /v1/realtime to stream, so dropping the flag is unreported")
	}
	if base["POST /v1/sessions/{sid}/input"].body != bodyJSON {
		t.Error("the restatement does not require a body on input, so dropping it is unreported")
	}
	if base["GET /v1/sessions"].body != bodyNone {
		t.Error("the restatement requires a body on the tenant list, which would refuse every first request")
	}
	if _, ok := base["HEAD /v1/agents"]; !ok {
		t.Error("HEAD is not expected anywhere, so refusing it would be unreported")
	}
	// The restatement carries the per-method readiness independently of the
	// table. Without this, moving owner onto methodRule would have added a
	// column the restatement could not disagree about.
	if !base["GET /v1/sessions"].implemented {
		t.Error("the restatement does not expect the tenant list to be served, so withdrawing its handler is unreported")
	}
	if base["POST /v1/sessions"].implemented {
		t.Error("the restatement expects the session create to be served, which A3.1 owns")
	}
	if base["GET /v1/agents"] != base["GET /v1/capabilities"] {
		t.Error("the restatement lets /v1/capabilities differ from /v1/agents, which are one aggregate under two paths")
	}
}

// sanctionedImplementedMethods names every (method, route) this build serves a
// handler for, with the reason its authorization rule is adequate.
//
// It is the tripwire for the deferred-route seam. Every method in routeTable
// carries the task that fills its body in and answers 501 until then, and
// clearing that owner is a one-word edit in a table -- so nothing stopped a
// later task shipping a handler on whatever authorization rule the placeholder
// happened to inherit. Now clearing an owner fails the suite until the method
// is named HERE, which forces the rule to be re-read by somebody writing down
// why it is adequate.
//
// It is keyed by METHOD and route together because a route can serve one method
// this task implements and another a later task owns; a route-level key would
// have sanctioned the create along with the list.
func sanctionedImplementedMethods() map[string]string {
	// HEAD is GET's rule exactly, so its sanction is GET's sanction. Writing
	// it twice would be two places for one argument to be revised in one.
	sanctioned := map[string]string{
		"GET /v1/sessions/{sid}/objects/{oid}":          "requires an object decision before catalog lookup and trusted committed-reference policy before metadata/body; frozen catalog binding selects the reader; success follows whole-object EOF and Close",
		"GET /v1/sessions/{sid}/objects/{oid}/metadata": "requires the same object and committed-reference decisions as bytes; exposes only Core metadata, which is not current blob-presence proof",
		"GET /v1/bootstrap": "serves the CALLER their own bounded tenant identity under " +
			"authAuthenticated. The value comes only from the Principal the credential verifier " +
			"placed in the operation context; the route accepts no query parameters, has no path " +
			"parameter and does not read a request body, so there is no caller-selected resource " +
			"for a stronger authorization rule to protect",
		"GET /v1/csrf-token": "serves the CALLER their own CSRF token under authAuthenticated; " +
			"the token is bound to the requesting principal by Guard.IssueToken, so there is " +
			"no tenant-scoped resource for a stronger rule to protect",
		"GET /v1/agents": "serves the DEPLOYMENT's launchable agent set under authAuthenticated. " +
			"Its two inputs have no tenant dimension: the configured Department is deployment " +
			"configuration, and SessionStore's Host target directory is deliberately not " +
			"partitioned by tenant because a pooled target may serve several tenants -- so " +
			"ListCompatibleHostsRequest has no TenantID member for a scope to fill. Authorizer " +
			"declares no decision covering it because there is no tenant-scoped resource for one " +
			"to protect. The response is a pure function of configuration and directory state and " +
			"is byte-identical for every authenticated principal, which is asserted rather than " +
			"assumed. The accepted cost, written down rather than discovered: a deployment whose " +
			"tenants may launch different agents cannot express that here, and every authenticated " +
			"principal learns every configured AgentID -- topology, not tenant data",
		"GET /v1/capabilities": "is the migration spelling of /v1/agents and serves the identical " +
			"aggregate under the identical rule; a weaker rule on either would be two answers to " +
			"one question",
		"GET /v1/sessions/{sid}/status": "serves the durable replay-free projection of ONE session " +
			"under authSessionRead, which is the decision AuthorizeSessionRead exists to make. The " +
			"record is the one serveRoute resolved through scope.catalogEntry from principal.Tenant(), " +
			"so the session was established to exist within the authenticated tenant before this " +
			"handler ran, and a session in another tenant is answered by the same sessionNotFound() " +
			"construction absence produces. It reads no Host and consults no directory: every member " +
			"it answers is a catalog record member",
		"GET /v1/sessions/{sid}/journal": "serves one BOUNDED page of a session's public events under " +
			"authSessionRead, the same decision the status makes and over the same resource. Its " +
			"position is either a cursor SessionStore issued for this session or an absolute sequence, " +
			"and neither is authority: the tenant comes from the principal through scope.journalPage, " +
			"so a cursor or position from another tenant reaches a query scoped to the caller's own. " +
			"The page is bounded by maxJournalPageLimit whatever the caller asks for, and a view with " +
			"no position is a bounded TAIL rather than a replay, so no credential buys an unbounded read",
		"GET /v1/sessions/{sid}/gates": "serves the session's open public gates under authSessionRead, " +
			"the same decision over the same resource. Core's GateProjection is the complete public " +
			"view of an open gate -- presentation-safe prompt data, never a submitted answer, a private " +
			"prepared payload, a credential or a signed URL -- and the page is SessionStore's, " +
			"forwarded whole, so there is no member for a stronger rule to protect that this one does " +
			"not already cover",
		"GET /v1/sessions": "serves the principal's OWN tenant's durable session page under " +
			"authSessionList, which is the decision AuthorizeSessionList exists to make. The page " +
			"is built by scope.sessionPage from principal.Tenant(), so the tenant is the " +
			"authenticated one and no identifier from the request reaches the query",
	}
	// The GET keys are collected BEFORE anything is inserted. Ranging over a
	// map while writing to it is defined in Go -- a new key may or may not be
	// produced -- and every key written here would be skipped anyway because
	// it is a HEAD, so this is deterministic either way. It is separated
	// because a reader should not have to establish that.
	gets := slices.Sorted(maps.Keys(sanctioned))
	for _, key := range gets {
		method, path, _ := strings.Cut(key, " ")
		if method == http.MethodGet {
			sanctioned[http.MethodHead+" "+path] = sanctioned[key]
		}
	}
	return sanctioned
}

// TestClearingAnOwnerRequiresAnExplicitSanction couples the two.
func TestClearingAnOwnerRequiresAnExplicitSanction(t *testing.T) {
	t.Parallel()

	sanctioned := sanctionedImplementedMethods()
	implemented := 0
	for _, entry := range routeTable() {
		for _, rule := range entry.rules {
			key := rule.method + " " + entry.pattern
			reason, ok := sanctioned[key]
			if rule.owner != "" {
				if ok {
					t.Errorf("%s is sanctioned as implemented but still names owner %q", key, rule.owner)
				}
				continue
			}
			implemented++
			if !ok {
				t.Errorf("%s serves a handler but is not in sanctionedImplementedMethods; "+
					"clearing an owner requires writing down why its authorization rule is adequate", key)
				continue
			}
			if reason == "" {
				t.Errorf("%s is sanctioned with an empty reason", key)
			}
		}
	}
	if implemented == 0 {
		t.Fatal("no method is implemented, so the sanction has no subject")
	}
	if len(sanctioned) != implemented {
		t.Errorf("%d methods are sanctioned and %d are implemented", len(sanctioned), implemented)
	}
}

// The object decision runs even when reference policy is not yet configured.
func TestTheObjectRouteMakesAnObjectDecision(t *testing.T) {
	t.Parallel()

	authorizer := &recordingAuthorizer{}
	f := newFixture(t, withAuthorizer(authorizer))
	recorder := f.get("/v1/sessions/" + string(fixtureSession) + "/objects/object-a")
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("without policy the object route answered %d, want 503", recorder.Code)
	}
	if got := authorizer.snapshot(); len(got) != 1 || got[0].operation != "object" {
		t.Fatalf("object decision missing: %+v", got)
	}
}

// TestEveryAuthorizationRuleAndCommandKindIsDeclared floors the per-method
// sweep on the RULE set rather than on the route set: a rule nothing declares
// is a branch of Router.authorize no test drives, and a command kind nothing
// declares is a constant with no reader.
func TestEveryAuthorizationRuleAndCommandKindIsDeclared(t *testing.T) {
	t.Parallel()

	rules := map[authRule]bool{}
	commands := map[sessionstore.CommandKind]bool{}
	bodies := map[bodyRule]bool{}
	for _, entry := range routeTable() {
		for _, rule := range entry.rules {
			rules[rule.auth] = true
			bodies[rule.body] = true
			if rule.auth == authControl {
				commands[rule.command] = true
			}
		}
	}
	for _, rule := range []authRule{authAuthenticated, authSessionList, authSessionRead, authObjectRead, authControl} {
		if !rules[rule] {
			t.Errorf("no method declares rule %d, so Router.authorize has a branch nothing drives", rule)
		}
	}
	for _, command := range []sessionstore.CommandKind{commandCreate, commandInput, commandInterrupt, commandRestore, commandGateResponse} {
		if !commands[command] {
			t.Errorf("no method authorizes under command kind %q", command)
		}
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
// it shares with create serves a bodied method.
func TestASafeMethodOnABodiedRouteNeedsNoBody(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	checked := 0
	for _, entry := range routeTable() {
		if _, bodied := bodiedMethod(entry); !bodied {
			continue
		}
		for _, rule := range entry.rules {
			if rule.body == bodyJSON {
				continue
			}
			checked++
			r := httptest.NewRequest(rule.method, fixtureOrigin+concreteTarget(entry.pattern), nil)
			r.Host = fixtureHost
			r.Header.Set("Authorization", "Bearer "+fixtureBearer)
			recorder := f.serve(r)
			if recorder.Code == http.StatusUnsupportedMediaType || recorder.Code == http.StatusBadRequest {
				t.Errorf("%s %s = %d; a bodiless method on a bodied route must not require a body",
					rule.method, entry.pattern, recorder.Code)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no route serves both a bodied and a bodiless method, so this claim has no subject")
	}
}

// TestEveryBodiedMethodIsBoundedAndTyped is the behavioural half of the body
// column's reader, and it sweeps every method that declares one.
//
// It used to drive /v1/sessions alone: the media-type table, the ceiling pair,
// the lying Content-Length and the empty body were all one route, so step 1's
// content-type and size requirement and step 3's bounded bodies were proved for
// one of five. Dropping the body rule from /v1/sessions/{sid}/input left the
// whole suite green. Now every declared bodied method is driven, so a rule
// dropped anywhere is reported.
func TestEveryBodiedMethodIsBoundedAndTyped(t *testing.T) {
	t.Parallel()

	const ceiling = 16
	driven := 0
	for _, entry := range routeTable() {
		for _, rule := range entry.rules {
			if rule.body != bodyJSON {
				continue
			}
			driven++
			t.Run(rule.method+" "+entry.pattern, func(t *testing.T) {
				t.Parallel()

				f := newFixture(t, withLimits(RouteLimits{MaxRequestBytes: ceiling, RequestTimeout: time.Second}))
				target := concreteTarget(entry.pattern)
				send := func(contentType string, body string, length int64) *httptest.ResponseRecorder {
					r := request(rule.method, target, strings.NewReader(body))
					r.Header.Set("Content-Type", contentType)
					if length >= 0 {
						r.ContentLength = length
					}
					return f.serve(r)
				}

				if got := send("text/plain", `{}`, -1); got.Code != http.StatusUnsupportedMediaType {
					t.Errorf("a text/plain body answered %d, want 415", got.Code)
				} else if code := decodeEnvelope(t, got).Error.Code; code != ErrorCodeUnsupportedMediaType {
					t.Errorf("415 code = %q", code)
				}
				if got := send("application/json", "", -1); got.Code != http.StatusBadRequest {
					t.Errorf("an empty body answered %d, want 400", got.Code)
				}
				// The ceiling is applied while READING, so a declared length
				// smaller than the body does not raise it.
				if got := send("application/json", strings.Repeat("x", 512), 4); got.Code != http.StatusRequestEntityTooLarge {
					t.Errorf("a lying Content-Length answered %d, want 413", got.Code)
				} else if code := decodeEnvelope(t, got).Error.Code; code != ErrorCodePayloadTooLarge {
					t.Errorf("413 code = %q", code)
				}
				// Both sides of the inclusive bound, which is what pins the
				// value rather than the presence of a check.
				if got := send("application/json", strings.Repeat("x", ceiling), -1); got.Code == http.StatusRequestEntityTooLarge {
					t.Errorf("a body of exactly %d bytes was refused by a ceiling of %d", ceiling, ceiling)
				}
				if got := send("application/json", strings.Repeat("x", ceiling+1), -1); got.Code != http.StatusRequestEntityTooLarge {
					t.Errorf("a body of %d bytes answered %d under a ceiling of %d, want 413", ceiling+1, got.Code, ceiling)
				}
			})
		}
	}
	// The sweep selects on the very column it is testing, so a rule dropped
	// from the table would silently shrink it to nothing rather than fail. The
	// floor comes from the independent restatement, which is not selecting on
	// the table at all.
	expected := 0
	for _, want := range expectedRoutes() {
		if want.body == bodyJSON {
			expected++
		}
	}
	if expected == 0 {
		t.Fatal("the restatement expects no bodied method, so this floor is vacuous")
	}
	if driven != expected {
		t.Fatalf("the table declares %d bodied methods, the restatement expects %d", driven, expected)
	}
}

// TestABoundedBodyIsStillReadableByTheHandler is the reader for the buffering.
//
// The body used to be read and DISCARDED, which made the ceiling real but left
// A3.1 with a consumed io.ReadCloser and no way to decode the command envelope
// without changing serveRoute's signature. This drives the production function
// directly, because at this task the only handler behind it answers 501 and so
// has nothing to read the body with.
func TestABoundedBodyIsStillReadableByTheHandler(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	const payload = `{"command_id":"c-1"}`
	r := request(http.MethodPost, "/v1/sessions", strings.NewReader(payload))
	recorder := httptest.NewRecorder()
	if !f.router.readBoundedJSONBody(recorder, r) {
		t.Fatalf("the body was refused: %d %s", recorder.Code, recorder.Body)
	}
	got, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("re-reading the body: %v", err)
	}
	if string(got) != payload {
		t.Errorf("the handler would read %q, want %q", got, payload)
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

// ---------------------------------------------------------------------------
// The bound on authentication, and the writer's optional interfaces.
// ---------------------------------------------------------------------------

// blockingVerifier waits for its context and reports what it saw, which is what
// lets a test distinguish "the deadline reached the verifier" from "the request
// happened to finish".
type blockingVerifier struct {
	started  chan struct{}
	deadline chan bool
	err      chan error
}

func newBlockingVerifier() *blockingVerifier {
	return &blockingVerifier{
		started:  make(chan struct{}, 1),
		deadline: make(chan bool, 1),
		err:      make(chan error, 1),
	}
}

func (v *blockingVerifier) VerifyCredential(ctx context.Context, _ internalidentity.Credential) (internalidentity.Claims, error) {
	_, hasDeadline := ctx.Deadline()
	select {
	case v.started <- struct{}{}:
	default:
	}
	select {
	case v.deadline <- hasDeadline:
	default:
	}
	<-ctx.Done()
	select {
	case v.err <- ctx.Err():
	default:
	}
	return internalidentity.Claims{}, ctx.Err()
}

// TestAWedgedCredentialServiceDoesNotParkTheHandler is the reader for the
// deadline authentication now runs under.
//
// Authentication calls a network dependency on EVERY request, and it ran
// outside RouteLimits.RequestTimeout because the per-route deadline is created
// after routing. Measured before the fix, at RequestTimeout 50ms: the handler
// goroutine was still inside VerifyCredential after 500ms, ten runs of ten.
// http.Server.WriteTimeout closes the connection but does not release the
// goroutine, so the accumulation was unbounded.
func TestAWedgedCredentialServiceDoesNotParkTheHandler(t *testing.T) {
	t.Parallel()

	verifier := newBlockingVerifier()
	f := newFixture(t,
		withLimits(RouteLimits{MaxRequestBytes: 1 << 20, RequestTimeout: 30 * time.Millisecond}),
		withRawVerifier(verifier))

	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- f.get("/v1/agents") }()

	select {
	case recorder := <-done:
		if !<-verifier.deadline {
			t.Fatal("the verifier was called with a context carrying no deadline")
		}
		if recorder.Code != http.StatusGatewayTimeout {
			t.Fatalf("status = %d, want 504; body %q", recorder.Code, recorder.Body)
		}
		if code := decodeEnvelope(t, recorder).Error.Code; code != ErrorCodeTimeout {
			t.Errorf("code = %q, want %q", code, ErrorCodeTimeout)
		}
		if err := <-verifier.err; !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("the verifier's context ended with %v, want a deadline", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the request did not return: authentication is running outside the request bound")
	}
}

// Object verification receives its own deadline after authentication.
func TestObjectVerificationGetsItsOwnPostAuthenticationBound(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withLimits(RouteLimits{MaxRequestBytes: 1 << 20, RequestTimeout: 20 * time.Millisecond}))
	f.get("/v1/sessions/" + string(fixtureSession) + "/objects/object-a")
	_, deadlines := f.reads.snapshot()
	if len(deadlines) != 1 {
		t.Fatalf("the store was called %d times, want 1", len(deadlines))
	}
	if !deadlines[0] {
		t.Error("object verification has no post-authentication deadline")
	}
}

// TestTheWrapperDoesNotConcealTheWritersOptionalInterfaces is the reader for
// recordingWriter.Unwrap.
//
// Embedding an http.ResponseWriter satisfies the interface and hides every
// optional one underneath it, so without Unwrap no handler below could flush,
// hijack or read from a source. A6.1 needs Hijacker for the WebSocket upgrade
// and the failure would be a nil
// assertion at run time in a task with no reason to suspect this type.
func TestTheWrapperDoesNotConcealTheWritersOptionalInterfaces(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	writer := &recordingWriter{ResponseWriter: recorder}
	if err := http.NewResponseController(writer).Flush(); err != nil {
		t.Fatalf("Flush through the wrapper: %v", err)
	}
	if !recorder.Flushed {
		t.Error("the flush did not reach the underlying writer")
	}
	// Reverting the mechanism must lose the capability, or the assertion above
	// would pass for a wrapper that never needed one.
	if err := http.NewResponseController(concealingWriter{recorder}).Flush(); err == nil {
		t.Error("a wrapper without Unwrap flushed, so this test cannot see the difference")
	}
}

// concealingWriter is recordingWriter without Unwrap: the shape the wrapper had
// before, kept here as the negative control.
type concealingWriter struct{ http.ResponseWriter }
