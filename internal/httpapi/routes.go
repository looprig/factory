package httpapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"path"
	"slices"
	"strings"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	internalidentity "github.com/looprig/factory/internal/identity"
	"github.com/looprig/sessionstore"
)

// ErrInvalidRouterConfig is the class of every NewRouter rejection.
var ErrInvalidRouterConfig = errors.New("httpapi: invalid router configuration")

// RequestIDHeader carries the identifier this router MINTS for each request.
//
// It is minted, never echoed. A client may send this header, and a router that
// copied it would put caller-controlled bytes into every log line and durable
// record that later correlates on the value -- including bytes a naive log
// formatter would treat as a line break. The response carries the minted value
// so a caller can still quote it in a support request.
const RequestIDHeader = "X-Request-Id"

// apiVersionSegment is the first path segment of every route this router
// serves. Membership is decided by SEGMENT rather than by string prefix, so
// /v1x is not inside it; the module already pays for that distinction in
// pathHasPrefix and this is the same rule at the request level.
const apiVersionSegment = "v1"

// IDSource supplies the per-request identifier. Its shape is Factory's public
// UUIDSource, so the composition satisfies it with no adapter.
type IDSource interface {
	NewUUID() (string, error)
}

// RouteLimits bounds one request's body and one request's work.
type RouteLimits struct {
	// MaxRequestBytes is the inclusive ceiling on a request body, applied while
	// READING rather than by trusting Content-Length.
	MaxRequestBytes int64

	// RequestTimeout bounds the work a handler may do, as a deadline on the
	// request's context.
	//
	// It is a deadline and not a response guillotine, and the difference is
	// worth stating because it bounds the claim. Every dependency call this
	// package makes takes the context, so the deadline is what actually stops
	// the work; a guillotine that raced a handler already writing bytes could
	// not produce a JSON envelope, which is the property step 2 asks for.
	// Bounding the SOCKET -- a peer that never finishes sending a header, or
	// never reads a response -- is http.Server's ReadHeaderTimeout and
	// WriteTimeout, and belongs to the task that builds the server (A9.1).
	RequestTimeout time.Duration
}

// DefaultRouteLimits is what a composition naming no limits receives.
//
// One mebibyte is generous for the V1 command envelopes this surface accepts --
// identities plus a bounded prompt -- and an oversized private payload goes to
// SessionObjectStore by reference rather than through this ceiling (A3.1 step
// 5). Thirty seconds is longer than any durable read here should take and short
// enough that a stuck dependency does not accumulate handlers.
func DefaultRouteLimits() RouteLimits {
	return RouteLimits{
		MaxRequestBytes: 1 << 20,
		RequestTimeout:  30 * time.Second,
	}
}

// Validate reports why these limits may not be used.
func (l RouteLimits) Validate() error {
	if l.MaxRequestBytes < 1 {
		return fmt.Errorf("%w: RouteLimits.MaxRequestBytes is %d, want at least 1", ErrInvalidRouterConfig, l.MaxRequestBytes)
	}
	if l.RequestTimeout <= 0 {
		return fmt.Errorf("%w: RouteLimits.RequestTimeout is %v, want a positive duration", ErrInvalidRouterConfig, l.RequestTimeout)
	}
	return nil
}

// RouterConfig composes the public REST plane.
type RouterConfig struct {
	// Credentials authenticates a request and derives its operation context.
	//
	// It is the CONCRETE authenticator rather than the Authenticator interface
	// this package declares, for the reason GuardConfig.Credentials is: the
	// operation context must record which credential authenticated the
	// request, NewOperationContext is a method so that answer comes from the
	// same derivation the authentication used, and a seam here would be a
	// second place for an edge to state it wrongly. A wrong answer there is a
	// CSRF guard that skips, silently.
	Credentials *internalidentity.Authenticator

	// Authorizer decides every public operation.
	Authorizer Authorizer

	// Reads is the durable read plane.
	Reads SessionReader

	// Guard is the origin and CSRF guard, mounted around the mux and inside
	// authentication; see Guard's own documentation for both reasons.
	Guard *Guard

	// IDs mints the per-request identifier.
	IDs IDSource

	// UI is the optional single-page application. It is served only OUTSIDE
	// the API version segment; see Router.ServeHTTP.
	UI http.Handler

	// Limits bounds bodies and work. Its zero value takes DefaultRouteLimits.
	Limits RouteLimits
}

// Router is Factory's public HTTP surface.
type Router struct {
	credentials *internalidentity.Authenticator
	authorizer  Authorizer
	reads       SessionReader
	guard       *Guard
	ids         IDSource
	ui          http.Handler
	limits      RouteLimits

	// api is the composed API chain: security headers, then authentication,
	// then the guard, then the mux. The guard is INSIDE authentication because
	// its tokens are principal-bound, and AROUND the mux so a rejected origin
	// cannot learn which routes exist by comparing a 403 with a 404.
	api http.Handler
}

// NewRouter validates a composition and returns the router. A rejection returns
// a nil Router, so a caller that ignores the error cannot serve from one built
// from a half-valid composition.
func NewRouter(cfg RouterConfig) (*Router, error) {
	if cfg.Credentials == nil {
		return nil, fmt.Errorf("%w: Credentials is required", ErrInvalidRouterConfig)
	}
	if cfg.Authorizer == nil {
		return nil, fmt.Errorf("%w: Authorizer is required", ErrInvalidRouterConfig)
	}
	if cfg.Reads == nil {
		return nil, fmt.Errorf("%w: Reads is required", ErrInvalidRouterConfig)
	}
	if cfg.Guard == nil {
		return nil, fmt.Errorf("%w: Guard is required", ErrInvalidRouterConfig)
	}
	if cfg.IDs == nil {
		return nil, fmt.Errorf("%w: IDs is required", ErrInvalidRouterConfig)
	}
	if cfg.Limits == (RouteLimits{}) {
		cfg.Limits = DefaultRouteLimits()
	}
	if err := cfg.Limits.Validate(); err != nil {
		return nil, err
	}

	router := &Router{
		credentials: cfg.Credentials,
		authorizer:  cfg.Authorizer,
		reads:       cfg.Reads,
		guard:       cfg.Guard,
		ids:         cfg.IDs,
		ui:          cfg.UI,
		limits:      cfg.Limits,
	}

	mux := http.NewServeMux()
	for _, entry := range routeTable() {
		mux.Handle(entry.pattern, router.serveRoute(entry))
	}
	// Everything under the version segment that matches no route is a route
	// failure, answered in the same envelope as everything else. Without this
	// the mux's own 404 would be net/http's plain text.
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeAPIError(w, routeNotFound())
	}))

	router.api = securityHeaders(router.authenticate(cfg.Guard.Wrap(mux)))
	return router, nil
}

// ServeHTTP splits the API surface from the optional single-page application.
//
// The split is by path and it is made HERE, before authentication, because the
// SPA's assets are public and the API's routes are not. Everything under the
// version segment goes through the API chain and can only leave it as JSON;
// everything else is the SPA's, or a JSON route failure when no SPA is mounted.
func (rt *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	writer := &recordingWriter{ResponseWriter: w}
	defer recoverPanic(writer)
	rt.stampRequestID(writer)

	requested := r.URL.Path
	cleaned := cleanRequestPath(requested)
	if isAPIPath(requested) || isAPIPath(cleaned) {
		// An unclean API path is refused rather than cleaned, because
		// http.ServeMux answers a redirect to the cleaned path -- which for
		// /v1/../assets/app.js leaves the API entirely and lands on the SPA,
		// with a Location header and an HTML body. A redirect out of the API is
		// a fallthrough by another name.
		//
		// The DECODED path is what is checked, although the mux cleans
		// r.URL.EscapedPath, and that is sufficient rather than approximate.
		// Percent-decoding only REPLACES a %XX triple; it never deletes a
		// literal ".." or "//", so any dot segment or empty segment that makes
		// the escaped form unclean is present verbatim in the decoded form as
		// well. The decoded check therefore refuses a superset of what the mux
		// would redirect, and adding the escaped spelling beside it changes no
		// outcome -- it was measured over seven constructions and never fired
		// alone.
		//
		// The superset costs one request: a path whose segment holds an
		// encoded slash, such as a session identifier spelled a%2Fb, decodes to
		// an empty segment and is refused. Factory mints session identifiers
		// from CryptoUUIDSource, so no identifier it issues contains one, and
		// the mux would in any case have delivered "a/b" as a single path value
		// -- refusing is the same answer arrived at earlier.
		if cleaned != requested {
			writeAPIError(writer, routeNotFound())
			return
		}
		rt.api.ServeHTTP(writer, r)
		return
	}
	if rt.ui != nil {
		rt.ui.ServeHTTP(writer, r)
		return
	}
	writeAPIError(writer, routeNotFound())
}

// isAPIPath reports whether p addresses this router's API surface. It compares
// the first SEGMENT, so /v1x/agents is not an API path and /v1 alone is.
func isAPIPath(p string) bool {
	trimmed := strings.TrimPrefix(p, "/")
	first, _, _ := strings.Cut(trimmed, "/")
	return first == apiVersionSegment
}

// cleanRequestPath is path.Clean with the trailing-slash and rooting rules
// net/http's own mux applies. It is the same normalization the mux performs, so
// a path this reports as unclean is one the mux would have redirected -- but it
// is applied to the DECODED path, so the converse does not hold and it reports
// some paths unclean that the mux would have routed; see ServeHTTP for why that
// direction is the safe one.
func cleanRequestPath(p string) string {
	if p == "" {
		return "/"
	}
	if p[0] != '/' {
		p = "/" + p
	}
	cleaned := path.Clean(p)
	if cleaned != "/" && strings.HasSuffix(p, "/") {
		cleaned += "/"
	}
	return cleaned
}

// stampRequestID mints the identifier and puts it on the response.
//
// A source failure leaves the header ABSENT rather than failing the request or
// fabricating a value: a correlation aid must not become an availability
// dependency, and a fabricated identifier is worse than none because it would
// be indistinguishable from a real one in a log.
func (rt *Router) stampRequestID(w http.ResponseWriter) {
	id, err := rt.ids.NewUUID()
	if err != nil || id == "" {
		return
	}
	w.Header().Set(RequestIDHeader, id)
}

// securityHeaders sets what every API response carries, before any handler can
// write, so a header is never missing from the responses that carry data and
// present only on the ones that carry errors.
//
// no-store is not caution: every API response here is either a caller's private
// session data or a failure about it, and a shared cache holding one would
// serve it to whoever asks next. The frame and CSP headers are for the failure
// bodies as much as the successes -- a JSON error rendered inside an attacker's
// frame is still a document.
func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := w.Header()
		header.Set("X-Content-Type-Options", "nosniff")
		header.Set("Cache-Control", "no-store")
		header.Set("Referrer-Policy", "no-referrer")
		header.Set("X-Frame-Options", "DENY")
		header.Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
		next.ServeHTTP(w, r)
	})
}

// authenticate is the middleware the guard is mounted inside.
//
// It runs BEFORE routing, so an anonymous caller receives the same 401 for a
// route that exists and one that does not and cannot enumerate the surface.
func (rt *Router) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, err := rt.credentials.AuthenticateRequest(r.Context(), r)
		if err != nil {
			failure := authenticationFailure(err)
			if failure.status == http.StatusUnauthorized {
				w.Header().Set("WWW-Authenticate", `Bearer realm="factory"`)
			}
			writeAPIError(w, failure)
			return
		}
		ctx := rt.credentials.NewOperationContext(r.Context(), r, principal)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// recoverPanic turns a panic into a JSON internal error, or -- once bytes are
// already on the wire -- into an abort.
//
// The second branch is the one a recovery middleware usually gets wrong.
// Appending an envelope after a partial body produces bytes that are neither
// the handler's answer nor an error, and a client parsing them sees corruption
// rather than a failure. http.ErrAbortHandler is net/http's contract for "close
// this connection without a log line", which is the only honest answer once a
// status has been sent.
func recoverPanic(w *recordingWriter) {
	value := recover()
	if value == nil {
		return
	}
	if w.wrote {
		panic(http.ErrAbortHandler)
	}
	writeAPIError(w, internalFailure())
}

// recoverInto wraps next in this router's panic recovery. It exists so the
// recovery has a caller a test can drive with a handler of its own; production
// composition reaches the same code through ServeHTTP's deferred call.
func (rt *Router) recoverInto(w *recordingWriter, next http.Handler) http.Handler {
	return http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		defer recoverPanic(w)
		next.ServeHTTP(w, r)
	})
}

// recordingWriter remembers whether a status has been sent, which is the fact
// panic recovery has to branch on and the only fact it records.
type recordingWriter struct {
	http.ResponseWriter
	wrote bool
}

func (w *recordingWriter) WriteHeader(status int) {
	w.wrote = true
	w.ResponseWriter.WriteHeader(status)
}

func (w *recordingWriter) Write(b []byte) (int, error) {
	w.wrote = true
	return w.ResponseWriter.Write(b)
}

// ---------------------------------------------------------------------------
// The route table.
// ---------------------------------------------------------------------------

// bodyRule says whether a route reads a request body.
type bodyRule int

const (
	// bodyNone is a route that reads no body.
	bodyNone bodyRule = iota
	// bodyJSON is a route whose STATE-CHANGING methods require a bounded JSON
	// body. It is keyed to the method rather than to the route because
	// /v1/sessions serves both a list and a create, and requiring a body on
	// the list would be a route that answers 400 to the request every client
	// makes first. The predicate is guard.go's stateChanging, so "which
	// methods carry a body" and "which methods the CSRF rules apply to" cannot
	// drift apart into two answers.
	bodyJSON
)

// authRule names which authorization decision a route requires. It is a value
// on the route rather than a call inside a handler so that a route added
// without one does not compile into an unauthorized surface.
type authRule int

const (
	// authAuthenticated requires a verified principal and nothing more. It is
	// the rule for routes describing the DEPLOYMENT rather than a tenant's
	// data, and for the token endpoint, which serves the caller their own.
	authAuthenticated authRule = iota
	// authSessionList is the tenant-scoped session list.
	authSessionList
	// authSessionRead is a durable read within the principal's tenant.
	authSessionRead
	// authControl is a state-changing command within the principal's tenant.
	authControl
)

// The command kinds the control routes authorize under.
//
// A3.1 owns the command envelope and the durable vocabulary; these exist here
// because AuthorizeControl takes a kind and a route that could not name one
// would have to skip the decision. sessionstore.CommandKind is deliberately an
// open string upstream, so these are values rather than an enumeration.
const (
	commandCreate       sessionstore.CommandKind = "create"
	commandInput        sessionstore.CommandKind = "input"
	commandInterrupt    sessionstore.CommandKind = "interrupt"
	commandRestore      sessionstore.CommandKind = "restore"
	commandGateResponse sessionstore.CommandKind = "gate_response"
)

// route is one entry of the public surface.
type route struct {
	// pattern is the http.ServeMux pattern, with no method: methods are
	// matched here so a refusal answers in this package's envelope rather than
	// in net/http's plain text.
	pattern string
	methods []string
	body    bodyRule
	auth    authRule
	command sessionstore.CommandKind

	// session says the route names a {sid} that must resolve within the
	// principal's tenant before the handler runs.
	session bool

	// streams says the route's response is not bounded by the request
	// deadline, because cutting it would truncate a body or close a socket the
	// deadline was never meant to bound.
	streams bool

	// owner is the runbook task that fills this route's body in. An empty
	// owner is a route implemented here; anything else answers 501 today, and
	// TestTheUnimplementedRoutesAreExactlyTheOnesLaterTasksOwn holds the two
	// sets equal to what the table says.
	owner string

	// handle is the route's own answer. It is nil for a route with an owner.
	handle func(*Router) http.Handler
}

// routeTable is the public surface of specification section 8.1.
//
// It is a function rather than a package variable so no caller can hold a
// reference that lets it add a route at run time, and so a test reads the same
// value the router was built from.
func routeTable() []route {
	get := []string{http.MethodGet}
	post := []string{http.MethodPost}
	return []route{
		{pattern: "/v1/agents", methods: get, auth: authAuthenticated, owner: "A2.2"},
		{pattern: "/v1/capabilities", methods: get, auth: authAuthenticated, owner: "A2.2"},
		{pattern: "/v1/sessions", methods: []string{http.MethodGet, http.MethodPost}, body: bodyJSON, auth: authSessionList, owner: "A2.2/A3.1"},
		{pattern: "/v1/sessions/{sid}/status", methods: get, session: true, auth: authSessionRead, owner: "A2.3"},
		{pattern: "/v1/sessions/{sid}/journal", methods: get, session: true, auth: authSessionRead, owner: "A2.3"},
		{pattern: "/v1/sessions/{sid}/gates", methods: get, session: true, auth: authSessionRead, owner: "A2.3"},
		{pattern: "/v1/sessions/{sid}/objects/{oid}", methods: get, session: true, streams: true, auth: authSessionRead, owner: "A2.4"},
		{pattern: "/v1/sessions/{sid}/input", methods: post, body: bodyJSON, session: true, auth: authControl, command: commandInput, owner: "A3.1"},
		{pattern: "/v1/sessions/{sid}/interrupt", methods: post, body: bodyJSON, session: true, auth: authControl, command: commandInterrupt, owner: "A3.1"},
		{pattern: "/v1/sessions/{sid}/restore", methods: post, body: bodyJSON, session: true, auth: authControl, command: commandRestore, owner: "A3.1"},
		{pattern: "/v1/sessions/{sid}/gates/{gid}", methods: post, body: bodyJSON, session: true, auth: authControl, command: commandGateResponse, owner: "A3.1"},
		{pattern: "/v1/realtime", methods: get, streams: true, auth: authAuthenticated, owner: "A6.1"},
		{pattern: "/v1/csrf-token", methods: get, auth: authAuthenticated, handle: func(rt *Router) http.Handler { return rt.guard.TokenHandler() }},
	}
}

// serveRoute is the per-route chain: method, deadline, identifier validation,
// authorization, body, session resolution, handler.
//
// The order is the order of what each step may reveal. Authorization runs
// before the body is read and before the store is touched, so a caller who may
// not read this tenant learns nothing by sending a large body or by naming a
// session; the session resolution runs last because it is the only step that
// consults durable state.
func (rt *Router) serveRoute(entry route) http.Handler {
	handler := http.Handler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeAPIError(w, notImplemented())
	}))
	if entry.handle != nil {
		handler = entry.handle(rt)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !slices.Contains(entry.methods, r.Method) {
			w.Header().Set("Allow", strings.Join(entry.methods, ", "))
			writeAPIError(w, methodNotAllowed())
			return
		}
		operation, authenticated := internalidentity.OperationContextFrom(r.Context())
		if !authenticated {
			// Unreachable through the composed chain, which authenticates
			// before it routes. It fails closed rather than trusting that,
			// because the cost of being wrong is an unauthenticated request
			// reaching a durable read.
			writeAPIError(w, authenticationFailure(identity.ErrUnauthenticated))
			return
		}

		ctx := r.Context()
		if !entry.streams {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, rt.limits.RequestTimeout)
			defer cancel()
			r = r.WithContext(ctx)
		}

		var session sessionwire.SessionID
		if entry.session {
			session = sessionwire.SessionID(r.PathValue("sid"))
			if err := session.Validate(); err != nil {
				writeAPIError(w, invalidSessionID())
				return
			}
		}

		if err := rt.authorize(ctx, entry, operation.Principal, session); err != nil {
			writeAPIError(w, authorizationFailure(err))
			return
		}

		if entry.body == bodyJSON && stateChanging(r.Method) && !rt.readBoundedJSONBody(w, r) {
			return
		}

		if entry.session && !rt.resolveSession(ctx, w, operation.Principal, session) {
			return
		}

		handler.ServeHTTP(w, r)
	})
}

// authorize applies the route's declared rule.
//
// Every branch calls the injected Authorizer. There is no route whose rule is
// "no decision": authAuthenticated means the decision was made by
// authentication, which has already run and refused an unverified caller.
func (rt *Router) authorize(ctx context.Context, entry route, principal identity.Principal, session sessionwire.SessionID) error {
	switch entry.auth {
	case authSessionList:
		return rt.authorizer.AuthorizeSessionList(ctx, principal)
	case authSessionRead:
		return rt.authorizer.AuthorizeSessionRead(ctx, principal, session)
	case authControl:
		return rt.authorizer.AuthorizeControl(ctx, principal, session, entry.command)
	default:
		return nil
	}
}

// resolveSession establishes that the session exists WITHIN the principal's
// tenant, and answers 404 when it does not.
//
// The entry itself is discarded. This task owns the existence decision and the
// answer to a caller who may not have one; the record's projection into a
// status, a journal page or a gate list is A2.2 to A2.4's, and returning a
// value nothing reads would be surface with no consumer.
//
// The 404 it writes is the same construction absence produces, which is what
// makes a session in another tenant indistinguishable from one that was never
// created: the query is scoped by the principal's tenant, so the store reports
// the cross-tenant row as missing rather than as forbidden.
func (rt *Router) resolveSession(ctx context.Context, w http.ResponseWriter, principal identity.Principal, session sessionwire.SessionID) bool {
	if _, err := rt.reads.GetCatalogEntry(ctx, newScope(principal).catalogEntry(session)); err != nil {
		writeAPIError(w, catalogFailure(err))
		return false
	}
	return true
}

// readBoundedJSONBody applies the media type and the ceiling.
//
// The bytes are read and DISCARDED. What this task owns is the three decisions
// the read produces -- the media type, the ceiling, and an empty body -- and
// the ceiling is only real if the body is actually read, because a caller can
// declare any Content-Length it likes. A3.1 owns the envelope and is where the
// bytes acquire a consumer; retaining them here would be a buffer nothing reads.
func (rt *Router) readBoundedJSONBody(w http.ResponseWriter, r *http.Request) bool {
	if !hasJSONContentType(r.Header.Get("Content-Type")) {
		writeAPIError(w, apiError{
			status:  http.StatusUnsupportedMediaType,
			code:    ErrorCodeUnsupportedMediaType,
			message: "the request body must be application/json",
		})
		return false
	}
	read, err := io.Copy(io.Discard, http.MaxBytesReader(w, r.Body, rt.limits.MaxRequestBytes))
	var tooLarge *http.MaxBytesError
	switch {
	case errors.As(err, &tooLarge):
		writeAPIError(w, apiError{
			status:  http.StatusRequestEntityTooLarge,
			code:    ErrorCodePayloadTooLarge,
			message: "the request body is larger than this deployment accepts",
		})
		return false
	case err != nil:
		writeAPIError(w, apiError{
			status:  http.StatusBadRequest,
			code:    sessionwire.ErrorCodeInvalidRequest,
			message: "the request body could not be read",
		})
		return false
	case read == 0:
		writeAPIError(w, apiError{
			status:  http.StatusBadRequest,
			code:    sessionwire.ErrorCodeInvalidRequest,
			message: "this route requires a JSON request body",
		})
		return false
	}
	return true
}

// hasJSONContentType reports whether the header names JSON.
//
// It parses the media type rather than comparing the header, because
// "application/json; charset=utf-8" is what a browser sends and string equality
// would refuse it. A charset that is not UTF-8 is refused: Go decodes JSON as
// UTF-8, so accepting a declared iso-8859-1 body would mean decoding it as
// something other than what the caller said it was.
func hasJSONContentType(header string) bool {
	mediaType, params, err := mime.ParseMediaType(header)
	if err != nil || mediaType != "application/json" {
		return false
	}
	if charset, ok := params["charset"]; ok && !strings.EqualFold(charset, "utf-8") {
		return false
	}
	return true
}

// ---------------------------------------------------------------------------
// The tenant scope.
// ---------------------------------------------------------------------------

// scope is the only place a tenant becomes part of a SessionStore request.
//
// A1.2's Authorizer never reads its resource parameters -- a decision it makes
// deliberately, because a tenant-local identifier is not authority -- so
// "a cross-tenant identifier is indistinguishable from a nonexistent one" was
// true there only because existence was never consulted. THIS is where it stops
// being vacuous: the query carries the authenticated principal's tenant, so a
// row belonging to another tenant is reported missing by the store rather than
// found and then refused.
//
// Routing every request through one type is not a convention.
// TestNoProductionFileBuildsAStoreRequestOutsideTheScope scans every production
// file of this package, enumerated from the directory, and refuses a
// SessionStore composite literal, a keyed TenantID member and an assignment to
// a TenantID field anywhere else. A2.2 to A2.4 add their request builders HERE
// or that scan fails.
type scope struct {
	principal identity.Principal
}

// newScope binds a scope to an authenticated principal.
func newScope(principal identity.Principal) scope {
	return scope{principal: principal}
}

// catalogEntry is the scoped read of one session's catalog record.
func (s scope) catalogEntry(session sessionwire.SessionID) sessionstore.GetCatalogEntryRequest {
	return sessionstore.GetCatalogEntryRequest{
		TenantID:  s.principal.Tenant(),
		SessionID: session,
	}
}

// ---------------------------------------------------------------------------
// The failures this file constructs.
// ---------------------------------------------------------------------------

// routeNotFound is the answer to a path this version serves no route for. It
// is deliberately distinct from a session that does not exist: conflating the
// two would make a client's retry depend on which one Factory meant.
func routeNotFound() apiError {
	return apiError{
		status:  http.StatusNotFound,
		code:    ErrorCodeRouteNotFound,
		message: "this deployment serves no such route",
	}
}

func methodNotAllowed() apiError {
	return apiError{
		status:  http.StatusMethodNotAllowed,
		code:    ErrorCodeMethodNotAllowed,
		message: "this route does not serve that method",
	}
}

// invalidSessionID is answered before any storage call. Validity is a pure
// function of the identifier, so separating it from absence discloses nothing
// about which sessions exist.
func invalidSessionID() apiError {
	return apiError{
		status:  http.StatusBadRequest,
		code:    sessionwire.ErrorCodeInvalidRequest,
		message: "the session identifier is not a valid identity",
	}
}

func notImplemented() apiError {
	return apiError{
		status:  http.StatusNotImplemented,
		code:    ErrorCodeNotImplemented,
		message: "this build serves no handler for that route",
	}
}
