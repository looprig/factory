package httpapi

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"path"
	"slices"
	"strings"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	"github.com/looprig/factory/internal/command"
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
	// ObjectPolicy proves committed-reference permission; nil fails closed.
	ObjectPolicy ObjectPolicy
	// ResolveObjectStore consumes the full immutable catalog pin, including
	// RuntimeSessionID and ProtocolMode, and must refuse unknown configurations.
	// Reads stay in canonical authenticated tenant/session scope. Any runtime
	// namespace translation belongs to the resolved adapter, never this handler.
	// Production resolver/public server composition remains A9's obligation.
	ResolveObjectStore func(context.Context, sessionstore.SessionBinding) (ObjectReader, error)
	ObjectLimits       ObjectLimits

	// Directory is the observed Host target directory, read by /v1/agents to
	// learn which configured launch targets are currently advertised.
	Directory Directory

	// Admissions is the durable command plane the control routes admit into.
	//
	// A nil Admissions FAILS CLOSED rather than being rejected by NewRouter,
	// and that is ObjectPolicy's rule for ObjectPolicy's reason: a deployment
	// composed for durable reading alone -- which is what factory.New builds
	// today, and every test of the read plane -- is a supported composition,
	// and requiring a command plane it never calls would make the read surface
	// unbuildable. A control request against such a composition is answered
	// 503 unavailable, which says the deployment cannot carry it out now
	// rather than that the caller's command was refused.
	Admissions ControlAdmitter

	// Realtime is the optional ClientLink entry point mounted at /v1/realtime.
	//
	// A nil Realtime FAILS CLOSED rather than being rejected by NewRouter, for
	// Admissions' reason and with Admissions' answer: a deployment composed
	// for durable reading alone is supported, and a realtime request against
	// one is answered 503 unavailable -- the deployment cannot carry it out
	// now, rather than the route not existing.
	//
	// It is a SUPPLIER and not a handler because the two halves have different
	// lifetimes. The router is built by factory.New and the ClientLink node is
	// RUN by factory.Server.Start -- clientlink.NewHandler runs it before it
	// returns, deliberately, so a composition cannot succeed and then refuse
	// every connection -- so a handler field would force New to acquire a
	// lifetime. A supplier answering nil until Start has run gives the same
	// 503, from the same line, with no second code path.
	Realtime func() http.Handler

	// Delivery is the optional local wake-up for an already admitted command.
	//
	// Nil means no attempt is made, which changes NO response: the durable
	// record is the acknowledgement and delivery is best effort either way.
	// See Router.deliverAdmitted.
	Delivery CommandDelivery

	// Department is the launch targets this deployment is configured to offer.
	//
	// It may be EMPTY, and an empty Department is a supported composition
	// rather than a degenerate one: a Factory serving an existing tenant's
	// durable session history needs no launchable agent at all. It answers
	// /v1/agents with an empty list, which is the truthful answer.
	//
	// Every entry is validated by NewRouter. See LaunchTemplate for why the
	// deployment supplies these at all, and Router.serveAgents for why the
	// aggregate they produce is not tenant-scoped.
	Department []LaunchTemplate

	// Guard is the origin and CSRF guard, mounted around the mux and inside
	// authentication; see Guard's own documentation for both reasons.
	Guard *Guard

	// IDs mints the per-request identifier.
	IDs IDSource

	// UI is the optional single-page application. It is served only OUTSIDE
	// the API version segment; see Router.ServeHTTP.
	//
	// # What the router sets on its responses, and what it does not
	//
	// The router sets X-Content-Type-Options, Referrer-Policy and
	// X-Frame-Options before this handler runs, so a bundle server that sets
	// none of them is still covered. It does NOT set a
	// Content-Security-Policy: the API's is default-src 'none', which would
	// forbid a document its own scripts and styles, and there is no policy
	// this package can write for a bundle it has never seen.
	//
	// So a UI handler owes its own Content-Security-Policy, and this package
	// DECLINES to interpose rather than being unable to.
	//
	// The distinction matters because the earlier wording -- "nothing here can
	// check that" -- is false at the layer that has the reader. ServeHTTP does
	// not hand this handler the raw writer; it wraps it in a recordingWriter,
	// which already implements WriteHeader, so the PRESENCE of a policy this
	// handler set is observable at that seam with machinery that exists today.
	//
	// What no code here can judge is ADEQUACY. A present Content-Security-
	// Policy may be default-src * and worth nothing, and the policy a bundle
	// needs is a fact about the bundle's own scripts, styles, fonts and
	// connect targets -- which this package has never seen. A presence check
	// would therefore fail a correct deployment that sets its policy at the
	// edge, in a meta tag, or on the document alone rather than on every asset,
	// and pass one whose policy is empty of meaning. Refusing at run time is
	// worse still: it turns a header opinion into an outage.
	//
	// Enforcement at COMPOSITION is what would be worth having, and it is not
	// available -- there is nothing to inspect in an http.Handler before it
	// serves. So the obligation is stated here, on the field a composer
	// supplies, which is the earliest place a human reads it.
	//
	// Every header above is a DEFAULT rather than a floor. They are written to
	// the header map before this handler is invoked, and net/http's header map
	// stays mutable until WriteHeader, so a UI that must be embedded in a
	// parent application can Set or Del X-Frame-Options itself.
	UI http.Handler
	// UIRoutes serves application-owned /ui/ paths after authentication,
	// origin/CSRF checks and AuthorizeUIRoute. It does not handle public assets.
	UIRoutes         http.Handler
	AuthorizeUIRoute func(context.Context, identity.Principal, string, string) error

	// Limits bounds bodies and work. Its zero value takes DefaultRouteLimits.
	Limits RouteLimits
}

// Router is Factory's public HTTP surface.
type Router struct {
	credentials        *internalidentity.Authenticator
	authorizer         Authorizer
	reads              SessionReader
	directory          Directory
	department         []LaunchTemplate
	guard              *Guard
	ids                IDSource
	ui                 http.Handler
	limits             RouteLimits
	admissions         ControlAdmitter
	realtime           func() http.Handler
	delivery           CommandDelivery
	objectPolicy       ObjectPolicy
	resolveObjectStore func(context.Context, sessionstore.SessionBinding) (ObjectReader, error)
	objectLimits       ObjectLimits

	// own is every response the router produces itself: the security headers,
	// then the path split, then authentication, then the guard, then the mux.
	// The guard is INSIDE authentication because its tokens are
	// principal-bound, and AROUND the mux so a rejected origin cannot learn
	// which routes exist by comparing a 403 with a 404.
	own         http.Handler
	protectedUI http.Handler
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
	if cfg.Directory == nil {
		return nil, fmt.Errorf("%w: Directory is required", ErrInvalidRouterConfig)
	}
	for i, template := range cfg.Department {
		if err := template.Validate(); err != nil {
			return nil, fmt.Errorf("%w (Department entry %d)", err, i)
		}
	}
	if cfg.Guard == nil {
		return nil, fmt.Errorf("%w: Guard is required", ErrInvalidRouterConfig)
	}
	if cfg.IDs == nil {
		return nil, fmt.Errorf("%w: IDs is required", ErrInvalidRouterConfig)
	}
	if (cfg.UIRoutes == nil) != (cfg.AuthorizeUIRoute == nil) {
		return nil, fmt.Errorf("%w: UIRoutes and AuthorizeUIRoute must be supplied together", ErrInvalidRouterConfig)
	}
	if cfg.Limits == (RouteLimits{}) {
		cfg.Limits = DefaultRouteLimits()
	}
	if err := cfg.Limits.Validate(); err != nil {
		return nil, err
	}
	if cfg.ObjectLimits == (ObjectLimits{}) {
		cfg.ObjectLimits = DefaultObjectLimits()
	}
	if err := cfg.ObjectLimits.Validate(); err != nil {
		return nil, err
	}

	router := &Router{
		credentials: cfg.Credentials,
		authorizer:  cfg.Authorizer,
		reads:       cfg.Reads,
		directory:   cfg.Directory,
		// The caller's configuration is COPIED rather than referenced, so a
		// composer that reuses its buffers after NewRouter returns cannot
		// change what a running router advertises.
		department:         cloneDepartment(cfg.Department),
		guard:              cfg.Guard,
		ids:                cfg.IDs,
		ui:                 cfg.UI,
		limits:             cfg.Limits,
		admissions:         cfg.Admissions,
		realtime:           cfg.Realtime,
		delivery:           cfg.Delivery,
		objectPolicy:       cfg.ObjectPolicy,
		resolveObjectStore: cfg.ResolveObjectStore,
		objectLimits:       cfg.ObjectLimits,
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

	router.own = apiSecurityHeaders(router.dispatch(router.authenticate(cfg.Guard.Wrap(mux))))
	if cfg.UIRoutes != nil {
		protected := router.authenticate(cfg.Guard.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			operation, ok := internalidentity.OperationContextFrom(r.Context())
			if !ok {
				writeAPIError(w, authenticationFailure(identity.ErrUnauthenticated))
				return
			}
			if err := cfg.AuthorizeUIRoute(r.Context(), operation.Principal, r.Method, r.URL.Path); err != nil {
				writeAPIError(w, authorizationFailure(err))
				return
			}
			cfg.UIRoutes.ServeHTTP(w, r)
		})))
		router.protectedUI = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "no-store")
			protected.ServeHTTP(w, r)
		})
	}
	return router, nil
}

// ServeHTTP splits the API surface, protected /ui/ application routes, and
// the optional single-page application.
//
// The split is by path and it is made HERE, before authentication, because the
// SPA's assets are public and the API's routes are not. A configured /ui/
// route also goes through authentication and the guard, then its own explicit
// authorizer. Everything under the version segment goes through the API chain;
// errors are JSON, and authorized
// object bodies are bounded binary responses;
// everything else is the SPA's, or a JSON route failure when no SPA is mounted.
//
// The security headers are set in two places, and the split is exactly as wide
// as the argument for it.
//
// setNeutralSecurityHeaders runs HERE, above the split, so it covers every
// response the router mounts, the SPA's included. apiSecurityHeaders runs
// inside Router.own, which is every response the router writes itself --
// including the two routeNotFound answers dispatch produces before the API
// chain is reached. Both once sat inside the API chain, which left an unclean
// path such as /v1/../assets/app.js answering 404 with none of them: the exact
// request an attacker chooses.
//
// The API-only half is TWO headers, not five, and each is withheld from the SPA
// for its own reason. Content-Security-Policy is default-src 'none', which
// would forbid a bundle its own scripts and styles, and no policy this package
// could write would suit a document it has never seen. Cache-Control: no-store
// is right for private session data and wrong for a hashed asset, where it
// costs a re-download of the whole bundle on every load. The three neutral
// headers have no such conflict -- a document's own policy has nothing to say
// about MIME sniffing, referrer leakage or framing -- so withholding them was a
// sentence about one header applied to five. See RouterConfig.UI for what a
// composer is left owing.
func (rt *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	writer := &recordingWriter{ResponseWriter: w}
	defer recoverPanic(writer)
	rt.stampRequestID(writer)
	setNeutralSecurityHeaders(writer.Header())
	if rt.protectedUI != nil && !isAPIRequest(r) && isUIRouteRequest(r) {
		if cleanRequestPath(r.URL.Path) != r.URL.Path {
			writeAPIError(writer, routeNotFound())
			return
		}
		rt.protectedUI.ServeHTTP(writer, r)
		return
	}

	if rt.ui != nil && !isAPIRequest(r) {
		rt.ui.ServeHTTP(writer, r)
		return
	}
	rt.own.ServeHTTP(writer, r)
}

func isUIRouteRequest(r *http.Request) bool {
	return strings.HasPrefix(r.URL.Path, "/ui/") || strings.HasPrefix(cleanRequestPath(r.URL.Path), "/ui/")
}

// isAPIRequest reports whether r addresses the API surface under either
// spelling of its path. The cleaned spelling is consulted as well as the
// requested one so that //v1/agents -- which the mux would clean into
// /v1/agents -- cannot be handed to the SPA.
func isAPIRequest(r *http.Request) bool {
	return isAPIPath(r.URL.Path) || isAPIPath(cleanRequestPath(r.URL.Path))
}

// dispatch is everything the router answers itself: the API chain, and the
// route failures on either side of it.
func (rt *Router) dispatch(api http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isAPIRequest(r) {
			writeAPIError(w, routeNotFound())
			return
		}
		requested := r.URL.Path
		cleaned := cleanRequestPath(requested)
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
		// outcome. The argument is the mechanism rather than a count of
		// probes: an escaped-unclean path is one holding a literal "..", "."
		// or "//" in its escaped form, and decoding leaves those bytes exactly
		// where they were, so the decoded form is unclean too.
		//
		// The superset costs one request: a path whose segment holds an
		// encoded slash, such as a session identifier spelled a%2Fb, decodes to
		// an empty segment and is refused. Factory mints session identifiers
		// from CryptoUUIDSource, so no identifier it issues contains one, and
		// the mux would in any case have delivered "a/b" as a single path value
		// -- refusing is the same answer arrived at earlier.
		if cleaned != requested {
			writeAPIError(w, routeNotFound())
			return
		}
		api.ServeHTTP(w, r)
	})
}

// isAPIPath reports whether p addresses this router's API surface. It compares
// the first SEGMENT, so /v1x/agents is not an API path and /v1 alone is.
func isAPIPath(p string) bool {
	trimmed := strings.TrimPrefix(p, "/")
	first, _, _ := strings.Cut(trimmed, "/")
	return first == apiVersionSegment
}

// cleanRequestPath roots a path and applies path.Clean.
//
// It deliberately does NOT restore a trailing slash the way net/http's own
// cleanPath does, so /v1/agents/ is reported unclean and refused. That is a
// difference from the mux, not an oversight: the mux keeps the trailing slash
// because a registered "/tree/" pattern means something to it, and this router
// registers no such pattern, so a trailing slash on an API path can only ever
// be a request for a route that does not exist. Refusing it here answers the
// same 404 one step earlier and removes a branch nothing else reads.
//
// A path this reports as unclean is one the mux would have redirected or
// missed. The converse does not hold -- it is applied to the DECODED path, so
// it reports some paths unclean that the mux would have routed; see ServeHTTP
// for why that direction is the safe one.
func cleanRequestPath(p string) string {
	if p == "" {
		return "/"
	}
	if p[0] != '/' {
		p = "/" + p
	}
	return path.Clean(p)
}

// stampRequestID mints the identifier and puts it on the response.
//
// A source failure leaves the header ABSENT rather than failing the request or
// fabricating a value: a correlation aid must not become an availability
// dependency, and a fabricated identifier is worse than none because it would
// be indistinguishable from a real one in a log.
//
// An EMPTY identifier returned with no error is refused for the same reason and
// is a separate case, because an empty header value is not the same thing as an
// absent one: Header().Set writes the field, and a caller quoting "" in a
// support request has quoted something. UUIDSource does not promise a non-empty
// result, so the check is on the value rather than on the error alone.
func (rt *Router) stampRequestID(w http.ResponseWriter) {
	id, err := rt.ids.NewUUID()
	if err != nil || id == "" {
		return
	}
	w.Header().Set(RequestIDHeader, id)
}

// setNeutralSecurityHeaders sets the headers that cannot conflict with any
// document a UI handler might serve, so they are applied to every response this
// router mounts rather than to the API alone.
//
// Each is here because a document's own policy has nothing to say about it:
//
//   - nosniff makes the declared content type binding. Without it a browser may
//     MIME-sniff a static asset a bundle server returns, which is the ordinary
//     route from "serves files" to "executes script".
//   - no-referrer stops the full console URL travelling in the Referer of every
//     outbound navigation and subresource. By A2.2 to A2.4's own route shapes
//     that URL carries a session identifier, so this is not hygiene.
//   - DENY refuses framing. The API sets frame-ancestors 'none' because a JSON
//     error rendered inside an attacker's frame is still a document; the
//     console that approves gates and interrupts sessions is the stronger case
//     of the same argument, and it is the one a clickjacking attack would aim
//     at. It is a DEFAULT, not a floor: the header map stays mutable until
//     WriteHeader, so a UI that must be embedded overrides it. See
//     RouterConfig.UI.
//
// X-Frame-Options rather than a frame-ancestors policy, for the SPA, because
// this package writes no Content-Security-Policy for a document it has not
// seen, and a UI that writes its own would then have to remember to restate a
// framing rule it never set. The API response carries both.
func setNeutralSecurityHeaders(header http.Header) {
	header.Set("X-Content-Type-Options", "nosniff")
	header.Set("Referrer-Policy", "no-referrer")
	header.Set("X-Frame-Options", "DENY")
}

// apiSecurityHeaders sets what only an API response carries, before any handler
// can write, so a header is never missing from the responses that carry data
// and present only on the ones that carry errors.
//
// no-store is not caution: every API response here is either a caller's private
// session data or a failure about it, and a shared cache holding one would
// serve it to whoever asks next. It is API-only because it is a real cost
// elsewhere -- applied to a hashed bundle asset it forces the whole bundle to
// be fetched again on every load, and a content-hashed URL is already safe to
// cache forever.
//
// The CSP is for the failure bodies as much as the successes, and its
// frame-ancestors clause is the modern spelling of the DENY that
// setNeutralSecurityHeaders applies more widely.
func apiSecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := w.Header()
		header.Set("Cache-Control", "no-store")
		header.Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
		next.ServeHTTP(w, r)
	})
}

// authenticate is the middleware the guard is mounted inside.
//
// It runs BEFORE routing, so an anonymous caller receives the same 401 for a
// route that exists and one that does not and cannot enumerate the surface.
//
// # Why the deadline is applied here and not only per route
//
// Authentication calls the injected Verifier, which a deployment backs with a
// token introspection endpoint or a shared session table -- a network
// dependency, and one that runs on EVERY request including the streaming
// routes. The per-route deadline is created after routing, so without this one
// a wedged credential service would leave a handler goroutine parked inside
// VerifyCredential for as long as it takes; http.Server.WriteTimeout closes the
// connection but does not release the goroutine, so the accumulation is
// unbounded. Measured before this bound existed: at RequestTimeout 50ms the
// goroutine was still inside VerifyCredential after 500ms, ten runs of ten.
//
// The deadline is cancelled as soon as authentication returns, so it bounds
// authentication ALONE and does not shorten a streaming route's lifetime. What
// it can and cannot do is the ordinary contract of a context: a Verifier that
// honours the one it is given returns at the deadline, and one that ignores it
// is not bounded by anything here. That is the seam's promise to keep, and it
// is the same promise every other dependency in this package makes.
func (rt *Router) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authCtx, cancel := context.WithTimeout(r.Context(), rt.limits.RequestTimeout)
		principal, err := rt.credentials.AuthenticateRequest(authCtx, r)
		// The context's own ending is read BEFORE cancel, because cancel
		// overwrites a deadline with a cancellation.
		ended := authCtx.Err()
		cancel()
		if err != nil {
			failure := authenticationFailure(err)
			// A deadline this router imposed is reported as one whatever the
			// verifier returned. internal/identity deliberately keeps a
			// verifier error's TEXT and drops its VALUE when it redacts, so
			// errors.Is cannot see the context error through it -- the reader
			// has to be the context this middleware created, not the error the
			// dependency chose.
			if ended != nil {
				if timedOut, ok := contextFailure(ended); ok {
					failure = timedOut
				}
			}
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

// recordingWriter remembers whether a status has been sent, which is the fact
// panic recovery has to branch on and the only fact it records.
type recordingWriter struct {
	http.ResponseWriter
	wrote bool
}

// Unwrap exposes the wrapped writer to http.ResponseController.
//
// Embedding an http.ResponseWriter satisfies the interface and CONCEALS every
// optional one the real writer implements: without this, no handler below can
// reach Flusher, Hijacker or ReaderFrom, because a type assertion sees only the
// wrapper. http.ResponseController follows the Unwrap chain, so a handler
// written against it reaches the real writer through any depth of wrapping.
func (w *recordingWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// Hijack hands the connection to a handler that asserts http.Hijacker DIRECTLY
// rather than through http.ResponseController.
//
// Unwrap was documented above as "the whole fix" for the WebSocket upgrade, and
// it was not, because the library that performs the upgrade does not consult
// it: gorilla/websocket@v1.5.3's Upgrader does `w.(http.Hijacker)` on the
// writer it is handed (server.go:175) and answers 500 "response does not
// implement http.Hijacker" when that fails. Every /v1/realtime upgrade through
// the composed router answered exactly that 500, and nothing measured it,
// because the composed case sent a plain GET (answered 400 before the hijack)
// and the origin-guard case wrapped the ClientLink handler directly, with no
// router in front of it. The end-to-end cookie case in the root package is what
// found it. This method delegates through the ResponseController so the rule
// stays "the real writer decides": a writer that cannot be hijacked reports
// http.ErrNotSupported here rather than being misreported by the wrapper.
func (w *recordingWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return http.NewResponseController(w.ResponseWriter).Hijack()
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
	// bodyJSON is a METHOD that requires a bounded JSON body.
	//
	// It sits on the method rather than on the route because /v1/sessions
	// serves both a list and a create, and requiring a body on the list would
	// answer 400 to the request every client makes first. It was once a route
	// column filtered at the call site by guard.go's stateChanging, which
	// worked but made "which methods carry a body" a derived fact nothing
	// could state per route; now the only methods that declare it are the ones
	// that carry one.
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
	authObjectRead
	// authControl is a state-changing command within the principal's tenant.
	authControl
)

// The command kinds the control routes authorize under.
//
// A3.1 owns the command envelope; these exist here because AuthorizeControl
// takes a kind and a route that could not name one would have to skip the
// decision. sessionstore.CommandKind is deliberately an open string upstream,
// so Factory's closed set is stated in internal/command.
//
// They are aliases of that package's rather than a private copy, and A6.1 is
// why: the ClientLink RPC vocabulary authorizes under the SAME kinds, and two
// private copies would satisfy every test either package could write while
// letting an RPC be admitted under a kind no route serves.
const (
	commandCreate       = command.KindCreateSession
	commandInput        = command.KindInput
	commandInterrupt    = command.KindInterrupt
	commandRestore      = command.KindRestore
	commandGateResponse = command.KindGateResponse
)

// methodRule is what one METHOD on one route declares.
//
// The rules are per method rather than per route because a route can serve two
// operations: /v1/sessions is a tenant list under GET and a session create
// under POST, and one rule for both would have to be the weaker of the two. It
// was authSessionList for both, and that is exactly the defect the split
// exists to prevent -- a principal authorized only to LIST a tenant could
// CREATE in it. The "the session does not exist yet" argument for keeping
// create out of the control rule does not survive contact with the signature:
// AuthorizeControl takes the SessionID as a blank parameter and never reads it,
// which A1.2 pins with TestAuthorizerOpaqueSeamParametersAreUnread, so a
// nonexistent session is no obstacle to the control decision.
type methodRule struct {
	method string
	auth   authRule
	// command is the kind the control decision is made under. It is meaningful
	// only when auth is authControl.
	command sessionstore.CommandKind
	body    bodyRule

	// owner is the runbook task that fills this METHOD in. An empty owner is a
	// method implemented here; anything else answers 501 today.
	//
	// It sits beside auth for the reason auth itself moved off route in A2.1:
	// one route can serve two operations at two readinesses. /v1/sessions is a
	// tenant list this task implements and a session create A3.1 owns, and a
	// single route-level owner could only be one of "implemented" or "pending"
	// -- either marking the create as served or holding the list back. The
	// column that says which task owes a body has to be as fine-grained as the
	// bodies are.
	//
	// TestTheUnimplementedMethodsAreExactlyTheOnesLaterTasksOwn holds the two
	// sets equal, and TestClearingAnOwnerRequiresAnExplicitSanction is what
	// stops a later task shipping a handler on a rule nobody re-read.
	owner string

	// reason is why this method is not served, in the words a CALLER can act
	// on, and it is written into the 501 rather than kept for a reviewer.
	//
	// It exists because an owner tag is a fact about this repository's task
	// roster and answers nothing a client asked. "A3.1" stayed on the session
	// create through two tasks that could not have implemented it, and by the
	// time A3.3 read it the tag named a task whose own remaining work was
	// blocked elsewhere -- so the response said the build had no handler and
	// the table said the wrong task owed one. A reason is checked against the
	// blocker each time it is read, which an owner tag is not.
	//
	// It is REQUIRED wherever owner is, and empty wherever owner is empty;
	// TestEveryPendingMethodExplainsItselfToTheCaller holds both directions.
	reason string

	// handle is this method's own answer. It is nil for a method with an owner.
	handle func(*Router) http.Handler
}

// route is one entry of the public surface.
type route struct {
	// pattern is the http.ServeMux pattern, with no method: methods are
	// matched here so a refusal answers in this package's envelope rather than
	// in net/http's plain text, with an Allow header the mux does not write.
	pattern string

	// rules are the methods this route serves, in Allow-header order.
	rules []methodRule

	// session says the route names a {sid} that must resolve within the
	// principal's tenant before the handler runs.
	session bool

	// streams says the route's response is not bounded by the request
	// deadline, because cutting it would truncate a body or close a socket the
	// deadline was never meant to bound.
	streams bool
}

// methods lists the methods this route serves, which is both the match set and
// the Allow header.
func (r route) methods() []string {
	names := make([]string, 0, len(r.rules))
	for _, rule := range r.rules {
		names = append(names, rule.method)
	}
	return names
}

// ruleFor reports the rule for a method, or false when the route does not serve
// it.
func (r route) ruleFor(method string) (methodRule, bool) {
	for _, rule := range r.rules {
		if rule.method == method {
			return rule, true
		}
	}
	return methodRule{}, false
}

// readRules is GET plus HEAD under one rule.
//
// HEAD is served wherever GET is, and that is a MUST rather than a
// convenience: RFC 9110 section 9.1 requires a general-purpose server to
// support GET and HEAD and makes every other method optional. It is the same
// sentence that makes refusing OPTIONS conformant, so it has to be read in both
// directions. It costs nothing to honour -- guard.go's stateChanging already
// classifies HEAD as safe, so it is CSRF-exempt exactly as GET is, and net/http
// suppresses the response body for a HEAD request without the handler knowing.
//
// It takes the whole rule rather than an authorization level so that the two
// methods cannot acquire different owners, handlers, bodies or command kinds:
// HEAD is GET's rule EXACTLY, and copying the value is what makes that true by
// construction instead of by two literals staying in step.
func readRules(rule methodRule) []methodRule {
	get, head := rule, rule
	get.method = http.MethodGet
	head.method = http.MethodHead
	return []methodRule{get, head}
}

// routeTable is the public surface of specification section 8.1.
//
// It is a function rather than a package variable so no caller can hold a
// reference that lets it add a route at run time, and so a test reads the same
// value the router was built from. Every column here is pinned against an
// independent restatement by TestEveryRouteDeclaresWhatItsShapeRequires; the
// table is not its own authority.
func routeTable() []route {
	// control is a state-changing command this build ADMITS: the route decodes
	// the V1 envelope, hands it to the admission service and answers from the
	// authoritative durable record.
	control := func(kind sessionstore.CommandKind) []methodRule {
		return []methodRule{{
			method: http.MethodPost, auth: authControl, command: kind, body: bodyJSON,
			handle: func(rt *Router) http.Handler { return rt.serveControl(kind) },
		}}
	}
	served := func(auth authRule, handle func(*Router) http.Handler) []methodRule {
		return readRules(methodRule{auth: auth, handle: handle})
	}
	agents := served(authAuthenticated, func(rt *Router) http.Handler { return rt.serveAgents() })
	return []route{
		{pattern: "/v1/bootstrap", rules: served(authAuthenticated, func(rt *Router) http.Handler { return rt.serveBootstrap() })},
		{pattern: "/v1/agents", rules: agents},
		{pattern: "/v1/capabilities", rules: agents},
		{pattern: "/v1/sessions", rules: append(
			served(authSessionList, func(rt *Router) http.Handler { return rt.serveSessionList() }),
			// The create, served as of A3.1.
			//
			// It answered 501 through two tasks, and the reason was never a
			// missing handler: a V1 create files a durable public-create
			// reservation carrying an immutable SessionBinding, and this
			// module had no source for three of its four members. It has one
			// now. StorageBindingID and BindingVersion are deployment
			// configuration supplied by WithSessionBinding -- the same two
			// members ObjectStoreResolver keys on, which is what makes them
			// configuration rather than a guess -- RuntimeSessionID is derived
			// from the create's own identity, and ProtocolMode is disposition
			// because a legacy session is one no Host can take residency on.
			//
			// A composition that supplies no binding still refuses, but it now
			// refuses in admission rather than here, so the route is served
			// unconditionally and the refusal is a property of the deployment
			// rather than of the build.
			control(commandCreate)...)},
		{pattern: "/v1/sessions/{sid}/status",
			rules:   served(authSessionRead, func(rt *Router) http.Handler { return rt.serveSessionStatus() }),
			session: true},
		{pattern: "/v1/sessions/{sid}/journal",
			rules:   served(authSessionRead, func(rt *Router) http.Handler { return rt.serveSessionJournal() }),
			session: true},
		{pattern: "/v1/sessions/{sid}/gates",
			rules:   served(authSessionRead, func(rt *Router) http.Handler { return rt.serveSessionGates() }),
			session: true},
		{pattern: "/v1/sessions/{sid}/objects/{oid}", rules: served(authObjectRead, func(rt *Router) http.Handler { return rt.serveObject(false) }), session: true},
		{pattern: "/v1/sessions/{sid}/objects/{oid}/metadata", rules: served(authObjectRead, func(rt *Router) http.Handler { return rt.serveObject(true) }), session: true},
		{pattern: "/v1/sessions/{sid}/input", rules: control(commandInput), session: true},
		{pattern: "/v1/sessions/{sid}/interrupt", rules: control(commandInterrupt), session: true},
		{pattern: "/v1/sessions/{sid}/restore", rules: control(commandRestore), session: true},
		{pattern: "/v1/sessions/{sid}/gates/{gid}", rules: control(commandGateResponse), session: true},
		// A6.1 built the ClientLink and A9.1 stage 2 composes it. The route is
		// SERVED rather than pending, and a build that composes no ClientLink
		// -- or one whose node has not been started -- answers 503 through
		// RouterConfig.Realtime, exactly as a build with no command plane
		// answers a control route.
		{pattern: "/v1/realtime", rules: served(authAuthenticated, func(rt *Router) http.Handler { return rt.serveRealtime() }), streams: true},
		{pattern: "/v1/csrf-token", rules: served(authAuthenticated, func(rt *Router) http.Handler { return rt.guard.TokenHandler() })},
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
	// Each method's handler is built ONCE, here, rather than per request: a
	// handler constructed inside the request path would rebuild whatever the
	// method's chain holds on every call, and the mux already caches nothing.
	handlers := make(map[string]http.Handler, len(entry.rules))
	for _, rule := range entry.rules {
		if rule.handle == nil {
			// The reason is bound HERE, per method, rather than shared: two
			// methods of one route may be pending for different reasons, and
			// /v1/sessions is exactly that shape waiting to happen.
			reason := rule.reason
			handlers[rule.method] = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeAPIError(w, notImplemented(reason))
			})
			continue
		}
		handlers[rule.method] = rule.handle(rt)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rule, served := entry.ruleFor(r.Method)
		if !served {
			w.Header().Set("Allow", strings.Join(entry.methods(), ", "))
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

		var authorizationErr error
		if rule.auth == authObjectRead {
			ref := sessionwire.ObjectReference{ObjectID: r.PathValue("oid")}
			if ref.Validate() != nil {
				writeAPIError(w, invalidObjectRequest())
				return
			}
			authorizationErr = rt.authorizer.AuthorizeObjectRead(ctx, operation.Principal, session, ref)
		} else {
			authorizationErr = rt.authorize(ctx, rule, operation.Principal, session)
		}
		if authorizationErr != nil {
			writeAPIError(w, authorizationFailure(authorizationErr))
			return
		}

		if rule.body == bodyJSON && !rt.readBoundedJSONBody(w, r) {
			return
		}

		if entry.session {
			resolved, ok := rt.resolveSession(w, r, operation.Principal, session)
			if !ok {
				return
			}
			r = resolved
		}

		handlers[rule.method].ServeHTTP(w, r)
	})
}

// authorize applies the method's declared rule.
//
// Every branch calls the injected Authorizer. There is no rule whose meaning is
// "no decision": authAuthenticated means the decision was made by
// authentication, which has already run and refused an unverified caller.
func (rt *Router) authorize(ctx context.Context, rule methodRule, principal identity.Principal, session sessionwire.SessionID) error {
	switch rule.auth {
	case authSessionList:
		return rt.authorizer.AuthorizeSessionList(ctx, principal)
	case authSessionRead:
		return rt.authorizer.AuthorizeSessionRead(ctx, principal, session)
	case authControl:
		return rt.authorizer.AuthorizeControl(ctx, principal, session, rule.command)
	default:
		return nil
	}
}

// resolveSession establishes that the session exists WITHIN the principal's
// tenant, answers 404 when it does not, and CARRIES the record it read forward
// on the request.
//
// The entry used to be discarded, because A2.1 owned only the existence
// decision. It is carried now because the record it read is the same record the
// status projects, and reading it twice would be a second durable round trip on
// the most polled route on the surface AND a second instant: the existence
// decision made against one record and the answer rendered from another. A
// handler that does not want it ignores it.
//
// The 404 it writes is the same construction absence produces, which is what
// makes a session in another tenant indistinguishable from one that was never
// created: the query is scoped by the principal's tenant, so the store reports
// the cross-tenant row as missing rather than as forbidden.
func (rt *Router) resolveSession(
	w http.ResponseWriter,
	r *http.Request,
	principal identity.Principal,
	session sessionwire.SessionID,
) (*http.Request, bool) {
	entry, err := rt.reads.GetCatalogEntry(r.Context(), newScope(principal).catalogEntry(session))
	if err != nil {
		writeAPIError(w, catalogFailure(err))
		return r, false
	}
	return withResolvedSession(r, entry), true
}

// readBoundedJSONBody applies the media type and the ceiling, and leaves the
// body READABLE.
//
// The ceiling is only real if the body is actually read, because a caller can
// declare any Content-Length it likes -- but a consumed body cannot be read
// again, so the bytes are buffered and r.Body is replaced with a reader over
// them. A3.1 decodes the command envelope from exactly those bytes and needs no
// signature change to reach them; discarding here would have forced one.
//
// The buffer is bounded by the same ceiling that produces the 413, so retaining
// it costs at most MaxRequestBytes per in-flight request, which is the bound
// the ceiling exists to state.
func (rt *Router) readBoundedJSONBody(w http.ResponseWriter, r *http.Request) bool {
	if !hasJSONContentType(r.Header.Get("Content-Type")) {
		writeAPIError(w, apiError{
			status:  http.StatusUnsupportedMediaType,
			code:    ErrorCodeUnsupportedMediaType,
			message: "the request body must be application/json",
		})
		return false
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, rt.limits.MaxRequestBytes))
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
	case len(body) == 0:
		writeAPIError(w, apiError{
			status:  http.StatusBadRequest,
			code:    sessionwire.ErrorCodeInvalidRequest,
			message: "this route requires a JSON request body",
		})
		return false
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
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
func (s scope) objectMetadata(session sessionwire.SessionID, ref sessionwire.ObjectReference, kind sessionstore.ObjectKind) sessionstore.GetObjectMetadataRequest {
	return sessionstore.GetObjectMetadataRequest{TenantID: s.principal.Tenant(), SessionID: session, ExpectedKind: kind, Reference: ref}
}
func (s scope) objectBody(session sessionwire.SessionID, m sessionwire.ObjectMetadata, kind sessionstore.ObjectKind) sessionstore.GetObjectRequest {
	return sessionstore.GetObjectRequest{TenantID: s.principal.Tenant(), SessionID: session, ExpectedKind: kind, Metadata: m}
}
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

// notImplemented answers a method this build serves no handler for, and NAMES
// THE BLOCKER.
//
// The message used to be the fixed text below with nothing after it, which told
// a caller only that the route exists and does nothing -- and left the actual
// reason in a table column reading "A3.1" long after that had stopped being
// true. A reason a caller reads is a reason somebody has to re-read.
//
// An empty reason degrades to the fixed sentence rather than producing a
// dangling colon. It is unreachable through the route table, which requires a
// reason wherever there is an owner, and it is not a branch this file relies on
// being unreachable: notImplemented is exported to no one but is reachable from
// any later handler.
func notImplemented(reason string) apiError {
	message := "this build serves no handler for that route"
	if reason != "" {
		message += ": " + reason
	}
	return apiError{
		status:  http.StatusNotImplemented,
		code:    ErrorCodeNotImplemented,
		message: message,
	}
}

// cloneDepartment copies a configured Department, INCLUDING each template's
// Capabilities.
//
// slices.Clone alone is shallow, and shallow is not a copy of this value:
// LaunchTemplate.Capabilities is itself a slice, so a cloned entry shares its
// backing array with the caller's. A composer reusing its buffer could
// therefore rewrite a live router's published capability strings -- measured,
// before this existed, from ["gates"] to ["root-shell"] after NewRouter
// returned -- and would silently move the ETag with them, which is the one
// validator on this surface that is meant to be a pure function of deployment
// state.
//
// Every configured slice this package retains is copied here, so "the
// composition is the router's" holds for the whole value rather than for its
// outermost level.
func cloneDepartment(department []LaunchTemplate) []LaunchTemplate {
	copied := slices.Clone(department)
	for i := range copied {
		copied[i].Capabilities = slices.Clone(copied[i].Capabilities)
	}
	return copied
}

// launchTargetPage is the scoped read of one launch target's current capacity
// page.
//
// It is the ONE SessionStore request on this surface that must not carry a
// tenant, and it lives here for exactly that reason. The scan that keeps every
// other request inside this type would otherwise report it, and the reviewer
// who followed the report would have to rediscover why it is different:
// SessionStore's Host target directory is deliberately not partitioned by
// tenant -- a pooled target may serve several tenants, so a row carries an
// isolation class instead -- and ListCompatibleHostsRequest therefore has no
// TenantID member for a tenant to reach. There is nothing to scope, and the
// type makes that unrepresentable rather than merely unwritten.
//
// The scope is still taken by receiver so the call site is identical to every
// other durable read's, and so this argument is read from the place a tenant
// would have gone.
func (s scope) launchTargetPage(key sessionstore.HostTargetKey) sessionstore.ListCompatibleHostsRequest {
	return sessionstore.ListCompatibleHostsRequest{
		Key:   key,
		Limit: agentProbePageLimit,
	}
}

// sessionPage is the scoped read of one tenant's recent-first session page.
func (s scope) sessionPage(cursor sessionwire.Cursor, limit int) sessionstore.ListSessionsRequest {
	return sessionstore.ListSessionsRequest{
		TenantID: s.principal.Tenant(),
		Cursor:   cursor,
		Limit:    limit,
	}
}

// journalPage is the scoped read of one bounded public journal page.
//
// Cursor, fromSeq and tail describe mutually exclusive positions. The handler
// validates caller positions before selecting Tail for an initial view.
// Every request, including a cursor continuation, caps examined records at its
// chosen page limit. Private records use this work budget without adding events.
func (s scope) journalPage(
	session sessionwire.SessionID,
	cursor sessionwire.Cursor,
	fromSeq uint64,
	limit int,
	tail bool,
) sessionstore.ReadPublicJournalRequest {
	return sessionstore.ReadPublicJournalRequest{
		TenantID:  s.principal.Tenant(),
		SessionID: session,
		FromSeq:   fromSeq,
		Tail:      tail,
		ScanLimit: limit,
		Cursor:    cursor,
		Limit:     limit,
	}
}

// gates is the scoped read of one session's open public gates. It carries no
// position because the store's read has none.
func (s scope) gates(session sessionwire.SessionID) sessionstore.ReadGatesRequest {
	return sessionstore.ReadGatesRequest{
		TenantID:  s.principal.Tenant(),
		SessionID: session,
	}
}
