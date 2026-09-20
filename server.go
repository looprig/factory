package factory

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sync"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	"github.com/looprig/factory/internal/httpapi"
	internalidentity "github.com/looprig/factory/internal/identity"
	"github.com/looprig/sessionstore"
)

// The composition seams.
//
// Each interface below is the UNION of the narrow interfaces the internal
// packages that call it declare for themselves. The narrow ones are the real
// contracts; these exist because a deployer supplies one object and cannot
// import github.com/looprig/factory/internal/... to name the parts. Every
// method here names only a standard-library, Core or SessionStore type, so
// each is assignable to its narrow counterpart with no adapter, and
// server_test.go asserts exactly that in both directions: a consumer that
// widens its interface fails TestPublicSeamsSatisfyTheirConsumers, and a public
// seam widened past its consumers fails
// TestPublicSeamsAreExactlyTheUnionOfTheirConsumers.

// There is deliberately no public Authenticator seam. Factory composes ONE
// authenticator, internal/identity's, from the credential verifier a deployer
// supplies through WithCredentialVerifier; server_test.go holds that the
// composed authenticator satisfies both consumers that declare one. See
// identity/credential.go for why the seam is the verifier.

// Authorizer decides every public operation and the one service operation.
// A denial must wrap identity.ErrUnauthorized. Any other error is a fault in
// the authorization dependency rather than a permissions decision.
type Authorizer interface {
	AuthorizeSessionList(ctx context.Context, principal identity.Principal) error
	AuthorizeSessionRead(ctx context.Context, principal identity.Principal, session sessionwire.SessionID) error
	AuthorizeObjectRead(ctx context.Context, principal identity.Principal, session sessionwire.SessionID, object sessionwire.ObjectReference) error
	AuthorizeControl(ctx context.Context, principal identity.Principal, session sessionwire.SessionID, kind sessionstore.CommandKind) error
	AuthorizeSubscribe(ctx context.Context, principal identity.Principal, channel string) error
	AuthorizeServiceSweep(ctx context.Context, principal identity.Principal) error
}

// SessionReader is the durable read plane.
type SessionReader interface {
	ListSessions(ctx context.Context, req sessionstore.ListSessionsRequest) (sessionstore.SessionPage, error)
	GetCatalogEntry(ctx context.Context, req sessionstore.GetCatalogEntryRequest) (sessionstore.CatalogEntry, error)
	ReadPublicJournal(ctx context.Context, req sessionstore.ReadPublicJournalRequest) (sessionwire.JournalPage, error)
	ReadGates(ctx context.Context, req sessionstore.ReadGatesRequest) (sessionwire.GatePage, error)
	GetObject(ctx context.Context, req sessionstore.GetObjectRequest) (io.ReadCloser, error)
	GetObjectMetadata(ctx context.Context, req sessionstore.GetObjectMetadataRequest) (sessionwire.ObjectMetadata, error)
}

// Commands is the durable command plane. A *sessionstore.Store satisfies it.
//
// ControlShards is on it because the periodic sweeps ask the store how many
// service-control shards it persisted, on every pass rather than once: the
// count is a decision of the backend and not a setting of this replica, and a
// replica caching it would keep sweeping a shard space that had changed.
//
// Every command is admitted into the DISPOSITION family -- the only one a Host
// can take residency on -- through AdmitDispositionCommand, with
// GetDispositionCommand as the retry read and PutCommandPayload for a payload
// too large to store inline. RejectDispositionCommand and
// ListDueDispositionCommands are the disposition deadline sweep, which rejects
// a command no Host applied before its apply deadline. RejectCommand and
// ListDueCommands remain for the LEGACY deadline sweep only, which settles
// legacy rows a store may already hold; nothing admits into that family.
type Commands interface {
	ControlShards() int
	AdmitDispositionCommand(ctx context.Context, req sessionstore.AdmitDispositionCommandRequest) (sessionstore.DispositionInboxEntry, bool, error)
	GetDispositionCommand(ctx context.Context, req sessionstore.GetDispositionCommandRequest) (sessionstore.DispositionInboxEntry, error)
	PutCommandPayload(ctx context.Context, req sessionstore.PutCommandPayloadRequest) (sessionwire.ObjectMetadata, error)
	RejectDispositionCommand(ctx context.Context, req sessionstore.RejectDispositionCommandRequest) (sessionstore.DispositionInboxEntry, bool, error)
	ListDueDispositionCommands(ctx context.Context, req sessionstore.ListDueDispositionCommandsRequest) (sessionstore.DispositionDueCommandPage, error)
	RejectCommand(ctx context.Context, req sessionstore.RejectCommandRequest) (sessionstore.InboxEntry, error)
	ListDueCommands(ctx context.Context, req sessionstore.ListDueCommandsRequest) (sessionstore.DueCommandPage, error)
	AcquireReconciliationClaim(ctx context.Context, req sessionstore.AcquireReconciliationClaimRequest) (sessionstore.ReconciliationClaimEntry, error)
	ReleaseReconciliationClaim(ctx context.Context, req sessionstore.ReleaseReconciliationClaimRequest) (sessionstore.ReconciliationClaimEntry, error)
}

// Directory is the observed target directory.
type Directory interface {
	Owner(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) (sessionwire.HostLinkRegistryObservation, bool, error)
	Candidates(ctx context.Context, req sessionstore.ListCompatibleHostsRequest) (sessionstore.HostTargetPage, error)
}

// PlacementController is the placement seam this module shipped before it had
// a placement caller.
//
// It is NO LONGER REQUIRED, and nothing reads it. EnsurePlacement takes a
// DesiredWorkload, which names no tenant, session or generation, so no
// implementation could identify what it was asked to place; H5 put the
// platform adapter behind WorkloadController instead, and pooled placement is
// driven by this module's own HostLink attach (see WithPendingCommands). The
// option is still ACCEPTED, so a composition that supplies one keeps
// composing; removing the type or the option would break that caller for no
// gain.
//
// Deprecated: nothing reads it. Pooled placement attaches through the HostLink
// and dedicated placement goes through WorkloadController.
type PlacementController interface {
	EnsurePlacement(ctx context.Context, desired sessionstore.DesiredWorkload) error
	ReleasePlacement(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) error
}

// PendingCommands is the disposition inbox's service-plane due query: the
// durable record of every session with open work. A *sessionstore.Store
// satisfies it.
//
// It is what TRIGGERS pooled placement. With it, every sweep interval pages
// one control shard for commands still open, and each session with one and no
// live owner is attached to a Host and woken; see WithPendingCommands.
type PendingCommands interface {
	ControlShards() int
	ListDueDispositionCommands(ctx context.Context, req sessionstore.ListDueDispositionCommandsRequest) (sessionstore.DispositionDueCommandPage, error)
}

// WorkloadController owns the lifecycle of a dedicated session's workload.
//
// It is OPTIONAL and only the controller binary supplies it. H5 (answered
// 2026-09-04) puts the platform adapter in a separate controller, so
// cmd/factory composes none and a dedicated placement there fails with a named
// refusal rather than silently doing nothing.
//
// The intent is the whole currency: Factory-authored desire, carrying its own
// generation and an opaque workload payload this module never parses. The
// lifecycle observations are Core records, so a Kubernetes PodSpec, a Nomad
// job and a future platform's manifest remain behind the adapter boundary.
type WorkloadController interface {
	EnsureWorkload(ctx context.Context, intent sessionstore.PlacementIntent) error
	ObserveWorkload(ctx context.Context, intent sessionstore.PlacementIntent) (sessionwire.HostLinkRegistryObservation, bool, error)
	RequestDrain(ctx context.Context, intent sessionstore.PlacementIntent) (sessionwire.HostLinkDrainObservation, error)
	DeleteWorkload(ctx context.Context, intent sessionstore.PlacementIntent) error
}

// WorkloadEndpointDiscovery is the optional pre-attach half of a dedicated
// workload controller. It returns the Host ID, desired generation and bare
// internal endpoint only once the intended workload is ready. The endpoint is
// a dial target, not a registry observation or proof of residency. Factory
// checks the generation and Core's bare-base rules; the controller must verify
// that the Host ID and endpoint belong to the exact intent and that its
// workload is ready before returning true. HostLink Attach then checks agent,
// runtime compatibility, session and the Host fence under the Host lease.
// Factory refuses a dedicated attach when a controller lacks this method; older
// controllers remain usable for lifecycle operations but cannot place a new
// dedicated session.
type WorkloadEndpointDiscovery interface {
	WorkloadEndpoint(ctx context.Context, intent sessionstore.PlacementIntent) (sessionwire.HostID, uint64, sessionwire.InternalEndpoint, bool, error)
}

// Clock is the time seam.
type Clock interface {
	Now() time.Time
	AfterFunc(d time.Duration, f func()) (stop func() bool)
}

// UUIDSource is the random-identifier seam.
type UUIDSource interface {
	NewUUID() (string, error)
}

// Server is the composed Factory.
//
// What it composes TODAY is the durable public HTTP surface and nothing else:
// the credential authenticator built from the deployer's verifier, the origin
// and CSRF guard, internal/httpapi's router -- routed reads, the authenticated
// bootstrap and the JSON error envelope -- and the optional user interface,
// mounted as that router's fallback outside the API version segment.
//
// What it does NOT compose is stated here rather than left to be discovered.
// Command admission, target placement, the ClientLink and HostLink engines and
// the multi-replica reconcilers are separate runbook tasks and are not built
// yet, so the seams they will use are validated at composition and then held
// unread. The router is composed with an EMPTY launch Department, a nil object
// policy and no object-store resolver, each of which fails closed: /v1/agents
// answers an empty list and an object read answers "unavailable" rather than
// serving bytes no policy authorized.
type Server struct {
	cfg         config
	router      *httpapi.Router
	credentials *internalidentity.Authenticator
	components  *components

	// mu guards the serving lifecycle only. The composition above it is
	// immutable after New, so nothing else needs it.
	// lifecycle serializes Start against Stop. See Start for why a second
	// mutex is needed rather than a wider hold of mu.
	lifecycle sync.Mutex

	mu          sync.Mutex
	state       serverState
	quiescing   bool
	quiesceDone chan struct{}
	quiesceErr  error
	started     bool
	http        *http.Server
	// stop cancels the sweep loops; done is closed when every one of them has
	// returned. Stop WAITS on it, so "Stop returned" means no sweep is still
	// touching a store.
	stop context.CancelFunc
	done chan struct{}
}

// New validates a composition and returns it.
//
// The checks run in a fixed order -- shape of the option list, then what each
// option carries, then what is missing, then what the values mean together --
// so a caller with more than one defect is told about the outermost one, and
// every later check is reached only when the earlier ones hold.
func New(opts ...Option) (*Server, error) {
	cfg := config{
		clock:     SystemClock(),
		uuids:     CryptoUUIDSource(),
		http:      DefaultHTTPLimits(),
		reconcile: DefaultReconcileLimits(),
		client:    DefaultClientLinkLimits(),
		host:      DefaultHostLinkLimits(),
	}

	// An option supplied twice is an error rather than a silent last-wins: two
	// WithCSRF calls with different keys would compose without complaint and
	// the replica would sign with whichever the caller happened to list second.
	supplied := make(map[string]bool, len(opts))
	for i, opt := range opts {
		if opt.apply == nil || opt.name == "" {
			return nil, &OptionError{Option: fmt.Sprintf("option %d", i), Err: ErrNilOption}
		}
		if supplied[opt.name] {
			return nil, &OptionError{Option: opt.name, Err: ErrDuplicateOption}
		}
		supplied[opt.name] = true
	}
	for _, opt := range opts {
		if err := opt.apply(&cfg); err != nil {
			return nil, err
		}
	}

	var missing []string
	for _, required := range []struct {
		name    string
		present bool
	}{
		{"WithCredentialVerifier", cfg.verifier != nil},
		{"WithAuthorizer", cfg.authorizer != nil},
		{"WithSessionReader", cfg.reads != nil},
		{"WithCommands", cfg.commands != nil},
		{"WithDirectory", cfg.directory != nil},
		{"WithCatalog", cfg.catalog != nil},
		{"WithGates", cfg.gates != nil},
		{"WithHostTargets", cfg.hostTargets != nil},
		{"WithHostLinkCredential", cfg.hostCredential != nil},
		{"WithServiceIdentity", cfg.serviceSet},
		{"WithReplicaID", cfg.replicaID != ""},
		{"WithCSRF", cfg.csrfSet},
	} {
		if !required.present {
			missing = append(missing, required.name)
		}
	}
	if len(missing) > 0 {
		slices.Sort(missing)
		return nil, &MissingSeamsError{Options: missing}
	}

	// Only now, with every seam present, do the values get read against each
	// other. Running this earlier told a caller that supplied NO seams and both
	// UI options about the UI, which is the defect it is least likely to care
	// about; TestMissingSeamsAreReportedBeforeAConflictingUI holds the order.
	if cfg.ui != nil && cfg.uiFS != nil {
		return nil, ErrConflictingUI
	}

	// An object policy with no resolver behind it is refused HERE rather than
	// discovered at the first legacy-bound object read. internal/httpapi
	// resolves a store only for a NON-ZERO binding and falls back to the read
	// plane for a zero one; a nil policy refuses first and unconditionally, so
	// the fallback is unreachable until a policy exists. Composing the policy
	// alone is exactly the change that makes it reachable with nothing behind
	// it, and it is a composition mistake rather than a request-time one.
	if cfg.objectPolicy != nil && cfg.objectStores == nil {
		return nil, ErrObjectPolicyWithoutResolver
	}
	// A session binding with no resolver behind it is refused for a STRONGER
	// reason than the policy above, and the difference is worth stating: an
	// object policy composed alone makes a read fail, which a later
	// composition fixes. A session binding composed alone writes an immutable
	// StorageBindingID into every session this Factory creates, and a
	// deployment that resolves no storage at all can never read their objects
	// back. Nothing here can verify the resolver knows this particular
	// binding; what it can refuse is the composition that is certainly wrong.
	if cfg.sessionBinding != (SessionBindingTemplate{}) && cfg.objectStores == nil {
		return nil, ErrSessionBindingWithoutResolver
	}
	// EITHER half alone is refused, rather than silently serving a create
	// route that can only answer runtime_unavailable. A deployment that
	// composed one half meant to serve creates, and learning at composition
	// that it has not is strictly better than learning it from a caller.
	if (cfg.sessionBinding != SessionBindingTemplate{}) != (cfg.publicCreates != nil) {
		return nil, ErrCreatePlaneIncomplete
	}

	if err := cfg.csrf.Validate(); err != nil {
		return nil, &OptionError{Option: "WithCSRF", Err: err}
	}
	for _, limits := range []struct {
		name string
		err  error
	}{
		{"WithHTTPLimits", cfg.http.Validate()},
		{"WithReconcileLimits", cfg.reconcile.Validate()},
		{"WithClientLinkLimits", cfg.client.Validate()},
		{"WithHostLinkLimits", cfg.host.Validate()},
	} {
		if limits.err != nil {
			return nil, &OptionError{Option: limits.name, Err: limits.err}
		}
	}

	// The caller's slices are copied here rather than referenced, so a caller
	// that zeroes its key buffer after New returns does not change the key a
	// running replica verifies with.
	cfg.csrf = cfg.csrf.Clone()

	if cfg.version == "" {
		cfg.version = DefaultVersion
	}
	if cfg.objects == (ObjectLimits{}) {
		cfg.objects = DefaultObjectLimits()
	}

	// A static bundle becomes a handler at composition time, so UI() has one
	// answer shape and a serving path that does not branch on which option the
	// deployer used.
	if cfg.uiFS != nil {
		cfg.ui = http.FileServerFS(cfg.uiFS)
	}

	credentials, err := composeCredentials(cfg)
	if err != nil {
		return nil, err
	}
	parts, err := composeComponents(cfg, credentials)
	if err != nil {
		return nil, err
	}
	router, err := composeRouter(cfg, credentials, parts)
	if err != nil {
		return nil, err
	}
	return &Server{cfg: cfg, router: router, credentials: credentials, components: parts, done: closedChannel()}, nil
}

// closedChannel is the done channel of a Server that never started, so Stop
// waits on a channel that is already closed rather than branching on whether
// there is anything to wait for.
func closedChannel() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

// composeRouter builds the authenticator, the guard and the router.
//
// It runs at composition rather than at the first request, so a deployment that
// cannot serve fails where an operator is watching. Every rejection below is
// attributed to the option that carries the offending value.
func composeCredentials(cfg config) (*internalidentity.Authenticator, error) {
	// The default tenant is validated HERE, ahead of NewAuthenticator, so the
	// attribution below is exact rather than guessed. NewAuthenticator checks
	// the verifier, then the cookie name, then the default tenant; the verifier
	// is already known to be non-nil and the tenant is already known to be
	// valid, so the only rejection it has left is the cookie name.
	if cfg.defaultTenant != "" {
		if err := cfg.defaultTenant.Validate(); err != nil {
			return nil, &OptionError{Option: "WithDefaultTenant", Err: err}
		}
	}
	credentials, err := internalidentity.NewAuthenticator(internalidentity.Config{
		Verifier:      cfg.verifier,
		Clock:         cfg.clock,
		CookieName:    cfg.cookieName,
		DefaultTenant: cfg.defaultTenant,
	})
	if err != nil {
		return nil, &OptionError{Option: "WithSessionCookieName", Err: err}
	}
	return credentials, nil
}

// composeRouter builds the guard and the router over the composed components.
func composeRouter(cfg config, credentials *internalidentity.Authenticator, parts *components) (*httpapi.Router, error) {
	guard, err := httpapi.NewGuard(httpapi.GuardConfig{
		CSRF:        cfg.csrf,
		Credentials: credentials,
		Clock:       cfg.clock,
	})
	if err != nil {
		return nil, &OptionError{Option: "WithCSRF", Err: err}
	}

	// The user interface is handed to the router as its SPA fallback rather
	// than mounted beside or above it, and that is the whole of the API/SPA
	// precedence rule. The router splits by path BEFORE authentication --
	// bundle assets are public and API routes are not -- and answers everything
	// under the version segment itself, so /v1/unknown is the API's JSON route
	// failure and never the application shell. A composition that consulted the
	// UI first would serve index.html with status 200 there.
	router, err := httpapi.NewRouter(httpapi.RouterConfig{
		Credentials:      credentials,
		Authorizer:       cfg.authorizer,
		Reads:            cfg.reads,
		Directory:        cfg.directory,
		Guard:            guard,
		IDs:              cfg.uuids,
		UI:               cfg.ui,
		UIRoutes:         cfg.uiRoutes,
		AuthorizeUIRoute: cfg.uiRouteAuthorize,
		// The four control routes admit into the composed service, so a
		// deployed binary answers them for real instead of 503.
		Admissions: parts.admissions,
		// The best-effort local wake-up for a command this replica just
		// admitted. It is the routing table rather than the pool: a delivery
		// does not open a route, so a session this replica holds no demand for
		// is a no-op here and the durable record is still the acknowledgement.
		Delivery: parts.bindings,
		Realtime: parts.realtimeHandler,
		Department: func() []httpapi.LaunchTemplate {
			published := make([]httpapi.LaunchTemplate, len(cfg.department))
			for i, template := range cfg.department {
				published[i] = template.published()
			}
			return published
		}(),
		ObjectPolicy:       cfg.objectPolicy,
		ResolveObjectStore: resolveObjectStore(cfg.objectStores),
		ObjectLimits:       cfg.objects,
	})
	if err != nil {
		// Unreachable from a composition New accepted: every value NewRouter
		// validates has been validated above. It is returned rather than
		// dropped because "unreachable" is a claim about today's checks.
		return nil, err
	}
	return router, nil
}

// Handler is Factory's public HTTP surface: the API under /v1, protected
// application routes under /ui/ when supplied, and the injected user interface
// on other paths when supplied.
//
// It is what a LIBRARY embedding uses. Factory does not own the socket in that
// shape -- the embedder supplies the http.Server, and MaxHeaderBytes with it --
// so nothing here reads a listener. A deployment that wants Factory to own the
// server calls Serve instead.
//
// The returned handler is the same one on every call and is safe for concurrent
// use.
func (s *Server) Handler() http.Handler { return s.router }

// UIRoutePrincipal returns the verified principal Factory placed on a request
// before invoking a protected /ui/ route. The principal is a read-only value;
// it does not expose the credential source or a reusable authorization grant.
// A request outside Factory's authenticated handler has no such principal.
func UIRoutePrincipal(r *http.Request) (identity.Principal, bool) {
	if r == nil {
		return identity.Principal{}, false
	}
	operation, ok := internalidentity.OperationContextFrom(r.Context())
	if !ok {
		return identity.Principal{}, false
	}
	return operation.Principal, true
}

// UI reports the optional user interface handler.
//
// The second result is false for a library composition that serves no UI,
// which is a supported configuration rather than a degraded one: the default
// binary mounts the Vite bundle, and an embedder that mounts its own or none
// at all composes the same Server.
func (s *Server) UI() (http.Handler, bool) {
	return s.cfg.ui, s.cfg.ui != nil
}
