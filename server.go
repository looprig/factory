package factory

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"slices"
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

// Commands is the durable command plane.
type Commands interface {
	AdmitCommand(ctx context.Context, req sessionstore.AdmitCommandRequest) (sessionstore.InboxEntry, bool, error)
	GetCommand(ctx context.Context, req sessionstore.GetCommandRequest) (sessionstore.InboxEntry, error)
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

// PlacementController drives desired placement.
type PlacementController interface {
	EnsurePlacement(ctx context.Context, desired sessionstore.DesiredWorkload) error
	ReleasePlacement(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) error
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
	cfg    config
	router *httpapi.Router
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
		{"WithPlacementController", cfg.placement != nil},
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

	if err := cfg.csrf.Validate(); err != nil {
		return nil, &OptionError{Option: "WithCSRF", Err: err}
	}
	for _, limits := range []struct {
		name string
		err  error
	}{
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

	// A static bundle becomes a handler at composition time, so UI() has one
	// answer shape and a serving path that does not branch on which option the
	// deployer used.
	if cfg.uiFS != nil {
		cfg.ui = http.FileServerFS(cfg.uiFS)
	}

	router, err := composeRouter(cfg)
	if err != nil {
		return nil, err
	}
	return &Server{cfg: cfg, router: router}, nil
}

// composeRouter builds the authenticator, the guard and the router.
//
// It runs at composition rather than at the first request, so a deployment that
// cannot serve fails where an operator is watching. Every rejection below is
// attributed to the option that carries the offending value.
func composeRouter(cfg config) (*httpapi.Router, error) {
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
		Credentials: credentials,
		Authorizer:  cfg.authorizer,
		Reads:       cfg.reads,
		Directory:   cfg.directory,
		Guard:       guard,
		IDs:         cfg.uuids,
		UI:          cfg.ui,
	})
	if err != nil {
		// Unreachable from a composition New accepted: every value NewRouter
		// validates has been validated above. It is returned rather than
		// dropped because "unreachable" is a claim about today's checks.
		return nil, err
	}
	return router, nil
}

// Handler is Factory's public HTTP surface: the API under /v1, and the injected
// user interface everywhere else when one was supplied.
//
// It is what a LIBRARY embedding uses. Factory does not own the socket in that
// shape -- the embedder supplies the http.Server, and MaxHeaderBytes with it --
// so nothing here reads a listener. A deployment that wants Factory to own the
// server calls Serve instead.
//
// The returned handler is the same one on every call and is safe for concurrent
// use.
func (s *Server) Handler() http.Handler { return s.router }

// UI reports the optional user interface handler.
//
// The second result is false for a library composition that serves no UI,
// which is a supported configuration rather than a degraded one: the default
// binary mounts the Vite bundle, and an embedder that mounts its own or none
// at all composes the same Server.
func (s *Server) UI() (http.Handler, bool) {
	return s.cfg.ui, s.cfg.ui != nil
}
