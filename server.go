package factory

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
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
// seam_test.go asserts exactly that: widening a consumer's interface without
// widening the seam fails to compile.

// Authenticator derives an immutable Principal from a request or a ClientLink
// connect credential.
type Authenticator interface {
	AuthenticateRequest(ctx context.Context, r *http.Request) (identity.Principal, error)
	AuthenticateLink(ctx context.Context, token string) (identity.Principal, error)
}

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

// Server is the composed Factory. At this task it holds the validated
// composition and nothing else: routing, identity, admission, placement and
// realtime are later tasks.
type Server struct {
	cfg config
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

	if cfg.ui != nil && cfg.uiFS != nil {
		return nil, ErrConflictingUI
	}

	for _, required := range []struct {
		name    string
		present bool
	}{
		{"WithAuthenticator", cfg.authenticator != nil},
		{"WithAuthorizer", cfg.authorizer != nil},
		{"WithSessionReader", cfg.reads != nil},
		{"WithCommands", cfg.commands != nil},
		{"WithDirectory", cfg.directory != nil},
		{"WithPlacementController", cfg.placement != nil},
		{"WithCSRF", cfg.csrfSet},
	} {
		if !required.present {
			return nil, &OptionError{Option: required.name, Err: ErrMissingDependency}
		}
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

	return &Server{cfg: cfg}, nil
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
