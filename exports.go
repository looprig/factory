package factory

import (
	"context"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	internalidentity "github.com/looprig/factory/internal/identity"
	"github.com/looprig/factory/internal/routing"
	"github.com/looprig/sessionstore"
)

// The exported implementations of two seams this module has always
// implemented internally, for a composition root that lives OUTSIDE it.
//
// factory.New takes thirteen seams, and until v0.2.0 a caller in another
// module had no usable implementation of two of them: the Authorizer lived in
// internal/identity and the Directory in internal/routing, and Go forbids a
// package outside github.com/looprig/factory from importing either. The
// consequence was measured rather than predicted -- the tests module had to
// hand-reimplement the store directory to compose a Factory at all -- so both
// are exported here as thin wrappers, which freezes nothing new: the Authorizer
// and Directory INTERFACES have been public since v0.1.0, and what a caller
// gets is the same value this module's own composition would have used.
//
// They are declared in the ROOT package and not in factory/identity, and that
// is a language constraint rather than a taste: internal/identity imports
// factory/identity for Principal, so an implementation in factory/identity
// that wrapped internal/identity would be an import cycle. The root package
// already declares the Authorizer and Directory interfaces they satisfy, and
// already imports both internal packages.

// TenantAuthorizer is the tenant-boundary Authorizer this module composes for
// its own tests and the one a deployment with no finer policy uses.
//
// Its zero value is ready to use and it holds no state. Every tenant-scoped
// decision is "the principal holds a tenant", every channel decision is "the
// channel's tenant is the principal's", and the one cross-tenant decision --
// the service sweep -- requires a service principal. It reads none of the
// resource identifiers it is handed, because a tenant-local identifier is not
// authority and consulting it before authorizing would disclose existence; the
// tenant boundary is then applied by every SessionStore request being built
// with the authenticated principal's tenant. A denial is identity.ErrUnauthorized
// and carries no identifier.
//
// It forwards to internal/identity.Authorizer method for method rather than
// embedding it, so the exported surface names no internal type;
// TestTenantAuthorizerIsTheInternalAuthorizer holds the two method sets equal.
type TenantAuthorizer struct{}

var _ Authorizer = TenantAuthorizer{}
var _ AuditAuthorizer = TenantAuthorizer{}

func (TenantAuthorizer) AuthorizeAuditRead(ctx context.Context, principal identity.Principal, session sessionwire.SessionID) error {
	return internalidentity.Authorizer{}.AuthorizeAuditRead(ctx, principal, session)
}

func (TenantAuthorizer) AuthorizeSessionList(ctx context.Context, principal identity.Principal) error {
	return internalidentity.Authorizer{}.AuthorizeSessionList(ctx, principal)
}

func (TenantAuthorizer) AuthorizeSessionRead(ctx context.Context, principal identity.Principal, session sessionwire.SessionID) error {
	return internalidentity.Authorizer{}.AuthorizeSessionRead(ctx, principal, session)
}

func (TenantAuthorizer) AuthorizeObjectRead(ctx context.Context, principal identity.Principal, session sessionwire.SessionID, object sessionwire.ObjectReference) error {
	return internalidentity.Authorizer{}.AuthorizeObjectRead(ctx, principal, session, object)
}

func (TenantAuthorizer) AuthorizeControl(ctx context.Context, principal identity.Principal, session sessionwire.SessionID, kind sessionstore.CommandKind) error {
	return internalidentity.Authorizer{}.AuthorizeControl(ctx, principal, session, kind)
}

func (TenantAuthorizer) AuthorizeSubscribe(ctx context.Context, principal identity.Principal, channel string) error {
	return internalidentity.Authorizer{}.AuthorizeSubscribe(ctx, principal, channel)
}

func (TenantAuthorizer) AuthorizeServiceSweep(ctx context.Context, principal identity.Principal) error {
	return internalidentity.Authorizer{}.AuthorizeServiceSweep(ctx, principal)
}

// DirectoryStore is the SessionStore surface NewStoreDirectory reads: the
// per-session registry, the ranked target listing, and the due-row reconcile
// that a lapsed advertisement provokes. A *sessionstore.Store satisfies it.
//
// The registry read and the capacity listing are separate methods here for the
// reason internal/routing keeps them separate: it makes turning an
// advertisement into an ownership claim impossible inside the directory,
// because nothing on this seam can answer "who owns the session" from a
// capacity row.
type DirectoryStore interface {
	GetHostRegistration(context.Context, sessionstore.GetHostRegistrationRequest) (sessionstore.HostRegistrationEntry, error)
	ListCompatibleHosts(context.Context, sessionstore.ListCompatibleHostsRequest) (sessionstore.HostTargetPage, error)
	ReconcileHostTargets(context.Context, sessionstore.ReconcileHostTargetsRequest) (sessionstore.HostTargetReconcileResult, error)
}

var _ DirectoryStore = (*sessionstore.Store)(nil)

// DirectoryLimits bounds the store directory's foreground capacity query and
// the due cleanup it may provoke. It is an alias for the reason ObjectLimits
// is: one declaration, read by the package that enforces it.
type DirectoryLimits = routing.Limits

// DefaultDirectoryLimits is what a composition naming no directory limits
// receives: a 32-row candidate page, 32-row reconcile pages, SessionStore's
// default reconcile page budget, and two cleanup attempts per query.
func DefaultDirectoryLimits() DirectoryLimits { return routing.DefaultLimits() }

// ErrInvalidDirectoryLimits is the class of every NewStoreDirectory rejection.
var ErrInvalidDirectoryLimits = routing.ErrInvalidConfig

// NewStoreDirectory returns the Directory this module reads a SessionStore
// through: session ownership from the epoch-fenced registry, and placement
// candidates from the ranked target index in the store's own capacity order.
//
// It holds no routing state. Absence, expiry and a released tombstone are all
// "no owner" rather than errors, a lapsed candidate row spends a bounded slice
// of the reconcile budget and the original page is retried, and the
// continuation handed back is always the honest continuation of the page the
// caller received. Limits are validated before any store call; a zero
// DirectoryLimits is refused rather than defaulted, because a directory that
// silently took a page size a caller did not name is one whose cost the caller
// cannot state. Use DefaultDirectoryLimits.
func NewStoreDirectory(store DirectoryStore, limits DirectoryLimits) (Directory, error) {
	directory, err := routing.NewDirectory(store, limits)
	if err != nil {
		return nil, err
	}
	return directory, nil
}
