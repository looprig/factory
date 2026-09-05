// Package admission accepts commands durably and reconciles the ones with no
// live owner.
//
// The interfaces here are the ones THIS package calls. Directory and
// PlacementController are declared here, not in internal/routing and
// internal/placement, because admission is the caller: routing and placement
// supply implementations of a contract their consumer states.
package admission

import (
	"context"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	"github.com/looprig/sessionstore"
)

// Authorizer decides both tenant command admission and the service sweep.
// Admission repeats the edge's control decision deliberately: it is a durable
// mutation boundary that can later be called by transports other than HTTP and
// ClientLink, and no such caller may acquire an authorization-free path.
type Authorizer interface {
	AuthorizeControl(ctx context.Context, principal identity.Principal, session sessionwire.SessionID, kind sessionstore.CommandKind) error

	// AuthorizeServiceSweep covers the cross-tenant due-work sweep over the
	// service-control shards. It is never reachable by a tenant principal and
	// its records never become a cross-tenant public response.
	AuthorizeServiceSweep(ctx context.Context, principal identity.Principal) error
}

// Commands is the durable command plane admission uses.
type Commands interface {
	AdmitCommand(ctx context.Context, req sessionstore.AdmitCommandRequest) (sessionstore.InboxEntry, bool, error)
	GetCommand(ctx context.Context, req sessionstore.GetCommandRequest) (sessionstore.InboxEntry, error)
	RejectCommand(ctx context.Context, req sessionstore.RejectCommandRequest) (sessionstore.InboxEntry, error)
	ListDueCommands(ctx context.Context, req sessionstore.ListDueCommandsRequest) (sessionstore.DueCommandPage, error)
	AcquireReconciliationClaim(ctx context.Context, req sessionstore.AcquireReconciliationClaimRequest) (sessionstore.ReconciliationClaimEntry, error)
	ReleaseReconciliationClaim(ctx context.Context, req sessionstore.ReleaseReconciliationClaimRequest) (sessionstore.ReconciliationClaimEntry, error)
}

// Directory is the observed target directory: which Host currently owns a
// session, and which Hosts could accept one.
type Directory interface {
	// Owner reports the observed registration for a session. The boolean is
	// false when no owner is observed, which is not an error.
	Owner(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) (sessionwire.HostLinkRegistryObservation, bool, error)

	// Candidates pages compatible accepting Hosts. Capacity is not authority:
	// a candidate confers nothing until the session lease is acquired.
	Candidates(ctx context.Context, req sessionstore.ListCompatibleHostsRequest) (sessionstore.HostTargetPage, error)
}

// PlacementController drives desired placement. It is idempotent in both
// directions: a repeated Ensure for the same desired workload creates nothing
// twice, and a repeated Release is not an error.
type PlacementController interface {
	EnsurePlacement(ctx context.Context, desired sessionstore.DesiredWorkload) error
	ReleasePlacement(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) error
}

// Clock is the time seam.
//
// It names only standard-library and builtin types on purpose. Factory's public
// option surface declares an identical Clock, and a shape with a named Timer
// type would make the two interfaces mutually unassignable across the
// internal/ boundary; AfterFunc's stop function is an unnamed func type, so
// they remain the same interface without an adapter.
type Clock interface {
	Now() time.Time
	// AfterFunc runs f after d and returns a stop function reporting whether
	// it prevented the call.
	AfterFunc(d time.Duration, f func()) (stop func() bool)
}

// UUIDSource supplies the random identifiers Factory proposes, notably the
// SessionID it allocates before a Host exists to allocate one.
type UUIDSource interface {
	NewUUID() (string, error)
}
