// Package routing reads the durable routing and capacity records that Factory
// uses to find Hosts. Capacity advertisements are placement hints only;
// session ownership comes exclusively from the epoch-fenced registry.
package routing

import (
	"context"
	"errors"
	"fmt"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"
)

// ErrInvalidConfig reports a Directory that cannot keep its advertised work
// bounds. Configuration is checked by NewDirectory before any store call.
var ErrInvalidConfig = errors.New("routing: invalid directory configuration")

// Store is the SessionStore surface used by Directory. Keeping the registry
// read separate from capacity listing makes it impossible to turn an
// advertisement into a session ownership claim inside this package.
type Store interface {
	GetHostRegistration(context.Context, sessionstore.GetHostRegistrationRequest) (sessionstore.HostRegistrationEntry, error)
	ListCompatibleHosts(context.Context, sessionstore.ListCompatibleHostsRequest) (sessionstore.HostTargetPage, error)
	ReconcileHostTargets(context.Context, sessionstore.ReconcileHostTargetsRequest) (sessionstore.HostTargetReconcileResult, error)
}

// Limits bounds both the foreground capacity query and the due cleanup it can
// provoke. ReconcileMaxPages is a total budget shared by every cleanup attempt,
// not a per-attempt allowance.
type Limits struct {
	CandidatePageLimit int
	ReconcilePageLimit int
	ReconcileMaxPages  int
	MaxCleanupAttempts int
}

// DefaultLimits returns bounded defaults for target discovery.
func DefaultLimits() Limits {
	return Limits{
		CandidatePageLimit: 32,
		ReconcilePageLimit: 32,
		ReconcileMaxPages:  sessionstore.DefaultHostTargetReconcilePages,
		MaxCleanupAttempts: 2,
	}
}

// Directory discovers placement candidates and the current owner of a
// session. It stores no routing state of its own.
type Directory struct {
	store  Store
	limits Limits
}

// NewDirectory validates all work bounds before constructing a Directory.
func NewDirectory(store Store, limits Limits) (*Directory, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: Store must not be nil", ErrInvalidConfig)
	}
	if limits.CandidatePageLimit < 1 || limits.CandidatePageLimit > storage.MaxOrderedPageLimit {
		return nil, fmt.Errorf("%w: CandidatePageLimit must be between 1 and %d", ErrInvalidConfig, storage.MaxOrderedPageLimit)
	}
	if limits.ReconcilePageLimit < 1 || limits.ReconcilePageLimit > storage.MaxOrderedPageLimit {
		return nil, fmt.Errorf("%w: ReconcilePageLimit must be between 1 and %d", ErrInvalidConfig, storage.MaxOrderedPageLimit)
	}
	if limits.ReconcileMaxPages < 1 || limits.ReconcileMaxPages > sessionstore.MaxHostTargetReconcilePages {
		return nil, fmt.Errorf("%w: ReconcileMaxPages must be between 1 and %d", ErrInvalidConfig, sessionstore.MaxHostTargetReconcilePages)
	}
	if limits.MaxCleanupAttempts < 1 || limits.MaxCleanupAttempts > limits.ReconcileMaxPages {
		return nil, fmt.Errorf("%w: MaxCleanupAttempts must be between 1 and ReconcileMaxPages", ErrInvalidConfig)
	}
	return &Directory{store: store, limits: limits}, nil
}

// Owner returns the live epoch-fenced registry observation for a session.
// Absence, expiry and a released tombstone all mean that no owner is routable;
// their different store codes remain an operational concern, not three kinds
// of ownership.
func (d *Directory) Owner(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) (sessionwire.HostLinkRegistryObservation, bool, error) {
	entry, err := d.store.GetHostRegistration(ctx, sessionstore.GetHostRegistrationRequest{TenantID: tenant, SessionID: session})
	if err != nil {
		var registryErr *sessionstore.RegistryError
		if errors.As(err, &registryErr) {
			switch registryErr.Code {
			case sessionstore.RegistryErrorNotFound, sessionstore.RegistryErrorExpired, sessionstore.RegistryErrorReleased:
				return sessionwire.HostLinkRegistryObservation{}, false, nil
			}
		}
		return sessionwire.HostLinkRegistryObservation{}, false, err
	}
	observation, err := entry.Registration.Observation()
	if err != nil {
		return sessionwire.HostLinkRegistryObservation{}, false, err
	}
	return observation, true, nil
}

// Candidates returns compatible accepting Hosts in SessionStore's capacity
// order. The store owns target scoping, liveness, accepting-state filtering and
// generation fencing; this adapter does not restate those rules.
//
// A lapsed row still occupies a ranked position. When a page reports one,
// Directory spends a bounded slice of one total due-page budget, then retries
// the caller's original position. It never follows the candidate continuation
// internally, so the continuation returned to the caller is always the honest
// continuation of the page it received.
func (d *Directory) Candidates(ctx context.Context, original sessionstore.ListCompatibleHostsRequest) (sessionstore.HostTargetPage, error) {
	req := original
	if req.Limit == 0 || req.Limit > d.limits.CandidatePageLimit {
		req.Limit = d.limits.CandidatePageLimit
	}

	remainingPages := d.limits.ReconcileMaxPages
	remainingAttempts := d.limits.MaxCleanupAttempts
	var dueCursor sessionwire.Cursor
	for {
		page, err := d.store.ListCompatibleHosts(ctx, req)
		if err != nil {
			return sessionstore.HostTargetPage{}, err
		}
		if page.LapsedSkipped == 0 || remainingAttempts == 0 || remainingPages == 0 {
			return page, nil
		}

		// Divide the remaining total page budget among the remaining attempts.
		// Ceiling division reserves at least one page for every later attempt
		// while allowing the complete configured budget to be spent.
		pagesThisAttempt := (remainingPages + remainingAttempts - 1) / remainingAttempts
		result, err := d.store.ReconcileHostTargets(ctx, sessionstore.ReconcileHostTargetsRequest{
			Limit: d.limits.ReconcilePageLimit, MaxPages: pagesThisAttempt, Cursor: dueCursor,
		})
		if err != nil {
			return sessionstore.HostTargetPage{}, err
		}
		remainingPages -= pagesThisAttempt
		remainingAttempts--
		if result.Exhausted {
			dueCursor = ""
		} else {
			dueCursor = result.NextCursor
		}
	}
}
