// Package identity holds the tenancy vocabulary that Factory's composition
// seams exchange.
//
// It is a public leaf package rather than an internal one for a reason that is
// a language constraint, not a preference: an Authenticator or Authorizer
// supplied by a deployer must be able to NAME the type in its own method
// signatures, and Go forbids a package outside this module from importing
// github.com/looprig/factory/internal/... . Everything else about the seams
// stays with the package that calls them.
package identity

import (
	"errors"
	"fmt"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// ErrInvalidPrincipal is the class of every NewPrincipal rejection.
var ErrInvalidPrincipal = errors.New("identity: invalid principal")

// Kind separates a human actor from a Factory service identity. The
// cross-tenant reconciliation sweep is authorized for a service identity and
// for nothing else, so the distinction is part of the principal rather than a
// property a handler recomputes.
type Kind string

const (
	// KindActor is a principal acting for one tenant.
	KindActor Kind = "actor"
	// KindService is a Factory service identity performing control-plane work.
	KindService Kind = "service"
)

// Principal is an authenticated caller.
//
// Every field is unexported and read through a method, so a handler holding one
// cannot widen its tenant or promote it to a service identity. That is the
// whole of the immutability claim: it is a value with no reference members, so
// a copy shares nothing with its original.
type Principal struct {
	tenant  sessionwire.TenantID
	subject string
	kind    Kind
}

// NewPrincipal builds a principal from verified credentials.
//
// A rejection returns the ZERO Principal, not a partially built one: a caller
// that ignores the error must not end up holding a usable tenant scope.
func NewPrincipal(tenant sessionwire.TenantID, subject string, kind Kind) (Principal, error) {
	if tenant == "" {
		return Principal{}, fmt.Errorf("%w: tenant is empty", ErrInvalidPrincipal)
	}
	if subject == "" {
		return Principal{}, fmt.Errorf("%w: subject is empty", ErrInvalidPrincipal)
	}
	switch kind {
	case KindActor, KindService:
	default:
		return Principal{}, fmt.Errorf("%w: kind %q is neither %q nor %q", ErrInvalidPrincipal, kind, KindActor, KindService)
	}
	return Principal{tenant: tenant, subject: subject, kind: kind}, nil
}

// Tenant is the tenant scope every later read and write is confined to.
func (p Principal) Tenant() sessionwire.TenantID { return p.tenant }

// Subject identifies the caller within its tenant.
func (p Principal) Subject() string { return p.subject }

// Kind reports whether this is an actor or a service identity.
func (p Principal) Kind() Kind { return p.kind }

// IsService reports whether this principal may perform control-plane work.
func (p Principal) IsService() bool { return p.kind == KindService }
