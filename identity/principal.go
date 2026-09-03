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
	"unicode/utf8"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// ErrInvalidPrincipal is the class of every NewPrincipal rejection.
var ErrInvalidPrincipal = errors.New("identity: invalid principal")

// ErrUnauthenticated is what an Authenticator reports when a request or
// ClientLink credential is absent, malformed, unverifiable, or expired.
//
// It is public for the same language reason Principal is: the deployer's
// Authenticator is what decides this, and Factory's edge must be able to
// separate "this caller presented no valid credential", which is answered with
// a challenge, from a failure of the credential service, which is not the
// caller's fault and must not sign every user out.
var ErrUnauthenticated = errors.New("identity: unauthenticated")

// ErrCredentialExpired is the expiry case of ErrUnauthenticated, and it WRAPS
// it, so a caller matching only ErrUnauthenticated needs no change.
//
// It is named separately because it is the one rejection a browser can act on
// without a human: a live session whose credential aged out refreshes and the
// ClientLink reconnects, where an invalid credential must send the user back
// through login. Only whoever verified the credential knows which it was.
var ErrCredentialExpired = fmt.Errorf("%w: credential expired", ErrUnauthenticated)

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
	// Core owns what a tenant identity may be, and this is the last point at
	// which the answer is cheap to refuse: past here the tenant is a prefix of
	// every SessionStore key and SessionObjectStore path the caller reaches, so
	// an over-long or non-UTF-8 one becomes a storage-layer failure attributed
	// to whatever operation happened to run first.
	if err := tenant.Validate(); err != nil {
		return Principal{}, fmt.Errorf("%w: tenant is not a valid identity: %v", ErrInvalidPrincipal, err)
	}
	if subject == "" {
		return Principal{}, fmt.Errorf("%w: subject is empty", ErrInvalidPrincipal)
	}
	// The subject is not a sessionwire identity, so Core does not bound it. It
	// is bounded to the same shape anyway because it is carried in the same
	// records and log lines, and an unbounded one is an unbounded write from
	// whatever minted the credential.
	if len(subject) > sessionwire.MaxIDBytes {
		return Principal{}, fmt.Errorf("%w: subject is %d bytes, want at most %d", ErrInvalidPrincipal, len(subject), sessionwire.MaxIDBytes)
	}
	if !utf8.ValidString(subject) {
		return Principal{}, fmt.Errorf("%w: subject is not valid UTF-8", ErrInvalidPrincipal)
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
