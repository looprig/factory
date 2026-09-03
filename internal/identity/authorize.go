package identity

import (
	"context"
	"errors"
	"regexp"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	factoryidentity "github.com/looprig/factory/identity"
	"github.com/looprig/sessionstore"
)

// ErrUnauthorized is the complete result of a denied authorization decision.
// It deliberately carries no tenant, session, command, object or channel value:
// a denial must not disclose whether the named resource exists in another
// tenant. Callers may add operation names, but must not add those identifiers.
var ErrUnauthorized = errors.New("identity: unauthorized")

// Authorizer applies Factory's tenant boundary to its public edges.
//
// Its zero value is ready to use and holds no resource directory. Tenant-local
// identifiers are not globally unique and therefore are not authority: after a
// successful decision, handlers must build every SessionStore request with the
// authenticated Principal's Tenant. In particular, looking up a SessionID,
// CommandID or ObjectID before authorizing would both cross that boundary and
// disclose existence.
type Authorizer struct{}

// AuthorizeSessionList authorizes the principal's own tenant-scoped list.
func (Authorizer) AuthorizeSessionList(_ context.Context, principal factoryidentity.Principal) error {
	return authorizeTenantPrincipal(principal)
}

// AuthorizeSessionRead authorizes a read within the principal's tenant. The
// session is intentionally not inspected: it is opaque and tenant-local.
func (Authorizer) AuthorizeSessionRead(_ context.Context, principal factoryidentity.Principal, _ sessionwire.SessionID) error {
	return authorizeTenantPrincipal(principal)
}

// AuthorizeObjectRead authorizes an object read within the principal's tenant.
// The session and object identifiers are intentionally not inspected: both are
// opaque and tenant-local.
func (Authorizer) AuthorizeObjectRead(_ context.Context, principal factoryidentity.Principal, _ sessionwire.SessionID, _ sessionwire.ObjectReference) error {
	return authorizeTenantPrincipal(principal)
}

// AuthorizeControl authorizes a command within the principal's tenant. The
// session and command kind are validated by the public protocol and admission
// layers; neither selects a tenant here.
func (Authorizer) AuthorizeControl(_ context.Context, principal factoryidentity.Principal, _ sessionwire.SessionID, _ sessionstore.CommandKind) error {
	return authorizeTenantPrincipal(principal)
}

// AuthorizeSubscribe authorizes a canonical session:{tenant}:{session}
// channel. Parsing is completed independently before the parsed tenant is
// compared with the principal; a syntactically valid channel is not a grant.
//
// It is the one tenant-scoped seam that does not call authorizeTenantPrincipal,
// and it needs no separate empty-tenant check because the grammar subsumes one:
// sessionChannelPattern's [^:]+ capture cannot match an empty string, so a
// parsed tenant that is "" is never valid, and an unconstructed Principal --
// whose tenant is "" -- therefore never equals a valid parsed tenant. The
// "subscribe" row of
// TestAuthorizerRejectsAnUnconstructedPrincipalForOperationsWithoutOpaqueValues
// exercises that case.
func (Authorizer) AuthorizeSubscribe(_ context.Context, principal factoryidentity.Principal, channel string) error {
	return authorizationResult(sessionChannelAllowed(principal.Tenant(), parseSessionChannel(channel)))
}

// AuthorizeServiceSweep authorizes the cross-tenant reconciliation sweep. It
// is the only decision that is not confined to the principal's own tenant and
// consequently requires a service identity.
func (Authorizer) AuthorizeServiceSweep(_ context.Context, principal factoryidentity.Principal) error {
	return authorizationResult(principal.IsService())
}

// authorizeTenantPrincipal is the sole check on the four tenant-scoped seams
// that take no channel: an authenticated principal is confined to its own
// tenant, so holding any non-empty tenant is the whole of the decision.
//
// The emptiness test is sufficient ONLY because factoryidentity.NewPrincipal is
// the sole constructor of a usable Principal -- it rejects an empty or invalid
// tenant and an empty subject, and returns the ZERO Principal on any rejection
// -- and because Principal's fields are unexported, so no package outside
// factory/identity can build a partially populated one. A composite literal
// written INSIDE package factory/identity would invalidate that: it can set a
// tenant without a subject, or a subject without a tenant. Two mutants that are
// equivalent today would then become live authorization bypasses with no test
// to catch them -- replacing Tenant() with Subject() here, and adding a
// magic-subject early grant -- because the analyzer's checks list in
// authorize_flow_test.go does not cover this function. Do not construct a
// Principal composite literal in this package without first pinning that
// invariant.
func authorizeTenantPrincipal(principal factoryidentity.Principal) error {
	return authorizationResult(principal.Tenant() != "")
}

func authorizationResult(allowed bool) error {
	if !allowed {
		return ErrUnauthorized
	}
	return nil
}

// parsedSessionChannel is the result of parseSessionChannel. Its tenant and
// session are meaningful only when valid is true. A channel that does not match
// the grammar yields the zero value; a channel that matches the grammar but
// whose segments fail TenantID/SessionID validation yields those captured
// segments with valid false, so an unvalidated tenant can be present in the
// struct. sessionChannelAllowed is the sole production reader of tenant and
// valid, and is the only place permitted to consult them. session has no
// production reader at all: it is captured for symmetry with the grammar and
// consumed once inside parseSessionChannel to compute valid, and nothing reads
// the stored field afterwards. What holds it in place is the analyzer's
// three-field shape check in authorize_flow_test.go, not a caller.
type parsedSessionChannel struct {
	tenant  sessionwire.TenantID
	session sessionwire.SessionID
	valid   bool
}

var sessionChannelPattern = regexp.MustCompile(`\Asession:([^:]+):([^:]+)\z`)

// sessionChannelAllowed compares the parsed tenant with the principal's. The
// parsed.valid conjunct is load-bearing, not defensive: it is what stops the
// unvalidated tenant described above from being compared. Dropping it is a
// behaviour change, not a simplification: it makes AuthorizeSubscribe admit
// "session:tenant-a:\xff", which
// TestSubscribeParsesBeforeComparingAndParsingDoesNotGrantAccess rejects.
func sessionChannelAllowed(principalTenant sessionwire.TenantID, parsed parsedSessionChannel) bool {
	return parsed.valid && parsed.tenant == principalTenant
}

func parseSessionChannel(channel string) parsedSessionChannel {
	matches := sessionChannelPattern.FindStringSubmatch(channel)
	if len(matches) != 3 {
		return parsedSessionChannel{}
	}
	tenant := sessionwire.TenantID(matches[1])
	session := sessionwire.SessionID(matches[2])
	return parsedSessionChannel{
		tenant:  tenant,
		session: session,
		valid:   tenant.Validate() == nil && session.Validate() == nil,
	}
}
