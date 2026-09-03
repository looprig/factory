package identity

import (
	"context"
	"errors"
	"strings"

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
func (Authorizer) AuthorizeSubscribe(_ context.Context, principal factoryidentity.Principal, channel string) error {
	tenant, _, ok := parseSessionChannel(channel)
	if !ok || authorizeTenantPrincipal(principal) != nil || tenant != principal.Tenant() {
		return ErrUnauthorized
	}
	return nil
}

// AuthorizeServiceSweep authorizes the cross-tenant reconciliation sweep. It
// is the only decision that is not confined to the principal's own tenant and
// consequently requires a service identity.
func (Authorizer) AuthorizeServiceSweep(_ context.Context, principal factoryidentity.Principal) error {
	if authorizeTenantPrincipal(principal) != nil || !principal.IsService() {
		return ErrUnauthorized
	}
	return nil
}

func authorizeTenantPrincipal(principal factoryidentity.Principal) error {
	if principal.Tenant() == "" {
		return ErrUnauthorized
	}
	return nil
}

func parseSessionChannel(channel string) (sessionwire.TenantID, sessionwire.SessionID, bool) {
	parts := strings.Split(channel, ":")
	if len(parts) != 3 || parts[0] != "session" {
		return "", "", false
	}
	tenant := sessionwire.TenantID(parts[1])
	session := sessionwire.SessionID(parts[2])
	if tenant.Validate() != nil || session.Validate() != nil {
		return "", "", false
	}
	return tenant, session, true
}
