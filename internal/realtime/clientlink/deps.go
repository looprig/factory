// Package clientlink holds Factory's browser-facing duplex connection.
//
// The interfaces here are the ones THIS package calls.
package clientlink

import (
	"context"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	"github.com/looprig/sessionstore"
)

// Authenticator derives a principal from a ClientLink connect token.
type Authenticator interface {
	// AuthenticateLink returns the principal the connect credential names.
	AuthenticateLink(ctx context.Context, token string) (identity.Principal, error)
}

// Authorizer decides subscription and RPC operations on an established link.
type Authorizer interface {
	// AuthorizeSubscribe covers session:{tenant}:{session}. Parsing a channel
	// name grants nothing: the tenant segment is compared to the principal.
	AuthorizeSubscribe(ctx context.Context, principal identity.Principal, channel string) error

	// AuthorizeControl covers session.input, session.interrupt and
	// gate.respond. See the note on httpapi.Authorizer.AuthorizeControl.
	AuthorizeControl(ctx context.Context, principal identity.Principal, session sessionwire.SessionID, kind sessionstore.CommandKind) error
}
