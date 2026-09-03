// Package hostlink holds Factory's client side of the Factory-Host connection.
//
// The interfaces here are the ones THIS package calls. Unlike the
// authenticator, authorizer and store seams, the dialer is NOT part of
// Factory's public option surface: the HostLink protocol is internal to this
// module, and the boundary rules already confine its engine to
// internal/realtime. What composition configures from outside is the pool's
// limits, not its transport.
package hostlink

import (
	"context"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// Dialer opens one authenticated, multiplexed connection to a Host. The pool
// calls it; a test substitutes it.
type Dialer interface {
	Dial(ctx context.Context, host sessionwire.HostID, endpoint sessionwire.InternalEndpoint) (Link, error)
}

// Link is one live Factory-Host connection. Session bindings and RPCs are
// multiplexed over it, so the pool holds at most one per Host.
type Link interface {
	// Host is the Host this link was dialled for.
	Host() sessionwire.HostID
	// Close releases the connection.
	Close(ctx context.Context) error
}
