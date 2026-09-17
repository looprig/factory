// Package hostlink holds Factory's client side of the Factory-Host connection.
//
// The interfaces here are the ones THIS package calls. Unlike the
// authenticator, authorizer and store seams, the dialer is NOT part of
// Factory's public option surface: the HostLink protocol is internal to this
// module, and the boundary rules already confine its engine to
// internal/realtime. What composition configures from outside is the pool's
// limits, not its transport.
//
// # What this package is and is not
//
// Pool holds at most one physical connection per Host and multiplexes every
// session binding to that Host over it. It owns the CONTROL plane: bind,
// unbind, command delivery, and the capacity and registry observations a Host
// pushes. It does NOT carry session event data. The per-binding queues and the
// backpressure repair now live ABOVE this package, in
// internal/realtime/delivery and internal/routing's Relay; what is still absent
// here is the LIVE TAIL itself. Core v0.9.1 names the session channel a Host
// publishes on (sessionwire.HostLinkChannel), so the framing gap this package
// used to record is closed for the control plane, but no subscription to that
// channel exists here yet. Nothing here may be read as having solved any of the
// three. It also does not decide WHICH Host a session belongs to; that is
// A7.2's demand-driven binding, which calls Bind and Unbind.
package hostlink

import (
	"context"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// Target is one Host and the address to reach it at.
//
// The two travel together because neither is usable alone: the identity is what
// the pool keys a connection by and what a bind record must name, and the
// endpoint is a durable routing observation that a registry entry or capacity
// report supplied. Core states what a usable endpoint is -- a bounded,
// credential-free WebSocket address -- and Validate defers to it rather than
// restating the rule.
type Target struct {
	Host     sessionwire.HostID
	Endpoint sessionwire.InternalEndpoint
}

// Validate reports why this target may not be dialled.
func (t Target) Validate() error {
	if err := validateHost(t.Host); err != nil {
		return err
	}
	return validateEndpoint(t.Endpoint)
}

// Dialer opens one authenticated, multiplexed connection to a Host. The pool
// calls it; a test substitutes it.
//
// The observer is handed over at DIAL time rather than registered afterwards,
// because a Host may push a capacity report the instant the connection is
// established. A link that had to be told where to send observations after it
// existed would have a window in which it could only drop them.
type Dialer interface {
	Dial(ctx context.Context, target Target, observer Observer) (Link, error)
}

// Link is one live Factory-Host connection. Session bindings and RPCs are
// multiplexed over it, so the pool holds at most one per Host.
type Link interface {
	// Host is the Host this link was dialled for.
	Host() sessionwire.HostID
	// Bind establishes one Factory-local route to a Host-owned session. It is
	// a routing optimization and not a claim of ownership: the Host validates
	// the tuple against its own durable lease and may refuse.
	Bind(ctx context.Context, req sessionwire.HostLinkBindRequest) error
	// Unbind releases one route.
	Unbind(ctx context.Context, req sessionwire.HostLinkUnbindRequest) error
	// DeliverCommand hands an already committed inbox record's public command
	// id to the Host. The tenant and session travel beside the record because
	// Core's framing makes the RPC METHOD the session's channel,
	// sessionwire.HostLinkChannel(tenant, session); the body names no session.
	// See Pool.DeliverCommand for what a failure means.
	DeliverCommand(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, req sessionwire.HostLinkCommandDelivery) error
	// Close releases the connection.
	Close(ctx context.Context) error
}

// Observer receives what a Host pushes about itself and about the sessions it
// owns.
//
// Both methods are called from the transport's read path, so an implementation
// that blocks stalls the connection. Neither may return an error: there is
// nothing a link could do with one, and a link that dropped a connection
// because a downstream consumer disliked an observation would make a
// reconciliation preference into an availability failure.
type Observer interface {
	// ObserveCapacity receives one Host-level advertisement.
	ObserveCapacity(report sessionwire.HostLinkCapacityReport)
	// ObserveRegistry receives one per-session ownership observation.
	ObserveRegistry(observation sessionwire.HostLinkRegistryObservation)
}

// Credential supplies the SERVICE identity Factory presents to a Host.
//
// It is deliberately not identity.Principal: a HostLink carries no end user.
// The token authenticates one Factory replica to one Host, and the Host's own
// authorization of a bind is made against the session's durable lease, not
// against a user this connection claims to act for.
type Credential interface {
	// ServiceToken returns the credential to present, or an error if one
	// cannot be obtained. It is called on every dial and on every reconnect,
	// so a short-lived token is refreshed rather than replayed.
	ServiceToken(ctx context.Context) (string, error)
}

// discardObserver is what a pool composed without an Observer hands the dialer.
//
// A nil Observer at the link would panic on the first report a Host pushes,
// which is a crash on the happy path of a deployment that simply did not want
// the reports. Refusing to compose without one would be the other extreme: the
// pool's control plane works without any consumer of the observations.
type discardObserver struct{}

func (discardObserver) ObserveCapacity(sessionwire.HostLinkCapacityReport)      {}
func (discardObserver) ObserveRegistry(sessionwire.HostLinkRegistryObservation) {}
