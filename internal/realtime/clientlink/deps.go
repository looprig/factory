// Package clientlink holds Factory's browser-facing duplex connection.
//
// The interfaces here are the ones THIS package calls.
package clientlink

import (
	"context"
	"time"

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
	//
	// A DENIAL must satisfy errors.Is(err, internalidentity.ErrUnauthorized),
	// and that is a requirement rather than a description of the implementation
	// that exists. rpcRefusal reads it to answer 103 (permission denied,
	// terminal for this principal); an implementation returning a
	// differently-typed refusal is answered 100 instead, which centrifuge marks
	// TEMPORARY -- a denial dressed as a fault, telling a client to retry an
	// operation it may never perform. Anything else the authorizer returns is a
	// fault and is correctly answered as one.
	AuthorizeControl(ctx context.Context, principal identity.Principal, session sessionwire.SessionID, kind sessionstore.CommandKind) error
}

// Admitter is the durable command plane an authorized RPC is routed into.
//
// It is internal/admission's V1 surface, declared here as the narrow interface
// THIS package calls. Every method takes a Core request type and returns the
// authoritative SessionInbox record, so nothing about a WebSocket, a frame or
// an RPC method name reaches the service: specification section 8.1 makes the
// REST control routes and these RPCs two spellings of ONE admission contract,
// and a seam carrying a transport type would be the first place they could
// diverge.
//
// The `bool` every method returns is admission's "this call is the one that
// accepted it". This edge deliberately does not read it -- see replyFor -- and
// the seam still declares it, because the seam's shape is the service's: an
// interface that dropped the result would need an adapter, and an adapter is a
// second place for the two edges to answer differently.
//
// AdmitLegacyCreate is deliberately ABSENT. It mints identities server-side and
// keeps the legacy unknown-outcome limitation, which is exactly the property a
// ClientLink RPC must not have; it remains a REST compatibility route.
type Admitter interface {
	AdmitCreate(ctx context.Context, principal identity.Principal, req sessionwire.CreateRequest) (sessionstore.DispositionInboxEntry, bool, error)
	AdmitInput(ctx context.Context, principal identity.Principal, req sessionwire.InputRequest) (sessionstore.InboxEntry, bool, error)
	AdmitInterrupt(ctx context.Context, principal identity.Principal, req sessionwire.InterruptRequest) (sessionstore.InboxEntry, bool, error)
	AdmitRestore(ctx context.Context, principal identity.Principal, req sessionwire.RestoreRequest) (sessionstore.InboxEntry, bool, error)
	AdmitGateResponse(ctx context.Context, principal identity.Principal, req sessionwire.GateResponseRequest) (sessionstore.InboxEntry, bool, error)
}

// DemandManager is Factory's local delivery-demand plane: which sessions this
// replica is currently serving to a browser, and therefore which sessions it
// needs a route to.
//
// It is stated in Core's vocabulary and names no routing, transport or binding
// type. The implementation A7.2 supplies (`internal/routing`) answers an
// Acquire with the local route it bound, and that value is not this edge's
// business: a ClientLink knows that a session has a local subscriber, and
// nothing about which Host serves it. Composing the two is one adapter, and it
// is A9.1's, exactly as `routing.Binder`'s doc says of the HostLink pool.
//
// # What an error from either method MEANS
//
// A FAULT, and nothing else. A session with no live owner is not a failure of
// demand -- A7.2 step 1 says a first subscriber must not restore a cold session
// merely for viewing -- so an implementation that answered "no owner" as an
// error would make every viewer of an idle session receive a refused
// subscription. This edge classifies an Acquire failure as an internal,
// TEMPORARY transport error, which is the correct advertisement for a
// dependency that could not be reached and the wrong one for a decision about
// the caller.
type DemandManager interface {
	// Acquire records that this replica has a local subscriber for a session.
	Acquire(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) error

	// Release gives that demand back. It is called once per session, after the
	// last local DeliveryBinding has been gone for the configured debounce.
	Release(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) error
}

// Clock is the time seam, and this package reads exactly one thing from it:
// when to run the debounced release.
//
// It declares AfterFunc and NOT Now, because nothing here reads a wall clock. A
// Now this package never called would be a method every composition supplies
// identically and no test could distinguish. factory.Clock carries both and is
// assignable to this without an adapter, which is the property
// server_test.go's seam pairing holds.
type Clock interface {
	// AfterFunc runs f after d and returns a stop function reporting whether
	// it prevented the call.
	AfterFunc(d time.Duration, f func()) (stop func() bool)
}
