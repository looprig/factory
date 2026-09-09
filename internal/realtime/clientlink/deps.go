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
	AdmitCreate(ctx context.Context, principal identity.Principal, req sessionwire.CreateRequest) (sessionstore.InboxEntry, bool, error)
	AdmitInput(ctx context.Context, principal identity.Principal, req sessionwire.InputRequest) (sessionstore.InboxEntry, bool, error)
	AdmitInterrupt(ctx context.Context, principal identity.Principal, req sessionwire.InterruptRequest) (sessionstore.InboxEntry, bool, error)
	AdmitRestore(ctx context.Context, principal identity.Principal, req sessionwire.RestoreRequest) (sessionstore.InboxEntry, bool, error)
	AdmitGateResponse(ctx context.Context, principal identity.Principal, req sessionwire.GateResponseRequest) (sessionstore.InboxEntry, bool, error)
}
