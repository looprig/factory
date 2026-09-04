// Package httpapi serves Factory's public REST plane.
//
// The interfaces here are the ones THIS package calls. They are declared here
// rather than in a shared dependency package so that widening what a handler
// needs is visible as a change to the handler's own contract, and so that no
// package acquires a dependency by importing a locator.
package httpapi

import (
	"context"
	"io"
	"net/http"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	"github.com/looprig/sessionstore"
)

// Authenticator derives a principal from an inbound HTTP request.
//
// Nothing in this package calls it, and that is deliberate rather than an
// oversight. Router takes the CONCRETE *internalidentity.Authenticator, for the
// reason GuardConfig.Credentials does: the operation context must record which
// credential authenticated the request, NewOperationContext is a method so that
// answer comes from the same derivation the authentication used, and an
// interface here would be a second place for an edge to state it wrongly -- a
// wrong answer there is a CSRF guard that skips.
//
// What this declaration is FOR is the public seam. A deployer implements
// factory.Authenticator from outside the module, and
// TestPublicSeamsAreExactlyTheUnionOfTheirConsumers derives that seam from the
// narrow interfaces its consumers declare, so this is where
// AuthenticateRequest's membership in it comes from. Deleting it would silently
// narrow the public surface a composition (A9.1) has to satisfy.
type Authenticator interface {
	// AuthenticateRequest returns the principal the request's verified
	// credentials name. It never reads a tenant from the body, query or path.
	AuthenticateRequest(ctx context.Context, r *http.Request) (identity.Principal, error)
}

// Authorizer decides every public REST operation. A nil error is the only
// grant; the caller never infers permission from the absence of a decision.
type Authorizer interface {
	// AuthorizeSessionList covers the tenant-scoped session list.
	AuthorizeSessionList(ctx context.Context, principal identity.Principal) error

	// AuthorizeSessionRead covers status, journal and gate reads.
	AuthorizeSessionRead(ctx context.Context, principal identity.Principal, session sessionwire.SessionID) error

	// AuthorizeObjectRead covers a session-scoped object body.
	AuthorizeObjectRead(ctx context.Context, principal identity.Principal, session sessionwire.SessionID, object sessionwire.ObjectReference) error

	// AuthorizeControl covers create, input, interrupt, restore and gate
	// response. Its signature is identical to the ClientLink authorizer's
	// because §8.1 makes the REST control routes and the ClientLink RPCs
	// alternatives obeying one admission contract, so one implementation
	// answers both.
	AuthorizeControl(ctx context.Context, principal identity.Principal, session sessionwire.SessionID, kind sessionstore.CommandKind) error
}

// SessionReader is the durable read plane the handlers use.
//
// It is a SessionStore DOMAIN interface: no method names a
// github.com/looprig/storage primitive, so a handler cannot reach a Ledger,
// Leaser, KV or Blobs value and re-derive a record the aggregate owns.
type SessionReader interface {
	ListSessions(ctx context.Context, req sessionstore.ListSessionsRequest) (sessionstore.SessionPage, error)
	GetCatalogEntry(ctx context.Context, req sessionstore.GetCatalogEntryRequest) (sessionstore.CatalogEntry, error)
	ReadPublicJournal(ctx context.Context, req sessionstore.ReadPublicJournalRequest) (sessionwire.JournalPage, error)
	ReadGates(ctx context.Context, req sessionstore.ReadGatesRequest) (sessionwire.GatePage, error)
	GetObject(ctx context.Context, req sessionstore.GetObjectRequest) (io.ReadCloser, error)
}
