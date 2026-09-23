package factory

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
)

// ErrHostSessionsWithoutJournalResolver reports a composition that creates or
// places Host-owned sessions (WithPublicCreates or WithPendingCommands) with no
// WithJournalResolver to read their journals. See WithJournalResolver.
var ErrHostSessionsWithoutJournalResolver = errors.New("factory: WithPublicCreates and WithPendingCommands require WithJournalResolver")

// JournalReader is the read of one session's RUNTIME journal: the journal a
// Host-owned session's runtime writes. A *sessionstore.Store opened over the
// runtime's own backend satisfies it -- for a Harness runtime that is
// sessionstore.Open(ctx, harnessBackend, sessionstore.WithLegacySingleTenant(tenant)),
// the layout Harness writes its journal in.
//
// It is addressed by the binding's RuntimeSessionID, never by the public
// session id: Factory rewrites the request before it is called.
type JournalReader interface {
	ReadPublicJournal(ctx context.Context, req sessionstore.ReadPublicJournalRequest) (sessionwire.JournalPage, error)
}

// JournalResolver answers the runtime journal a Host-owned session's binding
// names, for that session's tenant, and refuses any binding this deployment
// does not know.
//
// The refusal is the point, for ObjectStoreResolver's reason: a resolver that
// answered a default for an unknown StorageBindingID or BindingVersion would
// serve one deployment's journal under another's configuration. Return the
// store's own typed errors (or wrap them with %w): the /journal route classifies
// a *sessionstore.JournalError and the catalog's absence errors, and anything
// else is answered 500.
type JournalResolver func(ctx context.Context, tenant sessionwire.TenantID, binding sessionstore.SessionBinding) (JournalReader, error)

// WithJournalResolver supplies where a Host-owned session's journal is read.
//
// A Host-owned session is DISPOSITION-bound, and its journal is not in the
// SessionStore WithSessionReader reads: the Host's runtime keeps it on its own
// backend, under the binding's RuntimeSessionID, and SessionStore refuses to
// write a public journal for a disposition session at all. Read through
// WithSessionReader alone, every Host session's journal is empty at tip 0 --
// so /journal shows no history, the journal_tip hint says 0, and every
// live-tail repair after a delivered record cannot build a session.reset
// (Core refuses last_contiguous above journal_tip) and UNSUBSCRIBES the
// session's viewers instead of resetting them.
//
// With it, every journal read Factory makes -- the /journal route (and so its
// journal_tip), the journal_tip hint an unbound watched session publishes, and
// the tip every live-tail repair resets to -- reads the catalog binding for
// the PUBLIC session id and, for a disposition-bound session, reads the
// resolved runtime journal under the binding's RuntimeSessionID. Cursors are
// wrapped so a page's continuation is bound to the public session and its
// binding. A session that is not disposition-bound (legacy) is read from
// WithSessionReader, exactly as before.
//
// It is REQUIRED with WithPublicCreates or WithPendingCommands: a replica that
// creates or places Host sessions and cannot read their journals is refused by
// New with ErrHostSessionsWithoutJournalResolver.
func WithJournalResolver(r JournalResolver) Option {
	return option("WithJournalResolver", func(cfg *config) error {
		if r == nil {
			return nilDependency("WithJournalResolver")
		}
		cfg.journals = r
		return nil
	})
}

// resolvedJournals is the read plane every journal read goes through when a
// JournalResolver is composed: the deployer's SessionReader for everything
// else, and ReadPublicJournal routed by the session's binding.
type resolvedJournals struct {
	SessionReader
	resolve JournalResolver
}

func (r resolvedJournals) ReadPublicJournal(ctx context.Context, req sessionstore.ReadPublicJournalRequest) (sessionwire.JournalPage, error) {
	entry, err := r.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{TenantID: req.TenantID, SessionID: req.SessionID})
	if err != nil {
		return sessionwire.JournalPage{}, err
	}
	binding := entry.Record.Binding
	if binding.ProtocolMode != sessionstore.ProtocolModeDisposition {
		return r.SessionReader.ReadPublicJournal(ctx, req)
	}
	if binding.RuntimeSessionID == "" {
		return sessionwire.JournalPage{}, fmt.Errorf("factory: session %q names no runtime session: %w", req.SessionID,
			&sessionstore.JournalError{Code: sessionstore.JournalErrorInvalid, Field: "binding.runtime_session_id"})
	}
	reader, err := r.resolve(ctx, req.TenantID, binding)
	if err != nil {
		return sessionwire.JournalPage{}, fmt.Errorf("factory: resolve the journal of session %q: %w", req.SessionID, err)
	}
	if reader == nil {
		return sessionwire.JournalPage{}, fmt.Errorf("factory: the journal resolver answered no reader for session %q", req.SessionID)
	}
	inner := req
	inner.SessionID = sessionwire.SessionID(binding.RuntimeSessionID)
	if inner.Cursor, err = unwrapJournalCursor(req.Cursor, req.TenantID, req.SessionID, binding); err != nil {
		return sessionwire.JournalPage{}, err
	}
	page, err := reader.ReadPublicJournal(ctx, inner)
	if err != nil {
		return sessionwire.JournalPage{}, err
	}
	if page.NextCursor, err = wrapJournalCursor(page.NextCursor, req.TenantID, req.SessionID, binding); err != nil {
		return sessionwire.JournalPage{}, err
	}
	if page.PreviousCursor, err = wrapJournalCursor(page.PreviousCursor, req.TenantID, req.SessionID, binding); err != nil {
		return sessionwire.JournalPage{}, err
	}
	return page, nil
}

// The wrapped cursor binds the runtime store's opaque token to the public
// session and its immutable binding, so a cursor minted for one session -- or
// the runtime store's own token, handed straight to /journal -- is refused as
// JournalErrorCursor rather than read against another session's journal.
const (
	journalCursorVersion  = "j1"
	journalCursorDomain   = "looprig/factory/resolved-journal-cursor/v1\x00"
	maxJournalCursorBytes = 8192
)

func journalCursorScope(tenant sessionwire.TenantID, public sessionwire.SessionID, b sessionstore.SessionBinding) string {
	h := sha256.New()
	_, _ = h.Write([]byte(journalCursorDomain))
	for _, part := range []string{string(tenant), string(public), b.StorageBindingID, b.BindingVersion, b.RuntimeSessionID, string(b.ProtocolMode)} {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(part)))
		_, _ = h.Write(length[:])
		_, _ = h.Write([]byte(part))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func journalCursorError() error {
	return &sessionstore.JournalError{Code: sessionstore.JournalErrorCursor, Field: "cursor"}
}

func wrapJournalCursor(inner sessionwire.Cursor, tenant sessionwire.TenantID, public sessionwire.SessionID, b sessionstore.SessionBinding) (sessionwire.Cursor, error) {
	if inner == "" {
		return "", nil
	}
	token := journalCursorVersion + "." + journalCursorScope(tenant, public, b) + "." + base64.RawURLEncoding.EncodeToString([]byte(inner))
	if len(token) > maxJournalCursorBytes {
		return "", journalCursorError()
	}
	return sessionwire.Cursor(token), nil
}

func unwrapJournalCursor(token sessionwire.Cursor, tenant sessionwire.TenantID, public sessionwire.SessionID, b sessionstore.SessionBinding) (sessionwire.Cursor, error) {
	if token == "" {
		return "", nil
	}
	if len(token) > maxJournalCursorBytes {
		return "", journalCursorError()
	}
	parts := strings.Split(string(token), ".")
	if len(parts) != 3 || parts[0] != journalCursorVersion || parts[1] != journalCursorScope(tenant, public, b) || parts[2] == "" {
		return "", journalCursorError()
	}
	inner, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(inner) == 0 {
		return "", journalCursorError()
	}
	return sessionwire.Cursor(inner), nil
}
