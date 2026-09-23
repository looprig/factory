package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strconv"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	internalidentity "github.com/looprig/factory/internal/identity"
	"github.com/looprig/sessionstore"
)

// ---------------------------------------------------------------------------
// GET /v1/sessions/{sid}/status.
// ---------------------------------------------------------------------------

// serveSessionStatus answers one session's replay-free durable state.
//
// It is the projection that remains available while every Host is stopped, and
// it is the reason a cold session is readable at all: state, residency, the
// durable journal tip and the identifier of the gate the session is waiting on
// are all catalog record members, so answering them consults no Host, no
// runtime and no target directory. Core's SessionStatus documents that
// contract; this handler is where Factory keeps it.
//
// # It renders the record the CHAIN resolved
//
// serveRoute has already read the catalog entry, to establish that the session
// exists within the principal's tenant, and carries it forward on the request.
// Reading it again here would cost a second durable round trip on the most
// frequently polled route on the surface, and -- worse -- would let the two
// reads disagree: the existence decision made against one record and the answer
// rendered from another, with the 404 and the body describing different
// instants. See resolveSession.
func (rt *Router) serveSessionStatus() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entry, resolved := resolvedSessionFrom(r.Context())
		if !resolved {
			// Unreachable through the composed chain, which resolves every
			// session-scoped route before it dispatches.
			//
			// It is DEFENCE IN DEPTH here and nothing more, and that is
			// measured rather than assumed: deleting this branch leaves the
			// bare handler's answer byte-identical -- 500 internal_error, with
			// no durable read -- because Status() canonicalizes the record and
			// a zero record has an empty TenantID, which the released store
			// refuses. So no test can distinguish the two, and reporting one
			// that could would be inventing an assertion.
			//
			// It is kept because that identity rests on a DEPENDENCY's
			// validation rather than on this package's. A sessionstore that
			// projected a zero record without complaint would, without this,
			// answer a caller with a status naming no session at all. The two
			// sibling handlers do not share the property: deleting either of
			// their guards is killed, because both go on to make a durable
			// read with an empty session identifier.
			writeAPIError(w, internalFailure())
			return
		}
		// Core owns what a status means, and SessionStore owns the projection
		// from its own record; this package performs neither. A record the
		// store cannot canonicalize is a fault in durable state rather than
		// something the caller did, so it is answered as one.
		status, err := entry.Record.Status()
		if err != nil {
			writeAPIError(w, internalFailure())
			return
		}
		body, err := status.MarshalJSON()
		if err != nil {
			writeAPIError(w, internalFailure())
			return
		}
		writeJSONBytes(w, http.StatusOK, body)
	})
}

// ---------------------------------------------------------------------------
// GET /v1/sessions/{sid}/journal.
// ---------------------------------------------------------------------------

// The journal page bounds.
//
// maxJournalPageLimit is the largest page this surface asks SessionStore for,
// whatever a caller requests: a caller-supplied limit is an amplification lever
// and the cost of a large one is paid by the store and by Factory's memory
// rather than by the caller.
//
// It is HALF the tenant list's ceiling, and the difference is derived from what
// one row of each page costs rather than chosen for variety. A session summary
// is a fixed set of scalar members whose sizes Core's vocabulary bounds, so a
// page of two hundred has a size this package can predict. A journal event
// carries JournalEvent.Body -- the stored canonical public body, forwarded as
// raw JSON -- whose size is whatever a Host wrote, so the byte cost of a page
// is bounded only by the record count times an unbounded per-record term. Two
// surfaces whose per-row costs differ by orders of magnitude and whose ceilings
// agree have a ceiling derived from neither, and while the two numbers WERE
// equal nothing could observe boundedPageLimit keeping them apart: passing
// either constant at either call site was measured to survive the whole suite.
//
// defaultJournalPageLimit is the window used when a caller names no limit, and
// it exists because of something the session list does not have to decide. A
// missing limit on the tenant list is forwarded as zero, which SessionStore
// reads as its own configured page size; that is fine there because the store
// also chooses the position. The journal route instead promises a default
// window of 64 sequence positions, so it supplies that explicit limit with
// Tail and lets SessionStore derive the start from the same captured tip.
const (
	maxJournalPageLimit     = 100
	defaultJournalPageLimit = 64
)

// The ceiling is not a judgement: see storePageCeiling.
const (
	_ = uint(storePageCeiling - maxJournalPageLimit)
	_ = uint(storePageCeiling - defaultJournalPageLimit)
)

// serveSessionJournal answers a bounded page of a session's public events.
//
// # The initial view is the END of the journal, not the beginning
//
// A caller that names no position gets the last defaultJournalPageLimit
// sequences below the tip the store captured. That is the whole of step 3's
// rule and it is a requirement rather than a nicety: a client opening a
// long-running session wants what just happened, and a surface that answered
// from sequence one would make the first screen of every old session a replay
// of its oldest events -- and, if it followed its own cursor to reach the tip,
// an unbounded read on an authenticated route that any credential could
// trigger.
//
// One Tail request captures the tip and derives the sequence window inside
// SessionStore. A separate tip probe would race with appends: the second read
// could capture a newer tip and scan arbitrarily many private records beyond
// the intended window. The catalog's tip is also unsuitable because updating
// the catalog is a separate durable write from appending journal records.
//
// # Everything else is one read at a position the caller names
//
// Every positioning mode supplies ScanLimit equal to the chosen event limit.
// A private record consumes this scan budget without adding an event. A page
// can therefore have no events and still advance covered_through and issue a
// continuation. Only the cursor's absence means the captured walk is complete.
// A caller's limit is defaulted and clamped before it becomes either budget.
//
// A cursor is the store's own opaque token: it is handed out and handed back
// unread, because possessing one authorizes nothing and parsing one would make
// this package a second authority on a grammar SessionStore owns. from_seq is
// the absolute alternative, and it is how OLDER pages load -- a client walking
// backwards subtracts its own window from the position it holds. The two are
// mutually exclusive here because they are mutually exclusive in the store, and
// refusing the combination before the read is what keeps that a 400 rather than
// a dependency's typed error mapped to a fault.
//
// # What the response says
//
// Core's JournalPage, forwarded. Its captured tip, its authenticated
// covered_through watermark and its forward cursor are what let a client close
// a sequence gap without learning the kind or the bytes of the records that
// filled it -- see the private-record rule in ReadPublicJournal, which is
// SessionStore's to enforce and which this package must not undo by projecting
// the page into a shape of its own.
func (rt *Router) serveSessionJournal() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		operation, authenticated := internalidentity.OperationContextFrom(r.Context())
		entry, resolved := resolvedSessionFrom(r.Context())
		if !authenticated || !resolved {
			// Unreachable through the composed chain; see serveSessionStatus.
			writeAPIError(w, internalFailure())
			return
		}
		position, ok := journalPositionOf(w, r.URL.Query())
		if !ok {
			return
		}
		reads := newScope(operation.Principal)
		session := entry.Record.SessionID

		page, err := rt.reads.ReadPublicJournal(r.Context(),
			reads.journalPage(session, position.cursor, position.fromSeq, position.limit, !position.positioned))
		if err != nil {
			writeAPIError(w, journalFailure(err))
			return
		}
		// Core's own type is forwarded rather than re-projected, and that is
		// what step 6 turns on: JournalEvent.Body is the stored canonical
		// public body as raw JSON, so it reaches the client as the bytes
		// SessionStore holds. Decoding it into a Go value and re-encoding
		// would re-order members and rewrite escapes -- a body that no longer
		// matches the one the writer canonicalized -- and would require this
		// package to name a vocabulary only Harness defines.
		body, err := page.MarshalJSON()
		if err != nil {
			writeAPIError(w, internalFailure())
			return
		}
		writeJSONBytes(w, http.StatusOK, body)
	})
}

// journalPosition is one caller's request for a page: where to start and how
// much to return.
type journalPosition struct {
	cursor  sessionwire.Cursor
	fromSeq uint64
	// positioned reports that the caller named a position, by either spelling.
	// It is a separate member rather than "cursor or fromSeq is non-zero"
	// because from_seq=0 is a POSITION -- the first record -- and is not the
	// same request as naming none, which is the tail.
	positioned bool
	limit      int
}

// journalPositionOf reads the caller's position and bound, and answers every
// refusal it can make before the store is touched.
//
// Presence is read from the parsed values rather than from Query().Get for the
// reason singleValue gives, and it matters twice over here: a repeated cursor
// and a repeated from_seq are two different positions in one request.
//
// # An EMPTY cursor is refused, and the reason is a client bug this invites
//
// A present-but-empty parameter is refused by singleValue, which for the
// position is not a formality. SessionStore reads an empty cursor as NO cursor
// and a start below one as sequence one, so a request carrying "?cursor=" would
// be positioned -- selecting a forward read -- and would then walk from the
// journal's FIRST record. Measured before this was refused: on a
// three-thousand-record journal "?cursor=" returned events 1 through 95 while
// naming no cursor at all returned 2938 through 2999.
//
// That is the replay this route exists to forbid, and the client that reaches
// it is doing the obvious thing rather than something perverse. Core declares
// next_cursor omitempty, so the LAST page of a walk carries none; a client
// written as cursor=${page.next_cursor ?? ""} therefore sends an empty cursor
// exactly when it reaches the tip, is thrown back to sequence one, and walks
// the whole journal forward again -- an unbounded read loop on an
// authenticated route, driven by a client that believes it is paging normally.
//
// Refusing is chosen over silently treating it as absent because the two
// answers a client could get from an empty cursor -- the tail, or the head --
// are both guesses about what it meant, and one of them is the loop. A 400
// tells it at the first request rather than after it has walked the journal. It
// is also what from_seq already did, through ParseUint, so the two positions
// answer an empty value the same way.
func journalPositionOf(w http.ResponseWriter, query url.Values) (journalPosition, bool) {
	cursor, ok := singleValue(w, query, "cursor")
	if !ok {
		return journalPosition{}, false
	}
	position, ok := singleValue(w, query, "from_seq")
	if !ok {
		return journalPosition{}, false
	}
	if cursor.present && position.present {
		writeAPIError(w, apiError{
			status:  http.StatusBadRequest,
			code:    sessionwire.ErrorCodeInvalidRequest,
			message: "cursor and from_seq name two positions; send one",
		})
		return journalPosition{}, false
	}
	out := journalPosition{
		cursor:     sessionwire.Cursor(cursor.value),
		positioned: cursor.present || position.present,
		limit:      defaultJournalPageLimit,
	}
	if position.present {
		fromSeq, err := strconv.ParseUint(position.value, 10, 64)
		if err != nil {
			writeAPIError(w, apiError{
				status:  http.StatusBadRequest,
				code:    sessionwire.ErrorCodeInvalidRequest,
				message: "from_seq must be a whole number of at least zero",
			})
			return journalPosition{}, false
		}
		out.fromSeq = fromSeq
	}
	limit, present, ok := boundedPageLimit(w, query, maxJournalPageLimit)
	if !ok {
		return journalPosition{}, false
	}
	if present {
		out.limit = limit
	}
	return out, true
}

// ---------------------------------------------------------------------------
// GET /v1/sessions/{sid}/gates.
// ---------------------------------------------------------------------------

// serveSessionGates answers the session's open public gates.
//
// The page is SessionStore's, forwarded whole. Core's GateProjection is the
// complete public view of an open gate and says so in its own schema: it
// carries presentation-safe prompt data and never a submitted answer, a private
// prepared payload, a credential or a signed URL. So the way this package
// discloses one is not by including a field it should have withheld -- it has
// no such field to include -- but by reaching for material through some OTHER
// durable read and adding it. It does not: SessionReader names one gate read,
// and it returns Core's page.
//
// There is no cursor and no limit, because SessionStore's read has neither: the
// catalog holds a bounded open-gate set for a session, so the whole answer is
// one record read and a continuation would be a token that could never be
// issued. GatePage.OpenGateCount is what a later API that pages a larger set
// would use.
//
// # What it costs
//
// One catalog read beyond the one serveRoute already made, because
// SessionStore's ReadGates re-reads the record it projects from. The
// alternative -- projecting the resolved record's own OpenGates here -- would
// restate the store's projection, including the check that every gate's opening
// event is at or below the record's durable tip, whose failure the store
// classifies as a stored record disagreeing with itself. Restating that is how
// two readers of one record come to disagree, so the second read is accepted
// and stated rather than optimized away here; coalescing durable reads across a
// request belongs to the composition (A9.1).
func (rt *Router) serveSessionGates() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		operation, authenticated := internalidentity.OperationContextFrom(r.Context())
		entry, resolved := resolvedSessionFrom(r.Context())
		if !authenticated || !resolved {
			// Unreachable through the composed chain; see serveSessionStatus.
			writeAPIError(w, internalFailure())
			return
		}
		page, err := rt.reads.ReadGates(r.Context(), newScope(operation.Principal).gates(entry.Record.SessionID))
		if err != nil {
			writeAPIError(w, catalogFailure(err))
			return
		}
		page = rt.overlayAnswerability(r.Context(), operation.Principal.Tenant(), entry.Record, page)
		// Core validates on marshal, and part of what it validates is that the
		// gates are in ascending opened-sequence order with the captured tip at
		// or after every one of them. So "ordered" is enforced on the way out
		// by the vocabulary's own rule rather than by this package trusting the
		// store or re-sorting behind it.
		body, err := page.MarshalJSON()
		if err != nil {
			writeAPIError(w, internalFailure())
			return
		}
		writeJSONBytes(w, http.StatusOK, body)
	})
}

// GateOwners answers whether a gate response for a session would pass the
// gate_response write path's owner check now. *admission.Service satisfies it
// with the SAME check AdmitGateResponse makes, so the read and the write agree.
type GateOwners interface {
	GateResponsesAnswerable(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, record sessionstore.CatalogRecord) (bool, error)
}

// overlayAnswerability reports a stored "resident" gate as "unavailable" when
// an answer to it would be refused gate_not_resumable: its Host released the
// session or crashed (no fresh matching owner), or the owner does not
// advertise hostlink.command.gate_response. SessionStore keeps the
// answerability the Host wrote at open, and only a successor re-publishes a
// gate, so the stored value outlives its owner.
//
// The gate itself is never hidden: a successor restores it, and the prompt is
// still the session's. Only "resident" is overlaid -- every other stored value
// already says it cannot be answered. An owner that could not be ASKED also
// reads "unavailable": the write answers that 503, and a read cannot promise a
// button it could not check. The owner check runs once per read, and only when
// some gate is stored resident.
func (rt *Router) overlayAnswerability(ctx context.Context, tenant sessionwire.TenantID, record sessionstore.CatalogRecord, page sessionwire.GatePage) sessionwire.GatePage {
	if rt.gateOwners == nil {
		return page
	}
	resident := false
	for _, gate := range page.Gates {
		resident = resident || gate.Answerability == sessionwire.GateAnswerabilityResident
	}
	if !resident {
		return page
	}
	if answerable, err := rt.gateOwners.GateResponsesAnswerable(ctx, tenant, record.SessionID, record); err == nil && answerable {
		return page
	}
	gates := append([]sessionwire.GateProjection(nil), page.Gates...)
	for i := range gates {
		if gates[i].Answerability == sessionwire.GateAnswerabilityResident {
			gates[i].Answerability = sessionwire.GateAnswerabilityUnavailable
		}
	}
	page.Gates = gates
	return page
}

// ---------------------------------------------------------------------------
// The failures these handlers construct.
// ---------------------------------------------------------------------------

// journalFailure maps a SessionStore journal read onto a public answer.
//
// A refused CURSOR is the caller's, and it is the same public fact the catalog
// walk reports: the token is not one this session issued, so the walk restarts
// rather than anything being wrong with the journal. Every other journal code
// is a fault HERE by construction -- an out-of-range limit and a cursor sent
// beside a position are both refused above, before the read -- so they are
// answered as faults by DEFAULT rather than by enumeration, which keeps a code
// sessionstore adds later off the "the caller can fix this" path.
//
// What it does NOT decide is absence, and it does not decide a CLOSING store
// either. A journal read verifies the session's binding before it reads
// anything, so it can report a session that does not exist; and admitForeground
// refuses every read the same way once Close begins. Both arrive as the same
// errors the catalog read produces, so both decisions are made in one place, by
// catalogFailure, and this delegates rather than restating them.
//
// That delegation is why there is no storeUnavailable call here. Adding one was
// measured EQUIVALENT: deleting it again left the whole suite green, because
// *StoreClosedError is not a *JournalError, falls past the arm above, and is
// answered by catalogFailure. A second copy would have been a line with no
// reader sitting beside the one that decides.
func journalFailure(err error) apiError {
	if failure, ok := contextFailure(err); ok {
		return failure
	}
	var journal *sessionstore.JournalError
	if errors.As(err, &journal) {
		if journal.Code == sessionstore.JournalErrorCursor {
			return apiError{
				status:  http.StatusBadRequest,
				code:    sessionwire.ErrorCodeInvalidRequest,
				message: "the cursor is not one this session issued; restart the walk",
			}
		}
		return internalFailure()
	}
	return catalogFailure(err)
}

// resolvedSession is the catalog entry serveRoute read to establish that the
// session exists within the principal's tenant.
//
// It is carried on the request context rather than passed as an argument
// because the handlers are http.Handler values built once at composition, which
// is what keeps a handler from being rebuilt per request. The key is an
// unexported empty struct type, so nothing outside this package can write one
// and no other package's value can collide with it.
type resolvedSessionKey struct{}

// withResolvedSession carries one resolved entry to the handler.
func withResolvedSession(r *http.Request, entry sessionstore.CatalogEntry) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), resolvedSessionKey{}, entry))
}

// resolvedSessionFrom reports the entry the chain resolved, and whether it did.
func resolvedSessionFrom(ctx context.Context) (sessionstore.CatalogEntry, bool) {
	entry, ok := ctx.Value(resolvedSessionKey{}).(sessionstore.CatalogEntry)
	return entry, ok
}
