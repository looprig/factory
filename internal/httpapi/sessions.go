package httpapi

import (
	"context"
	"errors"
	"math"
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
// defaultJournalPageLimit is the window used when a caller names no limit, and
// it exists because of something the session list does not have to decide. A
// missing limit on the tenant list is forwarded as zero, which SessionStore
// reads as its own configured page size; that is fine there because the store
// also chooses the position. HERE Factory computes the position FROM the limit
// -- the tail is the last window of sequences below the captured tip -- so a
// limit only the store knows would leave the tail unanchored. Every journal
// read this package makes therefore carries a limit this package chose, which
// is why the two are one rule rather than two.
const (
	maxJournalPageLimit     = 200
	defaultJournalPageLimit = 64
)

// The ceiling is not a judgement: see storePageCeiling.
const (
	_ = uint(storePageCeiling - maxJournalPageLimit)
	_ = uint(storePageCeiling - defaultJournalPageLimit)
)

// journalTipProbeSeq positions a read ABOVE every sequence a journal can hold.
//
// It is how this surface learns the tip it must anchor a tail on, and it is
// exact rather than an estimate. SessionStore captures the journal's tip as
// part of planning ANY public read and reports it as JournalPage.CapturedTip;
// its walk then begins with "if the start is past the captured tip, return
// nothing", so a read positioned here costs the tip read and does not open the
// ledger at all. Measured against the released sessionstore v0.1.0, where that
// early return is walkJournal's first statement.
//
// The alternative was the catalog record's LastJournalSeq, which this handler
// already holds and which would have cost no extra read. It was rejected
// because it is a SEPARATE durable write from the journal append: a Host that
// has committed records and not yet updated the catalog leaves it low, which
// silently turns the tail into a middle page, and a record whose sequence ran
// ahead of the journal would anchor the window above the tip and answer an
// active session with an empty history. An anchor for a tail has to come from
// the same read that reports the tail.
const journalTipProbeSeq = uint64(math.MaxUint64)

// journalTipProbePageLimit is the smallest page the store accepts. Zero would
// mean the store's own page size, and the probe is meant to return nothing.
const journalTipProbePageLimit = 1

const _ = uint(storePageCeiling - journalTipProbePageLimit)

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
// It costs two bounded reads: journalTipProbeSeq explains why, and why the
// second one cannot be avoided by reusing the catalog's tip.
//
// # Everything else is one read at a position the caller names
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

		from := position.fromSeq
		if !position.positioned {
			probe, err := rt.reads.ReadPublicJournal(r.Context(),
				reads.journalPage(session, "", journalTipProbeSeq, journalTipProbePageLimit))
			if err != nil {
				writeAPIError(w, journalFailure(err))
				return
			}
			from = journalTailStart(probe.CapturedTip, position.limit)
		}
		page, err := rt.reads.ReadPublicJournal(r.Context(),
			reads.journalPage(session, position.cursor, from, position.limit))
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

// journalTailStart is the first sequence of the last window below tip.
//
// The window is a span of SEQUENCES, not a count of events, and the difference
// is the whole reason it is safe: a span of n sequences holds at most n
// records, so a page limit of n can never cut it short by itself, and the
// events it yields are however many of those positions are public. A journal
// whose tail happens to be mostly private records therefore returns a short
// page rather than a page that walked further back to fill itself -- which
// would be a read whose cost depended on data the caller cannot see.
//
// A journal no longer than the window starts at the beginning, which is zero:
// SessionStore reads zero as "the first record", so the tail of a short journal
// is the whole of it.
func journalTailStart(tip uint64, limit int) uint64 {
	// A limit below one is unreachable -- journalPositionOf refuses one and its
	// default is positive -- and it is floored here anyway rather than
	// converted, because the conversion is the failure. A negative int becomes
	// an enormous uint64, the window swallows the journal, and the tail becomes
	// the replay from the first sequence that step 3 exists to forbid: the one
	// wrong answer of the two available, arrived at silently.
	window := uint64(1)
	if limit > 1 {
		window = uint64(limit)
	}
	if tip <= window {
		return 0
	}
	return tip - window + 1
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
// reason sessionPageLimit gives: Get cannot tell "?limit=" from an absent
// parameter, and answers a repeated parameter with whichever came first, so a
// caller sending two would be served a page it did not unambiguously ask for.
// Here that matters twice over, because a repeated cursor and a repeated
// from_seq are two different positions in one request.
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
	limit, ok := singleValue(w, query, "limit")
	if !ok {
		return journalPosition{}, false
	}
	if limit.present {
		value, err := strconv.Atoi(limit.value)
		if err != nil || value < 1 {
			writeAPIError(w, apiError{
				status:  http.StatusBadRequest,
				code:    sessionwire.ErrorCodeInvalidRequest,
				message: "limit must be a positive whole number",
			})
			return journalPosition{}, false
		}
		out.limit = min(value, maxJournalPageLimit)
	}
	return out, true
}

// queryValue is one query parameter's presence and value.
type queryValue struct {
	present bool
	value   string
}

// singleValue reads a parameter that may appear at most once, answering a
// repeat as a 400 rather than choosing between the two.
func singleValue(w http.ResponseWriter, query url.Values, name string) (queryValue, bool) {
	values := query[name]
	switch len(values) {
	case 0:
		return queryValue{}, true
	case 1:
		return queryValue{present: true, value: values[0]}, true
	default:
		writeAPIError(w, apiError{
			status:  http.StatusBadRequest,
			code:    sessionwire.ErrorCodeInvalidRequest,
			message: name + " was given more than once",
		})
		return queryValue{}, false
	}
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
// What it does NOT decide is absence. A journal read verifies the session's
// binding before it reads anything, so it can report a session that does not
// exist, and it reports it with the SAME errors the catalog read does -- so
// that decision is made in one place, by catalogFailure, and this delegates to
// it rather than restating three codes.
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
