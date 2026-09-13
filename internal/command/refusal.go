package command

import (
	"errors"
	"net/http"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
)

// This file holds the three decisions the REST control routes and the
// ClientLink RPCs must not be able to answer differently.
//
// They live beside the command kinds for the same reason those do, and the
// package doc's sentence covers them without amendment: specification section
// 8.1 makes the two surfaces two spellings of ONE admission contract, so a
// decision each of them restates is a decision each of them can restate
// wrongly. Two private status tables would satisfy every test either package
// could write while telling a browser its command was permanently refused over
// one transport and worth retrying over the other.
//
// Nothing here is a dependency seam and nothing here does I/O. Each is a pure
// function of a value both edges already hold.

// refusalStatus maps each classified admission refusal onto the REST status
// that answers it.
//
// # Every status here is a 4xx, and that is the A3.3-retryable ruling
//
// `httpapi`'s retryableStatus -- RetryableStatus below, now that there is one
// of it -- reports 502, 503 and 504 retryable, because each names a condition
// the REQUEST did not cause. The ClientLink answers every classified refusal
// retryable:false. So a status in that set would make the identical refusal
// retryable over REST and not over the RPC, which is the divergence the open
// item named.
//
// runtime_unavailable is the one that had to be decided rather than assumed,
// and the obvious mapping is the wrong one. The set was ENUMERATED at the pin
// rather than recalled, because an earlier wording of this paragraph got it
// wrong in the one direction that matters -- see "the clause that is NOT the
// argument" below.
//
// `grep ErrorCodeRuntimeUnavailable` over every production file finds SIX
// refusal() sites in internal/admission/service.go, carrying FOUR distinct
// conditions:
//
//	:184, :358  the AgentID a create names resolves to no configured launch
//	            target (the V1 and legacy create paths)
//	:186        the V1 create reservation this composition cannot author
//	:272        an EXISTING session's own pinned target is no longer a
//	            configured one
//	:279, :391  the payload is past sessionstore.MaxInboxPayloadBytes
//
// :184 and :272 are kept apart rather than counted as one "unresolvable
// target": the first is a fact about the AgentID in the REQUEST and is fixed by
// naming a different agent, the second is a fact about a STORED record and is
// fixed by restoring the deployment's configuration. Merging them would make
// the argument below rest on a count nobody can re-derive.
//
// EVERY ONE OF THE FOUR IS PERMANENT until the deployment's configuration or
// the request itself changes, and that -- not any inability to tell them apart
// -- is the argument. retryable:true promises that repeating the IDENTICAL
// bytes could succeed with nothing else changing, and no member of this set can
// do that. So 503 is not a forced compromise over an indistinguishable pair; it
// is simply wrong for all four. It is 422: the request is well formed and this
// deployment cannot carry it out.
//
// # The clause that is NOT the argument
//
// An earlier wording said admission also mints this code "for a directory read
// that failed", making four members of which "three are permanent", and rested
// the ruling on "the CODE cannot distinguish them". That clause is VACUOUS at
// this pin: a failed target-directory read is returned by resolveTargetFault
// (service.go:115) and by the IsKnown fault (service.go:269) as a PLAIN WRAPPED
// ERROR carrying no public code, so errors.As finds no *admission.Error and it
// never reaches this table at all -- it is answered through each edge's fault
// channel. service.go:91-114 records that the folding of a transient outage
// into this code was REMOVED by an earlier task; the old wording restated
// pre-fix behaviour. It is called out rather than quietly deleted because it is
// exactly the premise a later reader would use to reopen the 503.
//
// # The fifth producer, which is not a refusal
//
// internal/admission/reconciler.go:446 (expiredCommandRejection) also mints
// runtime_unavailable, and it is deliberately absent from the four above: it is
// a durable Record.Rejection detail, so it reaches a client through StatusFor
// inside a 200/202 body with state "rejected", never through this table. A
// client can therefore see this code under both a 422 and a 2xx, which is
// coherent -- one is "this deployment will not accept your command", the other
// is "your accepted command was settled because no runtime applied it" -- and
// is stated here so the enumeration above is not read as every producer in the
// module.
//
// The four state-conflict codes are 409, which is also what the legacy surface
// answered for its nearest equivalent (`gate_not_ready`). A transient condition
// the legacy surface DID mark retryable -- gate capacity, 503 -- has no code in
// this vocabulary and must not acquire one by being folded into an existing
// one; a dependency fault is not a classified refusal at all and each edge
// answers it from its own fault channel.
func refusalStatus() map[sessionwire.ErrorCode]int {
	return map[sessionwire.ErrorCode]int{
		// The caller's own bytes, decided without reading any tenant state.
		sessionwire.ErrorCodeInvalidRequest: http.StatusBadRequest,
		// There is no such session in this tenant. It is the SAME answer for a
		// session that never existed, one that was deleted and one that belongs
		// to another tenant; see SessionAbsent.
		sessionwire.ErrorCodeSessionNotFound: http.StatusNotFound,
		// Durable state contradicts the request as sent: a command identity
		// already holds different content, or the gate this answers is no
		// longer the incarnation the caller saw.
		sessionwire.ErrorCodeCommandRejected:  http.StatusConflict,
		sessionwire.ErrorCodeGateResolved:     http.StatusConflict,
		sessionwire.ErrorCodeGateNotResumable: http.StatusConflict,
		sessionwire.ErrorCodeGateExpired:      http.StatusConflict,
		// The request is well formed and this deployment cannot carry it out.
		sessionwire.ErrorCodeRuntimeUnavailable: http.StatusUnprocessableEntity,
	}
}

// RefusalStatus reports the REST status for a classified admission refusal.
//
// The second result is false for a code this authority has no ruling for, and a
// caller must fail closed on it rather than defaulting: a public code with no
// status is a code somebody added to admission without deciding what it means
// to a client, and answering it with whatever the zero value happens to be
// would make that decision silently.
func RefusalStatus(code sessionwire.ErrorCode) (int, bool) {
	status, ok := refusalStatus()[code]
	return status, ok
}

// RefusalCodes lists the codes this authority classifies.
//
// The order is map order, which is to say unspecified, and nothing sorts it:
// its readers are sweeps over the whole set. It exists so a test can range over
// the authority's own subject rather than over a list beside it.
func RefusalCodes() []sessionwire.ErrorCode {
	table := refusalStatus()
	codes := make([]sessionwire.ErrorCode, 0, len(table))
	for code := range table {
		codes = append(codes, code)
	}
	return codes
}

// RetryableStatus reports whether a client may usefully repeat the identical
// request.
//
// The set is the three statuses that name a condition the REQUEST did not
// cause: a bad gateway, an unavailable dependency, and an expired deadline. A
// 500 is deliberately absent -- a fault Factory could not classify is not one a
// client should be told to hammer -- and so is 403 csrf_token_expired, which is
// recoverable but only after the client changes the request by fetching a new
// token, which is not the same claim.
//
// It is here rather than in `internal/httpapi` because the ClientLink has to
// reach the same answer, and a second literal there is the divergence this file
// exists to remove.
func RetryableStatus(status int) bool {
	switch status {
	case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

// SessionAbsent reports whether a SessionStore failure means "there is no such
// session in this tenant".
//
// # Why it is one function and not one per caller
//
// The released store has FOUR spellings of that one public fact, and the two
// callers had two different subsets of them. `internal/httpapi`'s catalogFailure
// read all four; `internal/admission`'s catalogNotFound read two, omitting
// CatalogErrorDeleted and CatalogErrorIdentity, so the same session answered a
// GET with 404 session_not_found and a control command with a bare fault --
// 500 over REST, a temporary transport error over the ClientLink. That is the
// `A9.1-notfound` carry-forward, and it is settled here by making the two
// readers one.
//
// # The four reasons the set is exactly these four
//
// not_found and deleted are the same public fact -- there is no session here --
// and reporting them apart would tell a caller that an identifier they cannot
// read WAS once real. binding_not_found is the keyspace's answer for a session
// whose collision witnesses do not resolve, which the store checks BEFORE it
// reads a record at all, and it is how the CROSS-TENANT case arrives: outside
// the legacy single-tenant layout sessionstore hashes the tenant into the
// session's scope, so another tenant's session fails at verifySessionScope and
// never reaches a record.
//
// identity is the fourth, and it is the one that had to be ruled rather than
// listed. An earlier wording of this paragraph credited it with the
// cross-tenant case and called it "unreachable today against the released
// store". BOTH halves are false, and they were checked at the pin rather than
// recalled: GetCatalogEntry (catalog.go:336) calls readCatalogEntry (:596)
// calls catalogEntry (:718), which mints CatalogErrorIdentity at :730 when a
// DECODED record's own TenantID or SessionID disagrees with the one asked for.
// That is the live read path this module uses, and catalogEntry's own doc says
// what the check is for: "a provider that hashes the stable key stores the
// original bytes for exactly this verification". It is the store's
// stable-key COLLISION guard, not a tenant check -- the tenant check already
// ran, one layer up, and answered binding_not_found.
//
// # So it is ruled deliberately, and the ruling is 404
//
// What the condition means is that the store cannot vouch that the record
// filed at this session's id IS this session's record. Two things follow.
//
// The record it declined to vouch for may be ANOTHER TENANT's -- that is
// precisely what a stable-key collision produces -- so any answer that
// distinguishes this condition from absence risks confirming that some other
// session occupies the id a caller named. Absence is the disclosure-safe
// direction.
//
// And the two edges must not disagree about one session. internal/httpapi has
// answered 404 for this code since before A3.3 (catalogFailure read all four
// spellings), so a READ of such a session already answers "there is no such
// session". Had admission kept it a fault, a control command and a read of the
// identical session would have disagreed about whether it exists, which is the
// divergence this predicate exists to remove.
//
// The COST is real and is stated rather than discovered: an operator loses the
// 500 that would have paged them about a genuinely corrupt or colliding record
// on the control path -- exactly as they already had on the read path. The
// condition is not silent, it is in the store's own error and in an operator's
// log; it is only the PUBLIC answer that is absence.
//
// # Everything else stays a fault
//
// An ambiguous marker, a layout mismatch or the keyspace's OWN hash-collision
// code is the deployment disagreeing with itself, not a session that is not
// there. Note that KeyspaceHashCollision and CatalogErrorIdentity are two
// different detectors and are ruled differently on purpose: the first is the
// keyspace reporting that two sessions' witnesses collide, which is a fact
// about the deployment and nothing about whether this session exists; the
// second is a record failing to vouch for itself, which is exactly "there is no
// session here that I can give you". An earlier wording of this doc used "a
// hash collision stays a fault" as though it covered both.
//
// The predicate is an enumeration of ABSENCE rather than of faults, so a code a
// later sessionstore adds lands on "fault" instead of on a 404 reporting a
// session missing because the store misbehaved.
func SessionAbsent(err error) bool {
	var catalog *sessionstore.CatalogError
	if errors.As(err, &catalog) {
		switch catalog.Code {
		case sessionstore.CatalogErrorNotFound, sessionstore.CatalogErrorDeleted, sessionstore.CatalogErrorIdentity:
			return true
		}
	}
	var keyspace *sessionstore.KeyspaceError
	return errors.As(err, &keyspace) && keyspace.Code == sessionstore.KeyspaceBindingNotFound
}

// StatusFor projects the authoritative SessionInbox record into Core's public
// command status.
//
// It is the RECORD that is described, never the call: admission's "this call
// created it" boolean is deliberately not an input, because a retry after an
// unknown outcome must receive the same answer the original acceptance
// received. If the two differed, a client could tell whether its earlier
// attempt landed -- and the whole point of the durable CommandID is that it does
// not have to know.
//
// # The state projection
//
// SessionStore's inbox has five states and Core's status has four, and they are
// not the same partition. What this acknowledges is durable ACCEPTANCE, which
// Core's own doc defines as "the inbox commit succeeded; it does not promise
// that a Host has already applied the command" -- true of pending, claimed and
// applying alike, so all three are accepted. Applied is applied. Rejected is
// rejected and carries the record's own stable ErrorDetail; reporting it as
// accepted would be a client believing a rejected command is in flight.
//
// Core's CommandStatePending is never minted here. It has no counterpart in the
// released inbox vocabulary -- a claimed command is still exactly "accepted, not
// yet applied" -- and inventing a mapping for it would put a state on the wire
// that no durable record means.
//
// An unrecognised state is a FAULT rather than an acceptance, which is what the
// second result is for. A record whose state this build does not know is a
// store disagreeing with this build, and defaulting it to accepted would report
// the one case that must never be reported optimistically.
func StatusFor(entry sessionstore.InboxEntry) (sessionwire.CommandStatus, bool) {
	status := sessionwire.CommandStatus{
		CommandID:     entry.Record.CommandID,
		AcceptedOrder: entry.AcceptedOrder,
	}
	switch entry.Record.State {
	case sessionstore.InboxStatePending, sessionstore.InboxStateClaimed, sessionstore.InboxStateApplying:
		status.State = sessionwire.CommandStateAccepted
	case sessionstore.InboxStateApplied:
		status.State = sessionwire.CommandStateApplied
	case sessionstore.InboxStateRejected:
		status.State = sessionwire.CommandStateRejected
		status.Error = entry.Record.Rejection
	default:
		return sessionwire.CommandStatus{}, false
	}
	return status, true
}
