package placement

import (
	"context"
	"errors"
	"fmt"
	"sync"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"
)

// ErrInvalidSweeperConfig reports a RecordSweeper configuration refused before
// it could reach a store.
//
// It is a SECOND error in this package rather than a reuse of ErrInvalidConfig,
// and the duplication is deliberate. ErrInvalidConfig belongs to Reconciler,
// which places one session; a caller that composes both would otherwise be
// unable to tell which of the two it misconfigured from the sentinel alone, and
// the two have no configuration member in common.
var ErrInvalidSweeperConfig = errors.New("placement: invalid record sweeper configuration")

// TargetSweep is the service-owned directory sweep.
//
// It is ONE method and it is the store's, not this package's, which is the
// whole structural claim of the target half. SessionStore revalidates every due
// row against that row's OWN STORED EXPIRY at a fresh clock reading and closes
// the write with a compare-and-swap onto the revision the due page reported
// (sessionstore@v0.4.0/host_targets.go:1631 reconcileHostTargetRow).
//
// THE TWO MECHANISMS CATCH DIFFERENT HEARTBEATS, and the distinction was stated
// wrongly here before. reconcileHostTargetRow takes the due page's FROZEN BYTES
// (stored storage.OrderedRecord) and NEVER RE-READS THE ROW; only the clock it
// compares against is fresh. So a heartbeat that landed AFTER the page was read
// cannot present an unlapsed expiry to the revalidation at all -- its only
// catcher is the compare-and-swap, which loses against the revision that
// heartbeat advanced. The expiry revalidation catches the EARLIER landing: one
// already carried in the page's bytes when they were captured, which a weakly
// consistent due view can still name as due.
//
// The conclusion is unchanged and rests on the pair: Factory must not re-derive
// that judgement, because a due observation this side of the seam is a hint
// about which rows to LOOK at and is never evidence that a Host is gone.
type TargetSweep interface {
	ReconcileHostTargets(context.Context, sessionstore.ReconcileHostTargetsRequest) (sessionstore.HostTargetReconcileResult, error)
}

// ClaimSweep is the one durable write the claim half makes.
//
// It is Release and nothing else. Acquire is deliberately absent: a sweeper
// that could acquire would be a second placement reconciler, and the work a
// claim suppresses is Reconciler's. Get is absent for a sharper reason —
// GetReconciliationClaim refuses a lapsed claim rather than returning it
// (sessionstore@v0.4.0/reconcile.go:450), so it cannot answer the question this
// sweep asks, and a caller that read it first would still have to attempt the
// release to learn anything.
type ClaimSweep interface {
	ReleaseReconciliationClaim(context.Context, sessionstore.ReleaseReconciliationClaimRequest) (sessionstore.ReconciliationClaimEntry, error)
}

// SweeperConfig is one replica's record-sweep configuration.
type SweeperConfig struct {
	Targets TargetSweep
	Claims  ClaimSweep

	// HolderID names this replica in a claim release. It must be the SAME
	// string this replica's Reconciler puts in Config.HolderID, because a
	// release is refused for any other holder and the whole reach of this
	// sweep is the claims this replica itself left behind.
	HolderID string

	// PageLimit bounds one due page.
	PageLimit int

	// MaxPages bounds one SweepTargets call.
	MaxPages int
}

// RecordSweeper sweeps the two CONTROL RECORDS placement leaves behind: a
// crashed Host's target advertisement and this replica's own placement claim.
//
// IT IS NOT Reconciler AND DOES NOT EXTEND IT, which is worth stating because
// both live in this package and both are called "reconciliation" upstream.
// Reconciler's subject is a SESSION: it reads a catalog record, takes that
// session's claim and drives a workload controller. This one's subject is a
// RECORD, it names no session of its own in the target half, and it makes no
// placement decision at all. They were kept separate rather than fused because
// a fused type would have to hold both a per-session request and a
// directory-wide continuation, and the continuation is the one piece of state
// this module carries between calls — putting it on the type that is invoked
// once per session would make its lifetime a function of traffic.
type RecordSweeper struct {
	cfg SweeperConfig

	// mu guards cursor alone. Two goroutines sweeping concurrently is not a
	// correctness problem for the STORE -- every row is revalidated and closed
	// by a compare-and-swap -- but a torn continuation is a Go data race, and
	// the cursor is the only state here.
	mu     sync.Mutex
	cursor sessionwire.Cursor
}

// NewRecordSweeper validates a configuration before it can reach a store.
func NewRecordSweeper(cfg SweeperConfig) (*RecordSweeper, error) {
	switch {
	case cfg.Targets == nil:
		return nil, fmt.Errorf("%w: Targets must not be nil", ErrInvalidSweeperConfig)
	case cfg.Claims == nil:
		return nil, fmt.Errorf("%w: Claims must not be nil", ErrInvalidSweeperConfig)
	case cfg.HolderID == "":
		return nil, fmt.Errorf("%w: HolderID must name this replica", ErrInvalidSweeperConfig)
	case cfg.PageLimit < 1 || cfg.PageLimit > storage.MaxOrderedPageLimit:
		return nil, fmt.Errorf("%w: PageLimit must be between 1 and %d", ErrInvalidSweeperConfig, storage.MaxOrderedPageLimit)
	case cfg.MaxPages < 1 || cfg.MaxPages > sessionstore.MaxHostTargetReconcilePages:
		return nil, fmt.Errorf("%w: MaxPages must be between 1 and %d", ErrInvalidSweeperConfig, sessionstore.MaxHostTargetReconcilePages)
	}
	return &RecordSweeper{cfg: cfg}, nil
}

// TargetSweepResult is what one SweepTargets call did.
//
// Unranked and Retained are RENAMED rather than passed through, and the names
// are the point of the type. The store's Withdrawn is the count of rows whose
// own stored expiry had lapsed at the revalidation instant and whose
// compare-and-swap then won -- those, and only those, left the placement rank.
// StillLive is the count the revalidation SAVED: rows a due page named and a
// heartbeat rescued. Anything that reported the due page's own count as
// "removed" would be claiming a Host is gone on the strength of an observation
// that is explicitly weakly consistent.
type TargetSweepResult struct {
	// Scanned is every row the store looked at. It is the sum of the five
	// outcomes below, which the store asserts for itself.
	Scanned int

	// Unranked is the store's Withdrawn: rows this sweep removed from the
	// placement rank.
	Unranked int

	// Retained is the store's StillLive: rows the revalidation refused to
	// withdraw. A nonzero value is the heartbeat race being won by the Host,
	// which is the correct outcome and not a failure.
	Retained int

	Contended  int
	Unreadable int
	Unverified int

	// Exhausted reports that the sweep reached the end of the deadline view.
	Exhausted bool

	// Resumed reports that this call presented a continuation retained from an
	// earlier one. It is observable state rather than a log detail: a sweeper
	// that never resumes makes no progress past its first page budget, and
	// nothing else about the result would say so.
	Resumed bool
}

// SessionRef names one session for the claim half.
type SessionRef struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
}

// ClaimSweepResult accounts for every session one SweepClaims call examined.
type ClaimSweepResult struct {
	Examined int

	// Released counts the sessions whose claim is this replica's and is no
	// longer live afterwards. It deliberately does NOT separate "a write
	// happened" from "the record was already ours and lapsed": the store
	// answers both with success and no error, because a repeat release is the
	// ordinary case rather than a mistake, and a sweeper that reported them
	// differently would be reporting the reply it happened to race.
	Released int

	// Held counts the sessions another replica is reconciling right now.
	Held int

	// Lapsed counts another replica's claim that has run out. Nothing is
	// written for one and nothing needs to be: a lapsed claim is already
	// indistinguishable from no claim to every reader of this record.
	Lapsed int

	// Absent counts the sessions with no claim record at all.
	Absent int
}

// SweepTargets runs one bounded pass of the directory's deadline view.
//
// THE CONTINUATION IS THE ONLY STATE THIS TYPE CARRIES BETWEEN CALLS, and it is
// carried rather than dropped because a page budget alone leaves this sweep able
// to make no progress at all. A row the store steps over -- one a heartbeat
// saved, one whose compare-and-swap lost, one it could not decode -- stays in
// the deadline view, so a sweeper that restarted at the head on every pass would
// meet that row again with its whole budget and never reach the rows behind it.
//
// What is NOT retained is a judgement. This call returns what the store did; it
// does not remember which Hosts were due, and a later placement decision reads
// the directory rather than anything recorded here.
func (s *RecordSweeper) SweepTargets(ctx context.Context) (TargetSweepResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	resumed := s.cursor != ""
	result, err := s.cfg.Targets.ReconcileHostTargets(ctx, sessionstore.ReconcileHostTargetsRequest{
		Limit: s.cfg.PageLimit, MaxPages: s.cfg.MaxPages, Cursor: s.cursor,
	})
	// The position is taken from the reply in every case, INCLUDING a failed
	// one. A sweep that lost its provider mid-walk returns the counts it
	// accrued beside the position it reached, precisely so a caller need not
	// pay the cost of every unreadable row ahead of it a second time; throwing
	// that away on any error would make a transient fault cost a full restart.
	s.cursor = result.NextCursor
	if err != nil {
		// A refused continuation is the one failure where keeping the position
		// is the wrong answer, because the position IS what was refused. This
		// package cannot inspect an opaque token, so a sweeper that kept
		// presenting one would present it forever: every call would return the
		// same error and the deployment would silently stop reclaiming ranked
		// capacity. Re-arming costs one restart at the head, which is the
		// ordinary cost of a sweep that has no continuation.
		if refusedContinuation(err) {
			s.cursor = ""
		}
		return translateTargetSweep(result, resumed), err
	}
	return translateTargetSweep(result, resumed), nil
}

// refusedContinuation reports the store refusing the token this sweep presented.
func refusedContinuation(err error) bool {
	var targetErr *sessionstore.HostTargetError
	return errors.As(err, &targetErr) && targetErr.Code == sessionstore.HostTargetErrorCursor
}

// translateTargetSweep renames the store's accounting into this package's.
//
// Withdrawn becomes Unranked and StillLive becomes Retained, and the renaming
// is the only thing here that could be wrong: a caller's evidence that a Host
// is gone is Unranked, and filling it from Scanned -- or from Withdrawn plus
// StillLive -- would report a Host that heartbeated as one that crashed, on the
// strength of a due page that says no such thing.
func translateTargetSweep(result sessionstore.HostTargetReconcileResult, resumed bool) TargetSweepResult {
	return TargetSweepResult{
		Scanned:    result.Scanned,
		Unranked:   result.Withdrawn,
		Retained:   result.StillLive,
		Contended:  result.Contended,
		Unreadable: result.Unreadable,
		Unverified: result.Unverified,
		Exhausted:  result.Exhausted,
		Resumed:    resumed,
	}
}

// SweepClaims releases this replica's own placement claims for the named
// sessions.
//
// ITS REACH IS THIS REPLICA'S OWN CLAIMS, and that is a limit of the pinned
// store rather than a choice. A reconciliation claim is filed with an EMPTY due
// state (sessionstore@v0.4.0/reconcile.go:708 reconciliationClaimDue), so there
// is no deadline view to walk and no operation that enumerates claims at all:
// the sessions have to come from the caller. And a release is refused for any
// holder but the claim's own, so another replica's lapsed claim can be reported
// but not written back. Nothing is lost by that -- a lapsed claim is already
// indistinguishable from no claim to every reader of the record -- but the
// counts say which happened, because "swept" and "was already harmless" are
// different facts to an operator.
//
// A FAILURE ENDS THE PASS rather than skipping the session. The four
// dispositions below are outcomes; anything else is the store declining to
// answer, and walking the rest of the list would spend one request per session
// against a store that has already said it cannot serve one.
func (s *RecordSweeper) SweepClaims(ctx context.Context, refs []SessionRef) (ClaimSweepResult, error) {
	var result ClaimSweepResult
	for _, ref := range refs {
		_, err := s.cfg.Claims.ReleaseReconciliationClaim(ctx, sessionstore.ReleaseReconciliationClaimRequest{
			TenantID: ref.TenantID, SessionID: ref.SessionID, HolderID: s.cfg.HolderID,
		})
		switch {
		case err == nil:
			result.Released++
		case claimCodeIs(err, sessionstore.ReconcileErrorHeld):
			result.Held++
		case claimCodeIs(err, sessionstore.ReconcileErrorLapsed):
			result.Lapsed++
		case claimCodeIs(err, sessionstore.ReconcileErrorNotFound) || noSuchSession(err):
			result.Absent++
		default:
			return result, fmt.Errorf("placement: release the claim on session %s: %w", ref.SessionID, err)
		}
		result.Examined++
	}
	return result, nil
}

// claimCodeIs reports a typed reconciliation failure carrying one code.
func claimCodeIs(err error, code sessionstore.ReconcileErrorCode) bool {
	var reconcileErr *sessionstore.ReconcileError
	return errors.As(err, &reconcileErr) && reconcileErr.Code == code
}

// noSuchSession is the OTHER spelling of "there is no claim here".
//
// Outside the legacy single-tenant layout the store verifies a session's
// collision witnesses before it reads a record, so a session that was never
// created fails on the binding and never reaches the claim at all. A sweeper
// that read only the reconcile codes would report a whole pass as failed
// because one name in its input list was stale -- which is the same defect
// internal/routing and internal/httpapi each found in their own readers.
// Keyspace codes OTHER than binding_not_found are real deployment faults and
// are deliberately not folded in here.
func noSuchSession(err error) bool {
	var keyspaceErr *sessionstore.KeyspaceError
	return errors.As(err, &keyspaceErr) && keyspaceErr.Code == sessionstore.KeyspaceBindingNotFound
}
