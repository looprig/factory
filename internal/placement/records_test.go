package placement

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

// ---------------------------------------------------------------------------
// The sweep fixture.
//
// It drives the REAL sessionstore over the REAL memstore. Nothing here fakes
// the sweep itself, and that is the point: the revalidation that decides the
// heartbeat race lives in sessionstore.reconcileHostTargetRow, so a fake sweep
// would be a fake of exactly the thing under test. The only interposition is at
// the PROVIDER seam -- a storage.OrderedIndex decorator -- which is where a
// concurrent Host actually writes.
// ---------------------------------------------------------------------------

const sweepRuntime = "runtime-v1"

var sweepTargetKey = sessionstore.HostTargetKey{
	AgentID:                testAgent,
	RuntimeCompatibilityID: sweepRuntime,
	Placement:              sessionwire.HostPlacementPooled,
}

// orderedHooks interposes on the ordered index a sessionstore.Store was opened
// over.
//
// afterListDue runs AFTER the inner provider has returned a due page and
// released whatever lock it held, which is the exact window the sweep's
// revalidation exists to cover: the page's bytes are in the sweep's hand and
// the row is still writable by anyone.
//
// failUpdateMatching refuses one row's compare-and-swap with the provider's own
// revision-conflict error. It models a Host that is heartbeating CONTINUOUSLY
// -- every sweep attempt loses the race -- which is the only way to hold a row
// in the deadline view across several pages, and the fidelity limit is stated
// rather than left implied: it produces the provider error a real concurrent
// writer produces, but it does so without advancing the stored revision, so the
// row's bytes do not change. That is weaker than a real heartbeat in one
// respect only (the value is stale) and the sweep never reads the value again
// after the due page, so nothing downstream of it can tell.
type orderedHooks struct {
	inner storage.OrderedIndex

	afterListDue func()
	listDueFired bool

	// failUpdateMatching refuses the compare-and-swap for any candidate value
	// containing this substring. The match is on the VALUE rather than on the
	// provider identity because sessionstore derives a target's OrderedID from
	// a keyed digest of its scope (host_targets.go:595), which this package
	// cannot compute and must not copy -- a copy would agree with itself while
	// disagreeing with the store.
	failUpdateMatching string
}

func (h *orderedHooks) Get(ctx context.Context, id storage.OrderedID) (storage.OrderedRecord, error) {
	return h.inner.Get(ctx, id)
}

func (h *orderedHooks) Create(
	ctx context.Context, id storage.OrderedID, rankingScope string, value []byte, rank storage.Rank, due storage.Due,
) (storage.OrderedRecord, bool, error) {
	return h.inner.Create(ctx, id, rankingScope, value, rank, due)
}

func (h *orderedHooks) Update(
	ctx context.Context, id storage.OrderedID, expectedRevision uint64, value []byte, rank storage.Rank, due storage.Due,
) (storage.OrderedRecord, error) {
	if h.failUpdateMatching != "" && strings.Contains(string(value), h.failUpdateMatching) {
		return storage.OrderedRecord{}, &storage.OrderedRevisionConflictError{
			ID: id, ExpectedRevision: expectedRevision, ActualRevision: expectedRevision + 1,
		}
	}
	return h.inner.Update(ctx, id, expectedRevision, value, rank, due)
}

func (h *orderedHooks) Delete(ctx context.Context, id storage.OrderedID, expectedRevision uint64) (storage.OrderedRecord, error) {
	return h.inner.Delete(ctx, id, expectedRevision)
}

func (h *orderedHooks) ListOrdered(
	ctx context.Context, namespace, orderingScope string, afterOrder uint64, limit int,
) (storage.OrderedPage, error) {
	return h.inner.ListOrdered(ctx, namespace, orderingScope, afterOrder, limit)
}

func (h *orderedHooks) ListRanked(
	ctx context.Context, namespace, rankingScope string, after storage.RankedCursor, limit int,
) (storage.RankedPage, error) {
	return h.inner.ListRanked(ctx, namespace, rankingScope, after, limit)
}

func (h *orderedHooks) ListDue(
	ctx context.Context, namespace string, dueAtOrBefore int64, after storage.DueCursor, limit int,
) (storage.DuePage, error) {
	page, err := h.inner.ListDue(ctx, namespace, dueAtOrBefore, after, limit)
	if err == nil && h.afterListDue != nil && !h.listDueFired {
		h.listDueFired = true
		h.afterListDue()
	}
	return page, err
}

type sweepFixture struct {
	store   *sessionstore.Store
	clock   *movableClock
	hooks   *orderedHooks
	sweeper *RecordSweeper
}

func newSweepFixture(t *testing.T, pageLimit, maxPages int) *sweepFixture {
	t.Helper()

	clock := &movableClock{now: reconcileNow}
	composite := memstore.New()
	hooks := &orderedHooks{inner: composite.OrderedIndex}
	composite.OrderedIndex = hooks

	store, err := sessionstore.Open(context.Background(), composite, sessionstore.WithClock(clock))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(context.Background()); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	sweeper, err := NewRecordSweeper(SweeperConfig{
		Targets: store, Claims: store, HolderID: "factory-1",
		PageLimit: pageLimit, MaxPages: maxPages,
	})
	if err != nil {
		t.Fatalf("NewRecordSweeper: %v", err)
	}
	return &sweepFixture{store: store, clock: clock, hooks: hooks, sweeper: sweeper}
}

func (f *sweepFixture) publish(t *testing.T, host sessionwire.HostID, generation, capacity uint64, expiresAt time.Time) {
	t.Helper()

	if _, err := f.store.PublishHostTarget(context.Background(), sessionstore.PublishHostTargetRequest{
		Key: sweepTargetKey, HostID: host, HostGeneration: generation, ObservedAt: f.clock.now,
		Advertisement: sessionstore.HostAdvertisement{
			InternalEndpoint:  sessionwire.InternalEndpoint("wss://" + string(host) + ".internal/hostlink"),
			IsolationClass:    sessionwire.HostIsolationClassCrossTenantIsolated,
			Accepting:         true,
			AvailableCapacity: capacity,
			ExpiresAt:         expiresAt,
		},
	}); err != nil {
		t.Fatalf("PublishHostTarget(%s): %v", host, err)
	}
}

// ranked reports the directory exactly as a placement caller would read it:
// every advertisement still occupying a ranked position, by Host and capacity.
// Capacity is carried because it is what tells a RENEWED row from the row the
// sweep was looking at.
func (f *sweepFixture) ranked(t *testing.T) map[sessionwire.HostID]uint64 {
	t.Helper()

	page, err := f.store.ListCompatibleHosts(context.Background(), sessionstore.ListCompatibleHostsRequest{
		Key: sweepTargetKey, Limit: 16,
	})
	if err != nil {
		t.Fatalf("ListCompatibleHosts: %v", err)
	}
	out := map[sessionwire.HostID]uint64{}
	for _, host := range page.Hosts {
		out[host.HostID] = host.AvailableCapacity
	}
	return out
}

func (f *sweepFixture) sweep(t *testing.T) TargetSweepResult {
	t.Helper()

	result, err := f.sweeper.SweepTargets(context.Background())
	if err != nil {
		t.Fatalf("SweepTargets: %v", err)
	}
	return result
}

// ---------------------------------------------------------------------------
// Step 1 -- crashed records leave the rank.
// ---------------------------------------------------------------------------

// TestACrashedHostsAdvertisementIsUnrankedBySweeping is the plain case the
// whole task exists for. sessionstore's placement READER declines to publish a
// lapsed row but leaves it ranked, so a deployment that never sweeps
// accumulates ranked capacity that no longer exists; this is the only thing
// that removes it.
func TestACrashedHostsAdvertisementIsUnrankedBySweeping(t *testing.T) {
	f := newSweepFixture(t, 8, 4)
	f.publish(t, "host-a", 1, 3, f.clock.now.Add(time.Minute))
	f.publish(t, "host-b", 1, 5, f.clock.now.Add(90*time.Second))

	if got := f.ranked(t); len(got) != 2 {
		t.Fatalf("before the crash the directory ranks %v, want both Hosts", got)
	}
	f.clock.now = f.clock.now.Add(2 * time.Minute)

	result := f.sweep(t)
	if result.Scanned != 2 || result.Unranked != 2 {
		t.Fatalf("sweep = %+v, want 2 scanned and 2 unranked", result)
	}
	if !result.Exhausted {
		t.Fatalf("sweep = %+v, want the deadline view exhausted within the budget", result)
	}
	if got := f.ranked(t); len(got) != 0 {
		t.Fatalf("after the sweep the directory still ranks %v, want nothing", got)
	}
}

// ---------------------------------------------------------------------------
// Step 2 -- the heartbeat race.
// ---------------------------------------------------------------------------

// heartbeatRace runs one sweep over two lapsed advertisements, with host-b
// either renewed or not renewed inside the window between the due page being
// read and the sweep acting on it.
//
// BOTH ARMS RUN THE SAME CODE, and the only difference is whether the injected
// write happens. That is what makes the surviving arm's assertion falsifiable:
// the control proves that this fixture, this sweep and this directory read DO
// remove host-b when nothing renews it, so "host-b is still ranked" is a fact
// about the renewal and not about a fixture in which removal was never
// reachable.
func heartbeatRace(t *testing.T, renew bool) (TargetSweepResult, map[sessionwire.HostID]uint64) {
	t.Helper()

	f := newSweepFixture(t, 8, 4)
	f.publish(t, "host-a", 1, 3, f.clock.now.Add(time.Minute))
	f.publish(t, "host-b", 1, 3, f.clock.now.Add(90*time.Second))
	f.clock.now = f.clock.now.Add(2 * time.Minute)

	f.hooks.afterListDue = func() {
		if !renew {
			return
		}
		// The Host heartbeats: a new generation, a fresh horizon and a
		// different capacity, so the row the directory ranks afterwards is
		// identifiable as the RENEWED one rather than the one the sweep read.
		f.publish(t, "host-b", 2, 7, f.clock.now.Add(time.Minute))
	}

	result := f.sweep(t)
	return result, f.ranked(t)
}

// TestARenewedAdvertisementIsNotRemovedByAnOldDueObservation is the
// substantive correctness property of A8.2.
//
// A due page is WEAKLY CONSISTENT: it names the rows that were past their
// deadline when the index was read, and a Host may have heartbeated since. A
// sweep that acted on that naming alone would take a healthy Host out of
// rotation, and the damage is invisible -- the Host is running, holding
// sessions, and simply stops being offered new ones.
//
// What actually stops it is not a check Factory performs. sessionstore
// revalidates the row's own stored expiry and closes the withdrawal with a
// compare-and-swap onto the revision the page reported
// (host_targets.go:1631), so a Host that heartbeated after the page was read
// has already advanced the revision and the write loses. Factory's obligation
// is the negative one: it must not re-derive the judgement, and it must not
// report a row the store refused to remove as removed. Both halves are asserted
// here.
func TestARenewedAdvertisementIsNotRemovedByAnOldDueObservation(t *testing.T) {
	result, ranked := heartbeatRace(t, true)

	if result.Scanned != 2 {
		t.Fatalf("sweep = %+v, want both due rows scanned", result)
	}
	if result.Unranked != 1 {
		t.Fatalf("sweep = %+v, want exactly the crashed Host unranked", result)
	}
	// The renewed row was SAVED, and in THIS fixture it is always saved by the
	// compare-and-swap, so the assertion names that mechanism rather than
	// accepting either.
	//
	// The earlier disjunction (Retained+Contended == 1) was justified by
	// "which of them wins is a timing property". That is true of production
	// and NOT true here. afterListDue is a synchronous callback on the
	// fixture's own goroutine: the renewal lands strictly AFTER the due page's
	// bytes were captured, and reconcileHostTargetRow revalidates those frozen
	// bytes without re-reading the row, so the expiry it sees is always the
	// stale one and Retained (the store's StillLive) is UNREACHABLE, not
	// merely improbable. Measured Contended=1, Retained=0 on every run of both
	// review gates (5/5 and 25/25).
	//
	// What would have to change for the disjunction to be needed: the renewal
	// would have to be able to land BEFORE the page's bytes are captured and
	// still be named by the page -- i.e. a weakly consistent due view, or an
	// asynchronous injection racing ListDue rather than following it. Under
	// either, Retained becomes reachable and this assertion must widen again.
	// Retained's own mapping does not depend on this test: it is pinned by
	// TestAProviderFaultKeepsThePositionTheSweepHadReached.
	if result.Contended != 1 {
		t.Fatalf("sweep = %+v, want the renewed row saved by the compare-and-swap", result)
	}
	if result.Retained != 0 {
		t.Fatalf("sweep = %+v, want StillLive unreachable in this synchronous fixture", result)
	}
	if capacity, ok := ranked["host-b"]; !ok || capacity != 7 {
		t.Fatalf("after the sweep the directory ranks %v, want host-b at the RENEWED capacity 7", ranked)
	}
	if _, ok := ranked["host-a"]; ok {
		t.Fatalf("after the sweep the directory ranks %v, want the crashed host-a gone", ranked)
	}
}

// TestTheHeartbeatRaceControlRemovesTheHostWhenNothingRenewsIt is the positive
// control for the test above, and it is not decoration.
//
// "X was not removed" is only a claim when removal was reachable. The defect it
// guards against is the one A7.3 produced five times in one task: an assertion
// against a fixture in which the thing asserted absent could never have been
// present. This runs the identical fixture, the identical sweep and the
// identical directory read with the injected renewal switched off, and requires
// the OPPOSITE answer on the same two assertions.
func TestTheHeartbeatRaceControlRemovesTheHostWhenNothingRenewsIt(t *testing.T) {
	result, ranked := heartbeatRace(t, false)

	if result.Scanned != 2 {
		t.Fatalf("sweep = %+v, want both due rows scanned", result)
	}
	if result.Unranked != 2 {
		t.Fatalf("sweep = %+v, want both rows unranked when neither heartbeats", result)
	}
	if result.Retained+result.Contended != 0 {
		t.Fatalf("sweep = %+v, want nothing saved when nothing renews", result)
	}
	if len(ranked) != 0 {
		t.Fatalf("after the sweep the directory ranks %v, want nothing", ranked)
	}
}

// TestASweepReportsWhatTheStoreRemovedRatherThanWhatItLookedAt holds the
// mapping this package owns.
//
// Unranked is the store's Withdrawn and Retained is its StillLive. Reporting
// Scanned, or Withdrawn+StillLive, as "removed" would be an upstream caller's
// only evidence that a Host is gone, derived from a page that says no such
// thing. The heartbeat fixture is used because it is the only one in which the
// three numbers differ.
func TestASweepReportsWhatTheStoreRemovedRatherThanWhatItLookedAt(t *testing.T) {
	result, _ := heartbeatRace(t, true)

	if result.Unranked == result.Scanned {
		t.Fatalf("sweep = %+v, want Unranked to differ from Scanned when a row was saved", result)
	}
	// The five outcomes must account for every scanned row. Be honest about
	// what this fixture exercises: only Unranked and Contended are nonzero
	// here, so this checks the sum with three terms at zero. It is not the
	// reader for the other three mappings -- Retained, Unreadable and
	// Unverified are each driven nonzero and asserted by
	// TestAProviderFaultKeepsThePositionTheSweepHadReached, which is where a
	// mutation replacing any of them with a constant dies.
	if result.Unranked+result.Retained+result.Contended+result.Unreadable+result.Unverified != result.Scanned {
		t.Fatalf("sweep = %+v, want the five outcomes to account for every scanned row", result)
	}
}

// ---------------------------------------------------------------------------
// Step 1 -- the continuation.
// ---------------------------------------------------------------------------

// TestASweepResumesItsOwnContinuationRatherThanRestartingAtTheHead drives the
// case sessionstore's DefaultHostTargetReconcilePages warns about: a page
// budget alone leaves the sweep able to make NO progress at all.
//
// A row the sweep steps over -- contended, unreadable, or saved by a heartbeat
// -- stays in the deadline view. A sweeper that restarted at the head on every
// pass would meet that row again with its whole budget and never reach the rows
// behind it. Here host-stuck is held in the view by a provider that refuses its
// compare-and-swap, and the assertion is that host-late is nevertheless
// reached.
func TestASweepResumesItsOwnContinuationRatherThanRestartingAtTheHead(t *testing.T) {
	f := newSweepFixture(t, 1, 1)
	f.publish(t, "host-stuck", 1, 3, f.clock.now.Add(time.Minute))
	f.publish(t, "host-late", 1, 5, f.clock.now.Add(90*time.Second))
	f.hooks.failUpdateMatching = `"host_id":"host-stuck"`
	f.clock.now = f.clock.now.Add(2 * time.Minute)

	first := f.sweep(t)
	if first.Resumed {
		t.Fatalf("first sweep = %+v, want it to start at the head", first)
	}
	// Exhausted must be the STORE's answer, not a constant. Every other case
	// here asserts it true, so a sweeper hard-coding it would pass them all --
	// and a caller reading it would stop sweeping with the view half walked.
	if first.Exhausted {
		t.Fatalf("first sweep = %+v, want a one-page budget to leave the view unexhausted", first)
	}
	if first.Scanned != 1 || first.Contended != 1 || first.Unranked != 0 {
		t.Fatalf("first sweep = %+v, want the stuck row scanned and contended", first)
	}

	second := f.sweep(t)
	if !second.Resumed {
		t.Fatalf("second sweep = %+v, want it to present the retained continuation", second)
	}
	if second.Unranked != 1 {
		t.Fatalf("second sweep = %+v, want the row BEHIND the stuck one reached and unranked", second)
	}
	// The positive observable that the stuck row was never removed. It cannot
	// come from the directory read: ListCompatibleHosts declines to PUBLISH a
	// lapsed row while leaving it ranked, which is the whole reason this sweep
	// exists, so a still-ranked crashed Host and a withdrawn one read
	// identically there. What does distinguish them is the deadline view --
	// a withdrawn row leaves it -- so the sweep is run to exhaustion, re-armed
	// at the head, and required to meet the stuck row again.
	third := f.sweep(t)
	if !third.Exhausted {
		t.Fatalf("third sweep = %+v, want the view exhausted behind the stuck row", third)
	}
	f.hooks.failUpdateMatching = ""
	fourth := f.sweep(t)
	if fourth.Resumed {
		t.Fatalf("fourth sweep = %+v, want the exhausted view to have re-armed at the head", fourth)
	}
	if fourth.Unranked != 1 {
		t.Fatalf("fourth sweep = %+v, want the stuck row still in the deadline view and now removable", fourth)
	}
	if ranked := f.ranked(t); len(ranked) != 0 {
		t.Fatalf("directory ranks %v, want nothing once the contention clears", ranked)
	}
}

// scriptedTargets is a fake for the CONTINUATION ERROR paths only.
//
// Its fidelity limit is stated because it matters: it does not sweep anything
// and it is used in no test that asserts a sweep outcome. It exists because the
// two failures below are refusals sessionstore raises BEFORE it touches a
// provider -- a continuation this store did not issue for a sweep
// (host_targets.go:1605), and a backend fault -- so no provider decorator can
// produce them.
type scriptedTargets struct {
	seen  []sessionwire.Cursor
	steps []func() (sessionstore.HostTargetReconcileResult, error)

	// bounds records the page bounds each call FORWARDED, which the cursor
	// alone does not say. Without it Limit and MaxPages are pinned only
	// against each other: substituting one configured field for the other is
	// invisible to every assertion, and a fixture whose PageLimit equals its
	// MaxPages cannot tell them apart even in principle. scriptedSweeper
	// configures 4 and 2 precisely so the two cannot stand in for one another.
	bounds []sweepBounds
}

// sweepBounds is one call's forwarded page budget.
type sweepBounds struct {
	Limit    int
	MaxPages int
}

func (s *scriptedTargets) ReconcileHostTargets(
	_ context.Context, req sessionstore.ReconcileHostTargetsRequest,
) (sessionstore.HostTargetReconcileResult, error) {
	s.seen = append(s.seen, req.Cursor)
	s.bounds = append(s.bounds, sweepBounds{Limit: req.Limit, MaxPages: req.MaxPages})
	if len(s.seen) > len(s.steps) {
		return sessionstore.HostTargetReconcileResult{Exhausted: true}, nil
	}
	return s.steps[len(s.seen)-1]()
}

func scriptedSweeper(t *testing.T, targets TargetSweep) *RecordSweeper {
	t.Helper()

	sweeper, err := NewRecordSweeper(SweeperConfig{
		Targets: targets, Claims: &scriptedClaims{}, HolderID: "factory-1", PageLimit: 4, MaxPages: 2,
	})
	if err != nil {
		t.Fatalf("NewRecordSweeper: %v", err)
	}
	return sweeper
}

// TestARefusedContinuationRearmsTheSweepInsteadOfWedgingIt.
//
// A continuation is opaque and this package cannot inspect it. If the store
// refuses one -- a restart on the other side of a key rotation, a token whose
// envelope no longer verifies -- a sweeper that kept presenting it would
// present it forever, and the deployment would silently stop reclaiming ranked
// capacity while every call returned the same error. Dropping it costs one
// restart at the head of the deadline view, which is the ordinary cost of a
// sweep with no continuation.
func TestARefusedContinuationRearmsTheSweepInsteadOfWedgingIt(t *testing.T) {
	targets := &scriptedTargets{steps: []func() (sessionstore.HostTargetReconcileResult, error){
		func() (sessionstore.HostTargetReconcileResult, error) {
			return sessionstore.HostTargetReconcileResult{Scanned: 4, Withdrawn: 4, NextCursor: "continue-1"}, nil
		},
		func() (sessionstore.HostTargetReconcileResult, error) {
			// THE POSITION IS RETURNED ALONGSIDE THE REFUSAL, and this fake is
			// deliberately LOOSER than the pinned store on exactly that axis.
			// sessionstore v0.7.0 refuses a continuation in
			// decodeHostTargetSweepCursor, before any provider work, and
			// returns the ZERO result (host_targets.go:1494) -- so against the
			// pinned store the re-arm below is indistinguishable from the
			// ordinary assignment and the branch cannot be driven at all.
			// TargetSweep is a declared SEAM, though, and nothing in it
			// requires an implementation to discard its position when it
			// refuses a token. The looseness is what makes the arm reachable,
			// and it is named here rather than left for a reader to discover:
			// without it, the mutation that deletes the re-arm is an EQUIVALENT
			// mutant against the pinned store and a live wedge against any
			// other implementation.
			return sessionstore.HostTargetReconcileResult{NextCursor: "continue-1"}, &sessionstore.HostTargetError{
				Code: sessionstore.HostTargetErrorCursor, Field: "cursor",
			}
		},
	}}
	sweeper := scriptedSweeper(t, targets)

	if _, err := sweeper.SweepTargets(context.Background()); err != nil {
		t.Fatalf("first SweepTargets: %v", err)
	}
	if _, err := sweeper.SweepTargets(context.Background()); err == nil {
		t.Fatalf("second SweepTargets returned no error, want the refused continuation")
	}
	if _, err := sweeper.SweepTargets(context.Background()); err != nil {
		t.Fatalf("third SweepTargets: %v", err)
	}
	want := []sessionwire.Cursor{"", "continue-1", ""}
	if !reflect.DeepEqual(targets.seen, want) {
		t.Fatalf("continuations presented = %v, want %v", targets.seen, want)
	}
	// The CONFIGURED bounds, not merely nonzero ones, and not each other:
	// scriptedSweeper sets PageLimit 4 and MaxPages 2, so a sweeper that
	// forwarded MaxPages as Limit (or the reverse) fails here. Re-arming the
	// continuation must not disturb them either.
	wantBounds := []sweepBounds{{Limit: 4, MaxPages: 2}, {Limit: 4, MaxPages: 2}, {Limit: 4, MaxPages: 2}}
	if !reflect.DeepEqual(targets.bounds, wantBounds) {
		t.Fatalf("bounds forwarded = %+v, want %+v", targets.bounds, wantBounds)
	}
}

// TestAProviderFaultKeepsThePositionTheSweepHadReached is the control for the
// test above, and it is the property that makes the rearming a decision rather
// than a blanket reset.
//
// A store that lost its provider mid-walk returns the counts it accrued AND the
// position it reached, precisely so a caller need not pay the cost of every
// unreadable row ahead of it again. Dropping the continuation on every error
// would throw that away, and the two tests together are what says the
// distinction is being made rather than one of the two answers being given
// always.
func TestAProviderFaultKeepsThePositionTheSweepHadReached(t *testing.T) {
	targets := &scriptedTargets{steps: []func() (sessionstore.HostTargetReconcileResult, error){
		func() (sessionstore.HostTargetReconcileResult, error) {
			return sessionstore.HostTargetReconcileResult{Scanned: 4, Withdrawn: 4, NextCursor: "continue-1"}, nil
		},
		func() (sessionstore.HostTargetReconcileResult, error) {
			// EVERY counter is distinct and every one is nonzero. This is
			// the only place Unreadable and Unverified are driven at all --
			// the real-store fixtures produce neither, so without this both
			// mappings could be replaced by the constant 0 undetected, even
			// though the store genuinely produces both in production (a frame
			// it could not decode; a withdrawal that committed but failed the
			// reply checks). Distinct values also mean no two of the six can
			// be substituted for each other.
			return sessionstore.HostTargetReconcileResult{
				Scanned: 15, Withdrawn: 1, StillLive: 2, Contended: 3,
				Unreadable: 4, Unverified: 5, NextCursor: "continue-2",
			}, &sessionstore.HostTargetError{
				Code: sessionstore.HostTargetErrorBackend, Field: "list_due",
			}
		},
	}}
	sweeper := scriptedSweeper(t, targets)

	if _, err := sweeper.SweepTargets(context.Background()); err != nil {
		t.Fatalf("first SweepTargets: %v", err)
	}
	failed, err := sweeper.SweepTargets(context.Background())
	if err == nil {
		t.Fatalf("second SweepTargets returned no error, want the provider fault")
	}
	if failed.Scanned != 15 || failed.Unranked != 1 || failed.Retained != 2 ||
		failed.Contended != 3 || failed.Unreadable != 4 || failed.Unverified != 5 {
		t.Fatalf("failed sweep = %+v, want the work it did reported beside the error", failed)
	}
	if _, err := sweeper.SweepTargets(context.Background()); err != nil {
		t.Fatalf("third SweepTargets: %v", err)
	}
	want := []sessionwire.Cursor{"", "continue-1", "continue-2"}
	if !reflect.DeepEqual(targets.seen, want) {
		t.Fatalf("continuations presented = %v, want %v", targets.seen, want)
	}
	wantBounds := []sweepBounds{{Limit: 4, MaxPages: 2}, {Limit: 4, MaxPages: 2}, {Limit: 4, MaxPages: 2}}
	if !reflect.DeepEqual(targets.bounds, wantBounds) {
		t.Fatalf("bounds forwarded = %+v, want %+v", targets.bounds, wantBounds)
	}
}

// concurrentTargets is the fake for the CONCURRENCY probe only.
//
// It keeps no unsynchronised state of its own -- calls are counted with an
// atomic and the returned continuation is derived, not stored -- so the ONLY
// unsynchronised state left in the system under test is RecordSweeper.cursor.
// That is deliberate: it means a -race report from the test below can come
// from nothing but the sweeper's own field.
type concurrentTargets struct {
	calls atomic.Int64
}

func (c *concurrentTargets) ReconcileHostTargets(
	_ context.Context, req sessionstore.ReconcileHostTargetsRequest,
) (sessionstore.HostTargetReconcileResult, error) {
	n := c.calls.Add(1)
	// A NONEMPTY continuation every time, so every call WRITES s.cursor and
	// every call READS it. A fake returning "" would leave the write storing
	// the value already there, which is still a data race but a far less
	// visible one.
	return sessionstore.HostTargetReconcileResult{
		Scanned: 1, Withdrawn: 1, NextCursor: sessionwire.Cursor(fmt.Sprintf("continue-%d", n)),
	}, nil
}

// TestConcurrentSweepsDoNotTearTheContinuation gives records.go's mutex comment
// a reader.
//
// The comment claims a specific hazard -- "a torn continuation is a Go data
// race, and the cursor is the only state here" -- and until this test nothing
// in the suite drove SweepTargets from two goroutines, so removing the
// Lock/Unlock pair would not have been caught by anything. The claim was wider
// than its probe.
//
// Be exact about what the reader is: it is the RACE DETECTOR, not an
// assertion. Dropping the mutex does not make any count below wrong
// deterministically -- a lost cursor update is a legitimate value -- so this
// test only bites under -race, which is how this repository's checks run it.
// The count assertion is here to prove the goroutines actually ran, not to
// detect the tear.
func TestConcurrentSweepsDoNotTearTheContinuation(t *testing.T) {
	// NOT t.Parallel. The reader here is the race detector, and a parallel
	// test lets its report land on whichever test happens to be running, so
	// the kill stops being attributable to this one.
	targets := &concurrentTargets{}
	sweeper := scriptedSweeper(t, targets)

	const goroutines, each = 8, 25
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < each; j++ {
				if _, err := sweeper.SweepTargets(context.Background()); err != nil {
					t.Errorf("SweepTargets: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	if got := targets.calls.Load(); got != goroutines*each {
		t.Fatalf("sweeps performed = %d, want %d", got, goroutines*each)
	}
	// Every call returned a continuation, so the sweeper must be holding one.
	// This is the one observable that a serialised read-modify-write of the
	// cursor happened at all.
	result, err := sweeper.SweepTargets(context.Background())
	if err != nil {
		t.Fatalf("final SweepTargets: %v", err)
	}
	if !result.Resumed {
		t.Fatalf("final sweep = %+v, want it to present a retained continuation", result)
	}
}

// TestAnExhaustedSweepStartsTheNextPassAtTheHead.
//
// The deadline view is not a queue that drains once. A sweep that reached the
// end and then kept presenting its last position would never see a Host that
// crashed afterwards, because a retained continuation carries the BOUND it was
// issued at and a later row is not below that bound.
func TestAnExhaustedSweepStartsTheNextPassAtTheHead(t *testing.T) {
	f := newSweepFixture(t, 8, 4)
	f.publish(t, "host-a", 1, 3, f.clock.now.Add(time.Minute))
	f.clock.now = f.clock.now.Add(2 * time.Minute)

	first := f.sweep(t)
	if !first.Exhausted || first.Unranked != 1 {
		t.Fatalf("first sweep = %+v, want one row unranked and the view exhausted", first)
	}

	f.publish(t, "host-c", 1, 5, f.clock.now.Add(time.Minute))
	f.clock.now = f.clock.now.Add(2 * time.Minute)

	second := f.sweep(t)
	if second.Resumed {
		t.Fatalf("second sweep = %+v, want an exhausted view to leave no continuation", second)
	}
	if second.Unranked != 1 {
		t.Fatalf("second sweep = %+v, want the Host that crashed afterwards unranked", second)
	}
	if ranked := f.ranked(t); len(ranked) != 0 {
		t.Fatalf("directory ranks %v, want nothing", ranked)
	}
}

// ---------------------------------------------------------------------------
// Step 1 -- the claim half.
// ---------------------------------------------------------------------------

func (f *sweepFixture) createSession(t *testing.T, session sessionwire.SessionID) SessionRef {
	t.Helper()

	if _, _, err := f.store.CreateCatalogEntry(context.Background(), sessionstore.CreateCatalogEntryRequest{
		TenantID: testTenant, SessionID: session, AgentID: testAgent, RuntimeCompatibilityID: sweepRuntime,
		CreatedAt: f.clock.now, LastActiveAt: f.clock.now,
		State: sessionwire.SessionStateIdle, Residency: sessionwire.SessionResidencyCold,
		DesiredPlacement: sessionwire.HostPlacementPooled, IdempotencyKey: "create-" + string(session),
	}); err != nil {
		t.Fatalf("CreateCatalogEntry(%s): %v", session, err)
	}
	return SessionRef{TenantID: testTenant, SessionID: session}
}

func (f *sweepFixture) acquire(t *testing.T, ref SessionRef, holder string, ttl time.Duration) {
	t.Helper()

	if _, err := f.store.AcquireReconciliationClaim(context.Background(), sessionstore.AcquireReconciliationClaimRequest{
		TenantID: ref.TenantID, SessionID: ref.SessionID, HolderID: holder, ExpiresAt: f.clock.now.Add(ttl),
	}); err != nil {
		t.Fatalf("AcquireReconciliationClaim(%s, %s): %v", ref.SessionID, holder, err)
	}
}

// claimHolder reports the holder currently recorded for a session, and whether
// that claim is live.
//
// A LAPSED CLAIM CANNOT BE READ BACK: GetReconciliationClaim answers one with
// ReconcileErrorLapsed and withholds the record, because a lapsed claim is not
// an answer to the question it asks. So liveness comes from that read, and the
// HOLDER of a lapsed claim is established the only way the pinned API allows --
// by asking to release it as a named holder, which for a claim that is not the
// asker's is refused with Lapsed and for one that is, is a success that writes
// nothing.
func (f *sweepFixture) claimHolder(t *testing.T, ref SessionRef, candidate string) (live bool, isCandidate bool) {
	t.Helper()

	_, err := f.store.GetReconciliationClaim(context.Background(), sessionstore.GetReconciliationClaimRequest{
		TenantID: ref.TenantID, SessionID: ref.SessionID,
	})
	live = err == nil

	_, err = f.store.ReleaseReconciliationClaim(context.Background(), sessionstore.ReleaseReconciliationClaimRequest{
		TenantID: ref.TenantID, SessionID: ref.SessionID, HolderID: candidate,
	})
	return live, err == nil
}

func (f *sweepFixture) sweepClaims(t *testing.T, refs ...SessionRef) ClaimSweepResult {
	t.Helper()

	result, err := f.sweeper.SweepClaims(context.Background(), refs)
	if err != nil {
		t.Fatalf("SweepClaims: %v", err)
	}
	return result
}

// TestThisReplicasOwnStrandedClaimIsSweptAndSweepingItAgainChangesNothing.
//
// A replica that died between taking a claim and releasing it leaves the
// session claimed until the TTL runs out. Sweeping is what shortens that, and
// the repeat has to be free: a sweeper runs on a timer and will meet the same
// session on every pass for as long as the caller lists it.
func TestThisReplicasOwnStrandedClaimIsSweptAndSweepingItAgainChangesNothing(t *testing.T) {
	f := newSweepFixture(t, 8, 4)
	ref := f.createSession(t, "session-stranded")
	f.acquire(t, ref, "factory-1", time.Minute)

	if live, _ := f.claimHolder(t, ref, "factory-2"); !live {
		t.Fatalf("the claim is not live before the sweep, so the sweep has nothing to do")
	}

	first := f.sweepClaims(t, ref)
	if first.Examined != 1 || first.Released != 1 {
		t.Fatalf("first sweep = %+v, want one session released", first)
	}
	live, ours := f.claimHolder(t, ref, "factory-1")
	if live {
		t.Fatalf("the claim is still live after the sweep")
	}
	if !ours {
		t.Fatalf("the swept claim is no longer this replica's, so the sweep rewrote a holder it should not have")
	}

	second := f.sweepClaims(t, ref)
	if !reflect.DeepEqual(second, first) {
		t.Fatalf("second sweep = %+v, want it identical to the first %+v", second, first)
	}
}

// TestAnotherReplicasLiveClaimIsLeftAlone.
//
// A live claim means another replica is reconciling that session NOW. The claim
// licenses nothing, so taking it away would not corrupt anything -- it would
// simply reinstate the duplicate work the record exists to suppress, and it
// would do so silently.
func TestAnotherReplicasLiveClaimIsLeftAlone(t *testing.T) {
	f := newSweepFixture(t, 8, 4)
	ref := f.createSession(t, "session-busy")
	f.acquire(t, ref, "factory-2", time.Minute)

	result := f.sweepClaims(t, ref)
	if result.Examined != 1 || result.Held != 1 || result.Released != 0 {
		t.Fatalf("sweep = %+v, want the session reported held and nothing released", result)
	}
	live, theirs := f.claimHolder(t, ref, "factory-2")
	if !live {
		t.Fatalf("the other replica's claim stopped being live across the sweep")
	}
	if !theirs {
		t.Fatalf("the other replica's claim changed holder across the sweep")
	}
}

// TestAnotherReplicasLapsedClaimIsReportedRatherThanRewritten.
//
// This is the reach of the claim half, and it is a limit rather than a
// behaviour: the store refuses a release to any holder but the claim's own, so
// a foreign replica's lapsed claim cannot be written back here. It does not
// need to be -- a lapsed claim is already indistinguishable from no claim to
// every reader of the record -- but a caller must not be told it was swept.
func TestAnotherReplicasLapsedClaimIsReportedRatherThanRewritten(t *testing.T) {
	f := newSweepFixture(t, 8, 4)
	ref := f.createSession(t, "session-abandoned")
	f.acquire(t, ref, "factory-2", time.Minute)
	f.clock.now = f.clock.now.Add(2 * time.Minute)

	result := f.sweepClaims(t, ref)
	if result.Examined != 1 || result.Lapsed != 1 || result.Released != 0 {
		t.Fatalf("sweep = %+v, want the session reported lapsed and nothing released", result)
	}
	live, theirs := f.claimHolder(t, ref, "factory-2")
	if live {
		t.Fatalf("the abandoned claim is live, so the fixture did not lapse it")
	}
	if !theirs {
		t.Fatalf("the abandoned claim changed holder across the sweep")
	}
}

// TestASessionWithNoClaimCostsNothingAndIsNotAFailure covers both spellings of
// absence, which are two different store errors and one fact.
//
// A session that was created and never claimed answers ReconcileErrorNotFound.
// A session that does not exist at all fails EARLIER, on its collision
// witnesses, with a *KeyspaceError -- the store verifies those before it reads
// a record. A sweeper that only read the reconcile codes would report the whole
// pass as failed for one stale name in its input list.
func TestASessionWithNoClaimCostsNothingAndIsNotAFailure(t *testing.T) {
	f := newSweepFixture(t, 8, 4)
	created := f.createSession(t, "session-unclaimed")
	absent := SessionRef{TenantID: testTenant, SessionID: "session-never-created"}

	result := f.sweepClaims(t, created, absent)
	if result.Examined != 2 || result.Absent != 2 {
		t.Fatalf("sweep = %+v, want both spellings of absence counted", result)
	}
}

// TestOneClaimSweepCoversEveryListedSessionIndependently.
//
// The four dispositions are reached in ONE call over a mixed list, so a sweeper
// that stopped at the first non-release -- or that let one session's answer
// decide another's -- fails here rather than passing four single-session tests.
func TestOneClaimSweepCoversEveryListedSessionIndependently(t *testing.T) {
	f := newSweepFixture(t, 8, 4)
	stranded := f.createSession(t, "session-stranded")
	busy := f.createSession(t, "session-busy")
	abandoned := f.createSession(t, "session-abandoned")
	unclaimed := f.createSession(t, "session-unclaimed")

	f.acquire(t, abandoned, "factory-2", time.Minute)
	f.clock.now = f.clock.now.Add(2 * time.Minute)
	f.acquire(t, stranded, "factory-1", time.Minute)
	f.acquire(t, busy, "factory-2", time.Minute)

	result := f.sweepClaims(t, abandoned, busy, stranded, unclaimed)
	want := ClaimSweepResult{Examined: 4, Released: 1, Held: 1, Lapsed: 1, Absent: 1}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("sweep = %+v, want %+v", result, want)
	}
}

// scriptedClaims is a fake for the claim half's fault path only.
type scriptedClaims struct {
	calls int
	err   error
}

func (c *scriptedClaims) ReleaseReconciliationClaim(
	_ context.Context, _ sessionstore.ReleaseReconciliationClaimRequest,
) (sessionstore.ReconciliationClaimEntry, error) {
	c.calls++
	if c.calls == 2 {
		return sessionstore.ReconciliationClaimEntry{}, c.err
	}
	return sessionstore.ReconciliationClaimEntry{}, nil
}

// TestAnUnexpectedStoreFailureEndsTheClaimSweepAndReportsTheWorkItDid.
//
// The four dispositions are outcomes; anything else is the store failing to
// answer, and continuing down the list would spend a request per session
// against a store that has already said it cannot serve one. The counts accrued
// so far travel with the error for the reason the target half's do: a caller's
// next decision rests on them.
func TestAnUnexpectedStoreFailureEndsTheClaimSweepAndReportsTheWorkItDid(t *testing.T) {
	backend := &sessionstore.ReconcileError{Code: sessionstore.ReconcileErrorBackend, Field: "record"}
	claims := &scriptedClaims{err: backend}
	sweeper, err := NewRecordSweeper(SweeperConfig{
		Targets: &scriptedTargets{}, Claims: claims, HolderID: "factory-1", PageLimit: 4, MaxPages: 2,
	})
	if err != nil {
		t.Fatalf("NewRecordSweeper: %v", err)
	}

	result, err := sweeper.SweepClaims(context.Background(), []SessionRef{
		{TenantID: testTenant, SessionID: "s1"},
		{TenantID: testTenant, SessionID: "s2"},
		{TenantID: testTenant, SessionID: "s3"},
	})
	if !errors.Is(err, backend) {
		t.Fatalf("SweepClaims error = %v, want the store's backend failure", err)
	}
	if claims.calls != 2 {
		t.Fatalf("the sweep made %d release calls, want it to stop at the failing one", claims.calls)
	}
	if result.Examined != 1 || result.Released != 1 {
		t.Fatalf("sweep = %+v, want the one session it settled reported beside the error", result)
	}
}

// keyspaceClaims refuses every release with one keyspace failure.
type keyspaceClaims struct {
	calls int
	code  sessionstore.KeyspaceErrorCode
}

func (c *keyspaceClaims) ReleaseReconciliationClaim(
	_ context.Context, _ sessionstore.ReleaseReconciliationClaimRequest,
) (sessionstore.ReconciliationClaimEntry, error) {
	c.calls++
	return sessionstore.ReconciliationClaimEntry{}, &sessionstore.KeyspaceError{Code: c.code}
}

// TestOnlyOneKeyspaceCodeMeansTheSessionIsAbsent.
//
// binding_not_found is the store saying "there is no such session". Every other
// keyspace code is a real deployment fault -- an ambiguous marker, a layout
// mismatch, a hash collision, a backend failure -- and folding the family into
// absence would make a sweeper report a corrupted keyspace as a clean pass over
// sessions that do not exist. The distinction is driven in BOTH directions in
// one table, because "binding_not_found is absent" and "the others are not" are
// two claims and a reader of one is not a reader of the other.
func TestOnlyOneKeyspaceCodeMeansTheSessionIsAbsent(t *testing.T) {
	for _, testCase := range []struct {
		code      sessionstore.KeyspaceErrorCode
		wantFault bool
	}{
		{sessionstore.KeyspaceBindingNotFound, false},
		{sessionstore.KeyspaceBindingAmbiguous, true},
		{sessionstore.KeyspaceMarkerAmbiguous, true},
		{sessionstore.KeyspaceLayoutMismatch, true},
		{sessionstore.KeyspaceHashCollision, true},
		{sessionstore.KeyspaceBackend, true},
	} {
		claims := &keyspaceClaims{code: testCase.code}
		sweeper, err := NewRecordSweeper(SweeperConfig{
			Targets: &scriptedTargets{}, Claims: claims, HolderID: "factory-1", PageLimit: 4, MaxPages: 2,
		})
		if err != nil {
			t.Fatalf("NewRecordSweeper: %v", err)
		}
		result, err := sweeper.SweepClaims(context.Background(), []SessionRef{
			{TenantID: testTenant, SessionID: "s1"},
		})
		switch {
		case testCase.wantFault && err == nil:
			t.Errorf("%s was absorbed as %+v, want a fault", testCase.code, result)
		case !testCase.wantFault && err != nil:
			t.Errorf("%s was reported as a fault (%v), want it counted as absence", testCase.code, err)
		case !testCase.wantFault && result.Absent != 1:
			t.Errorf("%s gave %+v, want one absent session", testCase.code, result)
		}
	}
}

// ---------------------------------------------------------------------------
// Step 3 -- a hint is not authority.
// ---------------------------------------------------------------------------

// forbiddenTypes are the values that carry session OWNERSHIP. A sweep that
// could name one could be read as deciding who holds a session.
//
// They are keyed by FULL import path, not by the last segment of one. Core's
// wire package is imported as `sessionwire` and its path ends in `v1`, so a key
// written the way the source spells the import matches nothing -- which is a
// guard that passes because it is looking in the wrong place, and is exactly
// what TestTheOwnershipWalkSeesOwnershipWhereItIsPresent caught.
var forbiddenTypes = map[string]bool{
	"github.com/looprig/core/sessionwire/v1.HostLinkRegistryObservation": true,
	"github.com/looprig/sessionstore.HostRegistrationEntry":              true,
	"github.com/looprig/sessionstore.HostRoute":                          true,
}

// forbiddenWords are the members that carry a LEASE. The Host lease and its
// epoch fence are specification section 15's correctness fence, and nothing
// this sweep produces may be mistaken for one.
//
// They are matched as whole CAMEL-CASE WORDS rather than as substrings, and
// that is a correction rather than a refinement: as substrings, "lease" is
// inside "Release" and "Released", so the claim-half's own vocabulary made this
// guard fail on the legitimate surface. A substring ban whose first real
// subject is a false positive gets deleted or narrowed under pressure, which is
// worse than one that is right.
var forbiddenWords = map[string]bool{
	"epoch": true, "lease": true, "registry": true, "registration": true, "observation": true,
}

// camelWords splits a Go identifier into its lowercased camel-case words.
//
// Its residue, stated as a residue and not as a boundary: it splits on an
// upper-case rune, so an identifier spelled in one case throughout
// ("leaseepoch", "LEASEEPOCH") is one word and is missed, an initialism run
// ("HTTPLease") keeps its run together as one word up to the last capital, and
// a word joined by an underscore or a digit is not separated. This list is NOT
// a closure -- it enumerates the spellings that were thought of. What makes the
// guard worth having anyway is that Go's own exported-member convention
// produces the covered spelling, and that the method-set pin below does not
// depend on any of it.
func camelWords(identifier string) []string {
	var words []string
	start := 0
	for i, r := range identifier {
		if i > 0 && r >= 'A' && r <= 'Z' {
			words = append(words, strings.ToLower(identifier[start:i]))
			start = i
		}
	}
	if start < len(identifier) {
		words = append(words, strings.ToLower(identifier[start:]))
	}
	return words
}

// forbiddenWord reports the first lease-shaped word in an identifier.
func forbiddenWord(identifier string) (string, bool) {
	for _, word := range camelWords(identifier) {
		if forbiddenWords[word] {
			return word, true
		}
	}
	return "", false
}

// reachableViolations walks a type and reports every ownership type and every
// lease-shaped member name it can reach.
//
// It is REFLECTION over the compiled types rather than a scan of source text,
// which is what makes it closed: a member is either in the type or it is not,
// and a spelling this package has not thought of cannot hide from a field name.
// Its one real limit is that it sees the DECLARED surface -- if a method took
// an `any`, or a type carried an interface whose dynamic value was an
// observation, no static walk could see it, and the guard below therefore also
// requires the two store interfaces to have exactly the method sets it names.
func reachableViolations(t reflect.Type, seen map[reflect.Type]bool, path string) []string {
	if t == nil || seen[t] {
		return nil
	}
	seen[t] = true

	var found []string
	if pkg := t.PkgPath(); pkg != "" {
		qualified := pkg + "." + t.Name()
		if forbiddenTypes[qualified] {
			found = append(found, path+": type "+qualified)
		}
	}
	if word, ok := forbiddenWord(t.Name()); ok {
		found = append(found, path+": type name "+t.Name()+" ("+word+")")
	}

	switch t.Kind() {
	case reflect.Struct:
		for i := range t.NumField() {
			field := t.Field(i)
			if word, ok := forbiddenWord(field.Name); ok {
				found = append(found, path+"."+field.Name+": field name ("+word+")")
			}
			found = append(found, reachableViolations(field.Type, seen, path+"."+field.Name)...)
		}
	case reflect.Interface:
		for i := range t.NumMethod() {
			method := t.Method(i)
			if word, ok := forbiddenWord(method.Name); ok {
				found = append(found, path+"."+method.Name+": method name ("+word+")")
			}
			found = append(found, reachableViolations(method.Type, seen, path+"."+method.Name)...)
		}
	case reflect.Func:
		for i := range t.NumIn() {
			found = append(found, reachableViolations(t.In(i), seen, fmt.Sprintf("%s(in %d)", path, i))...)
		}
		for i := range t.NumOut() {
			found = append(found, reachableViolations(t.Out(i), seen, fmt.Sprintf("%s(out %d)", path, i))...)
		}
	case reflect.Pointer, reflect.Slice, reflect.Array, reflect.Map:
		found = append(found, reachableViolations(t.Elem(), seen, path+"[]")...)
	}
	return found
}

// TestTheRecordSweepReachesNoLeaseAndNoOwnership is step 3 made structural.
//
// Reconciliation IMPROVES LIVENESS: it reclaims ranked capacity a crashed Host
// left behind and shortens a stranded claim. It decides nothing about who owns
// a session, and the hint it produces must never be able to stand in for the
// epoch-fenced lease check. Prose cannot hold that -- the two halves of this
// package would drift -- so the sweeper's entire declared surface is walked and
// required to reach no ownership type and no lease-shaped member.
func TestTheRecordSweepReachesNoLeaseAndNoOwnership(t *testing.T) {
	surfaces := []any{
		SweeperConfig{}, TargetSweepResult{}, ClaimSweepResult{}, SessionRef{}, RecordSweeper{},
	}
	for _, surface := range surfaces {
		typ := reflect.TypeOf(surface)
		if found := reachableViolations(typ, map[reflect.Type]bool{}, typ.Name()); len(found) > 0 {
			t.Errorf("%s reaches ownership or lease state: %v", typ.Name(), found)
		}
	}

	// The declared method sets are pinned exactly, because the walk above sees
	// only what a type declares: a store seam that grew a registry method would
	// be reachable from this package without any forbidden NAME appearing.
	for _, seam := range []struct {
		typ  reflect.Type
		want []string
	}{
		{reflect.TypeOf((*TargetSweep)(nil)).Elem(), []string{"ReconcileHostTargets"}},
		{reflect.TypeOf((*ClaimSweep)(nil)).Elem(), []string{"ReleaseReconciliationClaim"}},
	} {
		var got []string
		for i := range seam.typ.NumMethod() {
			got = append(got, seam.typ.Method(i).Name)
		}
		if !reflect.DeepEqual(got, seam.want) {
			t.Errorf("%s declares %v, want exactly %v", seam.typ.Name(), got, seam.want)
		}
	}
}

// TestTheOwnershipWalkSeesOwnershipWhereItIsPresent is the positive control for
// the guard above.
//
// A walk that returned nothing because it was stuck -- a wrong qualified name,
// a recursion that never descended, an empty forbidden list -- would report a
// clean sweeper and a clean everything else identically. Decision is this same
// package's placement answer and it DOES carry an ownership observation, so the
// walk must find it, and must find the lease epoch inside it.
func TestTheOwnershipWalkSeesOwnershipWhereItIsPresent(t *testing.T) {
	typ := reflect.TypeOf(Decision{})
	found := reachableViolations(typ, map[reflect.Type]bool{}, typ.Name())
	if len(found) == 0 {
		t.Fatalf("the walk found nothing in Decision, which carries a registry observation")
	}

	var sawType, sawEpoch bool
	for _, violation := range found {
		if strings.Contains(violation, "sessionwire/v1.HostLinkRegistryObservation") {
			sawType = true
		}
		if strings.Contains(strings.ToLower(violation), "epoch") {
			sawEpoch = true
		}
	}
	if !sawType {
		t.Errorf("the walk did not name the observation type among %v", found)
	}
	if !sawEpoch {
		t.Errorf("the walk did not reach the lease epoch inside it among %v", found)
	}
}

// ---------------------------------------------------------------------------
// Configuration.
// ---------------------------------------------------------------------------

func TestNewRecordSweeperRefusesAnIncompleteConfigurationBeforeAnyStoreCall(t *testing.T) {
	valid := SweeperConfig{
		Targets: &scriptedTargets{}, Claims: &scriptedClaims{}, HolderID: "factory-1",
		PageLimit: 8, MaxPages: 4,
	}
	if _, err := NewRecordSweeper(valid); err != nil {
		t.Fatalf("NewRecordSweeper(valid): %v", err)
	}

	for name, mutate := range map[string]func(*SweeperConfig){
		"no targets":       func(c *SweeperConfig) { c.Targets = nil },
		"no claims":        func(c *SweeperConfig) { c.Claims = nil },
		"no holder":        func(c *SweeperConfig) { c.HolderID = "" },
		"page limit zero":  func(c *SweeperConfig) { c.PageLimit = 0 },
		"page limit huge":  func(c *SweeperConfig) { c.PageLimit = storage.MaxOrderedPageLimit + 1 },
		"max pages zero":   func(c *SweeperConfig) { c.MaxPages = 0 },
		"max pages huge":   func(c *SweeperConfig) { c.MaxPages = sessionstore.MaxHostTargetReconcilePages + 1 },
		"max pages signed": func(c *SweeperConfig) { c.MaxPages = -1 },
	} {
		cfg := valid
		mutate(&cfg)
		sweeper, err := NewRecordSweeper(cfg)
		if !errors.Is(err, ErrInvalidSweeperConfig) {
			t.Errorf("NewRecordSweeper(%s) error = %v, want ErrInvalidSweeperConfig", name, err)
		}
		if sweeper != nil {
			t.Errorf("NewRecordSweeper(%s) returned a sweeper as well as an error", name)
		}
	}
}

// TestTheTwoConfigurationSentinelsAreDistinct.
//
// This package now refuses two different configurations. A caller composing a
// Reconciler and a RecordSweeper reads the sentinel to know which one it got
// wrong, so the two must not be the same error value -- and errors.Is must not
// relate them, which a shared wrapped cause would silently arrange.
func TestTheTwoConfigurationSentinelsAreDistinct(t *testing.T) {
	if errors.Is(ErrInvalidSweeperConfig, ErrInvalidConfig) || errors.Is(ErrInvalidConfig, ErrInvalidSweeperConfig) {
		t.Fatalf("the reconciler and sweeper configuration sentinels are related")
	}
	_, sweeperErr := NewRecordSweeper(SweeperConfig{})
	_, reconcilerErr := NewReconciler(Config{})
	if errors.Is(sweeperErr, ErrInvalidConfig) {
		t.Errorf("a sweeper configuration failure matches the reconciler's sentinel")
	}
	if errors.Is(reconcilerErr, ErrInvalidSweeperConfig) {
		t.Errorf("a reconciler configuration failure matches the sweeper's sentinel")
	}
}
