package placement

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage/memstore"
)

// A dedicated session is RELEASED by deletion desire: a dedicated placement
// whose DesiredWorkload is empty (sessionstore: "a dedicated placement with a
// zero DesiredWorkload already means no workload is wanted"). The D3.1 kind
// lane's F2 found that a later command for that session was admitted and then
// never placed: the reconciler handed the controller the released intent on
// every pass, and a real adapter refuses an empty workload. These cases hold
// the fix -- a live pending RESTORE re-expresses the session's launch template
// as a NEW desired generation -- and its bounds: no other open work (a
// leftover create or input, an applying command) brings a deleted session
// back, because it may be exactly the work the product abandoned by deleting.

var releasedTemplate = sessionstore.DesiredWorkload{
	PayloadVersion: "workload/v1",
	Payload:        []byte(`{"resources":{"cpu":"2"}}`),
}

// strictController mirrors controller v0.2.0's adapter on the one axis F2 is
// about: an intent naming no workload payload version is refused
// (kubernetes.ErrUnsupportedPayload), so a released intent can never place.
type strictController struct {
	recordingController
}

var errUnsupportedPayload = errors.New("strict controller: unsupported workload payload version")

func (c *strictController) EnsureWorkload(ctx context.Context, intent sessionstore.PlacementIntent) error {
	if err := c.recordingController.EnsureWorkload(ctx, intent); err != nil {
		return err
	}
	if intent.Workload.PayloadVersion != releasedTemplate.PayloadVersion {
		return errUnsupportedPayload
	}
	return nil
}

type releasedFixture struct {
	store      *sessionstore.Store
	clock      *movableClock
	controller *strictController
	reconciler *Reconciler
}

// newReleasedFixture creates a dedicated session through the public create
// path (so its catalog keeps the create's InitialWorkload as immutable
// provenance), leaves its create command pending as LEFTOVER open work (not a
// restore), and then releases it with deletion desire. The record is at
// generation 2 afterwards.
func newReleasedFixture(t *testing.T) *releasedFixture {
	t.Helper()

	clock := &movableClock{now: reconcileNow}
	store, err := sessionstore.Open(context.Background(), memstore.New(), sessionstore.WithClock(clock))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(context.Background()); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	admitDedicatedCreate(t, store, clock, releasedTemplate)
	release(t, store, "release-1")
	if got := catalogRecord(t, store); got.DesiredGeneration != 2 || !emptyWorkload(got.DesiredWorkload) {
		t.Fatalf("released record = generation %d workload %+v, want generation 2 with no workload", got.DesiredGeneration, got.DesiredWorkload)
	}
	controller := &strictController{}
	reconciler, err := NewReconciler(Config{
		Directory: mustDirectory(t, store), Catalog: store, Claims: store, Workloads: controller,
		Clock: clock, HolderID: "factory-1", ClaimTTL: time.Minute, CandidateLimit: 8,
	})
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}
	return &releasedFixture{store: store, clock: clock, controller: controller, reconciler: reconciler}
}

func admitDedicatedCreate(t *testing.T, store *sessionstore.Store, clock *movableClock, workload sessionstore.DesiredWorkload) {
	t.Helper()

	payload := []byte(`{"blocks":[{"kind":"text","text":"dedicated"}]}`)
	digest := sha256.Sum256(payload)
	identity := sessionstore.PublicCreateIdentity{
		TenantID: testTenant, SessionID: testSession, CommandID: "create-dedicated",
		Target: sessionstore.HostTargetKey{
			AgentID: testAgent, RuntimeCompatibilityID: testRuntime, Placement: sessionwire.HostPlacementDedicated,
		},
		Binding: sessionstore.SessionBinding{
			StorageBindingID: "storage-a", BindingVersion: "v1",
			RuntimeSessionID: "runtime-session-dedicated", ProtocolMode: sessionstore.ProtocolModeDisposition,
		},
		Kind:          sessionstore.CommandKind("create"),
		PayloadDigest: hex.EncodeToString(digest[:]),
		PayloadSize:   uint64(len(payload)),
	}
	if _, err := store.PreparePublicCreate(context.Background(), sessionstore.PreparePublicCreateRequest{
		Identity: identity, ProposedRuntimeCommandID: "runtime-command-dedicated",
		AcceptedAt: clock.now, ApplyDeadline: clock.now.Add(time.Minute),
		InitialWorkload: workload,
	}); err != nil {
		t.Fatalf("PreparePublicCreate: %v", err)
	}
	if _, created, err := store.AdmitPublicCreate(context.Background(), sessionstore.AdmitPublicCreateRequest{
		Identity: identity, Payload: payload,
	}); err != nil || !created {
		t.Fatalf("AdmitPublicCreate = %t, %v", created, err)
	}
}

// release writes deletion desire the way a product (or the controller lane)
// does: the same dedicated placement, naming no workload.
func release(t *testing.T, store *sessionstore.Store, key string) {
	t.Helper()

	entry, err := store.GetCatalogEntry(context.Background(), sessionstore.GetCatalogEntryRequest{TenantID: testTenant, SessionID: testSession})
	if err != nil {
		t.Fatalf("GetCatalogEntry: %v", err)
	}
	if _, err := store.UpdateCatalogDesiredState(context.Background(), sessionstore.UpdateCatalogDesiredStateRequest{
		TenantID: testTenant, SessionID: testSession, ExpectedRevision: entry.Revision, IdempotencyKey: key,
		DesiredPlacement: sessionwire.HostPlacementDedicated, RuntimeCompatibilityID: testRuntime,
	}); err != nil {
		t.Fatalf("release UpdateCatalogDesiredState: %v", err)
	}
}

func catalogRecord(t *testing.T, store *sessionstore.Store) sessionstore.CatalogRecord {
	t.Helper()

	entry, err := store.GetCatalogEntry(context.Background(), sessionstore.GetCatalogEntryRequest{TenantID: testTenant, SessionID: testSession})
	if err != nil {
		t.Fatalf("GetCatalogEntry: %v", err)
	}
	return entry.Record
}

func emptyWorkload(w sessionstore.DesiredWorkload) bool {
	return w.PayloadVersion == "" && len(w.Payload) == 0
}

func sameWorkload(a, b sessionstore.DesiredWorkload) bool {
	return a.PayloadVersion == b.PayloadVersion && bytes.Equal(a.Payload, b.Payload)
}

func (f *releasedFixture) sweeper(t *testing.T) *PendingSweeper {
	t.Helper()
	return newFakeSweeper(t, f.store, f.reconciler, f.clock, &allowSweeps{})
}

func (f *releasedFixture) sweepAll(t *testing.T) PendingSweepResult {
	t.Helper()

	sweeper := f.sweeper(t)
	total := PendingSweepResult{Outcomes: map[Outcome]int{}}
	for range f.store.ControlShards() {
		result, err := sweeper.Sweep(context.Background(), servicePrincipal(t))
		if err != nil {
			t.Fatalf("Sweep: %v", err)
		}
		total.Sessions += result.Sessions
		total.Failures += result.Failures
		total.Released += result.Released
		for outcome, n := range result.Outcomes {
			total.Outcomes[outcome] += n
		}
	}
	return total
}

// admit files one disposition command for the fixture session under the
// catalog's own binding, with an apply deadline one minute past the clock.
func (f *releasedFixture) admit(t *testing.T, kind sessionstore.CommandKind, id sessionwire.CommandID) sessionstore.DispositionInboxEntry {
	t.Helper()

	binding := catalogRecord(t, f.store).Binding
	entry, _, err := f.store.AdmitDispositionCommand(context.Background(), sessionstore.AdmitDispositionCommandRequest{
		TenantID: testTenant, SessionID: testSession, CommandID: id, Binding: binding,
		ProposedRuntimeCommandID: sessionstore.RuntimeCommandID("runtime-" + string(id)), Kind: kind,
		Payload: []byte(`{}`), AcceptedAt: f.clock.now, ApplyDeadline: f.clock.now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("AdmitDispositionCommand(%s): %v", kind, err)
	}
	return entry
}

// TestAReleasedDedicatedSessionWithAPendingRestoreIsPlacedAgain is F2's repro
// (D3.1 POST /restore). Before the fix the sweep failed this session on every
// pass with the adapter's refusal and no intent the controller could create
// was ever handed over.
func TestAReleasedDedicatedSessionWithAPendingRestoreIsPlacedAgain(t *testing.T) {
	t.Parallel()

	f := newReleasedFixture(t)
	f.admit(t, "restore", "restore-1")
	result := f.sweepAll(t)
	if result.Sessions != 1 || result.Failures != 0 || result.Released != 0 || result.Outcomes[OutcomeReconcileDedicated] != 1 {
		t.Fatalf("sweep = %+v, want one session reconciled dedicated without failure", result)
	}
	record := catalogRecord(t, f.store)
	if record.DesiredGeneration != 3 || !sameWorkload(record.DesiredWorkload, releasedTemplate) {
		t.Fatalf("record = generation %d workload %+v, want generation 3 carrying the create's template", record.DesiredGeneration, record.DesiredWorkload)
	}
	if record.DesiredPlacement != sessionwire.HostPlacementDedicated || record.RuntimeCompatibilityID != testRuntime {
		t.Fatalf("record placement/runtime = %q/%q, want the session's own", record.DesiredPlacement, record.RuntimeCompatibilityID)
	}
	intents := f.controller.intents
	if len(intents) != 1 || intents[0].Generation != 3 || !sameWorkload(intents[0].Workload, releasedTemplate) {
		t.Fatalf("controller intents = %+v, want exactly one generation-3 intent with the template", intents)
	}

	// A second pass re-expresses nothing: the generation is what tells a
	// controller its work is stale, so moving it again would restart a Host
	// that is coming up.
	if again := f.sweepAll(t); again.Failures != 0 {
		t.Fatalf("second sweep = %+v, want no failure", again)
	}
	if got := catalogRecord(t, f.store).DesiredGeneration; got != 3 {
		t.Fatalf("generation after second sweep = %d, want 3", got)
	}
	if last := f.controller.intents[len(f.controller.intents)-1]; last.Generation != 3 {
		t.Fatalf("second sweep ensured generation %d, want 3", last.Generation)
	}
}

// assertStillReleased is the not-revived outcome, asserted whole: counted as
// Released (not a Failure, so no WARN every pass), no desired write, and
// nothing handed to the controller.
func (f *releasedFixture) assertStillReleased(t *testing.T, result PendingSweepResult) {
	t.Helper()

	if result.Sessions != 1 || result.Released != 1 || result.Failures != 0 {
		t.Fatalf("sweep = %+v, want the one session counted Released without failure", result)
	}
	if record := catalogRecord(t, f.store); record.DesiredGeneration != 2 || !emptyWorkload(record.DesiredWorkload) {
		t.Fatalf("record = generation %d workload %+v, want the release untouched", record.DesiredGeneration, record.DesiredWorkload)
	}
	if len(f.controller.intents) != 0 {
		t.Fatalf("controller intents = %+v, want none", f.controller.intents)
	}
}

// TestPendingInputAloneDoesNotReviveAReleasedSession: deleting a session with
// queued input must not bring it back to run that input (review B-F1). The
// leftover create is pending too, so this also covers "any live pending".
func TestPendingInputAloneDoesNotReviveAReleasedSession(t *testing.T) {
	t.Parallel()

	f := newReleasedFixture(t)
	f.admit(t, "input", "input-1")
	f.assertStillReleased(t, f.sweepAll(t))
}

// TestAnApplyingCommandAloneDoesNotReviveAReleasedSession: an applying
// command never expires -- only a successor settles it -- so if it licensed a
// revival, deleting a session mid-turn would guarantee a resurrection.
func TestAnApplyingCommandAloneDoesNotReviveAReleasedSession(t *testing.T) {
	t.Parallel()

	f := newReleasedFixture(t)
	ctx := context.Background()
	create, err := f.store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{TenantID: testTenant, SessionID: testSession, CommandID: "create-dedicated"})
	if err != nil {
		t.Fatalf("GetDispositionCommand: %v", err)
	}
	grant, err := f.store.AcquireResidency(ctx, sessionstore.AcquireResidencyRequest{TenantID: testTenant, SessionID: testSession})
	if err != nil {
		t.Fatalf("AcquireResidency: %v", err)
	}
	t.Cleanup(func() { _ = grant.Release(context.Background()) })
	claimed, _, err := f.store.ClaimDispositionCommand(ctx, sessionstore.ClaimDispositionCommandRequest{
		TenantID: testTenant, SessionID: testSession, CommandID: "create-dedicated",
		ExpectedRevision: create.Revision, Residency: grant, ClaimExpiresAt: f.clock.now.Add(30 * time.Second),
	})
	if err != nil {
		t.Fatalf("ClaimDispositionCommand: %v", err)
	}
	applying, err := f.store.BeginDispositionAttempt(ctx, sessionstore.BeginDispositionAttemptRequest{
		TenantID: testTenant, SessionID: testSession, CommandID: "create-dedicated",
		ExpectedRevision: claimed.Revision, AttemptID: "attempt-1", JournalEpoch: 1,
		ResidencyEpoch: claimed.Record.Claim.ResidencyEpoch, StartedAt: f.clock.now,
	})
	if err != nil || applying.Record.State != sessionstore.InboxStateApplying {
		t.Fatalf("BeginDispositionAttempt = %q, %v; the premise is an applying command", applying.Record.State, err)
	}
	// Past its deadline: applying still makes the session need a Host.
	f.clock.now = f.clock.now.Add(2 * time.Minute)
	f.assertStillReleased(t, f.sweepAll(t))
}

// TestALiveClaimedLeftoverDoesNotReviveAReleasedSession: a claimed command
// inside its deadline keeps the session in the sweep (it needs a Host), but a
// claim is not a request to bring a deleted session back -- only a pending
// restore is. The leftover here is the session's own create, claimed by a
// residency that then went away (v0.8.0 review R-N1, mutant R8).
func TestALiveClaimedLeftoverDoesNotReviveAReleasedSession(t *testing.T) {
	t.Parallel()

	f := newReleasedFixture(t)
	ctx := context.Background()
	create, err := f.store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{TenantID: testTenant, SessionID: testSession, CommandID: "create-dedicated"})
	if err != nil {
		t.Fatalf("GetDispositionCommand: %v", err)
	}
	grant, err := f.store.AcquireResidency(ctx, sessionstore.AcquireResidencyRequest{TenantID: testTenant, SessionID: testSession})
	if err != nil {
		t.Fatalf("AcquireResidency: %v", err)
	}
	t.Cleanup(func() { _ = grant.Release(context.Background()) })
	claimed, _, err := f.store.ClaimDispositionCommand(ctx, sessionstore.ClaimDispositionCommandRequest{
		TenantID: testTenant, SessionID: testSession, CommandID: "create-dedicated",
		ExpectedRevision: create.Revision, Residency: grant, ClaimExpiresAt: f.clock.now.Add(30 * time.Second),
	})
	if err != nil || claimed.Record.State != sessionstore.InboxStateClaimed {
		t.Fatalf("ClaimDispositionCommand = %q, %v; the premise is a claimed command", claimed.Record.State, err)
	}
	if !f.clock.now.Before(claimed.Record.ApplyDeadline) {
		t.Fatalf("the claimed command's deadline %v is not after now %v; the premise is a LIVE claim", claimed.Record.ApplyDeadline, f.clock.now)
	}
	f.assertStillReleased(t, f.sweepAll(t))
}

// TestAnExpiredRestoreDoesNotReviveAReleasedSession: the licence is a LIVE
// restore. One at or past its deadline is the deadline sweep's to reject, even
// when a live input keeps the session in the sweep.
func TestAnExpiredRestoreDoesNotReviveAReleasedSession(t *testing.T) {
	t.Parallel()

	f := newReleasedFixture(t)
	f.admit(t, "restore", "restore-1")
	f.clock.now = f.clock.now.Add(time.Minute)
	f.admit(t, "input", "input-1")
	f.assertStillReleased(t, f.sweepAll(t))
}

func (f *releasedFixture) reconcileOpen(t *testing.T, restore bool) (Result, error) {
	t.Helper()
	return f.reconciler.Reconcile(context.Background(), Request{TenantID: testTenant, SessionID: testSession, RestoreRequested: restore})
}

// TestWithoutARestoreAReleasedSessionIsNotRedesired is the licence's other
// half at the reconciler: no desire is written, the released intent is never
// handed to a controller, and the answer is ErrSessionReleased by name.
func TestWithoutARestoreAReleasedSessionIsNotRedesired(t *testing.T) {
	t.Parallel()

	f := newReleasedFixture(t)
	result, err := f.reconcileOpen(t, false)
	if !errors.Is(err, ErrSessionReleased) || len(f.controller.intents) != 0 {
		t.Fatalf("Reconcile without a restore = %+v, %v, intents %+v; want ErrSessionReleased and no ensure", result, err, f.controller.intents)
	}
	if record := catalogRecord(t, f.store); record.DesiredGeneration != 2 || !emptyWorkload(record.DesiredWorkload) {
		t.Fatalf("record = generation %d workload %+v, want the release untouched", record.DesiredGeneration, record.DesiredWorkload)
	}
}

// TestADedicatedSessionThatIsNotReleasedIsNotRewritten: open work for a
// session whose desire already names a workload writes nothing.
func TestADedicatedSessionThatIsNotReleasedIsNotRewritten(t *testing.T) {
	t.Parallel()

	clock := &movableClock{now: reconcileNow}
	store, err := sessionstore.Open(context.Background(), memstore.New(), sessionstore.WithClock(clock))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(context.Background()) })
	admitDedicatedCreate(t, store, clock, releasedTemplate)
	controller := &strictController{}
	reconciler, err := NewReconciler(Config{
		Directory: mustDirectory(t, store), Catalog: store, Claims: store, Workloads: controller,
		Clock: clock, HolderID: "factory-1", ClaimTTL: time.Minute, CandidateLimit: 8,
	})
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}
	result, err := reconciler.Reconcile(context.Background(), Request{TenantID: testTenant, SessionID: testSession, RestoreRequested: true})
	if err != nil || result.DesiredWrites != 0 {
		t.Fatalf("Reconcile = writes %d, %v; want no write", result.DesiredWrites, err)
	}
	if got := catalogRecord(t, store).DesiredGeneration; got != 1 {
		t.Fatalf("generation = %d, want 1", got)
	}
}

// TestAReleasedSessionWithNoCreateProvenanceIsRefusedByName: a session made
// through CreateCatalogEntry carries no public create, so there is no template
// to vouch for; the session is refused with ErrNoLaunchTemplate and nothing is
// written or ensured.
func TestAReleasedSessionWithNoCreateProvenanceIsRefusedByName(t *testing.T) {
	t.Parallel()

	f := newFixture(t, sessionwire.HostPlacementDedicated, "factory-1")
	release(t, f.store, "release-1")
	before := f.generation(t)
	_, err := f.reconciler.Reconcile(context.Background(), Request{TenantID: testTenant, SessionID: testSession, RestoreRequested: true})
	if !errors.Is(err, ErrNoLaunchTemplate) {
		t.Fatalf("Reconcile = %v, want ErrNoLaunchTemplate", err)
	}
	if got := f.generation(t); got != before {
		t.Fatalf("generation = %d, want %d unchanged", got, before)
	}
	if len(f.controller.intents) != 0 {
		t.Fatalf("controller intents = %+v, want none", f.controller.intents)
	}
}

// TestAReplicaWithNoControllerStillRedesires: desire is Factory-authored and a
// controller process never writes it, so a Factory composed with no workload
// controller (H5's split) must re-express the template and only then report
// that it cannot ensure the workload itself.
func TestAReplicaWithNoControllerStillRedesires(t *testing.T) {
	t.Parallel()

	f := newReleasedFixture(t)
	reconciler, err := NewReconciler(Config{
		Directory: mustDirectory(t, f.store), Catalog: f.store, Claims: f.store,
		Clock: f.clock, HolderID: "factory-2", ClaimTTL: time.Minute, CandidateLimit: 8,
	})
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}
	_, err = reconciler.Reconcile(context.Background(), Request{TenantID: testTenant, SessionID: testSession, RestoreRequested: true})
	if !errors.Is(err, ErrNoWorkloadController) {
		t.Fatalf("Reconcile = %v, want ErrNoWorkloadController", err)
	}
	if record := catalogRecord(t, f.store); record.DesiredGeneration != 3 || !sameWorkload(record.DesiredWorkload, releasedTemplate) {
		t.Fatalf("record = generation %d workload %+v, want generation 3 with the template", record.DesiredGeneration, record.DesiredWorkload)
	}
}

// TestEveryReleaseCycleGetsANewGeneration: release, restore, release, restore.
// Each re-expression is its own generation, strictly above the release it
// replaces, and a later release is never mistaken for a replay of the first
// re-expression (the key names the released generation).
func TestEveryReleaseCycleGetsANewGeneration(t *testing.T) {
	t.Parallel()

	f := newReleasedFixture(t)
	if _, err := f.reconcileOpen(t, true); err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	release(t, f.store, "release-2")
	if got := catalogRecord(t, f.store).DesiredGeneration; got != 4 {
		t.Fatalf("second release generation = %d, want 4", got)
	}
	result, err := f.reconcileOpen(t, true)
	if err != nil || result.DesiredWrites != 1 {
		t.Fatalf("second Reconcile = writes %d, %v; want one write", result.DesiredWrites, err)
	}
	record := catalogRecord(t, f.store)
	if record.DesiredGeneration != 5 || !sameWorkload(record.DesiredWorkload, releasedTemplate) {
		t.Fatalf("record = generation %d workload %+v, want generation 5 with the template", record.DesiredGeneration, record.DesiredWorkload)
	}
	if last := f.controller.intents[len(f.controller.intents)-1]; last.Generation != 5 {
		t.Fatalf("ensured generation %d, want 5", last.Generation)
	}
}

// racingRedesire lets a racer write between this replica's read and its
// compare-and-swap, on the FIRST desired-state write only.
type racingRedesire struct {
	*sessionstore.Store
	race  func()
	raced bool
	keys  []string
}

func (c *racingRedesire) UpdateCatalogDesiredState(ctx context.Context, req sessionstore.UpdateCatalogDesiredStateRequest) (sessionstore.CatalogEntry, error) {
	c.keys = append(c.keys, req.IdempotencyKey)
	if !c.raced {
		c.raced = true
		c.race()
	}
	return c.Store.UpdateCatalogDesiredState(ctx, req)
}

func (f *releasedFixture) racingReconciler(t *testing.T, catalog Catalog) *Reconciler {
	t.Helper()
	reconciler, err := NewReconciler(Config{
		Directory: mustDirectory(t, f.store), Catalog: catalog, Claims: f.store, Workloads: f.controller,
		Clock: f.clock, HolderID: "factory-2", ClaimTTL: time.Minute, CandidateLimit: 8,
	})
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}
	return reconciler
}

// TestTwoReplicasReexpressingOneReleaseWriteOneGeneration: the racer is
// another replica re-expressing the same release (its own reconciler, so its
// own derived key). This replica's write is absorbed as a replay.
func TestTwoReplicasReexpressingOneReleaseWriteOneGeneration(t *testing.T) {
	t.Parallel()

	f := newReleasedFixture(t)
	catalog := &racingRedesire{Store: f.store}
	catalog.race = func() {
		entry, err := f.store.GetCatalogEntry(context.Background(), sessionstore.GetCatalogEntryRequest{TenantID: testTenant, SessionID: testSession})
		if err != nil {
			t.Errorf("racer read: %v", err)
			return
		}
		if _, _, err := f.reconciler.reviveReleased(context.Background(), Request{TenantID: testTenant, SessionID: testSession}, entry); err != nil {
			t.Errorf("racer revive: %v", err)
		}
	}
	result, err := f.racingReconciler(t, catalog).Reconcile(context.Background(), Request{TenantID: testTenant, SessionID: testSession, RestoreRequested: true})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if record := catalogRecord(t, f.store); record.DesiredGeneration != 3 || !sameWorkload(record.DesiredWorkload, releasedTemplate) {
		t.Fatalf("record = generation %d workload %+v, want ONE re-expression at generation 3", record.DesiredGeneration, record.DesiredWorkload)
	}
	if result.DesiredWrites != 1 || result.Intent.Generation != 3 {
		t.Fatalf("result = writes %d intent generation %d, want one absorbed write ensuring generation 3", result.DesiredWrites, result.Intent.Generation)
	}
}

// TestARacersWorkloadIsNeverOverwritten: a racer that wrote a DIFFERENT
// workload between the read and the swap wins; the conflict re-reads a record
// that is no longer released and this replica ensures the racer's intent.
func TestARacersWorkloadIsNeverOverwritten(t *testing.T) {
	t.Parallel()

	f := newReleasedFixture(t)
	other := sessionstore.DesiredWorkload{PayloadVersion: "workload/v1", Payload: []byte(`{"resources":{"cpu":"8"}}`)}
	catalog := &racingRedesire{Store: f.store}
	catalog.race = func() {
		entry, err := f.store.GetCatalogEntry(context.Background(), sessionstore.GetCatalogEntryRequest{TenantID: testTenant, SessionID: testSession})
		if err != nil {
			t.Errorf("racer read: %v", err)
			return
		}
		if _, err := f.store.UpdateCatalogDesiredState(context.Background(), sessionstore.UpdateCatalogDesiredStateRequest{
			TenantID: testTenant, SessionID: testSession, ExpectedRevision: entry.Revision, IdempotencyKey: "racer",
			DesiredPlacement: sessionwire.HostPlacementDedicated, RuntimeCompatibilityID: testRuntime, DesiredWorkload: other,
		}); err != nil {
			t.Errorf("racer write: %v", err)
		}
	}
	result, err := f.racingReconciler(t, catalog).Reconcile(context.Background(), Request{TenantID: testTenant, SessionID: testSession, RestoreRequested: true})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	record := catalogRecord(t, f.store)
	if record.DesiredGeneration != 3 || !sameWorkload(record.DesiredWorkload, other) {
		t.Fatalf("record = generation %d workload %+v, want the racer's generation 3 untouched", record.DesiredGeneration, record.DesiredWorkload)
	}
	if len(catalog.keys) != 1 || result.Intent.Generation != 3 || !sameWorkload(result.Intent.Workload, other) {
		t.Fatalf("writes %v, intent %+v; want one refused write then the racer's intent ensured", catalog.keys, result.Intent)
	}
}

// TestARacingReleaseIsAnsweredWithAFreshGeneration: a racer that RELEASED
// again between the read and the swap leaves the session released, so the
// re-read re-expresses once more, above the new release, under a new key.
func TestARacingReleaseIsAnsweredWithAFreshGeneration(t *testing.T) {
	t.Parallel()

	f := newReleasedFixture(t)
	catalog := &racingRedesire{Store: f.store}
	catalog.race = func() { release(t, f.store, "release-racer") }
	if _, err := f.racingReconciler(t, catalog).Reconcile(context.Background(), Request{TenantID: testTenant, SessionID: testSession, RestoreRequested: true}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	record := catalogRecord(t, f.store)
	if record.DesiredGeneration != 4 || !sameWorkload(record.DesiredWorkload, releasedTemplate) {
		t.Fatalf("record = generation %d workload %+v, want generation 4 with the template", record.DesiredGeneration, record.DesiredWorkload)
	}
	if len(catalog.keys) != 2 || catalog.keys[0] == catalog.keys[1] {
		t.Fatalf("keys = %v, want two writes under two keys", catalog.keys)
	}
}

// TestAnEmptyCreateWorkloadIsNotATemplate: a public create that proposed no
// workload leaves nothing to re-express, and re-expressing "nothing" would
// only write another release.
func TestAnEmptyCreateWorkloadIsNotATemplate(t *testing.T) {
	t.Parallel()

	clock := &movableClock{now: reconcileNow}
	store, err := sessionstore.Open(context.Background(), memstore.New(), sessionstore.WithClock(clock))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(context.Background()) })
	admitDedicatedCreate(t, store, clock, sessionstore.DesiredWorkload{})
	release(t, store, "release-1")
	reconciler, err := NewReconciler(Config{
		Directory: mustDirectory(t, store), Catalog: store, Claims: store, Workloads: &strictController{},
		Clock: clock, HolderID: "factory-1", ClaimTTL: time.Minute, CandidateLimit: 8,
	})
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}
	_, err = reconciler.Reconcile(context.Background(), Request{TenantID: testTenant, SessionID: testSession, RestoreRequested: true})
	if !errors.Is(err, ErrNoLaunchTemplate) {
		t.Fatalf("Reconcile = %v, want ErrNoLaunchTemplate", err)
	}
	if got := catalogRecord(t, store).DesiredGeneration; got != 2 {
		t.Fatalf("generation = %d, want 2", got)
	}
}
