package placement

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/routing"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

var reconcileNow = time.Date(2026, 9, 7, 9, 0, 0, 0, time.UTC)

const (
	testTenant  = sessionwire.TenantID("tenant-a")
	testSession = sessionwire.SessionID("session-a")
	testAgent   = sessionwire.AgentID("agent-a")
	testRuntime = "runtime-v1"
)

type movableClock struct{ now time.Time }

func (c *movableClock) Now() time.Time { return c.now }

// recordingController counts what a dedicated reconciliation asked for. It
// records the whole intent because the generation is what a controller records
// against the workload it created.
type recordingController struct {
	intents []sessionstore.PlacementIntent
	err     error
}

func (c *recordingController) EnsureWorkload(_ context.Context, intent sessionstore.PlacementIntent) error {
	c.intents = append(c.intents, intent)
	return c.err
}

type fixture struct {
	store      *sessionstore.Store
	clock      *movableClock
	controller *recordingController
	reconciler *Reconciler
}

func newFixture(t *testing.T, placement sessionwire.HostPlacement, holder string) *fixture {
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
	if _, _, err := store.CreateCatalogEntry(context.Background(), sessionstore.CreateCatalogEntryRequest{
		TenantID: testTenant, SessionID: testSession, AgentID: testAgent, RuntimeCompatibilityID: testRuntime,
		CreatedAt: clock.now, LastActiveAt: clock.now,
		State: sessionwire.SessionStateIdle, Residency: sessionwire.SessionResidencyCold,
		DesiredPlacement: placement, IdempotencyKey: "create-command-1",
	}); err != nil {
		t.Fatalf("CreateCatalogEntry: %v", err)
	}
	directory, err := routing.NewDirectory(store, routing.DefaultLimits())
	if err != nil {
		t.Fatalf("NewDirectory: %v", err)
	}
	controller := &recordingController{}
	reconciler, err := NewReconciler(Config{
		Directory: directory, Catalog: store, Claims: store, Workloads: controller,
		Clock: clock, HolderID: holder, ClaimTTL: time.Minute, CandidateLimit: 8,
	})
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}
	return &fixture{store: store, clock: clock, controller: controller, reconciler: reconciler}
}

func (f *fixture) reconcile(t *testing.T, desired *Desired) Result {
	t.Helper()

	result, err := f.reconciler.Reconcile(context.Background(), Request{
		TenantID: testTenant, SessionID: testSession, Desired: desired,
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	return result
}

func (f *fixture) generation(t *testing.T) uint64 {
	t.Helper()

	entry, err := f.store.GetCatalogEntry(context.Background(), sessionstore.GetCatalogEntryRequest{
		TenantID: testTenant, SessionID: testSession,
	})
	if err != nil {
		t.Fatalf("GetCatalogEntry: %v", err)
	}
	return entry.Record.DesiredGeneration
}

func (f *fixture) claimCode(t *testing.T) sessionstore.ReconcileErrorCode {
	t.Helper()

	_, err := f.store.GetReconciliationClaim(context.Background(), sessionstore.GetReconciliationClaimRequest{
		TenantID: testTenant, SessionID: testSession,
	})
	if err == nil {
		return ""
	}
	var reconcileErr *sessionstore.ReconcileError
	if !errors.As(err, &reconcileErr) {
		t.Fatalf("GetReconciliationClaim error = %v, want a typed reconcile error", err)
	}
	return reconcileErr.Code
}

func (f *fixture) publishTarget(t *testing.T, host sessionwire.HostID, capacity uint64, class sessionwire.HostIsolationClass) {
	t.Helper()

	if _, err := f.store.PublishHostTarget(context.Background(), sessionstore.PublishHostTargetRequest{
		Key:    sessionstore.HostTargetKey{AgentID: testAgent, RuntimeCompatibilityID: testRuntime, Placement: sessionwire.HostPlacementPooled},
		HostID: host, HostGeneration: 1, ObservedAt: f.clock.now,
		Advertisement: sessionstore.HostAdvertisement{
			InternalEndpoint: sessionwire.InternalEndpoint("wss://" + string(host) + ".internal/hostlink"),
			IsolationClass:   class, Accepting: true, AvailableCapacity: capacity,
			ExpiresAt: f.clock.now.Add(time.Minute),
		},
	}); err != nil {
		t.Fatalf("PublishHostTarget(%s): %v", host, err)
	}
}

func (f *fixture) putOwner(t *testing.T, placement sessionwire.HostPlacement) {
	t.Helper()

	if _, err := f.store.PutHostRegistration(context.Background(), sessionstore.PutHostRegistrationRequest{
		TenantID: testTenant, SessionID: testSession, LeaseEpoch: 6,
		ObservedAt: f.clock.now, ExpiresAt: f.clock.now.Add(time.Minute),
		Route: sessionstore.HostRoute{
			HostID: "host-owner", HostGeneration: 2, AgentID: testAgent, RuntimeCompatibilityID: testRuntime,
			Placement: placement, InternalEndpoint: "wss://host-owner.internal/hostlink",
			Residency: sessionwire.SessionResidencyResident, Accepting: true,
		},
	}); err != nil {
		t.Fatalf("PutHostRegistration: %v", err)
	}
}

// TestAResidentOwnerIsRoutedWithoutTakingAClaim is specification section 15
// step 1 read as a cost as well as a rule: an owned session is the common case,
// and serializing it behind a claim would put a durable compare-and-swap in
// front of every command for a session that needs no placement at all.
func TestAResidentOwnerIsRoutedWithoutTakingAClaim(t *testing.T) {
	t.Parallel()

	f := newFixture(t, sessionwire.HostPlacementPooled, "factory-1")
	f.putOwner(t, sessionwire.HostPlacementPooled)
	f.publishTarget(t, "host-free", 9, sessionwire.HostIsolationClassCrossTenantIsolated)

	result := f.reconcile(t, nil)
	if result.Decision.Outcome != OutcomeReuseOwner || result.Decision.Owner.HostID != "host-owner" {
		t.Fatalf("decision = %+v, want the resident owner", result.Decision)
	}
	if result.Decision.Owner.LeaseEpoch != 6 {
		t.Errorf("owner lease epoch = %d, want 6", result.Decision.Owner.LeaseEpoch)
	}
	if code := f.claimCode(t); code != sessionstore.ReconcileErrorNotFound {
		t.Errorf("claim state = %q, want no claim record at all", code)
	}
	if len(f.controller.intents) != 0 {
		t.Errorf("controller received %+v, want nothing for an owned session", f.controller.intents)
	}
}

// TestPooledPlacementSelectsACandidateUnderItsOwnClaimAndReleasesIt drives the
// whole pooled path against the real store: the claim is taken, the ranked
// admissible candidate is selected, and the claim is given back rather than
// left to lapse.
func TestPooledPlacementSelectsACandidateUnderItsOwnClaimAndReleasesIt(t *testing.T) {
	t.Parallel()

	f := newFixture(t, sessionwire.HostPlacementPooled, "factory-1")
	f.publishTarget(t, "host-exclusive", 50, sessionwire.HostIsolationClassTenantExclusive)
	f.publishTarget(t, "host-shared", 4, sessionwire.HostIsolationClassCrossTenantIsolated)

	result := f.reconcile(t, nil)
	if result.Decision.Outcome != OutcomeAttachPooled || result.Decision.Target.HostID != "host-shared" {
		t.Fatalf("decision = %+v, want the cross-tenant-isolated candidate", result.Decision)
	}
	if result.Deferred {
		t.Error("Deferred = true, want this replica to have done the work")
	}
	if code := f.claimCode(t); code != sessionstore.ReconcileErrorLapsed {
		t.Errorf("claim state after placement = %q, want a released claim", code)
	}
	if len(f.controller.intents) != 0 {
		t.Errorf("controller received %+v, want no workload for a pooled session", f.controller.intents)
	}
}

// TestNoAdmissibleCapacityIsReportedRatherThanRefused keeps "the pool is full"
// out of the error channel: it is a retryable state an autoscaler resolves.
func TestNoAdmissibleCapacityIsReportedRatherThanRefused(t *testing.T) {
	t.Parallel()

	f := newFixture(t, sessionwire.HostPlacementPooled, "factory-1")
	f.publishTarget(t, "host-exclusive", 50, sessionwire.HostIsolationClassTenantExclusive)

	result := f.reconcile(t, nil)
	if result.Decision.Outcome != OutcomeNoCapacity {
		t.Fatalf("decision = %+v, want %v", result.Decision, OutcomeNoCapacity)
	}
	if code := f.claimCode(t); code != sessionstore.ReconcileErrorLapsed {
		t.Errorf("claim state = %q, want the claim released even with nothing to place on", code)
	}
}

// TestASecondFactoryObservingAClaimDefersRatherThanScaling is A4.2 step 4 and
// H5's no-leader argument: the claim suppresses DUPLICATE SCALING and nothing
// else, so the loser must not reach the controller.
func TestASecondFactoryObservingAClaimDefersRatherThanScaling(t *testing.T) {
	t.Parallel()

	f := newFixture(t, sessionwire.HostPlacementDedicated, "factory-2")
	before := f.generation(t)
	if _, err := f.store.AcquireReconciliationClaim(context.Background(), sessionstore.AcquireReconciliationClaimRequest{
		TenantID: testTenant, SessionID: testSession, HolderID: "factory-1", ExpiresAt: f.clock.now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("AcquireReconciliationClaim: %v", err)
	}

	result := f.reconcile(t, &Desired{
		Placement: sessionwire.HostPlacementDedicated, RuntimeCompatibilityID: testRuntime,
		Workload: sessionstore.DesiredWorkload{PayloadVersion: "v1", Payload: []byte(`{"replicas":1}`)},
	})
	if !result.Deferred {
		t.Fatalf("result = %+v, want the second replica to defer", result)
	}
	if result.Decision.Outcome != OutcomeUndecided {
		t.Errorf("Outcome = %v, want no decision from a replica that did not do the work", result.Decision.Outcome)
	}
	if !result.ClaimExpiresAt.Equal(f.clock.now.Add(time.Minute)) {
		t.Errorf("ClaimExpiresAt = %v, want the holder's horizon %v", result.ClaimExpiresAt, f.clock.now.Add(time.Minute))
	}
	if len(f.controller.intents) != 0 {
		t.Errorf("controller received %+v, want no duplicate scaling", f.controller.intents)
	}
	if after := f.generation(t); after != before {
		t.Errorf("desired generation = %d, want %d unchanged by a deferred replica", after, before)
	}
	if code := f.claimCode(t); code != "" {
		t.Errorf("claim state = %q, want the holder's claim still live", code)
	}
}

// lateOwnerDirectory reports no owner on the first read and the real one
// afterwards, which is the interleaving the deferral path exists for: the
// winning replica finished placing between this replica's registry read and its
// refused acquisition.
type lateOwnerDirectory struct {
	*routing.Directory
	reads int
}

func (d *lateOwnerDirectory) Owner(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) (sessionwire.HostLinkRegistryObservation, bool, error) {
	d.reads++
	if d.reads == 1 {
		return sessionwire.HostLinkRegistryObservation{}, false, nil
	}
	return d.Directory.Owner(ctx, tenant, session)
}

// TestADeferringFactoryStillReportsAnOwnerThatAppeared is specification
// section 15 step 4: observing an existing claim means re-reading observed
// state, not answering "busy" for a session that now has a Host.
func TestADeferringFactoryStillReportsAnOwnerThatAppeared(t *testing.T) {
	t.Parallel()

	f := newFixture(t, sessionwire.HostPlacementPooled, "factory-2")
	if _, err := f.store.AcquireReconciliationClaim(context.Background(), sessionstore.AcquireReconciliationClaimRequest{
		TenantID: testTenant, SessionID: testSession, HolderID: "factory-1", ExpiresAt: f.clock.now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("AcquireReconciliationClaim: %v", err)
	}
	f.putOwner(t, sessionwire.HostPlacementPooled)
	directory := &lateOwnerDirectory{Directory: mustDirectory(t, f.store)}
	reconciler, err := NewReconciler(Config{
		Directory: directory, Catalog: f.store, Claims: f.store, Workloads: f.controller,
		Clock: f.clock, HolderID: "factory-2", ClaimTTL: time.Minute, CandidateLimit: 8,
	})
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}

	result, err := reconciler.Reconcile(context.Background(), Request{TenantID: testTenant, SessionID: testSession})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if directory.reads != 2 {
		t.Fatalf("registry reads = %d, want the first miss and the re-read under the refused claim", directory.reads)
	}
	if result.Deferred {
		t.Fatalf("result = %+v, want the owner reported rather than a deferral", result)
	}
	if result.Decision.Outcome != OutcomeReuseOwner || result.Decision.Owner.HostID != "host-owner" {
		t.Fatalf("decision = %+v, want the owner the winning replica produced", result.Decision)
	}
}

// TestALapsedClaimIsTakenOverByTheNextFactory is what recovers a crashed
// replica: the claim's expiry is the whole recovery mechanism, so a lapsed one
// must not keep a session unplaced.
func TestALapsedClaimIsTakenOverByTheNextFactory(t *testing.T) {
	t.Parallel()

	f := newFixture(t, sessionwire.HostPlacementPooled, "factory-2")
	f.publishTarget(t, "host-shared", 4, sessionwire.HostIsolationClassCrossTenantIsolated)
	if _, err := f.store.AcquireReconciliationClaim(context.Background(), sessionstore.AcquireReconciliationClaimRequest{
		TenantID: testTenant, SessionID: testSession, HolderID: "factory-1", ExpiresAt: f.clock.now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("AcquireReconciliationClaim: %v", err)
	}

	deferred := f.reconcile(t, nil)
	if !deferred.Deferred {
		t.Fatalf("result while the claim is live = %+v, want a deferral", deferred)
	}

	f.clock.now = f.clock.now.Add(2 * time.Minute)
	f.publishTarget(t, "host-shared", 4, sessionwire.HostIsolationClassCrossTenantIsolated)
	taken := f.reconcile(t, nil)
	if taken.Deferred || taken.Decision.Outcome != OutcomeAttachPooled || taken.Decision.Target.HostID != "host-shared" {
		t.Fatalf("result after the claim lapsed = %+v, want this replica to place", taken)
	}
}

// TestOneDesiredIntentIsWrittenOnceHoweverOftenItIsReconciled is A4.2 step 1's
// idempotent desired generation. The generation is what tells a controller its
// work is stale, so a reconciler that rewrote the same intent would invalidate
// every controller's completed work on every pass.
func TestOneDesiredIntentIsWrittenOnceHoweverOftenItIsReconciled(t *testing.T) {
	t.Parallel()

	f := newFixture(t, sessionwire.HostPlacementDedicated, "factory-1")
	before := f.generation(t)
	desired := &Desired{
		Placement: sessionwire.HostPlacementDedicated, RuntimeCompatibilityID: testRuntime,
		Workload: sessionstore.DesiredWorkload{PayloadVersion: "v1", Payload: []byte(`{"cpu":"2"}`)},
	}

	first := f.reconcile(t, desired)
	if first.Decision.Outcome != OutcomeReconcileDedicated {
		t.Fatalf("first decision = %+v, want %v", first.Decision, OutcomeReconcileDedicated)
	}
	if first.DesiredWrites != 1 {
		t.Errorf("DesiredWrites = %d, want one write to introduce the workload", first.DesiredWrites)
	}
	afterFirst := f.generation(t)
	if afterFirst != before+1 {
		t.Fatalf("desired generation after one write = %d, want %d", afterFirst, before+1)
	}

	second := f.reconcile(t, desired)
	if second.DesiredWrites != 0 {
		t.Errorf("DesiredWrites = %d, want no write for an intent already stored", second.DesiredWrites)
	}
	if after := f.generation(t); after != afterFirst {
		t.Fatalf("desired generation after a repeat = %d, want %d unchanged", after, afterFirst)
	}
	if len(f.controller.intents) != 2 {
		t.Fatalf("controller calls = %d, want one per reconciliation", len(f.controller.intents))
	}
	for i, intent := range f.controller.intents {
		if intent.Generation != afterFirst {
			t.Errorf("intent %d generation = %d, want %d", i, intent.Generation, afterFirst)
		}
		if intent.TenantID != testTenant || intent.SessionID != testSession || intent.AgentID != testAgent {
			t.Errorf("intent %d identity = %+v, want this session's", i, intent)
		}
		if intent.Placement != sessionwire.HostPlacementDedicated || intent.Workload.PayloadVersion != "v1" ||
			!bytes.Equal(intent.Workload.Payload, []byte(`{"cpu":"2"}`)) {
			t.Errorf("intent %d desired state = %+v, want the stored workload", i, intent)
		}
	}
	if result := f.reconcile(t, desired); result.Intent.Generation != afterFirst || result.Intent.Placement != sessionwire.HostPlacementDedicated {
		t.Errorf("Result.Intent = %+v, want the stored intent at generation %d", result.Intent, afterFirst)
	}
}

// TestAChangedWorkloadIsANewIntent is the other direction of the same rule: a
// generation that never moved would leave a controller reconciling the old
// workload forever.
func TestAChangedWorkloadIsANewIntent(t *testing.T) {
	t.Parallel()

	f := newFixture(t, sessionwire.HostPlacementDedicated, "factory-1")
	first := &Desired{
		Placement: sessionwire.HostPlacementDedicated, RuntimeCompatibilityID: testRuntime,
		Workload: sessionstore.DesiredWorkload{PayloadVersion: "v1", Payload: []byte(`{"cpu":"2"}`)},
	}
	f.reconcile(t, first)
	afterFirst := f.generation(t)

	changed := &Desired{
		Placement: sessionwire.HostPlacementDedicated, RuntimeCompatibilityID: testRuntime,
		Workload: sessionstore.DesiredWorkload{PayloadVersion: "v1", Payload: []byte(`{"cpu":"4"}`)},
	}
	result := f.reconcile(t, changed)
	if result.DesiredWrites != 1 {
		t.Errorf("DesiredWrites = %d, want one write for a changed workload", result.DesiredWrites)
	}
	if after := f.generation(t); after != afterFirst+1 {
		t.Fatalf("desired generation = %d, want %d", after, afterFirst+1)
	}
	last := f.controller.intents[len(f.controller.intents)-1]
	if !bytes.Equal(last.Workload.Payload, []byte(`{"cpu":"4"}`)) {
		t.Errorf("controller intent payload = %s, want the changed workload", last.Workload.Payload)
	}
}

// racingCatalog applies another replica's desired write immediately before
// forwarding this one, so the caller's compare-and-swap meets a record whose
// revision has already moved -- exactly as it would against a second Factory.
type racingCatalog struct {
	*sessionstore.Store
	race  func(sessionstore.UpdateCatalogDesiredStateRequest) sessionstore.UpdateCatalogDesiredStateRequest
	raced bool
}

func (c *racingCatalog) UpdateCatalogDesiredState(ctx context.Context, req sessionstore.UpdateCatalogDesiredStateRequest) (sessionstore.CatalogEntry, error) {
	if !c.raced {
		c.raced = true
		current, err := c.Store.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{TenantID: req.TenantID, SessionID: req.SessionID})
		if err != nil {
			return sessionstore.CatalogEntry{}, err
		}
		racer := c.race(req)
		racer.ExpectedRevision = current.Revision
		if _, err := c.Store.UpdateCatalogDesiredState(ctx, racer); err != nil {
			return sessionstore.CatalogEntry{}, err
		}
	}
	return c.Store.UpdateCatalogDesiredState(ctx, req)
}

func (f *fixture) racingReconciler(t *testing.T, catalog Catalog) *Reconciler {
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

func dedicatedDesired(payload string) *Desired {
	return &Desired{
		Placement: sessionwire.HostPlacementDedicated, RuntimeCompatibilityID: testRuntime,
		Workload: sessionstore.DesiredWorkload{PayloadVersion: "v1", Payload: []byte(payload)},
	}
}

// TestTwoReplicasWritingOneIntentApplyItOnce is what makes two Factories safe
// with no leader: both derive the SAME idempotency key from the same intent,
// SessionStore checks the key before the revision, and the loser's write is
// absorbed as a replay of the winner's rather than applied as a second
// generation a controller would have to reconcile again.
func TestTwoReplicasWritingOneIntentApplyItOnce(t *testing.T) {
	t.Parallel()

	f := newFixture(t, sessionwire.HostPlacementDedicated, "factory-2")
	catalog := &racingCatalog{Store: f.store, race: func(req sessionstore.UpdateCatalogDesiredStateRequest) sessionstore.UpdateCatalogDesiredStateRequest {
		return req
	}}
	before := f.generation(t)

	result, err := f.racingReconciler(t, catalog).Reconcile(context.Background(), Request{
		TenantID: testTenant, SessionID: testSession, Desired: dedicatedDesired(`{"cpu":"2"}`),
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !catalog.raced {
		t.Fatal("the racing write never ran, so this case proves nothing")
	}
	if result.Decision.Outcome != OutcomeReconcileDedicated {
		t.Fatalf("decision = %+v, want the dedicated reconciliation to continue", result.Decision)
	}
	if result.DesiredWrites != 1 {
		t.Errorf("DesiredWrites = %d, want the one write whose key was replayed", result.DesiredWrites)
	}
	if after := f.generation(t); after != before+1 {
		t.Fatalf("desired generation = %d, want %d: exactly one of two replicas applied", after, before+1)
	}
	if len(f.controller.intents) != 1 || f.controller.intents[0].Generation != before+1 {
		t.Fatalf("controller intents = %+v, want one at generation %d", f.controller.intents, before+1)
	}
}

// TestALostRevisionRaceIsRetriedOnceAgainstTheFreshRecord covers the other
// racer: one that wrote a DIFFERENT intent moves the revision without matching
// the key, so the compare-and-swap really is refused. One re-read and one retry
// resolve it, and the intent this replica was asked for is the one stored.
func TestALostRevisionRaceIsRetriedOnceAgainstTheFreshRecord(t *testing.T) {
	t.Parallel()

	f := newFixture(t, sessionwire.HostPlacementDedicated, "factory-2")
	catalog := &racingCatalog{Store: f.store, race: func(req sessionstore.UpdateCatalogDesiredStateRequest) sessionstore.UpdateCatalogDesiredStateRequest {
		other := req
		other.IdempotencyKey = "another-replicas-different-intent"
		other.DesiredWorkload = sessionstore.DesiredWorkload{PayloadVersion: "v1", Payload: []byte(`{"cpu":"8"}`)}
		return other
	}}
	before := f.generation(t)

	result, err := f.racingReconciler(t, catalog).Reconcile(context.Background(), Request{
		TenantID: testTenant, SessionID: testSession, Desired: dedicatedDesired(`{"cpu":"2"}`),
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.DesiredWrites != 2 {
		t.Errorf("DesiredWrites = %d, want the refused write and its one retry", result.DesiredWrites)
	}
	if after := f.generation(t); after != before+2 {
		t.Fatalf("desired generation = %d, want %d: the racer's intent and then this one", after, before+2)
	}
	if len(f.controller.intents) != 1 {
		t.Fatalf("controller intents = %+v, want one", f.controller.intents)
	}
	if !bytes.Equal(f.controller.intents[0].Workload.Payload, []byte(`{"cpu":"2"}`)) {
		t.Errorf("controller payload = %s, want this replica's intent", f.controller.intents[0].Workload.Payload)
	}
}

// TestADedicatedSessionWithoutAWorkloadControllerFailsClosed is H5's two-binary
// split made a runtime rule. cmd/factory carries no workload create/delete RBAC
// and composes no controller, so a dedicated session reaching the tenant-facing
// binary must be refused by name rather than silently reported as placed.
func TestADedicatedSessionWithoutAWorkloadControllerFailsClosed(t *testing.T) {
	t.Parallel()

	f := newFixture(t, sessionwire.HostPlacementDedicated, "factory-1")
	reconciler, err := NewReconciler(Config{
		Directory: mustDirectory(t, f.store), Catalog: f.store, Claims: f.store,
		Clock: f.clock, HolderID: "factory-1", ClaimTTL: time.Minute, CandidateLimit: 8,
	})
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}
	before := f.generation(t)

	_, err = reconciler.Reconcile(context.Background(), Request{TenantID: testTenant, SessionID: testSession})
	if !errors.Is(err, ErrNoWorkloadController) {
		t.Fatalf("error = %v, want ErrNoWorkloadController", err)
	}
	if after := f.generation(t); after != before {
		t.Errorf("desired generation = %d, want %d: a replica that cannot create wrote nothing", after, before)
	}
	if code := f.claimCode(t); code != sessionstore.ReconcileErrorLapsed {
		t.Errorf("claim state = %q, want the claim released on the refusal path", code)
	}
}

// TestTheClaimIsReleasedWhenTheControllerFails keeps a failing platform from
// holding a session's claim for its whole horizon.
func TestTheClaimIsReleasedWhenTheControllerFails(t *testing.T) {
	t.Parallel()

	f := newFixture(t, sessionwire.HostPlacementDedicated, "factory-1")
	controllerErr := errors.New("platform unavailable")
	f.controller.err = controllerErr

	_, err := f.reconciler.Reconcile(context.Background(), Request{TenantID: testTenant, SessionID: testSession})
	if !errors.Is(err, controllerErr) {
		t.Fatalf("error = %v, want the controller's own failure", err)
	}
	if code := f.claimCode(t); code != sessionstore.ReconcileErrorLapsed {
		t.Errorf("claim state = %q, want a released claim", code)
	}
}

// TestTheClaimHorizonComesFromTheConfiguredTTL pins the value a deferring
// replica backs off for.
func TestTheClaimHorizonComesFromTheConfiguredTTL(t *testing.T) {
	t.Parallel()

	f := newFixture(t, sessionwire.HostPlacementPooled, "factory-1")
	held := &heldClaims{Store: f.store}
	reconciler, err := NewReconciler(Config{
		Directory: mustDirectory(t, f.store), Catalog: f.store, Claims: held, Workloads: f.controller,
		Clock: f.clock, HolderID: "factory-1", ClaimTTL: 90 * time.Second, CandidateLimit: 8,
	})
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}
	if _, err := reconciler.Reconcile(context.Background(), Request{TenantID: testTenant, SessionID: testSession}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(held.acquired) != 1 {
		t.Fatalf("acquisitions = %d, want 1", len(held.acquired))
	}
	want := f.clock.now.Add(90 * time.Second)
	if !held.acquired[0].ExpiresAt.Equal(want) {
		t.Errorf("claim ExpiresAt = %v, want %v", held.acquired[0].ExpiresAt, want)
	}
	if held.acquired[0].HolderID != "factory-1" {
		t.Errorf("claim HolderID = %q, want the configured holder", held.acquired[0].HolderID)
	}
	if len(held.released) != 1 || held.released[0].HolderID != "factory-1" {
		t.Errorf("releases = %+v, want one by the same holder", held.released)
	}
}

type heldClaims struct {
	*sessionstore.Store
	acquired []sessionstore.AcquireReconciliationClaimRequest
	released []sessionstore.ReleaseReconciliationClaimRequest
}

func (c *heldClaims) AcquireReconciliationClaim(ctx context.Context, req sessionstore.AcquireReconciliationClaimRequest) (sessionstore.ReconciliationClaimEntry, error) {
	c.acquired = append(c.acquired, req)
	return c.Store.AcquireReconciliationClaim(ctx, req)
}

func (c *heldClaims) ReleaseReconciliationClaim(ctx context.Context, req sessionstore.ReleaseReconciliationClaimRequest) (sessionstore.ReconciliationClaimEntry, error) {
	c.released = append(c.released, req)
	return c.Store.ReleaseReconciliationClaim(ctx, req)
}

// TestNewReconcilerRefusesAnIncompleteConfigurationBeforeAnyStoreCall keeps a
// misconfigured replica from discovering the problem on a tenant's request.
func TestNewReconcilerRefusesAnIncompleteConfigurationBeforeAnyStoreCall(t *testing.T) {
	t.Parallel()

	f := newFixture(t, sessionwire.HostPlacementPooled, "factory-1")
	valid := Config{
		Directory: mustDirectory(t, f.store), Catalog: f.store, Claims: f.store, Workloads: f.controller,
		Clock: f.clock, HolderID: "factory-1", ClaimTTL: time.Minute, CandidateLimit: 8,
	}
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "no directory", mutate: func(c *Config) { c.Directory = nil }},
		{name: "no catalog", mutate: func(c *Config) { c.Catalog = nil }},
		{name: "no claims", mutate: func(c *Config) { c.Claims = nil }},
		{name: "no clock", mutate: func(c *Config) { c.Clock = nil }},
		{name: "no holder", mutate: func(c *Config) { c.HolderID = "" }},
		{name: "zero claim ttl", mutate: func(c *Config) { c.ClaimTTL = 0 }},
		{name: "claim ttl beyond the store's bound", mutate: func(c *Config) {
			c.ClaimTTL = sessionstore.MaxReconciliationClaimTTL + time.Second
		}},
		{name: "zero candidate limit", mutate: func(c *Config) { c.CandidateLimit = 0 }},
		{name: "candidate limit beyond the store's page bound", mutate: func(c *Config) {
			c.CandidateLimit = storage.MaxOrderedPageLimit + 1
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := valid
			test.mutate(&cfg)
			if _, err := NewReconciler(cfg); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("error = %v, want ErrInvalidConfig", err)
			}
		})
	}
	if _, err := NewReconciler(valid); err != nil {
		t.Fatalf("the valid configuration was refused: %v", err)
	}

	// A missing workload controller is NOT a configuration failure: H5's
	// tenant-facing binary composes exactly that, and it must still serve every
	// pooled session.
	noController := valid
	noController.Workloads = nil
	if _, err := NewReconciler(noController); err != nil {
		t.Fatalf("a replica with no workload controller was refused: %v", err)
	}
}

func mustDirectory(t *testing.T, store *sessionstore.Store) *routing.Directory {
	t.Helper()

	directory, err := routing.NewDirectory(store, routing.DefaultLimits())
	if err != nil {
		t.Fatalf("NewDirectory: %v", err)
	}
	return directory
}

// recordingDirectory captures the capacity query a placement made. It wraps
// the real Directory rather than replacing it, so what it observes is the
// request the store actually received.
type recordingDirectory struct {
	*routing.Directory
	requests []sessionstore.ListCompatibleHostsRequest
}

func (d *recordingDirectory) Candidates(ctx context.Context, req sessionstore.ListCompatibleHostsRequest) (sessionstore.HostTargetPage, error) {
	d.requests = append(d.requests, req)
	return d.Directory.Candidates(ctx, req)
}

// TestTheCapacityPageIsScopedToTheSessionsOwnTargetAndBound holds the two
// things a candidate query can get wrong and nothing downstream would report: a
// page scoped to another target still yields a plausible Host, and an unbounded
// one still yields the right one.
func TestTheCapacityPageIsScopedToTheSessionsOwnTargetAndBound(t *testing.T) {
	t.Parallel()

	f := newFixture(t, sessionwire.HostPlacementPooled, "factory-1")
	f.publishTarget(t, "host-shared", 4, sessionwire.HostIsolationClassCrossTenantIsolated)
	directory := &recordingDirectory{Directory: mustDirectory(t, f.store)}
	reconciler, err := NewReconciler(Config{
		Directory: directory, Catalog: f.store, Claims: f.store, Workloads: f.controller,
		Clock: f.clock, HolderID: "factory-1", ClaimTTL: time.Minute, CandidateLimit: 3,
	})
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}
	if _, err := reconciler.Reconcile(context.Background(), Request{TenantID: testTenant, SessionID: testSession}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(directory.requests) != 1 {
		t.Fatalf("candidate queries = %d, want 1", len(directory.requests))
	}
	want := sessionstore.ListCompatibleHostsRequest{
		Key: sessionstore.HostTargetKey{
			AgentID: testAgent, RuntimeCompatibilityID: testRuntime, Placement: sessionwire.HostPlacementPooled,
		},
		Limit: 3,
	}
	if directory.requests[0] != want {
		t.Errorf("candidate query = %+v, want %+v", directory.requests[0], want)
	}
}

// TestTwoIntentsNeverShareADesiredKey is the length prefixing, driven rather
// than asserted in a comment. Concatenation alone would give these two intents
// one key, and SessionStore treats a reused key as a replay -- so the second
// intent would succeed without applying anything and the controller would
// reconcile the first one forever.
func TestTwoIntentsNeverShareADesiredKey(t *testing.T) {
	t.Parallel()

	req := Request{TenantID: testTenant, SessionID: testSession}
	first := desiredKey(req, Desired{
		Placement: sessionwire.HostPlacementDedicated, RuntimeCompatibilityID: "runtime",
		Workload: sessionstore.DesiredWorkload{PayloadVersion: "v1", Payload: []byte("2x")},
	})
	second := desiredKey(req, Desired{
		Placement: sessionwire.HostPlacementDedicated, RuntimeCompatibilityID: "runtime",
		Workload: sessionstore.DesiredWorkload{PayloadVersion: "v12", Payload: []byte("x")},
	})
	if first == second {
		t.Fatalf("two different intents share the key %q", first)
	}

	// The same intent under a different session is a different key, and the
	// same intent twice is the same key: the first is what keeps one session's
	// replay from absorbing another's write, the second is what makes two
	// replicas collide instead of applying twice.
	other := desiredKey(Request{TenantID: testTenant, SessionID: "session-b"}, Desired{
		Placement: sessionwire.HostPlacementDedicated, RuntimeCompatibilityID: "runtime",
		Workload: sessionstore.DesiredWorkload{PayloadVersion: "v1", Payload: []byte("2x")},
	})
	if other == first {
		t.Error("two sessions share one intent key")
	}
	if repeated := desiredKey(req, Desired{
		Placement: sessionwire.HostPlacementDedicated, RuntimeCompatibilityID: "runtime",
		Workload: sessionstore.DesiredWorkload{PayloadVersion: "v1", Payload: []byte("2x")},
	}); repeated != first {
		t.Errorf("the same intent derived two keys, %q and %q", first, repeated)
	}
}
