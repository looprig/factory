package placement

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
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

func (c *recordingController) ObserveWorkload(context.Context, sessionstore.PlacementIntent) (sessionwire.HostLinkRegistryObservation, bool, error) {
	return sessionwire.HostLinkRegistryObservation{}, false, nil
}

func (c *recordingController) RequestDrain(context.Context, sessionstore.PlacementIntent) (sessionwire.HostLinkDrainObservation, error) {
	return sessionwire.HostLinkDrainObservation{}, nil
}

func (c *recordingController) DeleteWorkload(context.Context, sessionstore.PlacementIntent) error {
	return nil
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
			InternalEndpoint: sessionwire.InternalEndpoint("wss://" + string(host) + ".internal"),
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
			Placement: placement, InternalEndpoint: "wss://host-owner.internal",
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

// desiredAfterFirstRead changes the catalog only after the first snapshot. A
// loser must use the second snapshot together with the second registry read;
// returning the first record would report Deferred for an owner that is already
// compatible with the desired state the winner committed.
type desiredAfterFirstRead struct {
	*sessionstore.Store
	reads int
}

func (c *desiredAfterFirstRead) GetCatalogEntry(ctx context.Context, req sessionstore.GetCatalogEntryRequest) (sessionstore.CatalogEntry, error) {
	c.reads++
	entry, err := c.Store.GetCatalogEntry(ctx, req)
	if err != nil || c.reads != 1 {
		return entry, err
	}
	_, err = c.Store.UpdateCatalogDesiredState(ctx, sessionstore.UpdateCatalogDesiredStateRequest{
		TenantID: req.TenantID, SessionID: req.SessionID,
		ExpectedRevision: entry.Revision, IdempotencyKey: "winner-dedicated-intent",
		DesiredPlacement: sessionwire.HostPlacementDedicated, RuntimeCompatibilityID: testRuntime,
		DesiredWorkload: sessionstore.DesiredWorkload{PayloadVersion: "v1", Payload: []byte(`{"cpu":"2"}`)},
	})
	if err != nil {
		return sessionstore.CatalogEntry{}, err
	}
	return entry, nil
}

// TestAClaimLoserRereadsDesiredAndObservedState is the discriminating form of
// the claim race. The first snapshot says pooled while the winner's update and
// owner appear before the loser can reread. Only the fresh desired record lets
// the loser return the compatible owner instead of a generic Deferred body.
func TestAClaimLoserRereadsDesiredAndObservedState(t *testing.T) {
	t.Parallel()

	f := newFixture(t, sessionwire.HostPlacementPooled, "factory-2")
	if _, err := f.store.AcquireReconciliationClaim(context.Background(), sessionstore.AcquireReconciliationClaimRequest{
		TenantID: testTenant, SessionID: testSession, HolderID: "factory-1", ExpiresAt: f.clock.now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("AcquireReconciliationClaim: %v", err)
	}
	f.putOwner(t, sessionwire.HostPlacementDedicated)
	catalog := &desiredAfterFirstRead{Store: f.store}
	directory := &lateOwnerDirectory{Directory: mustDirectory(t, f.store)}
	reconciler, err := NewReconciler(Config{
		Directory: directory, Catalog: catalog, Claims: f.store, Workloads: f.controller,
		Clock: f.clock, HolderID: "factory-2", ClaimTTL: time.Minute, CandidateLimit: 8,
	})
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}

	result, err := reconciler.Reconcile(context.Background(), Request{TenantID: testTenant, SessionID: testSession})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if catalog.reads != 2 {
		t.Fatalf("catalog reads = %d, want initial and loser reread", catalog.reads)
	}
	if directory.reads != 2 {
		t.Fatalf("registry reads = %d, want initial and loser reread", directory.reads)
	}
	if result.Deferred {
		t.Fatalf("result = %+v, want the fresh owner returned", result)
	}
	if result.Decision.Outcome != OutcomeReuseOwner || result.Decision.Owner.HostID != "host-owner" {
		t.Fatalf("decision = %+v, want the dedicated owner from fresh state", result.Decision)
	}
	if len(f.controller.intents) != 0 {
		t.Fatalf("controller intents = %+v, want no Ensure after losing the claim", f.controller.intents)
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

// TestARealDispositionCreateDrivesDedicatedEnsureAfterRestart keeps the
// reconciler's most important composition edge on the real path. Public
// creation writes a disposition-bound catalog record and its initial desired
// workload atomically; Reconcile must then read that durable record and hand
// the exact intent to the consumer-owned controller. Re-opening the same
// backend proves the placement identity is not process-local and that a
// restarted Factory can safely drive the same desired generation again.
func TestARealDispositionCreateDrivesDedicatedEnsureAfterRestart(t *testing.T) {
	backend := memstore.New()
	clock := &movableClock{now: reconcileNow}
	store, err := sessionstore.Open(context.Background(), backend, sessionstore.WithClock(clock))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	closeStore := func() {
		if err := store.Close(context.Background()); err != nil {
			t.Errorf("Close: %v", err)
		}
	}
	t.Cleanup(closeStore)

	payload := []byte(`{"blocks":[{"kind":"text","text":"dedicated"}]}`)
	digest := sha256.Sum256(payload)
	identity := sessionstore.PublicCreateIdentity{
		TenantID:  testTenant,
		SessionID: testSession,
		CommandID: "create-dedicated",
		Target: sessionstore.HostTargetKey{
			AgentID: testAgent, RuntimeCompatibilityID: testRuntime,
			Placement: sessionwire.HostPlacementDedicated,
		},
		Binding: sessionstore.SessionBinding{
			StorageBindingID: "storage-a", BindingVersion: "v1",
			RuntimeSessionID: "runtime-session-dedicated",
			ProtocolMode:     sessionstore.ProtocolModeDisposition,
		},
		Kind:          sessionstore.CommandKind("create"),
		PayloadDigest: hex.EncodeToString(digest[:]),
		PayloadSize:   uint64(len(payload)),
	}
	workload := sessionstore.DesiredWorkload{
		PayloadVersion: "workload/v1",
		Payload:        []byte(`{"resources":{"cpu":"2"}}`),
	}
	prepared, err := store.PreparePublicCreate(context.Background(), sessionstore.PreparePublicCreateRequest{
		Identity:                 identity,
		ProposedRuntimeCommandID: sessionstore.RuntimeCommandID("runtime-command-dedicated"),
		AcceptedAt:               clock.now,
		ApplyDeadline:            clock.now.Add(time.Minute),
		InitialWorkload:          workload,
	})
	if err != nil {
		t.Fatalf("PreparePublicCreate: %v", err)
	}
	if prepared.Catalog.Record.Binding.ProtocolMode != sessionstore.ProtocolModeDisposition {
		t.Fatalf("prepared catalog protocol = %q, want disposition", prepared.Catalog.Record.Binding.ProtocolMode)
	}
	if _, created, err := store.AdmitPublicCreate(context.Background(), sessionstore.AdmitPublicCreateRequest{
		Identity: identity, Payload: payload,
	}); err != nil || !created {
		t.Fatalf("AdmitPublicCreate = created %t, err %v", created, err)
	}

	entry, err := store.GetCatalogEntry(context.Background(), sessionstore.GetCatalogEntryRequest{
		TenantID: testTenant, SessionID: testSession,
	})
	if err != nil {
		t.Fatalf("GetCatalogEntry after create: %v", err)
	}
	if entry.Record.Binding.ProtocolMode != sessionstore.ProtocolModeDisposition {
		t.Fatalf("stored catalog protocol = %q, want disposition", entry.Record.Binding.ProtocolMode)
	}
	if entry.Record.DesiredPlacement != sessionwire.HostPlacementDedicated || entry.Record.DesiredGeneration != 1 {
		t.Fatalf("stored desired placement/generation = %q/%d, want dedicated/1", entry.Record.DesiredPlacement, entry.Record.DesiredGeneration)
	}
	if entry.Record.DesiredWorkload.PayloadVersion != workload.PayloadVersion || !bytes.Equal(entry.Record.DesiredWorkload.Payload, workload.Payload) {
		t.Fatalf("stored workload = %+v, want %+v", entry.Record.DesiredWorkload, workload)
	}

	controller := &recordingController{}
	reconciler, err := NewReconciler(Config{
		Directory: mustDirectory(t, store), Catalog: store, Claims: store, Workloads: controller,
		Clock: clock, HolderID: "factory-1", ClaimTTL: time.Minute, CandidateLimit: 8,
	})
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}
	first, err := reconciler.Reconcile(context.Background(), Request{TenantID: testTenant, SessionID: testSession})
	if err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	if first.Decision.Outcome != OutcomeReconcileDedicated {
		t.Fatalf("first decision = %+v, want %v", first.Decision, OutcomeReconcileDedicated)
	}
	if first.DesiredWrites != 0 {
		t.Fatalf("first DesiredWrites = %d, want zero for create-persisted intent", first.DesiredWrites)
	}
	if len(controller.intents) != 1 || controller.intents[0].Generation != 1 {
		t.Fatalf("first controller intents = %+v, want one generation-1 intent", controller.intents)
	}
	if controller.intents[0].Placement != sessionwire.HostPlacementDedicated || !bytes.Equal(controller.intents[0].Workload.Payload, workload.Payload) {
		t.Fatalf("first controller intent = %+v, want the durable dedicated workload", controller.intents[0])
	}

	if err := store.Close(context.Background()); err != nil {
		t.Fatalf("close before restart: %v", err)
	}
	store, err = sessionstore.Open(context.Background(), backend, sessionstore.WithClock(clock))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	controllerAfterRestart := &recordingController{}
	reconcilerAfterRestart, err := NewReconciler(Config{
		Directory: mustDirectory(t, store), Catalog: store, Claims: store, Workloads: controllerAfterRestart,
		Clock: clock, HolderID: "factory-2", ClaimTTL: time.Minute, CandidateLimit: 8,
	})
	if err != nil {
		t.Fatalf("NewReconciler after restart: %v", err)
	}
	second, err := reconcilerAfterRestart.Reconcile(context.Background(), Request{TenantID: testTenant, SessionID: testSession})
	if err != nil {
		t.Fatalf("restarted Reconcile: %v", err)
	}
	if second.Decision.Outcome != OutcomeReconcileDedicated {
		t.Fatalf("restarted decision = %+v, want %v", second.Decision, OutcomeReconcileDedicated)
	}
	if second.DesiredWrites != 0 || len(controllerAfterRestart.intents) != 1 {
		t.Fatalf("restarted writes/intents = %d/%+v, want 0/one", second.DesiredWrites, controllerAfterRestart.intents)
	}
	if controllerAfterRestart.intents[0].Generation != first.Intent.Generation ||
		controllerAfterRestart.intents[0].Placement != first.Intent.Placement ||
		!bytes.Equal(controllerAfterRestart.intents[0].Workload.Payload, first.Intent.Workload.Payload) {
		t.Fatalf("restarted intent = %+v, want the same durable identity as %+v", controllerAfterRestart.intents[0], first.Intent)
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

// TestThePinnedWireCarriesAnAttachment is the POSITIVE form of the gap-marker
// that stood here at core v0.7.0, and it fails in the opposite direction.
//
// A4.2 step 2 says the selected candidate is asked to acquire or attach. At
// v0.7.0 no request could carry that: bind refused a zero lease epoch, unbind
// refused one too, drain asked a Host to give up a session it held, and
// TestThePinnedWireCannotCarryAnAttachment held the premise by failing on any
// growth of Core's HostLink vocabulary. Core v0.8.0 grew it by exactly the
// record that closes the gap, so that test failed BY DESIGN on the bump and
// this one replaces it. What it holds now:
//
//   - HostLinkAttachRequest exists and carries NO lease epoch, which is what
//     a session with no owner -- the only kind that reaches placement -- can
//     send. It DOES carry the host fence (host_id, host_generation) and
//     refuses a request without one, so the caller cannot attach to whatever
//     Host happens to serve an endpoint.
//   - bind still refuses a zero epoch. The attach is what SUPPLIES the epoch a
//     bind then names; a bind that accepted zero would be a second way to
//     establish residency, and Core's own decoder fails closed against that.
//   - the stale-registry answer is still epoch_mismatch carrying the OTHER
//     holder's epoch, and Core still names no lease_held. A caller must never
//     bind with that epoch.
//   - the reserved method for the record is Core's, and the record's mode set
//     is closed to create and restore.
//   - the pinned vocabulary is exactly the twelve names below, read from the
//     pinned package's source, so the next growth asks a human again.
//
// This asserts a DEPENDENCY's behaviour deliberately: it is the premise the
// caller of Reconcile is written against, and a premise nobody rechecks is how
// a stale citation survives a version bump.
func TestThePinnedWireCarriesAnAttachment(t *testing.T) {
	t.Parallel()

	attach := sessionwire.HostLinkAttachRequest{
		Version: sessionwire.CurrentWireVersion, TenantID: testTenant, SessionID: testSession,
		HostID: "host-shared", HostGeneration: 1,
		AgentID: testAgent, RuntimeCompatibilityID: testRuntime,
		Mode: sessionwire.HostLinkAttachModeCreate, ActorID: "factory-service", IdempotencyKey: "attach-1",
	}
	if err := attach.Validate(); err != nil {
		t.Fatalf("an attach naming no lease epoch was refused (%v); the wire cannot carry an attachment for an unowned session", err)
	}
	unfenced := attach
	unfenced.HostGeneration = 0
	if err := unfenced.Validate(); err == nil {
		t.Error("an attach with no host generation was accepted; the host fence is not required")
	}
	unfenced = attach
	unfenced.HostID = ""
	if err := unfenced.Validate(); err == nil {
		t.Error("an attach with no host id was accepted; the host fence is not required")
	}
	for _, mode := range []sessionwire.HostLinkAttachMode{"", "attach", "Create", "resume"} {
		bad := attach
		bad.Mode = mode
		if err := bad.Validate(); err == nil {
			t.Errorf("mode %q was accepted; the mode set is no longer closed to create and restore", mode)
		}
	}
	restore := attach
	restore.Mode = sessionwire.HostLinkAttachModeRestore
	if err := restore.Validate(); err != nil {
		t.Errorf("a restore-mode attach was refused: %v", err)
	}
	if sessionwire.HostLinkMethodAttach != "hostlink.attach" {
		t.Errorf("HostLinkMethodAttach = %q, want %q", sessionwire.HostLinkMethodAttach, "hostlink.attach")
	}

	bind := sessionwire.HostLinkBindRequest{
		Version: sessionwire.CurrentWireVersion, TenantID: testTenant, SessionID: testSession,
		HostID: "host-shared", HostGeneration: 1, LeaseEpoch: 0,
		RuntimeCompatibilityID: testRuntime, IdempotencyKey: "bind-1",
	}
	if err := bind.Validate(); err == nil {
		t.Fatal("a bind with no lease epoch was accepted; bind has become a second way to establish residency")
	}
	bind.LeaseEpoch = 1
	if err := bind.Validate(); err != nil {
		t.Fatalf("a bind naming a lease epoch was refused (%v), so the case above proves nothing about the epoch", err)
	}

	held := sessionwire.HostLinkError{Code: "lease_held"}
	if err := held.Validate(); err == nil {
		t.Error("core now names a lease_held refusal; the stale-registry path should read it")
	}
	mismatch := sessionwire.HostLinkError{Code: sessionwire.HostLinkErrorEpochMismatch, CurrentLeaseEpoch: 9}
	if err := mismatch.Validate(); err != nil {
		t.Errorf("epoch_mismatch with a current epoch was refused: %v", err)
	}
	if err := (sessionwire.HostLinkError{Code: sessionwire.HostLinkErrorEpochMismatch}).Validate(); err == nil {
		t.Error("epoch_mismatch without the holder's epoch was accepted; the stale-registry signal has lost its content")
	}

	got := exportedHostLinkTypes(t, pinnedSessionwireDir(t))
	if !slices.Equal(got, pinnedHostLinkTypes) {
		t.Errorf("the pinned HostLink record vocabulary is %q, not %q.\n"+
			"Core's HostLink surface changed under the pin. Re-read what the attach caller is written against "+
			"before updating the list.",
			got, pinnedHostLinkTypes)
	}
}

// pinnedHostLinkTypes is every exported HostLink* type declared by the pinned
// core sessionwire/v1 (v0.9.1), as ABSOLUTE literals. A set built from the package
// itself would pin nothing; this list is what a reader compared against.
//
// The whole HostLink prefix is pinned rather than only the *Request suffix,
// and the width is the point. A gap-marker that watched `HostLink*Request`
// would still miss a `HostLinkAttach` or a `HostLinkAdoption`, and naming is
// exactly what a future Core is free to choose. The cost is stated so nobody
// later files it as noise: ANY growth of the HostLink vocabulary fails this
// test, including growth that has nothing to do with attachment. That is the
// intended reading -- the failure asks a human to recheck a premise, and the
// answer may well be "still true, update the list".
//
// VersionNegotiation{Request,Response} are deliberately outside it: they
// negotiate a connection, name no session, and cannot carry placement.
var pinnedHostLinkTypes = []string{
	"HostLinkAttachMode",
	"HostLinkAttachRequest",
	"HostLinkBindRequest",
	"HostLinkCapacityReport",
	"HostLinkCommandDelivery",
	"HostLinkDrainObservation",
	"HostLinkDrainRequest",
	"HostLinkDrainState",
	// core v0.10.0: the per-tenant address derivation's typed refusal. It is
	// not a record, but it is what a pooled attach's dial now depends on: the
	// pool derives every address with HostLinkEndpoint(base, tenant).
	"HostLinkEndpointCode",
	"HostLinkEndpointError",
	"HostLinkError",
	"HostLinkErrorCode",
	"HostLinkRegistryObservation",
	"HostLinkUnbindRequest",
}

// pinnedCoreVersion is the core version this module's go.mod names, as an
// absolute literal. It exists so the scan below cannot silently read a
// DIFFERENT copy of core: under the workspace go.work, or after a pin moves,
// "the sessionwire on disk" and "the sessionwire this build resolves" are not
// the same directory, and a premise checked against the wrong one is the stale
// citation this test exists to prevent.
const pinnedCoreVersion = "v0.11.0"

const pinnedCoreModule = "github.com/looprig/core"

// pinnedSessionwireDir returns the module cache directory of the sessionwire
// package at exactly the version go.mod requires, failing if go.mod has moved.
//
// The cache path is the module path verbatim: escaping only applies to upper
// case letters and this path has none.
func pinnedSessionwireDir(t *testing.T) string {
	t.Helper()

	// internal/placement -> module root. A test's working directory is its own
	// package directory, which go test guarantees.
	gomod, err := os.ReadFile(filepath.Join("..", "..", "go.mod"))
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	required := requiredVersion(string(gomod), pinnedCoreModule)
	if required != pinnedCoreVersion {
		t.Fatalf("go.mod requires %s %q, but this test was written against %q; recheck every citation in this "+
			"package against the version now pinned before updating the constant", pinnedCoreModule, required, pinnedCoreVersion)
	}

	out, err := exec.Command("go", "env", "GOMODCACHE").Output()
	if err != nil {
		t.Fatalf("go env GOMODCACHE: %v", err)
	}
	cache := strings.TrimSpace(string(out))
	if cache == "" {
		t.Fatal("go env GOMODCACHE is empty, so the pinned package cannot be located")
	}
	dir := filepath.Join(cache, pinnedCoreModule+"@"+required, "sessionwire", "v1")
	if entries, err := os.ReadDir(dir); err != nil || len(entries) == 0 {
		t.Fatalf("pinned sessionwire at %s is unreadable or empty (%v); a scan over nothing proves nothing", dir, err)
	}
	return dir
}

// requiredVersion reports the version a go.mod requires for one module path,
// or "" when it requires none. Both the block and the single-line forms are
// read, and the module path is matched as a whole field so that a require of
// github.com/looprig/coreutil could not answer for github.com/looprig/core.
func requiredVersion(content, module string) string {
	for _, line := range strings.Split(content, "\n") {
		if idx := strings.Index(line, "//"); idx >= 0 {
			line = line[:idx]
		}
		fields := strings.Fields(line)
		if len(fields) > 0 && fields[0] == "require" {
			fields = fields[1:]
		}
		if len(fields) == 2 && fields[0] == module {
			return fields[1]
		}
	}
	return ""
}

// exportedHostLinkTypes reports every exported HostLink* type declared by the
// production files of a sessionwire package, sorted.
//
// It parses rather than greps for the reason command_vocabulary_test.go and
// import_boundary_test.go give: source text cannot tell a declaration from a
// comment or a string mentioning one, and this scan's whole value is that it
// notices a declaration nobody told it about. Test files are excluded because
// a dependency's own test fixtures are not its wire surface. A scan that
// parsed no file fails rather than reporting an empty vocabulary, which would
// otherwise pass as "nothing new".
func exportedHostLinkTypes(t *testing.T, dir string) []string {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	var names []string
	scanned := 0
	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		scanned++
		for _, decl := range parsed.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.TYPE {
				continue
			}
			for _, spec := range gen.Specs {
				typ, ok := spec.(*ast.TypeSpec)
				if !ok || !typ.Name.IsExported() || !strings.HasPrefix(typ.Name.Name, "HostLink") {
					continue
				}
				names = append(names, typ.Name.Name)
			}
		}
	}
	if scanned == 0 {
		t.Fatalf("no production Go file was parsed under %s; an empty vocabulary would pass as unchanged", dir)
	}
	slices.Sort(names)
	return names
}

// TestTheHostLinkVocabularyScanSeesANewType is the positive control for the
// scan itself. The assertion above can only report growth it is capable of
// seeing, and against the real pinned core it reports the same list every run
// whether it works or is stuck -- so the detector is driven over a package it
// is told the answer to, including a type declared inside a grouped
// declaration, one that only LOOKS like a record, and one in a test file.
func TestTheHostLinkVocabularyScanSeesANewType(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	write := func(name, body string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	write("hostlink.go", "package v1\n\ntype HostLinkBindRequest struct{}\n\n"+
		"type (\n\tHostLinkAttachRequest struct{}\n\thostLinkPrivate struct{}\n)\n\n"+
		"type HostPlacement string\n\n// type HostLinkCommentOnly struct{}\n\n"+
		"const notAType = \"type HostLinkStringOnly struct{}\"\n")
	write("hostlink_test.go", "package v1\n\ntype HostLinkFixture struct{}\n")

	want := []string{"HostLinkAttachRequest", "HostLinkBindRequest"}
	if got := exportedHostLinkTypes(t, dir); !slices.Equal(got, want) {
		t.Fatalf("the scan reported %q, want %q", got, want)
	}
}
