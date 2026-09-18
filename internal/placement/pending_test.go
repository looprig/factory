package placement

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage/memstore"
)

// The pending-work sweep is B5's trigger. The real-store cases hold the claim
// the trigger rests on: a session whose create is committed and not yet
// resident is found by a replica that did not admit it, and is placed.

type allowSweeps struct{ asked int }

func (a *allowSweeps) AuthorizeServiceSweep(context.Context, identity.Principal) error {
	a.asked++
	return nil
}

type denySweeps struct{}

var errDenied = errors.New("denied")

func (denySweeps) AuthorizeServiceSweep(context.Context, identity.Principal) error { return errDenied }

func servicePrincipal(t *testing.T) identity.Principal {
	t.Helper()
	principal, err := identity.NewPrincipal("tenant-ops", "factory-service", identity.KindService)
	if err != nil {
		t.Fatalf("NewPrincipal: %v", err)
	}
	return principal
}

// admitPooledCreate commits one V1 public create for a pooled session, with an
// apply deadline of deadline past the clock.
func admitPooledCreate(t *testing.T, store *sessionstore.Store, clock *movableClock, session sessionwire.SessionID, command sessionwire.CommandID, deadline time.Duration) {
	t.Helper()

	payload := []byte(`{"blocks":[{"kind":"text","text":"pooled"}]}`)
	digest := sha256.Sum256(payload)
	identity := sessionstore.PublicCreateIdentity{
		TenantID: testTenant, SessionID: session, CommandID: command,
		Target: sessionstore.HostTargetKey{AgentID: testAgent, RuntimeCompatibilityID: testRuntime, Placement: sessionwire.HostPlacementPooled},
		Binding: sessionstore.SessionBinding{
			StorageBindingID: "storage-a", BindingVersion: "v1",
			RuntimeSessionID: "runtime-" + string(session), ProtocolMode: sessionstore.ProtocolModeDisposition,
		},
		Kind:          sessionstore.CommandKind("create"),
		PayloadDigest: hex.EncodeToString(digest[:]),
		PayloadSize:   uint64(len(payload)),
	}
	if _, err := store.PreparePublicCreate(context.Background(), sessionstore.PreparePublicCreateRequest{
		Identity: identity, ProposedRuntimeCommandID: sessionstore.RuntimeCommandID("runtime-" + string(command)),
		AcceptedAt: clock.now, ApplyDeadline: clock.now.Add(deadline),
	}); err != nil {
		t.Fatalf("PreparePublicCreate(%s): %v", session, err)
	}
	if _, created, err := store.AdmitPublicCreate(context.Background(), sessionstore.AdmitPublicCreateRequest{Identity: identity, Payload: payload}); err != nil || !created {
		t.Fatalf("AdmitPublicCreate(%s) = %t, %v", session, created, err)
	}
}

type pendingFixture struct {
	store   *sessionstore.Store
	clock   *movableClock
	links   *scriptedLinks
	sweeper *PendingSweeper
}

func newPendingFixture(t *testing.T) *pendingFixture {
	t.Helper()

	clock := &movableClock{now: reconcileNow}
	store, err := sessionstore.Open(context.Background(), memstore.New(), sessionstore.WithClock(clock))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(context.Background()) })
	links := newScriptedLinks()
	links.anySession = true
	reconciler, err := NewReconciler(Config{
		Directory: mustDirectory(t, store), Catalog: store, Claims: store,
		Clock: clock, HolderID: "factory-2", ClaimTTL: time.Minute, CandidateLimit: 8,
		Links: links, ActorID: testActor,
	})
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}
	sweeper, err := NewPendingSweeper(PendingSweeperConfig{
		Authorizer: &allowSweeps{}, Pending: store, Placer: reconciler, Clock: clock,
		Horizon: 5 * time.Minute, PageLimit: 16, MaxPages: 4,
	})
	if err != nil {
		t.Fatalf("NewPendingSweeper: %v", err)
	}
	return &pendingFixture{store: store, clock: clock, links: links, sweeper: sweeper}
}

// sweepEveryShard runs one pass per shard and sums what they did.
func (f *pendingFixture) sweepEveryShard(t *testing.T) PendingSweepResult {
	t.Helper()
	total := PendingSweepResult{Outcomes: map[Outcome]int{}}
	for range f.store.ControlShards() {
		result, err := f.sweeper.Sweep(context.Background(), servicePrincipal(t))
		if err != nil {
			t.Fatalf("Sweep: %v", err)
		}
		total.Sessions += result.Sessions
		total.Attached += result.Attached
		total.Failures += result.Failures
		for outcome, n := range result.Outcomes {
			total.Outcomes[outcome] += n
		}
	}
	return total
}

func publishPooled(t *testing.T, store *sessionstore.Store, clock *movableClock, host sessionwire.HostID) {
	t.Helper()
	if _, err := store.PublishHostTarget(context.Background(), sessionstore.PublishHostTargetRequest{
		Key:    sessionstore.HostTargetKey{AgentID: testAgent, RuntimeCompatibilityID: testRuntime, Placement: sessionwire.HostPlacementPooled},
		HostID: host, HostGeneration: 3, ObservedAt: clock.now,
		Advertisement: sessionstore.HostAdvertisement{
			InternalEndpoint: sessionwire.InternalEndpoint("wss://" + string(host) + ".internal/hostlink"),
			IsolationClass:   sessionwire.HostIsolationClassCrossTenantIsolated, Accepting: true, AvailableCapacity: 8,
			ExpiresAt: clock.now.Add(time.Minute),
		},
	}); err != nil {
		t.Fatalf("PublishHostTarget: %v", err)
	}
}

// TestASweepPlacesEveryCommittedCreateItDidNotAdmit is I1.2-3's shape: the
// creates were committed by nobody this replica knows, and the sweep finds and
// places each, waking it with its own create command.
func TestASweepPlacesEveryCommittedCreateItDidNotAdmit(t *testing.T) {
	t.Parallel()

	f := newPendingFixture(t)
	publishPooled(t, f.store, f.clock, "host-a")
	admitPooledCreate(t, f.store, f.clock, "session-1", "create-1", 5*time.Minute)
	admitPooledCreate(t, f.store, f.clock, "session-2", "create-2", 5*time.Minute)

	total := f.sweepEveryShard(t)
	if total.Sessions != 2 || total.Attached != 2 || total.Failures != 0 {
		t.Fatalf("sweep = %+v, want both sessions attached", total)
	}
	got := map[sessionwire.SessionID]sessionwire.HostLinkAttachRequest{}
	for _, req := range f.links.attaches {
		got[req.SessionID] = req
	}
	for _, session := range []sessionwire.SessionID{"session-1", "session-2"} {
		req, ok := got[session]
		if !ok {
			t.Fatalf("%s was never attached (attaches: %+v)", session, f.links.attaches)
		}
		if req.HostID != "host-a" || req.HostGeneration != 3 || req.Mode != sessionwire.HostLinkAttachModeCreate || req.ActorID != testActor {
			t.Errorf("%s attach = %+v, want host-a/3 create by %s", session, req, testActor)
		}
	}
	slices.Sort(f.links.delivered)
	if !slices.Equal(f.links.delivered, []sessionwire.CommandID{"create-1", "create-2"}) {
		t.Errorf("delivered = %v, want each session's create command", f.links.delivered)
	}
}

// TestACommandPastItsDeadlineCausesNoPlacement: a pending command the deadline
// sweep is about to reject is not a reason to launch a runtime.
func TestACommandPastItsDeadlineCausesNoPlacement(t *testing.T) {
	t.Parallel()

	f := newPendingFixture(t)
	publishPooled(t, f.store, f.clock, "host-a")
	admitPooledCreate(t, f.store, f.clock, "session-1", "create-1", time.Minute)
	f.clock.now = f.clock.now.Add(2 * time.Minute)

	total := f.sweepEveryShard(t)
	if total.Sessions != 0 || len(f.links.attaches) != 0 {
		t.Fatalf("sweep = %+v attaches=%v, want nothing placed for an expired command", total, f.links.attaches)
	}
}

// fakePending scripts due pages for one shard.
type fakePending struct {
	mu       sync.Mutex
	shards   int
	pages    []sessionstore.DispositionDueCommandPage
	requests []sessionstore.ListDueDispositionCommandsRequest
	err      error
}

func (p *fakePending) ControlShards() int { return p.shards }

func (p *fakePending) ListDueDispositionCommands(_ context.Context, req sessionstore.ListDueDispositionCommandsRequest) (sessionstore.DispositionDueCommandPage, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.requests = append(p.requests, req)
	if p.err != nil {
		return sessionstore.DispositionDueCommandPage{}, p.err
	}
	if len(p.pages) == 0 {
		return sessionstore.DispositionDueCommandPage{}, nil
	}
	page := p.pages[0]
	p.pages = p.pages[1:]
	return page, nil
}

type recordingPlacer struct {
	mu       sync.Mutex
	requests []Request
	fail     map[sessionwire.SessionID]error
}

func (p *recordingPlacer) Reconcile(_ context.Context, req Request) (Result, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.requests = append(p.requests, req)
	if err := p.fail[req.SessionID]; err != nil {
		return Result{}, err
	}
	return Result{Decision: Decision{Outcome: OutcomeAttachPooled}}, nil
}

func open(session sessionwire.SessionID, command sessionwire.CommandID, state sessionstore.InboxState, deadline time.Time) sessionstore.DispositionInboxEntry {
	return sessionstore.DispositionInboxEntry{Record: sessionstore.DispositionInboxRecord{
		Descriptor:    sessionstore.DispositionCommandDescriptor{TenantID: testTenant, SessionID: session, CommandID: command},
		ApplyDeadline: deadline, State: state,
	}}
}

func newFakeSweeper(t *testing.T, pending PendingCommands, placer Placer, clock Clock, authorizer SweepAuthorizer) *PendingSweeper {
	t.Helper()
	sweeper, err := NewPendingSweeper(PendingSweeperConfig{
		Authorizer: authorizer, Pending: pending, Placer: placer, Clock: clock,
		Horizon: 5 * time.Minute, PageLimit: 7, MaxPages: 2,
	})
	if err != nil {
		t.Fatalf("NewPendingSweeper: %v", err)
	}
	return sweeper
}

// TestTheSweepGroupsBySessionAndWakesOnlyLivePendingCommands is the state
// table: which open commands make a session need a Host, and which are woken.
func TestTheSweepGroupsBySessionAndWakesOnlyLivePendingCommands(t *testing.T) {
	t.Parallel()

	clock := &movableClock{now: reconcileNow}
	live, dead := clock.now.Add(time.Minute), clock.now.Add(-time.Minute)
	pending := &fakePending{shards: 1, pages: []sessionstore.DispositionDueCommandPage{{
		Commands: []sessionstore.DispositionInboxEntry{
			open("s-pending", "c-1", sessionstore.InboxStatePending, live),
			open("s-pending", "c-2", sessionstore.InboxStatePending, live),
			open("s-pending", "c-expired", sessionstore.InboxStatePending, dead),
			open("s-claimed", "c-3", sessionstore.InboxStateClaimed, live),
			open("s-applying", "c-4", sessionstore.InboxStateApplying, dead),
			open("s-expired", "c-5", sessionstore.InboxStatePending, dead),
			open("s-lapsed-claim", "c-6", sessionstore.InboxStateClaimed, dead),
		},
	}}}
	placer := &recordingPlacer{}
	sweeper := newFakeSweeper(t, pending, placer, clock, &allowSweeps{})

	result, err := sweeper.Sweep(context.Background(), servicePrincipal(t))
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	want := []Request{
		{TenantID: testTenant, SessionID: "s-pending", Wake: []sessionwire.CommandID{"c-1", "c-2"}},
		{TenantID: testTenant, SessionID: "s-claimed"},
		{TenantID: testTenant, SessionID: "s-applying"},
	}
	if len(placer.requests) != len(want) {
		t.Fatalf("reconciled %+v, want %+v", placer.requests, want)
	}
	for i := range want {
		if placer.requests[i].SessionID != want[i].SessionID || !slices.Equal(placer.requests[i].Wake, want[i].Wake) {
			t.Errorf("request %d = %+v, want %+v", i, placer.requests[i], want[i])
		}
	}
	if result.Sessions != 3 {
		t.Errorf("Sessions = %d, want 3", result.Sessions)
	}
}

// TestTheDueReadReachesTheHorizonAndContinuesByCursorAlone pins the request
// shape: the first page is bounded at now+Horizon, a continuation carries only
// the cursor, and MaxPages ends the pass as Truncated.
func TestTheDueReadReachesTheHorizonAndContinuesByCursorAlone(t *testing.T) {
	t.Parallel()

	clock := &movableClock{now: reconcileNow}
	pending := &fakePending{shards: 1, pages: []sessionstore.DispositionDueCommandPage{
		{NextCursor: "cursor-1"}, {NextCursor: "cursor-2"}, {},
	}}
	sweeper := newFakeSweeper(t, pending, &recordingPlacer{}, clock, &allowSweeps{})

	result, err := sweeper.Sweep(context.Background(), servicePrincipal(t))
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	want := []sessionstore.ListDueDispositionCommandsRequest{
		{Shard: 0, DueAtOrBefore: reconcileNow.Add(5 * time.Minute), Limit: 7},
		{Shard: 0, Limit: 7, Cursor: "cursor-1"},
	}
	if !slices.Equal(pending.requests, want) {
		t.Errorf("requests = %+v, want %+v", pending.requests, want)
	}
	if !result.Truncated || result.Pages != 2 {
		t.Errorf("Truncated=%t Pages=%d, want a truncated two-page pass", result.Truncated, result.Pages)
	}
}

// TestOneSessionsFailureDoesNotStopThePass.
func TestOneSessionsFailureDoesNotStopThePass(t *testing.T) {
	t.Parallel()

	clock := &movableClock{now: reconcileNow}
	live := clock.now.Add(time.Minute)
	pending := &fakePending{shards: 1, pages: []sessionstore.DispositionDueCommandPage{{Commands: []sessionstore.DispositionInboxEntry{
		open("s-1", "c-1", sessionstore.InboxStatePending, live),
		open("s-dedicated", "c-2", sessionstore.InboxStatePending, live),
		open("s-2", "c-3", sessionstore.InboxStatePending, live),
	}}}}
	placer := &recordingPlacer{fail: map[sessionwire.SessionID]error{
		"s-1":         ErrRegistryStale,
		"s-dedicated": fmt.Errorf("reconcile: %w", ErrNoWorkloadController),
	}}
	sweeper := newFakeSweeper(t, pending, placer, clock, &allowSweeps{})

	result, err := sweeper.Sweep(context.Background(), servicePrincipal(t))
	if err != nil {
		t.Fatalf("Sweep = %v, want per-session failures kept in the result", err)
	}
	if len(placer.requests) != 3 || result.Failures != 1 || result.NoController != 1 {
		t.Fatalf("requests=%d result=%+v, want all three asked, one failure, one no-controller", len(placer.requests), result)
	}
}

// TestTheSweepIsAuthorizedBeforeAnyRead and the rotor.
func TestTheSweepIsAuthorizedBeforeAnyRead(t *testing.T) {
	t.Parallel()

	pending := &fakePending{shards: 1}
	sweeper := newFakeSweeper(t, pending, &recordingPlacer{}, &movableClock{now: reconcileNow}, denySweeps{})
	if _, err := sweeper.Sweep(context.Background(), servicePrincipal(t)); !errors.Is(err, errDenied) {
		t.Fatalf("Sweep = %v, want the denial", err)
	}
	if len(pending.requests) != 0 {
		t.Errorf("a denied sweep read %d pages", len(pending.requests))
	}
}

func TestTheSweepVisitsEveryShardInTurn(t *testing.T) {
	t.Parallel()

	pending := &fakePending{shards: 3}
	sweeper := newFakeSweeper(t, pending, &recordingPlacer{}, &movableClock{now: reconcileNow}, &allowSweeps{})
	var shards []int
	for range 4 {
		result, err := sweeper.Sweep(context.Background(), servicePrincipal(t))
		if err != nil {
			t.Fatalf("Sweep: %v", err)
		}
		shards = append(shards, result.Shard)
	}
	if !slices.Equal(shards, []int{0, 1, 2, 0}) {
		t.Errorf("shards = %v, want [0 1 2 0]", shards)
	}
}

func TestNewPendingSweeperRefusesAnIncompleteConfiguration(t *testing.T) {
	t.Parallel()

	valid := PendingSweeperConfig{
		Authorizer: &allowSweeps{}, Pending: &fakePending{shards: 1}, Placer: &recordingPlacer{},
		Clock: &movableClock{}, Horizon: time.Minute, PageLimit: 1, MaxPages: 1,
	}
	for name, mutate := range map[string]func(*PendingSweeperConfig){
		"authorizer": func(c *PendingSweeperConfig) { c.Authorizer = nil },
		"pending":    func(c *PendingSweeperConfig) { c.Pending = nil },
		"placer":     func(c *PendingSweeperConfig) { c.Placer = nil },
		"clock":      func(c *PendingSweeperConfig) { c.Clock = nil },
		"horizon":    func(c *PendingSweeperConfig) { c.Horizon = 0 },
		"page zero":  func(c *PendingSweeperConfig) { c.PageLimit = 0 },
		"page high":  func(c *PendingSweeperConfig) { c.PageLimit = 1001 },
		"max pages":  func(c *PendingSweeperConfig) { c.MaxPages = 0 },
	} {
		cfg := valid
		mutate(&cfg)
		if _, err := NewPendingSweeper(cfg); !errors.Is(err, ErrInvalidPendingSweeperConfig) {
			t.Errorf("%s: NewPendingSweeper = %v, want ErrInvalidPendingSweeperConfig", name, err)
		}
	}
	if _, err := NewPendingSweeper(valid); err != nil {
		t.Errorf("control: %v", err)
	}
}
