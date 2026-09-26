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
	admissionPlacer, err := NewReconciler(Config{
		Directory: mustDirectory(t, store), Catalog: store, Claims: store,
		Clock: clock, HolderID: "factory-2-admission", ClaimTTL: time.Minute, CandidateLimit: 8,
		Links: links, ActorID: testActor,
	})
	if err != nil {
		t.Fatalf("NewReconciler admission: %v", err)
	}
	sweeper, err := NewPendingSweeper(PendingSweeperConfig{
		Authorizer: &allowSweeps{}, Pending: store, Placer: reconciler, AdmissionPlacer: admissionPlacer, Clock: clock,
		Catalog: store, Directory: mustDirectory(t, store), SessionCommands: store,
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
			InternalEndpoint: sessionwire.InternalEndpoint("wss://" + string(host) + ".internal"),
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

// The admission signal places directly: the fake clock never advances to a
// periodic interval, and the durable sweep remains a separate recovery path.
func TestAdmissionPlacesColdSessionBeforeOneInterval(t *testing.T) {
	f := newPendingFixture(t)
	publishPooled(t, f.store, f.clock, "host-a")
	admitPooledCreate(t, f.store, f.clock, "session-cold", "create-cold", 5*time.Minute)
	entry, err := f.store.GetDispositionCommand(context.Background(), sessionstore.GetDispositionCommandRequest{
		TenantID: testTenant, SessionID: "session-cold", CommandID: "create-cold",
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := f.sweeper.PlaceAdmission(context.Background(), servicePrincipal(t), Notice(entry))
	if err != nil {
		t.Fatal(err)
	}
	if result.Attached == (sessionwire.HostLinkRegistryObservation{}) || len(f.links.attaches) != 1 {
		t.Fatalf("targeted placement = %+v, attaches = %+v", result, f.links.attaches)
	}
}

func TestConcurrentAdmissionAndSweepShareThePlacementClaim(t *testing.T) {
	f := newPendingFixture(t)
	publishPooled(t, f.store, f.clock, "host-a")
	admitPooledCreate(t, f.store, f.clock, "session-race", "create-race", 5*time.Minute)
	entry, err := f.store.GetDispositionCommand(context.Background(), sessionstore.GetDispositionCommandRequest{
		TenantID: testTenant, SessionID: "session-race", CommandID: "create-race",
	})
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	f.links.on("host-a", func(_ int, req sessionwire.HostLinkAttachRequest) (sessionwire.HostLinkRegistryObservation, error) {
		close(entered)
		<-release
		return acceptedObservation(req, attachedEpoch), nil
	})
	done := make(chan error, 1)
	go func() {
		_, err := f.sweeper.PlaceAdmission(context.Background(), servicePrincipal(t), Notice(entry))
		done <- err
	}()
	<-entered
	f.sweepEveryShard(t)
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	f.links.mu.Lock()
	count := len(f.links.attaches)
	f.links.mu.Unlock()
	if count != 1 {
		t.Fatalf("attach count = %d, want one", count)
	}
}

type openGateCatalog struct{}

func (openGateCatalog) GetCatalogEntry(context.Context, sessionstore.GetCatalogEntryRequest) (sessionstore.CatalogEntry, error) {
	return sessionstore.CatalogEntry{Record: sessionstore.CatalogRecord{OpenGates: []sessionwire.GateProjection{{}}}}, nil
}
func (openGateCatalog) UpdateCatalogDesiredState(context.Context, sessionstore.UpdateCatalogDesiredStateRequest) (sessionstore.CatalogEntry, error) {
	return sessionstore.CatalogEntry{}, nil
}
func (openGateCatalog) Owner(context.Context, sessionwire.TenantID, sessionwire.SessionID) (sessionwire.HostLinkRegistryObservation, bool, error) {
	return sessionwire.HostLinkRegistryObservation{}, false, nil
}
func (openGateCatalog) Candidates(context.Context, sessionstore.ListCompatibleHostsRequest) (sessionstore.HostTargetPage, error) {
	return sessionstore.HostTargetPage{}, nil
}

func TestAdmissionInventoriesOpenGateBeforePlacement(t *testing.T) {
	placer := &recordingPlacer{}
	gate := open(testSession, "answer-earlier", sessionstore.InboxStatePending, reconcileNow.Add(time.Minute))
	gate.Record.Descriptor.Kind = sessionstore.CommandKind("gate_response")
	input := open(testSession, "input-later", sessionstore.InboxStatePending, reconcileNow.Add(time.Minute))
	s := newFakeSweeper(t, &fakePending{shards: 1, pages: []sessionstore.DispositionDueCommandPage{{Commands: []sessionstore.DispositionInboxEntry{gate, input}}}}, placer, &movableClock{now: reconcileNow}, &allowSweeps{})
	s.cfg.Catalog = openGateCatalog{}
	s.cfg.Directory = openGateCatalog{}
	_, err := s.PlaceAdmission(context.Background(), servicePrincipal(t), Notice(input))
	if err != nil {
		t.Fatal(err)
	}
	if len(placer.requests) != 1 || !slices.Equal(placer.requests[0].GateResponses, []sessionwire.CommandID{"answer-earlier"}) {
		t.Fatalf("targeted placement = %+v, want earlier gate response in capability inventory", placer.requests)
	}
}

func TestAdmissionInventoriesEarlierAttributedCommands(t *testing.T) {
	placer := &recordingPlacer{}
	attributed := open(testSession, "attributed-earlier", sessionstore.InboxStatePending, reconcileNow.Add(time.Minute))
	attributed.Record.Descriptor.Principal = &sessionwire.Principal{}
	plain := open(testSession, "plain-later", sessionstore.InboxStatePending, reconcileNow.Add(time.Minute))
	s := newFakeSweeper(t, &fakePending{shards: 1, pages: []sessionstore.DispositionDueCommandPage{{Commands: []sessionstore.DispositionInboxEntry{attributed, plain}}}}, placer, &movableClock{now: reconcileNow}, &allowSweeps{})
	_, err := s.PlaceAdmission(context.Background(), servicePrincipal(t), Notice(plain))
	if err != nil {
		t.Fatal(err)
	}
	if len(placer.requests) != 1 || !slices.Equal(placer.requests[0].PrincipalCommands, []sessionwire.CommandID{"attributed-earlier"}) {
		t.Fatalf("targeted placement = %+v, want earlier attributed command in capability inventory", placer.requests)
	}
}

func TestAdmissionWaitsForItsOwnDurableDueEntry(t *testing.T) {
	placer := &recordingPlacer{}
	prior := open(testSession, "prior", sessionstore.InboxStatePending, reconcileNow.Add(time.Minute))
	s := newFakeSweeper(t, &fakePending{shards: 1, pages: []sessionstore.DispositionDueCommandPage{{Commands: []sessionstore.DispositionInboxEntry{prior}}}}, placer, &movableClock{now: reconcileNow}, &allowSweeps{})
	notice := Notice(open(testSession, "new-command", sessionstore.InboxStatePending, reconcileNow.Add(time.Minute)))
	_, err := s.PlaceAdmission(context.Background(), servicePrincipal(t), notice)
	if err != nil {
		t.Fatal(err)
	}
	if len(placer.requests) != 0 {
		t.Fatalf("placed without seeing the admitted command: %+v", placer.requests)
	}
}

type staleDedicatedOwner struct{}

func (staleDedicatedOwner) GetCatalogEntry(context.Context, sessionstore.GetCatalogEntryRequest) (sessionstore.CatalogEntry, error) {
	r := pooledRecord()
	r.DesiredPlacement = sessionwire.HostPlacementDedicated
	r.DesiredGeneration = 4
	return sessionstore.CatalogEntry{Record: r}, nil
}
func (staleDedicatedOwner) UpdateCatalogDesiredState(context.Context, sessionstore.UpdateCatalogDesiredStateRequest) (sessionstore.CatalogEntry, error) {
	return sessionstore.CatalogEntry{}, nil
}
func (staleDedicatedOwner) Owner(context.Context, sessionwire.TenantID, sessionwire.SessionID) (sessionwire.HostLinkRegistryObservation, bool, error) {
	r := pooledRecord()
	r.DesiredPlacement = sessionwire.HostPlacementDedicated
	o := residentOwner(r)
	o.ObservedAt = reconcileNow
	o.ExpiresAt = reconcileNow.Add(time.Minute)
	return o, true, nil
}
func (staleDedicatedOwner) Candidates(context.Context, sessionstore.ListCompatibleHostsRequest) (sessionstore.HostTargetPage, error) {
	return sessionstore.HostTargetPage{}, nil
}
func TestAdmissionDoesNotSkipDedicatedOwnerFromOlderGeneration(t *testing.T) {
	entry := open(testSession, "new-command", sessionstore.InboxStatePending, reconcileNow.Add(time.Minute))
	placer := &recordingPlacer{}
	s := newFakeSweeper(t, &fakePending{shards: 1, pages: []sessionstore.DispositionDueCommandPage{{Commands: []sessionstore.DispositionInboxEntry{entry}}}}, placer, &movableClock{now: reconcileNow}, &allowSweeps{})
	s.cfg.Catalog, s.cfg.Directory = staleDedicatedOwner{}, staleDedicatedOwner{}
	if _, err := s.PlaceAdmission(context.Background(), servicePrincipal(t), Notice(entry)); err != nil {
		t.Fatal(err)
	}
	if len(placer.requests) != 1 {
		t.Fatalf("placement calls = %d, want stale owner reconsidered", len(placer.requests))
	}
}

type sessionCommandReader struct {
	entry sessionstore.DispositionInboxEntry
	calls int
}

func (r *sessionCommandReader) ListSessionDispositionCommands(context.Context, sessionstore.ListSessionDispositionCommandsRequest) (sessionstore.SessionDispositionCommandPage, error) {
	r.calls++
	if r.calls == 1 {
		return sessionstore.SessionDispositionCommandPage{Commands: []sessionstore.DispositionInboxEntry{r.entry}, NextAfterOrder: r.entry.AcceptedOrder}, nil
	}
	return sessionstore.SessionDispositionCommandPage{}, nil
}

func TestAdmissionFindsNewCommandBeyondBusyShardHead(t *testing.T) {
	placer := &recordingPlacer{}
	busy := &fakePending{shards: 1, pages: []sessionstore.DispositionDueCommandPage{
		{NextCursor: "p1"}, {NextCursor: "p2"}, {NextCursor: "p3"}, {NextCursor: "p4"},
	}}
	entry := open(testSession, "new-command", sessionstore.InboxStatePending, reconcileNow.Add(time.Minute))
	entry.AcceptedOrder = 2050
	reader := &sessionCommandReader{entry: entry}
	s := newFakeSweeper(t, busy, placer, &movableClock{now: reconcileNow}, &allowSweeps{})
	s.cfg.SessionCommands = reader
	_, err := s.PlaceAdmission(context.Background(), servicePrincipal(t), Notice(entry))
	if err != nil {
		t.Fatal(err)
	}
	if reader.calls == 0 || len(busy.requests) != 0 || len(placer.requests) != 1 || placer.requests[0].SessionID != testSession {
		t.Fatalf("session reads=%d shard reads=%d placements=%+v, want one targeted placement without a shard scan", reader.calls, len(busy.requests), placer.requests)
	}
}

type pagedHistory struct {
	entry sessionstore.DispositionInboxEntry
	reads int
}

func (r *pagedHistory) ListSessionDispositionCommands(_ context.Context, req sessionstore.ListSessionDispositionCommandsRequest) (sessionstore.SessionDispositionCommandPage, error) {
	r.reads++
	page := sessionstore.SessionDispositionCommandPage{}
	for i := req.AfterOrder + 1; i <= uint64(300) && len(page.Commands) < req.Limit; i++ {
		old := open(testSession, sessionwire.CommandID(fmt.Sprintf("old-%d", i)), sessionstore.InboxStateApplied, reconcileNow)
		old.AcceptedOrder = i
		page.Commands = append(page.Commands, old)
		page.NextAfterOrder = i
	}
	if len(page.Commands) < req.Limit {
		page.Commands = append(page.Commands, r.entry)
		page.NextAfterOrder = r.entry.AcceptedOrder
	}
	return page, nil
}

func TestAdmissionPagesPastMoreThan256SettledCommands(t *testing.T) {
	entry := open(testSession, "new-command", sessionstore.InboxStatePending, reconcileNow.Add(time.Minute))
	entry.AcceptedOrder = 301
	if got := Notice(entry).AcceptedOrder; got != 301 {
		t.Fatalf("notice accepted order = %d", got)
	}
	reader := &pagedHistory{entry: entry}
	placer := &recordingPlacer{}
	pending := &fakePending{shards: 1}
	s := newFakeSweeper(t, pending, placer, &movableClock{now: reconcileNow}, &allowSweeps{})
	s.cfg.PageLimit = 256
	s.cfg.SessionCommands = reader
	if _, err := s.PlaceAdmission(context.Background(), servicePrincipal(t), Notice(entry)); err != nil {
		t.Fatal(err)
	}
	if reader.reads != 2 || len(pending.requests) != 0 || len(placer.requests) != 1 {
		t.Fatalf("session reads=%d due reads=%d placements=%d, want two session pages and one placement", reader.reads, len(pending.requests), len(placer.requests))
	}
}

type longHistoryReader struct {
	orders []uint64
}

func (r *longHistoryReader) ListSessionDispositionCommands(_ context.Context, req sessionstore.ListSessionDispositionCommandsRequest) (sessionstore.SessionDispositionCommandPage, error) {
	r.orders = append(r.orders, req.AfterOrder)
	if req.AfterOrder == 0 {
		page := sessionstore.SessionDispositionCommandPage{NextAfterOrder: uint64(req.Limit)}
		for i := 1; i <= req.Limit; i++ {
			old := open(testSession, sessionwire.CommandID(fmt.Sprintf("settled-%d", i)), sessionstore.InboxStateApplied, reconcileNow)
			old.AcceptedOrder = uint64(i)
			page.Commands = append(page.Commands, old)
		}
		return page, nil
	}
	return sessionstore.SessionDispositionCommandPage{}, nil
}

func TestAdmissionSkipsSettledHistoryWithoutMissingTheNewCommand(t *testing.T) {
	entry := open(testSession, "new-command", sessionstore.InboxStatePending, reconcileNow.Add(time.Minute))
	entry.AcceptedOrder = 10000
	reader := &longHistoryReader{}
	placer := &recordingPlacer{}
	pending := &fakePending{shards: 1, pages: []sessionstore.DispositionDueCommandPage{{Commands: []sessionstore.DispositionInboxEntry{entry}}}}
	s := newFakeSweeper(t, pending, placer, &movableClock{now: reconcileNow}, &allowSweeps{})
	s.cfg.SessionCommands = reader
	_, err := s.PlaceAdmission(context.Background(), servicePrincipal(t), Notice(entry))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(reader.orders, []uint64{0, 7}) || len(pending.requests) != 1 || len(placer.requests) != 1 || !slices.Equal(placer.requests[0].Wake, []sessionwire.CommandID{entry.Record.Descriptor.CommandID}) {
		t.Fatalf("history bounds=%v due reads=%d placement=%+v, want a bounded open-work read", reader.orders, len(pending.requests), placer.requests)
	}
	if len(placer.requests[0].GateResponses) != 0 || len(placer.requests[0].PrincipalCommands) != 0 {
		t.Fatalf("placement=%+v, plain command must not require optional Host capabilities", placer.requests[0])
	}
}

func TestAdmissionLongHistoryPreservesOlderOpenCapabilities(t *testing.T) {
	entry := open(testSession, "new-command", sessionstore.InboxStatePending, reconcileNow.Add(time.Minute))
	gate := open(testSession, "older-gate", sessionstore.InboxStatePending, reconcileNow.Add(time.Minute))
	gate.Record.Descriptor.Kind = sessionstore.CommandKind("gate_response")
	gate.Record.Descriptor.Principal = &sessionwire.Principal{}
	pending := &fakePending{shards: 1, pages: []sessionstore.DispositionDueCommandPage{{Commands: []sessionstore.DispositionInboxEntry{gate, entry}}}}
	placer := &recordingPlacer{}
	s := newFakeSweeper(t, pending, placer, &movableClock{now: reconcileNow}, &allowSweeps{})
	s.cfg.SessionCommands = &longHistoryReader{}
	_, err := s.PlaceAdmission(context.Background(), servicePrincipal(t), Notice(entry))
	if err != nil {
		t.Fatal(err)
	}
	if len(placer.requests) != 1 || !slices.Equal(placer.requests[0].GateResponses, []sessionwire.CommandID{"older-gate"}) || !slices.Equal(placer.requests[0].PrincipalCommands, []sessionwire.CommandID{"older-gate"}) {
		t.Fatalf("placement=%+v, want older open command's capabilities", placer.requests)
	}
}

func TestAdmissionLongHistoryDefersWhenDueInventoryIsTruncated(t *testing.T) {
	entry := open(testSession, "new-command", sessionstore.InboxStatePending, reconcileNow.Add(time.Minute))
	pending := &fakePending{shards: 1, pages: []sessionstore.DispositionDueCommandPage{
		{Commands: []sessionstore.DispositionInboxEntry{entry}, NextCursor: "p1"},
		{NextCursor: "p2"},
	}}
	placer := &recordingPlacer{}
	s := newFakeSweeper(t, pending, placer, &movableClock{now: reconcileNow}, &allowSweeps{})
	s.cfg.SessionCommands = &longHistoryReader{}
	_, err := s.PlaceAdmission(context.Background(), servicePrincipal(t), Notice(entry))
	if err != nil {
		t.Fatal(err)
	}
	if len(placer.requests) != 0 {
		t.Fatalf("placement=%+v, want deferred placement after an incomplete due scan", placer.requests)
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

// TestALiveCreateBehindMoreThanAPassOfExpiredRowsIsStillPlaced is the B5 spec
// gate's F3 probe, committed: one shard, two rows per pass, and more expired
// rows at the head of the deadline-ordered view than one pass reads. Nothing
// retires them here -- the disposition deadline sweep is not composed -- so
// this is the placement sweep's OWN progress guarantee, independent of
// retirement. Before per-shard positions, every pass re-read the same two
// expired rows from the head and the live create was never attached.
func TestALiveCreateBehindMoreThanAPassOfExpiredRowsIsStillPlaced(t *testing.T) {
	t.Parallel()

	clock := &movableClock{now: reconcileNow}
	store, err := sessionstore.Open(context.Background(), memstore.New(), sessionstore.WithClock(clock), sessionstore.WithControlShards(1))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(context.Background()) })
	// Five creates that will be expired, filed ahead of the live one.
	for i := range 5 {
		admitPooledCreate(t, store, clock, sessionwire.SessionID(fmt.Sprintf("session-expired-%d", i)), sessionwire.CommandID(fmt.Sprintf("create-expired-%d", i)), time.Minute)
	}
	clock.now = clock.now.Add(2 * time.Minute)
	admitPooledCreate(t, store, clock, "session-live", "create-live", time.Hour)

	placer := &recordingPlacer{}
	sweeper, err := NewPendingSweeper(PendingSweeperConfig{
		Authorizer: &allowSweeps{}, Pending: store, Placer: placer, Clock: clock,
		Horizon: 2 * time.Hour, PageLimit: 2, MaxPages: 1,
	})
	if err != nil {
		t.Fatalf("NewPendingSweeper: %v", err)
	}
	var resumed int
	for pass := range 5 {
		result, err := sweeper.Sweep(context.Background(), servicePrincipal(t))
		if err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		if result.Resumed {
			resumed++
		}
		for _, req := range placer.requests {
			if req.SessionID == "session-live" {
				if resumed == 0 {
					t.Fatalf("the live create was placed without a resumed pass; the probe did not put it behind the head")
				}
				if len(req.Wake) != 1 || req.Wake[0] != "create-live" {
					t.Fatalf("the live session was woken with %v, want its create", req.Wake)
				}
				return
			}
		}
	}
	t.Fatalf("the live create was never placed across 5 passes (%d resumed); reconciled %+v", resumed, placer.requests)
}

// TestAShardsPositionIsKeptOnlyWhileItsPassIsTruncated is the cursor
// lifecycle: a truncated pass keeps its continuation and the next pass over
// that shard presents it with no second bound; a pass that reaches the end
// forgets it and the next re-arms at the head against a fresh bound; a
// continuation the store REFUSES is dropped rather than presented forever; and
// a transient fault keeps it.
func TestAShardsPositionIsKeptOnlyWhileItsPassIsTruncated(t *testing.T) {
	t.Parallel()

	clock := &movableClock{now: reconcileNow}
	pending := &fakePending{shards: 1, pages: []sessionstore.DispositionDueCommandPage{
		{NextCursor: "cursor-1"}, {NextCursor: "cursor-2"}, // pass 1: truncated at cursor-2
		{},                                                 // pass 2: resumes and reaches the end
		{NextCursor: "cursor-3"}, {NextCursor: "cursor-4"}, // pass 3: fresh, truncated at cursor-4
	}}
	sweeper := newFakeSweeper(t, pending, &recordingPlacer{}, clock, &allowSweeps{})
	sweep := func() PendingSweepResult {
		t.Helper()
		result, err := sweeper.Sweep(context.Background(), servicePrincipal(t))
		if err != nil {
			t.Fatalf("Sweep: %v", err)
		}
		return result
	}
	if r := sweep(); !r.Truncated || r.Resumed {
		t.Fatalf("pass 1 = %+v, want a fresh truncated pass", r)
	}
	clock.now = clock.now.Add(time.Minute)
	if r := sweep(); r.Truncated || !r.Resumed {
		t.Fatalf("pass 2 = %+v, want a resumed pass that reaches the end", r)
	}
	if r := sweep(); !r.Truncated || r.Resumed {
		t.Fatalf("pass 3 = %+v, want a fresh truncated pass", r)
	}
	want := []sessionstore.ListDueDispositionCommandsRequest{
		{Shard: 0, DueAtOrBefore: reconcileNow.Add(5 * time.Minute), Limit: 7},
		{Shard: 0, Limit: 7, Cursor: "cursor-1"},
		{Shard: 0, Limit: 7, Cursor: "cursor-2"},
		{Shard: 0, DueAtOrBefore: reconcileNow.Add(time.Minute + 5*time.Minute), Limit: 7},
		{Shard: 0, Limit: 7, Cursor: "cursor-3"},
	}
	if !slices.Equal(pending.requests, want) {
		t.Fatalf("requests = %+v\nwant %+v", pending.requests, want)
	}

	// A transient fault keeps the position; a refused one drops it.
	pending.requests = nil
	pending.err = errors.New("the provider could not be reached")
	if _, err := sweeper.Sweep(context.Background(), servicePrincipal(t)); err == nil {
		t.Fatal("a failed page was not reported")
	}
	pending.err = &sessionstore.InboxError{Code: sessionstore.InboxErrorCursor}
	if _, err := sweeper.Sweep(context.Background(), servicePrincipal(t)); err == nil {
		t.Fatal("a refused continuation was not reported")
	}
	pending.err = nil
	sweep()
	if len(pending.requests) != 3 || pending.requests[0].Cursor != "cursor-4" || pending.requests[1].Cursor != "cursor-4" ||
		pending.requests[2].Cursor != "" || pending.requests[2].DueAtOrBefore.IsZero() {
		t.Fatalf("after a transient fault and a refusal, requests = %+v; want cursor-4 twice, then a fresh head read", pending.requests)
	}
}

// TestACommandAtExactlyItsDeadlineIsNoLongerLive pins the deadline instant
// (spec gate S4): the apply deadline is half-open, so a pending command whose
// deadline is exactly now is neither woken nor a reason to place.
func TestACommandAtExactlyItsDeadlineIsNoLongerLive(t *testing.T) {
	t.Parallel()

	clock := &movableClock{now: reconcileNow}
	pending := &fakePending{shards: 1, pages: []sessionstore.DispositionDueCommandPage{{
		Commands: []sessionstore.DispositionInboxEntry{
			open("s-edge", "c-edge", sessionstore.InboxStatePending, clock.now),
			open("s-live", "c-live", sessionstore.InboxStatePending, clock.now.Add(time.Nanosecond)),
		},
	}}}
	placer := &recordingPlacer{}
	if _, err := newFakeSweeper(t, pending, placer, clock, &allowSweeps{}).Sweep(context.Background(), servicePrincipal(t)); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(placer.requests) != 1 || placer.requests[0].SessionID != "s-live" {
		t.Fatalf("reconciled %+v, want only the command one nanosecond inside its deadline", placer.requests)
	}
}

// scriptedPlacer answers each session with a chosen Result, and can cancel the
// pass from inside a reconcile.
type scriptedPlacer struct {
	mu       sync.Mutex
	results  map[sessionwire.SessionID]Result
	onCall   func(sessionwire.SessionID) error
	requests []sessionwire.SessionID
}

func (p *scriptedPlacer) Reconcile(_ context.Context, req Request) (Result, error) {
	p.mu.Lock()
	p.requests = append(p.requests, req.SessionID)
	p.mu.Unlock()
	if p.onCall != nil {
		if err := p.onCall(req.SessionID); err != nil {
			return Result{}, err
		}
	}
	return p.results[req.SessionID], nil
}

// TestAttachedCountsOnlyAttachesAndNotOwnerWakes holds PendingSweepResult's
// Attached to its meaning (quality gate QM11): a session woken on its existing
// owner is bound, and is not an attach.
func TestAttachedCountsOnlyAttachesAndNotOwnerWakes(t *testing.T) {
	t.Parallel()

	clock := &movableClock{now: reconcileNow}
	live := clock.now.Add(time.Minute)
	pending := &fakePending{shards: 1, pages: []sessionstore.DispositionDueCommandPage{{
		Commands: []sessionstore.DispositionInboxEntry{
			open("s-attached", "c-1", sessionstore.InboxStatePending, live),
			open("s-woken", "c-2", sessionstore.InboxStatePending, live),
		},
	}}}
	placer := &scriptedPlacer{results: map[sessionwire.SessionID]Result{
		"s-attached": {Decision: Decision{Outcome: OutcomeAttachPooled}, Bound: true, Attached: sessionwire.HostLinkRegistryObservation{HostID: "host-a", LeaseEpoch: 1}},
		"s-woken":    {Decision: Decision{Outcome: OutcomeReuseOwner}, Bound: true},
	}}
	sweeper := newFakeSweeper(t, pending, placer, clock, &allowSweeps{})
	result, err := sweeper.Sweep(context.Background(), servicePrincipal(t))
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if result.Sessions != 2 || result.Attached != 1 || result.Outcomes[OutcomeReuseOwner] != 1 || result.Outcomes[OutcomeAttachPooled] != 1 {
		t.Fatalf("result = %+v, want 2 sessions, 1 attached, 1 owner reused", result)
	}
}

// TestACancelledPassStopsReconciling is the sweep's early return (quality gate
// QM10): once the pass context ends, a failing reconcile ends the pass rather
// than going on to every remaining session.
func TestACancelledPassStopsReconciling(t *testing.T) {
	t.Parallel()

	clock := &movableClock{now: reconcileNow}
	live := clock.now.Add(time.Minute)
	pending := &fakePending{shards: 1, pages: []sessionstore.DispositionDueCommandPage{{
		Commands: []sessionstore.DispositionInboxEntry{
			open("s-1", "c-1", sessionstore.InboxStatePending, live),
			open("s-2", "c-2", sessionstore.InboxStatePending, live),
			open("s-3", "c-3", sessionstore.InboxStatePending, live),
		},
	}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	placer := &scriptedPlacer{onCall: func(sessionwire.SessionID) error {
		cancel()
		return context.Canceled
	}}
	sweeper := newFakeSweeper(t, pending, placer, clock, &allowSweeps{})
	result, err := sweeper.Sweep(ctx, servicePrincipal(t))
	if !errors.Is(err, context.Canceled) || len(placer.requests) != 1 || result.Failures != 1 {
		t.Fatalf("Sweep = (%+v, %v) after %v, want it to stop at the first cancelled session", result, err, placer.requests)
	}
}

// shrinkingPending reports a shard count a test can change between passes.
type shrinkingPending struct {
	*fakePending
	count int
}

func (p *shrinkingPending) ControlShards() int { return p.count }

// TestAShrunkShardCountRestartsTheRotorAndDropsStalePositions is the rotor
// reset (quality gate QM14): a replica whose rotor is past the new count
// sweeps shard 0 next, and a position kept for a shard that no longer exists
// is never presented.
func TestAShrunkShardCountRestartsTheRotorAndDropsStalePositions(t *testing.T) {
	t.Parallel()

	clock := &movableClock{now: reconcileNow}
	pending := &shrinkingPending{fakePending: &fakePending{shards: 3, pages: []sessionstore.DispositionDueCommandPage{
		{},                                         // pass 1: shard 0
		{NextCursor: "c1-a"}, {NextCursor: "c1-b"}, // pass 2: shard 1, truncated at c1-b
		{NextCursor: "c2-a"}, {NextCursor: "c2-b"}, // pass 3: shard 2, truncated at c2-b
		{}, // pass 4: shard 0
		{}, // pass 5: shard 1 resumes from c1-b and ends; the rotor now points at 2
	}}, count: 3}
	sweeper := newFakeSweeper(t, pending, &recordingPlacer{}, clock, &allowSweeps{})
	sweep := func() PendingSweepResult {
		t.Helper()
		result, err := sweeper.Sweep(context.Background(), servicePrincipal(t))
		if err != nil {
			t.Fatalf("Sweep: %v", err)
		}
		return result
	}
	for range 5 {
		sweep()
	}
	pending.count = 2
	pending.requests = nil
	shards := []int{sweep().Shard, sweep().Shard}
	if !slices.Equal(shards, []int{0, 1}) {
		t.Fatalf("after shrinking to 2 shards the sweeps visited %v, want [0 1]", shards)
	}
	// Grow back: shard 2 exists again, and its old position must not be
	// presented to the view that has since been rebuilt.
	pending.count = 3
	sweep()
	sweep()
	pending.requests = nil
	if r := sweep(); r.Shard != 2 || r.Resumed {
		t.Fatalf("the regrown shard 2 pass = %+v, want a fresh pass", r)
	}
	for _, req := range pending.requests {
		if req.Cursor != "" {
			t.Fatalf("a position kept for a shard that had ceased to exist was presented: %+v", req)
		}
	}
}
