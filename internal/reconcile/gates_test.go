package reconcile

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

// ---------------------------------------------------------------------------
// The fixture.
//
// Every behavioural case drives the REAL sessionstore.Store over the REAL
// memstore. Nothing fakes the retirement: the decision that separates a remnant
// from a live gate lives inside Store.RetireGateDeadlineIntent, which re-reads
// the session's projection at its own clock reading, so a fake retirement would
// be a fake of exactly the thing under test.
//
// The only interposition is at the PROVIDER seam -- a storage.OrderedIndex
// decorator -- which is where a concurrent Host, or a crash, actually lands.
// ---------------------------------------------------------------------------

const (
	gateTenant  = sessionwire.TenantID("tenant-a")
	gateSession = sessionwire.SessionID("session-a")
	gateAgent   = sessionwire.AgentID("agent-a")
	gateRuntime = "runtime-v1"
)

var gateOrigin = time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)

type movableClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *movableClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *movableClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// orderedHooks interposes on the ordered index a Store was opened over.
//
// afterListDue runs AFTER the inner provider has returned a due page and
// released whatever lock it held, which is the exact window a retirement's own
// re-read exists to cover: the page's rows are in the sweep's hand and every
// one of them is still writable by anyone.
//
// failNamespaceWrite refuses the FIRST write whose namespace contains a marker,
// with the provider's own backend error. That is how a crash is injected at a
// named point inside a two-write store operation: OpenGate writes the intent
// and then the catalog record, so failing the catalog write leaves exactly the
// state a process death between them leaves.
//
// Its fidelity limit, stated rather than implied: a refused write is a refusal
// the provider reports, where a real crash simply stops. The store's own error
// path is therefore exercised where a crash would exercise none. That is
// observable only in the error the fixture discards, and never in the durable
// state the sweep then reads, which is what every assertion here is about.
type orderedHooks struct {
	inner storage.OrderedIndex

	mu sync.Mutex

	afterListDue func(page storage.DuePage)
	listDueFired bool

	failNamespaceWrite string
	writeFailed        bool

	failNamespaceRead string
	readFailed        bool

	// writes counts every mutating provider call by namespace marker. It is
	// the probe for "this operation wrote nothing", which no durable read can
	// answer: a repeat that rewrote the same bytes is indistinguishable from
	// one that did not, except at the seam.
	writes map[string]int
}

var errInjectedProvider = errors.New("injected provider failure")

func (h *orderedHooks) failWrites(marker string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.failNamespaceWrite, h.writeFailed = marker, false
}

// failReads refuses the FIRST read whose namespace contains a marker. It is how
// a provider fault is injected INSIDE a store operation whose own error
// classification is the thing under test.
func (h *orderedHooks) failReads(marker string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.failNamespaceRead, h.readFailed = marker, false
}

func (h *orderedHooks) shouldFailRead(namespace string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.failNamespaceRead == "" || h.readFailed || !strings.Contains(namespace, h.failNamespaceRead) {
		return false
	}
	h.readFailed = true
	return true
}

func (h *orderedHooks) onceAfterListDue(f func(page storage.DuePage)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.afterListDue, h.listDueFired = f, false
}

// writeCount reports the mutating provider calls made against namespaces
// containing marker.
func (h *orderedHooks) writeCount(marker string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	total := 0
	for namespace, count := range h.writes {
		if strings.Contains(namespace, marker) {
			total += count
		}
	}
	return total
}

func (h *orderedHooks) noteWrite(namespace string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.writes == nil {
		h.writes = map[string]int{}
	}
	h.writes[namespace]++
}

func (h *orderedHooks) shouldFail(namespace string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.failNamespaceWrite == "" || h.writeFailed || !strings.Contains(namespace, h.failNamespaceWrite) {
		return false
	}
	h.writeFailed = true
	return true
}

func (h *orderedHooks) Get(ctx context.Context, id storage.OrderedID) (storage.OrderedRecord, error) {
	if h.shouldFailRead(id.Namespace) {
		return storage.OrderedRecord{}, errInjectedProvider
	}
	return h.inner.Get(ctx, id)
}

func (h *orderedHooks) Create(
	ctx context.Context, id storage.OrderedID, rankingScope string, value []byte, rank storage.Rank, due storage.Due,
) (storage.OrderedRecord, bool, error) {
	if h.shouldFail(id.Namespace) {
		return storage.OrderedRecord{}, false, errInjectedProvider
	}
	h.noteWrite(id.Namespace)
	return h.inner.Create(ctx, id, rankingScope, value, rank, due)
}

func (h *orderedHooks) Update(
	ctx context.Context, id storage.OrderedID, expectedRevision uint64, value []byte, rank storage.Rank, due storage.Due,
) (storage.OrderedRecord, error) {
	if h.shouldFail(id.Namespace) {
		return storage.OrderedRecord{}, errInjectedProvider
	}
	h.noteWrite(id.Namespace)
	return h.inner.Update(ctx, id, expectedRevision, value, rank, due)
}

func (h *orderedHooks) Delete(
	ctx context.Context, id storage.OrderedID, expectedRevision uint64,
) (storage.OrderedRecord, error) {
	if h.shouldFail(id.Namespace) {
		return storage.OrderedRecord{}, errInjectedProvider
	}
	h.noteWrite(id.Namespace)
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
	h.mu.Lock()
	hook, fired := h.afterListDue, h.listDueFired
	if hook != nil && !fired {
		h.listDueFired = true
	}
	h.mu.Unlock()
	if err == nil && hook != nil && !fired {
		hook(page)
	}
	return page, err
}

// recordingAuthorizer is the service-sweep decision, and it records what it was
// asked so a test can require the principal to have reached it.
type recordingAuthorizer struct {
	refuse     error
	calls      int
	principals []identity.Principal
}

func (a *recordingAuthorizer) AuthorizeServiceSweep(_ context.Context, principal identity.Principal) error {
	a.calls++
	a.principals = append(a.principals, principal)
	return a.refuse
}

type gateFixture struct {
	t       *testing.T
	store   *sessionstore.Store
	clock   *movableClock
	hooks   *orderedHooks
	auth    *recordingAuthorizer
	sweeper *GateSweeper
	due     DueGates
	epoch   uint64
}

func newGateFixture(t *testing.T, shards, pageLimit, maxPages int) *gateFixture {
	t.Helper()
	return newGateFixtureWith(t, shards, pageLimit, maxPages, func(due DueGates) DueGates { return due })
}

// newGateFixtureWith builds the fixture with a caller-supplied wrapper around
// the store's due-page seam. The wrapper is always a DECORATOR over the real
// store; nothing here substitutes an answer the store would have given.
func newGateFixtureWith(t *testing.T, shards, pageLimit, maxPages int, wrap func(DueGates) DueGates) *gateFixture {
	t.Helper()

	clock := &movableClock{now: gateOrigin}
	composite := memstore.New()
	hooks := &orderedHooks{inner: composite.OrderedIndex}
	composite.OrderedIndex = hooks

	store, err := sessionstore.Open(context.Background(), composite,
		sessionstore.WithClock(clock), sessionstore.WithControlShards(shards))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(context.Background()); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	auth := &recordingAuthorizer{}
	due := wrap(store)
	sweeper, err := NewGateSweeper(GateSweeperConfig{
		Authorizer: auth, Due: due, Intents: store, Clock: clock,
		PageLimit: pageLimit, MaxPages: maxPages,
	})
	if err != nil {
		t.Fatalf("NewGateSweeper: %v", err)
	}
	return &gateFixture{t: t, store: store, clock: clock, hooks: hooks, auth: auth, sweeper: sweeper, due: due, epoch: 1}
}

func servicePrincipal(t *testing.T) identity.Principal {
	t.Helper()
	principal, err := identity.NewPrincipal(gateTenant, "factory-sweeper", identity.KindService)
	if err != nil {
		t.Fatalf("NewPrincipal: %v", err)
	}
	return principal
}

// prepare creates one session and gives it a durable journal tip, which is the
// precondition every open has: a gate names the event that opened it and that
// event must already be durable.
func (f *gateFixture) prepare(session sessionwire.SessionID) {
	f.t.Helper()
	if _, _, err := f.store.CreateCatalogEntry(context.Background(), sessionstore.CreateCatalogEntryRequest{
		TenantID: gateTenant, SessionID: session, AgentID: gateAgent, RuntimeCompatibilityID: gateRuntime,
		CreatedAt: f.clock.Now(), LastActiveAt: f.clock.Now(),
		State: sessionwire.SessionStateIdle, Residency: sessionwire.SessionResidencyCold,
		DesiredPlacement: sessionwire.HostPlacementPooled, IdempotencyKey: "create-" + string(session),
	}); err != nil {
		f.t.Fatalf("CreateCatalogEntry(%s): %v", session, err)
	}
	f.hostState(session, nil)
}

// hostState re-projects a session's Host-owned state, including its whole open
// gate set.
//
// It is the path SessionStore documents as leaving intents alone: it replaces
// OpenGates wholesale and never touches the deadline index. So dropping a gate
// here produces a remnant intent whose RecordedAt is the instant the gate was
// OPENED, which is what a test of the remnant AGE window needs and what a
// re-open through OpenGate could not give, since that re-stamps the intent.
func (f *gateFixture) hostState(session sessionwire.SessionID, gates []sessionwire.GateProjection) {
	f.t.Helper()
	if _, err := f.store.UpdateCatalogHostState(context.Background(), sessionstore.UpdateCatalogHostStateRequest{
		TenantID: gateTenant, SessionID: session, LeaseEpoch: f.epoch,
		State: sessionwire.SessionStateRunning, Residency: sessionwire.SessionResidencyResident,
		LastActiveAt: f.clock.Now(), LastJournalSeq: 4, LastEventID: "event-tip",
		OpenGates: gates,
	}); err != nil {
		f.t.Fatalf("UpdateCatalogHostState(%s): %v", session, err)
	}
}

func gateProjection(gate sessionwire.GateID, deadline time.Time) sessionwire.GateProjection {
	return sessionwire.GateProjection{
		GateID: gate, Kind: "approval",
		Prompt:           sessionwire.GatePrompt{Title: "approve?"},
		OpenedEventID:    sessionwire.EventID("event-" + gate),
		OpenedJournalSeq: 3,
		Deadline:         deadline,
		Answerability:    sessionwire.GateAnswerabilityResident,
	}
}

func (f *gateFixture) open(session sessionwire.SessionID, gate sessionwire.GateID, deadline time.Time) {
	f.t.Helper()
	if _, err := f.store.OpenGate(context.Background(), sessionstore.OpenGateRequest{
		TenantID: gateTenant, SessionID: session, LeaseEpoch: f.epoch,
		Gate: gateProjection(gate, deadline),
	}); err != nil {
		f.t.Fatalf("OpenGate(%s/%s): %v", session, gate, err)
	}
}

func (f *gateFixture) openGateIDs(session sessionwire.SessionID) []sessionwire.GateID {
	f.t.Helper()
	page, err := f.store.ReadGates(context.Background(), sessionstore.ReadGatesRequest{
		TenantID: gateTenant, SessionID: session,
	})
	if err != nil {
		f.t.Fatalf("ReadGates(%s): %v", session, err)
	}
	out := make([]sessionwire.GateID, 0, len(page.Gates))
	for _, gate := range page.Gates {
		out = append(out, gate.GateID)
	}
	return out
}

func (f *gateFixture) catalogRevision(session sessionwire.SessionID) uint64 {
	f.t.Helper()
	entry, err := f.store.GetCatalogEntry(context.Background(), sessionstore.GetCatalogEntryRequest{
		TenantID: gateTenant, SessionID: session,
	})
	if err != nil {
		f.t.Fatalf("GetCatalogEntry(%s): %v", session, err)
	}
	return entry.Revision
}

// dueNow pages every shard of the due view to exhaustion, independently of the
// sweeper, and reports what is still there. It is the probe for "still due".
func (f *gateFixture) dueNow() (gates []sessionwire.GateID, remnants []sessionwire.GateID, examined int) {
	f.t.Helper()
	for shard := range f.store.ControlShards() {
		req := sessionstore.ListDueGatesRequest{Shard: shard, DueAtOrBefore: f.clock.Now(), Limit: 64}
		for pages := 0; ; pages++ {
			if pages > 4096 {
				f.t.Fatalf("shard %d did not exhaust", shard)
			}
			page, err := f.store.ListDueGates(context.Background(), req)
			if err != nil {
				f.t.Fatalf("ListDueGates(shard %d): %v", shard, err)
			}
			examined += page.Examined
			for _, g := range page.Gates {
				gates = append(gates, g.Gate.GateID)
			}
			for _, r := range page.Remnants {
				remnants = append(remnants, r.GateID)
			}
			if page.NextCursor == "" {
				break
			}
			req = sessionstore.ListDueGatesRequest{Shard: shard, Cursor: page.NextCursor, Limit: 64}
		}
	}
	return gates, remnants, examined
}

// remnantsNow pages every shard to exhaustion and reports the retireable rows
// the store found, carrying the revision a retirement names.
func (f *gateFixture) remnantsNow() []sessionstore.RemnantGateIntent {
	f.t.Helper()
	var out []sessionstore.RemnantGateIntent
	for shard := range f.store.ControlShards() {
		req := sessionstore.ListDueGatesRequest{Shard: shard, DueAtOrBefore: f.clock.Now(), Limit: 64}
		for pages := 0; ; pages++ {
			if pages > 4096 {
				f.t.Fatalf("shard %d did not exhaust", shard)
			}
			page, err := f.store.ListDueGates(context.Background(), req)
			if err != nil {
				f.t.Fatalf("ListDueGates(shard %d): %v", shard, err)
			}
			out = append(out, page.Remnants...)
			if page.NextCursor == "" {
				break
			}
			req = sessionstore.ListDueGatesRequest{Shard: shard, Cursor: page.NextCursor, Limit: 64}
		}
	}
	return out
}

// sweepAll makes one pass over every shard and merges the results, which is
// what a replica driving this sweeper on a cadence does.
func (f *gateFixture) sweepAll() GateSweepResult {
	f.t.Helper()
	merged := GateSweepResult{Shard: -1, Dispositions: map[GateDisposition]int{}}
	for range f.store.ControlShards() {
		result := f.sweepOnce()
		merged.Pages += result.Pages
		merged.Examined += result.Examined
		merged.Unreadable += result.Unreadable
		merged.Remnants += result.Remnants
		merged.OpenPastDeadline += result.OpenPastDeadline
		merged.Retired += result.Retired
		merged.Queries += result.Queries
		merged.Truncated = merged.Truncated || result.Truncated
		merged.Resumed = merged.Resumed || result.Resumed
		for disposition, count := range result.Dispositions {
			merged.Dispositions[disposition] += count
		}
	}
	return merged
}

func (f *gateFixture) sweepOnce() GateSweepResult {
	f.t.Helper()
	result, err := f.sweeper.Sweep(context.Background(), servicePrincipal(f.t))
	if err != nil {
		f.t.Fatalf("Sweep: %v", err)
	}
	return result
}

// drain sweeps every shard repeatedly until no shard reports a continuation,
// which is what "page the whole view under a bounded pass" means for a caller.
func (f *gateFixture) drain(maxRounds int) GateSweepResult {
	f.t.Helper()
	merged := GateSweepResult{Shard: -1, Dispositions: map[GateDisposition]int{}}
	for round := 0; ; round++ {
		if round >= maxRounds {
			f.t.Fatalf("the view did not drain in %d rounds", maxRounds)
		}
		result := f.sweepAll()
		merged.Pages += result.Pages
		merged.Examined += result.Examined
		merged.Remnants += result.Remnants
		merged.OpenPastDeadline += result.OpenPastDeadline
		merged.Retired += result.Retired
		merged.Queries += result.Queries
		for disposition, count := range result.Dispositions {
			merged.Dispositions[disposition] += count
		}
		if !result.Truncated {
			return merged
		}
	}
}

// ---------------------------------------------------------------------------
// Configuration and authorization.
// ---------------------------------------------------------------------------

// countingDue reports a shard count and counts every page it was asked for, so
// a configuration test can require that a refused configuration never reached a
// store at all.
type countingDue struct {
	shards int
	pages  int
	inner  DueGates
}

func (d *countingDue) ControlShards() int { return d.shards }

func (d *countingDue) ListDueGates(
	ctx context.Context, req sessionstore.ListDueGatesRequest,
) (sessionstore.DueGatePage, error) {
	d.pages++
	if d.inner == nil {
		return sessionstore.DueGatePage{}, nil
	}
	return d.inner.ListDueGates(ctx, req)
}

type countingIntents struct {
	calls int
	inner GateIntents
	fail  error
}

func (i *countingIntents) RetireGateDeadlineIntent(
	ctx context.Context, req sessionstore.RetireGateDeadlineIntentRequest,
) error {
	i.calls++
	if i.fail != nil {
		return i.fail
	}
	if i.inner == nil {
		return nil
	}
	return i.inner.RetireGateDeadlineIntent(ctx, req)
}

func TestNewGateSweeperRefusesAnIncompleteConfigurationBeforeAnyStoreCall(t *testing.T) {
	due := &countingDue{shards: 4}
	intents := &countingIntents{}
	valid := GateSweeperConfig{
		Authorizer: &recordingAuthorizer{}, Due: due, Intents: intents,
		Clock: &movableClock{now: gateOrigin}, PageLimit: 8, MaxPages: 2,
	}
	if _, err := NewGateSweeper(valid); err != nil {
		t.Fatalf("the valid configuration was refused: %v", err)
	}
	for _, spoil := range []struct {
		name string
		with func(GateSweeperConfig) GateSweeperConfig
	}{
		{"no authorizer", func(c GateSweeperConfig) GateSweeperConfig { c.Authorizer = nil; return c }},
		{"no due seam", func(c GateSweeperConfig) GateSweeperConfig { c.Due = nil; return c }},
		{"no intent seam", func(c GateSweeperConfig) GateSweeperConfig { c.Intents = nil; return c }},
		{"no clock", func(c GateSweeperConfig) GateSweeperConfig { c.Clock = nil; return c }},
		{"zero page limit", func(c GateSweeperConfig) GateSweeperConfig { c.PageLimit = 0; return c }},
		{"negative page limit", func(c GateSweeperConfig) GateSweeperConfig { c.PageLimit = -1; return c }},
		{"page limit above the provider ceiling", func(c GateSweeperConfig) GateSweeperConfig {
			c.PageLimit = storage.MaxOrderedPageLimit + 1
			return c
		}},
		{"zero max pages", func(c GateSweeperConfig) GateSweeperConfig { c.MaxPages = 0; return c }},
		{"negative max pages", func(c GateSweeperConfig) GateSweeperConfig { c.MaxPages = -1; return c }},
	} {
		t.Run(spoil.name, func(t *testing.T) {
			sweeper, err := NewGateSweeper(spoil.with(valid))
			if err == nil {
				t.Fatalf("the configuration was accepted, sweeper = %v", sweeper)
			}
			if !errors.Is(err, ErrInvalidGateSweeperConfig) {
				t.Errorf("err = %v, want one wrapping ErrInvalidGateSweeperConfig", err)
			}
			if sweeper != nil {
				t.Errorf("a refused configuration returned a sweeper: %v", sweeper)
			}
		})
	}
	if due.pages != 0 || intents.calls != 0 {
		t.Errorf("a refused configuration reached the store: %d pages, %d retirements", due.pages, intents.calls)
	}
}

// TestTheSweepIsAuthorizedAsAServiceSweepAndARefusalStopsItBeforeAnyStoreCall
// is step 1's "with the service principal".
//
// The sweep is cross-tenant -- a shard holds whichever tenants hash into it --
// so the decision it needs is the service-sweep one and a tenant principal must
// never reach it. Both halves are measured: the principal REACHES the
// authorizer, and a refusal costs nothing downstream.
func TestTheSweepIsAuthorizedAsAServiceSweepAndARefusalStopsItBeforeAnyStoreCall(t *testing.T) {
	due := &countingDue{shards: 4}
	intents := &countingIntents{}
	auth := &recordingAuthorizer{}
	sweeper, err := NewGateSweeper(GateSweeperConfig{
		Authorizer: auth, Due: due, Intents: intents,
		Clock: &movableClock{now: gateOrigin}, PageLimit: 8, MaxPages: 2,
	})
	if err != nil {
		t.Fatalf("NewGateSweeper: %v", err)
	}
	principal := servicePrincipal(t)
	if _, err := sweeper.Sweep(context.Background(), principal); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if auth.calls != 1 {
		t.Fatalf("AuthorizeServiceSweep calls = %d, want 1", auth.calls)
	}
	if auth.principals[0] != principal {
		t.Errorf("the authorizer saw %v, want the principal the caller passed", auth.principals[0])
	}
	if due.pages != 1 {
		t.Errorf("an authorized sweep read %d pages, want 1", due.pages)
	}

	refused := errors.New("not a service principal")
	auth.refuse = refused
	before := due.pages
	result, err := sweeper.Sweep(context.Background(), principal)
	if !errors.Is(err, refused) {
		t.Fatalf("Sweep err = %v, want the authorizer's refusal", err)
	}
	if due.pages != before {
		t.Errorf("a refused sweep read %d pages, want none", due.pages-before)
	}
	if intents.calls != 0 {
		t.Errorf("a refused sweep retired %d intents, want none", intents.calls)
	}
	if !reflect.DeepEqual(result, GateSweepResult{}) {
		t.Errorf("a refused sweep returned %+v, want the zero result", result)
	}
}

// ---------------------------------------------------------------------------
// Step 2 -- the two ways a gate stops being open, and idempotence.
// ---------------------------------------------------------------------------

// TestAnOpenInterruptedBeforeItsProjectionIsRetired is step 2's first half:
// crash-before-open.
//
// OpenGate writes the deadline intent and THEN commits the open projection, so
// a process death between them leaves an intent whose gate is not projected
// open anywhere. The crash is injected at the provider, by refusing the catalog
// write that comes second, which leaves exactly that durable state.
func TestAnOpenInterruptedBeforeItsProjectionIsRetired(t *testing.T) {
	f := newGateFixture(t, 1, 8, 4)
	f.prepare(gateSession)

	f.hooks.failWrites("catalog")
	_, err := f.store.OpenGate(context.Background(), sessionstore.OpenGateRequest{
		TenantID: gateTenant, SessionID: gateSession, LeaseEpoch: f.epoch,
		Gate: gateProjection("gate-a", f.clock.Now().Add(time.Minute)),
	})
	if err == nil {
		t.Fatal("the injected crash did not stop OpenGate")
	}
	f.hooks.failWrites("")

	// The state the crash left: an intent, and no public gate.
	if gates := f.openGateIDs(gateSession); len(gates) != 0 {
		t.Fatalf("the interrupted open projected %v, want no public gate", gates)
	}
	f.clock.advance(sessionstore.MinGateIntentRemnantAge + time.Minute)
	if _, remnants, _ := f.dueNow(); len(remnants) != 1 || remnants[0] != "gate-a" {
		t.Fatalf("remnants before the sweep = %v, want [gate-a]", remnants)
	}

	result := f.sweepAll()
	if result.Remnants != 1 || result.Retired != 1 || result.Dispositions[GateDispositionRetired] != 1 {
		t.Fatalf("result = %+v, want one remnant retired", result)
	}
	if result.OpenPastDeadline != 0 {
		t.Errorf("OpenPastDeadline = %d, want 0: nothing was publicly open", result.OpenPastDeadline)
	}
	gates, remnants, _ := f.dueNow()
	if len(gates) != 0 || len(remnants) != 0 {
		t.Fatalf("after the sweep the due view holds gates %v and remnants %v, want neither", gates, remnants)
	}
}

// TestAResolveInterruptedBeforeItsCleanupIsRetired is step 2's second half: a
// gate that HAS a committed resolution, whose intent cleanup was lost.
//
// ResolveGate clears the projection and THEN retires the intent, so an
// interruption between them leaves the same shape the crashed open leaves. The
// two crash windows producing one remnant shape is the property this pair
// measures: one retirement path answers both, so there is no second sweep to
// write.
func TestAResolveInterruptedBeforeItsCleanupIsRetired(t *testing.T) {
	f := newGateFixture(t, 1, 8, 4)
	f.prepare(gateSession)
	f.open(gateSession, "gate-a", f.clock.Now().Add(time.Minute))

	f.hooks.failWrites("gates")
	if _, err := f.store.ResolveGate(context.Background(), sessionstore.ResolveGateRequest{
		TenantID: gateTenant, SessionID: gateSession, LeaseEpoch: f.epoch, GateID: "gate-a",
	}); err == nil {
		t.Fatal("the injected crash did not stop ResolveGate")
	}
	f.hooks.failWrites("")

	if gates := f.openGateIDs(gateSession); len(gates) != 0 {
		t.Fatalf("the interrupted resolve left %v projected open", gates)
	}
	f.clock.advance(sessionstore.MinGateIntentRemnantAge + time.Minute)
	if _, remnants, _ := f.dueNow(); len(remnants) != 1 {
		t.Fatalf("remnants before the sweep = %v, want one", remnants)
	}

	result := f.sweepAll()
	if result.Retired != 1 {
		t.Fatalf("result = %+v, want one retirement", result)
	}
	if _, remnants, _ := f.dueNow(); len(remnants) != 0 {
		t.Fatalf("after the sweep remnants = %v, want none", remnants)
	}
}

// TestRepeatingAValidatedRetirementChangesNothing is step 2's "idempotently",
// and the repeat is the ORDINARY case rather than a pathology: a sweeper cannot
// tell a lost reply from a failure, so it retries.
//
// The second pass is required to make the same decision from durable state,
// which is what "safe to repeat" means -- not that a repeat is rare. The probe
// is the intent row's own revision: a second write would move it.
func TestRepeatingAValidatedRetirementChangesNothing(t *testing.T) {
	f := newGateFixture(t, 1, 8, 4)
	f.prepare(gateSession)
	f.open(gateSession, "gate-a", f.clock.Now().Add(time.Minute))
	f.hostState(gateSession, nil) // drops the projection, leaves the intent
	f.clock.advance(sessionstore.MinGateIntentRemnantAge + time.Minute)

	remnants := f.remnantsNow()
	if len(remnants) != 1 {
		t.Fatalf("remnants before the sweep = %v, want one", remnants)
	}
	remnant := remnants[0]

	first := f.sweepAll()
	if first.Retired != 1 {
		t.Fatalf("first sweep = %+v, want one retirement", first)
	}
	writes := f.hooks.writeCount("gates")
	if writes == 0 {
		t.Fatalf("the first retirement wrote nothing, so the write probe cannot discriminate")
	}

	// The repeat presents the SAME remnant the first pass acted on, which is
	// exactly what a sweeper holding a page across a lost reply does. A retired
	// intent has left the due view, so sweeping again would not produce it.
	result := GateSweepResult{Dispositions: map[GateDisposition]int{}}
	if err := f.sweeper.retire(context.Background(), remnant, &result); err != nil {
		t.Fatalf("the repeat failed: %v", err)
	}
	if result.Retired != 1 || result.Dispositions[GateDispositionRetired] != 1 {
		t.Fatalf("the repeat reported %+v, want one retirement", result)
	}
	if after := f.hooks.writeCount("gates"); after != writes {
		t.Errorf("the repeat made %d gate writes, want none", after-writes)
	}

	// And a repeat under a revision the row never had is still the same answer,
	// because a tombstone is answered before any compare-and-swap.
	stale := remnant
	stale.Revision = remnant.Revision + 7
	again := GateSweepResult{Dispositions: map[GateDisposition]int{}}
	if err := f.sweeper.retire(context.Background(), stale, &again); err != nil {
		t.Fatalf("the stale-revision repeat failed: %v", err)
	}
	if again.Dispositions[GateDispositionRetired] != 1 {
		t.Errorf("the stale-revision repeat reported %+v, want a retirement", again)
	}
	if after := f.hooks.writeCount("gates"); after != writes {
		t.Errorf("the stale-revision repeat made %d gate writes, want none", after-writes)
	}
	if _, remnantsAfter, _ := f.dueNow(); len(remnantsAfter) != 0 {
		t.Errorf("the repeats left remnants %v", remnantsAfter)
	}
}

// ---------------------------------------------------------------------------
// Step 1 -- the persisted per-shard cursor.
//
// recordingDue is a DECORATOR over the real store, not a substitute for it:
// every answer below is the store's own. What it adds is the record of what was
// asked and what came back, which is the only way to observe a cursor this
// package deliberately cannot inspect.
// ---------------------------------------------------------------------------

type dueExchange struct {
	shard      int
	cursorIn   sessionwire.Cursor
	cursorOut  sessionwire.Cursor
	bound      time.Time
	remnants   int
	openGates  int
	failedWith error
}

type recordingDue struct {
	mu    sync.Mutex
	inner DueGates

	exchanges []dueExchange

	// corruptCursor replaces a nonempty continuation with a token the store
	// will refuse. The refusal is then the STORE's own, raised by its own
	// decoder, rather than an error this fixture invented.
	corruptCursor bool

	// afterPage runs once AFTER the store has returned a complete due page and
	// BEFORE the sweeper acts on it. That window is at the FACTORY seam rather
	// than the provider seam, and the difference is load-bearing: the store
	// joins each intent against the session's record inside ListDueGates, so a
	// write injected at the provider is still seen by the join and changes what
	// the page SAYS. Only a write after the page is complete leaves the sweeper
	// holding evidence the durable state has since contradicted.
	afterPage func()
	pageFired bool

	// shardsOverride makes the seam report a DIFFERENT shard count than the
	// store it decorates, which is how a backend re-shard is driven without
	// faking a single page: the pages remain the real store's.
	shardsOverride int

	// faultNext makes the next page fail with a store error that is about the
	// PAGE rather than about the position. It is the control for the refusal
	// above: the two must be told apart.
	faultNext error
}

func (d *recordingDue) ControlShards() int {
	d.mu.Lock()
	override := d.shardsOverride
	d.mu.Unlock()
	if override > 0 {
		return override
	}
	return d.inner.ControlShards()
}

func (d *recordingDue) ListDueGates(
	ctx context.Context, req sessionstore.ListDueGatesRequest,
) (sessionstore.DueGatePage, error) {
	d.mu.Lock()
	corrupt, fault := d.corruptCursor, d.faultNext
	d.faultNext = nil
	d.mu.Unlock()

	exchange := dueExchange{shard: req.Shard, cursorIn: req.Cursor, bound: req.DueAtOrBefore}
	if fault != nil {
		exchange.failedWith = fault
		d.note(exchange)
		return sessionstore.DueGatePage{}, fault
	}
	forwarded := req
	if corrupt && req.Cursor != "" {
		forwarded.Cursor = "not-a-cursor-this-store-ever-issued"
	}
	page, err := d.inner.ListDueGates(ctx, forwarded)
	d.mu.Lock()
	after, fired := d.afterPage, d.pageFired
	if after != nil && !fired {
		d.pageFired = true
	}
	d.mu.Unlock()
	if err == nil && after != nil && !fired {
		after()
	}
	exchange.failedWith = err
	exchange.cursorOut = page.NextCursor
	exchange.remnants = len(page.Remnants)
	exchange.openGates = len(page.Gates)
	d.note(exchange)
	return page, err
}

func (d *recordingDue) note(exchange dueExchange) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.exchanges = append(d.exchanges, exchange)
}

func (d *recordingDue) snapshot() []dueExchange {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]dueExchange(nil), d.exchanges...)
}

func newRecordingFixture(t *testing.T, shards, pageLimit, maxPages int) (*gateFixture, *recordingDue) {
	t.Helper()
	var recorder *recordingDue
	f := newGateFixtureWith(t, shards, pageLimit, maxPages, func(due DueGates) DueGates {
		recorder = &recordingDue{inner: due}
		return recorder
	})
	return f, recorder
}

// remnantOn leaves one session holding a deadline intent whose gate no session
// record projects as open, aged past the remnant window.
func (f *gateFixture) remnantOn(session sessionwire.SessionID, gate sessionwire.GateID) time.Time {
	f.t.Helper()
	f.prepare(session)
	deadline := f.clock.Now().Add(time.Minute)
	f.open(session, gate, deadline)
	f.hostState(session, nil)
	return deadline
}

// TestEachShardContinuesFromItsOwnPersistedPosition is step 1's "persisted
// per-shard cursor", and the per-SHARD half is the load-bearing one.
//
// A continuation is bound to the shard that issued it, so one cursor shared
// across a round-robin rotor is not a smaller version of the right thing: it is
// a token presented to a view that never issued it, refused on every pass after
// the first. The assertions are therefore two: a cursor presented to a shard is
// the one that shard's own previous page ended at, and no cursor ever crosses
// between shards.
func TestEachShardContinuesFromItsOwnPersistedPosition(t *testing.T) {
	f, recorder := newRecordingFixture(t, 8, 1, 1)
	// Enough sessions that several shards hold more than one row, which is what
	// makes a continuation exist at all.
	for i := range 24 {
		session := sessionwire.SessionID(fmt.Sprintf("session-%02d", i))
		f.prepare(session)
		f.open(session, sessionwire.GateID(fmt.Sprintf("gate-%02d", i)), f.clock.Now().Add(time.Minute))
	}
	f.clock.advance(sessionstore.MinGateIntentRemnantAge + time.Minute)

	// Every gate is still publicly OPEN, so nothing is retired and no row ever
	// leaves the view. That is what keeps the continuations alive across the
	// passes below rather than draining them away.
	for range 6 {
		f.sweepAll()
	}

	issued := map[sessionwire.Cursor]int{}
	lastOut := map[int]sessionwire.Cursor{}
	seen := map[int]int{}
	resumptions, crossings := 0, 0
	for _, exchange := range recorder.snapshot() {
		if exchange.cursorIn != "" {
			resumptions++
			if want := lastOut[exchange.shard]; exchange.cursorIn != want {
				t.Errorf("shard %d resumed from %q, want the position its own previous page ended at (%q)",
					exchange.shard, exchange.cursorIn, want)
			}
			if from, ok := issued[exchange.cursorIn]; ok && from != exchange.shard {
				crossings++
				t.Errorf("a cursor issued by shard %d was presented to shard %d", from, exchange.shard)
			}
			if !exchange.bound.IsZero() {
				t.Errorf("shard %d presented a cursor AND a bound (%v); the store refuses both",
					exchange.shard, exchange.bound)
			}
		} else if exchange.bound.IsZero() {
			t.Errorf("shard %d opened a pass with neither a cursor nor a bound", exchange.shard)
		}
		if exchange.cursorOut != "" {
			issued[exchange.cursorOut] = exchange.shard
		}
		lastOut[exchange.shard] = exchange.cursorOut
		seen[exchange.shard]++
	}
	if resumptions == 0 {
		t.Fatalf("no pass resumed a cursor, so the per-shard assertion is vacuous")
	}
	if len(seen) < 2 {
		t.Fatalf("only %d shard(s) were swept, so nothing could cross between them", len(seen))
	}
	if crossings != 0 {
		t.Errorf("%d cursors crossed shards", crossings)
	}
}

// TestAnExhaustedShardReArmsAtTheHeadAgainstAFreshBound is step 4's cursor
// wrap.
//
// A shard read to the end returns no continuation, and the position must be
// DROPPED rather than kept: the next pass has to open at the head against a NEW
// wall-clock bound, or a deadline that passed after the last pass would sit
// outside every bound the sweeper ever asks about again.
func TestAnExhaustedShardReArmsAtTheHeadAgainstAFreshBound(t *testing.T) {
	f, recorder := newRecordingFixture(t, 1, 8, 4)
	f.remnantOn("session-a", "gate-a")
	f.clock.advance(sessionstore.MinGateIntentRemnantAge + time.Minute)

	first := f.sweepOnce()
	if !first.Exhausted || first.Truncated || first.Resumed {
		t.Fatalf("first pass = %+v, want exhausted, untruncated and unresumed", first)
	}
	if first.Retired != 1 {
		t.Fatalf("first pass retired %d, want 1", first.Retired)
	}
	firstBound := recorder.snapshot()[0].bound

	// A gate whose deadline is AFTER every bound the first pass asked about.
	f.prepare("session-b")
	f.open("session-b", "gate-b", f.clock.Now().Add(time.Hour))
	f.hostState("session-b", nil)
	f.clock.advance(time.Hour + sessionstore.MinGateIntentRemnantAge)

	second := f.sweepOnce()
	exchanges := recorder.snapshot()
	reopened := exchanges[len(exchanges)-1]
	if reopened.cursorIn != "" {
		t.Errorf("the pass after exhaustion presented cursor %q, want the head", reopened.cursorIn)
	}
	if !reopened.bound.After(firstBound) {
		t.Errorf("the re-armed bound is %v, want one later than the first pass's %v", reopened.bound, firstBound)
	}
	if second.Resumed {
		t.Errorf("the pass after exhaustion reported Resumed")
	}
	if second.Retired != 1 {
		t.Fatalf("the pass after exhaustion retired %d, want the gate that became due since: %+v", second.Retired, second)
	}
	if _, remnants, _ := f.dueNow(); len(remnants) != 0 {
		t.Errorf("remnants remain after the wrap: %v", remnants)
	}
}

// TestARefusedContinuationReArmsTheShardInsteadOfWedgingIt covers the one page
// failure whose subject is the position itself.
//
// The position is what was refused, so keeping it would present it on every
// later pass and this shard would silently stop being swept for the lifetime of
// the replica. It is dropped, and the next pass opens at the head.
func TestARefusedContinuationReArmsTheShardInsteadOfWedgingIt(t *testing.T) {
	f, recorder := newRecordingFixture(t, 1, 1, 1)
	for i := range 3 {
		session := sessionwire.SessionID(fmt.Sprintf("open-%d", i))
		f.prepare(session)
		f.open(session, sessionwire.GateID(fmt.Sprintf("gate-%d", i)), f.clock.Now().Add(time.Minute))
	}
	f.clock.advance(sessionstore.MinGateIntentRemnantAge + time.Minute)

	if first := f.sweepOnce(); !first.Truncated || first.Exhausted {
		t.Fatalf("first pass = %+v, want a truncated, unexhausted pass holding a position", first)
	}
	recorder.mu.Lock()
	recorder.corruptCursor = true
	recorder.mu.Unlock()

	if _, err := f.sweeper.Sweep(context.Background(), servicePrincipal(t)); err == nil {
		t.Fatal("the refused continuation did not surface as an error")
	} else {
		var catalog *sessionstore.CatalogError
		if !errors.As(err, &catalog) || catalog.Code != sessionstore.CatalogErrorCursor {
			t.Fatalf("err = %v, want the store's cursor refusal", err)
		}
	}
	recorder.mu.Lock()
	recorder.corruptCursor = false
	recorder.mu.Unlock()

	third := f.sweepOnce()
	if third.Resumed {
		t.Errorf("the pass after the refusal resumed, so the refused position was kept")
	}
	last := recorder.snapshot()
	if got := last[len(last)-1]; got.cursorIn != "" {
		t.Errorf("the pass after the refusal presented %q, want the head", got.cursorIn)
	}
	if third.Pages != 1 {
		t.Errorf("the pass after the refusal read %d pages, want one: the shard is wedged", third.Pages)
	}
}

// TestAPageFaultKeepsThePositionTheSweepHadReached is the control for the test
// above.
//
// A sweeper that dropped its position on EVERY failure would pass that test
// while making a transient provider fault cost a full restart over every row it
// had already walked. The distinction must be made, so a fault that is about
// the page rather than the position must LEAVE the position in place.
func TestAPageFaultKeepsThePositionTheSweepHadReached(t *testing.T) {
	f, recorder := newRecordingFixture(t, 1, 1, 1)
	for i := range 3 {
		session := sessionwire.SessionID(fmt.Sprintf("open-%d", i))
		f.prepare(session)
		f.open(session, sessionwire.GateID(fmt.Sprintf("gate-%d", i)), f.clock.Now().Add(time.Minute))
	}
	f.clock.advance(sessionstore.MinGateIntentRemnantAge + time.Minute)

	if first := f.sweepOnce(); !first.Truncated || first.Exhausted {
		t.Fatalf("first pass = %+v, want a truncated, unexhausted pass holding a position", first)
	}
	reached := recorder.snapshot()[0].cursorOut
	if reached == "" {
		t.Fatal("the first pass reached no position, so there is nothing for a fault to keep")
	}

	fault := &sessionstore.CatalogError{Code: sessionstore.CatalogErrorBackend, Field: "due_gates"}
	recorder.mu.Lock()
	recorder.faultNext = fault
	recorder.mu.Unlock()
	if _, err := f.sweeper.Sweep(context.Background(), servicePrincipal(t)); !errors.Is(err, error(fault)) {
		t.Fatalf("err = %v, want the injected page fault", err)
	}

	third := f.sweepOnce()
	if !third.Resumed {
		t.Errorf("the pass after a page fault did not resume, so the position was discarded")
	}
	exchanges := recorder.snapshot()
	if got := exchanges[len(exchanges)-1].cursorIn; got != reached {
		t.Errorf("the pass after a page fault presented %q, want the position the first pass reached (%q)", got, reached)
	}
}

// ---------------------------------------------------------------------------
// Step 2 -- what a refused retirement means.
//
// Every case below drives the REAL store's own refusal. Nothing here scripts an
// answer: the classification is this package's, and a fixture that invented the
// error would be testing the switch against itself.
// ---------------------------------------------------------------------------

// TestAnIntentInsideTheRemnantWindowIsLeftDueRatherThanRetired holds the one
// refusal that is about TIME rather than about state.
//
// Inside sessionstore.MinGateIntentRemnantAge a remnant and an OpenGate still in
// flight are the same bytes, so the store refuses. Retiring anyway would
// tombstone the deadline of a gate that is about to become publicly open, under
// an identity that can never be reused. The row stays due and a later pass
// takes it.
func TestAnIntentInsideTheRemnantWindowIsLeftDueRatherThanRetired(t *testing.T) {
	f := newGateFixture(t, 1, 8, 4)
	f.prepare(gateSession)
	f.open(gateSession, "gate-a", f.clock.Now().Add(10*time.Second))
	f.hostState(gateSession, nil)
	// Past the DEADLINE, so the row is due, but short of the remnant window.
	f.clock.advance(time.Minute)

	early := f.sweepAll()
	if early.Remnants != 1 {
		t.Fatalf("the row was not reported as a remnant: %+v", early)
	}
	if early.Retired != 0 || early.Dispositions[GateDispositionTooSoon] != 1 {
		t.Fatalf("result = %+v, want one too_soon and no retirement", early)
	}
	if _, remnants, _ := f.dueNow(); len(remnants) != 1 {
		t.Fatalf("the refused row left the due view: %v", remnants)
	}

	// The control: the same fixture, the same sweeper, past the window.
	f.clock.advance(sessionstore.MinGateIntentRemnantAge)
	late := f.sweepAll()
	if late.Retired != 1 || late.Dispositions[GateDispositionTooSoon] != 0 {
		t.Fatalf("after the window result = %+v, want one retirement and no too_soon", late)
	}
	if _, remnants, _ := f.dueNow(); len(remnants) != 0 {
		t.Errorf("remnants after the window: %v", remnants)
	}
}

// TestAGateProjectedOpenBetweenThePageAndTheWriteIsNotRetired is the race the
// store's independent re-read exists for, driven at the instant it is about.
//
// A due page is weakly consistent evidence: it says "no record projected this
// gate when I looked". Between that look and the compare-and-swap a Host can
// re-project the gate open -- UpdateCatalogHostState replaces the open-gate set
// wholesale and deliberately does not touch intents, so it can do exactly that
// while leaving the intent's recorded age alone. Retiring then would leave a
// public gate with no deadline in any view.
func TestAGateProjectedOpenBetweenThePageAndTheWriteIsNotRetired(t *testing.T) {
	f, recorder := newRecordingFixture(t, 1, 8, 4)
	deadline := f.remnantOn(gateSession, "gate-a")
	f.clock.advance(sessionstore.MinGateIntentRemnantAge + time.Minute)

	recorder.mu.Lock()
	recorder.afterPage = func() {
		// The window: the page is in the sweeper's hand and the record is still
		// writable by anyone. The projection re-written here is the one the
		// intent indexes -- same identity, same opening event, same deadline --
		// because a due page joins the two on all three, so a re-open that
		// disagreed about the deadline would still read as a remnant and would
		// not be the race this test is about.
		f.hostState(gateSession, []sessionwire.GateProjection{gateProjection("gate-a", deadline)})
	}
	recorder.mu.Unlock()

	result := f.sweepAll()
	if !recorder.pageFired {
		t.Fatal("the re-open never fired, so the race was never injected")
	}
	if result.Retired != 0 {
		t.Fatalf("a gate re-projected open was retired: %+v", result)
	}
	if result.Dispositions[GateDispositionReopened] != 1 {
		t.Fatalf("result = %+v, want one reopened", result)
	}
	if gates := f.openGateIDs(gateSession); len(gates) != 1 || gates[0] != "gate-a" {
		t.Errorf("open gates after the sweep = %v, want [gate-a]", gates)
	}
	// And the deadline survived: the gate is in the due view as an OPEN gate,
	// which is the state step 3 then governs.
	gates, remnants, _ := f.dueNow()
	if len(gates) != 1 || gates[0] != "gate-a" {
		t.Errorf("due gates = %v, want [gate-a]: the deadline was tombstoned", gates)
	}
	if len(remnants) != 0 {
		t.Errorf("due remnants = %v, want none", remnants)
	}
}

// TestAnIntentThatMovedBetweenThePageAndTheWriteIsReportedRaceLost is the OTHER
// way the page's evidence goes stale, and it is a different fact from the one
// above.
//
// Here nothing reopened the gate; the row itself moved, so the compare-and-swap
// is aimed at bytes that no longer exist. A caller reads the two differently --
// one means the gate is live, the other means re-read -- so folding them would
// report a live gate as a lost race and a lost race as a live gate.
func TestAnIntentThatMovedBetweenThePageAndTheWriteIsReportedRaceLost(t *testing.T) {
	f := newGateFixture(t, 1, 8, 4)
	if deadline := f.remnantOn(gateSession, "gate-a"); deadline.IsZero() {
		t.Fatal("the fixture recorded no deadline")
	}
	f.clock.advance(sessionstore.MinGateIntentRemnantAge + time.Minute)

	moved := 0
	f.hooks.onceAfterListDue(func(page storage.DuePage) {
		for _, record := range page.Records {
			if !strings.Contains(record.ID.Namespace, "gates") {
				continue
			}
			// Rewrite the row with its OWN bytes and its own due state: the
			// revision advances and nothing else about it changes, so the only
			// thing that can refuse the retirement is the compare-and-swap.
			if _, err := f.hooks.inner.Update(
				context.Background(), record.ID, record.Revision, record.Value, record.Rank, record.Due,
			); err != nil {
				t.Errorf("moving the row: %v", err)
				return
			}
			moved++
		}
	})

	result := f.sweepAll()
	if moved != 1 {
		t.Fatalf("the fixture moved %d rows, want exactly 1", moved)
	}
	if result.Retired != 0 || result.Dispositions[GateDispositionRaceLost] != 1 {
		t.Fatalf("result = %+v, want one race_lost and no retirement", result)
	}
	if result.Dispositions[GateDispositionReopened] != 0 {
		t.Errorf("a moved row was reported as a reopened gate: %+v", result)
	}
	// The row is still a remnant and still due, so the next pass takes it.
	if _, remnants, _ := f.dueNow(); len(remnants) != 1 {
		t.Fatalf("remnants after the lost race = %v, want the row still there", remnants)
	}
	if next := f.sweepAll(); next.Retired != 1 {
		t.Errorf("the pass after the lost race retired %d, want 1: %+v", next.Retired, next)
	}
}

// TestAnAbsentIntentIsReportedRatherThanCountedAsRetired holds the distinction
// SessionStore draws and this package must not erase.
//
// SessionStore never erases, so a retired intent has a durable spelling and it
// is a TOMBSTONE, which answers success. An ABSENT row is one the store has
// never held, and counting it as retired would tell an operator the sweep had
// handled a row it never touched.
func TestAnAbsentIntentIsReportedRatherThanCountedAsRetired(t *testing.T) {
	f := newGateFixture(t, 1, 8, 4)
	f.prepare(gateSession)

	result := GateSweepResult{Dispositions: map[GateDisposition]int{}}
	absent := sessionstore.RemnantGateIntent{
		TenantID: gateTenant, SessionID: gateSession, GateID: "gate-never-opened", Revision: 1,
	}
	if err := f.sweeper.retire(context.Background(), absent, &result); err != nil {
		t.Fatalf("retiring an absent intent failed: %v", err)
	}
	if result.Retired != 0 || result.Dispositions[GateDispositionAbsent] != 1 {
		t.Fatalf("result = %+v, want one absent and no retirement", result)
	}

	// The control, in the same fixture with the same helper: a row that DOES
	// exist and has been tombstoned answers retired, so the two are being told
	// apart rather than one answer given always.
	f.open(gateSession, "gate-a", f.clock.Now().Add(time.Minute))
	f.hostState(gateSession, nil)
	f.clock.advance(sessionstore.MinGateIntentRemnantAge + time.Minute)
	remnants := f.remnantsNow()
	if len(remnants) != 1 {
		t.Fatalf("remnants = %v, want one", remnants)
	}
	present := GateSweepResult{Dispositions: map[GateDisposition]int{}}
	if err := f.sweeper.retire(context.Background(), remnants[0], &present); err != nil {
		t.Fatalf("retiring a real remnant failed: %v", err)
	}
	if present.Retired != 1 {
		t.Fatalf("a real remnant reported %+v, want a retirement", present)
	}
	repeat := GateSweepResult{Dispositions: map[GateDisposition]int{}}
	if err := f.sweeper.retire(context.Background(), remnants[0], &repeat); err != nil {
		t.Fatalf("repeating a real retirement failed: %v", err)
	}
	if repeat.Dispositions[GateDispositionRetired] != 1 {
		t.Errorf("a tombstone reported %+v, want a retirement rather than absence", repeat)
	}
}

// TestAnUnclassifiableRetirementFailureEndsThePassAndReportsTheWorkItDid is
// what happens to an answer this package has no vocabulary for.
//
// A provider fault, an unreadable record or a hash collision each say NOTHING
// about whether the gate is open. Continuing past one would report a clean
// sweep over rows nothing had decided, so the pass ends -- and it ends carrying
// the counts it had already accrued, because "the sweep failed" and "the sweep
// did nothing" are different facts.
func TestAnUnclassifiableRetirementFailureEndsThePassAndReportsTheWorkItDid(t *testing.T) {
	f := newGateFixture(t, 1, 8, 4)
	for i := range 3 {
		f.remnantOn(sessionwire.SessionID(fmt.Sprintf("session-%d", i)), sessionwire.GateID(fmt.Sprintf("gate-%d", i)))
	}
	f.clock.advance(sessionstore.MinGateIntentRemnantAge + time.Minute)

	// The fault lands inside RetireGateDeadlineIntent's own read of the intent
	// row, so the error this package classifies is the store's own.
	f.hooks.failReads("gates")

	var failed GateSweepResult
	var err error
	for range f.store.ControlShards() {
		failed, err = f.sweeper.Sweep(context.Background(), servicePrincipal(t))
		if err != nil {
			break
		}
	}
	if err == nil {
		t.Fatal("the injected provider fault did not end the pass")
	}
	if !errors.Is(err, errInjectedProvider) {
		t.Errorf("err = %v, want one wrapping the injected fault", err)
	}
	if failed.Pages == 0 {
		t.Errorf("the failed pass reported no pages, so the work it did was discarded: %+v", failed)
	}
	if failed.Remnants == 0 {
		t.Errorf("the failed pass reported no remnants: %+v", failed)
	}
	f.hooks.failReads("")

	// Nothing was silently skipped: the rows are all still there, and a pass
	// with no fault takes every one of them.
	if _, remnants, _ := f.dueNow(); len(remnants) != 3 {
		t.Fatalf("remnants after the fault = %v, want all three", remnants)
	}
	if clean := f.drain(8); clean.Retired != 3 {
		t.Errorf("the clean drain retired %d, want 3: %+v", clean.Retired, clean)
	}
}

// ---------------------------------------------------------------------------
// Step 3 -- a gate that is still publicly open is LEFT open and LEFT due.
//
// "The sweep did not resolve the gate" is the shape of assertion that cannot
// fail if the fixture never made a resolution reachable, so it is not asserted
// on its own anywhere below. It is asserted through a probe set that is run
// TWICE -- once over the sweep and once over a real resolution -- and required
// to give opposite verdicts.
// ---------------------------------------------------------------------------

// openGateProbe is everything about a still-open gate that a resolution would
// disturb.
//
// It deliberately watches MORE THAN ONE OBJECT, because resolving a gate is not
// one write: it clears the projection in the session's catalog record AND
// tombstones the deadline intent in the ordered index, and a further "answer"
// would admit a command in a third place. A probe over the catalog alone would
// pass while the deadline was destroyed. The total provider write count is
// carried for the same reason from the other end: it is the one observation
// that is not an enumeration of objects and so cannot miss one this fixture did
// not think of.
type openGateProbe struct {
	projections     []sessionwire.GateProjection
	catalogRevision uint64
	dueAsOpenGate   []sessionwire.GateID
	dueAsRemnant    []sessionwire.GateID
	providerWrites  int
}

func (f *gateFixture) probe(session sessionwire.SessionID) openGateProbe {
	f.t.Helper()
	page, err := f.store.ReadGates(context.Background(), sessionstore.ReadGatesRequest{
		TenantID: gateTenant, SessionID: session,
	})
	if err != nil {
		f.t.Fatalf("ReadGates(%s): %v", session, err)
	}
	gates, remnants, _ := f.dueNow()
	return openGateProbe{
		projections:     page.Gates,
		catalogRevision: f.catalogRevision(session),
		dueAsOpenGate:   gates,
		dueAsRemnant:    remnants,
		providerWrites:  f.hooks.writeCount(""),
	}
}

// differences names every member of the probe that moved. It is what makes the
// control below a control: the same comparison must report NOTHING for the
// sweep and SOMETHING for each watched object under a real resolution.
func (p openGateProbe) differences(other openGateProbe) []string {
	var moved []string
	if !reflect.DeepEqual(p.projections, other.projections) {
		moved = append(moved, "projection")
	}
	if p.catalogRevision != other.catalogRevision {
		moved = append(moved, "catalog_revision")
	}
	if !reflect.DeepEqual(p.dueAsOpenGate, other.dueAsOpenGate) {
		moved = append(moved, "due_open_gates")
	}
	if !reflect.DeepEqual(p.dueAsRemnant, other.dueAsRemnant) {
		moved = append(moved, "due_remnants")
	}
	if p.providerWrites != other.providerWrites {
		moved = append(moved, "provider_writes")
	}
	return moved
}

// stillOpenFixture is a session holding one gate that is publicly open and past
// its deadline: the exact state step 3 governs.
func stillOpenFixture(t *testing.T) *gateFixture {
	t.Helper()
	f := newGateFixture(t, 1, 8, 4)
	f.prepare(gateSession)
	f.open(gateSession, "gate-a", f.clock.Now().Add(time.Minute))
	f.clock.advance(sessionstore.MinGateIntentRemnantAge + time.Hour)
	if gates, remnants, _ := f.dueNow(); len(gates) != 1 || len(remnants) != 0 {
		t.Fatalf("the fixture is not a still-open due gate: gates %v, remnants %v", gates, remnants)
	}
	return f
}

// TestAGateStillPubliclyOpenIsLeftOpenAndLeftDue is step 3's behaviour.
//
// The gate is past its deadline and nobody has dealt with it, which is exactly
// the state that invites a sweeper to do something about it. It may not. Gate
// continuation -- deciding what happens when a gate expires -- belongs to the
// Host and is a later task; what this pass owes is to count the row, leave both
// of its durable objects alone, and move past it.
func TestAGateStillPubliclyOpenIsLeftOpenAndLeftDue(t *testing.T) {
	f := stillOpenFixture(t)
	before := f.probe(gateSession)

	result := f.sweepAll()
	if result.OpenPastDeadline != 1 {
		t.Fatalf("result = %+v, want the open gate counted once", result)
	}
	if result.Retired != 0 || result.Remnants != 0 || len(result.Dispositions) != 0 {
		t.Fatalf("result = %+v, want no retirement and no disposition at all", result)
	}
	if moved := before.differences(f.probe(gateSession)); len(moved) != 0 {
		t.Fatalf("the sweep moved %v; step 3 permits none of them", moved)
	}
	// And the gate is STILL DUE, which is the half a sweeper could satisfy by
	// quietly dropping the deadline instead of acting on it.
	gates, remnants, _ := f.dueNow()
	if len(gates) != 1 || gates[0] != "gate-a" || len(remnants) != 0 {
		t.Fatalf("after the sweep the due view holds gates %v and remnants %v, want the gate still due", gates, remnants)
	}
	if page := f.openGateIDs(gateSession); len(page) != 1 || page[0] != "gate-a" {
		t.Errorf("open gates after the sweep = %v, want [gate-a]", page)
	}
}

// TestTheStillOpenProbeSeesEveryObjectAResolutionDisturbs is the POSITIVE
// CONTROL for the test above, and it is the reason that test is evidence.
//
// A probe set that could not detect a resolution would report "the sweep
// resolved nothing" for a sweeper that resolved everything, and that is the
// defect shape this program keeps paying for. So the identical fixture, the
// identical probe and the identical comparison are run over a REAL
// ResolveGate, and every watched object is required to move. A probe that
// stopped discriminating -- because a member was dropped, because DeepEqual
// compared two nils, because the write counter stopped counting -- fails here
// even though the sweeper is untouched.
func TestTheStillOpenProbeSeesEveryObjectAResolutionDisturbs(t *testing.T) {
	f := stillOpenFixture(t)
	before := f.probe(gateSession)

	if _, err := f.store.ResolveGate(context.Background(), sessionstore.ResolveGateRequest{
		TenantID: gateTenant, SessionID: gateSession, LeaseEpoch: f.epoch, GateID: "gate-a",
	}); err != nil {
		t.Fatalf("ResolveGate: %v", err)
	}

	moved := f.probe(gateSession).differences(before)
	for _, want := range []string{"projection", "catalog_revision", "due_open_gates", "provider_writes"} {
		if !strings.Contains(strings.Join(moved, ","), want) {
			t.Errorf("a real resolution did not move %q; the probe cannot see that object", want)
		}
	}
	// The deadline intent is the SECOND object, and it is the one a probe over
	// the catalog alone would miss: the gate leaves the due view entirely.
	gates, remnants, _ := f.dueNow()
	if len(gates) != 0 || len(remnants) != 0 {
		t.Errorf("after a real resolution the due view holds gates %v and remnants %v, want neither", gates, remnants)
	}
}

// TestAStillOpenGateDoesNotStarveTheRecordsBehindIt is step 3's second half.
//
// A due view is ordered by deadline ASCENDING and a still-open gate never
// leaves it, so a sweeper that restarted at the head every pass would meet the
// same gate forever and the remnants behind it would never be retired. That is
// permanent head-of-line blocking rather than a slow sweep, and the persisted
// continuation is what turns it into a transient one.
//
// The budget here is one row per pass, so the blocking gate consumes a whole
// pass by itself. Progress past it is therefore only possible if the position
// survived the pass.
func TestAStillOpenGateDoesNotStarveTheRecordsBehindIt(t *testing.T) {
	f := newGateFixture(t, 1, 1, 1)
	// Two open gates at the HEAD of the view: earlier deadlines than the
	// remnant, and nothing this sweeper may do about either.
	for i := range 2 {
		session := sessionwire.SessionID(fmt.Sprintf("open-%d", i))
		f.prepare(session)
		f.open(session, sessionwire.GateID(fmt.Sprintf("open-gate-%d", i)),
			f.clock.Now().Add(time.Duration(i+1)*time.Minute))
	}
	// One remnant BEHIND them.
	f.prepare("stale")
	f.open("stale", "stale-gate", f.clock.Now().Add(time.Hour))
	f.hostState("stale", nil)
	f.clock.advance(time.Hour + sessionstore.MinGateIntentRemnantAge + time.Minute)

	first := f.sweepOnce()
	if first.OpenPastDeadline != 1 || first.Retired != 0 {
		t.Fatalf("first pass = %+v, want the head's open gate and no retirement", first)
	}
	if !first.Truncated || first.Exhausted {
		t.Fatalf("first pass = %+v, want a truncated, unexhausted pass holding a position", first)
	}

	retired, passes := 0, 1
	for ; passes < 8 && retired == 0; passes++ {
		result := f.sweepOnce()
		retired += result.Retired
	}
	if retired != 1 {
		t.Fatalf("the remnant behind the open gates was never retired in %d passes", passes)
	}
	if passes > 4 {
		t.Errorf("the remnant took %d passes to reach, want the bounded walk past two blocking rows", passes)
	}
	// The two open gates are untouched and still due, which is what makes this
	// "walked past" rather than "drained".
	gates, remnants, _ := f.dueNow()
	if len(gates) != 2 {
		t.Errorf("due open gates after the walk = %v, want both still there", gates)
	}
	if len(remnants) != 0 {
		t.Errorf("remnants after the walk = %v, want none", remnants)
	}
}

// ---------------------------------------------------------------------------
// Step 3 made structural.
//
// The behavioural tests above show that this sweeper DID NOT resolve a gate in
// the fixtures they build. The guards below are about what it COULD do, which
// is the part a fixture can never reach: a capability that is present but
// unexercised is exactly what ships.
//
// There are two of them because the boundary has two objects. A capability can
// arrive through a SEAM this package declares, or through an IMPORT of a
// package that already has it -- and a guard over either one alone is not a
// guard over the boundary. Host O7.1 shipped a cross-tenant bypass past a guard
// that watched one object of a two-object invariant; this is the same shape.
// ---------------------------------------------------------------------------

// forbiddenResolutionTypes are the request and result shapes of the operations
// step 3 names. A seam that named one of them would have the capability
// whatever its methods were called.
var forbiddenResolutionTypes = map[string]bool{
	"github.com/looprig/sessionstore.ResolveGateRequest":            true,
	"github.com/looprig/sessionstore.OpenGateRequest":               true,
	"github.com/looprig/sessionstore.AdmitCommandRequest":           true,
	"github.com/looprig/sessionstore.RejectCommandRequest":          true,
	"github.com/looprig/sessionstore.ClaimCommandRequest":           true,
	"github.com/looprig/sessionstore.CompleteCommandRequest":        true,
	"github.com/looprig/sessionstore.InboxEntry":                    true,
	"github.com/looprig/sessionstore.CatalogEntry":                  true,
	"github.com/looprig/sessionstore.UpdateCatalogHostStateRequest": true,
	"github.com/looprig/core/sessionwire/v1.GateResponseRequest":    true,
}

// forbiddenResolutionWords are the verbs step 3 forbids, matched as WHOLE
// words rather than substrings.
//
// The substring form was tried and is wrong in both directions here:
// "response" contains no forbidden word but "Respond" does, while "Remnants"
// contains no verb at all and "RetireGateDeadlineIntent" would match a
// substring ban on "tire". Whole words also keep the one legitimate
// near-miss readable -- GateProjection.Answerability is DATA this sweeper reads
// off a page and never acts on, and banning it would ban reading the page.
var forbiddenResolutionWords = map[string]bool{
	"answer": true, "answers": true, "answered": true,
	"deny": true, "denied": true,
	"resolve": true, "resolves": true, "resolved": true, "resolution": true,
	"suspend": true, "suspends": true, "suspended": true,
	"restore": true, "restores": true, "restored": true,
	"respond": true, "respond_": true,
	"admit": true, "admits": true, "admitted": true,
	"reject": true, "rejects": true, "rejected": true,
	"settle": true, "settles": true, "settled": true,
	"interrupt": true, "interrupts": true,
}

// camelWords splits an identifier on upper-case runes.
//
// Its residue, stated as a residue rather than as a boundary: it misses an
// identifier spelled in one case throughout ("resolvegate", "RESOLVEGATE"), it
// keeps an initialism run together as one word ("HTTPResolve" yields
// "httpresolve"), and it does not separate words joined by an underscore or a
// digit. THIS LIST ENUMERATES THE SPELLINGS THAT WERE THOUGHT OF AND IS NOT A
// CLOSURE. What limits the damage is that Go's exported-member convention
// produces the covered spelling, and that the method-set pins below do not
// depend on the word split at all.
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

func forbiddenResolutionWord(identifier string) (string, bool) {
	for _, word := range camelWords(identifier) {
		if forbiddenResolutionWords[word] {
			return word, true
		}
	}
	return "", false
}

// resolutionReach walks a type and reports every forbidden type and every
// forbidden verb it can reach.
//
// It is REFLECTION over compiled types rather than a scan of source text, which
// is what makes it closed against spelling: a member is either in the type or
// it is not. Its one real limit is that it sees the DECLARED surface -- a
// method taking `any`, or a field whose dynamic value is a store, is invisible
// to any static walk -- which is why the method sets are pinned exactly below
// and why the import guard exists beside it.
func resolutionReach(t reflect.Type, seen map[reflect.Type]bool, path string) []string {
	if t == nil || seen[t] {
		return nil
	}
	seen[t] = true

	var found []string
	if pkg := t.PkgPath(); pkg != "" && t.Name() != "" {
		if qualified := pkg + "." + t.Name(); forbiddenResolutionTypes[qualified] {
			found = append(found, path+": type "+qualified)
		}
	}
	if word, ok := forbiddenResolutionWord(t.Name()); ok {
		found = append(found, path+": type name "+t.Name()+" ("+word+")")
	}

	switch t.Kind() {
	case reflect.Struct:
		for i := range t.NumField() {
			field := t.Field(i)
			if word, ok := forbiddenResolutionWord(field.Name); ok {
				found = append(found, path+"."+field.Name+": field name ("+word+")")
			}
			found = append(found, resolutionReach(field.Type, seen, path+"."+field.Name)...)
		}
	case reflect.Interface:
		for i := range t.NumMethod() {
			method := t.Method(i)
			if word, ok := forbiddenResolutionWord(method.Name); ok {
				found = append(found, path+"."+method.Name+": method name ("+word+")")
			}
			found = append(found, resolutionReach(method.Type, seen, path+"."+method.Name)...)
		}
	case reflect.Func:
		for i := range t.NumIn() {
			found = append(found, resolutionReach(t.In(i), seen, fmt.Sprintf("%s(in %d)", path, i))...)
		}
		for i := range t.NumOut() {
			found = append(found, resolutionReach(t.Out(i), seen, fmt.Sprintf("%s(out %d)", path, i))...)
		}
	case reflect.Pointer, reflect.Slice, reflect.Array, reflect.Map:
		found = append(found, resolutionReach(t.Elem(), seen, path+"[]")...)
	}
	return found
}

// TestTheGateSweepReachesNoGateResolutionCapability is step 3's first guard.
//
// The whole declared surface of this sweeper is walked and required to reach no
// operation that could answer, deny, suspend, restore or resolve a gate. Then
// EVERY seam's method set is pinned exactly -- not the one that looks most
// dangerous -- because the walk sees only what a type declares: a seam that
// grew ResolveGate would be reachable with a name the word list happens not to
// hold, and a seam that grew AdmitGateResponse on the AUTHORIZER rather than on
// the store would be the same capability arriving through the object nobody was
// watching.
func TestTheGateSweepReachesNoGateResolutionCapability(t *testing.T) {
	for _, surface := range []any{
		GateSweeperConfig{}, GateSweepResult{}, GateSweeper{}, GateDisposition(0),
	} {
		typ := reflect.TypeOf(surface)
		if found := resolutionReach(typ, map[reflect.Type]bool{}, typ.Name()); len(found) > 0 {
			t.Errorf("%s reaches gate resolution capability: %v", typ.Name(), found)
		}
	}

	for _, seam := range []struct {
		typ  reflect.Type
		want []string
	}{
		{reflect.TypeOf((*DueGates)(nil)).Elem(), []string{"ControlShards", "ListDueGates"}},
		{reflect.TypeOf((*GateIntents)(nil)).Elem(), []string{"RetireGateDeadlineIntent"}},
		{reflect.TypeOf((*SweepAuthorizer)(nil)).Elem(), []string{"AuthorizeServiceSweep"}},
		{reflect.TypeOf((*Clock)(nil)).Elem(), []string{"Now"}},
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

// resolvingSeam and resolvingConfig exist ONLY as the positive control's
// subject. They are the shape this package would have if step 3 were violated
// the obvious way.
type resolvingSeam interface {
	ResolveGate(context.Context, sessionstore.ResolveGateRequest) (sessionstore.CatalogEntry, error)
}

type resolvingConfig struct {
	Due       DueGates
	Resolver  resolvingSeam
	Responses []sessionwire.GateResponseRequest
}

// TestTheResolutionWalkSeesResolutionCapabilityWhereItIsPresent is the positive
// control for the guard above.
//
// A walk that returned nothing because it was STUCK -- a qualified name spelled
// the way the import is aliased rather than the way the package path reads, a
// recursion that never descended, an empty word list -- would report this
// sweeper clean and a sweeper that resolved every gate equally clean. So the
// same walk is pointed at a type that really does carry the capability and is
// required to find it three separate ways: by forbidden type, by method name
// and by field name.
func TestTheResolutionWalkSeesResolutionCapabilityWhereItIsPresent(t *testing.T) {
	typ := reflect.TypeOf(resolvingConfig{})
	found := resolutionReach(typ, map[reflect.Type]bool{}, typ.Name())
	if len(found) == 0 {
		t.Fatal("the walk found nothing in a config that declares a ResolveGate seam")
	}
	joined := strings.Join(found, "\n")
	for _, want := range []struct{ what, needle string }{
		{"the request type", "sessionstore.ResolveGateRequest"},
		{"the wire response type", "sessionwire/v1.GateResponseRequest"},
		{"the method name", "ResolveGate: method name"},
		{"the request type name", "type name ResolveGateRequest"},
		{"the result type", "sessionstore.CatalogEntry"},
	} {
		if !strings.Contains(joined, want.needle) {
			t.Errorf("the walk did not report %s (%q); it found:\n%s", want.what, want.needle, joined)
		}
	}
	// THE RESIDUE, MEASURED RATHER THAN ASSERTED IN PROSE. camelWords splits on
	// upper-case runes, so an identifier whose forbidden word is not
	// capitalised is invisible to the NAME half of the walk. The interface here
	// is called resolvingSeam and the walk does NOT report its type name, which
	// is recorded as a fact rather than described as a limitation. It is caught
	// anyway, four other ways, and that is the argument for the pair of guards
	// rather than for the word list.
	if _, ok := forbiddenResolutionWord("resolvingSeam"); ok {
		t.Error("camelWords now catches a lower-case leading word; the residue note above is stale and must be rewritten")
	}
	if strings.Contains(joined, "type name resolvingSeam") {
		t.Error("the walk reported a name the word split cannot see; the residue note above is stale")
	}

	// And the walk must be able to find the capability from the ANSWER side
	// too, not only from "resolve": the runbook names five verbs.
	for _, identifier := range []string{"AdmitGateResponse", "DenyGate", "SuspendSession", "RestoreSession", "RespondToGate"} {
		if _, ok := forbiddenResolutionWord(identifier); !ok {
			t.Errorf("the word list does not catch %q, which is one of step 3's named verbs", identifier)
		}
	}
}

// TestTheGateSweepImportsNoPlaneThatCanAnswerAGate is step 3's SECOND guard,
// over the second object.
//
// The reflection guard bounds what arrives through a declared seam. It says
// nothing about a capability this package simply IMPORTS: a call to
// admission.Service.AdmitGateResponse needs no seam and names no forbidden type
// in any signature of this package. That is exactly why this code is not in
// internal/admission, where the answer path is a package-local call and neither
// guard could see it, and this test is what makes that placement load-bearing
// rather than a preference.
//
// WHAT IT PROVES AND WHAT IT DOES NOT. It parses the PRODUCTION files of this
// package and reads their import paths from the syntax tree; it is not a
// substring scan, so an alias, a line break or a mention in a comment cannot
// defeat or trigger it. Its residue, enumerated and explicitly NOT a closure:
// it does not see a capability reached through code generated at build time, or
// through a build-tagged file this parser's configuration excludes; it does not
// see reflection or linkname reaching another package; it does not constrain
// what the module's OTHER packages do with this one; and an allowed import
// (factory/identity, sessionstore, storage, core) that later grew a gate
// resolution API would satisfy it. The last of those is the one the reflection
// guard above covers, which is why the pair is the guard and neither half is.
func TestTheGateSweepImportsNoPlaneThatCanAnswerAGate(t *testing.T) {
	const modulePath = "github.com/looprig/factory"
	allowed := map[string]bool{
		modulePath + "/identity":                 true,
		"github.com/looprig/sessionstore":        true,
		"github.com/looprig/storage":             true,
		"github.com/looprig/core/sessionwire/v1": true,
	}

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	files, imports := 0, 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		files++
		parsed, err := parser.ParseFile(token.NewFileSet(), filepath.Join(".", name), nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parsing %s: %v", name, err)
		}
		for _, spec := range parsed.Imports {
			path, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatalf("%s: unquoting %s: %v", name, spec.Path.Value, err)
			}
			imports++
			if !strings.Contains(path, ".") {
				continue // a standard-library path has no domain and cannot be a Looprig plane
			}
			if allowed[path] {
				continue
			}
			t.Errorf("%s imports %q, which is outside this package's declared reach", name, path)
		}
		ast.Inspect(parsed, func(ast.Node) bool { return true })
	}
	if files == 0 {
		t.Fatal("the guard parsed no production files, so it is vacuous")
	}
	if imports == 0 {
		t.Fatal("the guard read no imports, so it would pass on a file it failed to parse")
	}
	// The guard's own positive control: the forbidden set must actually be
	// forbidden, or an empty allow-list check would pass by accident.
	for _, forbidden := range []string{
		modulePath + "/internal/admission",
		modulePath + "/internal/placement",
		modulePath + "/internal/httpapi",
		modulePath + "/internal/routing",
		"github.com/looprig/harness/pkg/rig",
	} {
		if allowed[forbidden] {
			t.Errorf("%q is in the allow-list, so the guard permits the capability it exists to ban", forbidden)
		}
	}
}

// ---------------------------------------------------------------------------
// Step 4 -- history does not accumulate in the due view.
// ---------------------------------------------------------------------------

// retireHistory opens and resolves count gates across sessions, leaving that
// many TOMBSTONED deadline intents behind and nothing live.
//
// It uses the ordinary path rather than the sweeper: ResolveGate clears the
// projection and retires the intent, which is how the overwhelming majority of
// a deployment's gates end. That is what makes these rows "historical".
func (f *gateFixture) retireHistory(sessions, count int) {
	f.t.Helper()
	for i := range sessions {
		f.prepare(sessionwire.SessionID(fmt.Sprintf("history-%03d", i)))
	}
	for i := range count {
		session := sessionwire.SessionID(fmt.Sprintf("history-%03d", i%sessions))
		gate := sessionwire.GateID(fmt.Sprintf("historic-gate-%06d", i))
		f.open(session, gate, f.clock.Now().Add(time.Minute))
		if _, err := f.store.ResolveGate(context.Background(), sessionstore.ResolveGateRequest{
			TenantID: gateTenant, SessionID: session, LeaseEpoch: f.epoch, GateID: gate,
		}); err != nil {
			f.t.Fatalf("ResolveGate(%s): %v", gate, err)
		}
	}
}

// TestHistoricalRetiredGatesAreAbsentFromDuePagesAtEverySize is step 4's scale
// case, and it is a FOR-ALL rather than a number.
//
// "Thousands of retired gates are absent from due pages" is not a claim about
// three gates, or about two thousand and six: it is the claim that the cost of
// a pass is a function of what is LIVE and not of how much history the
// deployment has accumulated. A fixture pinned to one size would pass against
// an implementation whose cost grew with history and merely had not grown
// enough yet.
//
// So the size is DRAWN, independently, several times, from a range that is in
// the thousands throughout, and the assertion is an equality that does not
// mention it: the rows a full drain examines equal the live rows and nothing
// else. The seed is logged so a failure is reproducible, and the drawn sizes
// are required to differ, or the "for all sizes" reading would be a
// coincidence.
func TestHistoricalRetiredGatesAreAbsentFromDuePagesAtEverySize(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("size seed = %d", seed)
	draw := rand.New(rand.NewSource(seed)) // #nosec G404 -- fixture sizing, not security

	const (
		minHistory = 1200
		maxHistory = 2600
		liveRows   = 5
		extraRows  = 3
	)
	sizes := make([]int, 0, 3)
	for len(sizes) < 3 {
		size := minHistory + draw.Intn(maxHistory-minHistory)
		if !slices.Contains(sizes, size) {
			sizes = append(sizes, size)
		}
	}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("history=%d", size), func(t *testing.T) {
			f := newGateFixture(t, 4, 8, 64)
			f.retireHistory(8, size)

			// Nothing historical is live: no open gate anywhere, and the whole
			// due view is empty. If this were false the equality below would be
			// measuring the wrong thing.
			gates, remnants, examined := f.dueNow()
			if len(gates) != 0 || len(remnants) != 0 || examined != 0 {
				t.Fatalf("after %d retirements the due view holds gates %v, remnants %v, %d examined rows",
					size, gates, remnants, examined)
			}

			// Now the live work, which is the only thing a pass may cost.
			for i := range liveRows {
				session := sessionwire.SessionID(fmt.Sprintf("live-%d", i))
				f.remnantOn(session, sessionwire.GateID(fmt.Sprintf("live-gate-%d", i)))
			}
			f.clock.advance(sessionstore.MinGateIntentRemnantAge + time.Minute)

			_, liveRemnants, liveExamined := f.dueNow()
			if len(liveRemnants) != liveRows {
				t.Fatalf("live remnants = %d, want %d", len(liveRemnants), liveRows)
			}
			// THE INVARIANT, and it does not mention the history size.
			if liveExamined != liveRows {
				t.Fatalf("a full drain examined %d rows over %d live rows and %d historical retirements;"+
					" history is being paged", liveExamined, liveRows, size)
			}

			// THE CONTROL. The same probe must GROW when live rows are added,
			// or "it examined exactly the live rows" would be satisfied by a
			// probe that counted nothing at all.
			for i := range extraRows {
				session := sessionwire.SessionID(fmt.Sprintf("extra-%d", i))
				f.remnantOn(session, sessionwire.GateID(fmt.Sprintf("extra-gate-%d", i)))
			}
			f.clock.advance(sessionstore.MinGateIntentRemnantAge + time.Minute)
			if _, _, grown := f.dueNow(); grown != liveRows+extraRows {
				t.Fatalf("after adding %d live rows the drain examined %d, want %d;"+
					" the probe does not track live work", extraRows, grown, liveRows+extraRows)
			}

			// And the sweeper's own cost is bounded the same way: it retires
			// every live remnant, and the pages it reads are a function of the
			// live rows and the shard count rather than of the history.
			swept := f.drain(16)
			if swept.Retired != liveRows+extraRows {
				t.Fatalf("the drain retired %d, want %d: %+v", swept.Retired, liveRows+extraRows, swept)
			}
			if swept.Examined != liveRows+extraRows {
				t.Errorf("the sweeper examined %d rows, want the %d live ones", swept.Examined, liveRows+extraRows)
			}
			if bound := liveRows + extraRows + f.store.ControlShards(); swept.Pages > bound {
				t.Errorf("the sweeper read %d pages over %d live rows, want at most %d", swept.Pages, liveRows+extraRows, bound)
			}
			if gates, remnants, _ := f.dueNow(); len(gates) != 0 || len(remnants) != 0 {
				t.Errorf("after the drain the due view holds gates %v and remnants %v", gates, remnants)
			}
		})
	}
	if len(sizes) < 3 || sizes[0] == sizes[1] {
		t.Fatalf("the sizes did not vary: %v", sizes)
	}
}

// TestTheRotorVisitsEveryShardOnceBeforeRepeatingAny holds the round-robin, and
// holds the reported shard to the shard actually read.
//
// A rotor that stuck would sweep shard zero forever and every other shard's
// gate intents would accumulate untouched, which nothing else here would
// notice: each individual pass looks correct. The reported Shard is checked
// against the store's own request because a result that named the wrong shard
// would make an operator's per-shard reading of this sweep silently wrong.
func TestTheRotorVisitsEveryShardOnceBeforeRepeatingAny(t *testing.T) {
	f, recorder := newRecordingFixture(t, 5, 8, 4)
	shards := f.store.ControlShards()

	var reported []int
	for range shards * 2 {
		reported = append(reported, f.sweepOnce().Shard)
	}
	asked := recorder.snapshot()
	if len(asked) != len(reported) {
		t.Fatalf("%d passes produced %d store requests", len(reported), len(asked))
	}
	for i, shard := range reported {
		if asked[i].shard != shard {
			t.Errorf("pass %d reported shard %d and read shard %d", i, shard, asked[i].shard)
		}
	}
	for round := range 2 {
		visited := map[int]bool{}
		for _, shard := range reported[round*shards : (round+1)*shards] {
			if visited[shard] {
				t.Errorf("round %d visited shard %d twice before covering every shard", round, shard)
			}
			visited[shard] = true
		}
		if len(visited) != shards {
			t.Errorf("round %d visited %d shards, want all %d", round, len(visited), shards)
		}
	}
}

// TestAStoreReportingNoShardsIsRefusedRatherThanSweptAsShardZero covers the one
// configuration this sweeper cannot validate at construction, because it is not
// a setting of this replica: the shard count is read from the store on every
// pass.
//
// A count below the store's own minimum is a broken backend, and sweeping shard
// zero anyway would report a clean pass over a view nobody can address.
func TestAStoreReportingNoShardsIsRefusedRatherThanSweptAsShardZero(t *testing.T) {
	due := &countingDue{shards: 0}
	intents := &countingIntents{}
	sweeper, err := NewGateSweeper(GateSweeperConfig{
		Authorizer: &recordingAuthorizer{}, Due: due, Intents: intents,
		Clock: &movableClock{now: gateOrigin}, PageLimit: 8, MaxPages: 2,
	})
	if err != nil {
		t.Fatalf("NewGateSweeper: %v", err)
	}
	result, err := sweeper.Sweep(context.Background(), servicePrincipal(t))
	if err == nil {
		t.Fatalf("a store reporting no shards was swept: %+v", result)
	}
	if due.pages != 0 {
		t.Errorf("a refused shard count still read %d pages", due.pages)
	}
	if !reflect.DeepEqual(result, GateSweepResult{}) {
		t.Errorf("result = %+v, want the zero result", result)
	}

	// The control: the minimum the store itself declares IS swept.
	due.shards = sessionstore.MinControlShards
	if _, err := sweeper.Sweep(context.Background(), servicePrincipal(t)); err != nil {
		t.Fatalf("the minimum shard count was refused: %v", err)
	}
	if due.pages != 1 {
		t.Errorf("the minimum shard count read %d pages, want 1", due.pages)
	}
}

// TestEveryDispositionRenders holds the closed vocabulary, including the
// unrecognized member.
//
// A disposition is operational data -- it is what an operator reads to know
// what a pass did about a row -- and "" is not a fate. A member added without a
// rendering would appear in a log as an empty string beside a count, which
// reads as an absence rather than as a kind.
func TestEveryDispositionRenders(t *testing.T) {
	want := map[GateDisposition]string{
		GateDispositionRetired:  "retired",
		GateDispositionTooSoon:  "too_soon",
		GateDispositionReopened: "reopened",
		GateDispositionRaceLost: "race_lost",
		GateDispositionAbsent:   "absent",
	}
	seen := map[string]GateDisposition{}
	for disposition, rendering := range want {
		if got := disposition.String(); got != rendering {
			t.Errorf("GateDisposition(%d).String() = %q, want %q", disposition, got, rendering)
		}
		if other, clash := seen[rendering]; clash {
			t.Errorf("%q renders both GateDisposition(%d) and GateDisposition(%d)", rendering, other, disposition)
		}
		seen[rendering] = disposition
	}
	// The walk over the contiguous range is what makes this closed rather than
	// a list: a member added to the const block is caught here without anyone
	// remembering to extend the map.
	for disposition := GateDispositionRetired; disposition <= GateDispositionAbsent; disposition++ {
		if _, named := want[disposition]; !named {
			t.Errorf("GateDisposition(%d) renders %q but is not in the expected set", disposition, disposition.String())
		}
	}
	if got := GateDisposition(200).String(); got != "unrecognized" {
		t.Errorf("an out-of-range disposition rendered %q, want %q", got, "unrecognized")
	}
	if GateDispositionAbsent.String() == GateDisposition(200).String() {
		t.Error("a real disposition renders the same as an unrecognized one")
	}
}

// TestAShrunkShardCountRestartsTheRotorAndDropsTheCursorsThatWentWithIt covers
// a re-shard under a running replica.
//
// A cursor is bound to the shard that issued it, so a position kept across a
// shrink would be presented to a view that never issued it and refused on every
// pass -- and the rotor, left past the end, would sweep shard zero forever. The
// shard count is the store's decision and is re-read every pass, so this is a
// state this replica really can be in.
func TestAShrunkShardCountRestartsTheRotorAndDropsTheCursorsThatWentWithIt(t *testing.T) {
	f, recorder := newRecordingFixture(t, 6, 1, 1)
	for i := range 24 {
		session := sessionwire.SessionID(fmt.Sprintf("session-%02d", i))
		f.prepare(session)
		f.open(session, sessionwire.GateID(fmt.Sprintf("gate-%02d", i)), f.clock.Now().Add(time.Minute))
	}
	f.clock.advance(sessionstore.MinGateIntentRemnantAge + time.Minute)

	// Two full rounds. With four rows per shard and a one-row budget, every
	// shard is left MID-WALK holding a position -- which is the state the
	// shrink has to act on. Sweeping to exhaustion instead would leave the
	// cursors already dropped and the whole test vacuous, and that is not a
	// hypothetical: it is what the first version of this fixture did.
	for range 2 {
		f.sweepAll()
	}
	// Three further single passes, so the rotor is left at shard 3 -- PAST the
	// end of the shrunk range. That is deliberate and it is the second thing
	// the shrink has to handle: a rotor left out of range would have the
	// sweeper read a shard the store no longer addresses. Leaving the rotor at
	// zero, as a whole number of rounds does, makes that half untestable.
	for range 3 {
		f.sweepOnce()
	}
	wide := recorder.snapshot()
	heldAtShrink := map[int]sessionwire.Cursor{}
	for _, exchange := range wide {
		heldAtShrink[exchange.shard] = exchange.cursorOut
	}
	wideHolders := 0
	for shard, cursor := range heldAtShrink {
		if shard >= 2 && cursor != "" {
			wideHolders++
		}
	}
	if wideHolders == 0 {
		t.Fatal("no shard outside the shrunk range held a position at the shrink, so nothing can be dropped")
	}

	recorder.mu.Lock()
	recorder.shardsOverride = 2
	recorder.mu.Unlock()

	for range 6 {
		if _, err := f.sweeper.Sweep(context.Background(), servicePrincipal(t)); err != nil {
			t.Fatalf("Sweep after the shrink: %v", err)
		}
	}
	for _, exchange := range recorder.snapshot()[len(wide):] {
		if exchange.shard >= 2 {
			t.Errorf("a pass after the shrink read shard %d, which no longer exists", exchange.shard)
		}
	}
	// And the rotor covers the shrunk range rather than sticking at zero.
	visited := map[int]bool{}
	for _, exchange := range recorder.snapshot()[len(wide):] {
		visited[exchange.shard] = true
	}
	if len(visited) != 2 {
		t.Errorf("after the shrink the rotor visited %v, want both remaining shards", visited)
	}

	// AND THE SHRINK MUST HAVE DROPPED THOSE POSITIONS, not merely stopped
	// reading them. A backend that is re-sharded back up -- a rollback, a
	// second migration -- makes the difference observable: a position merely
	// left unread is presented again the moment its shard index exists once
	// more, to a view that has since been rebuilt, and is refused on every pass
	// from then on. This half is why the shrink deletes rather than ignores.
	shrunk := len(recorder.snapshot())
	recorder.mu.Lock()
	recorder.shardsOverride = 0 // back to the store's own six
	recorder.mu.Unlock()

	for range 12 {
		if _, err := f.sweeper.Sweep(context.Background(), servicePrincipal(t)); err != nil {
			t.Fatalf("Sweep after the re-grow: %v", err)
		}
	}
	// The probe is the cursor the FIRST pass over each re-grown shard presents,
	// and it must be EMPTY. Comparing the presented token against the one that
	// shard issued before the shrink was tried first and does not discriminate:
	// this fixture's clock does not move, so a shard re-paged from the head
	// re-derives a byte-identical continuation, and a kept position and a
	// dropped-then-reissued one read the same. "Opened at the head" is the
	// property, and it is observable directly.
	regrown := recorder.snapshot()[shrunk:]
	firstAfterRegrow := map[int]sessionwire.Cursor{}
	order := []int{}
	for _, exchange := range regrown {
		if exchange.shard < 2 {
			continue
		}
		if _, seen := firstAfterRegrow[exchange.shard]; seen {
			continue
		}
		firstAfterRegrow[exchange.shard] = exchange.cursorIn
		order = append(order, exchange.shard)
	}
	if len(order) == 0 {
		t.Fatal("the re-grown rotor never revisited a wide shard, so the assertion is vacuous")
	}
	for _, shard := range order {
		if got := firstAfterRegrow[shard]; got != "" {
			t.Errorf("shard %d's first pass after the re-grow presented %q, want the head:"+
				" its pre-shrink position was kept across a re-shard", shard, got)
		}
	}
	// Non-vacuity from the other end, per shard rather than in aggregate: each
	// shard asserted above must actually have HELD a position at the moment of
	// the shrink, or "it opened at the head" says nothing.
	measured := 0
	for _, shard := range order {
		if heldAtShrink[shard] == "" {
			continue
		}
		measured++
	}
	if measured == 0 {
		t.Fatal("none of the re-grown shards held a position at the shrink, so the drop is not being measured")
	}
}

// TestAPassCountsEveryStoreCallThatReachesAProvider holds the cost report.
//
// Queries is what tells an operator whether a sweep that reported nothing was
// cheap or expensive, so a counter that silently stopped moving would make a
// runaway pass and an idle one look identical. It counts the two calls that
// reach a provider -- the due page and the retirement -- and deliberately not
// ControlShards, which reads a decision the store already holds in memory.
func TestAPassCountsEveryStoreCallThatReachesAProvider(t *testing.T) {
	f, recorder := newRecordingFixture(t, 1, 2, 8)
	for i := range 3 {
		f.remnantOn(sessionwire.SessionID(fmt.Sprintf("session-%d", i)), sessionwire.GateID(fmt.Sprintf("gate-%d", i)))
	}
	// One still-open gate, so a page carries work that is NOT a retirement.
	f.prepare("open-one")
	f.open("open-one", "open-gate", f.clock.Now().Add(time.Minute))
	f.clock.advance(sessionstore.MinGateIntentRemnantAge + time.Minute)

	before := len(recorder.snapshot())
	result := f.sweepOnce()

	pages := len(recorder.snapshot()) - before
	retirements := 0
	for _, count := range result.Dispositions {
		retirements += count
	}
	if pages == 0 || retirements == 0 {
		t.Fatalf("the pass made %d page calls and %d retirements; both must be nonzero or the sum is vacuous", pages, retirements)
	}
	if want := pages + retirements; result.Queries != want {
		t.Errorf("Queries = %d, want %d (%d pages + %d retirements)", result.Queries, want, pages, retirements)
	}
	if result.Queries == result.Pages {
		t.Errorf("Queries (%d) equals Pages, so retirements are not being counted", result.Queries)
	}
	// ControlShards is read once per pass and must NOT be in the count.
	if result.Queries > pages+retirements {
		t.Errorf("Queries = %d exceeds the provider calls made (%d)", result.Queries, pages+retirements)
	}
}
