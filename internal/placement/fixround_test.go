package placement

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/hostlink"
	"github.com/looprig/sessionstore"
)

// These cases close the B5 gates' findings on the placement package: Q1 (a
// Host's code-less failure moves on), Q3 (placement over the REAL pool), Q5
// (the production backoff and the untested counters), Q8 (backoff hardening)
// and the spec gate's P2 and S4 survivors.

// TestABrokenTopRankedHostDoesNotBlockPlacementOnAHealthyOne is the quality
// gate's Q1 probe, committed. host-broken answers every attach with the Host's
// code-less failure and, launching nothing, keeps the most free capacity, so
// it is asked first on every pass. Each of five passes must still place on
// host-healthy.
func TestABrokenTopRankedHostDoesNotBlockPlacementOnAHealthyOne(t *testing.T) {
	t.Parallel()

	f := newAttachFixture(t, nil)
	f.publishTarget(t, "host-broken", 9, sessionwire.HostIsolationClassCrossTenantIsolated)
	f.publishTarget(t, "host-healthy", 2, sessionwire.HostIsolationClassCrossTenantIsolated)
	f.links.on("host-broken", fail(fmt.Errorf("%w: hostlink: hostlink.attach: host answered 100 internal server error", ErrAttachFailed)))
	for pass := 1; pass <= 5; pass++ {
		result, err := f.place(t, "cmd-1")
		if err != nil || result.Decision.Outcome != OutcomeAttachPooled || result.Attached.HostID != "host-healthy" {
			t.Fatalf("pass %d = (%+v, %v), want attached to host-healthy", pass, result, err)
		}
		if len(result.Failed) != 1 || result.Failed[0] != "host-broken" || len(result.Refused) != 0 {
			t.Fatalf("pass %d: Failed=%v Refused=%v, want host-broken recorded as failed", pass, result.Failed, result.Refused)
		}
	}
	hosts := f.links.attachedHosts()
	if len(hosts) != 10 || hosts[0] != "host-broken" || hosts[1] != "host-healthy" {
		t.Fatalf("attaches = %v, want broken then healthy on each of 5 passes", hosts)
	}
	if !strings.Contains(f.logs.String(), `"msg":"placement: a pooled candidate failed the attach; trying the next","host_id":"host-broken"`) {
		t.Fatalf("no WARN naming host-broken: %s", f.logs.String())
	}
}

// TestAFailedAttachIsItsOwnClassAndNotAnAbortOrARefusal pins attachAnswer's
// new row against its neighbours.
func TestAFailedAttachIsItsOwnClassAndNotAnAbortOrARefusal(t *testing.T) {
	t.Parallel()

	if got, _ := attachAnswer(fmt.Errorf("x: %w", ErrAttachFailed)); got != answerFailed {
		t.Fatalf("ErrAttachFailed classified %d, want answerFailed", got)
	}
	if got, _ := attachAnswer(errors.New("context deadline exceeded")); got != answerAbort {
		t.Fatalf("an ambiguous failure classified %d, want abort", got)
	}
}

// ---- Q3: placement over the REAL hostlink.Pool --------------------------------

type realPoolLinks struct{ pool *hostlink.Pool }

func (l realPoolLinks) Attach(ctx context.Context, endpoint sessionwire.InternalEndpoint, req sessionwire.HostLinkAttachRequest) (sessionwire.HostLinkRegistryObservation, error) {
	return l.pool.Attach(ctx, hostlink.Target{Host: req.HostID, Endpoint: endpoint}, req)
}
func (l realPoolLinks) Bind(ctx context.Context, endpoint sessionwire.InternalEndpoint, req sessionwire.HostLinkBindRequest) error {
	return l.pool.Bind(ctx, hostlink.Target{Host: req.HostID, Endpoint: endpoint}, req)
}
func (l realPoolLinks) Unbind(ctx context.Context, req sessionwire.HostLinkUnbindRequest) error {
	return l.pool.Unbind(ctx, req)
}
func (l realPoolLinks) DeliverCommand(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, d sessionwire.HostLinkCommandDelivery) error {
	return l.pool.DeliverCommand(ctx, tenant, session, d)
}
func (l realPoolLinks) AcceptsGateResponses(ctx context.Context, owner sessionwire.HostLinkRegistryObservation) (bool, error) {
	return l.pool.AcceptsGateResponses(ctx, hostlink.Target{Host: owner.HostID, Endpoint: owner.InternalEndpoint}, owner.TenantID)
}
func (l realPoolLinks) AcceptsCommandPrincipal(ctx context.Context, owner sessionwire.HostLinkRegistryObservation) (bool, error) {
	return l.pool.AcceptsCommandPrincipal(ctx, hostlink.Target{Host: owner.HostID, Endpoint: owner.InternalEndpoint}, owner.TenantID)
}
func (l realPoolLinks) RouteFor(tenant sessionwire.TenantID, session sessionwire.SessionID) (sessionwire.HostID, bool) {
	return l.pool.RouteFor(tenant, session)
}

// acceptingLink is a Host that accepts everything and records what it saw.
type acceptingLink struct {
	host sessionwire.HostID
	mu   *sync.Mutex
	seen *[]string
}

func (l acceptingLink) record(what string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	*l.seen = append(*l.seen, what)
}
func (l acceptingLink) Host() sessionwire.HostID { return l.host }
func (l acceptingLink) Bind(context.Context, sessionwire.HostLinkBindRequest) error {
	l.record("bind")
	return nil
}
func (l acceptingLink) Unbind(context.Context, sessionwire.HostLinkUnbindRequest) error {
	l.record("unbind")
	return nil
}
func (l acceptingLink) Close(context.Context) error { return nil }
func (l acceptingLink) DeliverCommand(_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, d sessionwire.HostLinkCommandDelivery) error {
	l.record("deliver " + string(d.CommandID))
	return nil
}
func (l acceptingLink) Attach(_ context.Context, req sessionwire.HostLinkAttachRequest) (sessionwire.HostLinkRegistryObservation, error) {
	l.record("attach")
	return acceptedObservation(req, attachedEpoch), nil
}

type acceptingDialer struct {
	mu   sync.Mutex
	seen []string
}

func (d *acceptingDialer) Dial(_ context.Context, target hostlink.Target, _ hostlink.Observer) (hostlink.Link, error) {
	return acceptingLink{host: target.Host, mu: &d.mu, seen: &d.seen}, nil
}

func (d *acceptingDialer) wire() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.seen...)
}

func realPoolReconciler(t *testing.T) (*fixture, *hostlink.Pool, *acceptingDialer) {
	t.Helper()
	f := newFixture(t, sessionwire.HostPlacementPooled, "factory-1")
	dialer := &acceptingDialer{}
	pool, err := hostlink.NewPool(hostlink.Config{Dialer: dialer})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pool.Close(context.Background()) })
	r, err := NewReconciler(Config{
		Directory: mustDirectory(t, f.store), Catalog: f.store, Claims: f.store,
		Clock: f.clock, HolderID: "factory-1", ClaimTTL: time.Minute, CandidateLimit: 8,
		Links: realPoolLinks{pool: pool}, ActorID: testActor,
	})
	if err != nil {
		t.Fatal(err)
	}
	f.reconciler = r
	f.publishTarget(t, "host-a", 9, sessionwire.HostIsolationClassCrossTenantIsolated)
	return f, pool, dialer
}

// TestPlacementOverTheRealPoolGivesBackItsTransientRoute is the quality gate's
// Q3/QM24 reader. The scripted links answered RouteFor from a flag, so a
// placement that read the route AFTER its own bind -- and so thought every
// route it created was a viewer's -- leaked every transient route and passed.
// The real pool is what makes that visible.
func TestPlacementOverTheRealPoolGivesBackItsTransientRoute(t *testing.T) {
	t.Parallel()

	f, pool, dialer := realPoolReconciler(t)
	result, err := f.reconciler.Reconcile(context.Background(), Request{TenantID: testTenant, SessionID: testSession, Wake: []sessionwire.CommandID{"cmd-1"}})
	if err != nil || !result.Bound || result.Delivered != 1 {
		t.Fatalf("Reconcile = (%+v, %v)", result, err)
	}
	if host, routed := pool.RouteFor(testTenant, testSession); routed || pool.Bindings("host-a") != 0 {
		t.Fatalf("placement left its route behind: routed=%t host=%q bindings=%d", routed, host, pool.Bindings("host-a"))
	}
	if got, want := strings.Join(dialer.wire(), ","), "attach,bind,deliver cmd-1,unbind"; got != want {
		t.Fatalf("wire = %s, want %s", got, want)
	}
}

// TestPlacementOverTheRealPoolLeavesAViewersRoute is the other half: a route a
// viewer already holds to the same Host survives placement.
func TestPlacementOverTheRealPoolLeavesAViewersRoute(t *testing.T) {
	t.Parallel()

	f, pool, dialer := realPoolReconciler(t)
	viewer := sessionwire.HostLinkBindRequest{Version: sessionwire.CurrentWireVersion, TenantID: testTenant, SessionID: testSession,
		HostID: "host-a", HostGeneration: 1, LeaseEpoch: 1, RuntimeCompatibilityID: testRuntime, IdempotencyKey: "viewer"}
	if err := pool.Bind(context.Background(), hostlink.Target{Host: "host-a", Endpoint: "wss://attached.host-a.internal"}, viewer); err != nil {
		t.Fatal(err)
	}
	result, err := f.reconciler.Reconcile(context.Background(), Request{TenantID: testTenant, SessionID: testSession, Wake: []sessionwire.CommandID{"cmd-1"}})
	if err != nil || !result.Bound {
		t.Fatalf("Reconcile = (%+v, %v)", result, err)
	}
	if host, routed := pool.RouteFor(testTenant, testSession); !routed || host != "host-a" {
		t.Fatalf("the viewer's route is %q/%t after placement, want it kept", host, routed)
	}
	for _, step := range dialer.wire() {
		if step == "unbind" {
			t.Fatalf("placement unbound a route it did not create: %v", dialer.wire())
		}
	}
}

// ---- Q5 / P7 / P8 / Q8: the production backoff ---------------------------------

// TestTheProductionBackoffIsThreeAttemptsFromTwoHundredMilliseconds asserts the
// defaults BY VALUE (spec gate P7/P8), the doubling, and the cap that stops
// the old shift from overflowing negative (quality gate Q8).
func TestTheProductionBackoffIsThreeAttemptsFromTwoHundredMilliseconds(t *testing.T) {
	t.Parallel()

	r := &Reconciler{}
	if got := r.replaceAttempts(); got != 3 {
		t.Fatalf("default attempts = %d, want 3", got)
	}
	for n, want := range map[int]time.Duration{1: 200 * time.Millisecond, 2: 400 * time.Millisecond, 3: 800 * time.Millisecond, 5: 3200 * time.Millisecond, 6: 5 * time.Second, 64: 5 * time.Second, 1 << 20: 5 * time.Second} {
		if got := r.backoff(n); got != want {
			t.Errorf("default backoff(%d) = %v, want %v", n, got, want)
		}
	}
	configured := &Reconciler{cfg: Config{ReplaceBackoff: time.Second}}
	for _, n := range []int{35, 39, 1000} {
		if got := configured.backoff(n); got != 5*time.Second {
			t.Errorf("backoff(%d) with a 1s base = %v, want the 5s cap (the shift went negative here)", n, got)
		}
	}
}

// TestTheReplacementBoundsAreRefusedPastTheirCeilings holds the new ceilings.
func TestTheReplacementBoundsAreRefusedPastTheirCeilings(t *testing.T) {
	t.Parallel()

	base := Config{Links: newScriptedLinks(), ActorID: testActor}
	for _, test := range []struct {
		name string
		cfg  func(Config) Config
		ok   bool
	}{
		{"ten attempts", func(c Config) Config { c.ReplaceAttempts = 10; return c }, true},
		{"eleven attempts", func(c Config) Config { c.ReplaceAttempts = 11; return c }, false},
		{"negative attempts", func(c Config) Config { c.ReplaceAttempts = -1; return c }, false},
		{"five-second base", func(c Config) Config { c.ReplaceBackoff = 5 * time.Second; return c }, true},
		{"longer base", func(c Config) Config { c.ReplaceBackoff = 5*time.Second + 1; return c }, false},
		{"negative base", func(c Config) Config { c.ReplaceBackoff = -1; return c }, false},
	} {
		err := test.cfg(base).attachConfigError()
		if test.ok != (err == nil) || (err != nil && !errors.Is(err, ErrInvalidConfig)) {
			t.Errorf("%s: attachConfigError = %v", test.name, err)
		}
	}
}

// TestTheProductionWaitSleepsAJitteredBackoffAndHonoursCancellation executes
// the default Wait path, which no test ran (quality gate QM17/QM18): it sleeps
// at least half the nominal backoff and at most all of it, and a cancelled
// context ends it at once.
func TestTheProductionWaitSleepsAJitteredBackoffAndHonoursCancellation(t *testing.T) {
	t.Parallel()

	// Every nominal duration from 1ns to 2µs, 50 draws each: a draw outside
	// [d/2, d] for any of them is a failure, and with d >= 4 a jitter drawn
	// from [0, d] or [d, 2d] leaves the band on a draw nearly every time.
	for d := time.Duration(1); d <= 2*time.Microsecond; d++ {
		for range 50 {
			if got := jittered(d); got < d/2 || got > d {
				t.Fatalf("jittered(%v) = %v, want within [%v, %v]", d, got, d/2, d)
			}
		}
	}
	// d=1ns cannot halve, and must not draw zero: the band is exactly {1ns}.
	for range 50 {
		if got := jittered(1); got != 1 {
			t.Fatalf("jittered(1ns) = %v, want 1ns", got)
		}
	}
	// The default wait sleeps the DRAW, not the nominal duration, and not a
	// multiple of it: a one-second nominal backoff drawn as 100ms must return
	// in about 100ms (QJ1: jitter dropped would sleep 1s; QJ2: a doubled draw
	// would sleep 200ms).
	drawn := &Reconciler{draw: func(d time.Duration) time.Duration {
		if d != time.Second {
			t.Errorf("the draw was asked for %v, want the nominal 1s", d)
		}
		return 100 * time.Millisecond
	}}
	//
	// The ceiling is asserted on the FASTEST of up to five waits, not on one.
	// A single wait overshot 180ms under whole-module -race load (v0.8.0 review
	// R-N2), and a timer can only ever fire late, never early. The mutants it
	// exists to kill cannot pass that way: a doubled draw sleeps at least 200ms
	// and a dropped jitter at least 1s on EVERY attempt, so no minimum of theirs
	// is under the ceiling. The floor stays on every attempt, since a timer
	// firing early is not a load effect.
	fastest := time.Duration(-1)
	for range 5 {
		start := time.Now()
		if err := drawn.wait(context.Background(), time.Second); err != nil {
			t.Fatalf("wait = %v", err)
		}
		elapsed := time.Since(start)
		if elapsed < 100*time.Millisecond {
			t.Fatalf("a wait drawn as 100ms took %v, less than the draw", elapsed)
		}
		if fastest < 0 || elapsed < fastest {
			fastest = elapsed
		}
		if fastest <= 180*time.Millisecond {
			break
		}
	}
	if fastest > 180*time.Millisecond {
		t.Fatalf("the fastest of five waits drawn as 100ms took %v, want within [100ms, 180ms]", fastest)
	}
	// G4: production's draw -- a nil one -- IS jittered, not the nominal
	// duration or a multiple of it. Compared by identity, since two functions
	// that happen to agree on the durations tried are not the same draw.
	if got, want := reflect.ValueOf((&Reconciler{}).drawOrDefault()).Pointer(), reflect.ValueOf(jittered).Pointer(); got != want {
		t.Fatal("a Reconciler with no injected draw does not draw from jittered")
	}
	if got, want := reflect.ValueOf(drawn.drawOrDefault()).Pointer(), reflect.ValueOf(drawn.draw).Pointer(); got != want {
		t.Fatal("an injected draw is not the one the wait uses")
	}
	r := &Reconciler{}
	start := time.Now()
	if err := r.wait(context.Background(), 60*time.Millisecond); err != nil {
		t.Fatalf("wait = %v", err)
	}
	if elapsed := time.Since(start); elapsed < 30*time.Millisecond {
		t.Fatalf("a 60ms wait returned after %v, less than half of it", elapsed)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	start = time.Now()
	if err := r.wait(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled wait = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("a cancelled one-hour wait took %v", elapsed)
	}
}

// ---- Q5 / QM9: DeliveryFailures ------------------------------------------------

type failingDelivery struct{ *scriptedLinks }

func (failingDelivery) DeliverCommand(context.Context, sessionwire.TenantID, sessionwire.SessionID, sessionwire.HostLinkCommandDelivery) error {
	return errors.New("the link dropped the delivery")
}

func TestAFailedWakeIsCountedAndDoesNotFailThePlacement(t *testing.T) {
	t.Parallel()

	f := newFixture(t, sessionwire.HostPlacementPooled, "factory-1")
	links := failingDelivery{newScriptedLinks()}
	r, err := NewReconciler(Config{
		Directory: mustDirectory(t, f.store), Catalog: f.store, Claims: f.store,
		Clock: f.clock, HolderID: "factory-1", ClaimTTL: time.Minute, CandidateLimit: 8,
		Links: links, ActorID: testActor,
	})
	if err != nil {
		t.Fatal(err)
	}
	f.publishTarget(t, "host-a", 9, sessionwire.HostIsolationClassCrossTenantIsolated)
	result, err := r.Reconcile(context.Background(), Request{TenantID: testTenant, SessionID: testSession, Wake: []sessionwire.CommandID{"cmd-1", "cmd-2"}})
	if err != nil || !result.Bound || result.Delivered != 0 || result.DeliveryFailures != 2 {
		t.Fatalf("Reconcile = (%+v, %v), want bound with 2 delivery failures", result, err)
	}
}

// ---- spec gate P2: a desired placement changed under the claim -----------------

// TestAPlacementChangedUnderTheClaimIsDecidedFromTheNewRecord: a racer moves
// the session from pooled to dedicated between the pre-claim read and the
// claim. The record read under the claim decides, so a replica with Links and
// no workload controller refuses the dedicated arm by name and attaches
// nothing.
func TestAPlacementChangedUnderTheClaimIsDecidedFromTheNewRecord(t *testing.T) {
	t.Parallel()

	f := newFixture(t, sessionwire.HostPlacementPooled, "factory-1")
	f.publishTarget(t, "host-a", 9, sessionwire.HostIsolationClassCrossTenantIsolated)
	racer := f.racingReconciler(t, f.store)
	racer.cfg.HolderID = "factory-9"
	catalog := &racerOnFirstRead{Store: f.store, racer: func() {
		if _, err := racer.Reconcile(context.Background(), Request{TenantID: testTenant, SessionID: testSession, Desired: dedicatedDesired(`{"cpu":"8"}`)}); err != nil {
			t.Errorf("racer: %v", err)
		}
	}}
	links := newScriptedLinks()
	r, err := NewReconciler(Config{
		Directory: mustDirectory(t, f.store), Catalog: catalog, Claims: f.store,
		Clock: f.clock, HolderID: "factory-1", ClaimTTL: time.Minute, CandidateLimit: 8,
		Links: links, ActorID: testActor,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.Reconcile(context.Background(), Request{TenantID: testTenant, SessionID: testSession})
	if !errors.Is(err, ErrNoWorkloadController) || len(links.attaches) != 0 {
		t.Fatalf("Reconcile = %v with attaches %v, want ErrNoWorkloadController and no attach", err, links.attaches)
	}
}

// racerOnRead runs the racer right after the Nth catalog read returns.
type racerOnRead struct {
	*sessionstore.Store
	n     int
	reads int
	racer func()
}

func (c *racerOnRead) GetCatalogEntry(ctx context.Context, req sessionstore.GetCatalogEntryRequest) (sessionstore.CatalogEntry, error) {
	entry, err := c.Store.GetCatalogEntry(ctx, req)
	c.reads++
	if c.reads == c.n {
		c.racer()
	}
	return entry, err
}

// TestAPlacementChangedDuringThePooledArmIsUndecided is the spec gate's P2: the
// desired placement moves AFTER Reconcile's read under the claim and before
// placePooled's own re-read. placePooled must answer Undecided and attach
// nothing, because the record it would attach from no longer asks for a pooled
// Host.
func TestAPlacementChangedDuringThePooledArmIsUndecided(t *testing.T) {
	t.Parallel()

	f := newFixture(t, sessionwire.HostPlacementPooled, "factory-1")
	f.publishTarget(t, "host-a", 9, sessionwire.HostIsolationClassCrossTenantIsolated)
	catalog := &racerOnRead{Store: f.store, n: 2, racer: func() {
		entry, err := f.store.GetCatalogEntry(context.Background(), sessionstore.GetCatalogEntryRequest{TenantID: testTenant, SessionID: testSession})
		if err != nil {
			t.Errorf("racer read: %v", err)
			return
		}
		desired := dedicatedDesired(`{"cpu":"8"}`)
		if _, err := f.store.UpdateCatalogDesiredState(context.Background(), sessionstore.UpdateCatalogDesiredStateRequest{
			TenantID: testTenant, SessionID: testSession, ExpectedRevision: entry.Revision, IdempotencyKey: "racer-desired",
			DesiredPlacement: desired.Placement, RuntimeCompatibilityID: desired.RuntimeCompatibilityID, DesiredWorkload: desired.Workload,
		}); err != nil {
			t.Errorf("racer write: %v", err)
		}
	}}
	links := newScriptedLinks()
	r, err := NewReconciler(Config{
		Directory: mustDirectory(t, f.store), Catalog: catalog, Claims: f.store,
		Clock: f.clock, HolderID: "factory-1", ClaimTTL: time.Minute, CandidateLimit: 8,
		Links: links, ActorID: testActor,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := r.Reconcile(context.Background(), Request{TenantID: testTenant, SessionID: testSession})
	if err != nil || result.Decision.Outcome != OutcomeUndecided || len(links.attaches) != 0 {
		t.Fatalf("Reconcile = (%+v, %v) with attaches %v, want Undecided and no attach", result, err, links.attaches)
	}
	if catalog.reads != 3 {
		t.Fatalf("catalog reads = %d, want 3 (pre-claim, under the claim, pooled arm)", catalog.reads)
	}
}
