package admission

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	coreuuid "github.com/looprig/core/uuid"
	"github.com/looprig/factory/identity"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

// ---------------------------------------------------------------------------
// The scenario space, and how it was derived
// ---------------------------------------------------------------------------
//
// Step 2 of the runbook names eight scenarios. They are a FLOOR. What this file
// drives is the space those eight are samples of, derived from what the
// reconciler's answer about one due row is actually a function of. Everything
// it reads is in one of six axes, and nothing else is consulted -- it never
// reads the catalog, the registry or the target directory, which is why "cold
// placement" and "an existing Host owner" are not separate branches of the code
// but two points on axes C and D:
//
//	A  inbox state          pending | claimed | applying | applied | rejected
//	B  apply deadline       before now | exactly now | after now
//	C  command claim        none | lapsed | live
//	D  journal evidence     absent | prefix unresolved | committed | abandoned
//	E  reconciliation claim free | held by another replica | this replica's
//	F  caller               service principal | tenant principal
//
// The raw cross product is 5*3*3*4*3*2 = 1080 and most of it is unreachable,
// so it is PRUNED by the mechanisms rather than by taste, and each pruning rule
// is a fact this file can point at:
//
//   - validateInboxState (sessionstore/inbox.go) forbids a claim on a pending
//     record and requires one on claimed and applying, so A and C are not
//     independent: pending pairs only with "none", claimed and applying only
//     with lapsed or live.
//   - inboxDue (sessionstore/inbox.go:452) files a terminal record NOT DUE, so
//     A in {applied, rejected} cannot appear on a due page at all. It is
//     driven anyway, through the predicate directly, because a row CAN settle
//     between the page and the write.
//   - The due horizon is min(ApplyDeadline, Claim.ExpiresAt), so a row on a
//     page read at bound `now` has already passed one of B and C. B's "after
//     now" therefore only reaches the predicate together with a lapsed claim.
//   - D is read by the STORE, inside RejectCommand, and only for a row this
//     reconciler already decided to settle. It is therefore an axis of the
//     settlement's outcome and not of the predicate, and the four values
//     collapse into the two provesNoEffect() distinguishes.
//   - F is decided before any of the others and short-circuits the call, so it
//     is one axis crossed with "everything else", not with each of them.
//
// What that leaves is 14 reachable predicate points (TestTheSafetyPredicate...),
// the two evidence outcomes over the real journal, three reconciliation-claim
// states and two callers. The eight named scenarios are noted against the tests
// that drive them in each test's own comment.

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const sweepTenant = sessionwire.TenantID("tenant-sweep")

var sweepBase = time.Date(2026, 9, 11, 9, 0, 0, 0, time.UTC)

// sweepClock is ONE movable clock, shared by the store and the reconciler.
//
// Sharing it is not convenience. The apply-deadline half of the safety rule has
// exactly one authority -- the reconciler -- while the claim and evidence
// halves are decided again by the store against ITS clock, and two clocks would
// make a boundary case's outcome a fact about the skew rather than about the
// rule under test.
type sweepClock struct {
	mu  sync.Mutex
	now time.Time
}

func newSweepClock() *sweepClock { return &sweepClock{now: sweepBase} }

func (c *sweepClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *sweepClock) Set(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = now
}

func (c *sweepClock) AfterFunc(time.Duration, func()) func() bool {
	return func() bool { return true }
}

// countingStore is the real released store behind a counter.
//
// It counts by METHOD NAME, and it is what step 4's cost claim is measured
// with: SweepResult.Queries is the reconciler's own report, and this is an
// independent reading of the same quantity taken at the seam. The two are
// compared against each other in every store-backed case, so neither can drift
// into agreeing with itself.
type countingStore struct {
	faultInjector

	store *sessionstore.Store

	// pageBudget, when positive, fails every ListDueCommands after the first
	// pageBudget of them.
	//
	// faultInjector cannot express this: it fails a method from the FIRST call,
	// which is why the whole suite had no case where a sweep failed while
	// holding a claim -- the one state the release-on-every-path decision is
	// about. A budget is the smallest thing that reaches it.
	pageBudget int

	mu    sync.Mutex
	calls map[string]int
}

func newCountingStore(store *sessionstore.Store) *countingStore {
	return &countingStore{store: store, calls: map[string]int{}}
}

func (c *countingStore) note(method string) {
	c.mu.Lock()
	c.calls[method]++
	c.mu.Unlock()
}

func (c *countingStore) count(method string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls[method]
}

// providerQueries is the counted total, excluding ControlShards for the reason
// SweepResult.Queries states: it makes no provider request.
func (c *countingStore) providerQueries() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	total := 0
	for method, n := range c.calls {
		if method == "ControlShards" {
			continue
		}
		total += n
	}
	return total
}

func (c *countingStore) reset() {
	c.mu.Lock()
	c.calls = map[string]int{}
	c.mu.Unlock()
}

func (c *countingStore) ControlShards() int {
	c.note("ControlShards")
	return c.store.ControlShards()
}

func (c *countingStore) ListDueCommands(ctx context.Context, req sessionstore.ListDueCommandsRequest) (sessionstore.DueCommandPage, error) {
	c.note("ListDueCommands")
	if err := c.enter("ListDueCommands"); err != nil {
		return sessionstore.DueCommandPage{}, err
	}
	if c.pageBudget > 0 && c.count("ListDueCommands") > c.pageBudget {
		return sessionstore.DueCommandPage{}, errInjectedFault
	}
	return c.store.ListDueCommands(ctx, req)
}

func (c *countingStore) RejectCommand(ctx context.Context, req sessionstore.RejectCommandRequest) (sessionstore.InboxEntry, error) {
	c.note("RejectCommand")
	if err := c.enter("RejectCommand"); err != nil {
		return sessionstore.InboxEntry{}, err
	}
	return c.store.RejectCommand(ctx, req)
}

func (c *countingStore) AcquireReconciliationClaim(ctx context.Context, req sessionstore.AcquireReconciliationClaimRequest) (sessionstore.ReconciliationClaimEntry, error) {
	c.note("AcquireReconciliationClaim")
	if err := c.enter("AcquireReconciliationClaim"); err != nil {
		return sessionstore.ReconciliationClaimEntry{}, err
	}
	return c.store.AcquireReconciliationClaim(ctx, req)
}

func (c *countingStore) ReleaseReconciliationClaim(ctx context.Context, req sessionstore.ReleaseReconciliationClaimRequest) (sessionstore.ReconciliationClaimEntry, error) {
	c.note("ReleaseReconciliationClaim")
	if err := c.enter("ReleaseReconciliationClaim"); err != nil {
		return sessionstore.ReconciliationClaimEntry{}, err
	}
	return c.store.ReleaseReconciliationClaim(ctx, req)
}

// sweepAuthorizer is the service-sweep decision written as the rule rather than
// as a switch a test flips: a principal that is not a service identity cannot
// reach this query. It is the deployer's rule, restated here because a fake
// that admitted everything would make the tenant-principal case vacuous.
type sweepAuthorizer struct {
	faultInjector
	controlCalls int
	sweepCalls   int
}

func (a *sweepAuthorizer) AuthorizeControl(context.Context, identity.Principal, sessionwire.SessionID, sessionstore.CommandKind) error {
	a.controlCalls++
	if err := a.enter("AuthorizeControl"); err != nil {
		return err
	}
	return nil
}

func (a *sweepAuthorizer) AuthorizeServiceSweep(_ context.Context, principal identity.Principal) error {
	a.sweepCalls++
	if err := a.enter("AuthorizeServiceSweep"); err != nil {
		return err
	}
	if !principal.IsService() {
		return errors.New("the due-work sweep is not a tenant principal's query")
	}
	return nil
}

type sweepFixture struct {
	t     *testing.T
	store *sessionstore.Store
	seam  *countingStore
	clock *sweepClock
	auth  *sweepAuthorizer
	rec   *Reconciler

	service identity.Principal
	tenant  identity.Principal

	runtimes int
}

// newSweepFixture opens a real store on memstore and builds one replica over
// it. shards is passed through to WithControlShards so a full pass is cheap
// enough to drive repeatedly and so the fixed-shard cost claim has a number it
// can be compared against.
func newSweepFixture(t *testing.T, shards int) *sweepFixture {
	t.Helper()
	return newSweepFixtureWith(t, shards, nil)
}

// newSweepFixtureWith is newSweepFixture with the replica's configuration
// adjusted. It exists for the cases that need a page SMALLER than the due work,
// which is the only way to reach a second page read over a real store.
func newSweepFixtureWith(t *testing.T, shards int, tune func(*ReconcilerConfig)) *sweepFixture {
	t.Helper()
	store, clock := openSweepStore(t, shards)
	return newSweepReplicaWith(t, store, clock, "replica-a", tune)
}

// openSweepStore opens a real store on memstore and returns it WITH the clock
// it was given, so a caller building a second replica over the same store hands
// both the same value rather than reaching for a shared one.
func openSweepStore(t *testing.T, shards int) (*sessionstore.Store, *sweepClock) {
	t.Helper()
	ctx := context.Background()
	clock := newSweepClock()
	store, err := sessionstore.Open(ctx, memstore.New(),
		sessionstore.WithControlShards(shards),
		sessionstore.WithClock(sweepStoreClock{clock}),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(context.Background()); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return store, clock
}

// sweepStoreClock adapts the shared clock to sessionstore's own Clock, which
// names Now alone.
type sweepStoreClock struct{ clock *sweepClock }

func (c sweepStoreClock) Now() time.Time { return c.clock.Now() }

func newSweepReplica(t *testing.T, store *sessionstore.Store, clock *sweepClock, holder string) *sweepFixture {
	t.Helper()
	return newSweepReplicaWith(t, store, clock, holder, nil)
}

func newSweepReplicaWith(
	t *testing.T,
	store *sessionstore.Store,
	clock *sweepClock,
	holder string,
	tune func(*ReconcilerConfig),
) *sweepFixture {
	t.Helper()
	seam := newCountingStore(store)
	auth := &sweepAuthorizer{}
	config := ReconcilerConfig{
		Authorizer: auth, Due: seam, Settlement: seam, Claims: seam, Clock: clock,
		HolderID: holder, ClaimTTL: time.Minute, PageLimit: 32, MaxPages: 4,
	}
	if tune != nil {
		tune(&config)
	}
	rec, err := NewReconciler(config)
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}
	service, err := identity.NewPrincipal(sweepTenant, "factory-"+holder, identity.KindService)
	if err != nil {
		t.Fatal(err)
	}
	tenant, err := identity.NewPrincipal(sweepTenant, "actor-a", identity.KindActor)
	if err != nil {
		t.Fatal(err)
	}
	return &sweepFixture{t: t, store: store, seam: seam, clock: clock, auth: auth, rec: rec, service: service, tenant: tenant}
}

// runtimeID mints a distinct UUID per admitted command. It must be a UUID
// because scanCommandApplication parses the stored mapping and correlates a
// journal prefix against it; a non-UUID mapping correlates with nothing, which
// would make every evidence case pass for the wrong reason.
func (f *sweepFixture) runtimeID() sessionstore.RuntimeCommandID {
	f.runtimes++
	return sessionstore.RuntimeCommandID(fmt.Sprintf("2f1c7d1e-0f3a-4c5b-9f21-%012d", f.runtimes))
}

// admit puts one pending command in the inbox through the released store.
func (f *sweepFixture) admit(tenant sessionwire.TenantID, session, command string, deadline time.Time) sessionstore.InboxEntry {
	f.t.Helper()
	entry, _, err := f.store.AdmitCommand(context.Background(), sessionstore.AdmitCommandRequest{
		TenantID: tenant, SessionID: sessionwire.SessionID(session), CommandID: sessionwire.CommandID(command),
		ProposedRuntimeCommandID: f.runtimeID(), Kind: CommandInput,
		Payload: []byte(`{}`), AcceptedAt: sweepBase, ApplyDeadline: deadline,
	})
	if err != nil {
		f.t.Fatalf("AdmitCommand(%s/%s): %v", session, command, err)
	}
	return entry
}

// pass runs one full round-robin pass: one sweep per control shard.
func (f *sweepFixture) pass() []SweepResult {
	f.t.Helper()
	out := make([]SweepResult, 0, f.store.ControlShards())
	for range f.store.ControlShards() {
		result, err := f.rec.Sweep(context.Background(), f.service)
		if err != nil {
			f.t.Fatalf("Sweep: %v", err)
		}
		out = append(out, result)
	}
	return out
}

func totalRejected(results []SweepResult) int {
	total := 0
	for _, result := range results {
		total += result.Rejected
	}
	return total
}

func totalQueries(results []SweepResult) int {
	total := 0
	for _, result := range results {
		total += result.Queries
	}
	return total
}

func (f *sweepFixture) record(tenant sessionwire.TenantID, session, command string) sessionstore.InboxRecord {
	f.t.Helper()
	entry, err := f.store.GetCommand(context.Background(), sessionstore.GetCommandRequest{
		TenantID: tenant, SessionID: sessionwire.SessionID(session), CommandID: sessionwire.CommandID(command),
	})
	if err != nil {
		f.t.Fatalf("GetCommand(%s/%s): %v", session, command, err)
	}
	return entry.Record
}

// ---------------------------------------------------------------------------
// Step 1: a service principal, fixed shards, round-robin
// ---------------------------------------------------------------------------

// TestATenantPrincipalCannotInvokeTheSweep is step 1's second sentence, and the
// assertion that matters is not the error: it is that NOTHING was read. A
// reconciler that paged the shard and then discarded the answer would also
// return an error here, and it would have carried a cross-tenant page for a
// tenant principal on the way.
//
// The rotor is sampled too. An unauthorized call that advanced it would let a
// tenant principal choose which shard the next authorized sweep visits.
func TestATenantPrincipalCannotInvokeTheSweep(t *testing.T) {
	f := newSweepFixture(t, 4)
	f.admit(sweepTenant, "session-a", "command-a", sweepBase.Add(time.Minute))
	f.clock.Set(sweepBase.Add(time.Hour))

	if _, err := f.rec.Sweep(context.Background(), f.tenant); err == nil {
		t.Fatal("a tenant principal swept the control shards")
	}
	if got := f.seam.providerQueries(); got != 0 {
		t.Errorf("a refused sweep made %d provider queries; the refusal must precede every read", got)
	}
	if f.auth.controlCalls != 0 {
		t.Errorf("the sweep called AuthorizeControl %d times; it is not a caller's command", f.auth.controlCalls)
	}

	// The control, without which "nothing happened" is also the answer of a
	// reconciler that does nothing at all: the same shard, swept by the service
	// principal, settles the command.
	result, err := f.rec.Sweep(context.Background(), f.service)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if result.Shard != 0 {
		t.Errorf("the first authorized sweep visited shard %d; the refused call moved the rotor", result.Shard)
	}
	rest := f.pass()
	if got := result.Rejected + totalRejected(rest); got != 1 {
		t.Fatalf("a full pass by the service principal settled %d commands, want 1", got)
	}
}

// TestTheSweepAuthorizesAsAServiceAndNeverAsACommand is the structural half of
// the carry-forward, and it is the half a behavioural test cannot supply.
//
// admission.Authorizer declares BOTH decisions. AuthorizeServiceSweep exists
// because a command arrives at the httpapi or clientlink edge already
// authorized and admission repeats that decision as the durable mutation
// boundary -- a THIRD authorizing caller is how an unauthorized caller acquires
// a path. A sweep that also called AuthorizeControl would be that third caller,
// and no fixture could tell: an authorizer admitting both would be green.
func TestTheSweepAuthorizesAsAServiceAndNeverAsACommand(t *testing.T) {
	t.Parallel()

	selectors := selectorsIn(t, "reconciler.go")
	if !selectors["AuthorizeServiceSweep"] {
		t.Fatal("reconciler.go names no AuthorizeServiceSweep, so the sweep authorizes nothing")
	}
	if selectors["AuthorizeControl"] {
		t.Error("reconciler.go names AuthorizeControl; the sweep must not become a third authorizing caller of the command decision")
	}
	// The scan's own positive control. Against a file that does not name it,
	// the assertion above reports the same whether the scan works or is stuck.
	if !selectorsIn(t, "service.go")["AuthorizeControl"] {
		t.Fatal("the selector scan found no AuthorizeControl in service.go, where every admission path calls it; the scan is broken")
	}
}

// selectorsIn reports every selector name used anywhere in one production file
// of this package.
func selectorsIn(t *testing.T, name string) map[string]bool {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	out := map[string]bool{}
	ast.Inspect(file, func(n ast.Node) bool {
		if sel, ok := n.(*ast.SelectorExpr); ok {
			out[sel.Sel.Name] = true
		}
		return true
	})
	if len(out) == 0 {
		t.Fatalf("%s has no selectors at all; the scan is broken", name)
	}
	return out
}

// TestTheSweepVisitsEveryFixedShardRoundRobin is step 1's first sentence.
//
// Both halves are asserted. The shards a pass visits must be EVERY shard the
// store is committed to, exactly once -- a rotor that skipped one would leave
// whichever tenants hash into it unreconciled forever, and a count alone would
// not see that. And the sequence must repeat, so the pass after a pass covers
// the same set rather than drifting.
func TestTheSweepVisitsEveryFixedShardRoundRobin(t *testing.T) {
	f := newSweepFixture(t, 7)
	shards := f.store.ControlShards()
	if shards != 7 {
		t.Fatalf("ControlShards() = %d, want the 7 the store was opened with", shards)
	}

	for round := range 3 {
		var visited []int
		for _, result := range f.pass() {
			visited = append(visited, result.Shard)
		}
		want := make([]int, shards)
		for i := range want {
			want[i] = i
		}
		if !slices.Equal(visited, want) {
			t.Fatalf("round %d visited %v, want %v", round, visited, want)
		}
	}
}

// TestAFailingShardDoesNotStarveTheOthers is why the rotor advances before the
// work rather than after it. A shard whose page read fails forever would
// otherwise be swept forever and no other shard would ever be visited.
func TestAFailingShardDoesNotStarveTheOthers(t *testing.T) {
	f := newSweepFixture(t, 4)
	f.seam.failing = "ListDueCommands"

	var visited []int
	for range 4 {
		result, err := f.rec.Sweep(context.Background(), f.service)
		if err == nil {
			t.Fatal("a failing page read produced no error")
		}
		visited = append(visited, result.Shard)
	}
	if !slices.Equal(visited, []int{0, 1, 2, 3}) {
		t.Fatalf("four failing sweeps visited %v; a failing shard holds the rotor", visited)
	}
}

// ---------------------------------------------------------------------------
// Step 3: the safety predicate, at every boundary, in both directions
// ---------------------------------------------------------------------------

// TestTheSafetyPredicateIsMutatedAtEveryBoundaryInBothDirections is step 3's
// "ONLY" made checkable.
//
// "Reject only safe pending/expired claims with no application evidence" is a
// claim about what was NOT rejected, so every case below is driven at the
// boundary and one tick either side of it. The pairs are what make it a
// measurement rather than an assertion: `now == deadline` settles and
// `now == deadline - 1ns` does not, from records that differ in one nanosecond
// and nothing else.
//
// The half-open convention is SessionStore's own -- claimHeldAt and every
// deadline in that package hold up to but not including the instant -- and it
// is restated here rather than derived because a predicate is a total function
// and there is nothing to derive it from.
func TestTheSafetyPredicateIsMutatedAtEveryBoundaryInBothDirections(t *testing.T) {
	t.Parallel()

	now := sweepBase
	pending := func(deadline time.Time) sessionstore.InboxRecord {
		return sessionstore.InboxRecord{State: sessionstore.InboxStatePending, ApplyDeadline: deadline}
	}
	claimed := func(state sessionstore.InboxState, deadline, expiry time.Time) sessionstore.InboxRecord {
		return sessionstore.InboxRecord{
			State: state, ApplyDeadline: deadline,
			Claim: sessionstore.CommandClaim{LeaseEpoch: 7, ExpiresAt: expiry},
		}
	}

	for _, test := range []struct {
		name   string
		record sessionstore.InboxRecord
		want   Disposition
	}{
		// B, the apply deadline, against a pending record with no claim.
		{"pending one tick before the deadline", pending(now.Add(time.Nanosecond)), DispositionUnexpired},
		{"pending exactly at the deadline", pending(now), DispositionSettleable},
		{"pending one tick after the deadline", pending(now.Add(-time.Nanosecond)), DispositionSettleable},
		{"pending long past the deadline", pending(now.Add(-time.Hour)), DispositionSettleable},

		// C, the command claim, against a claimed record whose deadline has
		// passed. Only the claim moves.
		{"claimed with a claim live one tick longer", claimed(sessionstore.InboxStateClaimed, now.Add(-time.Hour), now.Add(time.Nanosecond)), DispositionClaimLive},
		{"claimed with a claim lapsing exactly now", claimed(sessionstore.InboxStateClaimed, now.Add(-time.Hour), now), DispositionSettleable},
		{"claimed with a claim lapsed one tick ago", claimed(sessionstore.InboxStateClaimed, now.Add(-time.Hour), now.Add(-time.Nanosecond)), DispositionSettleable},

		// The ONE case that reads the ARM ORDER rather than the rule: both
		// bounds are open, so both arms would refuse and only the reported
		// reason differs. The claim is the more specific fact -- a Host is
		// working on it now -- and it is pinned here so the order is not a
		// degree of freedom nothing reads.
		{"claimed, claim live, deadline still open", claimed(sessionstore.InboxStateClaimed, now.Add(time.Minute), now.Add(time.Minute)), DispositionClaimLive},

		// B again, from the other side: a lapsed claim does not license a
		// settlement while the apply deadline is still open. This is the
		// combination the due horizon actually produces -- min(deadline,
		// claim expiry) makes the row due at the claim's lapse.
		{"claimed, claim lapsed, deadline still open", claimed(sessionstore.InboxStateClaimed, now.Add(time.Minute), now.Add(-time.Minute)), DispositionUnexpired},

		// A, the state. Applying is never this reconciler's, at either claim
		// liveness, because the settlement that clears it rests on a journal
		// fence written at an epoch above the applying one and this reconciler
		// holds no lease.
		{"applying with a live claim", claimed(sessionstore.InboxStateApplying, now.Add(-time.Hour), now.Add(time.Minute)), DispositionApplying},
		{"applying with a lapsed claim", claimed(sessionstore.InboxStateApplying, now.Add(-time.Hour), now.Add(-time.Minute)), DispositionApplying},
		{"applying before its deadline", claimed(sessionstore.InboxStateApplying, now.Add(time.Hour), now.Add(-time.Minute)), DispositionApplying},

		// A's terminal arm. inboxDue files these not-due so they cannot reach
		// a page, but a row CAN settle between the page and the write.
		{"already applied", sessionstore.InboxRecord{State: sessionstore.InboxStateApplied, ApplyDeadline: now.Add(-time.Hour)}, DispositionTerminal},
		{"already rejected", sessionstore.InboxRecord{State: sessionstore.InboxStateRejected, ApplyDeadline: now.Add(-time.Hour)}, DispositionTerminal},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := settleable(test.record, now); got != test.want {
				t.Fatalf("settleable() = %v, want %v", got, test.want)
			}
		})
	}
}

// TestThePredicateCoversEveryDurableState is the anti-vacuity floor for the
// table above: a state this build does not classify would fall to whichever arm
// happened to be last, and a table driven from literals cannot notice one it
// does not name. The five states are derived from the package that declares
// them rather than listed here, so a sixth arrives as a failure.
func TestThePredicateCoversEveryDurableState(t *testing.T) {
	t.Parallel()

	states := declaredInboxStates(t)
	if len(states) < 5 {
		t.Fatalf("the pinned store declares %d inbox states; the derivation is broken", len(states))
	}
	seen := map[sessionstore.InboxState]bool{}
	for _, test := range predicateStateProbes(sweepBase) {
		seen[test.record.State] = true
	}
	for _, state := range states {
		if !seen[state] {
			t.Errorf("no probe drives the predicate at inbox state %q", state)
		}
	}
}

// predicateStateProbes is the state axis of the table above, in a form the
// coverage floor can read.
func predicateStateProbes(now time.Time) []struct {
	record sessionstore.InboxRecord
	want   Disposition
} {
	claim := sessionstore.CommandClaim{LeaseEpoch: 7, ExpiresAt: now.Add(-time.Minute)}
	return []struct {
		record sessionstore.InboxRecord
		want   Disposition
	}{
		{sessionstore.InboxRecord{State: sessionstore.InboxStatePending, ApplyDeadline: now.Add(-time.Hour)}, DispositionSettleable},
		{sessionstore.InboxRecord{State: sessionstore.InboxStateClaimed, ApplyDeadline: now.Add(-time.Hour), Claim: claim}, DispositionSettleable},
		{sessionstore.InboxRecord{State: sessionstore.InboxStateApplying, ApplyDeadline: now.Add(-time.Hour), Claim: claim}, DispositionApplying},
		{sessionstore.InboxRecord{State: sessionstore.InboxStateApplied, ApplyDeadline: now.Add(-time.Hour)}, DispositionTerminal},
		{sessionstore.InboxRecord{State: sessionstore.InboxStateRejected, ApplyDeadline: now.Add(-time.Hour)}, DispositionTerminal},
	}
}

// declaredInboxStates parses the PINNED sessionstore source for every
// InboxState constant it declares.
//
// The subject is derived rather than listed for the reason this module's
// classification pattern has been found wrong four times: a hand-written set of
// store codes is a second authority that nothing updates when the store gains a
// member. Constants are not reflectable, so the mechanism this reads is the
// declaration itself.
func declaredInboxStates(t *testing.T) []sessionstore.InboxState {
	t.Helper()
	var out []sessionstore.InboxState
	for _, value := range declaredStringConstants(t, "inbox.go", "InboxState") {
		out = append(out, sessionstore.InboxState(value))
	}
	return out
}

// ---------------------------------------------------------------------------
// Step 2's named scenarios, over the released store
// ---------------------------------------------------------------------------

// TestStartupRecoverySettlesCommandsAdmittedBeforeThisProcessExisted is
// scenario 1 and scenario 3 together: commands admitted and then abandoned --
// a Factory that crashed after admission -- are found by a reconciler that has
// never seen them, in one round-robin pass, and settled.
//
// The pass is what makes it startup recovery rather than "the sweep works": the
// rotor starts at zero on a fresh process and the sessions were placed in
// whichever shards their identities hash into, which no test chooses.
func TestStartupRecoverySettlesCommandsAdmittedBeforeThisProcessExisted(t *testing.T) {
	f := newSweepFixture(t, 4)
	const sessions = 9
	for i := range sessions {
		f.admit(sweepTenant, fmt.Sprintf("session-%02d", i), fmt.Sprintf("command-%02d", i), sweepBase.Add(time.Minute))
	}
	f.clock.Set(sweepBase.Add(time.Hour))

	if got := totalRejected(f.pass()); got != sessions {
		t.Fatalf("one pass settled %d of %d abandoned commands", got, sessions)
	}
	for i := range sessions {
		record := f.record(sweepTenant, fmt.Sprintf("session-%02d", i), fmt.Sprintf("command-%02d", i))
		if record.State != sessionstore.InboxStateRejected {
			t.Fatalf("session-%02d is %q, want rejected", i, record.State)
		}
		if record.Rejection == nil || record.Rejection.Code != sessionwire.ErrorCodeRuntimeUnavailable {
			t.Fatalf("session-%02d rejection = %+v", i, record.Rejection)
		}
	}
	// A second pass is the idempotence half and the anti-vacuity half at once:
	// a terminal command is filed not-due, so nothing is found and nothing is
	// written a second time.
	second := f.pass()
	if got := totalRejected(second); got != 0 {
		t.Fatalf("a second pass settled %d already-terminal commands", got)
	}
}

// TestAnUnexpiredCommandAndALiveHostClaimAreLeftAloneAtNoCost is scenario 6,
// "existing Host owner", and it is the case where the recorded disposition is
// the WEAKER observable.
//
// A reconciler whose predicate admitted these rows would still report them as
// claim_live or unexpired, because the store's own claim rule refuses the write
// and this package maps that refusal onto the same disposition. What cannot be
// faked is the COST: a row this sweep left alone costs the page it arrived on
// and nothing else -- no reconciliation claim, no compare-and-swap.
func TestAnUnexpiredCommandAndALiveHostClaimAreLeftAloneAtNoCost(t *testing.T) {
	for _, test := range []struct {
		name    string
		arrange func(*sweepFixture) sessionstore.InboxEntry
		want    Disposition
	}{
		{"the apply deadline has not passed", func(f *sweepFixture) sessionstore.InboxEntry {
			entry := f.admit(sweepTenant, "session-a", "command-a", sweepBase.Add(time.Hour))
			// The row is made DUE by a lapsed Host claim while its apply
			// deadline is still open, which is the only way an unexpired
			// command reaches a page at all.
			return f.claim(entry, 3, sweepBase.Add(time.Minute))
		}, DispositionUnexpired},
		{"a Host holds a live claim", func(f *sweepFixture) sessionstore.InboxEntry {
			entry := f.admit(sweepTenant, "session-a", "command-a", sweepBase.Add(time.Minute))
			return f.claim(entry, 3, sweepBase.Add(45*time.Minute))
		}, DispositionClaimLive},
		{"a Host is applying", func(f *sweepFixture) sessionstore.InboxEntry {
			entry := f.admit(sweepTenant, "session-a", "command-a", sweepBase.Add(time.Minute))
			return f.applying(f.claim(entry, 3, sweepBase.Add(45*time.Minute)), 3, sweepBase.Add(50*time.Minute))
		}, DispositionApplying},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newSweepFixture(t, 4)
			before := test.arrange(f)
			f.clock.Set(sweepBase.Add(30 * time.Minute))

			results := f.pass()
			if got := totalRejected(results); got != 0 {
				t.Fatalf("the sweep settled %d rows it had no licence to settle", got)
			}
			found := 0
			for _, result := range results {
				found += result.Dispositions[test.want]
			}
			if found != 1 {
				t.Fatalf("the row was recorded as %v %d times, want once (%v)", test.want, found, results)
			}
			// The cost observable. One page per shard and nothing else: an
			// acquisition or a settlement attempt would add to both counters.
			if got, want := totalQueries(results), f.store.ControlShards(); got != want {
				t.Errorf("the pass made %d queries, want %d -- one page per shard and no write", got, want)
			}
			if got := f.seam.count("AcquireReconciliationClaim"); got != 0 {
				t.Errorf("the sweep took %d reconciliation claims for a row it may not settle", got)
			}
			if got := f.seam.count("RejectCommand"); got != 0 {
				t.Errorf("the sweep attempted %d settlements it had no licence for", got)
			}
			after := f.record(sweepTenant, "session-a", "command-a")
			if after.State != before.Record.State {
				t.Errorf("the record moved from %q to %q", before.Record.State, after.State)
			}
			if after.Rejection != nil {
				t.Errorf("the record carries a rejection: %+v", after.Rejection)
			}
		})
	}
}

// claim puts a Host claim on an admitted command through the released store.
func (f *sweepFixture) claim(entry sessionstore.InboxEntry, epoch uint64, expiry time.Time) sessionstore.InboxEntry {
	f.t.Helper()
	claimed, err := f.store.ClaimCommand(context.Background(), sessionstore.ClaimCommandRequest{
		TenantID: entry.Record.TenantID, SessionID: entry.Record.SessionID, CommandID: entry.Record.CommandID,
		ExpectedRevision: entry.Revision, LeaseEpoch: epoch, ClaimExpiresAt: expiry,
	})
	if err != nil {
		f.t.Fatalf("ClaimCommand: %v", err)
	}
	return claimed
}

func (f *sweepFixture) applying(entry sessionstore.InboxEntry, epoch uint64, expiry time.Time) sessionstore.InboxEntry {
	f.t.Helper()
	applying, err := f.store.BeginApplyingCommand(context.Background(), sessionstore.BeginApplyingCommandRequest{
		TenantID: entry.Record.TenantID, SessionID: entry.Record.SessionID, CommandID: entry.Record.CommandID,
		ExpectedRevision: entry.Revision, LeaseEpoch: epoch, ClaimExpiresAt: expiry,
	})
	if err != nil {
		f.t.Fatalf("BeginApplyingCommand: %v", err)
	}
	return applying
}

// TestALapsedHostClaimPastTheDeadlineIsSettled is the other half of scenario 8,
// runtime disappearance: a Host CLAIMED the command and then vanished without
// entering applying. Its claim lapsed, the apply deadline passed, and nothing
// in the journal names the command.
//
// It is the one settled row whose record carries a NONZERO lease epoch, and
// that makes it the only case that can see the sweep name an epoch. Naming one
// -- any one -- would be measured by SessionStore's epoch fence against the
// claim's high-water mark, and the reconciler would start failing on exactly
// the rows it exists to clear. Zero is what keeps it out of that comparison,
// and this is the case that reads it.
func TestALapsedHostClaimPastTheDeadlineIsSettled(t *testing.T) {
	f := newSweepFixture(t, 4)
	entry := f.admit(sweepTenant, "session-a", "command-a", sweepBase.Add(time.Minute))
	f.claim(entry, 7, sweepBase.Add(2*time.Minute))
	f.clock.Set(sweepBase.Add(time.Hour))

	results := f.pass()
	if got := totalRejected(results); got != 1 {
		t.Fatalf("a lapsed Host claim past the apply deadline was not settled: %d", got)
	}
	record := f.record(sweepTenant, "session-a", "command-a")
	if record.State != sessionstore.InboxStateRejected {
		t.Fatalf("the record is %q, want rejected", record.State)
	}
	// The claim the Host left behind is preserved on the terminal record: the
	// row keeps naming the lease that held it, which is the store's rule and
	// not something this sweep may rewrite.
	if record.Claim.LeaseEpoch != 7 {
		t.Errorf("the settled record's claim epoch is %d, want the Host's 7", record.Claim.LeaseEpoch)
	}
}

// TestApplicationEvidenceWinsOverAnExpiredDeadline is scenario 8, runtime
// disappearance, and it is step 3's "prefix wins" driven through the real
// journal rather than through a fake that agrees with the reconciler.
//
// The reconciler's predicate says nothing about evidence -- it cannot, since
// reading the journal per row is the cost step 4 forbids. The authority is
// SessionStore's own scanCommandApplication, consulted inside RejectCommand,
// and the two outcomes it distinguishes are driven: a prefix at the tip whose
// writer may still be alive (unresolved), and a prefix followed by the public
// event that carried its effect (committed). Both refuse.
func TestApplicationEvidenceWinsOverAnExpiredDeadline(t *testing.T) {
	for _, test := range []struct {
		name   string
		commit bool
	}{
		{"a prefix at the tip", false},
		{"a prefix and its committed effect", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newSweepFixture(t, 4)
			entry := f.admit(sweepTenant, "session-a", "command-a", sweepBase.Add(time.Minute))

			writer, err := f.store.OpenJournal(context.Background(), sessionstore.OpenJournalRequest{
				TenantID: sweepTenant, SessionID: "session-a",
			})
			if err != nil {
				t.Fatalf("OpenJournal: %v", err)
			}
			t.Cleanup(func() { _ = writer.Close(context.Background()) })
			if _, err := writer.Append(context.Background(), sessionstore.Envelope{
				Kind:             sessionstore.EnvelopeKindApplicationPrefix,
				CommandID:        entry.Record.CommandID,
				RuntimeCommandID: coreuuid.MustParse(string(entry.Record.RuntimeCommandID)),
				CommandKind:      string(entry.Record.Kind),
			}); err != nil {
				t.Fatalf("Append prefix: %v", err)
			}
			if test.commit {
				if _, err := writer.Append(context.Background(), sessionstore.Envelope{
					Kind:    sessionstore.EnvelopeKindPublicEvent,
					EventID: "event-a",
					Public:  sessionstore.BodySlot{Inline: []byte(`{"kind":"effect"}`)},
				}); err != nil {
					t.Fatalf("Append effect: %v", err)
				}
			}

			f.clock.Set(sweepBase.Add(time.Hour))
			results := f.pass()
			if got := totalRejected(results); got != 0 {
				t.Fatalf("the sweep settled a command the journal says it may not: %d", got)
			}
			evidence := 0
			for _, result := range results {
				evidence += result.Dispositions[DispositionEvidence]
			}
			if evidence != 1 {
				t.Fatalf("the row was recorded as evidence %d times, want once (%v)", evidence, results)
			}
			if state := f.record(sweepTenant, "session-a", "command-a").State; state != sessionstore.InboxStatePending {
				t.Fatalf("the record is %q, want pending", state)
			}
			// The positive control, in the same store and the same pass: an
			// identical command in a session with an EMPTY journal is settled.
			// Without it, "nothing was rejected" is also the output of a
			// reconciler that stopped settling anything.
			f.admit(sweepTenant, "session-b", "command-b", sweepBase.Add(time.Minute))
			if got := totalRejected(f.pass()); got != 1 {
				t.Fatalf("the control command with no journal evidence was not settled: %d", got)
			}
		})
	}
}

// TestAColdSessionIsSettledWithoutReadingAnyPlacementState is scenario 7.
//
// "Cold placement" here is a session that has never been anywhere: no catalog
// record, no registry observation, no journal. The claim is not merely that the
// command is settled but that the reconciler ASKS none of those questions --
// and that is asserted structurally as well as behaviourally, because a
// reconciler that read the directory and ignored the answer would also settle
// it.
func TestAColdSessionIsSettledWithoutReadingAnyPlacementState(t *testing.T) {
	f := newSweepFixture(t, 4)
	f.admit(sweepTenant, "session-cold", "command-cold", sweepBase.Add(time.Minute))
	f.clock.Set(sweepBase.Add(time.Hour))

	if got := totalRejected(f.pass()); got != 1 {
		t.Fatalf("a cold session's abandoned command was not settled: %d", got)
	}
	if _, err := f.store.GetCatalogEntry(context.Background(), sessionstore.GetCatalogEntryRequest{
		TenantID: sweepTenant, SessionID: "session-cold",
	}); err == nil {
		t.Fatal("the fixture created a catalog record, so this case is not cold")
	}

	// The structural half: the reconciler's configuration names no catalog, no
	// directory and no placement controller, so there is no seam through which
	// it could consult one. A field added later fails here rather than being
	// noticed by review.
	cfg := reflect.TypeOf(ReconcilerConfig{})
	want := map[string]bool{
		"Authorizer": true, "Due": true, "Settlement": true, "Claims": true, "Clock": true,
		"HolderID": true, "ClaimTTL": true, "PageLimit": true, "MaxPages": true,
	}
	for i := range cfg.NumField() {
		if name := cfg.Field(i).Name; !want[name] {
			t.Errorf("ReconcilerConfig declares %s; a due-command sweep reads no placement or catalog state", name)
		}
	}
	if cfg.NumField() != len(want) {
		t.Errorf("ReconcilerConfig has %d fields, want the %d listed", cfg.NumField(), len(want))
	}
}

// TestTwoReplicasSweepingOneStoreSettleEachCommandExactlyOnce is scenario 4,
// and it is a CONCURRENCY claim: the two replicas run at the same time, over
// one store, under -race. A sequential version of this test proves nothing
// about the race it is named for.
//
// What is asserted is the invariant, not the winner. The reconciliation claim
// is explicitly not a fence, so either replica may be the one that settles any
// given command; what may not happen is a command settled twice, a command left
// unsettled, or an error reported on the losing path -- losing is the claim
// working, not a failure.
func TestTwoReplicasSweepingOneStoreSettleEachCommandExactlyOnce(t *testing.T) {
	store, clock := openSweepStore(t, 4)
	a := newSweepReplica(t, store, clock, "replica-a")
	b := newSweepReplica(t, store, clock, "replica-b")

	const sessions = 12
	for i := range sessions {
		a.admit(sweepTenant, fmt.Sprintf("session-%02d", i), fmt.Sprintf("command-%02d", i), sweepBase.Add(time.Minute))
	}
	clock.Set(sweepBase.Add(time.Hour))

	// A BARRIER BEFORE EVERY SWEEP, so the two replicas enter the SAME shard at
	// the same moment. Without it the goroutines are free to interleave in a
	// way that never contends, and a concurrency test that never produced
	// contention would pass for the same reason a sequential one would.
	rounds := 2 * store.ControlShards()
	barrier := make(chan struct{})
	var ready sync.WaitGroup
	var wg sync.WaitGroup
	var mu sync.Mutex
	rejected, deferred := 0, 0
	failures := make(chan error, 2*rounds)
	for _, replica := range []*sweepFixture{a, b} {
		wg.Add(1)
		ready.Add(1)
		go func() {
			defer wg.Done()
			ready.Done()
			<-barrier
			// Two full passes each, so a replica that lost a shard's work to
			// the other on the first pass meets what is left on the second.
			for range rounds {
				result, err := replica.rec.Sweep(context.Background(), replica.service)
				if err != nil {
					failures <- err
					return
				}
				mu.Lock()
				rejected += result.Rejected
				deferred += result.Dispositions[DispositionDeferred]
				mu.Unlock()
			}
		}()
	}
	ready.Wait()
	close(barrier)
	wg.Wait()
	close(failures)
	// Reported rather than asserted: contention is a property of the schedule,
	// so a run that happened not to contend is not a failure -- but a run that
	// NEVER contends, over repetitions, means this test is measuring nothing
	// and the number is what says so.
	t.Logf("the two replicas deferred %d rows to each other over %d rounds", deferred, rounds)
	for err := range failures {
		t.Errorf("a concurrent sweep failed: %v", err)
	}
	if rejected != sessions {
		t.Fatalf("two concurrent replicas settled %d commands, want exactly %d -- one per command", rejected, sessions)
	}
	for i := range sessions {
		if state := a.record(sweepTenant, fmt.Sprintf("session-%02d", i), fmt.Sprintf("command-%02d", i)).State; state != sessionstore.InboxStateRejected {
			t.Fatalf("session-%02d is %q after two concurrent replicas swept it", i, state)
		}
	}
}

// TestALapsedReconciliationClaimIsTakenOverAndALiveOneIsNot is scenario 5, and
// it is the claim's two directions in one test because one of them alone is
// satisfied by a reconciler that ignores claims entirely.
func TestALapsedReconciliationClaimIsTakenOverAndALiveOneIsNot(t *testing.T) {
	for _, test := range []struct {
		name     string
		expiry   time.Duration
		sweepAt  time.Duration
		settled  int
		wantLeft Disposition
	}{
		// MaxReconciliationClaimTTL is five minutes, so "another replica is
		// working now" is driven by sweeping INSIDE the other replica's
		// horizon rather than by writing a longer one the store refuses.
		{"a crashed replica's claim has lapsed", 4 * time.Minute, time.Hour, 1, DispositionRejected},
		{"another replica is working now", 4 * time.Minute, 2 * time.Minute, 0, DispositionDeferred},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newSweepFixture(t, 4)
			f.admit(sweepTenant, "session-a", "command-a", sweepBase.Add(time.Minute))
			// The other replica's claim, written at the base instant. Its
			// horizon is what decides, and MaxReconciliationClaimTTL bounds how
			// far ahead of the store's clock it may be placed.
			if _, err := f.store.AcquireReconciliationClaim(context.Background(), sessionstore.AcquireReconciliationClaimRequest{
				TenantID: sweepTenant, SessionID: "session-a",
				HolderID: "replica-crashed", ExpiresAt: sweepBase.Add(test.expiry),
			}); err != nil {
				t.Fatalf("AcquireReconciliationClaim: %v", err)
			}
			f.clock.Set(sweepBase.Add(test.sweepAt))

			results := f.pass()
			if got := totalRejected(results); got != test.settled {
				t.Fatalf("the sweep settled %d commands, want %d", got, test.settled)
			}
			found := 0
			for _, result := range results {
				found += result.Dispositions[test.wantLeft]
			}
			if found != 1 {
				t.Fatalf("the row was recorded as %v %d times, want once (%v)", test.wantLeft, found, results)
			}
			// The deferral must not be a write. A losing replica that rewrote
			// the row would restamp the winner's claim under its own name,
			// which is the one way this record could take work away from the
			// replica actually doing it.
			if test.settled == 0 {
				if got := f.seam.count("RejectCommand"); got != 0 {
					t.Errorf("a deferring sweep attempted %d settlements", got)
				}
				if got := f.seam.count("ReleaseReconciliationClaim"); got != 0 {
					t.Errorf("a deferring sweep released %d claims it never took", got)
				}
			}
		})
	}
}

// TestALostCreateRaceIsResolvedWithinTheSameSweep reads the bounded retry.
//
// AcquireReconciliationClaim CREATES when it finds no record, and a create that
// loses that race is reported as a CONFLICT -- the store's own instruction is
// that the caller re-reads and meets the live claim on the ordinary path. That
// is the first moment of every session's reconciliation, since the claim record
// does not exist until somebody takes it, so without the retry two replicas
// starting together both defer and the row waits a whole pass.
//
// The second attempt SUCCEEDS here, which is what makes the retry observable:
// with one attempt the row is deferred and settled a pass later, and "settled
// eventually" is true of both.
func TestALostCreateRaceIsResolvedWithinTheSameSweep(t *testing.T) {
	t.Parallel()

	f := newFakeFixture(t, nil)
	f.due.pages = []sessionstore.DueCommandPage{duePage("", abandoned("session-a", "command-a"))}
	f.claims.conflictOnce = true

	result, err := f.rec.Sweep(context.Background(), f.principal)
	if err != nil {
		t.Fatalf("a lost create race was reported as a failure: %v", err)
	}
	if result.Rejected != 1 {
		t.Fatalf("the row was not settled in the sweep that met it: %+v", result)
	}
	if result.Dispositions[DispositionDeferred] != 0 {
		t.Errorf("a lost create race was recorded as a deferral")
	}
	if f.claims.attempts != 2 {
		t.Errorf("the claim was attempted %d times, want 2", f.claims.attempts)
	}

	// The bound. A conflict this replica cannot resolve inside its attempts is
	// a DEFERRAL rather than a fault and rather than an unbounded loop: another
	// replica is writing the record right now, which is the same operational
	// fact a held claim reports.
	g := newFakeFixture(t, nil)
	g.due.pages = []sessionstore.DueCommandPage{duePage("", abandoned("session-a", "command-a"))}
	g.claims.acquireErr = &sessionstore.ReconcileError{Code: sessionstore.ReconcileErrorConflict}
	stubborn, err := g.rec.Sweep(context.Background(), g.principal)
	if err != nil {
		t.Fatalf("a persistent conflict was reported as a failure: %v", err)
	}
	if stubborn.Dispositions[DispositionDeferred] != 1 || stubborn.Rejected != 0 {
		t.Fatalf("result = %+v, want the row deferred and nothing settled", stubborn)
	}
	if g.claims.attempts != 2 {
		t.Errorf("a persistent conflict was attempted %d times; the retry is not bounded at two", g.claims.attempts)
	}
}

// TestTheSweepReleasesEveryClaimItTookAndNothingElse is the three-question
// answer for the release site, driven rather than reasoned.
//
// Q1, per statement: one release per session claimed, counted at the seam and
// reported on the result, and the two are compared. Q2, the positive
// observable: a session this sweep never claimed keeps its claim, so a release
// that walked the wrong set is visible. Q3: the release is synchronous inside
// Sweep, so the sample needs no pump.
func TestTheSweepReleasesEveryClaimItTookAndNothingElse(t *testing.T) {
	f := newSweepFixture(t, 4)
	const sessions = 5
	for i := range sessions {
		f.admit(sweepTenant, fmt.Sprintf("session-%02d", i), fmt.Sprintf("command-%02d", i), sweepBase.Add(time.Minute))
	}
	// The bystander: a session with a live claim of its own and NO due work,
	// so no sweep has any business touching it.
	if _, err := f.store.AcquireReconciliationClaim(context.Background(), sessionstore.AcquireReconciliationClaimRequest{
		TenantID: sweepTenant, SessionID: "session-bystander",
		HolderID: "replica-other", ExpiresAt: sweepBase.Add(4 * time.Minute),
	}); err != nil {
		t.Fatalf("AcquireReconciliationClaim: %v", err)
	}
	f.clock.Set(sweepBase.Add(2 * time.Minute))

	results := f.pass()
	claimed, released := 0, 0
	for _, result := range results {
		claimed += result.Claimed
		released += result.Released
	}
	if claimed != sessions || released != sessions {
		t.Fatalf("the pass claimed %d and released %d, want %d of each", claimed, released, sessions)
	}
	if got := f.seam.count("AcquireReconciliationClaim"); got != claimed {
		t.Errorf("the seam saw %d acquisitions and the result reports %d", got, claimed)
	}
	if got := f.seam.count("ReleaseReconciliationClaim"); got != released {
		t.Errorf("the seam saw %d releases and the result reports %d", got, released)
	}
	for i := range sessions {
		if _, err := f.store.GetReconciliationClaim(context.Background(), sessionstore.GetReconciliationClaimRequest{
			TenantID: sweepTenant, SessionID: sessionwire.SessionID(fmt.Sprintf("session-%02d", i)),
		}); !isReconcileCode(err, sessionstore.ReconcileErrorLapsed) {
			t.Errorf("session-%02d's claim is %v, want lapsed after the sweep released it", i, err)
		}
	}
	// The bystander is untouched and STILL LIVE, which is the assertion a
	// release that walked every claim rather than its own would fail.
	entry, err := f.store.GetReconciliationClaim(context.Background(), sessionstore.GetReconciliationClaimRequest{
		TenantID: sweepTenant, SessionID: "session-bystander",
	})
	if err != nil {
		t.Fatalf("the bystander's claim was released by a sweep that never took it: %v", err)
	}
	if entry.Claim.HolderID != "replica-other" {
		t.Errorf("the bystander's claim is held by %q", entry.Claim.HolderID)
	}
}

func isReconcileCode(err error, code sessionstore.ReconcileErrorCode) bool {
	var target *sessionstore.ReconcileError
	return errors.As(err, &target) && target.Code == code
}

// TestAFailedReleaseIsSwallowedAndReported is the other half of the release
// site's Q2. A claim licenses nothing and a lapsed claim needs no cleanup, so a
// release failure must not replace a correct settlement with an error -- but a
// swallowed error with no observable is indistinguishable from one that never
// happened, which is exactly the shape this repository has paid for five times.
func TestAFailedReleaseIsSwallowedAndReported(t *testing.T) {
	f := newSweepFixture(t, 4)
	f.admit(sweepTenant, "session-a", "command-a", sweepBase.Add(time.Minute))
	f.clock.Set(sweepBase.Add(time.Hour))
	f.seam.failing = "ReleaseReconciliationClaim"

	results := f.pass()
	if got := totalRejected(results); got != 1 {
		t.Fatalf("a release failure lost the settlement: %d rejected", got)
	}
	failures, released := 0, 0
	for _, result := range results {
		failures += result.ReleaseFailures
		released += result.Released
	}
	if failures != 1 {
		t.Fatalf("the sweep reports %d release failures, want 1", failures)
	}
	if released != 0 {
		t.Fatalf("the sweep reports %d successful releases while every release failed", released)
	}
}

// ---------------------------------------------------------------------------
// Step 2: the periodic bound
// ---------------------------------------------------------------------------

// fakeDue serves prepared pages, so a bound can be driven without manufacturing
// thousands of real rows, and records every request it received.
type fakeDue struct {
	faultInjector
	shards int
	pages  []sessionstore.DueCommandPage
	served int
	reqs   []sessionstore.ListDueCommandsRequest
}

func (d *fakeDue) ControlShards() int { return d.shards }

func (d *fakeDue) ListDueCommands(_ context.Context, req sessionstore.ListDueCommandsRequest) (sessionstore.DueCommandPage, error) {
	d.reqs = append(d.reqs, req)
	if err := d.enter("ListDueCommands"); err != nil {
		return sessionstore.DueCommandPage{}, err
	}
	if d.served >= len(d.pages) {
		return sessionstore.DueCommandPage{Limit: req.Limit}, nil
	}
	page := d.pages[d.served]
	d.served++
	return page, nil
}

type fakeSettlement struct {
	faultInjector
	err   error
	calls int
}

func (s *fakeSettlement) RejectCommand(_ context.Context, req sessionstore.RejectCommandRequest) (sessionstore.InboxEntry, error) {
	s.calls++
	if err := s.enter("RejectCommand"); err != nil {
		return sessionstore.InboxEntry{}, err
	}
	if s.err != nil {
		return sessionstore.InboxEntry{}, s.err
	}
	return sessionstore.InboxEntry{Record: sessionstore.InboxRecord{
		TenantID: req.TenantID, SessionID: req.SessionID, CommandID: req.CommandID,
		State: sessionstore.InboxStateRejected,
	}, Revision: req.ExpectedRevision + 1}, nil
}

type fakeClaims struct {
	faultInjector
	acquireErr error
	// conflictOnce answers the FIRST acquisition with the lost-create-race
	// conflict the store reports, and the second on the ordinary path.
	conflictOnce bool
	attempts     int
	acquired     int
	released     int
}

func (c *fakeClaims) AcquireReconciliationClaim(_ context.Context, req sessionstore.AcquireReconciliationClaimRequest) (sessionstore.ReconciliationClaimEntry, error) {
	if err := c.enter("AcquireReconciliationClaim"); err != nil {
		return sessionstore.ReconciliationClaimEntry{}, err
	}
	c.attempts++
	if c.conflictOnce && c.attempts == 1 {
		return sessionstore.ReconciliationClaimEntry{}, &sessionstore.ReconcileError{Code: sessionstore.ReconcileErrorConflict}
	}
	if c.acquireErr != nil {
		return sessionstore.ReconciliationClaimEntry{}, c.acquireErr
	}
	c.acquired++
	return sessionstore.ReconciliationClaimEntry{Claim: sessionstore.ReconciliationClaim{
		TenantID: req.TenantID, SessionID: req.SessionID, HolderID: req.HolderID, ExpiresAt: req.ExpiresAt,
	}, Revision: 1}, nil
}

func (c *fakeClaims) ReleaseReconciliationClaim(_ context.Context, req sessionstore.ReleaseReconciliationClaimRequest) (sessionstore.ReconciliationClaimEntry, error) {
	if err := c.enter("ReleaseReconciliationClaim"); err != nil {
		return sessionstore.ReconciliationClaimEntry{}, err
	}
	c.released++
	return sessionstore.ReconciliationClaimEntry{Claim: sessionstore.ReconciliationClaim{
		TenantID: req.TenantID, SessionID: req.SessionID, HolderID: req.HolderID,
	}, Revision: 2}, nil
}

type fakeFixture struct {
	rec        *Reconciler
	auth       *sweepAuthorizer
	due        *fakeDue
	settlement *fakeSettlement
	claims     *fakeClaims
	clock      *sweepClock
	principal  identity.Principal
}

func newFakeFixture(t *testing.T, cfg func(*ReconcilerConfig)) *fakeFixture {
	t.Helper()
	auth := &sweepAuthorizer{}
	due := &fakeDue{shards: 3}
	settlement := &fakeSettlement{}
	claims := &fakeClaims{}
	clock := newSweepClock()
	config := ReconcilerConfig{
		Authorizer: auth, Due: due, Settlement: settlement, Claims: claims, Clock: clock,
		HolderID: "replica-fake", ClaimTTL: time.Minute, PageLimit: 2, MaxPages: 3,
	}
	if cfg != nil {
		cfg(&config)
	}
	rec, err := NewReconciler(config)
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}
	principal, err := identity.NewPrincipal(sweepTenant, "factory-fake", identity.KindService)
	if err != nil {
		t.Fatal(err)
	}
	return &fakeFixture{rec, auth, due, settlement, claims, clock, principal}
}

func duePage(cursor sessionwire.Cursor, records ...sessionstore.InboxRecord) sessionstore.DueCommandPage {
	page := sessionstore.DueCommandPage{Limit: 2, Examined: len(records), NextCursor: cursor}
	for i, record := range records {
		page.Commands = append(page.Commands, sessionstore.DueCommand{
			Entry: sessionstore.InboxEntry{Record: record, Revision: uint64(i + 1), AcceptedOrder: uint64(i + 1)},
		})
	}
	return page
}

func abandoned(session, command string) sessionstore.InboxRecord {
	return sessionstore.InboxRecord{
		TenantID: sweepTenant, SessionID: sessionwire.SessionID(session), CommandID: sessionwire.CommandID(command),
		Kind: CommandInput, State: sessionstore.InboxStatePending,
		AcceptedAt: sweepBase.Add(-time.Hour), ApplyDeadline: sweepBase.Add(-time.Minute),
	}
}

// TestOneSweepIsBoundedByMaxPagesAndReportsTheRemainder is step 2's "periodic
// bounds". The bound is on a PASS, not on the work: what is left is still due
// and is met by the next pass over the shard, and Truncated is what says so.
func TestOneSweepIsBoundedByMaxPagesAndReportsTheRemainder(t *testing.T) {
	f := newFakeFixture(t, nil)
	f.due.pages = []sessionstore.DueCommandPage{
		duePage("cursor-1", abandoned("session-a", "command-a"), abandoned("session-b", "command-b")),
		duePage("cursor-2", abandoned("session-c", "command-c"), abandoned("session-d", "command-d")),
		duePage("cursor-3", abandoned("session-e", "command-e"), abandoned("session-f", "command-f")),
		duePage("cursor-4", abandoned("session-g", "command-g")),
	}

	result, err := f.rec.Sweep(context.Background(), f.principal)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if result.Pages != 3 {
		t.Fatalf("the sweep read %d pages, want the configured MaxPages of 3", result.Pages)
	}
	if !result.Truncated {
		t.Error("a sweep that stopped at MaxPages with a cursor outstanding is not reported truncated")
	}
	if result.Rejected != 6 {
		t.Fatalf("the sweep settled %d rows, want the 6 on the pages it read", result.Rejected)
	}
	if f.due.reqs[0].DueAtOrBefore.IsZero() {
		t.Error("the first page named no due bound")
	}
	for i, req := range f.due.reqs[1:] {
		if !req.DueAtOrBefore.IsZero() {
			t.Errorf("continuation %d named a due bound as well as a cursor; the store refuses two answers to one question", i+1)
		}
		if req.Cursor == "" {
			t.Errorf("continuation %d carried no cursor", i+1)
		}
	}

	// The un-truncated half, without which "truncated" could be constant.
	g := newFakeFixture(t, nil)
	g.due.pages = []sessionstore.DueCommandPage{duePage("", abandoned("session-a", "command-a"))}
	short, err := g.rec.Sweep(context.Background(), g.principal)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if short.Truncated || short.Pages != 1 {
		t.Fatalf("a sweep that drained its shard reports pages=%d truncated=%v", short.Pages, short.Truncated)
	}
}

// TestTheStoresOwnPageCostIsCarriedThroughUnfolded keeps Examined and
// Unreadable separable on the result. Examined equal to the limit with nothing
// returned is a different state from "nothing is due", and folding them would
// hide a shard whose rows the store cannot vouch for.
func TestTheStoresOwnPageCostIsCarriedThroughUnfolded(t *testing.T) {
	f := newFakeFixture(t, nil)
	f.due.pages = []sessionstore.DueCommandPage{
		{Limit: 2, Examined: 2, Unreadable: 2, NextCursor: "cursor-1"},
		{Limit: 2, Examined: 1, Unreadable: 1},
	}
	result, err := f.rec.Sweep(context.Background(), f.principal)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if result.Examined != 3 || result.Unreadable != 3 || result.Due != 0 || result.Rejected != 0 {
		t.Fatalf("result = %+v, want examined 3, unreadable 3, nothing due and nothing settled", result)
	}
}

// ---------------------------------------------------------------------------
// Step 4: the cost claim, measured
// ---------------------------------------------------------------------------

// TestSweepCostFollowsFixedShardsAndDueWorkNotTenantsOrTerminalHistory is step
// 4, and it is a FOR-ALL over two populations rather than a fixture.
//
// "Cost follows fixed shards / current due work, not total tenants or terminal
// history" cannot be defended by a store with one tenant and no history, so the
// measurement varies each population independently and requires the count not
// to move. Three stores, identical shard counts, identical due work (none):
//
//	1 tenant, no history
//	40 tenants, one settled command each
//	1 tenant, 200 settled commands
//
// The mechanism that makes this true is SessionStore's, not this package's --
// inboxDue files a terminal command NOT DUE, so a settled row leaves the
// ordered view entirely -- and the point of measuring is that this reconciler
// does nothing that would reintroduce the dependency, such as reading a catalog
// per row or enumerating a session's inbox.
//
// The positive control is the third axis: adding ONE due command must move the
// count. Without it, three equal numbers are also what a reconciler that
// queries nothing produces.
func TestSweepCostFollowsFixedShardsAndDueWorkNotTenantsOrTerminalHistory(t *testing.T) {
	const shards = 4

	measure := func(t *testing.T, name string, arrange func(*sweepFixture)) int {
		t.Helper()
		var cost int
		t.Run(name, func(t *testing.T) {
			f := newSweepFixture(t, shards)
			arrange(f)
			f.clock.Set(sweepBase.Add(time.Hour))
			// Settle whatever the arrangement admitted, so the history it
			// built is TERMINAL before the measured pass begins.
			f.pass()
			f.seam.reset()
			results := f.pass()
			cost = totalQueries(results)
			if seen := f.seam.providerQueries(); seen != cost {
				t.Fatalf("the result reports %d queries and the seam counted %d", cost, seen)
			}
			if got := totalRejected(results); got != 0 {
				t.Fatalf("the measured pass still had %d due rows, so it is not measuring a quiet deployment", got)
			}
		})
		return cost
	}

	baseline := measure(t, "one tenant and no history", func(*sweepFixture) {})
	manyTenants := measure(t, "forty tenants", func(f *sweepFixture) {
		for i := range 40 {
			tenant := sessionwire.TenantID(fmt.Sprintf("tenant-%02d", i))
			f.admit(tenant, fmt.Sprintf("session-%02d", i), fmt.Sprintf("command-%02d", i), sweepBase.Add(time.Minute))
		}
	})
	deepHistory := measure(t, "two hundred settled commands", func(f *sweepFixture) {
		for i := range 200 {
			f.admit(sweepTenant, fmt.Sprintf("session-%03d", i), fmt.Sprintf("command-%03d", i), sweepBase.Add(time.Minute))
		}
	})

	if baseline != shards {
		t.Fatalf("a quiet pass cost %d queries over %d shards; the fixed-shard floor is one page per shard", baseline, shards)
	}
	if manyTenants != baseline {
		t.Errorf("the cost moved with the tenant population: %d with one tenant, %d with forty", baseline, manyTenants)
	}
	if deepHistory != baseline {
		t.Errorf("the cost moved with terminal history: %d with none, %d with two hundred settled commands", baseline, deepHistory)
	}

	// The control. One due command in an otherwise identical deployment costs
	// strictly more, so the three equal numbers above are a measurement of a
	// quiet deployment rather than of a reconciler that asks nothing.
	f := newSweepFixture(t, shards)
	f.admit(sweepTenant, "session-due", "command-due", sweepBase.Add(time.Minute))
	f.clock.Set(sweepBase.Add(time.Hour))
	busy := totalQueries(f.pass())
	if busy <= baseline {
		t.Fatalf("a pass with one due command cost %d queries and a quiet one cost %d; the counter does not follow due work", busy, baseline)
	}
}

// TestTheQueryCounterMatchesTheSeamOnAPassThatExercisesEverySite closes the
// hole a gate found in the instrument itself, and the hole is worth naming
// because it is the shape a cost claim fails in.
//
// SweepResult.Queries is incremented at FOUR sites -- the due page, the claim
// acquisition, the settlement and the release -- and step 4's whole for-all is
// read through it. The cross-check that made it trustworthy ran only on a
// QUIET pass, where exactly one of the four executes, so deleting any of the
// other three left the suite green and the counter silently short. A cost
// observable with three unread increments is an assertion, not a measurement.
//
// So this pass is required to have RUN every site before the totals are
// compared: each of the four seam methods must have a nonzero count, which is
// what stops the cross-check from quietly narrowing again if a later change
// stops one of them happening.
func TestTheQueryCounterMatchesTheSeamOnAPassThatExercisesEverySite(t *testing.T) {
	f := newSweepFixture(t, 4)
	const sessions = 12
	for i := range sessions {
		// Three tenants, so the pass crosses the shard boundary the same way a
		// real deployment does rather than piling one tenant into one shard.
		tenant := sessionwire.TenantID(fmt.Sprintf("tenant-%d", i%3))
		f.admit(tenant, fmt.Sprintf("session-%02d", i), fmt.Sprintf("command-%02d", i), sweepBase.Add(time.Minute))
	}
	f.clock.Set(sweepBase.Add(time.Hour))

	results := f.pass()
	if got := totalRejected(results); got != sessions {
		t.Fatalf("the busy pass settled %d of %d rows, so it is not the pass this measures", got, sessions)
	}
	// Anti-vacuity, per site. Without this the comparison below is satisfied by
	// a pass that never reached three of the four counters -- which is exactly
	// the state that shipped.
	for _, method := range []string{
		"ListDueCommands", "AcquireReconciliationClaim", "RejectCommand", "ReleaseReconciliationClaim",
	} {
		if f.seam.count(method) == 0 {
			t.Fatalf("this pass never called %s, so it cannot cross-check that counter site", method)
		}
	}
	if got, want := totalQueries(results), f.seam.providerQueries(); got != want {
		t.Fatalf("the result reports %d queries and the seam counted %d; the counter has drifted "+
			"(per method: pages %d, acquisitions %d, settlements %d, releases %d)",
			got, want,
			f.seam.count("ListDueCommands"), f.seam.count("AcquireReconciliationClaim"),
			f.seam.count("RejectCommand"), f.seam.count("ReleaseReconciliationClaim"))
	}
	// The third layer, described as what it ACTUALLY buys rather than as a
	// second reading of the counter.
	//
	// It does NOT defend against a pair of compensating errors in the
	// reconciler's own counter: both comparisons put the same single scalar
	// totalQueries(results) on the left, nothing attributes Queries per site,
	// and against today's seam this identity is logically equivalent to the one
	// above. A gate demonstrated exactly that -- acquire counting twice with
	// release counting nothing survived both. The reader that breaks the
	// symmetry those errors cancel in is the ASYMMETRIC pass, and it lives in
	// TestADeferredSessionsRowsCostOneAcquisition.
	//
	// What this buys is the other direction: providerQueries() sums EVERY
	// counted seam method except ControlShards, while the sum below names four.
	// A sixth store call added to this reconciler is therefore counted by the
	// derived total and missing from the named one, and they disagree -- so a
	// future seam method that is reached but never named here fails rather than
	// quietly joining the cost without a reader.
	named := f.seam.count("ListDueCommands") + f.seam.count("AcquireReconciliationClaim") +
		f.seam.count("RejectCommand") + f.seam.count("ReleaseReconciliationClaim")
	if got := f.seam.providerQueries(); got != named {
		t.Fatalf("the seam counted %d provider calls and its four NAMED methods account for %d; "+
			"this sweep reaches a store method no reader here names", got, named)
	}
}

// TestAFailedSweepStillReleasesTheClaimsItTook reads the decision the release
// site's comment states and the three-question table answered Q3 with: the
// release runs on EVERY path, including the failing one.
//
// Nothing read it. Every failing case in the suite failed the FIRST page read,
// so no claim was ever held when a sweep errored -- the claim was vacuous
// exactly where the decision applies. Returning early on a page error survived,
// stranding claims for the whole TTL, which is the harm the production comment
// names.
//
// Reaching it needs a page SMALLER than the shard's due work and a failure on
// the SECOND read, so the first page has already taken a claim.
func TestAFailedSweepStillReleasesTheClaimsItTook(t *testing.T) {
	f := newSweepFixtureWith(t, 1, func(c *ReconcilerConfig) {
		c.PageLimit = 1
		c.MaxPages = 8
	})
	for i := range 3 {
		f.admit(sweepTenant, fmt.Sprintf("session-%02d", i), fmt.Sprintf("command-%02d", i), sweepBase.Add(time.Minute))
	}
	f.clock.Set(sweepBase.Add(time.Hour))
	f.seam.pageBudget = 1

	result, err := f.rec.Sweep(context.Background(), f.service)
	if err == nil {
		t.Fatal("the second page read was configured to fail and the sweep succeeded")
	}
	// Anti-vacuity: without a claim in hand at the moment of the failure this
	// case says nothing, and that is precisely how the shipped suite missed it.
	if result.Claimed == 0 {
		t.Fatal("the sweep failed before taking any claim, so the release path is untested here")
	}
	if result.Released != result.Claimed {
		t.Fatalf("a failing sweep took %d claims and released %d; the rest are stranded for the whole TTL",
			result.Claimed, result.Released)
	}
	// The store-side observable, written so that it FIRES ON A GREEN RUN.
	//
	// Its first version was `if err == nil && holder == "replica-a"`, which a
	// re-gate showed asserts nothing on a pass: every read returns an error --
	// ReconcileErrorLapsed for the session that was claimed and released,
	// ReconcileErrorNotFound for the ones this bounded sweep never reached --
	// so the body never ran. A check that treats every read error as a pass
	// reads identically whether the release happened or the record was never
	// there, which is the negative-observable shape this repository has paid
	// for.
	//
	// So the sessions are COUNTED by what the store says about them, and the
	// count of RELEASED claims is required to equal the count the sweep took.
	lapsed, held, absent := 0, 0, 0
	for i := range 3 {
		session := sessionwire.SessionID(fmt.Sprintf("session-%02d", i))
		entry, err := f.store.GetReconciliationClaim(context.Background(), sessionstore.GetReconciliationClaimRequest{
			TenantID: sweepTenant, SessionID: session,
		})
		switch {
		case err == nil:
			held++
			if entry.Claim.HolderID == "replica-a" {
				t.Errorf("%s is still claimed by the replica whose sweep failed, until %v", session, entry.Claim.ExpiresAt)
			}
		case isReconcileCode(err, sessionstore.ReconcileErrorLapsed):
			lapsed++
		case isReconcileCode(err, sessionstore.ReconcileErrorNotFound):
			// This bounded sweep never reached that session.
			absent++
		default:
			t.Fatalf("reading %s's claim: %v", session, err)
		}
	}
	if lapsed != result.Claimed {
		t.Errorf("the failing sweep took %d claims and the store reports %d released "+
			"(%d still live, %d never claimed); a claim left live holds every other replica off for the whole TTL",
			result.Claimed, lapsed, held, absent)
	}
	if lapsed+absent+held != 3 {
		t.Fatalf("the three sessions account for %d lapsed, %d live and %d absent claims", lapsed, held, absent)
	}
}

// TestADeferredSessionsRowsCostOneAcquisition reads the DEFERRAL half of the
// per-session claim memo.
//
// Its other half is read by the test below: four rows of one session whose
// claim this replica WINS cost one acquisition. The losing half was unread, and
// it is the half a busy deployment spends its time in -- a deferring replica
// re-asking per row pays N acquisitions for N rows of one session, which is a
// step-4 cost regression on exactly the path contention creates.
func TestADeferredSessionsRowsCostOneAcquisition(t *testing.T) {
	f := newSweepFixture(t, 1)
	for i := range 4 {
		f.admit(sweepTenant, "session-a", fmt.Sprintf("command-%02d", i), sweepBase.Add(time.Minute))
	}
	// Another replica is working on this session now. MaxReconciliationClaimTTL
	// is five minutes, so the horizon is placed inside it and the sweep runs
	// before it lapses.
	if _, err := f.store.AcquireReconciliationClaim(context.Background(), sessionstore.AcquireReconciliationClaimRequest{
		TenantID: sweepTenant, SessionID: "session-a", HolderID: "replica-other",
		ExpiresAt: sweepBase.Add(4 * time.Minute),
	}); err != nil {
		t.Fatalf("AcquireReconciliationClaim: %v", err)
	}
	f.clock.Set(sweepBase.Add(2 * time.Minute))

	results := f.pass()
	deferred := 0
	for _, result := range results {
		deferred += result.Dispositions[DispositionDeferred]
	}
	if deferred != 4 || totalRejected(results) != 0 {
		t.Fatalf("four rows behind another replica's claim gave %d deferrals and %d settlements",
			deferred, totalRejected(results))
	}
	if got := f.seam.count("AcquireReconciliationClaim"); got != 1 {
		t.Errorf("four deferred rows of one session cost %d claim acquisitions, want 1", got)
	}
	if got := f.seam.count("ReleaseReconciliationClaim"); got != 0 {
		t.Errorf("a deferring sweep released %d claims it never took", got)
	}

	// THE ASYMMETRIC PASS IS WHERE THE COUNTER'S COMPENSATING ERRORS DIE, and
	// that is this test's second job.
	//
	// Every other pass in this file is symmetric -- one acquisition per
	// release, one settlement per row -- so a counter that over-counts at one
	// site and under-counts at another by the same amount cancels exactly and
	// satisfies the busy-pass cross-check. A gate built that mutant (acquire
	// counting twice, release counting nothing) and it survived the whole
	// suite. Here acquisitions are 1 and releases are 0, so nothing cancels.
	//
	// The expected total is stated as a LITERAL composition rather than only as
	// an equality with the seam, because a counter and a seam that were both
	// wrong in the same direction would still agree: one page for the single
	// shard, one acquisition, no settlement, no release.
	const wantQueries = 1 + 1
	if got := totalQueries(results); got != wantQueries {
		t.Errorf("a deferring pass reports %d queries, want %d (one page, one acquisition, no write, no release)",
			got, wantQueries)
	}
	if got, want := totalQueries(results), f.seam.providerQueries(); got != want {
		t.Errorf("on an asymmetric pass the result reports %d queries and the seam counted %d; "+
			"the counter is wrong at a site whose error the symmetric passes cancel", got, want)
	}
}

// TestTheSettledRejectionIsNotAdvertisedRetryable reads a client-visible
// decision that had a reason written beside it and no assertion anywhere.
//
// Retrying THIS command id cannot succeed -- the record is terminal and a retry
// returns the rejection -- so advertising it retryable would send a client into
// a loop. Resubmitting the work under a NEW command id remains available and is
// a different request. The message is empty for the reason every other refusal
// in this package carries none: core makes message optional and code the member
// a client branches on.
func TestTheSettledRejectionIsNotAdvertisedRetryable(t *testing.T) {
	f := newSweepFixture(t, 4)
	f.admit(sweepTenant, "session-a", "command-a", sweepBase.Add(time.Minute))
	f.clock.Set(sweepBase.Add(time.Hour))

	if got := totalRejected(f.pass()); got != 1 {
		t.Fatalf("the command was not settled: %d", got)
	}
	record := f.record(sweepTenant, "session-a", "command-a")
	if record.Rejection == nil {
		t.Fatal("the settled record carries no rejection")
	}
	if record.Rejection.Retryable {
		t.Error("the settled rejection is advertised retryable; retrying this command id returns the rejection forever")
	}
	if record.Rejection.Message != "" {
		t.Errorf("the settled rejection carries the message %q; the code is the member a client branches on", record.Rejection.Message)
	}
	if record.Rejection.Code != sessionwire.ErrorCodeRuntimeUnavailable {
		t.Errorf("the settled rejection's code is %q", record.Rejection.Code)
	}
}

// TestCostFollowsTheShardCountAcrossEveryConfiguredCount is step 4's for-all on
// the axis the cost claim is actually ABOUT, and which the original measurement
// held fixed at four.
//
// "Cost follows fixed shards" is a statement about the shard count, so it is
// measured at six of them rather than demonstrated at one. The floor is exact
// and not a bound: a quiet pass is one page per shard, so the count IS the
// cost, and a reconciler that read two pages per shard or one page per pass
// would disagree at every point but four.
func TestCostFollowsTheShardCountAcrossEveryConfiguredCount(t *testing.T) {
	for _, shards := range []int{1, 2, 3, 5, 8, 16} {
		t.Run(fmt.Sprintf("%d shards", shards), func(t *testing.T) {
			f := newSweepFixture(t, shards)
			f.clock.Set(sweepBase.Add(time.Hour))
			results := f.pass()
			if got := totalQueries(results); got != shards {
				t.Fatalf("a quiet pass over %d shards cost %d queries", shards, got)
			}
			if seen := f.seam.providerQueries(); seen != shards {
				t.Fatalf("the seam counted %d queries over %d shards", seen, shards)
			}
			if len(results) != shards {
				t.Fatalf("the pass made %d sweeps over %d shards", len(results), shards)
			}
		})
	}
}

// TestCostDoesNotFollowPerSessionTerminalDepth varies terminal history WITHIN
// one session rather than across sessions.
//
// The original measurement spread 200 settled commands over 200 sessions, which
// leaves the reading open to a reconciler whose cost is per SESSION rather than
// per due row. One session carrying 300 settled commands separates them: if
// anything here enumerated a session's inbox, this is where it would show.
func TestCostDoesNotFollowPerSessionTerminalDepth(t *testing.T) {
	measure := func(t *testing.T, name string, commands int) int {
		t.Helper()
		var cost int
		t.Run(name, func(t *testing.T) {
			f := newSweepFixture(t, 4)
			for i := range commands {
				f.admit(sweepTenant, "session-deep", fmt.Sprintf("command-%03d", i), sweepBase.Add(time.Minute))
			}
			f.clock.Set(sweepBase.Add(time.Hour))
			// Passes until the shard is drained. One pass is bounded at
			// MaxPages*PageLimit rows, which is the periodic bound working:
			// 300 due rows in one shard take several passes, and that is the
			// point of the bound rather than a defect in this arrangement.
			settled := 0
			for range 16 {
				moved := totalRejected(f.pass())
				settled += moved
				if moved == 0 {
					break
				}
			}
			if settled != commands {
				t.Fatalf("the arranging passes settled %d of %d commands", settled, commands)
			}
			f.seam.reset()
			results := f.pass()
			if got := totalRejected(results); got != 0 {
				t.Fatalf("the measured pass still had %d due rows", got)
			}
			cost = totalQueries(results)
			if seen := f.seam.providerQueries(); seen != cost {
				t.Fatalf("the result reports %d queries and the seam counted %d", cost, seen)
			}
		})
		return cost
	}

	shallow := measure(t, "one settled command in the session", 1)
	deep := measure(t, "three hundred settled commands in the session", 300)
	if shallow != 4 {
		t.Fatalf("a quiet pass over four shards cost %d queries", shallow)
	}
	if deep != shallow {
		t.Errorf("the cost moved with one session's terminal depth: %d at one command, %d at three hundred", shallow, deep)
	}
}

// TestOneSessionsDueCommandsCostOneClaimAndOneRelease is the other half of "cost
// follows current due work": due work is counted in ROWS for the settlement and
// in SESSIONS for the claim, because the claim is per session and a page
// carrying several of one session's commands must not pay for it several times.
func TestOneSessionsDueCommandsCostOneClaimAndOneRelease(t *testing.T) {
	f := newSweepFixture(t, 1)
	for i := range 4 {
		f.admit(sweepTenant, "session-a", fmt.Sprintf("command-%02d", i), sweepBase.Add(time.Minute))
	}
	f.clock.Set(sweepBase.Add(time.Hour))

	results := f.pass()
	if got := totalRejected(results); got != 4 {
		t.Fatalf("the sweep settled %d of 4 commands", got)
	}
	if got := f.seam.count("AcquireReconciliationClaim"); got != 1 {
		t.Errorf("four commands of one session cost %d claim acquisitions, want 1", got)
	}
	if got := f.seam.count("ReleaseReconciliationClaim"); got != 1 {
		t.Errorf("four commands of one session cost %d claim releases, want 1", got)
	}
	if got := f.seam.count("RejectCommand"); got != 4 {
		t.Errorf("four due commands cost %d settlements, want 4", got)
	}
}

// ---------------------------------------------------------------------------
// The classification, derived rather than hand-listed
// ---------------------------------------------------------------------------

// TestEveryInboxErrorCodeIsClassifiedByTheSettlementReader is this module's
// systemic defect made unrepeatable for this package.
//
// Four rounds of it came from each package deciding independently which store
// codes mean absence, and every one of the six fault sites was found by
// patching rather than by derivation. The reader below cannot be derived -- a
// refusal's MEANING to a sweep is not recoverable from the constant -- so what
// is derived is its SUBJECT: every InboxErrorCode the PINNED store declares,
// parsed out of that package's own source. A release that adds one fails here
// instead of falling silently into a default arm.
//
// The classifier is also fail-closed, which is the direction that costs a
// spurious sweep failure rather than a spurious settlement.
func TestEveryInboxErrorCodeIsClassifiedByTheSettlementReader(t *testing.T) {
	t.Parallel()

	codes := declaredStringConstants(t, "errors.go", "InboxErrorCode")
	if len(codes) < 10 {
		t.Fatalf("the pinned store declares %d inbox error codes; the derivation is broken", len(codes))
	}
	for _, code := range codes {
		if _, classified := settlementRefusal(&sessionstore.InboxError{Code: sessionstore.InboxErrorCode(code)}); !classified {
			if _, recorded := unclassifiedSettlementCodes()[sessionstore.InboxErrorCode(code)]; !recorded {
				t.Errorf("the settlement reader does not classify %q and it is not recorded as a fault; "+
					"a code nobody classified falls to the fault arm silently", code)
			}
		}
	}
	// Both directions. A record naming a code the store no longer declares has
	// outlived its subject.
	declared := map[string]bool{}
	for _, code := range codes {
		declared[code] = true
	}
	for code, reason := range unclassifiedSettlementCodes() {
		if !declared[string(code)] {
			t.Errorf("%q is recorded as a fault (%q) but the pinned store no longer declares it", code, reason)
		}
		if _, classified := settlementRefusal(&sessionstore.InboxError{Code: code}); classified {
			t.Errorf("%q is recorded as a fault (%q) and the reader classifies it anyway; the record is stale", code, reason)
		}
	}
	// A code that is not the store's at all must not be classified, and an
	// error that is not an InboxError must not be either: both are faults.
	if _, classified := settlementRefusal(&sessionstore.InboxError{Code: "a-code-no-release-declares"}); classified {
		t.Error("the reader classified a code no store declares; it is not fail-closed")
	}
	if _, classified := settlementRefusal(errors.New("the provider could not be reached")); classified {
		t.Error("the reader classified an error that is not an InboxError")
	}
}

// unclassifiedSettlementCodes names the inbox codes a refused settlement
// deliberately treats as a FAULT, with the reason. An entry is a claim the test
// above checks in both directions.
func unclassifiedSettlementCodes() map[sessionstore.InboxErrorCode]string {
	return map[sessionstore.InboxErrorCode]string{
		sessionstore.InboxErrorInvalid:         "a malformed request is this reconciler's own defect, not a row's state",
		sessionstore.InboxErrorCursor:          "a cursor code cannot come from a named write",
		sessionstore.InboxErrorCommandMismatch: "a mismatch is an admission answer; a settlement names no payload",
		sessionstore.InboxErrorIdentity:        "a row disagreeing with its own filing needs an operator",
		sessionstore.InboxErrorEpoch:           "the settlement names no epoch, so the fence cannot refuse it",
		// Added by sessionstore v0.7.0, which took this vocabulary from 19 to
		// 20. It is the SECOND consumption-cursor fence, beside
		// InboxErrorEpoch, and it is recorded here on the same ground: it is
		// produced at exactly one site, consumption.go:669 inside
		// SaveDispositionCommandCursor, and this sweep saves no cursor. A
		// settlement names no cursor position, so the fence cannot refuse it.
		//
		// It is deliberately NOT given a Disposition. The release note is
		// explicit that "order" means RE-READ AND RETRY and must not be mapped
		// onto ErrEpochSuperseded -- so classifying it as DispositionRaceLost,
		// the arm a superseded write lands in, would tell a sweep it had lost a
		// race it never entered and settle on a stale read. The fault arm is
		// the fail-closed direction: it costs a named sweep failure an operator
		// sees, rather than a wrong settlement nobody does.
		sessionstore.InboxErrorOrder:     "the settlement names no cursor position, so the consumption fence cannot refuse it",
		sessionstore.InboxErrorDeadline:  "only a claiming transition checks the apply deadline",
		sessionstore.InboxErrorState:     "only a claiming transition refuses on state",
		sessionstore.InboxErrorUnknown:   "an unclassified provider failure is a fault",
		sessionstore.InboxErrorBackend:   "a provider failure is a fault",
		sessionstore.InboxErrorMalformed: "a row this store cannot decode needs an operator",
		sessionstore.InboxErrorVersion:   "a record version this build does not understand needs an operator",
		sessionstore.InboxErrorTooLarge:  "a settlement shrinks no record, so this cannot be a row's state",
	}
}

// declaredStringConstants parses one file of the PINNED sessionstore module for
// every constant declared with the named string type, and returns their values.
//
// It resolves the module from this module's own build rather than from a path
// somebody wrote down, so the source it reads is the source the build resolves.
//
// # The unit of analysis, stated as what it can actually SEE
//
// A gate found this scan's stated reach wider than its real reach, which is the
// fourth source-parsing guard in this workspace to have a hole found on first
// review, so the boundary is written from what scanStringConstants keys on
// rather than from what it is for:
//
//   - The subject is ONE named file. A code declared in another file of the
//     pinned module is outside the derivation entirely, and nothing here can
//     report that -- the anti-vacuity floor still sees the other codes.
//   - The subject is a CONST declaration. A `var` block of the same type is
//     invisible, and TestTheConstantScanReportsWhatItCannotRead measures that
//     rather than leaving it promised.
//   - Within that subject, the forms ENUMERATED HERE each produce a HARD
//     FAILURE naming themselves rather than a silent drop: a concatenation, a
//     call, a reference to another constant, an implicit repetition, an
//     untyped conversion to the type -- parenthesised or nested in a larger
//     constant expression -- and a declaration through a file-local alias of
//     the type, with parentheses stripped from the type, the alias and the
//     conversion. THIS IS A LIST, NOT A CLOSURE. The previous version of this
//     bullet said that NO declaration inside the subject is a silent drop, and
//     a third re-gate falsified it with five spellings that compile, survive
//     gofmt and sit inside the stated unit. All five now report or are read;
//     what is claimed is only that these enumerated forms do. Under-inclusion
//     is the dangerous direction: it shrinks the derived subject while every
//     anti-vacuity floor stays satisfied -- the floor below is 10 against a
//     real subject of 20, so ten codes could vanish with every floor green.
//
// # What remains outside, stated as a residue rather than as a boundary
//
// The first version of this comment claimed the residue was "another file or a
// `var`". A re-gate found two forms silently dropped INSIDE the stated unit
// (`const B = Code("b")` and a declaration through a file-local alias), and a
// THIRD re-gate found five more, all of them parenthesisation or expression
// nesting. That is three holes in one scan, so the residue is written as a
// list of things that ARE missed rather than as a boundary that sounds closed,
// and the list is not claimed to be complete either:
//
//   - a code declared in another FILE of the pinned package;
//   - a code declared as a `var` rather than a `const`;
//   - a code declared through a type alias declared in another file, which
//     fileLocalAliasesOf cannot see;
//   - a code declared inside a function body, which is not in File.Decls;
//   - a code whose untyped value is of the subject type through a constant
//     EXPRESSION that names no conversion -- `const F = A + "x"` with A
//     already a code. conversionTo searches for the SYNTAX `Code(...)`
//     anywhere in the expression, which is what closed `Code("e") + "x"`; it
//     cannot see a type that is only inferred, and settling that needs a type
//     checker rather than a parser.
//
// Each of those is absent from the subject and THIS CANNOT SAY SO. The blast
// radius is bounded by settlementRefusal being FAIL-CLOSED -- such a code
// becomes a fault and stops the sweep -- so the residue costs a spurious sweep
// failure, never a settlement this reconciler had no licence for.
func declaredStringConstants(t *testing.T, file, typeName string) []string {
	t.Helper()

	dir := sessionstoreSourceDir(t)
	parsed, err := parser.ParseFile(token.NewFileSet(), filepath.Join(dir, file), nil, 0)
	if err != nil {
		t.Fatalf("parse the pinned %s: %v", file, err)
	}
	scan := scanStringConstants(parsed, typeName)
	if len(scan.unreadable) != 0 {
		t.Fatalf("the pinned %s declares %d constant(s) of type %s this scan cannot read:\n\t%s\n"+
			"Each is silently ABSENT from the derived subject while every anti-vacuity floor stays "+
			"satisfied, so the scan fails here rather than reporting a subject it knows is short",
			file, len(scan.unreadable), typeName, strings.Join(scan.unreadable, "\n\t"))
	}
	if len(scan.values) == 0 {
		t.Fatalf("no %s constants were found in the pinned %s; the parse is broken", typeName, file)
	}
	slices.Sort(scan.values)
	return scan.values
}

// constantScan is one file's readable constants of a named string type,
// together with a report of every declaration of that type the scan could not
// read.
type constantScan struct {
	values     []string
	unreadable []string
}

// scanStringConstants is the parse, separated from the file so it can be driven
// against sources a pinned module does not contain.
//
// The carried type follows Go's own rule and is ENDED by an untyped
// declaration, so `const ( A T = "a"; B = "b" )` does not report B as a T. The
// previous version carried it and over-included; that direction is fail-safe,
// but a scan whose reach nobody can state is the thing being fixed.
func scanStringConstants(file *ast.File, typeName string) constantScan {
	aliases := fileLocalAliasesOf(file, typeName)
	var scan constantScan
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		declaredType := ""
		for _, spec := range gen.Specs {
			value, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			switch {
			case value.Type != nil:
				ident, named := unparenthesise(value.Type).(*ast.Ident)
				if !named {
					// A qualified or composite type is not this one, and
					// carrying the previous name past it would be a guess.
					declaredType = ""
					continue
				}
				// A FILE-LOCAL ALIAS IS THE SUBJECT TYPE UNDER ANOTHER NAME, so
				// a constant declared through one is a member of the subject
				// this scan does not resolve. It is REPORTED rather than
				// skipped: the previous version ended the carried type and fell
				// through the "not my type" test, which is a silent drop inside
				// the scan's own stated unit.
				if ident.Name != typeName && aliases[ident.Name] {
					scan.unreadable = append(scan.unreadable, specNames(value)+
						" is declared through the file-local alias "+ident.Name+
						" of "+typeName+", which this scan does not resolve into the subject")
					declaredType = ""
					continue
				}
				declaredType = ident.Name
			case len(value.Values) > 0:
				// AN UNTYPED CONVERSION IS THE OTHER SILENT DROP. `const B =
				// Code("b")` is legal, idiomatic Go and is a constant of the
				// subject type, but the spec carries no Type, so the statement
				// that (correctly) ends the carried type used to throw it away
				// before any report could be appended.
				if converted, name := conversionTo(value.Values, typeName, aliases); converted {
					scan.unreadable = append(scan.unreadable, specNames(value)+
						" is an untyped declaration whose value converts to "+name+
						", which this scan does not read as a declaration of that type")
					declaredType = ""
					continue
				}
				declaredType = ""
			}
			if declaredType != typeName {
				continue
			}
			names := specNames(value)
			if len(value.Values) == 0 {
				scan.unreadable = append(scan.unreadable,
					names+" repeats the previous expression, which this scan does not evaluate")
				continue
			}
			for _, expr := range value.Values {
				lit, ok := expr.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					scan.unreadable = append(scan.unreadable,
						names+" is not a plain string literal, so its value cannot be read without evaluation")
					continue
				}
				unquoted, err := strconv.Unquote(lit.Value)
				if err != nil {
					scan.unreadable = append(scan.unreadable, names+" is a string literal this scan cannot unquote")
					continue
				}
				scan.values = append(scan.values, unquoted)
			}
		}
	}
	return scan
}

// fileLocalAliasesOf reports every name this FILE declares as a type alias of
// typeName, transitively.
//
// It is deliberately file-local and deliberately only an ALIAS (`type A = T`),
// never a defined type (`type A T`): a defined type is a different type whose
// constants are not members of the subject, and treating the two alike would be
// over-inclusion dressed as thoroughness. An alias declared in ANOTHER file of
// the pinned package is outside this scan and is named in the residue.
func fileLocalAliasesOf(file *ast.File, typeName string) map[string]bool {
	// Collect every alias edge first, then close over them, so an alias of an
	// alias is found whatever order the declarations appear in.
	edges := map[string]string{}
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.TYPE {
			continue
		}
		for _, spec := range gen.Specs {
			typ, ok := spec.(*ast.TypeSpec)
			if !ok || typ.Assign == token.NoPos {
				continue
			}
			if ident, named := unparenthesise(typ.Type).(*ast.Ident); named {
				edges[typ.Name.Name] = ident.Name
			}
		}
	}
	out := map[string]bool{}
	for name := range edges {
		seen := map[string]bool{}
		for at := name; ; {
			next, ok := edges[at]
			if !ok || seen[at] {
				break
			}
			seen[at] = true
			if next == typeName {
				out[name] = true
				break
			}
			at = next
		}
	}
	return out
}

// conversionTo reports whether any of a spec's values is a conversion to the
// subject type or to one of its file-local aliases, and names the one it found.
//
// It keys on the syntax `Name(...)` with Name an identifier once parentheses
// are stripped, which is exactly what a conversion to a locally-named type
// looks like. A call to an ordinary FUNCTION of the same name is
// indistinguishable from it without types -- and is reported, which is the
// safe direction: a hard failure asks a human, where a silent drop shortens
// the derived subject with every floor still satisfied.
//
// It searches the WHOLE value expression, not just its top level, because
// `const E = Code("e") + "x"` is a constant of the subject type and is an
// *ast.BinaryExpr: stripping parentheses does not reach it. The same reach
// over-reports in the same safe direction -- `const N = len(Code("a"))` is an
// int and is reported anyway -- and it still only adds to `unreadable`, never
// to `values`, so no arm here can WIDEN the derived subject.
func conversionTo(values []ast.Expr, typeName string, aliases map[string]bool) (bool, string) {
	found, name := false, ""
	for _, expr := range values {
		ast.Inspect(expr, func(node ast.Node) bool {
			if found {
				return false
			}
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			ident, named := unparenthesise(call.Fun).(*ast.Ident)
			if !named {
				return true
			}
			if ident.Name == typeName || aliases[ident.Name] {
				found, name = true, ident.Name
				return false
			}
			return true
		})
		if found {
			return true, name
		}
	}
	return false, ""
}

// unparenthesise strips redundant parentheses from an expression.
//
// It exists as ONE helper rather than as three inline type switches because
// the omission it fixes was one mistake made at three independent sites --
// value.Type here, call.Fun in conversionTo, and typ.Type in
// fileLocalAliasesOf -- each of which required a bare *ast.Ident and silently
// dropped a spelling gofmt considers canonical. A fourth site cannot be added
// without reaching for this.
func unparenthesise(expr ast.Expr) ast.Expr {
	for {
		paren, ok := expr.(*ast.ParenExpr)
		if !ok {
			return expr
		}
		expr = paren.X
	}
}

func specNames(value *ast.ValueSpec) string {
	names := make([]string, 0, len(value.Names))
	for _, ident := range value.Names {
		names = append(names, ident.Name)
	}
	return strings.Join(names, ", ")
}

// TestTheConstantScanReportsWhatItCannotRead is the derivation's own control,
// and it is the part that was missing: a scan attacked only with the real
// pinned file reports the same answer whether it works or is stuck, and a
// silently short subject satisfies every floor above it.
//
// The pairs are controlled -- each synthetic source differs from the accepted
// one in exactly the construct under test.
func TestTheConstantScanReportsWhatItCannotRead(t *testing.T) {
	t.Parallel()

	const typeName = "Code"
	for _, test := range []struct {
		name       string
		source     string
		values     []string
		unreadable int
	}{
		{
			name:   "the accepted form",
			source: "package p\ntype Code string\nconst (\n\tA Code = \"a\"\n\tB Code = \"b\"\n)\n",
			values: []string{"a", "b"},
		},
		{
			name:       "a concatenation is reported, not dropped",
			source:     "package p\nconst prefix = \"x\"\nconst (\n\tA Code = \"a\"\n\tB Code = prefix + \"b\"\n)\n",
			values:     []string{"a"},
			unreadable: 1,
		},
		{
			name:       "a reference to another constant is reported",
			source:     "package p\nconst other = \"o\"\nconst (\n\tA Code = other\n)\n",
			unreadable: 1,
		},
		{
			name:       "an implicit repetition is reported",
			source:     "package p\nconst (\n\tA Code = \"a\"\n\tB\n)\n",
			values:     []string{"a"},
			unreadable: 1,
		},
		{
			name:   "an untyped declaration ends the carried type",
			source: "package p\nconst (\n\tA Code = \"a\"\n\tB = \"b\"\n)\n",
			values: []string{"a"},
		},
		{
			name:   "a var block of the same type is OUTSIDE the subject",
			source: "package p\nvar (\n\tA Code = \"a\"\n)\n",
		},
		{
			name:   "another type in the same file is not this one",
			source: "package p\nconst (\n\tA Other = \"a\"\n)\n",
		},
		// The two forms a re-gate found silently dropped INSIDE the stated
		// unit -- one file, a const declaration -- each with the negative
		// control that keeps the new arm from swallowing an unrelated
		// declaration.
		{
			name:       "an untyped conversion to the subject type is reported",
			source:     "package p\nconst (\n\tA Code = \"a\"\n)\nconst B = Code(\"b\")\n",
			values:     []string{"a"},
			unreadable: 1,
		},
		{
			name:   "an untyped conversion to a DIFFERENT type is not this scan's business",
			source: "package p\nconst (\n\tA Code = \"a\"\n)\nconst B = Other(\"b\")\n",
			values: []string{"a"},
		},
		{
			name:       "a constant declared through a file-local alias is reported",
			source:     "package p\ntype Alias = Code\nconst (\n\tA Alias = \"a\"\n)\n",
			unreadable: 1,
		},
		{
			name:       "an alias of an alias is followed",
			source:     "package p\ntype Inner = Code\ntype Outer = Inner\nconst (\n\tA Outer = \"a\"\n)\n",
			unreadable: 1,
		},
		{
			name:       "a conversion through an alias is reported",
			source:     "package p\ntype Alias = Code\nconst B = Alias(\"b\")\n",
			unreadable: 1,
		},
		{
			name:   "a DEFINED type of the same underlying type is a different type",
			source: "package p\ntype Defined Code\nconst (\n\tA Defined = \"a\"\n)\n",
		},
		{
			name:   "an alias of something else is not an alias of the subject",
			source: "package p\ntype Alias = Other\nconst (\n\tA Alias = \"a\"\n)\n",
		},
		// The five spellings a THIRD re-gate found silently dropped inside the
		// stated unit. Four are parenthesisation, which is one omission at
		// three sites; the fifth is a conversion nested in a larger constant
		// expression, which parenthesis-unwrapping alone does not reach. Each
		// is canonical Go that gofmt leaves alone, so each could appear
		// verbatim in a future sessionstore release. Every one carries the
		// negative control that keeps its arm from widening the subject.
		{
			name:   "a parenthesised TYPE in a typed spec is read",
			source: "package p\ntype Code string\nconst (\n\tA (Code) = \"a\"\n)\n",
			values: []string{"a"},
		},
		{
			name:   "a parenthesised type of a DIFFERENT type is still not this one",
			source: "package p\nconst (\n\tA (Other) = \"a\"\n)\n",
		},
		{
			name:       "a parenthesised conversion VALUE is reported",
			source:     "package p\nconst (\n\tA Code = \"a\"\n)\nconst B = (Code(\"b\"))\n",
			values:     []string{"a"},
			unreadable: 1,
		},
		{
			name:   "a parenthesised conversion value of a DIFFERENT type is not this scan's business",
			source: "package p\nconst B = (Other(\"b\"))\n",
		},
		{
			name:       "a parenthesised type NAME in a conversion is reported",
			source:     "package p\nconst C = (Code)(\"c\")\n",
			unreadable: 1,
		},
		{
			name:   "a parenthesised type name of a DIFFERENT type in a conversion is not this one",
			source: "package p\nconst C = (Other)(\"c\")\n",
		},
		{
			name:       "a conversion NESTED in a larger constant expression is reported",
			source:     "package p\nconst E = Code(\"e\") + \"x\"\n",
			unreadable: 1,
		},
		{
			name:   "a nested conversion to a DIFFERENT type is not this scan's business",
			source: "package p\nconst E = Other(\"e\") + \"x\"\n",
		},
		{
			name:       "a parenthesised ALIAS declaration is followed",
			source:     "package p\ntype Alias = (Code)\nconst (\n\tF Alias = \"f\"\n)\n",
			unreadable: 1,
		},
		{
			name:   "a parenthesised DEFINED type is still a different type",
			source: "package p\ntype Defined (Code)\nconst (\n\tA (Defined) = \"a\"\n)\n",
		},
		{
			name:       "the nested search over-reports in the SAFE direction",
			source:     "package p\nconst N = len(Code(\"a\"))\n",
			unreadable: 1,
		},
		{
			// RESIDUE, made observable rather than promised: this IS a
			// constant of the subject type and the scan drops it silently,
			// because nothing in the expression spells the conversion. It is
			// recorded here so the residue bullet above is a checked claim.
			name:   "a subject-typed constant naming NO conversion is silently dropped",
			source: "package p\nconst (\n\tA Code = \"a\"\n)\nconst F = A + \"x\"\n",
			values: []string{"a"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			parsed, err := parser.ParseFile(token.NewFileSet(), "probe.go", test.source, 0)
			if err != nil {
				t.Fatalf("parse the probe: %v", err)
			}
			scan := scanStringConstants(parsed, typeName)
			if !slices.Equal(scan.values, test.values) {
				t.Errorf("values = %q, want %q", scan.values, test.values)
			}
			if len(scan.unreadable) != test.unreadable {
				t.Errorf("unreadable = %q, want %d entries", scan.unreadable, test.unreadable)
			}
		})
	}

	// The positive control over the REAL subject: the pinned file this
	// derivation actually reads has nothing the scan cannot read, so the hard
	// failure above is not merely latent.
	dir := sessionstoreSourceDir(t)
	parsed, err := parser.ParseFile(token.NewFileSet(), filepath.Join(dir, "errors.go"), nil, 0)
	if err != nil {
		t.Fatalf("parse the pinned errors.go: %v", err)
	}
	scan := scanStringConstants(parsed, "InboxErrorCode")
	if len(scan.unreadable) != 0 {
		t.Errorf("the pinned errors.go has %d unreadable InboxErrorCode declarations: %q", len(scan.unreadable), scan.unreadable)
	}
	if len(scan.values) < 10 {
		t.Errorf("the pinned errors.go yielded %d InboxErrorCode values; the control is broken", len(scan.values))
	}
}

// pinnedSessionstoreVersion is the sessionstore version this module's go.mod
// names, as an absolute literal.
//
// It exists so the scans above cannot silently read a DIFFERENT copy of the
// store. Under the workspace go.work, or after a pin moves, "the sessionstore
// on disk" and "the sessionstore this build resolves" are not the same
// directory, and a derived subject read from the wrong one is a guard that
// looks derived and is not. internal/placement pins core the same way and for
// the same reason.
const pinnedSessionstoreVersion = "v0.7.0"

const pinnedSessionstoreModule = "github.com/looprig/sessionstore"

// sessionstoreSourceDir returns the module cache directory of the pinned
// sessionstore, failing if go.mod has moved.
//
// The cache path is the module path verbatim: escaping only applies to upper
// case letters and this path has none.
func sessionstoreSourceDir(t *testing.T) string {
	t.Helper()

	// internal/admission -> module root. A test's working directory is its own
	// package directory, which go test guarantees.
	gomod, err := os.ReadFile(filepath.Join("..", "..", "go.mod"))
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	required := requiredModuleVersion(string(gomod), pinnedSessionstoreModule)
	if required != pinnedSessionstoreVersion {
		t.Fatalf("go.mod requires %s %q, but the derived subjects in this file were written against %q; "+
			"recheck them against the version now pinned before updating the constant",
			pinnedSessionstoreModule, required, pinnedSessionstoreVersion)
	}

	out, err := exec.Command("go", "env", "GOMODCACHE").Output()
	if err != nil {
		t.Fatalf("go env GOMODCACHE: %v", err)
	}
	cache := strings.TrimSpace(string(out))
	if cache == "" {
		t.Fatal("go env GOMODCACHE is empty, so the pinned package cannot be located")
	}
	dir := filepath.Join(cache, pinnedSessionstoreModule+"@"+required)
	if entries, err := os.ReadDir(dir); err != nil || len(entries) == 0 {
		t.Fatalf("pinned sessionstore at %s is unreadable or empty (%v); a scan over nothing proves nothing", dir, err)
	}
	return dir
}

// requiredModuleVersion reports the version a go.mod requires for one module
// path, or "" when it requires none. Both the block and the single-line forms
// are read, and the module path is matched as a whole field so that a require
// of github.com/looprig/sessionstoreutil could not answer for
// github.com/looprig/sessionstore.
func requiredModuleVersion(content, module string) string {
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

// ---------------------------------------------------------------------------
// Faults are faults, not decisions
// ---------------------------------------------------------------------------

// TestNoReconcilerDependencyFaultBecomesAPublicCode is the A6.2 sweep in this
// reconciler's shape, and it is derived on both axes for the same reason: a
// table of named sites defends the property only where somebody looked.
//
// The dependency axis is ReconcilerConfig's INTERFACE-KIND FIELDS, and the
// fallible methods are read off their signatures. The entry-point axis is
// *Reconciler's exported method set, held by set equality so a second entry
// point cannot join the surface without joining the sweep.
func TestNoReconcilerDependencyFaultBecomesAPublicCode(t *testing.T) {
	sites := reconcilerFaultSites(t)
	if len(sites) < 2 {
		t.Fatalf("the derived dependency surface has %d fallible methods; the sweep is vacuous", len(sites))
	}
	entries := reconcilerEntryPoints(t)

	exercised := map[string]bool{}
	for _, site := range sites {
		for entryName, call := range entries {
			t.Run(site.dependency+"."+site.method+"/"+entryName, func(t *testing.T) {
				f := newFakeFixture(t, nil)
				f.due.pages = []sessionstore.DueCommandPage{duePage("", abandoned("session-a", "command-a"))}
				injector, armable := armReconcilerFault(f, site.dependency)
				if !armable {
					t.Fatalf("no fake can arm %s", site.dependency)
				}
				injector.failing = site.method

				err := call(f)

				if !injector.called[site.method] {
					return
				}
				exercisedMu.Lock()
				exercised[site.dependency+"."+site.method] = true
				exercisedMu.Unlock()

				if reason := toleratedReconcilerFaults()[site.dependency+"."+site.method]; reason != "" {
					if err != nil {
						t.Errorf("%s.%s is recorded as deliberately swallowed (%q) and %s reported %v anyway",
							site.dependency, site.method, reason, entryName, err)
					}
					return
				}
				if err == nil {
					t.Fatalf("%s.%s failed and %s succeeded anyway", site.dependency, site.method, entryName)
				}
				var classifiedCause *Error
				if !errors.Is(err, errInjectedFault) && !errors.As(err, &classifiedCause) {
					t.Errorf("%s.%s failed and %s answered %v, which neither wraps the injected fault nor classifies it",
						site.dependency, site.method, entryName, err)
				}
				var classified *Error
				if errors.As(err, &classified) {
					t.Errorf("%s.%s failed and %s answered with the public code %q; an edge renders that as a decision about a caller's command",
						site.dependency, site.method, entryName, classified.Code)
				}
			})
		}
	}

	for _, site := range sites {
		name := site.dependency + "." + site.method
		if exercised[name] || unreachedReconcilerMethods()[name] != "" {
			continue
		}
		t.Errorf("no entry point ever called %s with it failing, so the sweep proves nothing about it: "+
			"either drive it from an entry point or record why it is unreachable", name)
	}
	derived := map[string]bool{}
	for _, site := range sites {
		derived[site.dependency+"."+site.method] = true
	}
	for _, record := range []map[string]string{toleratedReconcilerFaults(), unreachedReconcilerMethods()} {
		for name, reason := range record {
			if !derived[name] {
				t.Errorf("%s is recorded (%q) but is no longer a fallible method of any dependency ReconcilerConfig declares; "+
					"either the interface lost a method and the sweep silently shrank, or the record has outlived its subject", name, reason)
			}
		}
	}
	for name, reason := range unreachedReconcilerMethods() {
		if exercised[name] {
			t.Errorf("%s is recorded as unreachable (%q) but the sweep reached it; the record is stale", name, reason)
		}
	}
}

// unreachedReconcilerMethods names the derived methods this entry point never
// calls, with the reason. An entry is a claim that must stay true: the sweep
// fails if one of these is ever reached.
//
// AuthorizeControl is the load-bearing one. A command arrives at the httpapi or
// clientlink edge already authorized and admission repeats that decision as the
// durable mutation boundary; a THIRD authorizing caller is how an unauthorized
// caller acquires a path. This record is the behavioural half of the structural
// scan in TestTheSweepAuthorizesAsAServiceAndNeverAsACommand -- the scan sees a
// selector, this sees a call.
func unreachedReconcilerMethods() map[string]string {
	return map[string]string{
		"Authorizer.AuthorizeControl": "the sweep authorizes as a service and is never a caller's command",
	}
}

// toleratedReconcilerFaults names the dependency failures the sweep deliberately
// does NOT report, with the reason. Each is checked in both directions above,
// and each has its own observable elsewhere in this file.
func toleratedReconcilerFaults() map[string]string {
	return map[string]string{
		"Claims.ReleaseReconciliationClaim": "a claim licenses nothing and a lapsed one needs no cleanup, so replacing a correct settlement with a release error would report the wrong thing; the observable is SweepResult.ReleaseFailures",
	}
}

func reconcilerFaultSites(t *testing.T) []faultSite {
	t.Helper()

	errorType := reflect.TypeOf((*error)(nil)).Elem()
	probe := newFakeFixture(t, nil)
	cfg := reflect.TypeOf(ReconcilerConfig{})
	var sites []faultSite
	dependencies := 0
	for i := range cfg.NumField() {
		field := cfg.Field(i)
		if field.Type.Kind() != reflect.Interface {
			continue
		}
		dependencies++
		if field.Type.NumMethod() == 0 {
			t.Errorf("%s declares no methods, so sweeping it proves nothing", field.Name)
		}
		fallible := 0
		for m := range field.Type.NumMethod() {
			method := field.Type.Method(m)
			out := method.Type.NumOut()
			if out == 0 || method.Type.Out(out-1) != errorType {
				continue
			}
			fallible++
			sites = append(sites, faultSite{dependency: field.Name, method: method.Name})
		}
		if fallible == 0 {
			continue
		}
		if _, armable := armReconcilerFault(probe, field.Name); !armable {
			t.Errorf("ReconcilerConfig declares the fallible dependency %s, which this fixture cannot arm", field.Name)
		}
	}
	if dependencies == 0 {
		t.Fatal("ReconcilerConfig declares no interface dependencies; the derivation is broken")
	}
	slices.SortFunc(sites, func(a, b faultSite) int {
		if a.dependency != b.dependency {
			return strings.Compare(a.dependency, b.dependency)
		}
		return strings.Compare(a.method, b.method)
	})
	return sites
}

func armReconcilerFault(f *fakeFixture, dependency string) (*faultInjector, bool) {
	switch dependency {
	case "Authorizer":
		return &f.auth.faultInjector, true
	case "Due":
		return &f.due.faultInjector, true
	case "Settlement":
		return &f.settlement.faultInjector, true
	case "Claims":
		return &f.claims.faultInjector, true
	default:
		return nil, false
	}
}

func reconcilerEntryPoints(t *testing.T) map[string]func(*fakeFixture) error {
	t.Helper()

	entries := map[string]func(*fakeFixture) error{
		"Sweep": func(f *fakeFixture) error {
			_, err := f.rec.Sweep(context.Background(), f.principal)
			return err
		},
	}
	rec := reflect.TypeOf(&Reconciler{})
	declared := map[string]bool{}
	for i := range rec.NumMethod() {
		declared[rec.Method(i).Name] = true
	}
	if len(declared) == 0 {
		t.Fatal("*Reconciler declares no exported methods; the derivation is broken")
	}
	for name := range declared {
		if entries[name] == nil {
			t.Errorf("*Reconciler declares %s, which no entry point in this sweep drives", name)
		}
	}
	for name := range entries {
		if !declared[name] {
			t.Errorf("this sweep drives %s, which *Reconciler no longer declares", name)
		}
	}
	return entries
}

// TestASettlementRefusalIsNeverAPublicCode is the site-specific half the
// reflective sweep cannot cover: a refusal SessionStore classifies -- evidence,
// a held claim, a lost race -- must leave the sweep running and must not be
// dressed as a decision about anyone's command.
func TestASettlementRefusalIsNeverAPublicCode(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		code sessionstore.InboxErrorCode
		want Disposition
	}{
		{sessionstore.InboxErrorEvidence, DispositionEvidence},
		{sessionstore.InboxErrorClaimHeld, DispositionClaimLive},
		{sessionstore.InboxErrorClaimLost, DispositionApplying},
		{sessionstore.InboxErrorConflict, DispositionRaceLost},
		{sessionstore.InboxErrorTerminal, DispositionTerminal},
		{sessionstore.InboxErrorNotFound, DispositionTerminal},
		{sessionstore.InboxErrorDeleted, DispositionTerminal},
	} {
		t.Run(string(test.code), func(t *testing.T) {
			f := newFakeFixture(t, nil)
			f.due.pages = []sessionstore.DueCommandPage{duePage("", abandoned("session-a", "command-a"))}
			f.settlement.err = &sessionstore.InboxError{Code: test.code}

			result, err := f.rec.Sweep(context.Background(), f.principal)
			if err != nil {
				t.Fatalf("a classified refusal stopped the sweep: %v", err)
			}
			if result.Rejected != 0 {
				t.Fatalf("a refused settlement was counted as %d rejections", result.Rejected)
			}
			if got := result.Dispositions[test.want]; got != 1 {
				t.Fatalf("the row was recorded as %v %d times, want once (%v)", test.want, got, result.Dispositions)
			}
			// The claim is still released. A refused settlement is not a
			// reason to leave another replica waiting out this one's horizon.
			if f.claims.released != 1 {
				t.Errorf("a refused settlement released %d claims, want 1", f.claims.released)
			}
		})
	}

	// The fault arm, which must stop the sweep rather than be recorded as an
	// outcome: a settlement refused for a reason nobody classified is not
	// evidence that the row is fine.
	f := newFakeFixture(t, nil)
	f.due.pages = []sessionstore.DueCommandPage{duePage("", abandoned("session-a", "command-a"))}
	f.settlement.err = &sessionstore.InboxError{Code: sessionstore.InboxErrorBackend}
	if _, err := f.rec.Sweep(context.Background(), f.principal); err == nil {
		t.Error("a backend failure inside the settlement was recorded as an outcome")
	}
}

// TestAHeldReconciliationClaimIsNotAFailure separates the two answers
// AcquireReconciliationClaim gives. Collapsing "another replica holds it" into
// an error would make an ordinary multi-replica deployment report failures on
// its happy path.
func TestAHeldReconciliationClaimIsNotAFailure(t *testing.T) {
	t.Parallel()

	f := newFakeFixture(t, nil)
	f.due.pages = []sessionstore.DueCommandPage{duePage("", abandoned("session-a", "command-a"), abandoned("session-a", "command-b"))}
	f.claims.acquireErr = &sessionstore.ReconcileError{Code: sessionstore.ReconcileErrorHeld, ExpiresAt: sweepBase.Add(time.Minute)}

	result, err := f.rec.Sweep(context.Background(), f.principal)
	if err != nil {
		t.Fatalf("a held claim was reported as a failure: %v", err)
	}
	if result.Dispositions[DispositionDeferred] != 2 || result.Rejected != 0 {
		t.Fatalf("result = %+v, want both rows deferred and nothing settled", result)
	}
	if f.settlement.calls != 0 {
		t.Errorf("a deferred row cost %d settlements", f.settlement.calls)
	}
	if f.claims.released != 0 {
		t.Errorf("a sweep that took no claim released %d", f.claims.released)
	}

	// The other refusal IS a failure, and the two must not be one arm.
	g := newFakeFixture(t, nil)
	g.due.pages = []sessionstore.DueCommandPage{duePage("", abandoned("session-a", "command-a"))}
	g.claims.acquireErr = &sessionstore.ReconcileError{Code: sessionstore.ReconcileErrorBackend}
	if _, err := g.rec.Sweep(context.Background(), g.principal); err == nil {
		t.Error("a provider failure inside the claim acquisition was treated as a deferral")
	}
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

func TestNewReconcilerRefusesAnUnusableConfiguration(t *testing.T) {
	t.Parallel()

	valid := ReconcilerConfig{
		Authorizer: &sweepAuthorizer{}, Due: &fakeDue{shards: 1}, Settlement: &fakeSettlement{},
		Claims: &fakeClaims{}, Clock: newSweepClock(),
		HolderID: "replica-a", ClaimTTL: time.Minute, PageLimit: 32, MaxPages: 2,
	}
	if _, err := NewReconciler(valid); err != nil {
		t.Fatalf("a valid configuration was refused: %v", err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*ReconcilerConfig)
	}{
		{"no authorizer", func(c *ReconcilerConfig) { c.Authorizer = nil }},
		{"no due query", func(c *ReconcilerConfig) { c.Due = nil }},
		{"no settlement", func(c *ReconcilerConfig) { c.Settlement = nil }},
		{"no claims", func(c *ReconcilerConfig) { c.Claims = nil }},
		{"no clock", func(c *ReconcilerConfig) { c.Clock = nil }},
		{"no holder", func(c *ReconcilerConfig) { c.HolderID = "" }},
		{"zero claim ttl", func(c *ReconcilerConfig) { c.ClaimTTL = 0 }},
		{"claim ttl above the store's ceiling", func(c *ReconcilerConfig) {
			c.ClaimTTL = sessionstore.MaxReconciliationClaimTTL + time.Nanosecond
		}},
		{"zero page limit", func(c *ReconcilerConfig) { c.PageLimit = 0 }},
		{"page limit above the store's ceiling", func(c *ReconcilerConfig) {
			c.PageLimit = storage.MaxOrderedPageLimit + 1
		}},
		{"zero max pages", func(c *ReconcilerConfig) { c.MaxPages = 0 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := valid
			test.mutate(&cfg)
			_, err := NewReconciler(cfg)
			if !errors.Is(err, ErrInvalidReconcilerConfig) {
				t.Fatalf("NewReconciler() = %v, want ErrInvalidReconcilerConfig", err)
			}
		})
	}
	// The ceilings are the store's own constants, named rather than restated,
	// and both are driven at the boundary so neither bound is off by one.
	atCeiling := valid
	atCeiling.ClaimTTL = sessionstore.MaxReconciliationClaimTTL
	atCeiling.PageLimit = storage.MaxOrderedPageLimit
	if _, err := NewReconciler(atCeiling); err != nil {
		t.Fatalf("a configuration exactly at the store's ceilings was refused: %v", err)
	}
}

// TestEveryDispositionRendersDistinctly keeps the vocabulary usable in a log
// line. An unrecognized member renders as itself rather than as the empty
// string, which is the rule placement.Outcome states.
func TestEveryDispositionRendersDistinctly(t *testing.T) {
	t.Parallel()

	seen := map[string]Disposition{}
	for d := DispositionSettleable; d <= DispositionDeferred; d++ {
		name := d.String()
		if name == "" || name == "unrecognized" {
			t.Errorf("Disposition(%d) renders as %q", d, name)
		}
		if prior, clash := seen[name]; clash {
			t.Errorf("Disposition(%d) and Disposition(%d) both render as %q", prior, d, name)
		}
		seen[name] = d
	}
	if got := Disposition(200).String(); got != "unrecognized" {
		t.Errorf("an unknown disposition renders as %q", got)
	}
}
