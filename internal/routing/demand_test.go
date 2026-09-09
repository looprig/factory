package routing

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/clientlink"
	"github.com/looprig/sessionstore"
)

// HOW THE SCENARIO SET BELOW WAS DERIVED, since the runbook's seven are a floor
// rather than a specification and this program's most expensive recurring
// defect is covering the instances a document names and leaving their
// neighbours.
//
// The mechanism is a product of three axes, each enumerated from the CODE
// rather than from the step:
//
// Axis A -- what the registry says at a poll. Read off Directory.Owner's three
// answers and Bindings.bindLocked's branches, which together are the complete
// set of things that can come back:
//
//	A1 no owner (found = false: absent, expired, released, or no such session)
//	A2 an owner, routable, the SAME tuple the table holds
//	A3 an owner, routable, a DIFFERENT tuple -- and "different" is three
//	   sub-cases, because BindingKey names three fields beyond the session
//	A4 an owner, routable, at an OLDER lease epoch than the table holds
//	A5 an owner Core will not carry (ErrUnroutableOwner)
//	A6 the read itself failed (a registry outage)
//	A7 an owner the Host refused to bind
//
// Axis B -- what the local routing table holds when that answer arrives:
//
//	B1 unbound
//	B2 bound
//	B3 demand held, route invalidated by an observation a Host pushed
//
// Axis C -- the demand transition:
//
//	C1 0->1 first subscriber      C5 0->1 again after 0 (a fresh page)
//	C2 1->2 second subscriber     C6 a release nobody holds demand for
//	C3 2->1 not the last          C7 work after Close
//	C4 1->0 last subscriber       C8 Close with demand outstanding
//
// Not every cell is reachable: A2, A3 and A4 are questions only about a session
// that is already bound, so they require B2 or B3. Every reachable cell has a
// case below, and its axis coordinates are named in the case's doc.
//
// Three properties are NOT cells and are stated as for-alls instead, because a
// fixed fixture cannot defend them: the hint's repeatability
// (FuzzTheJournalTipHintIsAFunctionOfTheTipAlone plus its table), the interval
// (asserted against an absolute off-default literal), and the property that
// nothing on this path can restore a session (behavioural here, structural over
// the whole module in the root package's no_restore_test.go).

// testDemandLimits is deliberately NOT DefaultDemandLimits. Every case in this
// file drives an off-default interval and an off-default timeout, because a
// bound every call site passes identically is untested by construction: with
// the defaults in place, an implementation reading the wrong member of
// DemandLimits, or a constant of its own, is indistinguishable.
var testDemandLimits = DemandLimits{
	OwnershipPollInterval: 1500 * time.Millisecond,
	PollTimeout:           4321 * time.Millisecond,
}

// armedTimer is one scheduled poll.
type armedTimer struct {
	d       time.Duration
	f       func()
	stopped bool
	fired   bool
}

// manualClock is the driven clock. Nothing in this file rests on wall time.
type manualClock struct {
	mu    sync.Mutex
	armed []*armedTimer
}

func (c *manualClock) AfterFunc(d time.Duration, f func()) func() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	timer := &armedTimer{d: d, f: f}
	c.armed = append(c.armed, timer)
	return func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		if timer.stopped || timer.fired {
			return false
		}
		timer.stopped = true
		return true
	}
}

// due returns the timers that are armed and neither stopped nor already fired.
func (c *manualClock) due() []*armedTimer {
	c.mu.Lock()
	defer c.mu.Unlock()
	var due []*armedTimer
	for _, timer := range c.armed {
		if !timer.stopped && !timer.fired {
			due = append(due, timer)
		}
	}
	return due
}

// tick fires every due timer exactly once and reports how many ran. A poll arms
// its successor, so the due set is SNAPSHOT before anything runs; without that
// one tick would run the whole future.
func (c *manualClock) tick(t *testing.T) int {
	t.Helper()

	due := c.due()
	if len(due) == 0 {
		t.Fatal("no poll was armed, so a tick would drive nothing")
	}
	for _, timer := range due {
		c.mu.Lock()
		timer.fired = true
		c.mu.Unlock()
		timer.f()
	}
	return len(due)
}

// fireEvenStopped runs a timer the demand plane believes it cancelled.
// time.Timer.Stop reports false when the callback has already started, so the
// losing side of that race is a state the production code must survive and a
// count of outcomes cannot see.
func (c *manualClock) fireEvenStopped(t *testing.T, index int) {
	t.Helper()

	c.mu.Lock()
	if index >= len(c.armed) {
		c.mu.Unlock()
		t.Fatalf("timer %d was never armed; %d exist", index, len(c.armed))
	}
	timer := c.armed[index]
	timer.fired = true
	c.mu.Unlock()
	timer.f()
}

// lastInterval is the delay the most recently armed poll was scheduled with.
func (c *manualClock) lastInterval(t *testing.T) time.Duration {
	t.Helper()

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.armed) == 0 {
		t.Fatal("no poll was armed")
	}
	return c.armed[len(c.armed)-1].d
}

func (c *manualClock) armedCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.armed)
}

// deadlineWitness records the bound each seam call was handed. A call whose
// context carries no deadline and a call whose context is already dead look
// identical in every other assertion in this file.
type deadlineWitness struct {
	mu        sync.Mutex
	remaining []time.Duration
	unbounded int
	dead      int
}

func (w *deadlineWitness) record(ctx context.Context) {
	w.mu.Lock()
	defer w.mu.Unlock()
	deadline, ok := ctx.Deadline()
	if !ok {
		w.unbounded++
		return
	}
	if ctx.Err() != nil {
		w.dead++
	}
	w.remaining = append(w.remaining, time.Until(deadline))
}

// require asserts every recorded bound sits inside (floor, ceiling].
func (w *deadlineWitness) require(t *testing.T, what string, floor, ceiling time.Duration) {
	t.Helper()

	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.remaining) == 0 {
		t.Fatalf("%s recorded no bounded call, so this assertion is vacuous", what)
	}
	if w.unbounded != 0 {
		t.Errorf("%s received %d call(s) with no deadline at all", what, w.unbounded)
	}
	if w.dead != 0 {
		t.Errorf("%s received %d already-cancelled context(s), which does nothing and looks like success", what, w.dead)
	}
	for i, remaining := range w.remaining {
		if remaining <= floor || remaining > ceiling {
			t.Errorf("%s call %d had %v left, want (%v, %v]", what, i, remaining, floor, ceiling)
		}
	}
}

// deadlineResolver is the registry read with its bound observed.
type deadlineResolver struct {
	*recordingResolver
	witness *deadlineWitness
}

func (r *deadlineResolver) Owner(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) (sessionwire.HostLinkRegistryObservation, bool, error) {
	r.witness.record(ctx)
	return r.recordingResolver.Owner(ctx, tenant, session)
}

// scriptedTips is the durable journal read, scripted per call and recording the
// WHOLE request, because "the tip read is bounded" is a claim about the request
// this package builds and not about the answer it gets back.
type scriptedTips struct {
	mu       sync.Mutex
	witness  *deadlineWitness
	requests []sessionstore.ReadPublicJournalRequest
	// tips is consumed one per call; the last value repeats forever, so a case
	// that cares about one tip writes one.
	tips []uint64
	next int
	err  error
}

func (s *scriptedTips) ReadPublicJournal(ctx context.Context, req sessionstore.ReadPublicJournalRequest) (sessionwire.JournalPage, error) {
	if s.witness != nil {
		s.witness.record(ctx)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, req)
	if s.err != nil {
		return sessionwire.JournalPage{}, s.err
	}
	tip := uint64(0)
	if len(s.tips) > 0 {
		if s.next >= len(s.tips) {
			tip = s.tips[len(s.tips)-1]
		} else {
			tip = s.tips[s.next]
		}
		s.next++
	}
	// A tip read looks at CapturedTip and nothing else, so the page carries an
	// event the production code must ignore.
	return sessionwire.JournalPage{
		CapturedTip:    tip,
		CoveredThrough: tip,
		Events: []sessionwire.JournalEvent{
			{EventID: "event-1", JournalSeq: tip, Body: json.RawMessage(`{"type":"noise"}`)},
		},
	}, nil
}

func (s *scriptedTips) reads() []sessionstore.ReadPublicJournalRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]sessionstore.ReadPublicJournalRequest(nil), s.requests...)
}

// recordingHinter is the local publication plane. It keeps the marshalled BYTES
// as well as the value, because repeatability is a claim about what two
// publications of one tip are, not about what two structs compare equal as.
type recordingHinter struct {
	mu         sync.Mutex
	witness    *deadlineWitness
	hints      []sessionwire.JournalTip
	encoded    []string
	marshalErr error
	err        error
}

func (h *recordingHinter) PublishJournalTip(ctx context.Context, hint sessionwire.JournalTip) error {
	if h.witness != nil {
		h.witness.record(ctx)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.hints = append(h.hints, hint)
	encoded, err := json.Marshal(hint)
	if err != nil {
		h.marshalErr = err
	}
	h.encoded = append(h.encoded, string(encoded))
	return h.err
}

func (h *recordingHinter) published() []sessionwire.JournalTip {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]sessionwire.JournalTip(nil), h.hints...)
}

func (h *recordingHinter) bytes() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.encoded...)
}

func (h *recordingHinter) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.hints)
}

// demandFixture is one composed demand plane over the REAL Bindings table.
//
// It is deliberately not a fake routing table: every ownership scenario below
// is driven through the table that production uses, so "the owner changed" is
// exercised as the table's own invalidation rule rather than as an expectation
// restated here.
type demandFixture struct {
	resolver *recordingResolver
	binder   *recordingBinder
	tips     *scriptedTips
	hints    *recordingHinter
	clock    *manualClock
	bindings *Bindings
	demand   *Demand
	witness  *deadlineWitness
}

func newDemandFixture(t *testing.T, limits DemandLimits, obs ...sessionwire.HostLinkRegistryObservation) *demandFixture {
	t.Helper()

	witness := &deadlineWitness{}
	resolver := newResolver(obs...)
	binder := &recordingBinder{}
	tips := &scriptedTips{witness: witness}
	hints := &recordingHinter{witness: witness}
	clock := &manualClock{}
	bindings := newBindings(t, &deadlineResolver{recordingResolver: resolver, witness: witness}, binder)
	demand, err := NewDemand(bindings, tips, hints, clock, limits)
	if err != nil {
		t.Fatalf("NewDemand: %v", err)
	}
	return &demandFixture{
		resolver: resolver, binder: binder, tips: tips, hints: hints,
		clock: clock, bindings: bindings, demand: demand, witness: witness,
	}
}

func (f *demandFixture) acquire(t *testing.T, session sessionwire.SessionID) {
	t.Helper()

	if err := f.demand.Acquire(context.Background(), bindTenant, session); err != nil {
		t.Fatalf("Acquire(%s): %v", session, err)
	}
}

func (f *demandFixture) release(t *testing.T, session sessionwire.SessionID) {
	t.Helper()

	if err := f.demand.Release(context.Background(), bindTenant, session); err != nil {
		t.Fatalf("Release(%s): %v", session, err)
	}
}

// bound reports what the routing table holds, which is the authority on whether
// this replica has a route -- not the demand plane's own bookkeeping.
func (f *demandFixture) bound(t *testing.T, session sessionwire.SessionID) (Binding, bool) {
	t.Helper()

	return f.bindings.Binding(bindTenant, session)
}

// ---------------------------------------------------------------------------
// Step 1 -- the first subscriber reads the registry and binds, or does not.
// ---------------------------------------------------------------------------

// TestTheFirstSubscriberBindsToTheSessionsOwner is A2/B1/C1.
func TestTheFirstSubscriberBindsToTheSessionsOwner(t *testing.T) {
	t.Parallel()

	f := newDemandFixture(t, testDemandLimits, observation("host-a", 1, 1))
	f.acquire(t, bindSession)

	binding, ok := f.bound(t, bindSession)
	if !ok {
		t.Fatal("the first subscriber left the session unbound although it has an owner")
	}
	if binding.Key.HostID != "host-a" {
		t.Errorf("bound to %q, want host-a", binding.Key.HostID)
	}
	if got := f.binder.bindCount(); got != 1 {
		t.Errorf("%d binds, want exactly 1", got)
	}
	if got := f.hints.count(); got != 0 {
		t.Errorf("a bound session published %d hints, want none: the hint is the degraded mode", got)
	}
}

// TestTheFirstSubscriberOfAnOwnerlessSessionIsWatchedUnbound is A1/B1/C1, and
// it is the carry-forward A7.2-subscribe-no-restore in its behavioural half:
// the subscriber is neither refused nor served by making the session resident.
func TestTheFirstSubscriberOfAnOwnerlessSessionIsWatchedUnbound(t *testing.T) {
	t.Parallel()

	f := newDemandFixture(t, testDemandLimits)
	if err := f.demand.Acquire(context.Background(), bindTenant, bindSession); err != nil {
		t.Fatalf("Acquire on an ownerless session was refused: %v", err)
	}
	if _, ok := f.bound(t, bindSession); ok {
		t.Error("an ownerless session was bound; there is nothing to bind to")
	}
	if got := f.demand.Watching(bindTenant, bindSession); got != 1 {
		t.Errorf("Watching = %d, want 1: the subscriber is still watched", got)
	}
	if got := f.resolver.count(); got != 1 {
		t.Errorf("%d registry reads, want exactly 1", got)
	}
	if got := f.binder.bindCount(); got != 0 {
		t.Errorf("%d binds against a session with no owner, want none", got)
	}
	if got := f.clock.armedCount(); got != 1 {
		t.Errorf("%d polls armed, want exactly 1: demand exists, so ownership is polled", got)
	}
}

// TestViewingAColdSessionNeitherRestoresItNorPlacesIt is the behavioural half
// of A7.2-subscribe-no-restore, stated over the WHOLE call log rather than over
// one absence.
//
// A restore is a durable command. The only seam here that could carry one is
// the Binder's DeliverCommand, and the only seam that could create an owner is
// one this type does not have; so the assertion is that a whole subscribe plus
// three polls of a cold session produces registry READS, journal READS and
// local publications, and nothing else at all. The structural half -- which is
// what a later composition would break, and what the seams alone cannot hold --
// is TestNothingOnTheViewingPathAdmitsACommand in the root package.
func TestViewingAColdSessionNeitherRestoresItNorPlacesIt(t *testing.T) {
	t.Parallel()

	f := newDemandFixture(t, testDemandLimits)
	f.tips.tips = []uint64{7}
	f.acquire(t, bindSession)
	for range 3 {
		f.clock.tick(t)
	}

	if got := f.binder.bindCount(); got != 0 {
		t.Errorf("%d binds, want none: viewing does not place a session", got)
	}
	if got := f.binder.unbindCount(); got != 0 {
		t.Errorf("%d unbinds, want none", got)
	}
	f.binder.mu.Lock()
	delivered := len(f.binder.delivered)
	f.binder.mu.Unlock()
	if delivered != 0 {
		t.Errorf("%d commands delivered, want none: viewing admits nothing", delivered)
	}
	// The positive control. Without it a demand plane that did nothing at all
	// would satisfy every assertion above.
	if got := f.resolver.count(); got != 4 {
		t.Errorf("%d registry reads, want 4 (one subscribe and three polls)", got)
	}
	if got := f.hints.count(); got != 4 {
		t.Errorf("%d hints, want 4: a cold session is served by repair hints", got)
	}
}

// TestAnUnroutableOwnerLeavesTheSessionWatchedAndHinting is A5/B1.
func TestAnUnroutableOwnerLeavesTheSessionWatchedAndHinting(t *testing.T) {
	t.Parallel()

	// A zero lease epoch: Core's bind request refuses it, so the tuple never
	// reaches the transport. It is bindings_test.go's own unroutable owner,
	// named the same way here so the two cannot drift onto different shapes.
	unroutable := observation("host-a", 1, 1)
	unroutable.LeaseEpoch = 0
	f := newDemandFixture(t, testDemandLimits, unroutable)
	f.acquire(t, bindSession)

	if _, ok := f.bound(t, bindSession); ok {
		t.Fatal("bound to an owner Core will not carry")
	}
	if got := f.binder.bindCount(); got != 0 {
		t.Errorf("%d binds, want none: the tuple never reached the transport", got)
	}
	if got := f.hints.count(); got != 1 {
		t.Errorf("%d hints, want 1: no binding means the degraded mode", got)
	}
}

// TestABindRefusedByTheHostIsRetriedAtTheNextPoll is A7/B1, and its second half
// is the recovery: a refusal is not terminal for the subscriber.
func TestABindRefusedByTheHostIsRetriedAtTheNextPoll(t *testing.T) {
	t.Parallel()

	f := newDemandFixture(t, testDemandLimits, observation("host-a", 1, 1))
	f.binder.bindErr = errors.New("host refused the lease epoch")
	f.acquire(t, bindSession)

	if _, ok := f.bound(t, bindSession); ok {
		t.Fatal("a refused bind left a route behind")
	}
	if got := f.hints.count(); got != 1 {
		t.Errorf("%d hints after a refused bind, want 1", got)
	}

	f.binder.mu.Lock()
	f.binder.bindErr = nil
	f.binder.mu.Unlock()
	f.clock.tick(t)

	if _, ok := f.bound(t, bindSession); !ok {
		t.Fatal("the next poll did not retry the bind")
	}
	if got := f.hints.count(); got != 1 {
		t.Errorf("%d hints once bound, want the original 1", got)
	}
}

// TestARegistryOutageWhileUnboundHintsAndRetriesNextPoll is A6/B1.
func TestARegistryOutageWhileUnboundHintsAndRetriesNextPoll(t *testing.T) {
	t.Parallel()

	f := newDemandFixture(t, testDemandLimits, observation("host-a", 1, 1))
	f.resolver.mu.Lock()
	f.resolver.err = errors.New("registry unreachable")
	f.resolver.mu.Unlock()

	if err := f.demand.Acquire(context.Background(), bindTenant, bindSession); err != nil {
		t.Fatalf("a registry outage refused the subscriber: %v", err)
	}
	if _, ok := f.bound(t, bindSession); ok {
		t.Fatal("bound during a registry outage")
	}
	if got := f.hints.count(); got != 1 {
		t.Errorf("%d hints during an outage, want 1", got)
	}

	f.resolver.mu.Lock()
	f.resolver.err = nil
	f.resolver.mu.Unlock()
	f.clock.tick(t)

	if _, ok := f.bound(t, bindSession); !ok {
		t.Fatal("the poll did not bind once the registry answered again")
	}
}

// ---------------------------------------------------------------------------
// Step 2 -- polling while demand exists.
// ---------------------------------------------------------------------------

// TestAnOwnerArrivingBetweenPollsBindsAndStopsHinting is the runbook's "Factory
// A subscriber while Factory B admits and places work", stated as the axis
// transition it actually is: A1/B1 becoming A2/B1 with no local event at all.
//
// Nothing on this replica caused the owner to appear. Factory B admitted the
// work and placement gave the session a Host; this replica learns it by
// polling, which is the whole reason the poll exists.
func TestAnOwnerArrivingBetweenPollsBindsAndStopsHinting(t *testing.T) {
	t.Parallel()

	f := newDemandFixture(t, testDemandLimits)
	f.acquire(t, bindSession)
	f.clock.tick(t)

	if got := f.hints.count(); got != 2 {
		t.Fatalf("%d hints before the owner appeared, want 2", got)
	}

	// Factory B's work lands: the session now has an owner in the registry.
	f.resolver.put(observation("host-b", 3, 9))
	f.clock.tick(t)

	binding, ok := f.bound(t, bindSession)
	if !ok {
		t.Fatal("the poll did not bind after an owner appeared")
	}
	if binding.Key.HostID != "host-b" || binding.Key.LeaseEpoch != 9 {
		t.Errorf("bound to %+v, want host-b at epoch 9", binding.Key)
	}
	if got := f.hints.count(); got != 2 {
		t.Errorf("%d hints, want the original 2: a bound session is served live", got)
	}
	f.clock.tick(t)
	if got := f.hints.count(); got != 2 {
		t.Errorf("%d hints after a further poll, want 2", got)
	}
}

// TestAnOwnerChangeRebindsToTheNewOwner is A3/B2, driven over each of the three
// fields that can differ. One case per field, because BindingKey names three
// and a case covering only the Host would leave a generation or an epoch change
// silently routed to a stale link.
func TestAnOwnerChangeRebindsToTheNewOwner(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		changed sessionwire.HostLinkRegistryObservation
	}{
		{"a different host", observation("host-b", 1, 1)},
		{"the same host restarted", observation("host-a", 2, 1)},
		{"a superseded lease epoch", observation("host-a", 1, 2)},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			f := newDemandFixture(t, testDemandLimits, observation("host-a", 1, 1))
			f.acquire(t, bindSession)
			f.resolver.put(testCase.changed)
			f.clock.tick(t)

			binding, ok := f.bound(t, bindSession)
			if !ok {
				t.Fatal("the owner changed and the poll left the session unbound")
			}
			want := BindingKey{
				TenantID: bindTenant, SessionID: bindSession,
				HostID: testCase.changed.HostID, HostGeneration: testCase.changed.HostGeneration,
				LeaseEpoch: testCase.changed.LeaseEpoch,
			}
			if binding.Key != want {
				t.Errorf("rebound to %+v, want %+v", binding.Key, want)
			}
			if got := f.binder.bindCount(); got != 2 {
				t.Errorf("%d binds, want 2: the old route is given back and a new one taken", got)
			}
			if got := f.binder.unbindCount(); got != 1 {
				t.Errorf("%d unbinds, want exactly 1: the stale route is released once", got)
			}
			// The demand accounting is what a rebind is easiest to get wrong.
			// Acquire COUNTS, so a rebind that did not release first would
			// leave this replica holding two units for one subscriber and the
			// last release would never unbind.
			if got := f.bindings.Demand(bindTenant, bindSession); got != 1 {
				t.Errorf("the routing table holds %d demand after a rebind, want 1", got)
			}
			if got := f.hints.count(); got != 0 {
				t.Errorf("%d hints, want none: the session was bound throughout", got)
			}
		})
	}
}

// TestAStaleRegistryReadKeepsTheBinding is A4/B2 -- the runbook's "stale
// registry".
//
// An observation at an OLDER lease epoch than the route holds cannot be current,
// so acting on it would let a lagging read flap a working route. The rule is
// Bindings.Observe's and is not restated by the poll; this case is what holds
// the poll to consulting it.
func TestAStaleRegistryReadKeepsTheBinding(t *testing.T) {
	t.Parallel()

	f := newDemandFixture(t, testDemandLimits, observation("host-a", 1, 5))
	f.acquire(t, bindSession)
	before, ok := f.bound(t, bindSession)
	if !ok {
		t.Fatal("the first subscriber did not bind")
	}

	f.resolver.put(observation("host-b", 1, 4))
	f.clock.tick(t)

	after, ok := f.bound(t, bindSession)
	if !ok {
		t.Fatal("a stale registry read dropped a live route")
	}
	if after != before {
		t.Errorf("the route moved to %+v on a stale read, want %+v", after.Key, before.Key)
	}
	if got := f.binder.unbindCount(); got != 0 {
		t.Errorf("%d unbinds on a stale read, want none", got)
	}
	if got := f.binder.bindCount(); got != 1 {
		t.Errorf("%d binds, want the original 1", got)
	}
}

// TestAnOwnerDisappearingDropsTheRouteAndResumesHinting is A1/B2: the reverse
// transition of the Factory-B case, which needs its own driver because
// "unbound" is reached by a different statement than "never bound".
func TestAnOwnerDisappearingDropsTheRouteAndResumesHinting(t *testing.T) {
	t.Parallel()

	f := newDemandFixture(t, testDemandLimits, observation("host-a", 1, 1))
	f.acquire(t, bindSession)

	f.resolver.mu.Lock()
	f.resolver.found[bindSession] = false
	f.resolver.mu.Unlock()
	f.clock.tick(t)

	if _, ok := f.bound(t, bindSession); ok {
		t.Fatal("the route survived its owner")
	}
	if got := f.binder.unbindCount(); got != 1 {
		t.Errorf("%d unbinds, want 1", got)
	}
	if got := f.bindings.Demand(bindTenant, bindSession); got != 0 {
		t.Errorf("the routing table still holds %d demand for an unbound session, want 0", got)
	}
	if got := f.hints.count(); got != 1 {
		t.Errorf("%d hints, want 1: with no owner the viewer is served by hints again", got)
	}
	// And it recovers, so this is not a terminal state.
	f.resolver.put(observation("host-c", 1, 4))
	f.clock.tick(t)
	if _, ok := f.bound(t, bindSession); !ok {
		t.Fatal("the session did not rebind when an owner reappeared")
	}
}

// TestARouteInvalidatedByAHostObservationIsRebound is B3: a Host pushed an
// observation that contradicted the route, so the table dropped it while the
// demand remained. That is a different state from A3 -- the registry has NOT
// moved by the time the poll reads it -- and it is the state Bindings' own doc
// says the next Acquire repairs.
func TestARouteInvalidatedByAHostObservationIsRebound(t *testing.T) {
	t.Parallel()

	f := newDemandFixture(t, testDemandLimits, observation("host-a", 1, 1))
	f.acquire(t, bindSession)

	// The Host says the session moved. The registry read the poll makes will
	// report the same thing, but the invalidation has already happened.
	moved := observation("host-b", 1, 2)
	if !f.bindings.Observe(context.Background(), moved) {
		t.Fatal("the pushed observation did not invalidate the route")
	}
	f.resolver.put(moved)
	f.clock.tick(t)

	binding, ok := f.bound(t, bindSession)
	if !ok {
		t.Fatal("an invalidated route was not rebound")
	}
	if binding.Key.HostID != "host-b" || binding.Key.LeaseEpoch != 2 {
		t.Errorf("rebound to %+v, want host-b at epoch 2", binding.Key)
	}
	if got := f.bindings.Demand(bindTenant, bindSession); got != 1 {
		t.Errorf("the routing table holds %d demand, want 1", got)
	}
}

// TestARegistryOutageKeepsAHeldRouteAndRetriesNextPoll is A6/B2, and it is the
// opposite decision from A6/B1 above: an outage must not be read as "no owner",
// because dropping a working route on every registry blip is a rebind storm
// that has delivered nothing.
func TestARegistryOutageKeepsAHeldRouteAndRetriesNextPoll(t *testing.T) {
	t.Parallel()

	f := newDemandFixture(t, testDemandLimits, observation("host-a", 1, 1))
	f.acquire(t, bindSession)

	f.resolver.mu.Lock()
	f.resolver.err = errors.New("registry unreachable")
	f.resolver.mu.Unlock()
	f.clock.tick(t)

	if _, ok := f.bound(t, bindSession); !ok {
		t.Fatal("a registry outage dropped a live route")
	}
	if got := f.binder.unbindCount(); got != 0 {
		t.Errorf("%d unbinds during an outage, want none", got)
	}
	if got := f.hints.count(); got != 0 {
		t.Errorf("%d hints while still bound, want none", got)
	}
	if got := f.clock.armedCount(); got != 2 {
		t.Errorf("%d polls armed, want 2: an outage does not stop the poll", got)
	}
}

// TestASteadyPollNeitherRebindsNorAccumulatesDemand is A2/B2: the ordinary case,
// where the registry agrees with the route and nothing should happen.
//
// It exists because "nothing should happen" was unasserted and a mutation
// deleting serveLocked's first condition -- so that every poll of a bound
// session called Bindings.Acquire again -- SURVIVED the rest of this file. The
// consequence is not a wasted call: Bindings.Acquire counts demand, so the
// routing table would gain a unit per poll and the last subscriber's release
// would give back one of many, leaving a HostLink route open for nobody until
// the replica restarts. The count is therefore the assertion, and the release
// at the end is what turns it into an observable outcome.
func TestASteadyPollNeitherRebindsNorAccumulatesDemand(t *testing.T) {
	t.Parallel()

	f := newDemandFixture(t, testDemandLimits, observation("host-a", 1, 1))
	f.acquire(t, bindSession)
	for range 3 {
		f.clock.tick(t)
	}

	if got := f.binder.bindCount(); got != 1 {
		t.Errorf("%d binds over three steady polls, want the original 1", got)
	}
	if got := f.binder.unbindCount(); got != 0 {
		t.Errorf("%d unbinds over three steady polls, want none", got)
	}
	if got := f.bindings.Demand(bindTenant, bindSession); got != 1 {
		t.Errorf("the routing table holds %d demand after three polls of ONE subscriber, want 1", got)
	}
	// The consequence, made observable: the single subscriber's release must
	// be the last one, so it unbinds.
	f.release(t, bindSession)
	if got := f.binder.unbindCount(); got != 1 {
		t.Errorf("the last subscriber left and %d unbinds happened, want 1: the route outlived its demand", got)
	}
	if _, ok := f.bound(t, bindSession); ok {
		t.Error("the route survived its last subscriber")
	}
}

// TestThePollIsArmedAtTheConfiguredInterval holds the bound to an ABSOLUTE
// literal, off the default, so a poll scheduled from any other duration in
// DemandLimits -- or from a constant of the implementation's own -- fails.
func TestThePollIsArmedAtTheConfiguredInterval(t *testing.T) {
	t.Parallel()

	limits := DemandLimits{OwnershipPollInterval: 1500 * time.Millisecond, PollTimeout: 4321 * time.Millisecond}
	f := newDemandFixture(t, limits, observation("host-a", 1, 1))
	f.acquire(t, bindSession)

	if got := f.clock.lastInterval(t); got != 1500*time.Millisecond {
		t.Errorf("the first poll was armed at %v, want 1.5s", got)
	}
	f.clock.tick(t)
	if got := f.clock.lastInterval(t); got != 1500*time.Millisecond {
		t.Errorf("the second poll was armed at %v, want 1.5s", got)
	}
	// The interval is a GAP: exactly one poll is outstanding at any moment, so
	// a slow poll delays its successor instead of overlapping with it.
	if got := len(f.clock.due()); got != 1 {
		t.Errorf("%d polls are outstanding, want exactly 1", got)
	}
	if got := DefaultDemandLimits().OwnershipPollInterval; got == limits.OwnershipPollInterval {
		t.Errorf("the case drives the default interval (%v), so it could not see a default being used instead", got)
	}
}

// TestEveryBackgroundCallIsBoundedByTheConfiguredTimeout covers the half a poll
// has nothing else for: it runs on the clock's goroutine with no caller and no
// request context, so PollTimeout is the entire bound on it.
func TestEveryBackgroundCallIsBoundedByTheConfiguredTimeout(t *testing.T) {
	t.Parallel()

	limits := DemandLimits{OwnershipPollInterval: 1500 * time.Millisecond, PollTimeout: 4321 * time.Millisecond}
	f := newDemandFixture(t, limits)
	f.acquire(t, bindSession)
	f.clock.tick(t)
	f.resolver.put(observation("host-a", 1, 1))
	f.clock.tick(t)

	// The floor is above the interval, so a bound taken from
	// OwnershipPollInterval fails here rather than merely being smaller.
	f.witness.require(t, "the demand plane's background calls", 4*time.Second, 4321*time.Millisecond)
}

// ---------------------------------------------------------------------------
// The hint, and what "repeatable" means.
// ---------------------------------------------------------------------------

// TestEveryUnboundPollRepublishesTheCurrentTip is the operational half of
// repeatability: the hint is published on EVERY unbound poll, including when
// the tip has not moved and including when it is zero.
//
// Publishing only on a change would make the hint a delta in everything but
// name, and a client that joined between two changes would wait for the next
// write to learn where the journal already was.
func TestEveryUnboundPollRepublishesTheCurrentTip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		tips []uint64
	}{
		{"an unmoving tip", []uint64{4, 4, 4, 4}},
		{"a cold session", []uint64{0, 0, 0, 0}},
		{"a growing journal", []uint64{0, 1, 5, 12}},
		{"a tip that repeats after moving", []uint64{3, 9, 9, 9}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			f := newDemandFixture(t, testDemandLimits)
			f.tips.tips = testCase.tips
			f.acquire(t, bindSession)
			for range len(testCase.tips) - 1 {
				f.clock.tick(t)
			}

			published := f.hints.published()
			if len(published) != len(testCase.tips) {
				t.Fatalf("%d hints for %d reads, want one per read", len(published), len(testCase.tips))
			}
			for i, hint := range published {
				want := sessionwire.JournalTip{TenantID: bindTenant, SessionID: bindSession, Tip: testCase.tips[i]}
				if hint != want {
					t.Errorf("hint %d = %+v, want %+v", i, hint, want)
				}
			}
		})
	}
}

// TestTwoHintsForOneTipAreTheSameBytes is the repeatability property itself.
// A hint carrying a sequence, a nonce, an attempt count or a timestamp would
// still satisfy the case above; this is what says a client may apply one twice
// and lose nothing.
func TestTwoHintsForOneTipAreTheSameBytes(t *testing.T) {
	t.Parallel()

	f := newDemandFixture(t, testDemandLimits)
	f.tips.tips = []uint64{11, 11, 11}
	f.acquire(t, bindSession)
	f.clock.tick(t)
	f.clock.tick(t)

	encoded := f.hints.bytes()
	if len(encoded) != 3 {
		t.Fatalf("%d hints, want 3", len(encoded))
	}
	if f.hints.marshalErr != nil {
		t.Fatalf("a published hint could not be marshalled: %v", f.hints.marshalErr)
	}
	for i, got := range encoded[1:] {
		if got != encoded[0] {
			t.Errorf("hint %d encoded as %s, want the identical %s", i+1, got, encoded[0])
		}
	}
	const want = `{"journal_tip":11,"session_id":"session-a","tenant_id":"tenant-a","type":"journal_tip"}`
	if encoded[0] != want {
		t.Errorf("the hint encoded as %s, want %s", encoded[0], want)
	}
}

// TestABoundSessionPublishesNoHint is a "nothing happened" assertion, so it
// carries its own positive control: the same fixture, one owner removed.
func TestABoundSessionPublishesNoHint(t *testing.T) {
	t.Parallel()

	bound := newDemandFixture(t, testDemandLimits, observation("host-a", 1, 1))
	bound.acquire(t, bindSession)
	bound.clock.tick(t)
	bound.clock.tick(t)
	if got := bound.hints.count(); got != 0 {
		t.Errorf("a bound session published %d hints, want none", got)
	}
	if got := len(bound.tips.reads()); got != 0 {
		t.Errorf("a bound session made %d journal reads, want none", got)
	}

	unbound := newDemandFixture(t, testDemandLimits)
	unbound.acquire(t, bindSession)
	unbound.clock.tick(t)
	unbound.clock.tick(t)
	if got := unbound.hints.count(); got != 3 {
		t.Fatalf("the control published %d hints over the same three polls, want 3; without it "+
			"a demand plane that never hinted at all would pass the assertion above", got)
	}
}

// TestATipThatCannotBeReadPublishesNothing is the fail-closed direction: a hint
// is a claim about the durable journal and this replica has no other source for
// one, so a read failure publishes nothing rather than publishing zero.
func TestATipThatCannotBeReadPublishesNothing(t *testing.T) {
	t.Parallel()

	f := newDemandFixture(t, testDemandLimits)
	f.tips.err = errors.New("journal unavailable")
	f.acquire(t, bindSession)
	f.clock.tick(t)

	if got := f.hints.count(); got != 0 {
		t.Errorf("%d hints published from a failed read, want none", got)
	}
	if got := len(f.tips.reads()); got != 2 {
		t.Errorf("%d journal reads, want 2: the read is retried, not abandoned", got)
	}

	f.tips.mu.Lock()
	f.tips.err = nil
	f.tips.tips = []uint64{6}
	f.tips.mu.Unlock()
	f.clock.tick(t)
	published := f.hints.published()
	if len(published) != 1 || published[0].Tip != 6 {
		t.Errorf("after the journal answered again the hints were %+v, want one at tip 6", published)
	}
}

// TestTheTipReadIsBoundedAndAsksOnlyForTheTip pins the REQUEST this package
// builds. A tip read that walked from sequence one, or that asked for a useful
// page, would produce the same hint and cost the store the whole journal.
func TestTheTipReadIsBoundedAndAsksOnlyForTheTip(t *testing.T) {
	t.Parallel()

	f := newDemandFixture(t, testDemandLimits)
	f.acquire(t, bindSession)

	reads := f.tips.reads()
	if len(reads) != 1 {
		t.Fatalf("%d journal reads, want 1", len(reads))
	}
	want := sessionstore.ReadPublicJournalRequest{
		TenantID: bindTenant, SessionID: bindSession,
		Tail: true, Limit: 1, ScanLimit: 1,
	}
	if reads[0] != want {
		t.Errorf("the tip read was %+v, want %+v", reads[0], want)
	}
}

// TestAHintNamingAnIdentityCoreWillNotCarryIsNotPublished covers the
// fail-closed arm on the publish. Core's own MarshalJSON validates, so an
// invalid hint reaching the transport is a marshalling failure naming nothing;
// this package refuses it where the identity is still in hand.
func TestAHintNamingAnIdentityCoreWillNotCarryIsNotPublished(t *testing.T) {
	t.Parallel()

	f := newDemandFixture(t, testDemandLimits)
	oversized := sessionwire.SessionID(strings.Repeat("s", sessionwire.MaxIDBytes+1))
	if err := f.demand.Acquire(context.Background(), bindTenant, oversized); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	if got := f.hints.count(); got != 0 {
		t.Errorf("%d hints published for an identity Core will not carry, want none", got)
	}
	// The control: the same fixture, an identity Core does carry.
	f.acquire(t, bindSession)
	if got := f.hints.count(); got != 1 {
		t.Errorf("%d hints for a valid identity, want 1", got)
	}
}

// TestAPublishFailureDoesNotStopTheNextPoll. Republishing IS the repair, so a
// transport that refused one hint must not end the sequence.
func TestAPublishFailureDoesNotStopTheNextPoll(t *testing.T) {
	t.Parallel()

	f := newDemandFixture(t, testDemandLimits)
	f.hints.err = errors.New("no local subscriber could be reached")
	f.tips.tips = []uint64{2}
	f.acquire(t, bindSession)
	f.clock.tick(t)

	f.hints.mu.Lock()
	f.hints.err = nil
	f.hints.mu.Unlock()
	f.clock.tick(t)

	if got := f.hints.count(); got != 3 {
		t.Errorf("%d publish attempts, want 3: a failure is retried on the next poll", got)
	}
}

// ---------------------------------------------------------------------------
// Axis C -- the demand transitions.
// ---------------------------------------------------------------------------

// TestASecondSubscriberCostsNoRegistryReadOrBind is C2. The claim is a COUNT:
// a plane that re-read and rebound would return the same route and pass any
// comparison of the two.
func TestASecondSubscriberCostsNoRegistryReadOrBind(t *testing.T) {
	t.Parallel()

	f := newDemandFixture(t, testDemandLimits, observation("host-a", 1, 1))
	f.acquire(t, bindSession)
	f.acquire(t, bindSession)
	f.acquire(t, bindSession)

	if got := f.resolver.count(); got != 1 {
		t.Errorf("%d registry reads for three subscribers, want 1", got)
	}
	if got := f.binder.bindCount(); got != 1 {
		t.Errorf("%d binds for three subscribers, want 1", got)
	}
	if got := f.demand.Watching(bindTenant, bindSession); got != 3 {
		t.Errorf("Watching = %d, want 3", got)
	}
	if got := f.clock.armedCount(); got != 1 {
		t.Errorf("%d polls armed for one session, want 1", got)
	}
}

// TestOnlyTheLastSubscriberGivesTheRouteBack is C3 and C4 in one order, because
// "the count reached zero" and "the count happens to be one" are different
// events and only the first may unbind.
func TestOnlyTheLastSubscriberGivesTheRouteBack(t *testing.T) {
	t.Parallel()

	f := newDemandFixture(t, testDemandLimits, observation("host-a", 1, 1))
	f.acquire(t, bindSession)
	f.acquire(t, bindSession)

	f.release(t, bindSession)
	if _, ok := f.bound(t, bindSession); !ok {
		t.Fatal("a non-final release dropped the route")
	}
	if got := f.binder.unbindCount(); got != 0 {
		t.Errorf("%d unbinds on a non-final release, want none", got)
	}

	f.release(t, bindSession)
	if _, ok := f.bound(t, bindSession); ok {
		t.Error("the last release left the route behind")
	}
	if got := f.binder.unbindCount(); got != 1 {
		t.Errorf("%d unbinds on the last release, want 1", got)
	}
	if got := f.demand.Len(); got != 0 {
		t.Errorf("%d sessions still watched, want 0", got)
	}
}

// TestTheLastSubscriberStopsThePoll is C4's other half, and it drives the
// SUPERSEDED timer deliberately: time.Timer.Stop reports false when the
// callback has already started, so the losing side of that race is a state the
// code must survive, and no count of outcomes can see it.
func TestTheLastSubscriberStopsThePoll(t *testing.T) {
	t.Parallel()

	f := newDemandFixture(t, testDemandLimits, observation("host-a", 1, 1))
	f.acquire(t, bindSession)
	f.release(t, bindSession)

	reads := f.resolver.count()
	f.clock.fireEvenStopped(t, 0)

	if got := f.resolver.count(); got != reads {
		t.Errorf("the superseded poll made %d further registry reads, want none", got-reads)
	}
	if got := f.hints.count(); got != 0 {
		t.Errorf("the superseded poll published %d hints for a session nobody watches, want none", got)
	}
	if got := f.demand.Len(); got != 0 {
		t.Errorf("the superseded poll left %d sessions watched, want 0", got)
	}
	if got := f.clock.armedCount(); got != 1 {
		t.Errorf("the superseded poll armed a successor: %d timers exist, want the original 1", got)
	}
}

// TestWatchingASessionAgainAfterTheLastSubscriberBindsAfresh is C5 -- the
// runbook's "reconnect", past the ClientLink edge's debounce, where this plane
// has genuinely been told the last subscriber went.
func TestWatchingASessionAgainAfterTheLastSubscriberBindsAfresh(t *testing.T) {
	t.Parallel()

	f := newDemandFixture(t, testDemandLimits, observation("host-a", 1, 1))
	f.acquire(t, bindSession)
	f.release(t, bindSession)
	f.acquire(t, bindSession)

	if _, ok := f.bound(t, bindSession); !ok {
		t.Fatal("the reconnecting subscriber did not bind")
	}
	if got := f.binder.bindCount(); got != 2 {
		t.Errorf("%d binds, want 2: the route was given back and taken again", got)
	}
	if got := f.bindings.Demand(bindTenant, bindSession); got != 1 {
		t.Errorf("the routing table holds %d demand, want 1", got)
	}
	// The old poll must not survive the gap. Firing it, even as the losing side
	// of the stop race, must not disturb the new one.
	f.clock.fireEvenStopped(t, 0)
	if got := f.binder.bindCount(); got != 2 {
		t.Errorf("the superseded poll acted: %d binds, want 2", got)
	}
	if got := f.clock.armedCount(); got != 2 {
		t.Errorf("%d timers armed, want 2: the superseded poll armed a successor", got)
	}
}

// TestReleasingSubscriberDemandNobodyHoldsIsRefused is C6.
func TestReleasingSubscriberDemandNobodyHoldsIsRefused(t *testing.T) {
	t.Parallel()

	f := newDemandFixture(t, testDemandLimits, observation("host-a", 1, 1))
	if err := f.demand.Release(context.Background(), bindTenant, bindSession); !errors.Is(err, ErrNoDemand) {
		t.Errorf("Release with no subscriber = %v, want ErrNoDemand", err)
	}
	f.acquire(t, bindSession)
	f.release(t, bindSession)
	if err := f.demand.Release(context.Background(), bindTenant, bindSession); !errors.Is(err, ErrNoDemand) {
		t.Errorf("a second final Release = %v, want ErrNoDemand", err)
	}
	if got := f.binder.unbindCount(); got != 1 {
		t.Errorf("%d unbinds, want exactly 1: a release nobody holds gives nothing back", got)
	}
}

// TestAClosedDemandPlaneRefusesLaterWork is C7.
func TestAClosedDemandPlaneRefusesLaterWork(t *testing.T) {
	t.Parallel()

	f := newDemandFixture(t, testDemandLimits, observation("host-a", 1, 1))
	if err := f.demand.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := f.demand.Acquire(context.Background(), bindTenant, bindSession); !errors.Is(err, ErrDemandClosed) {
		t.Errorf("Acquire after Close = %v, want ErrDemandClosed", err)
	}
	if err := f.demand.Release(context.Background(), bindTenant, bindSession); !errors.Is(err, ErrDemandClosed) {
		t.Errorf("Release after Close = %v, want ErrDemandClosed", err)
	}
	if got := f.binder.bindCount(); got != 0 {
		t.Errorf("%d binds after Close, want none", got)
	}
}

// TestCloseStopsEveryPollAndGivesEveryRouteBack is C8. Every session is
// attempted and every failure joined, so one Host's refusal cannot strand the
// routes on the others.
func TestCloseStopsEveryPollAndGivesEveryRouteBack(t *testing.T) {
	t.Parallel()

	second := sessionwire.SessionID("session-b")
	other := observation("host-b", 1, 1)
	other.SessionID = second
	f := newDemandFixture(t, testDemandLimits, observation("host-a", 1, 1), other)
	f.acquire(t, bindSession)
	f.acquire(t, second)

	f.binder.mu.Lock()
	f.binder.unbindErr = errors.New("host unreachable")
	f.binder.mu.Unlock()

	err := f.demand.Close(context.Background())
	if err == nil {
		t.Fatal("Close reported success although both unbinds failed")
	}
	if got := f.binder.unbindCount(); got != 2 {
		t.Errorf("%d unbinds, want 2: a failure on one session may not strand the other", got)
	}
	if got := f.demand.Len(); got != 0 {
		t.Errorf("%d sessions still watched after Close, want 0", got)
	}
	if got := len(f.clock.due()); got != 0 {
		t.Errorf("%d polls are still armed after Close, want none", got)
	}
	// Even the losing side of the stop race must do nothing.
	reads := f.resolver.count()
	f.clock.fireEvenStopped(t, 0)
	if got := f.resolver.count(); got != reads {
		t.Errorf("a poll ran after Close: %d further registry reads", got-reads)
	}
}

// TestTwoWatchedSessionsPollIndependently is the axis-B/axis-C interaction the
// per-session table exists for: one session's state may not decide another's.
func TestTwoWatchedSessionsPollIndependently(t *testing.T) {
	t.Parallel()

	second := sessionwire.SessionID("session-b")
	f := newDemandFixture(t, testDemandLimits, observation("host-a", 1, 1))
	f.acquire(t, bindSession)
	f.acquire(t, second)

	if _, ok := f.bound(t, bindSession); !ok {
		t.Fatal("the owned session did not bind")
	}
	if _, ok := f.bound(t, second); ok {
		t.Fatal("the ownerless session was bound")
	}
	f.clock.tick(t)
	published := f.hints.published()
	for _, hint := range published {
		if hint.SessionID != second {
			t.Errorf("a hint was published for %q, want only %q", hint.SessionID, second)
		}
	}
	if len(published) != 2 {
		t.Errorf("%d hints, want 2 for the unwatched-by-any-Host session alone", len(published))
	}

	f.release(t, second)
	if got := f.demand.Watching(bindTenant, bindSession); got != 1 {
		t.Errorf("releasing one session left the other at %d, want 1", got)
	}
	if _, ok := f.bound(t, bindSession); !ok {
		t.Error("releasing one session dropped the other's route")
	}
}

// ---------------------------------------------------------------------------
// Composition.
// ---------------------------------------------------------------------------

// TestNewDemandRefusesAnIncompleteComposition. A missing seam is a composition
// error before any call, not a nil dereference on the first subscriber.
func TestNewDemandRefusesAnIncompleteComposition(t *testing.T) {
	t.Parallel()

	bindings := newBindings(t, newResolver(), &recordingBinder{})
	cases := []struct {
		name     string
		bindings *Bindings
		tips     TipReader
		hints    Hinter
		clock    Clock
		limits   DemandLimits
		want     string
	}{
		{"no bindings", nil, &scriptedTips{}, &recordingHinter{}, &manualClock{}, testDemandLimits, "Bindings"},
		{"no tip reader", bindings, nil, &recordingHinter{}, &manualClock{}, testDemandLimits, "TipReader"},
		{"no hinter", bindings, &scriptedTips{}, nil, &manualClock{}, testDemandLimits, "Hinter"},
		{"no clock", bindings, &scriptedTips{}, &recordingHinter{}, nil, testDemandLimits, "Clock"},
		{"a zero poll interval", bindings, &scriptedTips{}, &recordingHinter{}, &manualClock{},
			DemandLimits{PollTimeout: time.Second}, "OwnershipPollInterval"},
		{"a zero poll timeout", bindings, &scriptedTips{}, &recordingHinter{}, &manualClock{},
			DemandLimits{OwnershipPollInterval: time.Second}, "PollTimeout"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			demand, err := NewDemand(testCase.bindings, testCase.tips, testCase.hints, testCase.clock, testCase.limits)
			if err == nil {
				t.Fatalf("NewDemand accepted a composition with %s missing", testCase.want)
			}
			if demand != nil {
				t.Error("NewDemand returned a value alongside its error")
			}
			if !errors.Is(err, ErrInvalidConfig) {
				t.Errorf("NewDemand = %v, want an ErrInvalidConfig", err)
			}
			if !strings.Contains(err.Error(), testCase.want) {
				t.Errorf("NewDemand = %q, want it to name %s", err, testCase.want)
			}
		})
	}
}

// TestDemandLimitsValidateRefusesEveryUnboundedMember drives the members
// directly, including the negative spellings a zero-only check would miss.
func TestDemandLimitsValidateRefusesEveryUnboundedMember(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		limits DemandLimits
		want   string
	}{
		{"the defaults", DefaultDemandLimits(), ""},
		{"the test limits", testDemandLimits, ""},
		{"a zero interval", DemandLimits{PollTimeout: time.Second}, "OwnershipPollInterval"},
		{"a negative interval", DemandLimits{OwnershipPollInterval: -time.Second, PollTimeout: time.Second}, "OwnershipPollInterval"},
		{"a zero timeout", DemandLimits{OwnershipPollInterval: time.Second}, "PollTimeout"},
		{"a negative timeout", DemandLimits{OwnershipPollInterval: time.Second, PollTimeout: -time.Second}, "PollTimeout"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			err := testCase.limits.Validate()
			if testCase.want == "" {
				if err != nil {
					t.Fatalf("Validate = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("Validate = %v, want an error naming %s", err, testCase.want)
			}
		})
	}
}

// TestDefaultDemandLimitsAreTheDocumentedNumbers pins the defaults as absolute
// literals. Nothing else in this file could notice one moving, since every case
// drives its own off-default limits on purpose.
func TestDefaultDemandLimitsAreTheDocumentedNumbers(t *testing.T) {
	t.Parallel()

	limits := DefaultDemandLimits()
	if limits.OwnershipPollInterval != 5*time.Second {
		t.Errorf("OwnershipPollInterval = %v, want 5s", limits.OwnershipPollInterval)
	}
	if limits.PollTimeout != 10*time.Second {
		t.Errorf("PollTimeout = %v, want 10s", limits.PollTimeout)
	}
	if limits.PollTimeout <= limits.OwnershipPollInterval {
		t.Error("the default timeout is not larger than the default interval, so a merely slow poll is abandoned for a fresh one that will be just as slow")
	}
}

// FuzzTheJournalTipHintIsAFunctionOfTheTipAlone is the for-all a table cannot
// supply. Repeatability is a claim over EVERY sequence of observed tips: the
// hint published at each poll must be exactly the canonical encoding of the tip
// observed at that poll, whatever came before it and however many polls have
// run.
//
// The seeds discriminate without -fuzz, because -fuzz is a mode only the
// Makefile's fuzz stage invokes: a sequence carrying a repeat, a regression and
// a boundary value each fail against an implementation that published a delta,
// suppressed an unchanged tip, or numbered its hints.
func FuzzTheJournalTipHintIsAFunctionOfTheTipAlone(f *testing.F) {
	f.Add([]byte{1, 2, 3})
	f.Add([]byte{0, 0, 0, 0})
	f.Add([]byte{9, 9, 1, 9})
	f.Add([]byte{255, 0, 255})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, seed []byte) {
		tips := make([]uint64, 0, len(seed)+1)
		for _, b := range seed {
			// Spread the byte over the whole range so a sequence reaches values
			// a small counter would agree with by accident.
			tips = append(tips, uint64(b)*0x0101010101010101)
		}
		tips = append(tips, 0)

		fixture := newDemandFixture(t, testDemandLimits)
		fixture.tips.tips = tips
		if err := fixture.demand.Acquire(context.Background(), bindTenant, bindSession); err != nil {
			t.Fatalf("Acquire: %v", err)
		}
		for range len(tips) - 1 {
			fixture.clock.tick(t)
		}

		published := fixture.hints.published()
		if len(published) != len(tips) {
			t.Fatalf("%d hints for %d polls, want one per poll", len(published), len(tips))
		}
		encoded := fixture.hints.bytes()
		canonical := map[uint64]string{}
		for i, hint := range published {
			want := sessionwire.JournalTip{TenantID: bindTenant, SessionID: bindSession, Tip: tips[i]}
			if hint != want {
				t.Fatalf("hint %d = %+v, want %+v", i, hint, want)
			}
			if seen, ok := canonical[tips[i]]; ok && seen != encoded[i] {
				t.Fatalf("tip %d encoded as %s at poll %d and %s earlier; a repeatable hint is the same bytes",
					tips[i], encoded[i], i, seen)
			}
			canonical[tips[i]] = encoded[i]
		}
	})
}

// TestTheDemandPlaneSatisfiesTheClientLinksSeam is the join between A6.3 and
// this task: the ClientLink declares DemandManager as the narrow interface IT
// calls, and A6.3 recorded that "nothing in production builds either side
// today" and that the implementation would be A7.2's. This is that side.
//
// It is a RUNTIME assertion rather than a compile-time `var _`, deliberately: a
// compile-time assertion's failure is a build error, and a build error is not
// an assertion kill -- so a mutation that changed the method set would be
// reported by the toolchain rather than by a named test.
func TestTheDemandPlaneSatisfiesTheClientLinksSeam(t *testing.T) {
	t.Parallel()

	seam := reflect.TypeOf((*clientlink.DemandManager)(nil)).Elem()
	if seam.NumMethod() == 0 {
		t.Fatal("clientlink.DemandManager declares no methods, so satisfying it proves nothing")
	}
	plane := reflect.TypeOf((*Demand)(nil))
	if !plane.Implements(seam) {
		t.Fatalf("*routing.Demand does not implement clientlink.DemandManager (%d methods)", seam.NumMethod())
	}
	// The negative control, so a satisfied-by-everything seam cannot pass this.
	if reflect.TypeOf((*Bindings)(nil)).Implements(seam) {
		t.Error("*routing.Bindings also satisfies the seam, so implementing it says nothing about Demand")
	}
}
