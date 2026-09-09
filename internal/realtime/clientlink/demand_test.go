package clientlink_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	internalidentity "github.com/looprig/factory/internal/identity"
	"github.com/looprig/factory/internal/realtime/clientlink"
)

// ---------------------------------------------------------------------------
// The two fakes A6.3 adds, and what each is allowed to be looser about.
// ---------------------------------------------------------------------------

// demandCall is one call into the demand plane, recorded whole.
//
// The DEADLINE is part of the record rather than a separate assertion, because
// "the call was made" and "the call was bounded" are properties of the same
// event and a fake that recorded only the first could not tell an unbounded
// call from a bounded one.
type demandCall struct {
	tenant   sessionwire.TenantID
	session  sessionwire.SessionID
	deadline time.Time
	bounded  bool
	// atClosedContext records a context that was ALREADY cancelled when the
	// call arrived. A release runs on a timer long after its connection is
	// gone, and a release handed a dead context would do nothing while looking
	// exactly like a release that worked.
	atClosedContext bool
}

func (c demandCall) session2() sessionwire.SessionID { return c.session }

// recordingDemand is the local delivery-demand plane, recorded.
//
// It is deliberately NOT looser than the seam it stands in for: the seam says
// an error from either method is a FAULT and never a decision, so this fake
// fails a call by returning an ordinary error and never by returning nil with
// nothing recorded. Both directions matter -- a fake that answered nil for
// everything would make "a fault refuses the subscription" unfalsifiable.
type recordingDemand struct {
	mu       sync.Mutex
	acquires []demandCall
	releases []demandCall
	// acquireErr and releaseErr, when set, fail every call.
	acquireErr error
	releaseErr error
	// blockAcquire, when non-nil, holds an Acquire until it is closed or the
	// context is done. It is how a wedged demand plane is driven.
	blockAcquire chan struct{}
	// selfReleased records that the backstop, not the context, ended a call:
	// a case using blockAcquire must assert it is false, or the backstop
	// silently stands in for the bound under test.
	selfReleased bool
}

// demandBackstop is far longer than any bound a case configures, so a bounded
// call is released by its context and an unbounded one is released by this and
// says so.
const demandBackstop = 5 * time.Second

func (d *recordingDemand) Acquire(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) error {
	d.mu.Lock()
	d.acquires = append(d.acquires, callOf(ctx, tenant, session))
	err, block := d.acquireErr, d.blockAcquire
	d.mu.Unlock()
	if block != nil {
		timer := time.NewTimer(demandBackstop)
		defer timer.Stop()
		select {
		case <-block:
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			d.mu.Lock()
			d.selfReleased = true
			d.mu.Unlock()
			return errDemandUnbounded
		}
	}
	return err
}

func (d *recordingDemand) Release(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.releases = append(d.releases, callOf(ctx, tenant, session))
	return d.releaseErr
}

var errDemandUnbounded = errors.New("nothing bounded this demand call; the fake released itself")

func callOf(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) demandCall {
	deadline, bounded := ctx.Deadline()
	return demandCall{
		tenant:          tenant,
		session:         session,
		deadline:        deadline,
		bounded:         bounded,
		atClosedContext: ctx.Err() != nil,
	}
}

func (d *recordingDemand) acquired() []demandCall {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]demandCall(nil), d.acquires...)
}

func (d *recordingDemand) released() []demandCall {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]demandCall(nil), d.releases...)
}

func (d *recordingDemand) unbounded() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.selfReleased
}

// sessionsOf reduces a call list to the sessions it names, in order.
func sessionsOf(calls []demandCall) []sessionwire.SessionID {
	out := make([]sessionwire.SessionID, 0, len(calls))
	for _, call := range calls {
		out = append(out, call.session2())
	}
	return out
}

// manualClock is the debounce, driven rather than waited for.
//
// Nothing here sleeps: a debounce asserted by waiting is a test that passes on
// a slow machine for the wrong reason, and a debounce asserted by NOT waiting
// is the "nothing happened yet" shape that passes whether the timer works or
// not. Firing is explicit, so both halves have a reader.
type manualClock struct {
	mu     sync.Mutex
	timers []*manualTimer
}

type manualTimer struct {
	d       time.Duration
	f       func()
	stopped bool
	fired   bool
}

func (c *manualClock) AfterFunc(d time.Duration, f func()) func() bool {
	timer := &manualTimer{d: d, f: f}
	c.mu.Lock()
	c.timers = append(c.timers, timer)
	c.mu.Unlock()
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

// pending reports the delays of the timers that are still armed.
func (c *manualClock) pending() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []time.Duration
	for _, timer := range c.timers {
		if !timer.stopped && !timer.fired {
			out = append(out, timer.d)
		}
	}
	return out
}

// fire runs every armed timer and reports how many ran. The callbacks are
// invoked OUTSIDE this lock: a callback reaches the engine's own table, and a
// clock that held its lock across one would be the first half of a cycle.
func (c *manualClock) fire() int {
	c.mu.Lock()
	var due []func()
	for _, timer := range c.timers {
		if timer.stopped || timer.fired {
			continue
		}
		timer.fired = true
		due = append(due, timer.f)
	}
	c.mu.Unlock()
	for _, f := range due {
		f()
	}
	return len(due)
}

// fireEvenStopped runs every timer that has not already run, INCLUDING ones
// whose stop reported success.
//
// It exists for one property that stop() alone cannot establish. A real
// time.Timer may already be running its callback when Stop is called -- Stop
// says so -- so "the debounce was cancelled" must not rest on the stop having
// won the race. This drives the losing side of that race deliberately.
func (c *manualClock) fireEvenStopped() int {
	c.mu.Lock()
	var due []func()
	for _, timer := range c.timers {
		if timer.fired {
			continue
		}
		timer.fired = true
		due = append(due, timer.f)
	}
	c.mu.Unlock()
	for _, f := range due {
		f()
	}
	return len(due)
}

// fireNth runs the timer armed nth, whatever its state, and reports whether
// there was one. It is how a case drives ONE of several timers -- the
// superseded one specifically -- which fire and fireEvenStopped cannot do.
func (c *manualClock) fireNth(n int) bool {
	c.mu.Lock()
	if n >= len(c.timers) || c.timers[n].fired {
		c.mu.Unlock()
		return false
	}
	c.timers[n].fired = true
	f := c.timers[n].f
	c.mu.Unlock()
	f()
	return true
}

// ---------------------------------------------------------------------------
// Helpers over the engine fixture.
// ---------------------------------------------------------------------------

// bind takes one DeliveryBinding for a session of this fixture's own tenant.
func (f *engineFixture) bind(t *testing.T, session string) func() {
	t.Helper()

	release, err := f.engine.Bind(t.Context(), f.principal, sessionChannel(tenantA, session))
	if err != nil {
		t.Fatalf("Bind(%q): %v", session, err)
	}
	return release
}

func wantSessions(t *testing.T, what string, got []demandCall, want ...sessionwire.SessionID) {
	t.Helper()

	sessions := sessionsOf(got)
	if len(sessions) != len(want) {
		t.Fatalf("%s: %v, want %v", what, sessions, want)
	}
	for i := range want {
		if sessions[i] != want[i] {
			t.Fatalf("%s: %v, want %v", what, sessions, want)
		}
	}
}

// ---------------------------------------------------------------------------
// Step 2 -- the edges. Every case below is about a TRANSITION.
// ---------------------------------------------------------------------------

// TestOnlyTheFirstLocalDeliveryBindingNotifiesTheDemandManager is the first
// half of step 2, driven as a transition rather than as a state.
//
// Three bindings are taken for one session, which is the shape a browser
// produces with three tabs open on one workspace, and the demand plane must
// hear about it once. Dropping two of the three must still say nothing, since
// demand is not gone until the LAST binding is.
func TestOnlyTheFirstLocalDeliveryBindingNotifiesTheDemandManager(t *testing.T) {
	t.Parallel()

	f := newEngineFixture(t)
	first := f.bind(t, "session-1")
	second := f.bind(t, "session-1")
	third := f.bind(t, "session-1")

	wantSessions(t, "acquires after three bindings", f.demand.acquired(), "session-1")
	if calls := f.demand.released(); len(calls) != 0 {
		t.Fatalf("the demand plane was released %d times while three bindings were held", len(calls))
	}

	first()
	second()
	if calls := f.demand.released(); len(calls) != 0 {
		t.Fatalf("the demand was released with one binding still held: %v", sessionsOf(calls))
	}
	if pending := f.clock.pending(); len(pending) != 0 {
		t.Fatalf("a release was scheduled with one binding still held: %v", pending)
	}

	// And the last one is the edge: it schedules, and firing the schedule
	// releases. Without this half the assertions above are satisfied by an
	// engine that never releases anything.
	third()
	if pending := f.clock.pending(); len(pending) != 1 {
		t.Fatalf("the last removal scheduled %d releases, want exactly 1", len(pending))
	}
	if fired := f.clock.fire(); fired != 1 {
		t.Fatalf("fired %d timers, want 1", fired)
	}
	wantSessions(t, "releases after the last binding", f.demand.released(), "session-1")
	wantSessions(t, "acquires over the whole case", f.demand.acquired(), "session-1")
}

// TestTheLastRemovalReleasesOnlyAfterTheConfiguredDebounce pins the delay to
// the configured value and pins that the release happens at all.
//
// The two halves are one case on purpose. "The demand was not released" alone
// is the shape that passes whether the timer exists or not; "the demand was
// released" alone cannot see a debounce that was skipped. The delay is compared
// against an absolute literal rather than against the configured field, so a
// release scheduled from some other duration -- the command bound, the ping
// cadence -- fails here.
func TestTheLastRemovalReleasesOnlyAfterTheConfiguredDebounce(t *testing.T) {
	t.Parallel()

	limits := testLimits()
	limits.DemandReleaseDebounce = 7 * time.Second
	f := newEngineFixtureWithLimits(t, limits)

	release := f.bind(t, "session-1")
	release()

	pending := f.clock.pending()
	if len(pending) != 1 {
		t.Fatalf("the last removal scheduled %v, want exactly one release", pending)
	}
	if pending[0] != 7*time.Second {
		t.Errorf("the release was scheduled after %v, want 7s", pending[0])
	}
	if calls := f.demand.released(); len(calls) != 0 {
		t.Fatalf("the demand was released before the debounce elapsed: %v", sessionsOf(calls))
	}

	if fired := f.clock.fire(); fired != 1 {
		t.Fatalf("fired %d timers, want 1", fired)
	}
	wantSessions(t, "releases after the debounce elapsed", f.demand.released(), "session-1")
}

// TestABindingArrivingInsideTheDebounceRetainsTheDemand is the transition the
// debounce exists for: a browser that drops a subscription and takes it again
// -- a reconnect, a navigation, a re-render -- must cost NOTHING on the demand
// plane, not one release and one re-acquire.
//
// The second half is the part a stop() alone does not establish. A real timer
// may already be running when Stop is called, so the case fires the cancelled
// timer DELIBERATELY and requires it to do nothing: cancellation rests on the
// engine's own supersession record, not on having won a race.
func TestABindingArrivingInsideTheDebounceRetainsTheDemand(t *testing.T) {
	t.Parallel()

	f := newEngineFixture(t)
	first := f.bind(t, "session-1")
	first()
	if len(f.clock.pending()) != 1 {
		t.Fatalf("the last removal scheduled no release")
	}

	second := f.bind(t, "session-1")
	if pending := f.clock.pending(); len(pending) != 0 {
		t.Errorf("the scheduled release survived a new binding: %v", pending)
	}
	if fired := f.clock.fireEvenStopped(); fired != 1 {
		t.Fatalf("fired %d timers, want the cancelled one", fired)
	}
	if calls := f.demand.released(); len(calls) != 0 {
		t.Fatalf("the demand was released although a binding had been taken again: %v", sessionsOf(calls))
	}
	wantSessions(t, "acquires across the reconnect", f.demand.acquired(), "session-1")

	// The positive control: the retained demand is still real demand, so
	// dropping it again releases exactly once.
	second()
	if fired := f.clock.fire(); fired != 1 {
		t.Fatalf("fired %d timers after the second removal, want 1", fired)
	}
	wantSessions(t, "releases after the retained demand was dropped", f.demand.released(), "session-1")
}

// TestASupersededReleaseDoesNotShortCircuitTheRestartedDebounce is the reader
// for the entry's generation, and it is a case the count of releases cannot
// see.
//
// A session bound, dropped, bound again and dropped again has TWO timers behind
// it: the one the first removal armed and was superseded, and the one the
// second removal armed. If the superseded one is allowed to act, the demand is
// released on the FIRST removal's schedule -- earlier than the debounce the
// second removal was entitled to -- and the total number of releases is
// identical either way, which is why this drives the superseded timer alone.
//
// The lost race is not hypothetical: time.Timer.Stop reports false when the
// callback has already started, and stopping cannot unstart it.
func TestASupersededReleaseDoesNotShortCircuitTheRestartedDebounce(t *testing.T) {
	t.Parallel()

	f := newEngineFixture(t)
	first := f.bind(t, "session-1")
	first()
	second := f.bind(t, "session-1")
	second()
	if pending := f.clock.pending(); len(pending) != 1 {
		t.Fatalf("armed %v, want exactly the second removal's release", pending)
	}

	if !f.clock.fireNth(0) {
		t.Fatal("the first removal armed no timer")
	}
	if calls := f.demand.released(); len(calls) != 0 {
		t.Fatalf("the superseded release acted: %v", sessionsOf(calls))
	}

	if !f.clock.fireNth(1) {
		t.Fatal("the second removal armed no timer")
	}
	wantSessions(t, "releases", f.demand.released(), "session-1")
}

// TestDemandIsAcquiredAgainOnceItHasBeenReleased is the other side of the
// transition above: after the release has actually happened, a new subscriber
// is a FIRST binding again.
func TestDemandIsAcquiredAgainOnceItHasBeenReleased(t *testing.T) {
	t.Parallel()

	f := newEngineFixture(t)
	release := f.bind(t, "session-1")
	release()
	f.clock.fire()
	wantSessions(t, "releases", f.demand.released(), "session-1")

	again := f.bind(t, "session-1")
	wantSessions(t, "acquires after the demand had been given back", f.demand.acquired(), "session-1", "session-1")
	again()
	f.clock.fire()
	wantSessions(t, "releases over the whole case", f.demand.released(), "session-1", "session-1")
}

// TestReleasingOneBindingTwiceReleasesTheDemandOnce holds the property the
// transport adapter depends on: the same binding is reported as gone by an
// unsubscribe, by a disconnect and by a subscribe that failed after the
// callback, and it must be given back exactly once.
func TestReleasingOneBindingTwiceReleasesTheDemandOnce(t *testing.T) {
	t.Parallel()

	f := newEngineFixture(t)
	release := f.bind(t, "session-1")
	other := f.bind(t, "session-1")

	release()
	release()
	release()
	if calls := f.demand.released(); len(calls) != 0 {
		t.Fatalf("repeated releases of one binding released demand another binding still held: %v", sessionsOf(calls))
	}
	if pending := f.clock.pending(); len(pending) != 0 {
		t.Fatalf("repeated releases of one binding scheduled %v", pending)
	}

	other()
	f.clock.fire()
	wantSessions(t, "releases", f.demand.released(), "session-1")
}

// TestEachSessionHoldsItsOwnDemand is the multiplexing half of step 2: one link
// holds many sessions, and their lifecycles are independent.
func TestEachSessionHoldsItsOwnDemand(t *testing.T) {
	t.Parallel()

	f := newEngineFixture(t)
	one := f.bind(t, "session-1")
	two := f.bind(t, "session-2")
	wantSessions(t, "acquires", f.demand.acquired(), "session-1", "session-2")

	one()
	if fired := f.clock.fire(); fired != 1 {
		t.Fatalf("fired %d timers, want the one session-1 scheduled", fired)
	}
	wantSessions(t, "releases after one session was dropped", f.demand.released(), "session-1")

	two()
	f.clock.fire()
	wantSessions(t, "releases", f.demand.released(), "session-1", "session-2")
}

// ---------------------------------------------------------------------------
// Step 1 -- the channels, and the refusals that take no demand.
// ---------------------------------------------------------------------------

// TestTheDemandedSessionIsTheChannelTheAuthorizerAllowed sweeps the channel
// shapes a browser can send.
//
// The set is derived from the grammar rather than from the four shapes A6.1
// happened to list: a channel is refused if it is not in the session namespace,
// if it has too few or too many segments, if either segment is empty, if either
// segment is not a valid Core identifier, or if its tenant is not the
// principal's. Every one of those is a way the demand plane could be told about
// a session nobody authorized.
func TestTheDemandedSessionIsTheChannelTheAuthorizerAllowed(t *testing.T) {
	t.Parallel()

	oversized := strings.Repeat("s", sessionwire.MaxIDBytes+1)
	for _, tt := range []struct {
		name    string
		channel string
		want    sessionwire.SessionID
	}{
		{name: "this principal's own session", channel: "session:tenant-a:session-1", want: "session-1"},
		{name: "another tenant's session", channel: "session:tenant-b:session-1"},
		{name: "no namespace at all", channel: "session-1"},
		{name: "another namespace", channel: "presence:tenant-a:session-1"},
		{name: "a namespace that merely starts the same way", channel: "sessions:tenant-a:session-1"},
		{name: "one segment", channel: "session:tenant-a"},
		{name: "an empty tenant", channel: "session::session-1"},
		{name: "an empty session", channel: "session:tenant-a:"},
		{name: "an extra segment", channel: "session:tenant-a:session-1:extra"},
		{name: "an empty extra segment", channel: "session:tenant-a:session-1:"},
		{name: "a colon inside the session", channel: "session:tenant-a:a:b"},
		{name: "a tenant that is not this principal's", channel: "session:tenant a:session-1"},
		// Core's identifiers are opaque UTF-8 of bounded length, so these are
		// the only two shapes a well-formed channel can carry that Core itself
		// refuses. A space is NOT one of them, which is why no row pretends it
		// is: the table is derived from validateID (core@v0.7.0
		// sessionwire/v1/ids.go:86-97), not from a guess about identifiers.
		{name: "a session Core cannot decode", channel: "session:tenant-a:s\xff"},
		{name: "a session Core will not carry", channel: "session:tenant-a:" + oversized},
		{name: "the empty channel", channel: ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newEngineFixture(t)
			release, err := f.engine.Bind(t.Context(), f.principal, tt.channel)
			if tt.want == "" {
				if err == nil {
					release()
					t.Fatalf("Bind(%q) succeeded, want a refusal", tt.channel)
				}
				if calls := f.demand.acquired(); len(calls) != 0 {
					t.Fatalf("a refused channel took demand for %v", sessionsOf(calls))
				}
				return
			}
			if err != nil {
				t.Fatalf("Bind(%q) = %v, want the binding to be taken", tt.channel, err)
			}
			wantSessions(t, "acquires", f.demand.acquired(), tt.want)
			if got := f.demand.acquired()[0].tenant; got != tenantA {
				t.Errorf("the demand was taken for tenant %q, want %q", got, tenantA)
			}
			release()
		})
	}
}

// TestARefusedAuthorizationIsADenialAndTakesNoDemand separates the two
// refusals Bind can produce, because they mean different things to a browser.
func TestARefusedAuthorizationIsADenialAndTakesNoDemand(t *testing.T) {
	t.Parallel()

	f := newEngineFixture(t)
	_, err := f.engine.Bind(t.Context(), f.principal, sessionChannel(tenantB, "session-1"))
	if err == nil {
		t.Fatal("a cross-tenant subscribe was accepted")
	}
	if !errors.Is(err, internalidentity.ErrUnauthorized) {
		t.Errorf("Bind() = %v, want a denial wrapping ErrUnauthorized", err)
	}
	if calls := f.demand.acquired(); len(calls) != 0 {
		t.Fatalf("a denied subscribe took demand for %v", sessionsOf(calls))
	}
	if calls := f.authorizer.subscribeCalls(); len(calls) != 1 {
		t.Fatalf("the authorizer was consulted %d times, want 1", len(calls))
	}
}

// TestAChannelTheAuthorizerAllowedButThisEdgeCannotNameIsAFault drives the arm
// no production authorizer reaches, with a permissive one that does.
//
// The seam is an interface, so the grammar an implementation authorizes under
// is not this package's. An implementation that allowed a channel this build
// cannot derive a session from must not produce demand for something else --
// and it must not be reported as a DENIAL either, because a denial is terminal
// and this condition is a fault in the composition.
func TestAChannelTheAuthorizerAllowedButThisEdgeCannotNameIsAFault(t *testing.T) {
	t.Parallel()

	limits := testLimits()
	demand := &recordingDemand{}
	clock := &manualClock{}
	principal, err := identity.NewPrincipal(tenantA, "user-a", identity.KindActor)
	if err != nil {
		t.Fatalf("NewPrincipal: %v", err)
	}
	engine, err := clientlink.NewEngine(clientlink.Config{
		Authenticator: fixedAuthenticator{principal: principal},
		Authorizer:    allowEverything{},
		Admitter:      &recordingAdmitter{created: true},
		Demand:        demand,
		Clock:         clock,
		Limits:        limits,
		Version:       buildVersion,
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	for _, channel := range []string{"anything", "session:tenant-b:session-1", "session:tenant-a:s\xff", "session:tenant-a:a:b"} {
		_, err := engine.Bind(t.Context(), principal, channel)
		if err == nil {
			t.Fatalf("Bind(%q) succeeded against a permissive authorizer", channel)
		}
		if !errors.Is(err, clientlink.ErrUnroutableChannel) {
			t.Errorf("Bind(%q) = %v, want ErrUnroutableChannel", channel, err)
		}
		if errors.Is(err, internalidentity.ErrUnauthorized) {
			t.Errorf("Bind(%q) reported a composition fault as a denial", channel)
		}
	}
	if calls := demand.acquired(); len(calls) != 0 {
		t.Fatalf("an unroutable channel took demand for %v", sessionsOf(calls))
	}
}

// allowEverything authorizes every subscription. It exists to reach the fault
// arm above and is not otherwise used.
type allowEverything struct{ denyAll }

func (allowEverything) AuthorizeSubscribe(context.Context, identity.Principal, string) error {
	return nil
}

// TestADemandPlaneFaultRefusesTheSubscription holds the fail-closed direction.
// A link that accepted a subscription whose demand was never recorded would
// serve a channel this replica has asked nothing to route to it, and the
// failure would appear to the user as an empty session rather than as an error.
func TestADemandPlaneFaultRefusesTheSubscription(t *testing.T) {
	t.Parallel()

	f := newEngineFixture(t)
	wedged := errors.New("the demand plane is unreachable")
	f.demand.acquireErr = wedged

	_, err := f.engine.Bind(t.Context(), f.principal, sessionChannel(tenantA, "session-1"))
	if !errors.Is(err, wedged) {
		t.Fatalf("Bind() = %v, want the demand plane's own failure", err)
	}
	if errors.Is(err, internalidentity.ErrUnauthorized) {
		t.Error("a demand-plane fault was reported as a denial")
	}

	// And it leaves no half-state: the next subscriber is a FIRST binding, so
	// the demand plane is asked again rather than being assumed to hold it.
	f.demand.mu.Lock()
	f.demand.acquireErr = nil
	f.demand.mu.Unlock()
	release := f.bind(t, "session-1")
	wantSessions(t, "acquires", f.demand.acquired(), "session-1", "session-1")
	release()
	f.clock.fire()
	wantSessions(t, "releases", f.demand.released(), "session-1")
}

// ---------------------------------------------------------------------------
// The bound on both demand calls.
// ---------------------------------------------------------------------------

// TestEveryDemandCallCarriesTheConfiguredBound is the reader for
// Limits.DemandTimeout, and it covers the release as well as the acquire.
//
// The release is the half worth writing: it runs on a timer after the
// connection that caused it is gone, so there is no request context in
// existence for it to inherit a deadline from. A release that derived one from
// a cancelled context would do nothing and look identical to one that worked,
// which is why the fake records that too.
func TestEveryDemandCallCarriesTheConfiguredBound(t *testing.T) {
	t.Parallel()

	limits := testLimits()
	limits.DemandTimeout = 11 * time.Second
	f := newEngineFixtureWithLimits(t, limits)

	before := time.Now()
	release := f.bind(t, "session-1")
	release()
	f.clock.fire()

	for what, calls := range map[string][]demandCall{"acquire": f.demand.acquired(), "release": f.demand.released()} {
		if len(calls) != 1 {
			t.Fatalf("%s was called %d times, want 1", what, len(calls))
		}
		call := calls[0]
		if !call.bounded {
			t.Errorf("the %s was handed a context with no deadline at all", what)
			continue
		}
		if call.atClosedContext {
			t.Errorf("the %s was handed an already-cancelled context", what)
		}
		if slack := call.deadline.Sub(before); slack <= 0 || slack > 11*time.Second+time.Minute {
			t.Errorf("the %s deadline is %v away, want the configured 11s", slack, what)
		}
	}
}

// TestAWedgedDemandPlaneDoesNotHangTheSubscribe is the property the bound
// exists for, driven rather than reasoned about. The fake honours its context
// and reports separately if the backstop -- not the deadline -- released it.
func TestAWedgedDemandPlaneDoesNotHangTheSubscribe(t *testing.T) {
	t.Parallel()

	limits := testLimits()
	limits.DemandTimeout = 50 * time.Millisecond
	f := newEngineFixtureWithLimits(t, limits)
	f.demand.blockAcquire = make(chan struct{})

	done := make(chan error, 1)
	go func() {
		_, err := f.engine.Bind(context.Background(), f.principal, sessionChannel(tenantA, "session-1"))
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a wedged demand plane produced a successful subscription")
		}
		if f.demand.unbounded() {
			t.Fatal("the fake's own backstop released the call; nothing bounded it")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Bind() = %v, want the configured deadline to have ended it", err)
		}
	case <-time.After(demandBackstop + 5*time.Second):
		t.Fatal("a wedged demand plane never returned")
	}
}

// ---------------------------------------------------------------------------
// Step 3 -- Factory does not retain the browser's durable journal cursor.
// ---------------------------------------------------------------------------

// TestNothingButTheSessionIdentityReachesTheDemandPlane is step 3 at the seam
// that could carry a cursor onward.
//
// It is a for-all rather than an absence: two bindings for one session are
// taken through channels that differ in nothing but the position a browser
// might be trying to smuggle, and the calls the demand plane receives must be
// INDISTINGUISHABLE. The positive control is the pair that differs in the
// session, which must be distinguishable -- without it, an engine that told the
// demand plane nothing at all would pass.
func TestNothingButTheSessionIdentityReachesTheDemandPlane(t *testing.T) {
	t.Parallel()

	f := newEngineFixture(t)
	one := f.bind(t, "session-1")
	one()
	f.clock.fire()
	two := f.bind(t, "session-1")
	two()
	f.clock.fire()

	acquires := f.demand.acquired()
	if len(acquires) != 2 {
		t.Fatalf("acquired %d times, want 2", len(acquires))
	}
	if acquires[0].tenant != acquires[1].tenant || acquires[0].session != acquires[1].session {
		t.Errorf("two bindings for one session reached the demand plane as %v and %v", acquires[0], acquires[1])
	}

	// The control.
	three := f.bind(t, "session-2")
	defer three()
	acquires = f.demand.acquired()
	if acquires[2].session == acquires[1].session {
		t.Error("a binding for a different session was indistinguishable from the previous one")
	}
}

// ---------------------------------------------------------------------------
// Shutdown.
// ---------------------------------------------------------------------------

// TestReleaseIdleDemandGivesBackEverythingScheduled covers the drain: a replica
// that stopped at node.Shutdown would leave the routing table holding demand
// for sessions no connection remains to serve.
func TestReleaseIdleDemandGivesBackEverythingScheduled(t *testing.T) {
	t.Parallel()

	f := newEngineFixture(t)
	one := f.bind(t, "session-1")
	two := f.bind(t, "session-2")
	held := f.bind(t, "session-3")
	defer held()
	one()
	two()

	if err := f.engine.ReleaseIdleDemand(t.Context()); err != nil {
		t.Fatalf("ReleaseIdleDemand: %v", err)
	}
	released := sessionsOf(f.demand.released())
	if len(released) != 2 {
		t.Fatalf("released %v, want the two sessions whose bindings were gone", released)
	}
	for _, session := range released {
		if session == "session-3" {
			t.Error("a session with a live binding was released")
		}
	}

	// Firing the timers afterwards must release nothing a second time: the
	// flush and the debounce are two paths to one release, not two releases.
	f.clock.fireEvenStopped()
	if got := len(f.demand.released()); got != 2 {
		t.Errorf("after the timers fired, %d releases had happened, want 2", got)
	}
}

// TestReleaseIdleDemandReportsWhatItCouldNotGiveBack holds the joined-failure
// shape: one session's failure must not hide the others, and it must not be
// swallowed either.
func TestReleaseIdleDemandReportsWhatItCouldNotGiveBack(t *testing.T) {
	t.Parallel()

	f := newEngineFixture(t)
	one := f.bind(t, "session-1")
	two := f.bind(t, "session-2")
	one()
	two()
	failure := errors.New("the demand plane is unreachable")
	f.demand.mu.Lock()
	f.demand.releaseErr = failure
	f.demand.mu.Unlock()

	err := f.engine.ReleaseIdleDemand(t.Context())
	if !errors.Is(err, failure) {
		t.Fatalf("ReleaseIdleDemand() = %v, want the demand plane's own failure", err)
	}
	if got := len(f.demand.released()); got != 2 {
		t.Errorf("%d sessions were attempted, want both", got)
	}
}

// ---------------------------------------------------------------------------
// The two grammars, held to one answer.
// ---------------------------------------------------------------------------

// FuzzTheDemandGrammarAgreesWithTheAuthorizers is the reader for the one place
// this package derives something from a channel that another package also
// parses.
//
// internal/identity decides the SUBSCRIPTION from a channel; this package
// derives the SESSION from the same string. Two grammars mean two answers, and
// the pair that matters is "authorized but unroutable" -- which fails closed
// today, but as a fault on a channel a browser was entitled to. The property is
// stated in both directions over the exported surface of each: for every
// channel and every tenant, the authorizer accepts exactly when this edge can
// name a session, and the session it names is the channel's third segment.
func FuzzTheDemandGrammarAgreesWithTheAuthorizers(f *testing.F) {
	for _, channel := range []string{
		"session:tenant-a:session-1", "session:tenant-a:", "session::s", "session:tenant-a",
		"session:tenant-a:a:b", "sessions:tenant-a:s", "", "session:tenant-a:sÿ",
		"session: :s", "SESSION:tenant-a:s", "session:tenant-a:s ",
	} {
		f.Add(channel, "tenant-a")
	}
	f.Fuzz(func(t *testing.T, channel, tenant string) {
		principal, err := identity.NewPrincipal(sessionwire.TenantID(tenant), "user", identity.KindActor)
		if err != nil {
			t.Skip("no principal can hold this tenant")
		}
		fixture := newEngineFixtureForPrincipal(t, principal)
		authorized := (internalidentity.Authorizer{}).AuthorizeSubscribe(t.Context(), principal, channel) == nil

		release, err := fixture.engine.Bind(t.Context(), principal, channel)
		routable := err == nil
		if routable {
			release()
		}
		if authorized != routable {
			t.Fatalf("channel %q under tenant %q: the authorizer says %v and this edge says %v",
				channel, tenant, authorized, routable)
		}
		if !routable {
			return
		}
		calls := fixture.demand.acquired()
		if len(calls) != 1 {
			t.Fatalf("channel %q took %d demands, want 1", channel, len(calls))
		}
		want := sessionwire.SessionID(strings.TrimPrefix(channel, fmt.Sprintf("session:%s:", tenant)))
		if calls[0].session != want || calls[0].tenant != sessionwire.TenantID(tenant) {
			t.Fatalf("channel %q was demanded as %q/%q, want %q/%q",
				channel, calls[0].tenant, calls[0].session, tenant, want)
		}
	})
}

// newEngineFixtureForPrincipal is newEngineFixtureWithLimits for a principal
// the case chooses, which the fuzz target needs and nothing else does.
func newEngineFixtureForPrincipal(t *testing.T, principal identity.Principal) *engineFixture {
	t.Helper()

	fixture := &engineFixture{
		authorizer: &recordingAuthorizer{},
		admitter:   &recordingAdmitter{created: true},
		demand:     &recordingDemand{},
		clock:      &manualClock{},
		principal:  principal,
	}
	engine, err := clientlink.NewEngine(clientlink.Config{
		Authenticator: fixedAuthenticator{principal: principal},
		Authorizer:    fixture.authorizer,
		Admitter:      fixture.admitter,
		Demand:        fixture.demand,
		Clock:         fixture.clock,
		Limits:        testLimits(),
		Version:       buildVersion,
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	fixture.engine = engine
	return fixture
}
