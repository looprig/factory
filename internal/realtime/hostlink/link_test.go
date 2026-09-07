package hostlink_test

import (
	"context"
	"errors"
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/hostlink"
)

// The pool's whole job is "one physical link per Host, many session bindings
// over it". Every case below is written against that sentence: a case that
// asserted only "Bind returned nil" would pass on a pool that dialled once per
// session, which is the defect this task exists to prevent.

const (
	// Absolute literals. A fixture derived from the constant under test pins
	// nothing, which this lane has already been burned by.
	hostOne   sessionwire.HostID           = "host-1"
	hostTwo   sessionwire.HostID           = "host-2"
	endpoint1 sessionwire.InternalEndpoint = "wss://host-1.internal:8443/hostlink"
	endpoint2 sessionwire.InternalEndpoint = "wss://host-2.internal:8443/hostlink"
	tenant    sessionwire.TenantID         = "tenant-a"
)

func TestABindOpensOneLinkAndReusesItForEverySessionOnThatHost(t *testing.T) {
	t.Parallel()

	dialer := newRecordingDialer()
	pool := newPool(t, dialer, hostlink.Limits{})

	for _, session := range []sessionwire.SessionID{"s-1", "s-2", "s-3"} {
		if err := pool.Bind(context.Background(), target(hostOne, endpoint1), bindRequest(hostOne, session)); err != nil {
			t.Fatalf("Bind(%s): %v", session, err)
		}
	}

	// Three assertions, because any one alone is satisfied by a wrong pool: a
	// dial count of 1 alone is satisfied by a pool that dropped two bindings,
	// and a binding count of 3 alone is satisfied by a pool that dialled three
	// times.
	if got := dialer.dials(); got != 1 {
		t.Errorf("dialer was called %d times for one Host, want 1", got)
	}
	if got := pool.Links(); got != 1 {
		t.Errorf("Links() = %d, want 1", got)
	}
	if got := pool.Bindings(hostOne); got != 3 {
		t.Errorf("Bindings(%s) = %d, want 3", hostOne, got)
	}
	// And the bind reached the Host: a pool that counted locally without
	// telling the Host anything would satisfy all three counts above.
	if got := len(dialer.link(hostOne).binds()); got != 3 {
		t.Errorf("the Host received %d bind requests, want 3", got)
	}
}

func TestTwoHostsGetTwoLinks(t *testing.T) {
	t.Parallel()

	dialer := newRecordingDialer()
	pool := newPool(t, dialer, hostlink.Limits{})

	mustBind(t, pool, target(hostOne, endpoint1), bindRequest(hostOne, "s-1"))
	mustBind(t, pool, target(hostTwo, endpoint2), bindRequest(hostTwo, "s-2"))

	if got := pool.Links(); got != 2 {
		t.Errorf("Links() = %d, want 2", got)
	}
	if got, want := dialer.targets(), []hostlink.Target{target(hostOne, endpoint1), target(hostTwo, endpoint2)}; !reflect.DeepEqual(got, want) {
		t.Errorf("dialled %v, want %v", got, want)
	}
}

// TestTheBindRequestMustNameTheHostItIsSentTo is the one place a routing
// mistake would be silent: a bind carrying host-2's tuple sent over host-1's
// link is a request the Host will refuse for a reason that reads as a lease
// problem rather than as a Factory bug.
func TestTheBindRequestMustNameTheHostItIsSentTo(t *testing.T) {
	t.Parallel()

	dialer := newRecordingDialer()
	pool := newPool(t, dialer, hostlink.Limits{})

	err := pool.Bind(context.Background(), target(hostOne, endpoint1), bindRequest(hostTwo, "s-1"))
	if !errors.Is(err, hostlink.ErrTargetMismatch) {
		t.Fatalf("Bind with a mismatched host = %v, want ErrTargetMismatch", err)
	}
	if got := dialer.dials(); got != 0 {
		t.Errorf("the pool dialled %d times for a request it refused, want 0", got)
	}
}

// TestAnInvalidBindRecordIsRefusedBeforeAnythingIsDialled keeps a malformed
// control record from costing a TCP connection and a service handshake.
func TestAnInvalidBindRecordIsRefusedBeforeAnythingIsDialled(t *testing.T) {
	t.Parallel()

	dialer := newRecordingDialer()
	pool := newPool(t, dialer, hostlink.Limits{})

	invalid := bindRequest(hostOne, "s-1")
	invalid.LeaseEpoch = 0 // Core rejects a zero lease epoch.

	if err := pool.Bind(context.Background(), target(hostOne, endpoint1), invalid); err == nil {
		t.Fatal("Bind accepted a record Core's own Validate rejects")
	}
	if got := dialer.dials(); got != 0 {
		t.Errorf("the pool dialled %d times for a record it refused, want 0", got)
	}
}

// TestAnInvalidTargetIsRefusedBeforeAnythingIsDialled covers the other half:
// the endpoint is a durable routing observation and Core states what a usable
// one is, including that it may not carry credentials.
func TestAnInvalidTargetIsRefusedBeforeAnythingIsDialled(t *testing.T) {
	t.Parallel()

	for name, endpoint := range map[string]sessionwire.InternalEndpoint{
		"empty":            "",
		"not websocket":    "https://host-1.internal/hostlink",
		"carries a secret": "wss://user:password@host-1.internal/hostlink",
		"carries a query":  "wss://host-1.internal/hostlink?token=abc",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			dialer := newRecordingDialer()
			pool := newPool(t, dialer, hostlink.Limits{})

			if err := pool.Bind(context.Background(), target(hostOne, endpoint), bindRequest(hostOne, "s-1")); err == nil {
				t.Fatalf("Bind accepted endpoint %q", endpoint)
			}
			if got := dialer.dials(); got != 0 {
				t.Errorf("the pool dialled %d times for a target it refused, want 0", got)
			}
		})
	}
}

func TestTheLinkCeilingRefusesAFurtherHostRatherThanExceedingIt(t *testing.T) {
	t.Parallel()

	dialer := newRecordingDialer()
	limits := defaultLimits()
	limits.MaxLinks = 1
	pool := newPool(t, dialer, limits)

	mustBind(t, pool, target(hostOne, endpoint1), bindRequest(hostOne, "s-1"))

	err := pool.Bind(context.Background(), target(hostTwo, endpoint2), bindRequest(hostTwo, "s-2"))
	if !errors.Is(err, hostlink.ErrLinkLimit) {
		t.Fatalf("Bind past the ceiling = %v, want ErrLinkLimit", err)
	}
	if got := pool.Links(); got != 1 {
		t.Errorf("Links() = %d after a refused bind, want 1", got)
	}
	// The ceiling bounds HOSTS, not sessions. A pool that counted sessions
	// would refuse this.
	mustBind(t, pool, target(hostOne, endpoint1), bindRequest(hostOne, "s-9"))
	if got := pool.Bindings(hostOne); got != 2 {
		t.Errorf("Bindings(%s) = %d, want 2", hostOne, got)
	}
}

// TestSessionsAlreadyBoundDoNotConsumeTheLinkCeiling is the ceiling case with
// room left in it, and it is a separate case because the one above cannot see
// the difference: with MaxLinks at 1 the second bind to the same Host never
// reaches the check at all, so a pool counting SESSIONS instead of LINKS passes
// it. Here the counts differ -- three sessions, two links allowed -- and a
// session count refuses a Host the pool has room for.
func TestSessionsAlreadyBoundDoNotConsumeTheLinkCeiling(t *testing.T) {
	t.Parallel()

	dialer := newRecordingDialer()
	limits := defaultLimits()
	limits.MaxLinks = 2
	pool := newPool(t, dialer, limits)

	for _, session := range []sessionwire.SessionID{"s-1", "s-2", "s-3"} {
		mustBind(t, pool, target(hostOne, endpoint1), bindRequest(hostOne, session))
	}
	if err := pool.Bind(context.Background(), target(hostTwo, endpoint2), bindRequest(hostTwo, "s-4")); err != nil {
		t.Fatalf("Bind to the second Host with a link to spare: %v", err)
	}
	if got := pool.Links(); got != 2 {
		t.Errorf("Links() = %d, want 2", got)
	}
	if got := pool.Bindings(hostOne); got != 3 {
		t.Errorf("Bindings(%s) = %d, want 3", hostOne, got)
	}
}

// TestASessionBoundToOneHostIsNotSilentlyMovedToAnother refuses rather than
// rebinding, because a silent move leaves the first Host holding a route the
// pool no longer tracks and therefore can never unbind.
func TestASessionBoundToOneHostIsNotSilentlyMovedToAnother(t *testing.T) {
	t.Parallel()

	dialer := newRecordingDialer()
	pool := newPool(t, dialer, hostlink.Limits{})

	mustBind(t, pool, target(hostOne, endpoint1), bindRequest(hostOne, "s-1"))

	err := pool.Bind(context.Background(), target(hostTwo, endpoint2), bindRequest(hostTwo, "s-1"))
	if !errors.Is(err, hostlink.ErrBindingConflict) {
		t.Fatalf("rebinding a bound session elsewhere = %v, want ErrBindingConflict", err)
	}
	if got := pool.Bindings(hostOne); got != 1 {
		t.Errorf("Bindings(%s) = %d, want the original binding intact", hostOne, got)
	}
	if got := pool.Bindings(hostTwo); got != 0 {
		t.Errorf("Bindings(%s) = %d, want 0", hostTwo, got)
	}
}

// TestRebindingTheSameSessionToTheSameHostResendsWithoutASecondBinding is the
// lease-epoch refresh path: the tuple moves, the route does not.
func TestRebindingTheSameSessionToTheSameHostResendsWithoutASecondBinding(t *testing.T) {
	t.Parallel()

	dialer := newRecordingDialer()
	pool := newPool(t, dialer, hostlink.Limits{})

	first := bindRequest(hostOne, "s-1")
	mustBind(t, pool, target(hostOne, endpoint1), first)
	second := first
	second.LeaseEpoch = first.LeaseEpoch + 1
	mustBind(t, pool, target(hostOne, endpoint1), second)

	if got := pool.Bindings(hostOne); got != 1 {
		t.Errorf("Bindings(%s) = %d, want 1", hostOne, got)
	}
	binds := dialer.link(hostOne).binds()
	if len(binds) != 2 {
		t.Fatalf("the Host received %d bind requests, want 2", len(binds))
	}
	if got := binds[1].LeaseEpoch; got != second.LeaseEpoch {
		t.Errorf("the second bind carried lease epoch %d, want %d", got, second.LeaseEpoch)
	}
}

func TestUnbindTellsTheHostAndReleasesTheLocalRoute(t *testing.T) {
	t.Parallel()

	dialer := newRecordingDialer()
	pool := newPool(t, dialer, hostlink.Limits{})

	mustBind(t, pool, target(hostOne, endpoint1), bindRequest(hostOne, "s-1"))
	mustBind(t, pool, target(hostOne, endpoint1), bindRequest(hostOne, "s-2"))

	if err := pool.Unbind(context.Background(), unbindRequest(hostOne, "s-1")); err != nil {
		t.Fatalf("Unbind: %v", err)
	}
	if got := pool.Bindings(hostOne); got != 1 {
		t.Errorf("Bindings(%s) = %d, want 1", hostOne, got)
	}
	unbinds := dialer.link(hostOne).unbinds()
	if len(unbinds) != 1 {
		t.Fatalf("the Host received %d unbind requests, want 1", len(unbinds))
	}
	if got := unbinds[0].SessionID; got != "s-1" {
		t.Errorf("the Host was told to unbind %q, want %q", got, "s-1")
	}
	// The link SURVIVES its last binding. Closing it here would make a
	// subscriber that reconnects within the idle window pay for a fresh
	// handshake, and the pool has an explicit reaper for that decision.
	if got := pool.Links(); got != 1 {
		t.Errorf("Links() = %d after the last binding was released, want 1", got)
	}
}

// TestUnbindingAnUnknownSessionIsRefusedRatherThanReportedAsDone stops a caller
// believing a route was released that was never held.
func TestUnbindingAnUnknownSessionIsRefusedRatherThanReportedAsDone(t *testing.T) {
	t.Parallel()

	dialer := newRecordingDialer()
	pool := newPool(t, dialer, hostlink.Limits{})

	err := pool.Unbind(context.Background(), unbindRequest(hostOne, "never-bound"))
	if !errors.Is(err, hostlink.ErrUnknownBinding) {
		t.Fatalf("Unbind of an unbound session = %v, want ErrUnknownBinding", err)
	}
}

// TestUnbindingWithTheWrongHostIsRefusedAndKeepsTheRoute is the Unbind half of
// the routing check Bind makes.
//
// An unbind carrying the wrong Host's tuple is a caller that has lost track of
// where a session lives. Releasing the route anyway would leave the Host that
// actually holds it with a binding nobody will ever release, and sending the
// request over the right link with the wrong tuple would have the Host refuse
// it for a reason that reads as a lease problem.
func TestUnbindingWithTheWrongHostIsRefusedAndKeepsTheRoute(t *testing.T) {
	t.Parallel()

	dialer := newRecordingDialer()
	pool := newPool(t, dialer, hostlink.Limits{})
	mustBind(t, pool, target(hostOne, endpoint1), bindRequest(hostOne, "s-1"))

	wrong := unbindRequest(hostTwo, "s-1")
	err := pool.Unbind(context.Background(), wrong)
	if !errors.Is(err, hostlink.ErrTargetMismatch) {
		t.Fatalf("Unbind naming the wrong host = %v, want ErrTargetMismatch", err)
	}
	if got := pool.Bindings(hostOne); got != 1 {
		t.Errorf("Bindings(%s) = %d after a refused unbind, want 1", hostOne, got)
	}
	if got := dialer.link(hostOne).unbinds(); len(got) != 0 {
		t.Errorf("the Host received %d unbind requests, want 0", len(got))
	}
}

// TestAFailedUnbindStillReleasesTheLocalRoute records a decision rather than an
// accident. A bind is a Factory-local routing optimization and the Host
// validates ownership against its own durable lease, so a Host that could not
// be told must not leave Factory holding a route it will never retry.
func TestAFailedUnbindStillReleasesTheLocalRoute(t *testing.T) {
	t.Parallel()

	dialer := newRecordingDialer()
	pool := newPool(t, dialer, hostlink.Limits{})
	mustBind(t, pool, target(hostOne, endpoint1), bindRequest(hostOne, "s-1"))
	dialer.link(hostOne).failUnbind(errors.New("host is draining"))

	if err := pool.Unbind(context.Background(), unbindRequest(hostOne, "s-1")); err == nil {
		t.Fatal("Unbind hid the Host's refusal")
	}
	if got := pool.Bindings(hostOne); got != 0 {
		t.Errorf("Bindings(%s) = %d after a failed unbind, want 0", hostOne, got)
	}
}

func TestACommandIsDeliveredOverTheLinkItsSessionIsBoundTo(t *testing.T) {
	t.Parallel()

	dialer := newRecordingDialer()
	pool := newPool(t, dialer, hostlink.Limits{})
	mustBind(t, pool, target(hostOne, endpoint1), bindRequest(hostOne, "s-1"))
	mustBind(t, pool, target(hostTwo, endpoint2), bindRequest(hostTwo, "s-2"))

	delivery := sessionwire.HostLinkCommandDelivery{CommandID: "cmd-abc"}
	if err := pool.DeliverCommand(context.Background(), tenant, "s-2", delivery); err != nil {
		t.Fatalf("DeliverCommand: %v", err)
	}

	// Both sides are asserted: the right Host got it AND the wrong Host did
	// not. Checking only the right one passes on a pool that broadcasts.
	if got := dialer.link(hostTwo).commands(); len(got) != 1 || got[0].CommandID != "cmd-abc" {
		t.Errorf("host-2 received %v, want one delivery of cmd-abc", got)
	}
	if got := dialer.link(hostOne).commands(); len(got) != 0 {
		t.Errorf("host-1 received %v, want nothing", got)
	}
}

// TestTwoTenantsMayHoldTheSameSessionIdOnDifferentHosts is why the route table
// is keyed by tenant AND session.
//
// Session ids are opaque and tenant-scoped -- the session channel is
// session:{tenant}:{session} for exactly this reason -- so a table keyed by
// session alone would let one tenant's binding answer for another's. The
// failure would not be a refusal, which is what makes it worth a case: the
// second bind would be refused as a conflict, and if it were not, one tenant's
// command would be delivered over the other tenant's link.
func TestTwoTenantsMayHoldTheSameSessionIdOnDifferentHosts(t *testing.T) {
	t.Parallel()

	const shared sessionwire.SessionID = "s-shared"
	const otherTenant sessionwire.TenantID = "tenant-b"

	dialer := newRecordingDialer()
	pool := newPool(t, dialer, hostlink.Limits{})

	first := bindRequest(hostOne, shared)
	second := bindRequest(hostTwo, shared)
	second.TenantID = otherTenant

	mustBind(t, pool, target(hostOne, endpoint1), first)
	mustBind(t, pool, target(hostTwo, endpoint2), second)

	if got := pool.Bindings(hostOne); got != 1 {
		t.Errorf("Bindings(%s) = %d, want 1", hostOne, got)
	}
	if got := pool.Bindings(hostTwo); got != 1 {
		t.Errorf("Bindings(%s) = %d, want 1", hostTwo, got)
	}
	// And each tenant's command goes to its own Host.
	if err := pool.DeliverCommand(context.Background(), otherTenant, shared, sessionwire.HostLinkCommandDelivery{CommandID: "cmd-b"}); err != nil {
		t.Fatalf("DeliverCommand: %v", err)
	}
	if got := dialer.link(hostTwo).commands(); len(got) != 1 || got[0].CommandID != "cmd-b" {
		t.Errorf("%s received %v, want one delivery of cmd-b", hostTwo, got)
	}
	if got := dialer.link(hostOne).commands(); len(got) != 0 {
		t.Errorf("%s received %v, want nothing", hostOne, got)
	}
	// Unbinding one tenant leaves the other's route intact.
	if err := pool.Unbind(context.Background(), unbindRequest(hostOne, shared)); err != nil {
		t.Fatalf("Unbind: %v", err)
	}
	if got := pool.Bindings(hostTwo); got != 1 {
		t.Errorf("Bindings(%s) = %d after the other tenant unbound, want 1", hostTwo, got)
	}
}

func TestDeliveringACommandForAnUnboundSessionIsRefused(t *testing.T) {
	t.Parallel()

	dialer := newRecordingDialer()
	pool := newPool(t, dialer, hostlink.Limits{})

	err := pool.DeliverCommand(context.Background(), tenant, "s-1", sessionwire.HostLinkCommandDelivery{CommandID: "cmd-abc"})
	if !errors.Is(err, hostlink.ErrUnknownBinding) {
		t.Fatalf("DeliverCommand for an unbound session = %v, want ErrUnknownBinding", err)
	}
}

// TestAFailedCommandRpcIsUndeliveredRatherThanRejected is runbook A7.1 step 3.
//
// The inbox record is already committed when this runs. The distinction the
// error must carry is "not handed over" versus "the Host said no": the first is
// retryable and leaves the record PENDING, and reporting it as the second would
// terminate a command the Host has never seen. The binding also survives,
// because a transport failure is not evidence that the route is wrong.
func TestAFailedCommandRpcIsUndeliveredRatherThanRejected(t *testing.T) {
	t.Parallel()

	dialer := newRecordingDialer()
	pool := newPool(t, dialer, hostlink.Limits{})
	mustBind(t, pool, target(hostOne, endpoint1), bindRequest(hostOne, "s-1"))

	cause := errors.New("write: broken pipe")
	dialer.link(hostOne).failCommand(cause)

	err := pool.DeliverCommand(context.Background(), tenant, "s-1", sessionwire.HostLinkCommandDelivery{CommandID: "cmd-abc"})
	if !errors.Is(err, hostlink.ErrCommandUndelivered) {
		t.Fatalf("DeliverCommand over a broken link = %v, want ErrCommandUndelivered", err)
	}
	if !errors.Is(err, cause) {
		t.Errorf("the cause was not wrapped: %v", err)
	}
	if got := pool.Bindings(hostOne); got != 1 {
		t.Errorf("Bindings(%s) = %d after an undelivered command, want the binding kept", hostOne, got)
	}
}

// TestAHostRefusalIsReportedAsTheHostsOwnAnswer is the other side of the same
// decision. A Host that answers with a HostLinkError has SEEN the command, so
// the caller must be able to tell that from a transport failure without reading
// the message text.
func TestAHostRefusalIsReportedAsTheHostsOwnAnswer(t *testing.T) {
	t.Parallel()

	dialer := newRecordingDialer()
	pool := newPool(t, dialer, hostlink.Limits{})
	mustBind(t, pool, target(hostOne, endpoint1), bindRequest(hostOne, "s-1"))

	refusal := &hostlink.HostRefusal{HostLinkError: sessionwire.HostLinkError{Code: sessionwire.HostLinkErrorNotAdmitting}}
	dialer.link(hostOne).failCommand(refusal)

	err := pool.DeliverCommand(context.Background(), tenant, "s-1", sessionwire.HostLinkCommandDelivery{CommandID: "cmd-abc"})
	if errors.Is(err, hostlink.ErrCommandUndelivered) {
		t.Errorf("a Host refusal was reported as undelivered: %v", err)
	}
	var got *hostlink.HostRefusal
	if !errors.As(err, &got) {
		t.Fatalf("DeliverCommand = %v, want a *HostRefusal", err)
	}
	if got.Code != sessionwire.HostLinkErrorNotAdmitting {
		t.Errorf("HostRefusal.Code = %q, want %q", got.Code, sessionwire.HostLinkErrorNotAdmitting)
	}
}

// TestAnInvalidCommandDeliveryNeverReachesTheHost keeps a record Core would
// refuse to marshal from being sent as something else.
func TestAnInvalidCommandDeliveryNeverReachesTheHost(t *testing.T) {
	t.Parallel()

	dialer := newRecordingDialer()
	pool := newPool(t, dialer, hostlink.Limits{})
	mustBind(t, pool, target(hostOne, endpoint1), bindRequest(hostOne, "s-1"))

	if err := pool.DeliverCommand(context.Background(), tenant, "s-1", sessionwire.HostLinkCommandDelivery{}); err == nil {
		t.Fatal("DeliverCommand accepted an empty command id")
	}
	if got := dialer.link(hostOne).commands(); len(got) != 0 {
		t.Errorf("the Host received %v, want nothing", got)
	}
}

func TestCapacityAndRegistryObservationsReachTheObserver(t *testing.T) {
	t.Parallel()

	dialer := newRecordingDialer()
	observer := &recordingObserver{}
	pool := newPoolWithObserver(t, dialer, defaultLimits(), observer)
	mustBind(t, pool, target(hostOne, endpoint1), bindRequest(hostOne, "s-1"))

	// The pool hands the observer to the dialer, so the link the dial produced
	// is the thing that must be able to reach it.
	link := dialer.link(hostOne)
	link.observer.ObserveCapacity(capacityReport(hostOne))
	link.observer.ObserveRegistry(registryObservation(hostOne, "s-1"))

	if got := observer.capacityHosts(); len(got) != 1 || got[0] != hostOne {
		t.Errorf("observed capacity for %v, want [%s]", got, hostOne)
	}
	if got := observer.registrySessions(); len(got) != 1 || got[0] != "s-1" {
		t.Errorf("observed registry for %v, want [s-1]", got)
	}
}

// TestAPoolComposedWithoutAnObserverStillDials records that the observer is
// optional and is never nil at the dialer: a link handed a nil observer would
// panic on the first capacity report a Host sends, which is a crash on the
// happy path of a deployment that simply did not want the reports.
func TestAPoolComposedWithoutAnObserverStillDials(t *testing.T) {
	t.Parallel()

	dialer := newRecordingDialer()
	pool, err := hostlink.NewPool(hostlink.Config{Dialer: dialer, Limits: defaultLimits()})
	if err != nil {
		t.Fatalf("NewPool without an observer: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close(context.Background()) })
	mustBind(t, pool, target(hostOne, endpoint1), bindRequest(hostOne, "s-1"))

	link := dialer.link(hostOne)
	if link.observer == nil {
		t.Fatal("the dialer was handed a nil Observer")
	}
	link.observer.ObserveCapacity(capacityReport(hostOne))
	link.observer.ObserveRegistry(registryObservation(hostOne, "s-1"))
}

func TestAnIdleLinkIsReapedOnlyAfterTheIdleWindow(t *testing.T) {
	t.Parallel()

	dialer := newRecordingDialer()
	limits := defaultLimits()
	limits.IdleTimeout = 30 * time.Second
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	pool := newPoolWithClock(t, dialer, limits, clock)

	mustBind(t, pool, target(hostOne, endpoint1), bindRequest(hostOne, "s-1"))

	// A link with a live binding is never idle, however long it has been open.
	clock.advance(time.Hour)
	if got := pool.ReapIdle(); got != 0 {
		t.Fatalf("ReapIdle() reaped %d links with a live binding, want 0", got)
	}

	if err := pool.Unbind(context.Background(), unbindRequest(hostOne, "s-1")); err != nil {
		t.Fatalf("Unbind: %v", err)
	}
	// One tick short of the window.
	clock.advance(limits.IdleTimeout - time.Nanosecond)
	if got := pool.ReapIdle(); got != 0 {
		t.Fatalf("ReapIdle() reaped %d links inside the idle window, want 0", got)
	}
	clock.advance(time.Nanosecond)
	if got := pool.ReapIdle(); got != 1 {
		t.Fatalf("ReapIdle() reaped %d links past the idle window, want 1", got)
	}
	if got := pool.Links(); got != 0 {
		t.Errorf("Links() = %d after the reap, want 0", got)
	}
	if got := dialer.link(hostOne).closes(); got != 1 {
		t.Errorf("the reaped link was closed %d times, want 1", got)
	}
}

// TestRebindingBeforeTheWindowKeepsTheLinkOpen is the reason the reaper exists
// at all: the idle window is what makes a reconnecting subscriber cheap.
func TestRebindingBeforeTheWindowKeepsTheLinkOpen(t *testing.T) {
	t.Parallel()

	dialer := newRecordingDialer()
	limits := defaultLimits()
	limits.IdleTimeout = 30 * time.Second
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	pool := newPoolWithClock(t, dialer, limits, clock)

	mustBind(t, pool, target(hostOne, endpoint1), bindRequest(hostOne, "s-1"))
	if err := pool.Unbind(context.Background(), unbindRequest(hostOne, "s-1")); err != nil {
		t.Fatalf("Unbind: %v", err)
	}
	clock.advance(limits.IdleTimeout / 2)
	mustBind(t, pool, target(hostOne, endpoint1), bindRequest(hostOne, "s-2"))
	clock.advance(limits.IdleTimeout)

	if got := pool.ReapIdle(); got != 0 {
		t.Errorf("ReapIdle() reaped %d links, want 0", got)
	}
	if got := dialer.dials(); got != 1 {
		t.Errorf("the dialer was called %d times, want 1 -- the link was not reused", got)
	}
}

func TestCloseClosesEveryLinkAndRefusesFurtherWork(t *testing.T) {
	t.Parallel()

	dialer := newRecordingDialer()
	pool := newPool(t, dialer, hostlink.Limits{})
	mustBind(t, pool, target(hostOne, endpoint1), bindRequest(hostOne, "s-1"))
	mustBind(t, pool, target(hostTwo, endpoint2), bindRequest(hostTwo, "s-2"))

	if err := pool.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for _, host := range []sessionwire.HostID{hostOne, hostTwo} {
		if got := dialer.link(host).closes(); got != 1 {
			t.Errorf("%s was closed %d times, want 1", host, got)
		}
	}
	if got := pool.Links(); got != 0 {
		t.Errorf("Links() = %d after Close, want 0", got)
	}

	if err := pool.Bind(context.Background(), target(hostOne, endpoint1), bindRequest(hostOne, "s-3")); !errors.Is(err, hostlink.ErrPoolClosed) {
		t.Errorf("Bind after Close = %v, want ErrPoolClosed", err)
	}
	if err := pool.DeliverCommand(context.Background(), tenant, "s-1", sessionwire.HostLinkCommandDelivery{CommandID: "c"}); !errors.Is(err, hostlink.ErrPoolClosed) {
		t.Errorf("DeliverCommand after Close = %v, want ErrPoolClosed", err)
	}
	// Closing twice is not an error and must not close a link twice.
	if err := pool.Close(context.Background()); err != nil {
		t.Errorf("second Close: %v", err)
	}
	if got := dialer.link(hostOne).closes(); got != 1 {
		t.Errorf("%s was closed %d times after two Close calls, want 1", hostOne, got)
	}
}

// TestCloseReportsEveryLinkFailureAndStillClosesTheRest keeps one wedged
// connection from stranding the others.
func TestCloseReportsEveryLinkFailureAndStillClosesTheRest(t *testing.T) {
	t.Parallel()

	dialer := newRecordingDialer()
	pool := newPool(t, dialer, hostlink.Limits{})
	mustBind(t, pool, target(hostOne, endpoint1), bindRequest(hostOne, "s-1"))
	mustBind(t, pool, target(hostTwo, endpoint2), bindRequest(hostTwo, "s-2"))
	cause := errors.New("close: connection reset")
	dialer.link(hostOne).failClose(cause)

	err := pool.Close(context.Background())
	if !errors.Is(err, cause) {
		t.Errorf("Close = %v, want the link's own failure", err)
	}
	if got := dialer.link(hostTwo).closes(); got != 1 {
		t.Errorf("%s was closed %d times, want 1 -- one failure stranded the rest", hostTwo, got)
	}
}

// TestADialFailureLeavesNoLinkBehind stops a failed dial from occupying a slot
// under MaxLinks forever.
func TestADialFailureLeavesNoLinkBehind(t *testing.T) {
	t.Parallel()

	dialer := newRecordingDialer()
	cause := errors.New("dial tcp: connection refused")
	dialer.failDial(cause)
	pool := newPool(t, dialer, hostlink.Limits{})

	if err := pool.Bind(context.Background(), target(hostOne, endpoint1), bindRequest(hostOne, "s-1")); !errors.Is(err, cause) {
		t.Fatalf("Bind = %v, want the dialer's failure", err)
	}
	if got := pool.Links(); got != 0 {
		t.Errorf("Links() = %d after a failed dial, want 0", got)
	}
	if got := pool.Bindings(hostOne); got != 0 {
		t.Errorf("Bindings(%s) = %d after a failed dial, want 0", hostOne, got)
	}
}

// TestAFailedBindOverAFreshLinkKeepsTheLinkButNotTheBinding. The connection is
// good -- the Host answered -- so throwing it away would turn one lease
// disagreement into a dial storm. The BINDING must not be recorded, because
// the Host does not have it.
func TestAFailedBindOverAFreshLinkKeepsTheLinkButNotTheBinding(t *testing.T) {
	t.Parallel()

	dialer := newRecordingDialer()
	dialer.onDial = func(l *fakeLink) {
		l.failBind(&hostlink.HostRefusal{HostLinkError: sessionwire.HostLinkError{Code: sessionwire.HostLinkErrorEpochMismatch, CurrentLeaseEpoch: 11}})
	}
	pool := newPool(t, dialer, hostlink.Limits{})

	var refusal *hostlink.HostRefusal
	err := pool.Bind(context.Background(), target(hostOne, endpoint1), bindRequest(hostOne, "s-1"))
	if !errors.As(err, &refusal) {
		t.Fatalf("Bind = %v, want a *HostRefusal", err)
	}
	if got := pool.Bindings(hostOne); got != 0 {
		t.Errorf("Bindings(%s) = %d after a refused bind, want 0", hostOne, got)
	}
	if got := pool.Links(); got != 1 {
		t.Errorf("Links() = %d after a refused bind, want the connection kept", got)
	}
}

// TestConcurrentBindsToOneHostOpenOneConnection is the property the whole pool
// exists for, under the concurrency it will actually see: many subscribers
// arriving at once for sessions on one Host.
func TestConcurrentBindsToOneHostOpenOneConnection(t *testing.T) {
	t.Parallel()

	dialer := newRecordingDialer()
	pool := newPool(t, dialer, hostlink.Limits{})

	const sessions = 32
	var wg sync.WaitGroup
	errs := make(chan error, sessions)
	for i := range sessions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			session := sessionwire.SessionID(fmt.Sprintf("s-%d", i))
			if err := pool.Bind(context.Background(), target(hostOne, endpoint1), bindRequest(hostOne, session)); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("Bind: %v", err)
	}

	if got := dialer.dials(); got != 1 {
		t.Errorf("the dialer was called %d times, want 1", got)
	}
	if got := pool.Bindings(hostOne); got != sessions {
		t.Errorf("Bindings(%s) = %d, want %d", hostOne, got, sessions)
	}
}

// TestTwoPoolsConnectToOneHostIndependently is runbook A7.1 step 2. Nothing is
// shared between replicas: no broker, no leader, no coordination.
func TestTwoPoolsConnectToOneHostIndependently(t *testing.T) {
	t.Parallel()

	dialerA, dialerB := newRecordingDialer(), newRecordingDialer()
	poolA, poolB := newPool(t, dialerA, hostlink.Limits{}), newPool(t, dialerB, hostlink.Limits{})

	mustBind(t, poolA, target(hostOne, endpoint1), bindRequest(hostOne, "s-1"))
	mustBind(t, poolB, target(hostOne, endpoint1), bindRequest(hostOne, "s-2"))

	if got := dialerA.dials(); got != 1 {
		t.Errorf("replica A dialled %d times, want 1", got)
	}
	if got := dialerB.dials(); got != 1 {
		t.Errorf("replica B dialled %d times, want 1", got)
	}
	// Closing one replica's pool must not disturb the other's link.
	if err := poolA.Close(context.Background()); err != nil {
		t.Fatalf("Close A: %v", err)
	}
	if got := dialerB.link(hostOne).closes(); got != 0 {
		t.Errorf("replica B's link was closed %d times by replica A, want 0", got)
	}
	if got := poolB.Bindings(hostOne); got != 1 {
		t.Errorf("replica B holds %d bindings, want 1", got)
	}
}

func TestNewPoolRejectsAnIncompleteComposition(t *testing.T) {
	t.Parallel()

	tests := map[string]hostlink.Config{
		"no dialer": {Limits: defaultLimits()},
		"invalid limits": {Dialer: newRecordingDialer(), Limits: hostlink.Limits{
			MaxLinks: 1, DialTimeout: time.Second, IdleTimeout: time.Millisecond,
			ReconnectMin: time.Second, ReconnectMax: time.Second,
		}},
	}
	for name, cfg := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if _, err := hostlink.NewPool(cfg); !errors.Is(err, hostlink.ErrInvalidConfig) {
				t.Fatalf("NewPool(%s) = %v, want ErrInvalidConfig", name, err)
			}
		})
	}
}

// TestZeroLimitsAreFilledFromTheDefaults is what every case above relies on
// when it passes hostlink.Limits{}: an unset pool is usable rather than a pool
// with a zero ceiling that refuses its first bind.
func TestZeroLimitsAreFilledFromTheDefaults(t *testing.T) {
	t.Parallel()

	if err := hostlink.DefaultLimits().Validate(); err != nil {
		t.Fatalf("DefaultLimits is not valid: %v", err)
	}
	pool, err := hostlink.NewPool(hostlink.Config{Dialer: newRecordingDialer()})
	if err != nil {
		t.Fatalf("NewPool with zero Limits: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close(context.Background()) })
	if got, want := pool.Limits(), hostlink.DefaultLimits(); got != want {
		t.Errorf("Limits() = %+v, want %+v", got, want)
	}
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

func defaultLimits() hostlink.Limits { return hostlink.DefaultLimits() }

func target(host sessionwire.HostID, endpoint sessionwire.InternalEndpoint) hostlink.Target {
	return hostlink.Target{Host: host, Endpoint: endpoint}
}

func bindRequest(host sessionwire.HostID, session sessionwire.SessionID) sessionwire.HostLinkBindRequest {
	return sessionwire.HostLinkBindRequest{
		Version:                sessionwire.CurrentWireVersion,
		TenantID:               tenant,
		SessionID:              session,
		HostID:                 host,
		HostGeneration:         7,
		LeaseEpoch:             3,
		RuntimeCompatibilityID: "runtime-1",
		IdempotencyKey:         "bind-" + string(session),
	}
}

func unbindRequest(host sessionwire.HostID, session sessionwire.SessionID) sessionwire.HostLinkUnbindRequest {
	return sessionwire.HostLinkUnbindRequest{
		Version:        sessionwire.CurrentWireVersion,
		TenantID:       tenant,
		SessionID:      session,
		HostID:         host,
		HostGeneration: 7,
		LeaseEpoch:     3,
		IdempotencyKey: "unbind-" + string(session),
	}
}

func capacityReport(host sessionwire.HostID) sessionwire.HostLinkCapacityReport {
	observed := time.Unix(1_700_000_000, 0).UTC()
	return sessionwire.HostLinkCapacityReport{
		Version:                sessionwire.CurrentWireVersion,
		HostID:                 host,
		HostGeneration:         7,
		AgentID:                "agent-1",
		RuntimeCompatibilityID: "runtime-1",
		Placement:              sessionwire.HostPlacementPooled,
		InternalEndpoint:       endpoint1,
		IsolationClass:         sessionwire.HostIsolationClassTenantExclusive,
		Accepting:              true,
		AvailableCapacity:      4,
		ObservedAt:             observed,
		ExpiresAt:              observed.Add(30 * time.Second),
	}
}

func registryObservation(host sessionwire.HostID, session sessionwire.SessionID) sessionwire.HostLinkRegistryObservation {
	observed := time.Unix(1_700_000_000, 0).UTC()
	return sessionwire.HostLinkRegistryObservation{
		Version:                sessionwire.CurrentWireVersion,
		TenantID:               tenant,
		SessionID:              session,
		HostID:                 host,
		HostGeneration:         7,
		AgentID:                "agent-1",
		RuntimeCompatibilityID: "runtime-1",
		Placement:              sessionwire.HostPlacementPooled,
		InternalEndpoint:       endpoint1,
		Residency:              sessionwire.SessionResidencyResident,
		Accepting:              true,
		LeaseEpoch:             3,
		ObservedAt:             observed,
		ExpiresAt:              observed.Add(30 * time.Second),
	}
}

func newPool(t *testing.T, dialer hostlink.Dialer, limits hostlink.Limits) *hostlink.Pool {
	t.Helper()

	return newPoolWithObserver(t, dialer, limits, &recordingObserver{})
}

func newPoolWithObserver(t *testing.T, dialer hostlink.Dialer, limits hostlink.Limits, observer hostlink.Observer) *hostlink.Pool {
	t.Helper()

	pool, err := hostlink.NewPool(hostlink.Config{Dialer: dialer, Observer: observer, Limits: limits})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close(context.Background()) })
	return pool
}

func newPoolWithClock(t *testing.T, dialer hostlink.Dialer, limits hostlink.Limits, clock *fakeClock) *hostlink.Pool {
	t.Helper()

	pool, err := hostlink.NewPool(hostlink.Config{
		Dialer:   dialer,
		Observer: &recordingObserver{},
		Limits:   limits,
		Now:      clock.Now,
	})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close(context.Background()) })
	return pool
}

func mustBind(t *testing.T, pool *hostlink.Pool, tgt hostlink.Target, req sessionwire.HostLinkBindRequest) {
	t.Helper()

	if err := pool.Bind(context.Background(), tgt, req); err != nil {
		t.Fatalf("Bind(%s/%s): %v", tgt.Host, req.SessionID, err)
	}
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// recordingDialer is the transport stand-in. It records what the pool asked
// for, which is how a case distinguishes "the pool reused a link" from "the
// pool dialled again and the counter happened to match".
type recordingDialer struct {
	mu      sync.Mutex
	dialed  []hostlink.Target
	links   map[sessionwire.HostID]*fakeLink
	dialErr error
	onDial  func(*fakeLink)
}

func newRecordingDialer() *recordingDialer {
	return &recordingDialer{links: map[sessionwire.HostID]*fakeLink{}}
}

func (d *recordingDialer) Dial(_ context.Context, tgt hostlink.Target, observer hostlink.Observer) (hostlink.Link, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.dialErr != nil {
		return nil, d.dialErr
	}
	d.dialed = append(d.dialed, tgt)
	link := &fakeLink{host: tgt.Host, observer: observer}
	if d.onDial != nil {
		d.onDial(link)
	}
	d.links[tgt.Host] = link
	return link, nil
}

func (d *recordingDialer) failDial(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.dialErr = err
}

func (d *recordingDialer) dials() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.dialed)
}

func (d *recordingDialer) targets() []hostlink.Target {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]hostlink.Target(nil), d.dialed...)
}

func (d *recordingDialer) link(host sessionwire.HostID) *fakeLink {
	d.mu.Lock()
	defer d.mu.Unlock()
	if link, ok := d.links[host]; ok {
		return link
	}
	return &fakeLink{host: host}
}

type fakeLink struct {
	host     sessionwire.HostID
	observer hostlink.Observer

	mu          sync.Mutex
	bindRecords []sessionwire.HostLinkBindRequest
	unbindRecs  []sessionwire.HostLinkUnbindRequest
	commandRecs []sessionwire.HostLinkCommandDelivery
	closeCount  int
	bindErr     error
	unbindErr   error
	commandErr  error
	closeErr    error
}

func (l *fakeLink) Host() sessionwire.HostID { return l.host }

func (l *fakeLink) Bind(_ context.Context, req sessionwire.HostLinkBindRequest) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.bindErr != nil {
		return l.bindErr
	}
	l.bindRecords = append(l.bindRecords, req)
	return nil
}

func (l *fakeLink) Unbind(_ context.Context, req sessionwire.HostLinkUnbindRequest) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.unbindErr != nil {
		return l.unbindErr
	}
	l.unbindRecs = append(l.unbindRecs, req)
	return nil
}

func (l *fakeLink) DeliverCommand(_ context.Context, req sessionwire.HostLinkCommandDelivery) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.commandErr != nil {
		return l.commandErr
	}
	l.commandRecs = append(l.commandRecs, req)
	return nil
}

func (l *fakeLink) Close(context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closeCount++
	return l.closeErr
}

func (l *fakeLink) binds() []sessionwire.HostLinkBindRequest {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]sessionwire.HostLinkBindRequest(nil), l.bindRecords...)
}

func (l *fakeLink) unbinds() []sessionwire.HostLinkUnbindRequest {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]sessionwire.HostLinkUnbindRequest(nil), l.unbindRecs...)
}

func (l *fakeLink) commands() []sessionwire.HostLinkCommandDelivery {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]sessionwire.HostLinkCommandDelivery(nil), l.commandRecs...)
}

func (l *fakeLink) closes() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.closeCount
}

func (l *fakeLink) failBind(err error)    { l.mu.Lock(); defer l.mu.Unlock(); l.bindErr = err }
func (l *fakeLink) failUnbind(err error)  { l.mu.Lock(); defer l.mu.Unlock(); l.unbindErr = err }
func (l *fakeLink) failCommand(err error) { l.mu.Lock(); defer l.mu.Unlock(); l.commandErr = err }
func (l *fakeLink) failClose(err error)   { l.mu.Lock(); defer l.mu.Unlock(); l.closeErr = err }

type recordingObserver struct {
	mu       sync.Mutex
	capacity []sessionwire.HostLinkCapacityReport
	registry []sessionwire.HostLinkRegistryObservation
}

func (o *recordingObserver) ObserveCapacity(r sessionwire.HostLinkCapacityReport) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.capacity = append(o.capacity, r)
}

func (o *recordingObserver) ObserveRegistry(r sessionwire.HostLinkRegistryObservation) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.registry = append(o.registry, r)
}

func (o *recordingObserver) capacityHosts() []sessionwire.HostID {
	o.mu.Lock()
	defer o.mu.Unlock()
	hosts := make([]sessionwire.HostID, 0, len(o.capacity))
	for _, r := range o.capacity {
		hosts = append(hosts, r.HostID)
	}
	return hosts
}

func (o *recordingObserver) registrySessions() []sessionwire.SessionID {
	o.mu.Lock()
	defer o.mu.Unlock()
	sessions := make([]sessionwire.SessionID, 0, len(o.registry))
	for _, r := range o.registry {
		sessions = append(sessions, r.SessionID)
	}
	return sessions
}

// TestDialerYieldsALinkForTheHostItWasAskedFor drives the seam the pool's test
// double stands in for, and pins the result TYPE: a Dialer returning something
// wider than Link would let the pool hold a connection it cannot close or
// attribute. It is the case A0.1 wrote against the placeholder seam, kept
// against the real one.
func TestDialerYieldsALinkForTheHostItWasAskedFor(t *testing.T) {
	t.Parallel()

	var dialer hostlink.Dialer = newRecordingDialer()
	link, err := dialer.Dial(context.Background(), target(hostOne, endpoint1), &recordingObserver{})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if got := link.Host(); got != hostOne {
		t.Errorf("Link.Host() = %q, want %q", got, hostOne)
	}
	if err := link.Close(context.Background()); err != nil {
		t.Errorf("Link.Close: %v", err)
	}

	dial, ok := reflect.TypeOf(&dialer).Elem().MethodByName("Dial")
	if !ok {
		t.Fatal("Dialer has no Dial method")
	}
	if want := reflect.TypeOf((*hostlink.Link)(nil)).Elem(); dial.Type.Out(0) != want {
		t.Errorf("Dial returns %s, want %s", dial.Type.Out(0), want)
	}
}

// TestThisPackageCannotTouchTheInbox is the STRUCTURAL half of runbook A7.1
// step 3.
//
// "A failed command RPC leaves the already committed inbox record pending" is
// two claims. The error classification is the first, measured by
// TestAFailedCommandRpcIsUndeliveredRatherThanRejected. The second is that
// nothing here could mark the record anything else even if it wanted to, and
// that is a property of the import graph rather than of a code path: this
// package holds no store handle, so there is no line to review and no future
// line to add without this case failing.
//
// It is scoped to what it proves. It does not show that the CALLER leaves the
// record pending -- that is the admission service's, and A7.2/A9.1 wire it.
func TestThisPackageCannotTouchTheInbox(t *testing.T) {
	t.Parallel()

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}

	files := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		files++
		parsed, err := parser.ParseFile(token.NewFileSet(), name, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		for _, spec := range parsed.Imports {
			path, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatalf("%s: unquote %s: %v", name, spec.Path.Value, err)
			}
			if path == "github.com/looprig/sessionstore" || strings.HasPrefix(path, "github.com/looprig/sessionstore/") {
				t.Errorf("%s imports %s: this package must not be able to write the inbox", name, path)
			}
		}
	}
	// A walk that found nothing would pass silently and prove nothing, which is
	// the failure mode this workspace names vacuous.
	if files == 0 {
		t.Fatal("parsed no production files")
	}
}
