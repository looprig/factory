package routing

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage/memstore"
)

const (
	bindTenant  = sessionwire.TenantID("tenant-a")
	bindSession = sessionwire.SessionID("session-a")
	bindAgent   = sessionwire.AgentID("agent-a")
	bindRuntime = "runtime-v1"
)

// observation builds a valid registry observation for one owner. Core's own
// Validate is strict about every member of it, so a helper is the only way a
// case can be about the ONE member it varies.
func observation(host sessionwire.HostID, generation, epoch uint64) sessionwire.HostLinkRegistryObservation {
	return sessionwire.HostLinkRegistryObservation{
		Version: sessionwire.CurrentWireVersion, TenantID: bindTenant, SessionID: bindSession,
		HostID: host, HostGeneration: generation, AgentID: bindAgent, RuntimeCompatibilityID: bindRuntime,
		Placement:        sessionwire.HostPlacementPooled,
		InternalEndpoint: sessionwire.InternalEndpoint("wss://" + string(host) + ".internal/hostlink"),
		Residency:        sessionwire.SessionResidencyResident, Accepting: true, LeaseEpoch: epoch,
		ObservedAt: directoryNow, ExpiresAt: directoryNow.Add(time.Minute),
	}
}

// recordingResolver answers Owner from a per-session script and counts the
// calls, because "reuse without a new placement lookup" is a claim about how
// MANY times the registry was read, not about the answer.
type recordingResolver struct {
	mu    sync.Mutex
	owner map[sessionwire.SessionID]sessionwire.HostLinkRegistryObservation
	found map[sessionwire.SessionID]bool
	err   error
	calls int
}

func newResolver(obs ...sessionwire.HostLinkRegistryObservation) *recordingResolver {
	r := &recordingResolver{
		owner: map[sessionwire.SessionID]sessionwire.HostLinkRegistryObservation{},
		found: map[sessionwire.SessionID]bool{},
	}
	for _, o := range obs {
		r.put(o)
	}
	return r
}

func (r *recordingResolver) put(o sessionwire.HostLinkRegistryObservation) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.owner[o.SessionID] = o
	r.found[o.SessionID] = true
}

func (r *recordingResolver) Owner(_ context.Context, _ sessionwire.TenantID, session sessionwire.SessionID) (sessionwire.HostLinkRegistryObservation, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.err != nil {
		return sessionwire.HostLinkRegistryObservation{}, false, r.err
	}
	return r.owner[session], r.found[session], nil
}

func (r *recordingResolver) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

type deliveredCommand struct {
	tenant   sessionwire.TenantID
	session  sessionwire.SessionID
	delivery sessionwire.HostLinkCommandDelivery
}

// recordingBinder stands in for the HostLink pool. It records the WHOLE
// request of every call, because the tuple a bind carries is the thing a Host
// validates against its lease and a case asserting only "one bind happened"
// cannot see it addressed to the wrong Host.
type recordingBinder struct {
	mu         sync.Mutex
	binds      []sessionwire.HostLinkBindRequest
	endpoints  []sessionwire.InternalEndpoint
	unbinds    []sessionwire.HostLinkUnbindRequest
	delivered  []deliveredCommand
	bindErr    error
	unbindErr  error
	deliverErr error
}

func (b *recordingBinder) Bind(_ context.Context, endpoint sessionwire.InternalEndpoint, req sessionwire.HostLinkBindRequest) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.bindErr != nil {
		return b.bindErr
	}
	b.binds = append(b.binds, req)
	b.endpoints = append(b.endpoints, endpoint)
	return nil
}

func (b *recordingBinder) Unbind(_ context.Context, req sessionwire.HostLinkUnbindRequest) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.unbinds = append(b.unbinds, req)
	return b.unbindErr
}

func (b *recordingBinder) DeliverCommand(_ context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, delivery sessionwire.HostLinkCommandDelivery) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.deliverErr != nil {
		return b.deliverErr
	}
	b.delivered = append(b.delivered, deliveredCommand{tenant: tenant, session: session, delivery: delivery})
	return nil
}

func (b *recordingBinder) bindCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.binds)
}

func (b *recordingBinder) unbindCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.unbinds)
}

func newBindings(t *testing.T, resolver Resolver, binder Binder) *Bindings {
	t.Helper()

	bindings, err := NewBindings(resolver, binder)
	if err != nil {
		t.Fatalf("NewBindings: %v", err)
	}
	return bindings
}

func acquire(t *testing.T, b *Bindings, session sessionwire.SessionID) Binding {
	t.Helper()

	binding, err := b.Acquire(context.Background(), bindTenant, session)
	if err != nil {
		t.Fatalf("Acquire(%s): %v", session, err)
	}
	return binding
}

// TestABindingIsKeyedByEveryIdentityItRoutesOn is A4.3 step 1. The key is the
// whole ownership tuple rather than the session, and each member earns its
// place: the tenant because a session id is unique only within one (the
// HostLink pool's own route table learned this), and the Host, its generation
// and the lease epoch because a route to a restarted Host or a superseded
// lease is not the same route -- it is one a Host will refuse.
func TestABindingIsKeyedByEveryIdentityItRoutesOn(t *testing.T) {
	t.Parallel()

	binder := &recordingBinder{}
	resolver := newResolver(observation("host-a", 3, 7))
	bindings := newBindings(t, resolver, binder)

	binding := acquire(t, bindings, bindSession)
	want := BindingKey{
		TenantID: bindTenant, SessionID: bindSession, HostID: "host-a",
		HostGeneration: 3, LeaseEpoch: 7,
	}
	if binding.Key != want {
		t.Fatalf("key = %+v, want %+v", binding.Key, want)
	}
	if binding.Endpoint != "wss://host-a.internal/hostlink" {
		t.Errorf("endpoint = %q, want the observed internal endpoint", binding.Endpoint)
	}
	if binding.RuntimeCompatibilityID != bindRuntime {
		t.Errorf("runtime = %q, want %q", binding.RuntimeCompatibilityID, bindRuntime)
	}

	if len(binder.binds) != 1 {
		t.Fatalf("binds = %d, want 1", len(binder.binds))
	}
	sent := binder.binds[0]
	if err := sent.Validate(); err != nil {
		t.Errorf("the bind request Core would carry is invalid: %v", err)
	}
	if sent.TenantID != bindTenant || sent.SessionID != bindSession || sent.HostID != "host-a" ||
		sent.HostGeneration != 3 || sent.LeaseEpoch != 7 || sent.RuntimeCompatibilityID != bindRuntime {
		t.Errorf("bind request = %+v, want the observed ownership tuple", sent)
	}
	if sent.Version != sessionwire.CurrentWireVersion {
		t.Errorf("bind version = %d, want the current wire version", sent.Version)
	}
	if binder.endpoints[0] != "wss://host-a.internal/hostlink" {
		t.Errorf("bind endpoint = %q, want the observed one", binder.endpoints[0])
	}
}

// TestOneSessionIDInTwoTenantsIsTwoBindings is the case a table keyed by
// session alone passes: one tenant's route must never answer for another's.
func TestOneSessionIDInTwoTenantsIsTwoBindings(t *testing.T) {
	t.Parallel()

	shared := sessionwire.SessionID("session-shared")
	first := observation("host-a", 1, 1)
	first.SessionID = shared
	second := observation("host-b", 1, 1)
	second.SessionID = shared
	second.TenantID = "tenant-b"

	binder := &recordingBinder{}
	resolver := newResolver(first)
	bindings := newBindings(t, resolver, binder)

	a := acquire(t, bindings, shared)
	resolver.put(second)
	b, err := bindings.Acquire(context.Background(), "tenant-b", shared)
	if err != nil {
		t.Fatalf("Acquire for the second tenant: %v", err)
	}
	if a.Key.HostID != "host-a" || b.Key.HostID != "host-b" {
		t.Fatalf("hosts = %q and %q, want one route per tenant", a.Key.HostID, b.Key.HostID)
	}
	if bindings.Len() != 2 {
		t.Errorf("Len = %d, want 2", bindings.Len())
	}
	if binder.bindCount() != 2 {
		t.Errorf("binds = %d, want one per tenant", binder.bindCount())
	}
}

// TestASecondAcquireReusesTheBindingWithoutAskingTheRegistry is A4.3 step 2's
// "reuse without new placement lookup", and it is asserted as a COUNT of
// registry reads rather than as an equal answer: a Bindings that re-read the
// registry every time would return an identical binding and pass any
// comparison of the two.
func TestASecondAcquireReusesTheBindingWithoutAskingTheRegistry(t *testing.T) {
	t.Parallel()

	binder := &recordingBinder{}
	resolver := newResolver(observation("host-a", 1, 4))
	bindings := newBindings(t, resolver, binder)

	first := acquire(t, bindings, bindSession)
	second := acquire(t, bindings, bindSession)
	if first != second {
		t.Fatalf("second acquire returned %+v, want the first binding %+v", second, first)
	}
	if resolver.count() != 1 {
		t.Errorf("registry reads = %d, want 1; a reused binding costs no lookup", resolver.count())
	}
	if binder.bindCount() != 1 {
		t.Errorf("binds = %d, want 1", binder.bindCount())
	}
	if got := bindings.Demand(bindTenant, bindSession); got != 2 {
		t.Errorf("demand = %d, want 2", got)
	}
}

// TestAChangedOwnershipTupleInvalidatesTheBinding drives every way an
// observation can contradict a live route, and the stale case in the opposite
// direction. Invalidation DROPS the route rather than rebinding from the push:
// an observation is a hint and the registry is the authority, so the next
// caller re-reads it. What the push is trusted for is only "what you hold is
// no longer current", which needs no trust at all.
func TestAChangedOwnershipTupleInvalidatesTheBinding(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		observed   sessionwire.HostLinkRegistryObservation
		invalidate bool
	}{
		{name: "a newer lease epoch", observed: observation("host-a", 2, 6), invalidate: true},
		{name: "another host at the same epoch", observed: observation("host-b", 2, 5), invalidate: true},
		{name: "the same host restarted", observed: observation("host-a", 3, 5), invalidate: true},
		{name: "the tuple it already holds", observed: observation("host-a", 2, 5), invalidate: false},
		{name: "an older lease epoch", observed: observation("host-a", 9, 4), invalidate: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			binder := &recordingBinder{}
			resolver := newResolver(observation("host-a", 2, 5))
			bindings := newBindings(t, resolver, binder)
			acquire(t, bindings, bindSession)

			if dropped := bindings.Observe(context.Background(), tc.observed); dropped != tc.invalidate {
				t.Fatalf("Observe reported dropped=%v, want %v", dropped, tc.invalidate)
			}
			_, bound := bindings.Binding(bindTenant, bindSession)
			if bound == tc.invalidate {
				t.Fatalf("bound=%v after an observation that should have invalidate=%v", bound, tc.invalidate)
			}
			if got := bindings.Demand(bindTenant, bindSession); got != 1 {
				t.Fatalf("demand = %d, want the subscriber's demand to survive invalidation", got)
			}
			if !tc.invalidate {
				if binder.unbindCount() != 0 {
					t.Fatalf("unbinds = %d, want none", binder.unbindCount())
				}
				return
			}
			if binder.unbindCount() != 1 {
				t.Fatalf("unbinds = %d, want the old host told once", binder.unbindCount())
			}
			// The unbind names the tuple the binding HELD, not the one that
			// replaced it: an unbind addressed to the new owner would leave
			// the old Host holding a route nobody will ever remove.
			sent := binder.unbinds[0]
			if err := sent.Validate(); err != nil {
				t.Errorf("the unbind request Core would carry is invalid: %v", err)
			}
			if sent.HostID != "host-a" || sent.HostGeneration != 2 || sent.LeaseEpoch != 5 {
				t.Errorf("unbind = %+v, want the tuple the dropped binding held", sent)
			}
		})
	}
}

// TestAnObservationForAnUnboundSessionChangesNothing keeps Observe from being
// a second way to create a route. Binding is demand-driven; a push about a
// session nobody is routing is information this replica has no use for.
func TestAnObservationForAnUnboundSessionChangesNothing(t *testing.T) {
	t.Parallel()

	binder := &recordingBinder{}
	bindings := newBindings(t, newResolver(), binder)

	if dropped := bindings.Observe(context.Background(), observation("host-a", 1, 1)); dropped {
		t.Error("Observe dropped a route for a session that had none")
	}
	if bindings.Len() != 0 || binder.bindCount() != 0 || binder.unbindCount() != 0 {
		t.Errorf("len=%d binds=%d unbinds=%d, want an untouched table", bindings.Len(), binder.bindCount(), binder.unbindCount())
	}
}

// TestConcurrentAcquirersProduceExactlyOneBind is A4.3 step 2's concurrent
// bind winner. Thirty-two subscribers arriving at once for one session must
// cost one bind and one registry read; the count is the assertion, because
// every racer receiving an equal binding is also what a Bindings that bound
// thirty-two times would produce.
func TestConcurrentAcquirersProduceExactlyOneBind(t *testing.T) {
	t.Parallel()

	binder := &recordingBinder{}
	resolver := newResolver(observation("host-a", 1, 1))
	bindings := newBindings(t, resolver, binder)

	const racers = 32
	var start sync.WaitGroup
	var done sync.WaitGroup
	start.Add(1)
	results := make([]Binding, racers)
	errs := make([]error, racers)
	for i := range racers {
		done.Add(1)
		go func() {
			defer done.Done()
			start.Wait()
			results[i], errs[i] = bindings.Acquire(context.Background(), bindTenant, bindSession)
		}()
	}
	start.Done()
	done.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("racer %d: %v", i, err)
		}
		if results[i] != results[0] {
			t.Fatalf("racer %d got %+v, want the one binding %+v", i, results[i], results[0])
		}
	}
	if binder.bindCount() != 1 {
		t.Errorf("binds = %d, want exactly one winner", binder.bindCount())
	}
	if resolver.count() != 1 {
		t.Errorf("registry reads = %d, want exactly one", resolver.count())
	}
	if got := bindings.Demand(bindTenant, bindSession); got != racers {
		t.Errorf("demand = %d, want %d", got, racers)
	}
}

// TestAnInvalidatedSessionRedeliversOverTheNewBinding is A4.3 step 2's command
// redelivery. The command is the same public CommandID both times, which is
// the point: a delivery wakes consumption of a durable inbox record, so the
// new owner is told about the record the old one never consumed. Nothing here
// makes the command safe to repeat -- that is the Host's lease and
// SessionStore's idempotency -- but a router that dropped it instead would
// leave an accepted command with nobody to deliver it.
func TestAnInvalidatedSessionRedeliversOverTheNewBinding(t *testing.T) {
	t.Parallel()

	binder := &recordingBinder{}
	resolver := newResolver(observation("host-a", 1, 5))
	bindings := newBindings(t, resolver, binder)
	acquire(t, bindings, bindSession)

	delivery := sessionwire.HostLinkCommandDelivery{CommandID: "cmd-1"}
	if err := bindings.Deliver(context.Background(), bindTenant, bindSession, delivery); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if len(binder.delivered) != 1 || binder.delivered[0].delivery != delivery {
		t.Fatalf("delivered = %+v, want the command once", binder.delivered)
	}

	moved := observation("host-b", 1, 6)
	resolver.put(moved)
	if dropped := bindings.Observe(context.Background(), moved); !dropped {
		t.Fatal("the move did not invalidate the binding")
	}
	if err := bindings.Deliver(context.Background(), bindTenant, bindSession, delivery); err != nil {
		t.Fatalf("redelivery: %v", err)
	}

	if len(binder.delivered) != 2 || binder.delivered[1].delivery != delivery {
		t.Fatalf("delivered = %+v, want the same command redelivered", binder.delivered)
	}
	if binder.bindCount() != 2 || binder.binds[1].HostID != "host-b" || binder.binds[1].LeaseEpoch != 6 {
		t.Fatalf("second bind = %+v, want a bind to the new owner", binder.binds)
	}
	if resolver.count() != 2 {
		t.Errorf("registry reads = %d, want the rebind to re-read the authority", resolver.count())
	}
	if got := bindings.Demand(bindTenant, bindSession); got != 1 {
		t.Errorf("demand = %d, want the subscriber's demand carried across the move", got)
	}
}

// TestADeliveryWithoutDemandIsRefused holds the lifetime rule to one thing.
// A delivery does not open a route, so the demand count is the only reason a
// binding exists and the only reason it survives; a caller delivering to an
// unwatched session takes demand first -- through Demand, never through this
// table directly. This doc used to end "brackets it with Acquire and Release",
// which is the instruction A7.2-sole-demand-holder settled against: see
// Demand.Rebind, and Deliver's own corrected paragraph.
func TestADeliveryWithoutDemandIsRefused(t *testing.T) {
	t.Parallel()

	binder := &recordingBinder{}
	bindings := newBindings(t, newResolver(observation("host-a", 1, 1)), binder)

	err := bindings.Deliver(context.Background(), bindTenant, bindSession, sessionwire.HostLinkCommandDelivery{CommandID: "cmd-1"})
	if !errors.Is(err, ErrNoBinding) {
		t.Fatalf("Deliver = %v, want ErrNoBinding", err)
	}
	if binder.bindCount() != 0 || len(binder.delivered) != 0 {
		t.Errorf("binds=%d delivered=%d, want a delivery to open nothing", binder.bindCount(), len(binder.delivered))
	}
}

// TestAFailedDeliveryKeepsTheBinding states where repair is NOT. A delivery
// failure is a transport fault, and this table cannot tell one that lost a
// connection from one the Host refused; per-binding repair is A7.3's, above
// the transport. Dropping the route here would turn one failure into a rebind
// storm and would still not have delivered the command.
func TestAFailedDeliveryKeepsTheBinding(t *testing.T) {
	t.Parallel()

	failure := errors.New("hostlink: command was not delivered")
	binder := &recordingBinder{deliverErr: failure}
	bindings := newBindings(t, newResolver(observation("host-a", 1, 1)), binder)
	before := acquire(t, bindings, bindSession)

	err := bindings.Deliver(context.Background(), bindTenant, bindSession, sessionwire.HostLinkCommandDelivery{CommandID: "cmd-1"})
	if !errors.Is(err, failure) {
		t.Fatalf("Deliver = %v, want the transport's own error unwrapped", err)
	}
	after, bound := bindings.Binding(bindTenant, bindSession)
	if !bound || after != before {
		t.Fatalf("binding after a failed delivery = %+v (bound=%v), want %+v", after, bound, before)
	}
	if binder.unbindCount() != 0 {
		t.Errorf("unbinds = %d, want a failed delivery to release nothing", binder.unbindCount())
	}
}

// TestOnlyTheLastReleaseUnbinds is A4.3 step 2's local demand count.
func TestOnlyTheLastReleaseUnbinds(t *testing.T) {
	t.Parallel()

	binder := &recordingBinder{}
	bindings := newBindings(t, newResolver(observation("host-a", 4, 9)), binder)
	for range 3 {
		acquire(t, bindings, bindSession)
	}

	for i := range 2 {
		if err := bindings.Release(context.Background(), bindTenant, bindSession); err != nil {
			t.Fatalf("release %d: %v", i, err)
		}
		if binder.unbindCount() != 0 {
			t.Fatalf("unbound while %d subscribers remain", bindings.Demand(bindTenant, bindSession))
		}
	}
	if got := bindings.Demand(bindTenant, bindSession); got != 1 {
		t.Fatalf("demand = %d, want 1", got)
	}
	if err := bindings.Release(context.Background(), bindTenant, bindSession); err != nil {
		t.Fatalf("last release: %v", err)
	}
	if binder.unbindCount() != 1 {
		t.Fatalf("unbinds = %d, want one at the last release", binder.unbindCount())
	}
	if sent := binder.unbinds[0]; sent.HostID != "host-a" || sent.HostGeneration != 4 || sent.LeaseEpoch != 9 {
		t.Errorf("unbind = %+v, want the tuple the binding held", sent)
	}
	if bindings.Len() != 0 || bindings.Demand(bindTenant, bindSession) != 0 {
		t.Errorf("len=%d demand=%d, want the session gone", bindings.Len(), bindings.Demand(bindTenant, bindSession))
	}
}

// TestReleasingDemandNobodyHoldsIsRefused keeps the count from going negative,
// which would make a later release unbind a route another subscriber holds.
func TestReleasingDemandNobodyHoldsIsRefused(t *testing.T) {
	t.Parallel()

	binder := &recordingBinder{}
	bindings := newBindings(t, newResolver(observation("host-a", 1, 1)), binder)

	if err := bindings.Release(context.Background(), bindTenant, bindSession); !errors.Is(err, ErrNoDemand) {
		t.Fatalf("Release = %v, want ErrNoDemand", err)
	}
	acquire(t, bindings, bindSession)
	if err := bindings.Release(context.Background(), bindTenant, bindSession); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if err := bindings.Release(context.Background(), bindTenant, bindSession); !errors.Is(err, ErrNoDemand) {
		t.Fatalf("second Release = %v, want ErrNoDemand", err)
	}
	if binder.unbindCount() != 1 {
		t.Errorf("unbinds = %d, want exactly one", binder.unbindCount())
	}
}

// TestAReleaseThatFailsStillDropsTheLocalRoute is the HostLink pool's rule one
// level up, and it is the same argument: a bind is Factory-local routing state
// the Host validates against its own lease, so a route kept after a failed
// unbind is kept forever -- there is no retry above it -- and the next bind
// would be refused as a conflict against a route nobody wants.
func TestAReleaseThatFailsStillDropsTheLocalRoute(t *testing.T) {
	t.Parallel()

	failure := errors.New("hostlink: pool is closed")
	binder := &recordingBinder{unbindErr: failure}
	bindings := newBindings(t, newResolver(observation("host-a", 1, 1)), binder)
	acquire(t, bindings, bindSession)

	if err := bindings.Release(context.Background(), bindTenant, bindSession); !errors.Is(err, failure) {
		t.Fatalf("Release = %v, want the transport's error reported", err)
	}
	if _, bound := bindings.Binding(bindTenant, bindSession); bound {
		t.Error("the local route survived a failed unbind")
	}
	if bindings.Len() != 0 {
		t.Errorf("Len = %d, want 0", bindings.Len())
	}
}

// TestABindRefusalLeavesNoLocalRouteOrDemand keeps a refused bind from
// counting as demand. A Bindings that recorded the subscriber anyway would
// answer the next Acquire from a route the Host never accepted.
func TestABindRefusalLeavesNoLocalRouteOrDemand(t *testing.T) {
	t.Parallel()

	refusal := errors.New("hostlink: session is bound to another host")
	binder := &recordingBinder{bindErr: refusal}
	bindings := newBindings(t, newResolver(observation("host-a", 1, 1)), binder)

	if _, err := bindings.Acquire(context.Background(), bindTenant, bindSession); !errors.Is(err, refusal) {
		t.Fatalf("Acquire = %v, want the refusal reported", err)
	}
	if bindings.Len() != 0 || bindings.Demand(bindTenant, bindSession) != 0 {
		t.Fatalf("len=%d demand=%d, want nothing recorded", bindings.Len(), bindings.Demand(bindTenant, bindSession))
	}

	binder.bindErr = nil
	acquire(t, bindings, bindSession)
	if bindings.Demand(bindTenant, bindSession) != 1 {
		t.Errorf("demand after a successful retry = %d, want 1", bindings.Demand(bindTenant, bindSession))
	}
}

// TestASessionWithNoOwnerIsRefusedRatherThanPlaced marks the boundary between
// this package and internal/placement. A session with no live registry owner
// needs placement, and nothing here may create one: a router that placed would
// be a second, unclaimed scaling authority beside the reconciler.
func TestASessionWithNoOwnerIsRefusedRatherThanPlaced(t *testing.T) {
	t.Parallel()

	binder := &recordingBinder{}
	bindings := newBindings(t, newResolver(), binder)

	if _, err := bindings.Acquire(context.Background(), bindTenant, bindSession); !errors.Is(err, ErrNoOwner) {
		t.Fatalf("Acquire = %v, want ErrNoOwner", err)
	}
	if binder.bindCount() != 0 {
		t.Errorf("binds = %d, want none", binder.bindCount())
	}
}

// TestAnUnusableOwnerIsRefusedBeforeTheTransport fails closed on a tuple Core
// itself would not carry, so a malformed registry row is a refusal here rather
// than a marshalling error inside the transport with no session named.
func TestAnUnusableOwnerIsRefusedBeforeTheTransport(t *testing.T) {
	t.Parallel()

	broken := observation("host-a", 1, 1)
	broken.LeaseEpoch = 0
	binder := &recordingBinder{}
	bindings := newBindings(t, newResolver(broken), binder)

	_, err := bindings.Acquire(context.Background(), bindTenant, bindSession)
	if err == nil {
		t.Fatal("an owner with no lease epoch was bound")
	}
	if !errors.Is(err, ErrUnroutableOwner) {
		t.Fatalf("Acquire = %v, want ErrUnroutableOwner", err)
	}
	if binder.bindCount() != 0 {
		t.Errorf("binds = %d, want the transport never reached", binder.bindCount())
	}
}

// TestAResolverFailureIsReportedRatherThanGuessed keeps a registry outage from
// looking like "no owner", which a caller would answer by placing a session
// that already has one.
func TestAResolverFailureIsReportedRatherThanGuessed(t *testing.T) {
	t.Parallel()

	outage := errors.New("store is closed")
	resolver := newResolver()
	resolver.err = outage
	bindings := newBindings(t, resolver, &recordingBinder{})

	_, err := bindings.Acquire(context.Background(), bindTenant, bindSession)
	if !errors.Is(err, outage) {
		t.Fatalf("Acquire = %v, want the store's error", err)
	}
	if errors.Is(err, ErrNoOwner) {
		t.Error("a registry failure was reported as an absent owner")
	}
}

// TestCloseUnbindsEveryRouteAndRefusesLaterWork is A4.3 step 2's Factory
// close. Every route is given back to its Host, and the table refuses
// afterwards rather than answering from state a closed replica still holds.
func TestCloseUnbindsEveryRouteAndRefusesLaterWork(t *testing.T) {
	t.Parallel()

	second := observation("host-b", 1, 2)
	second.SessionID = "session-b"
	binder := &recordingBinder{}
	resolver := newResolver(observation("host-a", 1, 1), second)
	bindings := newBindings(t, resolver, binder)
	acquire(t, bindings, bindSession)
	acquire(t, bindings, "session-b")

	if err := bindings.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if binder.unbindCount() != 2 {
		t.Fatalf("unbinds = %d, want one per route", binder.unbindCount())
	}
	if bindings.Len() != 0 {
		t.Errorf("Len = %d, want 0", bindings.Len())
	}
	if err := bindings.Close(context.Background()); err != nil {
		t.Errorf("second Close: %v, want it to be idempotent", err)
	}
	if binder.unbindCount() != 2 {
		t.Errorf("unbinds = %d after a second Close, want no repeat", binder.unbindCount())
	}

	if _, err := bindings.Acquire(context.Background(), bindTenant, bindSession); !errors.Is(err, ErrBindingsClosed) {
		t.Errorf("Acquire after Close = %v, want ErrBindingsClosed", err)
	}
	if err := bindings.Deliver(context.Background(), bindTenant, bindSession, sessionwire.HostLinkCommandDelivery{CommandID: "cmd-1"}); !errors.Is(err, ErrBindingsClosed) {
		t.Errorf("Deliver after Close = %v, want ErrBindingsClosed", err)
	}
	if err := bindings.Release(context.Background(), bindTenant, bindSession); !errors.Is(err, ErrBindingsClosed) {
		t.Errorf("Release after Close = %v, want ErrBindingsClosed", err)
	}
	if dropped := bindings.Observe(context.Background(), observation("host-c", 1, 9)); dropped {
		t.Error("Observe dropped a route after Close")
	}
}

// TestCloseReportsAFailedUnbindAndStillDropsEveryRoute keeps one Host's
// failure from stranding the rest, which a loop that returned on the first
// error would do.
func TestCloseReportsAFailedUnbindAndStillDropsEveryRoute(t *testing.T) {
	t.Parallel()

	second := observation("host-b", 1, 2)
	second.SessionID = "session-b"
	failure := errors.New("hostlink: pool is closed")
	binder := &recordingBinder{unbindErr: failure}
	bindings := newBindings(t, newResolver(observation("host-a", 1, 1), second), binder)
	acquire(t, bindings, bindSession)
	acquire(t, bindings, "session-b")

	err := bindings.Close(context.Background())
	if !errors.Is(err, failure) {
		t.Fatalf("Close = %v, want the unbind failure reported", err)
	}
	if binder.unbindCount() != 2 {
		t.Errorf("unbinds = %d, want every route attempted", binder.unbindCount())
	}
	if bindings.Len() != 0 {
		t.Errorf("Len = %d, want every local route dropped", bindings.Len())
	}
}

// TestARestartHoldsNoRouteAndRebuildsFromTheRegistryAndDemand is A4.3 step 3.
// A restart is modelled as what it is -- a new value over the same durable
// store -- and the assertion is that the new one knows nothing until a
// subscriber asks and the registry answers.
func TestARestartHoldsNoRouteAndRebuildsFromTheRegistryAndDemand(t *testing.T) {
	t.Parallel()

	binder := &recordingBinder{}
	resolver := newResolver(observation("host-a", 1, 1))
	before := newBindings(t, resolver, binder)
	acquire(t, before, bindSession)
	if err := before.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	after := newBindings(t, resolver, binder)
	if after.Len() != 0 || after.Demand(bindTenant, bindSession) != 0 {
		t.Fatalf("len=%d demand=%d, want a restarted replica to hold nothing", after.Len(), after.Demand(bindTenant, bindSession))
	}
	if _, bound := after.Binding(bindTenant, bindSession); bound {
		t.Fatal("a restarted replica answered from a route it never made")
	}
	readsBefore := resolver.count()
	acquire(t, after, bindSession)
	if resolver.count() != readsBefore+1 {
		t.Errorf("registry reads = %d, want the rebuild to read the registry", resolver.count()-readsBefore)
	}
	if after.Demand(bindTenant, bindSession) != 1 {
		t.Errorf("demand = %d, want it rebuilt from the subscriber", after.Demand(bindTenant, bindSession))
	}
}

// TestTheBindKeyIsRetryStableAndDistinguishesEveryTuple holds the two
// properties Core asks of an idempotency key. Stability is what makes a
// repeated bind a repeat rather than a new one; the collision case is the
// lesson internal/placement's desired key paid for -- concatenation is not
// injective when a member can contain the delimiter or vary in length, and an
// identifier here is arbitrary UTF-8 up to 256 bytes.
func TestTheBindKeyIsRetryStableAndDistinguishesEveryTuple(t *testing.T) {
	t.Parallel()

	base := BindingKey{TenantID: "a", SessionID: "bc", HostID: "h", HostGeneration: 1, LeaseEpoch: 1}
	first := bindIdempotencyKey(base)
	if repeated := bindIdempotencyKey(base); repeated != first {
		t.Errorf("the same tuple derived two keys, %q and %q", first, repeated)
	}
	if err := (sessionwire.HostLinkBindRequest{
		Version: sessionwire.CurrentWireVersion, TenantID: "a", SessionID: "bc", HostID: "h",
		HostGeneration: 1, LeaseEpoch: 1, RuntimeCompatibilityID: bindRuntime, IdempotencyKey: first,
	}).Validate(); err != nil {
		t.Errorf("the derived key is not one Core will carry: %v", err)
	}

	seen := map[string]BindingKey{first: base}
	for _, other := range []BindingKey{
		{TenantID: "ab", SessionID: "c", HostID: "h", HostGeneration: 1, LeaseEpoch: 1},
		{TenantID: "a", SessionID: "bc", HostID: "h", HostGeneration: 11, LeaseEpoch: 1},
		{TenantID: "a", SessionID: "bc", HostID: "h", HostGeneration: 1, LeaseEpoch: 11},
		{TenantID: "a", SessionID: "b", HostID: "ch", HostGeneration: 1, LeaseEpoch: 1},
		{TenantID: "", SessionID: "abc", HostID: "h", HostGeneration: 1, LeaseEpoch: 1},
	} {
		key := bindIdempotencyKey(other)
		if had, ok := seen[key]; ok {
			t.Errorf("%+v and %+v share the key %q", had, other, key)
		}
		seen[key] = other
	}
}

// TestTheUnbindKeyIsTheBindKey keeps a Host's two records of one route under
// one identity: the unbind Core carries must name the bind it removes.
func TestTheUnbindKeyIsTheBindKey(t *testing.T) {
	t.Parallel()

	binder := &recordingBinder{}
	bindings := newBindings(t, newResolver(observation("host-a", 2, 3)), binder)
	acquire(t, bindings, bindSession)
	if err := bindings.Release(context.Background(), bindTenant, bindSession); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if binder.binds[0].IdempotencyKey != binder.unbinds[0].IdempotencyKey {
		t.Errorf("bind key %q and unbind key %q name two routes",
			binder.binds[0].IdempotencyKey, binder.unbinds[0].IdempotencyKey)
	}
}

// TestNewBindingsRefusesAnIncompleteComposition checks the seams before any
// call, so a missing one is a composition error rather than a nil dereference
// on the first subscriber.
func TestNewBindingsRefusesAnIncompleteComposition(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		resolver Resolver
		binder   Binder
		want     string
	}{
		{name: "no resolver", binder: &recordingBinder{}, want: "Resolver"},
		{name: "no binder", resolver: newResolver(), want: "Binder"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewBindings(tc.resolver, tc.binder)
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("NewBindings = %v, want ErrInvalidConfig", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not name the missing seam %q", err, tc.want)
			}
		})
	}
	if _, err := NewBindings(newResolver(), &recordingBinder{}); err != nil {
		t.Errorf("a complete composition was refused: %v", err)
	}
}

// TestTheDirectoryIsTheResolver drives the seam with the production reader
// over a real store rather than asserting assignability alone: a compile-time
// var _ Resolver = (*Directory)(nil) would pass while Owner answered nothing
// this package can bind. It is also where the "no live owner" case is shown to
// be the store's real answer for a session nobody owns, not the fake's.
func TestTheDirectoryIsTheResolver(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clock := &movableClock{now: directoryNow}
	store, err := sessionstore.Open(ctx, memstore.New(), sessionstore.WithClock(clock))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(ctx); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	directory, err := NewDirectory(store, DefaultLimits())
	if err != nil {
		t.Fatalf("NewDirectory: %v", err)
	}
	binder := &recordingBinder{}
	bindings := newBindings(t, directory, binder)

	if _, err := bindings.Acquire(ctx, bindTenant, bindSession); !errors.Is(err, ErrNoOwner) {
		t.Fatalf("Acquire with no registration = %v, want ErrNoOwner", err)
	}

	if _, err := store.PutHostRegistration(ctx, sessionstore.PutHostRegistrationRequest{
		TenantID: bindTenant, SessionID: bindSession, LeaseEpoch: 12,
		ObservedAt: clock.now, ExpiresAt: clock.now.Add(time.Minute),
		Route: sessionstore.HostRoute{
			HostID: "host-a", HostGeneration: 4, AgentID: bindAgent, RuntimeCompatibilityID: bindRuntime,
			Placement: sessionwire.HostPlacementPooled, InternalEndpoint: "wss://host-a.internal/hostlink",
			Residency: sessionwire.SessionResidencyResident, Accepting: true,
		},
	}); err != nil {
		t.Fatalf("PutHostRegistration: %v", err)
	}

	binding, err := bindings.Acquire(ctx, bindTenant, bindSession)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	want := BindingKey{TenantID: bindTenant, SessionID: bindSession, HostID: "host-a", HostGeneration: 4, LeaseEpoch: 12}
	if binding.Key != want {
		t.Fatalf("key = %+v, want the registered tuple %+v", binding.Key, want)
	}
	if binder.binds[0].LeaseEpoch != 12 {
		t.Errorf("bind epoch = %d, want the registry's", binder.binds[0].LeaseEpoch)
	}
}
