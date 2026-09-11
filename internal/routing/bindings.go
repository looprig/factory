package routing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"strconv"
	"sync"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// ErrBindingsClosed reports work asked of a closed table. A closed replica may
// not answer from routes it is giving up.
var ErrBindingsClosed = errors.New("routing: bindings are closed")

// ErrNoOwner reports a session with no live registry owner. It is not a
// failure of this package and not something it may fix: a session needing an
// owner needs PLACEMENT, and a router that created one would be a second,
// unclaimed scaling authority beside internal/placement's reconciler.
var ErrNoOwner = errors.New("routing: session has no live owner")

// ErrUnroutableOwner reports a registry owner whose ownership tuple Core will
// not carry. It fails here, naming the session, rather than inside the
// transport as a marshalling error naming nothing.
var ErrUnroutableOwner = errors.New("routing: registry owner cannot be bound")

// ErrNoBinding reports a delivery for a session this replica holds no demand
// for. A delivery does not open a route; see Bindings.Deliver.
var ErrNoBinding = errors.New("routing: no local binding for session")

// ErrNoDemand reports a release of demand nobody holds.
var ErrNoDemand = errors.New("routing: no local demand for session")

// Resolver reads the authoritative owner of a session. *Directory implements
// it, and that is the only implementation this module has: ownership comes
// from the epoch-fenced registry and from nowhere else.
type Resolver interface {
	Owner(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) (sessionwire.HostLinkRegistryObservation, bool, error)
}

// Binder carries what this table decides. It is the HostLink pool's control
// surface stated in Core's vocabulary: nothing here names a transport type,
// because a binding is routing state that outlives any particular transport
// and because naming one would put the centrifuge dependency in this package's
// graph, where the boundary rules deliberately do not have it.
//
// The pool's Bind takes its target as a struct; the endpoint is passed
// separately here so this seam names only Core. Composing the two is one
// adapter, and it is A9.1's along with the rest of the composition.
type Binder interface {
	Bind(ctx context.Context, endpoint sessionwire.InternalEndpoint, req sessionwire.HostLinkBindRequest) error
	Unbind(ctx context.Context, req sessionwire.HostLinkUnbindRequest) error
	DeliverCommand(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, delivery sessionwire.HostLinkCommandDelivery) error
}

// BindingKey identifies one Factory-local route, and identity is the WHOLE
// ownership tuple rather than the session. A4.3 step 1.
//
// Each member earns its place by naming a way the route can become wrong
// without the session changing:
//
//   - TenantID, because a SessionID is unique only within a tenant. A table
//     keyed by session alone lets one tenant's route answer for another's.
//   - HostID, because a session that moved is served by a different link.
//   - HostGeneration, because a restarted Host has none of the state the old
//     route assumed, and answers as a different Host.
//   - LeaseEpoch, because the epoch is the fence: a bind naming a superseded
//     epoch is exactly what the owning Host refuses.
//
// So "invalidate the binding" and "the key changed" are one fact, and there is
// no second staleness rule that could disagree with the key.
type BindingKey struct {
	TenantID       sessionwire.TenantID
	SessionID      sessionwire.SessionID
	HostID         sessionwire.HostID
	HostGeneration uint64
	LeaseEpoch     uint64
}

// Binding is one live local route. It is comparable, so a caller can hold one
// and ask whether what it holds is still what the table has.
type Binding struct {
	Key                    BindingKey
	Endpoint               sessionwire.InternalEndpoint
	RuntimeCompatibilityID string
}

// Bindings is this replica's local routing table: which Host each session it
// is serving is bound to, and how many local subscribers want it.
//
// It is LOCAL STATE ONLY (A4.3 step 3). Nothing here is written durably and
// nothing here is coordinated with another replica; a restart is a new value
// that knows nothing, and it rebuilds from exactly two sources — SessionStore,
// through the Resolver, and subscriber demand, through Acquire. That is why
// there is no reconstruction path to write: reconstruction is what the
// ordinary path already does.
//
// A binding is a routing hint and never authority. The Host validates the
// tuple against its own durable lease, so the failure this table cannot have
// is delivering to a Host that will accept a route it should not — that
// failure is not available to it.
//
// One stated limit, so it is not discovered later: invalidation is driven by
// an observation arriving at Observe. A lease that changed with nobody pushing
// an observation leaves a stale route in place until the owning Host refuses
// what it carries. That is the same fail-closed shape as everything else here
// — the Host refuses — but it is a refusal, not a repair. The repair is
// Relay's, in repair.go: it learns from BELOW that a route is unusable and asks
// Demand.Rebind for a fresh one rather than waiting for an ownership poll.
type Bindings struct {
	resolver Resolver
	binder   Binder

	// mu guards everything below, INCLUDING across the registry read and the
	// bind. Holding it across the I/O is what makes thirty-two subscribers
	// arriving at once for one session cost one read and one bind rather than
	// thirty-two of which thirty-one are discarded — the same trade the
	// HostLink pool makes across its dial. The cost is stated rather than
	// hidden: a slow registry read delays work on an unrelated session, and
	// the bound on it is the caller's context. Sharding the table by session
	// is a composition decision A9.1 can take if the cost is measured to
	// matter; it cannot be taken here, where nothing runs.
	mu       sync.Mutex
	closed   bool
	sessions map[sessionKey]*sessionRoute
}

type sessionKey struct {
	tenant  sessionwire.TenantID
	session sessionwire.SessionID
}

// sessionRoute is a session's local demand and, when it has one, its binding.
// The two are separate because they have different lifetimes: demand survives
// an invalidation (the subscriber still wants the session) and is what makes
// the next call rebind rather than give up.
type sessionRoute struct {
	demand  int
	binding Binding
	bound   bool
}

// NewBindings validates the composition before any call, so a missing seam is
// a composition error rather than a nil dereference on the first subscriber.
func NewBindings(resolver Resolver, binder Binder) (*Bindings, error) {
	if resolver == nil {
		return nil, fmt.Errorf("%w: Resolver must not be nil", ErrInvalidConfig)
	}
	if binder == nil {
		return nil, fmt.Errorf("%w: Binder must not be nil", ErrInvalidConfig)
	}
	return &Bindings{resolver: resolver, binder: binder, sessions: map[sessionKey]*sessionRoute{}}, nil
}

// Acquire records one local subscriber's demand for a session and returns the
// route it should use, binding to the session's owner if this replica does not
// already hold one.
//
// A binding already held is REUSED and costs no registry read (A4.3 step 2).
// That is what makes a per-session subscription cheap, and it is also why
// invalidation is a push: the route is not re-derived on use, so something has
// to say it is no longer current.
func (b *Bindings) Acquire(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) (Binding, error) {
	key := sessionKey{tenant: tenant, session: session}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return Binding{}, ErrBindingsClosed
	}
	route, err := b.routeLocked(ctx, key)
	if err != nil {
		return Binding{}, err
	}
	route.demand++
	return route.binding, nil
}

// Release gives back one subscriber's demand. The LAST release unbinds: the
// route exists for the demand, so a route outliving it would hold a HostLink
// open for nobody.
//
// A failed unbind still drops the local route, and the error is reported
// rather than acted on. The reason is the HostLink pool's, one level up: a
// bind is Factory-local state the Host validates against its own lease, so a
// route kept after a failure is kept forever — nothing above retries it — and
// the next bind would be refused as a conflict against a route nobody wants.
func (b *Bindings) Release(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) error {
	key := sessionKey{tenant: tenant, session: session}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrBindingsClosed
	}
	route := b.sessions[key]
	if route == nil || route.demand == 0 {
		return ErrNoDemand
	}
	route.demand--
	if route.demand > 0 {
		return nil
	}
	delete(b.sessions, key)
	if !route.bound {
		return nil
	}
	return b.binder.Unbind(ctx, unbindRequest(route.binding))
}

// Deliver wakes consumption of an already accepted command on this session's
// binding.
//
// It requires demand and does not open a route (ErrNoBinding otherwise), which
// keeps the demand count the ONLY rule about a binding's lifetime. A caller
// delivering to a session nobody is watching must take demand FIRST -- and it
// must take it through Demand, not here.
//
// THAT IS A CORRECTION, and the earlier wording is the hazard A7.3 settled.
// This paragraph used to say "brackets the delivery with Acquire and Release",
// which instructs a caller to become a SECOND holder of this table's demand.
// Release only unbinds on the last release, so while a second holder exists a
// release decrements without unbinding, route.bound stays true, and
// routeLocked hands the next caller a route nobody re-read from the registry.
// The invariant is one holder per session and it is Demand; see Demand.Rebind
// for the whole argument and TestTheRoutingTablesDemandIsHeldOnlyByTheDemandPlane
// for the guard. The pool's idle window is still what keeps a bracketed
// delivery from costing a dial per command.
//
// A session whose binding was invalidated is rebound here first, so the
// command is REDELIVERED to the new owner (A4.3 step 2) rather than dropped
// with the durable inbox record left for nobody. Nothing here makes a command
// safe to repeat — that is the Host's lease and SessionStore's idempotency,
// and the delivery carries only the retry-stable public CommandID.
//
// A delivery FAILURE is reported unchanged and changes nothing. This table
// cannot tell a lost connection from a Host's refusal, and dropping the route
// on either would turn one failure into a rebind storm without having
// delivered anything. Per-binding repair is Relay's, above the transport.
func (b *Bindings) Deliver(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, delivery sessionwire.HostLinkCommandDelivery) error {
	key := sessionKey{tenant: tenant, session: session}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrBindingsClosed
	}
	if route := b.sessions[key]; route == nil || route.demand == 0 {
		return ErrNoBinding
	}
	route, err := b.routeLocked(ctx, key)
	if err != nil {
		return err
	}
	return b.binder.DeliverCommand(ctx, route.binding.Key.TenantID, route.binding.Key.SessionID, delivery)
}

// Observe applies a registry observation to the table and reports whether it
// invalidated a binding.
//
// It DROPS the route rather than rebinding from the observation, and never
// creates one. An observation is a hint and the registry is the authority, so
// the next caller re-reads it; what the push is trusted for is only "what you
// hold is no longer current", which needs no trust at all. Subscriber demand
// survives, so the rebind happens on the next Acquire or Deliver.
//
// An observation naming an OLDER lease epoch than the binding holds is stale
// and ignored. An observation at the same epoch naming a different Host or
// generation is not stale, it is a contradiction, and it invalidates: keeping
// a route the registry contradicts is how a command reaches a Host that is not
// the owner.
//
// The unbind is best effort and its error is deliberately not reported. There
// is no answer a caller could act on — the local route is gone either way, for
// the reason Release states — and Observe's own answer is about this table.
func (b *Bindings) Observe(ctx context.Context, observed sessionwire.HostLinkRegistryObservation) bool {
	key := sessionKey{tenant: observed.TenantID, session: observed.SessionID}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return false
	}
	route := b.sessions[key]
	if route == nil || !route.bound {
		return false
	}
	held := route.binding.Key
	if observed.LeaseEpoch < held.LeaseEpoch {
		return false
	}
	if observed.HostID == held.HostID && observed.HostGeneration == held.HostGeneration && observed.LeaseEpoch == held.LeaseEpoch {
		return false
	}
	dropped := route.binding
	route.binding, route.bound = Binding{}, false
	_ = b.binder.Unbind(ctx, unbindRequest(dropped))
	return true
}

// Binding reports the route this replica currently holds for a session.
func (b *Bindings) Binding(tenant sessionwire.TenantID, session sessionwire.SessionID) (Binding, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	route := b.sessions[sessionKey{tenant: tenant, session: session}]
	if route == nil || !route.bound {
		return Binding{}, false
	}
	return route.binding, true
}

// Demand reports how many local subscribers hold a session.
func (b *Bindings) Demand(tenant sessionwire.TenantID, session sessionwire.SessionID) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	route := b.sessions[sessionKey{tenant: tenant, session: session}]
	if route == nil {
		return 0
	}
	return route.demand
}

// Len reports how many sessions this replica is routing.
func (b *Bindings) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.sessions)
}

// Close gives every route back to its Host and refuses later work.
//
// Every route is attempted and every failure is joined, rather than returning
// at the first: one Host's failure must not strand the routes on the others,
// which are the ones a restarted replica would then conflict with.
//
// There is no `if b.closed { return nil }` fast path, and its absence is
// deliberate: idempotence comes from EMPTYING the table, and a mutant that
// deleted such a guard survived the whole suite for exactly that reason. The
// HostLink pool removed the same redundant guard on the same evidence. A
// second Close unbinds nothing because there is nothing left to unbind.
func (b *Bindings) Close(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	var failures []error
	for key, route := range b.sessions {
		delete(b.sessions, key)
		if !route.bound {
			continue
		}
		if err := b.binder.Unbind(ctx, unbindRequest(route.binding)); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

// routeLocked returns the session's route, binding it if this replica holds
// none. A route that fails to bind leaves no demand behind unless a subscriber
// already held some, so a refused first Acquire is as though it never
// happened and a retry binds.
func (b *Bindings) routeLocked(ctx context.Context, key sessionKey) (*sessionRoute, error) {
	route := b.sessions[key]
	if route != nil && route.bound {
		return route, nil
	}
	binding, err := b.bindLocked(ctx, key)
	if err != nil {
		return nil, err
	}
	if route == nil {
		route = &sessionRoute{}
		b.sessions[key] = route
	}
	route.binding, route.bound = binding, true
	return route, nil
}

// bindLocked reads the session's owner and binds to it.
func (b *Bindings) bindLocked(ctx context.Context, key sessionKey) (Binding, error) {
	observed, found, err := b.resolver.Owner(ctx, key.tenant, key.session)
	if err != nil {
		// Reported, never folded into ErrNoOwner: a registry outage answered
		// as "no owner" is an outage a caller responds to by placing a session
		// that already has one.
		return Binding{}, err
	}
	if !found {
		return Binding{}, ErrNoOwner
	}
	binding := Binding{
		Key: BindingKey{
			TenantID: key.tenant, SessionID: key.session, HostID: observed.HostID,
			HostGeneration: observed.HostGeneration, LeaseEpoch: observed.LeaseEpoch,
		},
		Endpoint:               observed.InternalEndpoint,
		RuntimeCompatibilityID: observed.RuntimeCompatibilityID,
	}
	req := bindRequest(binding)
	if err := req.Validate(); err != nil {
		return Binding{}, fmt.Errorf("%w: %w", ErrUnroutableOwner, err)
	}
	if err := b.binder.Bind(ctx, binding.Endpoint, req); err != nil {
		return Binding{}, err
	}
	return binding, nil
}

func bindRequest(binding Binding) sessionwire.HostLinkBindRequest {
	return sessionwire.HostLinkBindRequest{
		Version:                sessionwire.CurrentWireVersion,
		TenantID:               binding.Key.TenantID,
		SessionID:              binding.Key.SessionID,
		HostID:                 binding.Key.HostID,
		HostGeneration:         binding.Key.HostGeneration,
		LeaseEpoch:             binding.Key.LeaseEpoch,
		RuntimeCompatibilityID: binding.RuntimeCompatibilityID,
		IdempotencyKey:         bindIdempotencyKey(binding.Key),
	}
}

// unbindRequest removes exactly the bind that created the route: the same
// tuple and the same key, so a Host's two records of one route cannot name two
// different things.
func unbindRequest(binding Binding) sessionwire.HostLinkUnbindRequest {
	return sessionwire.HostLinkUnbindRequest{
		Version:        sessionwire.CurrentWireVersion,
		TenantID:       binding.Key.TenantID,
		SessionID:      binding.Key.SessionID,
		HostID:         binding.Key.HostID,
		HostGeneration: binding.Key.HostGeneration,
		LeaseEpoch:     binding.Key.LeaseEpoch,
		IdempotencyKey: bindIdempotencyKey(binding.Key),
	}
}

// bindIdempotencyKey derives Core's retry-stable bind key from the tuple the
// bind names. Deriving it rather than minting one is what makes a repeated
// bind a repeat: two attempts at the same route agree without coordinating,
// and a bind for a different tuple cannot look like one of them.
//
// The tuple is FRAMED before hashing, and that is not decoration. A
// sessionwire identifier is arbitrary UTF-8 up to MaxIDBytes, so concatenation
// is not injective — tenant "a" with session "bc" and tenant "ab" with session
// "c" are one string — and internal/placement paid for this lesson on its
// desired-state key, where a collision is a stale generation SessionStore
// absorbs as a replay. Each field is written as its decimal byte length, a
// NUL, then its bytes: the length prefix holds only ASCII digits, NUL cannot
// appear among them, and a field's content is read only after its length is
// known. No integer conversion appears anywhere in it.
func bindIdempotencyKey(key BindingKey) string {
	digest := sha256.New()
	frame(digest, "hostbind/v1")
	frame(digest, string(key.TenantID))
	frame(digest, string(key.SessionID))
	frame(digest, string(key.HostID))
	frame(digest, strconv.FormatUint(key.HostGeneration, 10))
	frame(digest, strconv.FormatUint(key.LeaseEpoch, 10))
	return hex.EncodeToString(digest.Sum(nil))
}

func frame(digest hash.Hash, field string) {
	digest.Write([]byte(strconv.Itoa(len(field))))
	digest.Write([]byte{0})
	digest.Write([]byte(field))
}
