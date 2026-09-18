package hostlink

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// ErrInvalidConfig is the class of every NewPool rejection.
var ErrInvalidConfig = errors.New("hostlink: invalid configuration")

// ErrPoolClosed reports work asked of a pool that has been shut down.
var ErrPoolClosed = errors.New("hostlink: pool is closed")

// ErrLinkLimit reports a bind that would open a link past MaxLinks.
var ErrLinkLimit = errors.New("hostlink: link limit reached")

// ErrTargetMismatch reports a bind record naming a Host other than the one the
// link goes to.
var ErrTargetMismatch = errors.New("hostlink: request names a different host")

// ErrBindingConflict reports a session already bound to a different Host.
var ErrBindingConflict = errors.New("hostlink: session is bound to another host")

// ErrUnknownBinding reports work for a session this pool holds no route for.
var ErrUnknownBinding = errors.New("hostlink: no binding for session")

// ErrUnsupportedMethod reports a reserved HostLink operation the connected
// Host did not advertise. It is a local refusal: no RPC is sent to a peer
// that did not promise to handle the method.
var ErrUnsupportedMethod = errors.New("hostlink: host did not advertise the requested method")

// ErrLinkReconnecting reports a reserved HostLink operation refused locally
// because the link is between a dropped connection and the next Host reply,
// so there is no capability set to admit it against. It is a TRANSIENT,
// deliberately distinct from ErrUnsupportedMethod: the Host may well advertise
// the method, and a caller that read "did not advertise" during a 250ms
// reconnect would remake a placement decision over a perfectly good Host.
// Retry after the reconnect settles; session-channel delivery is not gated
// and is queued by the transport instead.
var ErrLinkReconnecting = errors.New("hostlink: link is reconnecting")

// UnsupportedMethodError identifies the reserved operation refused locally
// because the negotiated HostLink capability set did not contain it.
type UnsupportedMethodError struct {
	Method string
}

func (e *UnsupportedMethodError) Error() string {
	return fmt.Sprintf("%s: %s", ErrUnsupportedMethod, e.Method)
}

func (e *UnsupportedMethodError) Unwrap() error { return ErrUnsupportedMethod }

// ErrMalformedAttachReply reports an attach reply that is neither of the two
// records Core allows: an observation or a HostLinkError. An EMPTY reply is one
// of these -- an accepted attach must say at which lease epoch the session is
// now resident, and a caller told nothing has no epoch it may bind with.
var ErrMalformedAttachReply = errors.New("hostlink: attach reply is not a Core record")

// ErrAttachMismatch reports an accepted attach whose observation does not name
// the session, the Host or the Host incarnation the request did. The pool
// refuses to hand it to a caller, because the caller's next act is to bind
// with its epoch, and a bind built from an observation of something else is a
// route to nowhere that the Host will refuse for a reason reading as a lease
// problem.
var ErrAttachMismatch = errors.New("hostlink: attach observation does not match the request")

// ErrCommandUndelivered reports that a committed command was NOT handed to a
// Host.
//
// It is the distinction runbook A7.1 step 3 turns on. The inbox record is
// already committed when DeliverCommand runs, so the caller's next decision is
// whether the command is finished. A transport failure means the Host has never
// seen it and the record stays PENDING for a later attempt; a Host refusal --
// reported as *HostRefusal, and deliberately NOT wrapped in this -- means the
// Host answered and the caller may act on that answer. Collapsing the two would
// either terminate a command nobody received or retry one that was refused.
var ErrCommandUndelivered = errors.New("hostlink: command was not delivered")

// HostRefusal is a Host's own answer to a control request.
//
// It carries Core's typed record rather than a message, because a caller must
// branch on the reason: an epoch mismatch is a stale route to re-resolve and a
// runtime mismatch is a placement decision to remake, and telling them apart by
// matching text is how a caller silently starts doing the wrong one.
//
// The record is EMBEDDED rather than copied field by field. Core's HostLinkError
// already states which detail belongs with which code -- an epoch only with a
// mismatch, a runtime id only with a runtime mismatch -- and a second flat copy
// of those three fields would be a second authority for that rule, which is the
// shape of defect this module has had to correct twice.
type HostRefusal struct {
	sessionwire.HostLinkError
}

func (e *HostRefusal) Error() string {
	return fmt.Sprintf("hostlink: host refused the request: %s", e.Code)
}

// Limits bounds one replica's HostLink pool.
//
// It restates factory.HostLinkLimits for the package that consumes it, in the
// same way clientlink.Limits restates ClientLinkLimits. A9.1 converts;
// TestHostLinkLimitsAreCarriedWhole in the root package holds the two shapes in
// step by name and type, so a field added to one and not the other, or renamed
// on one side, fails there rather than configuring a zero.
type Limits struct {
	// MaxLinks bounds concurrent HostLinks. One link multiplexes every session
	// binding to one Host, so this bounds Hosts, not sessions.
	MaxLinks int
	// DialTimeout bounds one dial.
	DialTimeout time.Duration
	// IdleTimeout is how long a link with no local demand is kept.
	IdleTimeout time.Duration
	// ReconnectMin and ReconnectMax bound the reconnect backoff.
	ReconnectMin time.Duration
	ReconnectMax time.Duration
}

// DefaultLimits is what a pool composed with a zero Limits gets.
//
// It restates factory.DefaultHostLinkLimits, held equal by
// TestTheTwoHostLinkDefaultsAreOneTable in the root package. A zero Limits is
// filled rather than rejected because the pool is composed from inside this
// module as well as from a deployer's options, and an in-module caller that had
// to restate five durations to open a connection would restate them wrongly.
func DefaultLimits() Limits {
	return Limits{
		MaxLinks:     256,
		DialTimeout:  5 * time.Second,
		IdleTimeout:  60 * time.Second,
		ReconnectMin: 250 * time.Millisecond,
		ReconnectMax: 10 * time.Second,
	}
}

// Validate reports why these limits may not be used.
//
// It is a SECOND check rather than a redundant one, for the reason
// clientlink.Limits.Validate is: the root package validates what a deployer
// supplied, and this validates what reached the engine, which is also what a
// direct in-module caller supplies.
func (l Limits) Validate() error {
	if l.MaxLinks < 1 {
		return fmt.Errorf("%w: MaxLinks is %d, want at least 1", ErrInvalidConfig, l.MaxLinks)
	}
	if l.DialTimeout <= 0 {
		return fmt.Errorf("%w: DialTimeout is %v, want a positive duration", ErrInvalidConfig, l.DialTimeout)
	}
	if l.IdleTimeout <= 0 {
		return fmt.Errorf("%w: IdleTimeout is %v, want a positive duration", ErrInvalidConfig, l.IdleTimeout)
	}
	if l.ReconnectMin <= 0 {
		return fmt.Errorf("%w: ReconnectMin is %v, want a positive duration", ErrInvalidConfig, l.ReconnectMin)
	}
	if l.ReconnectMax <= 0 {
		return fmt.Errorf("%w: ReconnectMax is %v, want a positive duration", ErrInvalidConfig, l.ReconnectMax)
	}
	if l.ReconnectMin > l.ReconnectMax {
		return fmt.Errorf("%w: ReconnectMin (%v) must not exceed ReconnectMax (%v)",
			ErrInvalidConfig, l.ReconnectMin, l.ReconnectMax)
	}
	// A link is opened on local subscriber demand. If a dial may take longer
	// than the idle window, a link can become reapable before it has served the
	// demand that opened it.
	if l.DialTimeout > l.IdleTimeout {
		return fmt.Errorf("%w: DialTimeout (%v) must not exceed IdleTimeout (%v)",
			ErrInvalidConfig, l.DialTimeout, l.IdleTimeout)
	}
	return nil
}

// Config composes the pool.
type Config struct {
	// Dialer opens one connection. Required.
	Dialer Dialer
	// Observer receives capacity and registry pushes. Optional; observations
	// are discarded when it is nil.
	Observer Observer
	// Limits bounds this replica's pool. A zero value takes DefaultLimits.
	Limits Limits
	// Now is the pool's clock, for the idle reaper. A nil value takes
	// time.Now. It is a seam so the reaper's boundary can be measured at an
	// exact instant rather than waited for.
	Now func() time.Time
}

// Pool holds at most one physical HostLink per Host.
//
// The invariant it exists for is in one sentence: a session binding never costs
// a connection, and a connection is never shared between Hosts. Everything else
// here -- the ceiling, the idle window, the conflict refusal -- protects that
// sentence against the ways a caller can accidentally violate it.
//
// Nothing in this type coordinates with another Factory replica. Runbook A7.1
// step 2 is a decision, not an omission: several replicas each hold their own
// connection to one Host, and the Host reconciles what it is told against its
// own durable leases. There is no broker and no leader to lose.
type Pool struct {
	dialer   Dialer
	observer Observer
	limits   Limits
	now      func() time.Time

	// mu guards everything below, INCLUDING across the dial. A dial holds the
	// lock so that thirty-two subscribers arriving at once for one Host open
	// one connection rather than thirty-two of which thirty-one are discarded.
	// The cost is that a slow dial to one Host delays a bind to another; that
	// is bounded by DialTimeout, which the dialer applies, and it is the
	// cheaper of the two failures.
	//
	// It is ALSO held across the link's Bind and Unbind RPCs (not across
	// DeliverCommand), and that is deliberate rather than an oversight, though
	// it has the same cost: one slow Host stalls every pool operation for as
	// long as the caller's context allows. It is not narrowed here because the
	// lock is what orders two operations on ONE session: released, a Bind to
	// Host B and an Unbind from Host A for the same session could pass the
	// conflict check together and reach the two Hosts in either order, leaving
	// A holding a route this pool no longer tracks and can never unbind --
	// the exact state the conflict refusal exists to prevent. Narrowing it
	// needs a per-session in-flight marker plus a re-check for close and reap
	// after the RPC, which is a design change, not a lock move. What made the
	// width dangerous rather than slow -- a link wedged forever inside an RPC
	// pinning this lock -- is gone: the link never blocks a caller past its
	// context or the next reconnect, so this lock is held for a bounded time.
	mu     sync.Mutex
	closed bool
	links  map[sessionwire.HostID]*pooledLink
	// routes maps a tenant-scoped session to the Host it is bound to. It is
	// keyed by BOTH ids: a session id is unique within a tenant, and a map
	// keyed by session alone would let one tenant's binding answer for
	// another's.
	routes map[routeKey]sessionwire.HostID
}

type routeKey struct {
	tenant  sessionwire.TenantID
	session sessionwire.SessionID
}

type pooledLink struct {
	link     Link
	bindings map[routeKey]struct{}
	// idleSince is when the last binding was released. It is the zero time
	// while the link has bindings, so "is it idle" and "how long for" are one
	// question with one answer rather than a flag and a timestamp that can
	// disagree.
	idleSince time.Time
}

// NewPool validates a composition and returns it.
func NewPool(cfg Config) (*Pool, error) {
	if cfg.Dialer == nil {
		return nil, fmt.Errorf("%w: Dialer is nil", ErrInvalidConfig)
	}
	limits := cfg.Limits
	if limits == (Limits{}) {
		limits = DefaultLimits()
	}
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	observer := cfg.Observer
	if observer == nil {
		observer = discardObserver{}
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Pool{
		dialer:   cfg.Dialer,
		observer: observer,
		limits:   limits,
		now:      now,
		links:    map[sessionwire.HostID]*pooledLink{},
		routes:   map[routeKey]sessionwire.HostID{},
	}, nil
}

// Limits returns the limits this pool was composed with, defaults included.
func (p *Pool) Limits() Limits { return p.limits }

// Links reports the physical connections this pool currently holds.
func (p *Pool) Links() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.links)
}

// Bindings reports the session routes multiplexed over one Host's link.
func (p *Pool) Bindings(host sessionwire.HostID) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	pooled, ok := p.links[host]
	if !ok {
		return 0
	}
	return len(pooled.bindings)
}

// RouteFor reports which Host a tenant-scoped session is bound to.
//
// It exists because the route table is the pool's actual routing authority and
// nothing else can observe it. Bindings counts entries in a DIFFERENT map, held
// per link, and the two are only kept in step by this file: a route released
// without its binding, or a binding released without its route, satisfies every
// count this type otherwise exposes. The second of those is the more expensive
// one -- an orphaned route names a Host the reaper is then free to collect,
// because the reaper's own test is the binding set -- so the table is made
// observable rather than inferred.
func (p *Pool) RouteFor(tenantID sessionwire.TenantID, sessionID sessionwire.SessionID) (sessionwire.HostID, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	host, ok := p.routes[routeKey{tenant: tenantID, session: sessionID}]
	return host, ok
}

// Bind establishes one session route over the pooled link to target.
//
// Both the target and the record are validated BEFORE anything is dialled, so a
// malformed control record costs neither a TCP connection nor a service
// handshake. The record must name the Host the link goes to: a bind carrying
// another Host's tuple would be refused by the receiving Host for a reason that
// reads as a lease problem rather than as a Factory routing bug.
//
// Re-binding a session to the SAME Host is allowed and resends -- that is how a
// lease epoch is refreshed. Re-binding it to a DIFFERENT Host is refused rather
// than moved: a silent move leaves the first Host holding a route this pool no
// longer tracks and can therefore never unbind. The caller unbinds first.
func (p *Pool) Bind(ctx context.Context, target Target, req sessionwire.HostLinkBindRequest) error {
	if err := target.Validate(); err != nil {
		return err
	}
	if req.HostID != target.Host {
		return fmt.Errorf("%w: request names %q, link goes to %q", ErrTargetMismatch, req.HostID, target.Host)
	}
	if err := req.Validate(); err != nil {
		return err
	}

	key := routeKey{tenant: req.TenantID, session: req.SessionID}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrPoolClosed
	}
	if bound, ok := p.routes[key]; ok && bound != target.Host {
		return fmt.Errorf("%w: session %q is bound to %q", ErrBindingConflict, req.SessionID, bound)
	}

	pooled, err := p.acquireLocked(ctx, target)
	if err != nil {
		return err
	}
	if err := pooled.link.Bind(ctx, req); err != nil {
		// The BINDING is not recorded, because the Host does not have it. The
		// LINK is kept: the Host answered, so the connection is good, and
		// discarding it would turn one lease disagreement into a dial storm.
		return err
	}
	pooled.bindings[key] = struct{}{}
	// idleSince is deliberately NOT cleared here. It is read only when the
	// binding set is empty, and Unbind is the only path that empties it, so a
	// reset on bind is unreachable -- a mutation that deleted it survived every
	// case, which is how it was found rather than argued.
	p.routes[key] = target.Host
	return nil
}

// Attach asks target to make one session resident and returns the Host's
// observation of the residency it now holds.
//
// The request must name the Host the link goes to, for Bind's reason, and it
// is validated before anything is dialled. The link is acquired under the
// pool's lock -- so a burst of attaches to one Host opens one connection, as a
// burst of binds does -- but the RPC is made OUTSIDE it, and that is the one
// way this differs from Bind. An attach can launch a runtime, which takes as
// long as a runtime takes to start; holding the pool across it would stall
// every bind, unbind and delivery on every Host for that long. Nothing here
// needs the ordering the lock gives Bind: an attach records no route, so there
// is no route table entry for a concurrent operation to race.
//
// What that costs is stated: a link that the reaper collects between the
// acquisition and the RPC fails the RPC as a transport error, which the caller
// sees as an attempt that did not complete and retries under the same
// idempotency key. A freshly dialled link is never reapable, because its idle
// window starts at the dial.
//
// An accepted observation is checked against the request before it is
// returned. The Host is required to answer for the session and incarnation it
// was asked about, and a caller is about to bind with the observation's epoch,
// so an answer about anything else is refused with ErrAttachMismatch rather
// than trusted.
func (p *Pool) Attach(ctx context.Context, target Target, req sessionwire.HostLinkAttachRequest) (sessionwire.HostLinkRegistryObservation, error) {
	if err := target.Validate(); err != nil {
		return sessionwire.HostLinkRegistryObservation{}, err
	}
	if req.HostID != target.Host {
		return sessionwire.HostLinkRegistryObservation{}, fmt.Errorf("%w: request names %q, link goes to %q", ErrTargetMismatch, req.HostID, target.Host)
	}
	if err := req.Validate(); err != nil {
		return sessionwire.HostLinkRegistryObservation{}, err
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return sessionwire.HostLinkRegistryObservation{}, ErrPoolClosed
	}
	pooled, err := p.acquireLocked(ctx, target)
	if err != nil {
		p.mu.Unlock()
		return sessionwire.HostLinkRegistryObservation{}, err
	}
	link := pooled.link
	p.mu.Unlock()

	observation, err := link.Attach(ctx, req)
	if err != nil {
		return sessionwire.HostLinkRegistryObservation{}, err
	}
	if err := attachAnswers(req, observation); err != nil {
		return sessionwire.HostLinkRegistryObservation{}, err
	}
	return observation, nil
}

// attachAnswers reports whether an accepted observation is about what the
// attach asked for: the same session, on the same Host incarnation, for the
// same agent and runtime.
//
// The incarnation is compared as well as the Host, and that is the fence doing
// its second job. Core requires a Host that is not the incarnation named to
// refuse BEFORE taking the lease; a Host that answered for a different
// generation anyway has told the caller a lease epoch from an incarnation the
// caller did not place on, and binding with it is exactly the stranded route
// the fence exists to prevent.
func attachAnswers(req sessionwire.HostLinkAttachRequest, observation sessionwire.HostLinkRegistryObservation) error {
	switch {
	case observation.TenantID != req.TenantID || observation.SessionID != req.SessionID:
		return fmt.Errorf("%w: observation is for session %q/%q, attach asked for %q/%q",
			ErrAttachMismatch, observation.TenantID, observation.SessionID, req.TenantID, req.SessionID)
	case observation.HostID != req.HostID || observation.HostGeneration != req.HostGeneration:
		return fmt.Errorf("%w: observation names host %q generation %d, attach fenced %q generation %d",
			ErrAttachMismatch, observation.HostID, observation.HostGeneration, req.HostID, req.HostGeneration)
	case observation.AgentID != req.AgentID || observation.RuntimeCompatibilityID != req.RuntimeCompatibilityID:
		return fmt.Errorf("%w: observation names agent %q runtime %q, attach asked for %q runtime %q",
			ErrAttachMismatch, observation.AgentID, observation.RuntimeCompatibilityID, req.AgentID, req.RuntimeCompatibilityID)
	}
	return nil
}

// Unbind releases one session route.
//
// The local route is released even when the Host could not be told. A bind is
// Factory-local routing state and the Host validates ownership against its own
// durable lease, so a pool that kept a route it had failed to release would
// hold it forever: there is no retry, and the next Bind for that session would
// be refused as a conflict against a route nobody wants.
func (p *Pool) Unbind(ctx context.Context, req sessionwire.HostLinkUnbindRequest) error {
	if err := req.Validate(); err != nil {
		return err
	}
	key := routeKey{tenant: req.TenantID, session: req.SessionID}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrPoolClosed
	}
	host, ok := p.routes[key]
	if !ok {
		return fmt.Errorf("%w: session %q", ErrUnknownBinding, req.SessionID)
	}
	if req.HostID != host {
		return fmt.Errorf("%w: request names %q, session is bound to %q", ErrTargetMismatch, req.HostID, host)
	}
	// The route and the link table are two maps. They are written and deleted
	// in the same critical section, so a route always names a live link -- but
	// nothing in the type system says so, and the cost of being wrong is not a
	// wrong answer but a nil dereference under this lock, which wedges every
	// later caller as well as killing this one. It fails closed instead, and
	// the orphaned route is DROPPED: a route naming nothing that survived its
	// own refusal would refuse every later bind for that session as a conflict.
	pooled, ok := p.links[host]
	if !ok {
		delete(p.routes, key)
		return fmt.Errorf("%w: session %q was routed to %q, which has no link", ErrUnknownBinding, req.SessionID, host)
	}
	delete(p.routes, key)
	delete(pooled.bindings, key)
	if len(pooled.bindings) == 0 {
		pooled.idleSince = p.now()
	}
	return pooled.link.Unbind(ctx, req)
}

// DeliverCommand hands one already committed inbox record to the Host that owns
// its session.
//
// The reply is about DELIVERY, not about the command's outcome. A failure to
// hand it over is wrapped in ErrCommandUndelivered and leaves the inbox record
// pending; the Host's own refusal arrives as *HostRefusal and is returned
// unwrapped, so a caller can tell "never seen" from "answered". The binding
// survives either way: a transport failure is not evidence that the route is
// wrong, and this pool holds no store handle with which it could record
// anything about the record at all.
func (p *Pool) DeliverCommand(ctx context.Context, tenantID sessionwire.TenantID, sessionID sessionwire.SessionID, delivery sessionwire.HostLinkCommandDelivery) error {
	if err := delivery.Validate(); err != nil {
		return err
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrPoolClosed
	}
	host, ok := p.routes[routeKey{tenant: tenantID, session: sessionID}]
	if !ok {
		p.mu.Unlock()
		return fmt.Errorf("%w: session %q", ErrUnknownBinding, sessionID)
	}
	// Fails closed for the reason Unbind does. The route is left in place here
	// rather than dropped, because a delivery is not the caller that owns the
	// route's lifetime; Unbind is.
	pooled, ok := p.links[host]
	if !ok {
		p.mu.Unlock()
		return fmt.Errorf("%w: session %q is routed to %q, which has no link", ErrUnknownBinding, sessionID, host)
	}
	link := pooled.link
	p.mu.Unlock()

	if err := link.DeliverCommand(ctx, tenantID, sessionID, delivery); err != nil {
		var refusal *HostRefusal
		if errors.As(err, &refusal) {
			return err
		}
		return fmt.Errorf("%w: command %q to %q: %w", ErrCommandUndelivered, delivery.CommandID, host, err)
	}
	return nil
}

// ReapIdle closes every link that has had no binding for longer than
// IdleTimeout and reports how many it closed.
//
// The cadence is the CALLER's, and the clock is read from the pool rather than
// passed in, so there is one authority for "now". A pool that ran its own timer
// would be a goroutine with a lifetime this type does not own, and A9.1 owns
// the start/stop ordering that would give it one.
func (p *Pool) ReapIdle() int {
	deadline := p.now().Add(-p.limits.IdleTimeout)

	p.mu.Lock()
	defer p.mu.Unlock()
	reaped := 0
	for host, pooled := range p.links {
		if len(pooled.bindings) > 0 {
			continue
		}
		// A link idle for EXACTLY the window is reaped, because IdleTimeout is
		// documented as how long a link with no demand is kept and "kept for
		// 60s" that survives 60s is kept for longer than that. The boundary is
		// measured one tick either side rather than left to the comparison.
		if pooled.idleSince.After(deadline) {
			continue
		}
		delete(p.links, host)
		// Close is best effort: the link is gone from the pool either way, and
		// a reaper that reported an error would have nobody to report it to.
		_ = pooled.link.Close(context.Background())
		reaped++
	}
	return reaped
}

// Close closes every link and refuses further work.
//
// Every link is closed even when one fails, and every failure is reported: one
// wedged connection must not strand the rest, and a Close that returned at the
// first error would leave the others open with no caller left to close them.
//
// It is idempotent, and the idempotence comes from EMPTYING the table rather
// than from a closed check: a second call finds no links and closes nothing. An
// `if p.closed { return nil }` fast path was written here first and removed
// after a mutation showed it to be inert -- this lane does not keep a guard
// that guards nothing.
func (p *Pool) Close(ctx context.Context) error {
	p.mu.Lock()
	p.closed = true
	links := make([]*pooledLink, 0, len(p.links))
	for _, pooled := range p.links {
		links = append(links, pooled)
	}
	p.links = map[sessionwire.HostID]*pooledLink{}
	p.routes = map[routeKey]sessionwire.HostID{}
	p.mu.Unlock()

	var errs []error
	for _, pooled := range links {
		if err := pooled.link.Close(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// acquireLocked returns the pooled link for target, dialling if needed.
//
// The ceiling is checked before the dial and only for a host that has no link,
// so it bounds HOSTS. A pool that checked it per bind would refuse the second
// session on a Host it is already connected to, which is the opposite of what
// this type is for.
func (p *Pool) acquireLocked(ctx context.Context, target Target) (*pooledLink, error) {
	if pooled, ok := p.links[target.Host]; ok {
		return pooled, nil
	}
	if len(p.links) >= p.limits.MaxLinks {
		return nil, fmt.Errorf("%w: %d links open, cannot dial %q", ErrLinkLimit, len(p.links), target.Host)
	}
	link, err := p.dialer.Dial(ctx, target, p.observer)
	if err != nil {
		return nil, err
	}
	pooled := &pooledLink{link: link, bindings: map[routeKey]struct{}{}, idleSince: p.now()}
	p.links[target.Host] = pooled
	return pooled, nil
}

func validateHost(host sessionwire.HostID) error {
	if err := host.Validate(); err != nil {
		return fmt.Errorf("%w: host_id: %w", ErrInvalidConfig, err)
	}
	return nil
}

func validateEndpoint(endpoint sessionwire.InternalEndpoint) error {
	if err := endpoint.Validate(); err != nil {
		return fmt.Errorf("%w: internal_endpoint: %w", ErrInvalidConfig, err)
	}
	return nil
}
