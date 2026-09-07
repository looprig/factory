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
	pooled := p.links[host]
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
	link := p.links[host].link
	p.mu.Unlock()

	if err := link.DeliverCommand(ctx, delivery); err != nil {
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
