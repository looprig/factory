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

// ErrLinkClosed reports a call on a link this replica has closed: by the
// pool's shutdown, the idle reaper, an eviction, or a handshake it refused. It
// is TERMINAL -- a closed link never answers again -- and it is returned
// before anything is sent.
var ErrLinkClosed = errors.New("hostlink: link is closed")

// ErrNoTenantEndpoint reports a Host whose advertised base cannot carry one
// tenant's HostLink address: Core's sessionwire.HostLinkEndpoint refused to
// derive it. It is a PER-TENANT fact about one Host -- a tenant too long for
// the address that base leaves room for (too_long), a tenant Core will not
// route (unroutable_tenant), or a base that is not a bare base at all
// (base_names_tenant, base_not_bare), which refuses every tenant -- and it is
// decided BEFORE anything is dialled, so it costs no connection.
//
// A caller treats it as "this Host cannot serve this tenant": placement skips
// the candidate for that tenant and asks the next one, and a routing bind
// leaves the session unbound for the next poll. It is never a reason to
// abandon the Host for another tenant.
var ErrNoTenantEndpoint = errors.New("hostlink: the host's advertised base cannot carry this tenant's address")

// EndpointError is ErrNoTenantEndpoint with its detail: which Host, which
// tenant, and Core's typed refusal, whose Code a caller may branch on and a log
// line should carry.
type EndpointError struct {
	Host   sessionwire.HostID
	Tenant sessionwire.TenantID
	Cause  *sessionwire.HostLinkEndpointError
}

func (e *EndpointError) Error() string {
	return fmt.Sprintf("%s: host %q tenant %q: %s", ErrNoTenantEndpoint, e.Host, e.Tenant, e.Cause.Code)
}

// Unwrap makes the error both ErrNoTenantEndpoint and Core's
// *HostLinkEndpointError, so a caller can test the class with errors.Is and
// read the code with errors.As without knowing this type.
func (e *EndpointError) Unwrap() []error { return []error{ErrNoTenantEndpoint, e.Cause} }

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
	// binding of ONE TENANT to one Host, so this bounds (Host, tenant) pairs --
	// Hosts times the tenants this replica serves on each -- never sessions.
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
	// GateResponses decides, from a Host's connect reply, whether that Host
	// can apply a gate_response command. A nil value takes
	// GateResponseCapable, which is what every production composition uses;
	// the field exists so the mechanism's ACCEPTING half can be driven before
	// any Host advertises the capability.
	GateResponses func(sessionwire.VersionNegotiationResponse) bool
}

// Pool holds at most one physical HostLink per (Host, tenant).
//
// The invariant it exists for is in one sentence: a session binding never costs
// a connection, and a connection is never shared between Hosts OR BETWEEN
// TENANTS. Everything else here -- the ceiling, the idle window, the conflict
// refusal -- protects that sentence against the ways a caller can accidentally
// violate it.
//
// The tenant half is v0.5.0's (Gap 1), and it is the Host's rule, not a
// preference: a Host serves each tenant's HostLink at its own derived address
// and authenticates the connection for the tenant that address names, so a
// link IS one tenant's. Keying by (Host, tenant) is what lets one pooled Host
// hold several tenants' sessions at once, and it is what keeps R-1 structural:
// every route names its tenant, a route's link is looked up under that tenant,
// so no path here can deliver, bind or subscribe one tenant's session over
// another tenant's connection. Every per-link structure -- the negotiated
// capability set, reconnect state, live-tail subscriptions and their regMu --
// lives INSIDE the link, and is therefore per (Host, tenant) by construction.
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
	// gateResponses is the gate_response capability predicate; see
	// AcceptsGateResponses.
	gateResponses func(sessionwire.VersionNegotiationResponse) bool

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
	links  map[linkKey]*pooledLink
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

// linkKey names one physical link: one tenant's HostLink to one Host. A route
// (tenant, session) -> Host resolves to exactly one of these, linkKey{Host,
// tenant}, and that lookup is the whole of R-1 in this package.
type linkKey struct {
	host   sessionwire.HostID
	tenant sessionwire.TenantID
}

func (k routeKey) link(host sessionwire.HostID) linkKey {
	return linkKey{host: host, tenant: k.tenant}
}

type pooledLink struct {
	link Link
	// endpoint and generation are the advertisement the link was dialled on:
	// the tenant's DERIVED address and the Host incarnation (zero if the
	// caller did not know it). A later advertisement of the same HostID at
	// another address, not older than this one, replaces the link; see
	// acquireLocked.
	endpoint   sessionwire.InternalEndpoint
	generation uint64
	bindings   map[routeKey]struct{}
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
	gateResponses := cfg.GateResponses
	if gateResponses == nil {
		gateResponses = GateResponseCapable
	}
	return &Pool{
		dialer:        cfg.Dialer,
		observer:      observer,
		limits:        limits,
		now:           now,
		gateResponses: gateResponses,
		links:         map[linkKey]*pooledLink{},
		routes:        map[routeKey]sessionwire.HostID{},
	}, nil
}

// Limits returns the limits this pool was composed with, defaults included.
func (p *Pool) Limits() Limits { return p.limits }

// Links reports the physical connections this pool currently holds: one per
// (Host, tenant) pair.
func (p *Pool) Links() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.links)
}

// TenantLinks reports how many tenants' links this pool holds to one Host.
func (p *Pool) TenantLinks(host sessionwire.HostID) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for key := range p.links {
		if key.host == host {
			n++
		}
	}
	return n
}

// Bindings reports the session routes multiplexed over one Host's links, over
// every tenant.
func (p *Pool) Bindings(host sessionwire.HostID) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for key, pooled := range p.links {
		if key.host == host {
			n += len(pooled.bindings)
		}
	}
	return n
}

// TenantBindings reports the session routes multiplexed over one tenant's link
// to one Host.
func (p *Pool) TenantBindings(host sessionwire.HostID, tenantID sessionwire.TenantID) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	pooled, ok := p.links[linkKey{host: host, tenant: tenantID}]
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
	dial, err := tenantTarget(target, req.TenantID)
	if err != nil {
		return err
	}
	// The request is fenced to one incarnation; that is the advertisement
	// this bind acts on.
	dial.Generation = req.HostGeneration

	key := routeKey{tenant: req.TenantID, session: req.SessionID}

	// A terminal link this bind evicts is closed AFTER the pool's lock is
	// released, as evict does: Client.Close waits for the link's callback
	// queue to drain, and a pool lock held across that is a deadlock the day
	// any callback reaches the pool. Deferred first, so it runs last.
	var dead Link
	defer func() {
		if dead != nil {
			_ = dead.Close(context.Background())
		}
	}()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrPoolClosed
	}
	if bound, ok := p.routes[key]; ok && bound != target.Host {
		return fmt.Errorf("%w: session %q is bound to %q", ErrBindingConflict, req.SessionID, bound)
	}

	pooled, replaced, err := p.acquireLocked(ctx, key.link(target.Host), dial)
	if err != nil {
		return err
	}
	if replaced != nil {
		dead = replaced
	}
	if err := pooled.link.Bind(ctx, req); err != nil {
		// The BINDING is not recorded, because the Host does not have it. The
		// LINK is kept: the Host answered, so the connection is good, and
		// discarding it would turn one lease disagreement into a dial storm.
		//
		// Except when the link is TERMINAL, which is the one failure that is
		// not the Host answering. A dead link kept here would refuse every
		// later bind for this Host before sending anything, and -- unlike an
		// attach's -- a viewer's route pins it against the reaper, so a Host
		// that closed this replica out would never be dialled again. Attach
		// has evicted on this evidence since B5; a bind is the path Gap 3's
		// re-bind after a lost tail takes, so it must too.
		if terminalLinkError(err) && p.dropLinkLocked(key.link(target.Host), pooled) {
			dead = pooled.link
		}
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
// What that costs is stated: an attach records no binding, so the link it runs
// on can be collected by the reaper at ANY point from the acquisition to the
// end of the RPC -- before the request is sent, or while the Host is working on
// it. Either way the RPC fails as a transport error, which the caller sees as
// an attempt that did not complete and retries under the same idempotency key;
// a Host that had already attached keeps the residency, and the next pass
// finds the owner. Only the attach that DIALLED the link is safe from it at the
// start, because a link's idle window starts at the dial.
//
// A link that turns out to be TERMINAL -- another wire version, or a close the
// transport will not reconnect from -- is evicted here, so the next attach to
// that Host dials afresh instead of being refused by a dead link forever.
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
	dial, err := tenantTarget(target, req.TenantID)
	if err != nil {
		return sessionwire.HostLinkRegistryObservation{}, err
	}
	dial.Generation = req.HostGeneration
	linked := linkKey{host: target.Host, tenant: req.TenantID}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return sessionwire.HostLinkRegistryObservation{}, ErrPoolClosed
	}
	pooled, replaced, err := p.acquireLocked(ctx, linked, dial)
	if err != nil {
		p.mu.Unlock()
		return sessionwire.HostLinkRegistryObservation{}, err
	}
	link := pooled.link
	p.mu.Unlock()
	if replaced != nil {
		_ = replaced.Close(context.Background())
	}

	observation, err := link.Attach(ctx, req)
	if err != nil {
		if terminalLinkError(err) {
			p.evict(linked, pooled)
		}
		return sessionwire.HostLinkRegistryObservation{}, err
	}
	if err := attachAnswers(req, observation); err != nil {
		return sessionwire.HostLinkRegistryObservation{}, err
	}
	return observation, nil
}

// terminalLinkError reports an error from a link that will never answer again:
// the Host speaks another wire version (ErrUnsupportedProtocol), or closed the
// connection with a code the transport will not reconnect from
// (*HostDisconnect), or this replica closed it (ErrLinkClosed). A link is
// marked terminal by any of them, and every later call on it returns the same
// error BEFORE anything is sent.
func terminalLinkError(err error) bool {
	var disconnect *HostDisconnect
	return errors.Is(err, ErrUnsupportedProtocol) || errors.Is(err, ErrLinkClosed) || errors.As(err, &disconnect)
}

// evict drops a dead link from the pool, with every route that names its Host
// FOR ITS TENANT, and closes it. Another tenant's link to the same Host is a
// different connection and is left alone: one tenant's terminal close is not
// evidence about another's.
//
// A terminal link kept in the table would be handed to every later caller for
// that Host, each of which would be refused before sending anything -- so a
// Host restarted at another wire version, or one that closed this replica out,
// would never be dialled again until the idle reaper collected a link that a
// viewer's route could pin forever. Dropping it lets the next caller dial
// afresh. The routes go with it because a route naming no link fails closed
// on its next use (see Unbind), and a route to a dead link is already one that
// can deliver nothing; the routing plane's repair rebinds a subscriber's.
//
// It removes the entry only if it is still the one the caller used: a racing
// caller may already have evicted it and dialled a replacement, which must not
// be closed on this caller's evidence.
func (p *Pool) evict(linked linkKey, stale *pooledLink) {
	p.mu.Lock()
	dropped := p.dropLinkLocked(linked, stale)
	p.mu.Unlock()
	if dropped {
		_ = stale.link.Close(context.Background())
	}
}

// dropLinkLocked removes a dead link and every route it carried -- the routes
// naming its Host under its tenant -- if the link is still the one the pool
// holds for that pair. It reports whether it removed anything; closing the
// link is the caller's.
func (p *Pool) dropLinkLocked(linked linkKey, stale *pooledLink) bool {
	current, ok := p.links[linked]
	if !ok || current != stale {
		return false
	}
	delete(p.links, linked)
	for key, routed := range p.routes {
		if key.link(routed) == linked {
			delete(p.routes, key)
		}
	}
	return true
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
	pooled, ok := p.links[key.link(host)]
	if !ok {
		delete(p.routes, key)
		return fmt.Errorf("%w: session %q was routed to %q, which has no link for tenant %q", ErrUnknownBinding, req.SessionID, host, req.TenantID)
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
	key := routeKey{tenant: tenantID, session: sessionID}
	host, ok := p.routes[key]
	if !ok {
		p.mu.Unlock()
		return fmt.Errorf("%w: session %q", ErrUnknownBinding, sessionID)
	}
	// Fails closed for the reason Unbind does. The route is left in place here
	// rather than dropped, because a delivery is not the caller that owns the
	// route's lifetime; Unbind is. The link is the ROUTE'S TENANT'S: a
	// delivery is never sent over another tenant's connection, even to the
	// same Host.
	pooled, ok := p.links[key.link(host)]
	if !ok {
		p.mu.Unlock()
		return fmt.Errorf("%w: session %q is routed to %q, which has no link for tenant %q", ErrUnknownBinding, sessionID, host, tenantID)
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

	// Reaped links are closed AFTER the pool's lock is released, as Bind and
	// evict close theirs: a link's Close waits for its RPCs in flight, up to
	// its bound, and one held behind a wedged RPC would otherwise stall every
	// bind, unbind and delivery on every Host for that long (v0.7.2 gate F1,
	// measured at 1.95s).
	var reaped []*pooledLink
	defer func() {
		// Close is best effort: the link is gone from the pool either way, and
		// a reaper that reported an error would have nobody to report it to.
		_ = closeLinks(context.Background(), reaped)
	}()
	p.mu.Lock()
	defer p.mu.Unlock()
	for linked, pooled := range p.links {
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
		delete(p.links, linked)
		reaped = append(reaped, pooled)
	}
	return len(reaped)
}

// closeLinks closes every link CONCURRENTLY and reports every failure. Each
// link's Close is bounded by ctx and its own cap, so closing them side by side
// bounds the whole call by one cap rather than one per link; the number of
// goroutines is at most the pool's MaxLinks. It must be called with no pool
// lock held.
func closeLinks(ctx context.Context, links []*pooledLink) error {
	errs := make([]error, len(links))
	var wg sync.WaitGroup
	for i, pooled := range links {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = pooled.link.Close(ctx)
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
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
	p.links = map[linkKey]*pooledLink{}
	p.routes = map[routeKey]sessionwire.HostID{}
	p.mu.Unlock()

	// Concurrently (v0.7.2 gate S3): each Close may wait out its cap behind a
	// wedged RPC, and N of them in turn would make Stop take N caps.
	return closeLinks(ctx, links)
}

// acquireLocked returns the pooled link for one (Host, tenant), dialling dial
// -- that tenant's DERIVED address -- if there is none, or if the one it holds
// was dialled on an advertisement the caller's has superseded.
//
// The ceiling is checked before the dial and only for a pair that has no link,
// so it bounds (Host, tenant) pairs. A pool that checked it per bind would
// refuse the second session of a tenant on a Host it is already connected to,
// which is the opposite of what this type is for. A replacement does not grow
// the table, so it is not checked against the ceiling.
//
// # A Host that moved (I3.1 D1)
//
// A Host restarted the way a pod is keeps its HostID and comes back at a
// higher generation on a NEW address. The cached link keeps redialling the old
// one, answering ErrLinkReconnecting, and before this rule the pair was
// reachable again only when the idle reaper collected it -- 60s, or never while
// a viewer's route pinned it. So a cached link whose address differs from the
// caller's is REPLACED when the caller's generation is HIGHER than the link's,
// and kept otherwise (a stale registry row must not drag the pool back to a
// dead address, and one incarnation cannot be at two addresses). The same address at a higher generation keeps the link:
// the transport's own reconnect reaches the restarted Host there. The replaced
// link is dropped with every route it carried, as evict drops a dead one --
// they named an incarnation that is gone, and the routing plane's repair
// rebinds a subscriber's -- and it is RETURNED for the caller to close after
// releasing the pool's lock, for Bind's reason. The new link is dialled before
// the old one is dropped, so a failed dial changes nothing.
func (p *Pool) acquireLocked(ctx context.Context, linked linkKey, dial Target) (*pooledLink, Link, error) {
	current, ok := p.links[linked]
	if ok && !current.supersededBy(dial) {
		current.observe(dial)
		return current, nil, nil
	}
	if !ok && len(p.links) >= p.limits.MaxLinks {
		return nil, nil, fmt.Errorf("%w: %d links open, cannot dial %q for tenant %q", ErrLinkLimit, len(p.links), linked.host, linked.tenant)
	}
	link, err := p.dialer.Dial(ctx, dial.dialable(), p.observer)
	if err != nil {
		return nil, nil, err
	}
	var replaced Link
	if ok && p.dropLinkLocked(linked, current) {
		replaced = current.link
	}
	pooled := p.newPooledLink(link, dial)
	p.links[linked] = pooled
	return pooled, replaced, nil
}

// newPooledLink records a freshly dialled link and the advertisement it was
// dialled on.
func (p *Pool) newPooledLink(link Link, dial Target) *pooledLink {
	return &pooledLink{link: link, bindings: map[routeKey]struct{}{}, idleSince: p.now(), endpoint: dial.Endpoint, generation: dial.Generation}
}

// supersededBy reports whether dial names the same HostID at ANOTHER address
// under a NEWER incarnation than the one this link was dialled on. Only a
// higher generation moves a link: two observations of one incarnation at two
// addresses contradict each other, and following whichever arrived last would
// let two stale sources flap the link. A link dialled with no known
// generation (zero) is moved by any known one; a caller that does not know
// the incarnation moves nothing.
func (l *pooledLink) supersededBy(dial Target) bool {
	return dial.Endpoint != l.endpoint && dial.Generation > l.generation
}

// observe raises the link's recorded generation to one seen at its own
// address, so an older advertisement of another address cannot later replace
// a link the Host has already been confirmed at. It is called under the pool's
// lock.
func (l *pooledLink) observe(dial Target) {
	if dial.Endpoint == l.endpoint && dial.Generation > l.generation {
		l.generation = dial.Generation
	}
}

// dialable is the target handed to the Dialer: the Host and its address. The
// generation is the pool's bookkeeping and is not part of what is dialled.
func (t Target) dialable() Target { return Target{Host: t.Host, Endpoint: t.Endpoint} }

// tenantTarget is the address this pool dials for one tenant's link to a Host:
// Core's HostLinkEndpoint over the Host's advertised BASE. It is the ONE place
// an address is derived, so every path that opens a link -- bind, attach, and
// through them delivery, the live tail and placement -- dials the same one.
//
// A refusal is an *EndpointError (ErrNoTenantEndpoint), returned before
// anything is dialled: nothing about the Host was learned, and nothing about
// another tenant was decided.
func tenantTarget(base Target, tenantID sessionwire.TenantID) (Target, error) {
	endpoint, err := sessionwire.HostLinkEndpoint(base.Endpoint, tenantID)
	if err != nil {
		var refused *sessionwire.HostLinkEndpointError
		if errors.As(err, &refused) {
			return Target{}, &EndpointError{Host: base.Host, Tenant: tenantID, Cause: refused}
		}
		return Target{}, fmt.Errorf("%w: host %q tenant %q: %w", ErrNoTenantEndpoint, base.Host, tenantID, err)
	}
	return Target{Host: base.Host, Endpoint: endpoint, Generation: base.Generation}, nil
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
