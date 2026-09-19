package factory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/admission"
	"github.com/looprig/factory/internal/httpapi"
	internalidentity "github.com/looprig/factory/internal/identity"
	"github.com/looprig/factory/internal/placement"
	"github.com/looprig/factory/internal/realtime/clientlink"
	"github.com/looprig/factory/internal/realtime/hostlink"
	"github.com/looprig/factory/internal/realtime/livetail"
	"github.com/looprig/factory/internal/reconcile"
	"github.com/looprig/factory/internal/routing"
	"github.com/looprig/sessionstore"
)

// components is everything factory.New builds behind the public surface.
//
// They are constructed at composition and NOT started there. The distinction
// is the one this stage exists to make: a value that runs a goroutine the
// moment it is built makes New a call with a lifetime, so a library embedding
// that composed a Server to read its Handler would leave a node, a pool and
// three sweep loops behind it. Everything that runs is started by Start and
// stopped by Stop, in the order Stop documents.
type components struct {
	admissions *admission.Service
	pool       *hostlink.Pool
	bindings   *routing.Bindings
	demand     *routing.Demand
	// live carries a watched session's output from its Host to its viewers
	// (Gap 3): it is the routing table's Binder, the demand plane's Hinter
	// and Watcher, and the Tail and Publisher of the relay it drives.
	live *livetail.Plane
	// realtime is nil until Start, and the router reads it through a supplier
	// on every request rather than holding it. See RouterConfig.Realtime.
	realtimeMu sync.RWMutex
	realtime   *clientlink.Handler

	commandSweeper *admission.Reconciler
	// dispositionSweeper rejects the disposition commands no Host applied
	// before their apply deadline. It is composed unconditionally: every
	// command this replica admits is a disposition command, and a Host leaves
	// the deadline to Factory.
	dispositionSweeper *admission.DispositionReconciler
	gateSweeper        *reconcile.GateSweeper
	placement          *placement.Reconciler
	records            *placement.RecordSweeper
	// pending is nil unless WithPendingCommands supplied the durable query
	// that triggers placement; see sweeps.
	pending *placement.PendingSweeper
}

// composeComponents builds the component graph from a validated composition.
//
// The ORDER here is dependency order and nothing else -- a value is built
// after everything it holds -- and it is not the start order, which Start
// states separately. Every rejection is attributed to the option carrying the
// offending value, for the reason composeRouter's are.
func composeComponents(cfg config, credentials *internalidentity.Authenticator) (*components, error) {
	dialer, err := hostlink.NewCentrifugeDialer(hostlink.DialerConfig{
		Credential: cfg.hostCredential,
		Version:    cfg.version,
		Limits:     hostlink.Limits(cfg.host),
	})
	if err != nil {
		return nil, &OptionError{Option: "WithHostLinkCredential", Err: err}
	}
	pool, err := hostlink.NewPool(hostlink.Config{
		Dialer: dialer,
		Limits: hostlink.Limits(cfg.host),
		Now:    cfg.clock.Now,
	})
	if err != nil {
		return nil, &OptionError{Option: "WithHostLinkLimits", Err: err}
	}

	service, err := admission.NewService(admission.Config{
		Authorizer: cfg.authorizer,
		Targets:    departmentTargets(cfg.department),
		Catalog:    cfg.catalog,
		Commands:   cfg.commands,
		Directory:  cfg.directory,
		Clock:      cfg.clock,
		IDs:        cfg.uuids,
		// The public-create plane and the binding travel together: the store
		// is always supplied and the binding is the deployment's choice, so a
		// composition with no WithSessionBinding refuses a create in
		// admission rather than at the route. See Config.createsServed.
		PublicCreates: cfg.publicCreates,
		Binding:       cfg.sessionBinding,
		// The apply deadline an accepted command is given is the SAME number
		// the sweeper settles against. Two values here would be a command
		// rejected before it was due, or one the sweeper never reached.
		ApplyDeadline: cfg.reconcile.ApplyDeadline,
		// The gate_response capability gate: a gate response is admitted
		// only for an owner that can apply one, asked of the owner's link
		// for the session's tenant and decided by
		// hostlink.GateResponseCapable: the owner's connect reply must carry
		// Core's token sessionwire.HostLinkCapabilityGateResponse.
		GateResponders: gateResponders{pool: pool},
	})
	if err != nil {
		return nil, &OptionError{Option: "WithReconcileLimits", Err: err}
	}

	// Gap 3: the live-tail plane and the routing it drives. The ClientLink it
	// publishes through does not exist until Start, so it is supplied late.
	c := &components{}
	live, bindings, demand, err := composeLive(cfg, pool, func() livetail.Viewers {
		if handler := c.clientLink(); handler != nil {
			return handler
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	commandSweeper, err := admission.NewReconciler(admission.ReconcilerConfig{
		Authorizer: cfg.authorizer,
		Due:        cfg.commands,
		Settlement: cfg.commands,
		Claims:     cfg.commands,
		Clock:      cfg.clock,
		// Its own holder too, for the disposition sweep's reason below (the
		// v0.3.0 regate's L1). It is unreachable today -- placement reconciles
		// only disposition-bound sessions and legacy rows live only on legacy
		// ones -- but under the replica's own id this sweep would EXTEND a
		// placement claim on a legacy session and then release it mid-attach.
		HolderID:  commandHolder(cfg.replicaID),
		ClaimTTL:  cfg.reconcile.ClaimTTL,
		PageLimit: cfg.reconcile.MaxDuePerSweep,
		MaxPages:  cfg.reconcile.MaxConcurrent,
	})
	if err != nil {
		return nil, &OptionError{Option: "WithReconcileLimits", Err: err}
	}
	dispositionSweeper, err := admission.NewDispositionReconciler(admission.DispositionReconcilerConfig{
		Authorizer: cfg.authorizer,
		Due:        cfg.commands,
		Settlement: cfg.commands,
		Claims:     cfg.commands,
		Clock:      cfg.clock,
		// Its OWN holder, not the replica's. The store treats an acquire by
		// the claim's current holder as an EXTENSION, so under the shared
		// replica id this sweep would "acquire" the claim placement holds
		// mid-attach, reject, and then RELEASE it -- letting another replica
		// attach the same session concurrently (B5 quality gate N1). Under
		// its own holder it meets placement's live claim as held and defers.
		HolderID:  dispositionHolder(cfg.replicaID),
		ClaimTTL:  cfg.reconcile.ClaimTTL,
		PageLimit: cfg.reconcile.MaxDuePerSweep,
		MaxPages:  cfg.reconcile.MaxConcurrent,
	})
	if err != nil {
		return nil, &OptionError{Option: "WithReconcileLimits", Err: err}
	}
	gateSweeper, err := reconcile.NewGateSweeper(reconcile.GateSweeperConfig{
		Authorizer: cfg.authorizer,
		Due:        cfg.gates,
		Intents:    cfg.gates,
		Clock:      cfg.clock,
		PageLimit:  cfg.reconcile.MaxDuePerSweep,
		MaxPages:   cfg.reconcile.MaxConcurrent,
	})
	if err != nil {
		return nil, &OptionError{Option: "WithGates", Err: err}
	}
	placer, err := placement.NewReconciler(placement.Config{
		Directory: cfg.directory,
		Catalog:   cfg.catalog,
		Claims:    cfg.commands,
		// Workloads is deliberately nil. H5 (answered 2026-09-04) puts the
		// Kubernetes adapter in a SEPARATE controller binary, so cmd/factory
		// ships with a ServiceAccount holding no workload RBAC and must reach
		// no Kubernetes client package. A dedicated placement here therefore
		// fails with placement.ErrNoWorkloadController, which is the refusal
		// that keeps the split honest; see WithWorkloadController, which only
		// the controller binary calls.
		Workloads:      cfg.workloads,
		Clock:          cfg.clock,
		HolderID:       cfg.replicaID,
		ClaimTTL:       cfg.reconcile.ClaimTTL,
		CandidateLimit: cfg.reconcile.MaxDuePerSweep,
		// B5: the pooled arm ATTACHES through this replica's HostLink pool
		// and binds with the epoch the Host answered, rather than naming a
		// candidate and stopping. The actor is the sweep's service identity:
		// placement runs from a sweeper with no user behind it, and Core's
		// actor_id names who asked for residency, never whose authority a
		// command carries.
		Links:   placementLinks{pool: pool},
		ActorID: cfg.service.Subject(),
		Logger:  logger(cfg),
	})
	if err != nil {
		return nil, &OptionError{Option: "WithReconcileLimits", Err: err}
	}
	var pending *placement.PendingSweeper
	if cfg.pending != nil {
		pending, err = placement.NewPendingSweeper(placement.PendingSweeperConfig{
			Authorizer: cfg.authorizer,
			Pending:    cfg.pending,
			Placer:     placer,
			Clock:      cfg.clock,
			// The horizon is the apply deadline admission gives every
			// command, so every command accepted up to now is inside it.
			Horizon:   cfg.reconcile.ApplyDeadline,
			PageLimit: cfg.reconcile.MaxDuePerSweep,
			MaxPages:  cfg.reconcile.MaxConcurrent,
			Logger:    logger(cfg),
		})
		if err != nil {
			return nil, &OptionError{Option: "WithPendingCommands", Err: err}
		}
	}
	records, err := placement.NewRecordSweeper(placement.SweeperConfig{
		Targets: cfg.hostTargets,
		Claims:  cfg.commands,
		// The SAME replica identifier the placement reconciler claims under.
		// A release is refused for any other holder, so the whole reach of
		// this sweep is the claims this replica itself left behind.
		HolderID:  cfg.replicaID,
		PageLimit: cfg.reconcile.MaxDuePerSweep,
		MaxPages:  cfg.reconcile.MaxConcurrent,
	})
	if err != nil {
		return nil, &OptionError{Option: "WithHostTargets", Err: err}
	}

	c.admissions = service
	c.pool = pool
	c.bindings = bindings
	c.demand = demand
	c.live = live
	c.commandSweeper = commandSweeper
	c.dispositionSweeper = dispositionSweeper
	c.gateSweeper = gateSweeper
	c.placement = placer
	c.records = records
	c.pending = pending
	return c, nil
}

// clientLink is the running ClientLink node, or nil before Start and after
// Stop. The live plane publishes through it.
func (c *components) clientLink() *clientlink.Handler {
	c.realtimeMu.RLock()
	defer c.realtimeMu.RUnlock()
	return c.realtime
}

// composeLive builds Gap 3's live-tail plane, the routing table and demand
// plane it is the Binder, Hinter and Watcher of, and the routing.Relay it
// drives -- constructed here for the first time since A7.3 built it.
//
// It is its own function so the WIRING is testable without a Host or a
// ClientLink: which seam each component was handed is not observable from the
// outside, and a composition that handed the demand plane some other Hinter,
// or the routing table the bare pool, would bind and poll exactly as before
// while no viewer ever saw anything.
func composeLive(cfg config, pool *hostlink.Pool, viewers func() livetail.Viewers) (*livetail.Plane, *routing.Bindings, *routing.Demand, error) {
	live, err := livetail.New(livetail.Config{
		Links:   pool,
		Viewers: viewers,
		// The inbound bound is the Relay's own HostBinding queue: one
		// session's undelivered tail, before anything fans out.
		MailboxLimit: routing.DefaultRepairLimits().HostBindingQueue,
		EventTimeout: cfg.client.DemandTimeout,
		Logger:       logger(cfg),
	})
	if err != nil {
		return nil, nil, nil, &OptionError{Option: "WithClientLinkLimits", Err: err}
	}
	// The routing table's Binder is the plane: a bind is followed by a
	// subscribe on the same HostLink connection, the order a Host requires.
	bindings, err := routing.NewBindings(cfg.directory, live)
	if err != nil {
		return nil, nil, nil, &OptionError{Option: "WithDirectory", Err: err}
	}
	// The hint publisher is the same ClientLink channel the live tail uses:
	// an unbound watched session's viewers are told the durable tip.
	demand, err := routing.NewDemand(bindings, cfg.reads, live, cfg.clock, routing.DemandLimits{
		OwnershipPollInterval: cfg.client.DemandReleaseDebounce + cfg.reconcile.Interval,
		PollTimeout:           cfg.client.DemandTimeout,
	})
	if err != nil {
		return nil, nil, nil, &OptionError{Option: "WithClientLinkLimits", Err: err}
	}
	demand.SetWatcher(live)
	// Its Rebinder is the demand plane, its Tail and Publisher the live plane,
	// and its tip reads the same durable read the hints use.
	relay, err := routing.NewRelay(cfg.reads, demand, live, live, routing.DefaultRepairLimits())
	if err != nil {
		return nil, nil, nil, &OptionError{Option: "WithSessionReader", Err: err}
	}
	live.Attach(relay, demand)
	return live, bindings, demand, nil
}

// startRealtime builds and RUNS the ClientLink node.
//
// It is here rather than in composeComponents because clientlink.NewHandler
// runs the centrifuge node before it returns -- deliberately, so a composition
// cannot succeed and then refuse every connection -- and a running node is a
// lifetime. Until it exists /v1/realtime answers 503, which is the same
// fail-closed shape the nil object policy has: composed, stated, and never a
// silent success.
//
// # What makes the node RUNNING but unpublished safe, and what does not
//
// The node is running before the mutex below publishes it, so between those
// two statements a node exists that nothing can reach or shut down. That is a
// window of the same family as the one Serve's state claim closes, and
// realtimeMu is NOT what keeps it shut: this mutex guards the FIELD, not the
// interval. What keeps it shut is that Server.Start and Server.Stop are
// serialized on Server.lifecycle and both callers of this function hold it, so
// no Stop can observe the interval at all.
//
// Stated because the protection is not local. A later change that removed the
// outer serialization -- believing realtimeMu sufficient, which it looks like
// from here -- would make a Stop landing inside this call leave a running
// centrifuge node behind for the life of the process, with no reference to it
// anywhere.
func (c *components) startRealtime(cfg config, credentials *internalidentity.Authenticator) error {
	handler, err := clientlink.NewHandler(clientlink.Config{
		Authenticator: credentials,
		Authorizer:    cfg.authorizer,
		Admitter:      c.admissions,
		Demand:        c.demand,
		Clock:         cfg.clock,
		Limits:        clientlink.Limits(cfg.client),
		Version:       cfg.version,
	})
	if err != nil {
		return fmt.Errorf("factory: compose the ClientLink: %w", err)
	}
	c.realtimeMu.Lock()
	c.realtime = handler
	c.realtimeMu.Unlock()
	return nil
}

// realtimeHandler is the supplier the router consults per request. It answers
// nil before Start and after Stop, which the router renders as 503.
func (c *components) realtimeHandler() http.Handler {
	c.realtimeMu.RLock()
	defer c.realtimeMu.RUnlock()
	if c.realtime == nil {
		return nil
	}
	return c.realtime
}

// ---------------------------------------------------------------------------
// The adapters this composition owns.
// ---------------------------------------------------------------------------

// placementLinks is the adapter between internal/placement and the HostLink
// pool, and the ONE place a transport failure is classified into placement's
// vocabulary. It holds no state.
//
// The classification is the whole of its job, and each arm is a decision:
//
//   - A Host's own HostLinkError becomes *placement.AttachRefusal, whose code
//     placement branches on.
//   - A Host whose advertised BASE cannot carry the session's tenant's address
//     (hostlink.ErrNoTenantEndpoint, Core's HostLinkEndpoint refusal) becomes
//     placement.ErrTenantUnaddressable, with Core's typed refusal kept for the
//     log. Nothing was dialled, and it says nothing about another tenant:
//     placement skips the candidate for this tenant and asks the next.
//   - A method the Host did not advertise becomes ErrAttachUnsupported: the
//     pool refused it LOCALLY and nothing was sent, which is what lets a mixed
//     fleet exclude a Host that predates attach rather than read its
//     runtime_unavailable as a refusal.
//   - A failure BEFORE the request left this process -- a dial that failed, a
//     link between connections, a pool at its link ceiling, a link made
//     terminal by a wire-version change (ErrUnsupportedProtocol, which the
//     pool also evicts) -- becomes ErrHostUnreachable.
//   - The Host's own code-less ANSWER (hostlink.ErrHostFailed, a transport
//     error reply such as centrifuge's ErrorInternal) becomes
//     ErrAttachFailed, and placement asks the next candidate. That is NOT
//     because the Host rolled back: host v0.2.1 also answers this way when
//     its rollback was incomplete, and when the session IS resident but the
//     observation could not be published. It is safe because the session
//     LEASE guards residency: a Host still holding it makes the next
//     candidate refuse epoch_mismatch, so no second residency can form, and
//     placement converges on the owner. host v0.2.1's own comment asks a
//     Factory to treat this as no placement outcome but a retry, and says
//     "the attach is idempotent per key" -- a retry to the SAME Host. Moving
//     on to the next candidate is therefore a DIVERGENCE from that contract,
//     not conformance with it, and it is taken knowingly: it is safe only
//     because of the lease, as above.
//   - Everything else is returned as itself, and placement ABORTS on it: a
//     cancelled or timed-out request, or a lost connection, may have reached
//     the Host, and the Host may have acted.
type placementLinks struct{ pool *hostlink.Pool }

func (l placementLinks) Attach(ctx context.Context, endpoint sessionwire.InternalEndpoint, req sessionwire.HostLinkAttachRequest) (sessionwire.HostLinkRegistryObservation, error) {
	observation, err := l.pool.Attach(ctx, hostlink.Target{Host: req.HostID, Endpoint: endpoint}, req)
	return observation, classifyAttach(err)
}

func (l placementLinks) Bind(ctx context.Context, endpoint sessionwire.InternalEndpoint, req sessionwire.HostLinkBindRequest) error {
	return l.pool.Bind(ctx, hostlink.Target{Host: req.HostID, Endpoint: endpoint}, req)
}

func (l placementLinks) Unbind(ctx context.Context, req sessionwire.HostLinkUnbindRequest) error {
	return l.pool.Unbind(ctx, req)
}

func (l placementLinks) DeliverCommand(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, delivery sessionwire.HostLinkCommandDelivery) error {
	return l.pool.DeliverCommand(ctx, tenant, session, delivery)
}

func (l placementLinks) RouteFor(tenant sessionwire.TenantID, session sessionwire.SessionID) (sessionwire.HostID, bool) {
	return l.pool.RouteFor(tenant, session)
}

func (l placementLinks) AcceptsGateResponses(ctx context.Context, owner sessionwire.HostLinkRegistryObservation) (bool, error) {
	return gateResponders(l).AcceptsGateResponses(ctx, owner)
}

// gateResponders is the gate_response capability question asked of the pool:
// the owner's link FOR THE SESSION'S TENANT, at the address derived from the
// owner's advertised base, answered by hostlink.GateResponseCapable. Admission
// asks it before writing a gate response; placement asks it before delivering
// one as a wake.
type gateResponders struct{ pool *hostlink.Pool }

func (g gateResponders) AcceptsGateResponses(ctx context.Context, owner sessionwire.HostLinkRegistryObservation) (bool, error) {
	return g.pool.AcceptsGateResponses(ctx, hostlink.Target{Host: owner.HostID, Endpoint: owner.InternalEndpoint}, owner.TenantID)
}

// classifyAttach maps a pool attach failure onto placement's vocabulary. See
// placementLinks for what each arm means.
func classifyAttach(err error) error {
	if err == nil {
		return nil
	}
	var refusal *hostlink.HostRefusal
	if errors.As(err, &refusal) {
		return &placement.AttachRefusal{HostLinkError: refusal.HostLinkError}
	}
	if errors.Is(err, hostlink.ErrNoTenantEndpoint) {
		return fmt.Errorf("%w: %w", placement.ErrTenantUnaddressable, err)
	}
	if errors.Is(err, hostlink.ErrUnsupportedMethod) {
		return fmt.Errorf("%w: %w", placement.ErrAttachUnsupported, err)
	}
	if errors.Is(err, hostlink.ErrDialFailed) || errors.Is(err, hostlink.ErrLinkReconnecting) ||
		errors.Is(err, hostlink.ErrLinkLimit) || errors.Is(err, hostlink.ErrUnsupportedProtocol) {
		return fmt.Errorf("%w: %w", placement.ErrHostUnreachable, err)
	}
	if errors.Is(err, hostlink.ErrHostFailed) {
		return fmt.Errorf("%w: %w", placement.ErrAttachFailed, err)
	}
	return err
}

// logger is the composition's logger, defaulting to the process's.
func logger(cfg config) *slog.Logger {
	if cfg.logger != nil {
		return cfg.logger
	}
	return slog.Default()
}

// departmentTargets resolves a create's agent against the CONFIGURED launch
// targets, and against nothing else.
//
// Capacity is deliberately not consulted. A configured target is the immutable
// launch identity a create is pinned to; which Hosts currently have room is a
// placement question answered later and separately, and a resolver that mixed
// the two would make a session's durable identity depend on the fleet's state
// at the instant it was created.
type departmentTargets []LaunchTemplate

func (d departmentTargets) ResolveAgent(_ context.Context, agent sessionwire.AgentID) (admission.Target, bool, error) {
	for _, template := range d {
		if template.Key.AgentID == agent {
			return admission.Target{Key: template.Key, Workload: template.Workload}, true, nil
		}
	}
	return admission.Target{}, false, nil
}

func (d departmentTargets) IsKnown(_ context.Context, key sessionstore.HostTargetKey) (bool, error) {
	for _, template := range d {
		if template.Key == key {
			return true, nil
		}
	}
	return false, nil
}

// ErrNoHintPublisher was what a journal-tip hint reported while this module
// composed no hint publisher. Since v0.4.0 the hint is published to the
// session's ClientLink channel, the same one the live tail uses, and nothing
// returns this any more.
//
// Deprecated: nothing returns it; it is kept so a caller comparing against it
// still compiles.
var ErrNoHintPublisher = errors.New("factory: this replica composes no journal-tip hint publisher")

// sweep is one named periodic pass.
//
// Every sweeper this Server drives has the same shape -- one call, bounded
// inside itself, reporting a result this composition does not read -- so the
// cadence is one mechanism rather than three. The name travels with it so a
// failure is attributable.
type sweep struct {
	name string
	run  func(context.Context) error
}

// sweeps is every periodic pass, in a FIXED order.
//
// The order is not load-bearing for correctness -- each pass claims what it
// touches, and a claim licenses nothing -- but it is fixed so a test can state
// what this replica does rather than observe what it happened to do.
func (c *components) sweeps(cfg config) []sweep {
	log := logger(cfg)
	passes := []sweep{
		{name: "commands", run: func(ctx context.Context) error {
			result, err := c.commandSweeper.Sweep(ctx, cfg.service)
			warnTruncated(ctx, log, "commands", result.Shard, result.Truncated)
			return err
		}},
		// The disposition deadline sweep. Unconditional, unlike "placement":
		// it needs nothing beyond the required Commands seam, and without it a
		// command no Host applied would stay open forever.
		{name: "dispositions", run: func(ctx context.Context) error {
			result, err := c.dispositionSweeper.Sweep(ctx, cfg.service)
			warnTruncated(ctx, log, "dispositions", result.Shard, result.Truncated)
			return err
		}},
		{name: "gates", run: func(ctx context.Context) error {
			result, err := c.gateSweeper.Sweep(ctx, cfg.service)
			warnTruncated(ctx, log, "gates", result.Shard, result.Truncated)
			return err
		}},
		// The TARGET half of the record sweep and not the claim half. There is
		// no SessionRef enumerator in this module: TargetSweepResult reports
		// counts, SweepClaims takes a list of sessions, and nothing produces
		// one. Driving it with an empty list every interval would be a loop
		// that cannot do anything, so it is left undriven and booked as owed.
		{name: "records", run: func(ctx context.Context) error {
			_, err := c.records.SweepTargets(ctx)
			return err
		}},
	}
	if c.pending != nil {
		// B5's trigger, composed only when the durable query it reads was
		// supplied, so a composition without one runs exactly the sweeps it
		// always did.
		passes = append(passes, sweep{name: "placement", run: func(ctx context.Context) error {
			result, err := c.pending.Sweep(ctx, cfg.service)
			warnTruncated(ctx, log, "placement", result.Shard, result.Truncated)
			return err
		}})
	}
	return passes
}

// warnTruncated reports a pass that ended with a shard's backlog unread.
//
// A truncated pass is not a failure -- the rest of the shard is met by the next
// pass over it, and the disposition, gate and placement sweeps resume from
// where they stopped -- but it IS the signal that a shard's due view is
// outgrowing MaxDuePerSweep*MaxConcurrent, which is how a sweep that re-reads
// from the head starves. It used to be computed and dropped here, which made
// that condition silent.
func warnTruncated(ctx context.Context, log *slog.Logger, sweep string, shard int, truncated bool) {
	if !truncated {
		return
	}
	log.WarnContext(ctx, "sweep: a pass ended with the shard's due backlog unread",
		slog.String("sweep", sweep), slog.Int("shard", shard))
}

// idleLink keeps the pool's idle reaper on the same cadence as the sweeps.
func (c *components) reapIdle() { c.pool.ReapIdle() }

// resolveObjectStore adapts the public resolver to the router's signature.
//
// A nil resolver must stay nil, not become a function returning nil: the
// router branches on the FIELD to decide whether a non-zero binding can be
// served at all, and a non-nil function would turn that composition fact into
// a per-request failure.
func resolveObjectStore(r ObjectStoreResolver) func(context.Context, sessionstore.SessionBinding) (httpapi.ObjectReader, error) {
	if r == nil {
		return nil
	}
	return func(ctx context.Context, binding sessionstore.SessionBinding) (httpapi.ObjectReader, error) {
		return r(ctx, binding)
	}
}

// stopRealtime shuts the ClientLink node down and forgets it, so the router's
// supplier answers nil and a request arriving during shutdown is answered 503
// rather than handed to a node that is closing.
func (c *components) stopRealtime(ctx context.Context) error {
	c.realtimeMu.Lock()
	handler := c.realtime
	c.realtime = nil
	c.realtimeMu.Unlock()
	if handler == nil {
		return nil
	}
	return handler.Shutdown(ctx)
}

// dispositionHolder is the reconciliation-claim holder the disposition deadline
// sweep claims under: the replica's id, scoped to the sweep, so the sweep and
// this replica's placement never mistake each other's claim for their own.
//
// A holder is a bounded opaque id in the store (at most sessionwire.MaxIDBytes),
// and WithReplicaID bounds nothing, so a replica id too long to carry the
// suffix is replaced by its SHA-256 before the suffix is added: still stable
// for the process, still distinct from the replica's own id, and never refused
// by the store on every pass.
func dispositionHolder(replicaID string) string { return sweepHolder(replicaID, "/dispositions") }

// commandHolder is the legacy command deadline sweep's holder, derived the same
// way and for the same reason as dispositionHolder.
func commandHolder(replicaID string) string { return sweepHolder(replicaID, "/commands") }

// sweepHolder scopes the replica's id to one sweep, hashing an id too long to
// carry the suffix within sessionwire.MaxIDBytes.
func sweepHolder(replicaID, suffix string) string {
	if len(replicaID)+len(suffix) <= sessionwire.MaxIDBytes {
		return replicaID + suffix
	}
	sum := sha256.Sum256([]byte(replicaID))
	return hex.EncodeToString(sum[:]) + suffix
}
