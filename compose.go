package factory

import (
	"context"
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
	})
	if err != nil {
		return nil, &OptionError{Option: "WithReconcileLimits", Err: err}
	}

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

	// The Binder adapter internal/routing's own documentation books to this
	// task. The pool takes its target as a struct because the identity and the
	// address are unusable apart; the routing table names only Core, so it
	// passes the endpoint beside the request. Composing the two is this one
	// function and no state.
	bindings, err := routing.NewBindings(cfg.directory, poolBinder{pool: pool})
	if err != nil {
		return nil, &OptionError{Option: "WithDirectory", Err: err}
	}
	demand, err := routing.NewDemand(bindings, cfg.reads, unpublishedHints{}, cfg.clock, routing.DemandLimits{
		OwnershipPollInterval: cfg.client.DemandReleaseDebounce + cfg.reconcile.Interval,
		PollTimeout:           cfg.client.DemandTimeout,
	})
	if err != nil {
		return nil, &OptionError{Option: "WithClientLinkLimits", Err: err}
	}

	commandSweeper, err := admission.NewReconciler(admission.ReconcilerConfig{
		Authorizer: cfg.authorizer,
		Due:        cfg.commands,
		Settlement: cfg.commands,
		Claims:     cfg.commands,
		Clock:      cfg.clock,
		HolderID:   cfg.replicaID,
		ClaimTTL:   cfg.reconcile.ClaimTTL,
		PageLimit:  cfg.reconcile.MaxDuePerSweep,
		MaxPages:   cfg.reconcile.MaxConcurrent,
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
		HolderID:   cfg.replicaID,
		ClaimTTL:   cfg.reconcile.ClaimTTL,
		PageLimit:  cfg.reconcile.MaxDuePerSweep,
		MaxPages:   cfg.reconcile.MaxConcurrent,
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

	return &components{
		admissions:         service,
		pool:               pool,
		bindings:           bindings,
		demand:             demand,
		commandSweeper:     commandSweeper,
		dispositionSweeper: dispositionSweeper,
		gateSweeper:        gateSweeper,
		placement:          placer,
		records:            records,
		pending:            pending,
	}, nil
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

// poolBinder is the one adapter between the routing table and the HostLink
// pool. It holds no state, so there is no second answer to "which Host serves
// this session" for it to hold.
type poolBinder struct{ pool *hostlink.Pool }

func (b poolBinder) Bind(ctx context.Context, endpoint sessionwire.InternalEndpoint, req sessionwire.HostLinkBindRequest) error {
	return b.pool.Bind(ctx, hostlink.Target{Host: req.HostID, Endpoint: endpoint}, req)
}

func (b poolBinder) Unbind(ctx context.Context, req sessionwire.HostLinkUnbindRequest) error {
	return b.pool.Unbind(ctx, req)
}

func (b poolBinder) DeliverCommand(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, delivery sessionwire.HostLinkCommandDelivery) error {
	return b.pool.DeliverCommand(ctx, tenant, session, delivery)
}

// placementLinks is the adapter between internal/placement and the HostLink
// pool, and the ONE place a transport failure is classified into placement's
// vocabulary. It holds no state.
//
// The classification is the whole of its job, and each arm is a decision:
//
//   - A Host's own HostLinkError becomes *placement.AttachRefusal, whose code
//     placement branches on.
//   - A method the Host did not advertise becomes ErrAttachUnsupported: the
//     pool refused it LOCALLY and nothing was sent, which is what lets a mixed
//     fleet exclude a Host that predates attach rather than read its
//     runtime_unavailable as a refusal.
//   - A failure BEFORE the request left this process -- a dial that failed, a
//     link between connections, a pool at its link ceiling -- becomes
//     ErrHostUnreachable, the one transport failure placement moves past.
//   - Everything else is returned as itself, and placement ABORTS on it: the
//     request may have reached the Host, and the Host may have acted.
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
	if errors.Is(err, hostlink.ErrUnsupportedMethod) {
		return fmt.Errorf("%w: %w", placement.ErrAttachUnsupported, err)
	}
	if errors.Is(err, hostlink.ErrDialFailed) || errors.Is(err, hostlink.ErrLinkReconnecting) || errors.Is(err, hostlink.ErrLinkLimit) {
		return fmt.Errorf("%w: %w", placement.ErrHostUnreachable, err)
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

// unpublishedHints is the journal-tip hint publisher this replica does not
// have, and it is a NAMED refusal rather than a convenient no-op.
//
// internal/realtime/clientlink registers no OnPublish handler and exports no
// publication surface at all, by design: A6.1's node is a command and
// subscription plane. So there is nowhere for a hint to go, and
// routing.Demand's poll already ignores the result because a hint is best
// effort -- a client that receives none reads the range it is missing through
// the durable query plane, which authorizes it in its own right.
//
// What is LOST is latency, not correctness, and it is on the owed list rather
// than hidden here: a viewer on this replica learns of work started elsewhere
// when it next reads, instead of when the tip moves.
type unpublishedHints struct{}

// ErrNoHintPublisher is what a hint publish reports. It is returned rather
// than nil so that a future composition which forgets to replace this cannot
// look like one that succeeded.
var ErrNoHintPublisher = errors.New("factory: this replica composes no journal-tip hint publisher")

func (unpublishedHints) PublishJournalTip(context.Context, sessionwire.JournalTip) error {
	return ErrNoHintPublisher
}

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
