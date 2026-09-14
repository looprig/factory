package factory

import (
	"context"
	"errors"
	"fmt"
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
	gateSweeper    *reconcile.GateSweeper
	placement      *placement.Reconciler
	records        *placement.RecordSweeper
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
	})
	if err != nil {
		return nil, &OptionError{Option: "WithReconcileLimits", Err: err}
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
		admissions:     service,
		pool:           pool,
		bindings:       bindings,
		demand:         demand,
		commandSweeper: commandSweeper,
		gateSweeper:    gateSweeper,
		placement:      placer,
		records:        records,
	}, nil
}

// startRealtime builds and RUNS the ClientLink node.
//
// It is here rather than in composeComponents because clientlink.NewHandler
// runs the centrifuge node before it returns -- deliberately, so a composition
// cannot succeed and then refuse every connection -- and a running node is a
// lifetime. Until it exists /v1/realtime answers 503 through realtimeGate,
// which is the same fail-closed shape the nil object policy has: composed,
// stated, and never a silent success.
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
	return []sweep{
		{name: "commands", run: func(ctx context.Context) error {
			_, err := c.commandSweeper.Sweep(ctx, cfg.service)
			return err
		}},
		{name: "gates", run: func(ctx context.Context) error {
			_, err := c.gateSweeper.Sweep(ctx, cfg.service)
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
