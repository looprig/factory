package factory_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory"
	"github.com/looprig/factory/identity"
	"github.com/looprig/sessionstore"
)

// This file holds the difference between COMPOSED and DRIVEN.
//
// Stage 1's suite proved that factory.New wires the read plane: substituting a
// different authorizer or a different reader at the composition site fails a
// case that names the wiring. Every component this stage adds is behind a
// BACKGROUND loop or an edge that stage 1 left answering 503, so "the
// constructor accepted it" says nothing at all about whether anything ever
// calls it.
//
// So each component below is probed at the seam it would actually touch: a
// durable call that RECORDS, and in several cases a seam that PANICS, which is
// the only probe that cannot be satisfied by a composition that merely built
// the value and dropped it.

// probe records what a composed component asked the durable plane for.
//
// Every method embeds FakeSeams, so a probe overrides only what it measures
// and a seam it does not name keeps answering zero values. That is what stops
// a probe from becoming a second fixture whose own shape decides the result.
type probe struct {
	factory.FakeSeams

	mu sync.Mutex

	dueCommands  int
	dueGates     int
	targetSweeps int
	admitted     []sessionstore.AdmitDispositionCommandRequest
	sweepHolders []string

	// duePage makes the due command view answer ONE settleable row, which is
	// what takes the sweep past its predicate and into the claim. Without it
	// every page is empty and no claim is ever attempted, so a case about the
	// claim holder would pass without reaching the holder at all.
	duePage bool

	// failDueCommands is how many due-command reads fail before one succeeds.
	failDueCommands int

	// target replaces the triple the catalog record names, so a case can put
	// a session on a launch target the deployment does not offer.
	target sessionstore.HostTargetKey

	// binding is the immutable SessionBinding the catalog record carries. A
	// non-zero one is what makes the router resolve an object store at all.
	binding sessionstore.SessionBinding

	// holdTargetSweep blocks a record sweep inside the store until it is
	// closed, which is how a case observes whether Stop WAITS.
	holdTargetSweep chan struct{}
	held            bool

	// panicOn names a seam method that must PANIC when it is reached. A probe
	// with one set is asserting reachability in the only way a composition
	// cannot fake: the panic escapes through whatever called it.
	panicOn string
}

// alive refuses the call when the context is already done.
//
// Every probe method consults it, and that is not tidiness: a fake that
// answered a dead context would make a composition which handed its loops a
// cancelled context look identical to one that did not. That exact mutant --
// deriving the sweep loop's context from the CALLER's rather than from
// Background -- survived this suite until the probe honoured its context, and
// a probe that ignores a context cannot measure anything about one.
func alive(ctx context.Context) error { return ctx.Err() }

func (p *probe) record(field *int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	*field++
}

func (p *probe) counts() (int, int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.dueCommands, p.dueGates, p.targetSweeps
}

func (p *probe) ListDueCommands(ctx context.Context, req sessionstore.ListDueCommandsRequest) (sessionstore.DueCommandPage, error) {
	if err := alive(ctx); err != nil {
		return sessionstore.DueCommandPage{}, err
	}
	if p.panicOn == "ListDueCommands" {
		panic("the command sweeper reached the composed due view")
	}
	p.mu.Lock()
	p.dueCommands++
	fail := p.failDueCommands > 0
	if fail {
		p.failDueCommands--
	}
	due := p.duePage
	p.mu.Unlock()
	if fail {
		return sessionstore.DueCommandPage{}, errProbeUnavailable
	}
	if !due {
		return sessionstore.DueCommandPage{}, nil
	}
	// A record whose state is neither applied, rejected nor applying, with no
	// live claim and an apply deadline in the past, is exactly the shape
	// admission's settleable predicate passes. Nothing else about it is read
	// before the claim.
	return sessionstore.DueCommandPage{
		Commands: []sessionstore.DueCommand{{Entry: sessionstore.InboxEntry{
			Record: sessionstore.InboxRecord{TenantID: "tenant-a", SessionID: "session-a"},
		}}},
		Examined: 1,
		Limit:    req.Limit,
	}, nil
}

// errProbeUnavailable is the dependency failure the loop must survive.
var errProbeUnavailable = errors.New("probe: the durable plane is unavailable")

func (p *probe) ListDueGates(ctx context.Context, _ sessionstore.ListDueGatesRequest) (sessionstore.DueGatePage, error) {
	if err := alive(ctx); err != nil {
		return sessionstore.DueGatePage{}, err
	}
	if p.panicOn == "ListDueGates" {
		panic("the gate sweeper reached the composed due view")
	}
	p.record(&p.dueGates)
	return sessionstore.DueGatePage{}, nil
}

func (p *probe) holding() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.held
}

func (p *probe) ReconcileHostTargets(ctx context.Context, _ sessionstore.ReconcileHostTargetsRequest) (sessionstore.HostTargetReconcileResult, error) {
	if err := alive(ctx); err != nil {
		return sessionstore.HostTargetReconcileResult{}, err
	}
	if p.holdTargetSweep != nil {
		p.mu.Lock()
		p.held = true
		p.mu.Unlock()
		<-p.holdTargetSweep
	}
	if p.panicOn == "ReconcileHostTargets" {
		panic("the record sweeper reached the composed target index")
	}
	p.record(&p.targetSweeps)
	return sessionstore.HostTargetReconcileResult{}, nil
}

// probeInstant is the fixture clock the catalog record is stamped with.
var probeInstant = time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)

// probeTemplate is the one launch target these cases configure, and the
// catalog record below names the SAME triple. Admission refuses a command for
// a session whose target this deployment does not offer, so a probe whose
// record and department disagreed would measure that refusal instead of the
// composition.
func probeTemplate() factory.LaunchTemplate {
	return factory.LaunchTemplate{
		Key: sessionstore.HostTargetKey{
			AgentID:                "agent-a",
			RuntimeCompatibilityID: "runtime-v1",
			Placement:              sessionwire.HostPlacementPooled,
		},
		Capabilities: []string{"gates"},
	}
}

func (p *probe) GetCatalogEntry(_ context.Context, req sessionstore.GetCatalogEntryRequest) (sessionstore.CatalogEntry, error) {
	target := p.target
	if target == (sessionstore.HostTargetKey{}) {
		target = probeTemplate().Key
	}
	return sessionstore.CatalogEntry{Record: sessionstore.CatalogRecord{
		TenantID:               req.TenantID,
		SessionID:              req.SessionID,
		AgentID:                target.AgentID,
		RuntimeCompatibilityID: target.RuntimeCompatibilityID,
		DesiredPlacement:       target.Placement,
		Binding:                p.binding,
		State:                  sessionwire.SessionStateIdle,
		Residency:              sessionwire.SessionResidencyCold,
		// The instants are REQUIRED, not decoration: the object route runs the
		// released canonicalizer over this record before it reaches any
		// policy, and a record with no timestamps is refused as invalid --
		// which would make a case about the resolver measure the canonicalizer
		// instead.
		CreatedAt:    probeInstant,
		LastActiveAt: probeInstant,
		// A record carrying a binding is canonicalized in full, and a zero
		// desired generation is refused. It is set here for the same reason
		// the instants are: so a case about the resolver measures the
		// resolver.
		DesiredGeneration: 1,
	}}, nil
}

// GetDispositionCommand answers NOT FOUND rather than a zero record.
//
// The difference decides the whole path: admission treats a found record as a
// retry of a command it already accepted and answers from it, so a probe
// returning a zero entry with a nil error would make every control request
// short-circuit before the durable write -- and a case asserting the write was
// reached would fail for a reason that has nothing to do with the composition.
func (p *probe) GetDispositionCommand(context.Context, sessionstore.GetDispositionCommandRequest) (sessionstore.DispositionInboxEntry, error) {
	return sessionstore.DispositionInboxEntry{}, &sessionstore.InboxError{Code: sessionstore.InboxErrorNotFound}
}

func (p *probe) AdmitDispositionCommand(_ context.Context, req sessionstore.AdmitDispositionCommandRequest) (sessionstore.DispositionInboxEntry, bool, error) {
	if p.panicOn == "AdmitDispositionCommand" {
		panic("a control route reached the composed durable command plane")
	}
	p.mu.Lock()
	p.admitted = append(p.admitted, req)
	p.mu.Unlock()
	return sessionstore.DispositionInboxEntry{}, true, nil
}

func (p *probe) AcquireReconciliationClaim(_ context.Context, req sessionstore.AcquireReconciliationClaimRequest) (sessionstore.ReconciliationClaimEntry, error) {
	p.mu.Lock()
	p.sweepHolders = append(p.sweepHolders, req.HolderID)
	p.mu.Unlock()
	return sessionstore.ReconciliationClaimEntry{}, nil
}

// composed builds a Server over the probe, with a cadence short enough that a
// test does not wait on the default five seconds.
//
// The interval is the ONLY value changed from the defaults, and it is changed
// through the public option rather than by reaching inside, so what these
// cases drive is a composition a deployment could build.
func composed(t *testing.T, p *probe, extra ...factory.Option) *factory.Server {
	t.Helper()

	limits := factory.DefaultReconcileLimits()
	limits.Interval = 5 * time.Millisecond
	limits.ClaimTTL = 50 * time.Millisecond
	limits.ApplyDeadline = 500 * time.Millisecond

	// WithReplicaID is dropped from the base list and supplied here, so a case
	// that wants to NAME the replica can pass its own without New rejecting a
	// duplicate. The default below is what every other case composes under.
	options := append(factory.RequiredOptionsExcept(
		"WithCommands", "WithGates", "WithHostTargets", "WithSessionReader",
		"WithReplicaID", "WithCatalog",
	),
		factory.WithReplicaID("replica-under-test"),
		factory.WithCommands(p),
		factory.WithGates(p),
		factory.WithHostTargets(p),
		factory.WithSessionReader(p),
		factory.WithReconcileLimits(limits),
		factory.WithCatalog(p),
		factory.WithDepartment(probeTemplate()),
	)
	server, err := factory.New(append(options, extra...)...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Stop(ctx); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	return server
}

// eventually waits for a condition the background loops produce.
//
// It fails with the condition's own name rather than a deadline, so a case
// that times out reports what did not happen rather than how long it waited.
func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("%s did not happen", what)
}

// TestStartDrivesEveryComposedSweep is the central claim of this stage.
//
// Each of the three counters is a DIFFERENT store call made by a DIFFERENT
// composed sweeper, so a composition that built one loop and dropped the other
// two fails on the two it dropped rather than passing on the one it kept.
//
// It asserts the calls and not a result, because every sweep here answers an
// empty page: what is being measured is that something reached the store at
// all, which is exactly what stage 1 could not claim.
func TestStartDrivesEveryComposedSweep(t *testing.T) {
	t.Parallel()

	p := &probe{}
	server := composed(t, p)
	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	eventually(t, "the command sweeper read the due command view", func() bool {
		commands, _, _ := p.counts()
		return commands > 0
	})
	eventually(t, "the gate sweeper read the due gate view", func() bool {
		_, gates, _ := p.counts()
		return gates > 0
	})
	eventually(t, "the record sweeper reconciled the Host target index", func() bool {
		_, _, targets := p.counts()
		return targets > 0
	})
}

// TestEachSweepRunsRepeatedlyRatherThanOnce separates a cadence from a single
// pass at startup.
//
// It is a separate case from the one above because the two failures are
// different: a composition that ran each sweeper exactly once at Start would
// satisfy every counter above forever, and the symptom in production would be
// a replica that reconciles for one interval after a restart and then never
// again.
func TestEachSweepRunsRepeatedlyRatherThanOnce(t *testing.T) {
	t.Parallel()

	p := &probe{}
	server := composed(t, p)
	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	eventually(t, "every sweep ran at least three times", func() bool {
		commands, gates, targets := p.counts()
		return commands >= 3 && gates >= 3 && targets >= 3
	})
}

// TestTheSweepsClaimUnderTheComposedReplicaIdentifier holds the one value that
// must be the same in three places.
//
// Extending one's own claim and taking over a crashed replica's lapsed one are
// the same store write, and the holder is what tells them apart; a record
// sweep releases only what this holder left behind. A composition that minted
// its own identifier per component would leave claims nothing could release,
// and nothing about a passing sweep would say so.
func TestTheSweepsClaimUnderTheComposedReplicaIdentifier(t *testing.T) {
	t.Parallel()

	p := &probe{}
	p.duePage = true
	server := composed(t, p)
	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	eventually(t, "a sweep took a reconciliation claim", func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return len(p.sweepHolders) > 0
	})
	p.mu.Lock()
	defer p.mu.Unlock()
	// Every holder is the replica's identifier or a sweep's scope of it
	// (v0.3.0: dispositions; v0.4.0: the legacy command sweep, regate L1) --
	// never an identifier minted independently of the composed one.
	derived := map[string]bool{
		"replica-under-test":              true,
		"replica-under-test/commands":     true,
		"replica-under-test/dispositions": true,
	}
	legacy := false
	for _, holder := range p.sweepHolders {
		if !derived[holder] {
			t.Fatalf("a claim names holder %q, want the composed replica identifier or a sweep's scope of it", holder)
		}
		legacy = legacy || holder == "replica-under-test/commands"
	}
	// The due row this probe answers is a LEGACY one, so the legacy sweep is
	// what claimed, and it must have claimed under its own holder (L1).
	if !legacy {
		t.Fatalf("no claim names the legacy sweep's holder: %v", p.sweepHolders)
	}
}

// TestAControlRouteAdmitsIntoTheComposedDurablePlane is A3.3's routes
// answering for real.
//
// The probe does not merely count: it keeps the AdmitDispositionCommand REQUEST, and the
// case asserts the CommandID and SessionID carried in the HTTP body arrive in
// it. That identity is what makes this a probe of this request rather than of
// the composition in general -- a background sweep cannot produce it, and a
// recording seam reached by something else would carry different values.
//
// A panic probe was tried here first and is NOT usable at this seam, which is
// worth recording rather than leaving for the next reader to rediscover:
// internal/httpapi wraps every handler in recoverPanic, so a seam that panics
// is converted into a 500 internal_error envelope -- indistinguishable from
// the several other ways this handler answers 500.
func TestAControlRouteAdmitsIntoTheComposedDurablePlane(t *testing.T) {
	t.Parallel()

	const (
		commandID = "11111111-1111-4111-8111-111111111111"
		sessionID = "22222222-2222-4222-8222-222222222222"
	)
	p := &probe{}
	server := composed(t, p)

	encoded, err := json.Marshal(map[string]any{
		"version": sessionwire.CurrentWireVersion, "command_id": commandID,
		"session_id": sessionID,
		"blocks":     []map[string]any{{"type": "text", "text": "hello"}},
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	request := httptest.NewRequest(http.MethodPost,
		trustedBase+"/v1/sessions/"+sessionID+"/input", strings.NewReader(string(encoded)))
	request.Header.Set("Authorization", "Bearer "+factory.FakeCredentialValue)
	request.Header.Set("Content-Type", "application/json")

	recorder := serveHandler(t, server.Handler(), request)
	if recorder.Code == http.StatusServiceUnavailable {
		t.Fatalf("the control route answered 503, so the composition supplies no admission plane: %s", recorder.Body)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.admitted) == 0 {
		t.Fatalf("nothing reached the composed durable command plane; the route answered %d %s", recorder.Code, recorder.Body)
	}
	admitted := p.admitted[0]
	if string(admitted.CommandID) != commandID || string(admitted.SessionID) != sessionID {
		t.Fatalf("the durable plane was asked for %q/%q, want the identities the request carried",
			admitted.CommandID, admitted.SessionID)
	}
	if admitted.Kind != sessionstore.CommandKind("input") {
		t.Fatalf("the durable plane was asked for kind %q, want the route's own kind", admitted.Kind)
	}
}

// TestRealtimeIsUnavailableBeforeStartAndNotNotImplemented is the composed
// half of /v1/realtime.
//
// Both halves are asserted because they fail differently. A 501 means the
// router still books the route to a later task -- the composition was never
// passed a ClientLink at all -- and a 503 means it was, and the node is not
// running yet. Only the second is what a Server between New and Start should
// say.
func TestRealtimeIsUnavailableBeforeStartAndNotNotImplemented(t *testing.T) {
	t.Parallel()

	server := composed(t, &probe{})
	recorder := serveHandler(t, server.Handler(), apiRequest(t, http.MethodGet, "/v1/realtime"))
	if recorder.Code == http.StatusNotImplemented {
		t.Fatal("/v1/realtime still answers 501, so the composition passes no ClientLink to the router")
	}
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 before Start", recorder.Code)
	}
	if code := decodeError(t, recorder); code != "unavailable" {
		t.Fatalf("code = %q, want unavailable", code)
	}
}

// TestRealtimeUpgradesOnceTheComposedClientLinkIsStarted is the other half.
//
// A WebSocket upgrade is asserted by its REFUSAL of a plain GET: the composed
// centrifuge handler answers 400 to a request carrying no upgrade headers,
// where the unstarted gate answers 503 and a pending route answers 501. Three
// distinguishable answers is what makes this a measurement rather than a
// tautology.
func TestRealtimeUpgradesOnceTheComposedClientLinkIsStarted(t *testing.T) {
	t.Parallel()

	server := composed(t, &probe{})
	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	recorder := serveHandler(t, server.Handler(), apiRequest(t, http.MethodGet, "/v1/realtime"))
	if recorder.Code == http.StatusServiceUnavailable || recorder.Code == http.StatusNotImplemented {
		t.Fatalf("status = %d after Start, so no ClientLink is serving", recorder.Code)
	}
}

// TestStopStopsPublicAdmissionBeforeItStopsAnythingElse is runbook step 4.
//
// It is asserted through the SURFACE rather than by reading Stop: after Stop
// returns, the realtime route answers 503 again, which is only true if the
// node was shut down and the router's supplier was cleared. A Stop that closed
// the listener and left the node running would pass a test that only checked
// the listener.
func TestStopStopsPublicAdmissionBeforeItStopsAnythingElse(t *testing.T) {
	t.Parallel()

	p := &probe{}
	server := composed(t, p)
	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	eventually(t, "the sweeps started", func() bool {
		commands, _, _ := p.counts()
		return commands > 0
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	recorder := serveHandler(t, server.Handler(), apiRequest(t, http.MethodGet, "/v1/realtime"))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("after Stop /v1/realtime answered %d, want 503", recorder.Code)
	}
}

// TestStopWaitsForEverySweepToReturn is the property that makes Stop mean
// something to a deployment.
//
// A Stop that cancelled the loops and returned would let a sweep still be
// writing to a store while the process exited, and the failure would appear as
// a claim taken by a replica that is gone. The probe counts calls after Stop
// returned; any at all is the defect.
func TestStopWaitsForEverySweepToReturn(t *testing.T) {
	t.Parallel()

	p := &probe{}
	server := composed(t, p)
	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	eventually(t, "the sweeps started", func() bool {
		commands, gates, targets := p.counts()
		return commands > 0 && gates > 0 && targets > 0
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	before := [3]int{}
	before[0], before[1], before[2] = p.counts()
	time.Sleep(50 * time.Millisecond)
	after := [3]int{}
	after[0], after[1], after[2] = p.counts()
	if before != after {
		t.Fatalf("a sweep was still running after Stop returned: %v then %v", before, after)
	}
}

// TestStopIsSafeOnAServerThatWasNeverStarted keeps Stop's idempotence claim
// true of the components this stage added, not only of the listener.
func TestStopIsSafeOnAServerThatWasNeverStarted(t *testing.T) {
	t.Parallel()

	server, err := factory.New(factory.RequiredOptions()...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Stop(ctx); err != nil {
		t.Fatalf("Stop on an unstarted Server: %v", err)
	}
	if err := server.Stop(ctx); err != nil {
		t.Fatalf("a second Stop: %v", err)
	}
	if err := server.Start(ctx); !errors.Is(err, factory.ErrServerStopped) {
		t.Fatalf("Start after Stop = %v, want ErrServerStopped", err)
	}
}

// TestASecondStartIsRefused holds the reason Start is not idempotent: two
// calls would run two sweep loops per pass and two ClientLink nodes on one
// composition, and neither is observable from outside.
func TestASecondStartIsRefused(t *testing.T) {
	t.Parallel()

	server := composed(t, &probe{})
	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := server.Start(context.Background()); !errors.Is(err, factory.ErrAlreadyStarted) {
		t.Fatalf("a second Start = %v, want ErrAlreadyStarted", err)
	}
}

// TestServeStartsTheBackgroundComponents is why a deployment that owns the
// socket through this module does not have to call Start.
//
// Without it a deployment calling only Serve would compose every reconciler
// and run none, and the symptom -- commands accepted and never settled --
// appears nowhere near the composition that caused it.
func TestServeStartsTheBackgroundComponents(t *testing.T) {
	t.Parallel()

	p := &probe{}
	server := composed(t, p)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	eventually(t, "Serve started the sweeps", func() bool {
		commands, _, _ := p.counts()
		return commands > 0
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := <-served; err != nil {
		t.Fatalf("Serve: %v", err)
	}
}

// TestASweepThatFailsDoesNotStopTheLoop is the operational property a periodic
// control plane needs: a store outage must delay work, not end it.
//
// The seam fails for the first few calls and then succeeds. A loop that
// returned on error would never reach the success, and the counter would stop
// at the failure count.
func TestASweepThatFailsDoesNotStopTheLoop(t *testing.T) {
	t.Parallel()

	p := &probe{failDueCommands: 3}
	server := composed(t, p)
	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	eventually(t, "the command sweep continued past its failures", func() bool {
		commands, _, _ := p.counts()
		return commands > 0
	})
}

// TestTheConfiguredWorkloadIsNeverPublished is why LaunchTemplate is a
// composition type rather than an alias of the published one.
//
// A dedicated target's Workload is a DEPLOYMENT's platform payload -- a pod
// spec, a job definition -- and /v1/agents is a tenant-facing response. The
// case asserts the agent IS listed and the payload is NOT anywhere in the
// body, so it fails both ways: a projection that dropped the agent and one
// that carried the payload through.
func TestTheConfiguredWorkloadIsNeverPublished(t *testing.T) {
	t.Parallel()

	const secret = "apiVersion: v1 # this must never reach a tenant"
	dedicated := factory.LaunchTemplate{
		Key: sessionstore.HostTargetKey{
			AgentID:                "agent-dedicated",
			RuntimeCompatibilityID: "runtime-v1",
			Placement:              sessionwire.HostPlacementDedicated,
		},
		Capabilities: []string{"gates"},
		Workload:     sessionstore.DesiredWorkload{PayloadVersion: "v1", Payload: []byte(secret)},
	}
	p := &probe{}
	options := append(factory.RequiredOptionsExcept("WithCommands", "WithGates", "WithHostTargets", "WithSessionReader", "WithCatalog"),
		factory.WithCommands(p), factory.WithGates(p), factory.WithHostTargets(p),
		factory.WithSessionReader(p), factory.WithCatalog(p),
		factory.WithDepartment(dedicated),
	)
	server, err := factory.New(options...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	recorder := serveHandler(t, server.Handler(), apiRequest(t, http.MethodGet, "/v1/agents"))
	body := recorder.Body.String()
	if recorder.Code != http.StatusOK {
		t.Fatalf("/v1/agents answered %d: %s", recorder.Code, body)
	}
	if !strings.Contains(body, "agent-dedicated") {
		t.Fatalf("the configured dedicated agent is not listed: %s", body)
	}
	// The capability strings are the OTHER half of the projection, and they
	// are asserted separately because they fail separately: a projection that
	// carried the identity and dropped the presentation would list an agent a
	// picker could not render, and the agent assertion above would pass.
	if !strings.Contains(body, "gates") {
		t.Fatalf("the configured capabilities were not published: %s", body)
	}
	if strings.Contains(body, secret) || strings.Contains(body, "apiVersion") {
		t.Fatalf("the configured workload payload reached a tenant response: %s", body)
	}
}

// TestTheComposedDepartmentIsCopiedFromTheCaller holds the deep copy.
//
// The published capability strings and the workload payload are both slices,
// so a shallow copy would let a composer that reuses its buffer rewrite what a
// running replica advertises -- and, for the workload, what a dedicated
// session would be created from.
func TestTheComposedDepartmentIsCopiedFromTheCaller(t *testing.T) {
	t.Parallel()

	capabilities := []string{"gates"}
	template := probeTemplate()
	template.Capabilities = capabilities
	p := &probe{}
	options := append(factory.RequiredOptionsExcept("WithCommands", "WithGates", "WithHostTargets", "WithSessionReader", "WithCatalog"),
		factory.WithCommands(p), factory.WithGates(p), factory.WithHostTargets(p),
		factory.WithSessionReader(p), factory.WithCatalog(p),
		factory.WithDepartment(template),
	)
	server, err := factory.New(options...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	capabilities[0] = "root-shell"
	recorder := serveHandler(t, server.Handler(), apiRequest(t, http.MethodGet, "/v1/agents"))
	if strings.Contains(recorder.Body.String(), "root-shell") {
		t.Fatalf("a running replica advertises a capability the caller wrote after New: %s", recorder.Body)
	}
}

// TestADedicatedTemplateNeedsAWorkloadAndAPooledOneMustNotHaveOne drives the
// one rule the composition type adds, in BOTH directions.
//
// The negative direction is the one worth having: a pooled session runs on a
// Host that already exists, so a workload configured for it is a value nothing
// would ever create, and accepting it silently would make a deployment believe
// it had configured something.
func TestADedicatedTemplateNeedsAWorkloadAndAPooledOneMustNotHaveOne(t *testing.T) {
	t.Parallel()

	workload := sessionstore.DesiredWorkload{PayloadVersion: "v1", Payload: []byte("spec")}
	for _, test := range []struct {
		name     string
		template factory.LaunchTemplate
		valid    bool
	}{
		{"a pooled target with no workload", factory.LaunchTemplate{
			Key: sessionstore.HostTargetKey{AgentID: "a", RuntimeCompatibilityID: "r", Placement: sessionwire.HostPlacementPooled},
		}, true},
		{"an omitted placement defaults to pooled", factory.LaunchTemplate{
			Key: sessionstore.HostTargetKey{AgentID: "a", RuntimeCompatibilityID: "r"},
		}, true},
		{"a pooled target with a workload", factory.LaunchTemplate{
			Key:      sessionstore.HostTargetKey{AgentID: "a", RuntimeCompatibilityID: "r", Placement: sessionwire.HostPlacementPooled},
			Workload: workload,
		}, false},
		{"a dedicated target with a workload", factory.LaunchTemplate{
			Key:      sessionstore.HostTargetKey{AgentID: "a", RuntimeCompatibilityID: "r", Placement: sessionwire.HostPlacementDedicated},
			Workload: workload,
		}, true},
		{"a dedicated target with no workload", factory.LaunchTemplate{
			Key: sessionstore.HostTargetKey{AgentID: "a", RuntimeCompatibilityID: "r", Placement: sessionwire.HostPlacementDedicated},
		}, false},
		{"a dedicated target with a payload but no version", factory.LaunchTemplate{
			Key:      sessionstore.HostTargetKey{AgentID: "a", RuntimeCompatibilityID: "r", Placement: sessionwire.HostPlacementDedicated},
			Workload: sessionstore.DesiredWorkload{Payload: []byte("spec")},
		}, false},
		// The identity half is still the published type's, deferred to rather
		// than restated. This row fails if that deferral is ever dropped.
		{"a target naming no agent", factory.LaunchTemplate{
			Key: sessionstore.HostTargetKey{RuntimeCompatibilityID: "r", Placement: sessionwire.HostPlacementPooled},
		}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := test.template.Validate()
			if test.valid && err != nil {
				t.Fatalf("Validate = %v, want nil", err)
			}
			if !test.valid && err == nil {
				t.Fatal("Validate accepted a template it must refuse")
			}
		})
	}
}

// TestAnObjectPolicyWithNoResolverIsRefusedAtComposition closes the trap stage
// 1's gate flagged for this stage.
//
// A nil object policy refuses an object read first and unconditionally, which
// is what makes the legacy-binding fallback unreachable today. Supplying a
// policy is exactly the change that makes it reachable, so composing one
// without a resolver behind it is refused where an operator sees it rather
// than discovered on the first read of a legacy-bound session.
func TestAnObjectPolicyWithNoResolverIsRefusedAtComposition(t *testing.T) {
	t.Parallel()

	policy := factory.ObjectPolicy(refusingObjectPolicy{})
	resolver := factory.ObjectStoreResolver(func(context.Context, sessionstore.SessionBinding) (factory.ObjectReader, error) {
		return nil, errProbeUnavailable
	})
	for _, test := range []struct {
		name    string
		options []factory.Option
		accept  bool
	}{
		{"neither", nil, true},
		{"a policy alone", []factory.Option{factory.WithObjectPolicy(policy)}, false},
		// A resolver alone is ACCEPTED, and deliberately: the nil policy still
		// refuses every object read first, so the resolver is unreachable and
		// the composition is not yet the one that makes the fallback live.
		{"a resolver alone", []factory.Option{factory.WithObjectStoreResolver(resolver)}, true},
		{"both", []factory.Option{factory.WithObjectPolicy(policy), factory.WithObjectStoreResolver(resolver)}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := factory.New(append(factory.RequiredOptions(), test.options...)...)
			switch {
			case test.accept && err != nil:
				t.Fatalf("New = %v, want a composition", err)
			case !test.accept && !errors.Is(err, factory.ErrObjectPolicyWithoutResolver):
				t.Fatalf("New = %v, want ErrObjectPolicyWithoutResolver", err)
			}
		})
	}
}

type refusingObjectPolicy struct{}

func (refusingObjectPolicy) AuthorizeReference(context.Context, identity.Principal, sessionstore.CatalogEntry, sessionwire.ObjectReference) (sessionstore.ObjectKind, error) {
	return "", errProbeUnavailable
}

// TestEveryNewRequiredSeamIsReportedWhenItIsMissing keeps the composition fail
// closed as it grew.
//
// Each seam is dropped ALONE, so a case names the one option that was not
// supplied rather than asserting that a composition missing everything fails.
func TestEveryNewRequiredSeamIsReportedWhenItIsMissing(t *testing.T) {
	t.Parallel()

	for _, name := range []string{
		"WithCatalog", "WithGates", "WithHostTargets",
		"WithHostLinkCredential", "WithServiceIdentity", "WithReplicaID",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := factory.New(factory.RequiredOptionsExcept(name)...)
			var missing *factory.MissingSeamsError
			if !errors.As(err, &missing) {
				t.Fatalf("New = %v, want a *MissingSeamsError", err)
			}
			if !slices.Contains(missing.Options, name) {
				t.Fatalf("the missing seams are %v, which does not name %s", missing.Options, name)
			}
			if len(missing.Options) != 1 {
				t.Fatalf("the missing seams are %v, want only %s", missing.Options, name)
			}
		})
	}
}

// TestASweepIdentityThatIsNotAServicePrincipalIsRefused is why the kind is
// checked at composition rather than trusted.
//
// Every sweep this Server drives is cross-tenant and calls AuthorizeServiceSweep
// with this value. A tenant principal reaching that call is a cross-tenant read
// authorized as one tenant's, and the authorizer is the deployer's -- so the
// composition must not be the place that lets it through.
func TestASweepIdentityThatIsNotAServicePrincipalIsRefused(t *testing.T) {
	t.Parallel()

	actor, err := identity.NewPrincipal("tenant-a", "someone", identity.KindActor)
	if err != nil {
		t.Fatalf("NewPrincipal: %v", err)
	}
	_, err = factory.New(append(factory.RequiredOptionsExcept("WithServiceIdentity"),
		factory.WithServiceIdentity(actor))...)
	if !errors.Is(err, factory.ErrNotAServiceIdentity) {
		t.Fatalf("New = %v, want ErrNotAServiceIdentity", err)
	}
}

// TestStartSurvivesACancelledCallerContext is the difference between bounding
// the START and bounding the LIFETIME.
//
// A Start made under a request context -- an operator's HTTP call, a
// supervisor's bounded boot step -- would stop every reconciler the moment
// that request ended if the loop context derived from it. The symptom is a
// replica that composed correctly, served for a moment, and silently stopped
// reconciling; nothing in a log would name the cause.
//
// The context here is cancelled BEFORE Start rather than during it, which is
// the sharper case: a loop context derived from it is already dead, so the
// sweeps never run at all.
func TestStartSurvivesACancelledCallerContext(t *testing.T) {
	t.Parallel()

	p := &probe{}
	server := composed(t, p)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := server.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	eventually(t, "the sweeps ran under a cancelled caller context", func() bool {
		commands, gates, targets := p.counts()
		return commands > 0 && gates > 0 && targets > 0
	})
}

// TestAnAdmittedCommandCarriesTheComposedApplyDeadline holds the one number
// that must be the same on both sides of the admission/reconciliation pair.
//
// The service stamps an accepted command's apply deadline and the command
// sweeper settles against it. Two values would be a command rejected before it
// was due, or one the sweeper never reached, and neither is visible in any
// response.
func TestAnAdmittedCommandCarriesTheComposedApplyDeadline(t *testing.T) {
	t.Parallel()

	limits := factory.DefaultReconcileLimits()
	limits.Interval = 5 * time.Millisecond
	limits.ClaimTTL = 50 * time.Millisecond
	limits.ApplyDeadline = 1234 * time.Millisecond

	p := &probe{}
	options := append(factory.RequiredOptionsExcept(
		"WithCommands", "WithGates", "WithHostTargets", "WithSessionReader", "WithCatalog"),
		factory.WithCommands(p), factory.WithGates(p), factory.WithHostTargets(p),
		factory.WithSessionReader(p), factory.WithCatalog(p),
		factory.WithDepartment(probeTemplate()),
		factory.WithReconcileLimits(limits),
	)
	server, err := factory.New(options...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	const sessionID = "22222222-2222-4222-8222-222222222222"
	encoded, err := json.Marshal(map[string]any{
		"version": sessionwire.CurrentWireVersion, "command_id": "11111111-1111-4111-8111-111111111111",
		"session_id": sessionID,
		"blocks":     []map[string]any{{"type": "text", "text": "hello"}},
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	request := httptest.NewRequest(http.MethodPost,
		trustedBase+"/v1/sessions/"+sessionID+"/input", strings.NewReader(string(encoded)))
	request.Header.Set("Authorization", "Bearer "+factory.FakeCredentialValue)
	request.Header.Set("Content-Type", "application/json")
	serveHandler(t, server.Handler(), request)

	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.admitted) == 0 {
		t.Fatal("nothing was admitted, so the deadline has no subject")
	}
	admitted := p.admitted[0]
	if got := admitted.ApplyDeadline.Sub(admitted.AcceptedAt); got != limits.ApplyDeadline {
		t.Fatalf("the admitted command's apply window is %v, want the composed %v", got, limits.ApplyDeadline)
	}
}

// TestAControlForASessionWhoseTargetIsNotConfiguredIsRefused is the Department
// acting as a gate on admission rather than only as a published list.
//
// It is the case a ONE-template fixture cannot produce. With a single
// configured target every "is this target known" question has the same answer,
// so a resolver that answered yes unconditionally would pass -- measured: that
// exact mutant survived until this case existed. Two templates and a record
// naming NEITHER is what makes the question discriminating.
func TestAControlForASessionWhoseTargetIsNotConfiguredIsRefused(t *testing.T) {
	t.Parallel()

	second := probeTemplate()
	second.Key.AgentID = "agent-b"

	for _, test := range []struct {
		name    string
		record  sessionstore.HostTargetKey
		refused bool
	}{
		{"a record naming the first configured target", probeTemplate().Key, false},
		{"a record naming the second configured target", second.Key, false},
		{"a record naming an agent nothing configures", sessionstore.HostTargetKey{
			AgentID: "agent-unknown", RuntimeCompatibilityID: "runtime-v1", Placement: sessionwire.HostPlacementPooled,
		}, true},
		// The same agent under a DIFFERENT compatibility boundary. It shares
		// two of the triple's three members with a configured target, so a
		// resolver comparing the agent alone would admit it.
		{"a configured agent on an unconfigured runtime", sessionstore.HostTargetKey{
			AgentID: "agent-a", RuntimeCompatibilityID: "runtime-v2", Placement: sessionwire.HostPlacementPooled,
		}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			p := &probe{target: test.record}
			options := append(factory.RequiredOptionsExcept(
				"WithCommands", "WithGates", "WithHostTargets", "WithSessionReader", "WithCatalog"),
				factory.WithCommands(p), factory.WithGates(p), factory.WithHostTargets(p),
				factory.WithSessionReader(p), factory.WithCatalog(p),
				factory.WithDepartment(probeTemplate(), second),
			)
			server, err := factory.New(options...)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			recorder := serveHandler(t, server.Handler(), inputRequest(t, "22222222-2222-4222-8222-222222222222"))

			p.mu.Lock()
			admitted := len(p.admitted)
			p.mu.Unlock()
			if test.refused && admitted != 0 {
				t.Fatalf("a session whose target this deployment does not offer was admitted (status %d)", recorder.Code)
			}
			if !test.refused && admitted == 0 {
				t.Fatalf("a session whose target IS configured was refused: %d %s", recorder.Code, recorder.Body)
			}
		})
	}
}

// inputRequest is one authenticated V1 input command for a session.
func inputRequest(t *testing.T, session string) *http.Request {
	t.Helper()

	encoded, err := json.Marshal(map[string]any{
		"version": sessionwire.CurrentWireVersion, "command_id": "11111111-1111-4111-8111-111111111111",
		"session_id": session,
		"blocks":     []map[string]any{{"type": "text", "text": "hello"}},
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	request := httptest.NewRequest(http.MethodPost,
		trustedBase+"/v1/sessions/"+session+"/input", strings.NewReader(string(encoded)))
	request.Header.Set("Authorization", "Bearer "+factory.FakeCredentialValue)
	request.Header.Set("Content-Type", "application/json")
	return request
}

// TestABoundSessionsObjectReadGoesThroughTheComposedResolver is the other half
// of the trap stage 1's gate flagged.
//
// The record carries a NON-ZERO SessionBinding, which is the only case in
// which the router resolves a store at all: a zero (legacy) binding falls back
// to the read plane. So this drives the path that composing an ObjectPolicy
// makes live, and asserts the binding the resolver is handed is the record's
// own rather than a default.
func TestABoundSessionsObjectReadGoesThroughTheComposedResolver(t *testing.T) {
	t.Parallel()

	binding := sessionstore.SessionBinding{
		StorageBindingID: "primary", BindingVersion: "v1",
		RuntimeSessionID: "33333333-3333-4333-8333-333333333333",
		ProtocolMode:     sessionstore.ProtocolModeDisposition,
	}
	var (
		mu       sync.Mutex
		resolved []sessionstore.SessionBinding
	)
	p := &probe{binding: binding}
	options := append(factory.RequiredOptionsExcept(
		"WithCommands", "WithGates", "WithHostTargets", "WithSessionReader", "WithCatalog"),
		factory.WithCommands(p), factory.WithGates(p), factory.WithHostTargets(p),
		factory.WithSessionReader(p), factory.WithCatalog(p),
		factory.WithDepartment(probeTemplate()),
		factory.WithObjectPolicy(grantingObjectPolicy{}),
		factory.WithObjectStoreResolver(func(_ context.Context, b sessionstore.SessionBinding) (factory.ObjectReader, error) {
			mu.Lock()
			resolved = append(resolved, b)
			mu.Unlock()
			return nil, errProbeUnavailable
		}),
	)
	server, err := factory.New(options...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	serveHandler(t, server.Handler(),
		apiRequest(t, http.MethodGet, "/v1/sessions/22222222-2222-4222-8222-222222222222/objects/obj-1/metadata"))

	mu.Lock()
	defer mu.Unlock()
	if len(resolved) == 0 {
		t.Fatal("a bound session's object read did not reach the composed resolver")
	}
	if resolved[0] != binding {
		t.Fatalf("the resolver was handed %+v, want the record's own binding %+v", resolved[0], binding)
	}
}

type grantingObjectPolicy struct{}

func (grantingObjectPolicy) AuthorizeReference(context.Context, identity.Principal, sessionstore.CatalogEntry, sessionwire.ObjectReference) (sessionstore.ObjectKind, error) {
	return sessionstore.ObjectKindCommandPayload, nil
}

// TestStopWaitsForASweepThatIsStillInFlight is the sharp form of the wait.
//
// The earlier case only showed that no FURTHER pass began, which a Stop that
// cancelled and returned immediately also satisfies -- measured: a mutant
// removing the wait survived it. Here a sweep is HELD inside the store call
// while Stop is called, so a Stop that did not wait returns while the sweep is
// still in it, and the case sees the ordering directly.
func TestStopWaitsForASweepThatIsStillInFlight(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	p := &probe{holdTargetSweep: release}
	server := composed(t, p)
	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	eventually(t, "a record sweep entered the store and is being held", func() bool {
		return p.holding()
	})

	stopped := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		stopped <- server.Stop(ctx)
	}()

	select {
	case err := <-stopped:
		t.Fatalf("Stop returned (%v) while a sweep was still inside the store", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("Stop: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Stop did not return after the held sweep was released")
	}
}

// stubPublicCreates is a create plane that admits nothing. Composition never
// calls it, so what it answers does not matter; that it is NON-NIL does.
type stubPublicCreates struct{}

var errNoCreatePlane = errors.New("stub create plane: this composition admits no create")

func (stubPublicCreates) PreparePublicCreate(context.Context, sessionstore.PreparePublicCreateRequest) (sessionstore.PublicCreatePreparation, error) {
	return sessionstore.PublicCreatePreparation{}, errNoCreatePlane
}

func (stubPublicCreates) PutCommandPayload(context.Context, sessionstore.PutCommandPayloadRequest) (sessionwire.ObjectMetadata, error) {
	return sessionwire.ObjectMetadata{}, errNoCreatePlane
}

func (stubPublicCreates) AdmitPublicCreate(context.Context, sessionstore.AdmitPublicCreateRequest) (sessionstore.DispositionInboxEntry, bool, error) {
	return sessionstore.DispositionInboxEntry{}, false, errNoCreatePlane
}

// TestTheCreateCompositionIsRefusedUnlessEveryHalfIsPresent rows the
// ENUMERATION of create compositions rather than the one that works.
//
// Three options interact and the rules are not symmetric, which is exactly the
// shape a single happy-path test would miss:
//
//   - Neither half is a supported composition: a Factory that serves no create
//     is a deployment choice, not a defect, and every other operation works.
//   - EITHER half alone is refused. A deployment that composed one meant to
//     serve creates, and learning that at composition beats learning it from
//     the first caller's runtime_unavailable.
//   - A binding with no ObjectStoreResolver is refused for a stronger reason
//     than the object policy's: a binding is IMMUTABLE AFTER CREATE, so a
//     session pinned to storage this deployment cannot resolve has permanently
//     unreadable objects.
func TestTheCreateCompositionIsRefusedUnlessEveryHalfIsPresent(t *testing.T) {
	t.Parallel()

	resolver := factory.WithObjectStoreResolver(func(context.Context, sessionstore.SessionBinding) (factory.ObjectReader, error) {
		return nil, errProbeUnavailable
	})
	binding := factory.WithSessionBinding("storage-a", "v1")
	plane := factory.WithPublicCreates(stubPublicCreates{})

	for _, test := range []struct {
		name    string
		options []factory.Option
		want    error
	}{
		{"no create composition at all", nil, nil},
		{"every half", []factory.Option{resolver, binding, plane}, nil},
		// A plane alone is accepted: it pins nothing, so nothing is
		// irreversible, and admission refuses the create for want of a
		// binding. This row is what stops the pairing rule being written as
		// "both or neither" when it is not.
		{"a plane alone", []factory.Option{plane}, factory.ErrCreatePlaneIncomplete},
		{"a binding with a plane but no resolver", []factory.Option{binding, plane}, factory.ErrSessionBindingWithoutResolver},
		{"a binding with a resolver but no plane", []factory.Option{resolver, binding}, factory.ErrCreatePlaneIncomplete},
		{"a binding alone", []factory.Option{binding}, factory.ErrSessionBindingWithoutResolver},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := factory.New(append(factory.RequiredOptions(), test.options...)...)
			if test.want == nil {
				if err != nil {
					t.Fatalf("New = %v, want a composition", err)
				}
				return
			}
			if !errors.Is(err, test.want) {
				t.Fatalf("New = %v, want %v", err, test.want)
			}
		})
	}
}

// TestAnIncompleteSessionBindingIsRefusedByTheOption rows the enumeration of
// half-filled bindings. A binding is immutable after create, so an empty member
// cannot be filled in later: it has to be refused at the option.
func TestAnIncompleteSessionBindingIsRefusedByTheOption(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name             string
		storage, version string
		accept           bool
	}{
		{"both members", "storage-a", "v1", true},
		{"no storage binding", "", "v1", false},
		{"no version", "storage-a", "", false},
		{"neither member", "", "", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			options := append(factory.RequiredOptions(),
				factory.WithObjectStoreResolver(func(context.Context, sessionstore.SessionBinding) (factory.ObjectReader, error) {
					return nil, errProbeUnavailable
				}),
				factory.WithPublicCreates(stubPublicCreates{}),
				factory.WithSessionBinding(test.storage, test.version))
			_, err := factory.New(options...)
			if test.accept {
				if err != nil {
					t.Fatalf("New = %v, want a composition", err)
				}
				return
			}
			if !errors.Is(err, factory.ErrIncompleteSessionBinding) {
				t.Fatalf("New = %v, want ErrIncompleteSessionBinding", err)
			}
		})
	}
}
