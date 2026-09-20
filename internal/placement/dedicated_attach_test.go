package placement

import (
	"context"
	"errors"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
)

func TestDedicatedAttachRequiresPreAttachDiscovery(t *testing.T) {
	t.Parallel()
	f := newFixture(t, sessionwire.HostPlacementDedicated, "factory-1")
	f.reconciler.cfg.Links, f.reconciler.cfg.ActorID = newScriptedLinks(), testActor
	_, err := f.reconciler.Reconcile(context.Background(), Request{TenantID: testTenant, SessionID: testSession})
	if !errors.Is(err, ErrWorkloadEndpointUnsupported) {
		t.Fatalf("Reconcile without pre-attach discovery = %v, want ErrWorkloadEndpointUnsupported", err)
	}
}

func TestDedicatedOwnerFromAnotherGenerationIsNotReused(t *testing.T) {
	t.Parallel()
	f := newFixture(t, sessionwire.HostPlacementDedicated, "factory-1")
	f.putOwner(t, sessionwire.HostPlacementDedicated) // helper publishes generation 2; desire is generation 1.
	result, err := f.reconciler.Reconcile(context.Background(), Request{TenantID: testTenant, SessionID: testSession})
	if err != nil {
		t.Fatal(err)
	}
	if result.Decision.Outcome != OutcomeReconcileDedicated || len(f.controller.intents) != 1 {
		t.Fatalf("old-generation owner reused: result=%+v, ensures=%d", result, len(f.controller.intents))
	}
}

func TestDedicatedClaimLoserDoesNotReuseAnotherGeneration(t *testing.T) {
	t.Parallel()
	f := newFixture(t, sessionwire.HostPlacementDedicated, "factory-2")
	if _, err := f.store.AcquireReconciliationClaim(context.Background(), sessionstore.AcquireReconciliationClaimRequest{
		TenantID: testTenant, SessionID: testSession, HolderID: "factory-1", ExpiresAt: f.clock.now.Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	f.putOwner(t, sessionwire.HostPlacementDedicated) // generation 2, while the desire is generation 1.
	result, err := f.reconciler.Reconcile(context.Background(), Request{TenantID: testTenant, SessionID: testSession})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Deferred || result.Decision.Outcome == OutcomeReuseOwner || len(f.controller.intents) != 0 {
		t.Fatalf("claim loser reused wrong-generation owner: result=%+v, ensures=%d", result, len(f.controller.intents))
	}
}

func TestDedicatedAttachWaitsForReadyEndpointAndRejectsWrongFence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		ready  bool
		change func(*DedicatedEndpoint)
		want   error
	}{
		{"not ready", false, nil, nil},
		{"wrong generation", true, func(e *DedicatedEndpoint) { e.HostGeneration++ }, ErrDedicatedEndpointInvalid},
		{"pathful base", true, func(e *DedicatedEndpoint) { e.InternalEndpoint += "/hostlink" }, ErrDedicatedEndpointInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, sessionwire.HostPlacementDedicated, "factory-1")
			controller := &endpointController{recordingController: f.controller, ready: tc.ready,
				endpoint: testDedicatedEndpoint(f.generation(t))}
			if tc.change != nil {
				tc.change(&controller.endpoint)
			}
			links := newScriptedLinks()
			f.reconciler.cfg.Workloads, f.reconciler.cfg.Links, f.reconciler.cfg.ActorID = controller, links, testActor
			_, err := f.reconciler.Reconcile(context.Background(), Request{TenantID: testTenant, SessionID: testSession})
			if !errors.Is(err, tc.want) || len(links.attaches) != 0 {
				t.Fatalf("Reconcile = %v, attaches=%d; want error %v and no attach", err, len(links.attaches), tc.want)
			}
		})
	}
}

func TestDedicatedEpochMismatchNeverBindsTheHoldersEpoch(t *testing.T) {
	t.Parallel()
	f := newFixture(t, sessionwire.HostPlacementDedicated, "factory-1")
	controller := &endpointController{recordingController: f.controller, ready: true,
		endpoint: testDedicatedEndpoint(f.generation(t))}
	links := newScriptedLinks()
	links.on(controller.endpoint.HostID, refuse(sessionwire.HostLinkError{
		Code: sessionwire.HostLinkErrorEpochMismatch, CurrentLeaseEpoch: holderEpoch,
	}))
	f.reconciler.cfg.Workloads, f.reconciler.cfg.Links, f.reconciler.cfg.ActorID = controller, links, testActor
	result, err := f.reconciler.Reconcile(context.Background(), Request{TenantID: testTenant, SessionID: testSession})
	if !errors.Is(err, ErrRegistryStale) || len(links.binds) != 0 || len(result.Refused) != 1 {
		t.Fatalf("epoch mismatch = result %+v, error %v, binds=%d", result, err, len(links.binds))
	}
}

func TestDedicatedEpochMismatchRereadsTheOwner(t *testing.T) {
	t.Parallel()
	f := newFixture(t, sessionwire.HostPlacementDedicated, "factory-1")
	controller := &endpointController{recordingController: f.controller, ready: true,
		endpoint: testDedicatedEndpoint(f.generation(t))}
	links := newScriptedLinks()
	links.on(controller.endpoint.HostID, func(_ int, req sessionwire.HostLinkAttachRequest) (sessionwire.HostLinkRegistryObservation, error) {
		_, err := f.store.PutHostRegistration(context.Background(), sessionstore.PutHostRegistrationRequest{
			TenantID: testTenant, SessionID: testSession, LeaseEpoch: holderEpoch,
			ObservedAt: f.clock.now, ExpiresAt: f.clock.now.Add(time.Minute),
			Route: sessionstore.HostRoute{
				HostID: req.HostID, HostGeneration: req.HostGeneration, AgentID: testAgent,
				RuntimeCompatibilityID: testRuntime, Placement: sessionwire.HostPlacementDedicated,
				InternalEndpoint: controller.endpoint.InternalEndpoint,
				Residency:        sessionwire.SessionResidencyResident, Accepting: true,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		return sessionwire.HostLinkRegistryObservation{}, &AttachRefusal{HostLinkError: sessionwire.HostLinkError{
			Code: sessionwire.HostLinkErrorEpochMismatch, CurrentLeaseEpoch: holderEpoch,
		}}
	})
	f.reconciler.cfg.Workloads, f.reconciler.cfg.Links, f.reconciler.cfg.ActorID = controller, links, testActor
	result, err := f.reconciler.Reconcile(context.Background(), Request{TenantID: testTenant, SessionID: testSession})
	if err != nil || result.Decision.Outcome != OutcomeReuseOwner || result.Decision.Owner.LeaseEpoch != holderEpoch || len(links.binds) != 0 {
		t.Fatalf("owner after refusal = result %+v, err %v, binds %d", result, err, len(links.binds))
	}
}

func TestDedicatedAttachRejectsMismatchedSuccessfulObservation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*sessionwire.HostLinkRegistryObservation)
	}{
		{"missing epoch", func(o *sessionwire.HostLinkRegistryObservation) { o.LeaseEpoch = 0 }},
		{"different host", func(o *sessionwire.HostLinkRegistryObservation) { o.HostID = "other-host" }},
		{"different generation", func(o *sessionwire.HostLinkRegistryObservation) { o.HostGeneration++ }},
		{"different endpoint", func(o *sessionwire.HostLinkRegistryObservation) { o.InternalEndpoint = "wss://other.internal" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, sessionwire.HostPlacementDedicated, "factory-1")
			controller := &endpointController{recordingController: f.controller, ready: true,
				endpoint: testDedicatedEndpoint(f.generation(t))}
			links := newScriptedLinks()
			links.on(controller.endpoint.HostID, func(_ int, req sessionwire.HostLinkAttachRequest) (sessionwire.HostLinkRegistryObservation, error) {
				obs := acceptedObservation(req, attachedEpoch)
				obs.Placement, obs.InternalEndpoint = sessionwire.HostPlacementDedicated, controller.endpoint.InternalEndpoint
				tc.change(&obs)
				return obs, nil
			})
			f.reconciler.cfg.Workloads, f.reconciler.cfg.Links, f.reconciler.cfg.ActorID = controller, links, testActor
			_, err := f.reconciler.Reconcile(context.Background(), Request{TenantID: testTenant, SessionID: testSession})
			if !errors.Is(err, ErrDedicatedObservationInvalid) || len(links.binds) != 0 {
				t.Fatalf("mismatched success = %v, binds=%d; want invalid observation and no bind", err, len(links.binds))
			}
		})
	}
}

func TestDedicatedGateProbeTransportFailureIsTransient(t *testing.T) {
	t.Parallel()
	f := newFixture(t, sessionwire.HostPlacementDedicated, "factory-1")
	controller := &endpointController{recordingController: f.controller, ready: true,
		endpoint: testDedicatedEndpoint(f.generation(t))}
	links := newScriptedLinks()
	links.gateErr = ErrHostUnreachable
	f.reconciler.cfg.Workloads, f.reconciler.cfg.Links, f.reconciler.cfg.ActorID = controller, links, testActor
	result, err := f.reconciler.Reconcile(context.Background(), Request{
		TenantID: testTenant, SessionID: testSession, GateResponses: []sessionwire.CommandID{"gate-command"},
	})
	if err != nil || len(result.Unreachable) != 1 || len(result.Incapable) != 0 || len(links.attaches) != 0 {
		t.Fatalf("transient gate probe = result %+v, error %v, attaches %d", result, err, len(links.attaches))
	}
}

type endpointController struct {
	*recordingController
	endpoint DedicatedEndpoint
	ready    bool
	calls    int
	hook     func()
}

func (c *endpointController) WorkloadEndpoint(context.Context, sessionstore.PlacementIntent) (sessionwire.HostID, uint64, sessionwire.InternalEndpoint, bool, error) {
	c.calls++
	if c.hook != nil {
		c.hook()
	}
	return c.endpoint.HostID, c.endpoint.HostGeneration, c.endpoint.InternalEndpoint, c.ready, nil
}

func TestDedicatedAttachDoesNotUseAnEndpointAfterTheDesireMoves(t *testing.T) {
	t.Parallel()
	f := newFixture(t, sessionwire.HostPlacementDedicated, "factory-1")
	desired := &Desired{Placement: sessionwire.HostPlacementDedicated, RuntimeCompatibilityID: testRuntime,
		Workload: sessionstore.DesiredWorkload{PayloadVersion: "v1", Payload: []byte(`{"cpu":"2"}`)}}
	f.reconcile(t, desired)
	controller := &endpointController{recordingController: f.controller, ready: true,
		endpoint: testDedicatedEndpoint(f.generation(t))}
	controller.hook = func() {
		entry, err := f.store.GetCatalogEntry(context.Background(), sessionstore.GetCatalogEntryRequest{TenantID: testTenant, SessionID: testSession})
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.store.UpdateCatalogDesiredState(context.Background(), sessionstore.UpdateCatalogDesiredStateRequest{
			TenantID: testTenant, SessionID: testSession, ExpectedRevision: entry.Revision,
			IdempotencyKey: "new-dedicated-intent", DesiredPlacement: sessionwire.HostPlacementDedicated,
			RuntimeCompatibilityID: testRuntime,
			DesiredWorkload:        sessionstore.DesiredWorkload{PayloadVersion: "v1", Payload: []byte(`{"cpu":"4"}`)},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	links := newScriptedLinks()
	f.reconciler.cfg.Workloads, f.reconciler.cfg.Links, f.reconciler.cfg.ActorID = controller, links, testActor
	result, err := f.reconciler.Reconcile(context.Background(), Request{TenantID: testTenant, SessionID: testSession})
	if err != nil {
		t.Fatal(err)
	}
	if len(links.attaches) != 0 || result.Decision.Outcome != OutcomeUndecided {
		t.Fatalf("stale desired generation attached: result=%+v, attaches=%d", result, len(links.attaches))
	}
}

func TestDedicatedAttachDoesNotUseACapabilityProbeAfterDesireMoves(t *testing.T) {
	t.Parallel()
	f := newFixture(t, sessionwire.HostPlacementDedicated, "factory-1")
	controller := &endpointController{recordingController: f.controller, ready: true,
		endpoint: testDedicatedEndpoint(f.generation(t))}
	links := newScriptedLinks()
	links.gateCapable = true
	links.gateHook = func() {
		entry, err := f.store.GetCatalogEntry(context.Background(), sessionstore.GetCatalogEntryRequest{TenantID: testTenant, SessionID: testSession})
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.store.UpdateCatalogDesiredState(context.Background(), sessionstore.UpdateCatalogDesiredStateRequest{
			TenantID: testTenant, SessionID: testSession, ExpectedRevision: entry.Revision,
			IdempotencyKey: "new-intent-after-capability", DesiredPlacement: sessionwire.HostPlacementDedicated,
			RuntimeCompatibilityID: testRuntime,
			DesiredWorkload:        sessionstore.DesiredWorkload{PayloadVersion: "v1", Payload: []byte(`{"cpu":"8"}`)},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	f.reconciler.cfg.Workloads, f.reconciler.cfg.Links, f.reconciler.cfg.ActorID = controller, links, testActor
	result, err := f.reconciler.Reconcile(context.Background(), Request{
		TenantID: testTenant, SessionID: testSession, GateResponses: []sessionwire.CommandID{"gate-command"},
	})
	if err != nil || result.Decision.Outcome != OutcomeUndecided || len(links.attaches) != 0 {
		t.Fatalf("desire moved during capability probe: result=%+v, err=%v, attaches=%d", result, err, len(links.attaches))
	}
}

func TestDedicatedWorkloadAttachesAndBindsFromHostReply(t *testing.T) {
	t.Parallel()
	f := newFixture(t, sessionwire.HostPlacementDedicated, "factory-1")
	desired := &Desired{Placement: sessionwire.HostPlacementDedicated, RuntimeCompatibilityID: testRuntime,
		Workload: sessionstore.DesiredWorkload{PayloadVersion: "v1", Payload: []byte(`{"cpu":"2"}`)}}
	f.reconcile(t, desired)
	controller := &endpointController{recordingController: f.controller, ready: true,
		endpoint: testDedicatedEndpoint(f.generation(t))}
	links := newScriptedLinks()
	links.on("dedicated-host", func(_ int, req sessionwire.HostLinkAttachRequest) (sessionwire.HostLinkRegistryObservation, error) {
		observation := acceptedObservation(req, attachedEpoch)
		observation.Placement = sessionwire.HostPlacementDedicated
		observation.InternalEndpoint = controller.endpoint.InternalEndpoint
		return observation, nil
	})
	f.reconciler.cfg.Workloads = controller
	f.reconciler.cfg.Links = links
	f.reconciler.cfg.ActorID = testActor
	result, err := f.reconciler.Reconcile(context.Background(), Request{
		TenantID: testTenant, SessionID: testSession,
		Desired: desired,
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if controller.calls != 1 || len(links.attaches) != 1 || len(links.binds) != 1 {
		t.Fatalf("endpoint calls=%d, attaches=%d, binds=%d; want one each", controller.calls, len(links.attaches), len(links.binds))
	}
	if links.binds[0].req.LeaseEpoch != attachedEpoch || !result.Bound {
		t.Fatalf("bind = %+v, result = %+v; want Host-issued epoch %d", links.binds[0], result, attachedEpoch)
	}
	second, err := f.reconciler.Reconcile(context.Background(), Request{TenantID: testTenant, SessionID: testSession})
	if err != nil {
		t.Fatal(err)
	}
	if len(links.attaches) != 2 || links.attaches[1].IdempotencyKey != links.attaches[0].IdempotencyKey ||
		second.DesiredWrites != 0 {
		t.Fatalf("repeat attach keys = %+v, desired writes = %d; want same key and no write", links.attaches, second.DesiredWrites)
	}
}

func testDedicatedEndpoint(generation uint64) DedicatedEndpoint {
	return DedicatedEndpoint{HostID: "dedicated-host", HostGeneration: generation,
		InternalEndpoint: "wss://dedicated-host.internal"}
}
