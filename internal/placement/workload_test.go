package placement

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
)

// lifecycleProbe is intentionally domain-only. A platform adapter implements
// this shape without importing a platform SDK, and the placement package can
// exercise every lifecycle operation against the records Factory and Host
// already exchange.
type lifecycleProbe struct {
	ensures  []sessionstore.PlacementIntent
	observes []sessionstore.PlacementIntent
	drains   []sessionstore.PlacementIntent
	deletes  []sessionstore.PlacementIntent
	intent   sessionstore.PlacementIntent
	set      bool
	state    sessionwire.HostLinkDrainState
	deleted  bool
}

func (p *lifecycleProbe) EnsureWorkload(_ context.Context, intent sessionstore.PlacementIntent) error {
	if p.set && !reflect.DeepEqual(p.intent, intent) {
		return errors.New("workload intent changed")
	}
	p.intent = intent
	p.set = true
	p.ensures = append(p.ensures, intent)
	return nil
}

func (p *lifecycleProbe) ObserveWorkload(_ context.Context, intent sessionstore.PlacementIntent) (sessionwire.HostLinkRegistryObservation, bool, error) {
	p.observes = append(p.observes, intent)
	if p.deleted || !p.set {
		return sessionwire.HostLinkRegistryObservation{}, false, nil
	}
	return sessionwire.HostLinkRegistryObservation{
		Version: sessionwire.CurrentWireVersion, TenantID: intent.TenantID, SessionID: intent.SessionID,
		HostID: "host-dedicated", HostGeneration: 1, AgentID: intent.AgentID,
		RuntimeCompatibilityID: intent.RuntimeCompatibilityID, Placement: intent.Placement,
		InternalEndpoint: "wss://host-dedicated.internal/hostlink", Residency: sessionwire.SessionResidencyResident,
		Accepting: p.state == "", LeaseEpoch: 1,
		ObservedAt: time.Unix(1, 0).UTC(), ExpiresAt: time.Unix(2, 0).UTC(),
	}, true, nil
}

func (p *lifecycleProbe) RequestDrain(_ context.Context, intent sessionstore.PlacementIntent) (sessionwire.HostLinkDrainObservation, error) {
	if !p.set || p.deleted {
		return sessionwire.HostLinkDrainObservation{}, errors.New("workload is absent")
	}
	p.drains = append(p.drains, intent)
	if p.state == "" {
		p.state = sessionwire.HostLinkDrainStateDraining
	}
	return sessionwire.HostLinkDrainObservation{
		HostID: "host-dedicated", HostGeneration: 1, DrainGeneration: 1,
		State: p.state, TenantID: intent.TenantID, SessionID: intent.SessionID,
	}, nil
}

func (p *lifecycleProbe) DeleteWorkload(_ context.Context, intent sessionstore.PlacementIntent) error {
	if p.deleted {
		p.deletes = append(p.deletes, intent)
		return nil
	}
	if p.state != sessionwire.HostLinkDrainStateDrained {
		return errors.New("workload has not completed drain")
	}
	p.deletes = append(p.deletes, intent)
	p.deleted = true
	return nil
}

func (p *lifecycleProbe) completeDrain() { p.state = sessionwire.HostLinkDrainStateDrained }

var _ WorkloadController = (*lifecycleProbe)(nil)

func testWorkloadIntent() sessionstore.PlacementIntent {
	return sessionstore.PlacementIntent{
		TenantID:               "tenant/unsafe",
		SessionID:              "session/unsafe",
		AgentID:                "agent/unsafe",
		RuntimeCompatibilityID: "runtime/unsafe",
		Placement:              sessionwire.HostPlacementDedicated,
		Generation:             7,
		Workload:               sessionstore.DesiredWorkload{PayloadVersion: "workload/v1", Payload: []byte("opaque")},
	}
}

func TestWorkloadControllerExposesAllDomainLifecycleOperations(t *testing.T) {
	probe := &lifecycleProbe{}
	intent := testWorkloadIntent()
	ctx := context.Background()
	var controller WorkloadController = probe

	if _, found, err := controller.ObserveWorkload(ctx, intent); err != nil || found {
		t.Fatalf("ObserveWorkload before Ensure = found %t, err %v, want absent", found, err)
	}
	if err := controller.DeleteWorkload(ctx, intent); err == nil {
		t.Fatal("DeleteWorkload before Ensure succeeded, want absent refusal")
	}
	if err := controller.EnsureWorkload(ctx, intent); err != nil {
		t.Fatalf("EnsureWorkload: %v", err)
	}
	if err := controller.EnsureWorkload(ctx, intent); err != nil {
		t.Fatalf("repeated EnsureWorkload: %v", err)
	}
	if observed, found, err := controller.ObserveWorkload(ctx, intent); err != nil || !found || !observed.Accepting {
		t.Fatalf("ObserveWorkload after Ensure = %+v, found %t, err %v, want accepting workload", observed, found, err)
	}
	if drain, err := controller.RequestDrain(ctx, intent); err != nil || drain.State != sessionwire.HostLinkDrainStateDraining {
		t.Fatalf("RequestDrain = %+v, err %v, want draining", drain, err)
	}
	if observed, found, err := controller.ObserveWorkload(ctx, intent); err != nil || !found || observed.Accepting {
		t.Fatalf("ObserveWorkload while draining = %+v, found %t, err %v, want non-accepting workload", observed, found, err)
	}
	if err := controller.DeleteWorkload(ctx, intent); err == nil {
		t.Fatal("DeleteWorkload before completed drain succeeded")
	}
	if drain, err := controller.RequestDrain(ctx, intent); err != nil || drain.State != sessionwire.HostLinkDrainStateDraining {
		t.Fatalf("repeated RequestDrain = %+v, err %v, want idempotent draining", drain, err)
	}
	probe.completeDrain()
	if drain, err := controller.RequestDrain(ctx, intent); err != nil || drain.State != sessionwire.HostLinkDrainStateDrained {
		t.Fatalf("completed RequestDrain = %+v, err %v, want drained", drain, err)
	}
	if err := controller.DeleteWorkload(ctx, intent); err != nil {
		t.Fatalf("DeleteWorkload: %v", err)
	}
	if err := controller.DeleteWorkload(ctx, intent); err != nil {
		t.Fatalf("repeated DeleteWorkload: %v", err)
	}
	if _, found, err := controller.ObserveWorkload(ctx, intent); err != nil || found {
		t.Fatalf("ObserveWorkload after Delete = found %t, err %v, want absent", found, err)
	}

	if len(probe.ensures) != 2 || len(probe.drains) != 3 || len(probe.deletes) != 2 {
		t.Fatalf("lifecycle calls = ensure %d drain %d delete %d, want idempotent calls recorded",
			len(probe.ensures), len(probe.drains), len(probe.deletes))
	}
}

func TestDesiredWorkloadIdentityIsStableAndPlatformSafe(t *testing.T) {
	intent := testWorkloadIntent()
	first := DeriveWorkloadIdentity(intent)
	second := DeriveWorkloadIdentity(intent)
	if first != second {
		t.Fatalf("identity changed across derivation/restart: first=%+v second=%+v", first, second)
	}
	if first.Name == "" || len(first.Name) > 63 {
		t.Fatalf("workload name = %q, want a bounded platform-safe name", first.Name)
	}
	if first.Name[0] == '-' || first.Name[len(first.Name)-1] == '-' || strings.Contains(first.Name, "/") {
		t.Fatalf("workload name = %q, contains an unsafe platform spelling", first.Name)
	}
	for _, raw := range []string{string(intent.TenantID), string(intent.SessionID), string(intent.AgentID), intent.RuntimeCompatibilityID} {
		if raw != "" && strings.Contains(first.Name, raw) {
			t.Fatalf("workload name %q leaked raw identity %q", first.Name, raw)
		}
	}

	mutations := []func(*sessionstore.PlacementIntent){
		func(i *sessionstore.PlacementIntent) { i.TenantID = "other" },
		func(i *sessionstore.PlacementIntent) { i.SessionID = "other" },
		func(i *sessionstore.PlacementIntent) { i.AgentID = "other" },
		func(i *sessionstore.PlacementIntent) { i.RuntimeCompatibilityID = "other" },
		func(i *sessionstore.PlacementIntent) { i.Generation++ },
	}
	for index, mutate := range mutations {
		changed := intent
		mutate(&changed)
		if got := DeriveWorkloadIdentity(changed); got.Name == first.Name {
			t.Errorf("mutation %d produced the same workload name %q", index, got.Name)
		}
	}
}
