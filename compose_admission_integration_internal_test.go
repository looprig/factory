package factory

import (
	"context"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/placement"
	"github.com/looprig/factory/internal/realtime/hostlink"
	"github.com/looprig/sessionstore"
)

type admissionWorld struct {
	FakeSeams
	mu         sync.Mutex
	entry      sessionstore.DispositionInboxEntry
	owner      sessionwire.HostLinkRegistryObservation
	attached   chan struct{}
	attachOnce sync.Once
}

func (w *admissionWorld) GetCatalogEntry(context.Context, sessionstore.GetCatalogEntryRequest) (sessionstore.CatalogEntry, error) {
	return sessionstore.CatalogEntry{Record: sessionstore.CatalogRecord{
		TenantID: FakeTenant, SessionID: "session-a", AgentID: "agent-a", RuntimeCompatibilityID: "runtime-v1",
		DesiredPlacement: sessionwire.HostPlacementPooled, DesiredGeneration: 1,
		Binding: sessionstore.SessionBinding{StorageBindingID: "storage-a", BindingVersion: "v1", RuntimeSessionID: "00000000-0000-4000-8000-0000000000aa", ProtocolMode: sessionstore.ProtocolModeDisposition},
	}}, nil
}
func (w *admissionWorld) GetDispositionCommand(context.Context, sessionstore.GetDispositionCommandRequest) (sessionstore.DispositionInboxEntry, error) {
	return sessionstore.DispositionInboxEntry{}, &sessionstore.InboxError{Code: sessionstore.InboxErrorNotFound}
}
func (w *admissionWorld) AdmitDispositionCommand(_ context.Context, req sessionstore.AdmitDispositionCommandRequest) (sessionstore.DispositionInboxEntry, bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.entry = sessionstore.DispositionInboxEntry{AcceptedOrder: 1, Record: sessionstore.DispositionInboxRecord{
		Descriptor: sessionstore.DispositionCommandDescriptor{TenantID: req.TenantID, SessionID: req.SessionID, CommandID: req.CommandID, Kind: req.Kind},
		State:      sessionstore.InboxStatePending, ApplyDeadline: req.ApplyDeadline,
	}}
	return w.entry, true, nil
}
func (w *admissionWorld) ListSessionDispositionCommands(context.Context, sessionstore.ListSessionDispositionCommandsRequest) (sessionstore.SessionDispositionCommandPage, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return sessionstore.SessionDispositionCommandPage{Commands: []sessionstore.DispositionInboxEntry{w.entry}, NextAfterOrder: 1}, nil
}
func (w *admissionWorld) Owner(context.Context, sessionwire.TenantID, sessionwire.SessionID) (sessionwire.HostLinkRegistryObservation, bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.owner, w.owner.HostID != "", nil
}
func (w *admissionWorld) Candidates(context.Context, sessionstore.ListCompatibleHostsRequest) (sessionstore.HostTargetPage, error) {
	return sessionstore.HostTargetPage{Hosts: []sessionwire.HostLinkCapacityReport{{
		HostID: "host-1", HostGeneration: 1, AgentID: "agent-a", RuntimeCompatibilityID: "runtime-v1",
		Placement: sessionwire.HostPlacementPooled, IsolationClass: sessionwire.HostIsolationClassCrossTenantIsolated,
		InternalEndpoint: "ws://host-1.internal", Accepting: true, AvailableCapacity: 1,
	}}}, nil
}
func (w *admissionWorld) ListDueDispositionCommands(context.Context, sessionstore.ListDueDispositionCommandsRequest) (sessionstore.DispositionDueCommandPage, error) {
	return sessionstore.DispositionDueCommandPage{}, nil
}

type admissionLink struct {
	*wireLink
	world *admissionWorld
}

func (l *admissionLink) Attach(_ context.Context, req sessionwire.HostLinkAttachRequest) (sessionwire.HostLinkRegistryObservation, error) {
	owner := sessionwire.HostLinkRegistryObservation{Version: sessionwire.CurrentWireVersion, TenantID: req.TenantID, SessionID: req.SessionID,
		HostID: req.HostID, HostGeneration: req.HostGeneration, AgentID: req.AgentID, RuntimeCompatibilityID: req.RuntimeCompatibilityID,
		Placement: sessionwire.HostPlacementPooled, InternalEndpoint: "ws://host-1.internal", Residency: sessionwire.SessionResidencyResident,
		Accepting: true, LeaseEpoch: 1, ObservedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}
	l.world.mu.Lock()
	l.world.owner = owner
	l.world.mu.Unlock()
	l.world.attachOnce.Do(func() { close(l.world.attached) })
	return owner, nil
}
func (*admissionLink) Negotiated() (sessionwire.VersionNegotiationResponse, error) {
	return sessionwire.VersionNegotiationResponse{Version: sessionwire.CurrentWireVersion}.WithHostLinkMethods(sessionwire.HostLinkMethodAttach), nil
}

type admissionDialer struct{ link *admissionLink }

func (d admissionDialer) Dial(context.Context, hostlink.Target, hostlink.Observer) (hostlink.Link, error) {
	return d.link, nil
}

func TestComposedAdmissionAttachesAndBindsBeforeSweepTick(t *testing.T) {
	w := &admissionWorld{attached: make(chan struct{})}
	link := &admissionLink{wireLink: &wireLink{}, world: w}
	clock := &holderClock{now: time.Now().UTC()}
	limits := DefaultReconcileLimits()
	limits.Interval = 20 * time.Second
	options := append(RequiredOptionsExcept("WithCommands", "WithCatalog", "WithDirectory"),
		WithCommands(w), WithCatalog(w), WithDirectory(w), WithPendingCommands(w), WithClock(clock), WithReconcileLimits(limits), WithFakeJournals(),
		WithDepartment(LaunchTemplate{Key: sessionstore.HostTargetKey{AgentID: "agent-a", RuntimeCompatibilityID: "runtime-v1", Placement: sessionwire.HostPlacementPooled}}),
		Option{name: "hostDialer (test only)", apply: func(c *config) error { c.hostDialer = admissionDialer{link}; return nil }},
	)
	s, err := New(options...)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.components.pending.SweepPlacer().(rebindingPlacer); !ok {
		t.Fatal("sweep placer has no rebind wrapper")
	}
	seen := map[string]bool{s.components.placement.HolderID(): true}
	for _, wrapped := range s.components.admissionPlacers {
		p, ok := wrapped.(rebindingPlacer)
		if !ok {
			t.Fatalf("admission placer %T has no rebind wrapper", wrapped)
		}
		id := p.placer.(*placement.Reconciler).HolderID()
		if seen[id] {
			t.Fatalf("claim holder %q is shared", id)
		}
		seen[id] = true
	}
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Stop(context.Background()) })
	if err := s.components.demand.Acquire(context.Background(), FakeTenant, "session-a"); err != nil {
		t.Fatal(err)
	}
	_, _, err = s.components.admissions.AdmitInterrupt(context.Background(), FakeServiceIdentity(), sessionwire.InterruptRequest{
		CommandEnvelope: sessionwire.CommandEnvelope{Version: sessionwire.CurrentWireVersion, CommandID: "11111111-1111-4111-8111-111111111111"}, SessionID: "session-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-w.attached:
	case <-time.After(2 * time.Second):
		t.Fatal("admission did not attach")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := s.components.bindings.Binding(FakeTenant, "session-a"); ok {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if _, ok := s.components.bindings.Binding(FakeTenant, "session-a"); !ok {
		t.Fatal("preexisting demand did not bind before the first sweep tick")
	}
	w.mu.Lock()
	owner := w.owner
	w.mu.Unlock()
	rebinder := &recordingRebinder{}
	wrapped := rebindingPlacer{placer: placedOnce{result: placement.Result{Attached: owner}}, rebinder: rebinder, bindings: s.components.bindings}
	if _, err := wrapped.Reconcile(context.Background(), placement.Request{TenantID: FakeTenant, SessionID: "session-a"}); err != nil {
		t.Fatal(err)
	}
	if rebinder.calls != 0 {
		t.Fatalf("same attached owner caused %d tail resets", rebinder.calls)
	}
}
