package factory

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/placement"
	"github.com/looprig/factory/internal/realtime/hostlink"
	"github.com/looprig/factory/internal/realtime/livetail"
	"github.com/looprig/sessionstore"
)

type placedOnce struct{ result placement.Result }

func (p placedOnce) Reconcile(context.Context, placement.Request) (placement.Result, error) {
	return p.result, nil
}

type recordingRebinder struct{ calls int }

func (r *recordingRebinder) Rebind(context.Context, sessionwire.TenantID, sessionwire.SessionID) error {
	r.calls++
	return nil
}

func TestPlacementImmediatelyRebindsAWatchingSession(t *testing.T) {
	rebinder := &recordingRebinder{}
	placer := rebindingPlacer{placer: placedOnce{result: placement.Result{Attached: sessionwire.HostLinkRegistryObservation{HostID: "host-a"}}}, rebinder: rebinder}
	_, err := placer.Reconcile(context.Background(), placement.Request{TenantID: "tenant-a", SessionID: "session-a"})
	if err != nil {
		t.Fatal(err)
	}
	if rebinder.calls != 1 {
		t.Fatalf("rebind calls = %d, want one immediately after attach", rebinder.calls)
	}
}

type changingOwner struct {
	Directory
	owner sessionwire.HostLinkRegistryObservation
	ready bool
}

func (d *changingOwner) Owner(context.Context, sessionwire.TenantID, sessionwire.SessionID) (sessionwire.HostLinkRegistryObservation, bool, error) {
	return d.owner, d.ready, nil
}
func (d *changingOwner) Candidates(context.Context, sessionstore.ListCompatibleHostsRequest) (sessionstore.HostTargetPage, error) {
	return sessionstore.HostTargetPage{}, nil
}

func TestViewerSubscribedBeforePlacementGetsTheLiveTailWithinOneInterval(t *testing.T) {
	s, err := New(RequiredOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	clock := &pollClock{}
	link := &wireLink{}
	pool, err := hostlink.NewPool(hostlink.Config{Dialer: wireDialer{link: link}})
	if err != nil {
		t.Fatal(err)
	}
	directory := &changingOwner{}
	cfg := s.cfg
	cfg.clock = clock
	cfg.directory = directory
	viewers := &recordingViewers{}
	live, _, demand, err := composeLive(cfg, pool, func() livetail.Viewers { return viewers })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = demand.Close(context.Background())
		_ = live.Close(context.Background())
		_ = pool.Close(context.Background())
	})
	if err := demand.Acquire(context.Background(), "tenant-a", "s-1"); err != nil {
		t.Fatal(err)
	}
	if len(clock.durations) != 1 || clock.durations[0] != 5*time.Second {
		t.Fatalf("ownership poll = %v, want default five seconds", clock.durations)
	}
	now := time.Now()
	directory.owner = sessionwire.HostLinkRegistryObservation{
		Version: sessionwire.CurrentWireVersion, TenantID: "tenant-a", SessionID: "s-1",
		HostID: "host-1", HostGeneration: 1, AgentID: "agent", RuntimeCompatibilityID: "runtime-1",
		Placement: sessionwire.HostPlacementPooled, InternalEndpoint: "ws://host-1.internal",
		Residency: sessionwire.SessionResidencyResident, Accepting: true, LeaseEpoch: 1,
		ObservedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	directory.ready = true
	placer := rebindingPlacer{placer: placedOnce{result: placement.Result{Attached: directory.owner}}, rebinder: demand}
	if _, err := placer.Reconcile(context.Background(), placement.Request{TenantID: "tenant-a", SessionID: "s-1"}); err != nil {
		t.Fatal(err)
	}
	link.mu.Lock()
	if len(link.sinks) != 1 {
		link.mu.Unlock()
		t.Fatal("placement did not bind the watched tail")
	}
	sink := link.sinks[0]
	link.mu.Unlock()
	record, err := (sessionwire.EnduringPublication{TenantID: "tenant-a", SessionID: "s-1", EventID: "e-1", JournalSeq: 1, CoveredThrough: 1, Body: json.RawMessage(`{}`)}).MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	sink.Publication(record)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		viewers.mu.Lock()
		got := strings.Join(viewers.published, "\n")
		viewers.mu.Unlock()
		if strings.Contains(got, string(record)) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("live publication did not reach the pre-placement viewer")
}
