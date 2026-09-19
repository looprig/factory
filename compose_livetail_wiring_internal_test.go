package factory

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/hostlink"
	"github.com/looprig/factory/internal/realtime/livetail"
	"github.com/looprig/sessionstore"
)

// wireLink is a HostLink that records what it was asked, in order, and hands
// the test the sink of every subscribe so the test can speak for the Host.
type wireLink struct {
	mu    sync.Mutex
	calls []string
	sinks []hostlink.SessionSink
}

func (l *wireLink) record(call string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, call)
}

func (l *wireLink) Host() sessionwire.HostID { return "host-1" }
func (l *wireLink) Bind(context.Context, sessionwire.HostLinkBindRequest) error {
	l.record("bind")
	return nil
}
func (l *wireLink) Unbind(context.Context, sessionwire.HostLinkUnbindRequest) error {
	l.record("unbind")
	return nil
}
func (l *wireLink) Attach(context.Context, sessionwire.HostLinkAttachRequest) (sessionwire.HostLinkRegistryObservation, error) {
	return sessionwire.HostLinkRegistryObservation{}, nil
}
func (l *wireLink) DeliverCommand(context.Context, sessionwire.TenantID, sessionwire.SessionID, sessionwire.HostLinkCommandDelivery) error {
	return nil
}
func (l *wireLink) Close(context.Context) error { return nil }
func (l *wireLink) Subscribe(_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, sink hostlink.SessionSink) error {
	l.record("subscribe")
	l.mu.Lock()
	l.sinks = append(l.sinks, sink)
	l.mu.Unlock()
	sink.Subscribed()
	return nil
}
func (l *wireLink) Unsubscribe(sessionwire.TenantID, sessionwire.SessionID) { l.record("unsubscribe") }

type wireDialer struct{ link *wireLink }

func (d wireDialer) Dial(context.Context, hostlink.Target, hostlink.Observer) (hostlink.Link, error) {
	return d.link, nil
}

// ownerDirectory answers one owner for every session.
type ownerDirectory struct {
	Directory
	owner sessionwire.HostLinkRegistryObservation
}

func (d ownerDirectory) Owner(context.Context, sessionwire.TenantID, sessionwire.SessionID) (sessionwire.HostLinkRegistryObservation, bool, error) {
	return d.owner, true, nil
}

func (d ownerDirectory) Candidates(context.Context, sessionstore.ListCompatibleHostsRequest) (sessionstore.HostTargetPage, error) {
	return sessionstore.HostTargetPage{}, nil
}

// TestComposeLiveBindsThenSubscribesAndDeliversTheFirstTailWithoutAReset is
// the committed reader for Gap 3's WIRING (v0.4.0 quality gate F4: mutants
// RW8 and RW10 were killed only by an uncommitted real-Host harness). Over
// composeLive exactly as New composes it: a first viewer's Acquire binds and
// then subscribes on the same link -- which a routing table handed the bare
// pool as its Binder would not -- and a record the Host publishes reaches the
// viewers byte for byte with NO reset before it, which needs the demand plane
// to have told the plane the tail started inside the first viewer's own
// subscribe (the Watcher).
func TestComposeLiveBindsThenSubscribesAndDeliversTheFirstTailWithoutAReset(t *testing.T) {
	t.Parallel()

	server, err := New(RequiredOptions()...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	link := &wireLink{}
	pool, err := hostlink.NewPool(hostlink.Config{Dialer: wireDialer{link: link}})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	now := time.Now()
	cfg := server.cfg
	cfg.directory = ownerDirectory{owner: sessionwire.HostLinkRegistryObservation{
		Version: sessionwire.CurrentWireVersion, TenantID: "tenant-a", SessionID: "s-1",
		HostID: "host-1", HostGeneration: 1, AgentID: "agent", RuntimeCompatibilityID: "runtime-1",
		Placement: sessionwire.HostPlacementPooled, InternalEndpoint: "ws://host-1.internal",
		Residency: sessionwire.SessionResidencyResident, Accepting: true, LeaseEpoch: 1,
		ObservedAt: now, ExpiresAt: now.Add(time.Hour),
	}}
	viewers := &recordingViewers{}
	live, _, demand, err := composeLive(cfg, pool, func() livetail.Viewers { return viewers })
	if err != nil {
		t.Fatalf("composeLive: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = demand.Close(ctx)
		_ = live.Close(ctx)
		_ = pool.Close(ctx)
	})
	if err := demand.Acquire(context.Background(), "tenant-a", "s-1"); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	link.mu.Lock()
	calls := strings.Join(link.calls, ",")
	link.mu.Unlock()
	if calls != "bind,subscribe" {
		t.Fatalf("the link was asked %q, want a bind and then a subscribe", calls)
	}
	record, err := sessionwire.EnduringPublication{
		TenantID: "tenant-a", SessionID: "s-1", EventID: "e-1", JournalSeq: 1, CoveredThrough: 1, Body: json.RawMessage(`{}`),
	}.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	link.mu.Lock()
	sink := link.sinks[0]
	link.mu.Unlock()
	sink.Publication(record)
	deadline := time.Now().Add(10 * time.Second)
	for {
		viewers.mu.Lock()
		published := append([]string(nil), viewers.published...)
		viewers.mu.Unlock()
		if len(published) > 0 {
			if want := "tenant-a/s-1 " + string(record); len(published) != 1 || published[0] != want {
				t.Fatalf("the viewers received %v, want exactly the Host's record and no reset before it", published)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the Host's record never reached the viewers")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
