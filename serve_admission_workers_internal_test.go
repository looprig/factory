package factory

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/placement"
	"github.com/looprig/sessionstore"
)

type workerPending struct{ active atomic.Bool }

func (*workerPending) ControlShards() int { return 1 }
func (p *workerPending) ListDueDispositionCommands(context.Context, sessionstore.ListDueDispositionCommandsRequest) (sessionstore.DispositionDueCommandPage, error) {
	if !p.active.Load() {
		return sessionstore.DispositionDueCommandPage{}, nil
	}
	deadline := time.Now().Add(time.Hour)
	entry := func(id sessionwire.SessionID) sessionstore.DispositionInboxEntry {
		return sessionstore.DispositionInboxEntry{Record: sessionstore.DispositionInboxRecord{
			Descriptor: sessionstore.DispositionCommandDescriptor{TenantID: "tenant-a", SessionID: id, CommandID: sessionwire.CommandID("command-" + string(id))},
			State:      sessionstore.InboxStatePending, ApplyDeadline: deadline,
		}}
	}
	return sessionstore.DispositionDueCommandPage{Commands: []sessionstore.DispositionInboxEntry{entry("cold-a"), entry("cold-b")}}, nil
}

type workerPlacer struct {
	a, b, release chan struct{}
	onceA, onceB  sync.Once
}

func (p *workerPlacer) Reconcile(ctx context.Context, req placement.Request) (placement.Result, error) {
	switch req.SessionID {
	case "cold-a":
		p.onceA.Do(func() { close(p.a) })
		select {
		case <-p.release:
		case <-ctx.Done():
			return placement.Result{}, ctx.Err()
		}
	case "cold-b":
		p.onceB.Do(func() { close(p.b) })
	}
	return placement.Result{}, nil
}

func TestStartedAdmissionWorkersDoNotQueueColdSessionsBehindOneSlowAttach(t *testing.T) {
	server, err := New(append(RequiredOptions(), WithPendingCommands(FakeSeams{}), WithFakeJournals())...)
	if err != nil {
		t.Fatal(err)
	}
	pending := &workerPending{}
	placer := &workerPlacer{a: make(chan struct{}), b: make(chan struct{}), release: make(chan struct{})}
	sweeper, err := placement.NewPendingSweeper(placement.PendingSweeperConfig{
		Authorizer: FakeSeams{}, Pending: pending, Placer: placer, Clock: FakeClock{},
		Horizon: time.Hour, PageLimit: 8, MaxPages: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	server.components.pending = sweeper
	server.components.admissionPlacers = []placement.Placer{placer, placer}
	if err := server.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { close(placer.release); _ = server.Stop(context.Background()) })
	pending.active.Store(true)
	notice := func(id sessionwire.SessionID) placement.AdmissionNotice {
		return placement.AdmissionNotice{TenantID: "tenant-a", SessionID: id, CommandID: sessionwire.CommandID("command-" + string(id)), Pending: true, Deadline: time.Now().Add(time.Hour)}
	}
	server.components.admitted <- notice("cold-a")
	select {
	case <-placer.a:
	case <-time.After(time.Second):
		t.Fatal("first placement never started")
	}
	server.components.admitted <- notice("cold-b")
	select {
	case <-placer.b:
	case <-time.After(time.Second):
		t.Fatal("second cold session waited behind the blocked first attach")
	}
}
