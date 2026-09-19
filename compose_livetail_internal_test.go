package factory

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/livetail"
)

// TestTheLiveTailIsComposedAndPublishesThroughTheRunningClientLink is Gap 3's
// wiring, held at the composition: the plane is built, the hint the demand
// plane publishes goes through it rather than through the named refusal the
// composition used to hold, and it reaches a ClientLink only while one runs --
// before Start and after Stop it answers livetail.ErrNoViewers.
func TestTheLiveTailIsComposedAndPublishesThroughTheRunningClientLink(t *testing.T) {
	t.Parallel()

	server, err := New(RequiredOptions()...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	live := server.components.live
	if live == nil {
		t.Fatal("the composition built no live-tail plane")
	}
	hint := sessionwire.JournalTip{TenantID: "tenant-a", SessionID: "s-1", Tip: 7}
	if err := live.PublishJournalTip(context.Background(), hint); !errors.Is(err, livetail.ErrNoViewers) {
		t.Fatalf("a hint before Start = %v, want livetail.ErrNoViewers", err)
	}
	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := live.PublishJournalTip(context.Background(), hint); err != nil {
		t.Fatalf("a hint with the ClientLink running = %v, want it published", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := live.PublishJournalTip(context.Background(), hint); !errors.Is(err, livetail.ErrNoViewers) {
		t.Fatalf("a hint after Stop = %v, want livetail.ErrNoViewers", err)
	}
}

// recordingViewers is the ClientLink side of composeLive, recorded.
type recordingViewers struct {
	mu        sync.Mutex
	published []string
}

func (v *recordingViewers) PublishSession(tenant sessionwire.TenantID, session sessionwire.SessionID, encoded []byte) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.published = append(v.published, string(tenant)+"/"+string(session)+" "+string(encoded))
	return nil
}

func (v *recordingViewers) CloseSession(sessionwire.TenantID, sessionwire.SessionID) {}

// TestTheDemandPlanesHintIsPublishedToTheSessionsViewers holds composeLive's
// wiring of the Hinter: a watched session with no owner has its durable tip
// published to that session's viewers through the live plane. Before Gap 3 the
// composition held a named refusal here and the hint went nowhere.
func TestTheDemandPlanesHintIsPublishedToTheSessionsViewers(t *testing.T) {
	t.Parallel()

	server, err := New(RequiredOptions()...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	viewers := &recordingViewers{}
	live, _, demand, err := composeLive(server.cfg, server.components.pool, func() livetail.Viewers { return viewers })
	if err != nil {
		t.Fatalf("composeLive: %v", err)
	}
	t.Cleanup(func() {
		_ = demand.Close(context.Background())
		_ = live.Close(context.Background())
	})
	if err := demand.Acquire(context.Background(), "tenant-a", "s-unowned"); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	viewers.mu.Lock()
	defer viewers.mu.Unlock()
	if len(viewers.published) != 1 || viewers.published[0] != `tenant-a/s-unowned {"journal_tip":0,"session_id":"s-unowned","tenant_id":"tenant-a","type":"journal_tip"}` {
		t.Fatalf("the viewers were sent %v, want the unbound session's journal_tip hint", viewers.published)
	}
}
