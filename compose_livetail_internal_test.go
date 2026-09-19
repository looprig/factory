package factory

import (
	"context"
	"errors"
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
