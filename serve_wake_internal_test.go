package factory

import (
	"context"
	"testing"
	"time"
)

// TestStopStopsTheHostWakesAndQuiesceDoesNot holds where the post-
// acknowledgement Host wakes (internal/httpapi's wakes) are owned: Quiesce
// leaves them running, as it leaves every HostLink running, and Stop cancels
// and waits for them before it closes the routing table they call into.
//
// Deleting the StopWakes call from Stop leaves every httpapi test green and
// fails here; so does moving it into Quiesce.
func TestStopStopsTheHostWakesAndQuiesceDoesNot(t *testing.T) {
	t.Parallel()

	server, err := New(RequiredOptions()...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Quiesce(ctx); err != nil {
		t.Fatalf("Quiesce: %v", err)
	}
	if server.router.WakesStopped() {
		t.Fatal("Quiesce stopped the Host wakes; HostLinks, and the wakes over them, run until Stop")
	}
	if err := server.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if !server.router.WakesStopped() {
		t.Fatal("Stop returned with the Host wakes still accepting work")
	}
}
