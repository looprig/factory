package hostlink_test

import (
	"context"
	"testing"

	"github.com/looprig/factory/internal/realtime/hostlink"
)

// TestABindThatMeetsATerminalLinkEvictsAndClosesIt (quality gate W24): the
// dead link is dropped from the pool AND closed -- a dropped-but-open link is
// a connection nobody can reach again -- and the next bind dials afresh.
func TestABindThatMeetsATerminalLinkEvictsAndClosesIt(t *testing.T) {
	t.Parallel()

	dialer := newRecordingDialer()
	pool := newPool(t, dialer, hostlink.Limits{})
	mustBind(t, pool, target(hostOne, endpoint1), bindRequest(hostOne, "s-1"))
	first := dialer.link(hostOne)
	first.failBind(&hostlink.HostDisconnect{Host: hostOne, Code: 3500})
	if err := pool.Bind(context.Background(), target(hostOne, endpoint1), bindRequest(hostOne, "s-2")); err == nil {
		t.Fatal("a bind on a terminal link succeeded")
	}
	if got := first.closes(); got != 1 {
		t.Fatalf("the evicted terminal link was closed %d times, want 1", got)
	}
	if got := pool.Links(); got != 0 {
		t.Fatalf("the pool kept %d links", got)
	}
	mustBind(t, pool, target(hostOne, endpoint1), bindRequest(hostOne, "s-2"))
	if got := dialer.dials(); got != 2 {
		t.Fatalf("dials = %d, want a fresh dial after the eviction", got)
	}
}
