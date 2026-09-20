package clientlink_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/looprig/factory/identity"
)

func TestShutdownBoundsBlockedDemandRelease(t *testing.T) {
	f := newFixture(t, testLimits())
	principal, err := identity.NewPrincipal(tenantA, "user-a", identity.KindActor)
	if err != nil {
		t.Fatal(err)
	}
	release, err := f.handler.Engine().Bind(context.Background(), principal, sessionChannel(tenantA, "session-a"))
	if err != nil {
		t.Fatal(err)
	}
	release()
	f.demand.mu.Lock()
	f.demand.blockRelease = make(chan struct{})
	f.demand.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := f.handler.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown = %v, want deadline", err)
	}
	f.demand.mu.Lock()
	defer f.demand.mu.Unlock()
	if len(f.demand.releases) != 1 || f.demand.selfReleased {
		t.Fatalf("releases=%d, backstop=%v", len(f.demand.releases), f.demand.selfReleased)
	}
}
