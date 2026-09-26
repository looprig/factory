package factory

import (
	"context"
	"testing"
	"time"

	"github.com/looprig/factory/internal/realtime/livetail"
	"github.com/looprig/factory/internal/routing"
)

type pollClock struct{ durations []time.Duration }

func (c *pollClock) Now() time.Time { return time.Now() }
func (c *pollClock) AfterFunc(d time.Duration, _ func()) func() bool {
	c.durations = append(c.durations, d)
	return func() bool { return true }
}

func TestPassDeadlineIsIndependentOfInterval(t *testing.T) {
	s, err := New(RequiredOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	s.cfg.reconcile.Interval = time.Millisecond
	s.cfg.reconcile.PassTimeout = time.Second
	var deadline time.Duration
	s.runOnce(context.Background(), sweep{name: "probe", run: func(ctx context.Context) error {
		until, ok := ctx.Deadline()
		if !ok {
			t.Fatal("pass has no deadline")
		}
		deadline = time.Until(until)
		return nil
	}})
	if deadline < 900*time.Millisecond {
		t.Fatalf("pass deadline in %v, want about one second", deadline)
	}
}

func TestOlderReconcileLimitsLiteralGetsThePassDefault(t *testing.T) {
	limits := DefaultReconcileLimits()
	limits.PassTimeout = 0
	s, err := New(append(RequiredOptions(), WithReconcileLimits(limits))...)
	if err != nil {
		t.Fatalf("older limits literal was refused: %v", err)
	}
	var remaining time.Duration
	s.runOnce(context.Background(), sweep{name: "probe", run: func(ctx context.Context) error {
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatal("pass has no deadline")
		}
		remaining = time.Until(deadline)
		return nil
	}})
	if remaining < 9*time.Second || remaining > 11*time.Second {
		t.Fatalf("default pass deadline = %v, want about ten seconds", remaining)
	}
}

func TestPassDeadlineStaysInsidePlacementClaim(t *testing.T) {
	limits := DefaultReconcileLimits()
	limits.ClaimTTL = 6 * time.Second
	if got := limits.passTimeout(); got >= limits.ClaimTTL {
		t.Fatalf("pass deadline %v can outlive claim TTL %v", got, limits.ClaimTTL)
	}
}

func TestOwnershipPollDoesNotIncludeReleaseDebounce(t *testing.T) {
	s, err := New(RequiredOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	clock := &pollClock{}
	cfg := s.cfg
	cfg.clock = clock
	_, _, demand, err := composeLive(cfg, s.components.pool, func() livetail.Viewers { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := demand.Acquire(context.Background(), "tenant-a", "s-1"); err != nil {
		t.Fatal(err)
	}
	if len(clock.durations) != 1 {
		t.Fatalf("armed timers = %v, want one poll", clock.durations)
	}
	if got := clock.durations[0]; got != routing.DefaultDemandLimits().OwnershipPollInterval {
		t.Fatalf("ownership poll = %v, want %v", got, routing.DefaultDemandLimits().OwnershipPollInterval)
	}
}
