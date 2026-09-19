package livetail_test

import (
	"context"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/hostlink"
	"github.com/looprig/factory/internal/realtime/livetail"
	"github.com/looprig/factory/internal/routing"
)

// slowBindLinks is the real pool, except that a Bind for one session takes
// `delay` -- a Host slow to answer one RPC.
type slowBindLinks struct {
	*hostlink.Pool
	slow  sessionwire.SessionID
	delay time.Duration
	armed chan struct{}
}

func (s *slowBindLinks) Bind(ctx context.Context, target hostlink.Target, req sessionwire.HostLinkBindRequest) error {
	if req.SessionID == s.slow {
		select {
		case <-s.armed:
			select {
			case <-time.After(s.delay):
			case <-ctx.Done():
				return ctx.Err()
			}
		default:
		}
	}
	return s.Pool.Bind(ctx, target, req)
}

// TestOneSessionsRepairDoesNotStallAnotherSessionsDelivery is the v0.4.0
// quality gate's F3 probe, committed: session B's repair re-binds against a
// Host that takes 3s to answer, and session A -- on another Host -- publishes
// meanwhile. The relay's one mutex was held across that re-bind, so A waited
// 2.9s; the repair now re-binds with it released.
func TestOneSessionsRepairDoesNotStallAnotherSessionsDelivery(t *testing.T) {
	t.Parallel()

	hostA, hostB := newStandIn(t, "host-a"), newStandIn(t, "host-b")
	dialer, err := hostlink.NewCentrifugeDialer(hostlink.DialerConfig{
		Credential: credential("service-token"), Version: "factory-test",
		Limits: hostlink.Limits{MaxLinks: 8, DialTimeout: 5 * time.Second, IdleTimeout: time.Minute,
			ReconnectMin: 100 * time.Millisecond, ReconnectMax: 100 * time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	pool, _ := hostlink.NewPool(hostlink.Config{Dialer: dialer})
	links := &slowBindLinks{Pool: pool, slow: "s-b", delay: 3 * time.Second, armed: make(chan struct{})}
	dir := &directory{owners: map[string]sessionwire.HostLinkRegistryObservation{}}
	tp := &tips{}
	vw := newViewers()
	plane, _ := livetail.New(livetail.Config{Links: links, Viewers: func() livetail.Viewers { return vw }, MailboxLimit: 1024, EventTimeout: 10 * time.Second})
	bindings, _ := routing.NewBindings(dir, plane)
	demand, _ := routing.NewDemand(bindings, tp, plane, &manualClock{}, routing.DemandLimits{OwnershipPollInterval: time.Hour, PollTimeout: 10 * time.Second})
	demand.SetWatcher(plane)
	relay, _ := routing.NewRelay(tp, demand, plane, plane, routing.DefaultRepairLimits())
	plane.Attach(relay, demand)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = demand.Close(ctx)
		_ = plane.Close(ctx)
		_ = pool.Close(ctx)
	})
	dir.put(hostA.observation(tenantA, "s-a", 1))
	dir.put(hostB.observation(tenantA, "s-b", 1))
	for _, s := range []sessionwire.SessionID{"s-a", "s-b"} {
		if err := demand.Acquire(context.Background(), tenantA, s); err != nil {
			t.Fatal(err)
		}
	}
	close(links.armed)
	// B: a record the Relay refuses -> HostLinkClosed -> Rebind (slow bind).
	hostB.publish(t, sessionwire.HostLinkChannel(tenantA, "s-b"), []byte(`{"type":"not-a-record"}`))
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	hostA.publish(t, sessionwire.HostLinkChannel(tenantA, "s-a"), enduring(t, tenantA, "s-a", 1))
	eventually(t, "A's record", func() bool { return len(vw.of(tenantA, "s-a")) == 1 })
	latency := time.Since(start)
	t.Logf("session A's record reached its viewers after %v while session B's repair re-bound against a 3s Host", latency)
	if latency > time.Second {
		t.Fatalf("an unrelated session's delivery stalled %v behind another session's repair RPC (Relay.mu held across Rebind)", latency)
	}
}
