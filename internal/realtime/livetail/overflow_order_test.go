package livetail_test

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/hostlink"
	"github.com/looprig/factory/internal/realtime/livetail"
	"github.com/looprig/factory/internal/routing"
)

// laggingLinks is the real pool, except that an Unsubscribe issued from a
// goroutine the overflow arm of the sink STARTED is held until the repair's
// re-subscribe has returned -- a scheduler running that goroutine late, which
// nothing orders against the repair.
type laggingLinks struct {
	*hostlink.Pool
	mu       sync.Mutex
	held     bool
	release  chan struct{}
	released bool
}

func (d *laggingLinks) Subscribe(ctx context.Context, tenant sessionwire.TenantID, sid sessionwire.SessionID, sink hostlink.SessionSink) error {
	err := d.Pool.Subscribe(ctx, tenant, sid, sink)
	d.mu.Lock()
	if d.held && !d.released && err == nil {
		d.released = true
		close(d.release)
	}
	d.mu.Unlock()
	return err
}

func (d *laggingLinks) Unsubscribe(tenant sessionwire.TenantID, sid sessionwire.SessionID) {
	buf := make([]byte, 1<<16)
	stack := string(buf[:runtime.Stack(buf, false)])
	if strings.Contains(stack, "created by github.com/looprig/factory/internal/realtime/livetail.(*sink)") {
		d.mu.Lock()
		d.held = true
		release := d.release
		d.mu.Unlock()
		select {
		case <-release:
		case <-time.After(10 * time.Second):
		}
	}
	d.Pool.Unsubscribe(tenant, sid)
}

// TestAnOverflowNeverWithdrawsTheRepairedTail is the v0.4.0 gates' F1 probe,
// committed: after a mailbox overflow the repair re-binds and re-subscribes,
// and nothing the overflow started may withdraw that NEW tail afterwards. A
// dead new tail is silent -- the route stays held, the Host holds no
// subscription, no Ended fires -- so the assertion is that a record the Host
// publishes after the repair reaches the viewers.
func TestAnOverflowNeverWithdrawsTheRepairedTail(t *testing.T) {
	t.Parallel()

	host := newStandIn(t, "host-1")
	dialer, err := hostlink.NewCentrifugeDialer(hostlink.DialerConfig{
		Credential: credential("service-token"), Version: "factory-test",
		Limits: hostlink.Limits{MaxLinks: 8, DialTimeout: 5 * time.Second, IdleTimeout: time.Minute,
			ReconnectMin: 100 * time.Millisecond, ReconnectMax: 100 * time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	pool, err := hostlink.NewPool(hostlink.Config{Dialer: dialer})
	if err != nil {
		t.Fatal(err)
	}
	links := &laggingLinks{Pool: pool, release: make(chan struct{})}
	dir := &directory{owners: map[string]sessionwire.HostLinkRegistryObservation{}}
	tp := &tips{}
	vw := newViewers()
	plane, err := livetail.New(livetail.Config{Links: links, Viewers: func() livetail.Viewers { return vw }, MailboxLimit: 2, EventTimeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	bindings, _ := routing.NewBindings(dir, plane)
	demand, _ := routing.NewDemand(bindings, tp, plane, &manualClock{}, routing.DemandLimits{OwnershipPollInterval: time.Hour, PollTimeout: 10 * time.Second})
	demand.SetWatcher(plane)
	relay, _ := routing.NewRelay(tp, demand, plane, plane, routing.DefaultRepairLimits())
	plane.Attach(relay, demand)
	gate := make(chan struct{})
	var once sync.Once
	open := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(func() {
		open()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = demand.Close(ctx)
		_ = plane.Close(ctx)
		_ = pool.Close(ctx)
	})

	dir.put(host.observation(tenantA, session, 3))
	if err := demand.Acquire(context.Background(), tenantA, session); err != nil {
		t.Fatal(err)
	}
	channel := sessionwire.HostLinkChannel(tenantA, session)
	vw.mu.Lock()
	vw.gate = gate
	vw.mu.Unlock()
	for seq := uint64(1); seq <= 10; seq++ {
		host.publish(t, channel, enduring(t, tenantA, session, seq))
	}
	tp.set(10)
	eventually(t, "the mailbox to overflow into a queued repair", func() bool {
		_, lost := livetail.Pending(plane, tenantA, session)
		return lost
	})
	vw.mu.Lock()
	vw.gate = nil
	vw.mu.Unlock()
	open()
	eventually(t, "the tail to be re-subscribed", func() bool { return host.count("subscribe", channel) >= 2 })
	time.Sleep(300 * time.Millisecond)
	before := len(vw.of(tenantA, session))
	host.publish(t, channel, enduring(t, tenantA, session, 11))
	eventually(t, "E11 on the repaired tail (a late withdrawal kills it silently)", func() bool {
		return len(vw.of(tenantA, session)) > before
	}, func() string {
		_, routed := pool.RouteFor(tenantA, session)
		return describe(t, vw.of(tenantA, session), host, channel) + " route_held=" + boolString(routed)
	})
}
