package livetail_test

import (
	"context"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// TestTheNextPollRepairsARouteAPlacementUnbindDropped is the v0.4.0 spec
// gate's C1 probe, committed and turned around: placement binds and unbinds
// through the same HostLink pool, so its transient unbind can drop the route
// a viewer's binding relies on. The tail keeps flowing -- a Host publishes to
// the subscription regardless of bind -- and the next ownership poll now
// notices the missing route and re-binds; before, the routing table trusted
// its own binding forever.
func TestTheNextPollRepairsARouteAPlacementUnbindDropped(t *testing.T) {
	t.Parallel()

	host := newStandIn(t, "host-1")
	r := newRig(t, rigOptions{})
	obs := host.observation(tenantA, session, 3)
	r.dir.put(obs)
	r.watch(t, tenantA, session)
	channel := sessionwire.HostLinkChannel(tenantA, session)
	host.publish(t, channel, enduring(t, tenantA, session, 1))
	r.wait(t, "E1", func() bool { return len(r.viewers.of(tenantA, session)) == 1 })

	if err := r.pool.Unbind(context.Background(), sessionwire.HostLinkUnbindRequest{
		Version: sessionwire.CurrentWireVersion, TenantID: tenantA, SessionID: session,
		HostID: obs.HostID, HostGeneration: obs.HostGeneration, LeaseEpoch: obs.LeaseEpoch,
		IdempotencyKey: "placement-unbind",
	}); err != nil {
		t.Fatalf("placement's Unbind: %v", err)
	}
	host.publish(t, channel, enduring(t, tenantA, session, 2))
	r.wait(t, "E2 over the route-less tail", func() bool { return len(r.viewers.of(tenantA, session)) == 2 })

	r.tips.set(2)
	r.clock.tick()
	r.wait(t, "the poll to restore the pool route", func() bool {
		_, routed := r.pool.RouteFor(tenantA, session)
		return routed
	})
	host.publish(t, channel, enduring(t, tenantA, session, 3))
	r.wait(t, "E3 on the re-bound tail", func() bool {
		got := kinds(t, r.viewers.of(tenantA, session))
		return len(got) > 0 && got[len(got)-1] == "E3"
	})
	time.Sleep(10 * time.Millisecond)
}
