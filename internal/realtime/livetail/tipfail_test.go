package livetail_test

import (
	"errors"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// TestATipFailureDuringADropRepairStillRecovers is the v0.4.0 quality gate's
// F1 probe, committed: a repair whose tip read fails once. After the store
// recovers and the link reconnects, the watched session delivers again.
func TestATipFailureDuringADropRepairStillRecovers(t *testing.T) {
	t.Parallel()
	host := newStandIn(t, "host-1")
	r := newRig(t, rigOptions{})
	r.dir.put(host.observation(tenantA, session, 3))
	r.watch(t, tenantA, session)
	channel := sessionwire.HostLinkChannel(tenantA, session)
	host.publish(t, channel, enduring(t, tenantA, session, 1))
	eventually(t, "E1", func() bool { return len(r.viewers.of(tenantA, session)) == 1 })

	r.tips.mu.Lock()
	r.tips.err = errors.New("store unavailable")
	r.tips.mu.Unlock()
	host.drop()
	eventually(t, "the viewers to be closed", func() bool { return r.viewers.closed(tenantA, session) >= 1 })
	r.tips.mu.Lock()
	r.tips.err = nil
	r.tips.tip = 4
	r.tips.mu.Unlock()
	// Give the link time to reconnect and Restored to run; poll a few times.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		r.clock.tick()
		host.publish(t, channel, enduring(t, tenantA, session, 5))
		time.Sleep(200 * time.Millisecond)
		for _, k := range kinds(t, r.viewers.of(tenantA, session)) {
			if k == "E5" {
				t.Logf("recovered: %v", kinds(t, r.viewers.of(tenantA, session)))
				return
			}
		}
	}
	t.Fatalf("after a one-off tip failure the session never delivered again; viewers=%v host=%+v subscribers=%d",
		kinds(t, r.viewers.of(tenantA, session)), host.events(), host.subscribers(channel))
}
