package livetail_test

import (
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/livetail"
)

// TestAFirstServeThatBoundNothingIsRecoveredBeforeAnyonePublishes is the
// quality gate's F2 probe, turned into a reader. The first serve binds nothing
// (no owner yet -- the load-free spelling of a subscribe that timed out under
// load), which is exactly the state in which the overflow case used to publish
// ten records to a channel nobody subscribed to and wait 20s for an overflow
// that could not happen. awaitTail must recover it -- by driving the poll the
// manual clock never fires -- so the overflow then happens.
func TestAFirstServeThatBoundNothingIsRecoveredBeforeAnyonePublishes(t *testing.T) {
	t.Parallel()

	host := newStandIn(t, "host-1")
	r := newRig(t, rigOptions{mailbox: 2})
	r.watch(t, tenantA, session) // no owner published: the first serve binds nothing
	channel := sessionwire.HostLinkChannel(tenantA, session)
	if got := host.subscribers(channel); got != 0 {
		t.Fatalf("precondition: the first serve left %d subscribers, want the probe's 0", got)
	}
	r.dir.put(host.observation(tenantA, session, 3))
	r.awaitTail(t, host, channel)

	gate := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-gate:
		default:
			close(gate)
		}
	})
	r.viewers.mu.Lock()
	r.viewers.gate = gate
	r.viewers.mu.Unlock()
	for seq := uint64(1); seq <= 10; seq++ {
		host.publish(t, channel, enduring(t, tenantA, session, seq))
	}
	r.wait(t, "the mailbox to overflow once the tail is live", func() bool {
		_, lost := livetail.Pending(r.plane, tenantA, session)
		return lost
	})
}
