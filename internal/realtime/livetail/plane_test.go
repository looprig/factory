package livetail_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/livetail"
)

// These cases run compose.go's composition -- the real HostLink pool and
// dialer, the real routing table, demand plane and relay -- against a Host
// stand-in with host v0.2.1's subscribe gate. Only the registry, the durable
// tip and the ClientLink are fakes, and each is the narrowest seam the
// production composition hands over.

// TestTheFirstViewersTailIsBoundThenSubscribedAndDeliveredInOrder is the
// headline: a viewer watches, the session is bound and then subscribed ON THE
// SAME CONNECTION, and everything the Host publishes reaches the viewer's
// channel byte for byte and in order -- with no reset, because a tail started
// inside the first viewer's own subscribe missed nothing that viewer relies on.
func TestTheFirstViewersTailIsBoundThenSubscribedAndDeliveredInOrder(t *testing.T) {
	t.Parallel()

	host := newStandIn(t, "host-1")
	r := newRig(t, rigOptions{})
	r.dir.put(host.observation(tenantA, session, 3))
	r.watch(t, tenantA, session)

	channel := sessionwire.HostLinkChannel(tenantA, session)
	events := host.events()
	if len(events) != 2 || events[0].kind != "bind" || events[1].kind != "subscribe" ||
		events[0].client != events[1].client || events[1].channel != channel {
		t.Fatalf("the Host saw %+v, want a bind then a subscribe to %s on one connection", events, channel)
	}
	var sent [][]byte
	for seq := uint64(4); seq <= 8; seq++ {
		record := enduring(t, tenantA, session, seq)
		sent = append(sent, record)
		host.publish(t, channel, record)
	}
	eventually(t, "five records at the viewers", func() bool { return len(r.viewers.of(tenantA, session)) == 5 })
	for index, got := range r.viewers.of(tenantA, session) {
		if got != string(sent[index]) {
			t.Fatalf("record %d = %s, want the Host's bytes %s", index, got, sent[index])
		}
	}
}

// TestAViewerOfAnotherTenantReceivesNothing: the same session id under two
// tenants is two sessions, two HostLink channels and two viewer channels.
func TestAViewerOfAnotherTenantReceivesNothing(t *testing.T) {
	t.Parallel()

	host := newStandIn(t, "host-1")
	r := newRig(t, rigOptions{})
	r.dir.put(host.observation(tenantA, session, 3))
	r.dir.put(host.observation(tenantB, session, 3))
	r.watch(t, tenantA, session)
	r.watch(t, tenantB, session)

	host.publish(t, sessionwire.HostLinkChannel(tenantA, session), enduring(t, tenantA, session, 1))
	host.publish(t, sessionwire.HostLinkChannel(tenantA, session), enduring(t, tenantA, session, 2))
	eventually(t, "tenant A's records", func() bool { return len(r.viewers.of(tenantA, session)) == 2 })
	// The control: B's own record arrives, and only it.
	host.publish(t, sessionwire.HostLinkChannel(tenantB, session), enduring(t, tenantB, session, 9))
	eventually(t, "tenant B's own record", func() bool { return len(r.viewers.of(tenantB, session)) == 1 })
	if got := joined(kinds(t, r.viewers.of(tenantB, session))); got != "E9" {
		t.Fatalf("tenant B's viewers received %s, want only E9", got)
	}
	for _, record := range r.viewers.of(tenantB, session) {
		if strings.Contains(record, string(tenantA)) {
			t.Fatalf("tenant A's record reached tenant B's viewers: %s", record)
		}
	}
}

// TestATailStartedByALaterPollOwesTheViewersAReset: a viewer watched a session
// that had no owner yet, so everything the Host published before this replica
// bound it reached nobody. When a poll binds it, the viewers must be told --
// AFTER the tail is live -- and everything after arrives behind the reset.
func TestATailStartedByALaterPollOwesTheViewersAReset(t *testing.T) {
	t.Parallel()

	host := newStandIn(t, "host-1")
	r := newRig(t, rigOptions{})
	r.tips.set(12)
	r.watch(t, tenantA, session)
	if got := joined(kinds(t, r.viewers.of(tenantA, session))); got != "T12" {
		t.Fatalf("an unbound watched session published %q, want the tip hint T12", got)
	}

	r.dir.put(host.observation(tenantA, session, 3))
	r.clock.tick()
	eventually(t, "the reset", func() bool { return len(r.viewers.of(tenantA, session)) == 2 })
	channel := sessionwire.HostLinkChannel(tenantA, session)
	host.publish(t, channel, enduring(t, tenantA, session, 13))
	eventually(t, "the record after it", func() bool { return len(r.viewers.of(tenantA, session)) == 3 })
	if got := joined(kinds(t, r.viewers.of(tenantA, session))); got != "T12 R0/12 E13" {
		t.Fatalf("viewers received %q, want the hint, a reset naming the tip, then E13", got)
	}
	if n := host.count("subscribe", channel); n != 1 {
		t.Fatalf("the Host answered %d subscribes, want 1", n)
	}
}

// TestAHostLinkDropMidStreamIsRepairedRebindThenSubscribeThenReset is the
// reconnect-ordering risk end to end. The connection drops mid-stream; the
// viewers are reset; once the link reconnects the session is re-bound on the
// NEW connection before it is subscribed there, the transport never
// resubscribes on its own, and the viewers are reset again once the new tail is
// live, before anything it carries.
func TestAHostLinkDropMidStreamIsRepairedRebindThenSubscribeThenReset(t *testing.T) {
	t.Parallel()

	host := newStandIn(t, "host-1")
	r := newRig(t, rigOptions{})
	r.dir.put(host.observation(tenantA, session, 3))
	r.watch(t, tenantA, session)
	channel := sessionwire.HostLinkChannel(tenantA, session)
	host.publish(t, channel, enduring(t, tenantA, session, 1))
	host.publish(t, channel, enduring(t, tenantA, session, 2))
	eventually(t, "the first two records", func() bool { return len(r.viewers.of(tenantA, session)) == 2 })
	firstClient := host.events()[0].client

	r.tips.set(5)
	host.drop()
	eventually(t, "the tail to be re-subscribed on the new connection", func() bool {
		return host.count("subscribe", channel) == 2
	})
	host.publish(t, channel, enduring(t, tenantA, session, 6))
	eventually(t, "E6 after the repair", func() bool {
		got := kinds(t, r.viewers.of(tenantA, session))
		return len(got) > 0 && got[len(got)-1] == "E6"
	})

	got := kinds(t, r.viewers.of(tenantA, session))
	if got[0] != "E1" || got[1] != "E2" {
		t.Fatalf("stream = %v, want it to begin E1 E2", got)
	}
	// Between the drop and E6 only repair traffic may appear: resets, and the
	// durable-tip hint an unbound watched session publishes while the link is
	// reconnecting (the repair's own re-bind fails fast then).
	var resets []string
	for _, k := range got[2 : len(got)-1] {
		switch {
		case strings.HasPrefix(k, "R"):
			resets = append(resets, k)
		case strings.HasPrefix(k, "T"):
		default:
			t.Fatalf("stream = %v: only resets and hints may sit between the drop and E6", got)
		}
	}
	if len(resets) == 0 || resets[len(resets)-1] != "R2/5" {
		t.Fatalf("resets = %v, want at least one, the last naming (2, 5): the viewers delivered 2 and the journal is at 5", resets)
	}

	// On the NEW connection: a bind, then the one subscribe, and no refused
	// subscribe anywhere -- which is what an automatic resubscribe racing
	// ahead of the bind would have produced.
	var second []string
	for _, e := range host.events() {
		if e.kind == "subscribe-refused" {
			t.Fatalf("the Host refused a subscribe: something subscribed before the bind (%+v)", host.events())
		}
		if e.client != firstClient && e.channel == channel {
			second = append(second, e.kind)
		}
	}
	if len(second) < 2 || second[0] != "bind" || second[len(second)-1] != "subscribe" {
		t.Fatalf("the new connection saw %v, want a bind first and the subscribe last", second)
	}
}

// TestTheLastViewerLeavingStopsTheTail: a Host never unsubscribes a Factory,
// so giving the route back must stop the tail, and nothing the Host publishes
// afterwards reaches anybody.
func TestTheLastViewerLeavingStopsTheTail(t *testing.T) {
	t.Parallel()

	host := newStandIn(t, "host-1")
	r := newRig(t, rigOptions{})
	r.dir.put(host.observation(tenantA, session, 3))
	r.watch(t, tenantA, session)
	channel := sessionwire.HostLinkChannel(tenantA, session)
	if err := r.demand.Release(context.Background(), tenantA, session); err != nil {
		t.Fatalf("Release: %v", err)
	}
	eventually(t, "the Host to drop the subscription", func() bool { return host.subscribers(channel) == 0 })
	host.publish(t, channel, enduring(t, tenantA, session, 1))
	time.Sleep(100 * time.Millisecond)
	if got := r.viewers.of(tenantA, session); len(got) != 0 {
		t.Fatalf("a released session delivered %v", kinds(t, got))
	}
}

// TestAnOwnerChangeStopsTheStaleTailAndResetsFromTheNewOne: the route drops
// because the session moved, and the old Host -- which never unsubscribes a
// Factory -- must not keep a tail.
func TestAnOwnerChangeStopsTheStaleTailAndResetsFromTheNewOne(t *testing.T) {
	t.Parallel()

	oldHost, newHost := newStandIn(t, "host-1"), newStandIn(t, "host-2")
	r := newRig(t, rigOptions{})
	r.dir.put(oldHost.observation(tenantA, session, 3))
	r.watch(t, tenantA, session)
	channel := sessionwire.HostLinkChannel(tenantA, session)
	oldHost.publish(t, channel, enduring(t, tenantA, session, 1))
	eventually(t, "E1", func() bool { return len(r.viewers.of(tenantA, session)) == 1 })

	r.tips.set(4)
	r.dir.put(newHost.observation(tenantA, session, 4))
	r.clock.tick()
	eventually(t, "the old Host to lose the tail", func() bool { return oldHost.subscribers(channel) == 0 })
	eventually(t, "the new tail", func() bool { return newHost.subscribers(channel) == 1 })
	newHost.publish(t, channel, enduring(t, tenantA, session, 5))
	eventually(t, "E5", func() bool { return len(r.viewers.of(tenantA, session)) == 3 })
	if got := joined(kinds(t, r.viewers.of(tenantA, session))); got != "E1 R1/4 E5" {
		t.Fatalf("viewers received %q, want E1, a reset from the new tail, then E5", got)
	}
}

// TestAMailboxOverflowIsRepairedNotBufferedWithoutBound: when the viewers fall
// behind the Host by more than the mailbox, the backlog is dropped and the tail
// is treated as lost -- reset and re-bound -- rather than growing.
func TestAMailboxOverflowIsRepairedNotBufferedWithoutBound(t *testing.T) {
	t.Parallel()

	host := newStandIn(t, "host-1")
	r := newRig(t, rigOptions{mailbox: 2})
	r.dir.put(host.observation(tenantA, session, 3))
	r.watch(t, tenantA, session)
	channel := sessionwire.HostLinkChannel(tenantA, session)
	gate := make(chan struct{})
	var release sync.Once
	open := func() { release.Do(func() { close(gate) }) }
	// Registered BEFORE the rig's own cleanup runs (cleanups run last-in
	// first-out), so a failing case releases the blocked drainer rather than
	// wedging the Relay's lock under the rig's Close.
	t.Cleanup(open)
	r.viewers.mu.Lock()
	r.viewers.gate = gate
	r.viewers.mu.Unlock()
	for seq := uint64(1); seq <= 10; seq++ {
		host.publish(t, channel, enduring(t, tenantA, session, seq))
	}
	eventually(t, "the tail to be withdrawn", func() bool { return host.count("unsubscribe", channel) >= 1 })
	r.tips.set(10)
	r.viewers.mu.Lock()
	r.viewers.gate = nil
	r.viewers.mu.Unlock()
	open()
	eventually(t, "a reset", func() bool {
		for _, k := range kinds(t, r.viewers.of(tenantA, session)) {
			if strings.HasPrefix(k, "R") && strings.HasSuffix(k, "/10") {
				return true
			}
		}
		return false
	})
	if got := len(r.viewers.of(tenantA, session)); got > 6 {
		t.Fatalf("the viewers received %d records: the backlog was not bounded (%v)", got, kinds(t, r.viewers.of(tenantA, session)))
	}
	eventually(t, "the tail to be re-subscribed", func() bool { return host.count("subscribe", channel) >= 2 })
}

// TestARecordTheRelayRefusesIsRepairedNotSkipped: a Host publication that is
// not a session-channel record is a hole in the stream, and a hole is
// repaired.
func TestARecordTheRelayRefusesIsRepairedNotSkipped(t *testing.T) {
	t.Parallel()

	host := newStandIn(t, "host-1")
	r := newRig(t, rigOptions{})
	r.dir.put(host.observation(tenantA, session, 3))
	r.watch(t, tenantA, session)
	channel := sessionwire.HostLinkChannel(tenantA, session)
	host.publish(t, channel, enduring(t, tenantA, session, 1))
	eventually(t, "E1", func() bool { return len(r.viewers.of(tenantA, session)) == 1 })
	r.tips.set(3)
	host.publish(t, channel, []byte(`{"type":"not-a-record"}`))
	eventually(t, "a reset", func() bool {
		got := kinds(t, r.viewers.of(tenantA, session))
		return len(got) >= 2 && got[1] == "R1/3"
	})
}

// TestAResetThatCannotBeBuiltMakesEveryViewerRepair: the fail-closed arm
// reaches the ClientLink as a channel-wide close.
func TestAResetThatCannotBeBuiltMakesEveryViewerRepair(t *testing.T) {
	t.Parallel()

	host := newStandIn(t, "host-1")
	r := newRig(t, rigOptions{})
	r.dir.put(host.observation(tenantA, session, 3))
	r.watch(t, tenantA, session)
	channel := sessionwire.HostLinkChannel(tenantA, session)
	r.tips.mu.Lock()
	r.tips.err = errors.New("store unavailable")
	r.tips.mu.Unlock()
	host.drop()
	eventually(t, "the viewers to be closed", func() bool { return r.viewers.closed(tenantA, session) >= 1 })
	_ = channel
}

// TestNewRefusesAnIncompleteComposition.
func TestNewRefusesAnIncompleteComposition(t *testing.T) {
	t.Parallel()

	good := livetail.Config{
		Links: nil, Viewers: func() livetail.Viewers { return nil }, MailboxLimit: 1, EventTimeout: time.Second,
	}
	if _, err := livetail.New(good); !errors.Is(err, livetail.ErrInvalidConfig) {
		t.Fatalf("nil Links = %v, want ErrInvalidConfig", err)
	}
}
