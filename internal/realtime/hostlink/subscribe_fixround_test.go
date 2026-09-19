package hostlink_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	centrifuge "github.com/centrifugal/centrifuge"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/hostlink"
)

// resubscribeRounds runs owner-unsubscribe then re-subscribe of ONE channel on
// ONE link, `rounds` times, and reports every round whose new tail was dead
// at the Host: the v0.4.0 spec gate's F2. centrifuge-go addresses the
// unsubscribe it sends a Host by CHANNEL NAME, so a late withdrawal of the old
// subscription that lands after the new one is registered kills the new one,
// silently -- the link believes it live and no sink hears anything.
func resubscribeRounds(t *testing.T, rounds int, between func(*hostlink.Pool, *hostServer)) {
	t.Helper()
	host := newHostServer(t, hostOptions{})
	pool := boundPool(t, host)
	channel := sessionwire.HostLinkChannel(tenant, "s-1")
	ctx := context.Background()
	var dead []string
	for round := 0; round < rounds; round++ {
		if err := pool.Subscribe(ctx, tenant, "s-1", &recordingSink{}); err != nil {
			t.Fatalf("round %d: first Subscribe: %v", round, err)
		}
		waitUntil(t, fmt.Sprintf("round %d: the first tail live (a late withdrawal of the previous round's tail kills it)", round), func() bool { return host.subscribers(channel) == 1 })
		pool.Unsubscribe(tenant, "s-1")
		between(pool, host)
		fresh := &recordingSink{}
		if err := pool.Subscribe(ctx, tenant, "s-1", fresh); err != nil {
			t.Fatalf("round %d: second Subscribe: %v", round, err)
		}
		// Every late callback and goroutine of the first tail lands here.
		time.Sleep(30 * time.Millisecond)
		host.publish(t, channel, payload(round))
		deadline := time.Now().Add(time.Second)
		for fresh.count("publication") == 0 && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if fresh.count("publication") == 0 {
			dead = append(dead, fmt.Sprintf("round %d: host subscribers=%d sink=%v", round, host.subscribers(channel), fresh.snapshot()))
		}
		pool.Unsubscribe(tenant, "s-1")
		waitUntil(t, "the round's tail withdrawn", func() bool { return host.subscribers(channel) == 0 })
	}
	if len(dead) > 0 {
		t.Fatalf("%d of %d re-subscriptions were silently dead at the Host (a late unsubscribe of the previous tail):\n%v",
			len(dead), rounds, dead)
	}
}

// TestAnImmediateResubscribeIsNotKilledByTheOldTailsWithdrawal is the gate's
// probe, committed: no RPC between the owner's unsubscribe and the new
// subscribe, which is the widest the window gets.
func TestAnImmediateResubscribeIsNotKilledByTheOldTailsWithdrawal(t *testing.T) {
	t.Parallel()
	resubscribeRounds(t, 40, func(*hostlink.Pool, *hostServer) {})
}

// TestAResubscribeAfterARebindIsNotKilledByTheOldTailsWithdrawal is the
// repair's own shape: an unbind and a bind between the two.
func TestAResubscribeAfterARebindIsNotKilledByTheOldTailsWithdrawal(t *testing.T) {
	t.Parallel()
	resubscribeRounds(t, 20, func(pool *hostlink.Pool, host *hostServer) {
		if err := pool.Unbind(context.Background(), unbindRequest(hostOne, "s-1")); err != nil {
			t.Fatalf("Unbind: %v", err)
		}
		if err := pool.Bind(context.Background(), host.target(), bindRequest(hostOne, "s-1")); err != nil {
			t.Fatalf("Bind: %v", err)
		}
	})
}

// TestAReconnectIntoAHostThatFailsNegotiationRestoresNothing (quality gate
// W14): Restored says the link is usable again, so it may be sent only after
// the new connection's reply is verified. A Host restarted at another wire
// version ends the tail and makes the link terminal; nothing is restored.
func TestAReconnectIntoAHostThatFailsNegotiationRestoresNothing(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{})
	pool := boundPool(t, host)
	sink := &recordingSink{}
	if err := pool.Subscribe(context.Background(), tenant, "s-1", sink); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	host.setNegotiation(`{"version":2}`)
	host.disconnectEveryone(centrifuge.Disconnect{Code: 4000, Reason: "test reconnect"})
	waitUntil(t, "a second handshake", func() bool { return len(host.connects()) >= 2 })
	waitUntil(t, "the link to go terminal", func() bool {
		err := pool.Subscribe(context.Background(), tenant, "s-1", &recordingSink{})
		return errors.Is(err, hostlink.ErrUnsupportedProtocol)
	})
	time.Sleep(100 * time.Millisecond)
	if got := sink.count("restored"); got != 0 {
		t.Fatalf("a reconnect whose reply failed verification told the sink Restored %d times (events %v)", got, sink.snapshot())
	}
	if got := sink.count("ended"); got != 1 {
		t.Fatalf("the tail was told Ended %d times, want 1 (events %v)", got, sink.snapshot())
	}
}
