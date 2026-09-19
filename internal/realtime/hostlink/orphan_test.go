package hostlink_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/hostlink"
)

// TestAWithdrawalInsideTheSubscribeWindowLeavesNoOrphanAtTheHost is the v0.4.0
// regate's N4, made deterministic. An owner Unsubscribe landing after a
// subscription is registered but before its subscribe is sent removed the
// entry and the registration -- and the subscribe went out anyway, leaving the
// Host holding a subscription nothing on this link would ever withdraw (seen
// 1 in 400 in a tight race; cleared by the Host within 3.5s). The hook puts
// the withdrawal exactly there. The Host must then hold no subscriber, and the
// next subscribe on the link must work and carry a publication (the control).
func TestAWithdrawalInsideTheSubscribeWindowLeavesNoOrphanAtTheHost(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{})
	link := mustDial(t, host)
	if err := link.Bind(context.Background(), bindRequest(hostOne, "s-1")); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	subscriber := link.(hostlink.Subscriber)
	channel := sessionwire.HostLinkChannel(tenant, "s-1")
	var once sync.Once
	hostlink.SetBeforeSubscribeSend(link, func() {
		once.Do(func() { subscriber.Unsubscribe(tenant, "s-1") })
	})

	err := subscriber.Subscribe(context.Background(), tenant, "s-1", &recordingSink{})
	if !errors.Is(err, hostlink.ErrSubscriptionWithdrawn) {
		t.Fatalf("a subscribe withdrawn inside its window = %v, want ErrSubscriptionWithdrawn", err)
	}
	// Give a stray subscribe every chance to arrive.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if n := host.subscribers(channel); n != 0 {
			t.Fatalf("the Host holds %d subscriber(s) for a subscription the link withdrew: an orphan", n)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if n := host.subscribes(channel); n != 0 {
		t.Fatalf("the Host answered %d subscribe(s) for a withdrawn subscription, want none sent", n)
	}

	fresh := &recordingSink{}
	if err := subscriber.Subscribe(context.Background(), tenant, "s-1", fresh); err != nil {
		t.Fatalf("the next subscribe on the link: %v", err)
	}
	host.publish(t, channel, payload(1))
	eventuallyTrue(t, "the next subscription to carry a publication", func() bool { return fresh.count("publication") == 1 })
}

func eventuallyTrue(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not happen", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
