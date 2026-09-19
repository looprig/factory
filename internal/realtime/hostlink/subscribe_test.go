package hostlink_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	centrifuge "github.com/centrifugal/centrifuge"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/hostlink"
)

// These cases drive the live tail over the REAL transport against the stand-in
// Host, whose subscribe gate is host v0.2.1's Multiplexer.MaySubscribe: a
// connection may subscribe to a session channel only while it holds the bind
// that minted it, and a reconnect is a new connection holding nothing.

// sinkEvent is one thing a sink was told, in order.
type sinkEvent struct {
	kind string // "subscribed", "publication", "ended", "restored"
	data string
}

// recordingSink records every call in arrival order and never blocks.
type recordingSink struct {
	mu     sync.Mutex
	events []sinkEvent
}

func (s *recordingSink) add(kind, data string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, sinkEvent{kind: kind, data: data})
}

func (s *recordingSink) Subscribed()             { s.add("subscribed", "") }
func (s *recordingSink) Publication(data []byte) { s.add("publication", string(data)) }
func (s *recordingSink) Ended()                  { s.add("ended", "") }
func (s *recordingSink) Restored()               { s.add("restored", "") }

func (s *recordingSink) snapshot() []sinkEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]sinkEvent(nil), s.events...)
}

func (s *recordingSink) count(kind string) int {
	n := 0
	for _, event := range s.snapshot() {
		if event.kind == kind {
			n++
		}
	}
	return n
}

func (s *recordingSink) publications() []string {
	var out []string
	for _, event := range s.snapshot() {
		if event.kind == "publication" {
			out = append(out, event.data)
		}
	}
	return out
}

// boundPool dials the stand-in through a real pool and binds s-1 on it.
func boundPool(t *testing.T, host *hostServer) *hostlink.Pool {
	t.Helper()
	pool := poolOver(t, dialerFor(t, serviceToken))
	if err := pool.Bind(context.Background(), host.target(), bindRequest(hostOne, "s-1")); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	return pool
}

func payload(seq int) []byte {
	return []byte(fmt.Sprintf(`{"type":"test","seq":%d}`, seq))
}

// TestASubscribeAfterABindCarriesTheHostsPublicationsInOrder is the headline:
// bind, subscribe on the SAME connection, and every payload the Host publishes
// reaches the sink byte for byte and in order.
func TestASubscribeAfterABindCarriesTheHostsPublicationsInOrder(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{})
	pool := boundPool(t, host)
	sink := &recordingSink{}
	if err := pool.Subscribe(context.Background(), tenant, "s-1", sink); err != nil {
		t.Fatalf("Subscribe after a bind: %v", err)
	}
	// Subscribed is told BEFORE Subscribe returns; see SessionSink.
	if got := sink.snapshot(); len(got) != 1 || got[0].kind != "subscribed" {
		t.Fatalf("sink after Subscribe returned = %v, want exactly [subscribed]", got)
	}
	channel := sessionwire.HostLinkChannel(tenant, "s-1")
	for seq := 1; seq <= 5; seq++ {
		host.publish(t, channel, payload(seq))
	}
	waitUntil(t, "five publications", func() bool { return sink.count("publication") == 5 })
	for index, got := range sink.publications() {
		if want := string(payload(index + 1)); got != want {
			t.Errorf("publication %d = %s, want %s", index, got, want)
		}
	}
	// Another session's channel is not this sink's.
	host.publish(t, sessionwire.HostLinkChannel(tenant, "s-2"), payload(99))
	host.publish(t, channel, payload(6))
	waitUntil(t, "the sixth publication", func() bool { return sink.count("publication") == 6 })
	if got := sink.publications()[5]; got != string(payload(6)) {
		t.Fatalf("a publication on another session's channel reached this sink: %s", got)
	}
}

// TestASubscribeWithNoBindIsRefused holds both halves of the ordering rule. The
// pool refuses a subscribe for a session it holds no route for, and a link
// subscribing without the bind is refused BY THE HOST, which is why the order
// is bind first. The control is the same subscribe after a bind.
func TestASubscribeWithNoBindIsRefused(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{})
	pool := poolOver(t, dialerFor(t, serviceToken))
	if err := pool.Subscribe(context.Background(), tenant, "s-1", &recordingSink{}); !errors.Is(err, hostlink.ErrUnknownBinding) {
		t.Fatalf("pool Subscribe with no route = %v, want ErrUnknownBinding", err)
	}

	// A route for ANOTHER session on a live link is no route for this one: the
	// pool must refuse locally rather than send a subscribe the Host refuses.
	if err := pool.Bind(context.Background(), host.target(), bindRequest(hostOne, "s-2")); err != nil {
		t.Fatalf("Bind s-2: %v", err)
	}
	if err := pool.Subscribe(context.Background(), tenant, "s-1", &recordingSink{}); !errors.Is(err, hostlink.ErrUnknownBinding) {
		t.Fatalf("pool Subscribe for an unrouted session on a live link = %v, want ErrUnknownBinding", err)
	}
	if n := host.subscribes(sessionwire.HostLinkChannel(tenant, "s-1")); n != 0 {
		t.Fatalf("the unrouted subscribe reached the Host %d times", n)
	}

	link := mustDial(t, host).(hostlink.Subscriber)
	sink := &recordingSink{}
	err := link.Subscribe(context.Background(), tenant, "s-1", sink)
	if !errors.Is(err, hostlink.ErrSubscribeRefused) {
		t.Fatalf("link Subscribe with no bind = %v, want ErrSubscribeRefused", err)
	}
	if got := sink.count("subscribed"); got != 0 {
		t.Fatalf("a refused subscribe told its sink Subscribed %d times", got)
	}

	// Control: bind on this link, and the same subscribe is accepted.
	if err := mustDialBind(t, link, "s-1"); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := link.Subscribe(context.Background(), tenant, "s-1", sink); err != nil {
		t.Fatalf("the control subscribe after a bind: %v", err)
	}
}

func mustDialBind(t *testing.T, link hostlink.Subscriber, session sessionwire.SessionID) error {
	t.Helper()
	return link.(hostlink.Link).Bind(context.Background(), bindRequest(hostOne, session))
}

// TestASecondSubscribeKeepsTheFirstSink holds idempotence: the Host sees one
// subscribe, and the first sink keeps receiving.
func TestASecondSubscribeKeepsTheFirstSink(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{})
	pool := boundPool(t, host)
	first, second := &recordingSink{}, &recordingSink{}
	if err := pool.Subscribe(context.Background(), tenant, "s-1", first); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := pool.Subscribe(context.Background(), tenant, "s-1", second); err != nil {
		t.Fatalf("second Subscribe: %v", err)
	}
	channel := sessionwire.HostLinkChannel(tenant, "s-1")
	if got := host.subscribes(channel); got != 1 {
		t.Fatalf("the Host answered %d subscribes, want 1", got)
	}
	host.publish(t, channel, payload(1))
	waitUntil(t, "the publication", func() bool { return first.count("publication") == 1 })
	if got := second.snapshot(); len(got) != 0 {
		t.Fatalf("the second sink was told %v, want nothing", got)
	}
}

// TestAnOwnersUnsubscribeStopsTheTailAndTellsTheSinkNothing: the Host drops
// the subscription, and the sink is not told Ended, because an owner that asked
// has nothing to repair.
func TestAnOwnersUnsubscribeStopsTheTailAndTellsTheSinkNothing(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{})
	pool := boundPool(t, host)
	sink := &recordingSink{}
	if err := pool.Subscribe(context.Background(), tenant, "s-1", sink); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	channel := sessionwire.HostLinkChannel(tenant, "s-1")
	waitUntil(t, "the Host to hold the subscription", func() bool { return host.subscribers(channel) == 1 })

	pool.Unsubscribe(tenant, "s-1")
	waitUntil(t, "the Host to drop the subscription", func() bool { return host.subscribers(channel) == 0 })
	host.publish(t, channel, payload(1))
	time.Sleep(100 * time.Millisecond)
	if got := sink.snapshot(); len(got) != 1 || got[0].kind != "subscribed" {
		t.Fatalf("sink after the owner's unsubscribe = %v, want only [subscribed]", got)
	}
}

// TestAnUnsubscribeReachesATailWhoseRouteIsAlreadyGone is the stale
// subscription the ruling names: a Host never unsubscribes a Factory, so a tail
// whose route was released first must still be stoppable.
func TestAnUnsubscribeReachesATailWhoseRouteIsAlreadyGone(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{})
	pool := boundPool(t, host)
	if err := pool.Subscribe(context.Background(), tenant, "s-1", &recordingSink{}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	channel := sessionwire.HostLinkChannel(tenant, "s-1")
	if err := pool.Unbind(context.Background(), unbindRequest(hostOne, "s-1")); err != nil {
		t.Fatalf("Unbind: %v", err)
	}
	// The stand-in, like Host, does not unsubscribe on unbind.
	time.Sleep(50 * time.Millisecond)
	if got := host.subscribers(channel); got != 1 {
		t.Fatalf("after an unbind the Host holds %d subscriptions; the stand-in no longer models Host", got)
	}
	pool.Unsubscribe(tenant, "s-1")
	waitUntil(t, "the stale subscription to go", func() bool { return host.subscribers(channel) == 0 })
}

// TestADroppedConnectionEndsTheTailAndIsNotResubscribedBehindABind is the
// reconnect-ordering risk the ruling calls the big one.
//
// centrifuge-go resubscribes every subscription on its own when the next
// connection is up. A Host refuses that (no bind on the new connection yet) --
// or, if a bind raced ahead of it, ACCEPTS it and the tail restarts over a hole
// nobody was told about. So the link must end every tail on the drop, tell the
// sink Ended, tell it Restored once the new connection has negotiated, and
// send the Host no subscribe of its own. The owner then re-binds and
// subscribes, in that order, and the tail is live again.
func TestADroppedConnectionEndsTheTailAndIsNotResubscribedBehindABind(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{})
	pool := boundPool(t, host)
	sink := &recordingSink{}
	if err := pool.Subscribe(context.Background(), tenant, "s-1", sink); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	channel := sessionwire.HostLinkChannel(tenant, "s-1")

	host.disconnectEveryone(centrifuge.Disconnect{Code: 4000, Reason: "test reconnect"})
	waitUntil(t, "Ended", func() bool { return sink.count("ended") == 1 })
	waitUntil(t, "Restored", func() bool { return sink.count("restored") == 1 })
	// The order the sink saw is the order a repair needs.
	got := sink.snapshot()
	if len(got) != 3 || got[0].kind != "subscribed" || got[1].kind != "ended" || got[2].kind != "restored" {
		t.Fatalf("sink = %v, want [subscribed ended restored]", got)
	}

	// Re-bind FIRST on the new connection. If the transport had resubscribed
	// on its own, the stand-in would now hold an accepted subscription this
	// link never asked for, or a refused one it counted.
	if err := pool.Bind(context.Background(), host.target(), bindRequest(hostOne, "s-1")); err != nil {
		t.Fatalf("re-bind after the reconnect: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	if n := host.subscribes(channel); n != 1 {
		t.Fatalf("the Host answered %d subscribes before the owner subscribed again, want 1 "+
			"(the transport resubscribed on its own)", n)
	}
	if n := host.subscribers(channel); n != 0 {
		t.Fatalf("the Host holds %d subscriptions nobody asked for", n)
	}

	resubscribed := &recordingSink{}
	if err := pool.Subscribe(context.Background(), tenant, "s-1", resubscribed); err != nil {
		t.Fatalf("Subscribe after re-bind: %v", err)
	}
	host.publish(t, channel, payload(7))
	waitUntil(t, "a publication on the new tail", func() bool { return resubscribed.count("publication") == 1 })
	if got := sink.count("publication"); got != 0 {
		t.Fatalf("the ended sink received %d publications from the new tail", got)
	}
}

// TestAServerSideUnsubscribeEndsTheTail covers both codes a Host could use. A
// code below 2500 unsubscribes; a code at or above it makes the transport
// resubscribe ON ITS OWN, which this link may not allow -- so both end the
// tail and the sink repairs.
func TestAServerSideUnsubscribeEndsTheTail(t *testing.T) {
	t.Parallel()

	for _, code := range []uint32{2000, 2500} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			t.Parallel()
			host := newHostServer(t, hostOptions{})
			pool := boundPool(t, host)
			sink := &recordingSink{}
			if err := pool.Subscribe(context.Background(), tenant, "s-1", sink); err != nil {
				t.Fatalf("Subscribe: %v", err)
			}
			channel := sessionwire.HostLinkChannel(tenant, "s-1")
			host.unsubscribeEveryone(channel, code)
			waitUntil(t, "Ended", func() bool { return sink.count("ended") == 1 })
			// The transport's own resubscribe (code 2500) is withdrawn: the
			// Host is left holding nothing, although the bind would have let
			// it accept one.
			waitUntil(t, "the Host to hold no subscription", func() bool { return host.subscribers(channel) == 0 })
			time.Sleep(100 * time.Millisecond)
			if n := host.subscribers(channel); n != 0 {
				t.Fatalf("the Host holds %d subscriptions after the tail ended", n)
			}
			host.publish(t, channel, payload(1))
			time.Sleep(100 * time.Millisecond)
			if got := sink.count("publication"); got != 0 {
				t.Fatalf("an ended tail delivered %d publications", got)
			}
			if got := sink.count("restored"); got != 0 {
				t.Fatalf("a tail lost on a live connection was told Restored %d times", got)
			}
		})
	}
}

// TestATerminalCloseEndsTheTailAndNeverRestoresIt: a terminal-band close is a
// link that will not come back, so there is no Restored to wait for.
func TestATerminalCloseEndsTheTailAndNeverRestoresIt(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{})
	pool := boundPool(t, host)
	sink := &recordingSink{}
	if err := pool.Subscribe(context.Background(), tenant, "s-1", sink); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	host.disconnectEveryone(centrifuge.Disconnect{Code: 3500, Reason: "terminal"})
	waitUntil(t, "Ended", func() bool { return sink.count("ended") == 1 })
	time.Sleep(200 * time.Millisecond)
	if got := sink.count("restored"); got != 0 {
		t.Fatalf("a terminal close was told Restored %d times", got)
	}
}

// TestABindOnATerminalLinkEvictsIt: the re-bind a lost tail makes must be able
// to reach a Host that closed this replica out, so the dead link is dropped
// and the next bind dials afresh.
func TestABindOnATerminalLinkEvictsIt(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{})
	pool := boundPool(t, host)
	sink := &recordingSink{}
	if err := pool.Subscribe(context.Background(), tenant, "s-1", sink); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	host.disconnectEveryone(centrifuge.Disconnect{Code: 3500, Reason: "terminal"})
	// Wait on the tail's Ended -- which the link reports from the same
	// terminal callback that marks it dead -- and NOT by polling Bind: an RPC
	// racing the transport's own close trips centrifuge-go v0.12.0's data race
	// (client.go:2187 against :457), the suite's known third-party flake.
	waitUntil(t, "the terminal close to reach the link", func() bool { return sink.count("ended") == 1 })
	var disconnect *hostlink.HostDisconnect
	if err := pool.Bind(context.Background(), host.target(), bindRequest(hostOne, "s-1")); !errors.As(err, &disconnect) {
		t.Fatalf("a bind on the terminal link = %v, want its *HostDisconnect", err)
	}
	if got := pool.Links(); got != 0 {
		t.Fatalf("the pool kept %d links after a bind met a terminal one", got)
	}
	if err := pool.Bind(context.Background(), host.target(), bindRequest(hostOne, "s-1")); err != nil {
		t.Fatalf("the next bind did not dial afresh: %v", err)
	}
	if got := len(host.connects()); got != 2 {
		t.Fatalf("the Host saw %d handshakes, want 2", got)
	}
}

// TestASubscribeTheHostNeverAnswersIsWithdrawn: a subscription that went live
// after its caller had given up would be a tail nobody sequenced, so the
// caller's deadline withdraws it and a late acceptance is dropped.
func TestASubscribeTheHostNeverAnswersIsWithdrawn(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	host := newHostServer(t, hostOptions{})
	host.holdSubscribes(release)
	pool := boundPool(t, host)
	sink := &recordingSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := pool.Subscribe(ctx, tenant, "s-1", sink); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Subscribe past its deadline = %v, want context.DeadlineExceeded", err)
	}
	close(release)
	channel := sessionwire.HostLinkChannel(tenant, "s-1")
	waitUntil(t, "the late subscription to be withdrawn", func() bool {
		return host.subscribes(channel) == 1 && host.subscribers(channel) == 0
	})
	host.publish(t, channel, payload(1))
	time.Sleep(100 * time.Millisecond)
	if got := sink.snapshot(); len(got) != 0 {
		t.Fatalf("a withdrawn subscribe told its sink %v", got)
	}
}

// TestAnOwnersUnsubscribeDuringTheReconnectStillHearsRestored: a repair stops
// a lost tail -- an owner Unsubscribe -- before it re-binds, and that re-bind
// fails while the link reconnects. Restored is then the only thing left that
// re-binds the session, so the owner's own stop must not cancel it.
func TestAnOwnersUnsubscribeDuringTheReconnectStillHearsRestored(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{})
	dialer, err := hostlink.NewCentrifugeDialer(hostlink.DialerConfig{
		Credential: staticCredential(serviceToken),
		Version:    buildVersion,
		Limits: hostlink.Limits{
			MaxLinks: 4, DialTimeout: 2 * time.Second, IdleTimeout: time.Minute,
			ReconnectMin: 500 * time.Millisecond, ReconnectMax: 500 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("NewCentrifugeDialer: %v", err)
	}
	pool := poolOver(t, dialer)
	if err := pool.Bind(context.Background(), host.target(), bindRequest(hostOne, "s-1")); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	sink := &recordingSink{}
	if err := pool.Subscribe(context.Background(), tenant, "s-1", sink); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	host.disconnectEveryone(centrifuge.Disconnect{Code: 4000, Reason: "test reconnect"})
	waitUntil(t, "Ended", func() bool { return sink.count("ended") == 1 })
	if got := sink.count("restored"); got != 0 {
		t.Fatalf("Restored arrived inside a 500ms reconnect delay; the case cannot drive the window")
	}
	pool.Unsubscribe(tenant, "s-1")
	waitUntil(t, "Restored", func() bool { return sink.count("restored") == 1 })
}
