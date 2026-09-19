package hostlink

import (
	"context"
	"errors"
	"fmt"
	"time"

	centrifugego "github.com/centrifugal/centrifuge-go"
	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// ErrSubscribeUnsupported reports a link that cannot carry a session's live
// tail. Every link this package dials can; the error exists for a Link a test
// or a later transport supplies without the Subscriber capability, so a pool
// holding one fails a subscribe closed rather than pretending it is live.
var ErrSubscribeUnsupported = errors.New("hostlink: link cannot carry a session's live tail")

// ErrSubscribeRefused reports a session subscription the Host did not accept,
// or that ended before it was ever live.
//
// host v0.2.1 answers a subscribe with ErrorPermissionDenied (103) unless the
// link already holds the bind for that exact channel ON THE SAME CONNECTION
// (Multiplexer.MaySubscribe), so a refusal here usually means the bind that
// preceded it went over a connection that has since been replaced.
var ErrSubscribeRefused = errors.New("hostlink: the host did not accept the session subscription")

// ErrSubscriptionWithdrawn reports a pending subscription its owner gave up
// before the Host answered.
var ErrSubscriptionWithdrawn = errors.New("hostlink: the session subscription was withdrawn")

// SessionSink receives one session channel's live tail.
//
// EVERY METHOD IS CALLED ON THE TRANSPORT'S CALLBACK GOROUTINE AND MUST NOT
// BLOCK. centrifuge-go runs a publication handler synchronously on the
// goroutine that READS the connection (runHandlerSync), so a sink that waited
// for anything would stall every RPC reply on the link -- including, if the
// thing it waited for were a caller of Bind, the reply that caller is waiting
// for. A sink hands the event to its own goroutine and returns.
//
// The order a sink sees is the order the transport delivered: Subscribed
// before the first Publication of that subscription, and nothing after Ended
// except, possibly, Restored.
type SessionSink interface {
	// Subscribed reports the Host accepted the subscription. It is called
	// BEFORE Subscribe returns to its caller, so a sink can tell a
	// subscription started inside a known window from one started later.
	Subscribed()
	// Publication is one payload the Host published on the session channel,
	// exactly as it arrived. The slice is the sink's to keep.
	Publication(data []byte)
	// Ended reports a LIVE subscription that stopped without its owner asking:
	// the connection dropped or was closed, or the Host removed it. Every
	// publication from then until a new subscription is live is gone -- the
	// Host keeps no history -- and that is what a sink must repair.
	Ended()
	// Restored reports that the connection a subscription was lost to has
	// reconnected and negotiated again. Nothing is resubscribed: a Host
	// refuses a subscribe until the link re-binds on the NEW connection, so
	// re-binding and then subscribing is the sink owner's to sequence.
	Restored()
}

// Subscriber is the capability of a Link that carries a session's live tail.
//
// It is separate from Link, and discovered by assertion in the pool, because a
// subscription is a transport operation rather than a HostLink method: Core
// names no reserved method for it, a Host advertises none, and the capability
// gate that admits bind and attach has nothing to say about it.
type Subscriber interface {
	// Subscribe starts one session's live tail on this link and returns once
	// the Host has accepted it, refused it, or the context or the link's own
	// bound expired. A session already subscribed returns nil and keeps its
	// existing sink.
	Subscribe(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, sink SessionSink) error
	// Unsubscribe stops one session's live tail. It is the OWNER's call, so
	// its sink is not told Ended. It does not block on the Host.
	Unsubscribe(tenant sessionwire.TenantID, session sessionwire.SessionID)
}

// Subscribe starts a session's live tail on the link its route names.
//
// The route is REQUIRED, and that is the Host's rule restated rather than a
// preference: a Host accepts a subscribe only from a link that already holds
// the bind for that channel on the same connection, so a subscribe with no
// route is one the Host will refuse. The pool's lock is released before the
// link is asked, because a subscribe waits for the Host's answer and holding
// the pool across it would stall every bind on every Host for that long.
func (p *Pool) Subscribe(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, sink SessionSink) error {
	if sink == nil {
		return fmt.Errorf("%w: a nil sink", ErrInvalidConfig)
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrPoolClosed
	}
	host, ok := p.routes[routeKey{tenant: tenant, session: session}]
	if !ok {
		p.mu.Unlock()
		return fmt.Errorf("%w: session %q", ErrUnknownBinding, session)
	}
	pooled, ok := p.links[host]
	if !ok {
		p.mu.Unlock()
		return fmt.Errorf("%w: session %q is routed to %q, which has no link", ErrUnknownBinding, session, host)
	}
	link := pooled.link
	p.mu.Unlock()

	subscriber, ok := link.(Subscriber)
	if !ok {
		return fmt.Errorf("%w: %s", ErrSubscribeUnsupported, host)
	}
	return subscriber.Subscribe(ctx, tenant, session, sink)
}

// Unsubscribe stops a session's live tail on EVERY link that carries one.
//
// It does not consult the route, deliberately: an unsubscribe is how the owner
// drops a tail whose route is being given up, and a route may already have
// been released -- or evicted with its link -- by the time the owner gets
// here. A tail left on a link because its route was gone first is the stale
// subscription Gap 3's ruling names: a Host never unsubscribes a Factory, so
// nothing else would ever stop it.
func (p *Pool) Unsubscribe(tenant sessionwire.TenantID, session sessionwire.SessionID) {
	p.mu.Lock()
	links := make([]Link, 0, len(p.links))
	for _, pooled := range p.links {
		links = append(links, pooled.link)
	}
	p.mu.Unlock()
	for _, link := range links {
		if subscriber, ok := link.(Subscriber); ok {
			subscriber.Unsubscribe(tenant, session)
		}
	}
}

// sessionSub is one session channel's subscription on one link.
//
// It is identified by POINTER, and every transport callback compares the entry
// it was registered for against the one the link now holds for its channel. A
// callback for an entry that is no longer current -- the owner unsubscribed,
// the connection dropped, a newer subscription replaced it -- does nothing.
// That comparison is what keeps a publication from a dead subscription out of
// a newer one's sink.
type sessionSub struct {
	channel string
	sink    SessionSink
	sub     *centrifugego.Subscription
	// live is set once the Host accepted the subscribe. Guarded by the link's mu.
	live bool
	// settled is closed exactly once, when the outcome is known; err is the
	// outcome and is written before the close.
	settled chan struct{}
	err     error
}

// subscriptionState is the part of centrifugeLink this file owns. It is
// embedded so centrifuge.go's struct doc stays about the control plane.
//
// Both maps are guarded by the link's mu, and NOTHING that calls into the
// transport runs while mu is held: centrifuge-go may run one of this file's
// callbacks synchronously from a goroutine holding its own client lock, and a
// callback that waited for mu while mu's holder waited for that client lock
// would be a deadlock of the link.
type subscriptionState struct {
	subs map[string]*sessionSub
	// orphans are the sinks whose LIVE subscription was lost to a dropped
	// connection. They are told Restored on the next successful handshake and
	// forgotten. An owner's Unsubscribe does not remove one; see Unsubscribe.
	orphans map[string]SessionSink
	// subscribeTimeout bounds one subscribe's wait for the Host's answer, on
	// top of the caller's context. It is the dial timeout: a subscribe is one
	// round trip on a connection that is already up, and a Host slower than a
	// whole handshake is not going to answer.
	subscribeTimeout time.Duration
}

// subscribingTransportClosed is centrifuge-go v0.12.0's code for a subscription
// moved back to subscribing because the TRANSPORT closed (codes.go:21). It is
// unexported there, so it is restated, and the one reader is the
// connection-level handlers below: a per-subscription event carrying it is
// handled by onConnecting or onDisconnected, which see every subscription at
// once. TestTheTransportClosedSubscribingCodeIsPinned holds the value.
const subscribingTransportClosed uint32 = 1

// Subscribe implements Subscriber.
func (l *centrifugeLink) Subscribe(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, sink SessionSink) error {
	channel := sessionwire.HostLinkChannel(tenant, session)

	l.mu.Lock()
	if l.terminal != nil {
		err := l.terminal
		l.mu.Unlock()
		return err
	}
	if l.connecting {
		// A subscribe issued now would be QUEUED by the transport and sent on
		// the next connection, before this link has re-bound anything there --
		// so the Host would refuse it, or, worse, accept it after a bind raced
		// ahead and hand this link a tail nobody sequenced.
		l.mu.Unlock()
		return fmt.Errorf("hostlink: subscribe %s: %w", channel, ErrLinkReconnecting)
	}
	if existing := l.subs[channel]; existing != nil {
		l.mu.Unlock()
		return l.await(ctx, existing)
	}
	entry := &sessionSub{channel: channel, sink: sink, settled: make(chan struct{})}
	l.subs[channel] = entry
	l.mu.Unlock()

	sub, err := l.newSubscription(channel)
	if err != nil {
		l.withdraw(entry, err)
		return fmt.Errorf("hostlink: subscribe %s: %w", channel, err)
	}
	l.install(entry, sub)

	l.mu.Lock()
	current := l.subs[channel] == entry
	if current {
		entry.sub = sub
	}
	l.mu.Unlock()
	if !current {
		// Withdrawn between the reservation and now: the owner unsubscribed,
		// or the connection dropped. Nothing was sent.
		_ = l.client.RemoveSubscription(sub)
		return l.await(ctx, entry)
	}
	if err := sub.Subscribe(); err != nil {
		l.withdraw(entry, err)
		return fmt.Errorf("hostlink: subscribe %s: %w", channel, err)
	}
	return l.await(ctx, entry)
}

// newSubscription registers a subscription for channel with the transport,
// replacing a stale registration a previous subscription left behind.
func (l *centrifugeLink) newSubscription(channel string) (*centrifugego.Subscription, error) {
	sub, err := l.client.NewSubscription(channel)
	if !errors.Is(err, centrifugego.ErrDuplicateSubscription) {
		return sub, err
	}
	// A previous subscription's registration outlived it -- its removal runs
	// off the callback goroutine -- so it is removed here and the new one
	// registered. An old registration that is not yet unsubscribed is made so
	// first, which is what RemoveSubscription requires.
	if old, ok := l.client.GetSubscription(channel); ok {
		_ = old.Unsubscribe()
		_ = l.client.RemoveSubscription(old)
	}
	return l.client.NewSubscription(channel)
}

// install registers the callbacks for one entry. Each one is a comparison
// against the link's current entry for the channel and then at most one sink
// call, made with no lock held.
func (l *centrifugeLink) install(entry *sessionSub, sub *centrifugego.Subscription) {
	sub.OnSubscribed(func(centrifugego.SubscribedEvent) {
		l.mu.Lock()
		accepted := l.subs[entry.channel] == entry && !entry.live
		if accepted {
			entry.live = true
		}
		l.mu.Unlock()
		if !accepted {
			return
		}
		// The sink hears it BEFORE the waiter is released, so a sink can tell
		// a subscription that went live inside its caller's window from one
		// that went live afterwards.
		entry.sink.Subscribed()
		l.settle(entry, nil)
	})
	sub.OnPublication(func(event centrifugego.PublicationEvent) {
		l.mu.Lock()
		live := l.subs[entry.channel] == entry && entry.live
		l.mu.Unlock()
		if !live {
			return
		}
		entry.sink.Publication(append([]byte(nil), event.Data...))
	})
	sub.OnUnsubscribed(func(event centrifugego.UnsubscribedEvent) {
		// Reached for a Host refusal, a server-side unsubscribe and a client
		// Close. The owner's own Unsubscribe removes the entry FIRST, so it is
		// never current here and the owner's sink is not told Ended.
		l.lose(entry, fmt.Errorf("%w: %d %s", ErrSubscribeRefused, event.Code, event.Reason))
		go l.discard(sub)
	})
	sub.OnSubscribing(func(event centrifugego.SubscribingEvent) {
		if event.Code == subscribingTransportClosed {
			// The connection-level handler sees every subscription at once
			// and is the one that decides; see onConnecting.
			return
		}
		l.mu.Lock()
		live := l.subs[entry.channel] == entry && entry.live
		l.mu.Unlock()
		if !live {
			// The subscribe this link itself issued fires this too.
			return
		}
		// A live subscription moved back to subscribing while the connection
		// stayed up: the Host unsubscribed it with a resubscribe code, and the
		// transport is ALREADY resubscribing it on its own. That automatic
		// resubscribe is exactly what this link may not allow -- publications
		// in between are gone and nobody would be told -- so the subscription
		// is ended and withdrawn, and the sink repairs.
		l.lose(entry, ErrSubscribeRefused)
		go l.discard(sub)
	})
	sub.OnError(func(event centrifugego.SubscriptionErrorEvent) {
		// A subscribe error the transport would retry on its own (a temporary
		// one) leaves a pending entry waiting; answering the waiter now keeps
		// the retry -- which could land after a later re-bind -- from being
		// the thing that makes this subscription live.
		l.mu.Lock()
		pending := l.subs[entry.channel] == entry && !entry.live
		if pending {
			delete(l.subs, entry.channel)
		}
		l.mu.Unlock()
		if !pending {
			return
		}
		l.settle(entry, fmt.Errorf("%w: %v", ErrSubscribeRefused, event.Error))
		go l.discard(sub)
	})
}

// lose ends one entry that stopped without its owner asking: a live one tells
// its sink Ended, a pending one answers its waiter.
func (l *centrifugeLink) lose(entry *sessionSub, cause error) {
	l.mu.Lock()
	current := l.subs[entry.channel] == entry
	live := entry.live
	if current {
		delete(l.subs, entry.channel)
	}
	l.mu.Unlock()
	if !current {
		return
	}
	if live {
		entry.sink.Ended()
		return
	}
	l.settle(entry, cause)
}

// withdraw removes an entry the owner's own path gave up on, answering its
// waiter with err. Its sink is not told anything: it never went live.
func (l *centrifugeLink) withdraw(entry *sessionSub, err error) {
	l.mu.Lock()
	if l.subs[entry.channel] == entry {
		delete(l.subs, entry.channel)
	}
	l.mu.Unlock()
	l.settle(entry, err)
}

// settle records an entry's outcome once.
func (l *centrifugeLink) settle(entry *sessionSub, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	select {
	case <-entry.settled:
		return
	default:
	}
	entry.err = err
	close(entry.settled)
}

// await waits for an entry's outcome, bounded by the caller's context and the
// link's own subscribe bound. An entry that is not answered in time is
// withdrawn: a subscription that went live after its caller had given up would
// be a tail nobody sequenced.
func (l *centrifugeLink) await(ctx context.Context, entry *sessionSub) error {
	timer := time.NewTimer(l.subscribeTimeout)
	defer timer.Stop()
	var cause error
	select {
	case <-entry.settled:
		return entry.err
	case <-ctx.Done():
		cause = ctx.Err()
	case <-timer.C:
		cause = fmt.Errorf("the host did not answer within %v", l.subscribeTimeout)
	}
	l.mu.Lock()
	current := l.subs[entry.channel] == entry
	if current {
		delete(l.subs, entry.channel)
	}
	sub := entry.sub
	l.mu.Unlock()
	l.settle(entry, fmt.Errorf("hostlink: subscribe %s: %w", entry.channel, cause))
	if current && sub != nil {
		l.discard(sub)
	}
	// The outcome may have landed between the select and the withdrawal; the
	// entry's recorded outcome is the answer either way.
	<-entry.settled
	return entry.err
}

// Unsubscribe implements Subscriber.
func (l *centrifugeLink) Unsubscribe(tenant sessionwire.TenantID, session sessionwire.SessionID) {
	channel := sessionwire.HostLinkChannel(tenant, session)
	l.mu.Lock()
	entry := l.subs[channel]
	if entry != nil {
		delete(l.subs, channel)
	}
	// The orphan, if any, is deliberately KEPT. A repair stops a lost tail
	// through this very call before it re-binds, and that re-bind fails while
	// the link reconnects -- so Restored is the only thing left that can
	// re-bind the session once the link is back. A session nobody watches any
	// more is the sink owner's to ignore.
	l.mu.Unlock()
	if entry == nil {
		return
	}
	l.settle(entry, ErrSubscriptionWithdrawn)
	l.mu.Lock()
	sub := entry.sub
	l.mu.Unlock()
	if sub != nil {
		l.discard(sub)
	}
}

// discard unsubscribes a subscription at the transport and forgets its
// registration. It never blocks on the Host: an unsubscribe while connected is
// one write, and while not connected it is local only.
//
// The registration is removed only while it is still THIS subscription's.
// RemoveSubscription deletes by channel name, so an unguarded removal running
// late would delete a newer subscription's registration for the same channel,
// and that one could then never be unsubscribed at the Host.
func (l *centrifugeLink) discard(sub *centrifugego.Subscription) {
	_ = sub.Unsubscribe()
	if current, ok := l.client.GetSubscription(sub.Channel); ok && current == sub {
		_ = l.client.RemoveSubscription(sub)
	}
}

// endAllSubscriptions ends every subscription on a connection that went away,
// and is the ONE place a dropped connection's subscriptions are handled.
//
// It runs inside the transport's connecting or disconnected callback.
// centrifuge-go moves every live subscription back to subscribing when a
// transport closes, and RESUBSCRIBES them itself as soon as the next
// connection is up (client.go:1476, :1531) -- before this link has re-bound
// anything on that connection. A Host refuses such a subscribe (MaySubscribe
// needs the bind on the SAME connection), or, if a queued bind raced ahead of
// it, accepts it and starts a tail with a hole behind it that nobody was told
// about.
//
// TWO MECHANISMS, and only the first is load-bearing. The entry is removed
// from subs HERE, synchronously, so every callback of the old subscription --
// including a publication from a resubscribe the Host accepted -- finds it not
// current and is dropped, and its sink hears Ended now. The transport-side
// withdrawal (discard) runs on its OWN GOROUTINE and normally lands inside the
// reconnect delay, while the client is not connected, so the automatic
// resubscribe finds nothing to send. It may not run inline: Client.Close holds
// the client's lock while it waits for this very callback queue to drain
// (moveToClosed), and a callback that asked for that lock would deadlock the
// close. If it lands late, the Host is sent an unsubscribe for a tail this
// link already ignores, and the owner's next Subscribe replaces the stale
// registration (newSubscription).
//
// orphan says whether the sinks should hear Restored after the next
// handshake: true for a reconnect, false for a terminal close.
func (l *centrifugeLink) endAllSubscriptions(orphan bool) {
	l.mu.Lock()
	entries := make([]*sessionSub, 0, len(l.subs))
	for channel, entry := range l.subs {
		entries = append(entries, entry)
		delete(l.subs, channel)
		if orphan && entry.live {
			l.orphans[channel] = entry.sink
		}
	}
	if !orphan {
		l.orphans = map[string]SessionSink{}
	}
	l.mu.Unlock()
	for _, entry := range entries {
		l.mu.Lock()
		live, sub := entry.live, entry.sub
		l.mu.Unlock()
		if sub != nil {
			go l.discard(sub)
		}
		if live {
			entry.sink.Ended()
			continue
		}
		l.settle(entry, fmt.Errorf("hostlink: subscribe %s: %w", entry.channel, ErrLinkReconnecting))
	}
}

// restoreOrphans tells every sink lost to the previous connection that the
// link is usable again.
func (l *centrifugeLink) restoreOrphans() {
	l.mu.Lock()
	sinks := make([]SessionSink, 0, len(l.orphans))
	for channel, sink := range l.orphans {
		sinks = append(sinks, sink)
		delete(l.orphans, channel)
	}
	l.mu.Unlock()
	for _, sink := range sinks {
		sink.Restored()
	}
}
