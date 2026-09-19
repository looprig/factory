// Package livetail carries a watched session's live output from the Host that
// owns it to the ClientLink viewers watching it on this replica. It is Gap 3.
//
// # What it composes, and what it adds
//
// Almost nothing here is new. routing.Relay already owns the bounded queues,
// the contiguity tracking, the session.reset and the A7.3 repair; routing.Demand
// already decides when a watched session is bound; the HostLink pool already
// holds the one connection per Host. What was missing is three edges:
//
//   - a HostLink SUBSCRIPTION to the session's channel, made after a successful
//     bind on the same connection (hostlink.Subscriber) -- Plane.Bind;
//   - a ClientLink PUBLISH to `session:{tenant}:{session}`, the channel a
//     viewer already subscribes to (Viewers) -- Plane.Publish;
//   - something to drive the Relay between them -- the per-session mailbox.
//
// # The rule the whole package serves
//
// A Host keeps NO history. Whatever it publishes while this replica is not
// subscribed is gone, and a viewer does not know. So every time a session's
// tail STARTS -- except the one start that happens inside the first viewer's
// own subscribe, which that viewer's durable read covers -- every viewer is sent
// a session.reset, AFTER the new tail is live, so its repair reads the journal
// only once nothing further can be missed. And every time a tail STOPS without
// being asked to, the Relay's HostLinkClosed repair runs: it resets every
// viewer and re-binds, and re-binding re-subscribes.
//
// # Reconnect ordering
//
// centrifuge-go resubscribes on its own after a reconnect, before anything is
// re-bound, and a Host refuses that (MaySubscribe needs the bind on the SAME
// connection). The link withdraws every subscription on the drop and tells this
// package Ended; this package runs the repair, whose re-bind fails fast while
// the link reconnects; the link then says Restored once the new connection has
// negotiated, and this package re-binds -- which subscribes -- and the new tail
// going live sends the reset. Re-bind, then subscribe, then reset: in that
// order, owned here.
//
// # Locking
//
// Plane.mu is a LEAF: nothing is called while it is held except starting a
// goroutine. That is what lets the transport's callbacks (hostlink sinks) and
// routing.Demand (Watcher, called under Demand's lock) reach it without ever
// being part of a cycle. The Relay is called only from a session's drainer
// goroutine, which holds no lock of this package while it does so.
package livetail

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/hostlink"
	"github.com/looprig/factory/internal/routing"
)

// ErrInvalidConfig is the class of every New rejection.
var ErrInvalidConfig = errors.New("livetail: invalid configuration")

// ErrNoViewers reports a publish with no ClientLink to publish through: the
// node is not running yet, or has stopped.
var ErrNoViewers = errors.New("livetail: no ClientLink is running")

// Links is the HostLink pool, narrowed to what this package calls.
type Links interface {
	Bind(ctx context.Context, target hostlink.Target, req sessionwire.HostLinkBindRequest) error
	Unbind(ctx context.Context, req sessionwire.HostLinkUnbindRequest) error
	DeliverCommand(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, delivery sessionwire.HostLinkCommandDelivery) error
	Subscribe(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, sink hostlink.SessionSink) error
	Unsubscribe(tenant sessionwire.TenantID, session sessionwire.SessionID)
	RouteFor(tenant sessionwire.TenantID, session sessionwire.SessionID) (sessionwire.HostID, bool)
}

// Viewers is the ClientLink side: publish to a session's viewers, or make them
// all repair. clientlink.Handler implements it; the composition supplies it
// late, through a func, because the node is started after composition.
type Viewers interface {
	PublishSession(tenant sessionwire.TenantID, session sessionwire.SessionID, encoded []byte) error
	CloseSession(tenant sessionwire.TenantID, session sessionwire.SessionID)
}

// Relay is routing.Relay, narrowed to what this package calls.
type Relay interface {
	Open(tenant sessionwire.TenantID, session sessionwire.SessionID) error
	Subscribe(tenant sessionwire.TenantID, session sessionwire.SessionID, link routing.LinkID) error
	Receive(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, frame routing.Frame) error
	Pump(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) error
	HostLinkClosed(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) error
	Resync(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) error
	Forget(tenant sessionwire.TenantID, session sessionwire.SessionID)
	Close()
}

// Config composes a Plane.
type Config struct {
	// Links is the HostLink pool. Required.
	Links Links
	// Viewers supplies the running ClientLink, or nil when there is none.
	// Required.
	Viewers func() Viewers
	// MailboxLimit bounds one session's undelivered events. A tail that
	// outruns it is treated as LOST -- repaired with a reset -- rather than
	// buffered without bound. Required, positive.
	MailboxLimit int
	// EventTimeout bounds the relay work one event may cost: a tip read, a
	// rebind and the publishes. Required, positive.
	EventTimeout time.Duration
	// Logger receives what a drainer could not do. Nil takes slog.Default.
	Logger *slog.Logger
}

// Plane is the live-tail composition. See the package documentation.
//
// It implements routing.Binder, routing.Tail, routing.Publisher,
// routing.Hinter and routing.Watcher, and those are the seams the composition
// hands it to; it holds the Relay and the Rebinder it drives, set by Attach.
type Plane struct {
	links   Links
	viewers func() Viewers
	limit   int
	timeout time.Duration
	log     *slog.Logger

	relay    Relay
	rebinder routing.Rebinder

	mu       sync.Mutex
	closed   bool
	nextGen  uint64
	sessions map[sessionKey]*sessionState
	wg       sync.WaitGroup
}

type sessionKey struct {
	tenant  sessionwire.TenantID
	session sessionwire.SessionID
}

// sessionState is one watched session's state. It outlives its watch while its
// drainer is still running, so a session unwatched and watched again reuses
// the one queue and its events stay in order.
type sessionState struct {
	key sessionKey
	// watched is whether a local viewer wants this session.
	watched bool
	// initial is set from Watching to Served: a tail that goes live inside that
	// window started inside the first viewer's own subscribe, and is owed no
	// reset. It is consumed by the first live tail either way.
	initial bool
	// gen names the subscription whose events are accepted. A sink carries the
	// generation it was created with, and anything it reports while gen names
	// another is from a tail that has been stopped, lost or replaced -- and is
	// dropped, which is what keeps a dead tail's late publication out of a live
	// one's stream. Zero accepts nothing.
	gen uint64
	// events is the mailbox; running is whether a drainer owns it.
	events  []event
	running bool
}

type eventKind int

const (
	evLive eventKind = iota + 1
	evFrame
	evLost
	evRestored
	evForget
)

type event struct {
	kind    eventKind
	initial bool
	data    []byte
}

// New validates a composition.
func New(cfg Config) (*Plane, error) {
	switch {
	case cfg.Links == nil:
		return nil, fmt.Errorf("%w: Links is nil", ErrInvalidConfig)
	case cfg.Viewers == nil:
		return nil, fmt.Errorf("%w: Viewers is nil", ErrInvalidConfig)
	case cfg.MailboxLimit < 1:
		return nil, fmt.Errorf("%w: MailboxLimit is %d, want at least 1", ErrInvalidConfig, cfg.MailboxLimit)
	case cfg.EventTimeout <= 0:
		return nil, fmt.Errorf("%w: EventTimeout is %v, want a positive duration", ErrInvalidConfig, cfg.EventTimeout)
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Plane{
		links: cfg.Links, viewers: cfg.Viewers, limit: cfg.MailboxLimit, timeout: cfg.EventTimeout,
		log: log, sessions: map[sessionKey]*sessionState{},
	}, nil
}

// Attach supplies the Relay this plane drives and the Rebinder a Restored link
// asks for. They are late because each is built from this plane: the Relay
// takes it as its Tail and Publisher, and the demand plane takes it as its
// Hinter and Watcher. It must be called before the first viewer.
func (p *Plane) Attach(relay Relay, rebinder routing.Rebinder) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.relay, p.rebinder = relay, rebinder
}

// Close stops accepting events, waits for every drainer to finish or ctx to
// end, and closes the Relay.
func (p *Plane) Close(ctx context.Context) error {
	p.mu.Lock()
	p.closed = true
	relay := p.relay
	p.mu.Unlock()
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	var err error
	select {
	case <-done:
	case <-ctx.Done():
		err = ctx.Err()
	}
	if relay != nil {
		relay.Close()
	}
	return err
}

// ---------------------------------------------------------------------------
// routing.Binder: a bind is followed by a subscribe on the same connection.
// ---------------------------------------------------------------------------

// Bind binds a session's route and then subscribes to its channel, in that
// order and over the same link.
//
// The order is the Host's rule: host v0.2.1 accepts a subscribe only from a
// connection that already holds the bind for that channel. A subscribe that
// fails undoes the bind and fails the whole call, so a route this replica
// holds is always one carrying a live tail -- a route without one would read,
// to every viewer, exactly like an idle session.
func (p *Plane) Bind(ctx context.Context, endpoint sessionwire.InternalEndpoint, req sessionwire.HostLinkBindRequest) error {
	if err := p.links.Bind(ctx, hostlink.Target{Host: req.HostID, Endpoint: endpoint}, req); err != nil {
		return err
	}
	if err := p.links.Subscribe(ctx, req.TenantID, req.SessionID, p.newSink(req.TenantID, req.SessionID)); err != nil {
		_ = p.links.Unbind(ctx, unbindFor(req))
		return fmt.Errorf("livetail: subscribe after bind: %w", err)
	}
	return nil
}

// Unbind stops the session's tail and then gives the route back. Stopping it
// is not optional: a Host never unsubscribes a Factory -- not on unbind, not
// when it invalidates the session's routes -- so a tail left behind would keep
// publishing into a route this replica no longer holds.
func (p *Plane) Unbind(ctx context.Context, req sessionwire.HostLinkUnbindRequest) error {
	p.stopTail(req.TenantID, req.SessionID)
	return p.links.Unbind(ctx, req)
}

// DeliverCommand is the pool's, unchanged.
func (p *Plane) DeliverCommand(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, delivery sessionwire.HostLinkCommandDelivery) error {
	return p.links.DeliverCommand(ctx, tenant, session, delivery)
}

func unbindFor(req sessionwire.HostLinkBindRequest) sessionwire.HostLinkUnbindRequest {
	return sessionwire.HostLinkUnbindRequest{
		Version:        req.Version,
		TenantID:       req.TenantID,
		SessionID:      req.SessionID,
		HostID:         req.HostID,
		HostGeneration: req.HostGeneration,
		LeaseEpoch:     req.LeaseEpoch,
		IdempotencyKey: req.IdempotencyKey,
	}
}

// ---------------------------------------------------------------------------
// routing.Tail: what the Relay's repair stops and resumes.
// ---------------------------------------------------------------------------

// Stop ends the session's tail. Anything its sink reports from now on is
// dropped.
func (p *Plane) Stop(_ context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) error {
	p.stopTail(tenant, session)
	return nil
}

// Resume makes sure a bound session has a tail. The Relay calls it after its
// rebind, and the rebind's own Bind has normally subscribed already, in which
// case this finds the subscription and does nothing. A session the rebind
// could not bind is left alone: its tail starts when a poll or a Restored link
// binds it, and that start sends the reset.
func (p *Plane) Resume(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, _ uint64) error {
	if _, bound := p.links.RouteFor(tenant, session); !bound {
		return nil
	}
	return p.links.Subscribe(ctx, tenant, session, p.newSink(tenant, session))
}

func (p *Plane) stopTail(tenant sessionwire.TenantID, session sessionwire.SessionID) {
	p.mu.Lock()
	if s := p.sessions[sessionKey{tenant: tenant, session: session}]; s != nil {
		s.gen = 0
	}
	p.mu.Unlock()
	p.links.Unsubscribe(tenant, session)
}

// ---------------------------------------------------------------------------
// routing.Publisher and routing.Hinter: the ClientLink side.
// ---------------------------------------------------------------------------

// channelLink is the ONE DeliveryBinding a session has in the Relay. Delivery
// is channel-wide (the ruling's decision): a record is published once to the
// session's ClientLink channel and the transport fans it out, so the Relay's
// per-client queue becomes a per-channel one and its reset is the channel's.
// The link id carries the session, because CloseLink names only a link.
func channelLink(tenant sessionwire.TenantID, session sessionwire.SessionID) routing.LinkID {
	encoded, _ := json.Marshal([2]string{string(tenant), string(session)})
	return routing.LinkID(encoded)
}

func channelOf(link routing.LinkID) (sessionwire.TenantID, sessionwire.SessionID, bool) {
	var pair [2]string
	if err := json.Unmarshal([]byte(link), &pair); err != nil {
		return "", "", false
	}
	return sessionwire.TenantID(pair[0]), sessionwire.SessionID(pair[1]), true
}

// Publish hands one record to every viewer of the session. It never answers
// routing.ErrWouldBlock: the transport's per-connection queue is the bound,
// and a viewer that falls behind it is disconnected and repairs.
func (p *Plane) Publish(_ context.Context, _ routing.LinkID, tenant sessionwire.TenantID, session sessionwire.SessionID, encoded []byte) error {
	viewers := p.viewers()
	if viewers == nil {
		return ErrNoViewers
	}
	return viewers.PublishSession(tenant, session, encoded)
}

// CloseLink makes every viewer of the session repair from the durable journal.
// It is reached when the Relay cannot tell them where to read from.
func (p *Plane) CloseLink(_ context.Context, link routing.LinkID, _ string) error {
	tenant, session, ok := channelOf(link)
	if !ok {
		return fmt.Errorf("livetail: %q names no session channel", link)
	}
	if viewers := p.viewers(); viewers != nil {
		viewers.CloseSession(tenant, session)
	}
	return nil
}

// PublishJournalTip is routing.Hinter: the unbound-session hint, published to
// the same channel.
func (p *Plane) PublishJournalTip(_ context.Context, hint sessionwire.JournalTip) error {
	encoded, err := hint.MarshalJSON()
	if err != nil {
		return err
	}
	viewers := p.viewers()
	if viewers == nil {
		return ErrNoViewers
	}
	return viewers.PublishSession(hint.TenantID, hint.SessionID, encoded)
}

// ---------------------------------------------------------------------------
// routing.Watcher: called under Demand's lock, so these only touch p.mu.
// ---------------------------------------------------------------------------

// Watching opens the window in which a tail going live is the first viewer's.
func (p *Plane) Watching(tenant sessionwire.TenantID, session sessionwire.SessionID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := sessionKey{tenant: tenant, session: session}
	s := p.sessions[key]
	if s == nil {
		s = &sessionState{key: key}
		p.sessions[key] = s
	}
	s.watched, s.initial = true, true
}

// Served closes it.
func (p *Plane) Served(tenant sessionwire.TenantID, session sessionwire.SessionID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if s := p.sessions[sessionKey{tenant: tenant, session: session}]; s != nil {
		s.initial = false
	}
}

// Unwatched forgets the session: its tail's events are dropped from now on,
// and the drainer drops its Relay state after anything already queued.
func (p *Plane) Unwatched(tenant sessionwire.TenantID, session sessionwire.SessionID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.sessions[sessionKey{tenant: tenant, session: session}]
	if s == nil {
		return
	}
	s.watched, s.initial, s.gen = false, false, 0
	p.pushLocked(s, event{kind: evForget})
}

// ---------------------------------------------------------------------------
// The sink: what a subscription reports, on the transport's goroutine.
// ---------------------------------------------------------------------------

type sink struct {
	plane *Plane
	key   sessionKey
	gen   uint64
}

func (p *Plane) newSink(tenant sessionwire.TenantID, session sessionwire.SessionID) *sink {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.nextGen++
	return &sink{plane: p, key: sessionKey{tenant: tenant, session: session}, gen: p.nextGen}
}

// Subscribed makes this subscription the accepted one and queues its start.
func (k *sink) Subscribed() {
	p := k.plane
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.sessions[k.key]
	if s == nil || !s.watched {
		return
	}
	s.gen = k.gen
	initial := s.initial
	s.initial = false
	p.pushLocked(s, event{kind: evLive, initial: initial})
}

// Publication queues one frame of the accepted subscription. A mailbox at its
// limit is not grown: the backlog of frames is dropped and the tail is treated
// as lost, which the repair answers with a reset.
func (k *sink) Publication(data []byte) {
	p := k.plane
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.sessions[k.key]
	if s == nil || !s.watched || s.gen == 0 || s.gen != k.gen {
		return
	}
	if len(s.events) >= p.limit {
		kept := s.events[:0]
		for _, queued := range s.events {
			if queued.kind != evFrame {
				kept = append(kept, queued)
			}
		}
		s.events = kept
		s.gen = 0
		p.pushLocked(s, event{kind: evLost})
		// The subscription itself is withdrawn too, off this goroutine: its
		// frames are being discarded, so it must not stay live under a route
		// that the repair is about to replace.
		go p.links.Unsubscribe(k.key.tenant, k.key.session)
		return
	}
	p.pushLocked(s, event{kind: evFrame, data: data})
}

// Ended queues the repair of a tail that stopped without being asked to.
func (k *sink) Ended() {
	p := k.plane
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.sessions[k.key]
	if s == nil || !s.watched || s.gen == 0 || s.gen != k.gen {
		return
	}
	s.gen = 0
	p.pushLocked(s, event{kind: evLost})
}

// Restored queues a re-bind: the link a tail was lost to is usable again.
func (k *sink) Restored() {
	p := k.plane
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.sessions[k.key]
	if s == nil || !s.watched {
		return
	}
	p.pushLocked(s, event{kind: evRestored})
}

// pushLocked appends an event and starts the session's drainer if none runs.
func (p *Plane) pushLocked(s *sessionState, ev event) {
	if p.closed {
		return
	}
	s.events = append(s.events, ev)
	if s.running {
		return
	}
	s.running = true
	p.wg.Add(1)
	go p.drain(s)
}

// ---------------------------------------------------------------------------
// The drainer: one goroutine per session with events, in order.
// ---------------------------------------------------------------------------

func (p *Plane) drain(s *sessionState) {
	defer p.wg.Done()
	for {
		p.mu.Lock()
		if len(s.events) == 0 || p.closed {
			s.events = nil
			s.running = false
			if !s.watched && p.sessions[s.key] == s {
				delete(p.sessions, s.key)
			}
			p.mu.Unlock()
			return
		}
		ev := s.events[0]
		s.events = s.events[1:]
		relay, rebinder := p.relay, p.rebinder
		p.mu.Unlock()
		if relay == nil {
			continue
		}
		p.handle(relay, rebinder, s.key, ev)
	}
}

func (p *Plane) handle(relay Relay, rebinder routing.Rebinder, key sessionKey, ev event) {
	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()
	tenant, session := key.tenant, key.session

	if ev.kind == evForget {
		relay.Forget(tenant, session)
		return
	}
	if ev.kind == evRestored {
		if _, bound := p.links.RouteFor(tenant, session); bound {
			return
		}
		if rebinder != nil {
			if err := rebinder.Rebind(ctx, tenant, session); err != nil && !errors.Is(err, routing.ErrNoDemand) {
				p.warn(ctx, "re-bind after a restored HostLink failed", key, err)
			}
		}
		return
	}
	// Open and the channel binding are ensured on every event: both are
	// idempotent, and the channel binding is removed whenever the Relay closes
	// it (CloseLink), after which the next record must still have somewhere
	// to go -- the viewers it closed repair, and later ones are live.
	if err := relay.Open(tenant, session); err != nil {
		return
	}
	if err := relay.Subscribe(tenant, session, channelLink(tenant, session)); err != nil {
		return
	}
	switch ev.kind {
	case evLive:
		if !ev.initial {
			if err := relay.Resync(ctx, tenant, session); err != nil {
				p.warn(ctx, "the reset owed to a restarted tail could not be built", key, err)
			}
		}
	case evFrame:
		frame := routing.Frame{
			Encoded: ev.data,
			// A DECLARED VACUOUS FENCE. The Relay fences an enduring record
			// against the Host's committed append sequence, and Core v0.9.1
			// carries no member a Host could state that in; the publication's
			// own covered_through is the best available and, since Core requires
			// covered_through == journal_seq, the fence can never refuse. An
			// additive Core member (a committed_append_seq on the publication
			// or the channel) can tighten it without changing anything else
			// here.
			CommittedAppendSeq: coveredThrough(ev.data),
		}
		if err := relay.Receive(ctx, tenant, session, frame); err != nil {
			// A frame the Relay would not take is a hole in the stream, and a
			// hole is repaired, never skipped.
			p.warn(ctx, "a Host publication was refused; repairing", key, err)
			if err := relay.HostLinkClosed(ctx, tenant, session); err != nil {
				p.warn(ctx, "the repair of a refused publication failed", key, err)
			}
		}
	case evLost:
		if err := relay.HostLinkClosed(ctx, tenant, session); err != nil {
			p.warn(ctx, "the repair of a lost tail did not complete", key, err)
		}
	}
	if err := relay.Pump(ctx, tenant, session); err != nil {
		p.warn(ctx, "delivery to a session's viewers failed", key, err)
	}
}

func (p *Plane) warn(ctx context.Context, msg string, key sessionKey, err error) {
	if errors.Is(err, routing.ErrRelayClosed) {
		return
	}
	p.log.WarnContext(ctx, "livetail: "+msg,
		slog.String("tenant_id", string(key.tenant)), slog.String("session_id", string(key.session)),
		slog.String("error", err.Error()))
}

// coveredThrough reads an enduring publication's covered_through, or zero for
// anything else -- which the Relay then classifies on Core's discriminator.
func coveredThrough(data []byte) uint64 {
	kind, err := sessionwire.SessionRecordTypeOf(data)
	if err != nil || kind != sessionwire.SessionRecordTypeEnduringPublication {
		return 0
	}
	var publication sessionwire.EnduringPublication
	if err := publication.UnmarshalJSON(data); err != nil {
		return 0
	}
	return publication.CoveredThrough
}
