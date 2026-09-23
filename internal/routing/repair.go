package routing

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/delivery"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"
)

// ErrRelayClosed reports work asked of a closed relay.
var ErrRelayClosed = errors.New("routing: relay is closed")

// ErrNoHostBinding reports a session this replica holds no HostBinding for.
var ErrNoHostBinding = errors.New("routing: no host binding for session")

// ErrUnexpectedControl reports a repair control arriving FROM a Host.
//
// A session.reset names a sequence the SENDER knows it forwarded in order, and
// on a DeliveryBinding that sender is this replica. A Host cannot know what
// this Factory delivered to a browser, so a control accepted from below would
// hand a client a coverage claim nobody is in a position to make.
var ErrUnexpectedControl = errors.New("routing: a host may not send a repair control")

// ErrForeignRecord reports a Host publication naming a tenant or session other
// than the one whose live tail it arrived on.
//
// A record carries its own routing envelope, and the tail it arrives on names
// the session a viewer subscribed to. Only a faulty or compromised Host can make
// the two disagree, and forwarding such a record would put another session's --
// or another TENANT's -- output in front of this session's viewers. It wraps
// delivery.ErrMalformed deliberately: the livetail drainer answers every refused
// record with the same repair (the tail is stopped, every viewer is reset from
// one durable tip, and the tail is re-bound), so a foreign record is a hole in
// the stream like any other, never a silent skip. Relay.ForeignRecords counts
// them.
var ErrForeignRecord = fmt.Errorf("routing: a host record names another tenant or session: %w", delivery.ErrMalformed)

// ErrWouldBlock reports a transport that cannot take a record right now.
//
// It is the ONE publish outcome that is not a failure: the record stays queued,
// and the queue -- not the transport -- is what bounds the backlog. Every other
// error means the ClientLink is gone, and the binding is dropped.
var ErrWouldBlock = errors.New("routing: transport cannot take the record now")

// LinkID identifies one ClientLink connection on this replica. It is opaque
// here: a relay never parses it and never derives anything from it.
type LinkID string

// Publisher carries one record to ONE ClientLink, and closes one ClientLink.
//
// Both methods name a link rather than a channel, because every repair in this
// file is per-link: closing a channel would take every session that link is
// watching, which is exactly the blast radius runbook A7.3 steps 3 and 4 forbid.
type Publisher interface {
	// Publish hands one encoded session-channel record to one link. It returns
	// ErrWouldBlock when the transport cannot take it now.
	Publish(ctx context.Context, link LinkID, tenant sessionwire.TenantID, session sessionwire.SessionID, encoded []byte) error
	// CloseLink closes one ClientLink. Peer links continue.
	CloseLink(ctx context.Context, link LinkID, reason string) error
}

// Tail is the Host live tail's control surface for one session.
//
// It is separate from Binder because the two have different lifetimes: a route
// is bound while a subscriber wants the session, and a tail is running only
// while this replica is keeping up with it. A repair stops the tail, repairs,
// and resumes from a sequence -- three things a bind cannot express.
type Tail interface {
	// Stop ends the live tail for one session's binding.
	Stop(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) error
	// Resume restarts it after a sequence, which is the tip the repair captured.
	Resume(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, afterSeq uint64) error
}

// Rebinder re-establishes a session's route WITHOUT changing local demand.
//
// *Demand implements it, and that is the whole reason this seam is spelled this
// way rather than as the routing table itself. See Demand.Rebind: the routing
// table's demand has exactly one holder per session, so a repair plane that
// took a unit of its own would make every rebind hand back the stale route it
// was repairing.
type Rebinder interface {
	Rebind(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) error
}

// RepairLimits bounds the two queues a session's delivery path holds.
//
// The two are SEPARATE NUMBERS and must stay separate. A HostBinding's queue
// holds one session's inbound tail; a DeliveryBinding's queue holds one
// client's outbound copy of it, and a session with thirty subscribers holds
// thirty of the second and one of the first. A single constant would be either
// thirty times too much memory or a HostBinding that overflows before any
// client does -- and, as internal/httpapi's two page ceilings measured, a
// parameter every call site passes identically is untested by construction.
type RepairLimits struct {
	// HostBindingQueue bounds one session's inbound route queue, in records.
	HostBindingQueue int
	// DeliveryBindingQueue bounds one client's outbound queue, in records.
	DeliveryBindingQueue int
}

// DefaultRepairLimits returns bounded defaults.
//
// Neither number is measured and saying so is the point. 512 is wui's own
// DEFAULT_MAX_QUEUED_FRAMES, described there as roughly eight seconds of a fast
// token stream, and the inbound queue is twice it because one inbound record
// fans out to every subscriber and the inbound side must not be the first to
// give up. The RELATION is what is asserted; the numbers are pinned only as
// literals.
func DefaultRepairLimits() RepairLimits {
	return RepairLimits{HostBindingQueue: 1024, DeliveryBindingQueue: 512}
}

// Validate reports why these limits may not bound a relay.
func (l RepairLimits) Validate() error {
	if l.HostBindingQueue < 1 {
		return fmt.Errorf("%w: HostBindingQueue is %d, want at least 1", ErrInvalidConfig, l.HostBindingQueue)
	}
	if l.DeliveryBindingQueue < 1 {
		return fmt.Errorf("%w: DeliveryBindingQueue is %d, want at least 1", ErrInvalidConfig, l.DeliveryBindingQueue)
	}
	return nil
}

// Frame is one record arriving on a session's live tail.
//
// CommittedAppendSeq is the Host's own committed append sequence at the moment
// it sent this frame, and it is an INPUT rather than something derived from the
// record: a record cannot vouch for itself, and the whole content of the upper
// watermark bound is that somebody other than the record says how far the
// journal has been committed.
//
// A DECLARED GAP. Core v0.9.1's session-channel records carry no member a Host
// could state its committed append sequence in. internal/realtime/livetail,
// which feeds this relay from the HostLink subscription since v0.4.0, passes
// the publication's own covered_through -- a vacuous fence, because Core
// requires covered_through == journal_seq -- until an additive Core member can
// tighten it. The honest reading of this field is "what the producer
// declares", and the relay fences against it rather than trusting the frame.
//
// CoalesceKey is FACTORY-LOCAL and read only for an ephemeral record. Core
// gives an ephemeral publication no identity at all, so a path that coalesced
// without a declared key would be guessing that two opaque bodies are
// interchangeable; an empty key means this delta supersedes nothing.
type Frame struct {
	Encoded            []byte
	CommittedAppendSeq uint64
	CoalesceKey        string
}

// Relay is this replica's bounded delivery path: one route queue per
// HostBinding, one outbound queue per DeliveryBinding, and the repair each
// overflow owes.
//
// # The invariant, in one sentence
//
// An enduring record is never discarded silently. Every bound in this file
// either evicts a record that carries no durable content, or reports an
// overflow that is repaired by telling the affected consumer -- and only the
// affected consumer -- where to read from.
//
// # Locking
//
// One mutex for every session, held across queue work and the publishes a pump
// performs -- which makes a fan-out to thirty subscribers one pass rather than
// thirty interleaved ones -- and deliberately NOT across a HostBinding repair's
// tail stop, tip read, rebind or resume, nor Resync's tip read (see repair).
// Those are I/O against a store and a Host, and one session's slow Host held
// under this mutex stalled every other session's delivery (v0.4.0 quality gate
// F3). What is still held under it is a DeliveryBinding overflow's tip read
// (fanOutLocked), which a channel-wide publisher that never answers
// ErrWouldBlock does not reach in composition. Nothing in Demand or Bindings
// names this type, and no repair holds this mutex while calling them.
//
// # PRECONDITION: calls for ONE session are serialised by the caller
//
// Because the mutex is released across a repair's I/O, this type no longer
// orders two calls for the same session against each other, and it relies on
// its caller to. Receive, Pump, HostLinkClosed, Resync and Forget for one
// (tenant, session) must never run concurrently: a second repair entered while
// the first is inside its tip read or rebind clears the first's stopped state
// early, admits a live frame between the two, and publishes a session.reset
// whose tip goes BACKWARDS (the v0.4.0 regate constructed
// E41, R41/77, E78, R41/45). Until d23cb6e the mutex serialised it; since
// then the precondition is load-bearing and is met by composition, not here:
// internal/realtime/livetail calls every one of these methods only from the
// session's single drainer goroutine (Plane.handle) and from Plane.Close after
// the drainers are stopped. Different sessions MAY run concurrently; that is
// the point of releasing the lock. A new caller that is not the plane's
// drainer must serialise per session itself.
type Relay struct {
	tips      TipReader
	rebinder  Rebinder
	tail      Tail
	publisher Publisher
	limits    RepairLimits

	mu     sync.Mutex
	closed bool
	hosts  map[sessionKey]*hostBinding
	// foreign counts records refused with ErrForeignRecord, under mu.
	foreign uint64
}

// hostBinding is one session's inbound route queue and the clients it feeds.
type hostBinding struct {
	queue *delivery.Queue
	// stopped is set while the live tail is stopped and a repair is in flight
	// or has failed. A frame arriving then is discarded rather than queued:
	// the tail it belonged to no longer exists, and queueing it would put
	// records from before the repair in front of records from after it.
	stopped    bool
	deliveries map[LinkID]*deliveryBinding

	// tailSeq is the greatest sequence this session's viewers are known to be
	// able to hold contiguously from the live tail plus the journal: the tip
	// a tail start anchored (Anchor), the tip the last reset sent every
	// binding to (Resync, repair), raised by every enduring record received
	// since. tailKnown says whether there is one. While it is known, a record
	// past tailSeq+1 is PROBED (Receive): the tail skipped positions, and
	// whether a public record is among them is the journal's to say (I3.1
	// D2).
	tailSeq   uint64
	tailKnown bool
}

// deliveryBinding is one ClientLink's outbound queue for one session.
type deliveryBinding struct {
	link  LinkID
	queue *delivery.Queue
	// lastContiguous is the greatest sequence PUBLISHED to this link in an
	// unbroken run. It is what a session.reset names, and it is deliberately
	// not "the greatest sequence queued": everything a repair discards was
	// never applied by the consumer, so a reset built from the queue would tell
	// a client it already had records it never saw.
	//
	// Zero means this binding has vouched for nothing, which Core's SessionReset
	// explicitly allows. Factory does not retain the browser's own durable
	// cursor -- A6.3 step 3 -- so zero is the only honest answer for a client
	// whose first record has not been delivered yet.
	lastContiguous uint64
	forwarded      uint64
}

// NewRelay validates the composition before any call.
func NewRelay(tips TipReader, rebinder Rebinder, tail Tail, publisher Publisher, limits RepairLimits) (*Relay, error) {
	if tips == nil {
		return nil, fmt.Errorf("%w: TipReader must not be nil", ErrInvalidConfig)
	}
	if rebinder == nil {
		return nil, fmt.Errorf("%w: Rebinder must not be nil", ErrInvalidConfig)
	}
	if tail == nil {
		return nil, fmt.Errorf("%w: Tail must not be nil", ErrInvalidConfig)
	}
	if publisher == nil {
		return nil, fmt.Errorf("%w: Publisher must not be nil", ErrInvalidConfig)
	}
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	return &Relay{
		tips: tips, rebinder: rebinder, tail: tail, publisher: publisher,
		limits: limits, hosts: map[sessionKey]*hostBinding{},
	}, nil
}

// Open creates this session's HostBinding and its bounded route queue.
func (r *Relay) Open(tenant sessionwire.TenantID, session sessionwire.SessionID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrRelayClosed
	}
	key := sessionKey{tenant: tenant, session: session}
	if _, exists := r.hosts[key]; exists {
		return nil
	}
	queue, err := delivery.NewQueue(r.limits.HostBindingQueue)
	if err != nil {
		return err
	}
	r.hosts[key] = &hostBinding{queue: queue, deliveries: map[LinkID]*deliveryBinding{}}
	return nil
}

// Subscribe adds one ClientLink's DeliveryBinding to a session.
func (r *Relay) Subscribe(tenant sessionwire.TenantID, session sessionwire.SessionID, link LinkID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrRelayClosed
	}
	host := r.hosts[sessionKey{tenant: tenant, session: session}]
	if host == nil {
		return ErrNoHostBinding
	}
	if _, exists := host.deliveries[link]; exists {
		return nil
	}
	queue, err := delivery.NewQueue(r.limits.DeliveryBindingQueue)
	if err != nil {
		return err
	}
	host.deliveries[link] = &deliveryBinding{link: link, queue: queue}
	return nil
}

// Unsubscribe removes one DeliveryBinding. Its queued records go with it: a
// binding nobody holds has no consumer, and there is nothing durable to lose
// because everything still queued was never applied.
func (r *Relay) Unsubscribe(tenant sessionwire.TenantID, session sessionwire.SessionID, link LinkID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	host := r.hosts[sessionKey{tenant: tenant, session: session}]
	if host == nil {
		return
	}
	if binding := host.deliveries[link]; binding != nil {
		binding.queue.Close()
		delete(host.deliveries, link)
	}
}

// Close refuses later work and drops every queue.
func (r *Relay) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	for key, host := range r.hosts {
		host.queue.Close()
		for _, binding := range host.deliveries {
			binding.queue.Close()
		}
		delete(r.hosts, key)
	}
}

// Receive takes one frame off a session's live tail onto its route queue.
//
// It DISPATCHES on Core's own discriminator rather than on anything the caller
// asserts, so an unsequenced delta cannot be queued as durable data by a caller
// that mislabelled it. Three answers:
//
//   - an enduring publication is parsed -- envelope only, bytes preserved --
//     and fenced against the frame's declared committed append sequence;
//   - an ephemeral publication is queued as droppable, with the caller's
//     declared coalesce key and no sequence check, because it has no sequence;
//   - a repair control is REFUSED. See ErrUnexpectedControl.
//
// A HostBinding overflow is repaired HERE and reported as nil: the overflow is
// handled, and the caller's next frame is the one that matters.
func (r *Relay) Receive(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, frame Frame) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrRelayClosed
	}
	key := sessionKey{tenant: tenant, session: session}
	host := r.hosts[key]
	if host == nil {
		r.mu.Unlock()
		return ErrNoHostBinding
	}
	if host.stopped {
		// The tail this frame belonged to has been stopped for repair. Queueing
		// it would interleave pre-repair records with post-repair ones.
		r.mu.Unlock()
		return nil
	}
	record, err := r.classify(key, frame)
	if err != nil {
		if errors.Is(err, ErrForeignRecord) {
			r.foreign++
		}
		r.mu.Unlock()
		return err
	}
	if record.Class == delivery.ClassEnduring {
		if host.tailKnown && record.Seq > host.tailSeq+1 {
			// The lock is released for the probe, as for every store read
			// here; this session's calls are serialised by its caller.
			last := host.tailSeq
			r.mu.Unlock()
			hole, tip, probeErr := r.probeGap(ctx, key, last, record.Seq)
			r.mu.Lock()
			if r.closed {
				r.mu.Unlock()
				return ErrRelayClosed
			}
			if r.hosts[key] != host || host.stopped {
				// Forgotten, or a repair began meanwhile: its reset covers this.
				r.mu.Unlock()
				return nil
			}
			if hole {
				r.resetForGapLocked(ctx, key, host, record.Seq, tip, probeErr)
			}
		}
		if !host.tailKnown || record.Seq > host.tailSeq {
			host.tailSeq = record.Seq
		}
	}
	switch err := host.queue.Enqueue(record); {
	case err == nil, errors.Is(err, delivery.ErrDropped):
		r.mu.Unlock()
		return nil
	case errors.Is(err, delivery.ErrOverflow):
		host.stopped = true
		r.mu.Unlock()
		return r.repair(ctx, key, host)
	default:
		r.mu.Unlock()
		return err
	}
}

// classify turns one frame into the queue record it is, or reports why it is
// not one. A publication whose own tenant or session is not key's is refused
// with ErrForeignRecord.
func (r *Relay) classify(key sessionKey, frame Frame) (delivery.Record, error) {
	recordType, err := sessionwire.SessionRecordTypeOf(frame.Encoded)
	if err != nil {
		return delivery.Record{}, fmt.Errorf("%w: %w", delivery.ErrMalformed, err)
	}
	switch recordType {
	case sessionwire.SessionRecordTypeEnduringPublication:
		parsed, err := delivery.ParseEnduring(frame.Encoded, frame.CommittedAppendSeq)
		if err != nil {
			return delivery.Record{}, err
		}
		if err := key.owns(parsed.TenantID, parsed.SessionID); err != nil {
			return delivery.Record{}, err
		}
		return parsed.Record(), nil
	case sessionwire.SessionRecordTypeEphemeralPublication:
		var publication sessionwire.EphemeralPublication
		if err := publication.UnmarshalJSON(frame.Encoded); err != nil {
			return delivery.Record{}, fmt.Errorf("%w: %w", delivery.ErrMalformed, err)
		}
		if err := key.owns(publication.TenantID, publication.SessionID); err != nil {
			return delivery.Record{}, err
		}
		return delivery.Record{
			Class:       delivery.ClassEphemeral,
			Encoded:     frame.Encoded,
			CoalesceKey: frame.CoalesceKey,
		}, nil
	default:
		return delivery.Record{}, fmt.Errorf("%w: %s", ErrUnexpectedControl, recordType)
	}
}

// owns reports ErrForeignRecord unless a record's own routing envelope names
// this key's tenant and session.
func (k sessionKey) owns(tenant sessionwire.TenantID, session sessionwire.SessionID) error {
	if tenant == k.tenant && session == k.session {
		return nil
	}
	return fmt.Errorf("%w: the tail is %s/%s, the record names %s/%s",
		ErrForeignRecord, k.tenant, k.session, tenant, session)
}

// ForeignRecords reports how many Host publications this relay has refused
// with ErrForeignRecord since it was built. It never decreases. Any nonzero
// value means a Host sent a record for a session other than the tail it was
// sent on, which a correct Host never does.
func (r *Relay) ForeignRecords() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.foreign
}

// Pump moves one session's queued route records out to its DeliveryBindings and
// on to the transport.
//
// It is a method rather than a goroutine for ReapIdle's reason: A9.1 owns the
// lifetime that would give a loop a start and a stop. What it buys a test is
// that the backlog is deterministic -- a queue fills because nothing pumped it,
// not because a scheduler was slow.
func (r *Relay) Pump(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrRelayClosed
	}
	key := sessionKey{tenant: tenant, session: session}
	host := r.hosts[key]
	if host == nil {
		return ErrNoHostBinding
	}

	var failures []error
	for {
		record, ok := host.queue.Next()
		if !ok {
			break
		}
		// Every binding is attempted and every failure joined rather than
		// returning at the first, for Bindings.Close's reason one layer down:
		// one client's repair must not strand the fan-out to its peers.
		for _, binding := range sortedBindings(host) {
			if err := r.fanOutLocked(ctx, key, host, binding, record); err != nil {
				failures = append(failures, err)
			}
		}
	}
	for _, binding := range sortedBindings(host) {
		if err := r.drainLocked(ctx, key, host, binding); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

// fanOutLocked puts one route record on one client's queue, repairing ONLY that
// client if its queue overflows.
func (r *Relay) fanOutLocked(ctx context.Context, key sessionKey, host *hostBinding, binding *deliveryBinding, record delivery.Record) error {
	switch err := binding.queue.Enqueue(record); {
	case err == nil, errors.Is(err, delivery.ErrDropped):
		return nil
	case errors.Is(err, delivery.ErrOverflow):
		tip, err := r.readTipLocked(ctx, key)
		if err != nil {
			// Fail closed. Without a tip there is no coherent reset to send,
			// and continuing to stream past a gap is the silent durable loss
			// this whole file exists to prevent.
			r.closeBindingLocked(ctx, host, binding, "repair tip unavailable")
			return err
		}
		return r.resetBindingLocked(ctx, key, host, binding, tip)
	default:
		return err
	}
}

// drainLocked hands one client's queued records to the transport, stopping at
// the first the transport cannot take.
//
// The record is left QUEUED in that case, which is what makes the queue the
// bound rather than the transport: a drain that dropped what the transport
// refused would turn every busy moment into durable loss.
func (r *Relay) drainLocked(ctx context.Context, key sessionKey, host *hostBinding, binding *deliveryBinding) error {
	for {
		record, ok := binding.queue.Head()
		if !ok {
			return nil
		}
		err := r.publisher.Publish(ctx, binding.link, key.tenant, key.session, record.Encoded)
		if errors.Is(err, ErrWouldBlock) {
			return nil
		}
		if err != nil {
			// Any other failure means this ClientLink is gone. Only this one.
			r.closeBindingLocked(ctx, host, binding, "publish failed")
			return err
		}
		binding.queue.Next()
		binding.advance(record)
	}
}

// advance records what this binding has now vouched for.
//
// "Contiguous" is enforced rather than assumed: the first published enduring
// record starts the run wherever it is, and a later one advances it only if it
// is the immediate successor. A gap therefore STICKS -- the run stays at the
// last sequence before it -- which is the answer a reset must carry, because a
// client told it has everything through a sequence it does not have will never
// read the hole.
func (b *deliveryBinding) advance(record delivery.Record) {
	if record.Class != delivery.ClassEnduring {
		return
	}
	if b.forwarded == 0 || record.Seq == b.lastContiguous+1 {
		b.lastContiguous = record.Seq
	}
	b.forwarded++
}

// HostLinkClosed repairs a session whose physical HostLink went away.
//
// It is the same repair a route-queue overflow takes, and that is deliberate:
// both mean the live tail stopped being a complete record of the session, and a
// second repair path would be a second place for the reset to be built
// differently.
func (r *Relay) HostLinkClosed(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrRelayClosed
	}
	key := sessionKey{tenant: tenant, session: session}
	host := r.hosts[key]
	if host == nil {
		r.mu.Unlock()
		return ErrNoHostBinding
	}
	host.stopped = true
	r.mu.Unlock()
	return r.repair(ctx, key, host)
}

// Resync tells every DeliveryBinding of a session where to read from, because
// the live tail feeding it has just (re)started and whatever the Host
// published while it was not running is gone.
//
// It is the half of a repair that does not touch the tail: no Stop, no rebind,
// no Resume. Gap 3 needs it because a tail can START over a hole -- a viewer
// watched a session this replica could not yet bind, a poll then bound it, and
// every record committed in between reached nobody -- and a Host keeps no
// history, so the only honest answer is a session.reset naming the one tip read
// here. It is built exactly as HostLinkClosed's is (resetBindingLocked, one tip
// for every binding), so the two cannot describe a gap differently.
//
// The caller must call it AFTER the new tail is live: a reset sent before
// would have its consumer re-read the journal while records were still going
// nowhere.
func (r *Relay) Resync(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrRelayClosed
	}
	key := sessionKey{tenant: tenant, session: session}
	if r.hosts[key] == nil {
		return ErrNoHostBinding
	}
	// The tip is read with the lock RELEASED, as repair reads it: a store
	// call held under the relay's one mutex would stall every other session's
	// delivery for its duration.
	r.mu.Unlock()
	tip, err := r.readTip(ctx, key)
	r.mu.Lock()
	if r.closed {
		return ErrRelayClosed
	}
	host := r.hosts[key]
	if host == nil {
		return ErrNoHostBinding
	}
	if err != nil {
		// Fail closed, as repair does: a reset naming no tip is not a
		// repair instruction, and streaming on past a gap nobody was told about
		// is the defect.
		for _, binding := range sortedBindings(host) {
			r.closeBindingLocked(ctx, host, binding, "resync tip unavailable")
		}
		host.queue.Clear()
		return err
	}
	var failures []error
	for _, binding := range sortedBindings(host) {
		if err := r.resetBindingLocked(ctx, key, host, binding, tip); err != nil {
			failures = append(failures, err)
		}
	}
	host.queue.Clear()
	host.anchorLocked(tip)
	return errors.Join(failures...)
}

// Forget drops a session's HostBinding and every queue it holds.
//
// It is Open's inverse, for a session this replica has stopped watching; Close
// is the same thing for every session at once. Nothing queued is lost that
// anybody could read: a binding nobody holds has no consumer.
func (r *Relay) Forget(tenant sessionwire.TenantID, session sessionwire.SessionID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := sessionKey{tenant: tenant, session: session}
	host := r.hosts[key]
	if host == nil {
		return
	}
	host.queue.Close()
	for _, binding := range host.deliveries {
		binding.queue.Close()
	}
	delete(r.hosts, key)
}

// repair is runbook A7.3 step 4, in its stated order.
//
// Stop the live tail; capture ONE durable tip; reset every affected
// DeliveryBinding independently from that one tip; rebind; resume after the
// tip. Unrelated HostBindings are not touched, which is a property of this
// function taking one and being the only thing that runs.
//
// ONE tip for every binding is the load-bearing word. Two reads could capture
// two different tips, so two clients of one session would be told two different
// places the journal had reached -- and a client whose tip was the older one
// would read a range that a peer was already past. The reset each binding gets
// still differs, because LastContiguous is that binding's own.
//
// # The relay's lock is NOT held across the tail, the tip read or the rebind
//
// The caller sets host.stopped under r.mu and releases it before calling this.
// The stop, the tip read, the rebind and the resume are I/O -- an unsubscribe,
// a store read, a bind RPC and a subscribe that waits for the Host -- and this
// relay has ONE mutex for every session, so holding it across them made one
// session's repair against a slow Host stall every other session's delivery on
// the replica (v0.4.0 quality gate F3: 2.9s for an unrelated session behind a
// 3s bind). The lock is taken only to apply the resets and to clear stopped.
// While it is released, host.stopped keeps this session's own frames out, and
// a Forget or Close in the meantime is noticed on re-lock and ends the repair.
//
// # A tip that cannot be read still rebinds
//
// Every affected ClientLink is closed -- a reset naming no tip is not a repair
// instruction -- and the repair then CONTINUES: rebind, resume, and the tail
// is started again. It used to stop there and leave the tail stopped, and that
// was a session silent for as long as it was watched (v0.4.0 quality gate F1):
// the route stayed held, so nothing a poll or a restored link does would ever
// re-bind it, and a stopped HostBinding discarded every frame after. Resuming
// is safe because nobody is left streaming past the gap: every binding that
// had been told anything was closed, and a binding subscribing afterwards has
// vouched for nothing. The resumed tail is resumed after sequence zero.
func (r *Relay) repair(ctx context.Context, key sessionKey, host *hostBinding) error {
	var failures []error
	if err := r.tail.Stop(ctx, key.tenant, key.session); err != nil {
		failures = append(failures, err)
	}
	tip, tipErr := r.readTip(ctx, key)

	r.mu.Lock()
	if r.closed || r.hosts[key] != host {
		// Forgotten or closed mid-repair: there is nobody left to repair.
		r.mu.Unlock()
		return errors.Join(failures...)
	}
	if tipErr != nil {
		for _, binding := range sortedBindings(host) {
			r.closeBindingLocked(ctx, host, binding, "repair tip unavailable")
		}
		failures = append(failures, tipErr)
		tip = 0
	} else {
		for _, binding := range sortedBindings(host) {
			if err := r.resetBindingLocked(ctx, key, host, binding, tip); err != nil {
				failures = append(failures, err)
			}
		}
		host.anchorLocked(tip)
	}
	host.queue.Clear()
	r.mu.Unlock()

	if err := r.rebinder.Rebind(ctx, key.tenant, key.session); err != nil {
		failures = append(failures, err)
	}
	if err := r.tail.Resume(ctx, key.tenant, key.session, tip); err != nil {
		failures = append(failures, err)
	}
	r.mu.Lock()
	if r.hosts[key] == host {
		host.stopped = false
	}
	r.mu.Unlock()
	return errors.Join(failures...)
}

// resetBindingLocked is runbook A7.3 step 3: record the last contiguous
// sequence, clear ONLY this queue, and queue a repeatable session.reset -- or
// close ONLY this ClientLink if the reset cannot be queued.
//
// The reset is built from Core's own type and marshalled through it, so
// "repeatable" is a property of the ENCODING rather than of this function: the
// record carries two absolute sequences and no nonce, no attempt count and no
// sequence of its own, which is what makes two resets for one (last, tip) the
// same bytes. Core's Validate is also what refuses an incoherent pair -- a last
// above the tip -- and that refusal is answered by closing this link, because a
// consumer that cannot be told where to repair from cannot be left streaming.
//
// THE RUNBOOK SAYS "if reset cannot QUEUE"; THIS CODE MEANS "cannot be BUILT",
// and the difference is stated rather than left to be found. There are two
// failures here and only the first is reachable at this composition:
//
//   - MarshalJSON refuses the pair. Reachable, and it is the arm the clause is
//     satisfied by: a captured tip behind what this binding was already sent is
//     an ordinary consequence of a lagging store read.
//     TestOnlyThatClientLinkIsClosedWhenTheResetCannotBeBuilt drives it.
//   - Queue.Repair refuses. UNREACHABLE today, because a binding reachable from
//     host.deliveries always has an open queue: closeBindingLocked closes the
//     queue and deletes the binding in one breath, so there is no state in which
//     a live binding holds a closed queue. The branch is kept rather than
//     deleted because Repair returns an error and ignoring it would be a silent
//     drop of the one control that matters; its BEHAVIOUR is read at the queue
//     (TestAClosedQueueRefusesEveryKindOfWork), and what has no reader is this
//     wiring of it. A composition that closed a queue without removing its
//     binding -- or a queue that grew a second refusal -- would make it live.
func (r *Relay) resetBindingLocked(ctx context.Context, key sessionKey, host *hostBinding, binding *deliveryBinding, tip uint64) error {
	reset := sessionwire.SessionReset{
		TenantID:       key.tenant,
		SessionID:      key.session,
		LastContiguous: binding.lastContiguous,
		JournalTip:     tip,
	}
	encoded, err := reset.MarshalJSON()
	if err != nil {
		r.closeBindingLocked(ctx, host, binding, "repair control is incoherent")
		return err
	}
	control := delivery.Record{Class: delivery.ClassControl, Encoded: encoded}
	if err := binding.queue.Repair(control); err != nil {
		r.closeBindingLocked(ctx, host, binding, "repair control could not be queued")
		return err
	}
	return nil
}

// Anchor records the durable tip a session's viewers are known to reach, as
// the point the live tail must continue: the caller calls it when a tail has
// gone live INSIDE the first viewer's subscribe (the start Gap 3 owes no
// reset), whose durable read comes after the tail was live and so reaches at
// least this tip. From then on a record past the tail's last position is
// probed (Receive) rather than assumed to continue it (I3.1 D2).
//
// The tip is read with the lock RELEASED, as Resync reads it. A tip that
// cannot be read anchors nothing and is returned; the session keeps the
// original rule, which is the state before this method existed.
func (r *Relay) Anchor(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) error {
	key := sessionKey{tenant: tenant, session: session}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrRelayClosed
	}
	if r.hosts[key] == nil {
		r.mu.Unlock()
		return ErrNoHostBinding
	}
	r.mu.Unlock()
	tip, err := r.readTip(ctx, key)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	host := r.hosts[key]
	if r.closed || host == nil {
		return ErrRelayClosed
	}
	host.anchorLocked(tip)
	return nil
}

// anchorLocked makes tip the tail's known position.
func (h *hostBinding) anchorLocked(tip uint64) {
	h.tailSeq, h.tailKnown = tip, true
}

// probeGap asks the journal whether any PUBLIC record lies strictly between
// the tail's last position and seq, the record that skipped past it.
//
// A Host publishes only public records, so a sparse tail is ordinary: a
// private record (a runtime control frame, a disposition) occupies a position
// and is never relayed. What is NOT ordinary is a public record the tail never
// carried -- a Host that committed it after it stopped relaying, as a warm
// release commits SessionResidencyReleased, before a re-placed session's new
// tail arrived on the same subscription. The probe is one bounded page:
// FromSeq just past the tail, one event, and a scan no longer than the gap. A
// hole is an event below seq, or a page that could not cover the gap (a scan
// or byte bound stopped it) -- unknown is answered as a hole, because a reset
// the viewer did not need costs a journal read and a skipped record costs the
// record. The page's captured tip is returned for the reset.
func (r *Relay) probeGap(ctx context.Context, key sessionKey, last, seq uint64) (hole bool, tip uint64, err error) {
	scan := seq - last - 1
	if scan > maxGapProbe {
		scan = maxGapProbe
	}
	page, err := r.tips.ReadPublicJournal(ctx, sessionstore.ReadPublicJournalRequest{
		TenantID: key.tenant, SessionID: key.session, FromSeq: last + 1, Limit: 1, ScanLimit: int(scan),
	})
	if err != nil {
		return true, 0, err
	}
	if len(page.Events) > 0 {
		return page.Events[0].JournalSeq < seq, page.CapturedTip, nil
	}
	return page.CoveredThrough+1 < seq, page.CapturedTip, nil
}

// maxGapProbe bounds the records one gap probe examines. It is
// storage.MaxOrderedPageLimit, SessionStore's ceiling for ScanLimit; a gap
// wider than it is answered as a hole.
const maxGapProbe = storage.MaxOrderedPageLimit

// resetForGapLocked tells every binding of a session where to read from,
// because the live tail skipped a public record: each is reset to tip, the
// probe's captured tip, which must reach the skipping record's predecessor. A
// tip that does not -- or a probe that failed and whose follow-up tip read
// fails too -- cannot describe the hole, so every binding is closed instead:
// the fail-closed arm every other repair here takes. The skipping record is
// then queued as usual, behind the reset.
func (r *Relay) resetForGapLocked(ctx context.Context, key sessionKey, host *hostBinding, seq, tip uint64, probeErr error) {
	if probeErr != nil {
		read, err := r.readTipLocked(ctx, key)
		if err != nil {
			tip = 0
		} else {
			tip = read
		}
	}
	if tip+1 < seq {
		for _, binding := range sortedBindings(host) {
			r.closeBindingLocked(ctx, host, binding, "a gap in the tail cannot be repaired from the journal tip")
		}
		host.tailKnown = false
		return
	}
	for _, binding := range sortedBindings(host) {
		_ = r.resetBindingLocked(ctx, key, host, binding, tip)
	}
	host.anchorLocked(tip)
}

// closeBindingLocked closes ONE ClientLink and removes its binding. Peer
// bindings on the same session, and every binding on every other session, are
// untouched -- which is the whole of "peer clients continue".
func (r *Relay) closeBindingLocked(ctx context.Context, host *hostBinding, binding *deliveryBinding, reason string) {
	binding.queue.Close()
	delete(host.deliveries, binding.link)
	// Best effort, and its error is deliberately not reported: the local
	// binding is gone either way, for the reason Bindings.Release gives about
	// an unbind, and a caller has nothing to do with the answer.
	_ = r.publisher.CloseLink(ctx, binding.link, reason)
}

// readTipLocked captures one durable journal tip.
//
// It asks for the tip and nothing else, through the same bounded request
// Demand's hint poll uses, so the two cannot ask the store for different things
// when they want the same fact.
func (r *Relay) readTipLocked(ctx context.Context, key sessionKey) (uint64, error) {
	return r.readTip(ctx, key)
}

// readTip is readTipLocked for a caller that has released the lock. It touches
// no relay state; the name records only where it may be called from.
func (r *Relay) readTip(ctx context.Context, key sessionKey) (uint64, error) {
	page, err := r.tips.ReadPublicJournal(ctx, tipRequest(key))
	if err != nil {
		return 0, err
	}
	return page.CapturedTip, nil
}

// LastContiguous reports what one DeliveryBinding has vouched for.
func (r *Relay) LastContiguous(tenant sessionwire.TenantID, session sessionwire.SessionID, link LinkID) (uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	host := r.hosts[sessionKey{tenant: tenant, session: session}]
	if host == nil {
		return 0, false
	}
	binding := host.deliveries[link]
	if binding == nil {
		return 0, false
	}
	return binding.lastContiguous, true
}

// Bindings reports how many DeliveryBindings a session holds.
func (r *Relay) Bindings(tenant sessionwire.TenantID, session sessionwire.SessionID) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	host := r.hosts[sessionKey{tenant: tenant, session: session}]
	if host == nil {
		return 0
	}
	return len(host.deliveries)
}

// Queued reports how many records one DeliveryBinding is holding.
func (r *Relay) Queued(tenant sessionwire.TenantID, session sessionwire.SessionID, link LinkID) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	host := r.hosts[sessionKey{tenant: tenant, session: session}]
	if host == nil {
		return 0
	}
	binding := host.deliveries[link]
	if binding == nil {
		return 0
	}
	return binding.queue.Len()
}

// sortedBindings returns a session's DeliveryBindings in a stable order.
//
// A map range would make the order of a fan-out, and therefore which peer a
// failure is attributed to, a property of the runtime's hash seed. Sorting by
// the link identity costs nothing at this size and makes every case in
// repair_test.go a function of the fixture rather than of the run.
func sortedBindings(host *hostBinding) []*deliveryBinding {
	links := make([]LinkID, 0, len(host.deliveries))
	for link := range host.deliveries {
		links = append(links, link)
	}
	slices.Sort(links)
	bindings := make([]*deliveryBinding, 0, len(links))
	for _, link := range links {
		bindings = append(bindings, host.deliveries[link])
	}
	return bindings
}
