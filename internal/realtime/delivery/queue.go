// Package delivery holds Factory's bounded record queues, ABOVE the transport.
//
// Nothing here names a transport type and nothing here imports centrifuge. A
// queue is the place a backlog forms when a consumer is slower than a producer,
// and the whole reason this package exists separately from the transport is
// that the transport's own outbound buffer cannot be that place: a relay that
// awaits its source and immediately hands one record to the transport is never
// the slow stage, so a bound applied there measures nothing. wui measured
// exactly this and recorded it
// (wui/packages/protocol/test/store-backpressure.test.ts, re-opened and read)
// before moving its bound inside the consumer.
//
// # The one inversion this package exists for
//
// An ENDURING record is never the victim of a bound. wui used to discard the
// oldest enduring frame and report the discard, and that is silent durable loss
// with a receipt attached: nothing re-delivers the frame, and the reconnect
// cursor walks past it the moment a later frame is applied, so the transcript
// has a hole AND a persisted cursor asserting the hole was covered. U2.2
// inverted it there; this is the same inversion on Factory's side of the same
// stream. A full queue holding nothing droppable does not choose a durable
// victim -- it reports ErrOverflow, and the caller repairs.
package delivery

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// ErrInvalidConfig is the class of every NewQueue rejection.
var ErrInvalidConfig = errors.New("delivery: invalid configuration")

// ErrQueueClosed reports work asked of a closed queue.
var ErrQueueClosed = errors.New("delivery: queue is closed")

// ErrOverflow reports a full queue holding nothing it may discard.
//
// It is a REPAIR SIGNAL, not a drop receipt. The queue is unchanged when it is
// returned and the arriving record was not taken, so a caller that ignored it
// would lose the arrival -- which is why internal/routing treats it as the
// trigger for a reset rather than as a counter to increment.
var ErrOverflow = errors.New("delivery: queue overflowed with nothing droppable")

// ErrDropped reports an arriving ephemeral record discarded because a full
// queue held nothing cheaper.
//
// It is deliberately NOT in the ErrOverflow class, and the two must not be
// collapsed. An ephemeral record is best effort by construction -- Core's own
// EphemeralPublication doc says a bounded delivery path may coalesce or drop
// one -- so a token stream arriving at a queue full of committed events has
// lost nothing durable, and answering it with ErrOverflow would make every
// busy session repair a queue that is intact.
var ErrDropped = errors.New("delivery: ephemeral record was dropped")

// ErrNotEnduring reports a record that is not an enduring publication.
var ErrNotEnduring = errors.New("delivery: record is not an enduring publication")

// ErrMalformed reports a record Core would not decode for a reason that is not
// the watermark. It is separate so that "the Host sent something broken" stays
// distinguishable from "the Host made a coverage claim it may not make";
// collapsing them would report every mistyped identifier as a watermark
// violation, which is the one failure a caller acts on differently.
var ErrMalformed = errors.New("delivery: record could not be decoded")

// ErrWatermarkNotItsEventSequence reports a coverage watermark that is not its
// own event's sequence.
//
// IT IS NAMED FOR WHAT IT REFUSES, which is wider than the runbook's clause and
// deliberately so. The rule is CORE's, not this package's:
// EnduringPublication.Validate requires covered_through to EQUAL journal_seq,
// because a live publication covers exactly the sequence it was committed at and
// later private records can only be covered by an authenticated journal page. So
// Core refuses INEQUALITY -- a watermark above its event as well as one below it
// -- and an earlier name here said "below", which misreported half the cases it
// fires on. This sentinel classifies Core's refusal for a caller; it does not
// restate the comparison, so there is no second authority for it here, and this
// package's acceptance is exactly Core's and never wider.
var ErrWatermarkNotItsEventSequence = errors.New("delivery: coverage watermark is not its event sequence")

// ErrWatermarkAboveCommitted reports coverage above the Host's committed append
// sequence.
//
// This bound IS this package's, and it fences a different quantity from the one
// above: the record's own sequence against what the Host says it has durably
// appended. A record claiming a sequence the Host has not committed is a claim
// this replica cannot authenticate and must not forward, because a client that
// took it would advance its cursor past a range no durable read will ever
// return.
var ErrWatermarkAboveCommitted = errors.New("delivery: coverage watermark is above the host's committed append sequence")

// Class partitions session-channel records by what a bounded path may do to
// one. It is a FACTORY-LOCAL partition and never travels: Core's own
// discriminator is the wire authority, and Class is the consequence drawn from
// it.
type Class int

const (
	// ClassEnduring is a committed public journal event. Never discarded.
	ClassEnduring Class = iota + 1
	// ClassEphemeral is a best-effort unsequenced delta. Coalesced or dropped.
	ClassEphemeral
	// ClassControl is a repair control -- a session.reset or a journal_tip.
	// Never discarded: discarding one leaves a consumer that will never be
	// told to repair, which is the same silent loss with an extra step.
	ClassControl
)

func (c Class) droppable() bool { return c == ClassEphemeral }

// Record is one queued session-channel record.
//
// Encoded is the record EXACTLY as it is to be published, and the queue never
// looks inside it. Seq and CoalesceKey are the two facts the bound needs, and
// each belongs to one class only: a sequence orders enduring data, and a
// coalesce key names which ephemeral deltas supersede one another.
type Record struct {
	// Class decides what the bound may do to this record.
	Class Class
	// Encoded is the published bytes, forwarded unchanged.
	Encoded []byte
	// Seq is the enduring record's journal sequence; zero otherwise.
	Seq uint64
	// CoalesceKey groups ephemeral deltas that supersede one another. It is
	// FACTORY-LOCAL and never on the wire: Core gives an ephemeral record no
	// identity at all, so a path that coalesced without a declared key would be
	// guessing that two opaque bodies are interchangeable.
	CoalesceKey string
}

// Queue is one bounded record queue above the transport.
//
// It is safe for concurrent use. The mutex is held only across slice work --
// there is no I/O inside it -- so this is not the trade internal/routing makes
// at its own lock.
type Queue struct {
	capacity int

	mu     sync.Mutex
	closed bool
	buf    []Record
}

// NewQueue returns a queue bounded at capacity records.
func NewQueue(capacity int) (*Queue, error) {
	if capacity < 1 {
		return nil, fmt.Errorf("%w: capacity is %d, want at least 1", ErrInvalidConfig, capacity)
	}
	return &Queue{capacity: capacity}, nil
}

// Capacity reports the bound this queue was composed with.
func (q *Queue) Capacity() int { return q.capacity }

// Len reports how many records are queued.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.buf)
}

// Enqueue adds one record, evicting a droppable one if the queue is full.
//
// THREE outcomes and not two, which is the whole of the policy:
//
//   - nil, the record was taken (possibly after evicting an ephemeral one);
//   - ErrDropped, the ARRIVAL was an ephemeral record a full durable queue had
//     no room for. Nothing durable was lost and no repair is owed;
//   - ErrOverflow, the queue is full of records none of which may be discarded
//     and the arrival is one of them. The queue is UNCHANGED and the caller
//     owes a repair.
func (q *Queue) Enqueue(record Record) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrQueueClosed
	}
	if len(q.buf) < q.capacity {
		q.buf = append(q.buf, record)
		return nil
	}
	victim := selectVictim(q.buf)
	if victim < 0 {
		if record.Class.droppable() {
			return ErrDropped
		}
		return ErrOverflow
	}
	q.buf = append(q.buf[:victim:victim], q.buf[victim+1:]...)
	q.buf = append(q.buf, record)
	return nil
}

// Head returns the oldest queued record WITHOUT removing it.
//
// It exists because a transport that cannot take a record right now must leave
// it queued: a drain that popped first and then discovered the transport was
// full would have to put the record back, and "put it back at the front" is a
// second ordering rule that can disagree with this one.
func (q *Queue) Head() (Record, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || len(q.buf) == 0 {
		return Record{}, false
	}
	return q.buf[0], true
}

// Clear discards the buffer unapplied and queues nothing in its place.
//
// It is Repair without a control record, for the queue whose consumer is not a
// client: a HostBinding's route queue has no one to send a session.reset to,
// because the reset is owed to the CLIENTS downstream of it and is minted per
// DeliveryBinding from that binding's own delivered sequence.
func (q *Queue) Clear() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.buf = nil
}

// Next removes and returns the oldest queued record. Arrival order is preserved
// across every class: an ephemeral delta that arrived between two committed
// events is delivered between them, because reordering the classes would show a
// client a token stream that ran ahead of the events it belongs to.
func (q *Queue) Next() (Record, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || len(q.buf) == 0 {
		return Record{}, false
	}
	record := q.buf[0]
	q.buf = q.buf[1:]
	return record, true
}

// Repair discards the buffer UNAPPLIED and queues one control record in its
// place.
//
// "Unapplied" is the load-bearing word. Everything discarded here is a record
// the consumer never saw, so the control that replaces it must name a sequence
// from what was DELIVERED and not from what was queued -- that is the caller's
// to supply, and internal/routing is where it is asserted.
//
// A second Repair SUPERSEDES a pending control rather than queueing beside it,
// and that falls out of clearing the whole buffer rather than being a second
// rule: two resets in a row are one instruction restated, not two instructions,
// which is exactly what makes the control repeatable.
func (q *Queue) Repair(control Record) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrQueueClosed
	}
	q.buf = append(q.buf[:0:0], control)
	return nil
}

// Close refuses later work and hands out nothing further. A closed queue's
// consumer is gone, so a record still buffered in it has no destination.
func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.buf = nil
}

// selectVictim reports the index of the record a full queue may discard, or -1
// when it holds nothing droppable.
//
// It is wui's selectFrameToDrop ported, including the ordering of its two
// preferences and the -1 that is the inversion:
//
//  1. COALESCE. Among the ephemeral records carrying a declared key, the
//     busiest key loses its OLDEST member. Preferring the busiest bucket is
//     what keeps a quiet key's only delta alive while a chatty one is thinned;
//     preferring the oldest within it is what makes the newest state survive,
//     which is what a coalescible delta is for.
//  2. DROP. With no keyed ephemeral record present, the oldest ephemeral record
//     is the victim.
//  3. Otherwise there is NO victim. An enduring or control record is never
//     chosen, whatever else the buffer holds.
//
// Ties between two equally busy keys are broken by the AGE of their oldest
// member, so the answer is a function of the buffer and not of map iteration
// order.
func selectVictim(buf []Record) int {
	oldestOfKey := map[string]int{}
	countOfKey := map[string]int{}
	oldestEphemeral := -1
	for i, record := range buf {
		if !record.Class.droppable() {
			continue
		}
		if oldestEphemeral < 0 {
			oldestEphemeral = i
		}
		if record.CoalesceKey == "" {
			continue
		}
		countOfKey[record.CoalesceKey]++
		if _, seen := oldestOfKey[record.CoalesceKey]; !seen {
			oldestOfKey[record.CoalesceKey] = i
		}
	}
	victim := -1
	best := 0
	for key, count := range countOfKey {
		oldest := oldestOfKey[key]
		if count > best || (count == best && oldest < victim) {
			victim, best = oldest, count
		}
	}
	if victim >= 0 {
		return victim
	}
	return oldestEphemeral
}

// Enduring is one parsed enduring publication: its routing and sequence
// envelope, the canonical body bytes, and the record exactly as received.
type Enduring struct {
	TenantID       sessionwire.TenantID
	SessionID      sessionwire.SessionID
	EventID        sessionwire.EventID
	JournalSeq     uint64
	CoveredThrough uint64
	// Body is the canonical public event body, opaque here and never decoded.
	Body json.RawMessage
	// Encoded is the record as received, which is the record that is
	// forwarded. Nothing re-encodes it.
	Encoded []byte
}

// Record returns the queue record that forwards this publication unchanged.
func (e Enduring) Record() Record {
	return Record{Class: ClassEnduring, Encoded: e.Encoded, Seq: e.JournalSeq}
}

// ParseEnduring parses one encoded session-channel record's ROUTING AND
// SEQUENCE ENVELOPE and returns it with its bytes preserved.
//
// The body is never decoded and never re-encoded. Encoded is the caller's own
// slice, so what is forwarded is byte-identical to what the Host sent --
// including a member order Core's marshaller would not produce, which is what
// TestForwardingPreservesTheExactBytesItReceived drives.
//
// TWO BOUNDS ON THE WATERMARK, against two different quantities:
//
//   - BELOW its own event sequence. That comparison is CORE's -- decoding
//     through EnduringPublication is what applies it -- and is classified here
//     rather than restated, so this package cannot disagree with the wire
//     contract about what a live publication covers.
//   - ABOVE the Host's committed append sequence. That bound is this
//     package's, and committedAppend is an INPUT rather than something derived
//     from the record: a record cannot vouch for itself. See the note in
//     internal/routing on where a Host declares it, and on the fact that Core
//     v0.8.0's HostLink vocabulary has no member carrying it yet.
func ParseEnduring(encoded []byte, committedAppend uint64) (Enduring, error) {
	recordType, err := sessionwire.SessionRecordTypeOf(encoded)
	if err != nil {
		return Enduring{}, fmt.Errorf("%w: %w", ErrNotEnduring, err)
	}
	if recordType != sessionwire.SessionRecordTypeEnduringPublication {
		return Enduring{}, fmt.Errorf("%w: record type is %q", ErrNotEnduring, recordType)
	}
	var publication sessionwire.EnduringPublication
	if err := json.Unmarshal(encoded, &publication); err != nil {
		// Core's EnduringPublication rejects a covered_through that is not the
		// event's own sequence, which is the lower bound of the pair. It names
		// the FIELD in a typed error rather than in a message, which is what
		// lets this classify the refusal without matching text and without
		// restating the comparison. Every other decode failure is a malformed
		// record and is reported as one.
		var invalid *sessionwire.RequestValidationError
		if errors.As(err, &invalid) && invalid.Field == "covered_through" {
			return Enduring{}, fmt.Errorf("%w: %w", ErrWatermarkNotItsEventSequence, err)
		}
		return Enduring{}, fmt.Errorf("%w: %w", ErrMalformed, err)
	}
	if publication.CoveredThrough > committedAppend {
		return Enduring{}, fmt.Errorf("%w: covered_through %d, committed append %d",
			ErrWatermarkAboveCommitted, publication.CoveredThrough, committedAppend)
	}
	return Enduring{
		TenantID:       publication.TenantID,
		SessionID:      publication.SessionID,
		EventID:        publication.EventID,
		JournalSeq:     publication.JournalSeq,
		CoveredThrough: publication.CoveredThrough,
		Body:           publication.Body,
		Encoded:        encoded,
	}, nil
}
