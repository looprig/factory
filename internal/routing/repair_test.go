package routing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/modfiles"
	"github.com/looprig/factory/internal/realtime/delivery"
)

// HOW THE OVERFLOW AND REPAIR SPACE BELOW WAS DERIVED.
//
// Runbook A7.3 steps 3 and 4 are each a "clear only that one, the peers
// continue" claim, which is a NEGATIVE assertion about everything else and the
// weakest shape this program has. A case that clears one queue and observes
// that the peers still work proves almost nothing: a relay that reset every
// binding would also leave them "working". So every peer assertion here is
// written against an OBSERVABLE a wrong repair produces -- a session.reset
// record in the peer's stream, a tail stop for the wrong session, a rebind of a
// session nobody repaired -- and each of those cases carries the positive half
// in the same function, asserting that the binding that WAS repaired got all of
// it. The repair is then visible as a difference, not as an absence.
//
// The space itself is the cross product of four axes, enumerated from the code:
//
//   - Axis A, WHICH BOUND GIVES WAY: a DeliveryBinding's outbound queue, a
//     HostBinding's route queue, or a physical HostLink closing. The last two
//     take the same repair on purpose, so the axis has two repairs and three
//     entry points.
//   - Axis B, WHAT THE FULL QUEUE HOLDS: enduring records only (no victim, so
//     the bound gives way), enduring records plus ephemeral ones (a victim
//     exists, so it does not). That is delivery.selectVictim's partition, and
//     it is what makes step 2's "separately" observable at THIS layer rather
//     than only at the queue's.
//   - Axis C, WHETHER THE REPAIR CAN BE COMPLETED: the reset queues; the reset
//     is incoherent because the captured tip is behind what this binding was
//     already told (Core's SessionReset.Validate refuses it); the tip cannot be
//     read at all.
//   - Axis D, THE FAN-OUT SHAPE: one binding; two peers on one session; two
//     sessions. Axis D is what makes every "only that one" claim checkable, and
//     a case that does not vary it is stated as a property of one binding.
//
// Two properties are NOT cells of that product and are stated as for-alls: that
// a session.reset is a function of its (last, tip) pair alone -- the fuzz target
// -- and that the routing table's demand has exactly one holder, which is a
// structural guard over this package's own sources.

const (
	linkA = LinkID("link-a")
	linkB = LinkID("link-b")
	linkC = LinkID("link-c")

	otherSession = sessionwire.SessionID("session-b")
)

// testRepairLimits is deliberately off-default AND asymmetric. Both halves
// matter: off-default so a bound taken from a constant of its own fails, and
// asymmetric so a relay that passed one limit to both queues fails too.
var testRepairLimits = RepairLimits{HostBindingQueue: 5, DeliveryBindingQueue: 3}

// ---------------------------------------------------------------------------
// Fakes.
// ---------------------------------------------------------------------------

type publishedRecord struct {
	link    LinkID
	tenant  sessionwire.TenantID
	session sessionwire.SessionID
	encoded string
}

type closedLink struct {
	link   LinkID
	reason string
}

// recordingPublisher is the transport. It keeps the published BYTES rather than
// a decoded value, because "forwarded unchanged" is a claim about bytes, and it
// can refuse per link, because a repair is only observable when one consumer is
// slower than its peers.
type recordingPublisher struct {
	mu        sync.Mutex
	blocked   map[LinkID]bool
	failing   map[LinkID]error
	published []publishedRecord
	closes    []closedLink
}

func newPublisher() *recordingPublisher {
	return &recordingPublisher{blocked: map[LinkID]bool{}, failing: map[LinkID]error{}}
}

func (p *recordingPublisher) Publish(_ context.Context, link LinkID, tenant sessionwire.TenantID, session sessionwire.SessionID, encoded []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.failing[link]; err != nil {
		return err
	}
	if p.blocked[link] {
		return ErrWouldBlock
	}
	p.published = append(p.published, publishedRecord{link: link, tenant: tenant, session: session, encoded: string(encoded)})
	return nil
}

func (p *recordingPublisher) CloseLink(_ context.Context, link LinkID, reason string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closes = append(p.closes, closedLink{link: link, reason: reason})
	return nil
}

func (p *recordingPublisher) block(link LinkID, blocked bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blocked[link] = blocked
}

// to returns everything published to one link, in order.
func (p *recordingPublisher) to(link LinkID) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []string
	for _, record := range p.published {
		if record.link == link {
			out = append(out, record.encoded)
		}
	}
	return out
}

// reasonFor returns why a link was closed. The reason is asserted rather than
// ignored because the three ways a repair gives up on a client -- no tip, an
// incoherent control, a queue that would not take it -- are operationally
// different, and one string for all three would make them one event in a log.
// scopeOf returns the (tenant, session) one link's records were published
// under. A relay that fanned a session's records to the right link under the
// wrong scope would pass every byte assertion here, because the scope is not in
// the record's own bytes as far as the transport is concerned.
func (p *recordingPublisher) scopeOf(link LinkID) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []string
	for _, record := range p.published {
		if record.link == link {
			out = append(out, string(record.tenant)+"/"+string(record.session))
		}
	}
	return out
}

func (p *recordingPublisher) reasonFor(link LinkID) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, closed := range p.closes {
		if closed.link == link {
			return closed.reason
		}
	}
	return ""
}

func (p *recordingPublisher) closedLinks() []LinkID {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]LinkID, 0, len(p.closes))
	for _, closed := range p.closes {
		out = append(out, closed.link)
	}
	return out
}

type resumeCall struct {
	session  sessionwire.SessionID
	afterSeq uint64
}

type recordingTail struct {
	mu        sync.Mutex
	stops     []sessionwire.SessionID
	resumes   []resumeCall
	stopErr   error
	order     []string
	resumeErr error
}

func (t *recordingTail) Stop(_ context.Context, _ sessionwire.TenantID, session sessionwire.SessionID) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.stops = append(t.stops, session)
	t.order = append(t.order, "stop:"+string(session))
	return t.stopErr
}

func (t *recordingTail) Resume(_ context.Context, _ sessionwire.TenantID, session sessionwire.SessionID, afterSeq uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.resumes = append(t.resumes, resumeCall{session: session, afterSeq: afterSeq})
	t.order = append(t.order, fmt.Sprintf("resume:%s:%d", session, afterSeq))
	return t.resumeErr
}

type recordingRebinder struct {
	mu       sync.Mutex
	sessions []sessionwire.SessionID
	err      error
	order    *recordingTail
}

func (r *recordingRebinder) Rebind(_ context.Context, _ sessionwire.TenantID, session sessionwire.SessionID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sessions = append(r.sessions, session)
	if r.order != nil {
		r.order.mu.Lock()
		r.order.order = append(r.order.order, "rebind:"+string(session))
		r.order.mu.Unlock()
	}
	return r.err
}

// ---------------------------------------------------------------------------
// Fixture and record builders.
// ---------------------------------------------------------------------------

type relayFixture struct {
	t        *testing.T
	relay    *Relay
	tips     *scriptedTips
	tail     *recordingTail
	pub      *recordingPublisher
	rebinder *recordingRebinder
}

func newRelayFixture(t *testing.T, limits RepairLimits, tips ...uint64) *relayFixture {
	t.Helper()

	if len(tips) == 0 {
		tips = []uint64{1000}
	}
	scripted := &scriptedTips{tips: tips}
	tail := &recordingTail{}
	rebinder := &recordingRebinder{order: tail}
	publisher := newPublisher()
	relay, err := NewRelay(scripted, rebinder, tail, publisher, limits)
	if err != nil {
		t.Fatalf("NewRelay: %v", err)
	}
	return &relayFixture{t: t, relay: relay, tips: scripted, tail: tail, pub: publisher, rebinder: rebinder}
}

func (f *relayFixture) open(session sessionwire.SessionID, links ...LinkID) {
	f.t.Helper()
	if err := f.relay.Open(bindTenant, session); err != nil {
		f.t.Fatalf("Open(%s): %v", session, err)
	}
	for _, link := range links {
		if err := f.relay.Subscribe(bindTenant, session, link); err != nil {
			f.t.Fatalf("Subscribe(%s, %s): %v", session, link, err)
		}
	}
}

// feed receives one enduring record at seq and pumps, which is the ordinary
// cycle. The committed append sequence is deliberately generous here so that
// the watermark fence is exercised by the cases written FOR it rather than
// incidentally by every other case.
func (f *relayFixture) feed(session sessionwire.SessionID, seq uint64) error {
	f.t.Helper()
	if err := f.relay.Receive(context.Background(), bindTenant, session, Frame{
		Encoded: enduringRecord(f.t, session, seq), CommittedAppendSeq: 1_000_000,
	}); err != nil {
		return err
	}
	return f.relay.Pump(context.Background(), bindTenant, session)
}

func (f *relayFixture) mustFeed(session sessionwire.SessionID, seqs ...uint64) {
	f.t.Helper()
	for _, seq := range seqs {
		if err := f.feed(session, seq); err != nil {
			f.t.Fatalf("feed(%s, %d): %v", session, seq, err)
		}
	}
}

func enduringRecord(t *testing.T, session sessionwire.SessionID, seq uint64) []byte {
	t.Helper()
	encoded, err := sessionwire.EnduringPublication{
		TenantID: bindTenant, SessionID: session,
		EventID:    sessionwire.EventID(fmt.Sprintf("event-%s-%d", session, seq)),
		JournalSeq: seq, CoveredThrough: seq, Body: json.RawMessage(`{"n":1}`),
	}.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal enduring: %v", err)
	}
	return encoded
}

// resetRecord builds the exact bytes a repair must publish for one (last, tip)
// pair. It is built through Core's own type, so a case asserting on it is
// asserting on the encoding the relay produces and not on a restatement of it.
func resetRecord(t *testing.T, session sessionwire.SessionID, last, tip uint64) []byte {
	t.Helper()
	encoded, err := sessionwire.SessionReset{
		TenantID: bindTenant, SessionID: session, LastContiguous: last, JournalTip: tip,
	}.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal reset: %v", err)
	}
	return encoded
}

func ephemeralRecord(t *testing.T, session sessionwire.SessionID, body string) []byte {
	t.Helper()
	encoded, err := sessionwire.EphemeralPublication{
		TenantID: bindTenant, SessionID: session, Body: json.RawMessage(body),
	}.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal ephemeral: %v", err)
	}
	return encoded
}

// resetsIn decodes every session.reset among a link's published records. The
// PRESENCE of one is the observable every "peers continue" assertion below
// turns on: a wrongly repaired peer has one and a correctly untouched peer has
// none, so the negative claim has a positive witness.
func resetsIn(t *testing.T, records []string) []sessionwire.SessionReset {
	t.Helper()
	var out []sessionwire.SessionReset
	for _, encoded := range records {
		recordType, err := sessionwire.SessionRecordTypeOf([]byte(encoded))
		if err != nil {
			t.Fatalf("a published record is not a session-channel record: %v", err)
		}
		if recordType != sessionwire.SessionRecordTypeSessionReset {
			continue
		}
		var reset sessionwire.SessionReset
		if err := reset.UnmarshalJSON([]byte(encoded)); err != nil {
			t.Fatalf("decode reset: %v", err)
		}
		out = append(out, reset)
	}
	return out
}

// ---------------------------------------------------------------------------
// Composition.
// ---------------------------------------------------------------------------

func TestNewRelayRefusesAnIncompleteComposition(t *testing.T) {
	tips, tail, pub, rebinder := &scriptedTips{}, &recordingTail{}, newPublisher(), &recordingRebinder{}
	for _, tc := range []struct {
		name     string
		tips     TipReader
		rebinder Rebinder
		tail     Tail
		pub      Publisher
		limits   RepairLimits
	}{
		{"no tip reader", nil, rebinder, tail, pub, testRepairLimits},
		{"no rebinder", tips, nil, tail, pub, testRepairLimits},
		{"no tail", tips, rebinder, nil, pub, testRepairLimits},
		{"no publisher", tips, rebinder, tail, nil, testRepairLimits},
		{"a zero host bound", tips, rebinder, tail, pub, RepairLimits{HostBindingQueue: 0, DeliveryBindingQueue: 3}},
		{"a zero delivery bound", tips, rebinder, tail, pub, RepairLimits{HostBindingQueue: 5, DeliveryBindingQueue: 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewRelay(tc.tips, tc.rebinder, tc.tail, tc.pub, tc.limits); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("NewRelay = %v, want ErrInvalidConfig", err)
			}
		})
	}
	if _, err := NewRelay(tips, rebinder, tail, pub, testRepairLimits); err != nil {
		t.Fatalf("the complete composition was refused: %v", err)
	}
}

// TestTheTwoQueueBoundsAreNotTheSameNumber. While they were equal, a relay that
// passed one constant to both queues would be indistinguishable from one that
// passed each to its own site -- the defect internal/httpapi measured on its two
// page ceilings and paid for with a surviving mutant.
func TestTheTwoQueueBoundsAreNotTheSameNumber(t *testing.T) {
	limits := DefaultRepairLimits()
	if limits.HostBindingQueue != 1024 || limits.DeliveryBindingQueue != 512 {
		t.Fatalf("defaults = %d/%d, want 1024/512", limits.HostBindingQueue, limits.DeliveryBindingQueue)
	}
	if limits.HostBindingQueue <= limits.DeliveryBindingQueue {
		t.Fatal("the inbound bound must exceed the outbound one: one inbound record fans out to every subscriber")
	}
	if err := limits.Validate(); err != nil {
		t.Fatalf("the defaults do not validate: %v", err)
	}
}

// TestEachBoundIsAppliedAtItsOwnSite drives the two off-default numbers to two
// different absolute answers, so a relay that swapped them, or that took either
// from a constant of its own, fails here.
func TestEachBoundIsAppliedAtItsOwnSite(t *testing.T) {
	t.Run("the delivery bound is 3", func(t *testing.T) {
		f := newRelayFixture(t, testRepairLimits)
		f.open(bindSession, linkA)
		f.pub.block(linkA, true)

		f.mustFeed(bindSession, 1, 2, 3)
		if got := f.relay.Queued(bindTenant, bindSession, linkA); got != 3 {
			t.Fatalf("queued = %d, want exactly the delivery bound of 3", got)
		}
		if resets := resetsIn(t, f.pub.to(linkA)); len(resets) != 0 {
			t.Fatalf("a repair ran at or below the bound: %v", resets)
		}
		f.mustFeed(bindSession, 4)
		if got := f.relay.Queued(bindTenant, bindSession, linkA); got != 1 {
			t.Fatalf("queued after the 4th record = %d, want 1: the queue is cleared and holds the reset", got)
		}
	})

	t.Run("the host bound is 5", func(t *testing.T) {
		f := newRelayFixture(t, testRepairLimits)
		// No DeliveryBinding at all, so nothing drains the route queue and the
		// inbound bound is the only one in play.
		f.open(bindSession)
		for seq := uint64(1); seq <= 5; seq++ {
			if err := f.relay.Receive(context.Background(), bindTenant, bindSession, Frame{
				Encoded: enduringRecord(t, bindSession, seq), CommittedAppendSeq: 1_000_000,
			}); err != nil {
				t.Fatalf("record %d was refused: %v", seq, err)
			}
		}
		if len(f.tail.stops) != 0 {
			t.Fatalf("a repair ran at or below the inbound bound of 5: %v", f.tail.stops)
		}
		if err := f.relay.Receive(context.Background(), bindTenant, bindSession, Frame{
			Encoded: enduringRecord(t, bindSession, 6), CommittedAppendSeq: 1_000_000,
		}); err != nil {
			t.Fatalf("the 6th record: %v", err)
		}
		if len(f.tail.stops) != 1 {
			t.Fatalf("tail stops = %d, want exactly 1 at the 6th record", len(f.tail.stops))
		}
	})
}

// ---------------------------------------------------------------------------
// Step 3: a DeliveryBinding's enduring overflow.
// ---------------------------------------------------------------------------

// TestADeliveryOverflowRepairsOnlyThatBindingAndItsPeersKeepStreaming is step 3
// whole, with the peer assertion written against an observable.
func TestADeliveryOverflowRepairsOnlyThatBindingAndItsPeersKeepStreaming(t *testing.T) {
	const tip = 42
	f := newRelayFixture(t, testRepairLimits, tip)
	f.open(bindSession, linkA, linkB)

	// A takes two records and then stops reading. B keeps reading throughout.
	f.mustFeed(bindSession, 21, 22)
	f.pub.block(linkA, true)
	f.mustFeed(bindSession, 23, 24, 25, 26)

	// THE POSITIVE HALF: A was repaired.
	f.pub.block(linkA, false)
	if err := f.relay.Pump(context.Background(), bindTenant, bindSession); err != nil {
		t.Fatalf("Pump: %v", err)
	}
	resetsForA := resetsIn(t, f.pub.to(linkA))
	if len(resetsForA) != 1 {
		t.Fatalf("link A received %d resets, want exactly 1", len(resetsForA))
	}
	if resetsForA[0].LastContiguous != 22 || resetsForA[0].JournalTip != tip {
		t.Errorf("A's reset = (%d, %d), want (22, %d): the last SEQUENCE A was actually sent (a COUNT of "+
			"what it received would be 2), and the captured tip",
			resetsForA[0].LastContiguous, resetsForA[0].JournalTip, tip)
	}

	// THE NEGATIVE HALF, against the same observable: B has no reset, and its
	// stream is exactly the six records in order. A relay that repaired every
	// binding produces a reset here; one that cleared B's queue produces a gap.
	if resets := resetsIn(t, f.pub.to(linkB)); len(resets) != 0 {
		t.Fatalf("link B was repaired although its own queue never overflowed: %v", resets)
	}
	var want []string
	for seq := uint64(21); seq <= 26; seq++ {
		want = append(want, string(enduringRecord(t, bindSession, seq)))
	}
	if got := f.pub.to(linkB); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("link B's stream is\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if last, ok := f.relay.LastContiguous(bindTenant, bindSession, linkB); !ok || last != 26 {
		t.Errorf("B's last contiguous = %d/%v, want 26 (a count of delivered records would be 6)", last, ok)
	}
	// Neither the tail nor the binding is touched by a per-client repair: the
	// Host is keeping up, one browser is not.
	if len(f.tail.stops) != 0 || len(f.rebinder.sessions) != 0 {
		t.Errorf("a delivery repair stopped the tail (%v) or rebound (%v)", f.tail.stops, f.rebinder.sessions)
	}
}

// TestAPeersQUEUEDBacklogSurvivesAnotherBindingsOverflow is step 3's "clear ONLY
// that queue", and it is a SEPARATE case from the one above because the two
// halves of that clause need two different fixtures.
//
// The case above asserts the RESET half: a peer must not be told to repair. It
// cannot see the CLEAR half at all, because its peer is never blocked -- the
// fixture pumps after every record, so the peer's queue is empty at every
// instant the victim can overflow, and clearing an empty queue is a no-op. A
// mutant that cleared every peer's queue on a victim's overflow survived the
// whole module until this case existed.
//
// So here BOTH bindings hold a backlog when the victim overflows, and the
// assertion is the peer's whole published stream: silently losing an enduring
// record from an innocent peer is the exact defect this file exists to prevent,
// and a hole in the middle of a stream is what it looks like.
func TestAPeersQUEUEDBacklogSurvivesAnotherBindingsOverflow(t *testing.T) {
	const tip = 300
	f := newRelayFixture(t, testRepairLimits, tip)
	f.open(bindSession, linkA, linkB)

	f.mustFeed(bindSession, 61)
	f.pub.block(linkA, true)
	f.mustFeed(bindSession, 62)
	// From here BOTH queues fill. The victim is A, which is one record ahead.
	f.pub.block(linkB, true)
	f.mustFeed(bindSession, 63, 64)
	if got := f.relay.Queued(bindTenant, bindSession, linkA); got != 3 {
		t.Fatalf("the victim holds %d records, want 3 -- it must be FULL when the next one arrives", got)
	}
	if got := f.relay.Queued(bindTenant, bindSession, linkB); got != 2 {
		t.Fatalf("the peer holds %d records, want 2 -- a peer with an EMPTY queue cannot see this defect", got)
	}

	f.mustFeed(bindSession, 65)

	f.pub.block(linkA, false)
	f.pub.block(linkB, false)
	if err := f.relay.Pump(context.Background(), bindTenant, bindSession); err != nil {
		t.Fatalf("Pump: %v", err)
	}

	// The peer: every record, in order, with no hole and no reset.
	var want []string
	for seq := uint64(61); seq <= 65; seq++ {
		want = append(want, string(enduringRecord(t, bindSession, seq)))
	}
	if got := f.pub.to(linkB); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("the peer's stream is\n%s\nwant\n%s\n(a hole here is an enduring record lost to a "+
			"binding whose own queue never overflowed)", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if resets := resetsIn(t, f.pub.to(linkB)); len(resets) != 0 {
		t.Errorf("the peer was told to repair: %v", resets)
	}
	if last, ok := f.relay.LastContiguous(bindTenant, bindSession, linkB); !ok || last != 65 {
		t.Errorf("the peer's last contiguous = %d/%v, want 65 (a count would be 5)", last, ok)
	}

	// THE POSITIVE HALF, in the same function: the victim's queue really was
	// cleared and it really was told to repair, so the case cannot pass on a
	// relay that clears nothing at all.
	resets := resetsIn(t, f.pub.to(linkA))
	if len(resets) != 1 || resets[0].LastContiguous != 61 || resets[0].JournalTip != tip {
		t.Fatalf("the victim's resets = %v, want exactly one (61, %d) -- record 61 is all it was "+
			"ever sent, 62 to 65 were queued rather than applied, and a COUNT would be 1", resets, tip)
	}
	if got := f.pub.to(linkA); len(got) != 2 {
		t.Fatalf("the victim published %d records, want 2 -- record 1, then the reset; its queued "+
			"2, 3, 4 and 5 were discarded unapplied", len(got))
	}
}

// TestTheResetNamesWhatWasDELIVEREDAndNotWhatWasQueued. Everything a repair
// discards was never applied by the consumer, so a reset built from the queue
// would tell a client it already holds records it never saw -- a hole with a
// cursor asserting the hole is covered, which is exactly the defect wui's U2.2
// inverted.
func TestTheResetNamesWhatWasDELIVEREDAndNotWhatWasQueued(t *testing.T) {
	f := newRelayFixture(t, testRepairLimits, 99)
	f.open(bindSession, linkA)

	// The run starts at 10 rather than at 1 so that the delivered SEQUENCE and
	// the COUNT of delivered records are different numbers. They were equal
	// while it started at 1, and a mutant naming the count instead of the
	// sequence was invisible here.
	f.mustFeed(bindSession, 10, 11)
	f.pub.block(linkA, true)
	f.mustFeed(bindSession, 12, 13, 14, 15, 16)
	f.pub.block(linkA, false)
	if err := f.relay.Pump(context.Background(), bindTenant, bindSession); err != nil {
		t.Fatalf("Pump: %v", err)
	}
	resets := resetsIn(t, f.pub.to(linkA))
	if len(resets) != 1 || resets[0].LastContiguous != 11 {
		t.Fatalf("reset last_contiguous = %v, want exactly one naming 11 -- 16 would be the queued "+
			"answer and 2 would be the count of delivered records", resets)
	}
}

// TestAGapInTheDeliveredStreamSticksTheContiguousSequence. "Contiguous" is the
// word the step uses and it is not "the last one sent": a client told it has
// everything through a sequence it does not have will never read the hole.
func TestAGapInTheDeliveredStreamSticksTheContiguousSequence(t *testing.T) {
	f := newRelayFixture(t, testRepairLimits, 99)
	f.open(bindSession, linkA)

	f.mustFeed(bindSession, 41, 42, 45, 46)
	if last, _ := f.relay.LastContiguous(bindTenant, bindSession, linkA); last != 42 {
		t.Fatalf("last contiguous = %d, want 42: 45 is not 42's successor", last)
	}
	// The control: a stream with no gap advances all the way, so the case
	// cannot pass by the run never advancing at all.
	g := newRelayFixture(t, testRepairLimits, 99)
	g.open(otherSession, linkA)
	g.mustFeed(otherSession, 51, 52, 53, 54)
	if last, _ := g.relay.LastContiguous(bindTenant, otherSession, linkA); last != 54 {
		t.Fatalf("the contiguous control = %d, want 54 (a count would be 4)", last)
	}
}

// TestOnlyThatClientLinkIsClosedWhenTheResetCannotBeBuilt is step 3's second
// arm. The captured tip is BEHIND what this binding was already sent, so Core's
// SessionReset.Validate refuses the pair and there is no coherent repair
// instruction to queue -- and the answer is to close one link, not to keep
// streaming past a gap nobody was told about.
func TestOnlyThatClientLinkIsClosedWhenTheResetCannotBeBuilt(t *testing.T) {
	f := newRelayFixture(t, testRepairLimits, 3)
	f.open(bindSession, linkA, linkB)

	f.mustFeed(bindSession, 10, 11)
	f.pub.block(linkA, true)
	f.mustFeed(bindSession, 12, 13, 14)
	// The overflow. The refusal to BUILD the reset is reported rather than
	// swallowed, because a caller with no repair to offer has nothing else to
	// learn from; the link going is the action, the error is the record of why.
	if err := f.feed(bindSession, 15); err == nil {
		t.Fatal("the overflow reported success although no coherent reset could be built")
	}

	if closed := f.pub.closedLinks(); len(closed) != 1 || closed[0] != linkA {
		t.Fatalf("closed links = %v, want exactly [%s]", closed, linkA)
	}
	if got := f.pub.reasonFor(linkA); got != "repair control is incoherent" {
		t.Errorf("close reason = %q, want the incoherent-control one", got)
	}
	if got := f.relay.Bindings(bindTenant, bindSession); got != 1 {
		t.Fatalf("bindings remaining = %d, want 1: only the refused link goes", got)
	}
	if resets := resetsIn(t, f.pub.to(linkB)); len(resets) != 0 {
		t.Fatalf("the peer was repaired: %v", resets)
	}
	if last, ok := f.relay.LastContiguous(bindTenant, bindSession, linkB); !ok || last != 15 {
		t.Errorf("the peer's last contiguous = %d/%v, want 15: it kept streaming", last, ok)
	}
}

// TestADeliveryOverflowWhoseTipCannotBeReadClosesOnlyThatLink is step 3's
// FAIL-CLOSED arm, and it had no reader of any kind: not the close, not the
// "only that one", not the direction. Three mutations of that one branch
// survived the whole module, and the serious one is the third.
//
//   - close every binding on the session instead of the one that overflowed;
//   - close nothing at all, but still report the failure;
//   - RETURN NIL: swallow the tip failure and keep streaming.
//
// The third does exactly what the branch's own comment forbids. A delivery
// binding overflowed, so records were discarded unapplied; the tip could not be
// read, so no reset can be built; and the relay carries on publishing as though
// nothing happened. The client is never told, its cursor walks past the hole,
// and the whole module is green. That is the silent durable loss this file
// exists to prevent, at the one call site nothing was asserting about.
//
// It is the sibling of the step-4 tip-failure arm, ONE CALL SITE DOWN. That one
// is read by TestATipThatCannotBeReadClosesTheAffectedLinksAndLeavesTheTailStopped
// and was strengthened in this task's second round; the sweep that strengthened
// it covered "only that one" claims and negative observables and did not descend
// to the fail-closed tip read beneath them.
//
// The peer's assertion is deliberately clear of the sequence/count collision:
// 86 is its last contiguous sequence and 6 is the number of records it received.
//
// All three mutations die at the error assertion and the closed set. The final
// stream arm is a hedge against a future edit, not the thing they die on, and
// its comment says so -- see the note there, which was corrected after a gate
// measured that stripping it left all three dying.
func TestADeliveryOverflowWhoseTipCannotBeReadClosesOnlyThatLink(t *testing.T) {
	f := newRelayFixture(t, testRepairLimits)
	f.open(bindSession, linkA, linkB)

	f.mustFeed(bindSession, 81, 82)
	f.pub.block(linkA, true)
	f.mustFeed(bindSession, 83, 84, 85)
	if got := f.relay.Queued(bindTenant, bindSession, linkA); got != 3 {
		t.Fatalf("the victim holds %d records, want the delivery bound of 3 -- it must be FULL "+
			"when the next one arrives, or no overflow occurs and this case measures nothing", got)
	}

	f.tips.err = errors.New("store is draining")
	if err := f.feed(bindSession, 86); err == nil {
		t.Fatal("the overflow reported success although the tip could not be read: a caller told " +
			"nothing went wrong will keep feeding a binding that has silently lost records")
	}

	// ONLY that link, and it really is closed -- one assertion kills both the
	// too-wide mutation and the too-narrow one, because both change this set.
	closed := f.pub.closedLinks()
	if len(closed) != 1 || closed[0] != linkA {
		t.Fatalf("closed links = %v, want exactly [%s]: the peer never overflowed, and the victim "+
			"cannot be left streaming past a gap it was never told about", closed, linkA)
	}
	if got := f.pub.reasonFor(linkA); got != "repair tip unavailable" {
		t.Errorf("close reason = %q, want the missing-tip one", got)
	}
	if got := f.relay.Bindings(bindTenant, bindSession); got != 1 {
		t.Fatalf("bindings remaining = %d, want 1", got)
	}

	// The peer kept streaming throughout, which is the positive half: without it
	// a relay that closed every binding on any trouble would satisfy everything
	// above except the closed set alone.
	if last, ok := f.relay.LastContiguous(bindTenant, bindSession, linkB); !ok || last != 86 {
		t.Errorf("the peer's last contiguous = %d/%v, want 86 (a count would be 6)", last, ok)
	}
	var want []string
	for seq := uint64(81); seq <= 86; seq++ {
		want = append(want, string(enduringRecord(t, bindSession, seq)))
	}
	if got := f.pub.to(linkB); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("the peer's stream is\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}

	// AND THE FAIL-CLOSED DIRECTION ITSELF, as a HEDGE rather than as the
	// assertion the three mutations die on. Stating that accurately matters,
	// because the obvious reading of it is wrong and a later reader could delete
	// the wrong thing: all three mutations above die at the error assertion and
	// at the closed set, and stripping this arm entirely leaves all three still
	// dying -- measured, not assumed. What this arm buys is the case a future
	// edit creates: a swallowed tip failure that DID close something, where the
	// bookkeeping looks right and the stream is what is wrong.
	//
	// It compares content and order rather than counting. A length check here
	// would be blind to both, and is only unexploitable today because the peer
	// arm above pins the whole stream -- which is a property of a neighbouring
	// assertion, not of this one, and neighbours move.
	f.pub.block(linkA, false)
	if err := f.relay.Pump(context.Background(), bindTenant, bindSession); err != nil {
		t.Fatalf("Pump: %v", err)
	}
	stalled := []string{
		string(enduringRecord(t, bindSession, 81)),
		string(enduringRecord(t, bindSession, 82)),
	}
	if got := f.pub.to(linkA); strings.Join(got, "\n") != strings.Join(stalled, "\n") {
		t.Fatalf("the victim's stream is\n%s\nwant exactly the two records from before it stalled\n%s\n"+
			"(anything more is a stream continuing across a gap with no reset to explain it)",
			strings.Join(got, "\n"), strings.Join(stalled, "\n"))
	}
	if resets := resetsIn(t, f.pub.to(linkA)); len(resets) != 0 {
		t.Errorf("a reset was published although no tip could be read to build one: %v", resets)
	}
}

// TestATransportRefusalLeavesTheRecordQueuedAndPublishesItExactlyOnce. The
// queue is the bound, not the transport, and a drain that dropped what the
// transport refused would be the durable loss this file exists to prevent.
func TestATransportRefusalLeavesTheRecordQueuedAndPublishesItExactlyOnce(t *testing.T) {
	f := newRelayFixture(t, testRepairLimits)
	f.open(bindSession, linkA)

	f.pub.block(linkA, true)
	f.mustFeed(bindSession, 1)
	if got := f.relay.Queued(bindTenant, bindSession, linkA); got != 1 {
		t.Fatalf("queued = %d, want 1: a refused publish leaves the record queued", got)
	}
	if got := len(f.pub.to(linkA)); got != 0 {
		t.Fatalf("published = %d, want 0", got)
	}
	f.pub.block(linkA, false)
	if err := f.relay.Pump(context.Background(), bindTenant, bindSession); err != nil {
		t.Fatalf("Pump: %v", err)
	}
	if got := f.pub.to(linkA); len(got) != 1 || got[0] != string(enduringRecord(t, bindSession, 1)) {
		t.Fatalf("published %v, want exactly the one record once", got)
	}
	if got := f.relay.Queued(bindTenant, bindSession, linkA); got != 0 {
		t.Fatalf("queued after the publish = %d, want 0", got)
	}
}

// TestAPublishFailureClosesOnlyThatLink. A refusal to take a record now and a
// dead connection are different facts, and collapsing them would either drop a
// live client on a busy moment or stream forever at a socket that is gone.
func TestAPublishFailureClosesOnlyThatLink(t *testing.T) {
	f := newRelayFixture(t, testRepairLimits)
	f.open(bindSession, linkA, linkB)
	f.pub.failing[linkA] = errors.New("connection reset")

	_ = f.relay.Receive(context.Background(), bindTenant, bindSession, Frame{
		Encoded: enduringRecord(t, bindSession, 1), CommittedAppendSeq: 10,
	})
	if err := f.relay.Pump(context.Background(), bindTenant, bindSession); err == nil {
		t.Fatal("Pump reported no failure although a publish failed")
	}
	if closed := f.pub.closedLinks(); len(closed) != 1 || closed[0] != linkA {
		t.Fatalf("closed = %v, want exactly [%s]", closed, linkA)
	}
	if got := f.pub.to(linkB); len(got) != 1 {
		t.Fatalf("the peer received %d records, want 1: one dead link does not stop the fan-out", len(got))
	}
}

// ---------------------------------------------------------------------------
// Step 2 at this layer: ephemeral pressure is not enduring pressure.
// ---------------------------------------------------------------------------

// TestEphemeralPressureDoesNotRepairAnything, with the enduring control beside
// it. The two are asserted in ONE function deliberately: the claim is that the
// relay treats them differently, and a case that drove only the ephemeral side
// would also pass on a relay that repaired nothing at all.
//
// A NOTE ON THE PRODUCER, recorded rather than assumed. No Host push carrying an
// EphemeralPublication exists: internal/realtime/hostlink's vocabulary has a
// capacity report and a registry observation and nothing else, and Core v0.7.0
// defines no session-event framing at all. So this drives Receive directly,
// which is the seam the HostLink edge will call, and no filter is built for a
// stream that carries nothing -- the dispatch is on Core's own discriminator.
func TestEphemeralPressureDoesNotRepairAnything(t *testing.T) {
	f := newRelayFixture(t, testRepairLimits, 77)
	f.open(bindSession, linkA)
	f.pub.block(linkA, true)

	if err := f.relay.Receive(context.Background(), bindTenant, bindSession, Frame{
		Encoded: enduringRecord(t, bindSession, 1), CommittedAppendSeq: 10,
	}); err != nil {
		t.Fatalf("the enduring record: %v", err)
	}
	for i := range 20 {
		if err := f.relay.Receive(context.Background(), bindTenant, bindSession, Frame{
			Encoded:     ephemeralRecord(t, bindSession, fmt.Sprintf(`{"d":%d}`, i)),
			CoalesceKey: "tokens",
		}); err != nil {
			t.Fatalf("ephemeral %d: %v", i, err)
		}
		if err := f.relay.Pump(context.Background(), bindTenant, bindSession); err != nil {
			t.Fatalf("Pump after ephemeral %d: %v", i, err)
		}
	}
	if len(f.pub.closedLinks()) != 0 {
		t.Fatalf("an ephemeral flood closed a link: %v", f.pub.closedLinks())
	}
	f.pub.block(linkA, false)
	if err := f.relay.Pump(context.Background(), bindTenant, bindSession); err != nil {
		t.Fatalf("Pump: %v", err)
	}
	if resets := resetsIn(t, f.pub.to(linkA)); len(resets) != 0 {
		t.Fatalf("an ephemeral flood repaired the binding: %v", resets)
	}
	delivered := f.pub.to(linkA)
	if len(delivered) == 0 || delivered[0] != string(enduringRecord(t, bindSession, 1)) {
		t.Fatalf("the enduring record did not survive the flood; stream head = %v", delivered)
	}

	// THE CONTROL, on an identical fixture: the same number of ENDURING records
	// does repair, so the case above cannot pass on a relay that never repairs.
	g := newRelayFixture(t, testRepairLimits, 77)
	g.open(otherSession, linkA)
	g.pub.block(linkA, true)
	for seq := uint64(1); seq <= 20; seq++ {
		if err := g.feed(otherSession, seq); err != nil {
			t.Fatalf("control feed %d: %v", seq, err)
		}
	}
	g.pub.block(linkA, false)
	if err := g.relay.Pump(context.Background(), bindTenant, otherSession); err != nil {
		t.Fatalf("control Pump: %v", err)
	}
	if resets := resetsIn(t, g.pub.to(linkA)); len(resets) == 0 {
		t.Fatal("the enduring control produced no reset, so the ephemeral assertion above measures nothing")
	}
}

// TestTheRelayCarriesTheDeclaredCoalesceKeyIntoTheQueue. The key is the only
// thing that makes coalescing possible -- Core gives an ephemeral publication no
// identity at all -- so a relay that dropped it on the way into the queue would
// silently turn coalescing into plain drop-oldest, with every queue-level case
// still green because they build their records directly.
//
// The fixture is built so the two policies choose DIFFERENT victims: the quiet
// key's only delta is the OLDEST record in the buffer, so drop-oldest takes it
// and coalesce-the-busiest-key does not. A case in which both policies pick the
// same record proves nothing, which is why the ordering here is not arbitrary.
func TestTheRelayCarriesTheDeclaredCoalesceKeyIntoTheQueue(t *testing.T) {
	f := newRelayFixture(t, testRepairLimits, 500)
	f.open(bindSession, linkA)
	f.pub.block(linkA, true)

	feed := func(key, body string) {
		t.Helper()
		if err := f.relay.Receive(context.Background(), bindTenant, bindSession, Frame{
			Encoded: ephemeralRecord(t, bindSession, body), CoalesceKey: key,
		}); err != nil {
			t.Fatalf("ephemeral %s/%s: %v", key, body, err)
		}
		if err := f.relay.Pump(context.Background(), bindTenant, bindSession); err != nil {
			t.Fatalf("Pump: %v", err)
		}
	}
	feed("quiet", `{"d":"quiet-only"}`)
	feed("busy", `{"d":"busy-1"}`)
	feed("busy", `{"d":"busy-2"}`)
	if got := f.relay.Queued(bindTenant, bindSession, linkA); got != 3 {
		t.Fatalf("queued = %d, want the delivery bound of 3 before the eviction", got)
	}
	feed("busy", `{"d":"busy-3"}`)

	f.pub.block(linkA, false)
	if err := f.relay.Pump(context.Background(), bindTenant, bindSession); err != nil {
		t.Fatalf("Pump: %v", err)
	}
	want := []string{
		string(ephemeralRecord(t, bindSession, `{"d":"quiet-only"}`)),
		string(ephemeralRecord(t, bindSession, `{"d":"busy-2"}`)),
		string(ephemeralRecord(t, bindSession, `{"d":"busy-3"}`)),
	}
	if got := f.pub.to(linkA); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("the relay published\n%s\nwant\n%s\n(losing the quiet key's only delta is what a "+
			"dropped coalesce key looks like: the policy degrades to drop-oldest)",
			strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

// TestTheRelayForwardsAnEphemeralRecordsBytesUnchanged. The enduring path had
// this and the ephemeral path did not, so a relay that re-marshalled an
// ephemeral record passed: the body is a json.RawMessage and survives a round
// trip, so what a re-encode changes is the ENVELOPE spelling, which only a
// deliberately non-canonical fixture can see.
func TestTheRelayForwardsAnEphemeralRecordsBytesUnchanged(t *testing.T) {
	// Core emits its members in ALPHABETICAL order, so a non-canonical spelling
	// has to break that order rather than merely look unusual -- the first
	// attempt here was body/session_id/tenant_id/type, which IS Core's order,
	// and the anti-vacuity guard below caught it.
	const shuffled = `{"tenant_id":"tenant-a","session_id":"session-a",` +
		`"type":"ephemeral_publication","body":{"d":"delta"}}`
	if shuffled == string(ephemeralRecord(t, bindSession, `{"d":"delta"}`)) {
		t.Fatal("the fixture is in Core's own member order, so byte identity would hold for a re-encoding relay too")
	}

	f := newRelayFixture(t, testRepairLimits)
	f.open(bindSession, linkA)
	if err := f.relay.Receive(context.Background(), bindTenant, bindSession, Frame{
		Encoded: []byte(shuffled), CoalesceKey: "tokens",
	}); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if err := f.relay.Pump(context.Background(), bindTenant, bindSession); err != nil {
		t.Fatalf("Pump: %v", err)
	}
	if got := f.pub.to(linkA); len(got) != 1 || got[0] != shuffled {
		t.Fatalf("published %v, want the received bytes unchanged", got)
	}
}

// ---------------------------------------------------------------------------
// Step 4: the HostBinding repair.
// ---------------------------------------------------------------------------

// TestAHostLinkCloseRepairsEveryBindingFromOneTipAndResumesAfterIt is step 4 in
// its stated order, with the count of TIP READS as the assertion that "one
// durable tip" is one and not one per binding.
func TestAHostLinkCloseRepairsEveryBindingFromOneTipAndResumesAfterIt(t *testing.T) {
	const tip = 500
	f := newRelayFixture(t, testRepairLimits, tip)
	f.open(bindSession, linkA, linkB, linkC)

	// Each binding is at a different delivered position, so "each binding's own
	// last contiguous" is distinguishable from "one number for all of them".
	f.mustFeed(bindSession, 11, 12)
	f.pub.block(linkC, true)
	f.mustFeed(bindSession, 13)
	f.pub.block(linkB, true)
	f.mustFeed(bindSession, 14)

	if err := f.relay.HostLinkClosed(context.Background(), bindTenant, bindSession); err != nil {
		t.Fatalf("HostLinkClosed: %v", err)
	}
	if got := len(f.tips.reads()); got != 1 {
		t.Fatalf("%d tip reads, want exactly 1: every binding is reset from ONE captured tip", got)
	}
	f.pub.block(linkB, false)
	f.pub.block(linkC, false)
	if err := f.relay.Pump(context.Background(), bindTenant, bindSession); err != nil {
		t.Fatalf("Pump: %v", err)
	}
	// 14/13/12 rather than 4/3/2 deliberately: the counts each link received ARE
	// 4, 3 and 2, so the earlier fixture could not tell a per-binding SEQUENCE
	// from a per-binding count, and a mutant naming the count survived here.
	for link, wantLast := range map[LinkID]uint64{linkA: 14, linkB: 13, linkC: 12} {
		resets := resetsIn(t, f.pub.to(link))
		if len(resets) != 1 {
			t.Fatalf("%s received %d resets, want exactly 1", link, len(resets))
		}
		if resets[0].LastContiguous != wantLast || resets[0].JournalTip != tip {
			t.Errorf("%s's reset = (%d, %d), want (%d, %d)", link,
				resets[0].LastContiguous, resets[0].JournalTip, wantLast, tip)
		}
	}
	if len(f.tail.stops) != 1 || f.tail.stops[0] != bindSession {
		t.Errorf("tail stops = %v, want exactly one for %s", f.tail.stops, bindSession)
	}
	if len(f.rebinder.sessions) != 1 || f.rebinder.sessions[0] != bindSession {
		t.Errorf("rebinds = %v, want exactly one for %s", f.rebinder.sessions, bindSession)
	}
	if len(f.tail.resumes) != 1 || f.tail.resumes[0].afterSeq != tip {
		t.Errorf("resumes = %v, want exactly one after the captured tip %d", f.tail.resumes, tip)
	}
	// The ORDER is the step's, and it is asserted rather than inferred: a
	// resume before the rebind would restart a tail on a route being replaced,
	// and a resume before the stop would run two tails.
	want := []string{"stop:" + string(bindSession), "rebind:" + string(bindSession),
		fmt.Sprintf("resume:%s:%d", bindSession, tip)}
	if strings.Join(f.tail.order, ",") != strings.Join(want, ",") {
		t.Errorf("repair order = %v, want %v", f.tail.order, want)
	}
}

// TestARouteQueueOverflowTakesTheSameRepairAsAHostLinkClose. Two entry points,
// one repair: a second repair path would be a second place for the reset to be
// built differently.
func TestARouteQueueOverflowTakesTheSameRepairAsAHostLinkClose(t *testing.T) {
	const tip = 31
	f := newRelayFixture(t, testRepairLimits, tip)
	f.open(bindSession, linkA)
	f.mustFeed(bindSession, 31)

	// Nothing pumps from here, so the inbound bound of 5 is what gives way.
	for seq := uint64(32); seq <= 37; seq++ {
		if err := f.relay.Receive(context.Background(), bindTenant, bindSession, Frame{
			Encoded: enduringRecord(t, bindSession, seq), CommittedAppendSeq: 1_000,
		}); err != nil {
			t.Fatalf("record %d: %v", seq, err)
		}
	}
	if err := f.relay.Pump(context.Background(), bindTenant, bindSession); err != nil {
		t.Fatalf("Pump: %v", err)
	}
	resets := resetsIn(t, f.pub.to(linkA))
	if len(resets) != 1 || resets[0].LastContiguous != 31 || resets[0].JournalTip != tip {
		t.Fatalf("resets = %v, want exactly one (31, %d) -- a count would be 1", resets, tip)
	}
	if len(f.tail.stops) != 1 || len(f.rebinder.sessions) != 1 || len(f.tail.resumes) != 1 {
		t.Errorf("stops/rebinds/resumes = %d/%d/%d, want 1/1/1",
			len(f.tail.stops), len(f.rebinder.sessions), len(f.tail.resumes))
	}
}

// TestNoPreRepairRecordReachesAClientAfterItsReset is step 4's "clear the route
// queue", which had no reader: deleting that one statement passed the whole
// module while delivering a record queued BEFORE the repair to a client AFTER
// its session.reset -- a stale record arriving behind a control that has already
// told the client it is repaired to the tip.
//
// EXACTLY ONE backlog record, and that is not an arbitrary fixture choice. With
// three, the uncleared backlog re-overflows the delivery queue on the next pump
// and triggers a SECOND repair, whose reset lands after the stale records and
// makes the stream look plausible again -- so the defect hides behind a single
// reset. One record is the size at which it stays visible.
func TestNoPreRepairRecordReachesAClientAfterItsReset(t *testing.T) {
	const tip = 77
	f := newRelayFixture(t, testRepairLimits, tip)
	f.open(bindSession, linkA)
	f.mustFeed(bindSession, 41)

	// Queued on the ROUTE queue and deliberately not pumped: this is the record
	// the repair must discard.
	if err := f.relay.Receive(context.Background(), bindTenant, bindSession, Frame{
		Encoded: enduringRecord(t, bindSession, 42), CommittedAppendSeq: 1_000,
	}); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if err := f.relay.HostLinkClosed(context.Background(), bindTenant, bindSession); err != nil {
		t.Fatalf("HostLinkClosed: %v", err)
	}
	if err := f.relay.Pump(context.Background(), bindTenant, bindSession); err != nil {
		t.Fatalf("Pump: %v", err)
	}

	got := f.pub.to(linkA)
	want := []string{string(enduringRecord(t, bindSession, 41)), string(resetRecord(t, bindSession, 41, tip))}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("link A's stream is\n%s\nwant\n%s\n(a third record here is the pre-repair backlog "+
			"arriving behind the reset that already told this client where it stands)",
			strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	// The positive half: the session is live again, and the next record -- which
	// belongs to the RESUMED tail, so it is after the tip the reset named --
	// does arrive. Without this the case would pass on a relay that had wedged
	// the session rather than repaired it.
	f.mustFeed(bindSession, tip+1)
	if got := f.pub.to(linkA); len(got) != 3 || got[2] != string(enduringRecord(t, bindSession, tip+1)) {
		t.Fatalf("the resumed tail's first record did not arrive; stream = %v", got)
	}

	// AND A PROPERTY THAT FALLS OUT OF IT, asserted rather than left to be
	// discovered: the binding does NOT resume claiming contiguity across the
	// repair. Its run is still 1, because the range between 1 and the tip was
	// closed by the CLIENT's own durable read, which this replica never sees.
	// So a later reset on this binding names 1 again -- conservative, and
	// deliberately so: a redundant re-read of a range the client already has is
	// idempotent, while claiming a sequence the client never received is the
	// hole-with-a-cursor defect this whole file exists to prevent.
	if last, ok := f.relay.LastContiguous(bindTenant, bindSession, linkA); !ok || last != 41 {
		t.Errorf("last contiguous after the repair = %d/%v, want 41: a repaired binding may not "+
			"vouch for a range it did not deliver", last, ok)
	}
}

// TestRepairingOneHostBindingLeavesEveryOtherSessionAlone is step 4's "unrelated
// HostBindings continue", written against three observables a wrong repair
// produces -- a reset in the other session's stream, a tail stop naming it, and
// a rebind of it -- with the positive half in the same function.
func TestRepairingOneHostBindingLeavesEveryOtherSessionAlone(t *testing.T) {
	f := newRelayFixture(t, testRepairLimits, 250)
	f.open(bindSession, linkA)
	f.open(otherSession, linkB)

	f.mustFeed(bindSession, 1, 2)
	f.mustFeed(otherSession, 71, 72, 73)

	if err := f.relay.HostLinkClosed(context.Background(), bindTenant, bindSession); err != nil {
		t.Fatalf("HostLinkClosed: %v", err)
	}
	if err := f.relay.Pump(context.Background(), bindTenant, bindSession); err != nil {
		t.Fatalf("Pump: %v", err)
	}

	// Positive half.
	if resets := resetsIn(t, f.pub.to(linkA)); len(resets) != 1 {
		t.Fatalf("the repaired session's link received %d resets, want 1", len(resets))
	}
	// Negative half, three observables. The tail stop and the rebind are DIRECT
	// state reads and are sound where they stand.
	if slices.Contains(f.tail.stops, otherSession) {
		t.Errorf("the unrelated session's tail was stopped: %v", f.tail.stops)
	}
	if slices.Contains(f.rebinder.sessions, otherSession) {
		t.Errorf("the unrelated session was rebound: %v", f.rebinder.sessions)
	}

	// THE RESET OBSERVABLE IS NOT A DIRECT STATE READ, AND THAT IS WHY IT IS
	// SAMPLED HERE RATHER THAN ABOVE. A reset is queued by Queue.Repair and is
	// not PUBLISHED until its own session is pumped, so a reset wrongly injected
	// into this session's binding sits in that queue, invisible, until the next
	// f.mustFeed below flushes it. Read before that pump, the observable cannot
	// be non-zero whatever the relay did -- a negative assertion that cannot
	// fail, which is the same vacuity a mutation found in the tip-failure case.
	// A mutant resetting every other session's bindings survived the whole
	// module until this check moved below the pump.
	f.mustFeed(otherSession, 74)
	if resets := resetsIn(t, f.pub.to(linkB)); len(resets) != 0 {
		t.Errorf("the unrelated session's link was reset (observed after its own next pump, which is "+
			"the first moment a queued control could reach it): %v", resets)
	}
	if last, ok := f.relay.LastContiguous(bindTenant, otherSession, linkB); !ok || last != 74 {
		t.Errorf("the unrelated session's last contiguous = %d/%v, want 74 (a count would be 4)", last, ok)
	}
	// And the stream itself has no hole, which is what a wrongly CLEARED queue
	// would leave behind where a wrongly RESET one leaves a control record.
	var want []string
	for seq := uint64(71); seq <= 74; seq++ {
		want = append(want, string(enduringRecord(t, otherSession, seq)))
	}
	if got := f.pub.to(linkB); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("the unrelated session's stream is\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

// TestATipThatCannotBeReadClosesTheAffectedLinksAndLeavesTheTailStopped is
// axis C's third value and the fail-closed direction. A reset naming no tip is
// not a repair instruction, and resuming a tail over a gap nobody was told
// about is exactly the defect.
func TestATipThatCannotBeReadClosesTheAffectedLinksAndLeavesTheTailStopped(t *testing.T) {
	f := newRelayFixture(t, testRepairLimits)
	f.open(bindSession, linkA, linkB)
	f.open(otherSession, linkC)
	f.mustFeed(bindSession, 1)
	f.mustFeed(otherSession, 1)

	// A PRE-REPAIR BACKLOG ON THE ROUTE QUEUE, deliberately not pumped. Without
	// it this case cannot see the failed path's own host.queue.Clear(): a
	// mutation deleting that one statement survived the whole module until this
	// line existed, because every other record in the fixture had already been
	// pumped out of the route queue. The record must not reach the client that
	// subscribes below, which never received a reset and has no idea a repair
	// was attempted.
	if err := f.relay.Receive(context.Background(), bindTenant, bindSession, Frame{
		Encoded: enduringRecord(t, bindSession, 2), CommittedAppendSeq: 1_000,
	}); err != nil {
		t.Fatalf("the pre-repair backlog: %v", err)
	}

	f.tips.err = errors.New("store is draining")
	if err := f.relay.HostLinkClosed(context.Background(), bindTenant, bindSession); err == nil {
		t.Fatal("HostLinkClosed reported success although the tip could not be read")
	}
	closed := f.pub.closedLinks()
	slices.Sort(closed)
	if !slices.Equal(closed, []LinkID{linkA, linkB}) {
		t.Fatalf("closed links = %v, want both of the repaired session's and neither of the other's", closed)
	}
	if got := f.pub.reasonFor(linkA); got != "repair tip unavailable" {
		t.Errorf("close reason = %q, want the missing-tip one", got)
	}
	if f.relay.Bindings(bindTenant, otherSession) != 1 {
		t.Error("the unrelated session lost a binding")
	}
	if len(f.rebinder.sessions) != 0 || len(f.tail.resumes) != 0 {
		t.Errorf("a failed repair rebound (%v) or resumed (%v) anyway", f.rebinder.sessions, f.tail.resumes)
	}
	// THE TAIL IS LEFT STOPPED, and this is where that is observable: a client
	// that subscribes AFTER the failed repair must not be served from the tail
	// the repair gave up on. A mutation deleting the stopped guard survived
	// until this arm existed, because the arm before it asserted about a link
	// the repair had already closed -- which receives nothing either way.
	if err := f.relay.Subscribe(bindTenant, bindSession, linkA); err != nil {
		t.Fatalf("Subscribe after the failed repair: %v", err)
	}
	if err := f.relay.Receive(context.Background(), bindTenant, bindSession, Frame{
		Encoded: enduringRecord(t, bindSession, 3), CommittedAppendSeq: 1_000,
	}); err != nil {
		t.Fatalf("a frame arriving on a stopped tail: %v", err)
	}
	if err := f.relay.Pump(context.Background(), bindTenant, bindSession); err != nil {
		t.Fatalf("Pump: %v", err)
	}
	if got := f.relay.Queued(bindTenant, bindSession, linkA); got != 0 {
		t.Errorf("the new binding queued %d records from a tail the repair abandoned, want 0", got)
	}
	if got := len(f.pub.to(linkA)); got != 1 {
		t.Errorf("link A received %d records, want the 1 from before the repair -- a second one is "+
			"the pre-repair route-queue backlog reaching a client that was never told to repair", got)
	}

	// The recovery path, which is also the control: a repair that CAN read a
	// tip resumes the tail, and the session serves again. Without it, a relay
	// that stopped every session forever would pass every assertion above.
	f.tips.err = nil
	if err := f.relay.HostLinkClosed(context.Background(), bindTenant, bindSession); err != nil {
		t.Fatalf("the second repair: %v", err)
	}
	if len(f.tail.resumes) != 1 {
		t.Fatalf("resumes = %v, want exactly one, from the repair that succeeded", f.tail.resumes)
	}
	if err := f.feed(bindSession, 4); err != nil {
		t.Fatalf("the session did not resume taking frames: %v", err)
	}
	if last, ok := f.relay.LastContiguous(bindTenant, bindSession, linkA); !ok || last != 4 {
		t.Errorf("the new binding's last contiguous = %d/%v, want 4", last, ok)
	}
}

// ---------------------------------------------------------------------------
// Step 5 at this layer.
// ---------------------------------------------------------------------------

// TestTheRelayForwardsTheBytesItReceived. The fixture is written in a member
// order Core's own marshaller does not use, so a relay that re-encoded the
// record would produce different bytes and this fails; a canonically ordered
// fixture would pass either way.
func TestTheRelayForwardsTheBytesItReceived(t *testing.T) {
	const shuffled = `{"body":{"n":1},"covered_through":7,"journal_seq":7,"event_id":"event-7",` +
		`"session_id":"session-a","tenant_id":"tenant-a","type":"enduring_publication"}`
	if shuffled == string(enduringRecord(t, bindSession, 7)) {
		t.Fatal("the fixture is in Core's own member order, so byte identity would hold for a re-encoding relay too")
	}

	f := newRelayFixture(t, testRepairLimits)
	f.open(bindSession, linkA)
	if err := f.relay.Receive(context.Background(), bindTenant, bindSession, Frame{
		Encoded: []byte(shuffled), CommittedAppendSeq: 7,
	}); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if err := f.relay.Pump(context.Background(), bindTenant, bindSession); err != nil {
		t.Fatalf("Pump: %v", err)
	}
	got := f.pub.to(linkA)
	if len(got) != 1 || got[0] != shuffled {
		t.Fatalf("published %v, want the received bytes unchanged", got)
	}
	if scope := f.pub.scopeOf(linkA); len(scope) != 1 || scope[0] != string(bindTenant)+"/"+string(bindSession) {
		t.Fatalf("published under %v, want %s/%s", scope, bindTenant, bindSession)
	}
}

// TestTheWatermarkFenceIsAppliedToEveryFrameTheRelayTakes drives the upper bound
// at its exact value in both directions. The lower bound is Core's and is driven
// at its own boundary in internal/realtime/delivery; what is asserted here is
// that the relay applies the fence at all and QUEUES NOTHING when it refuses.
func TestTheWatermarkFenceIsAppliedToEveryFrameTheRelayTakes(t *testing.T) {
	for _, tc := range []struct {
		name            string
		seq             uint64
		committedAppend uint64
		wantQueued      int
	}{
		{"exactly at the committed append", 7, 7, 1},
		{"one below the committed append", 7, 8, 1},
		{"one above the committed append", 7, 6, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRelayFixture(t, testRepairLimits)
			f.open(bindSession, linkA)
			f.pub.block(linkA, true)

			err := f.relay.Receive(context.Background(), bindTenant, bindSession, Frame{
				Encoded: enduringRecord(t, bindSession, tc.seq), CommittedAppendSeq: tc.committedAppend,
			})
			if tc.wantQueued == 0 && !errors.Is(err, delivery.ErrWatermarkAboveCommitted) {
				t.Fatalf("Receive = %v, want ErrWatermarkAboveCommitted", err)
			}
			if tc.wantQueued != 0 && err != nil {
				t.Fatalf("Receive = %v, want acceptance", err)
			}
			if err := f.relay.Pump(context.Background(), bindTenant, bindSession); err != nil {
				t.Fatalf("Pump: %v", err)
			}
			if got := f.relay.Queued(bindTenant, bindSession, linkA); got != tc.wantQueued {
				t.Fatalf("queued = %d, want %d: a refused frame must leave nothing behind", got, tc.wantQueued)
			}
		})
	}
}

// TestAHostMayNotSendARepairControl. A session.reset names what THIS replica
// forwarded in order; a Host is in no position to know that, so a control
// accepted from below would hand a client a coverage claim nobody can make.
func TestAHostMayNotSendARepairControl(t *testing.T) {
	reset, err := sessionwire.SessionReset{TenantID: bindTenant, SessionID: bindSession, LastContiguous: 1, JournalTip: 9}.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal reset: %v", err)
	}
	tip, err := sessionwire.JournalTip{TenantID: bindTenant, SessionID: bindSession, Tip: 9}.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal tip: %v", err)
	}
	for _, tc := range []struct {
		name    string
		encoded []byte
	}{
		{"a session reset", reset},
		{"a journal tip hint", tip},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRelayFixture(t, testRepairLimits)
			f.open(bindSession, linkA)
			f.pub.block(linkA, true)
			if err := f.relay.Receive(context.Background(), bindTenant, bindSession, Frame{Encoded: tc.encoded}); !errors.Is(err, ErrUnexpectedControl) {
				t.Fatalf("Receive = %v, want ErrUnexpectedControl", err)
			}
			if err := f.relay.Pump(context.Background(), bindTenant, bindSession); err != nil {
				t.Fatalf("Pump: %v", err)
			}
			if got := f.relay.Queued(bindTenant, bindSession, linkA); got != 0 {
				t.Fatalf("queued = %d, want 0", got)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Lifetime.
// ---------------------------------------------------------------------------

// TestUnsubscribeRemovesOneBindingAndLeavesItsPeersStreaming. Unsubscribe is a
// third "only that one" claim beside the two repairs, and it had no reader at
// all: a mutant closing every binding of the session passed the whole module,
// because the only case calling Unsubscribe held a single binding and asserted
// a count of zero, which is the same answer either way.
//
// This is the sibling sweep the two repair findings prompted, and it is why the
// sweep was worth doing: the defect here is not a repair going too wide, it is
// an ordinary disconnect taking every other viewer of the session with it.
func TestUnsubscribeRemovesOneBindingAndLeavesItsPeersStreaming(t *testing.T) {
	f := newRelayFixture(t, testRepairLimits, 400)
	f.open(bindSession, linkA, linkB, linkC)
	f.mustFeed(bindSession, 91)

	f.relay.Unsubscribe(bindTenant, bindSession, linkB)

	if got := f.relay.Bindings(bindTenant, bindSession); got != 2 {
		t.Fatalf("bindings after one Unsubscribe = %d, want 2", got)
	}
	if _, ok := f.relay.LastContiguous(bindTenant, bindSession, linkB); ok {
		t.Error("the unsubscribed binding is still held")
	}
	// The peers keep streaming, which is the half a count of bindings cannot
	// show: a relay that kept the map entries and stopped serving them would
	// pass the assertion above.
	f.mustFeed(bindSession, 92)
	for _, link := range []LinkID{linkA, linkC} {
		want := []string{
			string(enduringRecord(t, bindSession, 91)),
			string(enduringRecord(t, bindSession, 92)),
		}
		if got := f.pub.to(link); strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Errorf("%s's stream is %v, want both records", link, got)
		}
		if last, ok := f.relay.LastContiguous(bindTenant, bindSession, link); !ok || last != 92 {
			t.Errorf("%s's last contiguous = %d/%v, want 92", link, last, ok)
		}
	}
	// And the one that left is not served after it left.
	if got := len(f.pub.to(linkB)); got != 1 {
		t.Errorf("the unsubscribed link received %d records, want the 1 from before it left", got)
	}
}

func TestTheRelayRefusesWorkForSessionsAndBindingsItDoesNotHold(t *testing.T) {
	f := newRelayFixture(t, testRepairLimits)
	ctx := context.Background()

	if err := f.relay.Receive(ctx, bindTenant, bindSession, Frame{Encoded: enduringRecord(t, bindSession, 1), CommittedAppendSeq: 9}); !errors.Is(err, ErrNoHostBinding) {
		t.Errorf("Receive on an unopened session = %v, want ErrNoHostBinding", err)
	}
	if err := f.relay.Pump(ctx, bindTenant, bindSession); !errors.Is(err, ErrNoHostBinding) {
		t.Errorf("Pump on an unopened session = %v, want ErrNoHostBinding", err)
	}
	if err := f.relay.HostLinkClosed(ctx, bindTenant, bindSession); !errors.Is(err, ErrNoHostBinding) {
		t.Errorf("HostLinkClosed on an unopened session = %v, want ErrNoHostBinding", err)
	}
	if err := f.relay.Subscribe(bindTenant, bindSession, linkA); !errors.Is(err, ErrNoHostBinding) {
		t.Errorf("Subscribe to an unopened session = %v, want ErrNoHostBinding", err)
	}

	f.open(bindSession, linkA)
	f.relay.Unsubscribe(bindTenant, bindSession, linkA)
	if got := f.relay.Bindings(bindTenant, bindSession); got != 0 {
		t.Errorf("bindings after Unsubscribe = %d, want 0", got)
	}
	f.relay.Close()
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"Receive", f.relay.Receive(ctx, bindTenant, bindSession, Frame{Encoded: enduringRecord(t, bindSession, 1), CommittedAppendSeq: 9})},
		{"Pump", f.relay.Pump(ctx, bindTenant, bindSession)},
		{"HostLinkClosed", f.relay.HostLinkClosed(ctx, bindTenant, bindSession)},
		{"Open", f.relay.Open(bindTenant, bindSession)},
		{"Subscribe", f.relay.Subscribe(bindTenant, bindSession, linkA)},
	} {
		if !errors.Is(tc.err, ErrRelayClosed) {
			t.Errorf("%s after Close = %v, want ErrRelayClosed", tc.name, tc.err)
		}
	}
}

// ---------------------------------------------------------------------------
// "Repeatable" is a for-all, so a fixture is not its only reader.
// ---------------------------------------------------------------------------

// FuzzTheSessionResetIsAFunctionOfItsSequencePairAlone derives the space the
// fixtures above only sample. Two resets for one (last, tip) must be the same
// BYTES -- a record carrying a nonce, an attempt count or a sequence of its own
// would satisfy a table of values while making a repeated reset a different
// instruction each time -- and the pair Core refuses must stay refused.
func FuzzTheSessionResetIsAFunctionOfItsSequencePairAlone(f *testing.F) {
	f.Add(uint64(0), uint64(0))
	f.Add(uint64(1), uint64(1))
	f.Add(uint64(0), uint64(18446744073709551615))
	f.Add(uint64(5), uint64(4))
	f.Add(uint64(18446744073709551615), uint64(0))

	f.Fuzz(func(t *testing.T, last, tip uint64) {
		build := func() ([]byte, error) {
			return sessionwire.SessionReset{
				TenantID: bindTenant, SessionID: bindSession, LastContiguous: last, JournalTip: tip,
			}.MarshalJSON()
		}
		first, firstErr := build()
		second, secondErr := build()
		if (firstErr == nil) != (secondErr == nil) {
			t.Fatalf("two identical resets disagreed on validity: %v / %v", firstErr, secondErr)
		}
		if firstErr != nil {
			if last <= tip {
				t.Fatalf("a coherent pair (%d, %d) was refused: %v", last, tip, firstErr)
			}
			return
		}
		if last > tip {
			t.Fatalf("an incoherent pair (%d, %d) was accepted", last, tip)
		}
		if string(first) != string(second) {
			t.Fatalf("two resets for (%d, %d) differ:\n%s\n%s", last, tip, first, second)
		}
		var decoded sessionwire.SessionReset
		if err := decoded.UnmarshalJSON(first); err != nil {
			t.Fatalf("a reset this package built does not decode: %v", err)
		}
		if decoded.LastContiguous != last || decoded.JournalTip != tip {
			t.Fatalf("round trip = (%d, %d), want (%d, %d)", decoded.LastContiguous, decoded.JournalTip, last, tip)
		}
		// No member beyond the four the record declares: an extension captured
		// here would be a place a nonce could live.
		if extra := decoded.AdditionalFields(); len(extra) != 0 {
			t.Fatalf("the reset carries additional members %v", extra)
		}
	})
}

// ---------------------------------------------------------------------------
// A7.2-sole-demand-holder: the decision, its hazard, and its guard.
// ---------------------------------------------------------------------------

// TestRebindTakesAFreshRouteFromTheRegistry is the property Rebind exists for,
// and the reason the repair plane asks for it instead of taking demand of its
// own. The registry answer MOVES between the two observations, so a Rebind that
// reused the held route returns the old owner.
func TestRebindTakesAFreshRouteFromTheRegistry(t *testing.T) {
	resolver := newResolver(observation("host-a", 1, 1))
	binder := &recordingBinder{}
	bindings := newBindings(t, resolver, binder)
	demand, err := NewDemand(bindings, &scriptedTips{}, &recordingHinter{}, &manualClock{}, testDemandLimits)
	if err != nil {
		t.Fatalf("NewDemand: %v", err)
	}
	if err := demand.Acquire(context.Background(), bindTenant, bindSession); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if binding, ok := bindings.Binding(bindTenant, bindSession); !ok || binding.Key.HostID != "host-a" {
		t.Fatalf("first binding = %v/%v, want host-a", binding.Key.HostID, ok)
	}

	resolver.put(observation("host-b", 3, 4))
	if err := demand.Rebind(context.Background(), bindTenant, bindSession); err != nil {
		t.Fatalf("Rebind: %v", err)
	}
	binding, ok := bindings.Binding(bindTenant, bindSession)
	if !ok || binding.Key.HostID != "host-b" || binding.Key.HostGeneration != 3 || binding.Key.LeaseEpoch != 4 {
		t.Fatalf("rebound to %v, want host-b/3/4", binding.Key)
	}
	// The demand count is unchanged, which is what "without changing local
	// demand" means: one subscriber still holds exactly one unit, so the last
	// release still unbinds.
	if got := bindings.Demand(bindTenant, bindSession); got != 1 {
		t.Fatalf("demand after a rebind = %d, want 1", got)
	}
	if err := demand.Release(context.Background(), bindTenant, bindSession); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if _, stillBound := bindings.Binding(bindTenant, bindSession); stillBound {
		t.Fatal("the last release did not unbind, so the rebind leaked a unit of demand")
	}
}

// TestASecondHolderOfTheRoutingTablesDemandMakesARebindStale is the HAZARD the
// A7.2-sole-demand-holder decision was taken against, written down as a case so
// the decision has a reader rather than only a paragraph.
//
// It deliberately asserts the DEFECTIVE outcome. Bindings.Release counts, so
// with a second holder the release merely decrements, route.bound stays true,
// and the acquire inside Rebind is handed the same stale route with no registry
// read at all. If a later change makes this pass -- an owner-aware Release, say
// -- this case fails and sends the reader back to Demand.Rebind's argument for
// why owner-awareness is not the fix: two holders means the route legitimately
// outlives one holder's release, so the stale window is a property of there
// being two.
func TestASecondHolderOfTheRoutingTablesDemandMakesARebindStale(t *testing.T) {
	resolver := newResolver(observation("host-a", 1, 1))
	bindings := newBindings(t, resolver, &recordingBinder{})
	demand, err := NewDemand(bindings, &scriptedTips{}, &recordingHinter{}, &manualClock{}, testDemandLimits)
	if err != nil {
		t.Fatalf("NewDemand: %v", err)
	}
	if err := demand.Acquire(context.Background(), bindTenant, bindSession); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	// The second holder. Nothing in production does this, and
	// TestTheRoutingTablesDemandIsHeldOnlyByTheDemandPlane is what keeps it so.
	if _, err := bindings.Acquire(context.Background(), bindTenant, bindSession); err != nil {
		t.Fatalf("the second holder's Acquire: %v", err)
	}

	resolver.put(observation("host-b", 3, 4))
	readsBefore := resolver.count()
	if err := demand.Rebind(context.Background(), bindTenant, bindSession); err != nil {
		t.Fatalf("Rebind: %v", err)
	}
	binding, _ := bindings.Binding(bindTenant, bindSession)
	if binding.Key.HostID != "host-a" {
		t.Fatalf("the rebind produced %s. If Bindings.Release became owner-aware, re-read "+
			"Demand.Rebind's argument before deleting this case: the hazard it records is that a "+
			"route outliving one holder's release is adopted unre-read, which owner-awareness does "+
			"not remove.", binding.Key.HostID)
	}
	if resolver.count() != readsBefore {
		t.Errorf("the stale rebind read the registry %d times; the hazard is that it reads none",
			resolver.count()-readsBefore)
	}
}

func TestRebindRefusesASessionNobodyWatchesAndAClosedPlane(t *testing.T) {
	bindings := newBindings(t, newResolver(), &recordingBinder{})
	demand, err := NewDemand(bindings, &scriptedTips{}, &recordingHinter{}, &manualClock{}, testDemandLimits)
	if err != nil {
		t.Fatalf("NewDemand: %v", err)
	}
	if err := demand.Rebind(context.Background(), bindTenant, bindSession); !errors.Is(err, ErrNoDemand) {
		t.Errorf("Rebind of an unwatched session = %v, want ErrNoDemand", err)
	}
	if err := demand.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := demand.Rebind(context.Background(), bindTenant, bindSession); !errors.Is(err, ErrDemandClosed) {
		t.Errorf("Rebind after Close = %v, want ErrDemandClosed", err)
	}
}

// TestTheRoutingTablesDemandIsHeldOnlyByTheDemandPlane is the STRUCTURAL half of
// the A7.2-sole-demand-holder decision, and it is what makes the invariant a
// property of the module rather than of the code that happens to exist today.
//
// # The subject, and a correction
//
// An earlier version of this doc said "nothing outside internal/routing can hold
// this table's demand at all, because *Bindings is unexported vocabulary of an
// internal package". THAT IS FALSE and was found by a gate. `internal/` bars
// other MODULES, not other packages of this one, and Bindings, NewBindings,
// Acquire and Release are all EXPORTED -- so a second holder one package over
// compiles, passes, and is exactly the shape A9.1's composition will have, since
// the composition root is what hands a *Bindings to anything.
//
// So the scan has two arms:
//
//   - IN THIS PACKAGE, every demand-taking reference must sit inside a method on
//     *Demand. That is the whole surface here, because this is where *Bindings'
//     own methods and Demand's live.
//   - ELSEWHERE IN THE MODULE, a production file that imports this package may
//     not name Acquire or Release at all. That is deliberately wider than the
//     hazard -- it would also report a call on some unrelated type -- and wider
//     is the safe direction for a ban. If A9.1 legitimately needs one, this
//     fails loudly and a human re-reads Demand.Rebind's argument, which is the
//     outcome this invariant wants.
//
// # What it can and cannot see
//
// The unit is the SELECTOR, not the call, which is what closes a hole a gate
// found: `take := b.Acquire; take(ctx, t, s)` is a method VALUE and has no call
// expression to match, so a call-keyed scan missed the cheapest spelling of the
// hazard. Matching every `.Acquire`/`.Release` selector covers calls, method
// values and method expressions in one rule.
//
// It is name-keyed with no type information, deliberately and conservatively:
// every seam here is called through an interface, so a scan that only followed
// references it could resolve to a concrete receiver would follow none of them.
// What it still cannot see is a call through a func value reached from a struct
// field, a map or a slice whose contents it cannot trace, and a wrapper spelled
// some other name -- `qgateTake(b)` around Bindings.Acquire is outside its reach.
func TestTheRoutingTablesDemandIsHeldOnlyByTheDemandPlane(t *testing.T) {
	const moduleRoot = "../.."
	routingDir, err := filepath.Abs(".")
	if err != nil {
		t.Fatalf("resolve the package directory: %v", err)
	}
	importPath := routingImportPath(t, moduleRoot, routingDir)
	files, err := modfiles.Files(moduleRoot)
	if err != nil {
		t.Fatalf("modfiles.Files(%q): %v", moduleRoot, err)
	}

	fileSet := token.NewFileSet()
	var scanned, inPackage, importers, holders int
	var offenders []string
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		scanned++
		parsed, err := parser.ParseFile(fileSet, path, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		here := filepath.Dir(path) == routingDir
		if here {
			inPackage++
		} else {
			if !imports(parsed, importPath) {
				continue
			}
			importers++
		}
		held, found := scanDemandReferences(fileSet, parsed, here)
		holders += held
		offenders = append(offenders, found...)
	}

	// Anti-vacuity arms, because this passes on a clean tree whether it works or
	// not: it must have walked the module, it must have reached THIS package,
	// and it must have found the references the invariant is about.
	if scanned == 0 {
		t.Fatal("no production files were scanned, so this guard is vacuous")
	}
	if inPackage == 0 {
		t.Fatalf("scanned %d files and none of them was in %s: the scan is not reaching its own package", scanned, routingDir)
	}
	if holders < 3 {
		t.Fatalf("found %d demand-taking references inside *Demand's methods, want at least 3 "+
			"(bindLocked, refreshLocked, teardownLocked): the scan is not reaching them", holders)
	}
	if len(offenders) != 0 {
		t.Fatalf("the routing table's demand is taken outside the demand plane, which makes every "+
			"Rebind adopt a stale route -- see Demand.Rebind:\n  %s", strings.Join(offenders, "\n  "))
	}
	// The module arm is REPORTED rather than asserted, because zero importers is
	// the correct state today and will stop being it at A9.1. Saying so keeps a
	// later reader from mistaking a vacuous arm for a verified one.
	t.Logf("scanned %d production files; %d in this package, %d elsewhere importing it "+
		"(zero is expected until A9.1 composes)", scanned, inPackage, importers)
}

// routingImportPath is this package's import path, DERIVED END TO END: the
// module path from the go.mod the build resolves, and the package suffix from
// where this package actually sits relative to the module root.
//
// NEITHER HALF MAY BE A LITERAL, and the second half was one until a gate
// corrupted it: with "/internal/routing" spelled out, changing it left the
// module arm silently vacuous -- the t.Logf still reported "0 elsewhere
// importing it", which is the expected value today, so the corruption was
// invisible there too -- and the hazard it exists to catch escaped entirely.
// Worse, the arm's own control builds its fixtures from THIS function, so both
// sides moved together and the control could not see it either.
//
// That is exactly the class no_restore_test.go's key had to be corrected for
// twice, and it is the third time it has appeared in this repository: a guard
// naming its own subject with a literal is defeated by moving the subject.
// filepath.Rel is what makes the suffix follow the package instead.
func routingImportPath(t *testing.T, moduleRoot, packageDir string) string {
	t.Helper()
	source, err := os.ReadFile(filepath.Join(moduleRoot, "go.mod"))
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	modulePath := ""
	for line := range strings.SplitSeq(string(source), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 || fields[0] != "module" {
			continue
		}
		modulePath = fields[1]
		if unquoted, err := strconv.Unquote(modulePath); err == nil {
			modulePath = unquoted
		}
		break
	}
	if modulePath == "" {
		t.Fatal("go.mod declares no module path, so this guard cannot name its own subject")
	}
	root, err := filepath.Abs(moduleRoot)
	if err != nil {
		t.Fatalf("resolve the module root: %v", err)
	}
	suffix, err := filepath.Rel(root, packageDir)
	if err != nil {
		t.Fatalf("locate %s below %s: %v", packageDir, root, err)
	}
	derived := modulePath
	if suffix != "." {
		if strings.HasPrefix(suffix, "..") {
			t.Fatalf("this package resolved to %q, which is outside the module root %q", suffix, root)
		}
		derived = modulePath + "/" + filepath.ToSlash(suffix)
	}

	// AND CHECKED AGAINST THE COMPILER'S OWN ANSWER, which is the part that
	// makes the derivation unfalsifiable rather than merely unliteral.
	//
	// The module arm reaches zero files today, so a WRONG import path is
	// invisible in its result -- "0 elsewhere importing it" is the expected
	// value whether the path is right or corrupt -- and the arm's fixture
	// control is built from this same function, so both sides move together.
	// That is the shape this guard has now been corrected for twice, and
	// deriving the suffix from the filesystem removes the literal without
	// removing the blind spot.
	//
	// reflect.PkgPath is what closes it: it is the import path the LINKED
	// BINARY was built with, so it is the same string a real importer would
	// have to write, it costs nothing, and it follows a rename or a move
	// automatically. Requiring the two independent derivations to agree means a
	// corruption of either is a present-tense failure instead of a silently
	// vacuous arm.
	authoritative := reflect.TypeOf(Demand{}).PkgPath()
	if derived != authoritative {
		t.Fatalf("the derived import path is %q but this package is compiled as %q; the module arm "+
			"would scan for importers of a package that does not exist and report none",
			derived, authoritative)
	}
	return derived
}

func receiverIsDemand(function *ast.FuncDecl) bool {
	if function.Recv == nil || len(function.Recv.List) != 1 {
		return false
	}
	star, ok := function.Recv.List[0].Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	name, ok := star.X.(*ast.Ident)
	return ok && name.Name == "Demand"
}

func exprText(expr ast.Expr) string {
	switch value := expr.(type) {
	case *ast.Ident:
		return value.Name
	case *ast.SelectorExpr:
		return exprText(value.X) + "." + value.Sel.Name
	case *ast.ParenExpr:
		return "(" + exprText(value.X) + ")"
	case *ast.StarExpr:
		return "*" + exprText(value.X)
	default:
		return "?"
	}
}

func imports(file *ast.File, path string) bool {
	for _, spec := range file.Imports {
		if value, err := strconv.Unquote(spec.Path.Value); err == nil && value == path {
			return true
		}
	}
	return false
}

// scanDemandReferences reports how many demand-taking references sit inside
// *Demand's methods and which sit anywhere else.
//
// insideThePlane is false for a file outside internal/routing, where NO
// reference is permitted: a *Demand method cannot be declared there.
func scanDemandReferences(fileSet *token.FileSet, file *ast.File, insideThePlane bool) (holders int, offenders []string) {
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Body == nil {
			continue
		}
		permitted := insideThePlane && receiverIsDemand(function)
		ast.Inspect(function.Body, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok || (selector.Sel.Name != "Acquire" && selector.Sel.Name != "Release") {
				return true
			}
			if permitted {
				holders++
				return true
			}
			position := fileSet.Position(selector.Pos())
			offenders = append(offenders, fmt.Sprintf("%s:%d %s.%s",
				filepath.Base(position.Filename), position.Line, exprText(selector.X), selector.Sel.Name))
			return true
		})
	}
	return holders, offenders
}

// TestTheDemandGuardReportsEveryShapeOfHolderOutsideThePlane drives the scanner
// against fixtures rather than the real tree, because on a clean tree it passes
// whether it works or not. Four shapes and two controls, and the METHOD VALUE is
// the one a gate found escaping the previous call-keyed version.
func TestTheDemandGuardReportsEveryShapeOfHolderOutsideThePlane(t *testing.T) {
	for _, tc := range []struct {
		name           string
		source         string
		insideThePlane bool
		wantOffenders  int
		wantHolders    int
	}{
		{"a direct call from another type", `package routing
type Relay struct{ bindings *Bindings }
func (r *Relay) repair() { r.bindings.Acquire(nil, "", "") }`, true, 1, 0},
		{"a method VALUE from another type", `package routing
type Relay struct{ bindings *Bindings }
func (r *Relay) repair() { take := r.bindings.Acquire; take(nil, "", "") }`, true, 1, 0},
		{"a method EXPRESSION", `package routing
func reach(b *Bindings) { (*Bindings).Release(b, nil, "", "") }`, true, 1, 0},
		{"a closure in the wrong scope", `package routing
type Relay struct{ bindings *Bindings }
func (r *Relay) repair() { f := func() { r.bindings.Release(nil, "", "") }; f() }`, true, 1, 0},
		{"CONTROL: the same call on *Demand", `package routing
func (d *Demand) ok() { d.bindings.Release(nil, "", "") }`, true, 0, 1},
		{"CONTROL: a similarly named method that is not one of the two", `package routing
type Relay struct{ bindings *Bindings }
func (r *Relay) repair() { r.bindings.AcquireAll(nil, "", "") }`, true, 0, 0},
		{"outside the package, a *Demand receiver earns nothing", `package elsewhere
func (d *Demand) ok() { d.bindings.Release(nil, "", "") }`, false, 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fileSet := token.NewFileSet()
			parsed, err := parser.ParseFile(fileSet, "fixture.go", tc.source, parser.SkipObjectResolution)
			if err != nil {
				t.Fatalf("parse the fixture: %v", err)
			}
			holders, offenders := scanDemandReferences(fileSet, parsed, tc.insideThePlane)
			if len(offenders) != tc.wantOffenders || holders != tc.wantHolders {
				t.Fatalf("scan = %d offenders / %d holders, want %d / %d (offenders: %v)",
					len(offenders), holders, tc.wantOffenders, tc.wantHolders, offenders)
			}
		})
	}
}

// TestTheDemandGuardSeesAFileThatImportsThisPackage is the module arm's own
// control. That arm reaches zero files today, so without this it would be an
// assertion nobody could tell from a broken one.
func TestTheDemandGuardSeesAFileThatImportsThisPackage(t *testing.T) {
	fileSet := token.NewFileSet()
	packageDir, err := filepath.Abs(".")
	if err != nil {
		t.Fatalf("resolve the package directory: %v", err)
	}
	importPath := routingImportPath(t, "../..", packageDir)
	for _, tc := range []struct {
		name, source string
		want         bool
	}{
		{"imports this package", "package p\nimport \"" + importPath + "\"\nvar _ = routing.ErrNoDemand\n", true},
		{"imports a package whose path merely starts with it", "package p\nimport \"" + importPath + "extra\"\n", false},
		{"imports something else entirely", "package p\nimport \"strings\"\n", false},
		{"imports nothing", "package p\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := parser.ParseFile(fileSet, "fixture.go", tc.source, parser.SkipObjectResolution|parser.ImportsOnly)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if got := imports(parsed, importPath); got != tc.want {
				t.Fatalf("imports = %v, want %v", got, tc.want)
			}
		})
	}
}
