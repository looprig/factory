package delivery_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/delivery"
)

// HOW THIS SUITE'S SPACE WAS DERIVED, so a later reader can tell a hole from a
// deliberate omission.
//
// A bounded queue's whole behaviour is the cross product of three axes, each
// enumerated from the CODE rather than from the runbook's sentence:
//
//   - Axis A, WHAT ARRIVES: an enduring record, an ephemeral record with a
//     declared coalesce key, an ephemeral record without one, a control record.
//     Those are exactly the values Class can take, plus the one distinction the
//     ephemeral policy reads.
//   - Axis B, WHAT THE FULL BUFFER HOLDS: only enduring; only control; a mix of
//     enduring and one lone keyed ephemeral; two members of one key beside one
//     member of another; unkeyed ephemerals only; nothing (the queue is not
//     full). Those are the branches selectVictim can take.
//   - Axis C, THE QUEUE'S OWN STATE: open, or closed.
//
// Three properties are NOT cells of that product and are stated as for-alls
// instead: that the capacity is the number actually applied, that a record's
// bytes leave exactly as they arrived, and that parsing never invents or loses
// body bytes (the fuzz target).
//
// THE ONE INVERSION THIS FILE EXISTS FOR is wui's, ported: an enduring record
// is NEVER the victim. wui used to drop the oldest enduring frame and report
// it, which is silent durable loss with a receipt attached -- nothing
// redelivers the frame and the reconnect cursor walks past it as soon as a
// later frame is applied, so the transcript has a hole AND a persisted cursor
// asserting the hole was covered. U2.2 inverted it there
// (wui/packages/protocol/test/store-backpressure.test.ts:79, re-opened and
// read); this is the same inversion on Factory's side of the same stream.

const (
	tenant  = sessionwire.TenantID("tenant-a")
	session = sessionwire.SessionID("session-a")
)

// enduringBytes builds one encoded enduring publication through Core, so this
// suite never becomes a second authority for the wire shape.
func enduringBytes(t *testing.T, seq uint64, body string) []byte {
	t.Helper()
	encoded, err := sessionwire.EnduringPublication{
		TenantID: tenant, SessionID: session,
		EventID:    sessionwire.EventID(fmt.Sprintf("event-%d", seq)),
		JournalSeq: seq, CoveredThrough: seq, Body: json.RawMessage(body),
	}.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal enduring %d: %v", seq, err)
	}
	return encoded
}

func ephemeralBytes(t *testing.T, body string) []byte {
	t.Helper()
	encoded, err := sessionwire.EphemeralPublication{
		TenantID: tenant, SessionID: session, Body: json.RawMessage(body),
	}.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal ephemeral: %v", err)
	}
	return encoded
}

func resetBytes(t *testing.T, last, tip uint64) []byte {
	t.Helper()
	encoded, err := sessionwire.SessionReset{
		TenantID: tenant, SessionID: session, LastContiguous: last, JournalTip: tip,
	}.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal reset: %v", err)
	}
	return encoded
}

func enduring(t *testing.T, seq uint64) delivery.Record {
	return delivery.Record{Class: delivery.ClassEnduring, Encoded: enduringBytes(t, seq, `{"n":1}`), Seq: seq}
}

func ephemeral(t *testing.T, key, body string) delivery.Record {
	return delivery.Record{Class: delivery.ClassEphemeral, Encoded: ephemeralBytes(t, body), CoalesceKey: key}
}

func control(t *testing.T, last, tip uint64) delivery.Record {
	return delivery.Record{Class: delivery.ClassControl, Encoded: resetBytes(t, last, tip)}
}

func newQueue(t *testing.T, capacity int) *delivery.Queue {
	t.Helper()
	q, err := delivery.NewQueue(capacity)
	if err != nil {
		t.Fatalf("NewQueue(%d): %v", capacity, err)
	}
	return q
}

// drain empties the queue and returns each record's body as a comparable
// string, which is what makes "which record survived" an assertion rather than
// a pointer comparison.
func drain(q *delivery.Queue) []string {
	var out []string
	for {
		record, ok := q.Next()
		if !ok {
			return out
		}
		out = append(out, string(record.Encoded))
	}
}

func mustEnqueue(t *testing.T, q *delivery.Queue, records ...delivery.Record) {
	t.Helper()
	for i, record := range records {
		if err := q.Enqueue(record); err != nil {
			t.Fatalf("Enqueue #%d: %v", i, err)
		}
	}
}

// ---------------------------------------------------------------------------
// Axis B: what a full buffer holds decides whether there is a victim at all.
// ---------------------------------------------------------------------------

// TestAFullQueueOfEnduringRecordsOverflowsRatherThanDiscardingOne is the
// inversion itself, and the assertion is deliberately in TWO halves: the
// refusal, and the buffer being UNCHANGED by it. A queue that reported the
// overflow and dropped the record anyway would satisfy the first alone, and
// that is the exact defect wui had -- a receipt attached to a silent loss.
func TestAFullQueueOfEnduringRecordsOverflowsRatherThanDiscardingOne(t *testing.T) {
	q := newQueue(t, 3)
	mustEnqueue(t, q, enduring(t, 1), enduring(t, 2), enduring(t, 3))

	before := q.Len()
	err := q.Enqueue(enduring(t, 4))
	if !errors.Is(err, delivery.ErrOverflow) {
		t.Fatalf("Enqueue into a full enduring buffer = %v, want ErrOverflow", err)
	}
	if q.Len() != before {
		t.Fatalf("the refused enqueue changed the buffer: len %d -> %d", before, q.Len())
	}
	got := drain(q)
	want := []string{string(enduringBytes(t, 1, `{"n":1}`)), string(enduringBytes(t, 2, `{"n":1}`)), string(enduringBytes(t, 3, `{"n":1}`))}
	if len(got) != len(want) {
		t.Fatalf("drained %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("record %d = %s, want %s", i, got[i], want[i])
		}
	}
}

// TestAFullQueueOfControlRecordsOverflowsToo holds the other half of the
// non-droppable domain. A control is a repair instruction: discarding one
// leaves a consumer that will never be told to repair, which is the same
// silent loss with an extra step.
func TestAFullQueueOfControlRecordsOverflowsToo(t *testing.T) {
	q := newQueue(t, 2)
	mustEnqueue(t, q, control(t, 1, 9), control(t, 2, 9))
	if err := q.Enqueue(enduring(t, 3)); !errors.Is(err, delivery.ErrOverflow) {
		t.Fatalf("Enqueue into a full control buffer = %v, want ErrOverflow", err)
	}
	if q.Len() != 2 {
		t.Fatalf("len = %d, want 2", q.Len())
	}
}

// TestAFullQueueDiscardsTheOldestEphemeralBeforeAnyEnduringRecord is the DROP
// arm of step 2, and it is tested on a buffer that also holds enduring data so
// that "the ephemeral policy chose correctly" is distinguishable from "there
// was nothing else to choose".
func TestAFullQueueDiscardsTheOldestEphemeralBeforeAnyEnduringRecord(t *testing.T) {
	q := newQueue(t, 4)
	mustEnqueue(t, q,
		enduring(t, 1),
		ephemeral(t, "", `{"d":"first"}`),
		ephemeral(t, "", `{"d":"second"}`),
		enduring(t, 2),
	)
	mustEnqueue(t, q, enduring(t, 3))

	got := drain(q)
	want := []string{
		string(enduringBytes(t, 1, `{"n":1}`)),
		string(ephemeralBytes(t, `{"d":"second"}`)),
		string(enduringBytes(t, 2, `{"n":1}`)),
		string(enduringBytes(t, 3, `{"n":1}`)),
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("after eviction the queue holds\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

// TestAFullQueueCoalescesTheBusiestKeyAndLeavesTheQuietOneAlone is the COALESCE
// arm, and the quiet key is the positive control: a policy that simply dropped
// the oldest ephemeral would take the quiet key's only delta, so the assertion
// fails in a way that names which rule was applied rather than merely observing
// that something was dropped.
func TestAFullQueueCoalescesTheBusiestKeyAndLeavesTheQuietOneAlone(t *testing.T) {
	q := newQueue(t, 4)
	mustEnqueue(t, q,
		ephemeral(t, "quiet", `{"d":"quiet-only"}`),
		ephemeral(t, "busy", `{"d":"busy-1"}`),
		ephemeral(t, "busy", `{"d":"busy-2"}`),
		enduring(t, 1),
	)
	mustEnqueue(t, q, enduring(t, 2))

	got := drain(q)
	want := []string{
		string(ephemeralBytes(t, `{"d":"quiet-only"}`)),
		string(ephemeralBytes(t, `{"d":"busy-2"}`)),
		string(enduringBytes(t, 1, `{"n":1}`)),
		string(enduringBytes(t, 2, `{"n":1}`)),
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("after coalescing the queue holds\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

// TestTwoEquallyBusyKeysAreSeparatedByAge, so the policy is a function of the
// buffer and not of map iteration order. Without this the previous case passes
// whichever bucket a range happened to visit first.
func TestTwoEquallyBusyKeysAreSeparatedByAge(t *testing.T) {
	for attempt := range 32 {
		q := newQueue(t, 4)
		mustEnqueue(t, q,
			ephemeral(t, "older", `{"d":"older-1"}`),
			ephemeral(t, "newer", `{"d":"newer-1"}`),
			ephemeral(t, "older", `{"d":"older-2"}`),
			ephemeral(t, "newer", `{"d":"newer-2"}`),
		)
		mustEnqueue(t, q, enduring(t, 1))
		got := drain(q)
		if len(got) != 4 {
			t.Fatalf("attempt %d: drained %d records, want 4", attempt, len(got))
		}
		if got[0] != string(ephemeralBytes(t, `{"d":"newer-1"}`)) {
			t.Fatalf("attempt %d: the surviving head is %s, want the newer bucket's first member: "+
				"two equally busy keys must be separated by the age of their oldest member", attempt, got[0])
		}
	}
}

// TestAnArrivingEphemeralIsDroppedRatherThanRepairingADurableQueue. An
// ephemeral is best effort by construction, so a buffer with no victim must
// discard the ARRIVAL rather than report an overflow -- reporting one would
// make a token stream trigger a durable repair of a queue that lost nothing.
func TestAnArrivingEphemeralIsDroppedRatherThanRepairingADurableQueue(t *testing.T) {
	q := newQueue(t, 2)
	mustEnqueue(t, q, enduring(t, 1), enduring(t, 2))

	err := q.Enqueue(ephemeral(t, "k", `{"d":"late"}`))
	if !errors.Is(err, delivery.ErrDropped) {
		t.Fatalf("Enqueue of an ephemeral into a full durable buffer = %v, want ErrDropped", err)
	}
	if errors.Is(err, delivery.ErrOverflow) {
		t.Fatal("a dropped ephemeral must not be in the overflow class: it would repair a queue that lost nothing durable")
	}
	if q.Len() != 2 {
		t.Fatalf("len = %d, want 2", q.Len())
	}
}

// ---------------------------------------------------------------------------
// The bound itself.
// ---------------------------------------------------------------------------

// TestTheConfiguredCapacityIsTheNumberActuallyApplied drives TWO off-default
// capacities and asserts the overflow point against ABSOLUTE literals. A
// capacity every call site passes identically is untested by construction; two
// different values with two different answers is what makes the parameter
// observable at all.
func TestTheConfiguredCapacityIsTheNumberActuallyApplied(t *testing.T) {
	for _, capacity := range []int{1, 3, 7} {
		t.Run(fmt.Sprintf("capacity_%d", capacity), func(t *testing.T) {
			q := newQueue(t, capacity)
			if q.Capacity() != capacity {
				t.Fatalf("Capacity() = %d, want %d", q.Capacity(), capacity)
			}
			for i := range capacity {
				if err := q.Enqueue(enduring(t, uint64(i+1))); err != nil {
					t.Fatalf("record %d of %d was refused: %v", i+1, capacity, err)
				}
			}
			if err := q.Enqueue(enduring(t, uint64(capacity+1))); !errors.Is(err, delivery.ErrOverflow) {
				t.Fatalf("record %d = %v, want ErrOverflow", capacity+1, err)
			}
		})
	}
}

func TestNewQueueRefusesANonPositiveCapacity(t *testing.T) {
	for _, capacity := range []int{0, -1} {
		if _, err := delivery.NewQueue(capacity); !errors.Is(err, delivery.ErrInvalidConfig) {
			t.Errorf("NewQueue(%d) = %v, want ErrInvalidConfig", capacity, err)
		}
	}
}

func TestRecordsLeaveInArrivalOrder(t *testing.T) {
	q := newQueue(t, 8)
	mustEnqueue(t, q, enduring(t, 1), ephemeral(t, "k", `{"d":"a"}`), control(t, 1, 4), enduring(t, 2))
	got := drain(q)
	want := []string{
		string(enduringBytes(t, 1, `{"n":1}`)),
		string(ephemeralBytes(t, `{"d":"a"}`)),
		string(resetBytes(t, 1, 4)),
		string(enduringBytes(t, 2, `{"n":1}`)),
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("drained\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

// ---------------------------------------------------------------------------
// Repair.
// ---------------------------------------------------------------------------

// TestRepairDiscardsTheBufferUnappliedAndQueuesTheControl. "Unapplied" is the
// load-bearing word: the discarded records are ones the consumer never saw, so
// the control that replaces them must name a sequence from what was DELIVERED,
// which is the caller's to supply and is asserted in internal/routing.
func TestRepairDiscardsTheBufferUnappliedAndQueuesTheControl(t *testing.T) {
	q := newQueue(t, 3)
	mustEnqueue(t, q, enduring(t, 1), ephemeral(t, "k", `{"d":"a"}`), enduring(t, 2))

	if err := q.Repair(control(t, 7, 9)); err != nil {
		t.Fatalf("Repair: %v", err)
	}
	got := drain(q)
	if len(got) != 1 || got[0] != string(resetBytes(t, 7, 9)) {
		t.Fatalf("after Repair the queue holds %v, want exactly the control record", got)
	}
}

// TestASecondRepairSupersedesThePendingControlRatherThanQueueingBoth. This is
// the queue's half of "repeatable": two resets in a row are not two
// instructions, they are one instruction restated, and a queue that appended
// would make a flapping consumer pay a control record per episode.
func TestASecondRepairSupersedesThePendingControlRatherThanQueueingBoth(t *testing.T) {
	q := newQueue(t, 4)
	if err := q.Repair(control(t, 3, 9)); err != nil {
		t.Fatalf("first Repair: %v", err)
	}
	mustEnqueue(t, q, enduring(t, 10))
	if err := q.Repair(control(t, 3, 12)); err != nil {
		t.Fatalf("second Repair: %v", err)
	}
	got := drain(q)
	if len(got) != 1 || got[0] != string(resetBytes(t, 3, 12)) {
		t.Fatalf("after two repairs the queue holds %v, want exactly the second control", got)
	}
}

// TestAClosedQueueRefusesEveryKindOfWork covers the FIRST of Close's two
// statements, and it is deliberately not the whole of Close. See the case below
// for the second, and the note there for why they are separate.
func TestAClosedQueueRefusesEveryKindOfWork(t *testing.T) {
	q := newQueue(t, 4)
	mustEnqueue(t, q, enduring(t, 1))
	q.Close()

	if err := q.Enqueue(enduring(t, 2)); !errors.Is(err, delivery.ErrQueueClosed) {
		t.Errorf("Enqueue after Close = %v, want ErrQueueClosed", err)
	}
	if err := q.Repair(control(t, 1, 2)); !errors.Is(err, delivery.ErrQueueClosed) {
		t.Errorf("Repair after Close = %v, want ErrQueueClosed", err)
	}
	if _, ok := q.Next(); ok {
		t.Error("a closed queue handed out a record; its consumer is gone")
	}
	if _, ok := q.Head(); ok {
		t.Error("a closed queue offered a record; its consumer is gone")
	}
}

// TestClosingAQueueReleasesWhatItWasHolding is Close's SECOND statement, and it
// needs a case of its own because the first one hides it.
//
// Close does two things -- q.closed = true and q.buf = nil -- and every refusal
// the case above drives is answered by the FIRST alone. Deleting the second
// survived the whole module: a queue that refuses all later work looks closed
// from every entry point that guards on q.closed, and Len is the only method
// that does not. So the two statements need two assertions, and signing Close
// off on its refusals is signing off half a function.
//
// The statement is not decoration. A closed queue's consumer is gone, so its
// backlog has no destination; retaining it holds every buffered record alive
// until the binding itself is dropped, and Len would keep reporting a depth for
// a queue nobody can read. Neither is a defect a caller can see today, which is
// exactly why it needed an assertion rather than a reviewer.
func TestClosingAQueueReleasesWhatItWasHolding(t *testing.T) {
	q := newQueue(t, 4)
	mustEnqueue(t, q, enduring(t, 1), ephemeral(t, "k", `{"d":"a"}`), control(t, 1, 2))
	if q.Len() != 3 {
		t.Fatalf("len before Close = %d, want 3: the queue must be HOLDING something for this to measure anything", q.Len())
	}

	q.Close()

	if got := q.Len(); got != 0 {
		t.Fatalf("len after Close = %d, want 0: a closed queue's consumer is gone, so its backlog "+
			"has no destination and must not be retained", got)
	}
}

// ---------------------------------------------------------------------------
// Step 5: the envelope is parsed, the body is not.
// ---------------------------------------------------------------------------

// shuffled is one enduring record written with its members in an order Core's
// own marshaller does not use. It is the positive control for byte
// preservation: a path that re-encoded the record would produce Core's order,
// so an assertion of byte identity against THIS spelling fails where an
// assertion against a canonically-ordered fixture would pass either way.
const shuffled = `{"body":{"n":1},"covered_through":7,"journal_seq":7,"event_id":"event-7",` +
	`"session_id":"session-a","tenant_id":"tenant-a","type":"enduring_publication"}`

func TestForwardingPreservesTheExactBytesItReceived(t *testing.T) {
	canonical := enduringBytes(t, 7, `{"n":1}`)
	if string(canonical) == shuffled {
		t.Fatal("the fixture is in Core's own member order, so byte identity would hold for a re-encoding path too")
	}

	parsed, err := delivery.ParseEnduring([]byte(shuffled), 7)
	if err != nil {
		t.Fatalf("ParseEnduring: %v", err)
	}
	if string(parsed.Encoded) != shuffled {
		t.Errorf("Encoded = %s, want the received bytes %s", parsed.Encoded, shuffled)
	}
	if string(parsed.Body) != `{"n":1}` {
		t.Errorf("Body = %s, want the canonical body bytes", parsed.Body)
	}
	if parsed.EventID != "event-7" || parsed.JournalSeq != 7 || parsed.CoveredThrough != 7 {
		t.Errorf("envelope = %q/%d/%d, want event-7/7/7", parsed.EventID, parsed.JournalSeq, parsed.CoveredThrough)
	}
	if parsed.TenantID != tenant || parsed.SessionID != session {
		t.Errorf("scope = %q/%q, want %q/%q", parsed.TenantID, parsed.SessionID, tenant, session)
	}
	if record := parsed.Record(); record.Class != delivery.ClassEnduring ||
		string(record.Encoded) != shuffled || record.Seq != 7 {
		t.Errorf("Record() = %v/%s/%d, want ClassEnduring/the received bytes/7", record.Class, record.Encoded, record.Seq)
	}
}

// TestTheWatermarkBoundsAreCheckedAtTheirExactValues mutates the VALUE at each
// boundary rather than the mechanism, in both directions, because a bound that
// is one off in either direction is exactly the defect the pair exists to stop
// and is invisible to a fixture sitting in the middle of the range.
//
// The lower bound is CORE's: EnduringPublication.Validate requires
// covered_through to EQUAL journal_seq, with its own stated reason -- a live
// publication covers exactly the sequence it committed, and later private
// records can only be covered by an authenticated journal page. This package
// does not restate that rule, so there is no second authority for it here; what
// it does is refuse to forward a record Core refuses to decode. The upper bound
// is this package's, and the two quantities are different: one is the record's
// own sequence, the other is what the Host says it has durably appended.
func TestTheWatermarkBoundsAreCheckedAtTheirExactValues(t *testing.T) {
	const seq = 7
	body := `{"n":1}`
	raw := func(covered uint64) []byte {
		return fmt.Appendf(nil, `{"type":"enduring_publication","tenant_id":%q,"session_id":%q,`+
			`"event_id":"event-7","journal_seq":%d,"covered_through":%d,"body":%s}`,
			tenant, session, seq, covered, body)
	}

	for _, tc := range []struct {
		name            string
		covered         uint64
		committedAppend uint64
		wantErr         error
	}{
		{"one below its own event sequence", seq - 1, 100, delivery.ErrWatermarkNotItsEventSequence},
		{"exactly at its own event sequence", seq, 100, nil},
		{"exactly at the host's committed append", seq, seq, nil},
		{"one above the host's committed append", seq, seq - 1, delivery.ErrWatermarkAboveCommitted},
		{"far above the host's committed append", seq, 0, delivery.ErrWatermarkAboveCommitted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := delivery.ParseEnduring(raw(tc.covered), tc.committedAppend)
			switch {
			case tc.wantErr == nil && err != nil:
				t.Fatalf("ParseEnduring(covered=%d, committed=%d) = %v, want acceptance", tc.covered, tc.committedAppend, err)
			case tc.wantErr != nil && !errors.Is(err, tc.wantErr):
				t.Fatalf("ParseEnduring(covered=%d, committed=%d) = %v, want %v", tc.covered, tc.committedAppend, err, tc.wantErr)
			}
		})
	}
}

// TestOnlyAnEnduringPublicationIsParsedAsOne. The discriminator is Core's, and
// dispatching on it is what keeps an unsequenced delta from being queued as
// durable data.
func TestOnlyAnEnduringPublicationIsParsedAsOne(t *testing.T) {
	tip, err := sessionwire.JournalTip{TenantID: tenant, SessionID: session, Tip: 4}.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal tip: %v", err)
	}
	for _, tc := range []struct{ name, encoded string }{
		{"an ephemeral publication", string(ephemeralBytes(t, `{"d":"a"}`))},
		{"a journal tip hint", string(tip)},
		{"a session reset control", string(resetBytes(t, 1, 2))},
		{"an unknown record type", `{"type":"something_else","tenant_id":"tenant-a","session_id":"session-a"}`},
		{"no discriminator at all", `{"tenant_id":"tenant-a","session_id":"session-a"}`},
		{"not JSON", `nonsense`},
		{"empty", ``},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := delivery.ParseEnduring([]byte(tc.encoded), 1000); err == nil {
				t.Fatalf("ParseEnduring(%s) was accepted as an enduring publication", tc.name)
			}
		})
	}
}

// FuzzParsingNeverInventsOrLosesBodyBytes derives the space the two fixed
// fixtures above only sample: an arbitrary body and an arbitrary sequence, with
// the property being that what comes out is a SUBSLICE-equal copy of what went
// in and that the record forwarded is byte-identical to the record received.
func FuzzParsingNeverInventsOrLosesBodyBytes(f *testing.F) {
	f.Add("hello", uint64(1), uint64(1))
	f.Add("", uint64(1), uint64(0))
	f.Add("  <>&", uint64(18446744073709551615), uint64(18446744073709551615))
	f.Add("\"quoted\"", uint64(2), uint64(1))
	f.Add("a", uint64(0), uint64(5))

	f.Fuzz(func(t *testing.T, text string, seq, committed uint64) {
		bodyBytes, err := json.Marshal(text)
		if err != nil {
			t.Skip()
		}
		encoded := fmt.Appendf(nil, `{"type":"enduring_publication","tenant_id":%q,"session_id":%q,`+
			`"event_id":"event-x","journal_seq":%d,"covered_through":%d,"body":%s}`,
			tenant, session, seq, seq, bodyBytes)

		parsed, err := delivery.ParseEnduring(encoded, committed)
		if err != nil {
			// The only refusals this shape can produce are the zero sequence
			// Core rejects and the upper watermark bound. Anything else is a
			// finding.
			if seq == 0 || seq > committed {
				return
			}
			t.Fatalf("ParseEnduring(seq=%d, committed=%d) = %v, want acceptance", seq, committed, err)
		}
		if seq > committed {
			t.Fatalf("seq %d above committed append %d was accepted", seq, committed)
		}
		if !bytes.Equal(parsed.Encoded, encoded) {
			t.Fatalf("forwarded bytes differ from the received bytes:\n got %s\nwant %s", parsed.Encoded, encoded)
		}
		if !bytes.Equal(parsed.Body, bodyBytes) {
			t.Fatalf("body = %s, want %s", parsed.Body, bodyBytes)
		}
		if parsed.JournalSeq != seq || parsed.CoveredThrough != seq {
			t.Fatalf("envelope sequence = %d/%d, want %d", parsed.JournalSeq, parsed.CoveredThrough, seq)
		}
	})
}

// TestAMalformedRecordIsNotReportedAsAWatermarkViolation is the classification's
// positive control. Core answers both with the same typed error, distinguished
// only by the field it names, so a parse that wrapped every decode failure in
// the watermark sentinel would report a mistyped identifier as a coverage claim
// -- and a caller branching on the watermark to fence a Host would fence it for
// a typo.
func TestAMalformedRecordIsNotReportedAsAWatermarkViolation(t *testing.T) {
	for _, tc := range []struct{ name, encoded string }{
		{"no event id", `{"type":"enduring_publication","tenant_id":"tenant-a","session_id":"session-a",` +
			`"journal_seq":7,"covered_through":7,"body":{"n":1}}`},
		{"a zero journal sequence", `{"type":"enduring_publication","tenant_id":"tenant-a","session_id":"session-a",` +
			`"event_id":"event-7","journal_seq":0,"covered_through":0,"body":{"n":1}}`},
		{"an unusable tenant", `{"type":"enduring_publication","tenant_id":"","session_id":"session-a",` +
			`"event_id":"event-7","journal_seq":7,"covered_through":7,"body":{"n":1}}`},
		{"a non-canonical body", `{"type":"enduring_publication","tenant_id":"tenant-a","session_id":"session-a",` +
			`"event_id":"event-7","journal_seq":7,"covered_through":7,"body":"a<b"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := delivery.ParseEnduring([]byte(tc.encoded), 1000)
			if !errors.Is(err, delivery.ErrMalformed) {
				t.Fatalf("ParseEnduring = %v, want ErrMalformed", err)
			}
			if errors.Is(err, delivery.ErrWatermarkNotItsEventSequence) || errors.Is(err, delivery.ErrWatermarkAboveCommitted) {
				t.Fatalf("a malformed record was classified as a watermark violation: %v", err)
			}
		})
	}
	// The control: the watermark refusal is still classified as one, so the
	// case above cannot pass by nothing ever being a watermark violation.
	_, err := delivery.ParseEnduring([]byte(`{"type":"enduring_publication","tenant_id":"tenant-a",`+
		`"session_id":"session-a","event_id":"event-7","journal_seq":7,"covered_through":6,"body":{"n":1}}`), 1000)
	if !errors.Is(err, delivery.ErrWatermarkNotItsEventSequence) || errors.Is(err, delivery.ErrMalformed) {
		t.Fatalf("the control watermark refusal = %v, want ErrWatermarkNotItsEventSequence and not ErrMalformed", err)
	}
}

// TestHeadLeavesTheRecordQueuedAndNextTakesIt. A transport that cannot take a
// record must leave it where it is, and the two halves are asserted together
// because a Head that also removed would pass any assertion about what it
// returned.
func TestHeadLeavesTheRecordQueuedAndNextTakesIt(t *testing.T) {
	q := newQueue(t, 4)
	mustEnqueue(t, q, enduring(t, 1), enduring(t, 2))

	first, ok := q.Head()
	if !ok || string(first.Encoded) != string(enduringBytes(t, 1, `{"n":1}`)) {
		t.Fatalf("Head = %s/%v, want record 1", first.Encoded, ok)
	}
	if q.Len() != 2 {
		t.Fatalf("Head removed a record: len = %d, want 2", q.Len())
	}
	again, _ := q.Head()
	if string(again.Encoded) != string(first.Encoded) {
		t.Fatal("two Heads returned different records")
	}
	taken, _ := q.Next()
	if string(taken.Encoded) != string(first.Encoded) || q.Len() != 1 {
		t.Fatalf("Next took %s leaving %d, want record 1 leaving 1", taken.Encoded, q.Len())
	}
}

// TestClearDiscardsTheBufferAndQueuesNothing is Repair's sibling for the queue
// whose consumer is not a client. The distinction is asserted rather than
// described: a Clear that behaved like Repair would need a control record it is
// not given, and one that behaved like Close would refuse later work.
func TestClearDiscardsTheBufferAndQueuesNothing(t *testing.T) {
	q := newQueue(t, 4)
	mustEnqueue(t, q, enduring(t, 1), ephemeral(t, "k", `{"d":"a"}`))
	q.Clear()

	if q.Len() != 0 {
		t.Fatalf("len after Clear = %d, want 0", q.Len())
	}
	if _, ok := q.Next(); ok {
		t.Fatal("Clear left a record behind")
	}
	if err := q.Enqueue(enduring(t, 2)); err != nil {
		t.Fatalf("Enqueue after Clear = %v, want acceptance: Clear is not Close", err)
	}
}

// TestAClosedQueueIsNotClearedIntoAUsableOne. Close empties AND refuses; a
// Clear afterwards must not quietly reopen the buffer for a consumer that is
// already gone.
func TestAClosedQueueIsNotClearedIntoAUsableOne(t *testing.T) {
	q := newQueue(t, 4)
	q.Close()
	q.Clear()
	if err := q.Enqueue(enduring(t, 1)); !errors.Is(err, delivery.ErrQueueClosed) {
		t.Fatalf("Enqueue after Close+Clear = %v, want ErrQueueClosed", err)
	}
}
