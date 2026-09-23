package routing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
)

// These cases are I3.1's D2, reduced to the relay. A viewer on a replica that
// did not place the session watched it through a tail that went live inside
// its own subscribe; the Host warm-released the session, committing
// SessionResidencyReleased AFTER its tail stopped relaying, so that public
// record never travelled live; another replica re-placed the session and the
// new tail's records reached this relay on the same subscription. The relay
// relayed them as a continuation, so the viewer got E8.. with nothing in front
// and a later reset vouched for the hole. It happened on the first record of a
// binding (cycle 1 of the reproduction) and again mid-stream (cycle 3).
//
// Once the tail's position is KNOWN -- anchored at a first viewer's tail start,
// or at the tip a reset sent every viewer to -- a record past it is probed
// against the journal: a skipped PUBLIC record puts a reset in front; a
// skipped PRIVATE one (ordinary: a Host relays public records only) does not.

// gapJournal is a journal with a tip and a set of public sequences; every
// other position up to the tip is private. It answers a tail read with the tip
// and a positioned read with the first public event at or after FromSeq
// within ScanLimit, as SessionStore does.
type gapJournal struct {
	mu       sync.Mutex
	tip      uint64
	public   map[uint64]bool
	err      error
	failNext int
	requests []sessionstore.ReadPublicJournalRequest
}

func newGapJournal(tip uint64, public ...uint64) *gapJournal {
	j := &gapJournal{tip: tip, public: map[uint64]bool{}}
	for _, seq := range public {
		j.public[seq] = true
	}
	return j
}

func (j *gapJournal) set(tip uint64, public ...uint64) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.tip = tip
	for _, seq := range public {
		j.public[seq] = true
	}
}

func (j *gapJournal) ReadPublicJournal(_ context.Context, req sessionstore.ReadPublicJournalRequest) (sessionwire.JournalPage, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.requests = append(j.requests, req)
	if j.err != nil {
		return sessionwire.JournalPage{}, j.err
	}
	if j.failNext > 0 {
		j.failNext--
		return sessionwire.JournalPage{}, errors.New("a transient read failure")
	}
	if req.Tail {
		return sessionwire.JournalPage{CapturedTip: j.tip, CoveredThrough: j.tip}, nil
	}
	end := j.tip
	if req.ScanLimit > 0 && req.FromSeq+uint64(req.ScanLimit)-1 < end {
		end = req.FromSeq + uint64(req.ScanLimit) - 1
	}
	for seq := req.FromSeq; seq <= end; seq++ {
		if j.public[seq] {
			return sessionwire.JournalPage{CapturedTip: j.tip, CoveredThrough: seq, Events: []sessionwire.JournalEvent{
				{EventID: sessionwire.EventID(fmt.Sprintf("e-%d", seq)), JournalSeq: seq, Body: json.RawMessage(`{}`)},
			}}, nil
		}
	}
	return sessionwire.JournalPage{CapturedTip: j.tip, CoveredThrough: end}, nil
}

func (j *gapJournal) positioned() []sessionstore.ReadPublicJournalRequest {
	j.mu.Lock()
	defer j.mu.Unlock()
	var out []sessionstore.ReadPublicJournalRequest
	for _, req := range j.requests {
		if !req.Tail {
			out = append(out, req)
		}
	}
	return out
}

func gapFixture(t *testing.T, journal *gapJournal) *relayFixture {
	t.Helper()
	tail := &recordingTail{}
	rebinder := &recordingRebinder{order: tail}
	publisher := newPublisher()
	relay, err := NewRelay(journal, rebinder, tail, publisher, RepairLimits{HostBindingQueue: 16, DeliveryBindingQueue: 16})
	if err != nil {
		t.Fatalf("NewRelay: %v", err)
	}
	f := &relayFixture{t: t, relay: relay, tail: tail, pub: publisher, rebinder: rebinder}
	f.open(bindSession, linkA)
	return f
}

func seqs(from, to uint64) []uint64 {
	var out []uint64
	for seq := from; seq <= to; seq++ {
		out = append(out, seq)
	}
	return out
}

// streamOf renders what one link was published as E<seq> / R<last>/<tip>.
func streamOf(t *testing.T, records []string) string {
	t.Helper()
	out := ""
	for _, raw := range records {
		kind, err := sessionwire.SessionRecordTypeOf([]byte(raw))
		if err != nil {
			t.Fatal(err)
		}
		switch kind {
		case sessionwire.SessionRecordTypeSessionReset:
			var reset sessionwire.SessionReset
			if err := reset.UnmarshalJSON([]byte(raw)); err != nil {
				t.Fatal(err)
			}
			out += fmt.Sprintf("R%d/%d ", reset.LastContiguous, reset.JournalTip)
		case sessionwire.SessionRecordTypeEnduringPublication:
			var publication sessionwire.EnduringPublication
			if err := publication.UnmarshalJSON([]byte(raw)); err != nil {
				t.Fatal(err)
			}
			out += fmt.Sprintf("E%d ", publication.JournalSeq)
		}
	}
	return out
}

func anchor(t *testing.T, f *relayFixture) {
	t.Helper()
	if err := f.relay.Anchor(context.Background(), bindTenant, bindSession); err != nil {
		t.Fatalf("Anchor: %v", err)
	}
}

func TestARecordPastAnAnchoredTailsSkippedPublicRecordIsPrecededByAReset(t *testing.T) {
	journal := newGapJournal(6, seqs(1, 6)...)
	f := gapFixture(t, journal)
	anchor(t, f)
	// The release (7) is committed and never relayed; the new tail carries 8..13.
	journal.set(13, seqs(7, 13)...)
	f.mustFeed(bindSession, seqs(8, 13)...)

	if got, want := streamOf(t, f.pub.to(linkA)), "R0/13 E8 E9 E10 E11 E12 E13 "; got != want {
		t.Fatalf("the viewer received %q, want %q", got, want)
	}
	if last, _ := f.relay.LastContiguous(bindTenant, bindSession, linkA); last != 13 {
		t.Fatalf("last contiguous = %d, want 13", last)
	}
}

// Cycle 3 of the reproduction: the tail was already streaming when a public
// record went unrelayed.
func TestAPublicRecordSkippedMidStreamIsPrecededByAReset(t *testing.T) {
	journal := newGapJournal(6, seqs(1, 6)...)
	f := gapFixture(t, journal)
	anchor(t, f)
	journal.set(8, 7, 8)
	f.mustFeed(bindSession, 7, 8)
	journal.set(11, 9, 10, 11)
	f.mustFeed(bindSession, 10, 11)

	if got, want := streamOf(t, f.pub.to(linkA)), "E7 E8 R8/11 E10 E11 "; got != want {
		t.Fatalf("the viewer received %q, want %q", got, want)
	}
}

// A Host relays public records only, so a tail that skips a PRIVATE position
// is whole; the probe says so, and nothing is reset.
func TestASkippedPrivatePositionNeedsNoReset(t *testing.T) {
	journal := newGapJournal(6, seqs(1, 6)...)
	f := gapFixture(t, journal)
	anchor(t, f)
	journal.set(10, 8, 10) // 7 and 9 are private
	f.mustFeed(bindSession, 8, 10)

	if got, want := streamOf(t, f.pub.to(linkA)), "E8 E10 "; got != want {
		t.Fatalf("the viewer received %q, want %q", got, want)
	}
	probes := journal.positioned()
	if len(probes) != 2 || probes[0].FromSeq != 7 || probes[0].Limit != 1 || probes[0].ScanLimit != 1 ||
		probes[1].FromSeq != 9 || probes[1].ScanLimit != 1 {
		t.Fatalf("probes %+v, want one bounded probe per skipped span", probes)
	}
}

func TestAContiguousTailIsNeverProbed(t *testing.T) {
	journal := newGapJournal(6, seqs(1, 9)...)
	f := gapFixture(t, journal)
	anchor(t, f)
	f.mustFeed(bindSession, 5, 6, 7, 8, 9)
	if got, want := streamOf(t, f.pub.to(linkA)), "E5 E6 E7 E8 E9 "; got != want {
		t.Fatalf("the viewer received %q, want %q", got, want)
	}
	if probes := journal.positioned(); len(probes) != 0 {
		t.Fatalf("probed %+v, want none", probes)
	}
}

// A tail with no known position keeps the original rule: nothing is probed.
func TestAnUnanchoredTailIsNotProbed(t *testing.T) {
	journal := newGapJournal(99, 41, 42, 45)
	f := gapFixture(t, journal)
	f.mustFeed(bindSession, 41, 42, 45)
	if probes := journal.positioned(); len(probes) != 0 {
		t.Fatalf("probed %+v, want none", probes)
	}
}

// A reset anchors the tail at the tip it sent every viewer to.
func TestAResetAnchorsTheTailAtItsTip(t *testing.T) {
	journal := newGapJournal(5, seqs(1, 5)...)
	f := gapFixture(t, journal)
	if err := f.relay.Resync(context.Background(), bindTenant, bindSession); err != nil {
		t.Fatalf("Resync: %v", err)
	}
	journal.set(8, 6, 7, 8)
	f.mustFeed(bindSession, 8)
	// The gap reset supersedes the resync's unpublished R0/5: one instruction,
	// restated, now reaching 8.
	if got, want := streamOf(t, f.pub.to(linkA)), "R0/8 E8 "; got != want {
		t.Fatalf("the viewer received %q, want %q", got, want)
	}
}

// A probe whose tip does not reach the skipping record's predecessor cannot
// describe the hole: every viewer is closed rather than streamed past it.
func TestAGapTheTipCannotDescribeClosesTheViewers(t *testing.T) {
	journal := newGapJournal(6, seqs(1, 6)...)
	f := gapFixture(t, journal)
	anchor(t, f)
	// A lagging read plane: the tip is still 6 when E8 arrives, and nothing
	// covers 7, so the probe answers a hole it cannot bound.
	_ = f.feed(bindSession, 8)
	if closed := f.pub.closedLinks(); len(closed) != 1 || closed[0] != linkA {
		t.Fatalf("closed %v, want linkA", closed)
	}
	if got := streamOf(t, f.pub.to(linkA)); got != "" {
		t.Fatalf("published %q past an unrepairable gap", got)
	}
}

// A probe that fails is a hole; its reset takes a fresh tip, and a tip that
// cannot be read either closes the viewers.
func TestAFailedProbeIsAHole(t *testing.T) {
	journal := newGapJournal(6, seqs(1, 6)...)
	f := gapFixture(t, journal)
	anchor(t, f)
	journal.mu.Lock()
	journal.err = errors.New("store unavailable")
	journal.mu.Unlock()
	_ = f.feed(bindSession, 8)
	if closed := f.pub.closedLinks(); len(closed) != 1 {
		t.Fatalf("closed %v, want the viewer closed", closed)
	}
}

// A probe that fails transiently is still a hole, and the reset takes a fresh
// tip read: the viewers are reset, not closed.
func TestAFailedProbeIsResetFromAFreshTip(t *testing.T) {
	journal := newGapJournal(6, seqs(1, 6)...)
	f := gapFixture(t, journal)
	anchor(t, f)
	journal.set(8, 7, 8)
	journal.mu.Lock()
	journal.failNext = 1
	journal.mu.Unlock()
	f.mustFeed(bindSession, 8)
	if got, want := streamOf(t, f.pub.to(linkA)), "R0/8 E8 "; got != want {
		t.Fatalf("the viewer received %q, want %q", got, want)
	}
}
