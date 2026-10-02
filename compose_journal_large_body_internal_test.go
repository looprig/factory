package factory

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage/memstore"
)

// largeBodyWorld is hostSessionWorld with, after its three inline events, an
// offloaded public event whose body alone exceeds SessionStore's page budget
// and one small event after it -- both committed beneath the store, as
// Harness's journal commits an offloaded body. It returns every public event's
// sequence in order, the large event's index in seqs, and its body.
func largeBodyWorld(t *testing.T) (control, runtime *sessionstore.Store, ledger *namingLedger, seqs []uint64, large int, body []byte) {
	t.Helper()
	backend := memstore.New()
	ledger = &namingLedger{Ledger: backend.Ledger}
	backend.Ledger = ledger
	control, runtime, seqs, _ = hostSessionWorldOver(t, backend)
	seq, body := appendOffloadedPublicBody(t, runtime, ledger, "event-4", sessionstore.DefaultJournalPageBytes+1024)
	large = len(seqs)
	seqs = append(seqs, seq)
	seqs = append(seqs, appendBeneath(t, ledger, sessionstore.Envelope{
		Kind: sessionstore.EnvelopeKindPublicEvent, EventID: "event-5",
		Public: sessionstore.BodySlot{Inline: []byte(`{"n":5}`)},
	}))
	return control, runtime, ledger, seqs, large, body
}

// TestAHostSessionsJournalServesAnOffloadedEventAloneOnAPage: a cold reader
// walking /journal from the first record crosses a public event whose
// offloaded body (~1 MiB) alone exceeds the page budget. That event comes back
// ALONE on its page, byte-identical, with a next_cursor, and the walk
// continues past it to the end. Before SessionStore v0.15.0 the page holding
// it failed whole (too_large, answered 500 here), and a cold or reconnecting
// client was stuck at it forever.
func TestAHostSessionsJournalServesAnOffloadedEventAloneOnAPage(t *testing.T) {
	t.Parallel()

	control, runtime, _, seqs, large, body := largeBodyWorld(t)
	server := composedJournalServer(t, control, WithJournalResolver(runtimeJournals(runtime)))

	var pages []sessionwire.JournalPage
	query := "?from_seq=0"
	for len(pages) < 10 {
		code, page, raw := getJournal(t, server, query)
		if code != http.StatusOK {
			t.Fatalf("/journal%s = %d %.200s, want 200", query, code, raw)
		}
		pages = append(pages, page)
		if page.NextCursor == "" {
			break
		}
		query = "?cursor=" + string(page.NextCursor)
	}
	var walked []uint64
	for i, page := range pages {
		for _, event := range page.Events {
			walked = append(walked, event.JournalSeq)
			if event.JournalSeq != seqs[large] {
				continue
			}
			if len(page.Events) != 1 || page.NextCursor == "" {
				t.Fatalf("page %d holds the offloaded event among %d events (cursor %q), want it alone with a next_cursor",
					i, len(page.Events), page.NextCursor)
			}
			if !bytes.Equal(event.Body, body) {
				t.Fatalf("the offloaded event's body is %d bytes and not byte-identical to the %d committed", len(event.Body), len(body))
			}
		}
	}
	if fmt.Sprint(walked) != fmt.Sprint(seqs) {
		t.Fatalf("the walk returned %v over %d pages, want every public event %v", walked, len(pages), seqs)
	}
	if len(pages) != 3 {
		t.Fatalf("the walk took %d pages, want 3: the inline events, the offloaded event alone, the event after it", len(pages))
	}
}

// failedReads wraps a runtime journal and records every read that failed, so
// a case can tell a read that served an offloaded body from one that failed
// and was papered over by a tip-only fallback.
type failedReads struct {
	JournalReader
	mu       sync.Mutex
	failures []error
	probes   int
	tips     int
}

func (r *failedReads) ReadPublicJournal(ctx context.Context, req sessionstore.ReadPublicJournalRequest) (sessionwire.JournalPage, error) {
	page, err := r.JournalReader.ReadPublicJournal(ctx, req)
	r.mu.Lock()
	defer r.mu.Unlock()
	if isGapProbe(req) {
		r.probes++
	}
	if isTipRead(req) {
		r.tips++
	}
	if err != nil {
		r.failures = append(r.failures, err)
	}
	return page, err
}

func (r *failedReads) tipReads() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.tips
}

func (r *failedReads) snapshot() (probes int, failures []error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.probes, append([]error(nil), r.failures...)
}

// TestARelayGapProbeOverAnOffloadedEventResetsNotCloses: a viewer joins a
// quiet Host session, the Host commits an offloaded public event (~1 MiB) it
// never relays live, and the next record skips past it on the tail. The
// relay's gap probe reads a page holding the offloaded event, finds the hole,
// and resets the viewer to the journal tip -- every binding stays open, and no
// journal read fails along the way.
func TestARelayGapProbeOverAnOffloadedEventResetsNotCloses(t *testing.T) {
	t.Parallel()

	backend := memstore.New()
	ledger := &namingLedger{Ledger: backend.Ledger}
	backend.Ledger = ledger
	control, runtime, seqs, _ := hostSessionWorldOver(t, backend)
	reads := &failedReads{JournalReader: runtime}
	server := composedJournalServer(t, control, WithJournalResolver(func(ctx context.Context, tenant sessionwire.TenantID, binding sessionstore.SessionBinding) (JournalReader, error) {
		if _, err := runtimeJournals(runtime)(ctx, tenant, binding); err != nil {
			return nil, err
		}
		return reads, nil
	}))

	watched := watchHostSession(t, server)
	viewers := watched.closingViewers
	quietTip := seqs[len(seqs)-1]
	waitFor(t, "the first tail is anchored", func() bool { return reads.tipReads() > 0 })

	hidden, _ := appendOffloadedPublicBody(t, runtime, ledger, "event-hidden", sessionstore.DefaultJournalPageBytes+1024)
	next := appendBeneath(t, ledger, sessionstore.Envelope{
		Kind: sessionstore.EnvelopeKindPublicEvent, EventID: "event-next",
		Public: sessionstore.BodySlot{Inline: []byte(`{"n":"next"}`)},
	})
	if hidden != quietTip+1 || next != quietTip+2 {
		t.Fatalf("appended at %d and %d, want %d and %d", hidden, next, quietTip+1, quietTip+2)
	}
	record, err := sessionwire.EnduringPublication{
		TenantID: FakeTenant, SessionID: journalSession, EventID: "event-next",
		JournalSeq: next, CoveredThrough: next, Body: json.RawMessage(`{"n":"next"}`),
	}.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	watched.sink.Publication(record)

	waitFor(t, "the record reaches the viewer", func() bool {
		published, closes := viewers.snapshot()
		return closes > 0 || countType(t, published, sessionwire.SessionRecordTypeEnduringPublication) == 1
	})
	published, closes := viewers.snapshot()
	if closes != 0 {
		t.Fatalf("the viewer was closed %d times, want a reset and no close", closes)
	}
	var sawReset bool
	for _, raw := range published {
		switch recordType(t, raw) {
		case sessionwire.SessionRecordTypeSessionReset:
			var reset sessionwire.SessionReset
			if err := json.Unmarshal(raw, &reset); err != nil {
				t.Fatal(err)
			}
			if reset.JournalTip < hidden {
				t.Fatalf("reset %+v, want one reaching the offloaded event at %d", reset, hidden)
			}
			sawReset = true
		case sessionwire.SessionRecordTypeEnduringPublication:
			if !sawReset {
				t.Fatalf("the viewer got the record at %d with nothing in front of it: the offloaded event at %d is a silent hole", next, hidden)
			}
		}
	}
	probes, failures := reads.snapshot()
	if probes == 0 {
		t.Fatal("the relay made no gap probe; the case does not reach probeGap")
	}
	if len(failures) != 0 {
		t.Fatalf("journal reads failed over the offloaded event: %v", failures)
	}
}
