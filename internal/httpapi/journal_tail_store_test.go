package httpapi

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

// These observers delegate to the released store and real in-memory ledger.
// Counting cursor advances measures work even when appended records are private
// and therefore invisible in the public HTTP response.
type tailObservedLedger struct {
	storage.Ledger
	afterTip func()
	tips     int
	starts   []uint64
	next     int
}

func (l *tailObservedLedger) Tip(ctx context.Context, name string) (uint64, error) {
	tip, err := l.Ledger.Tip(ctx, name)
	l.tips++
	if hook := l.afterTip; hook != nil {
		l.afterTip = nil
		hook()
	}
	return tip, err
}

func (l *tailObservedLedger) Read(ctx context.Context, name string, from uint64) (storage.Cursor, error) {
	l.starts = append(l.starts, from)
	cursor, err := l.Ledger.Read(ctx, name, from)
	if err != nil {
		return nil, err
	}
	return &tailObservedCursor{Cursor: cursor, ledger: l}, nil
}

type tailObservedCursor struct {
	storage.Cursor
	ledger *tailObservedLedger
}

func (c *tailObservedCursor) Next(ctx context.Context) (storage.Record, error) {
	c.ledger.next++
	return c.Cursor.Next(ctx)
}

type tailObservedStore struct {
	*sessionstore.Store
	requests []sessionstore.ReadPublicJournalRequest
}

func (s *tailObservedStore) ReadPublicJournal(ctx context.Context, req sessionstore.ReadPublicJournalRequest) (sessionwire.JournalPage, error) {
	s.requests = append(s.requests, req)
	return s.Store.ReadPublicJournal(ctx, req)
}

func newRealTailFixture(t *testing.T, count int, body string) (*fixture, *sessionstore.JournalWriter, *tailObservedLedger, *tailObservedStore) {
	t.Helper()
	backend := memstore.New()
	ledger := &tailObservedLedger{Ledger: backend.Ledger}
	backend.Ledger = ledger
	store, err := sessionstore.Open(context.Background(), backend)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close(context.Background()) })
	f := newFixture(t)
	record := coldRecord(sessionwire.SessionStateIdle, sessionwire.SessionResidencyCold)
	_, _, err = store.CreateCatalogEntry(context.Background(), sessionstore.CreateCatalogEntryRequest{
		TenantID: fixtureTenant, SessionID: fixtureSession, AgentID: record.AgentID,
		RuntimeCompatibilityID: record.RuntimeCompatibilityID,
		CreatedAt:              record.CreatedAt, LastActiveAt: record.LastActiveAt,
		State: record.State, Residency: record.Residency, DesiredPlacement: record.DesiredPlacement,
		IdempotencyKey: "tail-test-create",
	})
	if err != nil {
		t.Fatal(err)
	}
	writer, err := store.OpenJournal(context.Background(), sessionstore.OpenJournalRequest{TenantID: fixtureTenant, SessionID: fixtureSession})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close(context.Background()) })
	for i := 1; i < count; i++ {
		appendTailRecord(t, writer, sessionstore.Envelope{
			Kind: sessionstore.EnvelopeKindPublicEvent, EventID: sessionwire.EventID(fmt.Sprintf("event-%d", i)),
			Public: sessionstore.BodySlot{Inline: []byte(body)},
		})
	}
	ledger.tips, ledger.next, ledger.starts = 0, 0, nil
	reader := &tailObservedStore{Store: store}
	f.router.reads = reader
	return f, writer, ledger, reader
}

func appendTailRecord(t *testing.T, writer *sessionstore.JournalWriter, envelope sessionstore.Envelope) {
	t.Helper()
	if _, err := writer.Append(context.Background(), envelope); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPJournalTailUsesOneTipAcrossPrivateAppends(t *testing.T) {
	f, writer, ledger, reader := newRealTailFixture(t, 100, `{"value":1}`)
	ledger.afterTip = func() {
		for i := 0; i < 1000; i++ {
			appendTailRecord(t, writer, sessionstore.Envelope{
				Kind: sessionstore.EnvelopeKindRuntimeControl, RecordID: fmt.Sprintf("late-%d", i),
				Runtime: sessionstore.BodySlot{Inline: []byte(`{"secret":"late-private"}`)},
			})
		}
	}
	page := decodeJournalPage(t, f.get(journalTarget(fixtureSession)))
	if ledger.tips != 1 || ledger.next != 64 || !slices.Equal(ledger.starts, []uint64{37}) {
		t.Errorf("tips=%d cursor advances=%d starts=%v, want 1/64/[37]", ledger.tips, ledger.next, ledger.starts)
	}
	if page.CapturedTip != 100 || page.CoveredThrough != 100 || page.NextCursor != "" || len(page.Events) != 64 {
		t.Errorf("tail tip=%d covered=%d cursor=%q events=%d, want 100/100/empty/64", page.CapturedTip, page.CoveredThrough, page.NextCursor, len(page.Events))
	}
	if len(reader.requests) != 1 || !reader.requests[0].Tail || reader.requests[0].FromSeq != 0 || reader.requests[0].Cursor != "" {
		t.Fatalf("initial requests = %+v, want one unpositioned Tail request", reader.requests)
	}
	// The late appends really landed. A subsequent tail consists entirely of
	// private records and must still cost only 64 cursor advances.
	ledger.tips, ledger.next, ledger.starts = 0, 0, nil
	page = decodeJournalPage(t, f.get(journalTarget(fixtureSession)))
	if page.CapturedTip != 1100 || page.CoveredThrough != 1100 || len(page.Events) != 0 || ledger.next != 64 {
		t.Errorf("private tail tip=%d covered=%d events=%d advances=%d", page.CapturedTip, page.CoveredThrough, len(page.Events), ledger.next)
	}
}

func TestHTTPJournalTailByteBudgetCursorPreservesCapturedTip(t *testing.T) {
	// Two dozen 60 KiB bodies exceed the real store's 1 MiB page budget while
	// staying below its per-record overflow threshold.
	body := `{"value":"` + strings.Repeat("x", 60<<10) + `"}`
	f, writer, _, reader := newRealTailFixture(t, 25, body)
	page := decodeJournalPage(t, f.get(journalTarget(fixtureSession)))
	if page.NextCursor == "" {
		t.Fatal("real byte budget did not produce a continuation")
	}
	appendTailRecord(t, writer, sessionstore.Envelope{
		Kind: sessionstore.EnvelopeKindPublicEvent, EventID: "late-public",
		Public: sessionstore.BodySlot{Inline: []byte(`{"late":true}`)},
	})
	var got []uint64
	for rounds := 0; ; rounds++ {
		if rounds > 24 {
			t.Fatal("tail continuation did not terminate")
		}
		if page.CapturedTip != 25 {
			t.Fatalf("cursor recaptured tip %d, want 25", page.CapturedTip)
		}
		got = append(got, eventSequences(page)...)
		if page.NextCursor == "" {
			break
		}
		page = decodeJournalPage(t, f.get(journalTarget(fixtureSession)+"?cursor="+string(page.NextCursor)))
	}
	var want []uint64
	for seq := uint64(2); seq <= 25; seq++ {
		want = append(want, seq)
	}
	if !slices.Equal(got, want) || page.CoveredThrough != 25 {
		t.Fatalf("continued sequences=%v covered=%d", got, page.CoveredThrough)
	}
	if !reader.requests[0].Tail {
		t.Fatal("initial byte-budget page did not request Tail")
	}
	for _, req := range reader.requests[1:] {
		if req.Tail || req.Cursor == "" || req.FromSeq != 0 {
			t.Errorf("cursor request changed positioning: %+v", req)
		}
	}
	for _, position := range []string{"0", "2"} {
		decodeJournalPage(t, f.get(journalTarget(fixtureSession)+"?from_seq="+position+"&limit=1"))
		req := reader.requests[len(reader.requests)-1]
		if req.Tail || req.Cursor != "" || req.Limit != 1 {
			t.Errorf("explicit from_seq=%s request changed positioning: %+v", position, req)
		}
	}
}
