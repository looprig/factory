package factory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"strings"
	"sync"
	"testing"

	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

// namingLedger remembers the ledger name a writer appends to, so a case can
// commit a frame the SessionStore writer would refuse to build onto the same
// journal.
type namingLedger struct {
	storage.Ledger
	mu   sync.Mutex
	name string
}

func (l *namingLedger) Append(ctx context.Context, name string, expected uint64, payload []byte) error {
	l.mu.Lock()
	l.name = name
	l.mu.Unlock()
	return l.Ledger.Append(ctx, name, expected, payload)
}

func (l *namingLedger) journal() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.name
}

// appendOffloadedPublicEvent commits, at the runtime journal's tip, a public
// event whose body is object-backed and larger than MaxInlineBodyBytes: the
// frame Harness's journal writes for every public body above its default
// 512 KiB offload threshold (PutObject the body, reference it, append). The
// SessionStore writer refuses to build it, which is why it is appended beneath
// the store. It returns the event's sequence.
func appendOffloadedPublicEvent(t *testing.T, runtime *sessionstore.Store, ledger *namingLedger) uint64 {
	t.Helper()
	ctx := context.Background()
	body := []byte(`{"n":4,"text":"` + strings.Repeat("x", sessionstore.MaxInlineBodyBytes+1024) + `"}`)
	metadata, err := runtime.PutObject(ctx, sessionstore.PutObjectRequest{
		TenantID: FakeTenant, SessionID: journalRuntime, Kind: sessionstore.ObjectKindJournalPublic,
		SizeBytes: uint64(len(body)), SHA256: sha256.Sum256(body), Body: bytes.NewReader(body),
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	reference, err := sessionstore.BodyReferenceFromObjectMetadata(metadata)
	if err != nil {
		t.Fatalf("BodyReferenceFromObjectMetadata: %v", err)
	}
	frame, err := sessionstore.EncodeEnvelope(sessionstore.Envelope{
		Kind: sessionstore.EnvelopeKindPublicEvent, EventID: "event-4",
		Public: sessionstore.BodySlot{Reference: &reference},
	})
	if err != nil {
		t.Fatalf("EncodeEnvelope: %v", err)
	}
	name := ledger.journal()
	tip, err := ledger.Tip(ctx, name)
	if err != nil {
		t.Fatalf("Tip: %v", err)
	}
	if err := ledger.Ledger.Append(ctx, name, tip, frame); err != nil {
		t.Fatalf("Append: %v", err)
	}
	return tip + 1
}

// TestAHostSessionsViewerIsResetNotClosedWhenTheTipIsAnOffloadedPublicEvent
// is the tip-only read (R5.3 factory v0.9.0 limit): the repair after a
// HostLink drop needs only how far the journal has got, but a one-record tail
// read resolves the tip record's body, and SessionStore refuses an offloaded
// public body above MaxInlineBodyBytes as too_large -- so a session whose
// newest event is a large public body (a long assistant message, a big tool
// result) had its viewers CLOSED by every repair, tip hint and resync. The tip
// is read without a body, so the viewer is reset to it instead.
func TestAHostSessionsViewerIsResetNotClosedWhenTheTipIsAnOffloadedPublicEvent(t *testing.T) {
	t.Parallel()

	backend := memstore.New()
	ledger := &namingLedger{Ledger: backend.Ledger}
	backend.Ledger = ledger
	control, runtime, seqs, _ := hostSessionWorldOver(t, backend)
	seqs = append(seqs, appendOffloadedPublicEvent(t, runtime, ledger))

	// The control: the store really does refuse that tip through a page read.
	if _, err := runtime.ReadPublicJournal(context.Background(), sessionstore.ReadPublicJournalRequest{
		TenantID: FakeTenant, SessionID: journalRuntime, Tail: true, Limit: 1, ScanLimit: 1,
	}); err == nil {
		t.Fatal("a tail read of an offloaded public body above MaxInlineBodyBytes succeeded; the case no longer reaches the defect")
	}

	server := composedJournalServer(t, control, WithJournalResolver(runtimeJournals(runtime)))
	resets, closes := dropAfterDelivery(t, server, seqs)
	last := seqs[len(seqs)-1]
	if closes != 0 {
		t.Fatalf("the viewer was closed %d times, want a reset and no close", closes)
	}
	if len(resets) == 0 {
		t.Fatal("the viewer was sent no reset after the drop")
	}
	for _, reset := range resets {
		if reset.LastContiguous != last || reset.JournalTip != last {
			t.Fatalf("the viewer was sent resets %+v, want each at last_contiguous %d and journal_tip %d", resets, last, last)
		}
	}
}
