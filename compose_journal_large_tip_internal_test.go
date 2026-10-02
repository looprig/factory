package factory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"strings"
	"sync"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
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
	seq, _ := appendOffloadedPublicBody(t, runtime, ledger, "event-4", sessionstore.MaxInlineBodyBytes+1024)
	return seq
}

// offloadedBody is a public JSON body of exactly size bytes.
func offloadedBody(t *testing.T, size int) []byte {
	t.Helper()
	const framing = len(`{"text":""}`)
	if size < framing {
		t.Fatalf("body size %d is below the JSON framing", size)
	}
	return []byte(`{"text":"` + strings.Repeat("x", size-framing) + `"}`)
}

// appendOffloadedPublicBody is appendOffloadedPublicEvent for a named event of
// a given body size. It returns the event's sequence and its body.
func appendOffloadedPublicBody(t *testing.T, runtime *sessionstore.Store, ledger *namingLedger, id sessionwire.EventID, size int) (uint64, []byte) {
	t.Helper()
	body := offloadedBody(t, size)
	metadata, err := runtime.PutObject(context.Background(), sessionstore.PutObjectRequest{
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
	return appendBeneath(t, ledger, sessionstore.Envelope{
		Kind: sessionstore.EnvelopeKindPublicEvent, EventID: id,
		Public: sessionstore.BodySlot{Reference: &reference},
	}), body
}

// appendBeneath appends env at the runtime journal's tip beneath the store --
// for a record committed after appendOffloadedPublicBody, whose append the
// SessionStore writer did not see. It returns the record's sequence.
func appendBeneath(t *testing.T, ledger *namingLedger, env sessionstore.Envelope) uint64 {
	t.Helper()
	ctx := context.Background()
	frame, err := sessionstore.EncodeEnvelope(env)
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
// read resolves the tip record's body, and SessionStore before v0.15.0 refused
// an offloaded public body above MaxInlineBodyBytes as too_large -- so a
// session whose newest event was a large public body (a long assistant
// message, a big tool result) had its viewers CLOSED by every repair, tip hint
// and resync. The tip is read without a body, so the viewer is reset to it
// instead, and the repair never pays for resolving that body.
func TestAHostSessionsViewerIsResetNotClosedWhenTheTipIsAnOffloadedPublicEvent(t *testing.T) {
	t.Parallel()

	backend := memstore.New()
	ledger := &namingLedger{Ledger: backend.Ledger}
	backend.Ledger = ledger
	control, runtime, seqs, _ := hostSessionWorldOver(t, backend)
	seqs = append(seqs, appendOffloadedPublicEvent(t, runtime, ledger))

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
