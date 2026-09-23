package factory

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
)

// TestAViewerJoiningAQuietHostSessionIsToldOfARecordItsTailNeverCarried is
// I3.1's D2 (TestAViewerOnAnotherReplicaIsToldOfEveryRecordAcrossAWarmRelease)
// ported to the composition, over a real runtime journal read through
// WithJournalResolver.
//
// A viewer joins a QUIET Host session: the runtime journal holds three events
// and the tail carries nothing yet. The Host then commits a record it never
// publishes live -- a warm release commits SessionResidencyReleased after its
// tail stopped relaying -- and a re-placed session's new tail delivers the next
// record on the same subscription. Before v0.9.0 the viewer received that
// record with nothing in front of it, and a later reset vouched for the hole.
// Now the first record past what the viewer's own read reached is preceded by
// a reset to the journal's tip.
func TestAViewerJoiningAQuietHostSessionIsToldOfARecordItsTailNeverCarried(t *testing.T) {
	t.Parallel()

	control, runtime, seqs, writer := hostSessionWorldWriter(t)
	tails := &tailReads{JournalReader: runtime}
	server := composedJournalServer(t, control, WithJournalResolver(func(ctx context.Context, tenant sessionwire.TenantID, binding sessionstore.SessionBinding) (JournalReader, error) {
		if _, err := runtimeJournals(runtime)(ctx, tenant, binding); err != nil {
			return nil, err
		}
		return tails, nil
	}))

	watched := watchHostSession(t, server)
	viewers := watched.closingViewers
	quietTip := seqs[len(seqs)-1]
	// The first tail's start anchors at the tip the viewer's read reaches;
	// nothing moves until it has.
	waitFor(t, "the first tail is anchored", func() bool { return tails.count() > 0 })

	// The release: committed, never published.
	released, err := writer.Append(context.Background(), sessionstore.Envelope{
		Kind: sessionstore.EnvelopeKindPublicEvent, EventID: "event-released",
		Public: sessionstore.BodySlot{Inline: []byte(`{"released":true}`)},
	})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	next, err := writer.Append(context.Background(), sessionstore.Envelope{
		Kind: sessionstore.EnvelopeKindPublicEvent, EventID: "event-next",
		Public: sessionstore.BodySlot{Inline: []byte(`{"n":"next"}`)},
	})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if released != quietTip+1 || next != quietTip+2 {
		t.Fatalf("appended at %d and %d, want %d and %d", released, next, quietTip+1, quietTip+2)
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
		published, _ := viewers.snapshot()
		return countType(t, published, sessionwire.SessionRecordTypeEnduringPublication) == 1
	})
	published, closes := viewers.snapshot()
	if closes != 0 {
		t.Fatalf("the viewer was closed %d times", closes)
	}
	var sawReset bool
	for _, raw := range published {
		switch recordType(t, raw) {
		case sessionwire.SessionRecordTypeSessionReset:
			var reset sessionwire.SessionReset
			if err := json.Unmarshal(raw, &reset); err != nil {
				t.Fatal(err)
			}
			if reset.LastContiguous != 0 || reset.JournalTip < released {
				t.Fatalf("reset %+v, want one vouching for nothing and reaching the release at %d", reset, released)
			}
			sawReset = true
		case sessionwire.SessionRecordTypeEnduringPublication:
			if !sawReset {
				t.Fatalf("the viewer got the record at %d with nothing in front of it: the release at %d is a silent hole\n%s",
					next, released, fmt.Sprint(published))
			}
		}
	}
}

// tailReads counts the tip-only reads made of a runtime journal.
type tailReads struct {
	JournalReader
	mu    sync.Mutex
	tails int
}

func (r *tailReads) ReadPublicJournal(ctx context.Context, req sessionstore.ReadPublicJournalRequest) (sessionwire.JournalPage, error) {
	if req.Tail {
		r.mu.Lock()
		r.tails++
		r.mu.Unlock()
	}
	return r.JournalReader.ReadPublicJournal(ctx, req)
}

func (r *tailReads) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.tails
}
