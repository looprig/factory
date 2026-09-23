package factory

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
)

// These cases hold v0.10.0's SessionJournalResolver: the resolver is handed the
// PUBLIC session id every journal read is for. A Host projects a runtime
// journal's bodies to public ids (host.NewPublicJournals), and the only place
// that id exists is the request Factory routed -- by the time the runtime
// journal is read the request names the binding's RuntimeSessionID, a one-way
// hash of the public id. Every read path is covered: /journal, the demand
// plane's journal_tip hint, the live-tail repair's tip, and the gap probe.

// resolvedRead is one runtime-journal read, paired with the public session the
// resolver was handed for it.
type resolvedRead struct {
	tenant  sessionwire.TenantID
	session sessionwire.SessionID
	binding sessionstore.SessionBinding
	req     sessionstore.ReadPublicJournalRequest
}

// sessionReads is a SessionJournalResolver over the runtime store that records
// what it was asked, and returns a reader that records each request under the
// session the resolver was handed.
type sessionReads struct {
	runtime *sessionstore.Store
	mu      sync.Mutex
	reads   []resolvedRead
}

func (s *sessionReads) resolve(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, binding sessionstore.SessionBinding) (JournalReader, error) {
	if _, err := runtimeJournals(s.runtime)(ctx, tenant, binding); err != nil {
		return nil, err
	}
	return journalReaderFunc(func(ctx context.Context, req sessionstore.ReadPublicJournalRequest) (sessionwire.JournalPage, error) {
		s.mu.Lock()
		s.reads = append(s.reads, resolvedRead{tenant: tenant, session: session, binding: binding, req: req})
		s.mu.Unlock()
		return s.runtime.ReadPublicJournal(ctx, req)
	}), nil
}

func (s *sessionReads) snapshot() []resolvedRead {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]resolvedRead(nil), s.reads...)
}

// matching counts the reads for which match holds, failing the test on any
// read whose resolver was not handed the public session.
func (s *sessionReads) matching(t *testing.T, match func(sessionstore.ReadPublicJournalRequest) bool) int {
	t.Helper()
	n := 0
	for _, read := range s.snapshot() {
		if read.session != journalSession || read.tenant != FakeTenant {
			t.Fatalf("the resolver was handed session %q (tenant %q), want the public %q", read.session, read.tenant, journalSession)
		}
		if read.binding.RuntimeSessionID != journalRuntime || read.req.SessionID != sessionwire.SessionID(journalRuntime) {
			t.Fatalf("read %+v, want it addressed by the runtime session %q", read, journalRuntime)
		}
		if match(read.req) {
			n++
		}
	}
	return n
}

type journalReaderFunc func(context.Context, sessionstore.ReadPublicJournalRequest) (sessionwire.JournalPage, error)

func (f journalReaderFunc) ReadPublicJournal(ctx context.Context, req sessionstore.ReadPublicJournalRequest) (sessionwire.JournalPage, error) {
	return f(ctx, req)
}

func isTipRead(req sessionstore.ReadPublicJournalRequest) bool { return req.Tail }

func isGapProbe(req sessionstore.ReadPublicJournalRequest) bool {
	return !req.Tail && req.FromSeq > 0 && req.Limit == 1 && req.ScanLimit > 0
}

func TestTheSessionJournalResolverIsHandedThePublicSessionForJournal(t *testing.T) {
	t.Parallel()

	control, runtime, seqs := hostSessionWorld(t)
	reads := &sessionReads{runtime: runtime}
	server := composedJournalServer(t, control, WithSessionJournalResolver(reads.resolve))
	code, page, body := getJournal(t, server, fmt.Sprintf("?from_seq=%d&limit=1", seqs[0]))
	if code != http.StatusOK || len(page.Events) != 1 || page.NextCursor == "" {
		t.Fatalf("/journal = %d %s, want one event and a cursor", code, body)
	}
	if code, next, body := getJournal(t, server, "?cursor="+string(page.NextCursor)+"&limit=1"); code != http.StatusOK || len(next.Events) != 1 || next.Events[0].JournalSeq != seqs[1] {
		t.Fatalf("the continuation = %d %s, want the event at %d", code, body, seqs[1])
	}
	if n := reads.matching(t, func(req sessionstore.ReadPublicJournalRequest) bool { return !req.Tail }); n != 2 {
		t.Fatalf("/journal made %d runtime reads, want 2", n)
	}
}

func TestTheSessionJournalResolverIsHandedThePublicSessionForHintAndRepair(t *testing.T) {
	t.Parallel()

	control, runtime, seqs := hostSessionWorld(t)
	reads := &sessionReads{runtime: runtime}
	server := composedJournalServer(t, control, WithSessionJournalResolver(reads.resolve))
	resets, closes := dropAfterDelivery(t, server, seqs)
	last := seqs[len(seqs)-1]
	if closes != 0 || len(resets) == 0 {
		t.Fatalf("resets %+v, closes %d; want a reset and no close", resets, closes)
	}
	for _, reset := range resets {
		if reset.LastContiguous != last || reset.JournalTip < last {
			t.Fatalf("reset %+v, want last_contiguous %d under a tip >= %d", reset, last, last)
		}
	}
	// The anchor/hint read and the repair's tip read are both tip reads; the
	// repair's tip is what the reset carries, so at least two were made.
	if n := reads.matching(t, isTipRead); n < 2 {
		t.Fatalf("%d tip reads, want the anchor and the repair", n)
	}
}

func TestTheSessionJournalResolverIsHandedThePublicSessionForTheGapProbe(t *testing.T) {
	t.Parallel()

	control, runtime, seqs, writer := hostSessionWorldWriter(t)
	reads := &sessionReads{runtime: runtime}
	server := composedJournalServer(t, control, WithSessionJournalResolver(reads.resolve))
	watched := watchHostSession(t, server)
	waitFor(t, "the tail is anchored", func() bool { return reads.matching(t, isTipRead) > 0 })

	// A record committed and never published, then one that skips past it.
	for _, id := range []sessionwire.EventID{"event-hidden", "event-next"} {
		if _, err := writer.Append(context.Background(), sessionstore.Envelope{
			Kind: sessionstore.EnvelopeKindPublicEvent, EventID: id,
			Public: sessionstore.BodySlot{Inline: []byte(`{}`)},
		}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	watched.sink.Publication(enduringAt(t, 4, seqs[len(seqs)-1]+2))
	waitFor(t, "the gap is probed", func() bool { return reads.matching(t, isGapProbe) > 0 })
}

// TestJournalResolverPrecedence: the two options are alternatives, and
// composing both is refused rather than one silently shadowing the other --
// the deprecated resolver cannot project a Host's bodies, so preferring it
// would leak runtime ids and preferring the other would ignore a deployer's
// explicit value.
func TestJournalResolverPrecedence(t *testing.T) {
	t.Parallel()

	legacy := WithJournalResolver(func(context.Context, sessionwire.TenantID, sessionstore.SessionBinding) (JournalReader, error) {
		return nil, errors.New("unused")
	})
	aware := WithSessionJournalResolver(func(context.Context, sessionwire.TenantID, sessionwire.SessionID, sessionstore.SessionBinding) (JournalReader, error) {
		return nil, errors.New("unused")
	})
	for name, options := range map[string][]Option{"legacy first": {legacy, aware}, "aware first": {aware, legacy}} {
		_, err := New(append(RequiredOptions(), options...)...)
		var named *OptionError
		if !errors.Is(err, ErrConflictingJournalResolvers) || !errors.As(err, &named) || named.Option != "WithSessionJournalResolver" {
			t.Fatalf("%s: New = %v, want ErrConflictingJournalResolvers naming WithSessionJournalResolver", name, err)
		}
	}
	if _, err := New(append(RequiredOptions(), WithSessionJournalResolver(nil))...); !errors.Is(err, ErrNilDependency) {
		t.Fatalf("a nil session resolver = %v, want ErrNilDependency", err)
	}
}

// TestTheDeprecatedResolverStillServesTheRuntimeJournal: WithJournalResolver
// keeps its v0.9.0 behaviour through the session-aware plane.
func TestTheDeprecatedResolverStillServesTheRuntimeJournal(t *testing.T) {
	t.Parallel()

	control, runtime, _ := hostSessionWorld(t)
	server := composedJournalServer(t, control, WithJournalResolver(runtimeJournals(runtime)))
	if code, page, body := getJournal(t, server, ""); code != http.StatusOK || len(page.Events) != 3 {
		t.Fatalf("/journal = %d %s, want three events", code, body)
	}
}
