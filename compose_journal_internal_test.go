package factory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/hostlink"
	"github.com/looprig/factory/internal/realtime/livetail"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage/memstore"
)

// These cases are the regression guard for the tip-0 defect
// (CLAUDE_DEBUG_TIP0_VIEWER_CLOSE.md): a Host-owned (disposition) session's
// journal is the RUNTIME's, kept on the runtime's backend under the binding's
// RuntimeSessionID, and SessionStore's public journal for the public id is
// never written. A Factory reading its tip from WithSessionReader alone saw
// tip 0 for every Host session, so every live-tail repair after a delivered
// record could not build a reset and UNSUBSCRIBED the viewers, and /journal was
// empty. They run over a real SessionStore and a real runtime journal, through
// New's own wiring (composeLive over the composed Server's config), with only
// the HostLink transport and the ClientLink replaced.

const (
	journalSession = sessionwire.SessionID("session-live")
	journalRuntime = "00000000-0000-4000-8000-0000000000bb"
)

// hostSessionWorld is a control store holding one disposition-bound session,
// admitted through the public create exactly as Factory admits one, and a
// runtime store holding three public events under the binding's
// RuntimeSessionID in harness's (legacy single-tenant) layout. It returns the
// sequences the three events were committed at.
func hostSessionWorld(t *testing.T) (control, runtime *sessionstore.Store, seqs []uint64) {
	t.Helper()
	control, runtime, seqs, _ = hostSessionWorldWriter(t)
	return control, runtime, seqs
}

// hostSessionWorldWriter is hostSessionWorld with the runtime journal's writer,
// for a case that commits more records to it.
func hostSessionWorldWriter(t *testing.T) (control, runtime *sessionstore.Store, seqs []uint64, writer *sessionstore.JournalWriter) {
	t.Helper()
	ctx := context.Background()
	control, err := sessionstore.Open(ctx, memstore.New())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = control.Close(context.Background()) })
	now := time.Now().UTC()
	payload := []byte(`{"blocks":[{"type":"text","text":"hi"}]}`)
	digest := sha256.Sum256(payload)
	identity := sessionstore.PublicCreateIdentity{
		TenantID: FakeTenant, SessionID: journalSession, CommandID: "create-1",
		Target: sessionstore.HostTargetKey{AgentID: e2eAgent, RuntimeCompatibilityID: e2eRuntime, Placement: sessionwire.HostPlacementPooled},
		Binding: sessionstore.SessionBinding{StorageBindingID: "storage-a", BindingVersion: "v1",
			RuntimeSessionID: journalRuntime, ProtocolMode: sessionstore.ProtocolModeDisposition},
		Kind: sessionstore.CommandKind("create"), PayloadDigest: hex.EncodeToString(digest[:]), PayloadSize: uint64(len(payload)),
	}
	if _, err := control.PreparePublicCreate(ctx, sessionstore.PreparePublicCreateRequest{Identity: identity,
		ProposedRuntimeCommandID: "runtime-create-1", AcceptedAt: now, ApplyDeadline: now.Add(time.Hour)}); err != nil {
		t.Fatalf("PreparePublicCreate: %v", err)
	}
	if _, _, err := control.AdmitPublicCreate(ctx, sessionstore.AdmitPublicCreateRequest{Identity: identity, Payload: payload}); err != nil {
		t.Fatalf("AdmitPublicCreate: %v", err)
	}

	runtime, err = sessionstore.Open(ctx, memstore.New(), sessionstore.WithLegacySingleTenant(FakeTenant))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close(context.Background()) })
	writer, err = runtime.OpenJournal(ctx, sessionstore.OpenJournalRequest{TenantID: FakeTenant, SessionID: journalRuntime})
	if err != nil {
		t.Fatalf("OpenJournal: %v", err)
	}
	t.Cleanup(func() { _ = writer.Close(context.Background()) })
	for i := 1; i <= 3; i++ {
		if _, err := writer.Append(ctx, sessionstore.Envelope{
			Kind: sessionstore.EnvelopeKindPublicEvent, EventID: sessionwire.EventID(fmt.Sprintf("event-%d", i)),
			Public: sessionstore.BodySlot{Inline: []byte(fmt.Sprintf(`{"n":%d}`, i))},
		}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	page, err := runtime.ReadPublicJournal(ctx, sessionstore.ReadPublicJournalRequest{TenantID: FakeTenant, SessionID: journalRuntime})
	if err != nil || len(page.Events) != 3 {
		t.Fatalf("the runtime journal = %d events, %v; want 3", len(page.Events), err)
	}
	for _, event := range page.Events {
		seqs = append(seqs, event.JournalSeq)
	}
	return control, runtime, seqs, writer
}

// runtimeJournals resolves the one binding this deployment knows to the
// runtime store and refuses any other.
func runtimeJournals(runtime *sessionstore.Store) JournalResolver {
	return func(_ context.Context, tenant sessionwire.TenantID, binding sessionstore.SessionBinding) (JournalReader, error) {
		if tenant != FakeTenant || binding.StorageBindingID != "storage-a" || binding.BindingVersion != "v1" {
			return nil, errors.New("unknown journal binding")
		}
		return runtime, nil
	}
}

func composedJournalServer(t *testing.T, control *sessionstore.Store, extra ...Option) *Server {
	t.Helper()
	options := append(RequiredOptionsExcept("WithSessionReader"), WithSessionReader(control))
	server, err := New(append(options, extra...)...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return server
}

// closingViewers records both halves of what the relay can do to a viewer:
// publish to it, or close its session channel (the ClientLink unsubscribe).
type closingViewers struct {
	mu        sync.Mutex
	published []json.RawMessage
	closes    int
}

func (v *closingViewers) PublishSession(_ sessionwire.TenantID, _ sessionwire.SessionID, encoded []byte) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.published = append(v.published, append(json.RawMessage(nil), encoded...))
	return nil
}

func (v *closingViewers) CloseSession(sessionwire.TenantID, sessionwire.SessionID) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.closes++
}

func (v *closingViewers) snapshot() ([]json.RawMessage, int) {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]json.RawMessage(nil), v.published...), v.closes
}

// watchedHostSession is the live plane composed over a Server's own
// configuration, one viewer watching the Host session, and the HostLink sink
// the test speaks for the Host through.
type watchedHostSession struct {
	*closingViewers
	sink hostlink.SessionSink
}

func watchHostSession(t *testing.T, server *Server) watchedHostSession {
	t.Helper()
	link := &wireLink{}
	pool, err := hostlink.NewPool(hostlink.Config{Dialer: wireDialer{link: link}})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	cfg := server.cfg
	cfg.directory = ownerDirectory{owner: sessionwire.HostLinkRegistryObservation{
		Version: sessionwire.CurrentWireVersion, TenantID: FakeTenant, SessionID: journalSession,
		HostID: "host-1", HostGeneration: 1, AgentID: e2eAgent, RuntimeCompatibilityID: e2eRuntime,
		Placement: sessionwire.HostPlacementPooled, InternalEndpoint: "ws://host-1.internal",
		Residency: sessionwire.SessionResidencyResident, Accepting: true, LeaseEpoch: 1,
		ObservedAt: now, ExpiresAt: now.Add(time.Hour),
	}}
	viewers := &closingViewers{}
	live, _, demand, err := composeLive(cfg, pool, func() livetail.Viewers { return viewers })
	if err != nil {
		t.Fatalf("composeLive: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = demand.Close(ctx)
		_ = live.Close(ctx)
		_ = pool.Close(ctx)
	})
	if err := demand.Acquire(context.Background(), FakeTenant, journalSession); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	link.mu.Lock()
	sink := link.sinks[0]
	link.mu.Unlock()
	return watchedHostSession{closingViewers: viewers, sink: sink}
}

// enduringAt is the Host's publication of runtime event i+1 at seq.
func enduringAt(t *testing.T, i int, seq uint64) []byte {
	t.Helper()
	record, err := sessionwire.EnduringPublication{
		TenantID: FakeTenant, SessionID: journalSession, EventID: sessionwire.EventID(fmt.Sprintf("event-%d", i+1)),
		JournalSeq: seq, CoveredThrough: seq, Body: json.RawMessage(fmt.Sprintf(`{"n":%d}`, i+1)),
	}.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	return record
}

// dropAfterDelivery has one viewer watch the Host session, delivers the three
// runtime records over the HostLink tail, then drops the HostLink
// subscription. It returns what the viewer saw after the drop settled.
func dropAfterDelivery(t *testing.T, server *Server, seqs []uint64) (resets []sessionwire.SessionReset, closes int) {
	t.Helper()
	watched := watchHostSession(t, server)
	viewers, sink := watched.closingViewers, watched.sink
	for i, seq := range seqs {
		sink.Publication(enduringAt(t, i, seq))
	}
	waitFor(t, "the three records reach the viewer", func() bool {
		published, _ := viewers.snapshot()
		return countType(t, published, sessionwire.SessionRecordTypeEnduringPublication) == len(seqs)
	})
	before, _ := viewers.snapshot()
	sink.Ended()
	waitFor(t, "the drop is repaired", func() bool {
		published, closes := viewers.snapshot()
		return closes > 0 || countType(t, published, sessionwire.SessionRecordTypeSessionReset) > countType(t, before, sessionwire.SessionRecordTypeSessionReset)
	})
	published, closes := viewers.snapshot()
	for _, raw := range published[len(before):] {
		if recordType(t, raw) != sessionwire.SessionRecordTypeSessionReset {
			continue
		}
		var reset sessionwire.SessionReset
		if err := json.Unmarshal(raw, &reset); err != nil {
			t.Fatal(err)
		}
		resets = append(resets, reset)
	}
	return resets, closes
}

func recordType(t *testing.T, raw json.RawMessage) sessionwire.SessionRecordType {
	t.Helper()
	var probe struct {
		Type sessionwire.SessionRecordType `json:"type"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		t.Fatal(err)
	}
	return probe.Type
}

func countType(t *testing.T, published []json.RawMessage, kind sessionwire.SessionRecordType) int {
	t.Helper()
	n := 0
	for _, raw := range published {
		if recordType(t, raw) == kind {
			n++
		}
	}
	return n
}

func waitFor(t *testing.T, what string, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting until %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestAHostSessionsViewerIsResetNotClosedAcrossAHostLinkDrop is the inverted
// TestTip0Repro, at the composition: with WithJournalResolver, a HostLink drop
// after three delivered records resets the viewer at the last delivered sequence with a
// tip the runtime journal actually holds, and never unsubscribes it.
func TestAHostSessionsViewerIsResetNotClosedAcrossAHostLinkDrop(t *testing.T) {
	t.Parallel()

	control, runtime, seqs := hostSessionWorld(t)
	server := composedJournalServer(t, control, WithJournalResolver(runtimeJournals(runtime)))
	resets, closes := dropAfterDelivery(t, server, seqs)
	last := seqs[len(seqs)-1]
	if closes != 0 {
		t.Fatalf("the viewer was closed %d times, want a reset and no close", closes)
	}
	if len(resets) == 0 {
		t.Fatal("the viewer was sent no reset after the drop")
	}
	// The repair's reset, and any the re-started tail repeats, all name what
	// the viewer holds and a tip the runtime journal really reached.
	for _, reset := range resets {
		if reset.LastContiguous != last || reset.JournalTip < last {
			t.Fatalf("the viewer was sent resets %+v, want each at last_contiguous %d with journal_tip >= %d", resets, last, last)
		}
	}
}

// TestWithoutAResolverAHostSessionsViewerIsClosed is the defect itself, kept
// as the control: the same world read through WithSessionReader alone has tip
// 0. Since v0.9.0's D2 fix the viewer's binding is anchored at that tip, so
// the FIRST record (at 2, past 0+1) is a gap no reset can describe and the
// viewer is closed before anything reaches it -- the relay's fail-closed arm,
// left alone. It pins that the resolver, and nothing else in this file, is
// what lets a Host session's viewer be served at all.
func TestWithoutAResolverAHostSessionsViewerIsClosed(t *testing.T) {
	t.Parallel()

	control, _, seqs := hostSessionWorld(t)
	server := composedJournalServer(t, control)
	viewers := watchHostSession(t, server)
	for i, seq := range seqs {
		viewers.sink.Publication(enduringAt(t, i, seq))
	}
	waitFor(t, "the viewer is closed", func() bool {
		_, closes := viewers.snapshot()
		return closes > 0
	})
	// The record past the unrepairable gap never reached the viewers it was
	// closed for. (Later records go to a fresh channel binding, which has
	// nobody on it: every viewer was unsubscribed.)
	published, _ := viewers.snapshot()
	first := string(enduringAt(t, 0, seqs[0]))
	for _, raw := range published {
		if string(raw) == first {
			t.Fatalf("the record at %d reached a viewer the read plane cannot repair", seqs[0])
		}
	}
}

func getJournal(t *testing.T, server *Server, query string) (int, sessionwire.JournalPage, string) {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, e2eOrigin+"/v1/sessions/"+string(journalSession)+"/journal"+query, nil)
	request.Header.Set("Authorization", "Bearer "+FakeCredentialValue)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	var page sessionwire.JournalPage
	if recorder.Code == http.StatusOK {
		if err := json.Unmarshal(recorder.Body.Bytes(), &page); err != nil {
			t.Fatalf("decode %s: %v", recorder.Body, err)
		}
	}
	return recorder.Code, page, recorder.Body.String()
}

// TestAHostSessionsJournalIsTheRuntimeJournal holds the read plane: /journal
// for the PUBLIC session id answers the runtime journal's three events and its
// tip, and a continuation cursor is wrapped to the public session -- it pages
// on, and the runtime store's own cursor is refused rather than honoured.
func TestAHostSessionsJournalIsTheRuntimeJournal(t *testing.T) {
	t.Parallel()

	control, runtime, seqs := hostSessionWorld(t)
	server := composedJournalServer(t, control, WithJournalResolver(runtimeJournals(runtime)))
	code, page, body := getJournal(t, server, "")
	if code != http.StatusOK || len(page.Events) != 3 || page.CapturedTip < seqs[2] {
		t.Fatalf("/journal = %d %s, want the runtime journal's three events", code, body)
	}
	for i, event := range page.Events {
		if event.JournalSeq != seqs[i] || event.EventID != sessionwire.EventID(fmt.Sprintf("event-%d", i+1)) {
			t.Fatalf("event %d = %+v, want seq %d", i, event, seqs[i])
		}
	}

	code, first, body := getJournal(t, server, fmt.Sprintf("?from_seq=%d&limit=1", seqs[0]))
	if code != http.StatusOK || len(first.Events) != 1 || first.NextCursor == "" {
		t.Fatalf("a one-event page = %d %s, want one event and a cursor", code, body)
	}
	code, next, body := getJournal(t, server, "?cursor="+string(first.NextCursor)+"&limit=1")
	if code != http.StatusOK || len(next.Events) != 1 || next.Events[0].JournalSeq != seqs[1] {
		t.Fatalf("the continuation = %d %s, want event at seq %d", code, body, seqs[1])
	}

	inner, err := runtime.ReadPublicJournal(context.Background(), sessionstore.ReadPublicJournalRequest{
		TenantID: FakeTenant, SessionID: journalRuntime, FromSeq: seqs[0], Limit: 1})
	if err != nil || inner.NextCursor == "" {
		t.Fatalf("runtime page: %v", err)
	}
	if code, _, body := getJournal(t, server, "?cursor="+string(inner.NextCursor)); code != http.StatusBadRequest {
		t.Fatalf("the runtime store's own cursor = %d %s, want 400", code, body)
	}
}

// TestAHostSessionsJournalFailsLoudWhenTheResolverRefuses: a binding the
// resolver does not know is an error, never a silently empty journal.
func TestAHostSessionsJournalFailsLoudWhenTheResolverRefuses(t *testing.T) {
	t.Parallel()

	control, _, _ := hostSessionWorld(t)
	server := composedJournalServer(t, control, WithJournalResolver(func(context.Context, sessionwire.TenantID, sessionstore.SessionBinding) (JournalReader, error) {
		return nil, errors.New("unknown journal binding")
	}))
	if code, _, body := getJournal(t, server, ""); code == http.StatusOK {
		t.Fatalf("/journal with a refusing resolver = %d %s, want an error", code, body)
	}
}

// TestALegacySessionIsStillReadFromTheSessionReader: the resolver is consulted
// only for a disposition-bound session; any other is read where it always was.
func TestALegacySessionIsStillReadFromTheSessionReader(t *testing.T) {
	t.Parallel()

	var resolved int
	reads := &legacyReads{}
	router := resolvedJournals{SessionReader: reads, resolve: func(context.Context, sessionwire.TenantID, sessionwire.SessionID, sessionstore.SessionBinding) (JournalReader, error) {
		resolved++
		return nil, errors.New("must not be asked")
	}}
	page, err := router.ReadPublicJournal(context.Background(), sessionstore.ReadPublicJournalRequest{TenantID: FakeTenant, SessionID: "legacy"})
	if err != nil || page.CapturedTip != 9 || resolved != 0 || reads.journal != 1 {
		t.Fatalf("a legacy read = %+v, %v (resolved %d, read %d); want the reader's page", page, err, resolved, reads.journal)
	}
}

type legacyReads struct {
	FakeSeams
	journal int
}

func (r *legacyReads) ReadPublicJournal(context.Context, sessionstore.ReadPublicJournalRequest) (sessionwire.JournalPage, error) {
	r.journal++
	return sessionwire.JournalPage{CapturedTip: 9, CoveredThrough: 9}, nil
}

// TestAHostPlacingCompositionWithoutAJournalResolverIsRefused: a composition
// that creates or places Host sessions and cannot read their journals is
// refused by New, naming the missing option; supplying the resolver admits it.
func TestAHostPlacingCompositionWithoutAJournalResolverIsRefused(t *testing.T) {
	t.Parallel()

	createPlane := []Option{
		WithObjectStoreResolver(func(context.Context, sessionstore.SessionBinding) (ObjectReader, error) {
			return nil, errors.New("unused")
		}),
		WithSessionBinding("storage-a", "v1"),
		WithPublicCreates(stubCreates{}),
	}
	resolver := WithJournalResolver(func(context.Context, sessionwire.TenantID, sessionstore.SessionBinding) (JournalReader, error) {
		return nil, errors.New("unused")
	})
	aware := WithSessionJournalResolver(func(context.Context, sessionwire.TenantID, sessionwire.SessionID, sessionstore.SessionBinding) (JournalReader, error) {
		return nil, errors.New("unused")
	})
	for name, row := range map[string]struct {
		options []Option
		refused bool
	}{
		"creates with a session resolver":   {append(append([]Option(nil), createPlane...), aware), false},
		"placement with a session resolver": {[]Option{WithPendingCommands(FakeSeams{}), aware}, false},
		"creates":                           {createPlane, true},
		"placement":                         {[]Option{WithPendingCommands(FakeSeams{})}, true},
		"creates and placement":             {append(append([]Option(nil), createPlane...), WithPendingCommands(FakeSeams{})), true},
		"creates with a resolver":           {append(append([]Option(nil), createPlane...), resolver), false},
		"placement with a resolver":         {[]Option{WithPendingCommands(FakeSeams{}), resolver}, false},
		"neither, without a resolver":       {nil, false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := New(append(RequiredOptions(), row.options...)...)
			if row.refused {
				var named *OptionError
				if !errors.Is(err, ErrHostSessionsWithoutJournalResolver) || !errors.As(err, &named) || named.Option != "WithSessionJournalResolver" {
					t.Fatalf("New = %v, want ErrHostSessionsWithoutJournalResolver naming WithSessionJournalResolver", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("New = %v, want it accepted", err)
			}
		})
	}
	if _, err := New(append(RequiredOptions(), WithJournalResolver(nil))...); !errors.Is(err, ErrNilDependency) {
		t.Fatalf("a nil resolver = %v, want ErrNilDependency", err)
	}
}

// stubCreates is a PublicCreates that is never called.
type stubCreates struct{ PublicCreates }

// TestAWrappedCursorIsBoundToItsSessionAndBinding: a cursor minted for one
// public session or binding is refused for another, before the runtime store
// is asked -- a JournalReader is any deployer value, and need not scope its own
// tokens.
func TestAWrappedCursorIsBoundToItsSessionAndBinding(t *testing.T) {
	t.Parallel()

	binding := sessionstore.SessionBinding{StorageBindingID: "storage-a", BindingVersion: "v1",
		RuntimeSessionID: journalRuntime, ProtocolMode: sessionstore.ProtocolModeDisposition}
	token, err := wrapJournalCursor("inner", FakeTenant, journalSession, binding)
	if err != nil {
		t.Fatal(err)
	}
	if inner, err := unwrapJournalCursor(token, FakeTenant, journalSession, binding); err != nil || inner != "inner" {
		t.Fatalf("the round trip = %q, %v", inner, err)
	}
	other := binding
	other.RuntimeSessionID = "00000000-0000-4000-8000-0000000000cc"
	otherStorage := binding
	otherStorage.StorageBindingID = "storage-b"
	otherVersion := binding
	otherVersion.BindingVersion = "v2"
	for name, row := range map[string]struct {
		tenant  sessionwire.TenantID
		session sessionwire.SessionID
		binding sessionstore.SessionBinding
	}{
		"another session":         {FakeTenant, "session-other", binding},
		"another tenant":          {"tenant-b", journalSession, binding},
		"another binding":         {FakeTenant, journalSession, other},
		"another storage binding": {FakeTenant, journalSession, otherStorage},
		"another binding version": {FakeTenant, journalSession, otherVersion},
	} {
		var journal *sessionstore.JournalError
		if _, err := unwrapJournalCursor(token, row.tenant, row.session, row.binding); !errors.As(err, &journal) || journal.Code != sessionstore.JournalErrorCursor {
			t.Fatalf("%s: unwrap = %v, want JournalErrorCursor", name, err)
		}
	}
}
