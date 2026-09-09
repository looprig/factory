package clientlink_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	centrifugego "github.com/centrifugal/centrifuge-go"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	"github.com/looprig/factory/internal/admission"
	internalidentity "github.com/looprig/factory/internal/identity"
	"github.com/looprig/factory/internal/realtime/clientlink"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage/memstore"
	"net/http/httptest"
)

// ---------------------------------------------------------------------------
// Step 2 -- the reply follows the SessionInbox commit, and waits for nothing
// after it.
// ---------------------------------------------------------------------------

// TestTheReplyFollowsTheInboxCommitAndWaitsForNothingElse holds both halves of
// step 2, and each half needs its own mechanism.
//
// "Not before the commit" is measured by holding the admission open and
// requiring the RPC to be unanswered while it is held. A test that only checked
// the answer's CONTENT would pass against a handler that replied first and
// admitted afterwards, which is precisely the failure -- a client told its
// command is durable while nothing has been written.
//
// "Not after the commit" is measured by the fixture itself rather than by a
// clock: there is no Host, no HostLink and no agent anywhere in this
// composition, so an answer that waited for a command to be APPLIED could never
// arrive. Timing out is what a handler that waited for completion would do
// here; returning is what one that acknowledges acceptance does.
func TestTheReplyFollowsTheInboxCommitAndWaitsForNothingElse(t *testing.T) {
	t.Parallel()

	f := newFixture(t, testLimits())
	commit := make(chan struct{})
	f.admitter.mu.Lock()
	f.admitter.gate = commit
	f.admitter.entry = acceptedEntry("session-1", "cmd-1")
	f.admitter.mu.Unlock()

	client, observed := dialSupported(t, f, "token-a")
	await(t, observed.connected, "connected event")

	ctx, cancel := context.WithTimeout(context.Background(), waitFor)
	defer cancel()

	type answer struct {
		result centrifugego.RPCResult
		err    error
	}
	answers := make(chan answer, 1)
	go func() {
		result, err := client.RPC(ctx, string(clientlink.MethodSessionInput),
			commandBody(clientlink.MethodSessionInput, "session-1", "cmd-1"))
		answers <- answer{result, err}
	}()

	// The admission must be REACHED -- otherwise "unanswered" would also be
	// true of a handler that never started -- and must still be running.
	waitUntil(t, func() bool { return len(f.admitter.recorded()) == 1 }, "the command to reach admission")
	select {
	case got := <-answers:
		t.Fatalf("the RPC was answered with (%s, %v) before the inbox commit returned", got.result.Data, got.err)
	case <-time.After(250 * time.Millisecond):
	}

	close(commit)
	got := await(t, answers, "the RPC reply")
	if got.err != nil {
		t.Fatalf("the RPC = %v, want the accepted record", got.err)
	}
	if status := statusOf(t, got.result.Data); status.State != sessionwire.CommandStateAccepted {
		t.Errorf("the reply reported %q, want %q", status.State, sessionwire.CommandStateAccepted)
	}
}

// waitUntil polls a condition. It is used only where the thing waited for is
// recorded state rather than a channel; nothing here asserts that anything is
// fast, and the bound is the suite's own generous one.
func waitUntil(t *testing.T, done func() bool, what string) {
	t.Helper()

	deadline := time.Now().Add(waitFor)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// Step 3 -- an unknown outcome, retried on a new Factory.
//
// Everything below drives the RELEASED SessionStore over memstore, with two
// independent admission services and two independent ClientLink handlers over
// one durable store. That is what a second Factory replica is: same durable
// state, different process, different clock, different proposed runtime
// identities.
// ---------------------------------------------------------------------------

// replica is one Factory: its own admission service, its own ClientLink, its
// own clock and its own identifier source, over a shared store.
type replica struct {
	fixture  *fixture
	service  *admission.Service
	clock    replicaClock
	shutdown func()
}

type replicaClock struct{ now time.Time }

func (c replicaClock) Now() time.Time { return c.now }
func (c replicaClock) AfterFunc(time.Duration, func()) func() bool {
	return func() bool { return true }
}

// replicaIDs mints the proposed RuntimeCommandIDs one replica offers. The
// prefix differs per replica so the WINNER's mapping is identifiable in the
// stored record: SessionStore keeps the winner's and hands a loser the same
// one, and a test that could not tell them apart could not see that happen.
type replicaIDs struct {
	prefix string
	next   int
}

func (s *replicaIDs) NewUUID() (string, error) {
	s.next++
	return s.prefix + "-" + string(rune('a'+s.next-1)), nil
}

// replicaAuthorizer grants everything. Authorization is A1.2's and A6.1's
// subject and is measured there; a case about DURABLE identity that also had
// to satisfy an authorizer would be two experiments at once.
type replicaAuthorizer struct{}

func (replicaAuthorizer) AuthorizeControl(context.Context, identity.Principal, sessionwire.SessionID, sessionstore.CommandKind) error {
	return nil
}

func (replicaAuthorizer) AuthorizeServiceSweep(context.Context, identity.Principal) error { return nil }

func (replicaAuthorizer) AuthorizeSubscribe(context.Context, identity.Principal, string) error {
	return nil
}

// replicaTargets is the configured launch identity. It answers for exactly one
// agent and one compatibility identity, so an unknown one is a real refusal
// rather than a fixture that says yes to everything.
type replicaTargets struct{}

func (replicaTargets) ResolveAgent(_ context.Context, agent sessionwire.AgentID) (admission.Target, bool, error) {
	if agent != "agent-a" {
		return admission.Target{}, false, nil
	}
	return admission.Target{Key: replicaTargetKey()}, true, nil
}

func (replicaTargets) IsKnown(_ context.Context, key sessionstore.HostTargetKey) (bool, error) {
	return key == replicaTargetKey(), nil
}

func replicaTargetKey() sessionstore.HostTargetKey {
	return sessionstore.HostTargetKey{
		AgentID: "agent-a", RuntimeCompatibilityID: "runtime-v1", Placement: sessionwire.HostPlacementPooled,
	}
}

// replicaDirectory observes no owner, which is the truth in this composition:
// no Host exists. It is what makes the acceptance measured here a purely
// durable one.
type replicaDirectory struct{}

func (replicaDirectory) Owner(context.Context, sessionwire.TenantID, sessionwire.SessionID) (sessionwire.HostLinkRegistryObservation, bool, error) {
	return sessionwire.HostLinkRegistryObservation{}, false, nil
}

// replicaApplyDeadline is the window this deployment gives a command to be
// applied. It is an absolute literal, distinct from every other duration in
// this file, so a stored deadline computed from something else is visible.
const replicaApplyDeadline = 17 * time.Minute

// newReplica builds one Factory over the shared store.
func newReplica(t *testing.T, store *sessionstore.Store, prefix string, now time.Time) *replica {
	t.Helper()

	service, err := admission.NewService(admission.Config{
		Authorizer: replicaAuthorizer{}, Targets: replicaTargets{},
		Catalog: store, Commands: store, Directory: replicaDirectory{},
		Clock: replicaClock{now}, IDs: &replicaIDs{prefix: prefix},
		ApplyDeadline: replicaApplyDeadline,
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	v := &verifier{claims: tokens()}
	authenticator, err := internalidentity.NewAuthenticator(internalidentity.Config{Verifier: v})
	if err != nil {
		t.Fatalf("NewAuthenticator: %v", err)
	}
	demand := &recordingDemand{}
	clock := &manualClock{}
	handler, err := clientlink.NewHandler(clientlink.Config{
		Authenticator: authenticator,
		Authorizer:    replicaAuthorizer{},
		Admitter:      service,
		Demand:        demand,
		Clock:         clock,
		Limits:        testLimits(),
		Version:       buildVersion,
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	server := httptest.NewServer(handler)
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		// Bounded Shutdown BEFORE the unbounded Close, for the reason
		// newFixture's cleanup gives. Correcting this one and claiming "the two
		// now say so in each other's terms" was itself the defect: there were
		// SIX such fixtures, and four were still wrong. The reader is
		// TestEveryBoundedShutdownPrecedesAnUnboundedClose, which censuses them
		// rather than trusting anyone to have looked.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = handler.Shutdown(ctx)
		server.Close()
	}
	t.Cleanup(stop)
	return &replica{
		fixture:  &fixture{handler: handler, url: "ws" + strings.TrimPrefix(server.URL, "http"), verifier: v, demand: demand, clock: clock},
		service:  service,
		clock:    replicaClock{now},
		shutdown: stop,
	}
}

// rpc drives one command over this replica's own socket, with its own client.
func (r *replica) rpc(t *testing.T, method clientlink.Method, body []byte) ([]byte, error) {
	t.Helper()

	client, observed := dialSupported(t, r.fixture, "token-a")
	await(t, observed.connected, "connected event")
	ctx, cancel := context.WithTimeout(context.Background(), waitFor)
	defer cancel()
	result, err := client.RPC(ctx, string(method), body)
	return result.Data, err
}

// openStore is the released SessionStore over an in-memory backend.
func openStore(t *testing.T) *sessionstore.Store {
	t.Helper()

	// Background rather than t.Context(): the store outlives the test body,
	// because Close runs from a cleanup and t.Context() is already cancelled by
	// then.
	store, err := sessionstore.Open(context.Background(), memstore.New())
	if err != nil {
		t.Fatalf("sessionstore.Open: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(context.Background()); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return store
}

// existingSession creates a durable session through the legacy create, which is
// the only create the released store can complete today: the V1 create is
// refused with runtime_unavailable until the immutable create-command
// reservation exists (admission.ErrCreateIdentityProtocolUnavailable), which is
// A3.1's remaining work and not this task's.
func existingSession(t *testing.T, r *replica, principal identity.Principal) sessionwire.SessionID {
	t.Helper()

	result, err := r.service.AdmitLegacyCreate(t.Context(), principal, admission.LegacyCreateRequest{
		AgentID: "agent-a", Blocks: []byte(`[{"text":"hello"}]`),
	})
	if err != nil {
		t.Fatalf("AdmitLegacyCreate: %v", err)
	}
	return result.SessionID
}

func testPrincipal(t *testing.T) identity.Principal {
	t.Helper()

	principal, err := identity.NewPrincipal(tenantA, "user-a", identity.KindActor)
	if err != nil {
		t.Fatalf("NewPrincipal: %v", err)
	}
	return principal
}

// storedCommand reads one durable record back, out of band of both replicas.
func storedCommand(t *testing.T, store *sessionstore.Store, session sessionwire.SessionID, id sessionwire.CommandID) sessionstore.InboxEntry {
	t.Helper()

	entry, err := store.GetCommand(t.Context(), sessionstore.GetCommandRequest{
		TenantID: tenantA, SessionID: session, CommandID: id,
	})
	if err != nil {
		t.Fatalf("GetCommand(%q): %v", id, err)
	}
	return entry
}

// TestAnUnknownOutcomeRetriedOnANewFactoryReturnsTheOriginal is step 3, with
// the enumerated cases rather than one smoke test.
//
// What is reproduced exactly, and what is not, stated rather than implied. The
// first replica commits the command and is then SHUT DOWN, and every retry
// below is sent to a Factory that has never seen it: a different process, a
// different clock, its own proposed runtime identities, and no memory of the
// first attempt beyond the durable record. That is the whole of what "a new
// Factory" means here.
//
// What is NOT reproduced is the client's ignorance: this case reads the first
// reply, because comparing the retry's bytes against the original's is the
// assertion. A client that never received it would send the identical retry --
// the bytes it sends do not depend on an answer it did not get -- so the
// difference cannot reach the Factory, and claiming the stronger thing would be
// a sentence wider than the probe.
func TestAnUnknownOutcomeRetriedOnANewFactoryReturnsTheOriginal(t *testing.T) {
	t.Parallel()

	store := openStore(t)
	principal := testPrincipal(t)

	accepted := time.Date(2026, 9, 8, 9, 0, 0, 0, time.UTC)
	first := newReplica(t, store, "runtime-first", accepted)
	session := existingSession(t, first, principal)

	const id = sessionwire.CommandID("cmd-unknown-outcome")
	body := commandBody(clientlink.MethodSessionInput, session, id)
	original, err := first.rpc(t, clientlink.MethodSessionInput, body)
	if err != nil {
		t.Fatalf("the original command = %v, want an accepted record", err)
	}
	originalStatus := statusOf(t, original)
	if originalStatus.State != sessionwire.CommandStateAccepted {
		t.Fatalf("the original command was answered %q, want accepted", originalStatus.State)
	}
	if originalStatus.AcceptedOrder == 0 {
		t.Fatal("the original acceptance carries no acceptance order")
	}

	stored := storedCommand(t, store, session, id)
	if got, want := stored.Record.ApplyDeadline.UTC(), accepted.Add(replicaApplyDeadline); !got.Equal(want) {
		t.Errorf("the durable apply deadline is %v, want %v", got, want)
	}
	if !strings.HasPrefix(string(stored.Record.RuntimeCommandID), "runtime-first") {
		t.Errorf("the winning runtime mapping is %q, want the first replica's", stored.Record.RuntimeCommandID)
	}

	// The outcome becomes unknown: the replica that accepted it is gone.
	first.shutdown()

	// The new Factory. Its clock is an hour later and its proposed runtime
	// identities are its own, so anything it recomputed rather than read back
	// would differ visibly.
	second := newReplica(t, store, "runtime-second", accepted.Add(time.Hour))

	for _, tt := range []struct {
		name string
		// method and body are what the retry sends.
		method clientlink.Method
		body   []byte
		// wantCode is the refusal expected, or empty for the original record.
		wantCode sessionwire.ErrorCode
	}{
		{name: "the identical retry", method: clientlink.MethodSessionInput, body: body},
		{name: "the identical retry, again", method: clientlink.MethodSessionInput, body: body},
		{
			name: "the same identity carrying different input",
			// The payload is what SessionStore compares, so this is the reuse
			// that must fail closed rather than return the first command's
			// record and tell a caller something was accepted that was not.
			method:   clientlink.MethodSessionInput,
			body:     []byte(`{"version":1,"command_id":"` + string(id) + `","session_id":"` + string(session) + `","blocks":[{"text":"a different thing"}]}`),
			wantCode: sessionwire.ErrorCodeCommandRejected,
		},
		{
			name:     "the same identity under a different kind",
			method:   clientlink.MethodSessionInterrupt,
			body:     commandBody(clientlink.MethodSessionInterrupt, session, id),
			wantCode: sessionwire.ErrorCodeCommandRejected,
		},
		{
			name:     "a session this tenant does not have",
			method:   clientlink.MethodSessionInput,
			body:     commandBody(clientlink.MethodSessionInput, "session-that-was-never-created", id),
			wantCode: sessionwire.ErrorCodeSessionNotFound,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			reply, err := second.rpc(t, tt.method, tt.body)
			if err != nil {
				t.Fatalf("the retry = %v, want a reply", err)
			}
			if tt.wantCode != "" {
				if got := envelopeCode(t, reply); got != tt.wantCode {
					t.Errorf("the retry was refused with %q, want %q", got, tt.wantCode)
				}
				return
			}
			if string(reply) != string(original) {
				t.Errorf("the retry was answered %s, want the original %s", reply, original)
			}
		})
	}

	// The durable record is the original throughout: one record, the first
	// replica's runtime mapping, the first replica's deadline, the first
	// acceptance order. A retry that had written a second record, or extended
	// the deadline from the new replica's clock, fails here rather than in a
	// reply a client cannot tell apart.
	after := storedCommand(t, store, session, id)
	if after.AcceptedOrder != stored.AcceptedOrder {
		t.Errorf("the acceptance order moved from %d to %d", stored.AcceptedOrder, after.AcceptedOrder)
	}
	if after.Record.RuntimeCommandID != stored.Record.RuntimeCommandID {
		t.Errorf("the runtime mapping moved from %q to %q", stored.Record.RuntimeCommandID, after.Record.RuntimeCommandID)
	}
	if !after.Record.ApplyDeadline.Equal(stored.Record.ApplyDeadline) {
		t.Errorf("the apply deadline moved from %v to %v; a retry may not extend it", stored.Record.ApplyDeadline, after.Record.ApplyDeadline)
	}
	if !after.Record.AcceptedAt.Equal(stored.Record.AcceptedAt) {
		t.Errorf("the acceptance instant moved from %v to %v", stored.Record.AcceptedAt, after.Record.AcceptedAt)
	}

	// The positive control for every "unchanged" assertion above: a DIFFERENT
	// command identity on the same session is accepted by the same replica and
	// gets its own record. Without it, "nothing moved" would also be the output
	// of a Factory that had stopped admitting anything at all.
	fresh, err := second.rpc(t, clientlink.MethodSessionInput,
		commandBody(clientlink.MethodSessionInput, session, "cmd-second"))
	if err != nil {
		t.Fatalf("a new command on the new replica = %v, want an acceptance", err)
	}
	freshStatus := statusOf(t, fresh)
	if freshStatus.State != sessionwire.CommandStateAccepted {
		t.Fatalf("a new command was answered %q, want accepted", freshStatus.State)
	}
	if freshStatus.AcceptedOrder <= originalStatus.AcceptedOrder {
		t.Errorf("the new command's order %d is not above the original's %d", freshStatus.AcceptedOrder, originalStatus.AcceptedOrder)
	}
	freshRecord := storedCommand(t, store, session, "cmd-second")
	if !strings.HasPrefix(string(freshRecord.Record.RuntimeCommandID), "runtime-second") {
		t.Errorf("the new command's runtime mapping is %q, want the second replica's", freshRecord.Record.RuntimeCommandID)
	}
	if got, want := freshRecord.Record.ApplyDeadline.UTC(), accepted.Add(time.Hour).Add(replicaApplyDeadline); !got.Equal(want) {
		t.Errorf("the new command's deadline is %v, want %v", got, want)
	}
}

// TestTwoFactoriesRacingOneCommandIdentityAgreeOnOneRecord is the concurrent
// half of the same property.
//
// The sequential case above cannot see a race: the first acceptance has
// returned before the retry is sent. Here both replicas admit the same identity
// at once, so exactly one CreateOrdered wins, and the requirement is that both
// clients are told the same thing -- the winner's order and the winner's
// runtime mapping -- rather than each being told about its own attempt.
func TestTwoFactoriesRacingOneCommandIdentityAgreeOnOneRecord(t *testing.T) {
	t.Parallel()

	store := openStore(t)
	principal := testPrincipal(t)
	accepted := time.Date(2026, 9, 8, 10, 0, 0, 0, time.UTC)

	left := newReplica(t, store, "runtime-left", accepted)
	right := newReplica(t, store, "runtime-right", accepted.Add(time.Minute))
	session := existingSession(t, left, principal)

	const id = sessionwire.CommandID("cmd-raced")
	body := commandBody(clientlink.MethodSessionInput, session, id)

	replies := make(chan []byte, 2)
	for _, r := range []*replica{left, right} {
		go func() {
			reply, err := r.rpc(t, clientlink.MethodSessionInput, body)
			if err != nil {
				replies <- nil
				return
			}
			replies <- reply
		}()
	}
	first, second := await(t, replies, "the first reply"), await(t, replies, "the second reply")
	if first == nil || second == nil {
		t.Fatalf("a racing command failed: %s / %s", first, second)
	}
	if string(first) != string(second) {
		t.Errorf("the two replicas answered %s and %s; one public record has one public answer", first, second)
	}
	stored := storedCommand(t, store, session, id)
	if got := statusOf(t, first).AcceptedOrder; got != stored.AcceptedOrder {
		t.Errorf("the clients were told order %d, want the durable %d", got, stored.AcceptedOrder)
	}
	// The winner is one of the two proposals, never a blend, and never the
	// loser's own.
	runtime := string(stored.Record.RuntimeCommandID)
	if !strings.HasPrefix(runtime, "runtime-left") && !strings.HasPrefix(runtime, "runtime-right") {
		t.Errorf("the stored runtime mapping %q is neither replica's proposal", runtime)
	}
}

// TestTheAcceptedRecordIsTheStoresOwn closes the loop the fake cannot: every
// member of the reply is compared against the record the RELEASED store holds,
// read back out of band.
//
// The engine-level cases drive a fake that returns whatever they hand it, so
// they establish the projection and nothing about the record. This establishes
// that the record projected is the durable one.
func TestTheAcceptedRecordIsTheStoresOwn(t *testing.T) {
	t.Parallel()

	store := openStore(t)
	principal := testPrincipal(t)
	only := newReplica(t, store, "runtime-only", time.Date(2026, 9, 8, 11, 0, 0, 0, time.UTC))
	session := existingSession(t, only, principal)

	for _, method := range []clientlink.Method{
		clientlink.MethodSessionInput,
		clientlink.MethodSessionInterrupt,
		clientlink.MethodSessionRestore,
	} {
		id := sessionwire.CommandID("cmd-" + string(method))
		reply, err := only.rpc(t, method, commandBody(method, session, id))
		if err != nil {
			t.Fatalf("%s = %v, want an acceptance", method, err)
		}
		status := statusOf(t, reply)
		record := storedCommand(t, store, session, id)

		if status.CommandID != record.Record.CommandID {
			t.Errorf("%s replied with command %q, want the stored %q", method, status.CommandID, record.Record.CommandID)
		}
		if status.AcceptedOrder != record.AcceptedOrder {
			t.Errorf("%s replied with order %d, want the stored %d", method, status.AcceptedOrder, record.AcceptedOrder)
		}
		if status.State != sessionwire.CommandStateAccepted {
			t.Errorf("%s replied %q, want accepted", method, status.State)
		}
		kind, _ := clientlink.CommandKindFor(method)
		if record.Record.Kind != kind {
			t.Errorf("%s stored kind %q, want %q", method, record.Record.Kind, kind)
		}
		// The stored payload is the canonical V1 command, which is what makes
		// a retry's payload comparison the same comparison on either edge.
		var stored map[string]json.RawMessage
		if err := json.Unmarshal(record.Record.Payload, &stored); err != nil {
			t.Fatalf("%s stored a payload that is not JSON: %v", method, err)
		}
		if got := string(stored["command_id"]); got != `"`+string(id)+`"` {
			t.Errorf("%s stored command_id %s, want %q", method, got, id)
		}
		if got := string(stored["session_id"]); got != `"`+string(session)+`"` {
			t.Errorf("%s stored session_id %s, want %q", method, got, session)
		}
	}
}

// TestAFaultIsAnsweredAsATemporaryTransportFailureOverTheWire is the wire-level
// reader for the split Engine.Admit makes.
//
// A refusal reaches the browser as a resolved RPC carrying a Core envelope; a
// condition admission decided nothing about must NOT, because a client cannot
// distinguish "rejected" from "unknown" once it has been given one of the nine
// public codes. Centrifuge's own internal error is code 100 and is marked
// temporary, which is what a client retrying the same CommandID needs to see.
func TestAFaultIsAnsweredAsATemporaryTransportFailureOverTheWire(t *testing.T) {
	t.Parallel()

	f := newFixture(t, testLimits())
	f.admitter.mu.Lock()
	f.admitter.err = errors.New("the durable plane could not be reached")
	f.admitter.mu.Unlock()

	client, observed := dialSupported(t, f, "token-a")
	await(t, observed.connected, "connected event")
	ctx, cancel := context.WithTimeout(context.Background(), waitFor)
	defer cancel()

	result, err := client.RPC(ctx, string(clientlink.MethodSessionInput),
		commandBody(clientlink.MethodSessionInput, "session-1", "cmd-1"))
	if err == nil {
		t.Fatalf("a fault was answered with the reply %s, want a transport failure", result.Data)
	}
	// 100 is ErrorInternal. 108 (not available) and 107 (bad request) are the
	// two neighbours that would read as decisions about the command.
	if got := codeOf(err); got != 100 {
		t.Errorf("a fault failed with code %d (%v), want 100 (internal)", got, err)
	}
	// The dependency's own text is an operator's diagnostic.
	if strings.Contains(err.Error(), "could not be reached") {
		t.Errorf("the transport failure %v carries the cause's text", err)
	}
	if len(result.Data) != 0 {
		t.Errorf("a fault carried the body %s", result.Data)
	}
}

// TestAStuckAdmissionDoesNotWedgeTheLink is the operational half of the command
// bound, measured over a real socket rather than argued from the engine.
//
// The hazard the bound closes is not "one slow command". centrifuge dispatches
// an RPC synchronously on the connection's read loop
// (centrifuge@v0.38.0/client.go:1385 -> 2259), so an admission that never
// returns holds the loop, and the loop is what would deliver every OTHER frame
// on that link and what must return before Handler.Shutdown can drain the
// connection. Before the bound, a single wedged store therefore hung the link
// forever and defeated graceful shutdown; the gate measured the connection
// context surviving Close, Shutdown and the server's own Close for 25 seconds.
//
// So the assertion is not that the first command fails. It is that the link is
// STILL SERVING afterwards: a second command, admitted normally, is answered on
// the same connection. That is the property head-of-line blocking would break
// and the one an unbounded admission removes.
//
// The stated limit, again, is the seam's: this measures a dependency that
// HONOURS its context. One that ignores it holds the loop regardless, and no
// deadline at this layer can change that.
func TestAStuckAdmissionDoesNotWedgeTheLink(t *testing.T) {
	t.Parallel()

	limits := testLimits()
	limits.CommandTimeout = 100 * time.Millisecond
	f := newFixture(t, limits)
	f.admitter.mu.Lock()
	f.admitter.waitForContext = true
	f.admitter.mu.Unlock()

	client, observed := dialSupported(t, f, "token-a")
	await(t, observed.connected, "connected event")
	ctx, cancel := context.WithTimeout(context.Background(), waitFor)
	defer cancel()

	// The wedged command. It is answered as a temporary fault, never as a
	// public refusal: whether it landed is unknown, which is what the durable
	// CommandID makes safe to retry.
	result, err := client.RPC(ctx, string(clientlink.MethodSessionInput),
		commandBody(clientlink.MethodSessionInput, "session-1", "cmd-stuck"))
	if err == nil {
		t.Fatalf("the wedged command was answered with %s, want a transport failure", result.Data)
	}
	if got := codeOf(err); got != 100 {
		t.Errorf("the wedged command failed with code %d (%v), want 100 (internal, temporary)", got, err)
	}
	// The engine's bound must be what released it, not the fake's backstop:
	// otherwise every assertion below is satisfied by a link that recovers
	// because the dependency eventually gave up on its own.
	if f.admitter.unbounded() {
		t.Fatal("the wedged admission was released by the fake's backstop, so nothing in the engine bounded it")
	}

	// The link is still serving. This is the assertion; the one above is its
	// precondition.
	f.admitter.mu.Lock()
	f.admitter.waitForContext = false
	f.admitter.entry = acceptedEntry("session-1", "cmd-after")
	f.admitter.mu.Unlock()

	after, err := client.RPC(ctx, string(clientlink.MethodSessionInput),
		commandBody(clientlink.MethodSessionInput, "session-1", "cmd-after"))
	if err != nil {
		t.Fatalf("the next command on the same link = %v; the wedged one is still holding the read loop", err)
	}
	if got := statusOf(t, after.Data).CommandID; got != "cmd-after" {
		t.Errorf("the next command was answered for %q, want %q", got, "cmd-after")
	}

	// And the replica can be drained. Shutdown returning is the whole of what
	// "graceful" means for this handler; a still-blocked read loop would hold
	// it to its own deadline instead.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), waitFor)
	defer shutdownCancel()
	if err := f.handler.Shutdown(shutdownCtx); err != nil {
		t.Errorf("Shutdown = %v, want a drained replica", err)
	}
}

// TestACrossTenantSessionIsIndistinguishableFromAnAbsentOne is the committed
// regression guard for the `A9.1-notfound` carry-forward.
//
// `internal/admission`'s catalogNotFound handles CatalogErrorNotFound and
// KeyspaceBindingNotFound and omits CatalogErrorDeleted and
// CatalogErrorIdentity. A6.2 is the first edge that routes a command into the
// catalog lookup at all, so the question "does this path make the gap
// reachable" became live with it; the measured answer is no, because the
// released store's default multi-tenant layout hashes the tenant into the
// session's scope and a foreign tenant therefore fails at the BINDING check,
// which is handled.
//
// That answer is a property of a dependency, not of this module, so it is
// pinned rather than reasoned about: a later SessionStore that classified the
// same condition as CatalogErrorDeleted or CatalogErrorIdentity would fall
// through catalogNotFound, arrive here as a bare fault, and be answered
// centrifuge-internal instead of session_not_found. Nothing else in the suite
// would say so -- the neighbouring "a session this tenant does not have" case
// uses a never-created session on the SAME tenant, which takes the
// CatalogErrorNotFound arm.
//
// The positive control is the point of the case. "session_not_found" is a
// "nothing was found" answer and is also what a Factory that had stopped
// resolving anything would produce, so the same fixture must admit the same
// session for its OWN tenant in the same run.
func TestACrossTenantSessionIsIndistinguishableFromAnAbsentOne(t *testing.T) {
	t.Parallel()

	store := openStore(t)
	owner := testPrincipal(t)
	r := newReplica(t, store, "runtime-tenancy", time.Date(2026, 9, 8, 13, 0, 0, 0, time.UTC))
	session := existingSession(t, r, owner)

	stranger, err := identity.NewPrincipal(tenantB, "user-b", identity.KindActor)
	if err != nil {
		t.Fatalf("NewPrincipal: %v", err)
	}
	engine := r.fixture.handler.Engine()

	foreign, err := engine.Admit(t.Context(), stranger, clientlink.MethodSessionInput,
		commandBody(clientlink.MethodSessionInput, session, "cmd-foreign"))
	if err != nil {
		t.Fatalf("a cross-tenant command = %v, want a refusal envelope; a bare fault here means "+
			"the store now reports this condition with a code catalogNotFound does not handle (A9.1-notfound)", err)
	}
	if got := envelopeCode(t, foreign); got != sessionwire.ErrorCodeSessionNotFound {
		t.Errorf("a cross-tenant command was refused with %q, want %q", got, sessionwire.ErrorCodeSessionNotFound)
	}

	// A session that never existed, for the same principal: the two must be the
	// same public fact, or the refusal discloses that the identifier is real
	// somewhere else.
	absent, err := engine.Admit(t.Context(), stranger, clientlink.MethodSessionInput,
		commandBody(clientlink.MethodSessionInput, "session-never-created", "cmd-foreign"))
	if err != nil {
		t.Fatalf("an absent session = %v, want a refusal envelope", err)
	}
	if string(absent) != string(foreign) {
		t.Errorf("a cross-tenant session answered %s and an absent one %s; the two must be indistinguishable", foreign, absent)
	}

	// The positive control: the SAME session, the SAME engine, the owning
	// tenant. Without it the two refusals above are also what a Factory that
	// resolves nothing would produce.
	own, err := engine.Admit(t.Context(), owner, clientlink.MethodSessionInput,
		commandBody(clientlink.MethodSessionInput, session, "cmd-own"))
	if err != nil {
		t.Fatalf("the owning tenant's command = %v, want an acceptance", err)
	}
	if status := statusOf(t, own); status.State != sessionwire.CommandStateAccepted {
		t.Fatalf("the owning tenant's command was answered %q (%s), want accepted", status.State, own)
	}
}
