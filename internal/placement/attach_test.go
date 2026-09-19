package placement

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
)

// These cases drive B5's caller against the REAL store: the claim, the
// registry, the capacity page and the catalog are sessionstore's, and only the
// Host side is scripted. Every mapping from a Host answer to an action has a
// case that fails if the mapping moves.

const (
	testActor     = "factory-service"
	attachedEpoch = uint64(41)
	holderEpoch   = uint64(93)
)

type boundRoute struct {
	endpoint sessionwire.InternalEndpoint
	req      sessionwire.HostLinkBindRequest
}

// scriptedLinks is a HostLinks whose attach answer is chosen per Host. A Host
// with no script accepts at attachedEpoch.
type scriptedLinks struct {
	mu        sync.Mutex
	script    map[sessionwire.HostID]func(int, sessionwire.HostLinkAttachRequest) (sessionwire.HostLinkRegistryObservation, error)
	attaches  []sessionwire.HostLinkAttachRequest
	endpoints []sessionwire.InternalEndpoint
	binds     []boundRoute
	bindErr   error
	unbinds   []sessionwire.HostLinkUnbindRequest
	delivered []sessionwire.CommandID
	// route is this replica's current route for the session, as the real
	// pool keeps it: a bind sets it, an unbind clears it, and a bind to a
	// DIFFERENT Host than the current route is refused as a conflict, which
	// is what hostlink.Pool answers (ErrBindingConflict). RouteFor reads it,
	// so a placement that asked for the route AFTER its own bind sees its own
	// route, as it would in production (B5 quality gate QM24).
	route sessionwire.HostID
	// anySession admits a delivery for any session; the pending sweep places
	// several.
	anySession bool
}

func newScriptedLinks() *scriptedLinks {
	return &scriptedLinks{script: map[sessionwire.HostID]func(int, sessionwire.HostLinkAttachRequest) (sessionwire.HostLinkRegistryObservation, error){}}
}

func (l *scriptedLinks) on(host sessionwire.HostID, answer func(int, sessionwire.HostLinkAttachRequest) (sessionwire.HostLinkRegistryObservation, error)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.script[host] = answer
}

func (l *scriptedLinks) Attach(_ context.Context, endpoint sessionwire.InternalEndpoint, req sessionwire.HostLinkAttachRequest) (sessionwire.HostLinkRegistryObservation, error) {
	l.mu.Lock()
	l.attaches = append(l.attaches, req)
	l.endpoints = append(l.endpoints, endpoint)
	calls := 0
	for _, previous := range l.attaches {
		if previous.HostID == req.HostID {
			calls++
		}
	}
	answer := l.script[req.HostID]
	l.mu.Unlock()
	if answer != nil {
		return answer(calls, req)
	}
	return acceptedObservation(req, attachedEpoch), nil
}

func (l *scriptedLinks) Bind(_ context.Context, endpoint sessionwire.InternalEndpoint, req sessionwire.HostLinkBindRequest) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.binds = append(l.binds, boundRoute{endpoint: endpoint, req: req})
	if l.bindErr != nil {
		return l.bindErr
	}
	if l.route != "" && l.route != req.HostID {
		return fmt.Errorf("scripted pool: session is bound to %q, bind names %q: binding conflict", l.route, req.HostID)
	}
	l.route = req.HostID
	return nil
}

func (l *scriptedLinks) Unbind(_ context.Context, req sessionwire.HostLinkUnbindRequest) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.unbinds = append(l.unbinds, req)
	if l.route == req.HostID {
		l.route = ""
	}
	return nil
}

func (l *scriptedLinks) DeliverCommand(_ context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, delivery sessionwire.HostLinkCommandDelivery) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if tenant != testTenant || (session != testSession && !l.anySession) {
		return fmt.Errorf("delivery addressed %q/%q", tenant, session)
	}
	l.delivered = append(l.delivered, delivery.CommandID)
	return nil
}

func (l *scriptedLinks) RouteFor(sessionwire.TenantID, sessionwire.SessionID) (sessionwire.HostID, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.route, l.route != ""
}

func (l *scriptedLinks) attachedHosts() []sessionwire.HostID {
	l.mu.Lock()
	defer l.mu.Unlock()
	hosts := make([]sessionwire.HostID, 0, len(l.attaches))
	for _, req := range l.attaches {
		hosts = append(hosts, req.HostID)
	}
	return hosts
}

func acceptedObservation(req sessionwire.HostLinkAttachRequest, epoch uint64) sessionwire.HostLinkRegistryObservation {
	return sessionwire.HostLinkRegistryObservation{
		Version: sessionwire.CurrentWireVersion, TenantID: req.TenantID, SessionID: req.SessionID,
		HostID: req.HostID, HostGeneration: req.HostGeneration, AgentID: req.AgentID,
		RuntimeCompatibilityID: req.RuntimeCompatibilityID, Placement: sessionwire.HostPlacementPooled,
		InternalEndpoint: sessionwire.InternalEndpoint("wss://" + string(req.HostID) + ".internal/attached"),
		Residency:        sessionwire.SessionResidencyResident, Accepting: true, LeaseEpoch: epoch,
		ObservedAt: reconcileNow, ExpiresAt: reconcileNow.Add(time.Minute),
	}
}

func refuse(err sessionwire.HostLinkError) func(int, sessionwire.HostLinkAttachRequest) (sessionwire.HostLinkRegistryObservation, error) {
	return func(int, sessionwire.HostLinkAttachRequest) (sessionwire.HostLinkRegistryObservation, error) {
		return sessionwire.HostLinkRegistryObservation{}, &AttachRefusal{HostLinkError: err}
	}
}

func fail(err error) func(int, sessionwire.HostLinkAttachRequest) (sessionwire.HostLinkRegistryObservation, error) {
	return func(int, sessionwire.HostLinkAttachRequest) (sessionwire.HostLinkRegistryObservation, error) {
		return sessionwire.HostLinkRegistryObservation{}, err
	}
}

// recordedWaits is a Wait seam that records each backoff and runs a hook, so a
// re-placement is observed at the instant it happens rather than waited for.
type recordedWaits struct {
	mu    sync.Mutex
	waits []time.Duration
	hook  func(n int)
}

func (w *recordedWaits) wait(_ context.Context, d time.Duration) error {
	w.mu.Lock()
	w.waits = append(w.waits, d)
	n := len(w.waits)
	hook := w.hook
	w.mu.Unlock()
	if hook != nil {
		hook(n)
	}
	return nil
}

type attachFixture struct {
	*fixture
	links *scriptedLinks
	waits *recordedWaits
	logs  *bytes.Buffer
}

func newAttachFixture(t *testing.T, directory Directory) *attachFixture {
	t.Helper()

	f := newFixture(t, sessionwire.HostPlacementPooled, "factory-1")
	if directory == nil {
		directory = mustDirectory(t, f.store)
	}
	links := newScriptedLinks()
	waits := &recordedWaits{}
	logs := &bytes.Buffer{}
	reconciler, err := NewReconciler(Config{
		Directory: directory, Catalog: f.store, Claims: f.store,
		Clock: f.clock, HolderID: "factory-1", ClaimTTL: time.Minute, CandidateLimit: 8,
		Links: links, ActorID: testActor, ReplaceAttempts: 3, ReplaceBackoff: 10 * time.Millisecond,
		Wait: waits.wait, Logger: slog.New(slog.NewJSONHandler(logs, nil)),
	})
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}
	f.reconciler = reconciler
	return &attachFixture{fixture: f, links: links, waits: waits, logs: logs}
}

func (f *attachFixture) place(t *testing.T, wake ...sessionwire.CommandID) (Result, error) {
	t.Helper()
	return f.reconciler.Reconcile(context.Background(), Request{TenantID: testTenant, SessionID: testSession, Wake: wake})
}

func (f *attachFixture) mustPlace(t *testing.T, wake ...sessionwire.CommandID) Result {
	t.Helper()
	result, err := f.place(t, wake...)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	return result
}

// TestAnUnownedPooledSessionIsAttachedAndBoundWithTheReturnedEpoch is the whole
// B5 path: the request's host fence is the CANDIDATE's capacity report, the
// bind names the epoch the ATTACH returned, the wake is delivered, the route
// this call created is released, and the claim is given back.
func TestAnUnownedPooledSessionIsAttachedAndBoundWithTheReturnedEpoch(t *testing.T) {
	t.Parallel()

	f := newAttachFixture(t, nil)
	f.publishTarget(t, "host-a", 4, sessionwire.HostIsolationClassCrossTenantIsolated)

	result := f.mustPlace(t, "cmd-1", "cmd-2")
	if result.Decision.Outcome != OutcomeAttachPooled || result.Decision.Target.HostID != "host-a" {
		t.Fatalf("decision = %+v, want host-a attached", result.Decision)
	}
	if len(f.links.attaches) != 1 {
		t.Fatalf("attaches = %d, want 1", len(f.links.attaches))
	}
	sent := f.links.attaches[0]
	want := sessionwire.HostLinkAttachRequest{
		Version: sessionwire.CurrentWireVersion, TenantID: testTenant, SessionID: testSession,
		HostID: "host-a", HostGeneration: 1, AgentID: testAgent, RuntimeCompatibilityID: testRuntime,
		Mode: sessionwire.HostLinkAttachModeCreate, ActorID: testActor, IdempotencyKey: sent.IdempotencyKey,
	}
	if sent != want {
		t.Errorf("attach = %+v, want %+v", sent, want)
	}
	if err := sent.Validate(); err != nil {
		t.Errorf("attach does not validate under Core: %v", err)
	}
	if f.links.endpoints[0] != "wss://host-a.internal/hostlink" {
		t.Errorf("attach endpoint = %q, want the capacity report's", f.links.endpoints[0])
	}
	if result.Attached.LeaseEpoch != attachedEpoch {
		t.Errorf("Result.Attached.LeaseEpoch = %d, want %d", result.Attached.LeaseEpoch, attachedEpoch)
	}
	if len(f.links.binds) != 1 {
		t.Fatalf("binds = %+v, want exactly one", f.links.binds)
	}
	bind := f.links.binds[0]
	if bind.req.LeaseEpoch != attachedEpoch || bind.req.HostID != "host-a" || bind.req.HostGeneration != 1 {
		t.Errorf("bind = %+v, want host-a/1 at the returned epoch %d", bind.req, attachedEpoch)
	}
	if bind.endpoint != "wss://host-a.internal/attached" {
		t.Errorf("bind endpoint = %q, want the observation's", bind.endpoint)
	}
	if err := bind.req.Validate(); err != nil {
		t.Errorf("bind does not validate under Core: %v", err)
	}
	if !result.Bound || result.Delivered != 2 || !slices.Equal(f.links.delivered, []sessionwire.CommandID{"cmd-1", "cmd-2"}) {
		t.Errorf("Bound=%t Delivered=%d delivered=%v, want both wakes delivered", result.Bound, result.Delivered, f.links.delivered)
	}
	if len(f.links.unbinds) != 1 || f.links.unbinds[0].LeaseEpoch != attachedEpoch || f.links.unbinds[0].IdempotencyKey != bind.req.IdempotencyKey {
		t.Errorf("unbinds = %+v, want the one route this call created given back under its bind key", f.links.unbinds)
	}
	if code := f.claimCode(t); code != sessionstore.ReconcileErrorLapsed {
		t.Errorf("claim state = %q, want the claim released", code)
	}
}

// TestACandidateThatDoesNotAdvertiseAttachIsExcludedAndLogged is the mixed
// fleet: the best-ranked Host predates attach, is skipped by name, and the
// session lands on the next.
func TestACandidateThatDoesNotAdvertiseAttachIsExcludedAndLogged(t *testing.T) {
	t.Parallel()

	f := newAttachFixture(t, nil)
	f.publishTarget(t, "host-old", 9, sessionwire.HostIsolationClassCrossTenantIsolated)
	f.publishTarget(t, "host-new", 2, sessionwire.HostIsolationClassCrossTenantIsolated)
	f.links.on("host-old", fail(fmt.Errorf("link: %w", ErrAttachUnsupported)))

	result := f.mustPlace(t)
	if result.Decision.Outcome != OutcomeAttachPooled || result.Decision.Target.HostID != "host-new" {
		t.Fatalf("decision = %+v, want host-new", result.Decision)
	}
	if !slices.Equal(result.Excluded, []sessionwire.HostID{"host-old"}) {
		t.Errorf("Excluded = %v, want [host-old]", result.Excluded)
	}
	if len(result.Refused) != 0 {
		t.Errorf("Refused = %+v, want an unsupported Host reported as excluded, not refused", result.Refused)
	}
	var logged struct {
		Msg    string `json:"msg"`
		HostID string `json:"host_id"`
	}
	line, _, _ := strings.Cut(f.logs.String(), "\n")
	if err := json.Unmarshal([]byte(line), &logged); err != nil {
		t.Fatalf("no exclusion was logged (%q): %v", f.logs.String(), err)
	}
	if logged.HostID != "host-old" || !strings.Contains(logged.Msg, "hostlink.attach") {
		t.Errorf("logged %+v, want the exclusion naming host-old", logged)
	}
}

// TestEveryNonEpochRefusalMovesToTheNextCandidate holds the refusal table:
// each code other than epoch_mismatch is this candidate's answer and the next
// ranked candidate is asked. runtime_unavailable is a row: from a Host that
// advertised attach it is a genuine refusal, not a capability fact.
func TestEveryNonEpochRefusalMovesToTheNextCandidate(t *testing.T) {
	t.Parallel()

	for _, refusal := range []sessionwire.HostLinkError{
		{Code: sessionwire.HostLinkErrorRuntimeMismatch, RuntimeCompatibilityID: "runtime-other"},
		{Code: sessionwire.HostLinkErrorNoCapacity},
		{Code: sessionwire.HostLinkErrorNotAdmitting},
		{Code: sessionwire.HostLinkErrorRuntimeUnavailable},
		{Code: sessionwire.HostLinkErrorReleasing},
	} {
		t.Run(string(refusal.Code), func(t *testing.T) {
			t.Parallel()
			f := newAttachFixture(t, nil)
			f.publishTarget(t, "host-a", 9, sessionwire.HostIsolationClassCrossTenantIsolated)
			f.publishTarget(t, "host-b", 2, sessionwire.HostIsolationClassCrossTenantIsolated)
			f.links.on("host-a", refuse(refusal))

			result := f.mustPlace(t)
			if result.Decision.Target.HostID != "host-b" || result.Attached.HostID != "host-b" {
				t.Fatalf("decision = %+v, want host-b after host-a refused", result.Decision)
			}
			wantRefused := []CandidateRefusal{{HostID: "host-a", HostGeneration: 1, Code: refusal.Code}}
			if !slices.Equal(result.Refused, wantRefused) {
				t.Errorf("Refused = %+v, want %+v", result.Refused, wantRefused)
			}
			if result.Replacements != 0 || len(f.waits.waits) != 0 {
				t.Errorf("Replacements=%d waits=%v, want no re-placement for a candidate refusal", result.Replacements, f.waits.waits)
			}
			if !slices.Equal(f.links.attachedHosts(), []sessionwire.HostID{"host-a", "host-b"}) {
				t.Errorf("asked %v, want host-a then host-b", f.links.attachedHosts())
			}
		})
	}
}

// TestEveryCandidateRefusingIsNoCapacityNotAnError keeps an exhausted page in
// the result channel, as Decide does.
func TestEveryCandidateRefusingIsNoCapacityNotAnError(t *testing.T) {
	t.Parallel()

	f := newAttachFixture(t, nil)
	f.publishTarget(t, "host-a", 9, sessionwire.HostIsolationClassCrossTenantIsolated)
	f.links.on("host-a", refuse(sessionwire.HostLinkError{Code: sessionwire.HostLinkErrorNoCapacity}))

	result := f.mustPlace(t)
	if result.Decision.Outcome != OutcomeNoCapacity || result.Bound || len(f.links.binds) != 0 {
		t.Fatalf("result = %+v binds=%v, want no capacity and no bind", result, f.links.binds)
	}
}

// TestAnEpochMismatchRerunsPlacementAndNeverBindsWithTheHoldersEpoch is section
// 15 step 5. The refusal's current_lease_epoch is the OTHER holder's; the
// registry is refreshed, and when it now shows the holder the wake is bound
// with the REGISTRY's epoch. No bind anywhere names the refusal's epoch.
func TestAnEpochMismatchRerunsPlacementAndNeverBindsWithTheHoldersEpoch(t *testing.T) {
	t.Parallel()

	f := newAttachFixture(t, nil)
	f.publishTarget(t, "host-a", 9, sessionwire.HostIsolationClassCrossTenantIsolated)
	f.links.on("host-a", refuse(sessionwire.HostLinkError{Code: sessionwire.HostLinkErrorEpochMismatch, CurrentLeaseEpoch: holderEpoch}))
	// The registry catches up during the backoff: the holder registers.
	f.waits.hook = func(int) { f.putOwner(t, sessionwire.HostPlacementPooled) }

	result := f.mustPlace(t, "cmd-1")
	if result.Decision.Outcome != OutcomeReuseOwner || result.Decision.Owner.HostID != "host-owner" {
		t.Fatalf("decision = %+v, want the holder the refreshed registry shows", result.Decision)
	}
	if result.Replacements != 1 || !slices.Equal(f.waits.waits, []time.Duration{10 * time.Millisecond}) {
		t.Errorf("Replacements=%d waits=%v, want one re-placement after one backoff", result.Replacements, f.waits.waits)
	}
	for _, bind := range f.links.binds {
		if bind.req.LeaseEpoch == holderEpoch {
			t.Fatalf("a bind named the refusal's holder epoch %d: %+v", holderEpoch, bind.req)
		}
	}
	if len(f.links.binds) != 1 || f.links.binds[0].req.LeaseEpoch != 6 || f.links.binds[0].req.HostID != "host-owner" {
		t.Errorf("binds = %+v, want one bind to host-owner at the registry's epoch 6", f.links.binds)
	}
	if result.Attached != (sessionwire.HostLinkRegistryObservation{}) {
		t.Errorf("Attached = %+v, want nothing attached by this call", result.Attached)
	}
	if !slices.Equal(f.links.delivered, []sessionwire.CommandID{"cmd-1"}) {
		t.Errorf("delivered = %v, want the wake delivered to the holder", f.links.delivered)
	}
	if len(result.Refused) != 1 || result.Refused[0].Code != sessionwire.HostLinkErrorEpochMismatch {
		t.Errorf("Refused = %+v, want the one epoch_mismatch", result.Refused)
	}
}

// TestEpochMismatchReplacementIsBounded holds the bound and the backoff: a
// registry that never catches up costs exactly ReplaceAttempts attaches and
// doubling waits between them, and ends in ErrRegistryStale.
func TestEpochMismatchReplacementIsBounded(t *testing.T) {
	t.Parallel()

	f := newAttachFixture(t, nil)
	f.publishTarget(t, "host-a", 9, sessionwire.HostIsolationClassCrossTenantIsolated)
	f.publishTarget(t, "host-b", 2, sessionwire.HostIsolationClassCrossTenantIsolated)
	f.links.on("host-a", refuse(sessionwire.HostLinkError{Code: sessionwire.HostLinkErrorEpochMismatch, CurrentLeaseEpoch: holderEpoch}))

	result, err := f.place(t)
	if !errors.Is(err, ErrRegistryStale) {
		t.Fatalf("Reconcile = (%+v, %v), want ErrRegistryStale", result, err)
	}
	if got := f.links.attachedHosts(); !slices.Equal(got, []sessionwire.HostID{"host-a", "host-a", "host-a"}) {
		t.Errorf("attaches = %v, want host-a three times: a stale answer abandons the page, it does not try host-b", got)
	}
	if !slices.Equal(f.waits.waits, []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}) {
		t.Errorf("waits = %v, want [10ms 20ms]", f.waits.waits)
	}
	if result.Replacements != 2 || len(f.links.binds) != 0 {
		t.Errorf("Replacements=%d binds=%v, want 2 and none", result.Replacements, f.links.binds)
	}
	if code := f.claimCode(t); code != sessionstore.ReconcileErrorLapsed {
		t.Errorf("claim state = %q, want the claim released on the stale path too", code)
	}
}

// TestAFailureAfterTheRequestLeftAbortsWithoutAskingAnotherHost is Core's
// "a failure that is not a placement outcome carries no code": the Host may
// have acted, so no second attach is put in flight.
func TestAFailureAfterTheRequestLeftAbortsWithoutAskingAnotherHost(t *testing.T) {
	t.Parallel()

	f := newAttachFixture(t, nil)
	f.publishTarget(t, "host-a", 9, sessionwire.HostIsolationClassCrossTenantIsolated)
	f.publishTarget(t, "host-b", 2, sessionwire.HostIsolationClassCrossTenantIsolated)
	transport := errors.New("rpc: internal error")
	f.links.on("host-a", fail(transport))

	_, err := f.place(t)
	if !errors.Is(err, transport) {
		t.Fatalf("Reconcile = %v, want the transport failure", err)
	}
	if got := f.links.attachedHosts(); !slices.Equal(got, []sessionwire.HostID{"host-a"}) {
		t.Errorf("attaches = %v, want host-a only", got)
	}
}

// TestAnUnreachableCandidateIsSkipped is the one transport failure that moves
// on: the request never left this process.
func TestAnUnreachableCandidateIsSkipped(t *testing.T) {
	t.Parallel()

	f := newAttachFixture(t, nil)
	f.publishTarget(t, "host-a", 9, sessionwire.HostIsolationClassCrossTenantIsolated)
	f.publishTarget(t, "host-b", 2, sessionwire.HostIsolationClassCrossTenantIsolated)
	f.links.on("host-a", fail(fmt.Errorf("dial: %w", ErrHostUnreachable)))

	result := f.mustPlace(t)
	if result.Decision.Target.HostID != "host-b" || !slices.Equal(result.Unreachable, []sessionwire.HostID{"host-a"}) {
		t.Fatalf("result = %+v, want host-b with host-a unreachable", result)
	}
}

// TestTheOwnerIsRereadUnderTheClaim is the read-before-claim window: an owner
// that appears between Reconcile's first read and the claim must stop the
// attach.
func TestTheOwnerIsRereadUnderTheClaim(t *testing.T) {
	t.Parallel()

	directory := &lateOwnerDirectory{}
	f := newAttachFixture(t, directory)
	// The fixture opens the store; the late directory reads through it.
	directory.Directory = mustDirectory(t, f.store)
	f.publishTarget(t, "host-a", 9, sessionwire.HostIsolationClassCrossTenantIsolated)
	f.putOwner(t, sessionwire.HostPlacementPooled)

	result := f.mustPlace(t)
	if result.Decision.Outcome != OutcomeReuseOwner {
		t.Fatalf("decision = %+v, want the owner that appeared before the claim", result.Decision)
	}
	if directory.reads != 2 {
		t.Errorf("registry reads = %d, want the pre-claim miss and the re-read under the claim", directory.reads)
	}
	if len(f.links.attaches) != 0 {
		t.Errorf("attaches = %+v, want none for a session that already has an owner", f.links.attaches)
	}
}

// TestATenantExclusivePooledCandidateIsNeverAskedToAttach is section 12: the
// restriction is enforced before any Host is asked.
func TestATenantExclusivePooledCandidateIsNeverAskedToAttach(t *testing.T) {
	t.Parallel()

	f := newAttachFixture(t, nil)
	f.publishTarget(t, "host-exclusive", 50, sessionwire.HostIsolationClassTenantExclusive)

	result := f.mustPlace(t)
	if result.Decision.Outcome != OutcomeNoCapacity || len(f.links.attaches) != 0 {
		t.Fatalf("result = %+v attaches=%v, want no capacity and no attach", result, f.links.attaches)
	}
}

// TestARouteThisCallDidNotCreateIsLeftInPlace: a viewer's route survives.
func TestARouteThisCallDidNotCreateIsLeftInPlace(t *testing.T) {
	t.Parallel()

	// A viewer's route to the SAME Host the attach lands on: the only
	// pre-existing route the real pool lets a placement bind over. (It used
	// to be a route to another Host, which the real pool refuses; that case
	// is TestAStaleRouteToAnotherHostIsReportedAsABindAfterAttach.)
	f := newAttachFixture(t, nil)
	f.publishTarget(t, "host-a", 9, sessionwire.HostIsolationClassCrossTenantIsolated)
	f.links.route = "host-a"

	result := f.mustPlace(t, "cmd-1")
	if !result.Bound || len(f.links.unbinds) != 0 {
		t.Fatalf("Bound=%t unbinds=%+v, want the existing route left alone", result.Bound, f.links.unbinds)
	}
	if host, routed := f.links.RouteFor(testTenant, testSession); !routed || host != "host-a" {
		t.Fatalf("the viewer's route is %q/%t after placement, want it kept", host, routed)
	}
}

// TestAStaleRouteToAnotherHostIsReportedAsABindAfterAttach is the reachable
// conflict: a route this replica still holds to a DIFFERENT Host when the
// attach lands elsewhere. The pool refuses the bind locally, placement reports
// the attachment, and the stale route is not touched.
func TestAStaleRouteToAnotherHostIsReportedAsABindAfterAttach(t *testing.T) {
	t.Parallel()

	f := newAttachFixture(t, nil)
	f.publishTarget(t, "host-a", 9, sessionwire.HostIsolationClassCrossTenantIsolated)
	f.links.route = "host-viewer"

	result, err := f.place(t, "cmd-1")
	if !errors.Is(err, ErrBindAfterAttach) || result.Attached.HostID != "host-a" || result.Bound {
		t.Fatalf("Reconcile = (%+v, %v), want ErrBindAfterAttach with the attachment reported", result, err)
	}
	if len(f.links.unbinds) != 0 || f.links.route != "host-viewer" {
		t.Fatalf("unbinds=%+v route=%q, want the other route untouched", f.links.unbinds, f.links.route)
	}
}

// TestABindRefusedAfterAnAttachIsReportedWithTheAttachment keeps the two facts
// apart: the session is resident, and this replica has no route.
func TestABindRefusedAfterAnAttachIsReportedWithTheAttachment(t *testing.T) {
	t.Parallel()

	f := newAttachFixture(t, nil)
	f.publishTarget(t, "host-a", 9, sessionwire.HostIsolationClassCrossTenantIsolated)
	f.links.bindErr = errors.New("bind refused")

	result, err := f.place(t, "cmd-1")
	if !errors.Is(err, ErrBindAfterAttach) {
		t.Fatalf("Reconcile = %v, want ErrBindAfterAttach", err)
	}
	if result.Attached.LeaseEpoch != attachedEpoch || result.Bound || len(f.links.delivered) != 0 {
		t.Errorf("result = %+v delivered=%v, want the attachment reported and nothing delivered", result, f.links.delivered)
	}
}

// TestAnOwnedSessionIsWokenWithTheRegistrysEpochAndNoClaim is the pre-claim
// path: section 15 step 1 binds with the registry's epoch and takes no claim.
func TestAnOwnedSessionIsWokenWithTheRegistrysEpochAndNoClaim(t *testing.T) {
	t.Parallel()

	f := newAttachFixture(t, nil)
	f.putOwner(t, sessionwire.HostPlacementPooled)

	result := f.mustPlace(t, "cmd-1")
	if result.Decision.Outcome != OutcomeReuseOwner || len(f.links.attaches) != 0 {
		t.Fatalf("result = %+v attaches=%v, want the owner reused and no attach", result, f.links.attaches)
	}
	if len(f.links.binds) != 1 || f.links.binds[0].req.LeaseEpoch != 6 {
		t.Fatalf("binds = %+v, want one at the registry's epoch 6", f.links.binds)
	}
	if code := f.claimCode(t); code != sessionstore.ReconcileErrorNotFound {
		t.Errorf("claim state = %q, want no claim for an owned session", code)
	}
	// With nothing to wake, the owned path touches no Host at all.
	f.links.binds = nil
	if result := f.mustPlace(t); result.Bound || len(f.links.binds) != 0 {
		t.Errorf("an empty wake bound %+v", f.links.binds)
	}
}

// TestTheAttachModeFollowsTheCatalog holds attachMode on each field it reads.
func TestTheAttachModeFollowsTheCatalog(t *testing.T) {
	t.Parallel()

	for name, row := range map[string]struct {
		record sessionstore.CatalogRecord
		want   sessionwire.HostLinkAttachMode
	}{
		"fresh":      {sessionstore.CatalogRecord{}, sessionwire.HostLinkAttachModeCreate},
		"journal":    {sessionstore.CatalogRecord{LastJournalSeq: 3}, sessionwire.HostLinkAttachModeRestore},
		"checkpoint": {sessionstore.CatalogRecord{Checkpoint: sessionstore.CheckpointSummary{JournalSeq: 2}}, sessionwire.HostLinkAttachModeRestore},
		"both":       {sessionstore.CatalogRecord{LastJournalSeq: 3, Checkpoint: sessionstore.CheckpointSummary{JournalSeq: 2}}, sessionwire.HostLinkAttachModeRestore},
	} {
		if got := attachMode(row.record); got != row.want {
			t.Errorf("%s: attachMode = %q, want %q", name, got, row.want)
		}
	}
}

// TestTheAttachKeyNamesTheIntentAndNotTheHost: section 15 step 5 retries
// through the SAME key, possibly to another Host.
func TestTheAttachKeyNamesTheIntentAndNotTheHost(t *testing.T) {
	t.Parallel()

	record := sessionstore.CatalogRecord{TenantID: testTenant, SessionID: testSession, AgentID: testAgent, RuntimeCompatibilityID: testRuntime, DesiredGeneration: 1}
	create := sessionwire.HostLinkAttachModeCreate
	a := attachRequest(record, sessionwire.HostLinkCapacityReport{HostID: "host-a", HostGeneration: 1}, create, testActor)
	b := attachRequest(record, sessionwire.HostLinkCapacityReport{HostID: "host-b", HostGeneration: 9}, create, testActor)
	if a.IdempotencyKey != b.IdempotencyKey {
		t.Errorf("two candidates got two keys: %q vs %q", a.IdempotencyKey, b.IdempotencyKey)
	}
	if a.HostID != "host-a" || a.HostGeneration != 1 || b.HostID != "host-b" || b.HostGeneration != 9 {
		t.Errorf("the fence is not the candidate's: %+v %+v", a, b)
	}
	differs := func(name string, mutate func(*sessionstore.CatalogRecord), mode sessionwire.HostLinkAttachMode) {
		changed := record
		mutate(&changed)
		if attachKey(changed, mode) == attachKey(record, create) {
			t.Errorf("%s: key unchanged", name)
		}
	}
	differs("session", func(r *sessionstore.CatalogRecord) { r.SessionID = "session-b" }, create)
	differs("tenant", func(r *sessionstore.CatalogRecord) { r.TenantID = "tenant-b" }, create)
	differs("agent", func(r *sessionstore.CatalogRecord) { r.AgentID = "agent-b" }, create)
	differs("runtime", func(r *sessionstore.CatalogRecord) { r.RuntimeCompatibilityID = "runtime-b" }, create)
	differs("generation", func(r *sessionstore.CatalogRecord) { r.DesiredGeneration = 2 }, create)
	differs("mode", func(*sessionstore.CatalogRecord) {}, sessionwire.HostLinkAttachModeRestore)
}

// TestAReconcilerWithLinksNeedsAnActor: an attach without a requesting service
// identity would be refused by Core at marshal; refuse it at composition.
func TestAReconcilerWithLinksNeedsAnActor(t *testing.T) {
	t.Parallel()

	f := newFixture(t, sessionwire.HostPlacementPooled, "factory-1")
	base := Config{
		Directory: mustDirectory(t, f.store), Catalog: f.store, Claims: f.store,
		Clock: f.clock, HolderID: "factory-1", ClaimTTL: time.Minute, CandidateLimit: 8,
		Links: newScriptedLinks(),
	}
	for name, mutate := range map[string]func(*Config){
		"no actor":         func(*Config) {},
		"negative bound":   func(c *Config) { c.ActorID = testActor; c.ReplaceAttempts = -1 },
		"negative backoff": func(c *Config) { c.ActorID = testActor; c.ReplaceBackoff = -time.Second },
	} {
		cfg := base
		mutate(&cfg)
		if _, err := NewReconciler(cfg); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%s: NewReconciler = %v, want ErrInvalidConfig", name, err)
		}
	}
	cfg := base
	cfg.ActorID = testActor
	if _, err := NewReconciler(cfg); err != nil {
		t.Errorf("control: NewReconciler = %v, want a valid composition", err)
	}
}
