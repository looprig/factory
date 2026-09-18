package admission

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	"github.com/looprig/factory/internal/command"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage/memstore"
)

// These cases drive the non-create commands and the disposition deadline sweep
// against the RELEASED store, because every rule they hold is the store's: the
// family a command lands in, binding equality, content-identity comparison on a
// retry, the legacy-session refusal's spelling, and which records the reject
// edge will and will not write. A fake could only restate them.

// movableClock is one timeline shared by the store and the service, so a case
// can move past an apply deadline or a claim expiry on both at once.
type movableClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *movableClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *movableClock) AfterFunc(time.Duration, func()) func() bool {
	return func() bool { return true }
}

func (c *movableClock) set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}

// sequentialIDs mints distinct proposed runtime identities.
type sequentialIDs struct {
	mu sync.Mutex
	n  int
}

func (s *sequentialIDs) NewUUID() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n++
	return "runtime-command-" + strings.Repeat("x", s.n), nil
}

type commandLane struct {
	store     *sessionstore.Store
	clock     *movableClock
	service   *Service
	directory *serviceDirectory
	principal identity.Principal
}

func newCommandLane(t *testing.T, options ...sessionstore.Option) *commandLane {
	t.Helper()
	clock := &movableClock{now: serviceNow}
	store, err := sessionstore.Open(context.Background(), memstore.New(), append([]sessionstore.Option{sessionstore.WithClock(clock)}, options...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(context.Background()); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	f := newServiceFixture(t)
	svc, err := NewService(Config{
		Authorizer: f.auth, Targets: f.targets, Catalog: store, Commands: store,
		Directory: f.directory, Clock: clock, IDs: &sequentialIDs{},
		PublicCreates: store,
		Binding:       SessionBindingTemplate{StorageBindingID: "storage-a", BindingVersion: "v1"},
		ApplyDeadline: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &commandLane{store: store, clock: clock, service: svc, directory: f.directory, principal: f.principal}
}

// create makes a disposition session through the real create path.
func (l *commandLane) create(t *testing.T, session sessionwire.SessionID) sessionstore.DispositionInboxEntry {
	t.Helper()
	entry, created, err := l.service.AdmitCreate(context.Background(), l.principal, createRequest("create-"+string(session), string(session), smallBlocks))
	if err != nil || !created {
		t.Fatalf("AdmitCreate(%s) = (%v, %v)", session, created, err)
	}
	return entry
}

func (l *commandLane) input(session sessionwire.SessionID, command, text string) (sessionstore.DispositionInboxEntry, bool, error) {
	return l.service.AdmitInput(context.Background(), l.principal, sessionwire.InputRequest{
		CommandEnvelope: envelope(command), SessionID: session, Blocks: []byte(`[{"text":"` + text + `"}]`),
	})
}

func (l *commandLane) catalogBinding(t *testing.T, session sessionwire.SessionID) sessionstore.SessionBinding {
	t.Helper()
	entry, err := l.store.GetCatalogEntry(context.Background(), sessionstore.GetCatalogEntryRequest{TenantID: l.principal.Tenant(), SessionID: session})
	if err != nil {
		t.Fatalf("GetCatalogEntry: %v", err)
	}
	return entry.Record.Binding
}

// TestEveryCommandKindIsAdmittedIntoTheSessionsDispositionInbox is the family
// rule. Each kind must land in the DISPOSITION inbox under the session's own
// binding, and nothing may land in the legacy inbox: a legacy row on a
// disposition session is a command no Host can reach, and a mixed-family
// session is exactly the state this change removes.
func TestEveryCommandKindIsAdmittedIntoTheSessionsDispositionInbox(t *testing.T) {
	lane := newCommandLane(t)
	const session = sessionwire.SessionID("session-family")
	lane.create(t, session)
	binding := lane.catalogBinding(t, session)
	if binding.ProtocolMode != sessionstore.ProtocolModeDisposition {
		t.Fatalf("the created session is bound %q, want disposition", binding.ProtocolMode)
	}
	for _, test := range []struct {
		kind  sessionstore.CommandKind
		admit func() (sessionstore.DispositionInboxEntry, bool, error)
	}{
		{CommandInput, func() (sessionstore.DispositionInboxEntry, bool, error) {
			return lane.input(session, "family-input", "hello")
		}},
		{CommandInterrupt, func() (sessionstore.DispositionInboxEntry, bool, error) {
			return lane.service.AdmitInterrupt(context.Background(), lane.principal, sessionwire.InterruptRequest{CommandEnvelope: envelope("family-interrupt"), SessionID: session})
		}},
		{CommandRestore, func() (sessionstore.DispositionInboxEntry, bool, error) {
			return lane.service.AdmitRestore(context.Background(), lane.principal, sessionwire.RestoreRequest{CommandEnvelope: envelope("family-restore"), SessionID: session})
		}},
	} {
		t.Run(string(test.kind), func(t *testing.T) {
			entry, created, err := test.admit()
			if err != nil || !created {
				t.Fatalf("admit = (%v, %v)", created, err)
			}
			command := entry.Record.Descriptor.CommandID
			stored, err := lane.store.GetDispositionCommand(context.Background(), sessionstore.GetDispositionCommandRequest{
				TenantID: lane.principal.Tenant(), SessionID: session, CommandID: command,
			})
			if err != nil {
				t.Fatalf("the command is not in the disposition inbox: %v", err)
			}
			if stored.Record.Descriptor.Kind != test.kind || stored.Record.Descriptor.Binding != binding || stored.Record.State != sessionstore.InboxStatePending {
				t.Fatalf("stored = kind %q, binding %+v, state %q; want %q under the session's binding, pending",
					stored.Record.Descriptor.Kind, stored.Record.Descriptor.Binding, stored.Record.State, test.kind)
			}
			if !stored.Record.ApplyDeadline.Equal(serviceNow.Add(time.Minute)) {
				t.Fatalf("apply deadline = %v, want the configured minute after acceptance", stored.Record.ApplyDeadline)
			}
			// And NOT in the legacy one. The store refuses a legacy read of a
			// disposition-bound session, so any answer but that refusal or a
			// plain not-found would be a legacy row.
			if _, err := lane.store.GetCommand(context.Background(), sessionstore.GetCommandRequest{
				TenantID: lane.principal.Tenant(), SessionID: session, CommandID: command,
			}); err == nil {
				t.Fatal("the command is ALSO readable from the legacy inbox")
			}
		})
	}
}

// TestACommandIsAdmittedUnderTheSessionsOwnBinding holds binding equality and
// the retry contract against the store that enforces them.
func TestACommandIsAdmittedUnderTheSessionsOwnBinding(t *testing.T) {
	lane := newCommandLane(t)
	const session = sessionwire.SessionID("session-retry")
	created := lane.create(t, session)

	first, fresh, err := lane.input(session, "retry-input", "hello")
	if err != nil || !fresh {
		t.Fatalf("first = (%v, %v)", fresh, err)
	}
	if first.Record.Descriptor.Binding != lane.catalogBinding(t, session) {
		t.Fatalf("admitted under %+v, want the catalog's own binding", first.Record.Descriptor.Binding)
	}

	t.Run("an identical retry is the original, after the clock moved", func(t *testing.T) {
		lane.clock.set(serviceNow.Add(10 * time.Second))
		defer lane.clock.set(serviceNow)
		retry, fresh, err := lane.input(session, "retry-input", "hello")
		if err != nil || fresh || !reflect.DeepEqual(retry, first) {
			t.Fatalf("retry = (%+v, %v, %v), want the original record", retry, fresh, err)
		}
	})
	t.Run("the same id with different content is command_rejected", func(t *testing.T) {
		if _, _, err := lane.input(session, "retry-input", "goodbye"); !IsCode(err, sessionwire.ErrorCodeCommandRejected) {
			t.Fatalf("error = %v, want command_rejected", err)
		}
	})
	t.Run("the same id under another kind is command_rejected", func(t *testing.T) {
		_, _, err := lane.service.AdmitInterrupt(context.Background(), lane.principal, sessionwire.InterruptRequest{CommandEnvelope: envelope("retry-input"), SessionID: session})
		if !IsCode(err, sessionwire.ErrorCodeCommandRejected) {
			t.Fatalf("error = %v, want command_rejected", err)
		}
	})
	t.Run("the create's own id is command_rejected", func(t *testing.T) {
		_, _, err := lane.input(session, string(created.Record.Descriptor.CommandID), "hello")
		if !IsCode(err, sessionwire.ErrorCodeCommandRejected) {
			t.Fatalf("error = %v, want command_rejected", err)
		}
	})
	t.Run("an oversized input is stored by reference and its retry matches", func(t *testing.T) {
		big := strings.Repeat("y", sessionstore.MaxInboxPayloadBytes)
		first, fresh, err := lane.input(session, "big-input", big)
		if err != nil || !fresh || first.Record.Descriptor.PayloadObject == nil {
			t.Fatalf("first = (%v, %v), object %v", fresh, err, first.Record.Descriptor.PayloadObject)
		}
		retry, fresh, err := lane.input(session, "big-input", big)
		if err != nil || fresh || !reflect.DeepEqual(retry, first) {
			t.Fatalf("retry = (%+v, %v, %v), want the original record", retry, fresh, err)
		}
	})
	t.Run("an input to a session nobody created is session_not_found", func(t *testing.T) {
		if _, _, err := lane.input("session-nobody", "ghost-input", "hello"); !IsCode(err, sessionwire.ErrorCodeSessionNotFound) {
			t.Fatalf("error = %v, want session_not_found", err)
		}
	})
}

// TestACommandForALegacyBoundSessionIsRuntimeUnavailable is the legacy mapping.
// A session some other writer created on the legacy protocol cannot be served:
// no Host can take residency on it. The store refuses the disposition read
// with a catalog error on binding.protocol_mode, and that must reach a caller
// as the classified runtime_unavailable, not as a fault, with nothing written
// to either inbox.
func TestACommandForALegacyBoundSessionIsRuntimeUnavailable(t *testing.T) {
	lane := newCommandLane(t)
	const session = sessionwire.SessionID("session-legacy")
	if _, _, err := lane.store.CreateCatalogEntry(context.Background(), sessionstore.CreateCatalogEntryRequest{
		TenantID: lane.principal.Tenant(), SessionID: session, AgentID: "agent-a",
		RuntimeCompatibilityID: "runtime-v1", CreatedAt: serviceNow, LastActiveAt: serviceNow,
		State: sessionwire.SessionStateIdle, Residency: sessionwire.SessionResidencyCold,
		DesiredPlacement: sessionwire.HostPlacementPooled, IdempotencyKey: "legacy-fixture",
	}); err != nil {
		t.Fatalf("CreateCatalogEntry: %v", err)
	}
	_, _, err := lane.input(session, "legacy-input", "hello")
	if !IsCode(err, sessionwire.ErrorCodeRuntimeUnavailable) || !errors.Is(err, ErrLegacySessionUnsupported) {
		t.Fatalf("error = %v, want runtime_unavailable wrapping ErrLegacySessionUnsupported", err)
	}
	var catalog *sessionstore.CatalogError
	if !errors.As(err, &catalog) || catalog.Field != "binding.protocol_mode" {
		t.Fatalf("error = %v, want the store's own protocol-mode refusal as the cause", err)
	}
	if _, err := lane.store.GetCommand(context.Background(), sessionstore.GetCommandRequest{
		TenantID: lane.principal.Tenant(), SessionID: session, CommandID: "legacy-input",
	}); err == nil {
		t.Fatal("a refused command was written to the legacy inbox")
	}
}

// sweepPrincipal is the service identity a sweep is authorized for.
func sweepPrincipal(t *testing.T) identity.Principal {
	t.Helper()
	p, err := identity.NewPrincipal("tenant-a", "factory-sweeper", identity.KindService)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// TestTheDispositionSweepRejectsOnlyExpiredUnattemptedCommands drives every
// record state the sweep can meet through the released store's own
// transitions, then sweeps every shard once.
//
// It is the "never reject applying" rule measured against the edge that
// enforces it: an APPLYING command past its deadline is left applying, and so
// is a claimed one whose claim is still live; a pending one inside its
// deadline is left pending; only the expired pending command and the expired
// command whose claim lapsed are rejected.
func TestTheDispositionSweepRejectsOnlyExpiredUnattemptedCommands(t *testing.T) {
	lane := newCommandLane(t)
	ctx := context.Background()
	tenant := lane.principal.Tenant()

	type row struct {
		session sessionwire.SessionID
		command sessionwire.CommandID
		want    sessionstore.InboxState
	}
	var rows []row
	admitted := func(session sessionwire.SessionID) sessionstore.DispositionInboxEntry {
		lane.create(t, session)
		entry, _, err := lane.input(session, "sweep-"+string(session), "hello")
		if err != nil {
			t.Fatalf("input(%s): %v", session, err)
		}
		return entry
	}
	claim := func(entry sessionstore.DispositionInboxEntry, expires time.Time) sessionstore.DispositionInboxEntry {
		grant, err := lane.store.AcquireResidency(ctx, sessionstore.AcquireResidencyRequest{TenantID: tenant, SessionID: entry.Record.Descriptor.SessionID})
		if err != nil {
			t.Fatalf("AcquireResidency: %v", err)
		}
		t.Cleanup(func() { _ = grant.Release(context.Background()) })
		claimed, _, err := lane.store.ClaimDispositionCommand(ctx, sessionstore.ClaimDispositionCommandRequest{
			TenantID: tenant, SessionID: entry.Record.Descriptor.SessionID, CommandID: entry.Record.Descriptor.CommandID,
			ExpectedRevision: entry.Revision, Residency: grant, ClaimExpiresAt: expires,
		})
		if err != nil {
			t.Fatalf("ClaimDispositionCommand: %v", err)
		}
		return claimed
	}

	// Pending, expired: rejected. Every create of the lane is pending and
	// expired too, and each is rejected, which is correct: an expired create
	// no Host applied is as dead as an expired input.
	pending := admitted("session-pending")
	rows = append(rows, row{"session-pending", pending.Record.Descriptor.CommandID, sessionstore.InboxStateRejected})

	// Claimed under a claim that lapses before the sweep: rejected.
	lapsed := claim(admitted("session-lapsed"), serviceNow.Add(30*time.Second))
	rows = append(rows, row{"session-lapsed", lapsed.Record.Descriptor.CommandID, sessionstore.InboxStateRejected})

	// Claimed under a claim still live at the sweep: the claim wins the
	// deadline race and the command stays claimed.
	live := claim(admitted("session-live"), serviceNow.Add(90*time.Second))
	rows = append(rows, row{"session-live", live.Record.Descriptor.CommandID, sessionstore.InboxStateClaimed})

	// Applying: an attempt exists, so only a successor runtime may close it.
	applyingClaim := claim(admitted("session-applying"), serviceNow.Add(30*time.Second))
	applying, err := lane.store.BeginDispositionAttempt(ctx, sessionstore.BeginDispositionAttemptRequest{
		TenantID: tenant, SessionID: "session-applying", CommandID: applyingClaim.Record.Descriptor.CommandID,
		ExpectedRevision: applyingClaim.Revision, AttemptID: "attempt-1", JournalEpoch: 1,
		ResidencyEpoch: applyingClaim.Record.Claim.ResidencyEpoch, StartedAt: serviceNow,
	})
	if err != nil {
		t.Fatalf("BeginDispositionAttempt: %v", err)
	}
	if applying.Record.State != sessionstore.InboxStateApplying {
		t.Fatalf("the premise is an applying command; it is %q", applying.Record.State)
	}
	rows = append(rows, row{"session-applying", applying.Record.Descriptor.CommandID, sessionstore.InboxStateApplying})

	// Admitted late enough that it is still inside its deadline at the sweep.
	lane.clock.set(serviceNow.Add(40 * time.Second))
	fresh := admitted("session-fresh")
	rows = append(rows, row{"session-fresh", fresh.Record.Descriptor.CommandID, sessionstore.InboxStatePending})

	// The sweep, past every deadline but the fresh one's and before the live
	// claim's expiry.
	lane.clock.set(serviceNow.Add(70 * time.Second))
	sweeper, err := NewDispositionReconciler(DispositionReconcilerConfig{
		Authorizer: &serviceAuthorizer{}, Due: lane.store, Settlement: lane.store, Claims: lane.store,
		Clock: lane.clock, HolderID: "replica-a", ClaimTTL: 30 * time.Second, PageLimit: 50, MaxPages: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	total := SweepResult{Dispositions: map[Disposition]int{}}
	for range lane.store.ControlShards() {
		result, err := sweeper.Sweep(ctx, sweepPrincipal(t))
		if err != nil {
			t.Fatalf("Sweep: %v", err)
		}
		total.Rejected += result.Rejected
		total.Claimed += result.Claimed
		total.Released += result.Released
		for d, n := range result.Dispositions {
			total.Dispositions[d] += n
		}
	}
	for _, r := range rows {
		got, err := lane.store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{TenantID: tenant, SessionID: r.session, CommandID: r.command})
		if err != nil {
			t.Fatalf("GetDispositionCommand(%s): %v", r.session, err)
		}
		if got.Record.State != r.want {
			t.Errorf("%s: state %q after the sweep, want %q", r.session, got.Record.State, r.want)
		}
		if r.want == sessionstore.InboxStateRejected {
			status, readable := command.StatusForDisposition(got)
			if !readable || status.State != sessionwire.CommandStateRejected || status.Error == nil || status.Error.Code != sessionwire.ErrorCodeRuntimeUnavailable {
				t.Errorf("%s: rejected status = (%+v, %v), want rejected/runtime_unavailable", r.session, status, readable)
			}
		}
	}
	// Five creates are expired too; the fresh session's create is not.
	if total.Rejected != 2+4 || total.Dispositions[DispositionApplying] != 1 || total.Dispositions[DispositionClaimLive] != 1 {
		t.Errorf("sweep totals = rejected %d, dispositions %v; want 6 rejected, one applying and one live claim left alone",
			total.Rejected, total.Dispositions)
	}
	if total.Claimed != total.Released {
		t.Errorf("claimed %d reconciliation claims and released %d", total.Claimed, total.Released)
	}
	// And a retry of a rejected input answers the rejection, not a fault.
	retry, _, err := lane.input("session-pending", string(pending.Record.Descriptor.CommandID), "hello")
	if err != nil || retry.Record.State != sessionstore.InboxStateRejected {
		t.Fatalf("retry after rejection = (%q, %v), want the rejected record", retry.Record.State, err)
	}
}

// TestARejectableCommandBehindMoreThanAPassOfApplyingOnesIsStillRejected is
// the deadline sweep's own progress guarantee. The rows it may never reject --
// applying commands, filed at their long-past deadlines -- sit at the HEAD of
// the deadline-ordered view, and with more of them than one pass reads, a
// sweep that restarted at the head every pass would never reach the expired
// pending command behind them. One shard, one row per pass.
func TestARejectableCommandBehindMoreThanAPassOfApplyingOnesIsStillRejected(t *testing.T) {
	lane := newCommandLane(t, sessionstore.WithControlShards(1))
	ctx := context.Background()
	tenant := lane.principal.Tenant()
	var applying []sessionwire.SessionID
	for _, session := range []sessionwire.SessionID{"session-stuck-1", "session-stuck-2", "session-stuck-3"} {
		created := lane.create(t, session)
		grant, err := lane.store.AcquireResidency(ctx, sessionstore.AcquireResidencyRequest{TenantID: tenant, SessionID: session})
		if err != nil {
			t.Fatalf("AcquireResidency: %v", err)
		}
		t.Cleanup(func() { _ = grant.Release(context.Background()) })
		claimed, _, err := lane.store.ClaimDispositionCommand(ctx, sessionstore.ClaimDispositionCommandRequest{
			TenantID: tenant, SessionID: session, CommandID: created.Record.Descriptor.CommandID,
			ExpectedRevision: created.Revision, Residency: grant, ClaimExpiresAt: serviceNow.Add(30 * time.Second),
		})
		if err != nil {
			t.Fatalf("ClaimDispositionCommand: %v", err)
		}
		if _, err := lane.store.BeginDispositionAttempt(ctx, sessionstore.BeginDispositionAttemptRequest{
			TenantID: tenant, SessionID: session, CommandID: created.Record.Descriptor.CommandID,
			ExpectedRevision: claimed.Revision, AttemptID: "attempt-1", JournalEpoch: 1,
			ResidencyEpoch: claimed.Record.Claim.ResidencyEpoch, StartedAt: serviceNow,
		}); err != nil {
			t.Fatalf("BeginDispositionAttempt: %v", err)
		}
		applying = append(applying, session)
	}
	// The rejectable command is filed BEHIND them: admitted later, so its
	// deadline is later.
	lane.clock.set(serviceNow.Add(10 * time.Second))
	behind := lane.create(t, "session-behind")
	lane.clock.set(serviceNow.Add(2 * time.Minute))

	sweeper, err := NewDispositionReconciler(DispositionReconcilerConfig{
		Authorizer: &serviceAuthorizer{}, Due: lane.store, Settlement: lane.store, Claims: lane.store,
		Clock: lane.clock, HolderID: "replica-a", ClaimTTL: 30 * time.Second, PageLimit: 1, MaxPages: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	for pass := range 6 {
		result, err := sweeper.Sweep(ctx, sweepPrincipal(t))
		if err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		if result.Rejected == 1 {
			if !result.Resumed {
				t.Fatalf("the rejection came from a pass that did not resume; the probe did not put it behind the head")
			}
			got, err := lane.store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{TenantID: tenant, SessionID: "session-behind", CommandID: behind.Record.Descriptor.CommandID})
			if err != nil || got.Record.State != sessionstore.InboxStateRejected {
				t.Fatalf("the command behind = (%q, %v), want rejected", got.Record.State, err)
			}
			for _, session := range applying {
				stuck, err := lane.store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{TenantID: tenant, SessionID: session, CommandID: sessionwire.CommandID("create-" + string(session))})
				if err != nil || stuck.Record.State != sessionstore.InboxStateApplying {
					t.Fatalf("%s = (%q, %v), want still applying", session, stuck.Record.State, err)
				}
			}
			return
		}
	}
	t.Fatal("the expired pending command behind three applying ones was never rejected across 6 passes")
}
