package admission

import (
	"context"
	"errors"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	"github.com/looprig/sessionstore"
)

// fakeDispositionDue serves prepared disposition pages and records every
// request, so the bound and the cursor can be read without real rows.
type fakeDispositionDue struct {
	faultInjector
	shards int
	pages  []sessionstore.DispositionDueCommandPage
	served int
	reqs   []sessionstore.ListDueDispositionCommandsRequest
}

func (d *fakeDispositionDue) ControlShards() int { return d.shards }

func (d *fakeDispositionDue) ListDueDispositionCommands(_ context.Context, req sessionstore.ListDueDispositionCommandsRequest) (sessionstore.DispositionDueCommandPage, error) {
	d.reqs = append(d.reqs, req)
	if err := d.enter("ListDueDispositionCommands"); err != nil {
		return sessionstore.DispositionDueCommandPage{}, err
	}
	if d.served >= len(d.pages) {
		return sessionstore.DispositionDueCommandPage{Limit: req.Limit}, nil
	}
	page := d.pages[d.served]
	d.served++
	return page, nil
}

// fakeDispositionSettlement records every rejection it was asked for.
type fakeDispositionSettlement struct {
	faultInjector
	err       error
	replayed  bool
	reqs      []sessionstore.RejectDispositionCommandRequest
	rejectedN int
}

func (s *fakeDispositionSettlement) RejectDispositionCommand(_ context.Context, req sessionstore.RejectDispositionCommandRequest) (sessionstore.DispositionInboxEntry, bool, error) {
	s.reqs = append(s.reqs, req)
	if err := s.enter("RejectDispositionCommand"); err != nil {
		return sessionstore.DispositionInboxEntry{}, false, err
	}
	if s.err != nil {
		return sessionstore.DispositionInboxEntry{}, false, s.err
	}
	entry := sessionstore.DispositionInboxEntry{Record: sessionstore.DispositionInboxRecord{
		Descriptor: sessionstore.DispositionCommandDescriptor{TenantID: req.TenantID, SessionID: req.SessionID, CommandID: req.CommandID},
		State:      sessionstore.InboxStateRejected,
	}, Revision: req.ExpectedRevision + 1}
	if s.replayed {
		return entry, false, nil
	}
	s.rejectedN++
	return entry, true, nil
}

type dispositionFixture struct {
	rec        *DispositionReconciler
	auth       *sweepAuthorizer
	due        *fakeDispositionDue
	settlement *fakeDispositionSettlement
	claims     *fakeClaims
	clock      *sweepClock
	principal  identity.Principal
}

func newDispositionFixture(t *testing.T, tune func(*DispositionReconcilerConfig)) *dispositionFixture {
	t.Helper()
	auth := &sweepAuthorizer{}
	due := &fakeDispositionDue{shards: 3}
	settlement := &fakeDispositionSettlement{}
	claims := &fakeClaims{}
	clock := newSweepClock()
	cfg := DispositionReconcilerConfig{
		Authorizer: auth, Due: due, Settlement: settlement, Claims: claims, Clock: clock,
		HolderID: "replica-fake", ClaimTTL: time.Minute, PageLimit: 2, MaxPages: 3,
	}
	if tune != nil {
		tune(&cfg)
	}
	rec, err := NewDispositionReconciler(cfg)
	if err != nil {
		t.Fatalf("NewDispositionReconciler: %v", err)
	}
	principal, err := identity.NewPrincipal(sweepTenant, "factory-fake", identity.KindService)
	if err != nil {
		t.Fatal(err)
	}
	return &dispositionFixture{rec, auth, due, settlement, claims, clock, principal}
}

// expiredDisposition is a pending disposition command whose deadline passed a
// minute before the fixture clock.
func expiredDisposition(session, command string) sessionstore.DispositionInboxRecord {
	return sessionstore.DispositionInboxRecord{
		Descriptor: sessionstore.DispositionCommandDescriptor{
			TenantID: sweepTenant, SessionID: sessionwire.SessionID(session), CommandID: sessionwire.CommandID(command), Kind: CommandInput,
		},
		AcceptedAt: sweepBase.Add(-time.Hour), ApplyDeadline: sweepBase.Add(-time.Minute),
		State: sessionstore.InboxStatePending,
	}
}

func dispositionPage(cursor sessionwire.Cursor, records ...sessionstore.DispositionInboxRecord) sessionstore.DispositionDueCommandPage {
	page := sessionstore.DispositionDueCommandPage{Limit: 2, Examined: len(records), NextCursor: cursor}
	for i, record := range records {
		page.Commands = append(page.Commands, sessionstore.DispositionInboxEntry{Record: record, Revision: uint64(10 + i), AcceptedOrder: uint64(i + 1)})
	}
	return page
}

// TestTheDispositionPredicateRejectsOnlyExpiredUnattemptedUnclaimedCommands is
// the safety predicate, arm by arm, at each boundary. It is where "never
// reject applying" and the deadline comparison are read directly.
func TestTheDispositionPredicateRejectsOnlyExpiredUnattemptedUnclaimedCommands(t *testing.T) {
	t.Parallel()

	now := sweepBase
	attempt := &sessionstore.DispositionAttempt{AttemptID: "attempt-1", JournalEpoch: 1, ResidencyEpoch: 1, StartedAt: now.Add(-time.Hour)}
	claim := func(expires time.Time) *sessionstore.DispositionClaim {
		return &sessionstore.DispositionClaim{ResidencyEpoch: 1, ExpiresAt: expires}
	}
	for _, test := range []struct {
		name   string
		mutate func(*sessionstore.DispositionInboxRecord)
		want   Disposition
	}{
		{"pending, expired", func(*sessionstore.DispositionInboxRecord) {}, DispositionSettleable},
		{"pending, deadline exactly now", func(r *sessionstore.DispositionInboxRecord) { r.ApplyDeadline = now }, DispositionSettleable},
		{"pending, one nanosecond before the deadline", func(r *sessionstore.DispositionInboxRecord) { r.ApplyDeadline = now.Add(time.Nanosecond) }, DispositionUnexpired},
		{"pending, deadline in an hour", func(r *sessionstore.DispositionInboxRecord) { r.ApplyDeadline = now.Add(time.Hour) }, DispositionUnexpired},
		{"claimed, lapsed claim, expired", func(r *sessionstore.DispositionInboxRecord) {
			r.State, r.Claim = sessionstore.InboxStateClaimed, claim(now.Add(-time.Second))
		}, DispositionSettleable},
		{"claimed, claim lapsing exactly now, expired", func(r *sessionstore.DispositionInboxRecord) {
			r.State, r.Claim = sessionstore.InboxStateClaimed, claim(now)
		}, DispositionSettleable},
		{"claimed, live claim, expired", func(r *sessionstore.DispositionInboxRecord) {
			r.State, r.Claim = sessionstore.InboxStateClaimed, claim(now.Add(time.Second))
		}, DispositionClaimLive},
		{"claimed, lapsed claim, unexpired", func(r *sessionstore.DispositionInboxRecord) {
			r.State, r.Claim, r.ApplyDeadline = sessionstore.InboxStateClaimed, claim(now.Add(-time.Second)), now.Add(time.Minute)
		}, DispositionUnexpired},
		{"APPLYING, expired, lapsed claim", func(r *sessionstore.DispositionInboxRecord) {
			r.State, r.Claim, r.Attempt = sessionstore.InboxStateApplying, claim(now.Add(-time.Hour)), attempt
		}, DispositionApplying},
		{"APPLYING with no attempt member", func(r *sessionstore.DispositionInboxRecord) { r.State = sessionstore.InboxStateApplying }, DispositionApplying},
		{"claimed but carrying an attempt", func(r *sessionstore.DispositionInboxRecord) {
			r.State, r.Claim, r.Attempt = sessionstore.InboxStateClaimed, claim(now.Add(-time.Hour)), attempt
		}, DispositionApplying},
		{"pending but carrying an attempt", func(r *sessionstore.DispositionInboxRecord) { r.Attempt = attempt }, DispositionApplying},
		{"applied", func(r *sessionstore.DispositionInboxRecord) { r.State = sessionstore.InboxStateApplied }, DispositionTerminal},
		{"rejected", func(r *sessionstore.DispositionInboxRecord) { r.State = sessionstore.InboxStateRejected }, DispositionTerminal},
		{"a state this build does not know", func(r *sessionstore.DispositionInboxRecord) { r.State = "reticulating" }, DispositionUnrecognized},
	} {
		t.Run(test.name, func(t *testing.T) {
			record := expiredDisposition("session-a", "command-a")
			test.mutate(&record)
			if got := dispositionSettleable(record, now); got != test.want {
				t.Fatalf("dispositionSettleable = %s, want %s", got, test.want)
			}
		})
	}
}

// TestTheDispositionSweepNeverAsksTheStoreToRejectAnAttemptedCommand is the
// rule at the sweep's own seam: an applying command, however long past its
// deadline, is counted and no rejection is even requested for it. The store
// would refuse it too; this holds that the sweep does not rely on that.
func TestTheDispositionSweepNeverAsksTheStoreToRejectAnAttemptedCommand(t *testing.T) {
	f := newDispositionFixture(t, nil)
	applying := expiredDisposition("session-a", "command-a")
	applying.State = sessionstore.InboxStateApplying
	applying.ApplyDeadline = sweepBase.Add(-24 * time.Hour)
	applying.Claim = &sessionstore.DispositionClaim{ResidencyEpoch: 1, ExpiresAt: sweepBase.Add(-23 * time.Hour)}
	applying.Attempt = &sessionstore.DispositionAttempt{AttemptID: "attempt-1", JournalEpoch: 1, ResidencyEpoch: 1, StartedAt: sweepBase.Add(-24 * time.Hour)}
	expired := expiredDisposition("session-b", "command-b")
	f.due.pages = []sessionstore.DispositionDueCommandPage{dispositionPage("", applying, expired)}

	result, err := f.rec.Sweep(context.Background(), f.principal)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(f.settlement.reqs) != 1 || f.settlement.reqs[0].CommandID != "command-b" {
		t.Fatalf("rejections requested = %+v, want exactly command-b's", f.settlement.reqs)
	}
	if result.Rejected != 1 || result.Dispositions[DispositionApplying] != 1 || result.Dispositions[DispositionRejected] != 1 {
		t.Fatalf("result = %+v", result)
	}
	// The applying session was never even claimed.
	if f.claims.acquired != 1 || f.claims.released != 1 {
		t.Fatalf("claims acquired %d released %d, want one each (the expired session only)", f.claims.acquired, f.claims.released)
	}
}

// TestTheDispositionRejectionNamesTheRowsRevisionAndNoResidency reads the one
// write: the revision the sweep decided on, and a ZERO residency, which is the
// honest statement that this replica holds none and is what confines it to
// commands nobody is working on.
func TestTheDispositionRejectionNamesTheRowsRevisionAndNoResidency(t *testing.T) {
	f := newDispositionFixture(t, nil)
	f.due.pages = []sessionstore.DispositionDueCommandPage{dispositionPage("", expiredDisposition("session-a", "command-a"))}
	if _, err := f.rec.Sweep(context.Background(), f.principal); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	want := sessionstore.RejectDispositionCommandRequest{TenantID: sweepTenant, SessionID: "session-a", CommandID: "command-a", ExpectedRevision: 10}
	if len(f.settlement.reqs) != 1 || f.settlement.reqs[0] != want {
		t.Fatalf("reject requests = %+v, want [%+v]", f.settlement.reqs, want)
	}
}

// TestTheDispositionDueReadIsBoundedAtNowAndContinuesByCursor reads the
// deadline bound. The index files an open command at its apply deadline, so a
// page bounded at now is exactly the expired ones; a later bound would page
// live commands, an earlier one would miss expired ones.
func TestTheDispositionDueReadIsBoundedAtNowAndContinuesByCursor(t *testing.T) {
	f := newDispositionFixture(t, nil)
	f.due.pages = []sessionstore.DispositionDueCommandPage{
		dispositionPage("cursor-1", expiredDisposition("session-a", "command-a")),
		dispositionPage("", expiredDisposition("session-b", "command-b")),
	}
	if _, err := f.rec.Sweep(context.Background(), f.principal); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(f.due.reqs) != 2 {
		t.Fatalf("due reads = %d, want 2", len(f.due.reqs))
	}
	first, second := f.due.reqs[0], f.due.reqs[1]
	if !first.DueAtOrBefore.Equal(sweepBase) || first.Cursor != "" || first.Limit != 2 || first.Shard != 0 {
		t.Fatalf("first read = %+v, want shard 0, limit 2, bounded at exactly now", first)
	}
	if !second.DueAtOrBefore.IsZero() || second.Cursor != "cursor-1" || second.Limit != 2 || second.Shard != 0 {
		t.Fatalf("continuation = %+v, want the cursor and no second bound", second)
	}
}

// TestADispositionSweepIsBoundedAndRotatesThroughEveryShard reads the shard
// bound: one shard per pass, round-robin over the store's own count, at most
// MaxPages pages, and the remainder reported rather than drained.
func TestADispositionSweepIsBoundedAndRotatesThroughEveryShard(t *testing.T) {
	f := newDispositionFixture(t, nil)
	f.due.pages = []sessionstore.DispositionDueCommandPage{
		dispositionPage("cursor-1", expiredDisposition("session-a", "command-a")),
		dispositionPage("cursor-2", expiredDisposition("session-b", "command-b")),
		dispositionPage("cursor-3", expiredDisposition("session-c", "command-c")),
		dispositionPage("", expiredDisposition("session-d", "command-d")),
	}
	result, err := f.rec.Sweep(context.Background(), f.principal)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if !result.Truncated || result.Pages != 3 || len(f.due.reqs) != 3 || result.Rejected != 3 {
		t.Fatalf("result = %+v after %d reads; want 3 pages, 3 rejected and Truncated", result, len(f.due.reqs))
	}
	var shards []int
	for range 4 {
		result, err := f.rec.Sweep(context.Background(), f.principal)
		if err != nil {
			t.Fatalf("Sweep: %v", err)
		}
		shards = append(shards, result.Shard)
	}
	if want := []int{1, 2, 0, 1}; !equalInts(shards, want) {
		t.Fatalf("shards visited = %v, want %v", shards, want)
	}
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestTheDispositionSweepIsAuthorizedBeforeAnyRead holds the service-sweep
// decision in front of the cross-tenant query.
func TestTheDispositionSweepIsAuthorizedBeforeAnyRead(t *testing.T) {
	f := newDispositionFixture(t, nil)
	tenant, err := identity.NewPrincipal(sweepTenant, "user-a", identity.KindActor)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.rec.Sweep(context.Background(), tenant); err == nil {
		t.Fatal("a tenant principal swept the disposition inbox")
	}
	if len(f.due.reqs) != 0 || f.auth.controlCalls != 0 {
		t.Fatalf("reads %d, control decisions %d; want none", len(f.due.reqs), f.auth.controlCalls)
	}
}

// TestEachDispositionRejectAnswerHasOneMeaning drives the store's answers to
// the write.
func TestEachDispositionRejectAnswerHasOneMeaning(t *testing.T) {
	for _, test := range []struct {
		name     string
		err      error
		replayed bool
		want     Disposition
		fault    bool
	}{
		{"written", nil, false, DispositionRejected, false},
		{"the store's idempotent replay", nil, true, DispositionTerminal, false},
		{"an attempt appeared first", &sessionstore.InboxError{Code: sessionstore.InboxErrorState}, false, DispositionApplying, false},
		{"a claim appeared first", &sessionstore.InboxError{Code: sessionstore.InboxErrorClaimHeld}, false, DispositionClaimLive, false},
		{"the record moved", &sessionstore.InboxError{Code: sessionstore.InboxErrorConflict}, false, DispositionRaceLost, false},
		{"already terminal", &sessionstore.InboxError{Code: sessionstore.InboxErrorTerminal}, false, DispositionTerminal, false},
		{"the session is gone", &sessionstore.CatalogError{Code: sessionstore.CatalogErrorNotFound}, false, DispositionTerminal, false},
		{"a provider outage", &sessionstore.InboxError{Code: sessionstore.InboxErrorBackend}, false, 0, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newDispositionFixture(t, nil)
			f.settlement.err, f.settlement.replayed = test.err, test.replayed
			f.due.pages = []sessionstore.DispositionDueCommandPage{dispositionPage("", expiredDisposition("session-a", "command-a"))}
			result, err := f.rec.Sweep(context.Background(), f.principal)
			if test.fault {
				if err == nil {
					t.Fatal("a provider outage was reported as an ordinary outcome")
				}
				if f.claims.released != f.claims.acquired || f.claims.acquired != 1 {
					t.Fatalf("claims acquired %d released %d after a fault; the claim must be given back", f.claims.acquired, f.claims.released)
				}
				return
			}
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if result.Dispositions[test.want] != 1 {
				t.Fatalf("dispositions = %v, want one %s", result.Dispositions, test.want)
			}
			if wantRejected := test.want == DispositionRejected; (result.Rejected == 1) != wantRejected {
				t.Fatalf("Rejected = %d for %s", result.Rejected, test.want)
			}
		})
	}
}

// TestEveryInboxErrorCodeIsClassifiedByTheDispositionSweep is the fail-closed
// classification, derived from the pinned store's vocabulary for the legacy
// reader's reason.
func TestEveryInboxErrorCodeIsClassifiedByTheDispositionSweep(t *testing.T) {
	t.Parallel()

	codes := declaredStringConstants(t, "errors.go", "InboxErrorCode")
	if len(codes) < 10 {
		t.Fatalf("the pinned store declares %d inbox error codes; the derivation is broken", len(codes))
	}
	declared := map[string]bool{}
	for _, code := range codes {
		declared[code] = true
		if _, classified := dispositionRefusal(&sessionstore.InboxError{Code: sessionstore.InboxErrorCode(code)}); !classified {
			if _, recorded := unclassifiedDispositionCodes()[sessionstore.InboxErrorCode(code)]; !recorded {
				t.Errorf("the disposition sweep does not classify %q and it is not recorded as a fault", code)
			}
		}
	}
	for code, reason := range unclassifiedDispositionCodes() {
		if !declared[string(code)] {
			t.Errorf("%q is recorded as a fault (%q) but the pinned store no longer declares it", code, reason)
		}
		if _, classified := dispositionRefusal(&sessionstore.InboxError{Code: code}); classified {
			t.Errorf("%q is recorded as a fault (%q) and the sweep classifies it anyway", code, reason)
		}
	}
	if _, classified := dispositionRefusal(errors.New("the provider could not be reached")); classified {
		t.Error("the sweep classified an error that is not the store's")
	}
}

func unclassifiedDispositionCodes() map[sessionstore.InboxErrorCode]string {
	return map[sessionstore.InboxErrorCode]string{
		sessionstore.InboxErrorInvalid:         "a malformed request is this sweep's own defect",
		sessionstore.InboxErrorCursor:          "a cursor code cannot come from a named write",
		sessionstore.InboxErrorCommandMismatch: "a mismatch is an admission answer",
		sessionstore.InboxErrorIdentity:        "a row disagreeing with its own filing needs an operator",
		sessionstore.InboxErrorEpoch:           "the rejection names no residency, so the fence cannot refuse it",
		sessionstore.InboxErrorOrder:           "the rejection names no cursor position; order means re-read, never a lost race",
		sessionstore.InboxErrorClaimLost:       "only a claim holder's transition can lose its claim",
		sessionstore.InboxErrorDeadline:        "only a claiming transition checks the apply deadline",
		sessionstore.InboxErrorEvidence:        "a pre-dispatch rejection reads no evidence",
		sessionstore.InboxErrorUnknown:         "an unclassified provider failure is a fault",
		sessionstore.InboxErrorBackend:         "a provider failure is a fault",
		sessionstore.InboxErrorMalformed:       "a row this store cannot decode needs an operator",
		sessionstore.InboxErrorVersion:         "a record version this build does not understand needs an operator",
		sessionstore.InboxErrorTooLarge:        "a rejection shrinks no record",
	}
}
