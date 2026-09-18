package admission

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"
)

// ErrInvalidReconcilerConfig reports a reconciler configuration refused before
// it could reach a store.
var ErrInvalidReconcilerConfig = errors.New("admission: invalid reconciler configuration")

// DueCommands is the service-plane due-work query.
//
// It is TWO methods because a sweeper needs both halves and neither can come
// from anywhere else. ControlShards is a READ of the count the backend is
// committed to, not a setting: the shard is a pure function of (TenantID,
// SessionID) computed identically by the writer and the sweeper, so a sweeper
// configured with its own count would silently never visit some shards.
// ListDueCommands answers one shard at a time, deliberately -- a store that
// swept every shard in one call would hold the whole deployment's
// reconciliation in one request's latency.
type DueCommands interface {
	ControlShards() int
	ListDueCommands(context.Context, sessionstore.ListDueCommandsRequest) (sessionstore.DueCommandPage, error)
}

// Settlement is the one durable write this reconciler makes.
//
// It is exactly one method, and the narrowness is the point rather than
// tidiness: the settlements a due row could invite are reject, claim, begin
// applying and complete, and three of those are a LEASE HOLDER's. A reconciler
// holds no session lease and names no epoch, so a seam offering them would be
// authority this package cannot legitimately exercise.
type Settlement interface {
	RejectCommand(context.Context, sessionstore.RejectCommandRequest) (sessionstore.InboxEntry, error)
}

// Claims is the short-lived reconciliation claim of specification section 10.
//
// It SUPPRESSES DUPLICATE WORK AND IS NEVER A FENCE, which is a property of
// SessionStore's record rather than a promise made here: the record cannot name
// ownership, and nothing in that package reads a claim to decide a write. What
// makes two replicas sweeping one shard safe is the compare-and-swap on the
// command's own revision and the store's evidence rule; the claim only stops
// them both paying for it.
type Claims interface {
	AcquireReconciliationClaim(context.Context, sessionstore.AcquireReconciliationClaimRequest) (sessionstore.ReconciliationClaimEntry, error)
	ReleaseReconciliationClaim(context.Context, sessionstore.ReleaseReconciliationClaimRequest) (sessionstore.ReconciliationClaimEntry, error)
}

// ReconcilerConfig is one replica's due-command sweep configuration.
type ReconcilerConfig struct {
	// Authorizer decides the sweep. The method called is AuthorizeServiceSweep
	// and never AuthorizeControl: see Reconciler.Sweep.
	Authorizer Authorizer

	Due        DueCommands
	Settlement Settlement
	Claims     Claims
	Clock      Clock

	// HolderID names this replica in a reconciliation claim. It is not
	// authority -- the claim licenses nothing -- but it must be stable for a
	// process, because extending one's own claim and taking over a crashed
	// replica's lapsed one are the same store write and the holder is what
	// tells them apart.
	HolderID string

	// ClaimTTL is how long this replica claims a session's reconciliation work
	// for.
	ClaimTTL time.Duration

	// PageLimit bounds one due page.
	PageLimit int

	// MaxPages bounds ONE sweep of ONE shard.
	//
	// It is the periodic bound, and it is a bound on a PASS rather than on the
	// work: a shard with more due rows than MaxPages*PageLimit is reported
	// Truncated and finished by the next pass over that shard. That is chosen
	// over draining the shard because the sweep is periodic and the alternative
	// lets one busy shard hold the rotor -- and therefore every other shard's
	// reconciliation -- for as long as its backlog lasts.
	MaxPages int
}

// Reconciler sweeps due commands in fixed control shards, round-robin.
//
// It holds exactly one piece of state, the rotor, and that state is advice
// rather than position: a restarted replica resumes at shard zero and
// re-derives everything else from the store within the call.
type Reconciler struct {
	cfg   ReconcilerConfig
	rotor rotor
}

// rotor is the round-robin shard position, shared by both deadline sweeps.
type rotor struct {
	mu   sync.Mutex
	next int
}

// claimant takes and gives back one replica's reconciliation claims. It is the
// half both deadline sweeps share, so the legacy and the disposition sweep
// cannot come to disagree about what a held, lost or contended claim means.
type claimant struct {
	claims Claims
	holder string
	ttl    time.Duration
}

func (r *Reconciler) claimant() claimant {
	return claimant{claims: r.cfg.Claims, holder: r.cfg.HolderID, ttl: r.cfg.ClaimTTL}
}

// NewReconciler validates a configuration before it can reach a store.
func NewReconciler(cfg ReconcilerConfig) (*Reconciler, error) {
	switch {
	case cfg.Authorizer == nil:
		return nil, fmt.Errorf("%w: Authorizer must not be nil", ErrInvalidReconcilerConfig)
	case cfg.Due == nil:
		return nil, fmt.Errorf("%w: Due must not be nil", ErrInvalidReconcilerConfig)
	case cfg.Settlement == nil:
		return nil, fmt.Errorf("%w: Settlement must not be nil", ErrInvalidReconcilerConfig)
	case cfg.Claims == nil:
		return nil, fmt.Errorf("%w: Claims must not be nil", ErrInvalidReconcilerConfig)
	case cfg.Clock == nil:
		return nil, fmt.Errorf("%w: Clock must not be nil", ErrInvalidReconcilerConfig)
	case cfg.HolderID == "":
		return nil, fmt.Errorf("%w: HolderID must name this replica", ErrInvalidReconcilerConfig)
	case cfg.ClaimTTL <= 0 || cfg.ClaimTTL > sessionstore.MaxReconciliationClaimTTL:
		return nil, fmt.Errorf("%w: ClaimTTL must be positive and at most %v",
			ErrInvalidReconcilerConfig, sessionstore.MaxReconciliationClaimTTL)
	case cfg.PageLimit < 1 || cfg.PageLimit > storage.MaxOrderedPageLimit:
		return nil, fmt.Errorf("%w: PageLimit must be between 1 and %d",
			ErrInvalidReconcilerConfig, storage.MaxOrderedPageLimit)
	case cfg.MaxPages < 1:
		return nil, fmt.Errorf("%w: MaxPages must be positive", ErrInvalidReconcilerConfig)
	}
	return &Reconciler{cfg: cfg}, nil
}

// Disposition is what one sweep did about one due row, or why it did nothing.
//
// It is a closed vocabulary with a rendering for every member including an
// unrecognized one, for the reason placement.Outcome is: a row's fate is
// operational data, and "" is not a fate.
type Disposition uint8

const (
	// DispositionSettleable is the predicate's positive answer and never a
	// recorded outcome: a row that reaches it goes on to be settled, deferred
	// or refused, and the recorded disposition is one of those.
	DispositionSettleable Disposition = iota

	// DispositionRejected means this sweep settled the command as rejected.
	DispositionRejected

	// DispositionUnexpired means the apply deadline has not passed. A Host may
	// still claim and apply the command, so nothing here may settle it.
	DispositionUnexpired

	// DispositionClaimLive means a live command claim holds the row: a Host is
	// working on it now.
	DispositionClaimLive

	// DispositionApplying means an application is in flight. Only the NEXT
	// lease holder can settle it, because the safety of that settlement rests
	// on a journal fence written at an epoch above the applying one -- and this
	// reconciler holds no lease and names no epoch.
	DispositionApplying

	// DispositionTerminal means the command had already reached applied or
	// rejected. inboxDue files a terminal command not-due, so this is a row
	// that settled between the page and the write.
	DispositionTerminal

	// DispositionEvidence means the store refused the settlement because the
	// session's journal does not prove the command had no effect. That is the
	// prefix winning, and it is an ordinary outcome rather than a fault.
	DispositionEvidence

	// DispositionRaceLost means the record moved between the page and the
	// compare-and-swap.
	DispositionRaceLost

	// DispositionDeferred means another replica holds this session's
	// reconciliation claim, so this one did nothing about the row.
	DispositionDeferred

	// DispositionUnrecognized means the record is in a state this build does
	// not know. It is refused rather than guessed at, so a newer store's state
	// cannot be rejected by an older sweeper. Only the disposition sweep
	// reports it: the legacy predicate's arms are total over its own states.
	DispositionUnrecognized
)

func (d Disposition) String() string {
	switch d {
	case DispositionSettleable:
		return "settleable"
	case DispositionRejected:
		return "rejected"
	case DispositionUnexpired:
		return "unexpired"
	case DispositionClaimLive:
		return "claim_live"
	case DispositionApplying:
		return "applying"
	case DispositionTerminal:
		return "terminal"
	case DispositionEvidence:
		return "evidence"
	case DispositionRaceLost:
		return "race_lost"
	case DispositionDeferred:
		return "deferred"
	case DispositionUnrecognized:
		return "unrecognized_state"
	default:
		return "unrecognized"
	}
}

// SweepResult is what one sweep of one shard did, and what producing it cost.
type SweepResult struct {
	// Shard is the control shard this sweep visited.
	Shard int

	// Pages, Examined and Unreadable are the store's own cost report,
	// accumulated across the pages this sweep read. Examined is rows the
	// provider returned and Unreadable is rows the store could not vouch for;
	// both are reported rather than folded, because Examined == Pages*PageLimit
	// with nothing returned is a different state from "nothing is due".
	Pages      int
	Examined   int
	Unreadable int

	// Due is the number of readable due rows this sweep considered.
	Due int

	// Rejected counts the commands this sweep settled.
	Rejected int

	// Claimed and Released count the reconciliation claims this sweep took and
	// gave back. They are separate counters rather than one, because a
	// difference between them is the thing worth seeing.
	Claimed  int
	Released int

	// Dispositions counts every recorded outcome by kind. DispositionSettleable
	// never appears in it.
	Dispositions map[Disposition]int

	// Queries counts the store calls this sweep made that reach a provider:
	// due pages, claim acquisitions, claim releases and settlements.
	// ControlShards is deliberately not counted -- it is a read of a decision
	// the store already holds in memory and makes no provider request.
	Queries int

	// ReleaseFailures counts releases that failed.
	//
	// A release failure is deliberately SWALLOWED -- replacing a correct
	// settlement with a release error would report the wrong thing about the
	// command, and a claim licenses nothing, so a claim left to lapse costs a
	// delayed takeover of work that was already safe to do concurrently. This
	// counter is what keeps "swallowed" from meaning "invisible", which is the
	// shape a destructive call site with no observable takes.
	ReleaseFailures int

	// Truncated reports that MaxPages was reached with a continuation still
	// outstanding. The remaining rows are still due and are met by the next
	// pass over this shard.
	Truncated bool
}

// Sweep reconciles the next control shard, round-robin.
//
// STEP ONE'S TWO HALVES ARE ONE CALL. This is a cross-tenant control-plane
// query -- a shard holds whichever tenants hash into it, so a page can mix them
// -- and AuthorizeServiceSweep is the decision that a tenant principal can
// never reach one. AuthorizeControl is deliberately NOT called here, and not
// because it would be redundant: a command arrives at the httpapi or clientlink
// edge already authorized, admission repeats that decision as the durable
// mutation boundary, and a THIRD authorizing caller is how an unauthorized
// caller acquires a path. What this sweep authorizes is the sweep, once, and
// the rows it then settles belong to no caller at all.
func (r *Reconciler) Sweep(ctx context.Context, principal identity.Principal) (SweepResult, error) {
	if err := r.cfg.Authorizer.AuthorizeServiceSweep(ctx, principal); err != nil {
		return SweepResult{}, err
	}
	shard, err := r.rotate()
	if err != nil {
		return SweepResult{}, err
	}
	result := SweepResult{Shard: shard, Dispositions: map[Disposition]int{}}
	held := map[sessionKey]bool{}
	sweepErr := r.page(ctx, shard, &result, held)
	// The release runs on EVERY path, including the failing one. A sweep that
	// gave up halfway still holds the claims it took, and leaving them to lapse
	// would keep every other replica off those sessions for the whole TTL.
	r.claimant().release(ctx, held, &result)
	return result, sweepErr
}

// rotate advances the round-robin rotor and returns the shard to sweep.
//
// It advances BEFORE the work and independently of its outcome. A rotor moved
// on success only would let one shard whose page read fails -- a poisoned row,
// a partition, a misconfigured provider -- hold the rotor forever, and every
// other shard's reconciliation with it.
//
// The count is read from the store on every pass rather than cached, because it
// is a persisted decision of the backend rather than a setting of this replica:
// a sweeper holding its own count would silently never visit some shards.
func (r *Reconciler) rotate() (int, error) {
	return r.rotor.rotate(r.cfg.Due.ControlShards())
}

func (r *rotor) rotate(shards int) (int, error) {
	if shards < sessionstore.MinControlShards {
		return 0, fmt.Errorf("admission: the store reports %d control shards, so no shard can be swept", shards)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.next >= shards {
		// The count shrank under a running replica. Restarting the rotor is
		// the only answer that visits every shard; continuing from a position
		// past the end would silently sweep shard zero forever.
		r.next = 0
	}
	shard := r.next
	r.next = (shard + 1) % shards
	return shard, nil
}

// sessionKey is one session's identity, as a comparable map key. The
// reconciliation claim is per SESSION while the settlement is per COMMAND, so a
// page carrying several of one session's commands must not pay for the claim
// several times.
type sessionKey struct {
	tenant  sessionwire.TenantID
	session sessionwire.SessionID
}

// page reads one shard's due rows, bounded by MaxPages.
//
// The bound is on the PASS. A shard with more due rows than MaxPages*PageLimit
// is reported Truncated and finished by the next pass over it; draining it
// instead would let one busy shard hold the rotor for as long as its backlog
// lasts, which is the starvation the round robin exists to prevent.
func (r *Reconciler) page(ctx context.Context, shard int, result *SweepResult, held map[sessionKey]bool) error {
	now := r.cfg.Clock.Now()
	var cursor sessionwire.Cursor
	for range r.cfg.MaxPages {
		req := sessionstore.ListDueCommandsRequest{Shard: shard, Limit: r.cfg.PageLimit, Cursor: cursor}
		if cursor == "" {
			// A continuation carries its own bound. Presenting both would be
			// two answers to one question, and the store refuses it.
			req.DueAtOrBefore = now
		}
		result.Queries++
		page, err := r.cfg.Due.ListDueCommands(ctx, req)
		if err != nil {
			return fmt.Errorf("admission: page control shard %d for due commands: %w", shard, err)
		}
		result.Pages++
		result.Examined += page.Examined
		result.Unreadable += page.Unreadable
		result.Due += len(page.Commands)
		for _, due := range page.Commands {
			if err := r.settle(ctx, due.Entry, now, result, held); err != nil {
				return err
			}
		}
		cursor = page.NextCursor
		if cursor == "" {
			return nil
		}
	}
	result.Truncated = true
	return nil
}

// settle decides one due row and, if it may, settles it.
//
// THE "ONLY" IN STEP 3 IS THE PREDICATE'S. settleable refuses a row this
// reconciler has no licence for BEFORE any claim or write, so an unsafe row
// costs the page it arrived on and nothing else.
//
// EACH HALF OF THE RULE HAS A DIFFERENT NUMBER OF AUTHORITIES, and the count is
// worth stating exactly rather than as "checked twice":
//
//   - the APPLY DEADLINE has exactly one, this predicate. RejectCommand
//     deliberately checks no deadline -- only the claiming transitions do -- so
//     nothing downstream would catch a mistake here.
//   - the CLAIM has two. This predicate refuses a live claim, and RejectCommand
//     refuses it again on the record's own terms (InboxErrorClaimHeld), which
//     is what makes a restatement here safe to get wrong.
//   - the APPLICATION EVIDENCE has exactly one, and it is NOT this package.
//     Nothing here reads a journal. The check is not removed, it is left inside
//     RejectCommand, where it runs against the record in the same operation
//     that writes it rather than being read here and then going stale between
//     the read and the write.
func (r *Reconciler) settle(
	ctx context.Context,
	entry sessionstore.InboxEntry,
	now time.Time,
	result *SweepResult,
	held map[sessionKey]bool,
) error {
	if disposition := settleable(entry.Record, now); disposition != DispositionSettleable {
		result.Dispositions[disposition]++
		return nil
	}
	claimed, err := r.claimant().claim(ctx, entry.Record.TenantID, entry.Record.SessionID, now, result, held)
	if err != nil {
		return err
	}
	if !claimed {
		result.Dispositions[DispositionDeferred]++
		return nil
	}
	result.Queries++
	// LeaseEpoch is ZERO, deliberately. A reconciler holds no session lease, and
	// SessionStore's rejection admits a caller that names none: what confines it
	// is the claim rule, which is a property of the record and applies at every
	// epoch. Naming an epoch here would be asserting a view of the session this
	// replica does not have.
	_, err = r.cfg.Settlement.RejectCommand(ctx, sessionstore.RejectCommandRequest{
		TenantID:         entry.Record.TenantID,
		SessionID:        entry.Record.SessionID,
		CommandID:        entry.Record.CommandID,
		ExpectedRevision: entry.Revision,
		Rejection:        expiredCommandRejection(),
	})
	if err == nil {
		result.Rejected++
		result.Dispositions[DispositionRejected]++
		return nil
	}
	if disposition, classified := settlementRefusal(err); classified {
		result.Dispositions[disposition]++
		return nil
	}
	return fmt.Errorf("admission: settle the expired command: %w", err)
}

// expiredCommandRejection is the public detail a settled command carries.
//
// THE CODE IS RUNTIME_UNAVAILABLE AND CORE HAS NO BETTER ONE. The nine codes
// core v0.7.0 declares name no "accepted, and then no runtime applied it before
// the deadline", and runtime_unavailable is the one whose meaning contains it:
// admission already mints it for an unresolvable target, so a client reading it
// here learns the same thing it would have learned had the target been
// unresolvable at admission time.
//
// There is NO MESSAGE, for the reason every other refusal in this package
// carries none: core makes message optional and code the member a client
// branches on, and a message here would be a second prose vocabulary. Retryable
// is false because retrying THIS command id is not a thing a client can do --
// the record is terminal and a retry returns the rejection -- so advertising it
// retryable would send a client into a loop. Resubmitting the work under a new
// command id remains available and is a different request.
func expiredCommandRejection() sessionwire.ErrorDetail {
	return sessionwire.ErrorDetail{Code: sessionwire.ErrorCodeRuntimeUnavailable}
}

// claim takes this session's reconciliation claim, at most once per sweep.
//
// The memo holds BOTH answers. A page carrying four commands of one session
// costs one acquisition whether this replica won the claim or lost it, and a
// loser that re-asked per row would pay the deferral several times over.
func (c claimant) claim(
	ctx context.Context,
	tenant sessionwire.TenantID,
	session sessionwire.SessionID,
	now time.Time,
	result *SweepResult,
	held map[sessionKey]bool,
) (bool, error) {
	key := sessionKey{tenant: tenant, session: session}
	if answer, asked := held[key]; asked {
		return answer, nil
	}
	// TWO ATTEMPTS, AND THE SECOND IS THE STORE'S OWN INSTRUCTION RATHER THAN
	// OPTIMISM. AcquireReconciliationClaim CREATES when it finds no record, and
	// a create that loses that race is reported as a CONFLICT so the caller
	// re-reads and meets the live claim on the ordinary path; the same code
	// answers a lost compare-and-swap over a lapsed claim. Reporting either as
	// a failure would make two replicas starting together fail rather than one
	// of them defer -- which is the ordinary first moment of every deployment,
	// since a session's claim record does not exist until someone reconciles
	// it. The retry is BOUNDED at one: a second conflict means a third racer,
	// and looping would let claim contention become unbounded sweep latency.
	const attempts = 2
	var err error
	for range attempts {
		result.Queries++
		_, err = c.claims.AcquireReconciliationClaim(ctx, sessionstore.AcquireReconciliationClaimRequest{
			TenantID: tenant, SessionID: session,
			HolderID: c.holder, ExpiresAt: now.Add(c.ttl),
		})
		if err == nil {
			held[key] = true
			result.Claimed++
			return true, nil
		}
		// The three answers are three different facts, and collapsing the
		// middle one into an error would make an ordinary multi-replica
		// deployment report failures on its happy path.
		var reconcileErr *sessionstore.ReconcileError
		if !errors.As(err, &reconcileErr) {
			break
		}
		if reconcileErr.Code == sessionstore.ReconcileErrorHeld {
			held[key] = false
			return false, nil
		}
		if reconcileErr.Code != sessionstore.ReconcileErrorConflict {
			break
		}
	}
	// A conflict this replica could not resolve in its bounded attempts is
	// reported as a DEFERRAL rather than as a fault: it means another replica
	// is writing this session's claim right now, which is the same operational
	// fact a held claim reports and the same thing to do about it.
	var conflict *sessionstore.ReconcileError
	if errors.As(err, &conflict) && conflict.Code == sessionstore.ReconcileErrorConflict {
		held[key] = false
		return false, nil
	}
	return false, fmt.Errorf("admission: claim the session's reconciliation work: %w", err)
}

// release gives back exactly the claims this sweep took.
//
// It walks the memo rather than the page, so a session this sweep DEFERRED --
// recorded false -- is not released: releasing another replica's live claim is
// the one way this record could take work away from the replica doing it.
func (c claimant) release(ctx context.Context, held map[sessionKey]bool, result *SweepResult) {
	for key, taken := range held {
		if !taken {
			continue
		}
		result.Queries++
		if _, err := c.claims.ReleaseReconciliationClaim(ctx, sessionstore.ReleaseReconciliationClaimRequest{
			TenantID: key.tenant, SessionID: key.session, HolderID: c.holder,
		}); err != nil {
			result.ReleaseFailures++
			continue
		}
		result.Released++
	}
}

// settleable is the safety predicate of step 3, and it is a TOTAL function of
// the record and the instant: no store call, no clock of its own, nothing it
// can change. Everything a reviewer checks about "only safe pending or expired
// claims" is therefore drivable directly.
//
// The order of the arms decides which REASON is reported, not whether the row
// is safe: every arm below DispositionSettleable refuses, so no reordering can
// admit a row. It is written state-first because the state is the coarsest
// question and an applying record is the one whose settlement belongs to
// somebody else entirely.
//
// The claim is asked before the deadline, and that choice is BEHAVIOUR-NEUTRAL
// rather than an improvement: the only row the two orders disagree about is one
// whose claim is live AND whose deadline is still open, and such a row cannot
// reach this predicate in production, because inboxDue files it at
// min(ApplyDeadline, Claim.ExpiresAt) and so it is not due at either bound.
// Both orders refuse it, and only the reported REASON differs. It is pinned by
// the predicate table anyway -- a unit-level pin on an unreachable case -- so
// the order is not a degree of freedom nothing reads; it is not a claim that
// the other order would have been wrong.
//
// The claim's liveness is spelled as SessionStore's claimHeldAt spells it --
// half-open, live up to but not including the expiry -- and the zero-claim
// conjunct that function carries is deliberately absent rather than forgotten:
// an unclaimed record's expiry is the zero instant, which is before every
// instant a sweep reads, so the conjunct would be a condition no case could
// distinguish.
func settleable(record sessionstore.InboxRecord, now time.Time) Disposition {
	switch {
	case record.State == sessionstore.InboxStateApplied || record.State == sessionstore.InboxStateRejected:
		return DispositionTerminal
	case record.State == sessionstore.InboxStateApplying:
		return DispositionApplying
	case now.Before(record.Claim.ExpiresAt):
		return DispositionClaimLive
	case now.Before(record.ApplyDeadline):
		return DispositionUnexpired
	default:
		return DispositionSettleable
	}
}

// settlementRefusal classifies a refusal of the settlement compare-and-swap
// into an ordinary outcome, or reports that it is a fault.
//
// IT IS FAIL-CLOSED, and the subject of the classification is DERIVED rather
// than trusted: TestEveryInboxErrorCodeIsClassifiedByTheSettlementReader parses
// the pinned store's own source for every InboxErrorCode it declares and
// requires each to be either classified here or recorded as a fault with a
// reason. A code that is neither -- including one a later release adds -- is a
// test failure rather than a silent fall into the default arm, which is the
// exact shape of the classification defect this module has paid for four times.
//
// What each arm means is about the ROW rather than about the store:
//
//   - evidence: the session's journal does not prove the command had no
//     effect. That is the prefix winning.
//   - claim_held / claim_lost: a writer with better authority has the row. Both
//     are reachable from a race with a Host between the page and the write.
//   - conflict: the record moved since the page.
//   - terminal / not_found / deleted: there is nothing left to settle.
func settlementRefusal(err error) (Disposition, bool) {
	var inbox *sessionstore.InboxError
	if !errors.As(err, &inbox) {
		return DispositionSettleable, false
	}
	switch inbox.Code {
	case sessionstore.InboxErrorEvidence:
		return DispositionEvidence, true
	case sessionstore.InboxErrorClaimHeld:
		return DispositionClaimLive, true
	case sessionstore.InboxErrorClaimLost:
		return DispositionApplying, true
	case sessionstore.InboxErrorConflict:
		return DispositionRaceLost, true
	case sessionstore.InboxErrorTerminal, sessionstore.InboxErrorNotFound, sessionstore.InboxErrorDeleted:
		return DispositionTerminal, true
	default:
		return DispositionSettleable, false
	}
}
