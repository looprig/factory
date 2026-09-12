// Package reconcile retires the durable remnants a crashed control-plane
// operation leaves behind. It holds no lease, names no epoch, and settles
// nothing a session's owner is responsible for.
//
// # Why this is not internal/admission
//
// The obvious home for a gate sweep is internal/admission: it already owns the
// due-command reconciler, the service-sweep authorization and the gate
// vocabulary. It is deliberately not used, and the reason is structural rather
// than a preference about package size.
//
// This sweep's one hard boundary is that it must never answer, deny, suspend,
// restore or synthesize a resolution for a gate -- see GateSweeper.Sweep. In
// internal/admission that boundary would be defended only by the seams the
// sweeper declared, because Service.AdmitGateResponse and Service.admit are
// package-local calls: a sweeper there can reach the answer path without a seam
// to walk and without a store to name. Here it cannot reach it at all, because
// this package does not import internal/admission and holds no way to express a
// command. TestTheGateSweepImportsNoPlaneThatCanAnswerAGate and
// TestTheGateSweepReachesNoGateResolutionCapability hold both halves.
package reconcile

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

// ErrInvalidGateSweeperConfig reports a gate sweeper configuration refused
// before it could reach a store.
//
// It is its own sentinel rather than a shared one, so a deployment composing
// several sweepers can tell which one it misconfigured.
var ErrInvalidGateSweeperConfig = errors.New("reconcile: invalid gate sweeper configuration")

// DueGates is the service-plane due-gate query.
//
// It is TWO methods for the reason admission.DueCommands is: a sweeper needs
// the shard count and one shard's page, and the count is a READ of the decision
// the backend is committed to rather than a setting. A sweeper configured with
// its own count would silently never visit some shards.
//
// It is deliberately NOT sessionstore.Store and deliberately not
// admission.Commands. Both carry admission and settlement methods, and a seam
// that carried them would put the answer path this sweep may not take within
// reach of the sweep -- which is the thing step 3 is about.
type DueGates interface {
	ControlShards() int
	ListDueGates(context.Context, sessionstore.ListDueGatesRequest) (sessionstore.DueGatePage, error)
}

// GateIntents is the one durable write this sweeper makes.
//
// Exactly one method, and the narrowness is load-bearing rather than tidy:
// OpenGate and ResolveGate live beside RetireGateDeadlineIntent on the same
// store value, and both are writes this package must never make. OpenGate would
// resurrect a deadline; ResolveGate is precisely the synthesized resolution
// step 3 forbids. A seam offering either is authority this package has no
// licence for.
type GateIntents interface {
	RetireGateDeadlineIntent(context.Context, sessionstore.RetireGateDeadlineIntentRequest) error
}

// SweepAuthorizer decides the cross-tenant service sweep.
//
// It declares AuthorizeServiceSweep ALONE, where admission.Authorizer declares
// AuthorizeControl beside it. That is the difference that matters here:
// AuthorizeControl is how a gate answer is authorized
// (admission.CommandGateResponse), so a sweeper holding admission.Authorizer
// would hold the authorization half of the path it may not take. Factory's
// authorizer satisfies both interfaces, so nothing is duplicated at the
// composition; what is removed is the reach.
type SweepAuthorizer interface {
	AuthorizeServiceSweep(ctx context.Context, principal identity.Principal) error
}

// Clock is the time seam.
//
// It is Now alone, where admission.Clock also has AfterFunc. A sweep is driven
// by its caller's cadence and does the work of one pass inside one call; a
// seam that could schedule would let it continue after the call it was made in
// returned, which is not a lifetime this package can account for.
type Clock interface {
	Now() time.Time
}

// GateSweeperConfig is one replica's gate-deadline sweep configuration.
type GateSweeperConfig struct {
	// Authorizer decides the sweep. The method called is AuthorizeServiceSweep
	// and there is no other; see GateSweeper.Sweep.
	Authorizer SweepAuthorizer

	Due     DueGates
	Intents GateIntents
	Clock   Clock

	// PageLimit bounds one due page.
	PageLimit int

	// MaxPages bounds ONE pass over ONE shard. It is a bound on the pass and
	// not on the work: a shard with more due rows than MaxPages*PageLimit is
	// reported Truncated and continued by the next pass over that shard, from
	// the cursor this sweeper persisted. Draining instead would let one busy
	// shard hold the rotor, and therefore every other shard's sweep, for as
	// long as its backlog lasts.
	MaxPages int
}

// GateDisposition is what one pass did about one remnant intent, or why it did
// nothing.
//
// It is a closed vocabulary with a rendering for every member including an
// unrecognized one, for the reason admission.Disposition is: a row's fate is
// operational data and "" is not a fate.
type GateDisposition uint8

const (
	// GateDispositionRetired means the intent is now a tombstone. It covers
	// the repeat: SessionStore answers a retirement of an already-tombstoned
	// intent with success and writes nothing, because a caller cannot tell a
	// lost reply from a failure. So a crash between the page and the write
	// costs a repeat and nothing else, which is what makes step 2's validation
	// safe to repeat rather than merely usually-repeated.
	GateDispositionRetired GateDisposition = iota

	// GateDispositionTooSoon means the intent is younger than
	// sessionstore.MinGateIntentRemnantAge. Inside that window a remnant and an
	// OpenGate still in flight are the same bytes, so the store refuses. It is
	// an ordinary outcome: the row stays due and a later pass retires it.
	GateDispositionTooSoon

	// GateDispositionReopened means the store refused because the session's own
	// durable record projects the gate as OPEN. The page said remnant and the
	// store's independent re-read said otherwise, which is a gate opened
	// between the two. Retiring it anyway would leave a public gate with no
	// deadline in any view, under an identity that can never be reused.
	GateDispositionReopened

	// GateDispositionRaceLost means the intent row moved between the page and
	// the compare-and-swap, so the retirement was aimed at bytes that no longer
	// exist. Distinct from Reopened because the two want different things from
	// a caller: re-read versus nothing at all.
	GateDispositionRaceLost

	// GateDispositionAbsent means there is no such intent row. SessionStore
	// never erases, so a retired intent has a durable spelling and it is a
	// tombstone; an absent row is one this store has never held.
	GateDispositionAbsent
)

func (d GateDisposition) String() string {
	switch d {
	case GateDispositionRetired:
		return "retired"
	case GateDispositionTooSoon:
		return "too_soon"
	case GateDispositionReopened:
		return "reopened"
	case GateDispositionRaceLost:
		return "race_lost"
	case GateDispositionAbsent:
		return "absent"
	default:
		return "unrecognized"
	}
}

// GateSweepResult is what one pass over one shard did, and what producing it
// cost.
type GateSweepResult struct {
	// Shard is the control shard this pass visited.
	Shard int

	// Pages, Examined and Unreadable are the store's own cost report,
	// accumulated over the pages this pass read. They are reported rather than
	// folded together: a full page that reported nothing is a different state
	// from nothing being due, and rows nothing could decode are a different
	// state again.
	Pages      int
	Examined   int
	Unreadable int

	// Remnants counts the retireable rows the store reported.
	Remnants int

	// OpenPastDeadline counts the due rows whose gate the session's durable
	// record still projects as OPEN.
	//
	// It is a COUNT AND NOTHING ELSE, and that is step 3. Such a gate is
	// current due work that gate continuation -- a later task, in the Host --
	// owns. This pass leaves it open, leaves it due, and moves past it on the
	// bounded cursor so it cannot starve the records behind it.
	OpenPastDeadline int

	// Retired counts the intents this pass tombstoned, which is the same number
	// as Dispositions[GateDispositionRetired] and is carried separately because
	// it is the one number an operator reads to know the sweep is doing
	// anything.
	Retired int

	// Dispositions counts every recorded outcome by kind.
	Dispositions map[GateDisposition]int

	// Queries counts the store calls this pass made that reach a provider: due
	// pages and retirements. ControlShards is deliberately not counted -- it
	// reads a decision the store already holds and makes no provider request.
	Queries int

	// Resumed reports that this pass continued a cursor a previous pass over
	// this shard persisted, rather than starting at the head of the view.
	Resumed bool

	// Exhausted reports that this pass reached the end of the shard's due view,
	// so the next pass over it re-arms at the head against a fresh wall-clock
	// bound.
	Exhausted bool

	// Truncated reports that MaxPages was reached with a continuation still
	// outstanding. The position is persisted, so the next pass over this shard
	// continues from it.
	Truncated bool
}

// GateSweeper retires stale gate deadline intents, one control shard per pass,
// round-robin.
//
// It holds two pieces of state and they are different in kind. The rotor is
// ADVICE: a restarted replica resumes at shard zero and loses nothing. The
// per-shard cursors are POSITION, and that is what step 3's anti-starvation
// rests on -- a shard whose head is occupied by gates this sweep may not touch
// is continued past them on the next pass instead of meeting them again.
//
// There is no reconciliation claim here, unlike admission.Reconciler, and the
// omission is deliberate. A claim is per SESSION while a due gate page mixes
// every tenant that hashes into the shard, so a claim would be taken and given
// back per row; and RetireGateDeadlineIntent is a compare-and-swap onto the
// revision the page reported, so two replicas racing one intent produce one
// retirement and one GateDispositionRaceLost rather than a wrong answer. The
// claim would suppress duplicate cost, not protect a decision. Separately,
// sessionstore v0.8.0's reconciliationClaimDue returns the zero storage.Due, so
// a claim taken here would sit in no deadline view at all.
type GateSweeper struct {
	cfg GateSweeperConfig

	mu      sync.Mutex
	next    int
	cursors map[int]sessionwire.Cursor
}

// NewGateSweeper validates a configuration before it can reach a store.
func NewGateSweeper(cfg GateSweeperConfig) (*GateSweeper, error) {
	switch {
	case cfg.Authorizer == nil:
		return nil, fmt.Errorf("%w: Authorizer must not be nil", ErrInvalidGateSweeperConfig)
	case cfg.Due == nil:
		return nil, fmt.Errorf("%w: Due must not be nil", ErrInvalidGateSweeperConfig)
	case cfg.Intents == nil:
		return nil, fmt.Errorf("%w: Intents must not be nil", ErrInvalidGateSweeperConfig)
	case cfg.Clock == nil:
		return nil, fmt.Errorf("%w: Clock must not be nil", ErrInvalidGateSweeperConfig)
	case cfg.PageLimit < 1 || cfg.PageLimit > storage.MaxOrderedPageLimit:
		return nil, fmt.Errorf("%w: PageLimit must be between 1 and %d",
			ErrInvalidGateSweeperConfig, storage.MaxOrderedPageLimit)
	case cfg.MaxPages < 1:
		return nil, fmt.Errorf("%w: MaxPages must be positive", ErrInvalidGateSweeperConfig)
	}
	return &GateSweeper{cfg: cfg, cursors: map[int]sessionwire.Cursor{}}, nil
}

// Sweep retires the stale gate deadline intents in the next control shard.
//
// STEP ONE'S AUTHORIZATION IS AuthorizeServiceSweep AND NOTHING ELSE. This is a
// cross-tenant control-plane query -- a shard holds whichever tenants hash into
// it, so one page can mix them -- and AuthorizeServiceSweep is the decision a
// tenant principal can never reach. AuthorizeControl is not called, and not
// because it would be redundant: AuthorizeControl is how a gate ANSWER is
// authorized, and a sweep that called it would be a sweep holding the licence
// for the write it may not make.
//
// STEP THREE IS THE HARD BOUNDARY. A due row whose gate is still publicly open
// is counted in OpenPastDeadline and nothing else happens to it. This sweeper
// cannot answer it, deny it, suspend the session, restore it, or write a
// resolution, and that is not a promise made in this comment -- it holds
// because the only two seams it has are DueGates and GateIntents, and neither
// can express any of those, which
// TestTheGateSweepReachesNoGateResolutionCapability pins against the compiled
// types.
func (s *GateSweeper) Sweep(ctx context.Context, principal identity.Principal) (GateSweepResult, error) {
	if err := s.cfg.Authorizer.AuthorizeServiceSweep(ctx, principal); err != nil {
		return GateSweepResult{}, err
	}
	shard, cursor, err := s.begin()
	if err != nil {
		return GateSweepResult{}, err
	}
	result := GateSweepResult{Shard: shard, Resumed: cursor != "", Dispositions: map[GateDisposition]int{}}
	next, sweepErr := s.page(ctx, shard, cursor, &result)
	s.commit(shard, next)
	return result, sweepErr
}

// begin advances the round-robin rotor and hands back the shard to sweep
// together with the position this sweeper last reached in it.
//
// The rotor advances BEFORE the work and independently of its outcome. Advanced
// on success only, one shard whose page read fails -- a poisoned row, a
// partition, a misconfigured provider -- would hold the rotor forever, and
// every other shard's sweep with it.
//
// The count is read from the store on every pass rather than cached, because it
// is a persisted decision of the backend rather than a setting of this replica.
func (s *GateSweeper) begin() (int, sessionwire.Cursor, error) {
	shards := s.cfg.Due.ControlShards()
	if shards < sessionstore.MinControlShards {
		return 0, "", fmt.Errorf("reconcile: the store reports %d control shards, so no shard can be swept", shards)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.next >= shards {
		// The count shrank under a running replica and the rotor is past the
		// end. Restarting it is the only answer that visits every shard;
		// continuing from a position past the end would sweep shard zero
		// forever.
		s.next = 0
	}
	// The positions of shards that no longer exist are dropped on EVERY pass
	// and not only on the pass that found the rotor out of range, because those
	// are different events: a shrink observed while the rotor happens to be
	// inside the new range moves no rotor at all, and a cursor merely left
	// UNREAD is not dropped -- it is presented again the moment its shard index
	// exists once more, to a view that has since been rebuilt, and is refused
	// from then on. A cursor is bound to the shard that issued it.
	for shard := range s.cursors {
		if shard >= shards {
			delete(s.cursors, shard)
		}
	}
	shard := s.next
	s.next = (shard + 1) % shards
	return shard, s.cursors[shard], nil
}

// commit persists the position a pass reached for its shard. An empty position
// is REMOVED rather than stored, so the next pass over that shard re-arms at
// the head against a fresh wall-clock bound.
func (s *GateSweeper) commit(shard int, cursor sessionwire.Cursor) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cursor == "" {
		delete(s.cursors, shard)
		return
	}
	s.cursors[shard] = cursor
}

// page reads one shard's due rows, bounded by MaxPages, and returns the
// position to persist.
func (s *GateSweeper) page(
	ctx context.Context,
	shard int,
	cursor sessionwire.Cursor,
	result *GateSweepResult,
) (sessionwire.Cursor, error) {
	now := s.cfg.Clock.Now()
	for range s.cfg.MaxPages {
		req := sessionstore.ListDueGatesRequest{Shard: shard, Limit: s.cfg.PageLimit, Cursor: cursor}
		if cursor == "" {
			// A continuation carries its own bound. Presenting both would be
			// two answers to one question, and the store refuses it.
			req.DueAtOrBefore = now
		}
		result.Queries++
		page, err := s.cfg.Due.ListDueGates(ctx, req)
		if err != nil {
			if refusedCursor(err) {
				// The position IS what was refused. Keeping it would present it
				// forever and this shard would silently stop being swept, so it
				// is dropped and the next pass re-arms at the head.
				return "", fmt.Errorf("reconcile: resume gate shard %d: %w", shard, err)
			}
			// Every other fault is about the page, not the position. Discarding
			// the position here would make a transient fault cost a full
			// restart over every row already walked.
			return cursor, fmt.Errorf("reconcile: page gate shard %d: %w", shard, err)
		}
		result.Pages++
		result.Examined += page.Examined
		result.Unreadable += page.Unreadable
		result.Remnants += len(page.Remnants)
		// Step 3. A gate the record still projects as open is counted here and
		// is not touched anywhere below.
		result.OpenPastDeadline += len(page.Gates)
		for _, remnant := range page.Remnants {
			if err := s.retire(ctx, remnant, result); err != nil {
				// The position returned is the one this page was read FROM, not
				// the one it ended at: the remnants after the failure were not
				// dealt with, and a store fault is not a property of a row, so
				// the next pass should meet the same page again rather than
				// step over work it never did.
				return cursor, err
			}
		}
		cursor = page.NextCursor
		if cursor == "" {
			result.Exhausted = true
			return "", nil
		}
	}
	result.Truncated = true
	return cursor, nil
}

// retire tombstones one remnant intent and records what happened.
//
// THE VALIDATION IS THE STORE'S AND IS USED RATHER THAN RESTATED.
// RetireGateDeadlineIntent independently re-reads the session's projection and
// the intent row, at its own clock reading, and refuses everything that is not
// a remnant. Re-deriving that judgement here would be a second decision point
// at which a gate could be reopened, and it is the point of step 1's "re-read
// the matching projection before any CAS" that the re-read happens in the same
// operation as the write rather than in this loop.
//
// What this package owns is the classification: which refusals are ordinary
// outcomes of a weakly consistent due page and which are faults that end the
// pass.
func (s *GateSweeper) retire(ctx context.Context, remnant sessionstore.RemnantGateIntent, result *GateSweepResult) error {
	result.Queries++
	// The request is built MEMBER BY MEMBER rather than converted from the
	// remnant, and staticcheck reports the conversion as available (S1016).
	// SessionStore keeps the two shapes as separate types deliberately -- one
	// is a sweep RESULT and the other an operation REQUEST -- and this call
	// site keeps them apart in the one case that matters.
	//
	// THE THREE WAYS THE TWO SHAPES CAN DIVERGE, SCORED HONESTLY, because the
	// conversion is NOT uniformly the weaker construct and an earlier version of
	// this comment claimed it was:
	//
	//   - BOTH types grow the same member, in lockstep. The conversion forwards
	//     it silently, so this operation would begin honouring a member added to
	//     a PAGE for a reader's convenience, with no edit here and no diff to
	//     review. The four keyed assignments leave it zero, which is the
	//     conservative answer and the reason for the deviation.
	//   - Only the REQUEST grows a member. The conversion stops compiling --
	//     loudly, which is better -- while these assignments compile and leave
	//     it zero.
	//   - Only the PAGE grows a member. Same: the conversion is the loud one.
	//
	// So the conversion is stricter in two cases of three, and the justification
	// is the FIRST case alone: explicit construction does not inherit an
	// upstream lockstep addition. It is NOT that "these assignments would not
	// compile against a request that grew a fifth" -- a keyed composite literal
	// compiles perfectly well against a struct with extra fields, and that
	// earlier claim was simply wrong about Go.
	//
	// What covers the two cases the conversion would have caught is
	// TestTheRetirementRequestIsBuiltFromTheRemnantMemberByMember, which pins
	// both shapes' field sets by reflection, so a divergence is loud here too.
	//lint:ignore S1016 explicit construction; an upstream lockstep addition must not be inherited
	err := s.cfg.Intents.RetireGateDeadlineIntent(ctx, sessionstore.RetireGateDeadlineIntentRequest{
		TenantID:  remnant.TenantID,
		SessionID: remnant.SessionID,
		GateID:    remnant.GateID,
		Revision:  remnant.Revision,
	})
	disposition, ok := classifyRetirement(err)
	if !ok {
		return fmt.Errorf("reconcile: retire gate deadline intent %s/%s/%s: %w",
			remnant.TenantID, remnant.SessionID, remnant.GateID, err)
	}
	result.Dispositions[disposition]++
	if disposition == GateDispositionRetired {
		result.Retired++
	}
	return nil
}

// classifyRetirement maps a retirement's answer onto a disposition, reporting
// false for an answer this package has no vocabulary for.
//
// The unrecognized case ENDS THE PASS rather than being counted as a skip. An
// invalid identity, a hash collision, an unreadable record or a provider fault
// each say nothing about whether the gate is open, and continuing past one
// would report a clean sweep over rows nothing had decided.
func classifyRetirement(err error) (GateDisposition, bool) {
	if err == nil {
		return GateDispositionRetired, true
	}
	var catalog *sessionstore.CatalogError
	if !errors.As(err, &catalog) {
		return 0, false
	}
	switch {
	case catalog.Code == sessionstore.CatalogErrorTooSoon:
		return GateDispositionTooSoon, true
	case catalog.Code == sessionstore.CatalogErrorConflict && catalog.Field == "gate_id":
		// refuseIfGateIsStillOpen's answer: the record projects the gate open.
		return GateDispositionReopened, true
	case catalog.Code == sessionstore.CatalogErrorConflict && catalog.Field == "gate_intent":
		// The compare-and-swap onto the page's revision lost.
		return GateDispositionRaceLost, true
	case catalog.Code == sessionstore.CatalogErrorNotFound && catalog.Field == "gate_intent":
		return GateDispositionAbsent, true
	}
	return 0, false
}

// refusedCursor reports the one page failure whose subject is the continuation
// itself. It keys on the store's dedicated cursor CODE rather than on a field
// name, because the field a cursor refusal carries differs between the two
// places one is raised.
func refusedCursor(err error) bool {
	var catalog *sessionstore.CatalogError
	return errors.As(err, &catalog) && catalog.Code == sessionstore.CatalogErrorCursor
}
