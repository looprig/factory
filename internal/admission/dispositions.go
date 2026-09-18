package admission

import (
	"context"
	"errors"
	"fmt"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	"github.com/looprig/factory/internal/command"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"
)

// This file is the DISPOSITION family's deadline sweep: what turns a command
// no Host applied before its apply deadline into a terminal rejection.
//
// It exists because every command this module admits is a disposition command
// and a Host relies on somebody doing this. A Host never rejects a command on
// its own reading of the clock -- host v0.2.1 declares no seam for
// RejectDispositionCommand at all, and its Kind.known comment names "Factory's
// deadline reconciler" as the thing that turns an unapplied command into a
// rejection -- while Reconciler, beside this file, sweeps only the LEGACY due
// view, which a disposition command is never filed in. Without this sweep an
// expired disposition command stayed open forever, and the placement sweep had
// merely stopped waking it.
//
// # The one rule it may not break
//
// THERE IS NO CALLER-AUTHORED REJECTION ONCE AN ATTEMPT EXISTS. An applying
// command has a durably authorized dispatch behind it, its terminal arm is the
// store's to decide from the runtime's evidence, and only a successor runtime
// may close one that has no evidence. So this sweep rejects a command only
// while it is PENDING, or CLAIMED under a claim that has lapsed, and only once
// its deadline has passed; everything else is counted and left alone. The
// store's reject edge refuses an attempt-carrying record itself
// (InboxErrorState), so this predicate is the first of two authorities for that
// rule and the store is the second -- but the DEADLINE has exactly one, this
// predicate, because RejectDispositionCommand checks no deadline.
//
// # What the rejection says
//
// The disposition record has no member for a reason, so the durable answer is
// the state alone. This sweep is the only producer of an ATTEMPTLESS rejection
// in this fleet (a Host authors none), which is what lets
// command.StatusForDisposition describe one publicly as runtime_unavailable --
// the same code the legacy sweep wrote into its record.

// DispositionDue is the disposition family's service-plane due query. It is
// the same two methods as DueCommands for the same reasons, over the other
// family's index.
type DispositionDue interface {
	ControlShards() int
	ListDueDispositionCommands(context.Context, sessionstore.ListDueDispositionCommandsRequest) (sessionstore.DispositionDueCommandPage, error)
}

// DispositionSettlement is the one durable write the disposition sweep makes.
// It is the pre-dispatch rejection and nothing else, for Settlement's reason:
// every other disposition transition belongs to a residency holder.
type DispositionSettlement interface {
	RejectDispositionCommand(context.Context, sessionstore.RejectDispositionCommandRequest) (sessionstore.DispositionInboxEntry, bool, error)
}

// DispositionReconcilerConfig is one replica's disposition deadline sweep. Its
// members mean exactly what ReconcilerConfig's do.
type DispositionReconcilerConfig struct {
	Authorizer Authorizer
	Due        DispositionDue
	Settlement DispositionSettlement
	Claims     Claims
	Clock      Clock
	HolderID   string
	ClaimTTL   time.Duration
	PageLimit  int
	MaxPages   int
}

// DispositionReconciler sweeps expired disposition commands in fixed control
// shards, round-robin, one shard per pass and at most MaxPages*PageLimit rows.
type DispositionReconciler struct {
	cfg   DispositionReconcilerConfig
	rotor rotor
}

// NewDispositionReconciler validates a configuration before it can reach a
// store.
func NewDispositionReconciler(cfg DispositionReconcilerConfig) (*DispositionReconciler, error) {
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
	return &DispositionReconciler{cfg: cfg}, nil
}

func (r *DispositionReconciler) claimant() claimant {
	return claimant{claims: r.cfg.Claims, holder: r.cfg.HolderID, ttl: r.cfg.ClaimTTL}
}

// Sweep reconciles the next control shard's expired disposition commands.
//
// It is authorized once, as the cross-tenant sweep it is, for Reconciler.Sweep's
// reasons, and it releases every claim it took on every path.
func (r *DispositionReconciler) Sweep(ctx context.Context, principal identity.Principal) (SweepResult, error) {
	if err := r.cfg.Authorizer.AuthorizeServiceSweep(ctx, principal); err != nil {
		return SweepResult{}, err
	}
	shard, err := r.rotor.rotate(r.cfg.Due.ControlShards())
	if err != nil {
		return SweepResult{}, err
	}
	result := SweepResult{Shard: shard, Dispositions: map[Disposition]int{}}
	held := map[sessionKey]bool{}
	sweepErr := r.page(ctx, shard, &result, held)
	r.claimant().release(ctx, held, &result)
	return result, sweepErr
}

// page reads one shard's due disposition rows, bounded by MaxPages.
//
// The bound is NOW. The disposition index files every open command at its
// apply deadline and nowhere else (sessionstore disposition_inbox.go
// dispositionInboxDue), so a page bounded at now holds exactly the commands
// whose deadline has passed -- to the store's millisecond, which is why the
// predicate compares the deadline again at full precision.
func (r *DispositionReconciler) page(ctx context.Context, shard int, result *SweepResult, held map[sessionKey]bool) error {
	now := r.cfg.Clock.Now()
	var cursor sessionwire.Cursor
	for range r.cfg.MaxPages {
		req := sessionstore.ListDueDispositionCommandsRequest{Shard: shard, Limit: r.cfg.PageLimit, Cursor: cursor}
		if cursor == "" {
			req.DueAtOrBefore = now
		}
		result.Queries++
		page, err := r.cfg.Due.ListDueDispositionCommands(ctx, req)
		if err != nil {
			return fmt.Errorf("admission: page control shard %d for due disposition commands: %w", shard, err)
		}
		result.Pages++
		result.Examined += page.Examined
		result.Unreadable += page.Unreadable
		result.Due += len(page.Commands)
		for _, due := range page.Commands {
			if err := r.settle(ctx, due, now, result, held); err != nil {
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

// settle decides one due row and, if it may, rejects it.
func (r *DispositionReconciler) settle(
	ctx context.Context,
	entry sessionstore.DispositionInboxEntry,
	now time.Time,
	result *SweepResult,
	held map[sessionKey]bool,
) error {
	if disposition := dispositionSettleable(entry.Record, now); disposition != DispositionSettleable {
		result.Dispositions[disposition]++
		return nil
	}
	descriptor := entry.Record.Descriptor
	claimed, err := r.claimant().claim(ctx, descriptor.TenantID, descriptor.SessionID, now, result, held)
	if err != nil {
		return err
	}
	if !claimed {
		result.Dispositions[DispositionDeferred]++
		return nil
	}
	result.Queries++
	// ResidencyEpoch is ZERO, deliberately, for the reason the legacy sweep
	// names none: this replica holds no residency, and the store confines a
	// zero-residency caller by the claim rule alone -- it may settle a command
	// nobody is working on, and nothing else.
	_, rejected, err := r.cfg.Settlement.RejectDispositionCommand(ctx, sessionstore.RejectDispositionCommandRequest{
		TenantID:         descriptor.TenantID,
		SessionID:        descriptor.SessionID,
		CommandID:        descriptor.CommandID,
		ExpectedRevision: entry.Revision,
	})
	if err == nil {
		if !rejected {
			// The store's idempotent arm: this edge's own earlier rejection,
			// returned unchanged. Somebody -- another replica, or this one on
			// an earlier pass whose answer was lost -- already settled it.
			result.Dispositions[DispositionTerminal]++
			return nil
		}
		result.Rejected++
		result.Dispositions[DispositionRejected]++
		return nil
	}
	if disposition, classified := dispositionRefusal(err); classified {
		result.Dispositions[disposition]++
		return nil
	}
	return fmt.Errorf("admission: reject the expired disposition command: %w", err)
}

// dispositionSettleable is the disposition sweep's safety predicate, and like
// settleable it is a TOTAL function of the record and the instant.
//
// The arms are state-first, and every arm but the last refuses, so no
// reordering can admit a row -- only which reason is reported changes:
//
//   - applied and rejected are terminal: nothing to do.
//   - APPLYING, or any record carrying an attempt, belongs to a successor
//     runtime. This is the rule this sweep may never break.
//   - a state this build does not know is refused rather than guessed at.
//   - a LIVE claim wins the deadline race: while it holds, only its holder may
//     reject, and the store would refuse this sweep with claim_held anyway.
//   - a command still inside its deadline may yet be applied.
//
// The deadline comparison is half-open in the store's convention -- a command
// is live up to but not including its deadline -- and it is checked here at
// full precision because the due index files deadlines to the millisecond.
func dispositionSettleable(record sessionstore.DispositionInboxRecord, now time.Time) Disposition {
	switch record.State {
	case sessionstore.InboxStateApplied, sessionstore.InboxStateRejected:
		return DispositionTerminal
	case sessionstore.InboxStateApplying:
		return DispositionApplying
	case sessionstore.InboxStatePending, sessionstore.InboxStateClaimed:
	default:
		return DispositionUnrecognized
	}
	switch {
	case record.Attempt != nil:
		return DispositionApplying
	case record.Claim != nil && now.Before(record.Claim.ExpiresAt):
		return DispositionClaimLive
	case now.Before(record.ApplyDeadline):
		return DispositionUnexpired
	default:
		return DispositionSettleable
	}
}

// dispositionRefusal classifies a refusal of the disposition reject edge into
// an ordinary outcome, or reports that it is a fault. It is fail-closed: a code
// not named here is a fault, and TestEveryDispositionRejectRefusalIsClassified
// holds the named set against the pinned store's vocabulary.
//
//   - state: the record carries an attempt. A runtime began applying it
//     between the page and the write, and it is now a successor's business.
//   - claim_held: a live claim took the row between the page and the write.
//   - conflict: the record moved since the page.
//   - terminal / not_found / deleted, or an absent session: nothing is left to
//     settle.
func dispositionRefusal(err error) (Disposition, bool) {
	if command.SessionAbsent(err) {
		return DispositionTerminal, true
	}
	var inbox *sessionstore.InboxError
	if !errors.As(err, &inbox) {
		return DispositionSettleable, false
	}
	switch inbox.Code {
	case sessionstore.InboxErrorState:
		return DispositionApplying, true
	case sessionstore.InboxErrorClaimHeld:
		return DispositionClaimLive, true
	case sessionstore.InboxErrorConflict:
		return DispositionRaceLost, true
	case sessionstore.InboxErrorTerminal, sessionstore.InboxErrorNotFound, sessionstore.InboxErrorDeleted:
		return DispositionTerminal, true
	default:
		return DispositionSettleable, false
	}
}
