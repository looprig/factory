package placement

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"
)

// This file is the TRIGGER for B5: what decides that a session needs placing.
//
// THE TRIGGER IS A SWEEP OVER DURABLE PENDING WORK, and not admission. The
// reason is specification section 10.4 and integration case I1.2-3 together:
// "any Factory replica may reconcile due work", and a Factory that dies after
// the inbox commit and before forwarding must be replaced by ANOTHER Factory
// that still gets the command applied. The only thing both replicas share is
// the durable inbox, so the thing that decides a session needs a Host must be
// a reader of the inbox, and it must run with no user in the loop -- which is
// why the attach carries a service ActorID. Admission-time placement would be
// a latency optimisation on top of this, never a substitute: it runs on the
// admitting replica, which is exactly the one I1.2-3 kills.
//
// The inbox index files every non-terminal disposition command at its apply
// deadline and only there, so "every command still open as of now" is one
// deadline-ordered page read with the bound at now + Horizon, where Horizon is
// the apply deadline every accepted command was given. A terminal command is
// filed not-due and costs nothing.

// PendingCommands is the disposition inbox's service-plane due query.
type PendingCommands interface {
	ControlShards() int
	ListDueDispositionCommands(context.Context, sessionstore.ListDueDispositionCommandsRequest) (sessionstore.DispositionDueCommandPage, error)
}

// SweepAuthorizer decides the sweep, as it does for every other cross-tenant
// sweep this module runs.
type SweepAuthorizer interface {
	AuthorizeServiceSweep(ctx context.Context, principal identity.Principal) error
}

// Placer is the per-session reconciliation a sweep drives. *Reconciler
// implements it.
type Placer interface {
	Reconcile(ctx context.Context, req Request) (Result, error)
}

// ErrInvalidPendingSweeperConfig reports a pending sweeper that cannot keep
// its own bounds. It is a third sentinel beside ErrInvalidConfig and
// ErrInvalidSweeperConfig for the reason records.go gives for the second.
var ErrInvalidPendingSweeperConfig = errors.New("placement: invalid pending-work sweeper configuration")

// PendingSweeperConfig is one replica's pending-work sweep.
type PendingSweeperConfig struct {
	Authorizer SweepAuthorizer
	Pending    PendingCommands
	Placer     Placer
	Clock      Clock

	// Horizon is how far past now the due read reaches. It is the apply
	// deadline admission gives every command, so every command accepted up to
	// now is inside it.
	Horizon time.Duration

	// PageLimit bounds one due page and MaxPages one pass over one shard, with
	// admission.Reconciler's meaning: a shard with more open commands than
	// PageLimit*MaxPages is reported Truncated and finished by the next pass.
	PageLimit int
	MaxPages  int

	// Logger receives one line per session whose placement failed. Nil
	// discards.
	Logger *slog.Logger
}

// PendingSweeper places the sessions with open commands, one control shard per
// pass, round-robin.
//
// It REMEMBERS WHERE EACH SHARD'S PASS STOPPED, and without that it starves.
// The due view is deadline-ordered from the head, and the head is where rows
// this sweep skips pile up: an applying command past its deadline, a claimed
// one whose claim outlived it, and -- until the disposition deadline sweep
// retires them -- expired pending ones. A pass that always read from the head
// would, once a shard held more than MaxPages*PageLimit of those, never again
// reach a live create behind them (the B5 spec gate's F3: 2 expired rows ahead
// of a live create at 2 rows per pass, never attached across 5 passes). So a
// truncated pass keeps the store's continuation for its shard, the next pass
// over that shard resumes from it, and only a pass that reaches the end
// re-arms at the head against a fresh bound. Retirement keeps the head short;
// this is what makes the sweep correct even when it does not.
//
// The cost is stated: a resumed pass carries the bound its cycle STARTED with,
// which is the store's rule for a continuation, so a command admitted after a
// long cycle began is met when the next cycle starts rather than immediately.
// The position is advice: a restarted replica starts every shard at the head.
type PendingSweeper struct {
	cfg PendingSweeperConfig

	mu      sync.Mutex
	next    int
	cursors map[int]sessionwire.Cursor
}

// NewPendingSweeper validates a configuration before it can reach a store.
func NewPendingSweeper(cfg PendingSweeperConfig) (*PendingSweeper, error) {
	switch {
	case cfg.Authorizer == nil:
		return nil, fmt.Errorf("%w: Authorizer must not be nil", ErrInvalidPendingSweeperConfig)
	case cfg.Pending == nil:
		return nil, fmt.Errorf("%w: Pending must not be nil", ErrInvalidPendingSweeperConfig)
	case cfg.Placer == nil:
		return nil, fmt.Errorf("%w: Placer must not be nil", ErrInvalidPendingSweeperConfig)
	case cfg.Clock == nil:
		return nil, fmt.Errorf("%w: Clock must not be nil", ErrInvalidPendingSweeperConfig)
	case cfg.Horizon <= 0:
		return nil, fmt.Errorf("%w: Horizon must be positive", ErrInvalidPendingSweeperConfig)
	case cfg.PageLimit < 1 || cfg.PageLimit > storage.MaxOrderedPageLimit:
		return nil, fmt.Errorf("%w: PageLimit must be between 1 and %d", ErrInvalidPendingSweeperConfig, storage.MaxOrderedPageLimit)
	case cfg.MaxPages < 1:
		return nil, fmt.Errorf("%w: MaxPages must be positive", ErrInvalidPendingSweeperConfig)
	}
	return &PendingSweeper{cfg: cfg, cursors: map[int]sessionwire.Cursor{}}, nil
}

// PendingSweepResult is what one pass over one shard did.
type PendingSweepResult struct {
	Shard      int
	Pages      int
	Examined   int
	Unreadable int
	Truncated  bool
	// Resumed reports that this pass continued from the position an earlier
	// truncated pass over the same shard kept, rather than from the head.
	Resumed bool

	// Sessions is the number of distinct sessions this pass reconciled.
	Sessions int
	// Outcomes counts the decisions those reconciliations reported.
	Outcomes map[Outcome]int
	// Attached counts the sessions this pass attached to a Host.
	Attached int
	// Deferred counts the sessions another replica held the claim for.
	Deferred int
	// NoController counts dedicated sessions this replica cannot place because
	// it composes no workload controller (ErrNoWorkloadController).
	NoController int
	// Failures counts the sessions whose reconciliation returned an error. A
	// failure is per session and never stops the pass: one session whose
	// registry is stale must not keep every other session in the shard cold.
	Failures int
}

// openSession is one session's open work as a pass saw it, in first-seen
// order.
type openSession struct {
	tenant sessionwire.TenantID
	id     sessionwire.SessionID
	// wake is the PENDING commands still inside their apply deadline: the ones
	// a Host has not claimed and may still apply.
	wake []sessionwire.CommandID
	// needsHost reports that some open command gives the session a reason to
	// be resident: one still inside its deadline, or one APPLYING, which only a
	// successor runtime can settle whatever the deadline says.
	needsHost bool
}

// Sweep places every session with open work in the next control shard.
func (s *PendingSweeper) Sweep(ctx context.Context, principal identity.Principal) (PendingSweepResult, error) {
	if err := s.cfg.Authorizer.AuthorizeServiceSweep(ctx, principal); err != nil {
		return PendingSweepResult{}, err
	}
	shard, cursor, err := s.begin()
	if err != nil {
		return PendingSweepResult{}, err
	}
	result := PendingSweepResult{Shard: shard, Resumed: cursor != "", Outcomes: map[Outcome]int{}}
	now := s.cfg.Clock.Now()
	sessions, next, err := s.collect(ctx, shard, cursor, now, &result)
	s.commit(shard, next)
	if err != nil {
		return result, err
	}
	for _, session := range sessions {
		if !session.needsHost {
			continue
		}
		result.Sessions++
		placed, err := s.cfg.Placer.Reconcile(ctx, Request{TenantID: session.tenant, SessionID: session.id, Wake: session.wake})
		if errors.Is(err, ErrNoWorkloadController) {
			// A dedicated session reaching a replica composed with no
			// workload controller is a composition fact, not a failure of
			// this pass, and it recurs every pass: counted, not logged.
			result.NoController++
			continue
		}
		if err != nil {
			result.Failures++
			s.logger().WarnContext(ctx, "placement: a session with open commands could not be placed",
				slog.String("tenant_id", string(session.tenant)),
				slog.String("session_id", string(session.id)),
				slog.String("error", err.Error()))
			if ctx.Err() != nil {
				return result, ctx.Err()
			}
			continue
		}
		if placed.Deferred {
			result.Deferred++
			continue
		}
		result.Outcomes[placed.Decision.Outcome]++
		if placed.Attached != (sessionwire.HostLinkRegistryObservation{}) {
			result.Attached++
		}
	}
	return result, nil
}

// collect reads one shard's open commands from cursor, bounded by MaxPages,
// groups them by session in first-seen order, and returns the position to
// keep.
//
// The bound is on the PASS for admission.Reconciler's reason: draining a busy
// shard here would hold the rotor, and with it every other shard's placement.
// What is not read this pass is read by the next pass over the shard, from
// where this one stopped.
func (s *PendingSweeper) collect(ctx context.Context, shard int, cursor sessionwire.Cursor, now time.Time, result *PendingSweepResult) ([]*openSession, sessionwire.Cursor, error) {
	var (
		order    []*openSession
		sessions = map[sessionKey]*openSession{}
	)
	for range s.cfg.MaxPages {
		req := sessionstore.ListDueDispositionCommandsRequest{Shard: shard, Limit: s.cfg.PageLimit, Cursor: cursor}
		if cursor == "" {
			req.DueAtOrBefore = now.Add(s.cfg.Horizon)
		}
		page, err := s.cfg.Pending.ListDueDispositionCommands(ctx, req)
		if err != nil {
			if refusedCursor(err) {
				// The position is what was refused; keeping it would present it
				// forever and the shard would silently stop being placed.
				return nil, "", fmt.Errorf("placement: resume control shard %d: %w", shard, err)
			}
			return nil, cursor, fmt.Errorf("placement: page control shard %d for open commands: %w", shard, err)
		}
		result.Pages++
		result.Examined += page.Examined
		result.Unreadable += page.Unreadable
		for _, entry := range page.Commands {
			descriptor := entry.Record.Descriptor
			key := sessionKey{tenant: descriptor.TenantID, session: descriptor.SessionID}
			session := sessions[key]
			if session == nil {
				session = &openSession{tenant: descriptor.TenantID, id: descriptor.SessionID}
				sessions[key] = session
				order = append(order, session)
			}
			live := now.Before(entry.Record.ApplyDeadline)
			switch entry.Record.State {
			case sessionstore.InboxStatePending:
				if live {
					session.wake = append(session.wake, descriptor.CommandID)
					session.needsHost = true
				}
			case sessionstore.InboxStateClaimed:
				if live {
					session.needsHost = true
				}
			case sessionstore.InboxStateApplying:
				session.needsHost = true
			}
		}
		cursor = page.NextCursor
		if cursor == "" {
			return order, "", nil
		}
	}
	result.Truncated = true
	return order, cursor, nil
}

// refusedCursor reports the store refusing a continuation itself.
func refusedCursor(err error) bool {
	var inbox *sessionstore.InboxError
	return errors.As(err, &inbox) && inbox.Code == sessionstore.InboxErrorCursor
}

// begin advances the round-robin rotor and hands back the shard to sweep with
// the position this sweep last reached in it, for admission.Reconciler.rotate's
// reasons: before the work, independent of its outcome, and against the
// store's own shard count read on every pass. A position for a shard that no
// longer exists is dropped, as the gate sweeper drops one.
func (s *PendingSweeper) begin() (int, sessionwire.Cursor, error) {
	shards := s.cfg.Pending.ControlShards()
	if shards < sessionstore.MinControlShards {
		return 0, "", fmt.Errorf("placement: the store reports %d control shards, so no shard can be swept", shards)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.next >= shards {
		s.next = 0
	}
	for shard := range s.cursors {
		if shard >= shards {
			delete(s.cursors, shard)
		}
	}
	shard := s.next
	s.next = (shard + 1) % shards
	return shard, s.cursors[shard], nil
}

// commit keeps the position a pass reached for its shard. An empty position is
// removed, so the next pass over that shard re-arms at the head.
func (s *PendingSweeper) commit(shard int, cursor sessionwire.Cursor) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cursor == "" {
		delete(s.cursors, shard)
		return
	}
	s.cursors[shard] = cursor
}

func (s *PendingSweeper) logger() *slog.Logger {
	if s.cfg.Logger != nil {
		return s.cfg.Logger
	}
	return slog.New(slog.DiscardHandler)
}

// sessionKey is one session's identity as a comparable map key.
type sessionKey struct {
	tenant  sessionwire.TenantID
	session sessionwire.SessionID
}
