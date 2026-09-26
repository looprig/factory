package placement

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	"github.com/looprig/factory/internal/command"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"
)

// This file carries the two triggers for B5: admission and durable pending work.
//
// THE DURABILITY TRIGGER IS A SWEEP OVER DURABLE PENDING WORK. The
// reason is specification section 10.4 and integration case I1.2-3 together:
// "any Factory replica may reconcile due work", and a Factory that dies after
// the inbox commit and before forwarding must be replaced by ANOTHER Factory
// that still gets the command applied. The only thing both replicas share is
// the durable inbox, so the thing that decides a session needs a Host must be
// a reader of the inbox, and it must run with no user in the loop -- which is
// why the attach carries a service ActorID. Admission-time placement is a
// latency optimisation on top of this, never a substitute: it runs on the
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

// SessionCommands is SessionStore's optional, acceptance-ordered read of one
// session's inbox. The released Store implements it; the legacy PendingCommands
// seam remains source compatible for adapters that do not.
type SessionCommands interface {
	ListSessionDispositionCommands(context.Context, sessionstore.ListSessionDispositionCommandsRequest) (sessionstore.SessionDispositionCommandPage, error)
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

// AdmissionNotice is the placement-relevant part of a committed command.
// It carries no immutable session binding across this notification boundary.
type AdmissionNotice struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
	CommandID sessionwire.CommandID
	// AcceptedOrder lets the targeted read stop at the committed command.
	AcceptedOrder uint64
	Deadline      time.Time
	Pending       bool
}

func Notice(entry sessionstore.DispositionInboxEntry) AdmissionNotice {
	d := entry.Record.Descriptor
	return AdmissionNotice{
		TenantID: d.TenantID, SessionID: d.SessionID, CommandID: d.CommandID,
		Deadline: entry.Record.ApplyDeadline, AcceptedOrder: entry.AcceptedOrder,
		Pending: entry.Record.State == sessionstore.InboxStatePending,
	}
}

// ErrInvalidPendingSweeperConfig reports a pending sweeper that cannot keep
// its own bounds. It is a third sentinel beside ErrInvalidConfig and
// ErrInvalidSweeperConfig for the reason records.go gives for the second.
var ErrInvalidPendingSweeperConfig = errors.New("placement: invalid pending-work sweeper configuration")

// PendingSweeperConfig is one replica's pending-work sweep.
type PendingSweeperConfig struct {
	Authorizer      SweepAuthorizer
	Pending         PendingCommands
	SessionCommands SessionCommands
	Placer          Placer
	// Catalog and Directory let the fast path skip an already-owned session.
	Catalog   Catalog
	Directory Directory
	// AdmissionPlacer uses a distinct claim holder from Placer when supplied.
	// Two concurrent calls from this replica must not renew one another's claim.
	AdmissionPlacer Placer
	Clock           Clock

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

// SweepPlacer reports the reconciler composed for periodic placement.
func (s *PendingSweeper) SweepPlacer() Placer { return s.cfg.Placer }

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
	// Released counts dedicated sessions whose desire is released and that
	// no pending restore asked to bring back (ErrSessionReleased). Their open
	// commands are left to the disposition deadline sweep.
	Released int
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
	// gates is the subset of wake that is gate responses, which placement
	// delivers only to a Host that can apply one.
	gates      []sessionwire.CommandID
	principals []sessionwire.CommandID
	// restore reports a PENDING restore command still inside its deadline:
	// the explicit intent that may bring a released dedicated session back.
	restore bool
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
		placed, err := s.cfg.Placer.Reconcile(ctx, Request{
			TenantID: session.tenant, SessionID: session.id, Wake: session.wake, GateResponses: session.gates, PrincipalCommands: session.principals,
			// Only a live pending restore licenses re-expressing a released
			// dedicated session's launch template (D3.1 F2); other open work
			// may be what the product abandoned by deleting it.
			RestoreRequested: session.restore,
		})
		if errors.Is(err, ErrSessionReleased) {
			// A deleted dedicated session with leftover commands but no
			// restore: the deletion working, counted rather than logged, and
			// it recurs every pass until the deadline sweep settles them.
			result.Released++
			continue
		}
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

// PlaceAdmission gives a freshly admitted command a targeted chance to place
// its session. The periodic durable sweep remains the recovery path if this
// notification is lost. Reconcile owns the same claim for both callers.
func (s *PendingSweeper) PlaceAdmission(ctx context.Context, principal identity.Principal, notice AdmissionNotice) (Result, error) {
	placer := s.cfg.AdmissionPlacer
	if placer == nil {
		placer = s.cfg.Placer
	}
	return s.PlaceAdmissionWith(ctx, principal, notice, placer)
}

// PlaceAdmissionWith lets each admission worker use its own claim holder.
// A slow attach on one worker must not renew another worker's claim.
func (s *PendingSweeper) PlaceAdmissionWith(ctx context.Context, principal identity.Principal, notice AdmissionNotice, placer Placer) (Result, error) {
	if err := s.cfg.Authorizer.AuthorizeServiceSweep(ctx, principal); err != nil {
		return Result{}, err
	}
	if !notice.Pending || !s.cfg.Clock.Now().Before(notice.Deadline) {
		return Result{}, nil
	}
	if s.cfg.Catalog != nil && s.cfg.Directory != nil {
		entry, err := s.cfg.Catalog.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{TenantID: notice.TenantID, SessionID: notice.SessionID})
		if err != nil {
			return Result{}, err
		}
		owner, observed, err := s.cfg.Directory.Owner(ctx, notice.TenantID, notice.SessionID)
		if err != nil {
			return Result{}, err
		}
		if observed && Decide(entry.Record, owner, observed, nil, s.cfg.Clock.Now()).Outcome == OutcomeReuseOwner &&
			(entry.Record.DesiredPlacement != sessionwire.HostPlacementDedicated || owner.HostGeneration == entry.Record.DesiredGeneration) {
			return Result{}, nil
		}
	}
	// Inventory this session's open commands before choosing a Host:
	// an earlier gate response or attributed command constrains selection for
	// a later plain command. Session history is paged to the admitted order.
	// If it cannot be reached inside MaxPages, a small due-view budget
	// inventories open work instead. Older adapters use that fallback too.
	var target *openSession
	var err error
	if s.cfg.SessionCommands != nil {
		target, err = s.collectAdmissionSession(ctx, notice)
	} else {
		target, err = s.collectAdmissionFallback(ctx, notice)
	}
	if err != nil {
		return Result{}, err
	}
	if target == nil || !target.needsHost {
		return Result{}, nil
	}
	req := Request{
		TenantID: target.tenant, SessionID: target.id,
		Wake: target.wake, GateResponses: target.gates,
		PrincipalCommands: target.principals, RestoreRequested: target.restore,
	}
	return placer.Reconcile(ctx, req)
}

func (s *PendingSweeper) collectAdmissionSession(ctx context.Context, notice AdmissionNotice) (*openSession, error) {
	target := &openSession{tenant: notice.TenantID, id: notice.SessionID}
	var after uint64
	for range s.cfg.MaxPages {
		page, err := s.cfg.SessionCommands.ListSessionDispositionCommands(ctx, sessionstore.ListSessionDispositionCommandsRequest{
			TenantID: notice.TenantID, SessionID: notice.SessionID, AfterOrder: after, Limit: s.cfg.PageLimit,
		})
		if err != nil {
			return nil, err
		}
		if len(page.Commands) == 0 {
			break
		}
		if page.NextAfterOrder <= after {
			return nil, fmt.Errorf("placement: session command page did not advance")
		}
		for _, entry := range page.Commands {
			target.add(entry, s.cfg.Clock.Now())
		}
		if page.NextAfterOrder >= notice.AcceptedOrder && slices.Contains(target.wake, notice.CommandID) {
			break
		}
		if len(page.Commands) < s.cfg.PageLimit {
			break
		}
		after = page.NextAfterOrder
	}
	if !slices.Contains(target.wake, notice.CommandID) {
		return s.collectAdmissionFallback(ctx, notice)
	}
	return target, nil
}

func (s *PendingSweeper) collectAdmissionFallback(ctx context.Context, notice AdmissionNotice) (*openSession, error) {
	const maxDuePages = 2
	remaining := maxDuePages
	for shard := range s.cfg.Pending.ControlShards() {
		if remaining == 0 {
			break
		}
		result := PendingSweepResult{}
		sessions, _, err := s.collectPages(ctx, shard, "", s.cfg.Clock.Now(), &result, remaining)
		remaining -= result.Pages
		if err != nil {
			return nil, err
		}
		for _, session := range sessions {
			if session.tenant == notice.TenantID && session.id == notice.SessionID {
				if result.Truncated || !slices.Contains(session.wake, notice.CommandID) {
					return nil, nil
				}
				return session, nil
			}
		}
	}
	return nil, nil
}

func (session *openSession) add(entry sessionstore.DispositionInboxEntry, now time.Time) {
	descriptor := entry.Record.Descriptor
	live := now.Before(entry.Record.ApplyDeadline)
	carries := descriptor.Principal != nil || len(descriptor.Metadata) > 0
	switch entry.Record.State {
	case sessionstore.InboxStatePending:
		if live {
			session.wake = append(session.wake, descriptor.CommandID)
			if carries {
				session.principals = append(session.principals, descriptor.CommandID)
			}
			switch descriptor.Kind {
			case command.KindGateResponse:
				session.gates = append(session.gates, descriptor.CommandID)
			case command.KindRestore:
				session.restore = true
			}
			session.needsHost = true
		}
	case sessionstore.InboxStateClaimed:
		if live {
			session.needsHost = true
			if carries {
				session.principals = append(session.principals, descriptor.CommandID)
			}
		}
	case sessionstore.InboxStateApplying:
		session.needsHost = true
	}
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
	return s.collectPages(ctx, shard, cursor, now, result, s.cfg.MaxPages)
}

func (s *PendingSweeper) collectPages(ctx context.Context, shard int, cursor sessionwire.Cursor, now time.Time, result *PendingSweepResult, maxPages int) ([]*openSession, sessionwire.Cursor, error) {
	var (
		order    []*openSession
		sessions = map[sessionKey]*openSession{}
	)
	for range maxPages {
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
			session.add(entry, now)
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

// Horizon and Logger report what this sweeper was composed with, for the
// composition's own tests (B5 spec gate S3): the horizon must be the apply
// deadline admission gives every command, or a command accepted up to now
// falls outside the due read.
func (s *PendingSweeper) Horizon() time.Duration { return s.cfg.Horizon }

// Logger reports the configured logger, nil when none was composed.
func (s *PendingSweeper) Logger() *slog.Logger { return s.cfg.Logger }
