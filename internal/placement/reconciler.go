package placement

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"
)

// ErrInvalidConfig reports a Reconciler that cannot keep its own bounds.
// Configuration is checked by NewReconciler before any store call.
var ErrInvalidConfig = errors.New("placement: invalid reconciler configuration")

// ErrNoWorkloadController reports a dedicated session reaching a replica that
// creates no workloads.
//
// It is H5's two-process split made a runtime rule rather than a convention.
// The decision of 2026-09-04 puts the Kubernetes adapter in the separate
// looprig/controller repository ALONE: a Factory process runs with a different
// ServiceAccount holding no workload create/delete RBAC, so it composes no
// controller and its import graph reaches
// no Kubernetes client package. A dedicated session that reaches that binary
// has therefore arrived somewhere that cannot serve it, and saying so by name
// is the only honest answer -- reporting it as "no capacity" would send a
// caller into a retry loop waiting for an autoscaler that will never run, and
// reporting success would claim a workload nobody created.
var ErrNoWorkloadController = errors.New("placement: this replica reconciles no dedicated workloads")

// ErrNoLaunchTemplate reports a RELEASED dedicated session with open work whose
// launch template cannot be recovered from durable state.
//
// A dedicated session is released by deletion desire: its dedicated placement
// names no workload. Open work for it means a workload is wanted again, and the
// only template this package re-expresses is the one the session was CREATED
// with -- the public create's InitialWorkload, which SessionStore keeps as
// immutable provenance beside the mutable desire. A session with no such record
// (one created through CreateCatalogEntry rather than the public create) has no
// template this package may vouch for. Substituting today's configured launch
// target would make a returning session's workload depend on configuration
// drift since it was created, so the session is refused by name instead, on
// every pass, until a caller supplies Request.Desired.
var ErrNoLaunchTemplate = errors.New("placement: a released dedicated session has no recoverable launch template")

// The interfaces here are the ones THIS package calls, declared where they are
// consumed. internal/routing supplies Directory and SessionStore supplies the
// other two; neither names this package.

// Directory is the observed target directory: which Host currently owns a
// session, and which Hosts could accept one.
type Directory interface {
	Owner(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) (sessionwire.HostLinkRegistryObservation, bool, error)
	Candidates(ctx context.Context, req sessionstore.ListCompatibleHostsRequest) (sessionstore.HostTargetPage, error)
}

// Catalog is the durable session record: the desired state a Factory authors
// and the record every decision here is made against.
type Catalog interface {
	GetCatalogEntry(ctx context.Context, req sessionstore.GetCatalogEntryRequest) (sessionstore.CatalogEntry, error)
	UpdateCatalogDesiredState(ctx context.Context, req sessionstore.UpdateCatalogDesiredStateRequest) (sessionstore.CatalogEntry, error)
}

// Claims is the short-lived reconciliation claim of specification section 10.
type Claims interface {
	AcquireReconciliationClaim(ctx context.Context, req sessionstore.AcquireReconciliationClaimRequest) (sessionstore.ReconciliationClaimEntry, error)
	ReleaseReconciliationClaim(ctx context.Context, req sessionstore.ReleaseReconciliationClaimRequest) (sessionstore.ReconciliationClaimEntry, error)
}

// Clock is the time seam. It declares only Now: this package sets a claim
// horizon and reads an owner's expiry, and schedules nothing.
type Clock interface {
	Now() time.Time
}

// Config is one replica's placement configuration.
type Config struct {
	Directory Directory
	Catalog   Catalog
	Claims    Claims

	// Workloads is OPTIONAL, and its absence is a supported deployment rather
	// than a degraded one: a Factory process without a controller composes exactly
	// this. See
	// ErrNoWorkloadController.
	Workloads WorkloadController

	Clock Clock

	// HolderID names this replica in a claim. It is not authority -- the claim
	// licenses nothing -- but it must be stable for a process, because
	// extending one's own claim and taking over a crashed replica's lapsed one
	// are the same store write and the holder is what tells them apart.
	HolderID string

	// ClaimTTL is how long this replica claims a session's scaling work for.
	ClaimTTL time.Duration

	// CandidateLimit bounds one capacity page.
	CandidateLimit int

	// Links is OPTIONAL. With it, a pooled session with no live owner is
	// ATTACHED to the first ranked admissible candidate that accepts it and
	// bound with the epoch that Host answered (B5). Without it, the reconciler
	// only NAMES the candidate in Result.Decision, which is what it did before
	// any caller existed; a composition with no HostLink pool uses that.
	Links HostLinks

	// ActorID is the requesting SERVICE identity an attach carries. Required
	// when Links is set. It names who asked for residency, never whose
	// authority a later command carries: placement runs from a sweeper with no
	// live user behind it.
	ActorID string

	// ReplaceAttempts bounds how many times one call re-runs placement after an
	// epoch_mismatch, counting the first attempt. Zero takes a small default.
	ReplaceAttempts int

	// ReplaceBackoff is the wait before the first re-placement, doubled for
	// each further one. Zero takes a small default.
	ReplaceBackoff time.Duration

	// Wait sleeps for a backoff or until the context ends. Nil uses a timer. It
	// is a seam so a re-placement's backoff is measured rather than waited for.
	Wait func(context.Context, time.Duration) error

	// Logger receives the one diagnostic this package emits: a pooled
	// candidate excluded because it does not advertise hostlink.attach, named
	// by Host, so a mixed fleet is diagnosable. Nil discards.
	Logger *slog.Logger
}

// Reconciler places one session at a time.
//
// It holds no per-session state. Everything it decides from is read within the
// call: the catalog record, the registry observation and one capacity page. A
// replica that restarts mid-placement leaves only a claim, which lapses.
type Reconciler struct {
	cfg Config
	// draw turns a nominal backoff into the wait actually slept. Nil is
	// jittered; a test sets a fixed draw so the default wait's use of it is
	// observable (B5 v0.3.0 quality gate QJ1/QJ2).
	draw func(time.Duration) time.Duration

	// reports is when each session's "waiting for a capable Host" WARN was
	// last written; see reportIncapable.
	reportsMu sync.Mutex
	reports   map[sessionKey]time.Time
}

// NewReconciler validates a configuration before it can reach a store.
func NewReconciler(cfg Config) (*Reconciler, error) {
	switch {
	case cfg.Directory == nil:
		return nil, fmt.Errorf("%w: Directory must not be nil", ErrInvalidConfig)
	case cfg.Catalog == nil:
		return nil, fmt.Errorf("%w: Catalog must not be nil", ErrInvalidConfig)
	case cfg.Claims == nil:
		return nil, fmt.Errorf("%w: Claims must not be nil", ErrInvalidConfig)
	case cfg.Clock == nil:
		return nil, fmt.Errorf("%w: Clock must not be nil", ErrInvalidConfig)
	case cfg.HolderID == "":
		return nil, fmt.Errorf("%w: HolderID must name this replica", ErrInvalidConfig)
	case cfg.ClaimTTL <= 0 || cfg.ClaimTTL > sessionstore.MaxReconciliationClaimTTL:
		return nil, fmt.Errorf("%w: ClaimTTL must be positive and at most %v", ErrInvalidConfig, sessionstore.MaxReconciliationClaimTTL)
	case cfg.CandidateLimit < 1 || cfg.CandidateLimit > storage.MaxOrderedPageLimit:
		return nil, fmt.Errorf("%w: CandidateLimit must be between 1 and %d", ErrInvalidConfig, storage.MaxOrderedPageLimit)
	}
	if err := cfg.attachConfigError(); err != nil {
		return nil, err
	}
	return &Reconciler{cfg: cfg}, nil
}

// Desired is the desired state a caller wants stored for a session.
//
// It is an INPUT rather than something this package derives, and that boundary
// is where A4.2 stops. What a dedicated session's workload payload should
// contain is a launch-template question owned by composition (A9.1); this
// package's job is that whatever the caller names is stored idempotently and
// handed to the controller with a generation.
type Desired struct {
	Placement              sessionwire.HostPlacement
	RuntimeCompatibilityID string
	Workload               sessionstore.DesiredWorkload
}

// Request is one reconciliation.
//
// Desired is optional. Nil means "reconcile what the record already says",
// which is the ordinary path for a session whose desired state was written at
// creation and has not changed since; no desired-state write is attempted at
// all in that case, so the generation cannot move.
type Request struct {
	TenantID  sessionwire.TenantID
	SessionID sessionwire.SessionID
	Desired   *Desired

	// Wake is the already accepted commands to deliver once the session has a
	// route: after an attach this call made, or to a live owner it found. It
	// is a HINT -- delivery wakes a Host's consumption of its durable inbox and
	// carries only the retry-stable public CommandID -- so an empty Wake still
	// places, and a failed delivery is counted, not returned.
	Wake []sessionwire.CommandID

	// GateResponses names the commands in Wake that are gate responses. A
	// gate response is delivered only to a Host that can apply one
	// (HostLinks.AcceptsGateResponses, answered by the one capability
	// predicate); for any other Host it is WITHHELD and counted, never sent. A
	// command not named here is delivered as before.
	GateResponses []sessionwire.CommandID

	// OpenWork reports that the caller observed commands this session still
	// needs a Host for. It is what licenses re-expressing a RELEASED dedicated
	// session's launch template: when the record's desire is a dedicated
	// placement naming no workload (deletion desire) and the caller has open
	// work, the reconciler writes a NEW desired generation carrying the
	// template the session was created with before ensuring its workload (see
	// reviveReleased). Without it a released session is reconciled as the
	// record says, and no desired-state write is made on its behalf.
	OpenWork bool
}

// Result is what one reconciliation did.
type Result struct {
	// Decision is the placement answer. It is the zero Decision when Deferred.
	Decision Decision

	// Deferred reports that another replica holds this session's claim and
	// this one did nothing. It is not an error: it is the claim working.
	Deferred bool

	// ClaimExpiresAt is the holder's horizon, populated only when Deferred. It
	// is how long a caller can wait before the work is anyone's again.
	ClaimExpiresAt time.Time

	// DesiredWrites counts the desired-state write calls this reconciliation
	// made: zero when the record already carried the intent, one for an
	// ordinary application, and two when a racer's different intent forced one
	// re-read and retry.
	//
	// It counts CALLS rather than reporting "this replica applied it", and the
	// difference is not a hedge. Two replicas deriving one intent derive one
	// idempotency key, so the loser's write is absorbed as a replay of the
	// winner's and the two are indistinguishable in the record they leave --
	// which is the whole point of the derived key. What is observable, and what
	// the caller can act on, is how much work this call did.
	DesiredWrites int

	// Intent is the stored desired state handed to EnsureWorkload. It remains
	// populated if a later read changes the decision to OutcomeUndecided or a
	// matching owner is reused; the decision, not Intent, describes the result.
	Intent sessionstore.PlacementIntent

	// Attached is the Host's observation of the residency an attach made by
	// THIS call produced. Its lease epoch is the one the bind named. Zero when
	// this call attached nothing.
	Attached sessionwire.HostLinkRegistryObservation

	// Bound reports that this call bound a route: to the Host it attached, or
	// to a live owner it woke.
	Bound bool

	// Delivered and DeliveryFailures count the Wake deliveries.
	// WithheldGateResponses counts the gate responses in Wake NOT delivered
	// because the bound Host cannot apply one, or could not be asked.
	Delivered             int
	DeliveryFailures      int
	WithheldGateResponses int

	// Excluded names the admissible candidates skipped because they do not
	// advertise hostlink.attach; Unreachable those this replica could not ask;
	// Unaddressable those whose advertised base cannot carry this session's
	// tenant's HostLink address (ErrTenantUnaddressable); Refused those that
	// answered with a HostLinkError; Failed those that answered with a failure
	// carrying no code (ErrAttachFailed). Each is in the order asked.
	Excluded      []sessionwire.HostID
	Unreachable   []sessionwire.HostID
	Unaddressable []sessionwire.HostID
	// Incapable names the candidates skipped because the session has a
	// pending gate response and the candidate answered that it cannot apply one.
	// A candidate that could not be asked is Unreachable or an error.
	Incapable []sessionwire.HostID
	Refused   []CandidateRefusal
	Failed    []sessionwire.HostID

	// Replacements counts the times placement re-ran after an epoch_mismatch.
	Replacements int
}

// Reconcile routes or places one session.
//
// The order is specification section 15's routing algorithm and the deviations
// from a naive reading are all deliberate:
//
//  1. The record and the owner are read FIRST, and a live owner returns without
//     taking a claim at all. An owned session is the common case, and putting a
//     durable compare-and-swap in front of it would serialize routing behind a
//     record that exists to coordinate SCALING.
//  2. Only then is the claim taken. Losing it is not a failure: step 4 of the
//     algorithm says a replica that observes an existing claim re-reads
//     desired and observed state rather than scaling again, so the deferral path can
//     report an owner the winner has just produced for the generation it wrote.
//  3. Desired state is written before the decision, because the write can
//     change the desired placement the decision branches on.
//
// With Links composed, the pooled arm ATTACHES (B5): see placePooled for the
// sequence and attachAnswer for what each Host answer means. The attach is
// gated on the candidate having advertised hostlink.attach, which the HostLink
// transport enforces locally, so a Host that predates the method is excluded
// rather than asked. Without Links, OutcomeAttachPooled only names the
// candidate for the caller, as it did before any caller existed.
func (r *Reconciler) Reconcile(ctx context.Context, req Request) (Result, error) {
	entry, err := r.cfg.Catalog.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{
		TenantID: req.TenantID, SessionID: req.SessionID,
	})
	if err != nil {
		return Result{}, err
	}
	owner, observed, err := r.cfg.Directory.Owner(ctx, req.TenantID, req.SessionID)
	if err != nil {
		return Result{}, err
	}
	now := r.cfg.Clock.Now()
	if decision := Decide(entry.Record, owner, observed, nil, now); decision.Outcome == OutcomeReuseOwner &&
		(entry.Record.DesiredPlacement != sessionwire.HostPlacementDedicated || owner.HostGeneration == entry.Record.DesiredGeneration) {
		return r.wake(ctx, req, owner, Result{Decision: decision})
	}

	claimed, held, err := r.claim(ctx, req, now)
	if err != nil {
		return Result{}, err
	}
	if !claimed {
		return r.deferToHolder(ctx, req, held)
	}
	// The release is best effort and its failure is deliberately not returned.
	// A claim licenses nothing, so a claim left to lapse costs a delayed
	// takeover of work that was already safe to do concurrently -- while
	// replacing a real placement failure, or a success, with a release error
	// would report the wrong thing about the session.
	defer func() {
		_, _ = r.cfg.Claims.ReleaseReconciliationClaim(ctx, sessionstore.ReleaseReconciliationClaimRequest{
			TenantID: req.TenantID, SessionID: req.SessionID, HolderID: r.cfg.HolderID,
		})
	}()

	// THE RECORD IS RE-READ UNDER THE CLAIM before anything is decided from
	// it. The read above was taken before this replica held anything, and a
	// racer may have written a new desired generation and released its claim
	// in between; the dedicated arm below hands the record's intent to
	// EnsureWorkload, and an intent from the pre-claim read is an OBSOLETE
	// generation a controller may create over the current one (B5 spec gate
	// F2). The pre-claim read is used for the owned fast path only, which
	// takes no claim; placePooled re-reads again for its own reason.
	entry, err = r.cfg.Catalog.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{
		TenantID: req.TenantID, SessionID: req.SessionID,
	})
	if err != nil {
		return Result{}, err
	}

	writes := 0
	if req.Desired != nil {
		entry, writes, err = r.ensureDesired(ctx, req, entry, *req.Desired)
		if err != nil {
			return Result{}, err
		}
	}
	// A released dedicated session with open work gets its template back
	// BEFORE the controller check: desire is Factory-authored, and a
	// controller process never writes it, so a Factory replica composed with
	// no controller (H5's split) must still be the one that re-expresses it.
	if req.OpenWork {
		var revived int
		entry, revived, err = r.reviveReleased(ctx, req, entry)
		writes += revived
		if err != nil {
			return Result{}, err
		}
	}

	decision := Decision{Outcome: OutcomeUndecided}
	switch entry.Record.DesiredPlacement {
	case sessionwire.HostPlacementPooled:
		if r.cfg.Links != nil {
			return r.placePooled(ctx, req, writes)
		}
		page, err := r.cfg.Directory.Candidates(ctx, sessionstore.ListCompatibleHostsRequest{
			Key:   targetKey(entry.Record),
			Limit: r.cfg.CandidateLimit,
		})
		if err != nil {
			return Result{}, err
		}
		decision = Decide(entry.Record, owner, observed, page.Hosts, now)
	case sessionwire.HostPlacementDedicated:
		if r.cfg.Workloads == nil {
			return Result{}, ErrNoWorkloadController
		}
		intent, err := entry.Record.PlacementIntent()
		if err != nil {
			return Result{}, err
		}
		if err := r.cfg.Workloads.EnsureWorkload(ctx, intent); err != nil {
			return Result{}, err
		}
		result := Result{
			Decision:      Decision{Outcome: OutcomeReconcileDedicated},
			DesiredWrites: writes,
			Intent:        intent,
		}
		return r.placeDedicated(ctx, req, result)
	}
	return Result{Decision: decision, DesiredWrites: writes}, nil
}

// claim takes this session's scaling claim.
//
// The three answers are separated because they are three different facts: the
// claim is this replica's, another replica holds it until an instant, or the
// store failed. Collapsing the middle one into an error would make an ordinary
// two-replica deployment report failures on its happy path.
func (r *Reconciler) claim(ctx context.Context, req Request, now time.Time) (bool, *sessionstore.ReconcileError, error) {
	_, err := r.cfg.Claims.AcquireReconciliationClaim(ctx, sessionstore.AcquireReconciliationClaimRequest{
		TenantID: req.TenantID, SessionID: req.SessionID,
		HolderID: r.cfg.HolderID, ExpiresAt: now.Add(r.cfg.ClaimTTL),
	})
	if err == nil {
		return true, nil, nil
	}
	var reconcileErr *sessionstore.ReconcileError
	if errors.As(err, &reconcileErr) && reconcileErr.Code == sessionstore.ReconcileErrorHeld {
		return false, reconcileErr, nil
	}
	return false, nil, err
}

// deferToHolder is specification section 15 step 4's "observing an existing
// claim means re-read desired/observed state rather than scaling again".
//
// The re-reads are the point. The winning replica may have committed a new
// desired generation and finished placing between this replica's first catalog
// and registry reads and its refused acquisition. A caller told only "busy"
// would wait out the holder's horizon for a session whose desired and observed
// state is already settled.
func (r *Reconciler) deferToHolder(
	ctx context.Context,
	req Request,
	held *sessionstore.ReconcileError,
) (Result, error) {
	entry, err := r.cfg.Catalog.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{
		TenantID: req.TenantID, SessionID: req.SessionID,
	})
	if err != nil {
		return Result{}, err
	}
	owner, observed, err := r.cfg.Directory.Owner(ctx, req.TenantID, req.SessionID)
	if err != nil {
		return Result{}, err
	}
	if decision := Decide(entry.Record, owner, observed, nil, r.cfg.Clock.Now()); decision.Outcome == OutcomeReuseOwner &&
		(entry.Record.DesiredPlacement != sessionwire.HostPlacementDedicated || owner.HostGeneration == entry.Record.DesiredGeneration) {
		return Result{Decision: decision}, nil
	}
	return Result{Deferred: true, ClaimExpiresAt: held.ExpiresAt}, nil
}

// ensureDesired stores one desired intent, at most once.
//
// TWO MECHANISMS MAKE IT IDEMPOTENT AND THEY GUARD DIFFERENT THINGS. The
// content comparison stops this replica from writing an intent that is already
// stored, which is what keeps the desired generation still across repeated
// reconciliations -- and the generation is exactly what tells a controller its
// work is stale, so a reconciler that rewrote the same intent would invalidate
// every controller's completed work on every pass. The DERIVED idempotency key
// stops a second replica from writing it a second time: two Factories computing
// the same intent compute the same key, and SessionStore checks the key BEFORE
// the revision, so the loser's write is absorbed as a replay of the winner's
// rather than applied as a second generation. That is the property H5's "no
// leader is introduced" rests on for the dedicated half.
//
// The conflict path is what remains: a racer that wrote a DIFFERENT intent
// moves the revision without matching the key. One re-read and one retry
// resolve it, and the re-read runs the content comparison again -- so a racer
// that wrote what this replica wanted ends the work rather than starting a
// third attempt.
func (r *Reconciler) ensureDesired(
	ctx context.Context,
	req Request,
	entry sessionstore.CatalogEntry,
	desired Desired,
) (sessionstore.CatalogEntry, int, error) {
	const attempts = 2
	writes := 0
	for attempt := range attempts {
		if storedDesired(entry.Record, desired) {
			return entry, writes, nil
		}
		writes++
		updated, err := r.cfg.Catalog.UpdateCatalogDesiredState(ctx, sessionstore.UpdateCatalogDesiredStateRequest{
			TenantID: req.TenantID, SessionID: req.SessionID,
			ExpectedRevision:       entry.Revision,
			IdempotencyKey:         desiredKey(req, desired),
			DesiredPlacement:       desired.Placement,
			RuntimeCompatibilityID: desired.RuntimeCompatibilityID,
			DesiredWorkload:        desired.Workload,
		})
		if err == nil {
			return updated, writes, nil
		}
		var catalogErr *sessionstore.CatalogError
		if !errors.As(err, &catalogErr) || catalogErr.Code != sessionstore.CatalogErrorConflict || attempt == attempts-1 {
			return sessionstore.CatalogEntry{}, 0, err
		}
		if entry, err = r.cfg.Catalog.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{
			TenantID: req.TenantID, SessionID: req.SessionID,
		}); err != nil {
			return sessionstore.CatalogEntry{}, 0, err
		}
	}
	return entry, writes, nil
}

// reviveReleased re-expresses a released dedicated session's launch template
// as a new desired generation, at most once per release.
//
// WHAT IS WRITTEN is the template the session was created with (launchTemplate)
// under the session's own placement and runtime compatibility, so the
// controller receives the same workload payload its first generation was built
// from. The store issues the generation, and it strictly increases: the
// released generation is never reused, so a termination recorded for the
// generation whose workload ended is never superseded by this write, and a
// later release names a generation above this one.
//
// ONLY A RELEASE IS REPLACED. The write is a compare-and-swap on the revision
// read, and a conflict re-reads and re-decides from the fresh record: a racer
// that wrote any non-empty workload -- this same template, or a different one
// -- ends the work rather than being overwritten. Two replicas re-expressing
// one release derive one idempotency key (reviveKey names the released
// generation), so the loser is absorbed as a replay of the winner's write, and
// each later release gets a key of its own.
func (r *Reconciler) reviveReleased(
	ctx context.Context,
	req Request,
	entry sessionstore.CatalogEntry,
) (sessionstore.CatalogEntry, int, error) {
	const attempts = 2
	writes := 0
	for attempt := range attempts {
		record := entry.Record
		if !released(record) {
			return entry, writes, nil
		}
		template, ok := launchTemplate(record)
		if !ok {
			return sessionstore.CatalogEntry{}, writes, fmt.Errorf("%w: session %q", ErrNoLaunchTemplate, req.SessionID)
		}
		desired := Desired{
			Placement:              record.DesiredPlacement,
			RuntimeCompatibilityID: record.RuntimeCompatibilityID,
			Workload:               template,
		}
		writes++
		updated, err := r.cfg.Catalog.UpdateCatalogDesiredState(ctx, sessionstore.UpdateCatalogDesiredStateRequest{
			TenantID: req.TenantID, SessionID: req.SessionID,
			ExpectedRevision:       entry.Revision,
			IdempotencyKey:         reviveKey(req, desired, record.DesiredGeneration),
			DesiredPlacement:       desired.Placement,
			RuntimeCompatibilityID: desired.RuntimeCompatibilityID,
			DesiredWorkload:        desired.Workload,
		})
		if err == nil {
			return updated, writes, nil
		}
		var catalogErr *sessionstore.CatalogError
		if !errors.As(err, &catalogErr) || catalogErr.Code != sessionstore.CatalogErrorConflict || attempt == attempts-1 {
			return sessionstore.CatalogEntry{}, writes, err
		}
		if entry, err = r.cfg.Catalog.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{
			TenantID: req.TenantID, SessionID: req.SessionID,
		}); err != nil {
			return sessionstore.CatalogEntry{}, writes, err
		}
	}
	return entry, writes, nil
}

// released reports deletion desire: a dedicated placement naming no workload.
func released(record sessionstore.CatalogRecord) bool {
	return record.DesiredPlacement == sessionwire.HostPlacementDedicated &&
		record.DesiredWorkload.PayloadVersion == "" && len(record.DesiredWorkload.Payload) == 0
}

// launchTemplate is the workload a released dedicated session is re-created
// from: the public create's InitialWorkload, retained by SessionStore as
// immutable provenance that desired-state writes never rewrite. See
// ErrNoLaunchTemplate for why nothing else is consulted.
func launchTemplate(record sessionstore.CatalogRecord) (sessionstore.DesiredWorkload, bool) {
	if record.PublicCreate == nil {
		return sessionstore.DesiredWorkload{}, false
	}
	workload := record.PublicCreate.InitialWorkload
	if workload.PayloadVersion == "" || len(workload.Payload) == 0 {
		return sessionstore.DesiredWorkload{}, false
	}
	return workload, true
}

// reviveKey derives the idempotency key for re-expressing one release.
//
// It is desiredKey's intent framing plus the RELEASED generation, and the
// addition is load-bearing: SessionStore compares a key only with the record's
// current one, so a content-only key would be the same for every release of a
// session, and two replicas racing one release must collide while a replica
// acting on a LATER release must not be taken for a replay of an earlier one.
func reviveKey(req Request, desired Desired, releasedGeneration uint64) string {
	return "revive-" + framedDigest(
		[]byte(req.TenantID),
		[]byte(req.SessionID),
		[]byte(desired.Placement),
		[]byte(desired.RuntimeCompatibilityID),
		[]byte(desired.Workload.PayloadVersion),
		desired.Workload.Payload,
		[]byte(strconv.FormatUint(releasedGeneration, 10)),
	)
}

// storedDesired reports whether the record already carries this intent.
//
// The payload is compared by BYTES rather than by the stored idempotency key.
// A record's key is whatever its last writer chose -- a create names its
// CommandID -- so a key comparison would answer "some other writer's intent"
// rather than "this intent", and a session created with a workload would be
// rewritten by the first reconciliation that agreed with it.
func storedDesired(record sessionstore.CatalogRecord, desired Desired) bool {
	return record.DesiredPlacement == desired.Placement &&
		record.RuntimeCompatibilityID == desired.RuntimeCompatibilityID &&
		record.DesiredWorkload.PayloadVersion == desired.Workload.PayloadVersion &&
		string(record.DesiredWorkload.Payload) == string(desired.Workload.Payload)
}

// desiredKey derives the idempotency key naming one intent for one session.
//
// It must be a function of the intent alone, because that is what makes two
// replicas that decided the same thing collide on SessionStore's key check
// instead of applying two generations. It is therefore NOT a UUID and not
// clock-derived, and the replica's own identity is deliberately absent.
//
// Every field is length-prefixed before hashing. Concatenation alone would let
// two different intents share a key -- a payload version of "v1" with a payload
// of "2x" hashes the same as "v12" with "x" -- and two intents sharing a key is
// precisely the case SessionStore's contract warns about: a reused key for a
// NEW intent succeeds without applying anything.
//
// The prefix is the DECIMAL length followed by a NUL, rather than a fixed-width
// integer, and that is not a style choice: a four-byte width has to narrow an
// int to a uint32 somewhere, which is a silent truncation for a field longer
// than four gigabytes -- exactly the collision this framing exists to prevent,
// reintroduced by the mechanism preventing it. A NUL cannot appear among
// decimal digits, so the boundary is unambiguous and no conversion is needed.
func desiredKey(req Request, desired Desired) string {
	return "placement-" + framedDigest(
		[]byte(req.TenantID),
		[]byte(req.SessionID),
		[]byte(desired.Placement),
		[]byte(desired.RuntimeCompatibilityID),
		[]byte(desired.Workload.PayloadVersion),
		desired.Workload.Payload,
	)
}

// framedDigest hashes length-prefixed fields with desiredKey's framing.
func framedDigest(fields ...[]byte) string {
	digest := sha256.New()
	for _, field := range fields {
		digest.Write([]byte(strconv.Itoa(len(field))))
		digest.Write([]byte{0})
		digest.Write(field)
	}
	return hex.EncodeToString(digest.Sum(nil))
}

// targetKey is the directory scope a session's own record names. It is the
// same derivation internal/admission applies, and it is what keeps a capacity
// page scoped to the runtime the session's records were written by.
func targetKey(record sessionstore.CatalogRecord) sessionstore.HostTargetKey {
	return sessionstore.HostTargetKey{
		AgentID:                record.AgentID,
		RuntimeCompatibilityID: record.RuntimeCompatibilityID,
		Placement:              record.DesiredPlacement,
	}
}
