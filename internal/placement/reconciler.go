package placement

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
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
// It is H5's two-binary split made a runtime rule rather than a convention.
// The decision of 2026-09-04 puts the Kubernetes adapter in
// internal/placement/kubernetes and links it into cmd/controller ALONE:
// cmd/factory ships with a different ServiceAccount holding no workload
// create/delete RBAC, so it composes no controller and its import graph reaches
// no Kubernetes client package. A dedicated session that reaches that binary
// has therefore arrived somewhere that cannot serve it, and saying so by name
// is the only honest answer -- reporting it as "no capacity" would send a
// caller into a retry loop waiting for an autoscaler that will never run, and
// reporting success would claim a workload nobody created.
var ErrNoWorkloadController = errors.New("placement: this replica reconciles no dedicated workloads")

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

// WorkloadController creates and updates the platform workload a dedicated
// session needs. A4.2 step 3.
//
// ITS SHAPE FOLLOWS H5 (answered 2026-09-04), and each part of the answer shows
// up here as a constraint rather than as prose:
//
//   - The adapter is INTERNAL to this module, at internal/placement/kubernetes,
//     so this interface may not name a platform type. sessionstore.PlacementIntent
//     is the whole currency: it is Factory-authored desire and nothing else --
//     no lease epoch, no HostID, no residency -- and its opaque workload payload
//     is a byte string this module never parses. A Kubernetes PodSpec, a Nomad
//     job and a future platform's manifest are the same value to it.
//   - The intent carries its GENERATION, which is what a controller records
//     against the workload it created. A controller that reconciled generation 7
//     and is handed 7 again has nothing to do, however many Host heartbeats have
//     moved the record's revision in between. That is what makes calling this on
//     every reconciliation cheap rather than wasteful. (Attribution, since it is
//     easy to over-read: H5's recorded text contains no "records generation"
//     clause. The requirement is A4.2 step 1's idempotent desired generation and
//     the field is sessionstore.PlacementIntent.Generation, a member by
//     construction. H5 decides the adapter's placement, not this.)
//   - Multiple replicas of either binary run with NO leader election, so this
//     is called concurrently for one session by design. Deterministic workload
//     identity is the mechanism that makes that safe -- the claim only
//     suppresses duplicate work -- and an implementation whose creation is not
//     name-deterministic breaks the decision rather than this interface.
//
// It has exactly one method. Deletion is deliberately absent: specification
// section 13 makes the ordering normative -- request drain, wait for an
// epoch-fenced checkpoint and an observed cold release, THEN delete -- and none
// of those steps exists in this module yet. A Delete declared here today would
// be a seam with no implementation, no caller and no test, which is the shape
// of the "guarantee" this lane has twice found inert. The operative widener is
// D1.1 step 1, which specifies ensure, observe, request-drain and delete;
// D2.2 supplies the drain protocol those last two depend on. A4.2 step 3 asks
// for a NARROW interface, so one method is what was requested.
type WorkloadController interface {
	EnsureWorkload(ctx context.Context, intent sessionstore.PlacementIntent) error
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
	// than a degraded one: H5's cmd/factory composes exactly this. See
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
}

// Reconciler places one session at a time.
//
// It holds no per-session state. Everything it decides from is read within the
// call: the catalog record, the registry observation and one capacity page. A
// replica that restarts mid-placement leaves only a claim, which lapses.
type Reconciler struct{ cfg Config }

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

	// Intent is the stored desired state handed to the controller, populated
	// only for OutcomeReconcileDedicated.
	Intent sessionstore.PlacementIntent
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
//     observed state rather than scaling again, so the deferral path re-reads
//     the registry and reports an owner the winner has just produced.
//  3. Desired state is written before the decision, because the write can
//     change the desired placement the decision branches on.
//
// What this does NOT do is attach the session to the pooled Host it selects.
// OutcomeAttachPooled names a candidate for the caller; see the package's
// README section for why the pinned Core wire version has no request that could
// carry that attachment.
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
	if decision := Decide(entry.Record, owner, observed, nil, now); decision.Outcome == OutcomeReuseOwner {
		return Result{Decision: decision}, nil
	}

	claimed, held, err := r.claim(ctx, req, now)
	if err != nil {
		return Result{}, err
	}
	if !claimed {
		return r.deferToHolder(ctx, req, entry.Record, held)
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

	writes := 0
	if req.Desired != nil {
		entry, writes, err = r.ensureDesired(ctx, req, entry, *req.Desired)
		if err != nil {
			return Result{}, err
		}
	}

	decision := Decision{Outcome: OutcomeUndecided}
	switch entry.Record.DesiredPlacement {
	case sessionwire.HostPlacementPooled:
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
		return Result{
			Decision:      Decision{Outcome: OutcomeReconcileDedicated},
			DesiredWrites: writes,
			Intent:        intent,
		}, nil
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
// The re-read is the point. The winning replica may have finished placing
// between this replica's first registry read and its refused acquisition, and a
// caller told only "busy" would wait out the holder's horizon for a session
// that already has a Host.
func (r *Reconciler) deferToHolder(
	ctx context.Context,
	req Request,
	record sessionstore.CatalogRecord,
	held *sessionstore.ReconcileError,
) (Result, error) {
	owner, observed, err := r.cfg.Directory.Owner(ctx, req.TenantID, req.SessionID)
	if err != nil {
		return Result{}, err
	}
	if decision := Decide(record, owner, observed, nil, r.cfg.Clock.Now()); decision.Outcome == OutcomeReuseOwner {
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
	digest := sha256.New()
	for _, field := range [][]byte{
		[]byte(req.TenantID),
		[]byte(req.SessionID),
		[]byte(desired.Placement),
		[]byte(desired.RuntimeCompatibilityID),
		[]byte(desired.Workload.PayloadVersion),
		desired.Workload.Payload,
	} {
		digest.Write([]byte(strconv.Itoa(len(field))))
		digest.Write([]byte{0})
		digest.Write(field)
	}
	return "placement-" + hex.EncodeToString(digest.Sum(nil))
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
