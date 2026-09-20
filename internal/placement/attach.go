package placement

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
)

// This file is B5: the caller that ACTS on OutcomeAttachPooled. Before it,
// Decide named a Host and nothing asked that Host for anything, so no Factory
// could make a session resident anywhere.
//
// The sequence is specification section 15 steps 4 and 5 and nothing more:
// claim, re-read, choose a ranked admissible candidate, ask it to attach, and
// bind with the epoch its answer carries. What each Host answer means is
// decided here, once, by attachAnswer, and every answer is typed before it is
// acted on.

// ErrAttachUnsupported classifies a candidate that did not advertise
// hostlink.attach in its connect reply. A HostLinks implementation wraps it.
//
// It is a CAPABILITY fact, not a refusal, and it excludes the candidate rather
// than failing the placement: a mixed fleet with some Hosts that predate attach
// must still place onto the ones that do. A Host that predates the method
// would resolve it as a channel and answer runtime_unavailable, which is why a
// candidate is never sent one it did not advertise -- the local refusal is the
// only way to tell "cannot" from "will not".
var ErrAttachUnsupported = errors.New("placement: host did not advertise hostlink.attach")

// ErrHostUnreachable classifies a candidate this replica could not ask at all:
// the dial failed, the link is between connections, or this replica already
// holds as many links as it may. A HostLinks implementation wraps it.
//
// It is the one transport failure that moves placement on to the next
// candidate, and it is reserved for failures BEFORE the request left this
// process. A failure after the request may have reached the Host is not this:
// the Host may have attached, and moving on would put a second attach in
// flight for no reason. That failure aborts the attempt instead, and the next
// sweep retries under the same idempotency key.
var ErrHostUnreachable = errors.New("placement: host could not be reached")

// ErrAttachFailed classifies a candidate that ANSWERED the attach with a
// failure carrying no Core code -- a transport error reply such as
// centrifuge's ErrorInternal. A HostLinks implementation wraps it.
//
// It moves placement on to the next candidate, like a coded refusal and unlike
// the ambiguous failures that abort. It is NOT a placement outcome and not a
// promise that the Host rolled back: host v0.2.1 sends it after a failed
// launch, but also after a rollback that could not complete, and after an
// attach that succeeded but whose observation could not be published -- the
// session may be RESIDENT there. What makes moving on safe is the session
// lease: a Host that still holds it makes the next candidate refuse
// epoch_mismatch, so no second residency can form, and the re-read converges
// on the owner. Aborting on it instead let one Host whose launches always fail
// -- and which therefore never loses capacity and is ranked first on every
// pass -- block placement for every session of its agent and runtime (B5
// quality gate Q1).
var ErrAttachFailed = errors.New("placement: host answered the attach with a failure")

// ErrTenantUnaddressable classifies a candidate whose advertised BASE cannot
// carry this session's tenant's HostLink address: Core's HostLinkEndpoint
// refused to derive it (a tenant too long for what the base leaves room for,
// a tenant Core will not route, or a base that is not bare). A HostLinks
// implementation wraps it, and keeps Core's *sessionwire.HostLinkEndpointError
// reachable with errors.As so the code can be logged.
//
// It is PER TENANT and decided before anything is dialled: the candidate is
// skipped for this session's tenant and the next one is asked, exactly as for
// an unreachable Host, and nothing about the Host is concluded for any other
// tenant. It is logged because it is a deployment fact an operator can act on
// -- a Host whose name leaves no room for a tenant, or one still advertising a
// pre-v0.3.0 endpoint -- and a silent skip would read as "no capacity".
var ErrTenantUnaddressable = errors.New("placement: the candidate's advertised base cannot carry this tenant's HostLink address")

// ErrRegistryStale reports that every re-placement this call was allowed ended
// in epoch_mismatch: the session's lease is held elsewhere and the registry has
// not caught up. The work is left for the next sweep, which re-reads the
// registry from the start.
var ErrRegistryStale = errors.New("placement: the session lease is held elsewhere and the registry did not show the holder")

// ErrBindAfterAttach reports an attach the Host accepted followed by a bind
// that failed -- refused by the Host, or refused LOCALLY by this replica's pool
// because it still routes the session to a different Host (a binding
// conflict). The session IS resident; what failed is this replica's route to
// it.
// It is reported rather than retried here because the attach is done and the
// next thing that needs a route -- a viewer, a delivery, the next sweep -- asks
// the registry, which now names the owner.
var ErrBindAfterAttach = errors.New("placement: the host attached the session but the bind that followed failed")

// ErrDedicatedEndpointInvalid reports a ready endpoint that does not describe
// the generation Factory just ensured, or cannot be dialled for this tenant.
var ErrDedicatedEndpointInvalid = errors.New("placement: dedicated workload endpoint is invalid")

// ErrDedicatedObservationInvalid reports a post-attach registry observation
// for the wrong intent. It must not be used as an attach target or bind source.
var ErrDedicatedObservationInvalid = errors.New("placement: dedicated workload observation does not match the intent")

// AttachRefusal is a Host's own coded answer to an attach. A HostLinks
// implementation returns it for a Host's HostLinkError.
//
// It embeds Core's record for the reason hostlink.HostRefusal does: Core
// already states which detail travels with which code, and a flat copy would
// be a second authority for that rule.
type AttachRefusal struct {
	sessionwire.HostLinkError
}

func (e *AttachRefusal) Error() string {
	return fmt.Sprintf("placement: host refused the attach: %s", e.Code)
}

// HostLinks is the Host-facing half of pooled placement, stated in Core's
// vocabulary. The HostLink pool implements it through one adapter in the
// composition, which is also where transport errors are classified onto
// ErrAttachUnsupported, ErrHostUnreachable and *AttachRefusal: this package
// names no transport type, for the reason internal/routing's Binder gives.
type HostLinks interface {
	Attach(ctx context.Context, endpoint sessionwire.InternalEndpoint, req sessionwire.HostLinkAttachRequest) (sessionwire.HostLinkRegistryObservation, error)
	Bind(ctx context.Context, endpoint sessionwire.InternalEndpoint, req sessionwire.HostLinkBindRequest) error
	Unbind(ctx context.Context, req sessionwire.HostLinkUnbindRequest) error
	DeliverCommand(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, delivery sessionwire.HostLinkCommandDelivery) error
	// RouteFor reports whether this replica already routes the session, and
	// to which Host. Placement leaves a route it did not create in place.
	RouteFor(tenant sessionwire.TenantID, session sessionwire.SessionID) (sessionwire.HostID, bool)
	// AcceptsGateResponses reports whether the Host an observation names can
	// apply a gate_response command, asked of that tenant's link and decided
	// by the one capability predicate (hostlink.GateResponseCapable). An error
	// means it could not be asked, and is never read as "can".
	AcceptsGateResponses(ctx context.Context, owner sessionwire.HostLinkRegistryObservation) (bool, error)
}

// placeDedicated discovers the Host created for the claimed intent. The
// controller's endpoint only tells us where to ask; the Host's attach reply is
// the first evidence of residency and the only source of a new bind epoch.
func (r *Reconciler) placeDedicated(ctx context.Context, req Request, result Result) (Result, error) {
	intent := result.Intent
	observed, found, err := r.cfg.Workloads.ObserveWorkload(ctx, intent)
	if err != nil {
		return result, err
	}
	entry, err := r.cfg.Catalog.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{TenantID: req.TenantID, SessionID: req.SessionID})
	if err != nil {
		return result, err
	}
	if entry.Record.DesiredPlacement != sessionwire.HostPlacementDedicated || entry.Record.DesiredGeneration != intent.Generation {
		result.Decision = Decision{Outcome: OutcomeUndecided}
		return result, nil
	}
	owner, hasOwner, err := r.cfg.Directory.Owner(ctx, req.TenantID, req.SessionID)
	if err != nil {
		return result, err
	}
	if hasOwner && ReusableOwner(owner, entry.Record, r.cfg.Clock.Now()) && owner.HostGeneration == intent.Generation {
		result.Decision = Decision{Outcome: OutcomeReuseOwner, Owner: owner}
		return r.wake(ctx, req, owner, result)
	}
	if found {
		if err := observed.Validate(); err != nil || !ReusableOwner(observed, entry.Record, r.cfg.Clock.Now()) || observed.HostGeneration != intent.Generation {
			return result, fmt.Errorf("%w: observed route conflicts with intended workload", ErrDedicatedObservationInvalid)
		}
		result.Decision = Decision{Outcome: OutcomeReuseOwner, Owner: observed}
		return r.wake(ctx, req, observed, result)
	}
	if r.cfg.Links == nil {
		return result, nil
	}
	discovery, ok := r.cfg.Workloads.(WorkloadEndpointDiscovery)
	if !ok {
		return result, ErrWorkloadEndpointUnsupported
	}
	hostID, generation, base, ready, err := discovery.WorkloadEndpoint(ctx, intent)
	if err != nil || !ready {
		return result, err
	}
	endpoint := DedicatedEndpoint{HostID: hostID, HostGeneration: generation, InternalEndpoint: base}
	if err := endpoint.HostID.Validate(); err != nil || endpoint.HostGeneration != intent.Generation {
		return result, fmt.Errorf("%w: host identity or generation", ErrDedicatedEndpointInvalid)
	}
	if _, err := sessionwire.HostLinkEndpoint(endpoint.InternalEndpoint, intent.TenantID); err != nil {
		return result, fmt.Errorf("%w: %w", ErrDedicatedEndpointInvalid, err)
	}
	// Endpoint discovery may block while a new desire is committed. The claim
	// suppresses duplicate scaling; it does not fence the catalog writer.
	entry, err = r.cfg.Catalog.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{TenantID: req.TenantID, SessionID: req.SessionID})
	if err != nil {
		return result, err
	}
	if entry.Record.DesiredPlacement != sessionwire.HostPlacementDedicated || entry.Record.DesiredGeneration != intent.Generation {
		result.Decision = Decision{Outcome: OutcomeUndecided}
		return result, nil
	}
	if len(req.GateResponses) > 0 {
		capable, err := r.cfg.Links.AcceptsGateResponses(ctx, sessionwire.HostLinkRegistryObservation{
			TenantID: req.TenantID, SessionID: req.SessionID, HostID: endpoint.HostID,
			HostGeneration: endpoint.HostGeneration, InternalEndpoint: endpoint.InternalEndpoint,
		})
		switch verdict, err := classifyGateCapability(capable, err); verdict {
		case gateUnaddressable:
			result.Unaddressable = append(result.Unaddressable, endpoint.HostID)
			return result, nil
		case gateUnreachable:
			result.Unreachable = append(result.Unreachable, endpoint.HostID)
			return result, nil
		case gateAbort:
			return result, err
		case gateIncapable:
			result.Incapable = append(result.Incapable, endpoint.HostID)
			return result, nil
		}
		entry, err = r.cfg.Catalog.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{TenantID: req.TenantID, SessionID: req.SessionID})
		if err != nil {
			return result, err
		}
		if entry.Record.DesiredPlacement != sessionwire.HostPlacementDedicated || entry.Record.DesiredGeneration != intent.Generation {
			result.Decision = Decision{Outcome: OutcomeUndecided}
			return result, nil
		}
	}
	candidate := sessionwire.HostLinkCapacityReport{HostID: endpoint.HostID, HostGeneration: endpoint.HostGeneration}
	observation, err := r.cfg.Links.Attach(ctx, endpoint.InternalEndpoint, attachRequest(entry.Record, candidate, attachMode(entry.Record), r.cfg.ActorID))
	switch answer, code := attachAnswer(err); answer {
	case answerAccepted:
		if err := observation.Validate(); err != nil || !ReusableOwner(observation, entry.Record, r.cfg.Clock.Now()) ||
			observation.HostID != endpoint.HostID || observation.HostGeneration != endpoint.HostGeneration || observation.InternalEndpoint != endpoint.InternalEndpoint {
			return result, fmt.Errorf("%w: attach reply conflicts with intended workload", ErrDedicatedObservationInvalid)
		}
		result.Attached = observation
		return r.bindAttached(ctx, req, observation, result)
	case answerStale:
		result.Refused = append(result.Refused, CandidateRefusal{HostID: endpoint.HostID, HostGeneration: endpoint.HostGeneration, Code: code})
		current, found, readErr := r.cfg.Directory.Owner(ctx, req.TenantID, req.SessionID)
		if readErr != nil {
			return result, readErr
		}
		if found && ReusableOwner(current, entry.Record, r.cfg.Clock.Now()) && current.HostGeneration == intent.Generation {
			result.Decision = Decision{Outcome: OutcomeReuseOwner, Owner: current}
			return r.wake(ctx, req, current, result)
		}
		return result, ErrRegistryStale
	case answerRefused:
		result.Refused = append(result.Refused, CandidateRefusal{HostID: endpoint.HostID, HostGeneration: endpoint.HostGeneration, Code: code})
		return result, nil
	case answerUnsupported:
		result.Excluded = append(result.Excluded, endpoint.HostID)
		return result, ErrAttachUnsupported
	case answerUnreachable:
		result.Unreachable = append(result.Unreachable, endpoint.HostID)
		return result, nil
	case answerUnaddressable:
		result.Unaddressable = append(result.Unaddressable, endpoint.HostID)
		return result, nil
	case answerFailed:
		result.Failed = append(result.Failed, endpoint.HostID)
		return result, nil
	default:
		return result, err
	}
}

// CandidateRefusal is one candidate a placement asked and was refused by.
type CandidateRefusal struct {
	HostID         sessionwire.HostID
	HostGeneration uint64
	Code           sessionwire.HostLinkErrorCode
}

// defaultReplaceAttempts and defaultReplaceBackoff are the defaults a Config with a zero
// bound takes. They are small on purpose: every re-placement runs inside the
// session's reconciliation claim, and the claim's TTL is what bounds how long a
// placement may hold it before another replica may start the same work.
const (
	defaultReplaceAttempts = 3
	defaultReplaceBackoff  = 200 * time.Millisecond

	// maxReplaceAttempts bounds a configured ReplaceAttempts. Every attempt
	// runs inside one reconciliation claim and one sweep pass, so a bound in
	// the tens would already outlive both; this one refuses a configuration
	// that could only ever be cut short by them.
	maxReplaceAttempts = 10
	// maxReplaceBackoff caps one wait. Doubling from the base would otherwise
	// overflow time.Duration to a NEGATIVE wait at about 35 attempts with a
	// one-second base (B5 quality gate Q8); the cap is reached long before.
	maxReplaceBackoff = 5 * time.Second
)

// attachConfigError reports why a configuration that composes Links cannot
// attach. A configuration without Links is the naming-only reconciler and
// needs none of these.
func (cfg Config) attachConfigError() error {
	if cfg.Links == nil {
		return nil
	}
	switch {
	case cfg.ActorID == "":
		return fmt.Errorf("%w: ActorID must name the requesting service when Links is set", ErrInvalidConfig)
	case cfg.ReplaceAttempts < 0 || cfg.ReplaceAttempts > maxReplaceAttempts:
		return fmt.Errorf("%w: ReplaceAttempts must be between 0 and %d", ErrInvalidConfig, maxReplaceAttempts)
	case cfg.ReplaceBackoff < 0 || cfg.ReplaceBackoff > maxReplaceBackoff:
		return fmt.Errorf("%w: ReplaceBackoff must be between 0 and %v", ErrInvalidConfig, maxReplaceBackoff)
	}
	return nil
}

func (r *Reconciler) replaceAttempts() int {
	if r.cfg.ReplaceAttempts == 0 {
		return defaultReplaceAttempts
	}
	return r.cfg.ReplaceAttempts
}

// backoff is the NOMINAL wait before re-placement number n (n >= 1): the
// base, doubled per further attempt, capped at maxReplaceBackoff. It doubles
// by multiplication against the cap rather than by shifting, so no n can
// overflow it.
func (r *Reconciler) backoff(n int) time.Duration {
	d := r.cfg.ReplaceBackoff
	if d == 0 {
		d = defaultReplaceBackoff
	}
	for i := 1; i < n && d < maxReplaceBackoff; i++ {
		d *= 2
	}
	return min(d, maxReplaceBackoff)
}

// jittered is the wait actually slept for a nominal backoff d: uniformly in
// [d/2, d]. Two replicas that met the same epoch_mismatch at the same instant
// -- which the claim makes rare but a lapsed claim allows -- then do not retry
// in lockstep. The Wait seam receives the NOMINAL duration, so a test states
// the schedule rather than a random draw.
func jittered(d time.Duration) time.Duration {
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + rand.N(d-half+1) // #nosec G404 -- retry jitter, not a secret
}

func (r *Reconciler) wait(ctx context.Context, d time.Duration) error {
	if r.cfg.Wait != nil {
		return r.cfg.Wait(ctx, d)
	}
	timer := time.NewTimer(r.drawOrDefault()(d))
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// drawOrDefault is the draw the default wait uses: the injected one, or
// jittered. It is its own function so production's choice is observable (B5
// v0.3.0 regate G4): a nil draw that defaulted to the nominal duration would
// never jitter, and every replica would retry a contended session in step.
func (r *Reconciler) drawOrDefault() func(time.Duration) time.Duration {
	if r.draw != nil {
		return r.draw
	}
	return jittered
}

func (r *Reconciler) logger() *slog.Logger {
	if r.cfg.Logger != nil {
		return r.cfg.Logger
	}
	return slog.New(slog.DiscardHandler)
}

// placePooled attaches a claimed, unowned pooled session to the first ranked
// admissible candidate that accepts it, and binds with the epoch the Host
// answered.
//
// THE OWNER IS RE-READ HERE, AFTER THE CLAIM, and not trusted from the read
// Reconcile made before it. That read was taken before this replica held
// anything: a racer can finish placing the session between it and the claim,
// release its claim, and leave this replica holding a claim over a session
// that is already resident. Acting on the earlier read would widen what should
// be a zero-width window into the whole claim acquisition. The re-read costs
// one registry read on the placement path only; the owned common path returns
// before any claim and never reaches here.
//
// EACH HOST ANSWER HAS ONE MEANING, and the table in attachAnswer is the whole
// of it:
//
//   - accepted: bind with the RETURNED epoch, deliver the wake, done.
//   - epoch_mismatch: the lease is held elsewhere, so the registry this
//     placement read was stale. Its current_lease_epoch is the OTHER holder's
//     and is never bound with -- it is not even read. The candidate list is
//     abandoned and placement re-runs from the owner read, bounded, with
//     backoff (section 15 step 5: refresh and retry through the same key).
//   - any other code: this candidate refused; try the next.
//   - unsupported or unreachable before the request left: exclude, try next.
//   - the Host answered with a code-less failure (ErrAttachFailed): try next.
//   - anything else: the Host may have acted, so stop and let the next sweep
//     retry under the same key rather than put a second attach in flight.
func (r *Reconciler) placePooled(ctx context.Context, req Request, writes int) (Result, error) {
	result := Result{DesiredWrites: writes}
	incapableWhy := map[sessionwire.HostID]string{}
	defer func() { r.reportIncapable(ctx, req, &result, incapableWhy) }()
	// The RECORD is re-read under the claim for the owner's reason: a racer may
	// have changed the desired state between Reconcile's first read and the
	// claim, and the attach is built from this record's agent and runtime.
	entry, err := r.cfg.Catalog.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{
		TenantID: req.TenantID, SessionID: req.SessionID,
	})
	if err != nil {
		return result, err
	}
	record := entry.Record
	if record.DesiredPlacement != sessionwire.HostPlacementPooled {
		// The desired placement moved under the claim. Placing it here would
		// act on a decision the record no longer makes; the next pass reads
		// the new one from the top.
		result.Decision = Decision{Outcome: OutcomeUndecided}
		return result, nil
	}
	mode := attachMode(record)
	attempts := r.replaceAttempts()

	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			result.Replacements++
			if err := r.wait(ctx, r.backoff(attempt)); err != nil {
				return result, err
			}
		}
		owner, observed, err := r.cfg.Directory.Owner(ctx, req.TenantID, req.SessionID)
		if err != nil {
			return result, err
		}
		now := r.cfg.Clock.Now()
		if observed && ReusableOwner(owner, record, now) {
			result.Decision = Decision{Outcome: OutcomeReuseOwner, Owner: owner}
			return r.wake(ctx, req, owner, result)
		}
		page, err := r.cfg.Directory.Candidates(ctx, sessionstore.ListCompatibleHostsRequest{
			Key:   targetKey(record),
			Limit: r.cfg.CandidateLimit,
		})
		if err != nil {
			return result, err
		}

		stale := false
	candidates:
		for _, candidate := range page.Hosts {
			if !admissible(candidate, record) {
				continue
			}
			if len(req.GateResponses) > 0 {
				switch verdict, err := r.appliesGateResponses(ctx, req, candidate); verdict {
				case gateUnaddressable:
					result.Unaddressable = append(result.Unaddressable, candidate.HostID)
					r.logUnaddressable(ctx, req, candidate, err)
					continue
				case gateIncapable:
					result.Incapable = append(result.Incapable, candidate.HostID)
					if err != nil {
						incapableWhy[candidate.HostID] = err.Error()
					}
					continue
				case gateUnreachable:
					result.Unreachable = append(result.Unreachable, candidate.HostID)
					continue
				case gateAbort:
					return result, err
				}
			}
			observation, err := r.cfg.Links.Attach(ctx, candidate.InternalEndpoint, attachRequest(record, candidate, mode, r.cfg.ActorID))
			switch answer, code := attachAnswer(err); answer {
			case answerAccepted:
				result.Decision = Decision{Outcome: OutcomeAttachPooled, Target: candidate}
				result.Attached = observation
				return r.bindAttached(ctx, req, observation, result)
			case answerStale:
				result.Refused = append(result.Refused, CandidateRefusal{HostID: candidate.HostID, HostGeneration: candidate.HostGeneration, Code: code})
				stale = true
				break candidates
			case answerRefused:
				result.Refused = append(result.Refused, CandidateRefusal{HostID: candidate.HostID, HostGeneration: candidate.HostGeneration, Code: code})
			case answerUnsupported:
				result.Excluded = append(result.Excluded, candidate.HostID)
				r.logger().WarnContext(ctx, "placement: excluded a pooled candidate that does not advertise hostlink.attach",
					slog.String("host_id", string(candidate.HostID)),
					slog.Uint64("host_generation", candidate.HostGeneration),
					slog.String("tenant_id", string(req.TenantID)),
					slog.String("session_id", string(req.SessionID)))
			case answerUnreachable:
				result.Unreachable = append(result.Unreachable, candidate.HostID)
			case answerUnaddressable:
				result.Unaddressable = append(result.Unaddressable, candidate.HostID)
				r.logUnaddressable(ctx, req, candidate, err)
			case answerFailed:
				result.Failed = append(result.Failed, candidate.HostID)
				r.logger().WarnContext(ctx, "placement: a pooled candidate failed the attach; trying the next",
					slog.String("host_id", string(candidate.HostID)),
					slog.Uint64("host_generation", candidate.HostGeneration),
					slog.String("tenant_id", string(req.TenantID)),
					slog.String("session_id", string(req.SessionID)),
					slog.String("error", err.Error()))
			default:
				return result, fmt.Errorf("placement: attach to %q: %w", candidate.HostID, err)
			}
		}
		if !stale {
			result.Decision = Decision{Outcome: OutcomeNoCapacity}
			return result, nil
		}
		if attempt+1 >= attempts {
			result.Decision = Decision{Outcome: OutcomeNoCapacity}
			return result, ErrRegistryStale
		}
	}
}

// gateCapability is the capable-only placement filter's answer for one
// candidate.
type gateCapability uint8

const (
	gateCapable gateCapability = iota
	gateIncapable
	gateUnaddressable
	gateUnreachable
	gateAbort
)

// appliesGateResponses is the capable-only placement filter: a session with a
// PENDING gate response is placed only on a Host whose connect reply carries
// Core's gate_response capability token (hostlink.GateResponseCapable, asked
// through the same Links seam the wake uses). A candidate that answers no is
// skipped as Incapable; a transiently unreachable candidate is Unreachable,
// and an unclassified read error aborts this pass. If none can apply the
// response, the session waits (OutcomeNoCapacity) rather than being placed
// where its answer would be refused or left applying.
//
// A candidate whose base cannot address the tenant is NOT "incapable": the
// question could not even be addressed, and it is recorded and logged as
// unaddressable with Core's code, exactly as the attach path does (spec gate
// N4). The incapable skips are reported by reportIncapable, once per pass.
func (r *Reconciler) appliesGateResponses(ctx context.Context, req Request, candidate sessionwire.HostLinkCapacityReport) (gateCapability, error) {
	capable, err := r.cfg.Links.AcceptsGateResponses(ctx, sessionwire.HostLinkRegistryObservation{
		TenantID: req.TenantID, SessionID: req.SessionID,
		HostID: candidate.HostID, HostGeneration: candidate.HostGeneration,
		InternalEndpoint: candidate.InternalEndpoint,
	})
	return classifyGateCapability(capable, err)
}

func classifyGateCapability(capable bool, err error) (gateCapability, error) {
	switch {
	case err == nil && capable:
		return gateCapable, nil
	case err == nil:
		return gateIncapable, nil
	case errors.Is(err, ErrTenantUnaddressable):
		return gateUnaddressable, err
	case errors.Is(err, ErrHostUnreachable):
		return gateUnreachable, err
	default:
		return gateAbort, err
	}
}

// logUnaddressable is the one WARN for a candidate whose advertised base cannot
// carry this session's tenant's address, from either the attach or the filter.
func (r *Reconciler) logUnaddressable(ctx context.Context, req Request, candidate sessionwire.HostLinkCapacityReport, err error) {
	r.logger().WarnContext(ctx, "placement: skipped a pooled candidate whose advertised base cannot carry this tenant's HostLink address",
		slog.String("host_id", string(candidate.HostID)),
		slog.Uint64("host_generation", candidate.HostGeneration),
		slog.String("tenant_id", string(req.TenantID)),
		slog.String("session_id", string(req.SessionID)),
		slog.String("code", string(endpointCode(err))),
		slog.String("internal_endpoint", string(candidate.InternalEndpoint)))
}

// incapableReportInterval is how long a session's "waiting for a capable
// Host" WARN is suppressed after it was last written. A session with a pending
// gate response and no capable Host is re-examined on every sweep; one line per
// skipped candidate per sweep (quality gate F6) buried the one fact an operator
// needs, which is that the session is waiting and on which Hosts.
const incapableReportInterval = 5 * time.Minute

// maxIncapableReports bounds the suppression table; past it, expired entries
// are pruned before a new one is added, and if none has expired the report is
// simply written (never silently dropped for want of room).
const maxIncapableReports = 4096

// reportIncapable writes ONE WARN per placement pass listing every candidate
// the filter skipped as incapable, and at most one per session per
// incapableReportInterval.
func (r *Reconciler) reportIncapable(ctx context.Context, req Request, result *Result, reasons map[sessionwire.HostID]string) {
	if len(result.Incapable) == 0 {
		return
	}
	key := sessionKey{tenant: req.TenantID, session: req.SessionID}
	now := r.cfg.Clock.Now()
	r.reportsMu.Lock()
	if last, ok := r.reports[key]; ok && now.Sub(last) < incapableReportInterval {
		r.reportsMu.Unlock()
		return
	}
	if r.reports == nil {
		r.reports = map[sessionKey]time.Time{}
	}
	if len(r.reports) >= maxIncapableReports {
		for k, at := range r.reports {
			if now.Sub(at) >= incapableReportInterval {
				delete(r.reports, k)
			}
		}
	}
	if len(r.reports) < maxIncapableReports {
		r.reports[key] = now
	}
	r.reportsMu.Unlock()
	hosts := make([]string, 0, len(result.Incapable))
	for _, host := range result.Incapable {
		entry := string(host)
		if reason := reasons[host]; reason != "" {
			entry += " (" + reason + ")"
		}
		hosts = append(hosts, entry)
	}
	r.logger().WarnContext(ctx, "placement: skipped pooled candidates that cannot apply this session's pending gate response",
		slog.String("tenant_id", string(req.TenantID)),
		slog.String("session_id", string(req.SessionID)),
		slog.Int("pending_gate_responses", len(req.GateResponses)),
		slog.Any("skipped_hosts", hosts),
		// waiting: no capable candidate was found this pass, so the session
		// waits -- for at most its pending answer's apply deadline.
		slog.Bool("waiting", result.Decision.Outcome != OutcomeAttachPooled))
}

// answer is attachAnswer's closed classification.
type answer uint8

const (
	answerAbort answer = iota
	answerAccepted
	answerStale
	answerRefused
	answerUnsupported
	answerUnreachable
	answerFailed
	answerUnaddressable
)

// attachAnswer classifies one attach outcome. It is total over the error, and
// the default is ABORT: an error this function does not recognise is one the
// Host may have acted on.
//
// runtime_unavailable is a REFUSAL here and not a capability fact. By the time
// an attach is sent the candidate advertised the method, so the answer is the
// Host's own -- an earlier incarnation named by a stale capacity report, a
// restore with no durable state, a runtime that had stopped -- and the gate is
// what made that distinguishable from a Host that could not attach at all.
func attachAnswer(err error) (answer, sessionwire.HostLinkErrorCode) {
	if err == nil {
		return answerAccepted, ""
	}
	var refusal *AttachRefusal
	if errors.As(err, &refusal) {
		if refusal.Code == sessionwire.HostLinkErrorEpochMismatch {
			return answerStale, refusal.Code
		}
		return answerRefused, refusal.Code
	}
	if errors.Is(err, ErrAttachUnsupported) {
		return answerUnsupported, ""
	}
	if errors.Is(err, ErrTenantUnaddressable) {
		return answerUnaddressable, ""
	}
	if errors.Is(err, ErrHostUnreachable) {
		return answerUnreachable, ""
	}
	if errors.Is(err, ErrAttachFailed) {
		return answerFailed, ""
	}
	return answerAbort, ""
}

// endpointCode is Core's derivation refusal code carried by err, or empty.
func endpointCode(err error) sessionwire.HostLinkEndpointCode {
	var refused *sessionwire.HostLinkEndpointError
	if errors.As(err, &refused) {
		return refused.Code
	}
	return ""
}

// bindAttached binds this replica's route with the epoch the ATTACH returned.
//
// That epoch is the only one this replica may bind with. The registry may not
// have caught up with the lease the attach just acquired, and an
// epoch_mismatch's current_lease_epoch belongs to somebody else; the
// observation is the Host's own statement of the residency it holds, checked
// by the transport against the request's session and host fence before it
// reached here.
func (r *Reconciler) bindAttached(ctx context.Context, req Request, observation sessionwire.HostLinkRegistryObservation, result Result) (Result, error) {
	return r.bindAndDeliver(ctx, req, observation, result, true)
}

// wake is the owned-session path: when the caller has commands to wake and the
// session already has a live owner, bind to that owner with the REGISTRY's
// epoch (section 15 step 1) and deliver. A caller with nothing to wake, or a
// reconciler composed without links, does nothing here.
func (r *Reconciler) wake(ctx context.Context, req Request, owner sessionwire.HostLinkRegistryObservation, result Result) (Result, error) {
	if r.cfg.Links == nil || len(req.Wake) == 0 {
		return result, nil
	}
	return r.bindAndDeliver(ctx, req, owner, result, false)
}

// bindAndDeliver binds, delivers the wake, and gives back a route it created.
//
// THE ROUTE IS TRANSIENT, and that is a decision about who owns a route's
// lifetime. internal/routing's demand plane is the long-lived route holder: it
// binds for a subscriber and unbinds on the last release. A route placement
// left behind would pin the Host's link against the idle reaper for a session
// nobody is watching, and would make the NEXT placement of the same session to
// a different Host fail as a binding conflict against a route nobody wants. So
// a route this call created is released once the wake has been delivered, and
// a route that already existed -- a viewer's -- is left exactly as it was.
//
// A delivery failure is counted and not returned: delivery is a hint, the
// command is durable in the inbox, and a Host consumes it from there.
func (r *Reconciler) bindAndDeliver(ctx context.Context, req Request, observation sessionwire.HostLinkRegistryObservation, result Result, attached bool) (Result, error) {
	bind := bindRequestFor(observation)
	_, routed := r.cfg.Links.RouteFor(req.TenantID, req.SessionID)
	if err := r.cfg.Links.Bind(ctx, observation.InternalEndpoint, bind); err != nil {
		if attached {
			return result, fmt.Errorf("%w: %w", ErrBindAfterAttach, err)
		}
		return result, err
	}
	result.Bound = true
	gate := gateResponseWake(req)
	var (
		asked    bool
		capable  bool
		askedErr error
	)
	for _, command := range req.Wake {
		if _, isGate := gate[command]; isGate {
			// Asked once, lazily, of the Host this route was just bound to:
			// a wake with no gate response costs no capability read.
			if !asked {
				capable, askedErr = r.cfg.Links.AcceptsGateResponses(ctx, observation)
				asked = true
			}
			if askedErr != nil || !capable {
				result.WithheldGateResponses++
				continue
			}
		}
		if err := r.cfg.Links.DeliverCommand(ctx, req.TenantID, req.SessionID, sessionwire.HostLinkCommandDelivery{CommandID: command}); err != nil {
			result.DeliveryFailures++
			continue
		}
		result.Delivered++
	}
	if !routed {
		// Best effort, for the reason the pool's own Unbind gives: the local
		// route is released whether or not the Host could be told.
		_ = r.cfg.Links.Unbind(ctx, unbindRequestFor(bind))
	}
	return result, nil
}

// gateResponseWake is the set of wake commands that are gate responses.
func gateResponseWake(req Request) map[sessionwire.CommandID]struct{} {
	if len(req.GateResponses) == 0 {
		return nil
	}
	set := make(map[sessionwire.CommandID]struct{}, len(req.GateResponses))
	for _, command := range req.GateResponses {
		set[command] = struct{}{}
	}
	return set
}

// attachMode is the mode an attach states, read from the catalog record.
//
// A session whose record shows no committed journal progress and no checkpoint
// has nothing to restore from, so it is created; any durable progress makes it
// a restore. The Host is the authority on what durable state exists and
// refuses a restore of a session with none, so this is the caller's statement
// of intent that Core asks for, not a guard.
//
// ONE LIMIT IS STATED RATHER THAN HIDDEN: no released Host writes the catalog's
// host-owned members (UpdateCatalogHostState has no production caller), so in
// today's fleet LastJournalSeq and the checkpoint stay zero and every attach
// says create. Core's own contract is that a create meeting existing durable
// state proceeds when the runtime build agrees; whether it should is the
// create identity's question, not this record's.
func attachMode(record sessionstore.CatalogRecord) sessionwire.HostLinkAttachMode {
	if record.LastJournalSeq == 0 && record.Checkpoint.JournalSeq == 0 {
		return sessionwire.HostLinkAttachModeCreate
	}
	return sessionwire.HostLinkAttachModeRestore
}

// attachRequest builds the attach for one candidate.
//
// The HOST FENCE COMES FROM THE CANDIDATE'S CAPACITY REPORT and from nowhere
// else. host_id and host_generation name the incarnation placement chose; a
// Host that is not that incarnation -- a replacement serving the same
// endpoint, or the same Host restarted -- refuses before it takes any lease,
// which is the whole point of carrying them.
func attachRequest(record sessionstore.CatalogRecord, candidate sessionwire.HostLinkCapacityReport, mode sessionwire.HostLinkAttachMode, actor string) sessionwire.HostLinkAttachRequest {
	return sessionwire.HostLinkAttachRequest{
		Version:                sessionwire.CurrentWireVersion,
		TenantID:               record.TenantID,
		SessionID:              record.SessionID,
		HostID:                 candidate.HostID,
		HostGeneration:         candidate.HostGeneration,
		AgentID:                record.AgentID,
		RuntimeCompatibilityID: record.RuntimeCompatibilityID,
		Mode:                   mode,
		ActorID:                actor,
		IdempotencyKey:         attachKey(record, mode),
	}
}

// attachKey is the attach's retry-stable key.
//
// It deliberately does NOT name the Host. Section 15 step 5 retries "through
// the same idempotency key" after a stale registry, and the retry may well
// reach a different Host; a key naming the Host would make every candidate a
// different request. It names the session, what is being launched, the mode
// and the desired generation, so two replicas placing the same intent send the
// same key and a changed intent sends a new one. Fields are framed as
// desiredKey frames them, for desiredKey's reason.
func attachKey(record sessionstore.CatalogRecord, mode sessionwire.HostLinkAttachMode) string {
	return framedKey("placement-attach/v1",
		string(record.TenantID),
		string(record.SessionID),
		string(record.AgentID),
		record.RuntimeCompatibilityID,
		string(mode),
		strconv.FormatUint(record.DesiredGeneration, 10),
	)
}

// bindRequestFor builds the bind that names exactly the residency an
// observation describes: its Host, its incarnation and its lease epoch.
func bindRequestFor(observation sessionwire.HostLinkRegistryObservation) sessionwire.HostLinkBindRequest {
	return sessionwire.HostLinkBindRequest{
		Version:                sessionwire.CurrentWireVersion,
		TenantID:               observation.TenantID,
		SessionID:              observation.SessionID,
		HostID:                 observation.HostID,
		HostGeneration:         observation.HostGeneration,
		LeaseEpoch:             observation.LeaseEpoch,
		RuntimeCompatibilityID: observation.RuntimeCompatibilityID,
		IdempotencyKey: framedKey("placement-bind/v1",
			string(observation.TenantID),
			string(observation.SessionID),
			string(observation.HostID),
			strconv.FormatUint(observation.HostGeneration, 10),
			strconv.FormatUint(observation.LeaseEpoch, 10),
		),
	}
}

// unbindRequestFor removes exactly the route bind created, under its key.
func unbindRequestFor(bind sessionwire.HostLinkBindRequest) sessionwire.HostLinkUnbindRequest {
	return sessionwire.HostLinkUnbindRequest{
		Version:        bind.Version,
		TenantID:       bind.TenantID,
		SessionID:      bind.SessionID,
		HostID:         bind.HostID,
		HostGeneration: bind.HostGeneration,
		LeaseEpoch:     bind.LeaseEpoch,
		IdempotencyKey: bind.IdempotencyKey,
	}
}

// framedKey hashes a domain and fields, each written as its decimal byte
// length, a NUL, then its bytes -- desiredKey's framing, so no two field lists
// share a key by concatenation.
func framedKey(domain string, fields ...string) string {
	digest := sha256.New()
	for _, field := range append([]string{domain}, fields...) {
		digest.Write([]byte(strconv.Itoa(len(field))))
		digest.Write([]byte{0})
		digest.Write([]byte(field))
	}
	return hex.EncodeToString(digest.Sum(nil))
}

// ActorID and Logger report what this reconciler was composed with. They exist
// for the composition's own tests: the actor an attach names and the logger an
// exclusion is written to are both decided by the root package, and neither is
// observable from outside a real Host exchange otherwise (B5 spec gate X4, Q2).
func (r *Reconciler) ActorID() string { return r.cfg.ActorID }

// Logger reports the configured logger, nil when none was composed.
func (r *Reconciler) Logger() *slog.Logger { return r.cfg.Logger }
