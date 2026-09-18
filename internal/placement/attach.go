package placement

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
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

// ErrRegistryStale reports that every re-placement this call was allowed ended
// in epoch_mismatch: the session's lease is held elsewhere and the registry has
// not caught up. The work is left for the next sweep, which re-reads the
// registry from the start.
var ErrRegistryStale = errors.New("placement: the session lease is held elsewhere and the registry did not show the holder")

// ErrBindAfterAttach reports an attach the Host accepted followed by a bind it
// did not. The session IS resident; what failed is this replica's route to it.
// It is reported rather than retried here because the attach is done and the
// next thing that needs a route -- a viewer, a delivery, the next sweep -- asks
// the registry, which now names the owner.
var ErrBindAfterAttach = errors.New("placement: the host attached the session but refused the bind")

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
	case cfg.ReplaceAttempts < 0:
		return fmt.Errorf("%w: ReplaceAttempts must not be negative", ErrInvalidConfig)
	case cfg.ReplaceBackoff < 0:
		return fmt.Errorf("%w: ReplaceBackoff must not be negative", ErrInvalidConfig)
	}
	return nil
}

func (r *Reconciler) replaceAttempts() int {
	if r.cfg.ReplaceAttempts == 0 {
		return defaultReplaceAttempts
	}
	return r.cfg.ReplaceAttempts
}

// backoff is the wait before re-placement number n (n >= 1): the base,
// doubled per further attempt.
func (r *Reconciler) backoff(n int) time.Duration {
	base := r.cfg.ReplaceBackoff
	if base == 0 {
		base = defaultReplaceBackoff
	}
	return base << (n - 1)
}

func (r *Reconciler) wait(ctx context.Context, d time.Duration) error {
	if r.cfg.Wait != nil {
		return r.cfg.Wait(ctx, d)
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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
//   - anything else: the Host may have acted, so stop and let the next sweep
//     retry under the same key rather than put a second attach in flight.
func (r *Reconciler) placePooled(ctx context.Context, req Request, writes int) (Result, error) {
	result := Result{DesiredWrites: writes}
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

// answer is attachAnswer's closed classification.
type answer uint8

const (
	answerAbort answer = iota
	answerAccepted
	answerStale
	answerRefused
	answerUnsupported
	answerUnreachable
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
	if errors.Is(err, ErrHostUnreachable) {
		return answerUnreachable, ""
	}
	return answerAbort, ""
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
	for _, command := range req.Wake {
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
