package admission

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	"github.com/looprig/factory/internal/command"
	"github.com/looprig/factory/internal/placement"
	"github.com/looprig/sessionstore"
)

// The kinds this service admits.
//
// They are ALIASES of internal/command's, not a private copy, and this package
// is the one where that matters most: the REST control routes and the ClientLink
// RPCs are two spellings of one admission contract (specification section 8.1),
// and both are routed here. Two private copies would satisfy every test either
// package could write while letting an RPC be authorized under one spelling and
// admitted under another. They were a private copy until A6.1's gate found it.
const (
	CommandCreate       = command.KindCreateSession
	CommandInput        = command.KindInput
	CommandInterrupt    = command.KindInterrupt
	CommandRestore      = command.KindRestore
	CommandGateResponse = command.KindGateResponse
)

// ErrLegacySessionUnsupported reports a command for a session whose immutable
// binding names the LEGACY protocol.
//
// Every command this service admits is admitted into the DISPOSITION family,
// because that is the only family a Host can reach: a Host takes residency
// through AcquireResidency, which pins ProtocolModeDisposition, so a command
// admitted into the legacy inbox is one no Host will ever apply. A
// legacy-bound session cannot be created by this module at all
// (ErrLegacyCreateUnsupported), so the refusal is reached only for a session
// some other writer created. It is runtime_unavailable for the reason every
// other "no runtime this deployment runs can serve this" is: nothing about the
// request can change the answer.
var ErrLegacySessionUnsupported = errors.New("admission: the session is bound to the legacy protocol, which no Host can take residency on")

// ErrGateResponseUnsupported is the cause of the gate_not_resumable refusal a
// gate response gets when the session's live owner cannot APPLY one.
//
// A gate response is a durable command the owning Host reads from the
// session's disposition stream, so admitting it IS delivering it: once it is
// in the inbox, the Host consumes it. A Host that cannot apply the kind would
// refuse it (host v0.3.0) or, worse, one that predates the refusal would hold
// it -- so the question is asked BEFORE anything is written, of the owner the
// fresh-owner check just accepted, and answered by the one capability
// predicate (hostlink.GateResponseCapable, through GateResponders).
//
// The predicate admits exactly a Host whose connect reply carries Core's token
// sessionwire.HostLinkCapabilityGateResponse (host >= v0.4.0). A v0.3.0 Host
// does not, so a gate response to a session it owns is refused here -- its
// behaviour unchanged. The catalog's LeaseEpoch is NOT consulted: it is never
// a capability signal (owner ruling, 2026-09-19).
var ErrGateResponseUnsupported = errors.New("admission: the session's owner cannot apply a gate response")

// ErrGateResponderUnavailable classifies a failure to ASK the owner whether it
// applies gate responses that is a transient condition of the path to it --
// its link reconnecting, this replica's link ceiling full, a dial that failed,
// a pool shutting down -- rather than an answer about the Host. The
// composition's GateResponders wraps it; admission returns it as a fault (no
// public code), and the HTTP edge answers it 503 unavailable, which is
// RETRYABLE, for storeUnavailable's reason: a transient deployment condition
// answered as a non-retryable 500 tells a client not to retry at exactly the
// moment retrying works (quality gate F1). Nothing is written before it, and a
// retry under the same CommandID is idempotent.
var ErrGateResponderUnavailable = errors.New("admission: the session's owner could not be reached to ask whether it applies gate responses")

// ErrPrincipalResponderUnavailable is a transient failure to ask a Host about
// its principal and metadata capability. It is not a command refusal.
var ErrPrincipalResponderUnavailable = errors.New("admission: the session's owner could not be reached to ask whether it applies command principal and metadata")

// ErrGateResponseTooLarge is the cause of the invalid_request refusal a NEW
// gate response gets when its canonical command payload is larger than
// sessionstore.MaxInboxPayloadBytes (64 KiB), the most the disposition inbox
// keeps inline.
//
// Every other kind above that size is stored by reference (runbook A3.1 step
// 5). A gate response is not, because host v0.4.0 treats a gate response whose
// body is held by reference as BLOCKING: the session's whole command stream
// waits behind it until its apply deadline, and every command admitted after
// it expires with it (host v0.4.0 spec gate C1). So it is refused before
// anything is written.
//
// The size measured is the CANONICAL ENCODED size -- len(canonicalCommand),
// the very bytes admit stores and the store checks against the bound -- not
// the size of the body the caller sent (quality gate F5). encoding/json
// escapes '<', '>' and '&' to six bytes each and compacts raw values, so a body
// of about 11 KiB of '<' canonicalises past 64 KiB and is refused; the refusal
// message carries the canonical size. It is still a function of the caller's
// own request alone, which is why the code is invalid_request (400) and not a
// state conflict. A retry of a gate
// response already stored -- which only an earlier Factory could have
// admitted by reference -- still answers from its record.
var ErrGateResponseTooLarge = errors.New("admission: a gate response larger than the inline inbox payload bound is refused")

// ErrLegacyCreateUnsupported reports that Factory cannot create a session on
// the legacy protocol. No runtime this program ships can host such a session.
var ErrLegacyCreateUnsupported = errors.New("admission: legacy create unsupported")

// ErrAdmissionQuiesced is a transient local shutdown condition. It is a fault,
// not a durable command refusal: the caller may retry on another replica.
var ErrAdmissionQuiesced = errors.New("admission: this replica is quiescing")

type Error struct {
	Code  sessionwire.ErrorCode
	Cause error
}

func (e *Error) Error() string {
	if e.Code == "" {
		return "admission: command refused"
	}
	return "admission: " + string(e.Code)
}
func (e *Error) Unwrap() error { return e.Cause }

func IsCode(err error, code sessionwire.ErrorCode) bool {
	var target *Error
	return errors.As(err, &target) && target.Code == code
}

func refusal(code sessionwire.ErrorCode, cause error) error { return &Error{Code: code, Cause: cause} }

// A dependency FAULT is not a refusal, and the two are returned differently.
//
// Every target and directory read this service makes has three outcomes -- yes,
// no, and "could not ask" -- and the third used to be folded into the second.
// An *Error is a CLASSIFIED PUBLIC refusal: an edge renders it as a decision
// about the caller's command, and a decision is what a caller stops retrying.
// So a transient directory outage arrived at a browser as runtime_unavailable
// or gate_not_resumable, with A6.2's ClientLink correctly reporting it
// non-retryable -- correctly, because those codes cannot distinguish their one
// transient cause from their permanent ones, which is exactly why the transient
// one must not be spelled with them.
//
// A fault is therefore returned as ITSELF, wrapped for an operator's log and
// carrying no public code, so errors.As finds no *Error and each edge answers
// from its own fault channel. The ClientLink answers centrifuge's temporary
// internal error; the REST controls (A3.3) will answer through httpapi's
// existing storeUnavailable and internalFailure mappings, which already
// separate a draining store from a fault. No new shared vocabulary is required,
// which is what makes this correctable here rather than behind the
// classification authority A9.1 owes.
//
// What did NOT change is the negative ANSWER: an unresolvable target is still
// runtime_unavailable and an absent or stale owner is still gate_not_resumable.
// The cause is now nil on those paths, because there was no failure to report.
func resolveTargetFault(err error) error {
	return fmt.Errorf("admission: resolve the launch target: %w", err)
}

type Catalog interface {
	GetCatalogEntry(context.Context, sessionstore.GetCatalogEntryRequest) (sessionstore.CatalogEntry, error)
	CreateCatalogEntry(context.Context, sessionstore.CreateCatalogEntryRequest) (sessionstore.CatalogEntry, bool, error)
}

// CommandStore is the durable command plane every command but a create is
// admitted into. It is the DISPOSITION family and only that family: see
// ErrLegacySessionUnsupported for why a legacy inbox write would be a command
// no Host could ever apply.
//
// PutCommandPayload is here, and not borrowed from PublicCreateStore, because
// an oversized input is stored by reference exactly as an oversized create is
// (runbook A3.1 step 5), and PublicCreates is an OPTIONAL seam: a composition
// serving no creates must still be able to admit a large input.
type CommandStore interface {
	AdmitDispositionCommand(context.Context, sessionstore.AdmitDispositionCommandRequest) (sessionstore.DispositionInboxEntry, bool, error)
	GetDispositionCommand(context.Context, sessionstore.GetDispositionCommandRequest) (sessionstore.DispositionInboxEntry, error)
	PutCommandPayload(context.Context, sessionstore.PutCommandPayloadRequest) (sessionwire.ObjectMetadata, error)
}

type OwnerDirectory interface {
	Owner(context.Context, sessionwire.TenantID, sessionwire.SessionID) (sessionwire.HostLinkRegistryObservation, bool, error)
}

// GateResponders answers whether a session's live owner can apply a
// gate_response command. The composition implements it over the HostLink
// pool, asking the owner's link for the session's tenant and deciding with
// hostlink.GateResponseCapable -- the one predicate. See
// ErrGateResponseUnsupported.
type GateResponders interface {
	// AcceptsGateResponses reports whether owner can apply a gate response.
	// (false, nil) is a decision about the Host; an error is a fault -- the
	// Host could not be asked -- and is never read as "cannot".
	AcceptsGateResponses(ctx context.Context, owner sessionwire.HostLinkRegistryObservation) (bool, error)
}

// PrincipalResponders asks whether a resident Host reads attribution members.
type PrincipalResponders interface {
	AcceptsCommandPrincipal(context.Context, sessionwire.HostLinkRegistryObservation) (bool, error)
}

// Target is the configured immutable launch identity selected for a create.
// It is configuration, not current Host capacity and not an ownership claim.
type Target struct {
	Key      sessionstore.HostTargetKey
	Workload sessionstore.DesiredWorkload
}

type TargetResolver interface {
	ResolveAgent(context.Context, sessionwire.AgentID) (Target, bool, error)
	IsKnown(context.Context, sessionstore.HostTargetKey) (bool, error)
}

type Config struct {
	Authorizer Authorizer
	Targets    TargetResolver
	Catalog    Catalog
	Commands   CommandStore
	Directory  OwnerDirectory
	Clock      Clock
	IDs        UUIDSource

	// GateResponders decides whether a gate response may be admitted for the
	// owner that would apply it. OPTIONAL, and nil REFUSES every gate response
	// with gate_not_resumable (ErrGateResponseUnsupported): the safe default is
	// the one that never hands a Host a command it cannot apply.
	GateResponders      GateResponders
	StampPrincipal      bool
	PrincipalResponders PrincipalResponders

	// PublicCreates is the durable public-create plane a V1 create admits
	// into. It is OPTIONAL, and its absence is a supported composition rather
	// than a broken one: without it, and without Binding, AdmitCreate refuses
	// and every other entry point is unaffected.
	PublicCreates PublicCreateStore

	// Binding is the deployment-configuration half of the immutable binding a
	// V1 create pins. Optional for the same reason.
	Binding SessionBindingTemplate

	ApplyDeadline time.Duration
}

// createsServed reports whether this composition can serve a V1 create. BOTH
// halves are required and neither implies the other: a store with no
// configured binding has nothing to pin, and a configured binding with no
// store has nowhere to put it.
func (c Config) createsServed() bool { return c.PublicCreates != nil && c.Binding.configured() }

type Service struct {
	cfg        Config
	gateMu     sync.Mutex
	gateClosed bool
	active     int
	drained    chan struct{}
}

// FenceAdmissions permanently refuses new commands and returns a signal that
// closes after every command already inside the service has returned. It does
// not wait, so the Server can serialize the fence with Start and then release
// its lifecycle lock before joining active calls.
func (s *Service) FenceAdmissions() <-chan struct{} {
	s.gateMu.Lock()
	if !s.gateClosed {
		s.gateClosed = true
		if s.drained == nil {
			s.drained = make(chan struct{})
		}
		if s.active == 0 {
			close(s.drained)
		}
	}
	done := s.drained
	s.gateMu.Unlock()
	return done
}

// Quiesce fences admission and waits for the preboundary calls to return.
// A canceled waiter may call again; the fence and active calls keep running.
func (s *Service) Quiesce(ctx context.Context) error {
	done := s.FenceAdmissions()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Service) beginAdmission() (func(), error) {
	s.gateMu.Lock()
	defer s.gateMu.Unlock()
	if s.gateClosed {
		return nil, ErrAdmissionQuiesced
	}
	s.active++
	return func() {
		s.gateMu.Lock()
		defer s.gateMu.Unlock()
		s.active--
		if s.gateClosed && s.active == 0 {
			close(s.drained)
		}
	}, nil
}

func NewService(cfg Config) (*Service, error) {
	if cfg.Authorizer == nil || cfg.Targets == nil || cfg.Catalog == nil || cfg.Commands == nil || cfg.Directory == nil || cfg.Clock == nil || cfg.IDs == nil {
		return nil, errors.New("admission: incomplete service configuration")
	}
	if cfg.ApplyDeadline <= 0 {
		return nil, errors.New("admission: apply deadline must be positive")
	}
	return &Service{cfg: cfg}, nil
}

// AdmitCreate admits a V1 create: runbook A3.1 steps 2 and 5.
//
// It returns a DISPOSITION entry, and the difference from the legacy inbox is
// the whole point rather than a type detail. A create is the one command that
// CHOOSES a session's protocol, and the only choice that produces a session a
// Host can ever take residency on is disposition. Every other command is then
// admitted into the same family (see admit), so there is no mixed-family
// session.
//
// An oversized create is stored by reference instead of refused. That is step
// 5, and admit applies the same rule to the other four kinds.
func (s *Service) AdmitCreate(ctx context.Context, principal identity.Principal, req sessionwire.CreateRequest) (sessionstore.DispositionInboxEntry, bool, error) {
	end, gateErr := s.beginAdmission()
	if gateErr != nil {
		return sessionstore.DispositionInboxEntry{}, false, gateErr
	}
	defer end()
	if err := s.attribute(principal, &req.Principal); err != nil {
		return sessionstore.DispositionInboxEntry{}, false, err
	}
	if err := req.Validate(); err != nil {
		return sessionstore.DispositionInboxEntry{}, false, refusal(sessionwire.ErrorCodeInvalidRequest, err)
	}
	payload, err := canonicalCommand(req)
	if err != nil {
		return sessionstore.DispositionInboxEntry{}, false, refusal(sessionwire.ErrorCodeInvalidRequest, err)
	}
	if err := s.cfg.Authorizer.AuthorizeControl(ctx, principal, req.SessionID, CommandCreate); err != nil {
		return sessionstore.DispositionInboxEntry{}, false, err
	}
	target, known, targetErr := s.cfg.Targets.ResolveAgent(ctx, req.AgentID)
	if targetErr != nil {
		return sessionstore.DispositionInboxEntry{}, false, resolveTargetFault(targetErr)
	}
	if !known || target.Key.AgentID != req.AgentID {
		return sessionstore.DispositionInboxEntry{}, false, refusal(sessionwire.ErrorCodeRuntimeUnavailable, nil)
	}
	if !s.cfg.createsServed() {
		return sessionstore.DispositionInboxEntry{}, false, refusal(sessionwire.ErrorCodeRuntimeUnavailable, ErrCreateBindingUnconfigured)
	}
	return s.admitPublicCreate(ctx, principal.Tenant(), req, target, payload)
}

func (s *Service) AdmitInput(ctx context.Context, principal identity.Principal, req sessionwire.InputRequest) (sessionstore.DispositionInboxEntry, bool, error) {
	end, gateErr := s.beginAdmission()
	if gateErr != nil {
		return sessionstore.DispositionInboxEntry{}, false, gateErr
	}
	defer end()
	if err := s.attribute(principal, &req.Principal); err != nil {
		return sessionstore.DispositionInboxEntry{}, false, err
	}
	if err := req.Validate(); err != nil {
		return sessionstore.DispositionInboxEntry{}, false, refusal(sessionwire.ErrorCodeInvalidRequest, err)
	}
	return s.admitExisting(ctx, principal, req.SessionID, req.CommandID, CommandInput, req, members{req.Principal, req.Metadata})
}

func (s *Service) AdmitInterrupt(ctx context.Context, principal identity.Principal, req sessionwire.InterruptRequest) (sessionstore.DispositionInboxEntry, bool, error) {
	end, gateErr := s.beginAdmission()
	if gateErr != nil {
		return sessionstore.DispositionInboxEntry{}, false, gateErr
	}
	defer end()
	if err := s.attribute(principal, &req.Principal); err != nil {
		return sessionstore.DispositionInboxEntry{}, false, err
	}
	if err := req.Validate(); err != nil {
		return sessionstore.DispositionInboxEntry{}, false, refusal(sessionwire.ErrorCodeInvalidRequest, err)
	}
	return s.admitExisting(ctx, principal, req.SessionID, req.CommandID, CommandInterrupt, req, members{principal: req.Principal})
}

func (s *Service) AdmitRestore(ctx context.Context, principal identity.Principal, req sessionwire.RestoreRequest) (sessionstore.DispositionInboxEntry, bool, error) {
	end, gateErr := s.beginAdmission()
	if gateErr != nil {
		return sessionstore.DispositionInboxEntry{}, false, gateErr
	}
	defer end()
	if err := s.attribute(principal, &req.Principal); err != nil {
		return sessionstore.DispositionInboxEntry{}, false, err
	}
	if err := req.Validate(); err != nil {
		return sessionstore.DispositionInboxEntry{}, false, refusal(sessionwire.ErrorCodeInvalidRequest, err)
	}
	return s.admitExisting(ctx, principal, req.SessionID, req.CommandID, CommandRestore, req, members{principal: req.Principal})
}

// AdmitGateResponse is runbook A3.1 step 4: a gate response needs the matching
// durable gate projection AND a fresh resident, accepting owner, or it is
// gate_not_resumable before anything is written. Moving the command into the
// disposition family changed where it is written and nothing about that rule.
func (s *Service) AdmitGateResponse(ctx context.Context, principal identity.Principal, req sessionwire.GateResponseRequest) (sessionstore.DispositionInboxEntry, bool, error) {
	end, gateErr := s.beginAdmission()
	if gateErr != nil {
		return sessionstore.DispositionInboxEntry{}, false, gateErr
	}
	defer end()
	if err := s.attribute(principal, &req.Principal); err != nil {
		return sessionstore.DispositionInboxEntry{}, false, err
	}
	if err := req.Validate(); err != nil {
		return sessionstore.DispositionInboxEntry{}, false, refusal(sessionwire.ErrorCodeInvalidRequest, err)
	}
	payload, err := canonicalCommand(req)
	if err != nil {
		return sessionstore.DispositionInboxEntry{}, false, refusal(sessionwire.ErrorCodeInvalidRequest, err)
	}
	if err := s.cfg.Authorizer.AuthorizeControl(ctx, principal, req.SessionID, CommandGateResponse); err != nil {
		return sessionstore.DispositionInboxEntry{}, false, err
	}
	m := members{principal: req.Principal}
	if retry, handled, err := s.retry(ctx, principal.Tenant(), req.SessionID, req.CommandID, CommandGateResponse, payload, m); handled || err != nil {
		return retry, false, err
	}
	if len(payload) > sessionstore.MaxInboxPayloadBytes {
		return sessionstore.DispositionInboxEntry{}, false, refusal(sessionwire.ErrorCodeInvalidRequest,
			fmt.Errorf("%w: %d bytes canonically encoded, the bound is %d", ErrGateResponseTooLarge, len(payload), sessionstore.MaxInboxPayloadBytes))
	}
	entry, err := s.existingCompatible(ctx, principal.Tenant(), req.SessionID)
	if err != nil {
		return sessionstore.DispositionInboxEntry{}, false, err
	}
	now := s.cfg.Clock.Now()
	if err := gateAdmission(entry.Record.OpenGates, req, now); err != nil {
		return sessionstore.DispositionInboxEntry{}, false, err
	}
	if err := s.gateOwnerAnswers(ctx, principal.Tenant(), req.SessionID, entry.Record, now); err != nil {
		return sessionstore.DispositionInboxEntry{}, false, err
	}
	if err := s.ownerAppliesPrincipal(ctx, principal.Tenant(), req.SessionID, entry.Record, m); err != nil {
		return sessionstore.DispositionInboxEntry{}, false, err
	}
	return s.admit(ctx, principal.Tenant(), req.SessionID, req.CommandID, CommandGateResponse, entry.Record.Binding, payload, m)
}

func (s *Service) attribute(principal identity.Principal, carried **sessionwire.Principal) error {
	if *carried != nil {
		return refusal(sessionwire.ErrorCodeInvalidRequest, fmt.Errorf("%w (field principal)", identity.ErrClientPrincipal))
	}
	if s.cfg.StampPrincipal {
		wire := principal.Wire()
		*carried = &wire
	}
	return nil
}

type members struct {
	principal *sessionwire.Principal
	metadata  sessionwire.MessageMetadata
}

// gateOwnerAnswers is the ONE owner check a gate response must pass: a fresh
// owner matching the session's record, and one that can apply a gate
// response. nil admits; a gate_not_resumable refusal is a decision; any other
// error is a fault. AdmitGateResponse and GateResponsesAnswerable both call it,
// so the gates read and the write cannot disagree about a gate's answerability.
func (s *Service) gateOwnerAnswers(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, record sessionstore.CatalogRecord, now time.Time) error {
	owner, ok, err := s.cfg.Directory.Owner(ctx, tenant, session)
	if err != nil {
		return fmt.Errorf("admission: observe the session's owner: %w", err)
	}
	if !ok || !freshMatchingOwner(owner, record, now) {
		return refusal(sessionwire.ErrorCodeGateNotResumable, nil)
	}
	return s.ownerAppliesGateResponses(ctx, owner)
}

// GateResponsesAnswerable reports, for the gates READ, whether a gate response
// for this session would pass AdmitGateResponse's owner check now: (true, nil)
// when it would, (false, nil) when the write would refuse it
// gate_not_resumable, and (false, err) when the owner could not be asked --
// where the write would answer a fault. It writes nothing and takes no
// admission slot: it is a read.
func (s *Service) GateResponsesAnswerable(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, record sessionstore.CatalogRecord) (bool, error) {
	err := s.gateOwnerAnswers(ctx, tenant, session, record, s.cfg.Clock.Now())
	if err == nil {
		return true, nil
	}
	var refused *Error
	if errors.As(err, &refused) {
		return false, nil
	}
	return false, err
}

// ownerAppliesGateResponses is the gate_response capability gate: nil when the
// owner can apply one, a gate_not_resumable refusal when it cannot or when no
// GateResponders is composed, and a FAULT -- no public code -- when the owner
// could not be asked, for resolveTargetFault's reason.
func (s *Service) ownerAppliesGateResponses(ctx context.Context, owner sessionwire.HostLinkRegistryObservation) error {
	if s.cfg.GateResponders == nil {
		return refusal(sessionwire.ErrorCodeGateNotResumable, ErrGateResponseUnsupported)
	}
	accepts, err := s.cfg.GateResponders.AcceptsGateResponses(ctx, owner)
	if err != nil {
		return fmt.Errorf("admission: ask the owner whether it applies gate responses: %w", err)
	}
	if !accepts {
		return refusal(sessionwire.ErrorCodeGateNotResumable, ErrGateResponseUnsupported)
	}
	return nil
}

func (s *Service) admitExisting(ctx context.Context, principal identity.Principal, session sessionwire.SessionID, command sessionwire.CommandID, kind sessionstore.CommandKind, request any, m members) (sessionstore.DispositionInboxEntry, bool, error) {
	payload, err := canonicalCommand(request)
	if err != nil {
		return sessionstore.DispositionInboxEntry{}, false, refusal(sessionwire.ErrorCodeInvalidRequest, err)
	}
	if err := s.cfg.Authorizer.AuthorizeControl(ctx, principal, session, kind); err != nil {
		return sessionstore.DispositionInboxEntry{}, false, err
	}
	if retry, handled, err := s.retry(ctx, principal.Tenant(), session, command, kind, payload, m); handled || err != nil {
		return retry, false, err
	}
	entry, err := s.existingCompatible(ctx, principal.Tenant(), session)
	if err != nil {
		return sessionstore.DispositionInboxEntry{}, false, err
	}
	if err := s.ownerAppliesPrincipal(ctx, principal.Tenant(), session, entry.Record, m); err != nil {
		return sessionstore.DispositionInboxEntry{}, false, err
	}
	return s.admit(ctx, principal.Tenant(), session, command, kind, entry.Record.Binding, payload, m)
}

func (s *Service) ownerAppliesPrincipal(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, record sessionstore.CatalogRecord, m members) error {
	if m.principal == nil && len(m.metadata) == 0 {
		return nil
	}
	owner, ok, err := s.cfg.Directory.Owner(ctx, tenant, session)
	if err != nil {
		return fmt.Errorf("admission: observe the session's owner: %w", err)
	}
	if !ok || !freshMatchingOwner(owner, record, s.cfg.Clock.Now()) {
		return nil
	}
	if s.cfg.PrincipalResponders == nil {
		return refusal(sessionwire.ErrorCodeRuntimeUnavailable, identity.ErrMetadataUnsupported)
	}
	accepts, err := s.cfg.PrincipalResponders.AcceptsCommandPrincipal(ctx, owner)
	if err != nil {
		return fmt.Errorf("admission: ask the owner whether it applies command principal and metadata: %w", err)
	}
	if !accepts {
		return refusal(sessionwire.ErrorCodeRuntimeUnavailable, identity.ErrMetadataUnsupported)
	}
	return nil
}

// existingCompatible reads the session a command is addressed to and refuses
// one this deployment cannot serve.
//
// It does NOT restate the legacy-session refusal. Every path reaches the retry
// read first, and GetDispositionCommand refuses a legacy-bound session in the
// store's own words before this function runs; commandRefusal is the one place
// that answer is classified. A second check here would be a second authority
// that no path could reach.
func (s *Service) existingCompatible(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) (sessionstore.CatalogEntry, error) {
	entry, err := s.cfg.Catalog.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{TenantID: tenant, SessionID: session})
	if catalogNotFound(err) {
		return sessionstore.CatalogEntry{}, refusal(sessionwire.ErrorCodeSessionNotFound, err)
	}
	if err != nil {
		return sessionstore.CatalogEntry{}, err
	}
	known, err := s.cfg.Targets.IsKnown(ctx, catalogTarget(entry.Record))
	if err != nil {
		return sessionstore.CatalogEntry{}, fmt.Errorf("admission: read the configured launch targets: %w", err)
	}
	if !known {
		return sessionstore.CatalogEntry{}, refusal(sessionwire.ErrorCodeRuntimeUnavailable, nil)
	}
	return entry, nil
}

// admit writes one command into the session's disposition inbox.
//
// THE BINDING IS THE CATALOG'S, passed in rather than configured. The store
// requires AdmitDispositionCommandRequest.Binding to EQUAL the session's
// immutable pin and refuses anything else as a command mismatch, and the only
// value that can equal it is the one the store holds: the entry
// existingCompatible read, or on a retry the winner's own descriptor, which
// the store verified against the same pin when it read it back.
//
// An oversized payload is uploaded first and admitted by reference (runbook
// A3.1 step 5). The store compares a retry on PayloadDigest and PayloadSize
// and deliberately not on the object reference, so a retry that uploads again
// under a fresh generation still matches; the losing upload may stay orphaned,
// which the store documents.
func (s *Service) admit(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, command sessionwire.CommandID, kind sessionstore.CommandKind, binding sessionstore.SessionBinding, payload []byte, m members) (sessionstore.DispositionInboxEntry, bool, error) {
	runtimeID, err := s.cfg.IDs.NewUUID()
	if err != nil {
		return sessionstore.DispositionInboxEntry{}, false, err
	}
	now := s.cfg.Clock.Now().UTC()
	req := sessionstore.AdmitDispositionCommandRequest{
		TenantID: tenant, SessionID: session, CommandID: command, Binding: binding,
		ProposedRuntimeCommandID: sessionstore.RuntimeCommandID(runtimeID), Kind: kind,
		AcceptedAt: now, ApplyDeadline: now.Add(s.cfg.ApplyDeadline),
		Principal: m.principal, Metadata: m.metadata,
	}
	if len(payload) > sessionstore.MaxInboxPayloadBytes {
		object, err := putCommandPayload(ctx, s.cfg.Commands, tenant, session, payload)
		if err != nil {
			return sessionstore.DispositionInboxEntry{}, false, commandRefusal(err)
		}
		req.PayloadObject = &object
	} else {
		req.Payload = payload
	}
	entry, created, err := s.cfg.Commands.AdmitDispositionCommand(ctx, req)
	if err != nil {
		return sessionstore.DispositionInboxEntry{}, false, commandRefusal(err)
	}
	return entry, created, nil
}

// retry resolves an already durable command before consulting mutable target,
// gate, or owner state. A retry asks what happened at the original acceptance
// instant; a gate closing or a configured default changing afterwards cannot
// turn that answer into a new pre-admission refusal. AdmitDispositionCommand
// performs the immutable content comparison -- kind, digest and size -- and
// returns the winner's runtime mapping, so the retry is re-admitted under the
// winner's own binding rather than answered from this read.
func (s *Service) retry(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, command sessionwire.CommandID, kind sessionstore.CommandKind, payload []byte, m members) (sessionstore.DispositionInboxEntry, bool, error) {
	found, err := s.cfg.Commands.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{TenantID: tenant, SessionID: session, CommandID: command})
	if commandNotFound(err) {
		return sessionstore.DispositionInboxEntry{}, false, nil
	}
	if err != nil {
		return sessionstore.DispositionInboxEntry{}, false, commandRefusal(err)
	}
	entry, _, err := s.admit(ctx, tenant, session, command, kind, found.Record.Descriptor.Binding, payload, m)
	return entry, true, err
}

// commandRefusal classifies the store's answer to a command admission.
//
// Two answers are decisions about the caller's command and everything else is
// a fault, returned as itself for the reason resolveTargetFault gives:
//
//   - a command MISMATCH is the caller reusing a CommandID for different
//     content, a different kind, or the session's own create: command_rejected.
//   - a PROTOCOL-MODE refusal is a legacy-bound session, which the store
//     reports as a catalog invalid or conflict on binding.protocol_mode:
//     runtime_unavailable, for ErrLegacySessionUnsupported's reason. The
//     store's BACKEND code on the same field is an outage and stays a fault.
func commandRefusal(err error) error {
	if legacyProtocol(err) {
		return refusal(sessionwire.ErrorCodeRuntimeUnavailable, fmt.Errorf("%w: %w", ErrLegacySessionUnsupported, err))
	}
	var inbox *sessionstore.InboxError
	if errors.As(err, &inbox) && inbox.Code == sessionstore.InboxErrorCommandMismatch {
		return refusal(sessionwire.ErrorCodeCommandRejected, err)
	}
	return err
}

// legacyProtocolField is the store's name for the member a legacy-bound
// session disagrees on.
const legacyProtocolField = "binding.protocol_mode"

// legacyProtocol reports the store refusing a disposition operation because
// the session is not bound to the disposition protocol.
func legacyProtocol(err error) bool {
	var catalog *sessionstore.CatalogError
	if !errors.As(err, &catalog) || catalog.Field != legacyProtocolField {
		return false
	}
	return catalog.Code == sessionstore.CatalogErrorInvalid || catalog.Code == sessionstore.CatalogErrorConflict
}

type LegacyCreateRequest struct {
	AgentID sessionwire.AgentID
	Blocks  json.RawMessage
}

type LegacyCreateResult struct {
	SessionID sessionwire.SessionID
	Entry     sessionstore.InboxEntry
}

func (s *Service) AdmitLegacyCreate(ctx context.Context, principal identity.Principal, req LegacyCreateRequest) (LegacyCreateResult, error) {
	end, gateErr := s.beginAdmission()
	if gateErr != nil {
		return LegacyCreateResult{}, gateErr
	}
	defer end()
	return LegacyCreateResult{}, refusal(sessionwire.ErrorCodeRuntimeUnavailable, ErrLegacyCreateUnsupported)
}

func canonicalCommand(request any) ([]byte, error) {
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("admission: encode canonical V1 command: %w", err)
	}
	return payload, nil
}

func catalogTarget(record sessionstore.CatalogRecord) sessionstore.HostTargetKey {
	return sessionstore.HostTargetKey{AgentID: record.AgentID, RuntimeCompatibilityID: record.RuntimeCompatibilityID, Placement: record.DesiredPlacement}
}

// catalogNotFound is internal/command's absence authority, called rather than
// restated. A3.3 removed the second one.
//
// This package used to read two of the released store's four spellings of "there
// is no such session", while internal/httpapi read all four: the same deleted or
// identity-mismatched session answered a durable READ with 404 session_not_found
// and a control COMMAND with a bare fault. That is `A9.1-notfound`, and two
// readers of one store vocabulary is exactly the shape the shared package
// exists to remove -- the same argument the command kinds moved for.
//
// See command.SessionAbsent for which codes are absence and why everything else
// stays a fault.
func catalogNotFound(err error) bool {
	return command.SessionAbsent(err)
}

// commandNotFound reports that the retry read found no command to resolve, in
// which case admission continues and the catalog read that follows answers for
// the SESSION.
//
// GetDispositionCommand reads the session's catalog before the inbox row, so
// an absent session arrives in every spelling command.SessionAbsent knows as
// well as the inbox's own not-found. All of them mean "no durable command";
// existingCompatible is what then turns an absent session into
// session_not_found, so this function does not need to decide that.
func commandNotFound(err error) bool {
	var target *sessionstore.InboxError
	if errors.As(err, &target) && target.Code == sessionstore.InboxErrorNotFound {
		return true
	}
	return command.SessionAbsent(err)
}

func gateAdmission(gates []sessionwire.GateProjection, request sessionwire.GateResponseRequest, now time.Time) error {
	for _, gate := range gates {
		if gate.GateID != request.GateID {
			continue
		}
		if !gate.Deadline.After(now) {
			return refusal(sessionwire.ErrorCodeGateExpired, nil)
		}
		matchingVersion := request.ExpectedOpenEventID != "" && gate.OpenedEventID == request.ExpectedOpenEventID
		matchingVersion = matchingVersion || request.ExpectedOpenJournalSeq != 0 && gate.OpenedJournalSeq == request.ExpectedOpenJournalSeq
		if !matchingVersion {
			return refusal(sessionwire.ErrorCodeGateResolved, nil)
		}
		if gate.Answerability != sessionwire.GateAnswerabilityResident {
			return refusal(sessionwire.ErrorCodeGateNotResumable, nil)
		}
		return nil
	}
	return refusal(sessionwire.ErrorCodeGateResolved, nil)
}

// freshMatchingOwner reports whether a resident Host may be handed this
// session's gate response.
//
// It is the placement package's rule, called rather than restated. The question
// -- is this observation this session's own live, admitting owner -- is exactly
// the one a placement decides before choosing to reuse an owner instead of
// re-placing, and the two answers must not be able to differ: an owner
// placement would re-place while admission still delivered to it is a command
// handed to a Host the router is about to abandon. A5's own copy of the
// comparison predated internal/placement and was identical to it; keeping both
// would have been a second authority for one rule.
func freshMatchingOwner(owner sessionwire.HostLinkRegistryObservation, record sessionstore.CatalogRecord, now time.Time) bool {
	return placement.ReusableOwner(owner, record, now)
}
