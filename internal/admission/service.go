package admission

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

// ErrPayloadProtocolUnavailable reports an input whose payload is too large
// for the inbox this module can write to.
//
// THE MISSING PIECE IS NOT A STORE PRIMITIVE, and the earlier wording of this
// comment said it was, which was true at sessionstore v0.4.0 and is false at
// the pinned version. The disposition inbox admits an oversized body by
// reference and compares a retry on PayloadDigest and PayloadSize rather than
// on the object reference, so re-PUTting under a fresh generation is no longer
// a mismatch: the hazard that made this unimplementable is gone.
//
// What is missing is the SESSION BINDING that reaches it. Every disposition
// entry point requires a complete immutable binding whose ProtocolMode is
// disposition, and the four commands this error governs are admitted into the
// LEGACY inbox, so there is no disposition record for such a reference to sit
// on. A3.1 changed which commands that covers but not the rule: a CREATE now
// takes the disposition path and stores an oversized payload by reference,
// while input, interrupt, restore and gate response remain legacy. On the
// legacy inbox the original hazard stands unchanged at this pin — the retry
// comparison still includes the object reference, and PutObject still mints a
// fresh generation per call — so admitting the reference there would make every
// legitimate retry a permanent CommandMismatch. Refusing is still correct for
// those four.
var ErrPayloadProtocolUnavailable = errors.New("admission: oversized payload needs a disposition session binding this module cannot author")

// ErrLegacyCreateUnsupported reports that Factory cannot create a session on
// the legacy protocol. No runtime this program ships can host such a session.
var ErrLegacyCreateUnsupported = errors.New("admission: legacy create unsupported")

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

type CommandStore interface {
	AdmitCommand(context.Context, sessionstore.AdmitCommandRequest) (sessionstore.InboxEntry, bool, error)
	GetCommand(context.Context, sessionstore.GetCommandRequest) (sessionstore.InboxEntry, error)
}

type OwnerDirectory interface {
	Owner(context.Context, sessionwire.TenantID, sessionwire.SessionID) (sessionwire.HostLinkRegistryObservation, bool, error)
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

type Service struct{ cfg Config }

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
// It returns a DISPOSITION entry, not a legacy InboxEntry, and the difference
// is the whole point rather than a type detail. A create is the one command
// that CHOOSES a session's protocol, and the only choice that produces a
// session a Host can ever take residency on is disposition.
//
// The payload ceiling is deliberately NOT checked here the way it is for the
// four existing commands. admissiblePayload refuses an oversized body because
// the legacy inbox cannot retain its identity; this path can, so an oversized
// create is stored by reference instead of refused. That is step 5.
func (s *Service) AdmitCreate(ctx context.Context, principal identity.Principal, req sessionwire.CreateRequest) (sessionstore.DispositionInboxEntry, bool, error) {
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

func (s *Service) AdmitInput(ctx context.Context, principal identity.Principal, req sessionwire.InputRequest) (sessionstore.InboxEntry, bool, error) {
	if err := req.Validate(); err != nil {
		return sessionstore.InboxEntry{}, false, refusal(sessionwire.ErrorCodeInvalidRequest, err)
	}
	return s.admitExisting(ctx, principal, req.SessionID, req.CommandID, CommandInput, req)
}

func (s *Service) AdmitInterrupt(ctx context.Context, principal identity.Principal, req sessionwire.InterruptRequest) (sessionstore.InboxEntry, bool, error) {
	if err := req.Validate(); err != nil {
		return sessionstore.InboxEntry{}, false, refusal(sessionwire.ErrorCodeInvalidRequest, err)
	}
	return s.admitExisting(ctx, principal, req.SessionID, req.CommandID, CommandInterrupt, req)
}

func (s *Service) AdmitRestore(ctx context.Context, principal identity.Principal, req sessionwire.RestoreRequest) (sessionstore.InboxEntry, bool, error) {
	if err := req.Validate(); err != nil {
		return sessionstore.InboxEntry{}, false, refusal(sessionwire.ErrorCodeInvalidRequest, err)
	}
	return s.admitExisting(ctx, principal, req.SessionID, req.CommandID, CommandRestore, req)
}

func (s *Service) AdmitGateResponse(ctx context.Context, principal identity.Principal, req sessionwire.GateResponseRequest) (sessionstore.InboxEntry, bool, error) {
	if err := req.Validate(); err != nil {
		return sessionstore.InboxEntry{}, false, refusal(sessionwire.ErrorCodeInvalidRequest, err)
	}
	payload, err := canonicalCommand(req)
	if err := admissiblePayload(payload, err); err != nil {
		return sessionstore.InboxEntry{}, false, err
	}
	if err := s.cfg.Authorizer.AuthorizeControl(ctx, principal, req.SessionID, CommandGateResponse); err != nil {
		return sessionstore.InboxEntry{}, false, err
	}
	if retry, handled, err := s.retry(ctx, principal.Tenant(), req.SessionID, req.CommandID, CommandGateResponse, payload); handled || err != nil {
		return retry, false, err
	}
	entry, err := s.existingCompatible(ctx, principal.Tenant(), req.SessionID)
	if err != nil {
		return sessionstore.InboxEntry{}, false, err
	}
	now := s.cfg.Clock.Now()
	if err := gateAdmission(entry.Record.OpenGates, req, now); err != nil {
		return sessionstore.InboxEntry{}, false, err
	}
	owner, ok, err := s.cfg.Directory.Owner(ctx, principal.Tenant(), req.SessionID)
	if err != nil {
		return sessionstore.InboxEntry{}, false, fmt.Errorf("admission: observe the session's owner: %w", err)
	}
	if !ok || !freshMatchingOwner(owner, entry.Record, now) {
		return sessionstore.InboxEntry{}, false, refusal(sessionwire.ErrorCodeGateNotResumable, nil)
	}
	return s.admit(ctx, principal.Tenant(), req.SessionID, req.CommandID, CommandGateResponse, payload)
}

func (s *Service) admitExisting(ctx context.Context, principal identity.Principal, session sessionwire.SessionID, command sessionwire.CommandID, kind sessionstore.CommandKind, request any) (sessionstore.InboxEntry, bool, error) {
	payload, err := canonicalCommand(request)
	if err := admissiblePayload(payload, err); err != nil {
		return sessionstore.InboxEntry{}, false, err
	}
	if err := s.cfg.Authorizer.AuthorizeControl(ctx, principal, session, kind); err != nil {
		return sessionstore.InboxEntry{}, false, err
	}
	if retry, handled, err := s.retry(ctx, principal.Tenant(), session, command, kind, payload); handled || err != nil {
		return retry, false, err
	}
	if _, err := s.existingCompatible(ctx, principal.Tenant(), session); err != nil {
		return sessionstore.InboxEntry{}, false, err
	}
	return s.admit(ctx, principal.Tenant(), session, command, kind, payload)
}

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

func (s *Service) admit(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, command sessionwire.CommandID, kind sessionstore.CommandKind, payload []byte) (sessionstore.InboxEntry, bool, error) {
	if len(payload) > sessionstore.MaxInboxPayloadBytes {
		return sessionstore.InboxEntry{}, false, refusal(sessionwire.ErrorCodeRuntimeUnavailable, ErrPayloadProtocolUnavailable)
	}
	runtimeID, err := s.cfg.IDs.NewUUID()
	if err != nil {
		return sessionstore.InboxEntry{}, false, err
	}
	now := s.cfg.Clock.Now().UTC()
	entry, created, err := s.cfg.Commands.AdmitCommand(ctx, sessionstore.AdmitCommandRequest{
		TenantID: tenant, SessionID: session, CommandID: command,
		ProposedRuntimeCommandID: sessionstore.RuntimeCommandID(runtimeID), Kind: kind,
		Payload: payload, AcceptedAt: now, ApplyDeadline: now.Add(s.cfg.ApplyDeadline),
	})
	var inbox *sessionstore.InboxError
	if errors.As(err, &inbox) && inbox.Code == sessionstore.InboxErrorCommandMismatch {
		return sessionstore.InboxEntry{}, false, refusal(sessionwire.ErrorCodeCommandRejected, err)
	}
	return entry, created, err
}

// retry resolves an already durable command before consulting mutable target,
// gate, or owner state. A retry asks what happened at the original acceptance
// instant; a gate closing or a configured default changing afterwards cannot
// turn that answer into a new pre-admission refusal. AdmitCommand performs the
// immutable payload comparison and returns the winner's runtime mapping.
func (s *Service) retry(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, command sessionwire.CommandID, kind sessionstore.CommandKind, payload []byte) (sessionstore.InboxEntry, bool, error) {
	_, err := s.cfg.Commands.GetCommand(ctx, sessionstore.GetCommandRequest{TenantID: tenant, SessionID: session, CommandID: command})
	if commandNotFound(err) {
		return sessionstore.InboxEntry{}, false, nil
	}
	if err != nil {
		return sessionstore.InboxEntry{}, false, err
	}
	entry, _, err := s.admit(ctx, tenant, session, command, kind, payload)
	return entry, true, err
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
	return LegacyCreateResult{}, refusal(sessionwire.ErrorCodeRuntimeUnavailable, ErrLegacyCreateUnsupported)
}

func canonicalCommand(request any) ([]byte, error) {
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("admission: encode canonical V1 command: %w", err)
	}
	return payload, nil
}

func admissiblePayload(payload []byte, encodeErr error) error {
	if encodeErr != nil {
		return refusal(sessionwire.ErrorCodeInvalidRequest, encodeErr)
	}
	if len(payload) > sessionstore.MaxInboxPayloadBytes {
		return refusal(sessionwire.ErrorCodeRuntimeUnavailable, ErrPayloadProtocolUnavailable)
	}
	return nil
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

func commandNotFound(err error) bool {
	var target *sessionstore.InboxError
	if errors.As(err, &target) && target.Code == sessionstore.InboxErrorNotFound {
		return true
	}
	var keyspace *sessionstore.KeyspaceError
	return errors.As(err, &keyspace) && keyspace.Code == sessionstore.KeyspaceBindingNotFound
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
