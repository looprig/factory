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

// ErrPayloadProtocolUnavailable reports an input that cannot be admitted by
// the released inbox protocol without weakening retry conflict detection.
// Inbox v1 retains either bytes or an ObjectReference, but not the immutable
// payload digest needed to compare a retry after PutObject minted a new object
// generation. The future disposition descriptor supplies that identity.
var ErrPayloadProtocolUnavailable = errors.New("admission: oversized payload requires an identity-bearing inbox protocol")

// ErrCreateIdentityProtocolUnavailable reports that the released Store cannot
// bind a client create CommandID independently of its proposed SessionID.
// Catalog desired idempotency is mutable placement state and is not used as a
// substitute for the missing immutable reservation.
var ErrCreateIdentityProtocolUnavailable = errors.New("admission: V1 create requires an immutable create-command reservation")

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
	Authorizer    Authorizer
	Targets       TargetResolver
	Catalog       Catalog
	Commands      CommandStore
	Directory     OwnerDirectory
	Clock         Clock
	IDs           UUIDSource
	ApplyDeadline time.Duration
}

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

func (s *Service) AdmitCreate(ctx context.Context, principal identity.Principal, req sessionwire.CreateRequest) (sessionstore.InboxEntry, bool, error) {
	if err := req.Validate(); err != nil {
		return sessionstore.InboxEntry{}, false, refusal(sessionwire.ErrorCodeInvalidRequest, err)
	}
	payload, err := canonicalCommand(req)
	if err := admissiblePayload(payload, err); err != nil {
		return sessionstore.InboxEntry{}, false, err
	}
	if err := s.cfg.Authorizer.AuthorizeControl(ctx, principal, req.SessionID, CommandCreate); err != nil {
		return sessionstore.InboxEntry{}, false, err
	}
	target, known, targetErr := s.cfg.Targets.ResolveAgent(ctx, req.AgentID)
	if targetErr != nil || !known || target.Key.AgentID != req.AgentID {
		return sessionstore.InboxEntry{}, false, refusal(sessionwire.ErrorCodeRuntimeUnavailable, targetErr)
	}
	return sessionstore.InboxEntry{}, false, refusal(sessionwire.ErrorCodeRuntimeUnavailable, ErrCreateIdentityProtocolUnavailable)
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
	if err != nil || !ok || !freshMatchingOwner(owner, entry.Record, now) {
		return sessionstore.InboxEntry{}, false, refusal(sessionwire.ErrorCodeGateNotResumable, err)
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
	if err != nil || !known {
		return sessionstore.CatalogEntry{}, refusal(sessionwire.ErrorCodeRuntimeUnavailable, err)
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
	sessionID, err := s.cfg.IDs.NewUUID()
	if err != nil {
		return LegacyCreateResult{}, err
	}
	commandID, err := s.cfg.IDs.NewUUID()
	if err != nil {
		return LegacyCreateResult{}, err
	}
	create := sessionwire.CreateRequest{
		CommandEnvelope: sessionwire.CommandEnvelope{Version: sessionwire.CurrentWireVersion, CommandID: sessionwire.CommandID(commandID)},
		SessionID:       sessionwire.SessionID(sessionID), AgentID: req.AgentID, Blocks: req.Blocks,
	}
	entry, err := s.admitLegacyCreate(ctx, principal, create)
	return LegacyCreateResult{SessionID: sessionwire.SessionID(sessionID), Entry: entry}, err
}

func (s *Service) admitLegacyCreate(ctx context.Context, principal identity.Principal, req sessionwire.CreateRequest) (sessionstore.InboxEntry, error) {
	if err := req.Validate(); err != nil {
		return sessionstore.InboxEntry{}, refusal(sessionwire.ErrorCodeInvalidRequest, err)
	}
	payload, err := canonicalCommand(req)
	if err := admissiblePayload(payload, err); err != nil {
		return sessionstore.InboxEntry{}, err
	}
	if err := s.cfg.Authorizer.AuthorizeControl(ctx, principal, req.SessionID, CommandCreate); err != nil {
		return sessionstore.InboxEntry{}, err
	}
	target, known, targetErr := s.cfg.Targets.ResolveAgent(ctx, req.AgentID)
	if targetErr != nil || !known || target.Key.AgentID != req.AgentID {
		return sessionstore.InboxEntry{}, refusal(sessionwire.ErrorCodeRuntimeUnavailable, targetErr)
	}
	now := s.cfg.Clock.Now().UTC()
	entry, _, err := s.cfg.Catalog.CreateCatalogEntry(ctx, sessionstore.CreateCatalogEntryRequest{
		TenantID: principal.Tenant(), SessionID: req.SessionID, AgentID: req.AgentID,
		RuntimeCompatibilityID: target.Key.RuntimeCompatibilityID, CreatedAt: now, LastActiveAt: now,
		State: sessionwire.SessionStateIdle, Residency: sessionwire.SessionResidencyCold,
		DesiredPlacement: target.Key.Placement, DesiredWorkload: target.Workload,
		IdempotencyKey: string(req.CommandID),
	})
	if err != nil {
		return sessionstore.InboxEntry{}, err
	}
	if entry.Record.AgentID != req.AgentID || entry.Record.DesiredIdempotencyKey != string(req.CommandID) {
		return sessionstore.InboxEntry{}, refusal(sessionwire.ErrorCodeCommandRejected, nil)
	}
	admitted, _, err := s.admit(ctx, principal.Tenant(), req.SessionID, req.CommandID, CommandCreate, payload)
	return admitted, err
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

func catalogNotFound(err error) bool {
	var target *sessionstore.CatalogError
	if errors.As(err, &target) && target.Code == sessionstore.CatalogErrorNotFound {
		return true
	}
	var keyspace *sessionstore.KeyspaceError
	return errors.As(err, &keyspace) && keyspace.Code == sessionstore.KeyspaceBindingNotFound
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
