package admission

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	"github.com/looprig/sessionstore"
)

var serviceNow = time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)

// faultInjector is the shared half of every dependency fake: which method is
// currently configured to fail, and which methods were actually CALLED.
//
// Both halves are keyed by the dependency's own METHOD NAME, because that is
// what TestNoDependencyFaultBecomesAPublicCode derives its subject from -- the
// method sets of the interfaces the Service declares, read by reflection. A
// fake that recorded a private enum instead would be a second list to keep in
// step with the interfaces, which is the thing that test exists not to have.
type faultInjector struct {
	// failing is the method name that returns errInjectedFault when called.
	failing string
	// called records every method name this fake was asked for.
	called map[string]bool
}

// errInjectedFault is the dependency failure every site is driven with. It is one
// value so "did this surface" is decidable by errors.Is at any depth.
var errInjectedFault = errors.New("the dependency could not be reached")

// enter records a call and reports the failure this method must return, if any.
func (f *faultInjector) enter(method string) error {
	if f.called == nil {
		f.called = map[string]bool{}
	}
	f.called[method] = true
	if f.failing == method {
		return errInjectedFault
	}
	return nil
}

type serviceAuthorizer struct {
	faultInjector
	err   error
	calls int
}

func (a *serviceAuthorizer) AuthorizeControl(context.Context, identity.Principal, sessionwire.SessionID, sessionstore.CommandKind) error {
	a.calls++
	if err := a.enter("AuthorizeControl"); err != nil {
		return err
	}
	return a.err
}

func (a *serviceAuthorizer) AuthorizeServiceSweep(context.Context, identity.Principal) error {
	if err := a.enter("AuthorizeServiceSweep"); err != nil {
		return err
	}
	return a.err
}

type serviceTargets struct {
	faultInjector
	target Target
	known  bool
	err    error
	calls  int
}

func (t *serviceTargets) ResolveAgent(context.Context, sessionwire.AgentID) (Target, bool, error) {
	t.calls++
	if err := t.enter("ResolveAgent"); err != nil {
		return Target{}, false, err
	}
	return t.target, t.known, t.err
}

func (t *serviceTargets) IsKnown(context.Context, sessionstore.HostTargetKey) (bool, error) {
	t.calls++
	if err := t.enter("IsKnown"); err != nil {
		return false, err
	}
	return t.known, t.err
}

type serviceCatalog struct {
	faultInjector
	entry       sessionstore.CatalogEntry
	getErr      error
	createCalls int
}

func (c *serviceCatalog) GetCatalogEntry(context.Context, sessionstore.GetCatalogEntryRequest) (sessionstore.CatalogEntry, error) {
	if err := c.enter("GetCatalogEntry"); err != nil {
		return sessionstore.CatalogEntry{}, err
	}
	return c.entry, c.getErr
}

func (c *serviceCatalog) CreateCatalogEntry(_ context.Context, req sessionstore.CreateCatalogEntryRequest) (sessionstore.CatalogEntry, bool, error) {
	c.createCalls++
	if err := c.enter("CreateCatalogEntry"); err != nil {
		return sessionstore.CatalogEntry{}, false, err
	}
	c.entry = sessionstore.CatalogEntry{Record: sessionstore.CatalogRecord{
		TenantID: req.TenantID, SessionID: req.SessionID, AgentID: req.AgentID,
		RuntimeCompatibilityID: req.RuntimeCompatibilityID, CreatedAt: req.CreatedAt,
		LastActiveAt: req.LastActiveAt, State: req.State, Residency: req.Residency,
		DesiredPlacement: req.DesiredPlacement, DesiredWorkload: req.DesiredWorkload,
		DesiredIdempotencyKey: req.IdempotencyKey, DesiredGeneration: 1,
	}, Revision: 1}
	c.getErr = nil
	return c.entry, true, nil
}

type serviceCommands struct {
	faultInjector
	records map[sessionwire.CommandID]sessionstore.InboxEntry
	calls   int
}

func (c *serviceCommands) GetCommand(_ context.Context, req sessionstore.GetCommandRequest) (sessionstore.InboxEntry, error) {
	if err := c.enter("GetCommand"); err != nil {
		return sessionstore.InboxEntry{}, err
	}
	entry, ok := c.records[req.CommandID]
	if !ok {
		return sessionstore.InboxEntry{}, &sessionstore.InboxError{Code: sessionstore.InboxErrorNotFound}
	}
	return entry, nil
}

func (c *serviceCommands) AdmitCommand(_ context.Context, req sessionstore.AdmitCommandRequest) (sessionstore.InboxEntry, bool, error) {
	c.calls++
	if err := c.enter("AdmitCommand"); err != nil {
		return sessionstore.InboxEntry{}, false, err
	}
	if prior, ok := c.records[req.CommandID]; ok {
		if prior.Record.Kind != req.Kind || !bytes.Equal(prior.Record.Payload, req.Payload) || prior.Record.PayloadRef != req.PayloadRef {
			return sessionstore.InboxEntry{}, false, &sessionstore.InboxError{Code: sessionstore.InboxErrorCommandMismatch}
		}
		return prior, false, nil
	}
	entry := sessionstore.InboxEntry{Record: sessionstore.InboxRecord{
		TenantID: req.TenantID, SessionID: req.SessionID, CommandID: req.CommandID,
		RuntimeCommandID: req.ProposedRuntimeCommandID, Kind: req.Kind,
		Payload: append([]byte(nil), req.Payload...), PayloadRef: req.PayloadRef,
		AcceptedAt: req.AcceptedAt, ApplyDeadline: req.ApplyDeadline, State: sessionstore.InboxStatePending,
	}, Revision: 1, AcceptedOrder: uint64(len(c.records) + 1)}
	c.records[req.CommandID] = entry
	return entry, true, nil
}

type serviceDirectory struct {
	faultInjector
	owner sessionwire.HostLinkRegistryObservation
	ok    bool
	err   error
	calls int
}

func (d *serviceDirectory) Owner(context.Context, sessionwire.TenantID, sessionwire.SessionID) (sessionwire.HostLinkRegistryObservation, bool, error) {
	d.calls++
	if err := d.enter("Owner"); err != nil {
		return sessionwire.HostLinkRegistryObservation{}, false, err
	}
	return d.owner, d.ok, d.err
}

type serviceClock struct{ now time.Time }

func (c serviceClock) Now() time.Time { return c.now }
func (c serviceClock) AfterFunc(time.Duration, func()) func() bool {
	return func() bool { return true }
}

type serviceIDs struct {
	faultInjector
	next int
}

func (s *serviceIDs) NewUUID() (string, error) {
	if err := s.enter("NewUUID"); err != nil {
		return "", err
	}
	s.next++
	return "generated-" + string(rune('0'+s.next)), nil
}

type serviceFixture struct {
	service   *Service
	auth      *serviceAuthorizer
	targets   *serviceTargets
	catalog   *serviceCatalog
	commands  *serviceCommands
	directory *serviceDirectory
	ids       *serviceIDs
	principal identity.Principal
}

func newServiceFixture(t *testing.T) *serviceFixture {
	t.Helper()
	p, err := identity.NewPrincipal("tenant-a", "actor-a", identity.KindActor)
	if err != nil {
		t.Fatal(err)
	}
	auth := &serviceAuthorizer{}
	targets := &serviceTargets{known: true, target: Target{Key: sessionstore.HostTargetKey{
		AgentID: "agent-a", RuntimeCompatibilityID: "runtime-v1", Placement: sessionwire.HostPlacementPooled,
	}}}
	catalog := &serviceCatalog{getErr: &sessionstore.CatalogError{Code: sessionstore.CatalogErrorNotFound}}
	commands := &serviceCommands{records: make(map[sessionwire.CommandID]sessionstore.InboxEntry)}
	directory := &serviceDirectory{}
	ids := &serviceIDs{}
	svc, err := NewService(Config{Authorizer: auth, Targets: targets, Catalog: catalog, Commands: commands,
		Directory: directory, Clock: serviceClock{serviceNow}, IDs: ids, ApplyDeadline: time.Minute})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return &serviceFixture{svc, auth, targets, catalog, commands, directory, ids, p}
}

func envelope(id string) sessionwire.CommandEnvelope {
	return sessionwire.CommandEnvelope{Version: sessionwire.CurrentWireVersion, CommandID: sessionwire.CommandID(id)}
}

func TestV1RequestsValidateBeforeAuthorizationOrDurableWork(t *testing.T) {
	tests := []struct {
		name string
		call func(*serviceFixture) error
	}{
		{"create missing command", func(f *serviceFixture) error {
			_, _, err := f.service.AdmitCreate(context.Background(), f.principal, sessionwire.CreateRequest{CommandEnvelope: envelope(""), SessionID: "session-a", AgentID: "agent-a"})
			return err
		}},
		{"create malformed session", func(f *serviceFixture) error {
			_, _, err := f.service.AdmitCreate(context.Background(), f.principal, sessionwire.CreateRequest{CommandEnvelope: envelope("create-a"), SessionID: "", AgentID: "agent-a"})
			return err
		}},
		{"input malformed payload", func(f *serviceFixture) error {
			_, _, err := f.service.AdmitInput(context.Background(), f.principal, sessionwire.InputRequest{CommandEnvelope: envelope("input-a"), SessionID: "session-a", Blocks: []byte(`{}`)})
			return err
		}},
		{"interrupt oversized command", func(f *serviceFixture) error {
			_, _, err := f.service.AdmitInterrupt(context.Background(), f.principal, sessionwire.InterruptRequest{CommandEnvelope: envelope(strings.Repeat("x", sessionwire.MaxIDBytes+1)), SessionID: "session-a"})
			return err
		}},
		{"restore missing session", func(f *serviceFixture) error {
			_, _, err := f.service.AdmitRestore(context.Background(), f.principal, sessionwire.RestoreRequest{CommandEnvelope: envelope("restore-a")})
			return err
		}},
		{"gate malformed values", func(f *serviceFixture) error {
			_, _, err := f.service.AdmitGateResponse(context.Background(), f.principal, sessionwire.GateResponseRequest{CommandEnvelope: envelope("gate-a"), SessionID: "session-a", GateID: "gate-a", Action: "submit", Values: nil, ExpectedOpenEventID: "event-a"})
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newServiceFixture(t)
			if err := test.call(f); !IsCode(err, sessionwire.ErrorCodeInvalidRequest) {
				t.Fatalf("error = %v, want invalid_request", err)
			}
			if f.auth.calls != 0 || f.targets.calls != 0 || f.catalog.createCalls != 0 || f.commands.calls != 0 {
				t.Fatalf("invalid request reached dependencies: %+v", f)
			}
		})
	}
}

func TestV1WireDecodersRejectDuplicateCommandFields(t *testing.T) {
	tests := []struct {
		name string
		body string
		out  any
	}{
		{"create", `{"version":"v1","command_id":"a","command_id":"b","session_id":"s","agent_id":"a"}`, new(sessionwire.CreateRequest)},
		{"input", `{"version":"v1","command_id":"a","command_id":"b","session_id":"s","blocks":[{"text":"hi"}]}`, new(sessionwire.InputRequest)},
		{"interrupt", `{"version":"v1","command_id":"a","command_id":"b","session_id":"s"}`, new(sessionwire.InterruptRequest)},
		{"restore", `{"version":"v1","command_id":"a","command_id":"b","session_id":"s"}`, new(sessionwire.RestoreRequest)},
		{"gate", `{"version":"v1","command_id":"a","command_id":"b","session_id":"s","gate_id":"g","action":"submit","values":{},"expected_open_event_id":"e"}`, new(sessionwire.GateResponseRequest)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var validation *sessionwire.RequestValidationError
			if err := json.Unmarshal([]byte(test.body), test.out); !errors.As(err, &validation) || validation.Code != sessionwire.RequestValidationCodeDuplicateField {
				t.Fatalf("Unmarshal() = %v, want duplicate_field", err)
			}
		})
	}
}

func TestAuthorizationAndUnknownTargetPrecedeEveryWrite(t *testing.T) {
	for _, test := range []struct {
		name      string
		configure func(*serviceFixture)
	}{
		{"denied", func(f *serviceFixture) { f.auth.err = errors.New("denied") }},
		{"unknown target", func(f *serviceFixture) { f.targets.known = false }},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newServiceFixture(t)
			test.configure(f)
			_, _, err := f.service.AdmitCreate(context.Background(), f.principal, sessionwire.CreateRequest{CommandEnvelope: envelope("create-a"), SessionID: "session-a", AgentID: "agent-a"})
			if test.name == "unknown target" && !IsCode(err, sessionwire.ErrorCodeRuntimeUnavailable) {
				t.Fatalf("error = %v", err)
			}
			if err == nil {
				t.Fatal("admission succeeded")
			}
			if f.catalog.createCalls != 0 || f.commands.calls != 0 {
				t.Fatalf("refusal wrote durable state")
			}
		})
	}
}

func TestV1CreateFailsClosedWithoutTheImmutableCreateReservation(t *testing.T) {
	f := newServiceFixture(t)
	req := sessionwire.CreateRequest{CommandEnvelope: envelope("create-a"), SessionID: "session-a", AgentID: "agent-a", Blocks: []byte(`[{"text":"hi"}]`)}
	if _, _, err := f.service.AdmitCreate(context.Background(), f.principal, req); !errors.Is(err, ErrCreateIdentityProtocolUnavailable) || !IsCode(err, sessionwire.ErrorCodeRuntimeUnavailable) {
		t.Fatalf("error = %v, want fail-closed create protocol refusal", err)
	}
	if f.targets.calls != 1 {
		t.Fatalf("target checks = %d, want 1", f.targets.calls)
	}
	if f.catalog.createCalls != 0 || f.commands.calls != 0 {
		t.Fatal("unsupported V1 create changed durable state")
	}
}

func TestExistingCommandsReturnTheOriginalAndConflictingReuseFails(t *testing.T) {
	tests := []struct {
		name string
		call func(*serviceFixture, sessionwire.CommandID) (sessionstore.InboxEntry, bool, error)
	}{
		{"input", func(f *serviceFixture, id sessionwire.CommandID) (sessionstore.InboxEntry, bool, error) {
			return f.service.AdmitInput(context.Background(), f.principal, sessionwire.InputRequest{CommandEnvelope: envelope(string(id)), SessionID: "session-a", Blocks: []byte(`[{"text":"hi"}]`)})
		}},
		{"interrupt", func(f *serviceFixture, id sessionwire.CommandID) (sessionstore.InboxEntry, bool, error) {
			return f.service.AdmitInterrupt(context.Background(), f.principal, sessionwire.InterruptRequest{CommandEnvelope: envelope(string(id)), SessionID: "session-a"})
		}},
		{"restore", func(f *serviceFixture, id sessionwire.CommandID) (sessionstore.InboxEntry, bool, error) {
			return f.service.AdmitRestore(context.Background(), f.principal, sessionwire.RestoreRequest{CommandEnvelope: envelope(string(id)), SessionID: "session-a"})
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newServiceFixture(t)
			f.catalog.getErr = nil
			f.catalog.entry.Record = sessionstore.CatalogRecord{TenantID: "tenant-a", SessionID: "session-a", AgentID: "agent-a", RuntimeCompatibilityID: "runtime-v1", DesiredPlacement: sessionwire.HostPlacementPooled}
			first, created, err := test.call(f, "command-a")
			if err != nil || !created {
				t.Fatalf("first = (%+v, %v, %v)", first, created, err)
			}
			retry, created, err := test.call(f, "command-a")
			if err != nil || created || !reflect.DeepEqual(retry, first) {
				t.Fatalf("retry = (%+v, %v, %v)", retry, created, err)
			}
			if _, _, err := f.service.AdmitInterrupt(context.Background(), f.principal, sessionwire.InterruptRequest{CommandEnvelope: envelope("command-a"), SessionID: "session-a"}); test.name != "interrupt" && err == nil {
				t.Fatal("same command ID was reused for another kind")
			}
		})
	}
}

func TestInputReuseWithDifferentPayloadFails(t *testing.T) {
	f := newServiceFixture(t)
	f.catalog.getErr = nil
	f.catalog.entry.Record = sessionstore.CatalogRecord{TenantID: "tenant-a", SessionID: "session-a", AgentID: "agent-a", RuntimeCompatibilityID: "runtime-v1", DesiredPlacement: sessionwire.HostPlacementPooled}
	first := sessionwire.InputRequest{CommandEnvelope: envelope("input-a"), SessionID: "session-a", Blocks: []byte(`[{"text":"first"}]`)}
	if _, _, err := f.service.AdmitInput(context.Background(), f.principal, first); err != nil {
		t.Fatal(err)
	}
	first.Blocks = []byte(`[{"text":"second"}]`)
	if _, _, err := f.service.AdmitInput(context.Background(), f.principal, first); err == nil {
		t.Fatal("conflicting input payload was accepted")
	}
}

func TestUnknownPinnedRuntimePrecedesInboxAdmission(t *testing.T) {
	f := newServiceFixture(t)
	f.catalog.getErr = nil
	f.catalog.entry.Record = sessionstore.CatalogRecord{TenantID: "tenant-a", SessionID: "session-a", AgentID: "agent-a", RuntimeCompatibilityID: "runtime-old", DesiredPlacement: sessionwire.HostPlacementPooled}
	f.targets.known = false
	_, _, err := f.service.AdmitInterrupt(context.Background(), f.principal, sessionwire.InterruptRequest{CommandEnvelope: envelope("interrupt-a"), SessionID: "session-a"})
	if !IsCode(err, sessionwire.ErrorCodeRuntimeUnavailable) {
		t.Fatalf("error = %v", err)
	}
	if f.commands.calls != 0 {
		t.Fatal("unknown runtime reached inbox")
	}
}

func TestOversizedPrivatePayloadFailsClosedUntilTheStoreRetainsItsIdentity(t *testing.T) {
	f := newServiceFixture(t)
	f.catalog.getErr = nil
	f.catalog.entry.Record = sessionstore.CatalogRecord{TenantID: "tenant-a", SessionID: "session-a", AgentID: "agent-a", RuntimeCompatibilityID: "runtime-v1", DesiredPlacement: sessionwire.HostPlacementPooled}
	blocks := []byte(`["` + strings.Repeat("x", sessionstore.MaxInboxPayloadBytes) + `"]`)
	_, _, err := f.service.AdmitInput(context.Background(), f.principal, sessionwire.InputRequest{CommandEnvelope: envelope("input-a"), SessionID: "session-a", Blocks: blocks})
	if !errors.Is(err, ErrPayloadProtocolUnavailable) {
		t.Fatalf("error = %v, want ErrPayloadProtocolUnavailable", err)
	}
	if f.commands.calls != 0 {
		t.Fatal("unsupported payload reached durable writes")
	}
}

func TestOversizedCreateDoesNotCreateCatalogState(t *testing.T) {
	f := newServiceFixture(t)
	blocks := []byte(`["` + strings.Repeat("x", sessionstore.MaxInboxPayloadBytes) + `"]`)
	_, _, err := f.service.AdmitCreate(context.Background(), f.principal, sessionwire.CreateRequest{CommandEnvelope: envelope("create-a"), SessionID: "session-a", AgentID: "agent-a", Blocks: blocks})
	if !errors.Is(err, ErrPayloadProtocolUnavailable) {
		t.Fatalf("error = %v", err)
	}
	if f.auth.calls != 0 || f.catalog.createCalls != 0 || f.commands.calls != 0 {
		t.Fatal("unsupported create changed state")
	}
}

func TestGateResponseRequiresMatchingProjectionAndFreshResidentOwner(t *testing.T) {
	base := func(f *serviceFixture) sessionwire.GateResponseRequest {
		f.catalog.getErr = nil
		f.catalog.entry.Record = sessionstore.CatalogRecord{TenantID: "tenant-a", SessionID: "session-a", AgentID: "agent-a", RuntimeCompatibilityID: "runtime-v1", DesiredPlacement: sessionwire.HostPlacementPooled,
			OpenGates: []sessionwire.GateProjection{{GateID: "gate-a", OpenedEventID: "event-a", OpenedJournalSeq: 7, Deadline: serviceNow.Add(time.Hour), Answerability: sessionwire.GateAnswerabilityResident}}}
		f.directory.ok = true
		f.directory.owner = sessionwire.HostLinkRegistryObservation{TenantID: "tenant-a", SessionID: "session-a", AgentID: "agent-a", RuntimeCompatibilityID: "runtime-v1", Placement: sessionwire.HostPlacementPooled, Residency: sessionwire.SessionResidencyResident, Accepting: true, ExpiresAt: serviceNow.Add(time.Minute)}
		return sessionwire.GateResponseRequest{CommandEnvelope: envelope("gate-command"), SessionID: "session-a", GateID: "gate-a", Action: "submit", Values: map[string]json.RawMessage{"answer": json.RawMessage(`"yes"`)}, ExpectedOpenEventID: "event-a"}
	}
	for _, test := range []struct {
		name   string
		mutate func(*serviceFixture, *sessionwire.GateResponseRequest)
		want   bool
		code   sessionwire.ErrorCode
	}{
		{"matching", func(*serviceFixture, *sessionwire.GateResponseRequest) {}, true, ""},
		{"missing gate", func(f *serviceFixture, _ *sessionwire.GateResponseRequest) { f.catalog.entry.Record.OpenGates = nil }, false, sessionwire.ErrorCodeGateResolved},
		{"different incarnation", func(_ *serviceFixture, r *sessionwire.GateResponseRequest) { r.ExpectedOpenEventID = "event-b" }, false, sessionwire.ErrorCodeGateResolved},
		{"cold", func(f *serviceFixture, _ *sessionwire.GateResponseRequest) { f.directory.ok = false }, false, sessionwire.ErrorCodeGateNotResumable},
		{"releasing", func(f *serviceFixture, _ *sessionwire.GateResponseRequest) {
			f.directory.owner.Residency = sessionwire.SessionResidencyReleasing
		}, false, sessionwire.ErrorCodeGateNotResumable},
		{"not accepting", func(f *serviceFixture, _ *sessionwire.GateResponseRequest) { f.directory.owner.Accepting = false }, false, sessionwire.ErrorCodeGateNotResumable},
		{"expired", func(f *serviceFixture, _ *sessionwire.GateResponseRequest) { f.directory.owner.ExpiresAt = serviceNow }, false, sessionwire.ErrorCodeGateNotResumable},
		{"gate deadline expired", func(f *serviceFixture, _ *sessionwire.GateResponseRequest) {
			f.catalog.entry.Record.OpenGates[0].Deadline = serviceNow
		}, false, sessionwire.ErrorCodeGateExpired},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newServiceFixture(t)
			req := base(f)
			test.mutate(f, &req)
			_, _, err := f.service.AdmitGateResponse(context.Background(), f.principal, req)
			if test.want && err != nil {
				t.Fatal(err)
			}
			if !test.want && !IsCode(err, test.code) {
				t.Fatalf("error = %v", err)
			}
			if !test.want && f.commands.calls != 0 {
				t.Fatal("non-resumable gate reached inbox/object store")
			}
		})
	}
}

func TestGateResponseRetryReturnsOriginalAndDifferentAnswerConflicts(t *testing.T) {
	f := newServiceFixture(t)
	f.catalog.getErr = nil
	f.catalog.entry.Record = sessionstore.CatalogRecord{TenantID: "tenant-a", SessionID: "session-a", AgentID: "agent-a", RuntimeCompatibilityID: "runtime-v1", DesiredPlacement: sessionwire.HostPlacementPooled,
		OpenGates: []sessionwire.GateProjection{{GateID: "gate-a", OpenedEventID: "event-a", OpenedJournalSeq: 7, Deadline: serviceNow.Add(time.Hour), Answerability: sessionwire.GateAnswerabilityResident}}}
	f.directory.ok = true
	f.directory.owner = sessionwire.HostLinkRegistryObservation{TenantID: "tenant-a", SessionID: "session-a", AgentID: "agent-a", RuntimeCompatibilityID: "runtime-v1", Placement: sessionwire.HostPlacementPooled, Residency: sessionwire.SessionResidencyResident, Accepting: true, ExpiresAt: serviceNow.Add(time.Minute)}
	req := sessionwire.GateResponseRequest{CommandEnvelope: envelope("gate-command"), SessionID: "session-a", GateID: "gate-a", Action: "submit", Values: map[string]json.RawMessage{"answer": json.RawMessage(`"yes"`)}, ExpectedOpenEventID: "event-a"}
	first, created, err := f.service.AdmitGateResponse(context.Background(), f.principal, req)
	if err != nil || !created {
		t.Fatalf("first = (%+v, %v, %v)", first, created, err)
	}
	retry, created, err := f.service.AdmitGateResponse(context.Background(), f.principal, req)
	if err != nil || created || !reflect.DeepEqual(retry, first) {
		t.Fatalf("retry = (%+v, %v, %v)", retry, created, err)
	}
	f.directory.ok = false
	f.catalog.entry.Record.OpenGates = nil
	if retry, created, err := f.service.AdmitGateResponse(context.Background(), f.principal, req); err != nil || created || !reflect.DeepEqual(retry, first) {
		t.Fatalf("retry after gate closed = (%+v, %v, %v)", retry, created, err)
	}
	f.directory.ok = true
	f.catalog.entry.Record.OpenGates = []sessionwire.GateProjection{{GateID: "gate-a", OpenedEventID: "event-a", OpenedJournalSeq: 7, Deadline: serviceNow.Add(time.Hour), Answerability: sessionwire.GateAnswerabilityResident}}
	req.Values["answer"] = json.RawMessage(`"no"`)
	if _, _, err := f.service.AdmitGateResponse(context.Background(), f.principal, req); err == nil {
		t.Fatal("different answer reused a command ID")
	}
}

func TestLegacyCreateMintsBothIdentities(t *testing.T) {
	f := newServiceFixture(t)
	result, err := f.service.AdmitLegacyCreate(context.Background(), f.principal, LegacyCreateRequest{AgentID: "agent-a"})
	if err != nil {
		t.Fatal(err)
	}
	if result.SessionID == "" || result.Entry.Record.CommandID == "" {
		t.Fatalf("legacy result = %+v", result)
	}
}

// TestADependencyFaultIsNotADecisionAboutTheCommand is the fault/refusal split
// at the layer that has the reader.
//
// Every site below asks a dependency a question with three possible outcomes --
// yes, no, and "could not ask" -- and each one used to fold the third into the
// second. The consequence is not cosmetic and it reaches a browser: an
// *admission.Error is a CLASSIFIED public refusal, so a transient directory
// outage was delivered as runtime_unavailable or gate_not_resumable, which
// every edge renders as a decision about the caller's command. A6.2's
// ClientLink additionally fixes retryable at false for every classified
// refusal, precisely because those codes conflate one transient cause with
// three permanent ones -- so the client was told a permanent "no" for a
// condition that was neither a decision nor permanent, and stopped retrying.
//
// A fault is therefore returned as ITSELF: not an *admission.Error, carrying no
// public code, so an edge answers it from its own fault channel (the ClientLink
// answers centrifuge's temporary internal error; the REST controls A3.3 owns
// will answer through httpapi's existing storeUnavailable/internalFailure
// mapping). No new vocabulary is needed anywhere, which is why the fix belongs
// here rather than behind a shared classification authority.
//
// The negative ANSWER is unchanged and is still a refusal. Both halves are
// driven for every site, because a fix that turned "no" into a fault as well
// would pass a table that only drove the faults.
func TestADependencyFaultIsNotADecisionAboutTheCommand(t *testing.T) {
	boom := errors.New("the directory could not be reached")

	gateRequest := func() sessionwire.GateResponseRequest {
		return sessionwire.GateResponseRequest{
			CommandEnvelope: envelope("gate-a"), SessionID: "session-a", GateID: "gate-a",
			Action: "submit", Values: map[string]json.RawMessage{}, ExpectedOpenEventID: "event-a",
		}
	}
	// residentGate puts the fixture in the state where the gate response's
	// OWNER read is the next thing that happens: the catalog entry exists, its
	// target is known, and the gate is open, resident and version-matched.
	residentGate := func(f *serviceFixture) {
		f.catalog.getErr = nil
		f.catalog.entry.Record = sessionstore.CatalogRecord{
			TenantID: "tenant-a", SessionID: "session-a", AgentID: "agent-a",
			RuntimeCompatibilityID: "runtime-v1", DesiredPlacement: sessionwire.HostPlacementPooled,
			OpenGates: []sessionwire.GateProjection{{
				GateID: "gate-a", OpenedEventID: "event-a", Deadline: serviceNow.Add(time.Hour),
				Answerability: sessionwire.GateAnswerabilityResident,
			}},
		}
	}

	for _, tt := range []struct {
		name string
		// fault configures the dependency to FAIL; answer configures it to say
		// no. Everything else about the fixture is identical.
		fault  func(*serviceFixture)
		answer func(*serviceFixture)
		call   func(*serviceFixture) error
		// code is the refusal the negative ANSWER must still produce.
		code sessionwire.ErrorCode
	}{
		{
			name:   "resolving a create's launch target",
			fault:  func(f *serviceFixture) { f.targets.err = boom },
			answer: func(f *serviceFixture) { f.targets.known = false },
			call: func(f *serviceFixture) error {
				_, _, err := f.service.AdmitCreate(context.Background(), f.principal,
					sessionwire.CreateRequest{CommandEnvelope: envelope("create-a"), SessionID: "session-a", AgentID: "agent-a"})
				return err
			},
			code: sessionwire.ErrorCodeRuntimeUnavailable,
		},
		{
			name:   "resolving a legacy create's launch target",
			fault:  func(f *serviceFixture) { f.targets.err = boom },
			answer: func(f *serviceFixture) { f.targets.known = false },
			call: func(f *serviceFixture) error {
				_, err := f.service.AdmitLegacyCreate(context.Background(), f.principal,
					LegacyCreateRequest{AgentID: "agent-a", Blocks: []byte(`[{"text":"hello"}]`)})
				return err
			},
			code: sessionwire.ErrorCodeRuntimeUnavailable,
		},
		{
			name: "checking an existing session's pinned runtime",
			fault: func(f *serviceFixture) {
				f.catalog.getErr = nil
				f.targets.err = boom
			},
			answer: func(f *serviceFixture) {
				f.catalog.getErr = nil
				f.targets.known = false
			},
			call: func(f *serviceFixture) error {
				_, _, err := f.service.AdmitInterrupt(context.Background(), f.principal,
					sessionwire.InterruptRequest{CommandEnvelope: envelope("interrupt-a"), SessionID: "session-a"})
				return err
			},
			code: sessionwire.ErrorCodeRuntimeUnavailable,
		},
		{
			name: "reading a gate response's owner",
			fault: func(f *serviceFixture) {
				residentGate(f)
				f.directory.err = boom
			},
			answer: func(f *serviceFixture) {
				residentGate(f)
				f.directory.ok = false
			},
			call: func(f *serviceFixture) error {
				_, _, err := f.service.AdmitGateResponse(context.Background(), f.principal, gateRequest())
				return err
			},
			code: sessionwire.ErrorCodeGateNotResumable,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Run("a fault is not classified", func(t *testing.T) {
				f := newServiceFixture(t)
				tt.fault(f)
				err := tt.call(f)
				if err == nil {
					t.Fatal("a dependency fault was admitted")
				}
				if !errors.Is(err, boom) {
					t.Errorf("error %v does not wrap the dependency's own failure", err)
				}
				var classified *Error
				if errors.As(err, &classified) {
					t.Errorf("a fault was answered with the public code %q; an edge will render that as a decision about the command",
						classified.Code)
				}
				if f.commands.calls != 0 || f.catalog.createCalls != 0 {
					t.Errorf("a fault wrote durable state: %d commands, %d catalog entries", f.commands.calls, f.catalog.createCalls)
				}
			})
			// The control. Without it, "a fault is not classified" would also
			// be the output of a service that had stopped classifying at all.
			t.Run("a negative answer is still a refusal", func(t *testing.T) {
				f := newServiceFixture(t)
				tt.answer(f)
				err := tt.call(f)
				if !IsCode(err, tt.code) {
					t.Errorf("error = %v, want the public code %q", err, tt.code)
				}
				if errors.Is(err, boom) {
					t.Error("the refusal carries a dependency failure that was never configured")
				}
			})
		})
	}
}

// TestNoDependencyFaultBecomesAPublicCode is the fault/refusal property as a
// FOR-ALL over the mechanism, replacing a fixed table that grew one row per
// gate.
//
// # How the subject is derived, which is the whole point
//
// The property is service-wide: an error this service did not itself classify
// must never leave carrying one of Core's nine public codes, because an edge
// renders a code as a decision about the caller's command and a decision is
// what a caller stops retrying. A table of named sites cannot defend that -- it
// is a fixed fixture against a "for all X" claim (defect class 10), and two
// rounds of gates found exactly that: the first table named four sites and two
// more of identical shape sat unread in the same two functions, one of them
// telling a retried unknown-outcome command it had been permanently rejected.
//
// So the subject is derived rather than listed. Every dependency this Service
// can call is an interface it declares; the METHOD SETS of those interfaces,
// read by reflection, are every place a dependency error can enter. The sweep
// makes each method fail in turn, on every entry point, and asserts:
//
//	if the failing method was CALLED, the answer carries no public code.
//
// The "was called" guard is what makes it exact rather than approximate: an
// entry point that never reaches the failing dependency legitimately answers
// with a code decided by something else, and folding that in would make the
// sweep assert nothing on most pairs.
//
// # Why the assertion is not "the error wraps the fault"
//
// Because a fold that DROPS the cause would then be invisible, and the fix that
// prompted this test drops the cause on purpose (`refusal(code, nil)`). The
// assertion is over the ANSWER's classification, not over the cause's survival,
// so a site that classified and discarded the failure is caught too.
//
// # Anti-vacuity
//
// A sweep whose faults never reach anything is green and worthless. Every
// derived method must be exercised by at least one entry point, except those
// named in unreachedDependencyMethods with a reason -- so a dependency method
// added later that nothing drives fails HERE and demands a decision, rather
// than joining the set of untested folds this test exists to end.
func TestNoDependencyFaultBecomesAPublicCode(t *testing.T) {
	// The dependency surface, derived from the interfaces the Service declares
	// rather than from a list. A method added to any of them joins the sweep.
	dependencies := map[string]reflect.Type{
		"Authorizer": reflect.TypeOf((*Authorizer)(nil)).Elem(),
		"Targets":    reflect.TypeOf((*TargetResolver)(nil)).Elem(),
		"Catalog":    reflect.TypeOf((*Catalog)(nil)).Elem(),
		"Commands":   reflect.TypeOf((*CommandStore)(nil)).Elem(),
		"Directory":  reflect.TypeOf((*OwnerDirectory)(nil)).Elem(),
		"IDs":        reflect.TypeOf((*UUIDSource)(nil)).Elem(),
	}
	// Clock is deliberately absent: neither of its methods can fail, so there
	// is no fault to inject. That is a property of the interface -- if a Clock
	// method ever returns an error, this comment is wrong and the map above is
	// where it is corrected.
	type site struct{ dependency, method string }
	var sites []site
	for name, typ := range dependencies {
		if typ.NumMethod() == 0 {
			t.Fatalf("%s declares no methods, so sweeping it proves nothing", name)
		}
		for i := range typ.NumMethod() {
			sites = append(sites, site{dependency: name, method: typ.Method(i).Name})
		}
	}
	slices.SortFunc(sites, func(a, b site) int {
		if a.dependency != b.dependency {
			return strings.Compare(a.dependency, b.dependency)
		}
		return strings.Compare(a.method, b.method)
	})
	if len(sites) < 2 {
		t.Fatalf("the derived dependency surface has %d methods; the sweep is vacuous", len(sites))
	}

	// The entry points. Each is configured to reach as deep as the fixture
	// allows, so a dependency read late in the chain is exercised rather than
	// short-circuited by an earlier refusal.
	entries := map[string]func(*serviceFixture) error{
		"AdmitCreate": func(f *serviceFixture) error {
			_, _, err := f.service.AdmitCreate(context.Background(), f.principal,
				sessionwire.CreateRequest{CommandEnvelope: envelope("create-a"), SessionID: "session-a", AgentID: "agent-a"})
			return err
		},
		"AdmitLegacyCreate": func(f *serviceFixture) error {
			_, err := f.service.AdmitLegacyCreate(context.Background(), f.principal,
				LegacyCreateRequest{AgentID: "agent-a", Blocks: []byte(`[{"text":"hello"}]`)})
			return err
		},
		"AdmitInput": func(f *serviceFixture) error {
			_, _, err := f.service.AdmitInput(context.Background(), f.principal,
				sessionwire.InputRequest{CommandEnvelope: envelope("input-a"), SessionID: "session-a", Blocks: []byte(`[{"text":"hello"}]`)})
			return err
		},
		"AdmitInterrupt": func(f *serviceFixture) error {
			_, _, err := f.service.AdmitInterrupt(context.Background(), f.principal,
				sessionwire.InterruptRequest{CommandEnvelope: envelope("interrupt-a"), SessionID: "session-a"})
			return err
		},
		"AdmitRestore": func(f *serviceFixture) error {
			_, _, err := f.service.AdmitRestore(context.Background(), f.principal,
				sessionwire.RestoreRequest{CommandEnvelope: envelope("restore-a"), SessionID: "session-a"})
			return err
		},
		"AdmitGateResponse": func(f *serviceFixture) error {
			_, _, err := f.service.AdmitGateResponse(context.Background(), f.principal,
				sessionwire.GateResponseRequest{
					CommandEnvelope: envelope("gate-a"), SessionID: "session-a", GateID: "gate-a",
					Action: "submit", Values: map[string]json.RawMessage{"answer": json.RawMessage(`"yes"`)},
					ExpectedOpenEventID: "event-a",
				})
			return err
		},
	}

	exercised := map[string]bool{}
	for _, s := range sites {
		for entryName, call := range entries {
			t.Run(s.dependency+"."+s.method+"/"+entryName, func(t *testing.T) {
				f := newServiceFixture(t)
				resolvableSession(f)
				injectFault(f, s.dependency, s.method)

				err := call(f)

				if !dependencyWasCalled(f, s.dependency, s.method) {
					// This entry point does not reach that dependency method.
					// Nothing to assert; the coverage floor below is what makes
					// sure SOME entry point does.
					return
				}
				exercisedMu.Lock()
				exercised[s.dependency+"."+s.method] = true
				exercisedMu.Unlock()

				// Two assertions, and the second is what keeps the sweep
				// from passing vacuously. A broken injector -- one that armed
				// nothing -- would leave every call on its happy path, and
				// "nothing was classified" is also true of a service that was
				// never asked anything. A dependency that failed must produce
				// SOME failure; a swallowed one is its own defect.
				if err == nil {
					t.Fatalf("%s.%s failed and %s succeeded anyway", s.dependency, s.method, entryName)
				}
				var classified *Error
				if errors.As(err, &classified) {
					t.Errorf("%s.%s failed and %s answered with the public code %q; an edge renders that as a decision about the command",
						s.dependency, s.method, entryName, classified.Code)
				}
			})
		}
	}

	// The coverage floor. A method nothing drives is either dead or an untested
	// fold, and both need a human rather than a silent pass.
	for _, s := range sites {
		name := s.dependency + "." + s.method
		if exercised[name] || unreachedDependencyMethods()[name] != "" {
			continue
		}
		t.Errorf("no entry point ever called %s with it failing, so the sweep proves nothing about it: "+
			"either drive it from an entry point or record why it is unreachable", name)
	}
	for name, reason := range unreachedDependencyMethods() {
		if exercised[name] {
			t.Errorf("%s is recorded as unreachable (%q) but the sweep reached it; the record is stale", name, reason)
		}
	}
}

// exercisedMu guards the coverage map, which parallel subtests would otherwise
// race. The subtests here are deliberately NOT parallel, but the mutex costs
// nothing and removes the trap from a later edit.
var exercisedMu sync.Mutex

// unreachedDependencyMethods names the derived methods no admission entry point
// calls, with the reason. An entry here is a claim that must stay true: the
// sweep fails if one of these is ever reached.
func unreachedDependencyMethods() map[string]string {
	return map[string]string{
		"Authorizer.AuthorizeServiceSweep": "the cross-tenant due-work sweep is the reconciler's, never a tenant command's",
	}
}

// resolvableSession puts a fixture in the state where every dependency read on
// every path is reachable: the session exists, its pinned target is configured,
// and its gate is open, resident and version-matched.
func resolvableSession(f *serviceFixture) {
	f.catalog.getErr = nil
	f.catalog.entry.Record = sessionstore.CatalogRecord{
		TenantID: "tenant-a", SessionID: "session-a", AgentID: "agent-a",
		RuntimeCompatibilityID: "runtime-v1", DesiredPlacement: sessionwire.HostPlacementPooled,
		OpenGates: []sessionwire.GateProjection{{
			GateID: "gate-a", OpenedEventID: "event-a", OpenedJournalSeq: 7,
			Deadline: serviceNow.Add(time.Hour), Answerability: sessionwire.GateAnswerabilityResident,
		}},
	}
	f.directory.ok = true
	f.directory.owner = sessionwire.HostLinkRegistryObservation{
		TenantID: "tenant-a", SessionID: "session-a", AgentID: "agent-a",
		RuntimeCompatibilityID: "runtime-v1", Placement: sessionwire.HostPlacementPooled,
		Residency: sessionwire.SessionResidencyResident, Accepting: true, ExpiresAt: serviceNow.Add(time.Minute),
	}
}

// injectFault arms one dependency method to fail.
func injectFault(f *serviceFixture, dependency, method string) {
	switch dependency {
	case "Authorizer":
		f.auth.failing = method
	case "Targets":
		f.targets.failing = method
	case "Catalog":
		f.catalog.failing = method
	case "Commands":
		f.commands.failing = method
	case "Directory":
		f.directory.failing = method
	case "IDs":
		f.ids.failing = method
	default:
		panic("no fake for dependency " + dependency)
	}
}

// dependencyWasCalled reports whether the armed method was reached.
func dependencyWasCalled(f *serviceFixture, dependency, method string) bool {
	switch dependency {
	case "Authorizer":
		return f.auth.called[method]
	case "Targets":
		return f.targets.called[method]
	case "Catalog":
		return f.catalog.called[method]
	case "Commands":
		return f.commands.called[method]
	case "Directory":
		return f.directory.called[method]
	case "IDs":
		return f.ids.called[method]
	default:
		panic("no fake for dependency " + dependency)
	}
}
