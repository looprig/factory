package admission

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
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
// # Both axes are derived, and neither from the thing they probe
//
// The first version derived METHODS from interfaces and took the INTERFACES
// from a hand-written map of six. That is one axis short, and it was measured:
// deleting a dependency from the map shrank the sweep and the module stayed
// green, and a seventh dependency or a seventh entry point joined the service
// without joining the sweep. The claim "a dependency added later cannot join the
// set of untested folds" was therefore false of everything except a method on an
// interface somebody had already listed.
//
// Both axes now come from production types:
//
//	dependencies   the INTERFACE-KIND FIELDS of admission.Config
//	fault sites    the methods of those interfaces whose last result is an error
//	entry points   the exported methods of *Service
//
// and both are held by SET EQUALITY, so a seventh of either fails until it is
// driven or recorded with a reason. Clock drops out of the sweep by the
// MECHANISM rather than by a note: neither of its methods can return an error,
// so it contributes no fault site.
//
// The universe may not be computed from the thing it probes. Config and *Service
// are the production declarations the service is BUILT from, not values it
// indexes at run time, so shrinking either is a compile error in service.go
// rather than a quieter test -- which is the trap sessionstore's own derivation
// (commit 7f9f598) had to avoid and names explicitly.
//
// # Anti-vacuity
//
// A sweep whose faults never reach anything is green and worthless. Every
// derived method must be exercised by at least one entry point, except those
// named in unreachedDependencyMethods with a reason; and every fault must
// produce SOME failure, or an injector that armed nothing would leave every
// call on its happy path with "nothing was classified" trivially true.
func TestNoDependencyFaultBecomesAPublicCode(t *testing.T) {
	sites := faultSites(t)
	if len(sites) < 2 {
		t.Fatalf("the derived dependency surface has %d fallible methods; the sweep is vacuous", len(sites))
	}
	entries := admissionEntryPoints(t)

	exercised := map[string]bool{}
	for _, s := range sites {
		for entryName, call := range entries {
			t.Run(s.dependency+"."+s.method+"/"+entryName, func(t *testing.T) {
				f := newServiceFixture(t)
				resolvableSession(f)
				injector, armable := armFault(f, s.dependency, s.method)
				if !armable {
					t.Fatalf("no fake can arm %s", s.dependency)
				}

				err := call(f)

				if !injector.called[s.method] {
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
				// The failure must be THIS failure. "Something went wrong" is a
				// much weaker claim than "the injected fault surfaced", and the
				// gap between them is where a service that answered its own
				// unrelated refusal would hide -- the sweep would then be
				// asserting that some other error is unclassified while the
				// dependency's own was folded and discarded.
				//
				// The one shape that legitimately does not wrap it is a fold
				// that DROPS the cause, which is precisely what the
				// classification assertion below catches, so the two are
				// reported apart rather than as one condition.
				var classifiedCause *Error
				if !errors.Is(err, errInjectedFault) && !errors.As(err, &classifiedCause) {
					t.Errorf("%s.%s failed and %s answered %v, which neither wraps the injected fault nor classifies it; "+
						"the dependency's failure was replaced by something else", s.dependency, s.method, entryName, err)
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
	// The record is checked in BOTH directions, and the second one is the method
	// axis's only pin. Config's FIELDS are compile-pinned -- deleting one stops
	// service.go compiling -- but an interface's METHOD SET is not: deleting
	// AuthorizeServiceSweep from Authorizer compiles everywhere and silently
	// shrank this sweep from 60 pairs to 54, which a gate measured. Nothing in
	// production reads that method, so nothing else would have said so.
	derived := map[string]bool{}
	for _, s := range sites {
		derived[s.dependency+"."+s.method] = true
	}
	for name, reason := range unreachedDependencyMethods() {
		if exercised[name] {
			t.Errorf("%s is recorded as unreachable (%q) but the sweep reached it; the record is stale", name, reason)
		}
		if !derived[name] {
			t.Errorf("%s is recorded as unreachable (%q) but is no longer a fallible method of any dependency Config declares; "+
				"either the interface lost a method and the sweep silently shrank, or the record has outlived its subject", name, reason)
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

// armFault arms one dependency method to fail and returns that fake's injector,
// so the caller can ask afterwards whether the method was reached.
//
// The boolean is "this fixture has a fake for that dependency", and it is a
// RESULT rather than a panic because faultSites consults it while deriving:
// a dependency added to Config that the fixture cannot arm must be a named test
// failure, not a crash in a helper.
//
// It is deliberately the same lookup for both questions. Arming and asking are
// two views of one fake, so an injector that armed one dependency and reported
// another's calls is not expressible.
func armFault(f *serviceFixture, dependency, method string) (*faultInjector, bool) {
	var injector *faultInjector
	switch dependency {
	case "Authorizer":
		injector = &f.auth.faultInjector
	case "Targets":
		injector = &f.targets.faultInjector
	case "Catalog":
		injector = &f.catalog.faultInjector
	case "Commands":
		injector = &f.commands.faultInjector
	case "Directory":
		injector = &f.directory.faultInjector
	case "IDs":
		injector = &f.ids.faultInjector
	default:
		return nil, false
	}
	injector.failing = method
	return injector, true
}

// ---------------------------------------------------------------------------
// The derivation. Both axes, and the reader that stops the ratchet.
// ---------------------------------------------------------------------------

// faultSite is one place a dependency error can enter the service: a fallible
// method on one of the interfaces Config declares.
type faultSite struct{ dependency, method string }

// admissionDependencies derives the dependency surface from Config's own
// INTERFACE-KIND FIELDS.
//
// Config is the authority because it is what the service is constructed from
// and what service.go dereferences field by field; a dependency removed from it
// does not shrink this sweep quietly, it stops the production file compiling.
// A test whose universe came from the same map its fixture indexes could be
// narrowed by narrowing that map, which is the failure mode this replaces.
func admissionDependencies(t *testing.T) map[string]reflect.Type {
	t.Helper()

	cfg := reflect.TypeOf(Config{})
	out := map[string]reflect.Type{}
	for i := range cfg.NumField() {
		field := cfg.Field(i)
		if field.Type.Kind() == reflect.Interface {
			out[field.Name] = field.Type
		}
	}
	if len(out) == 0 {
		t.Fatal("Config declares no interface dependencies; the derivation is broken")
	}
	return out
}

// faultSites is the cross-product's first axis: every fallible method of every
// dependency.
//
// "Fallible" is read off the SIGNATURE -- a method whose last result is an error
// -- so a dependency that cannot fail contributes nothing and needs no note
// exempting it. It also means a method that GAINS an error result joins the
// sweep on the same day it becomes able to fail.
func faultSites(t *testing.T) []faultSite {
	t.Helper()

	errorType := reflect.TypeOf((*error)(nil)).Elem()
	// A real fixture, because "can this be armed" is a question about the
	// fakes, and asking it of a zero value would answer about nil pointers.
	probe := newServiceFixture(t)
	var sites []faultSite
	for name, typ := range admissionDependencies(t) {
		if typ.NumMethod() == 0 {
			t.Errorf("%s declares no methods, so sweeping it proves nothing", name)
		}
		fallible := 0
		for i := range typ.NumMethod() {
			method := typ.Method(i)
			out := method.Type.NumOut()
			if out == 0 || method.Type.Out(out-1) != errorType {
				continue
			}
			fallible++
			sites = append(sites, faultSite{dependency: name, method: method.Name})
		}
		if fallible == 0 {
			// A dependency that cannot fail contributes no site and needs no
			// fake. Clock is the one, and it leaves the sweep by this arm
			// rather than by a note somebody has to keep true.
			continue
		}
		// The fixture must be able to arm every dependency that CAN fail. A
		// seventh interface on Config fails HERE, naming itself, rather than
		// silently sitting outside the sweep.
		if _, armable := armFault(probe, name, ""); !armable {
			t.Errorf("Config declares the fallible dependency %s, which this fixture cannot arm; "+
				"a fault in it would be outside the sweep", name)
		}
	}
	slices.SortFunc(sites, func(a, b faultSite) int {
		if a.dependency != b.dependency {
			return strings.Compare(a.dependency, b.dependency)
		}
		return strings.Compare(a.method, b.method)
	})
	return sites
}

// nonEntryPointServiceMethods names exported Service methods that are not
// command entry points, with the reason. An entry is a claim: the check below
// fails if one of these is ever driven, so a stale record cannot survive.
func nonEntryPointServiceMethods() map[string]string {
	return map[string]string{}
}

// admissionEntryPoints is the cross-product's second axis, held to *Service's
// exported method set by SET EQUALITY in both directions.
//
// Containment would not do. A seventh entry point added to the service is
// exactly the case the first version of this sweep missed: it joined the public
// surface without joining the sweep, and nothing said so.
func admissionEntryPoints(t *testing.T) map[string]func(*serviceFixture) error {
	t.Helper()

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

	service := reflect.TypeOf(&Service{})
	declared := map[string]bool{}
	for i := range service.NumMethod() {
		declared[service.Method(i).Name] = true
	}
	if len(declared) == 0 {
		t.Fatal("*Service declares no exported methods; the derivation is broken")
	}
	excluded := nonEntryPointServiceMethods()
	for name := range declared {
		if entries[name] == nil && excluded[name] == "" {
			t.Errorf("*Service declares %s, which no entry point in this sweep drives: "+
				"drive it, or record why it is not a command entry point", name)
		}
	}
	for name := range entries {
		if !declared[name] {
			t.Errorf("this sweep drives %s, which *Service no longer declares; the driver has outlived its site", name)
		}
		if reason := excluded[name]; reason != "" {
			t.Errorf("%s is recorded as a non-entry-point (%q) and is driven anyway; the record is stale", name, reason)
		}
	}
	return entries
}

// TestEveryClassifiedRefusalIsReachableFromADrivenEntryPoint is the third layer,
// and it is the one that stops the ratchet.
//
// The two reflective axes above defend today's surface: a dependency or an entry
// point added to a production TYPE fails until it is driven. Neither can see a
// classified refusal minted inside a function that no driven entry point
// reaches, because a call site is not reflectable -- which is the same wall
// sessionstore hit on InboxState.terminal() and closed with go/parser at commit
// 7f9f598.
//
// So this parses the package's production sources, builds the intra-package call
// graph by name, and requires:
//
//	every function that calls refusal() is reachable from a driven entry point
//	every driven entry point exists as a production function
//
// refusal() is the ONE constructor of a classified public code -- an *Error is
// minted nowhere else -- so its call sites are exactly the places the property
// can be broken. A site nobody drives is either dead code or an untested fold,
// and both want a human.
//
// # The unit of analysis, stated so the claim cannot be wider than it
//
// A node is a function DECLARATION or a function LITERAL, in a non-test .go
// file directly in this package's directory. That is what the guard keys on and
// therefore the boundary of what it can see:
//
//   - Another PACKAGE cannot host one of these sites: refusal is unexported, so
//     nothing outside this directory can call it. There are no subdirectories,
//     and one would be a different package. This is not a file-name prefix rule
//     -- every non-test .go file here is scanned -- so a site cannot hide by
//     being in a file somebody did not think to name.
//   - A literal is its own node with no incoming edge, so a refusal minted
//     inside a closure is reported unreached rather than inheriting the
//     reachability of the function that declares it.
//   - A call this scan cannot attribute is not an edge, so its target looks
//     unreached and fails. That is the direction the whole construction has to
//     err in, and packageCallGraph derives it rather than asserting it.
//
// What remains: a local variable of func type whose name shadows a package
// function would produce a spurious identifier edge. That one is stated rather
// than closed.
func TestEveryClassifiedRefusalIsReachableFromADrivenEntryPoint(t *testing.T) {
	t.Parallel()

	callers, callees, duplicates, err := packageCallGraph(".")
	if err != nil {
		t.Fatalf("parse the admission sources: %v", err)
	}
	if len(duplicates) != 0 {
		t.Fatalf("these names are declared more than once in this package: %v. "+
			"The call graph is keyed by name, so an edge to one of them reaches both; "+
			"the guard cannot distinguish them and must not pretend to", duplicates)
	}
	if len(callees) == 0 {
		t.Fatal("vacuous: the scanner found no functions at all")
	}
	minting := callers["refusal"]
	if len(minting) == 0 {
		t.Fatal("vacuous: no production function calls refusal(), so this proves nothing")
	}

	driven := admissionEntryPoints(t)
	reached := map[string]bool{}
	var walk func(string)
	walk = func(fn string) {
		if reached[fn] {
			return
		}
		reached[fn] = true
		for _, callee := range callees[fn] {
			if _, declared := callees[callee]; declared {
				walk(callee)
			}
		}
	}
	for name := range driven {
		if _, declared := callees[name]; !declared {
			t.Errorf("this sweep drives %s, which is not a function in these sources", name)
			continue
		}
		walk(name)
	}

	for _, fn := range minting {
		if !reached[fn] {
			t.Errorf("%s mints a classified public code and is not reachable from any entry point the fault sweep drives; "+
				"either drive the path that reaches it or establish that it is dead", fn)
		}
	}
}

// packageCallGraph parses root's production files and reports, for every
// declared function, the names it calls -- and, inverted, the functions that
// call each name.
//
// # Which calls become edges, and why the first version was wrong
//
// An edge is recorded for exactly two call shapes:
//
//	f(...)     a bare identifier
//	s.f(...)   a selector whose receiver is the ENCLOSING function's own
//	           receiver identifier
//
// and for nothing else. The first version recorded the selector's final
// identifier for EVERY selector call, which made `req.Validate()` -- a Core
// method every entry point calls -- an edge to any package function named
// Validate. Both gates built the same counterexample from that: an orphaned
// `func Validate(...) error` minting refusal(), called by nobody, was "reached"
// and passed, while the byte-identical body under a unique name failed. I
// reproduced the pair before changing anything, one identifier apart, opposite
// outcomes. So `reached` GRANTED coverage while the test's own comment claimed
// it demanded it, and the collision surface was every method name in Core and
// SessionStore: Validate, Error, Unwrap, Tenant.
//
// The receiver rule is what attributes a selector call to a function THIS
// package declares. `s.admit(...)` inside a method with receiver `s` is a call
// to a method of this package's own type; `req.Validate()` and
// `s.cfg.Catalog.GetCatalogEntry(...)` are not, and are dropped -- the second
// because its receiver expression is a selector, not the receiver identifier.
//
// # Which way it errs, derived rather than asserted
//
// It UNDER-approximates, which is the direction that demands coverage: a call
// this scan cannot attribute makes its target look unreached and fails, rather
// than quietly making a site look covered. What it drops is a call to a package
// function reached through some other value -- a func stored in a field, a
// method called on a variable of this package's type that is not the receiver.
// Neither exists here today, and if one appears the guard reports the site it
// can no longer see instead of passing.
//
// One residual over-approximation is unavoidable in a name-keyed graph and is
// therefore CHECKED rather than argued: two declarations sharing a name would
// be one node, so an edge to either would reach both. duplicates reports them
// and the caller fails on a non-empty result. A local variable of func type
// shadowing a package function's name would also produce a spurious identifier
// edge; that is the one residual left standing, and it is stated rather than
// claimed away.
func packageCallGraph(root string) (callers, callees map[string][]string, duplicates []string, err error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, nil, nil, err
	}
	callers, callees = map[string][]string{}, map[string][]string{}
	declared := map[string]int{}
	fset := token.NewFileSet()
	var record func(string, ast.Node)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, parseErr := parser.ParseFile(fset, filepath.Join(root, name), nil, 0)
		if parseErr != nil {
			return nil, nil, nil, parseErr
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			declared[fn.Name.Name]++
			if _, seen := callees[fn.Name.Name]; !seen {
				callees[fn.Name.Name] = nil
			}
			// The receiver's identifier, when this declaration has one. A
			// method declared with no receiver NAME -- func (*Service) f() --
			// cannot make an attributable selector call, and an empty string
			// here matches no identifier.
			receiver := ""
			if fn.Recv != nil && len(fn.Recv.List) == 1 && len(fn.Recv.List[0].Names) == 1 {
				receiver = fn.Recv.List[0].Names[0].Name
			}
			record = func(from string, body ast.Node) {
				ast.Inspect(body, func(n ast.Node) bool {
					// A function literal is its OWN node, so its calls are not
					// attributed to the declaration that encloses it. That is
					// escape (C), reported from the sessionstore lane's audit of
					// the same construction: a call nested inside an
					// already-reached function inherits its reachability, and a
					// closure that is declared but never invoked would inherit
					// coverage it does not have. A literal has no incoming edge
					// here at all, so a refusal minted inside one is reported
					// unreached and has to be argued for. There are none today.
					if lit, isLit := n.(*ast.FuncLit); isLit && n != body {
						name := from + "@literal:" + strconv.Itoa(fset.Position(lit.Pos()).Line)
						if _, seen := callees[name]; !seen {
							callees[name] = nil
						}
						record(name, lit.Body)
						return false
					}
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					var called string
					switch fun := call.Fun.(type) {
					case *ast.Ident:
						called = fun.Name
					case *ast.SelectorExpr:
						ident, isIdent := fun.X.(*ast.Ident)
						if !isIdent || receiver == "" || ident.Name != receiver {
							return true
						}
						called = fun.Sel.Name
					default:
						return true
					}
					if !slices.Contains(callees[from], called) {
						callees[from] = append(callees[from], called)
					}
					if !slices.Contains(callers[called], from) {
						callers[called] = append(callers[called], from)
					}
					return true
				})
			}
			record(fn.Name.Name, fn.Body)
		}
	}
	for name, count := range declared {
		if count > 1 {
			duplicates = append(duplicates, name)
		}
	}
	slices.Sort(duplicates)
	return callers, callees, duplicates, nil
}
