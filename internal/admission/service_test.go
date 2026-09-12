package admission

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/build"
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
	// lastGet and lastCreate retain the REQUESTS, not just the call, because
	// the tenant a durable read is scoped by is carried in the request and a
	// counter cannot see it.
	lastGet    sessionstore.GetCatalogEntryRequest
	lastCreate sessionstore.CreateCatalogEntryRequest
	// createReturn, when set, is the entry CreateCatalogEntry answers with
	// instead of echoing the request: the durable "somebody else already owns
	// this session id" outcome the echoing fake cannot produce.
	createReturn *sessionstore.CatalogEntry
}

func (c *serviceCatalog) GetCatalogEntry(_ context.Context, req sessionstore.GetCatalogEntryRequest) (sessionstore.CatalogEntry, error) {
	c.lastGet = req
	if err := c.enter("GetCatalogEntry"); err != nil {
		return sessionstore.CatalogEntry{}, err
	}
	return c.entry, c.getErr
}

func (c *serviceCatalog) CreateCatalogEntry(_ context.Context, req sessionstore.CreateCatalogEntryRequest) (sessionstore.CatalogEntry, bool, error) {
	c.createCalls++
	c.lastCreate = req
	if err := c.enter("CreateCatalogEntry"); err != nil {
		return sessionstore.CatalogEntry{}, false, err
	}
	if c.createReturn != nil {
		c.entry = *c.createReturn
		c.getErr = nil
		return c.entry, false, nil
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
	// lastAdmit and lastGet retain the REQUESTS for the same reason the
	// catalog fake does: tenant scoping lives in the request.
	lastAdmit sessionstore.AdmitCommandRequest
	lastGet   sessionstore.GetCommandRequest
	// notFound, when set, is the error a miss answers with. The released
	// store has TWO not-found spellings -- an InboxError and the keyspace's
	// binding-not-found -- and a fake that could only produce one would leave
	// the other arm of commandNotFound unreadable.
	notFound error
}

func (c *serviceCommands) GetCommand(_ context.Context, req sessionstore.GetCommandRequest) (sessionstore.InboxEntry, error) {
	c.lastGet = req
	if err := c.enter("GetCommand"); err != nil {
		return sessionstore.InboxEntry{}, err
	}
	entry, ok := c.records[req.CommandID]
	if !ok {
		if c.notFound != nil {
			return sessionstore.InboxEntry{}, c.notFound
		}
		return sessionstore.InboxEntry{}, &sessionstore.InboxError{Code: sessionstore.InboxErrorNotFound}
	}
	return entry, nil
}

func (c *serviceCommands) AdmitCommand(_ context.Context, req sessionstore.AdmitCommandRequest) (sessionstore.InboxEntry, bool, error) {
	c.calls++
	c.lastAdmit = req
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
	// lastTenant and lastSession retain the ARGUMENTS, because the tenant an
	// ownership question is asked about is an argument here rather than a
	// request field.
	lastTenant  sessionwire.TenantID
	lastSession sessionwire.SessionID
}

func (d *serviceDirectory) Owner(_ context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) (sessionwire.HostLinkRegistryObservation, bool, error) {
	d.calls++
	d.lastTenant, d.lastSession = tenant, session
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
// are production declarations, not values the service indexes at run time, so
// neither axis can be narrowed by narrowing this file -- which is the trap
// sessionstore's own derivation (commit 7f9f598) had to avoid and names.
//
// What PINS each axis is different, and the sentence that stood here said one
// thing about both. Measured, on this tree:
//
//	a Config FIELD          deleting it stops service.go compiling, because the
//	                        service dereferences s.cfg.Directory and friends.
//	                        `go build ./...` fails. Nothing else is needed.
//	an interface METHOD     deleting it compiles everywhere and shrank this
//	                        sweep 60 -> 54, green. Pinned only by the
//	                        bidirectional record check below.
//	a *Service METHOD       deleting AdmitInterrupt leaves `go build ./...` at
//	                        EXIT 0 -- I ran it. The only breakage is this test
//	                        file failing to compile, which is a real failure but
//	                        is not what "a compile error in service.go" claims,
//	                        and it is the test file that would have to be edited
//	                        to make it go away. The set-equality check on the
//	                        entry points is what makes that edit visible.
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
// graph over the files the toolchain builds, and requires:
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

	callers, callees, unattributable, duplicates, err := packageCallGraph(".")
	if err != nil {
		t.Fatalf("parse the admission sources: %v", err)
	}
	if len(duplicates) != 0 {
		t.Fatalf("these QUALIFIED names are declared more than once in the files the toolchain builds: %v. "+
			"Two declarations of one node merge their callers, so an edge to either reaches both", duplicates)
	}
	// A call that LOOKS like a receiver call and is not a method this package
	// declares on that receiver's type is a hard failure rather than a dropped
	// edge, because the two shapes that produce it are exactly the two escapes
	// this guard was found not to close: a shadowed receiver identifier, and a
	// func-typed field. Neither can be told from a method call syntactically,
	// so the analysis reports that it cannot see rather than guessing.
	if len(unattributable) != 0 {
		t.Fatalf("this package makes %d declaration(s) or call(s) the reachability analysis cannot attribute:\n\t%s\n"+
			"The producers are: a receiver identifier re-declared in its own body; a func-typed FIELD called "+
			"through the receiver; a method PROMOTED from an embedded type, which this package does not declare; "+
			"and a receiver whose type cannot be named, such as a generic one. None is distinguishable from an "+
			"ordinary method call without types, so the analysis fails here rather than granting coverage it has "+
			"not established", len(unattributable), strings.Join(unattributable, "\n\t"))
	}
	if len(callees) == 0 {
		t.Fatal("vacuous: the scanner found no functions at all")
	}
	minting := callers["refusal"]
	if len(minting) == 0 {
		t.Fatal("vacuous: no production function calls refusal(), so this proves nothing")
	}

	// The driven set is qualified the way the graph is: these are methods on
	// *Service, so their nodes are Service.AdmitX.
	driven := map[string]bool{}
	for name := range admissionEntryPoints(t) {
		driven["Service."+name] = true
	}
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
// declared function, the QUALIFIED names it calls -- and, inverted, the
// functions that call each name -- together with every call it could not
// attribute.
//
// # The unit of analysis, as tested rather than as intended
//
// The subject is the set of files build.Context.MatchFile selects for THIS
// GOOS/GOARCH -- the same decision the compiler makes -- minus _test.go. A node
// within them is one of:
//
//	funcName            a package-level function
//	Type.MethodName     a method, keyed by its receiver TYPE
//	Node@literal:LINE   a function literal, its own node with no incoming edge
//
// An edge is a bare identifier call to a function this package declares, or a
// selector call on the enclosing declaration's receiver identifier whose
// selector is a method this package declares on that receiver's type. Every
// other call shape produces NO edge, so its target looks unreached and fails.
//
// Four constructions defeat qualification, and each is a HARD FAILURE naming
// itself rather than a silent drop, because none is distinguishable from an
// ordinary method call without type information:
//
//	a re-declared receiver identifier   `s := &Error{}; s.orphanShadow()`
//	a func-typed field on the receiver  `s.hook()`
//	a promoted method                   declared on an embedded type
//	a receiver that cannot be named     a generic `Box[T]`
//
// The first and last were found by a gate AFTER the previous round claimed the
// boundary was closed, and both were reproduced here before being fixed.
//
// # What it does not cover, measured
//
//   - Code the toolchain does not build for this platform is OUTSIDE the graph.
//     A constrained file's refusal site is not audited by this run, and a run on
//     another GOOS would audit a different set. What IS checked across all
//     files, built or not, is duplicate qualified declarations: a
//     service_windows.go redeclaring a *Service method cannot exist inside one
//     build, so the only axis it can appear on is between files the build
//     selects between -- and merging those was how a dead orphan was made
//     reachable.
//   - A call through any other value -- a func in a map, an interface method on
//     a field -- is not an edge, and fails rather than passing.
//   - Another package cannot host a site: refusal is unexported and this
//     directory has no subdirectories.
func packageCallGraph(root string) (callers, callees map[string][]string, unattributable, duplicates []string, err error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	var files, allFiles []*ast.File
	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		// The toolchain's own file selection, not just the .go suffix.
		// go/parser applies NO build constraints and no GOOS/GOARCH filename
		// rule, so service_windows.go was parsed on darwin and its
		// declarations merged with the real ones -- measured: a *Service
		// method there sharing a qualified name with a reached method made a
		// DEAD orphan reachable, with `go build ./...` green because the file
		// is not compiled at all. build.Context.MatchFile is the same decision
		// the compiler makes, so the graph now describes the program that is
		// actually built.
		matched, matchErr := build.Default.MatchFile(root, name)
		if matchErr != nil {
			return nil, nil, nil, nil, matchErr
		}
		file, parseErr := parser.ParseFile(fset, filepath.Join(root, name), nil, 0)
		if parseErr != nil {
			return nil, nil, nil, nil, parseErr
		}
		// Two sets, deliberately. The GRAPH is the program the toolchain
		// builds. The DUPLICATE census is every file, because a duplicate
		// qualified name cannot exist within one build -- Go rejects it -- so
		// the only axis on which one can appear is across files the build
		// selects between. That is exactly the shape a gate found: a
		// service_windows.go declaring a *Service method that already exists,
		// merged into the graph by a parser that applies no constraints.
		// Reporting it is worth doing in its own right: two platforms with
		// different bodies for one method means coverage established on this
		// one says nothing about the other.
		if matched {
			files = append(files, file)
		}
		allFiles = append(allFiles, file)
	}

	// First pass: what this package declares. The method set per receiver type
	// is what the second pass attributes against.
	freeFunctions := map[string]bool{}
	methodsOf := map[string]map[string]bool{}
	declaredAt := map[string]int{}
	for _, file := range allFiles {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			typeName, _, kind := receiverOf(fn)
			if kind == receiverNamed {
				declaredAt[typeName+"."+fn.Name.Name]++
			} else if kind == receiverNone {
				declaredAt[fn.Name.Name]++
			}
		}
	}
	for _, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			typeName, _, kind := receiverOf(fn)
			switch kind {
			case receiverNone:
				freeFunctions[fn.Name.Name] = true
			case receiverNamed:
				if methodsOf[typeName] == nil {
					methodsOf[typeName] = map[string]bool{}
				}
				methodsOf[typeName][fn.Name.Name] = true
			case receiverUnqualifiable:
				// A GENERIC receiver -- Box[T] is an IndexExpr, not an Ident.
				// The previous version reported isMethod false, so the method
				// landed among the free functions under its BARE name and
				// became one node with any free function of that name: a dead
				// (*probeBox[T]).orphanProbe was "reached" through a live
				// orphanProbe(). Failing hard is the cheaper of the two fixes
				// and matches the unattributable precedent -- the analysis says
				// it cannot key this rather than keying it wrongly.
				unattributable = append(unattributable, fmt.Sprintf(
					"%s at %s has a receiver this scan cannot qualify by type, so its node would collide with any function of the same bare name",
					fn.Name.Name, fset.Position(fn.Pos())))
			}
		}
	}
	// Restored on the QUALIFIED key. It was deleted last round on the premise
	// that qualification made it unable to fire; the premise was false and a
	// gate fired it twice. Qualification removes the free-function/method
	// collision, and it does NOT remove two declarations of the same qualified
	// name -- which is what a file the toolchain skips, or an unqualifiable
	// receiver, produces.
	for name, count := range declaredAt {
		if count > 1 {
			duplicates = append(duplicates, name)
		}
	}
	slices.Sort(duplicates)

	callers, callees = map[string][]string{}, map[string][]string{}
	var record func(from string, body ast.Node, recvType, recvName string)
	record = func(from string, body ast.Node, recvType, recvName string) {
		if _, seen := callees[from]; !seen {
			callees[from] = nil
		}
		ast.Inspect(body, func(n ast.Node) bool {
			if lit, isLit := n.(*ast.FuncLit); isLit && n != body {
				record(from+"@literal:"+strconv.Itoa(fset.Position(lit.Pos()).Line), lit.Body, recvType, recvName)
				return false
			}
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			var called string
			switch fun := call.Fun.(type) {
			case *ast.Ident:
				if !freeFunctions[fun.Name] {
					return true
				}
				called = fun.Name
			case *ast.SelectorExpr:
				ident, isIdent := fun.X.(*ast.Ident)
				if !isIdent || recvName == "" || ident.Name != recvName {
					return true
				}
				if !methodsOf[recvType][fun.Sel.Name] {
					unattributable = append(unattributable, fmt.Sprintf("%s calls %s.%s at %s, which is not a method this package declares on %s",
						from, ident.Name, fun.Sel.Name, fset.Position(call.Pos()), recvType))
					return true
				}
				called = recvType + "." + fun.Sel.Name
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

	for _, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			typeName, recvName, kind := receiverOf(fn)
			key := fn.Name.Name
			if kind == receiverNamed {
				key = typeName + "." + fn.Name.Name
			}
			// A receiver identifier re-declared anywhere in the body makes
			// every selector call on that identifier ambiguous to a scan with
			// no types. The type rule alone narrowed this rather than closing
			// it: a shadow whose selector the ENCLOSING receiver's type also
			// declares -- `s := &Error{}; s.orphanShadow()` inside a *Service
			// method, where both types declare orphanShadow -- booked an edge
			// to the dead Service.orphanShadow. Measured, and green.
			if recvName != "" && redeclares(fn.Body, recvName) {
				unattributable = append(unattributable, fmt.Sprintf(
					"%s at %s re-declares its receiver identifier %q, so no selector call on it can be attributed",
					key, fset.Position(fn.Pos()), recvName))
				recvName = ""
			}
			record(key, fn.Body, typeName, recvName)
		}
	}
	slices.Sort(unattributable)
	return callers, callees, unattributable, duplicates, nil
}

// receiverKind is what receiverOf could establish about a declaration.
type receiverKind int

const (
	// receiverNone is a package-level function.
	receiverNone receiverKind = iota
	// receiverNamed is a method whose receiver type this scan can name.
	receiverNamed
	// receiverUnqualifiable is a method whose receiver type it cannot -- a
	// generic receiver, today. It is NOT folded into receiverNone: doing that
	// is what put a generic method among the free functions under its bare
	// name and merged it with an unrelated node.
	receiverUnqualifiable
)

// receiverOf reports a declaration's receiver TYPE and the identifier it is
// spelled with. A method declared with no receiver name -- func (*Service) f()
// -- yields an empty name, which matches no identifier, so it can make no
// attributable selector call.
func receiverOf(fn *ast.FuncDecl) (typeName, name string, kind receiverKind) {
	if fn.Recv == nil || len(fn.Recv.List) != 1 {
		return "", "", receiverNone
	}
	field := fn.Recv.List[0]
	if len(field.Names) == 1 {
		name = field.Names[0].Name
	}
	expr := field.Type
	if star, isStar := expr.(*ast.StarExpr); isStar {
		expr = star.X
	}
	ident, isIdent := expr.(*ast.Ident)
	if !isIdent {
		return "", name, receiverUnqualifiable
	}
	return ident.Name, name, receiverNamed
}

// redeclares reports whether body introduces a new binding for name, in any
// scope. It is deliberately scope-blind: a scan with no types cannot tell which
// binding a later selector call refers to, so ANY re-declaration makes every
// such call unattributable in that declaration.
func redeclares(body *ast.BlockStmt, name string) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		switch decl := n.(type) {
		case *ast.AssignStmt:
			if decl.Tok != token.DEFINE {
				return true
			}
			for _, lhs := range decl.Lhs {
				if ident, ok := lhs.(*ast.Ident); ok && ident.Name == name {
					found = true
				}
			}
		case *ast.ValueSpec:
			for _, ident := range decl.Names {
				if ident.Name == name {
					found = true
				}
			}
		case *ast.RangeStmt:
			for _, expr := range []ast.Expr{decl.Key, decl.Value} {
				if ident, ok := expr.(*ast.Ident); ok && ident.Name == name {
					found = true
				}
			}
		case *ast.FuncLit:
			if decl.Type.Params == nil {
				return true
			}
			for _, param := range decl.Type.Params.List {
				for _, ident := range param.Names {
					if ident.Name == name {
						found = true
					}
				}
			}
		}
		return !found
	})
	return found
}

// ---------------------------------------------------------------------------
// A3.1 completion. Each test below was written against a SURVIVING mutant of
// the production line it names: the line could be deleted or weakened and the
// pre-existing suite stayed green. The mutant is named in the comment so the
// claim is checkable rather than asserted.
// ---------------------------------------------------------------------------

// gateFixture is the fully resumable gate-response starting point: an open,
// resident, version-matched projection and a fresh owner that matches the
// record in every member. Every test below moves ONE thing away from it.
func gateFixture(t *testing.T) (*serviceFixture, sessionwire.GateResponseRequest) {
	t.Helper()
	f := newServiceFixture(t)
	resolvableSession(f)
	return f, sessionwire.GateResponseRequest{
		CommandEnvelope: envelope("gate-command"), SessionID: "session-a", GateID: "gate-a",
		Action: "submit", Values: map[string]json.RawMessage{"answer": json.RawMessage(`"yes"`)},
		ExpectedOpenEventID: "event-a",
	}
}

// TestAProjectionThatIsNotResidentIsNotAnswerable reads the ANSWERABILITY
// member of the durable projection, which nothing read.
//
// Mutant: deleting the `gate.Answerability != GateAnswerabilityResident` arm
// of gateAdmission left the whole module green. The existing table's "cold",
// "releasing", "not accepting" and "expired" rows all move the DIRECTORY's
// observation, so they exercise freshMatchingOwner and never the projection --
// two independent halves of runbook step 4's "matching durable public gate
// projection PLUS a fresh resident/accepting owner", of which only one was read.
//
// The rows are derived from Core's declared answerability values rather than
// listed, so a value added to the vocabulary joins this test on the day it is
// declared instead of defaulting to answerable.
func TestAProjectionThatIsNotResidentIsNotAnswerable(t *testing.T) {
	// The five values the pinned Core declares. THIS IS A LIST, NOT A
	// CLOSURE: Core exports no enumeration and its validity predicate is
	// unexported, so a SIXTH value added to the vocabulary joins the wire
	// without joining this table. What is checked below is the weaker thing
	// that IS checkable here -- that the vocabulary is closed at all, so a
	// sixth value cannot arrive without a Core release, which is the point at
	// which this list must be re-read.
	answerabilities := []sessionwire.GateAnswerability{
		sessionwire.GateAnswerabilityResident,
		sessionwire.GateAnswerabilitySuspended,
		sessionwire.GateAnswerabilitySubmitted,
		sessionwire.GateAnswerabilityUnavailable,
		sessionwire.GateAnswerabilityExpired,
	}
	for _, value := range append(answerabilities, "invented-by-this-test") {
		projection := sessionwire.GateProjection{
			GateID: "gate-a", Kind: "question", OpenedEventID: "event-a", OpenedJournalSeq: 7,
			Deadline: serviceNow.Add(time.Hour), Answerability: value,
		}
		// The subject is the ANSWERABILITY field alone: the projection above
		// is otherwise incomplete, so asserting err == nil would assert about
		// the prompt instead. A declared value must not be the field Core
		// complains about; the invented one must be.
		var validation *sessionwire.RequestValidationError
		rejected := errors.As(projection.Validate(), &validation) && validation.Field == "answerability"
		if declared := value != "invented-by-this-test"; declared == rejected {
			t.Fatalf("Core's own validation of the answerability %q rejected = %v; this list no longer matches the vocabulary", value, rejected)
		}
	}
	resident := 0
	for _, answerability := range answerabilities {
		t.Run(string(answerability), func(t *testing.T) {
			f, req := gateFixture(t)
			f.catalog.entry.Record.OpenGates[0].Answerability = answerability
			_, created, err := f.service.AdmitGateResponse(context.Background(), f.principal, req)
			if answerability == sessionwire.GateAnswerabilityResident {
				if err != nil || !created {
					t.Fatalf("a resident projection was refused: (%v, %v)", created, err)
				}
				if f.commands.calls == 0 {
					t.Fatal("the accepted control never reached the inbox, so the refusals below assert nothing")
				}
				return
			}
			if !IsCode(err, sessionwire.ErrorCodeGateNotResumable) {
				t.Fatalf("error = %v, want gate_not_resumable", err)
			}
			if f.commands.calls != 0 {
				t.Fatal("a non-resident projection reached the inbox")
			}
		})
	}
	for _, answerability := range answerabilities {
		if answerability == sessionwire.GateAnswerabilityResident {
			resident++
		}
	}
	if resident != 1 {
		t.Fatalf("the accepted control appears %d times, want exactly 1", resident)
	}
}

// TestTheGateIncarnationMatchesByEitherWitnessAndNeverByAbsence covers both
// disjuncts of the version comparison and the emptiness guard on the first.
//
// Two mutants survived here. Deleting the ExpectedOpenJournalSeq disjunct
// entirely left the module green -- no test supplied a journal sequence, so
// the sequence-witnessed spelling of a gate answer, which Core declares as one
// of exactly two, was never admitted at all. And dropping the
// `ExpectedOpenEventID != ""` guard also left it green.
//
// The second mutant's reachability is worth stating precisely, because the
// obvious reading of it is wrong. Core's GateResponseRequest.Validate requires
// EXACTLY ONE witness (commands.go:265-274, `hasEventID == hasSequence` is
// invalid), so a request carrying neither cannot reach gateAdmission and the
// guard is NOT defending against that. What it defends against is the other
// side: a STORED PROJECTION whose OpenedEventID is empty. Without the guard, a
// sequence-witnessed request -- whose ExpectedOpenEventID is necessarily empty
// -- matches such a projection on the first disjunct regardless of the
// sequence it named, and answers a gate incarnation it never observed. The row
// below drives exactly that.
//
// Every row supplies exactly one witness, because Core refuses the rest before
// this code is reached; the one row that supplies both asserts that refusal
// rather than smuggling an unreachable state into the table.
func TestTheGateIncarnationMatchesByEitherWitnessAndNeverByAbsence(t *testing.T) {
	const (
		accepted = "accepted"
		resolved = "gate_resolved"
		invalid  = "invalid_request"
	)
	for _, test := range []struct {
		name           string
		gateEventID    string
		gateSeq        uint64
		requestEventID string
		requestSeq     uint64
		want           string
	}{
		{"an event id witnesses the open", "event-a", 7, "event-a", 0, accepted},
		{"a journal sequence witnesses the open", "event-a", 7, "", 7, accepted},
		{"a sequence witnesses an open whose event id is absent", "", 7, "", 7, accepted},
		{"a stale event id is a resolved incarnation", "event-a", 7, "event-b", 0, resolved},
		{"a stale journal sequence is a resolved incarnation", "event-a", 7, "", 6, resolved},
		{"an absent projection event id does not match an absent witness", "", 7, "", 6, resolved},
		{"a projection witnessing nothing matches nothing", "", 0, "", 5, resolved},
		{"an event id cannot be checked against a projection carrying none", "", 5, "event-a", 0, resolved},
		{"Core refuses a request naming both witnesses", "event-a", 7, "event-a", 7, invalid},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, req := gateFixture(t)
			f.catalog.entry.Record.OpenGates[0].OpenedEventID = sessionwire.EventID(test.gateEventID)
			f.catalog.entry.Record.OpenGates[0].OpenedJournalSeq = test.gateSeq
			req.ExpectedOpenEventID = sessionwire.EventID(test.requestEventID)
			req.ExpectedOpenJournalSeq = test.requestSeq
			_, created, err := f.service.AdmitGateResponse(context.Background(), f.principal, req)
			switch test.want {
			case accepted:
				if err != nil || !created {
					t.Fatalf("a witnessed incarnation was refused: (%v, %v)", created, err)
				}
				if f.commands.calls == 0 {
					t.Fatal("the accepted row reached no durable write")
				}
			case invalid:
				if !IsCode(err, sessionwire.ErrorCodeInvalidRequest) {
					t.Fatalf("error = %v, want invalid_request", err)
				}
			default:
				if !IsCode(err, sessionwire.ErrorCodeGateResolved) {
					t.Fatalf("error = %v, want gate_resolved", err)
				}
				if f.commands.calls != 0 {
					t.Fatal("an unwitnessed incarnation reached the inbox")
				}
			}
		})
	}
}

// TestAnOwnerThatIsNotThisSessionsIsNotAFreshMatch reads the IDENTITY half of
// freshMatchingOwner, which nothing read.
//
// Mutant: replacing the placement.ReusableOwner call with its liveness clauses
// alone -- resident, accepting, unexpired, and no comparison against the
// record -- left the module green. Every pre-existing row moved a liveness
// member, so "a FRESH RESIDENT owner" was covered and "the SESSION's own
// owner" was not: an observation for another tenant's session would have been
// handed this session's gate answer.
//
// The five members are derived from the comparison placement actually makes,
// stated here as the list it is: tenant, session, agent, runtime, placement.
// It is a list, not a closure -- a sixth member added to ReusableOwner joins
// that function's own tests, not this one, and this test would not notice.
func TestAnOwnerThatIsNotThisSessionsIsNotAFreshMatch(t *testing.T) {
	for _, test := range []struct {
		name     string
		disagree func(*sessionwire.HostLinkRegistryObservation)
	}{
		{"another tenant", func(o *sessionwire.HostLinkRegistryObservation) { o.TenantID = "tenant-b" }},
		{"another session", func(o *sessionwire.HostLinkRegistryObservation) { o.SessionID = "session-b" }},
		{"another agent", func(o *sessionwire.HostLinkRegistryObservation) { o.AgentID = "agent-b" }},
		{"another runtime", func(o *sessionwire.HostLinkRegistryObservation) { o.RuntimeCompatibilityID = "runtime-v2" }},
		{"another placement", func(o *sessionwire.HostLinkRegistryObservation) {
			o.Placement = sessionwire.HostPlacementDedicated
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, req := gateFixture(t)
			// The control: unmutated, this same fixture is accepted. Without
			// it every row below would pass against a service that refused
			// everything.
			control, _ := gateFixture(t)
			if _, created, err := control.service.AdmitGateResponse(context.Background(), control.principal, req); err != nil || !created {
				t.Fatalf("the matching control was refused: (%v, %v)", created, err)
			}
			test.disagree(&f.directory.owner)
			_, _, err := f.service.AdmitGateResponse(context.Background(), f.principal, req)
			if !IsCode(err, sessionwire.ErrorCodeGateNotResumable) {
				t.Fatalf("error = %v, want gate_not_resumable", err)
			}
			if f.commands.calls != 0 {
				t.Fatal("an owner that is not this session's reached the inbox")
			}
		})
	}
}

// TestAConflictingReuseIsClassifiedCommandRejected reads the CODE, which
// nothing read.
//
// Mutant: deleting the InboxErrorCommandMismatch arm of admit -- so the
// store's mismatch left as a raw store error with no public code -- left the
// module green. Both pre-existing conflict tests assert only `err == nil`
// fails, which is satisfied by any error at all, including a dependency fault
// spelled as one. That is a sentence ("conflicting reuse FAILS") wider than
// its probe ("something non-nil came back"): runbook steps 1 and 2 ask for a
// conflicting reuse to be REFUSED, and a refusal is a classified public code.
func TestAConflictingReuseIsClassifiedCommandRejected(t *testing.T) {
	for _, test := range []struct {
		name   string
		first  func(*serviceFixture) error
		second func(*serviceFixture) error
	}{
		{"a different payload under one command id",
			func(f *serviceFixture) error {
				_, _, err := f.service.AdmitInput(context.Background(), f.principal, sessionwire.InputRequest{
					CommandEnvelope: envelope("command-a"), SessionID: "session-a", Blocks: []byte(`[{"text":"first"}]`)})
				return err
			},
			func(f *serviceFixture) error {
				_, _, err := f.service.AdmitInput(context.Background(), f.principal, sessionwire.InputRequest{
					CommandEnvelope: envelope("command-a"), SessionID: "session-a", Blocks: []byte(`[{"text":"second"}]`)})
				return err
			}},
		{"a different kind under one command id",
			func(f *serviceFixture) error {
				_, _, err := f.service.AdmitInterrupt(context.Background(), f.principal, sessionwire.InterruptRequest{
					CommandEnvelope: envelope("command-a"), SessionID: "session-a"})
				return err
			},
			func(f *serviceFixture) error {
				_, _, err := f.service.AdmitRestore(context.Background(), f.principal, sessionwire.RestoreRequest{
					CommandEnvelope: envelope("command-a"), SessionID: "session-a"})
				return err
			}},
		{"a different gate answer under one command id",
			func(f *serviceFixture) error {
				_, _, err := f.service.AdmitGateResponse(context.Background(), f.principal, sessionwire.GateResponseRequest{
					CommandEnvelope: envelope("command-a"), SessionID: "session-a", GateID: "gate-a", Action: "submit",
					Values: map[string]json.RawMessage{"answer": json.RawMessage(`"yes"`)}, ExpectedOpenEventID: "event-a"})
				return err
			},
			func(f *serviceFixture) error {
				_, _, err := f.service.AdmitGateResponse(context.Background(), f.principal, sessionwire.GateResponseRequest{
					CommandEnvelope: envelope("command-a"), SessionID: "session-a", GateID: "gate-a", Action: "submit",
					Values: map[string]json.RawMessage{"answer": json.RawMessage(`"no"`)}, ExpectedOpenEventID: "event-a"})
				return err
			}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newServiceFixture(t)
			resolvableSession(f)
			if err := test.first(f); err != nil {
				t.Fatalf("the first command was refused: %v", err)
			}
			err := test.second(f)
			if !IsCode(err, sessionwire.ErrorCodeCommandRejected) {
				t.Fatalf("error = %v, want command_rejected", err)
			}
			// The cause survives for an operator even though the code is what
			// the caller sees.
			var inbox *sessionstore.InboxError
			if !errors.As(err, &inbox) || inbox.Code != sessionstore.InboxErrorCommandMismatch {
				t.Fatalf("the store's mismatch did not survive as the cause: %v", err)
			}
		})
	}
}

// TestAResolvedTargetForAnotherAgentIsRuntimeUnavailable reads the recheck of
// the resolver's answer, which nothing read.
//
// Mutant: deleting `target.Key.AgentID != req.AgentID` from both create paths
// left the module green -- the only "unknown target" row flips the resolver's
// BOOLEAN, so a resolver that confidently answered `known` with another
// agent's launch identity was accepted and would have launched the wrong
// agent under the caller's session id. Runbook step 3 asks for known runtime
// COMPATIBILITY, not merely for a non-empty answer.
//
// The discriminator matters: this must be runtime_unavailable with NO cause,
// not the create-reservation refusal, which also carries runtime_unavailable.
// Asserting the code alone would pass on the wrong path.
func TestAResolvedTargetForAnotherAgentIsRuntimeUnavailable(t *testing.T) {
	t.Run("V1 create", func(t *testing.T) {
		f := newServiceFixture(t)
		f.targets.target.Key.AgentID = "agent-b"
		_, _, err := f.service.AdmitCreate(context.Background(), f.principal, sessionwire.CreateRequest{
			CommandEnvelope: envelope("create-a"), SessionID: "session-a", AgentID: "agent-a"})
		if !IsCode(err, sessionwire.ErrorCodeRuntimeUnavailable) {
			t.Fatalf("error = %v, want runtime_unavailable", err)
		}
		if errors.Is(err, ErrCreateIdentityProtocolUnavailable) {
			t.Fatal("the mismatched agent was refused by the create-reservation guard, not by the target check")
		}
		if f.catalog.createCalls != 0 || f.commands.calls != 0 {
			t.Fatal("a mismatched target wrote durable state")
		}
	})
	t.Run("legacy create", func(t *testing.T) {
		f := newServiceFixture(t)
		f.targets.target.Key.AgentID = "agent-b"
		_, err := f.service.AdmitLegacyCreate(context.Background(), f.principal, LegacyCreateRequest{AgentID: "agent-a"})
		if !IsCode(err, sessionwire.ErrorCodeRuntimeUnavailable) {
			t.Fatalf("error = %v, want runtime_unavailable", err)
		}
		if f.catalog.createCalls != 0 || f.commands.calls != 0 {
			t.Fatal("a mismatched target wrote durable state")
		}
		// The control: the same call with a matching target IS admitted, so
		// the two assertions above are not trivially true.
		ok := newServiceFixture(t)
		if _, err := ok.service.AdmitLegacyCreate(context.Background(), ok.principal, LegacyCreateRequest{AgentID: "agent-a"}); err != nil {
			t.Fatalf("the matching control was refused: %v", err)
		}
		if ok.catalog.createCalls == 0 || ok.commands.calls == 0 {
			t.Fatal("the accepted control wrote nothing, so the refusals above assert nothing")
		}
	})
}

// TestLegacyCreateRefusesACatalogEntryItDidNotCreate reads the post-create
// identity recheck, which nothing read.
//
// Mutant: replacing the `entry.Record.AgentID != req.AgentID ||
// entry.Record.DesiredIdempotencyKey != string(req.CommandID)` condition with
// a constant false left the module green. The fake echoed every create back,
// so the case the guard exists for -- a durable entry already at that session
// id, belonging to another agent or another create command -- could not be
// produced at all. That is a fake looser than the dependency: the released
// CreateCatalogEntry returns the INCUMBENT with created false.
func TestLegacyCreateRefusesACatalogEntryItDidNotCreate(t *testing.T) {
	for _, test := range []struct {
		name      string
		incumbent sessionstore.CatalogRecord
	}{
		{"another agent already holds the session id", sessionstore.CatalogRecord{
			AgentID: "agent-b", DesiredIdempotencyKey: "generated-2"}},
		{"another create command already holds the session id", sessionstore.CatalogRecord{
			AgentID: "agent-a", DesiredIdempotencyKey: "someone-elses-command"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newServiceFixture(t)
			f.catalog.createReturn = &sessionstore.CatalogEntry{Record: test.incumbent, Revision: 1}
			_, err := f.service.AdmitLegacyCreate(context.Background(), f.principal, LegacyCreateRequest{AgentID: "agent-a"})
			if !IsCode(err, sessionwire.ErrorCodeCommandRejected) {
				t.Fatalf("error = %v, want command_rejected", err)
			}
			if f.commands.calls != 0 {
				t.Fatal("a create whose catalog entry is not its own reached the inbox")
			}
		})
	}
}

// TestAnAbsentSessionIsSessionNotFoundInEitherSpelling reads BOTH arms of
// catalogNotFound, neither of which anything read.
//
// Two mutants survived: disabling the CatalogError arm, and disabling the
// KeyspaceError arm. Together that means the session_not_found refusal -- one
// of the classified public codes this service mints -- had no test at all;
// TestEveryClassifiedRefusalIsReachableFromADrivenEntryPoint proves its call
// site is REACHABLE in the call graph, which is a different claim from any
// caller ever receiving it.
//
// Both spellings are driven because the released store produces both: the
// catalog's own not-found and the keyspace binding's, and a reader that
// handled one would silently turn the other into an unclassified fault.
func TestAnAbsentSessionIsSessionNotFoundInEitherSpelling(t *testing.T) {
	for _, spelling := range []struct {
		name string
		err  error
	}{
		{"the catalog's not-found", &sessionstore.CatalogError{Code: sessionstore.CatalogErrorNotFound}},
		{"the keyspace's binding-not-found", &sessionstore.KeyspaceError{Code: sessionstore.KeyspaceBindingNotFound}},
	} {
		for _, entry := range []struct {
			name string
			call func(*serviceFixture) error
		}{
			{"input", func(f *serviceFixture) error {
				_, _, err := f.service.AdmitInput(context.Background(), f.principal, sessionwire.InputRequest{
					CommandEnvelope: envelope("command-a"), SessionID: "session-a", Blocks: []byte(`[{"text":"hi"}]`)})
				return err
			}},
			{"interrupt", func(f *serviceFixture) error {
				_, _, err := f.service.AdmitInterrupt(context.Background(), f.principal, sessionwire.InterruptRequest{
					CommandEnvelope: envelope("command-a"), SessionID: "session-a"})
				return err
			}},
			{"restore", func(f *serviceFixture) error {
				_, _, err := f.service.AdmitRestore(context.Background(), f.principal, sessionwire.RestoreRequest{
					CommandEnvelope: envelope("command-a"), SessionID: "session-a"})
				return err
			}},
			{"gate response", func(f *serviceFixture) error {
				_, _, err := f.service.AdmitGateResponse(context.Background(), f.principal, sessionwire.GateResponseRequest{
					CommandEnvelope: envelope("command-a"), SessionID: "session-a", GateID: "gate-a", Action: "submit",
					Values: map[string]json.RawMessage{"answer": json.RawMessage(`"yes"`)}, ExpectedOpenEventID: "event-a"})
				return err
			}},
		} {
			t.Run(spelling.name+"/"+entry.name, func(t *testing.T) {
				f := newServiceFixture(t)
				resolvableSession(f)
				// The control: this same call succeeds while the session
				// exists, so the refusal below is caused by its absence and
				// not by anything else in the fixture.
				control := newServiceFixture(t)
				resolvableSession(control)
				if err := entry.call(control); err != nil {
					t.Fatalf("the present-session control was refused: %v", err)
				}
				if control.commands.calls == 0 {
					t.Fatal("the control reached no durable write, so the assertion below is vacuous")
				}
				f.catalog.getErr = spelling.err
				err := entry.call(f)
				if !IsCode(err, sessionwire.ErrorCodeSessionNotFound) {
					t.Fatalf("error = %v, want session_not_found", err)
				}
				if f.commands.calls != 0 {
					t.Fatal("an absent session reached the inbox")
				}
			})
		}
	}
}

// TestARetryIsRecognisedInEitherNotFoundSpelling reads the KeyspaceError arm
// of commandNotFound, which nothing read.
//
// Mutant: deleting that arm left the module green. The consequence is not
// cosmetic. commandNotFound answers "this command is NOT already durable"; a
// spelling it fails to recognise becomes a raw error out of the retry probe,
// so a caller retrying after an unknown outcome is handed a dependency fault
// instead of its original record -- exactly inverting what runbook step 2's
// reuse rule exists to provide. The pre-existing fake could only produce the
// InboxError spelling, which is a fake narrower than the dependency.
func TestARetryIsRecognisedInEitherNotFoundSpelling(t *testing.T) {
	for _, spelling := range []struct {
		name string
		err  error
	}{
		{"the inbox's not-found", &sessionstore.InboxError{Code: sessionstore.InboxErrorNotFound}},
		{"the keyspace's binding-not-found", &sessionstore.KeyspaceError{Code: sessionstore.KeyspaceBindingNotFound}},
	} {
		t.Run(spelling.name, func(t *testing.T) {
			f := newServiceFixture(t)
			resolvableSession(f)
			f.commands.notFound = spelling.err
			req := sessionwire.InputRequest{CommandEnvelope: envelope("command-a"), SessionID: "session-a", Blocks: []byte(`[{"text":"hi"}]`)}
			first, created, err := f.service.AdmitInput(context.Background(), f.principal, req)
			if err != nil || !created {
				t.Fatalf("first = (%v, %v)", created, err)
			}
			retry, created, err := f.service.AdmitInput(context.Background(), f.principal, req)
			if err != nil || created || !reflect.DeepEqual(retry, first) {
				t.Fatalf("retry = (%+v, %v, %v)", retry, created, err)
			}
		})
	}
}

// TestAcceptanceRecordsTheClockAndTheConfiguredApplyDeadline reads the two
// times the admitted record carries, neither of which anything read.
//
// Mutant: replacing `now.Add(s.cfg.ApplyDeadline)` with `now` left the module
// green, so a build in which no command ever became due would have passed --
// and the due-work reconciler A8.1 built is driven entirely by that deadline.
// The deadline is asserted as a FUNCTION of the configured duration rather
// than against a constant, so a service configured differently is covered by
// the same rows.
func TestAcceptanceRecordsTheClockAndTheConfiguredApplyDeadline(t *testing.T) {
	for _, deadline := range []time.Duration{time.Minute, 90 * time.Second, time.Hour} {
		t.Run(deadline.String(), func(t *testing.T) {
			f := newServiceFixture(t)
			resolvableSession(f)
			svc, err := NewService(Config{Authorizer: f.auth, Targets: f.targets, Catalog: f.catalog,
				Commands: f.commands, Directory: f.directory, Clock: serviceClock{serviceNow},
				IDs: f.ids, ApplyDeadline: deadline})
			if err != nil {
				t.Fatal(err)
			}
			entry, created, err := svc.AdmitInput(context.Background(), f.principal, sessionwire.InputRequest{
				CommandEnvelope: envelope("command-a"), SessionID: "session-a", Blocks: []byte(`[{"text":"hi"}]`)})
			if err != nil || !created {
				t.Fatalf("admission = (%v, %v)", created, err)
			}
			if !entry.Record.AcceptedAt.Equal(serviceNow) {
				t.Errorf("AcceptedAt = %v, want %v", entry.Record.AcceptedAt, serviceNow)
			}
			if want := serviceNow.Add(deadline); !entry.Record.ApplyDeadline.Equal(want) {
				t.Errorf("ApplyDeadline = %v, want %v", entry.Record.ApplyDeadline, want)
			}
			if entry.Record.ApplyDeadline.Equal(entry.Record.AcceptedAt) {
				t.Error("the apply deadline is the acceptance instant, so nothing ever becomes due")
			}
		})
	}
}

// TestEveryDurableIdentityIsScopedToThePrincipalsTenant reads the tenant on
// every request this service issues.
//
// Mutant: blanking the tenant on the catalog read left the module green. Only
// the inbox write's tenant was read anywhere, and by one integration test. The
// A2.1 carry-forward names tenant scoping of the SessionStore query as the
// obligation A2 owns and A1.2 satisfies only vacuously; this is that
// obligation at the admission seam.
//
// The control is a SECOND principal: the same call under tenant-b must carry
// tenant-b, so a service that hard-coded the fixture's tenant fails even
// though every single-tenant assertion would pass.
func TestEveryDurableIdentityIsScopedToThePrincipalsTenant(t *testing.T) {
	for _, tenant := range []string{"tenant-a", "tenant-b"} {
		t.Run(tenant, func(t *testing.T) {
			f := newServiceFixture(t)
			resolvableSession(f)
			f.catalog.entry.Record.TenantID = sessionwire.TenantID(tenant)
			f.directory.owner.TenantID = sessionwire.TenantID(tenant)
			principal, err := identity.NewPrincipal(sessionwire.TenantID(tenant), "actor-a", identity.KindActor)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := f.service.AdmitGateResponse(context.Background(), principal, sessionwire.GateResponseRequest{
				CommandEnvelope: envelope("command-a"), SessionID: "session-a", GateID: "gate-a", Action: "submit",
				Values: map[string]json.RawMessage{"answer": json.RawMessage(`"yes"`)}, ExpectedOpenEventID: "event-a",
			}); err != nil {
				t.Fatalf("admission: %v", err)
			}
			want := sessionwire.TenantID(tenant)
			for _, got := range []struct {
				site  string
				value sessionwire.TenantID
			}{
				{"the catalog read", f.catalog.lastGet.TenantID},
				{"the retry probe", f.commands.lastGet.TenantID},
				{"the ownership question", f.directory.lastTenant},
				{"the inbox write", f.commands.lastAdmit.TenantID},
			} {
				if got.value != want {
					t.Errorf("%s was scoped to %q, want %q", got.site, got.value, want)
				}
			}
		})
	}
}

// TestARefusedCommandStopsAtItsGuardAndTouchesNothingFurther is the positive
// control for the two NEGATIVE assertions runbook steps 3 and 4 make.
//
// Step 3 says an unknown runtime produces "no inbox record" and step 4 says a
// non-resumable gate does "not place/restore a Host". Both are assertions that
// something did NOT happen, and both were written as `f.commands.calls != 0`
// -- which cannot fail on a path that reaches no dependency for any reason,
// including a service that refuses everything or a fixture that never drove
// anything.
//
// So the subject here is the whole observed CALL TRACE, held by SET EQUALITY
// rather than by a "did not" on one counter:
//
//   - The accepted row is the control. It observes a trace CONTAINING the
//     durable write, which is what proves the probe can see one at all.
//   - Each refused row must observe exactly its own prefix. An extra call --
//     a durable write, or a placement drive if Config ever declared one --
//     makes the observed set differ and fails, and so does a MISSING call, so
//     the guard cannot be satisfied by a service that stopped doing its work.
//
// The residue, stated rather than closed: the trace can only see collaborators
// the fakes implement, which are exactly Config's interface-kind fields minus
// Clock. That is not a file-name or naming rule -- admissionDependencies
// derives the set from Config, and the check below fails if a dependency
// appears there that no fake records. A side effect reached WITHOUT a Config
// dependency is outside this guard; service.go has no such path today because
// its only non-stdlib collaborators are s.cfg fields, and
// TestNoDependencyFaultBecomesAPublicCode holds that set.
func TestARefusedCommandStopsAtItsGuardAndTouchesNothingFurther(t *testing.T) {
	trace := func(f *serviceFixture) []string {
		var out []string
		for _, injector := range []*faultInjector{
			&f.auth.faultInjector, &f.targets.faultInjector, &f.catalog.faultInjector,
			&f.commands.faultInjector, &f.directory.faultInjector, &f.ids.faultInjector,
		} {
			for method := range injector.called {
				out = append(out, method)
			}
		}
		slices.Sort(out)
		return out
	}

	// The fakes must cover every dependency Config declares, or this trace is
	// blind to one and would call an unobserved side effect "nothing".
	recorded := map[string]bool{"Clock": true}
	for name := range admissionDependencies(t) {
		if _, armable := armFault(newServiceFixture(t), name, ""); armable {
			recorded[name] = true
		}
	}
	for name := range admissionDependencies(t) {
		if !recorded[name] {
			t.Fatalf("Config declares %s, which no fake records; this trace cannot see its side effects", name)
		}
	}

	for _, test := range []struct {
		name      string
		configure func(*serviceFixture)
		call      func(*serviceFixture) error
		want      []string
	}{
		{
			name:      "an accepted gate response is the control and DOES write",
			configure: func(*serviceFixture) {},
			call: func(f *serviceFixture) error {
				_, _, err := f.service.AdmitGateResponse(context.Background(), f.principal, sessionwire.GateResponseRequest{
					CommandEnvelope: envelope("command-a"), SessionID: "session-a", GateID: "gate-a", Action: "submit",
					Values: map[string]json.RawMessage{"answer": json.RawMessage(`"yes"`)}, ExpectedOpenEventID: "event-a"})
				return err
			},
			want: []string{"AdmitCommand", "AuthorizeControl", "GetCatalogEntry", "GetCommand", "IsKnown", "NewUUID", "Owner"},
		},
		{
			name:      "an unknown runtime stops before the inbox",
			configure: func(f *serviceFixture) { f.targets.known = false },
			call: func(f *serviceFixture) error {
				_, _, err := f.service.AdmitInterrupt(context.Background(), f.principal, sessionwire.InterruptRequest{
					CommandEnvelope: envelope("command-a"), SessionID: "session-a"})
				return err
			},
			want: []string{"AuthorizeControl", "GetCatalogEntry", "GetCommand", "IsKnown"},
		},
		{
			name:      "a cold owner stops before the inbox and drives no placement",
			configure: func(f *serviceFixture) { f.directory.ok = false },
			call: func(f *serviceFixture) error {
				_, _, err := f.service.AdmitGateResponse(context.Background(), f.principal, sessionwire.GateResponseRequest{
					CommandEnvelope: envelope("command-a"), SessionID: "session-a", GateID: "gate-a", Action: "submit",
					Values: map[string]json.RawMessage{"answer": json.RawMessage(`"yes"`)}, ExpectedOpenEventID: "event-a"})
				return err
			},
			want: []string{"AuthorizeControl", "GetCatalogEntry", "GetCommand", "IsKnown", "Owner"},
		},
		{
			name:      "a resolved gate stops before the inbox and drives no placement",
			configure: func(f *serviceFixture) { f.catalog.entry.Record.OpenGates = nil },
			call: func(f *serviceFixture) error {
				_, _, err := f.service.AdmitGateResponse(context.Background(), f.principal, sessionwire.GateResponseRequest{
					CommandEnvelope: envelope("command-a"), SessionID: "session-a", GateID: "gate-a", Action: "submit",
					Values: map[string]json.RawMessage{"answer": json.RawMessage(`"yes"`)}, ExpectedOpenEventID: "event-a"})
				return err
			},
			want: []string{"AuthorizeControl", "GetCatalogEntry", "GetCommand", "IsKnown"},
		},
		{
			name:      "a denied principal stops at the authorizer",
			configure: func(f *serviceFixture) { f.auth.err = errors.New("denied") },
			call: func(f *serviceFixture) error {
				_, _, err := f.service.AdmitInput(context.Background(), f.principal, sessionwire.InputRequest{
					CommandEnvelope: envelope("command-a"), SessionID: "session-a", Blocks: []byte(`[{"text":"hi"}]`)})
				return err
			},
			want: []string{"AuthorizeControl"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newServiceFixture(t)
			resolvableSession(f)
			test.configure(f)
			err := test.call(f)
			accepted := strings.Contains(test.name, "accepted")
			if accepted != (err == nil) {
				t.Fatalf("error = %v, accepted = %v", err, accepted)
			}
			if got := trace(f); !slices.Equal(got, test.want) {
				t.Fatalf("call trace = %v, want %v", got, test.want)
			}
			if accepted != slices.Contains(test.want, "AdmitCommand") {
				t.Fatal("the control's expectation disagrees with its outcome")
			}
		})
	}
}
