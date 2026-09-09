package admission

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	"github.com/looprig/sessionstore"
)

var serviceNow = time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)

type serviceAuthorizer struct {
	err   error
	calls int
}

func (a *serviceAuthorizer) AuthorizeControl(context.Context, identity.Principal, sessionwire.SessionID, sessionstore.CommandKind) error {
	a.calls++
	return a.err
}

func (a *serviceAuthorizer) AuthorizeServiceSweep(context.Context, identity.Principal) error {
	return a.err
}

type serviceTargets struct {
	target Target
	known  bool
	err    error
	calls  int
}

func (t *serviceTargets) ResolveAgent(context.Context, sessionwire.AgentID) (Target, bool, error) {
	t.calls++
	return t.target, t.known, t.err
}

func (t *serviceTargets) IsKnown(context.Context, sessionstore.HostTargetKey) (bool, error) {
	t.calls++
	return t.known, t.err
}

type serviceCatalog struct {
	entry       sessionstore.CatalogEntry
	getErr      error
	createCalls int
}

func (c *serviceCatalog) GetCatalogEntry(context.Context, sessionstore.GetCatalogEntryRequest) (sessionstore.CatalogEntry, error) {
	return c.entry, c.getErr
}

func (c *serviceCatalog) CreateCatalogEntry(_ context.Context, req sessionstore.CreateCatalogEntryRequest) (sessionstore.CatalogEntry, bool, error) {
	c.createCalls++
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
	records map[sessionwire.CommandID]sessionstore.InboxEntry
	calls   int
}

func (c *serviceCommands) GetCommand(_ context.Context, req sessionstore.GetCommandRequest) (sessionstore.InboxEntry, error) {
	entry, ok := c.records[req.CommandID]
	if !ok {
		return sessionstore.InboxEntry{}, &sessionstore.InboxError{Code: sessionstore.InboxErrorNotFound}
	}
	return entry, nil
}

func (c *serviceCommands) AdmitCommand(_ context.Context, req sessionstore.AdmitCommandRequest) (sessionstore.InboxEntry, bool, error) {
	c.calls++
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
	owner sessionwire.HostLinkRegistryObservation
	ok    bool
	err   error
	calls int
}

func (d *serviceDirectory) Owner(context.Context, sessionwire.TenantID, sessionwire.SessionID) (sessionwire.HostLinkRegistryObservation, bool, error) {
	d.calls++
	return d.owner, d.ok, d.err
}

type serviceClock struct{ now time.Time }

func (c serviceClock) Now() time.Time { return c.now }
func (c serviceClock) AfterFunc(time.Duration, func()) func() bool {
	return func() bool { return true }
}

type serviceIDs struct{ next int }

func (s *serviceIDs) NewUUID() (string, error) {
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
	svc, err := NewService(Config{Authorizer: auth, Targets: targets, Catalog: catalog, Commands: commands,
		Directory: directory, Clock: serviceClock{serviceNow}, IDs: &serviceIDs{}, ApplyDeadline: time.Minute})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return &serviceFixture{svc, auth, targets, catalog, commands, directory, p}
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
