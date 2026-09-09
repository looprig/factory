package clientlink_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	"github.com/looprig/factory/internal/admission"
	"github.com/looprig/factory/internal/command"
	internalidentity "github.com/looprig/factory/internal/identity"
	"github.com/looprig/factory/internal/realtime/clientlink"
	"github.com/looprig/sessionstore"
)

// ---------------------------------------------------------------------------
// The command bodies.
//
// Every body below is the JSON a REST caller would POST for the same operation.
// That is the whole of the envelope half of step 1's parity claim and it is
// checkable because Core owns the shape: these bytes are decoded by the SAME
// sessionwire type on either edge, so an envelope the RPC accepts and the route
// refuses cannot exist without Core disagreeing with itself.
// ---------------------------------------------------------------------------

// commandBody is the canonical V1 request for one method, naming one session
// and one client command identity.
func commandBody(method clientlink.Method, session sessionwire.SessionID, id sessionwire.CommandID) []byte {
	switch method {
	case clientlink.MethodSessionCreate:
		return fmt.Appendf(nil, `{"version":1,"command_id":%q,"session_id":%q,"agent_id":"agent-a","blocks":[{"text":"hello"}]}`, id, session)
	case clientlink.MethodSessionInput:
		return fmt.Appendf(nil, `{"version":1,"command_id":%q,"session_id":%q,"blocks":[{"text":"hello"}]}`, id, session)
	case clientlink.MethodSessionInterrupt, clientlink.MethodSessionRestore:
		return fmt.Appendf(nil, `{"version":1,"command_id":%q,"session_id":%q}`, id, session)
	case clientlink.MethodGateRespond:
		return fmt.Appendf(nil, `{"version":1,"command_id":%q,"session_id":%q,"gate_id":"gate-1","action":"approve","values":{},"expected_open_journal_seq":7}`, id, session)
	default:
		panic("no canonical body for method " + string(method))
	}
}

// wantRequest is the typed value commandBody must decode as. It is written out
// independently rather than derived from the bytes, so a decoder that dropped a
// member or read the wrong one is visible.
func wantRequest(method clientlink.Method, session sessionwire.SessionID, id sessionwire.CommandID) any {
	envelope := sessionwire.CommandEnvelope{Version: sessionwire.CurrentWireVersion, CommandID: id}
	blocks := json.RawMessage(`[{"text":"hello"}]`)
	switch method {
	case clientlink.MethodSessionCreate:
		return sessionwire.CreateRequest{CommandEnvelope: envelope, SessionID: session, AgentID: "agent-a", Blocks: blocks}
	case clientlink.MethodSessionInput:
		return sessionwire.InputRequest{CommandEnvelope: envelope, SessionID: session, Blocks: blocks}
	case clientlink.MethodSessionInterrupt:
		return sessionwire.InterruptRequest{CommandEnvelope: envelope, SessionID: session}
	case clientlink.MethodSessionRestore:
		return sessionwire.RestoreRequest{CommandEnvelope: envelope, SessionID: session}
	case clientlink.MethodGateRespond:
		return sessionwire.GateResponseRequest{
			CommandEnvelope: envelope, SessionID: session, GateID: "gate-1",
			Action: "approve", Values: map[string]json.RawMessage{}, ExpectedOpenJournalSeq: 7,
		}
	default:
		panic("no expected request for method " + string(method))
	}
}

// ---------------------------------------------------------------------------
// The admission fake.
//
// It stands in for internal/admission.Service, and the shape of that stand-in
// is audited in BOTH directions by TestTheAdmissionServiceIsExactlyTheSeamThisEdgeCalls:
// a fake looser than the service it replaces is how a test suite comes to prove
// something about nothing.
// ---------------------------------------------------------------------------

type admitCall struct {
	method    clientlink.Method
	principal identity.Principal
	request   any
}

type recordingAdmitter struct {
	mu sync.Mutex
	// entry is returned for every accepted command.
	entry sessionstore.InboxEntry
	// created is admission's "this call accepted it" answer.
	created bool
	// err, when set, is returned instead of entry.
	err error
	// gate, when non-nil, is received from before the admission returns: it is
	// what makes "the reply follows the commit" measurable rather than timed.
	gate  chan struct{}
	calls []admitCall
}

func (a *recordingAdmitter) record(method clientlink.Method, principal identity.Principal, request any) (sessionstore.InboxEntry, bool, error) {
	a.mu.Lock()
	a.calls = append(a.calls, admitCall{method: method, principal: principal, request: request})
	gate, entry, created, err := a.gate, a.entry, a.created, a.err
	a.mu.Unlock()
	if gate != nil {
		<-gate
	}
	return entry, created, err
}

func (a *recordingAdmitter) recorded() []admitCall {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.calls)
}

func (a *recordingAdmitter) AdmitCreate(_ context.Context, p identity.Principal, req sessionwire.CreateRequest) (sessionstore.InboxEntry, bool, error) {
	return a.record(clientlink.MethodSessionCreate, p, req)
}

func (a *recordingAdmitter) AdmitInput(_ context.Context, p identity.Principal, req sessionwire.InputRequest) (sessionstore.InboxEntry, bool, error) {
	return a.record(clientlink.MethodSessionInput, p, req)
}

func (a *recordingAdmitter) AdmitInterrupt(_ context.Context, p identity.Principal, req sessionwire.InterruptRequest) (sessionstore.InboxEntry, bool, error) {
	return a.record(clientlink.MethodSessionInterrupt, p, req)
}

func (a *recordingAdmitter) AdmitRestore(_ context.Context, p identity.Principal, req sessionwire.RestoreRequest) (sessionstore.InboxEntry, bool, error) {
	return a.record(clientlink.MethodSessionRestore, p, req)
}

func (a *recordingAdmitter) AdmitGateResponse(_ context.Context, p identity.Principal, req sessionwire.GateResponseRequest) (sessionstore.InboxEntry, bool, error) {
	return a.record(clientlink.MethodGateRespond, p, req)
}

// acceptedEntry is one durable record, with every member an absolute literal
// different from every other, so a projection that read the wrong one fails.
func acceptedEntry(session sessionwire.SessionID, id sessionwire.CommandID) sessionstore.InboxEntry {
	return sessionstore.InboxEntry{
		Record: sessionstore.InboxRecord{
			TenantID: tenantA, SessionID: session, CommandID: id,
			RuntimeCommandID: "runtime-77", Kind: command.KindInput,
			State: sessionstore.InboxStatePending,
		},
		Revision:      41,
		AcceptedOrder: 4242,
	}
}

// engineFixture is the policy half with no socket: the same Engine the
// transport drives, reachable directly.
type engineFixture struct {
	engine     *clientlink.Engine
	authorizer *recordingAuthorizer
	admitter   *recordingAdmitter
	principal  identity.Principal
}

func newEngineFixture(t *testing.T) *engineFixture {
	t.Helper()

	principal, err := identity.NewPrincipal(tenantA, "user-a", identity.KindActor)
	if err != nil {
		t.Fatalf("NewPrincipal: %v", err)
	}
	f := &engineFixture{
		authorizer: &recordingAuthorizer{},
		admitter:   &recordingAdmitter{created: true},
		principal:  principal,
	}
	engine, err := clientlink.NewEngine(clientlink.Config{
		Authenticator: fixedAuthenticator{principal: principal},
		Authorizer:    f.authorizer,
		Admitter:      f.admitter,
		Limits:        testLimits(),
		Version:       buildVersion,
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	f.engine = engine
	return f
}

// admit drives one RPC through the engine.
func (f *engineFixture) admit(t *testing.T, method clientlink.Method, data []byte) ([]byte, error) {
	t.Helper()

	return f.engine.Admit(t.Context(), f.principal, method, data)
}

// envelopeCode reads the stable code out of a refusal body, insisting that the
// body IS a refusal: a CommandStatus and an ErrorEnvelope are two different
// answers and a test that accepted either would assert nothing.
func envelopeCode(t *testing.T, body []byte) sessionwire.ErrorCode {
	t.Helper()

	var envelope sessionwire.ErrorEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("the reply %s is not a Core error envelope: %v", body, err)
	}
	if strings.Contains(string(body), "command_id") {
		t.Fatalf("the refusal %s carries a command_id, so a client cannot tell it from an acceptance", body)
	}
	return envelope.Error.Code
}

// statusOf reads the accepted record out of a reply, insisting it is one.
func statusOf(t *testing.T, body []byte) sessionwire.CommandStatus {
	t.Helper()

	var status sessionwire.CommandStatus
	if err := json.Unmarshal(body, &status); err != nil {
		t.Fatalf("the reply %s is not a Core command status: %v", body, err)
	}
	return status
}

// ---------------------------------------------------------------------------
// Step 1 -- the envelope, the identities, and the admission each method reaches.
// ---------------------------------------------------------------------------

// TestEveryMethodDecodesItsOwnV1RequestAndReachesItsOwnAdmission is the parity
// claim at the layer that has a reader.
//
// The REST control routes are not implemented (their route-table rows carry an
// owner), so "the same request reaches the same handler" cannot be measured
// end to end today. What CAN be measured is the thing parity actually rests on:
// each method decodes the canonical V1 body with Core's own decoder into Core's
// own type and hands THAT VALUE to the admission service -- the same value the
// route will hand it, because neither edge is free to invent a different one.
func TestEveryMethodDecodesItsOwnV1RequestAndReachesItsOwnAdmission(t *testing.T) {
	t.Parallel()

	for _, method := range clientlink.Methods() {
		t.Run(string(method), func(t *testing.T) {
			t.Parallel()

			f := newEngineFixture(t)
			session := sessionwire.SessionID("session-" + string(method))
			id := sessionwire.CommandID("cmd-" + string(method))
			f.admitter.entry = acceptedEntry(session, id)

			if _, err := f.admit(t, method, commandBody(method, session, id)); err != nil {
				t.Fatalf("Admit(%s) = %v, want an admitted reply", method, err)
			}

			calls := f.admitter.recorded()
			if len(calls) != 1 {
				t.Fatalf("Admit(%s) made %d admissions, want exactly 1", method, len(calls))
			}
			if calls[0].method != method {
				t.Errorf("%s was admitted through %s's service method", method, calls[0].method)
			}
			if got, want := calls[0].request, wantRequest(method, session, id); !reflect.DeepEqual(got, want) {
				t.Errorf("%s admitted %#v, want %#v", method, got, want)
			}
			if calls[0].principal != f.principal {
				t.Errorf("%s admitted for principal %+v, want %+v", method, calls[0].principal, f.principal)
			}

			// The kind is the ROUTE's kind, and the shared constant is what
			// makes that true rather than a coincidence of two spellings.
			kind, known := clientlink.CommandKindFor(method)
			if !known {
				t.Fatalf("CommandKindFor(%s) reported the method unknown", method)
			}
			control := f.authorizer.controlCalls()
			if len(control) != 1 {
				t.Fatalf("%s made %d authorization decisions, want exactly 1", method, len(control))
			}
			if control[0].kind != kind {
				t.Errorf("%s was authorized under kind %q, want %q", method, control[0].kind, kind)
			}
		})
	}
}

// TestTheAuthorizedSessionIsTheAdmittedSession closes the gap A6.1's envelope
// left open.
//
// A6.1 read session_id through a private struct of its own, so the session the
// AUTHORIZER was asked about and the session the SERVICE would admit came from
// two decoders over the same bytes. This drives a body whose strict decode and
// a lenient one disagree -- a duplicate member, which Core refuses outright and
// encoding/json resolves to the LAST occurrence -- and requires the two readers
// to be one.
func TestTheAuthorizedSessionIsTheAdmittedSession(t *testing.T) {
	t.Parallel()

	f := newEngineFixture(t)
	const session = sessionwire.SessionID("session-authorized")
	f.admitter.entry = acceptedEntry(session, "cmd-1")

	if _, err := f.admit(t, clientlink.MethodSessionInterrupt, commandBody(clientlink.MethodSessionInterrupt, session, "cmd-1")); err != nil {
		t.Fatalf("Admit = %v, want an admitted reply", err)
	}
	control := f.authorizer.controlCalls()
	if len(control) != 1 {
		t.Fatalf("%d authorization decisions, want 1", len(control))
	}
	if control[0].session != session {
		t.Errorf("the authorizer was asked about session %q, want %q", control[0].session, session)
	}
	admitted := f.admitter.recorded()
	if len(admitted) != 1 {
		t.Fatalf("%d admissions, want 1", len(admitted))
	}
	if got := admitted[0].request.(sessionwire.InterruptRequest).SessionID; got != session {
		t.Errorf("session %q was admitted, want %q", got, session)
	}

	// The second half: a body naming two sessions is refused, so there is no
	// answer to "which one" for the two readers to differ on.
	second := newEngineFixture(t)
	body := []byte(`{"version":1,"command_id":"cmd-1","session_id":"session-authorized","session_id":"session-elsewhere"}`)
	reply, err := second.admit(t, clientlink.MethodSessionInterrupt, body)
	if err != nil {
		t.Fatalf("Admit(duplicate session_id) = %v, want a refusal reply", err)
	}
	if got := envelopeCode(t, reply); got != sessionwire.ErrorCodeInvalidRequest {
		t.Errorf("a body naming two sessions was refused with %q, want %q", got, sessionwire.ErrorCodeInvalidRequest)
	}
	if calls := second.authorizer.controlCalls(); len(calls) != 0 {
		t.Errorf("a body naming two sessions reached the authorizer %d times: %+v", len(calls), calls)
	}
	if calls := second.admitter.recorded(); len(calls) != 0 {
		t.Errorf("a body naming two sessions reached admission %d times", len(calls))
	}
}

// TestABodyTheV1VocabularyRefusesIsAnInvalidRequestRefusal drives every way a
// body can fail Core's own decoder, on every method.
//
// The per-method loop matters because each method has its own member list: a
// decoder wired to the wrong Core type would accept a sibling's body and be
// invisible to a table that only tested one method.
func TestABodyTheV1VocabularyRefusesIsAnInvalidRequestRefusal(t *testing.T) {
	t.Parallel()

	for _, method := range clientlink.Methods() {
		for name, body := range map[string][]byte{
			"no data at all":              nil,
			"empty bytes":                 {},
			"not JSON":                    []byte("not json"),
			"a JSON array":                []byte(`[]`),
			"an empty object":             []byte(`{}`),
			"an unknown member":           append(commandBody(method, "session-1", "cmd-1")[:len(commandBody(method, "session-1", "cmd-1"))-1], []byte(`,"tenant_id":"tenant-b"}`)...),
			"no command id":               []byte(`{"version":1,"session_id":"session-1"}`),
			"an empty command id":         []byte(`{"version":1,"command_id":"","session_id":"session-1"}`),
			"an empty session id":         []byte(`{"version":1,"command_id":"cmd-1","session_id":""}`),
			"an unsupported wire version": []byte(`{"version":9,"command_id":"cmd-1","session_id":"session-1"}`),
		} {
			t.Run(string(method)+"/"+name, func(t *testing.T) {
				t.Parallel()

				f := newEngineFixture(t)
				reply, err := f.admit(t, method, body)
				if err != nil {
					t.Fatalf("Admit(%s, %s) = %v, want a refusal reply", method, name, err)
				}
				if got := envelopeCode(t, reply); got != sessionwire.ErrorCodeInvalidRequest {
					t.Errorf("Admit(%s, %s) was refused with %q, want %q", method, name, got, sessionwire.ErrorCodeInvalidRequest)
				}
				// Nothing durable, and no decision about a tenant: the refusal
				// is derived from the caller's own bytes alone.
				if calls := f.admitter.recorded(); len(calls) != 0 {
					t.Errorf("Admit(%s, %s) reached admission %d times", method, name, len(calls))
				}
				if calls := f.authorizer.controlCalls(); len(calls) != 0 {
					t.Errorf("Admit(%s, %s) reached the authorizer %d times", method, name, len(calls))
				}
			})
		}
	}
}

// TestACanonicalBodyIsAcceptedByExactlyOneMethod is the positive control the
// refusal table needs.
//
// Without it, "the body was refused" has the same output whether the decoder
// works or is wired to a type that refuses everything. Each canonical body is
// offered to every OTHER method as well: only the method it belongs to may
// accept it, because the member lists differ, and a decoder table with two
// entries swapped fails here rather than passing both halves.
func TestACanonicalBodyIsAcceptedByExactlyOneMethod(t *testing.T) {
	t.Parallel()

	// interrupt and restore are the one pair Core gives identical member lists
	// -- version, command_id, session_id -- so each accepts the other's body.
	// Naming that here is what keeps this a measurement rather than a claim
	// about a coincidence.
	interchangeable := map[clientlink.Method]clientlink.Method{
		clientlink.MethodSessionInterrupt: clientlink.MethodSessionRestore,
		clientlink.MethodSessionRestore:   clientlink.MethodSessionInterrupt,
	}
	accepted := 0
	for _, body := range clientlink.Methods() {
		for _, method := range clientlink.Methods() {
			f := newEngineFixture(t)
			f.admitter.entry = acceptedEntry("session-1", "cmd-1")
			reply, err := f.admit(t, method, commandBody(body, "session-1", "cmd-1"))
			if err != nil {
				t.Fatalf("Admit(%s with %s's body) = %v", method, body, err)
			}
			want := method == body || interchangeable[body] == method
			got := len(f.admitter.recorded()) == 1
			if got != want {
				t.Errorf("Admit(%s with %s's body) reached admission = %t, want %t (reply %s)", method, body, got, want, reply)
			}
			if got {
				accepted++
			}
		}
	}
	if accepted == 0 {
		t.Fatal("no body was accepted by any method, so the refusal table proves nothing")
	}
}

// ---------------------------------------------------------------------------
// Step 1 -- the errors.
// ---------------------------------------------------------------------------

// TestEveryAdmissionRefusalReachesTheClientAsItsOwnCode is the error half of
// parity, and it is a for-all over Core's whole vocabulary rather than over the
// codes admission happens to mint today.
//
// A mapping that enumerated the codes it knew would answer a code Core adds
// later -- or one a later admission task starts minting -- with something else.
// Carrying the classification whole is what makes the RPC's answer and the
// route's answer the same string by construction.
func TestEveryAdmissionRefusalReachesTheClientAsItsOwnCode(t *testing.T) {
	t.Parallel()

	codes := []sessionwire.ErrorCode{
		sessionwire.ErrorCodeInvalidRequest,
		sessionwire.ErrorCodeUnsupportedVersion,
		sessionwire.ErrorCodeSessionNotFound,
		sessionwire.ErrorCodeCommandRejected,
		sessionwire.ErrorCodeGateResolved,
		sessionwire.ErrorCodeGateNotResumable,
		sessionwire.ErrorCodeGateExpired,
		sessionwire.ErrorCodeGateResponseInvalid,
		sessionwire.ErrorCodeRuntimeUnavailable,
	}
	for _, code := range codes {
		t.Run(string(code), func(t *testing.T) {
			t.Parallel()

			f := newEngineFixture(t)
			// The cause is wrapped, as admission wraps its own: a classifier
			// reading the outermost type rather than unwrapping would fail.
			f.admitter.err = fmt.Errorf("routed: %w", &admission.Error{Code: code, Cause: errors.New("a dependency's diagnostic")})

			reply, err := f.admit(t, clientlink.MethodSessionInput, commandBody(clientlink.MethodSessionInput, "session-1", "cmd-1"))
			if err != nil {
				t.Fatalf("Admit = %v, want the refusal as a reply body", err)
			}
			if got := envelopeCode(t, reply); got != code {
				t.Errorf("the refusal carried code %q, want %q", got, code)
			}
			var envelope sessionwire.ErrorEnvelope
			if err := json.Unmarshal(reply, &envelope); err != nil {
				t.Fatalf("unmarshal reply: %v", err)
			}
			if envelope.Error.Retryable {
				t.Errorf("%q was advertised as retryable; no admission refusal is repeatable without changing the request", code)
			}
			// There is no message at all, which is stronger than "no cause
			// text" and is the property refusalBody documents: the code is the
			// contract, and a message here would be a second prose vocabulary
			// the REST alternative would have to reproduce word for word. It
			// also makes the leak impossible rather than filtered -- a
			// dependency's diagnostic belongs in an operator's log.
			if envelope.Error.Message != "" {
				t.Errorf("the refusal carries the message %q; the code is the whole contract", envelope.Error.Message)
			}
			if strings.Contains(string(reply), "diagnostic") {
				t.Errorf("the refusal %s carries the cause's text", reply)
			}
		})
	}
}

// TestAFaultIsNotARefusal separates "admission decided" from "nobody knows".
//
// An error admission did not classify -- a cancelled context, a closing store,
// a provider outage -- must not be dressed as one of the nine public codes. A
// client told its command was REJECTED stops retrying; a client told the link
// failed retries with the same CommandID, which is the whole point of having
// one.
func TestAFaultIsNotARefusal(t *testing.T) {
	t.Parallel()

	for name, cause := range map[string]error{
		"a cancelled context":     context.Canceled,
		"an expired deadline":     context.DeadlineExceeded,
		"a closing store":         &sessionstore.StoreClosedError{},
		"an unclassified failure": errors.New("connection refused"),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			f := newEngineFixture(t)
			f.admitter.err = cause

			reply, err := f.admit(t, clientlink.MethodSessionInput, commandBody(clientlink.MethodSessionInput, "session-1", "cmd-1"))
			if err == nil {
				t.Fatalf("Admit returned the reply %s, want %v as an error", reply, cause)
			}
			if !errors.Is(err, cause) {
				t.Errorf("Admit error %v does not wrap %v", err, cause)
			}
			if reply != nil {
				t.Errorf("Admit returned a body %s beside an error; a fault has no public answer", reply)
			}
		})
	}
}

// TestAnUnknownMethodIsRefusedBeforeAnythingElse holds the order Admit
// documents. There is no command kind to ask about and no Core type to decode
// as, so a method outside the vocabulary must reach neither.
func TestAnUnknownMethodIsRefusedBeforeAnythingElse(t *testing.T) {
	t.Parallel()

	for _, method := range []clientlink.Method{"", "session", "session.destroy", "SESSION.INPUT", "session.input "} {
		f := newEngineFixture(t)
		reply, err := f.admit(t, method, commandBody(clientlink.MethodSessionInput, "session-1", "cmd-1"))
		if !errors.Is(err, clientlink.ErrUnknownMethod) {
			t.Errorf("Admit(%q) = (%s, %v), want ErrUnknownMethod", method, reply, err)
		}
		if reply != nil {
			t.Errorf("Admit(%q) returned a body %s; an unknown method has no admission answer", method, reply)
		}
		if calls := f.authorizer.controlCalls(); len(calls) != 0 {
			t.Errorf("Admit(%q) consulted the authorizer %d times", method, len(calls))
		}
		if calls := f.admitter.recorded(); len(calls) != 0 {
			t.Errorf("Admit(%q) reached admission %d times", method, len(calls))
		}
	}
}

// TestADeniedCommandNeverReachesAdmission is the arm the recording authorizer's
// refusal covers: a decision consulted and discarded would leave every case
// above passing.
func TestADeniedCommandNeverReachesAdmission(t *testing.T) {
	t.Parallel()

	f := newEngineFixture(t)
	f.admitter.entry = acceptedEntry("session-1", "cmd-2")
	f.authorizer.mu.Lock()
	f.authorizer.denyKind = command.KindInput
	f.authorizer.mu.Unlock()

	reply, err := f.admit(t, clientlink.MethodSessionInput, commandBody(clientlink.MethodSessionInput, "session-1", "cmd-1"))
	if !errors.Is(err, internalidentity.ErrUnauthorized) {
		t.Fatalf("Admit = (%s, %v), want the authorizer's refusal", reply, err)
	}
	if reply != nil {
		t.Errorf("a denied command was answered with the body %s", reply)
	}
	if calls := f.admitter.recorded(); len(calls) != 0 {
		t.Errorf("a denied command reached admission %d times", len(calls))
	}
	// The sibling kind, denied by nothing, still admits: otherwise "denied"
	// would be indistinguishable from "nothing is admitted".
	if _, err := f.admit(t, clientlink.MethodSessionInterrupt, commandBody(clientlink.MethodSessionInterrupt, "session-1", "cmd-2")); err != nil {
		t.Errorf("an allowed interrupt = %v, want an admitted reply", err)
	}
}

// ---------------------------------------------------------------------------
// Step 1 -- the accepted record, and the idempotency it carries.
// ---------------------------------------------------------------------------

// TestTheReplyDescribesTheDurableRecord pins every member of the projection
// against absolute literals, and sweeps the durable states.
func TestTheReplyDescribesTheDurableRecord(t *testing.T) {
	t.Parallel()

	rejection := &sessionwire.ErrorDetail{Code: sessionwire.ErrorCodeCommandRejected, Message: "the command cannot be applied"}
	for _, tt := range []struct {
		name    string
		state   sessionstore.InboxState
		detail  *sessionwire.ErrorDetail
		want    sessionwire.CommandState
		wantErr bool
	}{
		{name: "pending", state: sessionstore.InboxStatePending, want: sessionwire.CommandStateAccepted},
		{name: "claimed", state: sessionstore.InboxStateClaimed, want: sessionwire.CommandStateAccepted},
		{name: "applying", state: sessionstore.InboxStateApplying, want: sessionwire.CommandStateAccepted},
		{name: "applied", state: sessionstore.InboxStateApplied, want: sessionwire.CommandStateApplied},
		{name: "rejected", state: sessionstore.InboxStateRejected, detail: rejection, want: sessionwire.CommandStateRejected},
		// A rejected record with no detail cannot be described publicly, and a
		// state this build does not know must not be reported optimistically.
		{name: "rejected with no detail", state: sessionstore.InboxStateRejected, wantErr: true},
		{name: "a state this build does not know", state: sessionstore.InboxState("quarantined"), wantErr: true},
		{name: "no state at all", state: "", wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newEngineFixture(t)
			entry := acceptedEntry("session-1", "cmd-77")
			entry.Record.State = tt.state
			entry.Record.Rejection = tt.detail
			f.admitter.entry = entry

			reply, err := f.admit(t, clientlink.MethodSessionInput, commandBody(clientlink.MethodSessionInput, "session-1", "cmd-77"))
			if tt.wantErr {
				if !errors.Is(err, clientlink.ErrUnreadableRecord) {
					t.Fatalf("Admit = (%s, %v), want ErrUnreadableRecord", reply, err)
				}
				if reply != nil {
					t.Errorf("an undescribable record was answered with %s", reply)
				}
				return
			}
			if err != nil {
				t.Fatalf("Admit = %v, want a reply", err)
			}
			status := statusOf(t, reply)
			if status.CommandID != "cmd-77" {
				t.Errorf("command_id = %q, want %q", status.CommandID, "cmd-77")
			}
			if status.State != tt.want {
				t.Errorf("status = %q, want %q", status.State, tt.want)
			}
			if status.AcceptedOrder != 4242 {
				t.Errorf("accepted_order = %d, want 4242", status.AcceptedOrder)
			}
			if tt.detail == nil && status.Error != nil {
				t.Errorf("the reply carries an error %+v for a %s record", status.Error, tt.state)
			}
			if tt.detail != nil && (status.Error == nil || status.Error.Code != tt.detail.Code) {
				t.Errorf("the reply carries error %+v, want code %q", status.Error, tt.detail.Code)
			}
		})
	}
}

// TestTheReplyIsTheRecordAndNotTheCall is the idempotency property at this
// edge: admission's "this call accepted it" answer is not readable from the
// reply, so an original acceptance and a retry that found the stored record are
// the same bytes.
//
// If they differed, a client could learn whether its earlier attempt landed --
// which is exactly the knowledge a durable CommandID exists to make unnecessary.
func TestTheReplyIsTheRecordAndNotTheCall(t *testing.T) {
	t.Parallel()

	entry := acceptedEntry("session-1", "cmd-1")
	body := commandBody(clientlink.MethodSessionInput, "session-1", "cmd-1")

	original := newEngineFixture(t)
	original.admitter.entry, original.admitter.created = entry, true
	first, err := original.admit(t, clientlink.MethodSessionInput, body)
	if err != nil {
		t.Fatalf("the original acceptance = %v", err)
	}

	retry := newEngineFixture(t)
	retry.admitter.entry, retry.admitter.created = entry, false
	second, err := retry.admit(t, clientlink.MethodSessionInput, body)
	if err != nil {
		t.Fatalf("the retry = %v", err)
	}
	if string(first) != string(second) {
		t.Errorf("the retry was answered with %s and the original with %s; a client can tell them apart", second, first)
	}
}

// ---------------------------------------------------------------------------
// The seam, audited against the service it stands for.
// ---------------------------------------------------------------------------

// TestTheAdmissionServiceIsExactlyTheSeamThisEdgeCalls audits the fake in both
// directions, by deriving the expected seam from the SERVICE rather than from a
// list this file maintains.
//
// A hand-written list cannot fail for a method that did not exist when it was
// written, which is precisely the case that matters: an admission task adding a
// sixth V1 command would leave the ClientLink unable to serve it, silently.
func TestTheAdmissionServiceIsExactlyTheSeamThisEdgeCalls(t *testing.T) {
	t.Parallel()

	// The compile-time half: the real service satisfies the seam with no
	// adapter, so nothing about the RPC path is shaped by the fake.
	var _ clientlink.Admitter = (*admission.Service)(nil)

	service := reflect.TypeOf((*admission.Service)(nil))
	seam := reflect.TypeOf((*clientlink.Admitter)(nil)).Elem()

	// AdmitLegacyCreate is the ONE V1-shaped exclusion, and it is named rather
	// than filtered by a pattern: it mints identities server-side and keeps the
	// legacy unknown-outcome limitation, which is the property step 3 requires
	// a ClientLink command not to have.
	const legacy = "AdmitLegacyCreate"
	want := map[string]bool{}
	for i := range service.NumMethod() {
		name := service.Method(i).Name
		if strings.HasPrefix(name, "Admit") && name != legacy {
			want[name] = true
		}
	}
	if len(want) == 0 {
		t.Fatal("the service declares no admission methods, so this comparison is vacuous")
	}
	got := map[string]bool{}
	for i := range seam.NumMethod() {
		got[seam.Method(i).Name] = true
	}
	for name := range want {
		if !got[name] {
			t.Errorf("admission.Service declares %s, which the ClientLink seam does not call: an RPC cannot reach it", name)
		}
	}
	for name := range got {
		if !want[name] {
			t.Errorf("the ClientLink seam declares %s, which admission.Service does not implement", name)
		}
	}
	if got[legacy] {
		t.Errorf("the ClientLink seam declares %s; a ClientLink command may not mint its own identities", legacy)
	}
}
