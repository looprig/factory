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
	"time"

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
	// ctx is the context the edge handed the service. The fake used to discard
	// it, which made it looser than the real service in exactly the dimension
	// the command bound lives in -- the gate's class-4 finding. It is kept so
	// the deadline has a reader.
	ctx context.Context
}

// deadline reports the bound the edge imposed on this admission.
func (c admitCall) deadline() (time.Time, bool) {
	if c.ctx == nil {
		return time.Time{}, false
	}
	return c.ctx.Deadline()
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
	gate chan struct{}
	// waitForContext makes the fake behave like a dependency that honours the
	// context it is given: it returns when that context ends, and reports the
	// context's own error.
	//
	// A case that sets it MUST assert unbounded() is false. Without that the
	// backstop below silently stands in for the property under test, which is
	// the masking bug this whole mechanism exists to avoid; both current call
	// sites assert it, and a gate confirmed the flag is genuinely read.
	waitForContext bool
	// selfReleased records that the BACKSTOP released the admission instead of
	// the context. It is the difference between "the bound worked" and "the
	// fake gave up", and it exists because the first version of this fake had
	// no backstop at all: a mutation removing the bound then hung the whole
	// package until Go's ten-minute timeout, which is a kill by hang and is not
	// an assertion kill. A backstop converts that into a clean assertion, and
	// the flag is what a case reads so the backstop cannot silently stand in
	// for the property under test.
	selfReleased bool
	calls        []admitCall
}

// admitterBackstop is the wall-clock ceiling on a context-honouring fake.
//
// It is 50x to 100x every CommandTimeout any case configures -- 5s against
// 100ms and 50ms -- so on a green tree the context always wins and nothing here
// depends on the machine being fast. (It said "two orders of magnitude", which
// is true of one bound and not the other; in a round about narrowing claims,
// that one is now stated as measured.) It is reached only when NOTHING bounded
// the admission, which is exactly the condition a case must fail on rather than
// wait out.
const admitterBackstop = 5 * time.Second

func (a *recordingAdmitter) record(ctx context.Context, method clientlink.Method, principal identity.Principal, request any) (sessionstore.InboxEntry, bool, error) {
	a.mu.Lock()
	a.calls = append(a.calls, admitCall{method: method, principal: principal, request: request, ctx: ctx})
	gate, wait, entry, created, err := a.gate, a.waitForContext, a.entry, a.created, a.err
	a.mu.Unlock()
	if gate != nil {
		<-gate
	}
	if wait {
		timer := time.NewTimer(admitterBackstop)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return sessionstore.InboxEntry{}, false, ctx.Err()
		case <-timer.C:
			a.mu.Lock()
			a.selfReleased = true
			a.mu.Unlock()
			return sessionstore.InboxEntry{}, false, errAdmissionUnbounded
		}
	}
	return entry, created, err
}

// errAdmissionUnbounded is what the fake returns when it had to release itself.
var errAdmissionUnbounded = errors.New("nothing bounded this admission; the fake released itself")

// unbounded reports that the backstop, not the context, ended an admission.
func (a *recordingAdmitter) unbounded() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.selfReleased
}

func (a *recordingAdmitter) recorded() []admitCall {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.calls)
}

func (a *recordingAdmitter) AdmitCreate(ctx context.Context, p identity.Principal, req sessionwire.CreateRequest) (sessionstore.InboxEntry, bool, error) {
	return a.record(ctx, clientlink.MethodSessionCreate, p, req)
}

func (a *recordingAdmitter) AdmitInput(ctx context.Context, p identity.Principal, req sessionwire.InputRequest) (sessionstore.InboxEntry, bool, error) {
	return a.record(ctx, clientlink.MethodSessionInput, p, req)
}

func (a *recordingAdmitter) AdmitInterrupt(ctx context.Context, p identity.Principal, req sessionwire.InterruptRequest) (sessionstore.InboxEntry, bool, error) {
	return a.record(ctx, clientlink.MethodSessionInterrupt, p, req)
}

func (a *recordingAdmitter) AdmitRestore(ctx context.Context, p identity.Principal, req sessionwire.RestoreRequest) (sessionstore.InboxEntry, bool, error) {
	return a.record(ctx, clientlink.MethodSessionRestore, p, req)
}

func (a *recordingAdmitter) AdmitGateResponse(ctx context.Context, p identity.Principal, req sessionwire.GateResponseRequest) (sessionstore.InboxEntry, bool, error) {
	return a.record(ctx, clientlink.MethodGateRespond, p, req)
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
	demand     *recordingDemand
	clock      *manualClock
	principal  identity.Principal
}

func newEngineFixture(t *testing.T) *engineFixture {
	t.Helper()

	return newEngineFixtureWithLimits(t, testLimits())
}

func newEngineFixtureWithLimits(t *testing.T, limits clientlink.Limits) *engineFixture {
	t.Helper()

	principal, err := identity.NewPrincipal(tenantA, "user-a", identity.KindActor)
	if err != nil {
		t.Fatalf("NewPrincipal: %v", err)
	}
	f := &engineFixture{
		authorizer: &recordingAuthorizer{},
		admitter:   &recordingAdmitter{created: true},
		demand:     &recordingDemand{},
		clock:      &manualClock{},
		principal:  principal,
	}
	engine, err := clientlink.NewEngine(clientlink.Config{
		Authenticator: fixedAuthenticator{principal: principal},
		Authorizer:    f.authorizer,
		Admitter:      f.admitter,
		Demand:        f.demand,
		Clock:         f.clock,
		Limits:        limits,
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

	// The expected set is derived from the SIGNATURE, not from the name, and
	// that is the gate's finding: a prefix rule cannot require a future V1
	// command method that happens not to be called Admit*, which is the same
	// class-6 hole this test closes everywhere else. A V1 admission is exactly
	// "(context, Principal, <one Core request type>) -> (InboxEntry, bool,
	// error)", and nothing else on the service has that shape.
	//
	// AdmitLegacyCreate is excluded by shape rather than by name -- it takes
	// admission's own LegacyCreateRequest and returns a LegacyCreateResult, so
	// it never matches. It is ALSO named below, because the reason it must stay
	// off this seam is a decision rather than an accident of its signature: it
	// mints identities server-side and keeps the legacy unknown-outcome
	// limitation, which is the property step 3 requires a ClientLink command
	// not to have. If a later change gave it the V1 shape, the named check is
	// what would still refuse it.
	const legacy = "AdmitLegacyCreate"
	ctxType := reflect.TypeOf((*context.Context)(nil)).Elem()
	principalType := reflect.TypeOf(identity.Principal{})
	entryType := reflect.TypeOf(sessionstore.InboxEntry{})
	errorType := reflect.TypeOf((*error)(nil)).Elem()
	isV1Admission := func(fn reflect.Type) bool {
		// NumIn counts the receiver on a method obtained from the type.
		if fn.NumIn() != 4 || fn.NumOut() != 3 {
			return false
		}
		return fn.In(1) == ctxType && fn.In(2) == principalType &&
			fn.Out(0) == entryType && fn.Out(1) == reflect.TypeOf(false) && fn.Out(2) == errorType
	}
	want := map[string]bool{}
	for i := range service.NumMethod() {
		method := service.Method(i)
		if isV1Admission(method.Type) && method.Name != legacy {
			want[method.Name] = true
		}
	}
	if len(want) == 0 {
		t.Fatal("the service declares no V1 admission methods, so this comparison is vacuous")
	}
	// The shape rule needs a positive control over known-answer inputs, and the
	// first version of this check was not one: it asked whether `want` held
	// AdmitLegacyCreate, which the loop above had already excluded BY NAME, so
	// the map could never contain that key and the guard could never fire. A
	// gate then gutted isV1Admission to two of its SEVEN conditions and the
	// suite passed. That is the same class-6 defect this test exists to close,
	// in the check written to prove it was closed.
	//
	// So the rule is driven against the real method it must reject, looked up
	// OUTSIDE the name filter, and against a table of near misses.
	//
	// What is claimed for the table is that every condition in isV1Admission has
	// at least one row that fails without it, established by neutralising each
	// condition in turn. What is NOT claimed is that each row differs in exactly
	// one dimension: dropping an argument or a result shifts every position
	// after it, so MissingPrincipal and MissingCreated each differ in two.
	legacyMethod, found := service.MethodByName(legacy)
	if !found {
		t.Fatalf("%s is not declared on the service, so the exclusion below proves nothing", legacy)
	}
	if isV1Admission(legacyMethod.Type) {
		t.Errorf("the shape rule matches %s, so it is not distinguishing a V1 admission from a legacy one", legacy)
	}
	for name, probe := range map[string]any{
		"the exact V1 shape":       shapeProbeType.AdmitExact,
		"one argument short":       shapeProbeType.MissingPrincipal,
		"one argument too many":    shapeProbeType.ExtraArgument,
		"no context":               shapeProbeType.NoContext,
		"a principal by pointer":   shapeProbeType.PointerPrincipal,
		"one result short":         shapeProbeType.MissingCreated,
		"one result too many":      shapeProbeType.ExtraResult,
		"a catalog entry returned": shapeProbeType.WrongEntry,
		"a non-boolean second":     shapeProbeType.WrongCreated,
		"a non-error third":        shapeProbeType.WrongError,
	} {
		want := name == "the exact V1 shape"
		if got := isV1Admission(reflect.TypeOf(probe)); got != want {
			t.Errorf("isV1Admission(%s) = %t, want %t", name, got, want)
		}
	}
	got := map[string]bool{}
	for i := range seam.NumMethod() {
		got[seam.Method(i).Name] = true
	}
	for name := range want {
		if !got[name] {
			t.Errorf("admission.Service declares %s with the V1 admission shape, which the ClientLink seam does not call: an RPC cannot reach it", name)
		}
	}
	for name := range got {
		if !want[name] {
			t.Errorf("the ClientLink seam declares %s, which admission.Service does not implement with the V1 admission shape", name)
		}
	}
	if got[legacy] {
		t.Errorf("the ClientLink seam declares %s; a ClientLink command may not mint its own identities", legacy)
	}
}

// ---------------------------------------------------------------------------
// The command bound.
// ---------------------------------------------------------------------------

// TestAnAdmissionIsBoundedByTheConfiguredCommandTimeout is the reader for the
// only bound that exists on a durable admission.
//
// # Why a bound has to be here, and what it is worth
//
// It was documented as "bounded by the LINK's lifetime". Measured on the pinned
// transport, it was bounded by NOTHING. centrifuge@v0.38.0 dispatches an RPC
// SYNCHRONOUSLY on the connection's read loop (client.go:1385 -> 2259), and the
// connection context is cancelled by the websocket handler's
// `defer close(ctxCh)` (handler_websocket.go:218-222) -- that is, when the read
// loop RETURNS. An in-flight admission is the very thing keeping the loop from
// returning, so a disconnect cannot cancel it, and neither can Shutdown.
//
// Two consequences made this a hazard rather than a wrong sentence: a wedged
// store hangs one link with no bound, and because dispatch is serial, it
// head-of-line blocks every other frame on that link -- so a replica cannot be
// drained while one admission is stuck.
//
// # What the bound can and cannot promise
//
// It is a DEADLINE on the context, not a guillotine on the reply, and the limit
// is the ordinary contract of a context seam -- the same one httpapi.RouteLimits
// states for its own: an Admitter that honours the context it is given returns
// at the deadline, and one that ignores it is not bounded by anything here.
// That is why the case below drives BOTH: a ctx-honouring admitter, which is
// released, and the deadline's presence on the context itself, which is what a
// well-behaved dependency reads.
func TestAnAdmissionIsBoundedByTheConfiguredCommandTimeout(t *testing.T) {
	t.Parallel()

	t.Run("the context handed to admission carries the deadline", func(t *testing.T) {
		t.Parallel()

		f := newEngineFixture(t)
		f.admitter.entry = acceptedEntry("session-1", "cmd-1")
		before := time.Now()
		if _, err := f.admit(t, clientlink.MethodSessionInput, commandBody(clientlink.MethodSessionInput, "session-1", "cmd-1")); err != nil {
			t.Fatalf("Admit = %v", err)
		}
		after := time.Now()
		calls := f.admitter.recorded()
		if len(calls) != 1 {
			t.Fatalf("%d admissions, want 1", len(calls))
		}
		deadline, ok := calls[0].deadline()
		if !ok {
			t.Fatal("the context handed to admission carries no deadline, so nothing bounds a wedged dependency")
		}
		// The window is bracketed by two readings of the clock taken around the
		// call, so neither bound can fail because the machine was slow: the
		// deadline may not be earlier than a clock read BEFORE the call plus
		// the window, nor later than one taken AFTER it plus the window. A
		// bound of some other duration -- twice the window, a hard-coded
		// minute, no window at all -- falls outside on one side or the other.
		window := testLimits().CommandTimeout
		if earliest := before.Add(window); deadline.Before(earliest) {
			t.Errorf("the deadline is %v, earlier than the configured window allows (%v)", deadline, earliest)
		}
		if latest := after.Add(window); deadline.After(latest) {
			t.Errorf("the deadline is %v, later than the configured window allows (%v)", deadline, latest)
		}
	})

	t.Run("an admission that outlives the bound is released as a fault", func(t *testing.T) {
		t.Parallel()

		limits := testLimits()
		limits.CommandTimeout = 50 * time.Millisecond
		f := newEngineFixtureWithLimits(t, limits)
		// An admitter that waits for its context, which is what a dependency
		// honouring the seam does. Nothing releases it but the deadline.
		f.admitter.mu.Lock()
		f.admitter.waitForContext = true
		f.admitter.mu.Unlock()

		reply, err := f.admit(t, clientlink.MethodSessionInput, commandBody(clientlink.MethodSessionInput, "session-1", "cmd-1"))
		if err == nil {
			t.Fatalf("Admit returned the reply %s, want the deadline as an error", reply)
		}
		// Which side released it is the assertion. The fake carries a backstop
		// so that an engine imposing NO bound fails here in seconds instead of
		// hanging the package until Go's ten-minute timeout -- a hang is not an
		// assertion kill, and a probe removing the bound produced exactly one
		// before this backstop existed.
		if f.admitter.unbounded() {
			t.Fatal("the admission was released by the fake's own backstop, so nothing in the engine bounded it")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Admit error %v does not wrap context.DeadlineExceeded", err)
		}
		// It is a FAULT, not a refusal: the command may or may not have landed,
		// which is the unknown outcome the durable CommandID exists for. A
		// public code here would tell the client to stop retrying.
		if reply != nil {
			t.Errorf("a bounded-out admission was answered with the body %s", reply)
		}
	})

	// The AUTHORIZER half. The bound is placed above AuthorizeControl rather
	// than below it, and that placement was a claim with no reader: moving the
	// deadline below the authorization call was measured surviving the whole
	// module suite. A wedged authorizer produces the identical hazard to a
	// wedged store -- it holds the serial read loop and defeats Shutdown -- and
	// it is at least as plausible a dependency, since it is a seam a deployer
	// supplies and A9.1 composes.
	t.Run("a wedged authorizer is bounded too", func(t *testing.T) {
		t.Parallel()

		limits := testLimits()
		limits.CommandTimeout = 50 * time.Millisecond
		f := newEngineFixtureWithLimits(t, limits)
		f.authorizer.mu.Lock()
		f.authorizer.waitForContext = true
		f.authorizer.mu.Unlock()

		reply, err := f.admit(t, clientlink.MethodSessionInput, commandBody(clientlink.MethodSessionInput, "session-1", "cmd-1"))
		if err == nil {
			t.Fatalf("Admit returned the reply %s, want the deadline as an error", reply)
		}
		if f.authorizer.unbounded() {
			t.Fatal("the authorization decision was released by the fake's own backstop, so the bound does not cover it")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Admit error %v does not wrap context.DeadlineExceeded", err)
		}
		// It never reached admission, which is the other half of the placement:
		// a command whose authorization never completed must not be admitted.
		if calls := f.admitter.recorded(); len(calls) != 0 {
			t.Errorf("a bounded-out authorization still reached admission %d times", len(calls))
		}
	})

	t.Run("the bound does not shorten an admission that answers", func(t *testing.T) {
		t.Parallel()

		// The control. Without it, every assertion above is also satisfied by
		// an engine that refuses every command immediately.
		f := newEngineFixture(t)
		f.admitter.entry = acceptedEntry("session-1", "cmd-1")
		reply, err := f.admit(t, clientlink.MethodSessionInput, commandBody(clientlink.MethodSessionInput, "session-1", "cmd-1"))
		if err != nil {
			t.Fatalf("Admit = %v, want the accepted record", err)
		}
		if statusOf(t, reply).CommandID != "cmd-1" {
			t.Errorf("the reply names %q", statusOf(t, reply).CommandID)
		}
	})
}

// TestARefusalWithNoCodeIsAFaultNotAnEmptyReply drives refusalBody's
// marshal-failure arm, which was documented as unreachable and is not.
//
// Core refuses to marshal an ErrorDetail with an empty code, and an empty code
// is a value admission can produce: (*admission.Error).Error() contemplates
// Code == "" explicitly, so a zero-valued refusal from any admission path lands
// here. The answer that matters is what a client would otherwise get -- a
// SUCCESSFUL RPC carrying an empty body, which is neither a status nor an
// envelope and which no consumer can classify.
func TestARefusalWithNoCodeIsAFaultNotAnEmptyReply(t *testing.T) {
	t.Parallel()

	f := newEngineFixture(t)
	f.admitter.err = &admission.Error{}

	reply, err := f.admit(t, clientlink.MethodSessionInput, commandBody(clientlink.MethodSessionInput, "session-1", "cmd-1"))
	if !errors.Is(err, clientlink.ErrUnreadableRecord) {
		t.Fatalf("Admit = (%s, %v), want ErrUnreadableRecord", reply, err)
	}
	if reply != nil {
		t.Errorf("a codeless refusal was answered with the body %q, which a client cannot classify", reply)
	}

	// The positive control on the same path: a refusal that DOES carry a code
	// is still answered as an envelope, so the arm above is reached by the
	// missing code rather than by refusals having stopped working.
	control := newEngineFixture(t)
	control.admitter.err = &admission.Error{Code: sessionwire.ErrorCodeCommandRejected}
	body, err := control.admit(t, clientlink.MethodSessionInput, commandBody(clientlink.MethodSessionInput, "session-1", "cmd-1"))
	if err != nil {
		t.Fatalf("a coded refusal = %v, want an envelope", err)
	}
	if got := envelopeCode(t, body); got != sessionwire.ErrorCodeCommandRejected {
		t.Errorf("the control refusal carried %q", got)
	}
}

// shapeProbe supplies known-answer inputs for isV1Admission.
//
// Each method differs from the V1 admission shape in exactly ONE dimension, so
// a rule that dropped any single condition fails on one row rather than on all
// of them. They are reached as METHOD EXPRESSIONS, which produce a func whose
// first parameter is the receiver -- the same shape reflect.Type.Method yields,
// which is why isV1Admission counts from index 1.
type shapeProbeType struct{}

func (shapeProbeType) AdmitExact(context.Context, identity.Principal, sessionwire.InputRequest) (sessionstore.InboxEntry, bool, error) {
	return sessionstore.InboxEntry{}, false, nil
}

func (shapeProbeType) MissingPrincipal(context.Context, sessionwire.InputRequest) (sessionstore.InboxEntry, bool, error) {
	return sessionstore.InboxEntry{}, false, nil
}

func (shapeProbeType) ExtraArgument(context.Context, identity.Principal, sessionwire.InputRequest, string) (sessionstore.InboxEntry, bool, error) {
	return sessionstore.InboxEntry{}, false, nil
}

func (shapeProbeType) NoContext(string, identity.Principal, sessionwire.InputRequest) (sessionstore.InboxEntry, bool, error) {
	return sessionstore.InboxEntry{}, false, nil
}

func (shapeProbeType) PointerPrincipal(context.Context, *identity.Principal, sessionwire.InputRequest) (sessionstore.InboxEntry, bool, error) {
	return sessionstore.InboxEntry{}, false, nil
}

func (shapeProbeType) MissingCreated(context.Context, identity.Principal, sessionwire.InputRequest) (sessionstore.InboxEntry, error) {
	return sessionstore.InboxEntry{}, nil
}

// ExtraResult is the row the first version of this table did not have, and its
// absence made "every condition has a row that fails without it" false: the
// arity-out check was the one condition with no sole reader.
//
// It has to be a FOURTH result rather than a second short one, and the reason
// given here was wrong until a gate ran the experiment and I re-ran it. It does
// NOT panic: with `NumOut() != 3` removed, a two-result probe reaches
// `fn.Out(1) == reflect.TypeOf(false)`, which is false for an error, and `&&`
// short-circuits before Out(2). So MissingCreated is rejected by a TYPE check
// even with the arity gone, which makes it no reader at all for the arity.
//
// This probe is. Every result position matches the V1 shape, so nothing but the
// count can reject it: with the arity check removed it is the one row that
// fires -- measured, on this tree, `isV1Admission(one result too many) = true,
// want false`.
//
// The signature has to put error at index 2 with a fourth result after it,
// which is ST1008 ("error should be returned as the last argument").
// That is the point of the probe rather than an oversight: it is a shape the
// rule must REJECT, and a lint that keeps production code from having it is not
// a reason the rule may go unread.
//
//lint:ignore ST1008 this probe exists to be an ill-formed signature the shape rule must reject
func (shapeProbeType) ExtraResult(context.Context, identity.Principal, sessionwire.InputRequest) (sessionstore.InboxEntry, bool, error, string) {
	return sessionstore.InboxEntry{}, false, nil, ""
}

func (shapeProbeType) WrongEntry(context.Context, identity.Principal, sessionwire.InputRequest) (sessionstore.CatalogEntry, bool, error) {
	return sessionstore.CatalogEntry{}, false, nil
}

func (shapeProbeType) WrongCreated(context.Context, identity.Principal, sessionwire.InputRequest) (sessionstore.InboxEntry, string, error) {
	return sessionstore.InboxEntry{}, "", nil
}

func (shapeProbeType) WrongError(context.Context, identity.Principal, sessionwire.InputRequest) (sessionstore.InboxEntry, bool, string) {
	return sessionstore.InboxEntry{}, false, ""
}
