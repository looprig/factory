package factory_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	factory "github.com/looprig/factory"
	"github.com/looprig/factory/identity"
	"github.com/looprig/factory/internal/admission"
	"github.com/looprig/factory/internal/command"
	"github.com/looprig/factory/internal/httpapi"
	internalidentity "github.com/looprig/factory/internal/identity"
	"github.com/looprig/factory/internal/realtime/clientlink"
	"github.com/looprig/sessionstore"
)

// This file is the parity measurement A6.2 asked for and A3.3 owes.
//
// Specification section 8.1 makes the REST control routes and the ClientLink
// RPCs "two alternatives obeying one admission contract". Until A3.3 that was
// unmeasurable in the direction that mattered, because one of the two answered
// 501: every property either edge could state was a property of one edge. Both
// now serve the same four operations over one admission seam, so the claim can
// be driven rather than argued.
//
// # What is measured, and what is deliberately NOT
//
// Measured, for input, interrupt, restore and gate-response:
//
//   - the two edges call the SAME admission method with the SAME Core request
//     value, from bodies that differ only in the transport that carried them;
//   - a classified refusal produces BYTE-IDENTICAL Core error envelopes;
//   - an acceptance produces BYTE-IDENTICAL Core CommandStatus bodies.
//
// Not measured, because the two are not the same surface: the HTTP status, the
// headers, the framing, the authorization call each edge makes for itself. The
// RPC has no status code to compare, which is exactly why the REST status is
// checked against the AUTHORITY here and against the legacy surface in
// internal/httpapi -- the authority is the shared half.
//
// # The create is excluded, and this file says why
//
// There is no create row. Factory cannot author the immutable SessionBinding a
// V1 create reservation carries, and this module had no source for three of its
// four members.
//
// A3.1 SUPPLIED THEM, so the create is served and IS in parityOperations below,
// alongside the other four. Every parity assertion in this file therefore
// covers it. What is kept as its own test is the half the shared table cannot
// express: the create is the one operation whose edges legitimately disagree
// about the shape of a SUCCESS -- 201 with a body over REST, an envelope over
// the RPC -- so TestTheCreateAnswersTheSameRefusalOverBothEdges drives the axis
// where they must still agree exactly, which is a refusal.

// ---------------------------------------------------------------------------
// The shared admitter.
// ---------------------------------------------------------------------------

// parityAdmitter satisfies BOTH edges' seams, and that is the point: one object
// answers both, so a difference in what the edges DID is visible as a
// difference in what it was asked.
type parityAdmitter struct {
	// asked records every call, in Core's own request types.
	asked []any
	// refuse, when set, is the classified refusal every method returns.
	refuse sessionwire.ErrorCode
	// refusing reports whether refuse is armed, so the zero ErrorCode can be
	// armed too.
	refusing bool
	entry    sessionstore.InboxEntry
}

func newParityAdmitter() *parityAdmitter {
	return &parityAdmitter{entry: sessionstore.InboxEntry{
		Record:        sessionstore.InboxRecord{CommandID: "command-a", State: sessionstore.InboxStatePending},
		AcceptedOrder: 11,
	}}
}

func (a *parityAdmitter) answer(req any) (sessionstore.InboxEntry, bool, error) {
	a.asked = append(a.asked, req)
	if a.refusing {
		return sessionstore.InboxEntry{}, false, &admission.Error{Code: a.refuse}
	}
	return a.entry, true, nil
}

// AdmitCreate answers in the DISPOSITION family, because a create is the one
// command that chooses a session's protocol. The projection of the two families
// is internal/command's single authority, so the two edges still compare.
func (a *parityAdmitter) AdmitCreate(_ context.Context, _ identity.Principal, req sessionwire.CreateRequest) (sessionstore.DispositionInboxEntry, bool, error) {
	entry, created, err := a.answer(req)
	if err != nil {
		return sessionstore.DispositionInboxEntry{}, created, err
	}
	return sessionstore.DispositionInboxEntry{
		Record: sessionstore.DispositionInboxRecord{
			Descriptor: sessionstore.DispositionCommandDescriptor{
				PublicCreate: true, TenantID: entry.Record.TenantID, SessionID: entry.Record.SessionID,
				CommandID: entry.Record.CommandID, RuntimeCommandID: entry.Record.RuntimeCommandID,
				Kind: entry.Record.Kind,
			},
			AcceptedAt: entry.Record.AcceptedAt, ApplyDeadline: entry.Record.ApplyDeadline,
			State: entry.Record.State,
		},
		Revision: entry.Revision, AcceptedOrder: entry.AcceptedOrder,
	}, created, nil
}

func (a *parityAdmitter) AdmitInput(_ context.Context, _ identity.Principal, req sessionwire.InputRequest) (sessionstore.InboxEntry, bool, error) {
	return a.answer(req)
}

func (a *parityAdmitter) AdmitInterrupt(_ context.Context, _ identity.Principal, req sessionwire.InterruptRequest) (sessionstore.InboxEntry, bool, error) {
	return a.answer(req)
}

func (a *parityAdmitter) AdmitRestore(_ context.Context, _ identity.Principal, req sessionwire.RestoreRequest) (sessionstore.InboxEntry, bool, error) {
	return a.answer(req)
}

func (a *parityAdmitter) AdmitGateResponse(_ context.Context, _ identity.Principal, req sessionwire.GateResponseRequest) (sessionstore.InboxEntry, bool, error) {
	return a.answer(req)
}

// parityDemand is the ClientLink's other required seam. Nothing here subscribes.
type parityDemand struct{}

func (parityDemand) Acquire(context.Context, sessionwire.TenantID, sessionwire.SessionID) error {
	return nil
}

func (parityDemand) Release(context.Context, sessionwire.TenantID, sessionwire.SessionID) error {
	return nil
}

// ---------------------------------------------------------------------------
// The two edges, built over one admitter.
// ---------------------------------------------------------------------------

const (
	parityOrigin  = "https://app.example.com"
	paritySession = sessionwire.SessionID("session-a")
)

func parityRouter(t *testing.T, admitter *parityAdmitter) *httpapi.Router {
	t.Helper()

	seams := factory.FakeSeams{}
	credentials, err := internalidentity.NewAuthenticator(internalidentity.Config{
		Verifier: factory.FakeVerifier{},
		Clock:    systemClock{},
	})
	if err != nil {
		t.Fatalf("NewAuthenticator: %v", err)
	}
	guard, err := httpapi.NewGuard(httpapi.GuardConfig{
		CSRF:        factory.ValidCSRF(),
		Credentials: credentials,
		Clock:       systemClock{},
	})
	if err != nil {
		t.Fatalf("NewGuard: %v", err)
	}
	router, err := httpapi.NewRouter(httpapi.RouterConfig{
		Credentials: credentials,
		Authorizer:  seams,
		Reads:       seams,
		Directory:   seams,
		Guard:       guard,
		IDs:         factory.FakeUUIDs{},
		Admissions:  admitter,
	})
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	return router
}

// systemClock is the wall clock, because FakeVerifier's expiry is relative to
// it and the guard must agree with the authenticator about now.
type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

func (systemClock) AfterFunc(d time.Duration, f func()) func() bool {
	timer := time.AfterFunc(d, f)
	return timer.Stop
}

func parityEngine(t *testing.T, admitter *parityAdmitter) *clientlink.Engine {
	t.Helper()

	engine, err := clientlink.NewEngine(clientlink.Config{
		Authenticator: parityAuthenticator{},
		Authorizer:    factory.FakeSeams{},
		Admitter:      admitter,
		Demand:        parityDemand{},
		Clock:         factory.FakeClock{},
		// The limits are the composition's own defaults, translated member by
		// member: factory.ClientLinkLimits and clientlink.Limits are two
		// structs, and the test does not need a third opinion about the
		// numbers, only a composition NewEngine accepts.
		Limits:  parityLimits(),
		Version: "parity",
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	return engine
}

func parityLimits() clientlink.Limits {
	defaults := factory.DefaultClientLinkLimits()
	return clientlink.Limits{
		MaxConnections:           defaults.MaxConnections,
		MaxChannelsPerConnection: defaults.MaxChannelsPerConnection,
		PerConnectionQueueBytes:  defaults.PerConnectionQueueBytes,
		WriteTimeout:             defaults.WriteTimeout,
		PingInterval:             defaults.PingInterval,
		PongTimeout:              defaults.PongTimeout,
		CommandTimeout:           defaults.CommandTimeout,
		DemandReleaseDebounce:    defaults.DemandReleaseDebounce,
		DemandTimeout:            defaults.DemandTimeout,
	}
}

// parityAuthenticator satisfies the ClientLink's handshake seam. Nothing here
// performs a handshake: Engine.Admit takes the principal directly, because the
// authentication is the transport's and the admission is the engine's.
type parityAuthenticator struct{}

func (parityAuthenticator) AuthenticateLink(context.Context, string) (identity.Principal, error) {
	return parityPrincipal()
}

func parityPrincipal() (identity.Principal, error) {
	return identity.NewPrincipal(factory.FakeTenant, factory.FakeSubject, identity.KindActor)
}

// ---------------------------------------------------------------------------
// The four operations, in both spellings.
// ---------------------------------------------------------------------------

// parityOperation is one operation named on both surfaces, with the ONE body
// both edges are given.
//
// The body is one string, not two, and that is load-bearing: two bodies could
// differ in a member and the two edges would then be compared on two different
// commands. The REST route additionally carries the identifiers in its PATH,
// which is the only difference between the two requests.
type parityOperation struct {
	name   string
	method clientlink.Method
	path   string
	body   string
}

func parityOperations() []parityOperation {
	sid := string(paritySession)
	return []parityOperation{
		{
			// The create, added by A3.1. It is the one operation whose REST
			// path names no session -- the caller chooses one in the body --
			// and the one that answers 201 rather than 200, because it is the
			// only control that brings a resource into existence.
			name: "create", method: clientlink.MethodSessionCreate,
			path: "/v1/sessions",
			body: fmt.Sprintf(`{"version":%d,"command_id":"command-a","session_id":%q,"agent_id":"agent-a"}`,
				sessionwire.CurrentWireVersion, sid),
		},
		{
			name: "input", method: clientlink.MethodSessionInput,
			path: "/v1/sessions/" + sid + "/input",
			body: fmt.Sprintf(`{"version":%d,"command_id":"command-a","session_id":%q,"blocks":[{"type":"text","text":"hi"}]}`,
				sessionwire.CurrentWireVersion, sid),
		},
		{
			name: "interrupt", method: clientlink.MethodSessionInterrupt,
			path: "/v1/sessions/" + sid + "/interrupt",
			body: fmt.Sprintf(`{"version":%d,"command_id":"command-a","session_id":%q}`,
				sessionwire.CurrentWireVersion, sid),
		},
		{
			name: "restore", method: clientlink.MethodSessionRestore,
			path: "/v1/sessions/" + sid + "/restore",
			body: fmt.Sprintf(`{"version":%d,"command_id":"command-a","session_id":%q}`,
				sessionwire.CurrentWireVersion, sid),
		},
		{
			name: "gate response", method: clientlink.MethodGateRespond,
			path: "/v1/sessions/" + sid + "/gates/gate-a",
			body: fmt.Sprintf(`{"version":%d,"command_id":"command-a","session_id":%q,"gate_id":"gate-a",`+
				`"action":"submit","values":{"answer":"yes"},"expected_open_event_id":"event-a"}`,
				sessionwire.CurrentWireVersion, sid),
		},
	}
}

func (op parityOperation) overREST(t *testing.T, router *httpapi.Router) *httptest.ResponseRecorder {
	t.Helper()

	r := httptest.NewRequest(http.MethodPost, parityOrigin+op.path, strings.NewReader(op.body))
	r.Host = "app.example.com"
	r.Header.Set("Authorization", "Bearer "+factory.FakeCredentialValue)
	r.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, r)
	return recorder
}

func (op parityOperation) overRPC(t *testing.T, engine *clientlink.Engine) []byte {
	t.Helper()

	principal, err := parityPrincipal()
	if err != nil {
		t.Fatalf("NewPrincipal: %v", err)
	}
	reply, err := engine.Admit(t.Context(), principal, op.method, []byte(op.body))
	if err != nil {
		t.Fatalf("the RPC answered a transport error rather than a body: %v", err)
	}
	return reply
}

// ---------------------------------------------------------------------------
// The measurement.
// ---------------------------------------------------------------------------

// TestBothEdgesAdmitTheIdenticalCommand is the first half: the SERVICE cannot
// tell the two apart.
//
// The recorded request values are compared with %#v over Core's own types, so a
// member either edge dropped, defaulted or rewrote is a difference. It is what
// makes "one admission contract" a measurement rather than a shape: an edge
// that rebuilt the command from its own path values instead of forwarding the
// caller's body would pass every test written against it alone.
func TestBothEdgesAdmitTheIdenticalCommand(t *testing.T) {
	t.Parallel()

	for _, op := range parityOperations() {
		t.Run(op.name, func(t *testing.T) {
			rest, rpc := newParityAdmitter(), newParityAdmitter()
			op.overREST(t, parityRouter(t, rest))
			op.overRPC(t, parityEngine(t, rpc))

			if len(rest.asked) != 1 || len(rpc.asked) != 1 {
				t.Fatalf("the service was asked %d times over REST and %d over the RPC, want once each",
					len(rest.asked), len(rpc.asked))
			}
			if got, want := fmt.Sprintf("%#v", rest.asked[0]), fmt.Sprintf("%#v", rpc.asked[0]); got != want {
				t.Errorf("the two edges admitted different commands:\n REST %s\n  RPC %s", got, want)
			}
		})
	}
}

// TestBothEdgesAnswerAnAcceptanceWithTheSameBody is the second half for the
// success path. The status differs by transport and the BODY must not: it is
// Core's CommandStatus, projected by one function.
func TestBothEdgesAnswerAnAcceptanceWithTheSameBody(t *testing.T) {
	t.Parallel()

	for _, op := range parityOperations() {
		t.Run(op.name, func(t *testing.T) {
			rest, rpc := newParityAdmitter(), newParityAdmitter()
			recorder := op.overREST(t, parityRouter(t, rest))
			reply := op.overRPC(t, parityEngine(t, rpc))

			if recorder.Code/100 != 2 {
				t.Fatalf("the REST route answered %d (%s), want a success", recorder.Code, recorder.Body)
			}
			if recorder.Body.String() != string(reply) {
				t.Errorf("the two edges described one durable record differently:\n REST %s\n  RPC %s",
					recorder.Body, reply)
			}
			// The positive control: a body that describes nothing would also be
			// two equal strings.
			var status sessionwire.CommandStatus
			if err := json.Unmarshal(reply, &status); err != nil {
				t.Fatalf("the shared body is not a Core CommandStatus: %v", err)
			}
			if status.CommandID != "command-a" || status.State != sessionwire.CommandStateAccepted {
				t.Fatalf("the shared body describes %+v, which is not the admitted record", status)
			}
		})
	}
}

// TestBothEdgesRefuseWithTheIdenticalEnvelope is the measurement the
// A3.3-retryable open item asked for, and it sweeps the authority's OWN code
// set rather than a list beside it.
//
// Byte identity is the assertion, not equality of the code: the code was
// already carried whole by both edges, and what diverged was everything around
// it. `retryable` is the member the open item named -- a 503 mapping over REST
// would make the identical refusal retryable here and not there -- and
// `message` is the one A6.2 named in advance, since prose at either edge is a
// second vocabulary the other would have to reproduce word for word.
func TestBothEdgesRefuseWithTheIdenticalEnvelope(t *testing.T) {
	t.Parallel()

	codes := command.RefusalCodes()
	if len(codes) == 0 {
		t.Fatal("the authority classifies nothing, so this sweep is vacuous")
	}
	for _, op := range parityOperations() {
		for _, code := range codes {
			t.Run(op.name+"/"+string(code), func(t *testing.T) {
				rest, rpc := newParityAdmitter(), newParityAdmitter()
				rest.refuse, rest.refusing = code, true
				rpc.refuse, rpc.refusing = code, true

				recorder := op.overREST(t, parityRouter(t, rest))
				reply := op.overRPC(t, parityEngine(t, rpc))

				if recorder.Body.String() != string(reply) {
					t.Errorf("the two edges refused %q differently:\n REST %s\n  RPC %s",
						code, recorder.Body, reply)
				}
				// The status is the REST edge's alone, so it is checked against
				// the authority rather than against the RPC.
				if want, _ := command.RefusalStatus(code); recorder.Code != want {
					t.Errorf("%q answered %d over REST, want the authority's %d", code, recorder.Code, want)
				}
				var envelope sessionwire.ErrorEnvelope
				if err := json.Unmarshal(reply, &envelope); err != nil {
					t.Fatalf("the shared refusal is not a Core ErrorEnvelope: %v", err)
				}
				if envelope.Error.Code != code {
					t.Fatalf("the shared refusal carries %q, want %q; this row is comparing the wrong thing",
						envelope.Error.Code, code)
				}
				if envelope.Error.Retryable {
					t.Errorf("%q is advertised retryable on both edges; no refusal admission mints is "+
						"repeatable without changing the request", code)
				}
			})
		}
	}
}

// TestTheParityComparisonCanSeeADifference is the positive control for the
// three sweeps above, and it exists because all three are equality assertions:
// two edges that both answered an empty body would satisfy every one of them.
//
// It perturbs each compared quantity by hand and requires the comparison to
// report it.
func TestTheParityComparisonCanSeeADifference(t *testing.T) {
	t.Parallel()

	op := parityOperations()[0]
	admitter := newParityAdmitter()
	accepted := op.overREST(t, parityRouter(t, admitter))
	if accepted.Body.Len() == 0 {
		t.Fatal("the success body is empty, so comparing two of them proves nothing")
	}

	refusing := newParityAdmitter()
	refusing.refuse, refusing.refusing = sessionwire.ErrorCodeCommandRejected, true
	refused := op.overREST(t, parityRouter(t, refusing))
	if refused.Body.String() == accepted.Body.String() {
		t.Fatal("a refusal and an acceptance produce the same bytes, so byte identity is not a measurement")
	}

	// Two DIFFERENT refusals must differ too, or the sweep over the code set is
	// one assertion repeated.
	other := newParityAdmitter()
	other.refuse, other.refusing = sessionwire.ErrorCodeSessionNotFound, true
	if op.overREST(t, parityRouter(t, other)).Body.String() == refused.Body.String() {
		t.Fatal("two different refusal codes produce the same bytes")
	}
}

// TestTheCreateAnswersTheSameRefusalOverBothEdges is what
// TestTheCreateIsTheOneOperationWithoutParity became.
//
// That test held an EXCLUSION: the REST create answered 501 while the RPC
// create reached the service and was refused, and it said that if the binding
// blocker were ever lifted it would fail and the signal would be "add the
// create row to parityOperations rather than delete this". A3.1 lifted it, the
// row is added, and every parity assertion in this file now covers the create
// alongside the other four.
//
// What survives as its own test is the half the shared table cannot express.
// The create is the one operation whose two edges disagree about the SHAPE of a
// success -- 201 over REST, an envelope over the RPC -- so this drives the one
// axis where they must still agree exactly: a REFUSAL. Both edges must answer
// the same classified code for the same refused create.
func TestTheCreateAnswersTheSameRefusalOverBothEdges(t *testing.T) {
	t.Parallel()

	create := parityOperations()[0]
	if create.name != "create" {
		t.Fatalf("the first parity operation is %q; this test drives the create", create.name)
	}
	for _, code := range []sessionwire.ErrorCode{
		sessionwire.ErrorCodeRuntimeUnavailable,
		sessionwire.ErrorCodeCommandRejected,
		sessionwire.ErrorCodeInvalidRequest,
	} {
		t.Run(string(code), func(t *testing.T) {
			admitter := newParityAdmitter()
			admitter.refuse, admitter.refusing = code, true

			recorder := create.overREST(t, parityRouter(t, admitter))
			if recorder.Code == http.StatusCreated {
				t.Fatalf("a refused create answered 201 over REST: %s", recorder.Body)
			}
			var rest sessionwire.ErrorEnvelope
			if err := json.Unmarshal(recorder.Body.Bytes(), &rest); err != nil {
				t.Fatalf("the REST create answered %s, which is not an error envelope: %v", recorder.Body, err)
			}

			reply := create.overRPC(t, parityEngine(t, admitter))
			var rpc sessionwire.ErrorEnvelope
			if err := json.Unmarshal(reply, &rpc); err != nil {
				t.Fatalf("the RPC create answered %s, which is not an error envelope: %v", reply, err)
			}
			if rest.Error.Code != rpc.Error.Code {
				t.Errorf("the two edges refused one create differently: REST %q, RPC %q",
					rest.Error.Code, rpc.Error.Code)
			}
			if rest.Error.Code != code {
				t.Errorf("the edges agreed on %q, which is not the service's refusal %q",
					rest.Error.Code, code)
			}
		})
	}
}
