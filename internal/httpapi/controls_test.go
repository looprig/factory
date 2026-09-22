package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	"github.com/looprig/factory/internal/admission"
	"github.com/looprig/factory/internal/command"
	"github.com/looprig/sessionstore"
)

// ---------------------------------------------------------------------------
// The fakes.
// ---------------------------------------------------------------------------

// admittedCommand is what the fake admitter was asked, recorded as the CORE
// REQUEST VALUE rather than as a summary of it.
//
// The whole request is kept because the property that matters is that the
// bytes a caller sent reach the service unaltered: a fake recording only the
// session and the command id could not tell an edge that forwarded the body
// from one that rebuilt it.
type admittedCommand struct {
	principal identity.Principal
	input     sessionwire.InputRequest
	interrupt sessionwire.InterruptRequest
	restore   sessionwire.RestoreRequest
	gate      sessionwire.GateResponseRequest
	create    sessionwire.CreateRequest
	kind      sessionstore.CommandKind
}

type fakeAdmitter struct {
	calls []admittedCommand
	// entry is what a successful admission returns. Every kind answers in the
	// DISPOSITION family, the only one admission writes.
	entry sessionstore.DispositionInboxEntry
	// err, when set, is returned INSTEAD of entry, after recording the call.
	err error
}

func newFakeAdmitter() *fakeAdmitter {
	return &fakeAdmitter{entry: sessionstore.DispositionInboxEntry{
		Record: sessionstore.DispositionInboxRecord{
			Descriptor: sessionstore.DispositionCommandDescriptor{CommandID: "command-a"},
			State:      sessionstore.InboxStatePending,
		},
		AcceptedOrder: 3,
	}}
}

func (f *fakeAdmitter) record(call admittedCommand) (sessionstore.DispositionInboxEntry, bool, error) {
	f.calls = append(f.calls, call)
	if f.err != nil {
		return sessionstore.DispositionInboxEntry{}, false, f.err
	}
	entry := f.entry
	entry.Record.Descriptor.Kind = call.kind
	return entry, true, nil
}

// AdmitCreate answers with the request's own session, as the store does: a
// create names a session that does not exist yet, so the record can only carry
// the one the caller proposed.
func (f *fakeAdmitter) AdmitCreate(_ context.Context, p identity.Principal, req sessionwire.CreateRequest) (sessionstore.DispositionInboxEntry, bool, error) {
	entry, created, err := f.record(admittedCommand{principal: p, create: req, kind: commandCreate})
	if err != nil {
		return sessionstore.DispositionInboxEntry{}, created, err
	}
	entry.Record.Descriptor.PublicCreate = true
	entry.Record.Descriptor.SessionID = req.SessionID
	return entry, created, nil
}

func (f *fakeAdmitter) AdmitInput(_ context.Context, p identity.Principal, req sessionwire.InputRequest) (sessionstore.DispositionInboxEntry, bool, error) {
	return f.record(admittedCommand{principal: p, input: req, kind: commandInput})
}

func (f *fakeAdmitter) AdmitInterrupt(_ context.Context, p identity.Principal, req sessionwire.InterruptRequest) (sessionstore.DispositionInboxEntry, bool, error) {
	return f.record(admittedCommand{principal: p, interrupt: req, kind: commandInterrupt})
}

func (f *fakeAdmitter) AdmitRestore(_ context.Context, p identity.Principal, req sessionwire.RestoreRequest) (sessionstore.DispositionInboxEntry, bool, error) {
	return f.record(admittedCommand{principal: p, restore: req, kind: commandRestore})
}

func (f *fakeAdmitter) AdmitGateResponse(_ context.Context, p identity.Principal, req sessionwire.GateResponseRequest) (sessionstore.DispositionInboxEntry, bool, error) {
	return f.record(admittedCommand{principal: p, gate: req, kind: commandGateResponse})
}

// storeSettledAttempt is the attempt a store-settled rejection (not_applied)
// carries. Its presence is what makes a rejected disposition record
// undescribable at the edge.
var storeSettledAttempt = &sessionstore.DispositionAttempt{AttemptID: "attempt-1", JournalEpoch: 1, ResidencyEpoch: 1}

type deliveredCommand struct {
	// The context is read AT THE CALL, not retained, because what the seam's
	// contract is about is the context the attempt RUNS on. A retained context
	// is cancelled by deliverAdmitted's own caller returning, whatever it was
	// derived from, so inspecting it afterwards cannot tell a derived context
	// from a locally-bounded one -- measured: a mutant substituting
	// context.WithDeadline(context.Background(), <the same deadline>) survived
	// an after-the-fact Done() check.
	deadline    time.Time
	hasDeadline bool
	errAtCall   error

	tenant   sessionwire.TenantID
	session  sessionwire.SessionID
	delivery sessionwire.HostLinkCommandDelivery
}

type fakeDelivery struct {
	mu    sync.Mutex
	calls []deliveredCommand
	err   error
}

// Deliver captures the CONTEXT as well as the arguments, because the seam's
// contract includes which context the attempt runs on and nothing else could
// see it. See TestTheDeliveryAttemptIsBoundedAndOutlivesItsCaller.
//
// It is called on a wake goroutine, after the answer, so it is locked and read
// through settled.
func (f *fakeDelivery) Deliver(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, delivery sessionwire.HostLinkCommandDelivery) error {
	deadline, hasDeadline := ctx.Deadline()
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, deliveredCommand{
		deadline: deadline, hasDeadline: hasDeadline, errAtCall: ctx.Err(),
		tenant: tenant, session: session, delivery: delivery,
	})
	return f.err
}

// settled waits for want deliveries to be made, then for every wake the
// fixture's router started to RETURN, and reports what was delivered.
//
// The first wait is a bounded poll, because a wake runs after the answer on a
// goroutine of its own. It must come first: StopWakes CANCELS the wakes'
// context, so a wake that had not yet reached Deliver would record a
// cancelled context the production path never hands it. The second wait is
// StopWakes, which is exact rather than a poll: it returns only when no wake
// is running, so the count read after it is final -- a delivery beyond want
// that was going to happen has happened, and with want zero, one that never
// started never will.
//
// Once settled, the fixture's router makes no further wake.
func (f *fakeDelivery) settled(t *testing.T, fx *fixture, want int) []deliveredCommand {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		f.mu.Lock()
		made := len(f.calls)
		f.mu.Unlock()
		if made >= want || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	stopWakes(t, fx)
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]deliveredCommand(nil), f.calls...)
}

func withAdmitter(admitter ControlAdmitter) fixtureOption {
	return func(cfg *RouterConfig, _ *fixture) { cfg.Admissions = admitter }
}

func withDelivery(delivery CommandDelivery) fixtureOption {
	return func(cfg *RouterConfig, _ *fixture) { cfg.Delivery = delivery }
}

// ---------------------------------------------------------------------------
// The four requests, and the one place their bodies are written.
// ---------------------------------------------------------------------------

// controlProbe is one control operation: its route, the body a caller sends,
// the status a success answers, and what the recorded call must contain.
type controlProbe struct {
	name    string
	target  string
	body    string
	success int
	// admitted reports whether the fake was asked the right thing.
	admitted func(admittedCommand) error
}

func controlProbes(session sessionwire.SessionID) []controlProbe {
	sid := string(session)
	return []controlProbe{
		{
			name:   "input",
			target: "/v1/sessions/" + sid + "/input",
			body: fmt.Sprintf(`{"version":%d,"command_id":"command-a","session_id":%q,"blocks":[{"type":"text","text":"hi"}]}`,
				sessionwire.CurrentWireVersion, sid),
			// The legacy surface answered POST .../input 200 with a body
			// carrying command_id; see the success-status test.
			success: http.StatusOK,
			admitted: func(got admittedCommand) error {
				if got.kind != commandInput {
					return fmt.Errorf("admitted as %q, want %q", got.kind, commandInput)
				}
				if got.input.CommandID != "command-a" || got.input.SessionID != session {
					return fmt.Errorf("admitted %+v", got.input)
				}
				if string(got.input.Blocks) != `[{"type":"text","text":"hi"}]` {
					return fmt.Errorf("blocks reached the service as %s, want the caller's own bytes", got.input.Blocks)
				}
				return nil
			},
		},
		{
			name:   "interrupt",
			target: "/v1/sessions/" + sid + "/interrupt",
			body: fmt.Sprintf(`{"version":%d,"command_id":"command-a","session_id":%q}`,
				sessionwire.CurrentWireVersion, sid),
			success: http.StatusOK,
			admitted: func(got admittedCommand) error {
				if got.kind != commandInterrupt {
					return fmt.Errorf("admitted as %q, want %q", got.kind, commandInterrupt)
				}
				if got.interrupt.CommandID != "command-a" || got.interrupt.SessionID != session {
					return fmt.Errorf("admitted %+v", got.interrupt)
				}
				return nil
			},
		},
		{
			name:   "restore",
			target: "/v1/sessions/" + sid + "/restore",
			body: fmt.Sprintf(`{"version":%d,"command_id":"command-a","session_id":%q}`,
				sessionwire.CurrentWireVersion, sid),
			success: http.StatusOK,
			admitted: func(got admittedCommand) error {
				if got.kind != commandRestore {
					return fmt.Errorf("admitted as %q, want %q", got.kind, commandRestore)
				}
				if got.restore.CommandID != "command-a" || got.restore.SessionID != session {
					return fmt.Errorf("admitted %+v", got.restore)
				}
				return nil
			},
		},
		{
			name:   "gate response",
			target: "/v1/sessions/" + sid + "/gates/gate-a",
			body: fmt.Sprintf(`{"version":%d,"command_id":"command-a","session_id":%q,"gate_id":"gate-a",`+
				`"action":"submit","values":{"answer":"yes"},"expected_open_event_id":"event-a"}`,
				sessionwire.CurrentWireVersion, sid),
			// The legacy surface answered the gate route 202, not 200, and
			// this is the row that makes the success status a table rather
			// than a constant.
			success: http.StatusAccepted,
			admitted: func(got admittedCommand) error {
				if got.kind != commandGateResponse {
					return fmt.Errorf("admitted as %q, want %q", got.kind, commandGateResponse)
				}
				if got.gate.CommandID != "command-a" || got.gate.SessionID != session || got.gate.GateID != "gate-a" {
					return fmt.Errorf("admitted %+v", got.gate)
				}
				if got.gate.Action != "submit" || string(got.gate.Values["answer"]) != `"yes"` {
					return fmt.Errorf("the gate answer reached the service as %+v", got.gate)
				}
				return nil
			},
		},
	}
}

func postJSON(f *fixture, target, body string) *httptest.ResponseRecorder {
	return f.serve(request(http.MethodPost, target, strings.NewReader(body)))
}

func decodeCommandStatus(t *testing.T, recorder *httptest.ResponseRecorder) sessionwire.CommandStatus {
	t.Helper()

	if got := recorder.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json; body was %q", got, recorder.Body)
	}
	var status sessionwire.CommandStatus
	if err := json.Unmarshal(recorder.Body.Bytes(), &status); err != nil {
		t.Fatalf("body %q is not a Core CommandStatus: %v", recorder.Body, err)
	}
	return status
}

// ---------------------------------------------------------------------------
// What a control route does with a well-formed command.
// ---------------------------------------------------------------------------

// TestEveryControlRouteAdmitsItsOwnCommandAndAnswersFromTheRecord is the whole
// happy path, for all four routes.
//
// The assertion is in three parts because three different things could be
// wrong: the service could be asked the wrong question (the kind and the
// request value), the answer could be built from the wrong thing (the status is
// the RECORD's, not the call's), and the caller's own bytes could be rebuilt
// rather than forwarded (the blocks and the gate values).
func TestEveryControlRouteAdmitsItsOwnCommandAndAnswersFromTheRecord(t *testing.T) {
	t.Parallel()

	for _, probe := range controlProbes(fixtureSession) {
		t.Run(probe.name, func(t *testing.T) {
			admitter := newFakeAdmitter()
			f := newFixture(t, withAdmitter(admitter))

			recorder := postJSON(f, probe.target, probe.body)

			if recorder.Code != probe.success {
				t.Fatalf("answered %d (%s), want %d", recorder.Code, recorder.Body, probe.success)
			}
			if len(admitter.calls) != 1 {
				t.Fatalf("the service was asked %d times, want once", len(admitter.calls))
			}
			if err := probe.admitted(admitter.calls[0]); err != nil {
				t.Error(err)
			}
			if got := admitter.calls[0].principal.Tenant(); got != fixtureTenant {
				t.Errorf("admitted for tenant %q, want the authenticated %q", got, fixtureTenant)
			}
			status := decodeCommandStatus(t, recorder)
			if status.CommandID != "command-a" {
				t.Errorf("answered for command %q, want %q", status.CommandID, "command-a")
			}
			if status.State != sessionwire.CommandStateAccepted {
				t.Errorf("answered state %q, want %q", status.State, sessionwire.CommandStateAccepted)
			}
			if status.AcceptedOrder != 3 {
				t.Errorf("answered accepted order %d, want 3", status.AcceptedOrder)
			}
		})
	}
}

// TestTheSuccessStatusIsTheLegacySurfacesOwn is the reader for "port compatible
// status codes", and it is a TABLE because the legacy statuses are not one
// number.
//
// Measured in harness/pkg/serve at the commit this task was written against:
// handleInput and handleInterrupt answer `writeJSON(w, http.StatusOK, …)`,
// handleRestore answers 200 on both of its arms, and handleGate answers
// `http.StatusAccepted`. A client written against that surface reads
// `command_id` out of a 200 from three of these routes, and Core's
// CommandStatus carries `command_id` too -- so both halves of a legacy client's
// success path survive. Answering a uniform 202 would have been defensible on
// HTTP grounds and would have broken every caller that compares against 200.
//
// The gate's 202 is what makes this measurable at all: with one status for all
// four, a handler that ignored the table and wrote a constant would pass.
func TestTheSuccessStatusIsTheLegacySurfacesOwn(t *testing.T) {
	t.Parallel()

	distinct := map[int]bool{}
	for _, probe := range controlProbes(fixtureSession) {
		distinct[probe.success] = true
	}
	if len(distinct) < 2 {
		t.Fatal("every control route expects the same success status, so a handler writing a constant is untested")
	}
	for _, probe := range controlProbes(fixtureSession) {
		t.Run(probe.name, func(t *testing.T) {
			f := newFixture(t, withAdmitter(newFakeAdmitter()))
			if recorder := postJSON(f, probe.target, probe.body); recorder.Code != probe.success {
				t.Errorf("answered %d, want the legacy surface's %d", recorder.Code, probe.success)
			}
		})
	}
}

// TestEveryDurableStateIsAnsweredUnderTheSameSuccessStatus is the other half of
// the status decision, and it is what makes the number a property of the ROUTE
// rather than of the record it happened to read.
//
// A rejected command is a command that WAS durably admitted, and the inbox's
// answer about it is a successful read of that fact; it is Core's `status`
// member that says a Host refused it, which is the one member a client
// branches on. Splitting the HTTP status by durable state would put that
// distinction in two places and let them disagree -- and would report a durable
// acceptance the caller must not retry as though the request had failed.
func TestEveryDurableStateIsAnsweredUnderTheSameSuccessStatus(t *testing.T) {
	t.Parallel()

	probe := controlProbes(fixtureSession)[0]
	for _, durable := range []struct {
		state sessionstore.InboxState
		want  sessionwire.CommandState
	}{
		{sessionstore.InboxStatePending, sessionwire.CommandStateAccepted},
		{sessionstore.InboxStateClaimed, sessionwire.CommandStateAccepted},
		{sessionstore.InboxStateApplying, sessionwire.CommandStateAccepted},
		{sessionstore.InboxStateApplied, sessionwire.CommandStateApplied},
		{sessionstore.InboxStateRejected, sessionwire.CommandStateRejected},
	} {
		t.Run(string(durable.state), func(t *testing.T) {
			admitter := newFakeAdmitter()
			admitter.entry.Record.State = durable.state
			f := newFixture(t, withAdmitter(admitter))

			recorder := postJSON(f, probe.target, probe.body)

			if recorder.Code != probe.success {
				t.Fatalf("a %q record answered %d (%s), want %d", durable.state, recorder.Code, recorder.Body, probe.success)
			}
			if got := decodeCommandStatus(t, recorder).State; got != durable.want {
				t.Errorf("a %q record answered state %q, want %q", durable.state, got, durable.want)
			}
		})
	}
}

// TestAnUnreadableRecordIsAFaultRatherThanASuccess: a state this build does not
// know is a store disagreeing with this build, and 200 with a body describing
// it optimistically is the one answer that must never be produced.
func TestAnUnreadableRecordIsAFaultRatherThanASuccess(t *testing.T) {
	t.Parallel()

	admitter := newFakeAdmitter()
	admitter.entry.Record.State = "settled"
	f := newFixture(t, withAdmitter(admitter))
	probe := controlProbes(fixtureSession)[0]

	recorder := postJSON(f, probe.target, probe.body)

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("an unrecognised durable state answered %d (%s), want 500", recorder.Code, recorder.Body)
	}
	if got := decodeEnvelope(t, recorder).Error.Code; got != ErrorCodeInternal {
		t.Errorf("code = %q, want %q", got, ErrorCodeInternal)
	}
}

// ---------------------------------------------------------------------------
// The body, and the identifiers in the path.
// ---------------------------------------------------------------------------

// TestABodyNamingAnotherSessionIsRefusedRatherThanAdmitted is the reader for
// the comparison this edge exists to make.
//
// The authorization decision was made about the PATH's session, before the body
// was read -- serveRoute takes it from {sid} and hands it to AuthorizeControl --
// and the admission is made about the BODY's. A handler that did not compare
// them would authorize one session and admit another, which is precisely the
// defect A6.2 removed from the RPC path, where the authorized session and the
// admitted session came from two decoders.
//
// Rewriting the body's session to match the path is the other available answer
// and is worse: it silently admits a command the caller did not send. The
// refusal is derived from the caller's own bytes and the path they chose, so it
// discloses nothing about which sessions exist.
func TestABodyNamingAnotherSessionIsRefusedRatherThanAdmitted(t *testing.T) {
	t.Parallel()

	for _, probe := range controlProbes("session-elsewhere") {
		t.Run(probe.name, func(t *testing.T) {
			admitter := newFakeAdmitter()
			f := newFixture(t, withAdmitter(admitter))
			// The PATH names the fixture's session; the BODY names another.
			target := strings.Replace(probe.target, "session-elsewhere", string(fixtureSession), 1)

			recorder := postJSON(f, target, probe.body)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("answered %d (%s), want 400", recorder.Code, recorder.Body)
			}
			if got := decodeEnvelope(t, recorder).Error.Code; got != sessionwire.ErrorCodeInvalidRequest {
				t.Errorf("code = %q, want %q", got, sessionwire.ErrorCodeInvalidRequest)
			}
			if len(admitter.calls) != 0 {
				t.Fatalf("a command naming another session was admitted: %+v", admitter.calls)
			}
		})
	}
}

// TestAGateResponseNamingAnotherGateIsRefused is the same comparison on the
// second identifier the gate route carries.
//
// It is a separate case because {gid} is a separate path value with a separate
// member to disagree with, and a handler comparing only the session would pass
// every row of the test above.
func TestAGateResponseNamingAnotherGateIsRefused(t *testing.T) {
	t.Parallel()

	admitter := newFakeAdmitter()
	f := newFixture(t, withAdmitter(admitter))
	probe := controlProbes(fixtureSession)[3]
	target := strings.Replace(probe.target, "/gates/gate-a", "/gates/gate-b", 1)

	recorder := postJSON(f, target, probe.body)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("answered %d (%s), want 400", recorder.Code, recorder.Body)
	}
	if len(admitter.calls) != 0 {
		t.Fatalf("a gate response naming another gate was admitted: %+v", admitter.calls)
	}

	// The control: the SAME request at the gate it names is admitted, so the
	// refusal above is caused by the mismatch and not by the route refusing
	// every gate response.
	control := newFixture(t, withAdmitter(newFakeAdmitter()))
	if got := postJSON(control, probe.target, probe.body); got.Code != probe.success {
		t.Fatalf("the matching-gate control answered %d (%s), want %d", got.Code, got.Body, probe.success)
	}
}

// TestABodyCoreRefusesIsInvalidRequestBeforeAnyAdmission drives the strict
// decoder Core owns, at this edge, and requires the refusal to reach no durable
// plane.
//
// Every row is a body the RPC edge refuses identically, because both decode
// through the Core type's own UnmarshalJSON: "the bytes decode" and "the
// request is a valid V1 command" are one answer, and it is Core's.
func TestABodyCoreRefusesIsInvalidRequestBeforeAnyAdmission(t *testing.T) {
	t.Parallel()

	sid := string(fixtureSession)
	for name, body := range map[string]string{
		"not JSON at all":      `{`,
		"an empty object":      `{}`,
		"no command id":        `{"version":1,"session_id":"` + sid + `","blocks":[{"type":"text","text":"hi"}]}`,
		"an unknown member":    `{"version":1,"command_id":"c","session_id":"` + sid + `","blocks":[{"type":"text","text":"hi"}],"extra":1}`,
		"a duplicate member":   `{"version":1,"command_id":"c","command_id":"d","session_id":"` + sid + `","blocks":[{"type":"text","text":"hi"}]}`,
		"a wrong-typed member": `{"version":1,"command_id":7,"session_id":"` + sid + `","blocks":[{"type":"text","text":"hi"}]}`,
		"an unsupported version": `{"version":9,"command_id":"c","session_id":"` + sid +
			`","blocks":[{"type":"text","text":"hi"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			admitter := newFakeAdmitter()
			f := newFixture(t, withAdmitter(admitter))

			recorder := postJSON(f, "/v1/sessions/"+sid+"/input", body)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("answered %d (%s), want 400", recorder.Code, recorder.Body)
			}
			if got := decodeEnvelope(t, recorder).Error.Code; got != sessionwire.ErrorCodeInvalidRequest {
				t.Errorf("code = %q, want %q", got, sessionwire.ErrorCodeInvalidRequest)
			}
			if len(admitter.calls) != 0 {
				t.Fatalf("a body Core refuses reached the admission service: %+v", admitter.calls)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// The refusal mapping. This is the A3.3-retryable ruling, at the edge.
// ---------------------------------------------------------------------------

// TestEveryClassifiedRefusalIsAnsweredThroughTheSharedAuthority sweeps the
// authority's OWN code set rather than a list beside it, so a code added to
// admission without a ruling fails here as well as in internal/command.
func TestEveryClassifiedRefusalIsAnsweredThroughTheSharedAuthority(t *testing.T) {
	t.Parallel()

	codes := command.RefusalCodes()
	if len(codes) == 0 {
		t.Fatal("the authority classifies nothing, so this sweep is vacuous")
	}
	probe := controlProbes(fixtureSession)[0]
	for _, code := range codes {
		t.Run(string(code), func(t *testing.T) {
			admitter := newFakeAdmitter()
			admitter.err = &admission.Error{Code: code}
			f := newFixture(t, withAdmitter(admitter))

			recorder := postJSON(f, probe.target, probe.body)

			want, _ := command.RefusalStatus(code)
			if recorder.Code != want {
				t.Errorf("%q answered %d, want the authority's %d", code, recorder.Code, want)
			}
			envelope := decodeEnvelope(t, recorder)
			if envelope.Error.Code != code {
				t.Errorf("%q was answered with code %q; admission's classification is carried whole", code, envelope.Error.Code)
			}
			if envelope.Error.Retryable {
				t.Errorf("%q was advertised retryable over REST; the ClientLink answers the identical refusal retryable:false", code)
			}
			if envelope.Error.Message != "" {
				t.Errorf("%q carries the message %q; a message here is a second prose vocabulary the RPC edge would have "+
					"to reproduce word for word for the two answers to stay identical", code, envelope.Error.Message)
			}
		})
	}
}

// TestARuntimeUnavailableRefusalIsNotTheRetryable503 names the specific defect
// the A3.3-retryable open item predicted, at the edge that would have produced
// it. The authority's own test holds the same property one level down; this one
// holds that this handler consults it.
func TestARuntimeUnavailableRefusalIsNotTheRetryable503(t *testing.T) {
	t.Parallel()

	admitter := newFakeAdmitter()
	admitter.err = &admission.Error{Code: sessionwire.ErrorCodeRuntimeUnavailable}
	f := newFixture(t, withAdmitter(admitter))
	probe := controlProbes(fixtureSession)[0]

	recorder := postJSON(f, probe.target, probe.body)

	if recorder.Code == http.StatusServiceUnavailable {
		t.Fatal("runtime_unavailable answered 503, which retryableStatus reports retryable: " +
			"admission mints this code for three permanent conditions as well as one transient one")
	}
	if decodeEnvelope(t, recorder).Error.Retryable {
		t.Error("runtime_unavailable was advertised retryable")
	}
}

func TestQuiescedAdmissionIsRetryable503(t *testing.T) {
	admitter := newFakeAdmitter()
	admitter.err = admission.ErrAdmissionQuiesced
	f := newFixture(t, withAdmitter(admitter))
	probe := controlProbes(fixtureSession)[0]
	recorder := postJSON(f, probe.target, probe.body)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", recorder.Code)
	}
	envelope := decodeEnvelope(t, recorder)
	if envelope.Error.Code != ErrorCodeUnavailable || !envelope.Error.Retryable {
		t.Fatalf("response = %+v, want retryable unavailable", envelope.Error)
	}
}

// TestARefusalCarryingNoCodeIsAFault holds the authority's second return at
// this edge. (*admission.Error).Error() explicitly contemplates Code == "", so
// a zero-valued refusal from any present or future admission path arrives here,
// and answering it with whatever status a lookup miss produced would be a
// ruling nobody made.
func TestARefusalCarryingNoCodeIsAFault(t *testing.T) {
	t.Parallel()

	for name, err := range map[string]error{
		"a refusal with no code": &admission.Error{},
		"a code with no ruling":  &admission.Error{Code: sessionwire.ErrorCodeGateResponseInvalid},
	} {
		t.Run(name, func(t *testing.T) {
			admitter := newFakeAdmitter()
			admitter.err = err
			f := newFixture(t, withAdmitter(admitter))
			probe := controlProbes(fixtureSession)[0]

			recorder := postJSON(f, probe.target, probe.body)

			if recorder.Code != http.StatusInternalServerError {
				t.Fatalf("answered %d (%s), want 500", recorder.Code, recorder.Body)
			}
			if got := decodeEnvelope(t, recorder).Error.Code; got != ErrorCodeInternal {
				t.Errorf("code = %q, want %q", got, ErrorCodeInternal)
			}
		})
	}
}

// TestADependencyFaultIsNotADecisionAboutTheCommand is the other side of the
// same seam: admission returns a fault AS ITSELF, carrying no public code, so
// this edge must answer from its own fault channel rather than inventing a
// classification.
//
// The three rows are the three conditions the mapping separates, and each has a
// different consequence for a caller: a draining replica is retryable and
// another replica will serve, a deadline is the router's own, and anything else
// is a fault an operator hears about and a client does not hammer.
func TestADependencyFaultIsNotADecisionAboutTheCommand(t *testing.T) {
	t.Parallel()

	probe := controlProbes(fixtureSession)[0]
	for _, fault := range []struct {
		name      string
		err       error
		status    int
		code      sessionwire.ErrorCode
		retryable bool
	}{
		{
			name: "a draining store", err: &sessionstore.StoreClosedError{},
			status: http.StatusServiceUnavailable, code: ErrorCodeUnavailable, retryable: true,
		},
		{
			name: "the deadline this router imposed", err: context.DeadlineExceeded,
			status: http.StatusGatewayTimeout, code: ErrorCodeTimeout, retryable: true,
		},
		{
			name: "an unclassified failure", err: errors.New("the backend is confused"),
			status: http.StatusInternalServerError, code: ErrorCodeInternal, retryable: false,
		},
	} {
		t.Run(fault.name, func(t *testing.T) {
			admitter := newFakeAdmitter()
			admitter.err = fmt.Errorf("admission: observe the session's owner: %w", fault.err)
			f := newFixture(t, withAdmitter(admitter))

			recorder := postJSON(f, probe.target, probe.body)

			if recorder.Code != fault.status {
				t.Fatalf("answered %d (%s), want %d", recorder.Code, recorder.Body, fault.status)
			}
			envelope := decodeEnvelope(t, recorder)
			if envelope.Error.Code != fault.code {
				t.Errorf("code = %q, want %q", envelope.Error.Code, fault.code)
			}
			if envelope.Error.Retryable != fault.retryable {
				t.Errorf("retryable = %t, want %t", envelope.Error.Retryable, fault.retryable)
			}
		})
	}
}

// TestACancelledCallerIsNotCountedAsAServerFault: the caller went away, so
// there is nobody to read a response and the status says so rather than adding
// to the deployment's error rate.
func TestACancelledCallerIsNotCountedAsAServerFault(t *testing.T) {
	t.Parallel()

	admitter := newFakeAdmitter()
	admitter.err = fmt.Errorf("admission: %w", context.Canceled)
	f := newFixture(t, withAdmitter(admitter))
	probe := controlProbes(fixtureSession)[0]

	if recorder := postJSON(f, probe.target, probe.body); recorder.Code != statusClientClosedRequest {
		t.Fatalf("answered %d, want %d", recorder.Code, statusClientClosedRequest)
	}
}

// ---------------------------------------------------------------------------
// The session the chain resolved.
// ---------------------------------------------------------------------------

// TestAControlOnAnAbsentSessionIsTheSame404TheReadsAnswer is the reader for the
// not-found authority, END TO END and BYTE FOR BYTE.
//
// A2.1's rule is that a session in another tenant and one that never existed
// produce the same response; this asserts that a control command produces the
// same response as a READ of the same session, which is the direction the
// A9.1-notfound divergence was in. All four store spellings are driven, because
// the two readers of them disagreed about two.
func TestAControlOnAnAbsentSessionIsTheSame404TheReadsAnswer(t *testing.T) {
	t.Parallel()

	for name, spelling := range map[string]error{
		"the catalog's not-found":          &sessionstore.CatalogError{Code: sessionstore.CatalogErrorNotFound},
		"the catalog's deleted":            &sessionstore.CatalogError{Code: sessionstore.CatalogErrorDeleted},
		"the catalog's identity mismatch":  &sessionstore.CatalogError{Code: sessionstore.CatalogErrorIdentity},
		"the keyspace's binding-not-found": &sessionstore.KeyspaceError{Code: sessionstore.KeyspaceBindingNotFound},
	} {
		t.Run(name, func(t *testing.T) {
			admitter := newFakeAdmitter()
			f := newFixture(t, withAdmitter(admitter))
			f.reads.fail = spelling
			probe := controlProbes(fixtureSession)[0]

			control := postJSON(f, probe.target, probe.body)
			read := f.get("/v1/sessions/" + string(fixtureSession) + "/status")

			if control.Code != http.StatusNotFound {
				t.Fatalf("the command answered %d (%s), want 404", control.Code, control.Body)
			}
			if read.Code != http.StatusNotFound {
				t.Fatalf("the read answered %d (%s), want 404; this case cannot compare them", read.Code, read.Body)
			}
			if control.Body.String() != read.Body.String() {
				t.Errorf("the command answered %s and the read %s; one absent session must be one public fact",
					control.Body, read.Body)
			}
			if len(admitter.calls) != 0 {
				t.Fatalf("a command on an absent session reached the admission service: %+v", admitter.calls)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// The composition that has no command plane.
// ---------------------------------------------------------------------------

// TestAControlRouteWithNoAdmissionPlaneFailsClosed.
//
// A nil Admissions is a supported composition -- factory.New builds one today,
// and so does every test of the read plane -- so the route answers rather than
// panicking, and it answers the DEPLOYMENT's condition rather than a decision
// about the caller's command. 503 is the same answer a draining store
// produces, and for the same reason: another replica may be able to serve it.
func TestAControlRouteWithNoAdmissionPlaneFailsClosed(t *testing.T) {
	t.Parallel()

	for _, probe := range controlProbes(fixtureSession) {
		t.Run(probe.name, func(t *testing.T) {
			f := newFixture(t)

			recorder := postJSON(f, probe.target, probe.body)

			if recorder.Code != http.StatusServiceUnavailable {
				t.Fatalf("answered %d (%s), want 503", recorder.Code, recorder.Body)
			}
			if got := decodeEnvelope(t, recorder).Error.Code; got != ErrorCodeUnavailable {
				t.Errorf("code = %q, want %q", got, ErrorCodeUnavailable)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Step 3: best-effort delivery, and the acknowledgement that does not depend
// on it.
// ---------------------------------------------------------------------------

// TestAnAdmittedCommandIsDeliveredBestEffort holds runbook step 3 in both
// directions at once: the delivery IS attempted, with the durable record's own
// public CommandID, and the response is the durable acknowledgement whether it
// worked or not.
func TestAnAdmittedCommandIsDeliveredBestEffort(t *testing.T) {
	t.Parallel()

	probe := controlProbes(fixtureSession)[0]
	for name, deliveryErr := range map[string]error{
		"a delivery that lands": nil,
		"a delivery that fails": errors.New("no local subscriber holds a route"),
	} {
		t.Run(name, func(t *testing.T) {
			delivery := &fakeDelivery{err: deliveryErr}
			f := newFixture(t, withAdmitter(newFakeAdmitter()), withDelivery(delivery))

			recorder := postJSON(f, probe.target, probe.body)

			if recorder.Code != probe.success {
				t.Fatalf("answered %d (%s), want the durable %d whatever delivery did",
					recorder.Code, recorder.Body, probe.success)
			}
			if got := decodeCommandStatus(t, recorder).State; got != sessionwire.CommandStateAccepted {
				t.Errorf("answered state %q, want %q", got, sessionwire.CommandStateAccepted)
			}
			calls := delivery.settled(t, f, 1)
			if len(calls) != 1 {
				t.Fatalf("delivery was attempted %d times, want once", len(calls))
			}
			got := calls[0]
			if got.tenant != fixtureTenant || got.session != fixtureSession {
				t.Errorf("delivered to %q/%q, want the authenticated tenant's own session", got.tenant, got.session)
			}
			if got.delivery.CommandID != "command-a" {
				t.Errorf("delivered command %q, want the durable record's %q", got.delivery.CommandID, "command-a")
			}
		})
	}
}

// TestARefusedCommandIsNeverDelivered is the negative half, and it carries its
// positive control in the same function: the same fixture delivers when the
// command IS admitted, so "nothing was delivered" is not also what a composition
// with a broken delivery seam would produce.
func TestARefusedCommandIsNeverDelivered(t *testing.T) {
	t.Parallel()

	probe := controlProbes(fixtureSession)[0]

	refused := &fakeDelivery{}
	admitter := newFakeAdmitter()
	admitter.err = &admission.Error{Code: sessionwire.ErrorCodeCommandRejected}
	f := newFixture(t, withAdmitter(admitter), withDelivery(refused))
	if recorder := postJSON(f, probe.target, probe.body); recorder.Code == probe.success {
		t.Fatalf("the refused command answered %d, which is the success status", recorder.Code)
	}
	if calls := refused.settled(t, f, 0); len(calls) != 0 {
		t.Errorf("a refused command was delivered: %+v", calls)
	}

	accepted := &fakeDelivery{}
	control := newFixture(t, withAdmitter(newFakeAdmitter()), withDelivery(accepted))
	if recorder := postJSON(control, probe.target, probe.body); recorder.Code != probe.success {
		t.Fatalf("the control answered %d (%s), want %d", recorder.Code, recorder.Body, probe.success)
	}
	if calls := accepted.settled(t, control, 1); len(calls) != 1 {
		t.Fatal("the control delivered nothing, so the assertion above is vacuous")
	}
}

// ---------------------------------------------------------------------------
// The pending methods.
// ---------------------------------------------------------------------------

// TestEveryPendingMethodExplainsItselfToTheCaller holds methodRule.reason in
// both directions, and requires the answer a CALLER gets to carry it.
//
// The owner column alone is a fact about this repository's task roster: the
// create carried `owner: "A3.1"` through two tasks that could not have
// implemented it, and a client reading the 501 was told only that the build has
// no handler. A reason that reaches the response is one somebody re-reads.
func TestEveryPendingMethodExplainsItselfToTheCaller(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withAdmitter(newFakeAdmitter()))
	pending := 0
	for _, route := range routeTable() {
		for _, rule := range route.rules {
			key := rule.method + " " + route.pattern
			if rule.owner == "" {
				if rule.reason != "" {
					t.Errorf("%s is served and still carries the reason %q", key, rule.reason)
				}
				continue
			}
			pending++
			if rule.reason == "" {
				t.Errorf("%s is owned by %s and gives the caller no reason", key, rule.owner)
				continue
			}
			recorder := f.driveRule(route, rule)
			if recorder.Code != http.StatusNotImplemented {
				t.Errorf("%s answered %d, want 501", key, recorder.Code)
				continue
			}
			if got := decodeEnvelope(t, recorder).Error.Message; !strings.Contains(got, rule.reason) {
				t.Errorf("%s answered %q, which does not carry its reason %q", key, got, rule.reason)
			}
		}
	}
	// Zero as of A3.1: every method is served. What this test still holds is
	// the other direction, asserted in the loop above -- a SERVED method must
	// carry no reason, so a cleared owner cannot leave a stale explanation
	// behind that nothing renders.
	if pending != 0 {
		t.Logf("%d methods are still pending", pending)
	}
}

// TestTheCreateIsServedAndAnswersTheDurableRecord is what
// TestTheCreateRefusalNamesTheBindingAndNotATaskTag became.
//
// That test pinned the create's 501 and required the refusal to name the
// BINDING rather than a task tag, because "A3.1" had sat in the reason column
// through two tasks and told a caller nothing. A3.1 supplies the binding, so
// the create is served and the refusal it guarded no longer exists.
//
// What replaces it is the assertion the old one could not make: the create
// answers from the authoritative durable record, with 201 rather than the 200
// the other four controls use, because it is the only control that brings a
// resource into existence.
func TestTheCreateIsServedAndAnswersTheDurableRecord(t *testing.T) {
	t.Parallel()

	admitter := newFakeAdmitter()
	f := newFixture(t, withAdmitter(admitter))
	recorder := postJSON(f, "/v1/sessions",
		`{"version":1,"command_id":"command-a","session_id":"session-new","agent_id":"agent-a"}`)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("the create answered %d (%s), want 201", recorder.Code, recorder.Body)
	}
	if len(admitter.calls) != 1 || admitter.calls[0].kind != commandCreate {
		t.Fatalf("the route did not admit exactly one create: %+v", admitter.calls)
	}
	// The SESSION reaches admission from the BODY. There is no {sid} on this
	// route, so a handler that read the path would have admitted the zero
	// session.
	if got := admitter.calls[0].create.SessionID; got != "session-new" {
		t.Errorf("admitted session %q, want the body's", got)
	}
	if got := admitter.calls[0].create.CommandID; got != "command-a" {
		t.Errorf("admitted command %q, want the body's", got)
	}
	var status sessionwire.CommandStatus
	if err := json.Unmarshal(recorder.Body.Bytes(), &status); err != nil {
		t.Fatalf("the create answered a body that is not a CommandStatus: %v (%s)", err, recorder.Body)
	}
	if status.CommandID != "command-a" || status.State != sessionwire.CommandStateAccepted {
		t.Errorf("status = %+v, want the durable record's accepted command", status)
	}
}

// TestAKindWithNoControlSpecIsAFaultRatherThanAHandler drives serveControl's
// own table lookup, which routeTable cannot reach.
//
// The comparison's input is STRUCTURE -- which kinds controlSpecs names -- so
// no request can distinguish it and no fixture built through the route table
// can either. It is driven directly instead, because the arm it guards is what
// a later task hits by adding a control route and forgetting the spec: the
// alternative to failing closed is a nil spec whose zero success status is 0
// and whose nil admit function panics on the first request.
func TestAKindWithNoControlSpecIsAFaultRatherThanAHandler(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withAdmitter(newFakeAdmitter()))
	recorder := httptest.NewRecorder()
	f.router.serveControl("no_such_kind").ServeHTTP(recorder, request(http.MethodPost, "/v1/sessions/x/input", strings.NewReader(`{}`)))

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("a kind with no spec answered %d (%s), want 500", recorder.Code, recorder.Body)
	}

	// The control: a kind the table DOES name builds a handler that admits.
	if got := postJSON(f, controlProbes(fixtureSession)[0].target, controlProbes(fixtureSession)[0].body); got.Code != http.StatusOK {
		t.Fatalf("the control answered %d (%s), want 200", got.Code, got.Body)
	}
}

// TestARecordCoreWillNotMarshalIsAFault covers the last comparison on the
// success path: Core validates on MARSHAL, so a record that projects into a
// coherent CommandStatus can still fail to become bytes.
//
// The fixture is a rejected command carrying no rejection detail, which Core's
// CommandStatus.Validate refuses ("missing_required_field"). It is a fault in
// durable state rather than something the caller did, and answering it as an
// acceptance would put an empty body on a 200 -- the one answer a client cannot
// classify at all.
func TestARecordCoreWillNotMarshalIsAFault(t *testing.T) {
	t.Parallel()

	admitter := newFakeAdmitter()
	admitter.entry.Record.State = sessionstore.InboxStateRejected
	admitter.entry.Record.Attempt = storeSettledAttempt
	f := newFixture(t, withAdmitter(admitter))
	probe := controlProbes(fixtureSession)[0]

	recorder := postJSON(f, probe.target, probe.body)

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("a record Core will not marshal answered %d (%s), want 500", recorder.Code, recorder.Body)
	}
	if got := decodeEnvelope(t, recorder).Error.Code; got != ErrorCodeInternal {
		t.Errorf("code = %q, want %q", got, ErrorCodeInternal)
	}
}

// TestTheDeliveryAttemptIsBoundedAndOutlivesItsCaller holds the two halves of
// the wake's context contract, and it REPLACES a test that asserted the
// opposite of its second half.
//
// Through v0.7.0 the attempt ran on the request's own context, before the
// answer, and a test required a caller that went away to cancel it. I1.2
// measured the cost: every admitted POST waited out the HostLink RPC bound
// against a Host that dropped the delivery reply. The wake now runs after the
// answer on a context the ROUTER owns (see wakes), so:
//
//   - It is still bounded: the attempt carries a deadline no longer than the
//     route's own RequestTimeout. Deleting the wake's WithTimeout fails here.
//   - It is NOT the caller's: a request whose caller has already gone away
//     still reaches the seam with a live context, because the command was
//     committed and waking its Host is still correct.
//
// What stops a wake is StopWakes, and that is
// TestStopWakesCancelsARunningWakeAndRefusesLaterOnes's subject.
func TestTheDeliveryAttemptIsBoundedAndOutlivesItsCaller(t *testing.T) {
	t.Parallel()

	probe := controlProbes(fixtureSession)[0]

	t.Run("it carries the route's own bound", func(t *testing.T) {
		delivery := &fakeDelivery{}
		f := newFixture(t, withAdmitter(newFakeAdmitter()), withDelivery(delivery))

		if recorder := postJSON(f, probe.target, probe.body); recorder.Code != probe.success {
			t.Fatalf("answered %d (%s), want %d", recorder.Code, recorder.Body, probe.success)
		}
		calls := delivery.settled(t, f, 1)
		if len(calls) != 1 {
			t.Fatalf("delivery was attempted %d times, want once", len(calls))
		}
		call := calls[0]
		if !call.hasDeadline {
			t.Fatal("the delivery attempt carries no deadline, so a wedged delivery plane is bounded by nothing")
		}
		if remaining := time.Until(call.deadline); remaining > f.limits.RequestTimeout {
			t.Errorf("the delivery attempt's deadline is %v away, longer than the router's own %v bound",
				remaining, f.limits.RequestTimeout)
		}
		if call.errAtCall != nil {
			t.Errorf("the delivery attempt's context was already %v at the call", call.errAtCall)
		}
	})

	t.Run("a caller that went away does not stop it", func(t *testing.T) {
		delivery := &fakeDelivery{}
		f := newFixture(t, withAdmitter(newFakeAdmitter()), withDelivery(delivery))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		r := request(http.MethodPost, probe.target, strings.NewReader(probe.body)).WithContext(ctx)

		f.serve(r)

		calls := delivery.settled(t, f, 1)
		if len(calls) != 1 {
			t.Fatalf("delivery was attempted %d times, want once: this row cannot see the property "+
				"unless the attempt is made", len(calls))
		}
		if err := calls[0].errAtCall; err != nil {
			t.Errorf("the caller was gone and the delivery attempt's context reported %v at the call; "+
				"the wake is derived from the request again, so a committed command's Host is not woken "+
				"when its caller hangs up", err)
		}
	})
}

// TestAnAdmittedCreateIsDeliveredToTheSessionItsBodyNamed is the create's half
// of the delivery contract, and it exists because the other four commands
// cannot cover it.
//
// Every session-scoped control takes its delivery target from the URL, which
// decodeCommand has already held equal to the body's -- so for those four the
// two sources are indistinguishable and a handler reading either passes. The
// create has NO {sid} in its path, so the URL source yields the empty session
// and only the body's is correct. Two mutations survived the whole suite
// before this test existed: dropping the create's session from the projection,
// and making serveControl ignore the override and always use the URL's.
func TestAnAdmittedCreateIsDeliveredToTheSessionItsBodyNamed(t *testing.T) {
	t.Parallel()

	const created = sessionwire.SessionID("session-new")
	delivery := &fakeDelivery{}
	f := newFixture(t, withAdmitter(newFakeAdmitter()), withDelivery(delivery))

	recorder := postJSON(f, "/v1/sessions",
		`{"version":1,"command_id":"command-a","session_id":"`+string(created)+`","agent_id":"agent-a"}`)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("the create answered %d (%s), want 201", recorder.Code, recorder.Body)
	}
	calls := delivery.settled(t, f, 1)
	if len(calls) != 1 {
		t.Fatalf("delivery was attempted %d times, want once", len(calls))
	}
	got := calls[0]
	if got.session != created {
		t.Errorf("delivered to session %q, want the body's %q; the create's path carries no session, "+
			"so a handler reading the URL delivers to nothing", got.session, created)
	}
	// The session must differ from the fixture's own, or a handler that read a
	// fixture default would pass this.
	if created == fixtureSession {
		t.Fatal("the created session is the fixture's, so reading either source would pass")
	}
	if got.tenant != fixtureTenant {
		t.Errorf("delivered to tenant %q, want the authenticated %q", got.tenant, fixtureTenant)
	}
	if got.delivery.CommandID != "command-a" {
		t.Errorf("delivered command %q, want the durable record's", got.delivery.CommandID)
	}
}

// TestAMalformedCreateBodyIsRefusedBeforeAdmission rows the create's decoder.
//
// A mutation that DISCARDED decodeCreate's unmarshal error survived the whole
// suite, and the reason it was invisible is worth stating: with the error
// dropped the request struct is simply left partly filled, and admission's own
// Validate then refuses it with the same invalid_request a caller sees anyway.
// The two paths are indistinguishable from the STATUS alone.
//
// What distinguishes them is (a) whether admission was reached at all, and (b)
// the duplicate-field row, which Core's decoder refuses and a dropped error
// would silently resolve to one of the two values -- admitting a command the
// caller did not unambiguously send.
func TestAMalformedCreateBodyIsRefusedBeforeAdmission(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		body string
	}{
		{"not JSON at all", `{`},
		{"a duplicate command id", `{"version":1,"command_id":"a","command_id":"b","session_id":"session-new","agent_id":"agent-a"}`},
		{"a duplicate session id", `{"version":1,"command_id":"a","session_id":"s1","session_id":"s2","agent_id":"agent-a"}`},
		{"a body that is not an object", `["not","a","create"]`},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			admitter := newFakeAdmitter()
			f := newFixture(t, withAdmitter(admitter))
			recorder := postJSON(f, "/v1/sessions", test.body)

			if recorder.Code == http.StatusCreated {
				t.Fatalf("a malformed create was admitted: %s", recorder.Body)
			}
			if recorder.Code/100 != 4 {
				t.Errorf("answered %d (%s), want a client refusal", recorder.Code, recorder.Body)
			}
			// The discriminating assertion: the decoder refused it, so the
			// service was never asked. Without this the rows above pass
			// against a handler that decodes nothing and lets admission
			// refuse a zero request.
			if len(admitter.calls) != 0 {
				t.Errorf("a malformed create reached the admission service: %+v", admitter.calls)
			}
		})
	}
	// The positive control: a WELL-FORMED body does reach admission and is
	// created, so the assertions above are about malformedness rather than
	// about a handler that admits nothing.
	admitter := newFakeAdmitter()
	f := newFixture(t, withAdmitter(admitter))
	if recorder := postJSON(f, "/v1/sessions",
		`{"version":1,"command_id":"command-a","session_id":"session-new","agent_id":"agent-a"}`); recorder.Code != http.StatusCreated {
		t.Fatalf("the well-formed control answered %d (%s), want 201", recorder.Code, recorder.Body)
	}
	if len(admitter.calls) != 1 {
		t.Fatalf("the well-formed control reached admission %d times, want once", len(admitter.calls))
	}
}
