package clientlink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	"github.com/looprig/factory/internal/admission"
	"github.com/looprig/factory/internal/command"
	"github.com/looprig/sessionstore"
)

// ErrUnknownMethod reports an RPC method that is not in the command vocabulary.
var ErrUnknownMethod = errors.New("clientlink: unknown rpc method")

// ErrUnreadableRecord reports a durable record this surface cannot project into
// a public command status. It is a fault in Factory or in what it depends on,
// never a decision about the caller's request, and it is deliberately NOT one
// of the codes a refusal envelope can carry.
var ErrUnreadableRecord = errors.New("clientlink: the accepted record cannot be described")

// Method is a typed ClientLink command RPC.
//
// Commands are RPCs and events are server publications, and the split is not
// symmetry for its own sake: a command needs a correlated reply carrying the
// admitted CommandID, and an event needs fan-out to every subscriber of a
// channel. Client PUBLICATION is disabled outright, so this vocabulary is the
// only way a browser can ask for anything.
type Method string

// The command vocabulary. Each name is the REST path it is the alternative to,
// because specification section 8.1 makes the two alternatives obeying one
// admission contract rather than two surfaces with two spellings.
const (
	MethodSessionCreate    Method = "session.create"
	MethodSessionInput     Method = "session.input"
	MethodSessionInterrupt Method = "session.interrupt"
	MethodSessionRestore   Method = "session.restore"
	MethodGateRespond      Method = "gate.respond"
)

// rpcCommand is one DECODED V1 request, reduced to the two things this edge
// does with it: authorize it, and hand it to the admission service.
//
// The session is read from the decoded request rather than from a second parse
// of the same bytes, and that is the whole reason this type exists. A6.1 read
// session_id through a private envelope struct because there was nothing else
// to do with the data yet; keeping that would have meant the session the
// AUTHORIZER was asked about and the session the SERVICE admitted came from two
// decoders, which is how a body that decodes differently under two grammars --
// a duplicate member, a member the strict decoder refuses -- gets authorized
// under one session and admitted under another.
type rpcCommand struct {
	session sessionwire.SessionID
	// admit hands the decoded request to the service and projects the durable
	// answer. It closes over the concrete Core type, so no type switch and no
	// `any` reaches the seam.
	//
	// It returns the PROJECTED status rather than the record because the two
	// command families return different record types -- a create is admitted
	// into the disposition inbox and everything else into the legacy one -- and
	// the projection is the point at which that difference stops mattering.
	// Keeping the raw record here would force a type switch or an `any` at
	// exactly the seam this signature exists to keep concrete. The bool is
	// "this record can be described publicly"; see command.StatusFor.
	admit func(context.Context, Admitter, identity.Principal) (sessionwire.CommandStatus, bool, error)
}

// commandSpec is what one method IS: the durable kind it is authorized under,
// and the V1 request its data must decode as.
//
// Kind and decoder are one table entry rather than two tables keyed by Method,
// so a method cannot acquire a decoder without a kind or a kind without a
// decoder. The kinds are internal/command's constants, which are also the ones
// the REST route table authorizes under -- see that package's doc for why two
// private copies would satisfy every test either edge could write.
type commandSpec struct {
	kind   sessionstore.CommandKind
	decode func([]byte) (rpcCommand, error)
}

var commandSpecs = map[Method]commandSpec{
	MethodSessionCreate:    {kind: command.KindCreateSession, decode: decodeCreate},
	MethodSessionInput:     {kind: command.KindInput, decode: decodeInput},
	MethodSessionInterrupt: {kind: command.KindInterrupt, decode: decodeInterrupt},
	MethodSessionRestore:   {kind: command.KindRestore, decode: decodeRestore},
	MethodGateRespond:      {kind: command.KindGateResponse, decode: decodeGateResponse},
}

// The five decoders. Each names ONE Core request type and calls ONE service
// method, so the RPC and the REST route that serves the same operation cannot
// reach different admissions.
//
// json.Unmarshal here runs the Core type's own strict UnmarshalJSON, which
// refuses an unknown member, a duplicate member and a wrong-typed one, and then
// calls the type's Validate. So "the bytes decode" and "the request is a valid
// V1 command" are one answer rather than two, and it is Core's answer, not a
// grammar this package restates.
//
// The duplicate-member refusal covers the REQUEST's own members and stops at
// the opaque ones. `blocks` and a gate response's `values` entries are
// json.RawMessage, so a duplicate key INSIDE a block is carried through
// verbatim into the durable payload; measured, `[{"text":"x","text":"y"}]` is
// admitted. Two consequences belong to whoever consumes that payload rather
// than to this edge: the stored bytes are not canonicalised, so a Host applying
// them and a viewer rendering them may resolve the duplicate differently, and
// since SessionStore compares payload BYTES on retry, two logically identical
// commands spelled with different duplicate orderings are command_rejected
// rather than idempotent. Canonicalising here would mean this package parsing a
// vocabulary only Harness defines, which is the same reason the journal route
// forwards stored bodies whole.
//
// Note what the refusal is and is not load-bearing for. The session is
// single-reader BY CONSTRUCTION -- rpcCommand.session and the closure handed to
// the service close over the same decoded value -- so even a lenient duplicate
// resolution could not make the authorized session differ from the admitted
// one. The refusal is defence in depth on top of that, not the thing that
// closes the hazard.

func decodeCreate(data []byte) (rpcCommand, error) {
	var req sessionwire.CreateRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return rpcCommand{}, err
	}
	return rpcCommand{session: req.SessionID, admit: func(ctx context.Context, a Admitter, p identity.Principal) (sessionwire.CommandStatus, bool, error) {
		entry, _, err := a.AdmitCreate(ctx, p, req)
		if err != nil {
			return sessionwire.CommandStatus{}, false, err
		}
		status, readable := command.StatusForDisposition(entry)
		return status, readable, nil
	}}, nil
}

func decodeInput(data []byte) (rpcCommand, error) {
	var req sessionwire.InputRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return rpcCommand{}, err
	}
	return rpcCommand{session: req.SessionID, admit: func(ctx context.Context, a Admitter, p identity.Principal) (sessionwire.CommandStatus, bool, error) {
		entry, _, err := a.AdmitInput(ctx, p, req)
		if err != nil {
			return sessionwire.CommandStatus{}, false, err
		}
		status, readable := command.StatusFor(entry)
		return status, readable, nil
	}}, nil
}

func decodeInterrupt(data []byte) (rpcCommand, error) {
	var req sessionwire.InterruptRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return rpcCommand{}, err
	}
	return rpcCommand{session: req.SessionID, admit: func(ctx context.Context, a Admitter, p identity.Principal) (sessionwire.CommandStatus, bool, error) {
		entry, _, err := a.AdmitInterrupt(ctx, p, req)
		if err != nil {
			return sessionwire.CommandStatus{}, false, err
		}
		status, readable := command.StatusFor(entry)
		return status, readable, nil
	}}, nil
}

func decodeRestore(data []byte) (rpcCommand, error) {
	var req sessionwire.RestoreRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return rpcCommand{}, err
	}
	return rpcCommand{session: req.SessionID, admit: func(ctx context.Context, a Admitter, p identity.Principal) (sessionwire.CommandStatus, bool, error) {
		entry, _, err := a.AdmitRestore(ctx, p, req)
		if err != nil {
			return sessionwire.CommandStatus{}, false, err
		}
		status, readable := command.StatusFor(entry)
		return status, readable, nil
	}}, nil
}

func decodeGateResponse(data []byte) (rpcCommand, error) {
	var req sessionwire.GateResponseRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return rpcCommand{}, err
	}
	return rpcCommand{session: req.SessionID, admit: func(ctx context.Context, a Admitter, p identity.Principal) (sessionwire.CommandStatus, bool, error) {
		entry, _, err := a.AdmitGateResponse(ctx, p, req)
		if err != nil {
			return sessionwire.CommandStatus{}, false, err
		}
		status, readable := command.StatusFor(entry)
		return status, readable, nil
	}}, nil
}

// CommandKindFor reports the command kind a method admits.
//
// The boolean is false for every method not in the vocabulary, and a caller
// must treat that as a refusal rather than defaulting to some kind: an unknown
// method that fell through to a real command kind would let a client name an
// operation the vocabulary does not contain.
func CommandKindFor(method Method) (sessionstore.CommandKind, bool) {
	spec, ok := commandSpecs[method]
	return spec.kind, ok
}

// Methods returns the command vocabulary in a stable order.
func Methods() []Method {
	return []Method{
		MethodSessionCreate,
		MethodSessionInput,
		MethodSessionInterrupt,
		MethodSessionRestore,
		MethodGateRespond,
	}
}

// Admit decodes, authorizes and durably admits one command RPC.
//
// # What the two results MEAN, which is the whole contract
//
// The bytes are the REPLY BODY, and they are returned for every outcome the
// admission service classified -- an acceptance and a refusal alike. The error
// is returned only for a condition about which NO admission decision exists:
// an unknown method, a refused authorization, or a fault. A caller must not
// collapse the two, because a refusal is a durable answer about the caller's
// command and a fault is not an answer at all.
//
// A refusal travels as a Core ErrorEnvelope in the reply body rather than as a
// transport error, and that is REST parity rather than a preference: the REST
// alternative answers the identical condition with the identical
// sessionwire.ErrorCode in the identical envelope, and a transport error would
// force the browser to switch on a second, numeric vocabulary for exactly the
// nine conditions Core already names. The consumer is written for it --
// wui's protocol package resolves an RPC whose data carries `error` and no
// `command_id` into its Core error hierarchy.
//
// # The order, and why decoding precedes authorization
//
// An unknown method is refused first: there is no command kind to ask about,
// and consulting the authorizer with a zero kind would ask a question whose
// answer means nothing. Then the body is decoded, because the session an
// authorization decision is made about is the one the decoded request names --
// there is no path segment here to read it from, and re-parsing the same bytes
// to find it early is what lets the authorized session and the admitted session
// differ. A body that is not a valid V1 command is therefore refused before any
// authorization decision, and that discloses nothing: the refusal is derived
// from the caller's own bytes and from no tenant state whatever.
//
// The authorization here is the EDGE's, and it is not repeated inside this
// package. admission.Service makes its own control decision for the reason its
// Authorizer doc gives -- it is a durable mutation boundary reachable by more
// than one transport -- so a command that reaches the service arrives already
// authorized and is authorized again there. Adding a third caller is what
// admission.Authorizer's declaration exists to prevent.
func (e *Engine) Admit(ctx context.Context, principal identity.Principal, method Method, data []byte) ([]byte, error) {
	spec, known := commandSpecs[method]
	if !known {
		return nil, fmt.Errorf("%w: %q", ErrUnknownMethod, method)
	}
	decoded, err := spec.decode(data)
	if err != nil {
		// Core's own decoder refused the bytes, and Core's own vocabulary
		// names that: RequestValidationError is what admission itself maps to
		// invalid_request when a request fails Validate, so a body that never
		// reached the service is answered with the code it would have been
		// answered with had it got there.
		return refusalBody(sessionwire.ErrorCodeInvalidRequest)
	}
	// The bound is applied here rather than in the transport adapter, for the
	// reason every other decision on this surface is the Engine's: the deadline
	// is policy, it is read from the composed Limits, and a test must be able
	// to drive it without a socket. It covers the authorization decision as
	// well as the admission, because both are dependency calls a wedged
	// deployment can stall and neither is work the caller should wait on
	// forever. See Limits.CommandTimeout for what the bound is worth.
	ctx, cancel := context.WithTimeout(ctx, e.cfg.Limits.CommandTimeout)
	defer cancel()
	if err := e.cfg.Authorizer.AuthorizeControl(ctx, principal, decoded.session, spec.kind); err != nil {
		return nil, err
	}
	status, readable, err := decoded.admit(ctx, e.cfg.Admitter, principal)
	if err != nil {
		var refusal *admission.Error
		if errors.As(err, &refusal) {
			return refusalBody(refusal.Code)
		}
		// Everything else is a fault or a dependency condition rather than a
		// decision about this command: a cancelled context, a closing store, a
		// provider outage. Answering it in a refusal envelope would tell the
		// client its command was rejected when the truth is that nobody knows
		// whether it was admitted -- which is precisely the unknown outcome
		// the retry contract exists for.
		return nil, err
	}
	return replyFor(status, readable)
}

// replyFor projects the authoritative SessionInbox record into Core's public
// command status.
//
// It is the RECORD that is described, never the call: admission's "this call
// created it" boolean is deliberately unread, because a retry after an unknown
// outcome must receive the same bytes the original acceptance received. If the
// two differed, a client could tell whether its earlier attempt landed -- and
// the whole point of the durable CommandID is that it does not have to know.
//
// # The state projection
//
// SessionStore's inbox has five states and Core's status has four, and they are
// not the same partition. What this acknowledges is durable ACCEPTANCE, which
// Core's own doc defines as "the inbox commit succeeded; it does not promise
// that a Host has already applied the command" -- true of pending, claimed and
// applying alike, so all three are accepted. Applied is applied. Rejected is
// rejected and carries the record's own stable ErrorDetail; reporting it as
// accepted would be a client believing a rejected command is in flight.
//
// Core's CommandStatePending is never minted here. It has no counterpart in the
// released inbox vocabulary -- a claimed command is still exactly "accepted,
// not yet applied" -- and inventing a mapping for it would put a state on the
// wire that no durable record means.
//
// An unrecognised state is a FAULT rather than an acceptance. A record whose
// state this build does not know is a store disagreeing with this build, and
// defaulting it to accepted would report the one case that must never be
// reported optimistically.
//
// The PROJECTION is internal/command's, applied by the decoder rather than
// here: A3.3 gave the REST controls the same answer to build, and two
// projections of one five-state vocabulary is two places for one durable record
// to be described differently over two transports. The paragraphs above are the
// argument for the mapping and now live beside it, in command.StatusFor.
func replyFor(status sessionwire.CommandStatus, readable bool) ([]byte, error) {
	if !readable {
		return nil, ErrUnreadableRecord
	}
	body, err := status.MarshalJSON()
	if err != nil {
		// Core validates on marshal, so this is a record that cannot be
		// described publicly -- a rejected command with no rejection detail, an
		// empty CommandID. It is a fault, and it must not be reported as one of
		// the nine public codes.
		return nil, fmt.Errorf("%w: %v", ErrUnreadableRecord, err)
	}
	return body, nil
}

// refusalBody is the ONE construction of a refusal reply.
//
// # There is no message, deliberately
//
// Core makes `message` optional and `code` the member a client switches on, and
// its own doc says so. A message here would be a second prose vocabulary that
// the REST alternative (A3.3) would then have to reproduce word for word to
// keep the two answers identical -- and the one thing that must be identical is
// the code, which is carried whole from admission's own classification. The
// consumer falls back to the code when there is no message.
//
// # retryable is false for every code this surface can answer with
//
// retryable:true is a promise that repeating the IDENTICAL bytes could succeed
// with nothing else changing, and no refusal admission mints today is one.
// invalid_request, command_rejected, session_not_found and the three gate codes
// are all facts about the request or about durable state the request cannot
// change by being sent again. runtime_unavailable is the one worth stating
// rather than assuming: admission mints it for an unresolvable launch target,
// for an oversized payload and for the missing V1 create reservation, and it
// mints it for a directory READ FAILURE too -- so the code cannot distinguish
// the single transient cause from three permanent ones, and advertising the
// whole code as retryable would tell a client to hammer a misconfiguration.
//
// A dependency outage that IS transient does not arrive here at all: it is not
// an *admission.Error, so Admit returns it as an error and the transport
// answers with its own temporary failure.
func refusalBody(code sessionwire.ErrorCode) ([]byte, error) {
	// retryable is READ FROM THE SHARED AUTHORITY rather than written false
	// here, and the value it returns today is false for every code -- so this
	// is not a behaviour change, it is the removal of the second literal.
	//
	// The paragraph above states the property; a literal states it only for as
	// long as nobody edits the other edge. A3.3 had to choose a REST status for
	// each of these codes, retryability over REST is a function of that status,
	// and the two answers must be one: `runtime_unavailable` mapped to 503
	// would have been retryable over REST and false here, for the identical
	// refusal. A code the authority has no ruling for is not retryable, which
	// is the fail-closed direction.
	status, ruled := command.RefusalStatus(code)
	body, err := json.Marshal(sessionwire.ErrorEnvelope{Error: sessionwire.ErrorDetail{
		Code:      code,
		Retryable: ruled && command.RetryableStatus(status),
	}})
	if err != nil {
		// REACHABLE, and it was described as unreachable until a gate measured
		// it. Core refuses to marshal an ErrorDetail with an empty code
		// ("missing_required_field"), and an empty code is a value admission
		// can produce: (*admission.Error).Error() explicitly contemplates
		// Code == "", so a zero-valued &admission.Error{} from any present or
		// future admission path arrives here. Without this arm the browser
		// would receive an EMPTY successful RPC reply -- the one answer a
		// client cannot classify at all -- so it fails closed as a fault
		// instead, and TestARefusalWithNoCodeIsAFaultNotAnEmptyReply drives it.
		return nil, fmt.Errorf("%w: %v", ErrUnreadableRecord, err)
	}
	return body, nil
}
