package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	"github.com/looprig/factory/internal/admission"
	"github.com/looprig/factory/internal/command"
	internalidentity "github.com/looprig/factory/internal/identity"
	"github.com/looprig/sessionstore"
)

// This file is the REST half of one admission contract.
//
// Specification section 8.1 makes these routes and the ClientLink RPCs two
// spellings of the same operation, so the rule applied throughout is that where
// the two edges must answer alike, the answer comes from `internal/command` and
// is called rather than restated here: the status and retryability of a
// classified refusal (command.RefusalStatus, command.RetryableStatus), the
// meaning of a store's "no such session" (command.SessionAbsent), and the
// projection of a durable record into Core's public status (command.StatusFor).
// What is genuinely this edge's own -- the path identifiers, the media type,
// the success status a legacy client expects -- is decided here and nowhere
// else.
//
// The create is deliberately absent; see routeTable for the binding it cannot
// author, and ControlAdmitter below for what that absence buys.

// ControlAdmitter is the durable command plane the control routes admit into.
//
// It is `internal/admission`'s V1 surface, declared here as the narrow
// interface THIS package calls. Every method takes a Core request type and
// returns the authoritative SessionInbox record, so nothing about a URL, a
// header or a path value reaches the service.
//
// AdmitCreate is PRESENT as of A3.1, and it was absent for two tasks before
// that because this composition could not author the immutable SessionBinding a
// V1 create reservation carries. It now can: WithSessionBinding supplies the
// two deployment members and admission derives the third from the create's own
// identity. Adding the method here and clearing the create's owner in
// routeTable were one change, as that absence note predicted.
//
// Every method returns a DISPOSITION entry. A create chooses the session's
// protocol mode, disposition is the only choice a Host can take residency on,
// and every later command is admitted into the same family -- a command in the
// legacy inbox of a disposition session is one no Host can ever apply.
//
// AdmitLegacyCreate is absent for a second, independent reason: it is a create.
//
// The `bool` every method returns is admission's "this call is the one that
// accepted it". This edge deliberately does not read it -- see serveControl --
// and the seam still declares it, because the seam's shape is the service's and
// an adapter would be a second place for the two edges to answer differently.
type ControlAdmitter interface {
	AdmitCreate(ctx context.Context, principal identity.Principal, req sessionwire.CreateRequest) (sessionstore.DispositionInboxEntry, bool, error)
	AdmitInput(ctx context.Context, principal identity.Principal, req sessionwire.InputRequest) (sessionstore.DispositionInboxEntry, bool, error)
	AdmitInterrupt(ctx context.Context, principal identity.Principal, req sessionwire.InterruptRequest) (sessionstore.DispositionInboxEntry, bool, error)
	AdmitRestore(ctx context.Context, principal identity.Principal, req sessionwire.RestoreRequest) (sessionstore.DispositionInboxEntry, bool, error)
	AdmitGateResponse(ctx context.Context, principal identity.Principal, req sessionwire.GateResponseRequest) (sessionstore.DispositionInboxEntry, bool, error)
}

// CommandDelivery wakes consumption of an already admitted command on whatever
// route this replica holds for the session.
//
// Its signature is `internal/routing`.Bindings.Deliver's, stated in Core's
// vocabulary, so A9.1 composes the two without an adapter. What it is NOT is an
// acknowledgement path: the durable inbox record is the acknowledgement, this
// is a wake-up, and every failure it can produce -- no local subscriber, no
// route, a Host that refused -- leaves a correct durable record behind. See
// Router.deliverAdmitted.
type CommandDelivery interface {
	Deliver(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, delivery sessionwire.HostLinkCommandDelivery) error
}

// controlSpec is what one control route IS: the status a success answers with,
// and the decode-and-admit step that names exactly one Core request type and
// exactly one service method.
//
// The two live in one entry rather than in two tables keyed by kind, so a route
// cannot acquire a status without an admission or an admission without a
// status.
type controlSpec struct {
	// success is the status a durably admitted command is answered with.
	//
	// It is per ROUTE because the legacy surface's was: harness/pkg/serve
	// answered input, interrupt and restore 200 and the gate response 202.
	// "Port compatible status codes" is the runbook's step 1, and a client
	// written against that surface compares against those numbers.
	//
	// It does not vary by durable STATE. Pending, claimed, applying, applied
	// and rejected are all answers about a command that WAS durably admitted,
	// and Core's `status` member is the one a client branches on; a second
	// distinction in the HTTP status would be two places for one fact. See
	// TestEveryDurableStateIsAnsweredUnderTheSameSuccessStatus.
	success int

	// admit decodes the body and calls the one service method for this route.
	// It returns a classified refusal or a fault exactly as the service does;
	// nothing here reclassifies one into the other.
	admit func(rt *Router, r *http.Request, principal identity.Principal, session sessionwire.SessionID, body []byte) (admitted, error)
}

// admitted is what a control spec answers: the PUBLIC projection of the
// durable record, plus the identity a delivery attempt names.
//
// The projection itself is internal/command's single authority, called by each
// spec; nothing here re-derives a state.
//
// readable is "this record can be described publicly". A record whose state
// this build does not recognise, or a rejected one carrying no error detail,
// is a fault rather than an acceptance -- see command.StatusForDisposition for
// why the second case exists at all.
type admitted struct {
	status   sessionwire.CommandStatus
	readable bool
	command  sessionwire.CommandID
	// session OVERRIDES the URL's session for a delivery attempt, and it is
	// set by exactly one spec.
	//
	// The four session-scoped controls leave it empty and keep using the URL
	// value, which decodeCommand has already held equal to the body's. The
	// CREATE names no session in its path -- /v1/sessions is where a caller
	// CHOOSES one -- so it is the one control whose delivery target can only
	// come from the request it just admitted.
	session sessionwire.SessionID
}

// dispositionAdmitted projects every admitted command. The create's spec
// calls it too, and adds the one member only a create sets.
func dispositionAdmitted(entry sessionstore.DispositionInboxEntry, err error) (admitted, error) {
	if err != nil {
		return admitted{}, err
	}
	status, readable := command.StatusForDisposition(entry)
	return admitted{status: status, readable: readable, command: entry.Record.Descriptor.CommandID}, nil
}

// dropCreated discards admission's "this call is the one that accepted it".
// This edge deliberately does not read it -- a retry and a first acceptance get
// the same answer -- and the seam still returns it because the seam's shape is
// the service's. See ControlAdmitter.
func dropCreated(entry sessionstore.DispositionInboxEntry, _ bool, err error) (sessionstore.DispositionInboxEntry, error) {
	return entry, err
}

// controlSpecs is the table, keyed by the same command kinds the route table
// authorizes under and the ClientLink RPC vocabulary names.
//
// It is a function rather than a package variable so no caller can hold a
// reference that lets it add an operation at run time.
func controlSpecs() map[sessionstore.CommandKind]controlSpec {
	return map[sessionstore.CommandKind]controlSpec{
		commandInput: {success: http.StatusOK, admit: func(rt *Router, r *http.Request, p identity.Principal, session sessionwire.SessionID, body []byte) (admitted, error) {
			var req sessionwire.InputRequest
			if err := decodeCommand(body, &req, session); err != nil {
				return admitted{}, err
			}
			return dispositionAdmitted(dropCreated(rt.admissions.AdmitInput(r.Context(), p, req)))
		}},
		commandInterrupt: {success: http.StatusOK, admit: func(rt *Router, r *http.Request, p identity.Principal, session sessionwire.SessionID, body []byte) (admitted, error) {
			var req sessionwire.InterruptRequest
			if err := decodeCommand(body, &req, session); err != nil {
				return admitted{}, err
			}
			return dispositionAdmitted(dropCreated(rt.admissions.AdmitInterrupt(r.Context(), p, req)))
		}},
		commandRestore: {success: http.StatusOK, admit: func(rt *Router, r *http.Request, p identity.Principal, session sessionwire.SessionID, body []byte) (admitted, error) {
			var req sessionwire.RestoreRequest
			if err := decodeCommand(body, &req, session); err != nil {
				return admitted{}, err
			}
			return dispositionAdmitted(dropCreated(rt.admissions.AdmitRestore(r.Context(), p, req)))
		}},
		commandGateResponse: {success: http.StatusAccepted, admit: func(rt *Router, r *http.Request, p identity.Principal, session sessionwire.SessionID, body []byte) (admitted, error) {
			var req sessionwire.GateResponseRequest
			if err := decodeCommand(body, &req, session); err != nil {
				return admitted{}, err
			}
			// The gate route carries a SECOND identifier, and it is compared
			// for the session identifier's reason one line up: {gid} is in the
			// URL a caller chose and gate_id is in the body the service will
			// admit, so a handler comparing only the session would answer one
			// gate's question with another gate's answer.
			if gate := sessionwire.GateID(r.PathValue("gid")); req.GateID != gate {
				return admitted{}, mismatchedIdentifier("gate_id")
			}
			return dispositionAdmitted(dropCreated(rt.admissions.AdmitGateResponse(r.Context(), p, req)))
		}},
		// The CREATE, runbook A3.1. It is the one control whose session
		// identifier comes from the BODY rather than the URL -- /v1/sessions
		// names no session because the caller is choosing one -- so it is the
		// one spec that passes no URL session to decodeCommand.
		//
		// 201 Created, unlike the four above: this is the only control that
		// brings a resource into existence.
		commandCreate: {success: http.StatusCreated, admit: func(rt *Router, r *http.Request, p identity.Principal, _ sessionwire.SessionID, body []byte) (admitted, error) {
			var req sessionwire.CreateRequest
			if err := decodeCreate(body, &req); err != nil {
				return admitted{}, err
			}
			entry, err := dropCreated(rt.admissions.AdmitCreate(r.Context(), p, req))
			result, err := dispositionAdmitted(entry, err)
			result.session = entry.Record.Descriptor.SessionID
			return result, err
		}},
	}
}

// decodeCommand applies Core's own strict decoder and then compares the session
// the body names against the session the PATH named.
//
// # The decode
//
// json.Unmarshal here runs the Core type's own UnmarshalJSON, which refuses an
// unknown member, a duplicate member and a wrong-typed one, and then calls the
// type's Validate. So "the bytes decode" and "the request is a valid V1
// command" are one answer rather than two, and it is Core's answer rather than
// a grammar this package restates -- which is also what makes the refusal
// identical to the one the ClientLink produces for the identical bytes.
//
// A body Core refuses is answered invalid_request before any durable work, and
// that discloses nothing: the answer is derived from the caller's own bytes and
// from no tenant state whatever.
//
// # The comparison, which is the one this edge exists to make
//
// The authorization decision was already made, by serveRoute, about the session
// in {sid}; the admission will be made about the session in the body. A handler
// that did not compare them would authorize one session and admit another --
// exactly the defect A6.2 removed from the RPC path, where the authorized
// session and the admitted session came from two decoders. The RPC has no path
// segment and closes that by having ONE reader; this edge has two identifiers
// by construction and closes it by refusing a disagreement.
//
// Rewriting the body to match the path is the other available answer and is
// worse: it durably admits a command the caller did not send, under an identity
// the caller will retry with.
//
// The decoded session is recovered by a type switch over the four request
// types rather than through an interface, because Core declares none that
// exposes the member and declaring one HERE would be a second grammar for a
// value Core already owns. The default arm fails closed, so a fifth command
// added to controlSpecs without a case is refused rather than admitted
// uncompared.
func decodeCommand(body []byte, req any, path sessionwire.SessionID) error {
	if err := json.Unmarshal(body, req); err != nil {
		return refusedRequest(err)
	}
	var decoded sessionwire.SessionID
	switch typed := req.(type) {
	case *sessionwire.InputRequest:
		decoded = typed.SessionID
	case *sessionwire.InterruptRequest:
		decoded = typed.SessionID
	case *sessionwire.RestoreRequest:
		decoded = typed.SessionID
	case *sessionwire.GateResponseRequest:
		decoded = typed.SessionID
	case *sessionwire.CreateRequest:
		// A create reaching HERE would be one routed at a session-scoped path,
		// which routeTable does not do. It is listed so that if one ever is,
		// it is COMPARED like every other command rather than falling into the
		// fault arm below -- the create's own route uses decodeCreate, which
		// has no path identifier to compare against at all.
		decoded = typed.SessionID
	default:
		// Unreachable through controlSpecs, which names the four types above.
		// It fails closed rather than trusting that, because the cost of being
		// wrong is a command admitted for a session nobody compared.
		return fmt.Errorf("httpapi: %T is not a V1 command request", req)
	}
	if decoded != path {
		return mismatchedIdentifier("session_id")
	}
	return nil
}

// decodeCreate decodes the one command whose route carries no session.
//
// It is separate from decodeCommand rather than a sentinel path through it,
// because a sentinel meaning "do not compare" is a value the other four routes
// could also pass -- by a copy-paste, or by a later refactor threading an empty
// path value -- and it would silently disable the comparison that stops a body
// naming another session from being admitted. There is no such value here: this
// function has no path parameter to be given one.
//
// What replaces the comparison is that the body's session is the ONLY session
// in play. /v1/sessions is where a caller chooses one, so there is nothing for
// it to disagree with, and admission files it under the caller's own tenant.
func decodeCreate(body []byte, req *sessionwire.CreateRequest) error {
	if err := json.Unmarshal(body, req); err != nil {
		return refusedRequest(err)
	}
	return nil
}

// refusedRequest classifies a body Core's decoder refused.
//
// It carries admission's own refusal type rather than an apiError, so every
// refusal this file produces reaches one mapping -- admissionFailure -- whether
// it was minted here or by the service. Two constructions of one code is how
// the REST answer and the RPC answer drift apart.
func refusedRequest(cause error) error {
	return &admission.Error{Code: sessionwire.ErrorCodeInvalidRequest, Cause: cause}
}

// mismatchedIdentifier is the refusal for a body that names a different
// resource than the path did. It names the MEMBER and not the values: the
// values are the caller's own, but echoing an identifier into an error message
// is how one gets into a log that a later reader treats as authoritative.
func mismatchedIdentifier(member string) error {
	return refusedRequest(fmt.Errorf("httpapi: the request body's %s is not the one the path names", member))
}

// serveControl admits one state-changing command and answers from the
// authoritative durable record.
//
// # The order
//
// Everything that can be decided without the caller's body has already been
// decided by serveRoute: the method, the deadline, the session identifier's
// validity, the authorization decision under this route's own command kind, the
// media type and the body ceiling, and the session's existence within the
// principal's tenant. What is left here is the body, the service, and the
// answer.
//
// # The answer describes the RECORD, never the call
//
// Admission's "this call created it" boolean is deliberately unread, so an
// original acceptance and a retry that found the stored record produce the same
// bytes. If the two differed a client could learn whether its earlier attempt
// landed -- and the whole point of the durable CommandID is that it does not
// have to.
func (rt *Router) serveControl(kind sessionstore.CommandKind) http.Handler {
	spec, known := controlSpecs()[kind]
	if !known {
		// Unreachable from routeTable, which builds a control rule only for a
		// kind this table names. It is a handler rather than a panic because a
		// router that refused to build would take the whole surface down for
		// one route; this answers the one route.
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			writeAPIError(w, internalFailure())
		})
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		operation, authenticated := internalidentity.OperationContextFrom(r.Context())
		if !authenticated {
			// Unreachable through the composed chain, which authenticates
			// before it routes. It fails closed rather than trusting that.
			writeAPIError(w, internalFailure())
			return
		}
		if rt.admissions == nil {
			writeAPIError(w, controlUnavailable())
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			// The bytes were already read once and buffered by
			// readBoundedJSONBody, so a failure here is not a socket: it is
			// this process. It is answered as the caller's own request being
			// unreadable, which is what the ceiling path answers too.
			writeAPIError(w, apiError{
				status:  http.StatusBadRequest,
				code:    sessionwire.ErrorCodeInvalidRequest,
				message: "the request body could not be read",
			})
			return
		}
		session := sessionwire.SessionID(r.PathValue("sid"))

		result, err := spec.admit(rt, r, operation.Principal, session, body)
		if err != nil {
			writeAPIError(w, admissionFailure(err))
			return
		}
		if !result.readable {
			// A record whose state this build does not recognise is a store
			// disagreeing with this build. Reporting it as an acceptance is the
			// one answer that must never be produced optimistically.
			writeAPIError(w, internalFailure())
			return
		}
		payload, err := result.status.MarshalJSON()
		if err != nil {
			// Core validates on marshal, so this is a record that cannot be
			// described publicly -- a rejected command with no rejection
			// detail, an empty CommandID. It is a fault and must not be
			// reported as one of the public codes.
			writeAPIError(w, internalFailure())
			return
		}
		// The URL's session, except for the one route that has none. Keeping
		// the URL authoritative where there IS one preserves the property
		// decodeCommand establishes: the path a caller addressed and the body
		// it sent name the same session.
		target := session
		if result.session != "" {
			target = result.session
		}
		rt.deliverAdmitted(r.Context(), operation.Principal.Tenant(), target, result.command)
		writeJSONBytes(w, spec.success, payload)
	})
}

// deliverAdmitted is runbook A3.3 step 3: after admission, schedule best-effort
// immediate delivery, and return the durable accepted status even if delivery
// fails.
//
// # Why the error is dropped, rather than logged into the answer
//
// The durable record is the acknowledgement. A delivery failure means a Host
// has not been woken yet, not that the command was lost: the record is in the
// inbox, the session's owner consumes it when it next reads, and the reconciler
// is what repairs a command nobody claimed. Reporting the failure to the caller
// would invite a retry that can only find the same record, and reporting it as
// a refusal would tell a client its command was rejected when it was accepted.
//
// The common case is a FAILURE, and that is expected rather than a defect: the
// routing table delivers only to a session some local subscriber holds demand
// for, and a REST caller is not a subscriber. A deployment where every command
// arrives over REST and every viewer over a ClientLink is exactly the shape
// where this attempt usually finds no route and the durable path always works.
//
// It is called BEFORE the response is written and on the request's own context,
// so the attempt is bounded by the same deadline everything else on this
// request is, and so a test can observe it without a goroutine to synchronise
// on. "Schedule" is not read as "detach": a detached attempt would outlive the
// request's context, and A6.2 measured what an unbounded admission does to a
// link.
//
// A nil seam makes no attempt and changes no response.
func (rt *Router) deliverAdmitted(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, command sessionwire.CommandID) {
	if rt.delivery == nil {
		return
	}
	// The PUBLIC CommandID, which is the retry-stable identity both sides of
	// the HostLink agree on. The proposed runtime identity is the store's own
	// and means nothing to a Host that did not win the admission.
	_ = rt.delivery.Deliver(ctx, tenant, session, sessionwire.HostLinkCommandDelivery{
		CommandID: command,
	})
}

// admissionFailure maps everything the admission plane can return onto a public
// answer.
//
// # The two classes, and why they must not be folded
//
// A classified *admission.Error is a DECISION about the caller's command, and a
// client stops retrying a decision. Anything else is a fault or a dependency
// condition -- a cancelled context, a closing store, a provider outage -- and
// answering one of those in a refusal envelope would tell the client its
// command was rejected when the truth is that nobody knows whether it was
// admitted, which is precisely the unknown outcome the durable CommandID exists
// for. internal/admission returns a fault AS ITSELF, carrying no public code,
// so the two are told apart by type rather than by inspecting a message.
//
// # The status comes from the shared authority
//
// A code with no ruling is answered as a fault. That is the fail-closed
// direction and it is the second return's whole purpose: a public code nobody
// has decided a status for is a code somebody added to admission without
// deciding what it means to a client, and picking one here would make that
// decision silently. internal/command's own sweep holds that set complete, so
// this branch is unreachable through the released service and is kept because
// "unreachable" is a claim about today's service.
//
// # The refusal carries NO message
//
// Core makes `message` optional and `code` the member a client switches on. The
// ClientLink's refusal body carries the code alone, so an empty message here is
// what makes the two edges' refusal bytes IDENTICAL rather than merely
// equivalent -- and it is what A6.2 asked for in advance: a message here would
// be a second prose vocabulary that edge would have to reproduce word for word.
func admissionFailure(err error) apiError {
	var refusal *admission.Error
	if errors.As(err, &refusal) {
		status, ruled := command.RefusalStatus(refusal.Code)
		if !ruled {
			return internalFailure()
		}
		return apiError{status: status, code: refusal.Code}
	}
	if failure, ok := contextFailure(err); ok {
		return failure
	}
	if failure, ok := storeUnavailable(err); ok {
		return failure
	}
	return internalFailure()
}

// controlUnavailable is the answer when this composition has no command plane.
//
// It is the DEPLOYMENT's condition rather than a decision about the caller, so
// it is the retryable 503 a draining store produces and not a 501: the route
// exists and this build serves it, and another replica composed with an
// admission service can carry the identical request out.
func controlUnavailable() apiError {
	return apiError{
		status:  http.StatusServiceUnavailable,
		code:    ErrorCodeUnavailable,
		message: "this deployment cannot admit commands at the moment",
	}
}

// serveRealtime hands an authenticated request to the composed ClientLink.
//
// The supplier is consulted per REQUEST rather than once, because the node is
// started after the router is built; reading it once would capture the nil a
// composition holds between New and Start and answer 503 forever.
//
// Authentication has already happened above this handler, and the ClientLink
// authenticates its own connect credential again. That is not redundant: the
// two credentials are the same material verified by the same Verifier, but the
// HTTP one bounds who may reach the upgrade at all, and a replica that admitted
// every unauthenticated peer to the transport would have to refuse them after
// the handshake, which costs a connection slot per attempt.
func (rt *Router) serveRealtime() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if rt.realtime == nil {
			writeAPIError(w, realtimeUnavailable())
			return
		}
		handler := rt.realtime()
		if handler == nil {
			writeAPIError(w, realtimeUnavailable())
			return
		}
		handler.ServeHTTP(w, r)
	})
}

func realtimeUnavailable() apiError {
	return apiError{
		status:  http.StatusServiceUnavailable,
		code:    ErrorCodeUnavailable,
		message: "this deployment is not serving realtime connections at the moment",
	}
}
