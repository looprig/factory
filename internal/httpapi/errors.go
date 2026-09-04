package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	internalidentity "github.com/looprig/factory/internal/identity"
	"github.com/looprig/sessionstore"
)

// Factory's public error codes are Core's ErrorCode vocabulary, extended.
//
// Core v0.7.0 names nine codes, and every one of them is a condition about a
// SESSION or a COMMAND: invalid_request, unsupported_version,
// session_not_found, command_rejected, gate_resolved, gate_not_resumable,
// gate_expired, gate_response_invalid, runtime_unavailable. It names none of
// the conditions a public HTTP surface answers before it reaches a session --
// an unauthenticated caller, a denied operation, an unknown route, a wrong
// method, an oversized body, a fault inside Factory. That is measured, not
// assumed: `grep` over sessionwire/v1 at v0.7.0 finds one occurrence of
// "not_found" and none of "unauthorized", "unauthenticated" or "internal".
//
// So Factory declares those, and it declares them AS sessionwire.ErrorCode
// values rather than as a second type. Core's ErrorDetail.Validate requires
// only a non-empty code, and its unknown-member capture is explicitly a
// forward-compatibility mechanism, so an extended code travels in a Core
// envelope without a second body shape for a client to branch on. The whole
// point of "use Core stable error codes and JSON envelopes" is that a client
// reads ONE shape and switches on ONE member; inventing a parallel
// {"code":...} body for the pre-session conditions -- which is what this
// package's guard did before A2.1 -- defeats that more thoroughly than
// extending the vocabulary does.
//
// A condition Core DOES name must be spelled with Core's constant.
// TestACodeCoreNamesIsSpelledWithCoresConstant holds the two sets disjoint, and
// states its own limit: it cannot see a code a later Core adds.
const (
	// ErrorCodeUnauthenticated reports that no verified credential was
	// presented. It is 401 and a client recovers by authenticating.
	ErrorCodeUnauthenticated sessionwire.ErrorCode = "unauthenticated"
	// ErrorCodeNotAuthorized reports that a verified principal may not perform
	// the operation. It names no tenant, session, object or command: a denial
	// must not disclose whether the named resource exists elsewhere.
	ErrorCodeNotAuthorized sessionwire.ErrorCode = "not_authorized"
	// ErrorCodeRouteNotFound reports a path this version serves no route for.
	// It is deliberately NOT session_not_found: conflating "there is no such
	// endpoint" with "there is no such session" would make a client's retry
	// logic depend on which of the two Factory happened to mean.
	ErrorCodeRouteNotFound sessionwire.ErrorCode = "route_not_found"
	// ErrorCodeMethodNotAllowed reports a method this route does not serve.
	// The response carries an Allow header naming the ones it does.
	ErrorCodeMethodNotAllowed sessionwire.ErrorCode = "method_not_allowed"
	// ErrorCodeUnsupportedMediaType reports a request body that is not JSON.
	ErrorCodeUnsupportedMediaType sessionwire.ErrorCode = "unsupported_media_type"
	// ErrorCodePayloadTooLarge reports a request body past the configured
	// ceiling. The ceiling is applied while READING, so it bounds a body whose
	// Content-Length lied as well as one that declared its size honestly.
	ErrorCodePayloadTooLarge sessionwire.ErrorCode = "payload_too_large"
	// ErrorCodeNotImplemented reports a route this build serves no handler for.
	// Every occurrence is a later task in runbook 05; see routeTable.
	ErrorCodeNotImplemented sessionwire.ErrorCode = "not_implemented"
	// ErrorCodeInternal reports a fault inside Factory. Its message is fixed
	// text: a dependency's error string is diagnosis for an operator's log, not
	// for a caller's response body.
	ErrorCodeInternal sessionwire.ErrorCode = "internal_error"
	// ErrorCodeUnavailable reports a dependency Factory could not reach, which
	// is a different operational condition from a rejected request and is
	// retryable.
	ErrorCodeUnavailable sessionwire.ErrorCode = "unavailable"
	// ErrorCodeTimeout reports that the deadline this router imposed on the
	// request's work expired.
	ErrorCodeTimeout sessionwire.ErrorCode = "timeout"
)

// statusClientClosedRequest is the status recorded when the CALLER's context
// was cancelled. There is nobody left to read the response, so the code is one
// nginx made conventional for exactly this case rather than an invented 5xx
// that would count against the deployment's error rate.
const statusClientClosedRequest = 499

// factoryErrorCodes are the codes this package declares because Core names no
// equivalent: the constants above plus the origin guard's rejection reasons,
// which travel in the same envelope and are therefore part of the same
// vocabulary. The guard half is DERIVED from reasonMessages rather than listed,
// so a reason added in guard.go is checked against Core's set without anybody
// remembering to add it here.
func factoryErrorCodes() []sessionwire.ErrorCode {
	return append(declaredErrorCodes(), guardErrorCodes()...)
}

// guardErrorCodes projects the origin and CSRF guard's reasons into the public
// code vocabulary.
func guardErrorCodes() []sessionwire.ErrorCode {
	codes := make([]sessionwire.ErrorCode, 0, len(reasonMessages))
	for reason := range reasonMessages {
		codes = append(codes, sessionwire.ErrorCode(reason))
	}
	slices.Sort(codes)
	return codes
}

func declaredErrorCodes() []sessionwire.ErrorCode {
	return []sessionwire.ErrorCode{
		ErrorCodeUnauthenticated,
		ErrorCodeNotAuthorized,
		ErrorCodeRouteNotFound,
		ErrorCodeMethodNotAllowed,
		ErrorCodeUnsupportedMediaType,
		ErrorCodePayloadTooLarge,
		ErrorCodeNotImplemented,
		ErrorCodeInternal,
		ErrorCodeUnavailable,
		ErrorCodeTimeout,
	}
}

// coreErrorCodes are Core's own, named through Core's constants so a rename or
// a removal upstream is a compile failure here rather than a silent divergence.
func coreErrorCodes() []sessionwire.ErrorCode {
	return []sessionwire.ErrorCode{
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
}

// apiErrorCodes is every code this package can answer with: its own, plus the
// Core codes its mappings produce. It is what the marshalling sweep draws from,
// so a code added to either list is covered without editing a test.
func apiErrorCodes() []sessionwire.ErrorCode {
	codes := factoryErrorCodes()
	for _, code := range coreErrorCodes() {
		if !slices.Contains(codes, code) {
			codes = append(codes, code)
		}
	}
	return codes
}

// apiError is one public failure: the status, the stable code, and the fixed
// message that goes with them.
//
// There is no retryable field. Retryability is a function of the status --
// see retryableStatus -- because a per-construction boolean is a place a caller
// can disagree with another caller about what the same status means.
type apiError struct {
	status  int
	code    sessionwire.ErrorCode
	message string
}

// retryableStatus reports whether a client may usefully repeat the identical
// request.
//
// The set is the three statuses that name a condition the REQUEST did not
// cause: a bad gateway, an unavailable dependency, and an expired deadline. A
// 500 is deliberately absent -- a fault Factory could not classify is not one a
// client should be told to hammer -- and so is 403 csrf_token_expired, which is
// recoverable but only after the client changes the request by fetching a new
// token, which is not the same claim.
func retryableStatus(status int) bool {
	switch status {
	case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

// fallbackErrorBody is the answer when this package cannot build an envelope.
//
// It is a fixed literal rather than a second marshalling attempt because the
// only way to reach it is that marshalling already failed. Core refuses to
// marshal an ErrorDetail with an empty code, so without this an API failure
// could answer with an EMPTY body -- which a browser is free to sniff and a
// client cannot branch on. "API 404/401/500 must never fall through to SPA
// HTML" is a claim about every path out of this function, including this one.
var fallbackErrorBody = []byte(`{"error":{"code":"internal_error","message":"the server could not describe this failure","retryable":false}}`)

// writeAPIError writes e as a Core ErrorEnvelope.
//
// It sets the three headers that make the answer unambiguous on its own, so it
// is safe to call from a path the router's header middleware did not run on:
// the content type, the sniffing refusal that makes the content type binding,
// and no-store, because every response this package produces is either private
// authenticated data or a failure about it.
func writeAPIError(w http.ResponseWriter, e apiError) {
	status := e.status
	body, err := json.Marshal(sessionwire.ErrorEnvelope{Error: sessionwire.ErrorDetail{
		Code:      e.code,
		Message:   e.message,
		Retryable: retryableStatus(status),
	}})
	if err != nil {
		status = http.StatusInternalServerError
		body = fallbackErrorBody
	}
	writeJSONBytes(w, status, body)
}

// writeJSONBytes is the single write path for every JSON body this package
// produces, so no response can acquire a different header set by being written
// somewhere else.
func writeJSONBytes(w http.ResponseWriter, status int, body []byte) {
	header := w.Header()
	header.Set("Content-Type", "application/json")
	header.Set("X-Content-Type-Options", "nosniff")
	header.Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// authenticationFailure maps an authentication error onto a public answer.
//
// The two classes are kept apart on purpose, and the direction of each is the
// one internal/identity's ErrVerifierUnavailable doc argues for: a rejected
// credential is the caller's to fix, and a verifier that cannot decide is the
// deployment's. Reporting the second as a rejection signs every user out for as
// long as the credential service is down.
func authenticationFailure(err error) apiError {
	switch {
	case errors.Is(err, identity.ErrUnauthenticated):
		return apiError{
			status:  http.StatusUnauthorized,
			code:    ErrorCodeUnauthenticated,
			message: "the request presented no credential this deployment accepts",
		}
	case errors.Is(err, internalidentity.ErrVerifierUnavailable):
		return apiError{
			status:  http.StatusServiceUnavailable,
			code:    ErrorCodeUnavailable,
			message: "credentials cannot be verified at the moment",
		}
	default:
		return internalFailure()
	}
}

// authorizationFailure maps an authorization error onto a public answer.
//
// A denial stays a denial: it carries internal/identity's sentinel and nothing
// derived from the resource, because ErrUnauthorized's whole contract is that a
// refusal discloses no identifier. Anything else the authorizer returns is a
// fault, not a decision, and must not be answered as one -- reporting a broken
// authorizer as 403 would make an outage look like a permissions change.
func authorizationFailure(err error) apiError {
	if errors.Is(err, internalidentity.ErrUnauthorized) {
		return apiError{
			status:  http.StatusForbidden,
			code:    ErrorCodeNotAuthorized,
			message: "this principal may not perform that operation",
		}
	}
	return internalFailure()
}

// catalogFailure maps a SessionStore catalog read onto a public answer.
//
// Three catalog codes become one 404. not_found and deleted are the same
// public fact -- there is no session here -- and reporting them apart would
// tell a caller that an identifier they cannot read WAS once real. identity is
// with them because it is what the store returns for a record whose stored
// tenant or session does not match the one asked for, which is precisely the
// cross-tenant case that must be indistinguishable from absence.
//
// Everything else is 500 by DEFAULT rather than by enumeration, so a code
// sessionstore adds later lands on the safe answer instead of falling through
// to a 404 that would report a session absent because the store misbehaved.
func catalogFailure(err error) apiError {
	if failure, ok := contextFailure(err); ok {
		return failure
	}
	var catalog *sessionstore.CatalogError
	if !errors.As(err, &catalog) {
		return internalFailure()
	}
	switch catalog.Code {
	case sessionstore.CatalogErrorNotFound, sessionstore.CatalogErrorDeleted, sessionstore.CatalogErrorIdentity:
		return sessionNotFound()
	case sessionstore.CatalogErrorCursor:
		return apiError{
			status:  http.StatusBadRequest,
			code:    sessionwire.ErrorCodeInvalidRequest,
			message: "the cursor is not one this session issued; restart the walk",
		}
	default:
		return internalFailure()
	}
}

// contextFailure separates the two ways a context ends, because they need
// opposite answers. A deadline is the one this router imposed, so the caller is
// told to try again; a cancellation is the CALLER's, so there is nobody left to
// read a response and the status says so rather than counting as a server
// fault.
func contextFailure(err error) (apiError, bool) {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return apiError{
			status:  http.StatusGatewayTimeout,
			code:    ErrorCodeTimeout,
			message: "the request exceeded the time this deployment allows it",
		}, true
	case errors.Is(err, context.Canceled):
		return apiError{
			status:  statusClientClosedRequest,
			code:    ErrorCodeTimeout,
			message: "the request was cancelled",
		}, true
	default:
		return apiError{}, false
	}
}

// sessionNotFound is the ONE construction of the 404 a session read answers
// with. It is a function rather than repeated literals because the property it
// carries is byte identity: a session in another tenant and a session that
// never existed must produce the same response, and two literals are two places
// for one of them to acquire a distinguishing word.
func sessionNotFound() apiError {
	return apiError{
		status:  http.StatusNotFound,
		code:    sessionwire.ErrorCodeSessionNotFound,
		message: "there is no such session",
	}
}

// internalFailure is the fixed answer to a fault. Its message names no
// dependency and carries no wrapped text: a storage DSN, a provider path or a
// verifier's diagnostic belongs in an operator's log, and this is the one place
// that decision is made for every mapping above.
func internalFailure() apiError {
	return apiError{
		status:  http.StatusInternalServerError,
		code:    ErrorCodeInternal,
		message: "the request could not be completed",
	}
}
