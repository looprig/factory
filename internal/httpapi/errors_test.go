package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	factoryidentity "github.com/looprig/factory/identity"
	internalidentity "github.com/looprig/factory/internal/identity"
	"github.com/looprig/sessionstore"
)

// This file is the error foundation's own tests. It is an INTERNAL test package
// because the mapping functions are the seam between a dependency's typed
// failure and a public envelope, and that seam has no external caller: every
// public reader of it goes through the router, which routes_test.go drives from
// outside.

// TestEveryAPIErrorMarshalsAsACoreEnvelope drives every code the package can
// answer with through the real writer and requires a Core ErrorEnvelope back.
//
// The floor matters more than the rows: the set comes from apiErrorCodes, which
// is the same list writeAPIError's callers draw from, so a code added without a
// test is a code this sweep already covers.
func TestEveryAPIErrorMarshalsAsACoreEnvelope(t *testing.T) {
	t.Parallel()

	if len(apiErrorCodes()) == 0 {
		t.Fatal("no error codes are declared, so this sweep proves nothing")
	}
	for _, code := range apiErrorCodes() {
		recorder := httptest.NewRecorder()
		writeAPIError(recorder, apiError{status: http.StatusTeapot, code: code, message: "a message"})

		if got := recorder.Header().Get("Content-Type"); got != "application/json" {
			t.Errorf("%s: Content-Type = %q, want application/json", code, got)
		}
		if got := recorder.Header().Get("X-Content-Type-Options"); got != "nosniff" {
			t.Errorf("%s: X-Content-Type-Options = %q, want nosniff", code, got)
		}
		if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
			t.Errorf("%s: Cache-Control = %q, want no-store", code, got)
		}
		if recorder.Code != http.StatusTeapot {
			t.Errorf("%s: status = %d, want %d", code, recorder.Code, http.StatusTeapot)
		}

		var envelope sessionwire.ErrorEnvelope
		if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
			t.Fatalf("%s: the body %q is not a Core ErrorEnvelope: %v", code, recorder.Body, err)
		}
		if envelope.Error.Code != code {
			t.Errorf("%s: envelope code = %q", code, envelope.Error.Code)
		}
		if envelope.Error.Message != "a message" {
			t.Errorf("%s: envelope message = %q", code, envelope.Error.Message)
		}
	}
}

// TestACodeCoreNamesIsSpelledWithCoresConstant is the half of "use Core stable
// error codes" that a body-shape assertion cannot reach.
//
// Its limit is measured rather than promised: coreErrorCodes is a list of
// NAMED constants, so a code Core RENAMES or REMOVES breaks the compile, but a
// code Core ADDS in a later version leaves this list stale. It is the strongest
// check available without loading Core's source at test time, and the condition
// it defends -- Factory inventing a second spelling for a condition Core
// already names -- is the one that actually costs a client a branch.
func TestACodeCoreNamesIsSpelledWithCoresConstant(t *testing.T) {
	t.Parallel()

	core := coreErrorCodes()
	if len(core) == 0 {
		t.Fatal("no Core codes are named, so this comparison is vacuous")
	}
	for _, code := range factoryErrorCodes() {
		if slices.Contains(core, code) {
			t.Errorf("Factory declares %q, which Core already names; use Core's constant", code)
		}
	}
	// Anti-vacuity in the other direction: the split must actually split.
	if len(factoryErrorCodes()) == 0 {
		t.Fatal("no Factory-local codes are declared, so the disjointness above is vacuous")
	}
	for _, code := range apiErrorCodes() {
		if !slices.Contains(core, code) && !slices.Contains(factoryErrorCodes(), code) {
			t.Errorf("%q is answered by this package but belongs to neither list", code)
		}
	}
}

// TestRetryabilityIsAFunctionOfTheStatus pins the rule rather than the rows. A
// per-call-site boolean is a place a caller can be wrong; a function of the
// status is not, and it is what makes "retryable" mean one thing across every
// route.
func TestRetryabilityIsAFunctionOfTheStatus(t *testing.T) {
	t.Parallel()

	for status, want := range map[int]bool{
		http.StatusBadRequest:            false,
		http.StatusUnauthorized:          false,
		http.StatusForbidden:             false,
		http.StatusNotFound:              false,
		http.StatusMethodNotAllowed:      false,
		http.StatusRequestEntityTooLarge: false,
		http.StatusUnsupportedMediaType:  false,
		http.StatusInternalServerError:   false,
		http.StatusNotImplemented:        false,
		http.StatusBadGateway:            true,
		http.StatusServiceUnavailable:    true,
		http.StatusGatewayTimeout:        true,
	} {
		if got := retryableStatus(status); got != want {
			t.Errorf("retryableStatus(%d) = %v, want %v", status, got, want)
		}
		recorder := httptest.NewRecorder()
		writeAPIError(recorder, apiError{status: status, code: ErrorCodeInternal, message: "m"})
		var envelope sessionwire.ErrorEnvelope
		if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
			t.Fatalf("status %d: %v", status, err)
		}
		if envelope.Error.Retryable != want {
			t.Errorf("status %d: envelope retryable = %v, want %v", status, envelope.Error.Retryable, want)
		}
	}
}

// TestAnUnmarshallableEnvelopeStillAnswersJSON is the guarantee the whole task
// rests on: an API failure never falls through to something a browser renders.
// Core refuses to marshal an ErrorDetail with an empty code, so a construction
// bug on this path would otherwise produce an empty body a browser is free to
// sniff.
func TestAnUnmarshallableEnvelopeStillAnswersJSON(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	writeAPIError(recorder, apiError{status: http.StatusTeapot, code: "", message: "m"})

	if recorder.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500: an envelope this package could not build is this package's fault", recorder.Code)
	}
	if got := recorder.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}
	var envelope sessionwire.ErrorEnvelope
	if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("the fallback body %q is not a Core ErrorEnvelope: %v", recorder.Body, err)
	}
	if envelope.Error.Code != ErrorCodeInternal {
		t.Errorf("fallback code = %q, want %q", envelope.Error.Code, ErrorCodeInternal)
	}
}

// TestAuthenticationFailuresSeparateTheCallerFromTheDeployment holds the
// distinction internal/identity created ErrVerifierUnavailable for: a rejected
// credential is the caller's problem and a verifier that cannot answer is the
// deployment's. Collapsing them signs every user out while a credential service
// is down, and collapsing them the other way tells a caller to retry a
// credential that will never be accepted.
func TestAuthenticationFailuresSeparateTheCallerFromTheDeployment(t *testing.T) {
	t.Parallel()

	for _, row := range []struct {
		name   string
		err    error
		status int
		code   sessionwire.ErrorCode
	}{
		{"rejected credential", factoryidentity.ErrUnauthenticated, http.StatusUnauthorized, ErrorCodeUnauthenticated},
		{"expired credential", factoryidentity.ErrCredentialExpired, http.StatusUnauthorized, ErrorCodeUnauthenticated},
		{"wrapped rejection", fmt.Errorf("verifying: %w", factoryidentity.ErrUnauthenticated), http.StatusUnauthorized, ErrorCodeUnauthenticated},
		{"verifier unavailable", internalidentity.ErrVerifierUnavailable, http.StatusServiceUnavailable, ErrorCodeUnavailable},
		{"wrapped unavailability", fmt.Errorf("dialling: %w", internalidentity.ErrVerifierUnavailable), http.StatusServiceUnavailable, ErrorCodeUnavailable},
		{"an error of neither class", errors.New("something else"), http.StatusInternalServerError, ErrorCodeInternal},
	} {
		got := authenticationFailure(row.err)
		if got.status != row.status || got.code != row.code {
			t.Errorf("%s: authenticationFailure = %d/%q, want %d/%q", row.name, got.status, got.code, row.status, row.code)
		}
	}
}

// TestAuthorizationFailuresNeverBecomeAnInternalError keeps a denial a denial.
func TestAuthorizationFailuresNeverBecomeAnInternalError(t *testing.T) {
	t.Parallel()

	if got := authorizationFailure(internalidentity.ErrUnauthorized); got.status != http.StatusForbidden || got.code != ErrorCodeNotAuthorized {
		t.Errorf("authorizationFailure(ErrUnauthorized) = %d/%q, want 403/%q", got.status, got.code, ErrorCodeNotAuthorized)
	}
	if got := authorizationFailure(errors.New("the authorizer itself failed")); got.status != http.StatusInternalServerError {
		t.Errorf("a non-denial from the authorizer answered %d, want 500", got.status)
	}
}

// TestACatalogFailureMapsToOneStablePublicCode drives every CatalogErrorCode
// the released module declared WHEN THIS LIST WAS WRITTEN, which is fourteen at
// sessionstore v0.8.0.
//
// The list names the constants rather than the branches under test, which is
// what keeps it from being a tautology over the mapping it checks. It is NOT
// self-maintaining, and an earlier wording claimed it was: "a code sessionstore
// adds and this mapping forgets lands on the DEFAULT" is true of the PRODUCTION
// mapping -- which enumerates absence and defaults everything else to 500 -- and
// false of this test, which is a hand-written slice that a new code does not
// join. This test would never drive it and would stay green having observed
// nothing.
//
// What fails on growth is internal/command's
// TestTheStoreErrorVocabularyHasNotGrownSinceTheAbsenceSetWasDerived, one
// tripwire for this list and the two others like it. A value added to the
// vocabulary joins THIS test on the day somebody lists it.
func TestACatalogFailureMapsToOneStablePublicCode(t *testing.T) {
	t.Parallel()

	notFound := map[sessionstore.CatalogErrorCode]bool{
		sessionstore.CatalogErrorNotFound: true,
		sessionstore.CatalogErrorDeleted:  true,
		sessionstore.CatalogErrorIdentity: true,
	}
	badRequest := map[sessionstore.CatalogErrorCode]bool{
		sessionstore.CatalogErrorCursor: true,
	}
	codes := []sessionstore.CatalogErrorCode{
		sessionstore.CatalogErrorInvalid, sessionstore.CatalogErrorCursor,
		sessionstore.CatalogErrorNotFound, sessionstore.CatalogErrorDeleted,
		sessionstore.CatalogErrorIdentity, sessionstore.CatalogErrorEpoch,
		sessionstore.CatalogErrorSequence, sessionstore.CatalogErrorTooSoon,
		sessionstore.CatalogErrorConflict, sessionstore.CatalogErrorUnknown,
		sessionstore.CatalogErrorBackend, sessionstore.CatalogErrorMalformed,
		sessionstore.CatalogErrorVersion, sessionstore.CatalogErrorTooLarge,
	}
	for _, code := range codes {
		got := catalogFailure(&sessionstore.CatalogError{Code: code, Field: "session_id"})
		switch {
		case notFound[code]:
			if got.status != http.StatusNotFound || got.code != sessionwire.ErrorCodeSessionNotFound {
				t.Errorf("%s: %d/%q, want 404/session_not_found", code, got.status, got.code)
			}
		case badRequest[code]:
			if got.status != http.StatusBadRequest || got.code != sessionwire.ErrorCodeInvalidRequest {
				t.Errorf("%s: %d/%q, want 400/invalid_request", code, got.status, got.code)
			}
		default:
			if got.status != http.StatusInternalServerError || got.code != ErrorCodeInternal {
				t.Errorf("%s: %d/%q, want 500/internal_error", code, got.status, got.code)
			}
		}
	}
}

// TestACancelledReadIsNotAnInternalError separates the two context endings a
// storage call can return, because they need opposite answers: a client that
// went away gets nothing worth computing, and a deadline this router imposed is
// the deployment telling the caller to try again.
func TestACancelledReadIsNotAnInternalError(t *testing.T) {
	t.Parallel()

	if got := catalogFailure(context.DeadlineExceeded); got.status != http.StatusGatewayTimeout || got.code != ErrorCodeTimeout {
		t.Errorf("catalogFailure(DeadlineExceeded) = %d/%q, want 504/%q", got.status, got.code, ErrorCodeTimeout)
	}
	if got := catalogFailure(fmt.Errorf("reading: %w", context.DeadlineExceeded)); got.status != http.StatusGatewayTimeout {
		t.Errorf("a wrapped deadline answered %d, want 504", got.status)
	}
	if got := catalogFailure(context.Canceled); got.status != statusClientClosedRequest {
		t.Errorf("catalogFailure(Canceled) = %d, want %d", got.status, statusClientClosedRequest)
	}
}

// TestNoDependencyErrorTextReachesTheBody is the redaction property, driven
// from both ends: every mapping function is fed an error carrying a marker no
// public message may contain, and the marker must not appear in the response.
//
// The marker is a fresh random-looking string rather than a word, for the
// reason internal/identity's nonce sweep uses one: a fixed token that happens
// to be ordinary English is indistinguishable from message text.
func TestNoDependencyErrorTextReachesTheBody(t *testing.T) {
	t.Parallel()

	const marker = "b41f7c02e8d94a15-secret-backend-dsn"
	carriers := []error{
		fmt.Errorf("verifying credential against %s", marker),
		fmt.Errorf("%w: %s", internalidentity.ErrVerifierUnavailable, marker),
		fmt.Errorf("%w: %s", internalidentity.ErrUnauthorized, marker),
		&sessionstore.CatalogError{Code: sessionstore.CatalogErrorBackend, Field: marker},
		fmt.Errorf("reading catalog: %s", marker),
	}
	mappers := map[string]func(error) apiError{
		"authenticationFailure": authenticationFailure,
		"authorizationFailure":  authorizationFailure,
		"catalogFailure":        catalogFailure,
	}
	checked := 0
	for name, mapper := range mappers {
		for _, carrier := range carriers {
			recorder := httptest.NewRecorder()
			writeAPIError(recorder, mapper(carrier))
			checked++
			if strings.Contains(recorder.Body.String(), marker) {
				t.Errorf("%s leaked the dependency's error text into %q", name, recorder.Body)
			}
		}
	}
	if checked != len(mappers)*len(carriers) {
		t.Fatalf("the sweep ran %d combinations, want %d", checked, len(mappers)*len(carriers))
	}
	// Anti-vacuity: the marker must be detectable by this test's own check when
	// it really is present, or the sweep above cannot fail.
	recorder := httptest.NewRecorder()
	writeAPIError(recorder, apiError{status: http.StatusInternalServerError, code: ErrorCodeInternal, message: marker})
	if !strings.Contains(recorder.Body.String(), marker) {
		t.Fatal("the leak detector cannot see a marker that is present, so the sweep above proves nothing")
	}
}
