package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	internalcommand "github.com/looprig/factory/internal/command"
	internalidentity "github.com/looprig/factory/internal/identity"
	"github.com/looprig/sessionstore"
)

// serveCommandStatus returns public status for one durable disposition command.
// Principal and metadata are appended only after an optional audit grant.
func (rt *Router) serveCommandStatus() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		operation, ok := internalidentity.OperationContextFrom(r.Context())
		if !ok {
			writeAPIError(w, internalFailure())
			return
		}
		if rt.commandReads == nil {
			writeAPIError(w, controlUnavailable())
			return
		}
		id := sessionwire.CommandID(r.PathValue("cid"))
		if err := id.Validate(); err != nil {
			writeAPIError(w, apiError{status: http.StatusBadRequest, code: sessionwire.ErrorCodeInvalidRequest, message: "the command identifier is not a valid identity"})
			return
		}
		session := sessionwire.SessionID(r.PathValue("sid"))
		entry, err := rt.commandReads.GetDispositionCommand(r.Context(), newScope(operation.Principal).commandEntry(session, id))
		if err != nil {
			writeAPIError(w, commandReadFailure(err))
			return
		}
		status, readable := internalcommand.StatusForDisposition(entry)
		if !readable {
			writeAPIError(w, internalFailure())
			return
		}
		body, err := status.MarshalJSON()
		if err != nil {
			writeAPIError(w, internalFailure())
			return
		}
		if rt.auditGranted(r.Context(), operation.Principal, session) {
			body, err = withAuditMembers(body, entry.Record.Descriptor.Principal, entry.Record.Descriptor.Metadata)
			if err != nil {
				writeAPIError(w, internalFailure())
				return
			}
		}
		writeJSONBytes(w, http.StatusOK, body)
	})
}

func (rt *Router) auditGranted(ctx context.Context, principal identity.Principal, session sessionwire.SessionID) bool {
	authorizer, ok := rt.authorizer.(AuditAuthorizer)
	return ok && authorizer.AuthorizeAuditRead(ctx, principal, session) == nil
}

func withAuditMembers(body []byte, principal *sessionwire.Principal, metadata sessionwire.MessageMetadata) ([]byte, error) {
	if principal == nil && len(metadata) == 0 {
		return body, nil
	}
	if len(body) < 2 || body[len(body)-1] != '}' {
		return nil, errors.New("httpapi: command status is not a JSON object")
	}
	out := append([]byte(nil), body[:len(body)-1]...)
	if principal != nil {
		encoded, err := json.Marshal(principal)
		if err != nil {
			return nil, err
		}
		out = append(append(out, `,"principal":`...), encoded...)
	}
	if len(metadata) > 0 {
		encoded, err := json.Marshal(metadata)
		if err != nil {
			return nil, err
		}
		out = append(append(out, `,"metadata":`...), encoded...)
	}
	return append(out, '}'), nil
}

func commandReadFailure(err error) apiError {
	var inbox *sessionstore.InboxError
	if (errors.As(err, &inbox) && inbox.Code == sessionstore.InboxErrorNotFound) || internalcommand.SessionAbsent(err) || commandLegacyBound(err) {
		return commandNotFound()
	}
	if failure, ok := contextFailure(err); ok {
		return failure
	}
	if failure, ok := storeUnavailable(err); ok {
		return failure
	}
	return internalFailure()
}

func commandLegacyBound(err error) bool {
	var catalog *sessionstore.CatalogError
	return errors.As(err, &catalog) && catalog.Field == "binding.protocol_mode" && (catalog.Code == sessionstore.CatalogErrorInvalid || catalog.Code == sessionstore.CatalogErrorConflict)
}

func commandNotFound() apiError {
	return apiError{status: http.StatusNotFound, code: ErrorCodeCommandNotFound, message: "there is no such command"}
}
