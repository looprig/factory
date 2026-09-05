package httpapi

import (
	"net/http"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	internalidentity "github.com/looprig/factory/internal/identity"
)

// bootstrapResponse is Factory's browser bootstrap document. It is deliberately
// not a Core session-wire contract: it describes the authenticated HTTP caller,
// not a session or a runtime exchange.
type bootstrapResponse struct {
	Tenant sessionwire.TenantID `json:"tenant_id"`
}

// serveBootstrap returns the tenant established by authentication and no other
// principal or credential material. The request has no authority-bearing
// input: no path parameter, no body member and no accepted query parameter can
// replace the tenant carried in the operation context.
func (rt *Router) serveBootstrap() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		operation, authenticated := internalidentity.OperationContextFrom(r.Context())
		if !authenticated {
			// Unreachable through the composed chain. Keep the handler fail-closed
			// if it is ever mounted without authentication during a refactor.
			writeAPIError(w, authenticationFailure(identity.ErrUnauthenticated))
			return
		}
		if r.URL.RawQuery != "" {
			writeAPIError(w, apiError{
				status:  http.StatusBadRequest,
				code:    sessionwire.ErrorCodeInvalidRequest,
				message: "bootstrap accepts no query parameters",
			})
			return
		}
		tenant := operation.Principal.Tenant()
		if err := tenant.Validate(); err != nil {
			// Authentication constructs Principal through its validating
			// constructor, so an invalid tenant here is an internal invariant
			// failure rather than caller input.
			writeAPIError(w, internalFailure())
			return
		}
		writeJSON(w, http.StatusOK, bootstrapResponse{Tenant: tenant})
	})
}
