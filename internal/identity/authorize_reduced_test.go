package identity

import (
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	factoryidentity "github.com/looprig/factory/identity"
)

func TestAuthorizationResultExhaustsItsDomain(t *testing.T) {
	t.Parallel()

	if err := authorizationResult(true); err != nil {
		t.Fatalf("authorizationResult(true) = %v, want nil", err)
	}
	if err := authorizationResult(false); err != ErrUnauthorized {
		t.Fatalf("authorizationResult(false) = %v, want exact ErrUnauthorized", err)
	}
}

// TestSessionChannelAllowedExhaustsItsReducedDomain walks the whole 3x3x2
// reduced domain, but its oracle restates sessionChannelAllowed's body in the
// same terms, so it is a domain-exhaustion smoke test rather than an independent
// check. Do not cite it as coverage of the tenant comparison. The independent
// oracles are the table in
// TestSubscribeParsesBeforeComparingAndParsingDoesNotGrantAccess
// (authorize_test.go) and the Cut/Contains oracle in
// FuzzAuthorizeSubscribeMatchesIndependentGrammar (root package).
func TestSessionChannelAllowedExhaustsItsReducedDomain(t *testing.T) {
	t.Parallel()

	tenants := []sessionwire.TenantID{"", "tenant-a", "tenant-b"}
	for _, principalTenant := range tenants {
		for _, parsedTenant := range tenants {
			for _, valid := range []bool{false, true} {
				parsed := parsedSessionChannel{tenant: parsedTenant, session: "session-a", valid: valid}
				want := valid && parsedTenant == principalTenant
				if got := sessionChannelAllowed(principalTenant, parsed); got != want {
					t.Errorf("sessionChannelAllowed(%q, {tenant:%q, valid:%t}) = %t, want %t",
						principalTenant, parsedTenant, valid, got, want)
				}
			}
		}
	}
}

func TestActorAndServiceWithSameTenantAndSubjectHaveOnlyTheServicePrivilegeDifference(t *testing.T) {
	t.Parallel()

	actor, err := factoryidentity.NewPrincipal("tenant-a", "same-subject", factoryidentity.KindActor)
	if err != nil {
		t.Fatalf("NewPrincipal(actor) = %v", err)
	}
	service, err := factoryidentity.NewPrincipal("tenant-a", "same-subject", factoryidentity.KindService)
	if err != nil {
		t.Fatalf("NewPrincipal(service) = %v", err)
	}
	authorizer := Authorizer{}
	for name, principal := range map[string]factoryidentity.Principal{"actor": actor, "service": service} {
		t.Run(name+" subscribe", func(t *testing.T) {
			if err := authorizer.AuthorizeSubscribe(t.Context(), principal, "session:tenant-a:session-a"); err != nil {
				t.Fatalf("AuthorizeSubscribe() = %v, want nil", err)
			}
		})
	}
	if err := authorizer.AuthorizeServiceSweep(t.Context(), actor); err != ErrUnauthorized {
		t.Fatalf("actor AuthorizeServiceSweep() = %v, want exact ErrUnauthorized", err)
	}
	if err := authorizer.AuthorizeServiceSweep(t.Context(), service); err != nil {
		t.Fatalf("service AuthorizeServiceSweep() = %v, want nil", err)
	}
}
