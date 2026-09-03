package factory_test

import (
	"strings"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	factoryidentity "github.com/looprig/factory/identity"
	internalidentity "github.com/looprig/factory/internal/identity"
)

func FuzzAuthorizeSubscribeMatchesIndependentGrammar(f *testing.F) {
	for _, seed := range [][2]string{
		{"", "tenant-a"},
		{"session", "tenant-a"},
		{"session:", "tenant-a"},
		{"session::", "tenant-a"},
		{"session:tenant-a", "tenant-a"},
		{"session:tenant-a:", "tenant-a"},
		{"session::session-a", "tenant-a"},
		{"session:tenant-a:session-a", "tenant-a"},
		{"session:tenant-a:session-a", "tenant-b"},
		{"session:tenant-a:session-a:extra", "tenant-a"},
		{"Session:tenant-a:session-a", "tenant-a"},
		{"session:tenant:a:session-a", "tenant"},
		{"session:tenant-a:session:a", "tenant-a"},
		{"session:\xff:session-a", "tenant-a"},
		{"session:tenant-a:\xff", "tenant-a"},
		{"session:tenant-a:session-a", ""},
	} {
		f.Add(seed[0], seed[1])
	}

	f.Fuzz(func(t *testing.T, channel, principalTenantText string) {
		principalTenant := sessionwire.TenantID(principalTenantText)
		if principalTenant.Validate() != nil {
			t.Skip()
		}
		principal := fuzzPrincipal(t, principalTenant)
		oracleTenant, _, oracleValid := oracleSessionChannel(channel)
		wantAllowed := oracleValid && oracleTenant == principalTenant
		err := (internalidentity.Authorizer{}).AuthorizeSubscribe(t.Context(), principal, channel)
		if wantAllowed {
			if err != nil {
				t.Fatalf("AuthorizeSubscribe(%q, tenant %q) = %v, want nil", channel, principalTenant, err)
			}
		} else if err != internalidentity.ErrUnauthorized {
			t.Fatalf("AuthorizeSubscribe(%q, tenant %q) = %v, want exact ErrUnauthorized", channel, principalTenant, err)
		}

		if !oracleValid {
			return
		}
		matching := fuzzPrincipal(t, oracleTenant)
		if err := (internalidentity.Authorizer{}).AuthorizeSubscribe(t.Context(), matching, channel); err != nil {
			t.Fatalf("AuthorizeSubscribe(%q) = %v for matching tenant, want nil", channel, err)
		}
		distinct := sessionwire.TenantID("tenant-oracle-other")
		if distinct == oracleTenant {
			distinct = "tenant-oracle-second"
		}
		if err := (internalidentity.Authorizer{}).AuthorizeSubscribe(t.Context(), fuzzPrincipal(t, distinct), channel); err != internalidentity.ErrUnauthorized {
			t.Fatalf("AuthorizeSubscribe(%q) = %v for distinct tenant, want exact ErrUnauthorized", channel, err)
		}
	})
}

// oracleSessionChannel deliberately uses Cut and Contains, independently of
// production's declarative regexp.
func oracleSessionChannel(channel string) (sessionwire.TenantID, sessionwire.SessionID, bool) {
	prefix, remainder, found := strings.Cut(channel, ":")
	if !found || prefix != "session" {
		return "", "", false
	}
	tenantPart, sessionPart, found := strings.Cut(remainder, ":")
	if !found || strings.Contains(sessionPart, ":") {
		return "", "", false
	}
	tenant := sessionwire.TenantID(tenantPart)
	session := sessionwire.SessionID(sessionPart)
	if tenant.Validate() != nil || session.Validate() != nil {
		return "", "", false
	}
	return tenant, session, true
}

func fuzzPrincipal(t *testing.T, tenant sessionwire.TenantID) factoryidentity.Principal {
	t.Helper()
	principal, err := factoryidentity.NewPrincipal(tenant, "fuzz-subject", factoryidentity.KindActor)
	if err != nil {
		t.Fatalf("NewPrincipal(%q) = %v", tenant, err)
	}
	return principal
}
