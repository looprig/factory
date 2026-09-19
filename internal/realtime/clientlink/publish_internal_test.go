package clientlink

import (
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
)

// TestSessionChannelIsTheInverseOfTheDemandGrammar: a record published on
// SessionChannel(t, s) must land on exactly the channel whose subscription was
// routed to (t, s), for every identity Core carries and the grammar admits.
func TestSessionChannelIsTheInverseOfTheDemandGrammar(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		tenant  sessionwire.TenantID
		session sessionwire.SessionID
	}{
		{"tenant-a", "s-1"},
		{"tenant-b", "s-1"},
		{"t", "01J0000000000000000000000"},
		{"tenant.with.dots", "session_with-marks.9"},
	} {
		principal, err := identity.NewPrincipal(tc.tenant, "user", identity.KindActor)
		if err != nil {
			t.Fatalf("NewPrincipal: %v", err)
		}
		key, err := demandKeyOf(principal, SessionChannel(tc.tenant, tc.session))
		if err != nil {
			t.Fatalf("demandKeyOf(SessionChannel(%s, %s)): %v", tc.tenant, tc.session, err)
		}
		if key.tenant != tc.tenant || key.session != tc.session {
			t.Fatalf("round trip of (%s, %s) = (%s, %s)", tc.tenant, tc.session, key.tenant, key.session)
		}
	}
}
