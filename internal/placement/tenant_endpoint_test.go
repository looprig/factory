package placement

import (
	"fmt"
	"strings"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// TestACandidateThatCannotAddressTheTenantIsSkippedLoggedAndTheNextOneAsked is
// Gap 1's per-tenant refusal at placement: a Host whose advertised base cannot
// carry this session's tenant (Core's too_long here) is skipped FOR THIS TENANT
// -- recorded as unaddressable, never as refused, failed or excluded -- a WARN
// names the Host, the tenant and Core's code, and the next ranked candidate is
// attached.
func TestACandidateThatCannotAddressTheTenantIsSkippedLoggedAndTheNextOneAsked(t *testing.T) {
	t.Parallel()

	f := newAttachFixture(t, nil)
	f.publishTarget(t, "host-longname", 9, sessionwire.HostIsolationClassCrossTenantIsolated)
	f.publishTarget(t, "host-short", 2, sessionwire.HostIsolationClassCrossTenantIsolated)
	f.links.on("host-longname", fail(fmt.Errorf("%w: hostlink: host %q tenant %q: %w", ErrTenantUnaddressable, "host-longname", testTenant,
		&sessionwire.HostLinkEndpointError{Code: sessionwire.HostLinkEndpointCodeTooLong})))

	result := f.mustPlace(t, "cmd-1")
	if result.Decision.Outcome != OutcomeAttachPooled || result.Attached.HostID != "host-short" {
		t.Fatalf("Reconcile = %+v, want attached to host-short", result)
	}
	if len(result.Unaddressable) != 1 || result.Unaddressable[0] != "host-longname" {
		t.Fatalf("Unaddressable = %v, want [host-longname]", result.Unaddressable)
	}
	if len(result.Refused) != 0 || len(result.Failed) != 0 || len(result.Excluded) != 0 || len(result.Unreachable) != 0 {
		t.Fatalf("the skip was also recorded as Refused=%v Failed=%v Excluded=%v Unreachable=%v", result.Refused, result.Failed, result.Excluded, result.Unreachable)
	}
	logs := f.logs.String()
	for _, want := range []string{
		`"msg":"placement: skipped a pooled candidate whose advertised base cannot carry this tenant's HostLink address"`,
		`"host_id":"host-longname"`, `"tenant_id":"` + string(testTenant) + `"`, `"code":"too_long"`,
	} {
		if !strings.Contains(logs, want) {
			t.Fatalf("the skip's WARN lacks %s: %s", want, logs)
		}
	}
	if hosts := f.links.attachedHosts(); len(hosts) != 2 || hosts[0] != "host-longname" || hosts[1] != "host-short" {
		t.Fatalf("attaches = %v, want host-longname then host-short", hosts)
	}
}

// TestAnUnaddressableCandidateIsItsOwnClass pins attachAnswer's new row
// against the neighbour it must not collapse into: an unreachable Host is
// silent, an unaddressable one is logged with its code.
func TestAnUnaddressableCandidateIsItsOwnClass(t *testing.T) {
	t.Parallel()

	if got, _ := attachAnswer(fmt.Errorf("x: %w", ErrTenantUnaddressable)); got != answerUnaddressable {
		t.Fatalf("ErrTenantUnaddressable classified %d, want answerUnaddressable", got)
	}
	both := fmt.Errorf("%w: %w", ErrTenantUnaddressable, ErrHostUnreachable)
	if got, _ := attachAnswer(both); got != answerUnaddressable {
		t.Fatalf("an error carrying both classes classified %d, want answerUnaddressable first", got)
	}
}
