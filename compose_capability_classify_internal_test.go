package factory

import (
	"errors"
	"fmt"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/admission"
	"github.com/looprig/factory/internal/realtime/hostlink"
)

// TestOnlyATransientCapabilityReadIsClassifiedUnavailable holds
// classifyCapabilityRead arm by arm (quality gate F1): a path condition a
// retry can clear is admission.ErrGateResponderUnavailable (503 retryable at
// the edge); a deployment fact is not, and the cause is kept either way.
func TestOnlyATransientCapabilityReadIsClassifiedUnavailable(t *testing.T) {
	t.Parallel()
	for name, row := range map[string]struct {
		err       error
		transient bool
	}{
		"reconnecting":   {fmt.Errorf("hostlink: capability read: %w", hostlink.ErrLinkReconnecting), true},
		"link ceiling":   {fmt.Errorf("%w: 256 links", hostlink.ErrLinkLimit), true},
		"dial failed":    {fmt.Errorf("%w: refused", hostlink.ErrDialFailed), true},
		"terminal close": {&hostlink.HostDisconnect{Host: "host-a", Code: 3500}, true},
		"wire version":   {fmt.Errorf("%w: host selected 2", hostlink.ErrUnsupportedProtocol), true},
		"pool closed":    {hostlink.ErrPoolClosed, true},
		// v0.7.2 gate S2: a read racing a reap or eviction of the link. The
		// pool evicts it, so a retry redials.
		"link closed": {fmt.Errorf("%w: host-a", hostlink.ErrLinkClosed), true},
		"tenant unaddressable": {&hostlink.EndpointError{Host: "host-a", Tenant: "tenant-a",
			Cause: &sessionwire.HostLinkEndpointError{Code: sessionwire.HostLinkEndpointCodeTooLong}}, false},
		"anything else": {errors.New("boom"), false},
	} {
		got := classifyCapabilityRead(row.err, admission.ErrGateResponderUnavailable)
		if errors.Is(got, admission.ErrGateResponderUnavailable) != row.transient {
			t.Errorf("%s: classified %v, transient = %v", name, got, row.transient)
		}
		if !errors.Is(got, row.err) {
			t.Errorf("%s: the cause was dropped: %v", name, got)
		}
	}
	if classifyCapabilityRead(nil, admission.ErrGateResponderUnavailable) != nil {
		t.Error("classifyCapabilityRead(nil) is not nil")
	}
}
