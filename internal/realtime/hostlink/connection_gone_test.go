package hostlink

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	centrifuge "github.com/centrifugal/centrifuge"
	centrifugego "github.com/centrifugal/centrifuge-go"
	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// TestAnUnbindOnAGoneConnectionIsConnectionGoneAndSendsNothing measures the
// two facts ConnectionGone's doc rests on, over the real transport: an unbind
// on a link that is reconnecting, and on one this replica closed, is refused
// BEFORE anything reaches the Host, and both refusals are ConnectionGone. The
// route it named lived on the connection that is gone, which the Host drops
// with every route it held.
func TestAnUnbindOnAGoneConnectionIsConnectionGoneAndSendsNothing(t *testing.T) {
	t.Parallel()

	host := newLivenessHost(t)
	link := dialLiveness(t, host, 300*time.Millisecond)
	serverClient := <-host.connected

	serverClient.Disconnect(centrifuge.Disconnect{Code: 4000, Reason: "test drop"})
	waitForState(t, link.client, centrifugego.StateConnecting)
	time.Sleep(20 * time.Millisecond)
	if got := link.client.State(); got != centrifugego.StateConnecting {
		t.Fatalf("left the Connecting window early: %s", got)
	}

	err := link.Unbind(context.Background(), livenessUnbindRequest(host.target.Host))
	if !errors.Is(err, ErrLinkReconnecting) || !ConnectionGone(err) {
		t.Fatalf("Unbind while reconnecting = %v, want ErrLinkReconnecting classified ConnectionGone", err)
	}

	if err := link.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	err = link.Unbind(context.Background(), livenessUnbindRequest(host.target.Host))
	if !errors.Is(err, ErrLinkClosed) || !ConnectionGone(err) {
		t.Fatalf("Unbind after Close = %v, want ErrLinkClosed classified ConnectionGone", err)
	}
	if got := host.calls(); got != 0 {
		t.Fatalf("Host received %d RPCs, want 0: neither unbind may be sent", got)
	}
}

// TestConnectionGoneIsNarrow keeps the classification to failures that prove
// nothing was sent on a live connection. A cancelled or timed-out call may
// have left on a connection the Host still holds, and a Host's own answer
// says the connection is up.
func TestConnectionGoneIsNarrow(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"reconnecting", fmt.Errorf("hostlink: hostlink.unbind: %w", ErrLinkReconnecting), true},
		{"closed", fmt.Errorf("%w: host-1", ErrLinkClosed), true},
		{"terminal Host close", &HostDisconnect{Host: "host-1", Code: 3500}, true},
		{"wire version", fmt.Errorf("%w: host selected 2", ErrUnsupportedProtocol), true},
		{"nil", nil, false},
		{"cancelled", fmt.Errorf("hostlink: hostlink.unbind: %w", context.Canceled), false},
		{"deadline", fmt.Errorf("hostlink: hostlink.unbind: %w", context.DeadlineExceeded), false},
		{"Host refusal", &HostRefusal{HostLinkError: sessionwire.HostLinkError{Code: sessionwire.HostLinkErrorRuntimeUnavailable}}, false},
		{"Host failure", &HostFailure{Method: sessionwire.HostLinkMethodUnbind, Code: 100}, false},
		{"pool closed", ErrPoolClosed, false},
		{"unknown binding", ErrUnknownBinding, false},
	} {
		if got := ConnectionGone(tc.err); got != tc.want {
			t.Errorf("%s: ConnectionGone(%v) = %v, want %v", tc.name, tc.err, got, tc.want)
		}
	}
}

func livenessUnbindRequest(host sessionwire.HostID) sessionwire.HostLinkUnbindRequest {
	bind := livenessBindRequest(host)
	return sessionwire.HostLinkUnbindRequest{
		Version: bind.Version, TenantID: bind.TenantID, SessionID: bind.SessionID, HostID: bind.HostID,
		HostGeneration: bind.HostGeneration, LeaseEpoch: bind.LeaseEpoch, IdempotencyKey: bind.IdempotencyKey,
	}
}
