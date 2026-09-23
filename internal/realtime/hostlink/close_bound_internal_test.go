package hostlink

import (
	"context"
	"errors"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// TestASubscribeRacingAHungCloseReturnsPromptly is the v0.7.2 gate's S1. A
// Subscribe that passed the terminal check before Close marked the link then
// needs regMu for its send. centrifuge-go's Client.Close can hang forever
// (see closeBound), and a Close that held regMu across it parked that
// Subscribe -- and every later withdrawal on the link -- for good.
func TestASubscribeRacingAHungCloseReturnsPromptly(t *testing.T) {
	t.Parallel()

	host := newLivenessHost(t)
	link := dialLiveness(t, host, time.Second)
	<-host.connected

	release := make(chan struct{})
	defer close(release)
	link.closeTransport = func() { <-release }
	// The hook runs inside Subscribe after its reservation and before it takes
	// regMu for the send: exactly where a Close landing concurrently leaves it.
	link.beforeSubscribeSend = func() {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_ = link.Close(ctx)
	}

	done := make(chan error, 1)
	go func() {
		done <- link.Subscribe(context.Background(), "tenant-a", sessionwire.SessionID("s-1"), nopSink{})
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrLinkClosed) {
			t.Fatalf("Subscribe = %v, want ErrLinkClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a Subscribe racing a hung transport close did not return: regMu is held across the close")
	}

	// A late withdrawal on the same link must not park either.
	withdrawn := make(chan struct{})
	go func() {
		link.Unsubscribe("tenant-a", "s-1")
		close(withdrawn)
	}()
	select {
	case <-withdrawn:
	case <-time.After(2 * time.Second):
		t.Fatal("an Unsubscribe after a hung transport close did not return")
	}
}
