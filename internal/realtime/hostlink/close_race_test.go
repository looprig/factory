package hostlink_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/hostlink"
)

// TestCloseDoesNotRaceAnRPCInFlight is the tests-lane I1.3 regate's O-1, made
// deterministic enough to fail under -race.
//
// centrifuge-go v0.12.0's Client.send reads c.transport WITHOUT c.mu
// (client.go:2187) on the goroutine that called RPC, while Client.Close's
// moveToDisconnected writes it UNDER c.mu (client.go:457). The transport gives
// the two no order, so a Close that runs while any RPC is still inside send is
// a data race. The link runs every client.RPC on its own goroutine, which
// outlives a caller whose context ended (see rpc), so a caller returning is not
// evidence its RPC is done -- that is how Pool.Close, reached through
// Server.Stop, met a placement Bind the Stop had just abandoned.
func TestCloseDoesNotRaceAnRPCInFlight(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{})
	// Several rounds, each a fresh link closed under load: one round meets the
	// race about one run in ten, so a single round would mostly pass without
	// the fix.
	for round := 0; round < 10; round++ {
		closeUnderLoad(t, mustDial(t, host))
	}
}

// closeUnderLoad closes link while callers keep starting RPCs on it and
// abandoning them.
func closeUnderLoad(t *testing.T, link hostlink.Link) {
	t.Helper()
	var (
		wg      sync.WaitGroup
		stop    atomic.Bool
		started sync.WaitGroup
	)
	const callers = 8
	started.Add(callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			first := true
			for !stop.Load() {
				// A short deadline, so callers abandon RPCs whose goroutines
				// are still running -- the shape Server.Stop produces.
				ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
				_ = link.DeliverCommand(ctx, tenant, "s-1", sessionwire.HostLinkCommandDelivery{CommandID: "cmd-race"})
				cancel()
				if first {
					started.Done()
					first = false
				}
			}
		}()
	}
	started.Wait()
	time.Sleep(5 * time.Millisecond)

	closed := time.Now()
	if err := link.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Bounded: centrifuge-go's own close can hang forever when a reply races
	// it (see closeBound), and this round reproduces that about once in 350,
	// so the bound -- not a wait for the transport -- is what is asserted.
	if elapsed := time.Since(closed); elapsed > 3*time.Second {
		t.Errorf("Close took %v; it is bounded at about 2s", elapsed)
	}
	stop.Store(true)
	wg.Wait()

	// A closed link refuses before anything reaches the transport, and says
	// so terminally.
	err := link.DeliverCommand(context.Background(), tenant, "s-1", sessionwire.HostLinkCommandDelivery{CommandID: "cmd-after"})
	if !errors.Is(err, hostlink.ErrLinkClosed) {
		t.Errorf("a call after Close returned %v, want ErrLinkClosed", err)
	}
}

// TestCloseDoesNotRaceASubscribeInFlight is the subscription half of the same
// race: Subscription.Subscribe and Unsubscribe reach the same unlocked
// Client.send on the caller's goroutine. They are sent under regMu, so Close
// closes the client under regMu too.
func TestCloseDoesNotRaceASubscribeInFlight(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{})
	for round := 0; round < 10; round++ {
		link := mustDial(t, host)
		subscriber, ok := link.(hostlink.Subscriber)
		if !ok {
			t.Fatal("the real link is not a Subscriber")
		}
		var (
			wg      sync.WaitGroup
			stop    atomic.Bool
			started sync.WaitGroup
		)
		const callers = 8
		started.Add(callers)
		for i := 0; i < callers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				first := true
				for n := 0; !stop.Load(); n++ {
					session := sessionwire.SessionID(fmt.Sprintf("s-%d-%d", i, n))
					ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
					_ = subscriber.Subscribe(ctx, tenant, session, nopSink{})
					cancel()
					subscriber.Unsubscribe(tenant, session)
					if first {
						started.Done()
						first = false
					}
				}
			}()
		}
		started.Wait()
		time.Sleep(5 * time.Millisecond)
		if err := link.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
		stop.Store(true)
		wg.Wait()
	}
}
