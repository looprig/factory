package hostlink

import (
	"context"
	"errors"
	"testing"
	"time"

	centrifugego "github.com/centrifugal/centrifuge-go"
)

func TestInflightRefusesAfterCloseAndDrainsOnTheLastLeave(t *testing.T) {
	var f inflight
	for range 2 {
		if !f.enter() {
			t.Fatal("an open count refused an operation")
		}
	}
	drained := f.close()
	if f.enter() {
		t.Fatal("a closed count admitted an operation")
	}
	if again := f.close(); again != drained {
		t.Fatal("a second close returned another channel")
	}
	f.leave()
	select {
	case <-drained:
		t.Fatal("drained with one operation still inside")
	default:
	}
	f.leave()
	select {
	case <-drained:
	default:
		t.Fatal("not drained after the last operation left")
	}

	var empty inflight
	select {
	case <-empty.close():
	default:
		t.Fatal("an empty count did not report drained at close")
	}
}

// TestCloseIsBoundedByItsContextWhenAnRPCNeverLeaves pins that the drain wait
// cannot make Close -- and through it Pool.Close and Server.Stop -- unkillable:
// an RPC wedged in centrifuge-go's double callback never leaves the count.
func TestCloseIsBoundedByItsContextWhenAnRPCNeverLeaves(t *testing.T) {
	link := &centrifugeLink{host: "host-1", client: centrifugego.NewJsonClient("ws://127.0.0.1:1/never", centrifugego.Config{})}
	if !link.rpcs.enter() {
		t.Fatal("enter refused on a fresh link")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := link.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if elapsed := time.Since(start); elapsed > closeBound/2 {
		t.Fatalf("Close took %v; its context allowed 50ms", elapsed)
	}
	if _, err := link.rpc(context.Background(), "hostlink.bind", nil); !errors.Is(err, ErrLinkClosed) {
		t.Fatalf("an RPC after Close returned %v, want ErrLinkClosed", err)
	}
	start = time.Now()
	if err := link.Close(context.Background()); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if elapsed := time.Since(start); elapsed > closeBound/2 {
		t.Fatalf("a second Close waited %v for an RPC the first had already given up on", elapsed)
	}
}

// TestCloseIsBoundedWhenTheTransportCloseNeverReturns pins the second half of
// the bound: centrifuge-go's own Close can wait forever for a reader wedged in
// its double completion callback (see closeBound), and that must not hold
// Close, Pool.Close or Server.Stop.
func TestCloseIsBoundedWhenTheTransportCloseNeverReturns(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	link := &centrifugeLink{host: "host-1", closeTransport: func() { <-release }}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := link.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if elapsed := time.Since(start); elapsed > closeBound/2 {
		t.Fatalf("Close took %v; its context allowed 50ms", elapsed)
	}

	// With no context bound at all, closeBound still ends the wait.
	other := &centrifugeLink{host: "host-2", closeTransport: func() { <-release }}
	start = time.Now()
	if err := other.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if elapsed := time.Since(start); elapsed < closeBound || elapsed > closeBound+time.Second {
		t.Fatalf("an unbounded-context Close took %v, want about closeBound (%v)", elapsed, closeBound)
	}
}
