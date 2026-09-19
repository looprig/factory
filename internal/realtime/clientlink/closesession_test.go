package clientlink_test

import (
	"testing"
	"time"
)

// TestCloseSessionSparesAViewerWhoseSubscribeIsInFlight is the v0.4.0 quality
// gate's TI4 probe, committed (F5): CloseSession unsubscribes only a viewer
// the node holds as SUBSCRIBED. A viewer whose subscribe is still in flight
// is spared, and correctly -- its join's durable read comes after its
// subscribe completes, so it misses nothing. An implementation that
// unsubscribed every client regardless would unsubscribe it with 2000 the
// moment its subscribe landed, a spurious repair.
func TestCloseSessionSparesAViewerWhoseSubscribeIsInFlight(t *testing.T) {
	t.Parallel()

	f := newFixture(t, testLimits())
	block := make(chan struct{})
	f.demand.mu.Lock()
	f.demand.blockAcquire = block
	f.demand.mu.Unlock()
	client, _ := dialSupported(t, f, "token-a")
	done := make(chan *publicationRecorder, 1)
	go func() { done <- watchNoFatal(client, sessionChannel(tenantA, "s-1")) }()
	eventually(t, "the subscribe to reach Acquire", func() bool {
		f.demand.mu.Lock()
		defer f.demand.mu.Unlock()
		return len(f.demand.acquires) == 1
	})
	f.handler.CloseSession(tenantA, "s-1")
	time.Sleep(100 * time.Millisecond)
	close(block)
	var recorder *publicationRecorder
	select {
	case recorder = <-done:
	case <-time.After(waitFor):
		t.Fatal("the in-flight subscribe never completed")
	}
	if recorder == nil {
		t.Fatal("the in-flight subscribe failed")
	}
	time.Sleep(300 * time.Millisecond)
	if got := recorder.unsubs(); len(got) != 0 {
		t.Fatalf("a viewer whose subscribe was in flight at CloseSession was unsubscribed %v", got)
	}
}

// TestCloseSessionDoesNotWaitForTheEnginesLock (quality gate W26):
// CloseSession's server-side unsubscribe runs this handler's OnUnsubscribe,
// which releases a DeliveryBinding under the Engine's lock, and the relay
// calls CloseSession while a subscriber elsewhere may hold that lock inside a
// slow Demand.Acquire. So CloseSession must hand the unsubscribes to its own
// goroutine and return; done inline it would wait on that Acquire.
func TestCloseSessionDoesNotWaitForTheEnginesLock(t *testing.T) {
	t.Parallel()

	f := newFixture(t, testLimits())
	client, _ := dialSupported(t, f, "token-a")
	watched := watch(t, client, sessionChannel(tenantA, "s-1"))

	block := make(chan struct{})
	f.demand.mu.Lock()
	f.demand.blockAcquire = block
	f.demand.mu.Unlock()
	other, _ := dialSupported(t, f, "token-a")
	go func() { _ = watchNoFatal(other, sessionChannel(tenantA, "s-2")) }()
	eventually(t, "the second session's Acquire to hold the Engine's lock", func() bool {
		f.demand.mu.Lock()
		defer f.demand.mu.Unlock()
		return len(f.demand.acquires) == 2
	})
	returned := make(chan struct{})
	go func() {
		f.handler.CloseSession(tenantA, "s-1")
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		close(block)
		t.Fatal("CloseSession waited on the Engine's lock, which an in-flight Acquire held")
	}
	close(block)
	eventually(t, "the viewer to be unsubscribed", func() bool { return len(watched.unsubs()) == 1 })
}
