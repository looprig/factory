package httpapi

import (
	"context"
	"sync"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// maxConcurrentWakes is how many post-acknowledgement Host wakes one router
// runs at once. A wake that finds every slot taken is DROPPED, not queued.
//
// Dropping is safe because a wake is a latency optimisation over a path that
// is already correct without it: the durable inbox record is the
// acknowledgement, the owning Host reads its command stream when it next
// consumes, and the placement and redelivery sweeps are what guarantee a
// command nobody woke for is applied. What a full set of slots means is that
// this many wakes are already waiting on Hosts that have not answered, and
// queueing a further one would buy nothing but memory: the sweeps deliver it
// anyway. (routing.Bindings.Deliver no longer holds the routing table's lock
// across the Host RPC, so a wake to a silent Host costs only its own slot, not
// every other session's wake or bind.)
//
// Sixty-four is a ceiling on goroutines, not a throughput target: each slot is
// one goroutine bounded by the route's RequestTimeout, so the worst case this
// admits is 64 goroutines for 30 seconds.
const maxConcurrentWakes = 64

// wakes owns every post-acknowledgement Host wake a Router starts.
//
// # Why a wake is detached from the request (I1.2)
//
// A wake used to run on the request's own context BEFORE the answer was
// written. The answer never depended on it -- it is computed from the durable
// record before the wake starts, and a wake's error is discarded -- so the
// only thing that ordering bought was latency: I1.2 measured every admitted
// POST waiting out the HostLink RPC bound (5.004-5.006 s) against a Host that
// was alive but dropped the delivery reply, for a command that was already
// committed. Commit before acknowledge (runbook A3.2) is about the STORE; no
// Host round trip is owed before the answer.
//
// # What replaced the request's bound
//
// The earlier doc refused to detach because "a detached attempt would outlive
// the request's context". That objection is to an UNBOUNDED, UNOWNED attempt,
// and each of its halves has its own answer here:
//
//   - Bounded in time: each wake runs under the route's own RequestTimeout,
//     the same bound it had on the request's context, derived from this
//     owner's context rather than from Background.
//   - Bounded in number: at most maxConcurrentWakes run at once; see there.
//   - Owned: the wakes' context is cancelled by Stop, which then WAITS for
//     every running wake to return, so "Stop returned" means no wake is still
//     calling into the routing table the Server is about to close.
//
// A wake is deliberately NOT tied to the caller: a client that disconnects
// after its command was acknowledged -- or before, once the commit landed --
// has a committed command, and waking the Host for it is still correct.
//
// # Quiesce does not stop wakes
//
// Server.Quiesce closes public admission and leaves "sweeps and HostLinks
// running until Stop". A wake is HostLink traffic for a command that was
// committed, including one admitted by a request Quiesce is draining, so it is
// stopped where the HostLinks are: Stop.
type wakes struct {
	delivery CommandDelivery
	timeout  time.Duration

	// slots is the concurrency bound: a send takes one, the wake's return
	// gives it back.
	slots chan struct{}

	// ctx is every wake's parent; cancel is Stop's.
	ctx    context.Context
	cancel context.CancelFunc

	// mu orders schedule's stopped check and wg.Add against stop, so no wake
	// is added after stop has begun waiting.
	mu      sync.Mutex
	stopped bool
	wg      sync.WaitGroup
}

func newWakes(delivery CommandDelivery, timeout time.Duration, limit int) *wakes {
	ctx, cancel := context.WithCancel(context.Background())
	return &wakes{
		delivery: delivery,
		timeout:  timeout,
		slots:    make(chan struct{}, limit),
		ctx:      ctx,
		cancel:   cancel,
	}
}

// schedule starts one wake and returns at once. It reports whether a wake was
// started: false for no delivery seam, a stopped owner, or every slot taken.
// No caller changes its answer on the result; it exists for the tests.
func (w *wakes) schedule(tenant sessionwire.TenantID, session sessionwire.SessionID, command sessionwire.CommandID) bool {
	if w.delivery == nil {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopped {
		return false
	}
	select {
	case w.slots <- struct{}{}:
	default:
		return false
	}
	w.wg.Add(1)
	go w.run(tenant, session, command)
	return true
}

func (w *wakes) run(tenant sessionwire.TenantID, session sessionwire.SessionID, command sessionwire.CommandID) {
	defer w.wg.Done()
	defer func() { <-w.slots }()
	// A wake is off the request's goroutine, so the handler's recoverPanic no
	// longer covers it; a panicking delivery seam must not take the process
	// down over a best-effort wake.
	defer func() { _ = recover() }()
	ctx, cancel := context.WithTimeout(w.ctx, w.timeout)
	defer cancel()
	// The PUBLIC CommandID, which is the retry-stable identity both sides of
	// the HostLink agree on. The proposed runtime identity is the store's own
	// and means nothing to a Host that did not win the admission.
	_ = w.delivery.Deliver(ctx, tenant, session, sessionwire.HostLinkCommandDelivery{
		CommandID: command,
	})
}

// stop refuses every later wake, cancels every running one, and waits for them
// to return or for ctx to end. It is idempotent.
func (w *wakes) stop(ctx context.Context) error {
	w.mu.Lock()
	w.stopped = true
	w.mu.Unlock()
	w.cancel()

	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// StopWakes cancels every Host wake this router started after an
// acknowledgement, refuses any later one, and waits for the running ones to
// return or for ctx to end. factory.Server.Stop calls it, including on a
// Server that never served, so an embedder mounting Handler on its own
// http.Server is covered by the Stop it already owes.
//
// A wake that ignores its context is waited for until ctx ends, and then
// abandoned: the one goroutine left behind is the delivery seam's, bounded by
// whatever that seam honours.
func (rt *Router) StopWakes(ctx context.Context) error {
	return rt.wakes.stop(ctx)
}

// WakesStopped reports whether StopWakes has run. It exists so the composition
// root's own tests can hold that Server.Stop -- and not Quiesce -- is what
// stops the wakes, which no response can show: a stopped router is also a
// quiesced one and admits nothing to wake for.
func (rt *Router) WakesStopped() bool {
	rt.wakes.mu.Lock()
	defer rt.wakes.mu.Unlock()
	return rt.wakes.stopped
}
