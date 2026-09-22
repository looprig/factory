package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// silentHost is the HostLink stand-in for a Host that is alive but never
// answers a delivery: every Deliver blocks until its context ends, which is
// what the real link does against a Host that dropped the reply (I1.2
// measured the HostLink RPC bound, 5.004-5.006 s, on every admitted POST).
//
// It records how many attempts STARTED and how many are still RUNNING, so a
// test can prove both that the wake happened and that nothing outlived Stop.
type silentHost struct {
	mu      sync.Mutex
	started int
	running int
	// ctxErrs is each attempt's context error at the moment it ended.
	ctxErrs []error
	// entered receives once per attempt, after it is counted.
	entered chan struct{}
}

func newSilentHost() *silentHost {
	return &silentHost{entered: make(chan struct{}, 1024)}
}

func (h *silentHost) Deliver(ctx context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, _ sessionwire.HostLinkCommandDelivery) error {
	h.mu.Lock()
	h.started++
	h.running++
	h.mu.Unlock()
	h.entered <- struct{}{}
	<-ctx.Done()
	h.mu.Lock()
	h.running--
	h.ctxErrs = append(h.ctxErrs, ctx.Err())
	h.mu.Unlock()
	return ctx.Err()
}

func (h *silentHost) counts() (started, running int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.started, h.running
}

func (h *silentHost) awaitStarted(t *testing.T, n int) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for i := 0; i < n; i++ {
		select {
		case <-h.entered:
		case <-deadline:
			started, _ := h.counts()
			t.Fatalf("only %d wake attempts started, want %d", started, n)
		}
	}
}

// ackBound is how long a durable acknowledgement may take against the
// in-memory fakes. It is generous for a scheduler under -race and two orders
// of magnitude below the HostLink RPC bound the defect cost every POST.
const ackBound = 1 * time.Second

// serveWithin serves r and fails if the answer took longer than ackBound. The
// request runs on its own goroutine so a regression fails in ackBound rather
// than hanging for the route's own 30 s deadline.
func serveWithin(t *testing.T, f *fixture, r *http.Request) (int, time.Duration) {
	t.Helper()
	type answer struct {
		code    int
		elapsed time.Duration
	}
	done := make(chan answer, 1)
	start := time.Now()
	go func() {
		recorder := f.serve(r)
		done <- answer{code: recorder.Code, elapsed: time.Since(start)}
	}()
	select {
	case got := <-done:
		return got.code, got.elapsed
	case <-time.After(ackBound):
		// Release the handler in the background and fail NOW: a regression
		// that runs the wake under the scheduler's own lock would deadlock a
		// synchronous StopWakes here, turning a failure into a hang.
		go func() { _ = f.router.StopWakes(context.Background()) }()
		t.Fatalf("the durable acknowledgement took longer than %v: the answer is waiting on the Host's wake", ackBound)
		return 0, 0
	}
}

func stopWakes(t *testing.T, f *fixture) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := f.router.StopWakes(ctx); err != nil {
		t.Errorf("StopWakes: %v", err)
	}
}

// TestTheAcknowledgementDoesNotWaitForASilentHost is the I1.2 finding as a
// test: a Host that never answers a delivery must not delay the durable
// acknowledgement of a command that is already committed. Commit before
// acknowledge (A3.2) is about the STORE; nothing about the Host is owed before
// the answer, and the wake it gets is best effort either way (A3.3 step 3).
//
// Every control route is rowed, including the create, whose delivery target
// comes from its body rather than its path.
func TestTheAcknowledgementDoesNotWaitForASilentHost(t *testing.T) {
	t.Parallel()

	probes := append(controlProbes(fixtureSession), controlProbe{
		name:    "create",
		target:  "/v1/sessions",
		body:    `{"version":1,"command_id":"command-a","session_id":"session-new","agent_id":"agent-a"}`,
		success: http.StatusCreated,
	})
	for _, probe := range probes {
		t.Run(probe.name, func(t *testing.T) {
			t.Parallel()
			host := newSilentHost()
			f := newFixture(t, withAdmitter(newFakeAdmitter()), withDelivery(host))
			t.Cleanup(func() { stopWakes(t, f) })

			code, elapsed := serveWithin(t, f, postRequest(probe.target, probe.body))
			if code != probe.success {
				t.Fatalf("answered %d, want the durable %d", code, probe.success)
			}
			t.Logf("acknowledged in %v with the Host silent", elapsed)

			// The wake is still made: detaching it must not have dropped it.
			host.awaitStarted(t, 1)
		})
	}
}

func postRequest(target, body string) *http.Request {
	return request(http.MethodPost, target, strings.NewReader(body))
}

// TestStopWakesCancelsARunningWakeAndRefusesLaterOnes is the ownership half of
// the detached wake: StopWakes CANCELS a wake blocked on a silent Host, does
// not return until it has returned, and no wake starts afterwards.
//
// "Returned" is read from the stand-in's own running count, which is the
// goroutine-leak property stated at the only place it can be observed without
// counting the whole process's goroutines under t.Parallel.
func TestStopWakesCancelsARunningWakeAndRefusesLaterOnes(t *testing.T) {
	t.Parallel()

	probe := controlProbes(fixtureSession)[0]
	host := newSilentHost()
	f := newFixture(t, withAdmitter(newFakeAdmitter()), withDelivery(host))

	if code, _ := serveWithin(t, f, postRequest(probe.target, probe.body)); code != probe.success {
		t.Fatalf("answered %d, want %d", code, probe.success)
	}
	host.awaitStarted(t, 1)

	stopWakes(t, f)

	started, running := host.counts()
	if running != 0 {
		t.Fatalf("StopWakes returned with %d wake(s) still running: a goroutine outlives Stop", running)
	}
	host.mu.Lock()
	ended := append([]error(nil), host.ctxErrs...)
	host.mu.Unlock()
	if len(ended) != 1 || !errors.Is(ended[0], context.Canceled) {
		t.Fatalf("the wake ended with %v, want context.Canceled from StopWakes (not its own deadline)", ended)
	}

	// A command admitted after Stop is still acknowledged -- the durable path
	// does not depend on the wake -- and starts no wake.
	if code, _ := serveWithin(t, f, postRequest(probe.target, strings.Replace(probe.body, "command-a", "command-b", 1))); code != probe.success {
		t.Fatalf("after StopWakes the command answered %d, want the durable %d", code, probe.success)
	}
	// Settle before counting: a wake wrongly started here runs on its own
	// goroutine, and StopWakes is what waits for it to reach the Host.
	stopWakes(t, f)
	if again, _ := host.counts(); again != started {
		t.Fatalf("a wake started after StopWakes (%d attempts, want %d)", again, started)
	}

}

// TestStopWakesIsBoundedByItsCaller drives a delivery seam that ignores its
// context: StopWakes must give up when its own context ends rather than make
// Stop unkillable, and report that it did.
func TestStopWakesIsBoundedByItsCaller(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	w := newWakes(deliveryFunc(func(context.Context) {
		entered <- struct{}{}
		<-release
	}), time.Minute, 1)
	if !w.schedule(fixtureTenant, fixtureSession, "command-a") {
		t.Fatal("the wake was not started")
	}
	<-entered

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	stopped := make(chan error, 1)
	go func() { stopped <- w.stop(ctx) }()
	select {
	case err := <-stopped:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("stop over a wake that ignores cancellation = %v, want the caller's deadline", err)
		}
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("stop did not return when its caller's context ended: Stop is unkillable behind one wedged wake")
	}
	close(release)
	if err := w.stop(context.Background()); err != nil {
		t.Fatalf("stop after the wake returned = %v, want nil", err)
	}
}

// TestWakesAreBoundedInNumber holds maxConcurrentWakes' rule at a small limit:
// with every slot held by a silent Host, a further wake is DROPPED (not queued,
// not blocking the caller), and a slot freed is a slot reused.
func TestWakesAreBoundedInNumber(t *testing.T) {
	t.Parallel()

	host := newSilentHost()
	w := newWakes(host, time.Minute, 2)
	t.Cleanup(func() { _ = w.stop(context.Background()) })

	for i := range 2 {
		if !w.schedule(fixtureTenant, fixtureSession, "command-a") {
			t.Fatalf("wake %d was refused below the limit", i)
		}
	}
	host.awaitStarted(t, 2)
	if w.schedule(fixtureTenant, fixtureSession, "command-a") {
		t.Fatal("a third wake was started over a limit of two")
	}
	if started, _ := host.counts(); started != 2 {
		t.Fatalf("%d attempts reached the Host, want exactly the limit", started)
	}
}

// TestTheProductionLimitIsTheDocumentedOne pins maxConcurrentWakes to the
// number its doc argues for, and that the router is built with it: a router
// wired to a different limit passes every behavioural test above.
func TestTheProductionLimitIsTheDocumentedOne(t *testing.T) {
	t.Parallel()

	if maxConcurrentWakes != 64 {
		t.Fatalf("maxConcurrentWakes = %d, want the documented 64", maxConcurrentWakes)
	}
	f := newFixture(t, withDelivery(newSilentHost()))
	if got := cap(f.router.wakes.slots); got != maxConcurrentWakes {
		t.Fatalf("the router's wake limit is %d, want maxConcurrentWakes (%d)", got, maxConcurrentWakes)
	}
	if got := f.router.wakes.timeout; got != f.limits.RequestTimeout {
		t.Fatalf("the router's wake bound is %v, want the route's RequestTimeout %v", got, f.limits.RequestTimeout)
	}
}

// TestAPanickingDeliveryDoesNotEscapeTheWake: the wake is off the handler's
// goroutine, so the router's recoverPanic no longer covers it, and a panic
// there would end the process. The slot must be given back too.
func TestAPanickingDeliveryDoesNotEscapeTheWake(t *testing.T) {
	t.Parallel()

	w := newWakes(deliveryFunc(func(context.Context) { panic("delivery seam") }), time.Minute, 1)
	for i := range 3 {
		if !w.schedule(fixtureTenant, fixtureSession, "command-a") {
			t.Fatalf("wake %d was refused: a panicking wake did not give its slot back", i)
		}
		// Settle each before the next, so the single slot must have been freed.
		deadline := time.Now().Add(5 * time.Second)
		for len(w.slots) != 0 {
			if time.Now().After(deadline) {
				t.Fatal("the panicking wake never gave its slot back")
			}
			time.Sleep(time.Millisecond)
		}
	}
	if err := w.stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// TestNoDeliverySeamStartsNoWake: a nil seam is a supported composition and
// must not spend a slot or a goroutine.
func TestNoDeliverySeamStartsNoWake(t *testing.T) {
	t.Parallel()

	w := newWakes(nil, time.Minute, 1)
	if w.schedule(fixtureTenant, fixtureSession, "command-a") {
		t.Fatal("a wake was started with no delivery seam")
	}
	if len(w.slots) != 0 {
		t.Fatal("a nil seam consumed a slot")
	}
}

type deliveryFunc func(context.Context)

func (f deliveryFunc) Deliver(ctx context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, _ sessionwire.HostLinkCommandDelivery) error {
	f(ctx)
	return nil
}
