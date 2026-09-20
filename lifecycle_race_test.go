package factory

import (
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/hostlink"
	"github.com/looprig/factory/internal/routing"
)

// This file is in package factory rather than factory_test, and the reason is
// the property it holds rather than convenience.
//
// The defect it exists for is a WINDOW: a Stop that lands after Serve has
// entered and before Serve has claimed the serving state leaves the caller's
// listening socket open and unserved, because everything Stop can close about
// the public surface it reaches through s.http. Reproducing that from outside
// means racing and hoping -- it surfaced as roughly one run in three under CPU
// load, and it does not reproduce in isolation at -count=30. A case that can
// only fail sometimes is not a guard.
//
// So the window is driven DIRECTLY, by holding the lifecycle mutex the fix
// introduced. That makes the interleaving certain instead of likely, and it
// makes the assertion about the mechanism rather than about the symptom.

// serveState reports the claim Serve makes, for a test that must observe the
// ORDER of two steps inside one method.
func (s *Server) serveState() (serverState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state, s.http != nil
}

// TestServeClaimsTheServingStateBeforeItStartsAnything is the ordering itself.
//
// The lifecycle mutex is taken by the test, so Serve's call to Start BLOCKS.
// While it is blocked, Serve must ALREADY have claimed stateServing and built
// the http.Server -- because those are the only things a concurrent Stop can
// find, and a Stop that finds no server skips Shutdown and closes nothing.
//
// A composition that starts the background planes before the claim leaves
// s.http nil for the whole of Start, and this case times out on exactly that
// observation with the mechanism named.
func TestServeClaimsTheServingStateBeforeItStartsAnything(t *testing.T) {
	t.Parallel()

	server := raceServer(t)
	listener := localListener(t)

	release := pinStart(server)
	defer release()
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()

	waitForServing(t, server)
	release()

	stopServer(t, server)
	if err := await(t, "Serve", served); err != nil {
		t.Fatalf("Serve: %v", err)
	}
	assertListenerClosed(t, listener)
}

// TestAStopThatOvertakesServeStillClosesTheListener is the consequence.
//
// The interleaving is made certain the same way: Serve is pinned inside Start
// while a Stop runs to completion. Under the defect Serve then returned
// ErrServerStopped having never handed the listener to net/http, and nothing
// anywhere closed it -- the caller's socket stayed open and unserved for the
// life of the process.
//
// Two things are asserted, because they fail separately. The ordinary ending
// is reported as SUCCESS, which is Serve's own written contract; and the
// listener is CLOSED, which is what runbook step 4 asks for. A fix that
// returned nil and still leaked would pass the first and fail the second.
func TestAStopThatOvertakesServeStillClosesTheListener(t *testing.T) {
	t.Parallel()

	server := raceServer(t)
	listener := localListener(t)

	release := pinStart(server)
	defer release()
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()

	// Wait until Serve is demonstrably inside Start: it has claimed the state
	// and is blocked on the mutex this test holds.
	waitForServing(t, server)

	stopped := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		stopped <- server.Stop(ctx)
	}()
	// Stop is serialized behind Start, so it cannot proceed until the pin is
	// released. Releasing it lets Start finish and Stop run immediately after,
	// which is the losing interleaving.
	time.Sleep(10 * time.Millisecond)
	release()

	if err := await(t, "Stop", stopped); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := await(t, "Serve", served); err != nil {
		t.Fatalf("Serve reported %v; a deliberate Stop is the ordinary ending and is reported as success", err)
	}
	assertListenerClosed(t, listener)
}

// TestServeClosesTheListenerWhenStartRefusesIsTheOtherRouteOut covers the one
// path out of Serve, past the claim, that never reaches net/http.
//
// It is a SEPARATE case from the one above, and it has to be, because the two
// paths close the listener by different machinery. When Serve reaches
// server.Serve(ln), net/http's own deferred l.Close() is what closes the
// socket even for a server already shut down. When Start REFUSES, that line is
// never reached and Serve must close the listener itself -- and a mutant that
// deletes exactly that close survives every other case in this file.
//
// The stopped state is set DIRECTLY rather than by calling Stop, and the
// reason is determinism rather than convenience: Start and Stop are serialized
// on one mutex, two goroutines blocked on it have no ordering guarantee, and a
// case that depends on which one wins is a case that fails one run in N. What
// is written here is exactly the transition Stop makes -- the same field,
// under the same lock -- so the branch driven is the production one.
func TestServeClosesTheListenerWhenStartRefusesIsTheOtherRouteOut(t *testing.T) {
	t.Parallel()

	server := raceServer(t)
	listener := localListener(t)

	release := pinStart(server)
	defer release()
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	waitForServing(t, server)

	// The transition a Stop that overtook this Serve would have made.
	server.mu.Lock()
	server.state = stateStopped
	server.mu.Unlock()
	release()

	if err := await(t, "Serve", served); err != nil {
		t.Fatalf("Serve reported %v; a Stop that overtook it is the ordinary ending and is reported as success", err)
	}
	assertListenerClosed(t, listener)
}

func TestServeClosesListenerWhenQuiesceOvertakesStart(t *testing.T) {
	server := raceServer(t)
	listener := localListener(t)
	release := pinStart(server)
	defer release()
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	waitForServing(t, server)
	server.mu.Lock()
	server.quiescing = true
	server.quiesceDone = make(chan struct{})
	close(server.quiesceDone)
	server.mu.Unlock()
	release()
	if err := await(t, "Serve", served); err != nil {
		t.Fatalf("Serve = %v", err)
	}
	assertListenerClosed(t, listener)
}

// TestServeClosesTheListenerWhenStartReallyFails is the THIRD route out of
// Serve past the claim, and the last one.
//
// Serve's own documentation commits to a property over ROUTES -- "every route
// out of this method past the claim either serves the listener or closes it"
// -- and a property over routes is only held when every route has a row. Two
// were rowed and this one, the sibling branch three lines from the one
// …WhenStartRefuses… rows, was not: deleting its close survived the whole
// suite. That is the blocker's own shape a second time, one branch over, and
// it is why the three cases are enumerated from the switch rather than from
// the failures that happened to be found.
//
// The failure is induced by emptying the build version the ClientLink engine
// requires, which is a production error path: clientlink.NewEngine refuses an
// empty Version, startRealtime wraps that, and Start returns it. It is done
// from inside the package because no composition factory.New accepts can reach
// it -- New defaults the version and re-validates every limit the engine
// checks, so a deployer cannot compose a Start that fails this way, and a case
// that could only drive it through a composition New rejects would be driving
// New rather than Serve.
//
// # Why NOT by nulling a composed component, which was tried first
//
// Setting components.admissions to nil does NOT make the engine refuse.
// clientlink.Config.Admitter is an INTERFACE and the field assigned to it is a
// *admission.Service, so a nil pointer becomes a non-nil interface holding a
// nil pointer and `cfg.Admitter == nil` is false. Start then succeeded, Serve
// blocked in net/http with no Stop coming, and the case hung for its whole
// timeout. Recorded because it is a live trap for anyone writing the next
// probe here: the engine's nil guards cannot see a typed nil, and nothing in
// this module hands it one today only because New requires every seam.
func TestServeClosesTheListenerWhenStartReallyFails(t *testing.T) {
	t.Parallel()

	server := raceServer(t)
	listener := localListener(t)

	// The ClientLink engine requires a non-empty build version. Without one
	// Start fails with a real error -- neither ErrAlreadyStarted nor
	// ErrServerStopped -- which is the arm under test.
	server.cfg.version = ""

	// Serve runs on its own goroutine and the receive is BOUNDED, for the
	// reason await states: a mutant that stops Serve from consulting Start at
	// all leaves this call blocked in net/http forever, and an unbounded
	// receive would turn that mutant into a hang rather than a scorable
	// failure. Measured -- it did exactly that on the first draft of this row.
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	err := await(t, "Serve", served)
	switch {
	case err == nil:
		t.Fatal("Serve reported success although Start failed")
	case errors.Is(err, ErrServerStopped), errors.Is(err, ErrAlreadyStarted):
		t.Fatalf("Serve returned %v, which is one of the OTHER two arms; this case is no longer driving the real-error arm", err)
	}
	assertListenerClosed(t, listener)
}

// TestEveryRouteOutOfServeIsRowed is the anti-vacuity guard for the three
// cases above, and it is a comment made checkable rather than a new claim.
//
// Serve's error handling is a switch with three arms, and the failure this
// file exists for is a route that has no row. A fourth arm added later would
// be a fourth route, and nothing in Go would make anyone notice; this reads
// the source and fails when the arm count moves, naming the three cases whose
// enumeration has to be re-derived.
func TestEveryRouteOutOfServeIsRowed(t *testing.T) {
	t.Parallel()

	source, err := os.ReadFile("serve.go")
	if err != nil {
		t.Fatalf("read serve.go: %v", err)
	}
	body := string(source)
	start := strings.Index(body, "switch err := s.Start(context.Background()); {")
	if start < 0 {
		t.Fatal("Serve no longer starts the background planes through a switch; re-derive the routes out of it")
	}
	end := strings.Index(body[start:], "\n\t}\n")
	if end < 0 {
		t.Fatal("could not find the end of Serve's Start switch")
	}
	arms := strings.Count(body[start:start+end], "\n\tcase ") + strings.Count(body[start:start+end], "\n\tdefault:")
	if arms != 3 {
		t.Fatalf("Serve's Start switch has %d arms, want 3. Each is a ROUTE out of Serve past the state claim, and each needs a "+
			"case asserting the listener is served or closed on it -- the three today are "+
			"TestAStopThatOvertakesServeStillClosesTheListener, …WhenStartRefusesIsTheOtherRouteOut and …WhenStartReallyFails.", arms)
	}
}

// TestStopClosesEveryLocalPlaneItComposed is Stop's THIRD phase, rowed.
//
// Phase 3 was ordered and documented and held by nothing: deleting all three
// closes survived the whole suite, which is the same "nothing was closed"
// shape as the listener defect above and is why that defect got past this
// file's first draft. The phases running is not the property; the planes being
// CLOSED is.
//
// Each plane is asserted through its OWN refusal sentinel, so deleting one
// close fails one row rather than all three -- a single combined assertion
// would report "something is not closed" and name nothing.
func TestStopClosesEveryLocalPlaneItComposed(t *testing.T) {
	t.Parallel()

	server := raceServer(t)
	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// The negative half of the row, taken BEFORE Stop: each plane must be OPEN
	// here, or "it is closed afterwards" would be true of a plane that was
	// never open and the assertion below would hold for free.
	assertPlanesOpen(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	for _, plane := range closedPlanes(server) {
		if err := plane.probe(ctx); !errors.Is(err, plane.closed) {
			t.Errorf("after Stop, %s answered %v, want %v -- Stop did not close it", plane.name, err, plane.closed)
		}
	}
}

func TestQuiesceLeavesRoutingAndHostLinksOpen(t *testing.T) {
	server := raceServer(t)
	if err := server.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := server.Quiesce(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertPlanesOpen(t, server)
	if err := server.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestStopContinuesAfterCompletedQuiesceDiagnostic(t *testing.T) {
	server := raceServer(t)
	if err := server.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertPlanesOpen(t, server)
	shutdownErr := errors.New("ClientLink shutdown failed")
	server.mu.Lock()
	server.quiescing = true
	server.quiesceDone = make(chan struct{})
	server.quiesceErr = shutdownErr
	close(server.quiesceDone)
	server.mu.Unlock()
	if err := server.Stop(context.Background()); !errors.Is(err, shutdownErr) {
		t.Fatalf("Stop = %v, want shutdown diagnostic", err)
	}
	for _, plane := range closedPlanes(server) {
		if err := plane.probe(context.Background()); !errors.Is(err, plane.closed) {
			t.Errorf("%s after Stop = %v, want closed", plane.name, err)
		}
	}
	// The fixture's cleanup expects a clean idempotent Stop; the injected
	// diagnostic belongs only to this assertion.
	server.mu.Lock()
	server.quiesceErr = nil
	server.mu.Unlock()
}

func TestCanceledQuiesceWaiterDoesNotBlockOnStart(t *testing.T) {
	server := raceServer(t)
	release := pinStart(server)
	defer release()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() { done <- server.Quiesce(ctx) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Quiesce = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled Quiesce blocked on Start")
	}
	release()
	if err := server.Quiesce(context.Background()); err != nil {
		t.Fatalf("retry Quiesce = %v", err)
	}
}

// ---------------------------------------------------------------------------
// Fixtures for the cases above.
// ---------------------------------------------------------------------------

// plane is one local component Stop's third phase closes, with the refusal it
// answers once it is.
type plane struct {
	name   string
	probe  func(context.Context) error
	closed error
}

func closedPlanes(s *Server) []plane {
	const (
		tenant  = "tenant-a"
		session = "session-a"
	)
	return []plane{
		{name: "the demand plane", closed: routing.ErrDemandClosed,
			probe: func(ctx context.Context) error { return s.components.demand.Acquire(ctx, tenant, session) }},
		{name: "the routing table", closed: routing.ErrBindingsClosed,
			probe: func(ctx context.Context) error {
				_, err := s.components.bindings.Acquire(ctx, tenant, session)
				return err
			}},
		{name: "the HostLink pool", closed: hostlink.ErrPoolClosed,
			probe: func(ctx context.Context) error {
				// The request must be WELL FORMED, because the pool
				// validates the target and the request before it consults
				// its own closed flag. A malformed probe would answer a
				// validation error whether the pool were closed or not,
				// which is a probe that cannot see the thing it is for --
				// measured: the first draft of this row answered
				// ErrTargetMismatch and proved nothing.
				return s.components.pool.Bind(ctx, hostlink.Target{
					Host:     "host-a",
					Endpoint: sessionwire.InternalEndpoint("ws://127.0.0.1:1"),
				}, sessionwire.HostLinkBindRequest{
					Version:                sessionwire.CurrentWireVersion,
					TenantID:               tenant,
					SessionID:              session,
					HostID:                 "host-a",
					HostGeneration:         1,
					LeaseEpoch:             1,
					RuntimeCompatibilityID: "runtime-v1",
					IdempotencyKey:         "probe",
				})
			}},
	}
}

// assertPlanesOpen is the negative half: each plane must NOT be answering its
// closed sentinel before Stop runs.
//
// It deliberately does not require the probes to SUCCEED -- a bind with no
// Host to dial fails, and a demand acquire with no observed owner fails -- so
// what it asserts is precisely the discriminating fact: whatever else is
// wrong, the plane is not reporting itself closed.
func assertPlanesOpen(t *testing.T, s *Server) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, p := range closedPlanes(s) {
		if err := p.probe(ctx); errors.Is(err, p.closed) {
			t.Fatalf("%s reports itself closed BEFORE Stop, so the case cannot tell a close from a no-op", p.name)
		}
	}
}

// await bounds a receive that a defect could make block forever.
//
// It exists because of what a mutation table can and cannot score. Removing
// the Start/Stop serialization leaves Serve blocked in net/http with no Stop
// coming, and an unbounded receive here turns that mutant into a HANG -- which
// is not an assertion failure and cannot be scored. A bounded receive turns
// the same mutant into a named failure that says which call never returned.
func await(t *testing.T, what string, ch <-chan error) error {
	t.Helper()

	select {
	case err := <-ch:
		return err
	case <-time.After(20 * time.Second):
		t.Fatalf("%s never returned", what)
		return nil
	}
}

func raceServer(t *testing.T) *Server {
	t.Helper()

	limits := DefaultReconcileLimits()
	limits.Interval = 5 * time.Millisecond
	limits.ClaimTTL = 50 * time.Millisecond
	limits.ApplyDeadline = 500 * time.Millisecond

	server, err := New(append(RequiredOptions(), WithReconcileLimits(limits))...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { stopServer(t, server) })
	return server
}

func stopServer(t *testing.T, s *Server) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.Stop(ctx); err != nil {
		t.Errorf("Stop: %v", err)
	}
}

func localListener(t *testing.T) net.Listener {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	return listener
}

// pinStart holds the lifecycle mutex so a concurrent Start BLOCKS, and hands
// back an idempotent release.
//
// Idempotence is not tidiness here, it is what keeps a FAILING case from
// hanging. Every case below both defers the release and calls it at the point
// the interleaving needs, and a case that fails before reaching that point
// unwinds through the defer -- t.Fatal runs deferred functions. Without this
// the mutant that reverts the ordering made waitForServing time out while the
// test still held the pin, its cleanup Stop then blocked on the same mutex,
// and the whole binary hung until go test's ten-minute timeout. A hang is not
// an assertion failure, and a mutation table cannot score one.
func pinStart(s *Server) func() {
	s.lifecycle.Lock()
	var once sync.Once
	return func() { once.Do(s.lifecycle.Unlock) }
}

// waitForServing blocks until Serve has claimed the state and built the
// server. It is the ASSERTION of the ordering case and the precondition of the
// other two, which is why it names the mechanism rather than the wait.
func waitForServing(t *testing.T, s *Server) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if state, built := s.serveState(); state == stateServing && built {
			return
		}
		time.Sleep(time.Millisecond)
	}
	state, built := s.serveState()
	t.Fatalf("Serve is inside Start with state=%v and http-built=%v; it has not claimed the serving state, "+
		"so a Stop arriving here would find no server to shut down and would close nothing -- "+
		"the listener would be orphaned", state, built)
}

// assertListenerClosed is the whole point of the two race cases.
//
// A closed listener answers its own Close with an error and refuses Accept; an
// ORPHANED one accepts a Close cleanly, which is what "nothing closed it"
// looks like from the outside. Closing is the probe rather than Accept because
// Accept on an open, unserved listener BLOCKS, and a case that hangs reports
// nothing.
func assertListenerClosed(t *testing.T, ln net.Listener) {
	t.Helper()

	if err := ln.Close(); err == nil {
		t.Fatalf("ORPHANED: the listener at %v was still open after Serve returned; nothing closed it", ln.Addr())
	}
}
