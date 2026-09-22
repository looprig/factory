package factory

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/hostlink"
)

// silentDial dials links to a Host that is alive but never answers a command
// delivery, and records any teardown -- an Unbind or a Close -- that reaches a
// link while a delivery is still in flight on it.
type silentDial struct {
	methods []string

	mu         sync.Mutex
	inFlight   int
	violations []string
	entered    chan struct{}
}

func (d *silentDial) Dial(_ context.Context, target hostlink.Target, _ hostlink.Observer) (hostlink.Link, error) {
	return &silentLink{scriptedLink: scriptedLink{host: target.Host, methods: d.methods}, dial: d}, nil
}

func (d *silentDial) teardown(what string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.inFlight > 0 {
		d.violations = append(d.violations, what)
	}
}

type silentLink struct {
	scriptedLink
	dial *silentDial
}

func (l *silentLink) DeliverCommand(ctx context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, _ sessionwire.HostLinkCommandDelivery) error {
	l.dial.mu.Lock()
	l.dial.inFlight++
	l.dial.mu.Unlock()
	l.dial.entered <- struct{}{}
	<-ctx.Done()
	l.dial.mu.Lock()
	l.dial.inFlight--
	l.dial.mu.Unlock()
	return ctx.Err()
}

// Subscribe accepts the live tail the demand plane asks for after a bind; the
// case is about deliveries, not publications.
func (l *silentLink) Subscribe(_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, sink hostlink.SessionSink) error {
	sink.Subscribed()
	return nil
}

func (l *silentLink) Unsubscribe(sessionwire.TenantID, sessionwire.SessionID) {}

func (l *silentLink) Unbind(context.Context, sessionwire.HostLinkUnbindRequest) error {
	l.dial.teardown("Unbind")
	return nil
}

func (l *silentLink) Close(context.Context) error {
	l.dial.teardown("Close")
	return nil
}

// TestStopEndsTheHostWakesBeforeItTearsDownTheRoutes pins the ORDER inside
// Server.Stop that TestStopStopsTheHostWakesAndQuiesceDoesNot cannot: the
// post-acknowledgement wakes are cancelled and waited for BEFORE the demand
// plane, the routing table and the HostLink pool are closed underneath them.
//
// The reader is the transport: a wake to a silent Host is in flight when Stop
// begins, and no route may be unbound, and no link closed, while it is. Moving
// StopWakes after the pool's Close (the v0.7.1 gate's surviving mutant A')
// unbinds the session and closes its link with the delivery still running, and
// fails here.
func TestStopEndsTheHostWakesBeforeItTearsDownTheRoutes(t *testing.T) {
	t.Parallel()

	dial := &silentDial{
		methods: []string{sessionwire.HostLinkMethodBind, sessionwire.HostLinkCapabilityGateResponse},
		entered: make(chan struct{}, 8),
	}
	server := composedGateServer(t, gateWorld(t), dial)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// A viewer on this replica: the wake only reaches a Host for a session
	// the replica holds demand for.
	if err := server.components.demand.Acquire(ctx, FakeTenant, e2eSession); err != nil {
		t.Fatalf("demand.Acquire: %v", err)
	}
	if _, bound := server.components.bindings.Binding(FakeTenant, e2eSession); !bound {
		t.Fatal("the viewer's demand did not bind the session to its owner")
	}
	if answer := postGateResponse(t, server, "answer-1"); answer.Code != http.StatusAccepted {
		t.Fatalf("gate response = %d %s, want 202", answer.Code, answer.Body)
	}
	select {
	case <-dial.entered:
	case <-ctx.Done():
		t.Fatal("the acknowledged command's wake never reached the Host")
	}

	if err := server.Stop(ctx); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Stop: %v", err)
	}

	dial.mu.Lock()
	defer dial.mu.Unlock()
	if len(dial.violations) != 0 {
		t.Fatalf("Stop reached the transport with a Host wake still in flight: %v; "+
			"StopWakes must run before the routing state and the pool close", dial.violations)
	}
	if dial.inFlight != 0 {
		t.Fatalf("Stop returned with %d Host wake(s) still in flight", dial.inFlight)
	}
}
