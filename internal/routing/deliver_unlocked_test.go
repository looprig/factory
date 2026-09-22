package routing

import (
	"context"
	"errors"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// silentHostBinder stands in for a HostLink pool whose link to ONE Host is
// alive but never answers a delivery: DeliverCommand for the silent session
// blocks until its context ends or the case lets it go. Every other call is
// the recording binder's, and answers at once.
type silentHostBinder struct {
	*recordingBinder
	silent  sessionwire.SessionID
	entered chan struct{}
	release chan struct{}
}

func newSilentHostBinder(silent sessionwire.SessionID) *silentHostBinder {
	return &silentHostBinder{
		recordingBinder: &recordingBinder{},
		silent:          silent,
		entered:         make(chan struct{}, 64),
		release:         make(chan struct{}),
	}
}

func (b *silentHostBinder) DeliverCommand(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, delivery sessionwire.HostLinkCommandDelivery) error {
	if session != b.silent {
		return b.recordingBinder.DeliverCommand(ctx, tenant, session, delivery)
	}
	b.entered <- struct{}{}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.release:
		return errors.New("silent host: no reply")
	}
}

// prompt is how long an operation on an UNRELATED session may take while a
// delivery to a silent Host is in flight. It is generous for a lock-free
// in-memory call and far below any RPC bound, so a table that holds its lock
// across the RPC fails here rather than passing slowly.
const prompt = 2 * time.Second

func within(t *testing.T, what string, op func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- op() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	case <-time.After(prompt):
		t.Fatalf("%s did not complete within %v while a delivery to a silent Host was in flight; "+
			"the table's lock is held across the Host RPC", what, prompt)
	}
}

// TestADeliveryToASilentHostDoesNotStallTheReplica is gate finding S1 of the
// v0.7.1 review. Deliver used to hold the table's lock across the HostLink
// RPC, so one Host that was alive but dropped delivery replies stalled every
// Acquire, Bind, Release and delivery on the replica -- for EVERY session --
// for up to the RPC bound, and queued wakes stretched it to RequestTimeout.
// The route is resolved under the lock; the RPC runs outside it.
func TestADeliveryToASilentHostDoesNotStallTheReplica(t *testing.T) {
	t.Parallel()

	const (
		silent  = sessionwire.SessionID("session-silent")
		healthy = sessionwire.SessionID("session-healthy")
		fresh   = sessionwire.SessionID("session-fresh")
	)
	owned := func(session sessionwire.SessionID, host sessionwire.HostID) sessionwire.HostLinkRegistryObservation {
		o := observation(host, 1, 1)
		o.SessionID = session
		return o
	}
	binder := newSilentHostBinder(silent)
	resolver := newResolver(owned(silent, "host-silent"), owned(healthy, "host-healthy"), owned(fresh, "host-healthy"))
	bindings := newBindings(t, resolver, binder)
	acquire(t, bindings, silent)
	acquire(t, bindings, healthy)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stuck := make(chan error, 1)
	go func() {
		stuck <- bindings.Deliver(ctx, bindTenant, silent, sessionwire.HostLinkCommandDelivery{CommandID: "cmd-silent"})
	}()
	select {
	case <-binder.entered:
	case <-time.After(prompt):
		t.Fatal("the delivery to the silent Host never reached the binder")
	}

	// An Acquire that BINDS -- a registry read and a Bind -- for another session.
	within(t, "Acquire (with a Bind) of an unrelated session", func() error {
		_, err := bindings.Acquire(context.Background(), bindTenant, fresh)
		return err
	})
	// An Acquire that reuses a held route.
	within(t, "Acquire of an already bound session", func() error {
		_, err := bindings.Acquire(context.Background(), bindTenant, healthy)
		return err
	})
	// A healthy session's wake is not delayed behind the silent one.
	within(t, "Deliver to a healthy session", func() error {
		return bindings.Deliver(context.Background(), bindTenant, healthy, sessionwire.HostLinkCommandDelivery{CommandID: "cmd-healthy"})
	})
	within(t, "Release of an unrelated session", func() error {
		return bindings.Release(context.Background(), bindTenant, fresh)
	})
	within(t, "Binding, Demand and Len", func() error {
		_, _ = bindings.Binding(bindTenant, healthy)
		_ = bindings.Demand(bindTenant, healthy)
		_ = bindings.Len()
		return nil
	})

	cancel()
	if err := <-stuck; !errors.Is(err, context.Canceled) {
		t.Fatalf("the silent delivery = %v, want its own context's cancellation reported unchanged", err)
	}
}

// TestADeliveryInFlightDoesNotHoldCloseOrAMoveHostage is the other half of
// taking the RPC out of the lock: what a delivery captured may go stale while
// it runs, and neither Close nor an invalidating observation waits for it. The
// in-flight delivery is a WAKE HINT for an already committed record and names
// only (tenant, session, command) -- the binder resolves the route it goes
// over at call time, and the Host fences on its own lease -- so a route that
// moved or a table that closed underneath it costs nothing but the hint.
func TestADeliveryInFlightDoesNotHoldCloseOrAMoveHostage(t *testing.T) {
	t.Parallel()

	binder := newSilentHostBinder(bindSession)
	resolver := newResolver(observation("host-a", 1, 1))
	bindings := newBindings(t, resolver, binder)
	acquire(t, bindings, bindSession)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stuck := make(chan error, 1)
	go func() {
		stuck <- bindings.Deliver(ctx, bindTenant, bindSession, sessionwire.HostLinkCommandDelivery{CommandID: "cmd-1"})
	}()
	<-binder.entered

	moved := observation("host-b", 1, 2)
	resolver.put(moved)
	within(t, "Observe of a move", func() error {
		if !bindings.Observe(context.Background(), moved) {
			return errors.New("the move did not invalidate the binding")
		}
		return nil
	})
	within(t, "Close", func() error { return bindings.Close(context.Background()) })

	close(binder.release)
	if err := <-stuck; err == nil {
		t.Fatal("the stale delivery reported success; want the binder's failure reported unchanged")
	}
	// Nothing the stale delivery did re-opened a route on the closed table.
	if bindings.Len() != 0 {
		t.Errorf("Len = %d after Close, want the stale delivery to have left nothing", bindings.Len())
	}
	if err := bindings.Deliver(context.Background(), bindTenant, bindSession, sessionwire.HostLinkCommandDelivery{CommandID: "cmd-2"}); !errors.Is(err, ErrBindingsClosed) {
		t.Errorf("Deliver after Close = %v, want ErrBindingsClosed", err)
	}
}
