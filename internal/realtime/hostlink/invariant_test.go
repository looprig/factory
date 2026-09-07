package hostlink

import (
	"context"
	"errors"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// The route table and a link's binding set are two maps kept in step by one
// critical section, and Unbind and DeliverCommand both read a link out of
// p.links by a Host the ROUTE named. Neither lookup can be reached with a
// missing link through the public API, which is exactly why the coupling is
// worth a case: if it is ever broken -- by a dropped delete, by a reaper that
// learns a second reason to collect a link, by a future partial-failure path --
// the failure is not a wrong answer but a nil dereference inside the control
// plane, and a nil dereference in a control plane takes the replica with it.
//
// This case is in-package because the state it needs is unreachable from
// outside on purpose. It builds the broken invariant directly and requires each
// caller to fail closed with its own typed error, so the guards are live and
// pinned rather than defence that nothing ever executes.

type unusedDialer struct{}

func (unusedDialer) Dial(context.Context, Target, Observer) (Link, error) {
	return nil, errors.New("this pool must not dial")
}

func TestARouteNamingAHostWithNoLinkFailsClosedRatherThanPanicking(t *testing.T) {
	t.Parallel()

	const (
		orphanTenant  sessionwire.TenantID  = "tenant-a"
		orphanSession sessionwire.SessionID = "s-orphan"
		orphanHost    sessionwire.HostID    = "host-1"
	)

	pool, err := NewPool(Config{Dialer: unusedDialer{}})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close(context.Background()) })

	key := routeKey{tenant: orphanTenant, session: orphanSession}
	pool.mu.Lock()
	pool.routes[key] = orphanHost
	pool.mu.Unlock()

	t.Run("DeliverCommand", func(t *testing.T) {
		err := pool.DeliverCommand(context.Background(), orphanTenant, orphanSession,
			sessionwire.HostLinkCommandDelivery{CommandID: "cmd-abc"})
		if !errors.Is(err, ErrUnknownBinding) {
			t.Fatalf("DeliverCommand over an orphaned route = %v, want ErrUnknownBinding", err)
		}
	})

	t.Run("Unbind releases the orphan", func(t *testing.T) {
		unbind := sessionwire.HostLinkUnbindRequest{
			Version:        sessionwire.CurrentWireVersion,
			TenantID:       orphanTenant,
			SessionID:      orphanSession,
			HostID:         orphanHost,
			HostGeneration: 7,
			LeaseEpoch:     3,
			IdempotencyKey: "unbind-orphan",
		}
		err := pool.Unbind(context.Background(), unbind)
		if !errors.Is(err, ErrUnknownBinding) {
			t.Fatalf("Unbind of an orphaned route = %v, want ErrUnknownBinding", err)
		}
		// And the orphan is DROPPED rather than kept. A route naming nothing
		// that survived its own refusal would refuse every later bind for that
		// session as a conflict, which is the permanent wedge Unbind's own
		// documented policy -- release the local route even when the Host
		// could not be told -- exists to prevent.
		if host, ok := pool.RouteFor(orphanTenant, orphanSession); ok {
			t.Errorf("RouteFor = %q after the refusal, want the orphaned route dropped", host)
		}
	})
}
