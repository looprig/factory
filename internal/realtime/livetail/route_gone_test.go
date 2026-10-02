package livetail_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/hostlink"
	"github.com/looprig/factory/internal/realtime/livetail"
	"github.com/looprig/factory/internal/routing"
)

// unbindFailingLinks is scriptedLinks whose Unbind answers a fixed error, the
// way the HostLink pool answers when the route's link is down.
type unbindFailingLinks struct {
	scriptedLinks
	unbindErr error
}

func (l *unbindFailingLinks) Unbind(context.Context, sessionwire.HostLinkUnbindRequest) error {
	return l.unbindErr
}

// TestUnbindClassifiesAGoneConnectionAsRouteGone is the classification the
// shutdown paths rely on. Each error below is what the pool's link returns
// BEFORE sending anything, on a connection that is already gone (reconnecting,
// or terminally closed by either side) -- and host's Multiplexer drops every
// route a connection held when it disconnects, a reconnect being a new
// connection that inherits none. So the unbind had nothing left to release.
// The transport's own cause stays in the chain.
func TestUnbindClassifiesAGoneConnectionAsRouteGone(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		err  error
		gone bool
	}{
		{"reconnecting", fmt.Errorf("hostlink: hostlink.unbind: %w", hostlink.ErrLinkReconnecting), true},
		{"closed by this replica", fmt.Errorf("%w: host-1", hostlink.ErrLinkClosed), true},
		{"closed terminally by the Host", &hostlink.HostDisconnect{Host: "host-1", Code: 3500, Reason: "bye"}, true},
		{"reconnected to another wire version", fmt.Errorf("%w: host selected 2", hostlink.ErrUnsupportedProtocol), true},
		// The request may have reached the Host, or the Host answered: neither
		// says the route is gone.
		{"Host refusal", &hostlink.HostRefusal{HostLinkError: sessionwire.HostLinkError{Code: sessionwire.HostLinkErrorRuntimeUnavailable}}, false},
		{"deadline with the request possibly in flight", fmt.Errorf("hostlink: hostlink.unbind: %w", context.DeadlineExceeded), false},
		{"pool has no route", fmt.Errorf("%w: session %q", hostlink.ErrUnknownBinding, session), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			plane := newRouteGonePlane(t, &unbindFailingLinks{unbindErr: tc.err})

			err := plane.Unbind(context.Background(), unbindRequest())
			if got := errors.Is(err, routing.ErrRouteGone); got != tc.gone {
				t.Fatalf("Unbind = %v: errors.Is(ErrRouteGone) = %v, want %v", err, got, tc.gone)
			}
			var refusal *hostlink.HostRefusal
			var disconnect *hostlink.HostDisconnect
			if !errors.Is(err, tc.err) && !errors.As(err, &refusal) && !errors.As(err, &disconnect) {
				t.Errorf("Unbind = %v, want the transport's cause %v kept in the chain", err, tc.err)
			}
		})
	}
}

// TestStopOverAReconnectingLinkReportsNoUnbindFailure reproduces the stack
// teardown race at Factory's own seams: the routing table and the demand plane
// over this plane, with every unbind refused because the Host went away first.
// Before the fix both Close calls reported "hostlink: link is reconnecting",
// which Server.Stop returned to its caller.
func TestStopOverAReconnectingLinkReportsNoUnbindFailure(t *testing.T) {
	t.Parallel()

	links := &unbindFailingLinks{}
	plane := newRouteGonePlane(t, links)
	dir := &directory{owners: map[string]sessionwire.HostLinkRegistryObservation{}}
	now := time.Now()
	for _, sid := range []sessionwire.SessionID{"s-1", "s-2"} {
		dir.put(sessionwire.HostLinkRegistryObservation{
			Version: sessionwire.CurrentWireVersion, TenantID: tenantA, SessionID: sid,
			HostID: "host-1", HostGeneration: 1, AgentID: "agent", RuntimeCompatibilityID: "runtime-1",
			Placement: sessionwire.HostPlacementPooled, InternalEndpoint: "ws://host-1",
			Residency: sessionwire.SessionResidencyResident, Accepting: true, LeaseEpoch: 1,
			ObservedAt: now, ExpiresAt: now.Add(time.Hour),
		})
	}

	// The routing table alone (Server.Stop phase 3, second call).
	bindings, err := routing.NewBindings(dir, plane)
	if err != nil {
		t.Fatalf("NewBindings: %v", err)
	}
	for _, sid := range []sessionwire.SessionID{"s-1", "s-2"} {
		if _, err := bindings.Acquire(context.Background(), tenantA, sid); err != nil {
			t.Fatalf("Acquire(%s): %v", sid, err)
		}
	}
	links.unbindErr = fmt.Errorf("hostlink: hostlink.unbind: %w", hostlink.ErrLinkReconnecting)
	if err := bindings.Close(context.Background()); err != nil {
		t.Fatalf("Bindings.Close = %v, want nil: the Host dropped these routes with the connection", err)
	}

	// The demand plane over a fresh table (Server.Stop phase 3, first call).
	links.unbindErr = nil
	table, err := routing.NewBindings(dir, plane)
	if err != nil {
		t.Fatalf("NewBindings: %v", err)
	}
	demand, err := routing.NewDemand(table, &tips{}, plane, &manualClock{}, routing.DemandLimits{
		OwnershipPollInterval: time.Hour, PollTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewDemand: %v", err)
	}
	for _, sid := range []sessionwire.SessionID{"s-1", "s-2"} {
		if err := demand.Acquire(context.Background(), tenantA, sid); err != nil {
			t.Fatalf("Demand.Acquire(%s): %v", sid, err)
		}
	}
	links.unbindErr = fmt.Errorf("%w: host-1", hostlink.ErrLinkClosed)
	if err := demand.Close(context.Background()); err != nil {
		t.Fatalf("Demand.Close = %v, want nil: the Host dropped these routes with the connection", err)
	}
}

func newRouteGonePlane(t *testing.T, links livetail.Links) *livetail.Plane {
	t.Helper()
	plane, err := livetail.New(livetail.Config{
		Links: links, Viewers: func() livetail.Viewers { return nil }, MailboxLimit: 8, EventTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = plane.Close(context.Background()) })
	return plane
}

func unbindRequest() sessionwire.HostLinkUnbindRequest {
	return sessionwire.HostLinkUnbindRequest{
		Version: sessionwire.CurrentWireVersion, TenantID: tenantA, SessionID: session,
		HostID: "host-1", HostGeneration: 1, LeaseEpoch: 1, IdempotencyKey: "key-1",
	}
}
