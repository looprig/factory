package hostlink_test

import (
	"context"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/hostlink"
)

// These cases are I3.1's D1: a Host restarted the way a pod is -- the SAME
// HostID, a HIGHER generation, a NEW address -- was unreachable from a replica
// that held a link to its predecessor, because the pool returned the cached
// (HostID, tenant) link without looking at where it went. The replica
// redialled the dead address, answered ErrLinkReconnecting, and recovered only
// when the idle reaper collected the link (60s) -- never, if a viewer's route
// pinned it. The pool now replaces a cached link whose Host's advertisement
// moved, and keeps it for an observation older than the one it was dialled on.

const movedEndpoint sessionwire.InternalEndpoint = "wss://host-1-restarted.internal:8443"

func bindAt(host sessionwire.HostID, session sessionwire.SessionID, generation uint64) sessionwire.HostLinkBindRequest {
	req := bindRequest(host, session)
	req.HostGeneration = generation
	return req
}

func TestABindToAHostThatMovedReplacesItsLink(t *testing.T) {
	t.Parallel()

	dialer := newRecordingDialer()
	pool := newPool(t, dialer, hostlink.Limits{})
	mustBind(t, pool, target(hostOne, endpoint1), bindAt(hostOne, "s-old", 7))
	predecessor := dialer.link(hostOne)

	mustBind(t, pool, target(hostOne, movedEndpoint), bindAt(hostOne, "s-new", 8))

	targets := dialer.targets()
	if len(targets) != 2 || targets[1].Endpoint != movedEndpoint+"/hostlink/"+sessionwire.InternalEndpoint(tenant) {
		t.Fatalf("dialled %v, want a second dial to the moved Host's tenant address", targets)
	}
	if got := pool.TenantLinks(hostOne); got != 1 {
		t.Fatalf("TenantLinks = %d, want the one replacement link", got)
	}
	predecessor.mu.Lock()
	closed := predecessor.closeCount
	predecessor.mu.Unlock()
	if closed != 1 {
		t.Fatalf("the predecessor's link was closed %d times, want 1", closed)
	}
	// The predecessor's routes went with its link: the Host they named is gone.
	if _, ok := pool.RouteFor(tenant, "s-old"); ok {
		t.Fatal("a route to the predecessor survived its link")
	}
	if host, ok := pool.RouteFor(tenant, "s-new"); !ok || host != hostOne {
		t.Fatalf("RouteFor(s-new) = %q, %v; want the successor", host, ok)
	}
	if got := len(dialer.link(hostOne).bindRecords); got != 1 {
		t.Fatalf("the successor's link carried %d binds, want 1", got)
	}
}

func TestAnOlderObservationDoesNotReplaceANewerLink(t *testing.T) {
	t.Parallel()

	dialer := newRecordingDialer()
	pool := newPool(t, dialer, hostlink.Limits{})
	mustBind(t, pool, target(hostOne, movedEndpoint), bindAt(hostOne, "s-1", 8))
	// A stale registry row still names the predecessor's address.
	mustBind(t, pool, target(hostOne, endpoint1), bindAt(hostOne, "s-2", 7))
	// The same address at a higher generation is the same connection: the
	// transport's own reconnect reaches the restarted Host.
	mustBind(t, pool, target(hostOne, movedEndpoint), bindAt(hostOne, "s-3", 9))
	if got := dialer.dials(); got != 1 {
		t.Fatalf("dialled %d times, want the one link kept (%v)", got, dialer.targets())
	}
	// The same incarnation at another address contradicts the link and does
	// not move it.
	mustBind(t, pool, target(hostOne, endpoint2), bindAt(hostOne, "s-4", 9))
	if got := dialer.dials(); got != 1 {
		t.Fatalf("dialled %d times, want one incarnation's second address ignored", got)
	}
	// And a newer generation that DID move is still followed after that.
	mustBind(t, pool, target(hostOne, endpoint2), bindAt(hostOne, "s-5", 10))
	if got := dialer.dials(); got != 2 {
		t.Fatalf("dialled %d times, want a move at the latest generation followed", got)
	}
}

func TestAnAttachToAHostThatMovedReplacesItsLink(t *testing.T) {
	t.Parallel()

	dialer := newRecordingDialer()
	pool := newPool(t, dialer, hostlink.Limits{})
	mustBind(t, pool, target(hostOne, endpoint1), bindAt(hostOne, "s-viewer", 7))
	predecessor := dialer.link(hostOne)

	req := attachRequest(hostOne, "s-placed")
	req.HostGeneration = 8
	if _, err := pool.Attach(context.Background(), target(hostOne, movedEndpoint), req); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if got := dialer.dials(); got != 2 {
		t.Fatalf("dialled %d times, want the moved Host dialled", got)
	}
	if got := len(dialer.link(hostOne).attaches()); got != 1 {
		t.Fatalf("the successor carried %d attaches, want 1", got)
	}
	predecessor.mu.Lock()
	closed := predecessor.closeCount
	predecessor.mu.Unlock()
	if closed != 1 {
		t.Fatalf("the predecessor was closed %d times, want 1 (a viewer's route must not pin it)", closed)
	}
}

func TestAGateCapabilityReadOfAHostThatMovedReplacesItsLink(t *testing.T) {
	t.Parallel()

	dialer := newRecordingDialer()
	pool := newPool(t, dialer, hostlink.Limits{})
	mustBind(t, pool, target(hostOne, endpoint1), bindAt(hostOne, "s-1", 7))
	if _, err := pool.AcceptsGateResponses(context.Background(), hostlink.Target{Host: hostOne, Endpoint: movedEndpoint, Generation: 8}, tenant); err != nil {
		t.Fatalf("AcceptsGateResponses: %v", err)
	}
	if got := dialer.dials(); got != 2 {
		t.Fatalf("dialled %d times, want the moved Host dialled", got)
	}
	if _, err := pool.AcceptsGateResponses(context.Background(), hostlink.Target{Host: hostOne, Endpoint: endpoint1, Generation: 7}, tenant); err != nil {
		t.Fatalf("AcceptsGateResponses: %v", err)
	}
	if got := dialer.dials(); got != 2 {
		t.Fatalf("dialled %d times, want an older observation answered from the current link", got)
	}
}
