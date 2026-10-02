package routing

import (
	"context"
	"errors"
	"fmt"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// errGoneUnbind is what a Binder reports when the connection that carried the
// route is already gone: the Host dropped the route with the connection, so
// the unbind had nothing left to release. The Binder keeps the transport's own
// cause in the chain; only the classification is this package's.
var errGoneUnbind = fmt.Errorf("%w: hostlink.unbind: hostlink: link is reconnecting", ErrRouteGone)

// TestCloseTreatsARouteWhoseConnectionIsGoneAsReleased is the shutdown half of
// the stack teardown race: a Host stopped before Factory leaves the link
// reconnecting, and every unbind Close attempts is refused locally. The Host
// dropped those routes when the connection went, so there is nothing to give
// back and Close must not report a failure for it.
func TestCloseTreatsARouteWhoseConnectionIsGoneAsReleased(t *testing.T) {
	t.Parallel()

	second := observation("host-b", 1, 2)
	second.SessionID = "session-b"
	binder := &recordingBinder{unbindErr: errGoneUnbind}
	bindings := newBindings(t, newResolver(observation("host-a", 1, 1), second), binder)
	acquire(t, bindings, bindSession)
	acquire(t, bindings, "session-b")

	if err := bindings.Close(context.Background()); err != nil {
		t.Fatalf("Close = %v, want nil: a route whose connection is gone is already released", err)
	}
	if binder.unbindCount() != 2 {
		t.Errorf("unbinds = %d, want every route still attempted", binder.unbindCount())
	}
	if bindings.Len() != 0 {
		t.Errorf("Len = %d, want every local route dropped", bindings.Len())
	}
}

// TestCloseStillReportsAnUnbindThatMayHaveLeftARoute keeps the tolerance from
// swallowing everything: a failure that is not ErrRouteGone -- a Host refusal,
// a timeout with the request possibly in flight -- says nothing about whether
// the Host still holds the route, and is reported beside the tolerated one.
func TestCloseStillReportsAnUnbindThatMayHaveLeftARoute(t *testing.T) {
	t.Parallel()

	second := observation("host-b", 1, 2)
	second.SessionID = "session-b"
	refused := errors.New("hostlink: host refused unbind")
	binder := &perHostUnbinder{recordingBinder: &recordingBinder{}, errs: map[sessionwire.HostID]error{
		"host-a": errGoneUnbind,
		"host-b": refused,
	}}
	bindings := newBindings(t, newResolver(observation("host-a", 1, 1), second), binder)
	acquire(t, bindings, bindSession)
	acquire(t, bindings, "session-b")

	err := bindings.Close(context.Background())
	if !errors.Is(err, refused) {
		t.Fatalf("Close = %v, want the refusal reported", err)
	}
	if errors.Is(err, ErrRouteGone) {
		t.Errorf("Close = %v, want the gone route tolerated rather than reported beside the refusal", err)
	}
}

// TestReleaseStillReportsARouteWhoseConnectionIsGone pins the scope: only the
// shutdown paths absorb ErrRouteGone. A Release during normal operation keeps
// reporting it unchanged, so a caller can still see that the link is down.
func TestReleaseStillReportsARouteWhoseConnectionIsGone(t *testing.T) {
	t.Parallel()

	binder := &recordingBinder{unbindErr: errGoneUnbind}
	bindings := newBindings(t, newResolver(observation("host-a", 1, 1)), binder)
	acquire(t, bindings, bindSession)

	if err := bindings.Release(context.Background(), bindTenant, bindSession); !errors.Is(err, ErrRouteGone) {
		t.Fatalf("Release = %v, want ErrRouteGone reported during normal operation", err)
	}
}

// TestDemandCloseTreatsARouteWhoseConnectionIsGoneAsReleased is the same
// shutdown race one layer up: Server.Stop closes the demand plane first, and
// its last Release of each watched session is the unbind that meets the
// reconnecting link.
func TestDemandCloseTreatsARouteWhoseConnectionIsGoneAsReleased(t *testing.T) {
	t.Parallel()

	second := sessionwire.SessionID("session-b")
	other := observation("host-b", 1, 1)
	other.SessionID = second
	f := newDemandFixture(t, testDemandLimits, observation("host-a", 1, 1), other)
	f.acquire(t, bindSession)
	f.acquire(t, second)

	f.binder.mu.Lock()
	f.binder.unbindErr = errGoneUnbind
	f.binder.mu.Unlock()

	if err := f.demand.Close(context.Background()); err != nil {
		t.Fatalf("Close = %v, want nil: a route whose connection is gone is already released", err)
	}
	if got := f.binder.unbindCount(); got != 2 {
		t.Errorf("%d unbinds, want 2", got)
	}
	if got := f.demand.Len(); got != 0 {
		t.Errorf("%d sessions still watched after Close, want 0", got)
	}
}

// perHostUnbinder answers Unbind per addressed Host.
type perHostUnbinder struct {
	*recordingBinder
	errs map[sessionwire.HostID]error
}

func (b *perHostUnbinder) Unbind(ctx context.Context, req sessionwire.HostLinkUnbindRequest) error {
	_ = b.recordingBinder.Unbind(ctx, req)
	return b.errs[req.HostID]
}
