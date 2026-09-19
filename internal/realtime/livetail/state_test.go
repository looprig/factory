package livetail_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/hostlink"
	"github.com/looprig/factory/internal/realtime/livetail"
)

func newScriptedPlane(t *testing.T, links livetail.Links, relay *recordingRelay) *livetail.Plane {
	t.Helper()
	plane, err := livetail.New(livetail.Config{
		Links: links, Viewers: func() livetail.Viewers { return nil }, MailboxLimit: 8, EventTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	plane.Attach(relay, nil)
	t.Cleanup(func() { _ = plane.Close(context.Background()) })
	return plane
}

func settle(t *testing.T, plane *livetail.Plane, what string, done func() bool) {
	t.Helper()
	eventually(t, what, done, func() string {
		n, lost := livetail.Pending(plane, tenantA, session)
		return "pending=" + boolString(n > 0) + " lost=" + boolString(lost)
	})
}

// TestAWatchThatEndsLeavesNoStateBehind (quality gate W6, W7): when the last
// viewer leaves, the session's Relay state is forgotten and the plane's own
// entry is deleted once its drainer finishes. Neither leak is visible to a
// viewer, which is why each needs its own reader.
func TestAWatchThatEndsLeavesNoStateBehind(t *testing.T) {
	t.Parallel()

	links, relay := &scriptedLinks{}, &recordingRelay{}
	plane := newScriptedPlane(t, links, relay)
	plane.Watching(tenantA, session)
	if err := plane.Bind(context.Background(), "ws://host-1", sessionwire.HostLinkBindRequest{TenantID: tenantA, SessionID: session, HostID: "host-1"}); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	plane.Served(tenantA, session)
	plane.Unwatched(tenantA, session)
	settle(t, plane, "the plane to forget the session", func() bool { return livetail.Sessions(plane) == 0 })
	if ops := relay.opsSeen(); len(ops) == 0 || ops[len(ops)-1] != "forget" {
		t.Fatalf("relay ops = %v, want the session forgotten last", ops)
	}
}

// TestALateSubscribedForAnUnwatchedSessionOpensNothing (quality gate W4): a
// sink's Subscribed can land after the session's last viewer left. Accepting
// it would re-open Relay state for a session nobody watches -- after its
// Forget, so it would never be forgotten.
func TestALateSubscribedForAnUnwatchedSessionOpensNothing(t *testing.T) {
	t.Parallel()

	links, relay := &scriptedLinks{}, &recordingRelay{}
	plane := newScriptedPlane(t, links, relay)
	plane.Watching(tenantA, session)
	if err := plane.Bind(context.Background(), "ws://host-1", sessionwire.HostLinkBindRequest{TenantID: tenantA, SessionID: session, HostID: "host-1"}); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	plane.Served(tenantA, session)
	plane.Unwatched(tenantA, session)
	settle(t, plane, "the forget", func() bool { return livetail.Sessions(plane) == 0 })
	before := len(relay.opsSeen())

	links.sink(0).Subscribed()
	links.sink(0).Publication([]byte(`"late"`))
	time.Sleep(50 * time.Millisecond)
	if got := relay.opsSeen()[before:]; len(got) != 0 {
		t.Fatalf("a late Subscribed for an unwatched session reached the Relay: %v", got)
	}
	if got := livetail.Sessions(plane); got != 0 {
		t.Fatalf("a late Subscribed re-created %d plane entries", got)
	}
}

// failingSubscribeLinks binds, then refuses the subscribe, and records unbinds.
type failingSubscribeLinks struct {
	scriptedLinks
	mu      sync.Mutex
	unbinds []sessionwire.HostLinkUnbindRequest
}

func (l *failingSubscribeLinks) Subscribe(context.Context, sessionwire.TenantID, sessionwire.SessionID, hostlink.SessionSink) error {
	return hostlink.ErrSubscribeRefused
}

func (l *failingSubscribeLinks) Unbind(_ context.Context, req sessionwire.HostLinkUnbindRequest) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.unbinds = append(l.unbinds, req)
	return nil
}

// TestABindWhoseSubscribeFailsGivesTheRouteBack (quality gate W10): a route
// this replica holds must carry a tail -- a route without one reads, to every
// viewer, exactly like an idle session -- so a subscribe that fails undoes the
// bind it followed, with the same tuple and key, and fails the call.
func TestABindWhoseSubscribeFailsGivesTheRouteBack(t *testing.T) {
	t.Parallel()

	links := &failingSubscribeLinks{}
	plane := newScriptedPlane(t, links, &recordingRelay{})
	req := sessionwire.HostLinkBindRequest{
		Version: sessionwire.CurrentWireVersion, TenantID: tenantA, SessionID: session, HostID: "host-1",
		HostGeneration: 2, LeaseEpoch: 3, RuntimeCompatibilityID: "runtime-1", IdempotencyKey: "bind-key",
	}
	err := plane.Bind(context.Background(), "ws://host-1", req)
	if !errors.Is(err, hostlink.ErrSubscribeRefused) || !strings.Contains(err.Error(), "subscribe after bind") {
		t.Fatalf("Bind with a refused subscribe = %v, want the refusal", err)
	}
	links.mu.Lock()
	defer links.mu.Unlock()
	if len(links.unbinds) != 1 {
		t.Fatalf("unbinds = %v, want the bind given back exactly once", links.unbinds)
	}
	got := links.unbinds[0]
	if got.TenantID != req.TenantID || got.SessionID != req.SessionID || got.HostID != req.HostID ||
		got.HostGeneration != req.HostGeneration || got.LeaseEpoch != req.LeaseEpoch || got.IdempotencyKey != req.IdempotencyKey {
		t.Fatalf("the unbind %+v does not name the bind it undoes (%+v)", got, req)
	}
}

// blockingCloseRelay's Close waits until released, as a Relay whose mutex a
// drainer still holds does.
type blockingCloseRelay struct {
	recordingRelay
	release chan struct{}
}

func (r *blockingCloseRelay) Close() { <-r.release }

// TestCloseIsBoundedByItsContextEvenWhenTheRelayIsBusy (quality gate F8).
func TestCloseIsBoundedByItsContextEvenWhenTheRelayIsBusy(t *testing.T) {
	t.Parallel()

	plane, err := livetail.New(livetail.Config{
		Links: &scriptedLinks{}, Viewers: func() livetail.Viewers { return nil }, MailboxLimit: 8, EventTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	relay := &blockingCloseRelay{release: make(chan struct{})}
	defer close(relay.release)
	plane.Attach(relay, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	returned := make(chan error, 1)
	go func() { returned <- plane.Close(ctx) }()
	select {
	case err := <-returned:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Close past its deadline = %v, want context.DeadlineExceeded", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close ignored its context while the Relay was busy")
	}
}

// recordingRebinder records Rebind calls.
type recordingRebinder struct {
	mu    sync.Mutex
	calls int
}

func (r *recordingRebinder) Rebind(context.Context, sessionwire.TenantID, sessionwire.SessionID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	return nil
}

func (r *recordingRebinder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

// TestRestoredDoesNotRebindARouteStillHeld (quality gate W2): Restored asks
// for a re-bind only when the route is gone. One that is held -- a poll or the
// repair already re-bound it -- is live, and re-binding it would tear that
// tail down and send every viewer a needless reset.
func TestRestoredDoesNotRebindARouteStillHeld(t *testing.T) {
	t.Parallel()

	links, relay, rebinder := &scriptedLinks{}, &recordingRelay{}, &recordingRebinder{}
	plane, err := livetail.New(livetail.Config{
		Links: links, Viewers: func() livetail.Viewers { return nil }, MailboxLimit: 8, EventTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	plane.Attach(relay, rebinder)
	t.Cleanup(func() { _ = plane.Close(context.Background()) })
	plane.Watching(tenantA, session)
	if err := plane.Bind(context.Background(), "ws://host-1", sessionwire.HostLinkBindRequest{TenantID: tenantA, SessionID: session, HostID: "host-1"}); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	plane.Served(tenantA, session)
	links.sink(0).Restored() // scriptedLinks.RouteFor always reports the route held
	settle(t, plane, "the Restored event to drain", func() bool { n, _ := livetail.Pending(plane, tenantA, session); return n == 0 })
	time.Sleep(20 * time.Millisecond)
	if got := rebinder.count(); got != 0 {
		t.Fatalf("Restored re-bound a route still held %d times", got)
	}
}

// TestAnEndedTailsLatePublicationIsDropped (quality gate W3): after Ended the
// plane accepts nothing more from that subscription; the repair it queued
// resets the viewers from a tip, and a frame from the dead tail delivered
// after that would sit behind a reset that already covered it.
func TestAnEndedTailsLatePublicationIsDropped(t *testing.T) {
	t.Parallel()

	links, relay := &scriptedLinks{}, &recordingRelay{}
	plane := newScriptedPlane(t, links, relay)
	plane.Watching(tenantA, session)
	if err := plane.Bind(context.Background(), "ws://host-1", sessionwire.HostLinkBindRequest{TenantID: tenantA, SessionID: session, HostID: "host-1"}); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	plane.Served(tenantA, session)
	links.sink(0).Ended()
	links.sink(0).Publication([]byte(`"after-ended"`))
	time.Sleep(50 * time.Millisecond)
	if got := relay.got(); len(got) != 0 {
		t.Fatalf("an ended tail's publication reached the Relay: %v", got)
	}
}

// TestCloseClosesTheRelay (quality gate W12).
func TestCloseClosesTheRelay(t *testing.T) {
	t.Parallel()

	relay := &closeRecordingRelay{}
	plane, err := livetail.New(livetail.Config{
		Links: &scriptedLinks{}, Viewers: func() livetail.Viewers { return nil }, MailboxLimit: 8, EventTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	plane.Attach(relay, nil)
	if err := plane.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !relay.closed() {
		t.Fatal("Plane.Close left the Relay open")
	}
}

type closeRecordingRelay struct {
	recordingRelay
	mu   sync.Mutex
	done bool
}

func (r *closeRecordingRelay) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.done = true
}

func (r *closeRecordingRelay) closed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.done
}

// forgetGatedRelay blocks its Forget until released, holding a drainer inside
// the unwatched session's last event.
type forgetGatedRelay struct {
	recordingRelay
	entered chan struct{}
	release chan struct{}
}

func (r *forgetGatedRelay) Forget(tenant sessionwire.TenantID, session sessionwire.SessionID) {
	close(r.entered)
	<-r.release
	r.recordingRelay.Forget(tenant, session)
}

// TestASubscribedArrivingWhileTheForgetIsDrainingOpensNothing (quality gate
// W4, the window its first reader missed): between Unwatched and the drainer
// deleting the entry, a late Subscribed finds the entry still there. It must
// be refused as unwatched; accepted, it would queue a start behind the Forget
// and re-open Relay state for a session nobody watches.
func TestASubscribedArrivingWhileTheForgetIsDrainingOpensNothing(t *testing.T) {
	t.Parallel()

	links := &scriptedLinks{}
	relay := &forgetGatedRelay{entered: make(chan struct{}), release: make(chan struct{})}
	plane, err := livetail.New(livetail.Config{
		Links: links, Viewers: func() livetail.Viewers { return nil }, MailboxLimit: 8, EventTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	plane.Attach(relay, nil)
	t.Cleanup(func() { _ = plane.Close(context.Background()) })
	plane.Watching(tenantA, session)
	if err := plane.Bind(context.Background(), "ws://host-1", sessionwire.HostLinkBindRequest{TenantID: tenantA, SessionID: session, HostID: "host-1"}); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	plane.Served(tenantA, session)
	plane.Unwatched(tenantA, session)
	select {
	case <-relay.entered:
	case <-time.After(waitFor):
		t.Fatal("the drainer never reached the Forget")
	}
	links.sink(0).Subscribed()
	close(relay.release)
	settle(t, plane, "the plane to forget the session", func() bool { return livetail.Sessions(plane) == 0 })
	ops := relay.opsSeen()
	if len(ops) == 0 || ops[len(ops)-1] != "forget" {
		t.Fatalf("relay ops = %v: something reached the Relay after the Forget", ops)
	}
}
