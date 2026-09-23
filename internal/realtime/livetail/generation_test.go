package livetail_test

import (
	"context"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/hostlink"
	"github.com/looprig/factory/internal/realtime/livetail"
	"github.com/looprig/factory/internal/routing"
)

// scriptedLinks accepts every bind and subscribe, and hands the test each sink
// so the test can speak for the transport.
type scriptedLinks struct {
	mu    sync.Mutex
	sinks []hostlink.SessionSink
}

func (l *scriptedLinks) Bind(context.Context, hostlink.Target, sessionwire.HostLinkBindRequest) error {
	return nil
}
func (l *scriptedLinks) Unbind(context.Context, sessionwire.HostLinkUnbindRequest) error { return nil }
func (l *scriptedLinks) DeliverCommand(context.Context, sessionwire.TenantID, sessionwire.SessionID, sessionwire.HostLinkCommandDelivery) error {
	return nil
}
func (l *scriptedLinks) Subscribe(_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, sink hostlink.SessionSink) error {
	l.mu.Lock()
	l.sinks = append(l.sinks, sink)
	l.mu.Unlock()
	sink.Subscribed()
	return nil
}
func (l *scriptedLinks) Unsubscribe(sessionwire.TenantID, sessionwire.SessionID) {}
func (l *scriptedLinks) RouteFor(sessionwire.TenantID, sessionwire.SessionID) (sessionwire.HostID, bool) {
	return "host-1", true
}

func (l *scriptedLinks) sink(i int) hostlink.SessionSink {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.sinks[i]
}

// recordingRelay records what reached the Relay.
type recordingRelay struct {
	mu       sync.Mutex
	received []string
	ops      []string
}

func (r *recordingRelay) op(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ops = append(r.ops, name)
}

func (r *recordingRelay) opsSeen() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.ops...)
}

func (r *recordingRelay) Open(sessionwire.TenantID, sessionwire.SessionID) error {
	r.op("open")
	return nil
}
func (r *recordingRelay) Subscribe(sessionwire.TenantID, sessionwire.SessionID, routing.LinkID) error {
	return nil
}
func (r *recordingRelay) Receive(_ context.Context, _ sessionwire.TenantID, _ sessionwire.SessionID, frame routing.Frame) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.received = append(r.received, string(frame.Encoded))
	return nil
}
func (r *recordingRelay) Pump(context.Context, sessionwire.TenantID, sessionwire.SessionID) error {
	return nil
}
func (r *recordingRelay) HostLinkClosed(context.Context, sessionwire.TenantID, sessionwire.SessionID) error {
	return nil
}
func (r *recordingRelay) Resync(context.Context, sessionwire.TenantID, sessionwire.SessionID) error {
	return nil
}
func (r *recordingRelay) Anchor(context.Context, sessionwire.TenantID, sessionwire.SessionID) error {
	return nil
}
func (r *recordingRelay) Forget(sessionwire.TenantID, sessionwire.SessionID) { r.op("forget") }
func (r *recordingRelay) Close()                                             {}

func (r *recordingRelay) got() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.received...)
}

// TestAStoppedOrReplacedTailsLatePublicationIsDropped: a sink reports on the
// transport's goroutine, and a publication from a tail that has since been
// stopped -- or replaced by a newer subscription -- may still be in flight. It
// must never reach the Relay, where it would sit in front of the new tail's
// records after a reset that already covered it.
func TestAStoppedOrReplacedTailsLatePublicationIsDropped(t *testing.T) {
	t.Parallel()

	links := &scriptedLinks{}
	relay := &recordingRelay{}
	plane, err := livetail.New(livetail.Config{
		Links: links, Viewers: func() livetail.Viewers { return nil }, MailboxLimit: 8, EventTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	plane.Attach(relay, nil)
	t.Cleanup(func() { _ = plane.Close(context.Background()) })

	plane.Watching(tenantA, session)
	req := sessionwire.HostLinkBindRequest{TenantID: tenantA, SessionID: session, HostID: "host-1"}
	if err := plane.Bind(context.Background(), "ws://host-1", req); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	plane.Served(tenantA, session)
	old := links.sink(0)

	if err := plane.Stop(context.Background(), tenantA, session); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	old.Publication([]byte(`"after-stop"`))
	if err := plane.Resume(context.Background(), tenantA, session, 0); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	fresh := links.sink(1)
	old.Publication([]byte(`"from-the-replaced-tail"`))
	fresh.Publication([]byte(`"from-the-live-tail"`))

	deadline := time.Now().Add(waitFor)
	for len(relay.got()) < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	if got := relay.got(); len(got) != 1 || got[0] != `"from-the-live-tail"` {
		t.Fatalf("the Relay received %v, want only the live tail's record", got)
	}
}
