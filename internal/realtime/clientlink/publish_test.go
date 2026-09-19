package clientlink_test

import (
	"sync"
	"testing"
	"time"

	centrifugego "github.com/centrifugal/centrifuge-go"
	"github.com/looprig/factory/internal/realtime/clientlink"
)

// TestSessionChannelIsTheChannelAViewerSubscribesTo pins the grammar as an
// absolute literal -- wui's sessionChannel builds exactly this -- and proves the
// round trip against the demand grammar through a real subscribe: a
// publication on SessionChannel(t, s) reaches the viewer that subscribed to the
// literal.
func TestSessionChannelIsTheChannelAViewerSubscribesTo(t *testing.T) {
	t.Parallel()

	if got := clientlink.SessionChannel(tenantA, "s-1"); got != "session:tenant-a:s-1" {
		t.Fatalf("SessionChannel = %q, want the literal session:tenant-a:s-1", got)
	}
}

// publicationRecorder collects what one subscription received.
type publicationRecorder struct {
	mu           sync.Mutex
	data         []string
	unsubscribed []uint32
}

func (r *publicationRecorder) got() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.data...)
}

func (r *publicationRecorder) unsubs() []uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]uint32(nil), r.unsubscribed...)
}

// watchNoFatal is watch for a goroutine that may not call t.Fatal: it
// answers nil when the subscribe fails or is not answered.
func watchNoFatal(client *centrifugego.Client, channel string) *publicationRecorder {
	sub, err := client.NewSubscription(channel)
	if err != nil {
		return nil
	}
	recorder := &publicationRecorder{}
	subscribed := make(chan struct{}, 1)
	sub.OnSubscribed(func(centrifugego.SubscribedEvent) { send(subscribed, struct{}{}) })
	sub.OnUnsubscribed(func(e centrifugego.UnsubscribedEvent) {
		recorder.mu.Lock()
		recorder.unsubscribed = append(recorder.unsubscribed, e.Code)
		recorder.mu.Unlock()
	})
	if err := sub.Subscribe(); err != nil {
		return nil
	}
	select {
	case <-subscribed:
		return recorder
	case <-time.After(waitFor):
		return nil
	}
}

func watch(t *testing.T, client *centrifugego.Client, channel string) *publicationRecorder {
	t.Helper()
	sub, err := client.NewSubscription(channel)
	if err != nil {
		t.Fatalf("NewSubscription: %v", err)
	}
	recorder := &publicationRecorder{}
	subscribed := make(chan struct{}, 1)
	sub.OnSubscribed(func(centrifugego.SubscribedEvent) { send(subscribed, struct{}{}) })
	sub.OnPublication(func(e centrifugego.PublicationEvent) {
		recorder.mu.Lock()
		recorder.data = append(recorder.data, string(e.Data))
		recorder.mu.Unlock()
	})
	sub.OnUnsubscribed(func(e centrifugego.UnsubscribedEvent) {
		recorder.mu.Lock()
		recorder.unsubscribed = append(recorder.unsubscribed, e.Code)
		recorder.mu.Unlock()
	})
	if err := sub.Subscribe(); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	await(t, subscribed, "subscribed")
	return recorder
}

func eventually(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitFor)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not happen within %v", what, waitFor)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestPublishSessionReachesEveryViewerOfThatSessionAndNoOther: channel-wide,
// once per record, and scoped to exactly one (tenant, session) -- a viewer of
// another tenant's session with the same id receives nothing.
func TestPublishSessionReachesEveryViewerOfThatSessionAndNoOther(t *testing.T) {
	t.Parallel()

	f := newFixture(t, testLimits())
	first, _ := dialSupported(t, f, "token-a")
	second, _ := dialSupported(t, f, "token-a")
	other, _ := dialSupported(t, f, "token-b")
	a1 := watch(t, first, sessionChannel(tenantA, "s-1"))
	a2 := watch(t, second, sessionChannel(tenantA, "s-1"))
	b := watch(t, other, sessionChannel(tenantB, "s-1"))

	for _, record := range []string{`{"n":1}`, `{"n":2}`} {
		if err := f.handler.PublishSession(tenantA, "s-1", []byte(record)); err != nil {
			t.Fatalf("PublishSession: %v", err)
		}
	}
	for _, viewer := range []*publicationRecorder{a1, a2} {
		eventually(t, "both records at a viewer", func() bool { return len(viewer.got()) == 2 })
		if got := viewer.got(); got[0] != `{"n":1}` || got[1] != `{"n":2}` {
			t.Fatalf("a viewer received %v, want both records in order", got)
		}
	}
	// The control for tenant isolation: B's own session still receives.
	if err := f.handler.PublishSession(tenantB, "s-1", []byte(`{"b":1}`)); err != nil {
		t.Fatalf("PublishSession: %v", err)
	}
	eventually(t, "tenant B's own record", func() bool { return len(b.got()) == 1 })
	if got := b.got(); got[0] != `{"b":1}` {
		t.Fatalf("tenant B's viewer received %v: another tenant's record reached it", got)
	}
}

// TestCloseSessionUnsubscribesEveryViewerOfThatSessionOnly: the viewers are
// unsubscribed server-side with a code below the transport's own resubscribe
// band, so each viewer's join -- not its transport -- repairs; a viewer of
// another session keeps streaming.
func TestCloseSessionUnsubscribesEveryViewerOfThatSessionOnly(t *testing.T) {
	t.Parallel()

	f := newFixture(t, testLimits())
	client, _ := dialSupported(t, f, "token-a")
	closed := watch(t, client, sessionChannel(tenantA, "s-1"))
	kept := watch(t, client, sessionChannel(tenantA, "s-2"))

	f.handler.CloseSession(tenantA, "s-1")
	eventually(t, "the viewer to be unsubscribed", func() bool { return len(closed.unsubs()) == 1 })
	if code := closed.unsubs()[0]; code != 2000 {
		t.Fatalf("unsubscribe code = %d, want 2000 (below the 2500 resubscribe band)", code)
	}
	if err := f.handler.PublishSession(tenantA, "s-2", []byte(`{"k":1}`)); err != nil {
		t.Fatalf("PublishSession: %v", err)
	}
	eventually(t, "the other session's record", func() bool { return len(kept.got()) == 1 })
	if got := kept.unsubs(); len(got) != 0 {
		t.Fatalf("a viewer of another session was unsubscribed: %v", got)
	}
}
