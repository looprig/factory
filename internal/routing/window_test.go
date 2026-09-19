package routing

// The window Relay.repair and Relay.Resync open by releasing r.mu across their
// I/O (d23cb6e). These are the v0.4.0 regate's probes, committed (N2): each
// guard in that window had no reader -- X3b (the repair's tip read taken back
// under the lock), X6 (a Close or Forget inside the window not noticed on
// re-lock) and X7 (Resync not noticing a Close on re-lock) all survived the
// suite.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
)

// gatedTips answers tip read n with tips[n], after closing in[n] and waiting
// for gates[n], so a case can hold a read open at a known point.
type gatedTips struct {
	mu    sync.Mutex
	calls int
	gates map[int]chan struct{}
	tips  map[int]uint64
	in    map[int]chan struct{}
}

func (g *gatedTips) ReadPublicJournal(context.Context, sessionstore.ReadPublicJournalRequest) (sessionwire.JournalPage, error) {
	g.mu.Lock()
	n := g.calls
	g.calls++
	gate, tip, in := g.gates[n], g.tips[n], g.in[n]
	g.mu.Unlock()
	if in != nil {
		close(in)
	}
	if gate != nil {
		<-gate
	}
	return sessionwire.JournalPage{CapturedTip: tip, CoveredThrough: tip}, nil
}

type countingRebinder struct {
	mu    sync.Mutex
	calls int
}

func (c *countingRebinder) Rebind(context.Context, sessionwire.TenantID, sessionwire.SessionID) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	return nil
}

func (c *countingRebinder) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func waitClosed(t *testing.T, what string, ch chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func newWindowRelay(t *testing.T, tips *gatedTips) (*relayFixture, *countingRebinder) {
	t.Helper()
	rebinder := &countingRebinder{}
	tail := &recordingTail{}
	pub := newPublisher()
	relay, err := NewRelay(tips, rebinder, tail, pub, testRepairLimits)
	if err != nil {
		t.Fatal(err)
	}
	return &relayFixture{t: t, relay: relay, tail: tail, pub: pub}, rebinder
}

// TestARepairEndsWithoutARebindWhenTheRelayIsClosedOrTheSessionForgottenInsideItsWindow
// kills X6: a repair parked in its unlocked tip read must notice, on re-lock,
// that the relay was closed or the session forgotten, and rebind nothing --
// a rebind for a session nobody holds would open a tail nothing drains.
func TestARepairEndsWithoutARebindWhenTheRelayIsClosedOrTheSessionForgottenInsideItsWindow(t *testing.T) {
	for name, interrupt := range map[string]func(*Relay){
		"closed":    func(r *Relay) { r.Close() },
		"forgotten": func(r *Relay) { r.Forget(bindTenant, bindSession) },
	} {
		t.Run(name, func(t *testing.T) {
			gate, in := make(chan struct{}), make(chan struct{})
			f, rebinder := newWindowRelay(t, &gatedTips{gates: map[int]chan struct{}{0: gate}, tips: map[int]uint64{0: 60}, in: map[int]chan struct{}{0: in}})
			f.open(bindSession, linkA)
			f.mustFeed(bindSession, 50)
			done := make(chan error, 1)
			go func() { done <- f.relay.HostLinkClosed(context.Background(), bindTenant, bindSession) }()
			waitClosed(t, "the repair to reach its tip read", in)
			interrupt(f.relay)
			close(gate)
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("the interrupted repair never returned")
			}
			if got := rebinder.count(); got != 0 {
				t.Fatalf("a repair %s inside its window rebound %d times", name, got)
			}
			for _, record := range f.pub.to(linkA)[1:] {
				t.Fatalf("a repair %s inside its window published %s", name, record)
			}
		})
	}
}

// TestAResyncInterruptedByCloseReportsTheRelayClosed kills X7: a Close landing
// during Resync's unlocked tip read is answered ErrRelayClosed -- not
// ErrNoHostBinding, which tells the plane a different story about why nothing
// was reset.
func TestAResyncInterruptedByCloseReportsTheRelayClosed(t *testing.T) {
	gate, in := make(chan struct{}), make(chan struct{})
	f, _ := newWindowRelay(t, &gatedTips{gates: map[int]chan struct{}{0: gate}, tips: map[int]uint64{0: 60}, in: map[int]chan struct{}{0: in}})
	f.open(bindSession, linkA)
	f.mustFeed(bindSession, 50)
	done := make(chan error, 1)
	go func() { done <- f.relay.Resync(context.Background(), bindTenant, bindSession) }()
	waitClosed(t, "Resync to reach its tip read", in)
	f.relay.Close()
	close(gate)
	var err error
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Resync never returned")
	}
	if !errors.Is(err, ErrRelayClosed) {
		t.Fatalf("Resync across a Close = %v, want ErrRelayClosed", err)
	}
}

// TestAnotherSessionIsDeliveredWhileARepairIsParkedInItsTipRead kills X3b: the
// repair's tip read is a store call, and taken under the relay's one mutex it
// stalls every other session's delivery for its duration. The bind half of
// this property was already read (TestOneSessionsRepairDoesNotStallAnother
// SessionsDelivery); this is the tip-read half. Bounded, so the mutant fails
// with a message rather than hanging the package.
func TestAnotherSessionIsDeliveredWhileARepairIsParkedInItsTipRead(t *testing.T) {
	gate, in := make(chan struct{}), make(chan struct{})
	f, _ := newWindowRelay(t, &gatedTips{gates: map[int]chan struct{}{0: gate}, tips: map[int]uint64{0: 60}, in: map[int]chan struct{}{0: in}})
	defer func() {
		select {
		case <-gate:
		default:
			close(gate)
		}
	}()
	f.open(bindSession, linkA)
	f.open(otherSession, linkB)
	f.mustFeed(bindSession, 50)
	f.mustFeed(otherSession, 80)
	done := make(chan error, 1)
	go func() { done <- f.relay.HostLinkClosed(context.Background(), bindTenant, bindSession) }()
	waitClosed(t, "the repair to reach its tip read", in)

	delivered := make(chan error, 1)
	go func() { delivered <- f.feed(otherSession, 81) }()
	select {
	case err := <-delivered:
		if err != nil {
			t.Fatalf("the other session's frame: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("another session's frame waited behind this session's tip read: the relay's lock is held across the store call")
	}
	if got := f.pub.to(linkB); len(got) != 2 || got[1] != string(enduringRecord(t, otherSession, 81)) {
		t.Fatalf("the other session's stream = %v, want E80 E81 while the repair is parked", got)
	}
	close(gate)
	if err := <-done; err != nil {
		t.Fatalf("repair: %v", err)
	}
}
