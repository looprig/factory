package routing

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// The seams Gap 3 added to this package: Relay.Resync and Relay.Forget, and
// the demand plane's Watcher.

// TestResyncResetsEveryBindingFromOneTipAndTouchesNoTail: a restarted tail owes
// its viewers a reset, built exactly as a repair's is -- ONE tip for every
// binding, each binding's own last contiguous sequence -- and nothing else. A
// Resync that stopped, rebound or resumed would tear down the tail that just
// went live.
func TestResyncResetsEveryBindingFromOneTipAndTouchesNoTail(t *testing.T) {
	const tip = 700
	f := newRelayFixture(t, testRepairLimits, tip)
	f.open(bindSession, linkA, linkB)
	f.mustFeed(bindSession, 21, 22)
	f.pub.block(linkB, true)
	f.mustFeed(bindSession, 23)

	if err := f.relay.Resync(context.Background(), bindTenant, bindSession); err != nil {
		t.Fatalf("Resync: %v", err)
	}
	if got := len(f.tips.reads()); got != 1 {
		t.Fatalf("%d tip reads, want exactly 1", got)
	}
	f.pub.block(linkB, false)
	if err := f.relay.Pump(context.Background(), bindTenant, bindSession); err != nil {
		t.Fatalf("Pump: %v", err)
	}
	for link, wantLast := range map[LinkID]uint64{linkA: 23, linkB: 22} {
		resets := resetsIn(t, f.pub.to(link))
		if len(resets) != 1 || resets[0].LastContiguous != wantLast || resets[0].JournalTip != tip {
			t.Errorf("%s resets = %+v, want exactly one (%d, %d)", link, resets, wantLast, tip)
		}
	}
	// B's queued 23 was DISCARDED by the reset, as a repair discards it: the
	// reset tells B to read it from the journal.
	for _, encoded := range f.pub.to(linkB) {
		if strings.Contains(encoded, `"journal_seq":23`) {
			t.Errorf("%s received 23 after its reset replaced it", linkB)
		}
	}
	if len(f.tail.order) != 0 || len(f.rebinder.sessions) != 0 {
		t.Fatalf("Resync touched the tail: order %v, rebinds %v", f.tail.order, f.rebinder.sessions)
	}
	// Streaming continues after it.
	f.mustFeed(bindSession, 24)
	if got := f.pub.to(linkA); !strings.Contains(got[len(got)-1], `"journal_seq":24`) {
		t.Fatalf("a record after the reset did not reach %s: %v", linkA, got)
	}
}

// TestAResyncWhoseTipCannotBeReadClosesEveryBinding is the fail-closed arm, the
// same as a repair's: a reset naming no tip is no instruction.
func TestAResyncWhoseTipCannotBeReadClosesEveryBinding(t *testing.T) {
	f := newRelayFixture(t, testRepairLimits)
	f.open(bindSession, linkA, linkB)
	f.open(otherSession, linkC)
	f.tips.err = errors.New("store unavailable")

	err := f.relay.Resync(context.Background(), bindTenant, bindSession)
	if err == nil || !strings.Contains(err.Error(), "store unavailable") {
		t.Fatalf("Resync = %v, want the tip read's failure", err)
	}
	closed := f.pub.closedLinks()
	if len(closed) != 2 || closed[0] != linkA || closed[1] != linkB {
		t.Fatalf("closed links = %v, want exactly [%s %s]", closed, linkA, linkB)
	}
	if got := f.relay.Bindings(bindTenant, otherSession); got != 1 {
		t.Fatalf("another session lost its binding: %d", got)
	}
}

// TestForgetDropsOneSessionAndResyncRefusesWhatIsNotHeld.
func TestForgetDropsOneSessionAndResyncRefusesWhatIsNotHeld(t *testing.T) {
	f := newRelayFixture(t, testRepairLimits)
	f.open(bindSession, linkA)
	f.open(otherSession, linkB)

	f.relay.Forget(bindTenant, bindSession)
	if err := f.feed(bindSession, 1); !errors.Is(err, ErrNoHostBinding) {
		t.Fatalf("a record for a forgotten session = %v, want ErrNoHostBinding", err)
	}
	if err := f.relay.Resync(context.Background(), bindTenant, bindSession); !errors.Is(err, ErrNoHostBinding) {
		t.Fatalf("Resync of a forgotten session = %v, want ErrNoHostBinding", err)
	}
	f.mustFeed(otherSession, 5)
	if got := len(f.pub.to(linkB)); got != 1 {
		t.Fatalf("the other session delivered %d records after a Forget, want 1", got)
	}
	// Forgetting again, or a session never held, is harmless.
	f.relay.Forget(bindTenant, bindSession)
	f.relay.Close()
	if err := f.relay.Resync(context.Background(), bindTenant, otherSession); !errors.Is(err, ErrRelayClosed) {
		t.Fatalf("Resync on a closed relay = %v, want ErrRelayClosed", err)
	}
}

// recordingWatcher records every Watcher call, and how many binds the binder
// had seen at that instant -- which is what places each call before or after
// the bind it brackets.
type recordingWatcher struct {
	mu     sync.Mutex
	binder *recordingBinder
	calls  []string
}

func (w *recordingWatcher) note(kind string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls = append(w.calls, fmt.Sprintf("%s@%d", kind, w.binder.bindCount()))
}

func (w *recordingWatcher) Watching(_ sessionwire.TenantID, session sessionwire.SessionID) {
	w.note("watching:" + string(session))
}
func (w *recordingWatcher) Served(_ sessionwire.TenantID, session sessionwire.SessionID) {
	w.note("served:" + string(session))
}
func (w *recordingWatcher) Unwatched(_ sessionwire.TenantID, session sessionwire.SessionID) {
	w.note("unwatched:" + string(session))
}

func (w *recordingWatcher) log() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return strings.Join(w.calls, ",")
}

// TestTheWatcherBracketsTheFirstServeAndHearsTheLastRelease: Watching before
// the first subscriber's bind, Served after it, nothing for a second
// subscriber, and Unwatched when the last one goes -- or the plane closes.
func TestTheWatcherBracketsTheFirstServeAndHearsTheLastRelease(t *testing.T) {
	t.Parallel()

	f := newDemandFixture(t, testDemandLimits, observation("host-a", 1, 1))
	watcher := &recordingWatcher{binder: f.binder}
	f.demand.SetWatcher(watcher)

	f.acquire(t, bindSession)
	f.acquire(t, bindSession)
	if got, want := watcher.log(), "watching:"+string(bindSession)+"@0,served:"+string(bindSession)+"@1"; got != want {
		t.Fatalf("watcher after two subscribers = %s, want %s", got, want)
	}
	f.release(t, bindSession)
	if got := watcher.log(); strings.Contains(got, "unwatched") {
		t.Fatalf("a release that left a subscriber was reported unwatched: %s", got)
	}
	f.release(t, bindSession)
	if got := watcher.log(); !strings.HasSuffix(got, ",unwatched:"+string(bindSession)+"@1") {
		t.Fatalf("the last release was not reported: %s", got)
	}

	f.acquire(t, otherSession)
	if err := f.demand.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := watcher.log(); !strings.HasSuffix(got, "unwatched:"+string(otherSession)+"@1") {
		t.Fatalf("Close did not report the session it tore down: %s", got)
	}
}
