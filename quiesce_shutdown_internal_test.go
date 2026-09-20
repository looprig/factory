package factory

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

type shutdownProbe struct {
	mu      sync.Mutex
	calls   int
	entered chan struct{}
	release chan struct{}
	err     error
}

func (*shutdownProbe) ServeHTTP(http.ResponseWriter, *http.Request) {}
func (*shutdownProbe) PublishSession(sessionwire.TenantID, sessionwire.SessionID, []byte) error {
	return nil
}
func (*shutdownProbe) CloseSession(sessionwire.TenantID, sessionwire.SessionID) {}
func (p *shutdownProbe) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	p.calls++
	if p.entered != nil {
		close(p.entered)
	}
	p.mu.Unlock()
	if p.release != nil {
		select {
		case <-p.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return p.err
}
func (p *shutdownProbe) count() int { p.mu.Lock(); defer p.mu.Unlock(); return p.calls }

func TestFailedRealtimeShutdownRetainsHandleAndReplaysResult(t *testing.T) {
	failure := errors.New("demand release failed")
	p := &shutdownProbe{err: failure}
	c := &components{realtime: p}
	if err := c.stopRealtime(context.Background()); !errors.Is(err, failure) {
		t.Fatalf("first stop = %v", err)
	}
	if c.realtime != p {
		t.Fatal("failed shutdown lost ClientLink handle")
	}
	if err := c.stopRealtime(context.Background()); !errors.Is(err, failure) {
		t.Fatalf("retry = %v", err)
	}
	if p.count() != 1 {
		t.Fatalf("Shutdown called %d times, want once", p.count())
	}
}

func TestBlockedRealtimeShutdownSharesOneAttemptAndBoundsWaiters(t *testing.T) {
	p := &shutdownProbe{entered: make(chan struct{}), release: make(chan struct{})}
	c := &components{realtime: p}
	first := make(chan error, 1)
	go func() { first <- c.stopRealtime(context.Background()) }()
	<-p.entered
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.stopRealtime(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled waiter = %v", err)
	}
	close(p.release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := c.stopRealtime(context.Background()); err != nil {
		t.Fatal(err)
	}
	if p.count() != 1 || c.realtime != nil {
		t.Fatalf("calls=%d, retained=%v", p.count(), c.realtime != nil)
	}
}

func TestQuiesceShutdownHasOwnBoundWhenCallerCancels(t *testing.T) {
	s := raceServer(t)
	s.cfg.client.CommandTimeout = 15 * time.Millisecond
	s.cfg.client.DemandTimeout = 15 * time.Millisecond
	p := &shutdownProbe{entered: make(chan struct{}), release: make(chan struct{})}
	s.components.realtime = p
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Quiesce(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled caller = %v", err)
	}
	if err := s.Quiesce(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("owned shutdown = %v, want bounded deadline", err)
	}
	if p.count() != 1 || s.components.realtime != p {
		t.Fatalf("calls=%d, retained=%v", p.count(), s.components.realtime != nil)
	}
	// The test's synthetic failure is retained by the server; Stop still has to
	// close all remaining planes and report it.
	if err := s.Stop(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop = %v", err)
	}
	s.mu.Lock()
	s.quiesceErr = nil
	s.mu.Unlock()
}
