package hostlink_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/hostlink"
)

// parkingDialer parks every dial to one address until released, and dials
// every other address at once.
type parkingDialer struct {
	park    sessionwire.InternalEndpoint
	release chan struct{}
	entered chan struct{}
	once    sync.Once
	dials   atomic.Int32
	closes  atomic.Int32
}

func (d *parkingDialer) Dial(ctx context.Context, tgt hostlink.Target, _ hostlink.Observer) (hostlink.Link, error) {
	d.dials.Add(1)
	if tgt.Endpoint == d.park {
		d.once.Do(func() { close(d.entered) })
		select {
		case <-d.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return &closeCounting{negotiatingLink: &negotiatingLink{opLink: &opLink{host: tgt.Host}}, closes: &d.closes}, nil
}

type closeCounting struct {
	*negotiatingLink
	closes *atomic.Int32
}

func (c *closeCounting) Close(context.Context) error { c.closes.Add(1); return nil }

// TestACapabilityDialDoesNotStallThePool is spec gate N2: the capability read
// is reachable from a public HTTP request, and it may have to open a link. Its
// dial is made with the pool's lock RELEASED, so an owner that black-holes the
// dial cannot stall a bind to any other (Host, tenant) behind it.
func TestACapabilityDialDoesNotStallThePool(t *testing.T) {
	t.Parallel()
	dialer := &parkingDialer{park: base1 + "/hostlink/tenant-b", release: make(chan struct{}), entered: make(chan struct{})}
	pool, err := hostlink.NewPool(hostlink.Config{Dialer: dialer})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pool.Close(context.Background()) })
	defer func() {
		select {
		case <-dialer.release:
		default:
			close(dialer.release)
		}
	}()

	asked := make(chan error, 1)
	go func() {
		_, err := pool.AcceptsGateResponses(context.Background(), target(hostOne, base1), tenantB)
		asked <- err
	}()
	<-dialer.entered

	bound := make(chan error, 1)
	go func() { bound <- pool.Bind(context.Background(), target(hostOne, base1), tenantBind(tenant, hostOne, "s-1")) }()
	select {
	case err := <-bound:
		if err != nil {
			t.Fatalf("Bind: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a bind to another tenant waited behind a parked capability dial: the pool's lock is held across the dial")
	}
	close(dialer.release)
	if err := <-asked; err != nil {
		t.Fatalf("the capability read: %v", err)
	}
	if got := pool.TenantLinks(hostOne); got != 2 {
		t.Fatalf("TenantLinks = %d, want the bind's and the capability read's", got)
	}
}

// TestTwoCapabilityDialsRacingForOnePairKeepOneLink: with the lock released
// across the dial, two first questions for the same (Host, tenant) may both
// dial. One link is kept and the surplus one is closed -- never two links for
// one pair, and never a leaked connection.
func TestTwoCapabilityDialsRacingForOnePairKeepOneLink(t *testing.T) {
	t.Parallel()
	dialer := &parkingDialer{park: base1 + "/hostlink/tenant-a", release: make(chan struct{}), entered: make(chan struct{})}
	pool, err := hostlink.NewPool(hostlink.Config{Dialer: dialer})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pool.Close(context.Background()) })

	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := pool.AcceptsGateResponses(context.Background(), target(hostOne, base1), tenant); err != nil {
				t.Errorf("capability read: %v", err)
			}
		}()
	}
	<-dialer.entered
	deadline := time.Now().Add(2 * time.Second)
	for dialer.dials.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	close(dialer.release)
	wg.Wait()
	if got := pool.Links(); got != 1 {
		t.Fatalf("Links = %d after two racing first questions, want 1", got)
	}
	if dials, closes := dialer.dials.Load(), dialer.closes.Load(); dials-closes != 1 {
		t.Fatalf("%d dials and %d closes: a surplus link was leaked", dials, closes)
	}
}
