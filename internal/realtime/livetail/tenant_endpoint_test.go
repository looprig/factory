package livetail_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/hostlink"
	"github.com/looprig/factory/internal/realtime/livetail"
)

type countingDialer struct {
	mu    sync.Mutex
	dials int
}

func (d *countingDialer) Dial(context.Context, hostlink.Target, hostlink.Observer) (hostlink.Link, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.dials++
	return nil, errors.New("this case must dial nothing")
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestABindToAnOwnerWhoseBaseCannotAddressTheTenantIsLoggedAndDialsNothing is
// Gap 1 on the viewing path: the owner's advertised base cannot carry this
// session's tenant, so the bind fails (the session stays unbound and the next
// poll tries again) WITHOUT a dial, and a WARN names the Host, the tenant and
// Core's code -- the only place an operator would learn why a watched session
// is silent.
func TestABindToAnOwnerWhoseBaseCannotAddressTheTenantIsLoggedAndDialsNothing(t *testing.T) {
	t.Parallel()

	dialer := &countingDialer{}
	pool, err := hostlink.NewPool(hostlink.Config{Dialer: dialer})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pool.Close(context.Background()) })
	logs := &lockedBuffer{}
	plane, err := livetail.New(livetail.Config{
		Links: pool, Viewers: func() livetail.Viewers { return nil }, MailboxLimit: 8, EventTimeout: time.Second,
		Logger: slog.New(slog.NewJSONHandler(logs, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = plane.Close(context.Background()) })

	req := sessionwire.HostLinkBindRequest{
		Version: sessionwire.CurrentWireVersion, TenantID: tenantA, SessionID: session, HostID: "host-1",
		HostGeneration: 1, LeaseEpoch: 1, RuntimeCompatibilityID: "runtime-1", IdempotencyKey: "bind-1",
	}
	err = plane.Bind(context.Background(), "ws://host-1.internal/pods/host-1", req)
	if !errors.Is(err, hostlink.ErrNoTenantEndpoint) {
		t.Fatalf("Bind = %v, want ErrNoTenantEndpoint", err)
	}
	dialer.mu.Lock()
	dials := dialer.dials
	dialer.mu.Unlock()
	if dials != 0 {
		t.Fatalf("the pool dialled %d times for an address it could not derive", dials)
	}
	for _, want := range []string{`"level":"WARN"`, `"host_id":"host-1"`, `"tenant_id":"` + string(tenantA) + `"`, `"code":"base_not_bare"`} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("no WARN carrying %s: %s", want, logs.String())
		}
	}
	if _, held := pool.RouteFor(tenantA, session); held {
		t.Fatal("a refused bind left a route")
	}
}
