package hostlink_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/hostlink"
)

// Gap 1 (v0.5.0): one pooled Host serves several tenants at once, each over its
// own HostLink at Core's derived address. These cases are written against the
// two halves of that sentence -- ONE LINK PER (Host, tenant), and NOTHING OF ONE
// TENANT'S EVER CROSSES ANOTHER TENANT'S LINK (R-1) -- and every assertion is
// on the link an operation reached, never only on a count: a pool that dialled
// one link per tenant and then sent tenant-b's delivery over tenant-a's would
// satisfy every count below.

const (
	tenantB sessionwire.TenantID = "tenant-b"
	// base1 is a BARE base, as a host v0.3.0 advertises. Absolute literals, and
	// the derived addresses below are spelled out rather than derived with the
	// function under test.
	base1 sessionwire.InternalEndpoint = "ws://10.0.4.7:9000"
)

func TestOneHostHoldsOneLinkPerTenantAtTheDerivedAddress(t *testing.T) {
	t.Parallel()
	dialer := newTenantDialer()
	pool := newPool(t, dialer, hostlink.Limits{})

	mustBind(t, pool, target(hostOne, base1), tenantBind(tenant, hostOne, "s-1"))
	mustBind(t, pool, target(hostOne, base1), tenantBind(tenant, hostOne, "s-2"))
	mustBind(t, pool, target(hostOne, base1), tenantBind(tenantB, hostOne, "s-1"))

	want := []hostlink.Target{
		{Host: hostOne, Endpoint: "ws://10.0.4.7:9000/hostlink/tenant-a"},
		{Host: hostOne, Endpoint: "ws://10.0.4.7:9000/hostlink/tenant-b"},
	}
	if got := dialer.targets(); !reflect.DeepEqual(got, want) {
		t.Fatalf("dialled %v, want each tenant's derived address exactly once: %v", got, want)
	}
	if got := pool.Links(); got != 2 {
		t.Errorf("Links() = %d, want 2 (one per tenant)", got)
	}
	if got := pool.TenantLinks(hostOne); got != 2 {
		t.Errorf("TenantLinks(%s) = %d, want 2", hostOne, got)
	}
	if got := pool.TenantBindings(hostOne, tenant); got != 2 {
		t.Errorf("TenantBindings(%s, %s) = %d, want 2", hostOne, tenant, got)
	}
	if got := pool.TenantBindings(hostOne, tenantB); got != 1 {
		t.Errorf("TenantBindings(%s, %s) = %d, want 1", hostOne, tenantB, got)
	}
	if got := pool.Bindings(hostOne); got != 3 {
		t.Errorf("Bindings(%s) = %d, want 3 over both tenants", hostOne, got)
	}
	// And each bind went over its OWN tenant's connection.
	if got := dialer.at("ws://10.0.4.7:9000/hostlink/tenant-a").bindTenants(); !reflect.DeepEqual(got, []sessionwire.TenantID{tenant, tenant}) {
		t.Errorf("tenant-a's link carried binds for %v", got)
	}
	if got := dialer.at("ws://10.0.4.7:9000/hostlink/tenant-b").bindTenants(); !reflect.DeepEqual(got, []sessionwire.TenantID{tenantB}) {
		t.Errorf("tenant-b's link carried binds for %v", got)
	}
}

// TestEveryOperationTravelsOverItsOwnTenantsLink is R-1 at the pool: the same
// session id under two tenants, bound to the same Host, and every per-session
// operation -- delivery, subscribe, unsubscribe, unbind, attach -- lands on the
// link of the tenant it names and on no other.
func TestEveryOperationTravelsOverItsOwnTenantsLink(t *testing.T) {
	t.Parallel()
	dialer := newTenantDialer()
	pool := newPool(t, dialer, hostlink.Limits{})
	ctx := context.Background()

	mustBind(t, pool, target(hostOne, base1), tenantBind(tenant, hostOne, "s-1"))
	mustBind(t, pool, target(hostOne, base1), tenantBind(tenantB, hostOne, "s-1"))
	a := dialer.at("ws://10.0.4.7:9000/hostlink/tenant-a")
	b := dialer.at("ws://10.0.4.7:9000/hostlink/tenant-b")

	if err := pool.DeliverCommand(ctx, tenantB, "s-1", sessionwire.HostLinkCommandDelivery{CommandID: "c-b"}); err != nil {
		t.Fatalf("DeliverCommand(tenant-b): %v", err)
	}
	if err := pool.Subscribe(ctx, tenantB, "s-1", nopSink{}); err != nil {
		t.Fatalf("Subscribe(tenant-b): %v", err)
	}
	pool.Unsubscribe(tenantB, "s-1")
	if _, err := pool.Attach(ctx, target(hostOne, base1), tenantAttach(tenantB, hostOne, "s-2")); err != nil {
		t.Fatalf("Attach(tenant-b): %v", err)
	}
	if err := pool.Unbind(ctx, tenantUnbind(tenantB, hostOne, "s-1")); err != nil {
		t.Fatalf("Unbind(tenant-b): %v", err)
	}

	if got, want := b.ops(), []string{"bind tenant-b/s-1", "deliver tenant-b/s-1", "subscribe tenant-b/s-1", "unsubscribe tenant-b/s-1", "attach tenant-b/s-2", "unbind tenant-b/s-1"}; !reflect.DeepEqual(got, want) {
		t.Errorf("tenant-b's link carried %v, want %v", got, want)
	}
	if got, want := a.ops(), []string{"bind tenant-a/s-1"}; !reflect.DeepEqual(got, want) {
		t.Errorf("tenant-a's link carried %v, want only its own bind: another tenant's operation crossed it", got)
	}
	// tenant-a's route is untouched by tenant-b's unbind of the same session id.
	if host, ok := pool.RouteFor(tenant, "s-1"); !ok || host != hostOne {
		t.Errorf("RouteFor(tenant-a, s-1) = %q, %v after tenant-b unbound its s-1", host, ok)
	}
}

// TestAnAttachForATenantWithNoLinkDialsThatTenantsAddress: an attach opens the
// link placement will bind over, so it must be the tenant's own, not whichever
// link the pool happens to hold for the Host.
func TestAnAttachForATenantWithNoLinkDialsThatTenantsAddress(t *testing.T) {
	t.Parallel()
	dialer := newTenantDialer()
	pool := newPool(t, dialer, hostlink.Limits{})

	mustBind(t, pool, target(hostOne, base1), tenantBind(tenant, hostOne, "s-1"))
	if _, err := pool.Attach(context.Background(), target(hostOne, base1), tenantAttach(tenantB, hostOne, "s-9")); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if got := dialer.at("ws://10.0.4.7:9000/hostlink/tenant-b").ops(); !reflect.DeepEqual(got, []string{"attach tenant-b/s-9"}) {
		t.Errorf("tenant-b's link carried %v, want the attach", got)
	}
	if got := dialer.at("ws://10.0.4.7:9000/hostlink/tenant-a").ops(); !reflect.DeepEqual(got, []string{"bind tenant-a/s-1"}) {
		t.Errorf("tenant-a's link carried %v: the attach went over another tenant's connection", got)
	}
}

// TestATenantTheBaseCannotCarryIsRefusedBeforeAnyDialAndOnlyForThatTenant is
// the per-tenant refusal: Core's typed code reaches the caller through both
// errors.Is and errors.As, nothing is dialled for it, and the same Host still
// serves a tenant that fits.
func TestATenantTheBaseCannotCarryIsRefusedBeforeAnyDialAndOnlyForThatTenant(t *testing.T) {
	t.Parallel()
	// A base of 240 bytes leaves room for a tenant of at most
	// 256 - 240 - len("/hostlink/") = 6 escaped bytes.
	long := sessionwire.InternalEndpoint("ws://" + strings.Repeat("h", 235))
	if len(long) != 240 {
		t.Fatalf("fixture base is %d bytes", len(long))
	}
	cases := []struct {
		name     string
		base     sessionwire.InternalEndpoint
		tenantID sessionwire.TenantID
		code     sessionwire.HostLinkEndpointCode
	}{
		{"a tenant too long for the base", long, "tenant-a", sessionwire.HostLinkEndpointCodeTooLong},
		{"a base that already names a tenant", "ws://10.0.4.7:9000/hostlink/tenant-a", "tenant-a", sessionwire.HostLinkEndpointCodeBaseNamesTenant},
		{"a base carrying a path", "ws://10.0.4.7:9000/pods/host-7", "tenant-a", sessionwire.HostLinkEndpointCodeBaseNotBare},
		{"a tenant Core will not route", base1, ".", sessionwire.HostLinkEndpointCodeUnroutableTenant},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dialer := newTenantDialer()
			pool := newPool(t, dialer, hostlink.Limits{})

			err := pool.Bind(context.Background(), target(hostOne, tc.base), tenantBind(tc.tenantID, hostOne, "s-1"))
			assertEndpointRefusal(t, err, tc.tenantID, tc.code)
			_, err = pool.Attach(context.Background(), target(hostOne, tc.base), tenantAttach(tc.tenantID, hostOne, "s-1"))
			assertEndpointRefusal(t, err, tc.tenantID, tc.code)
			if got := dialer.dials(); got != 0 {
				t.Errorf("the pool dialled %d times for a tenant it could not address, want 0", got)
			}
			if _, ok := pool.RouteFor(tc.tenantID, "s-1"); ok {
				t.Error("a refused bind left a route")
			}
		})
	}

	// The positive control, on the SAME base as the too-long row: a tenant that
	// fits is served, so the refusal is per tenant and not per Host.
	dialer := newTenantDialer()
	pool := newPool(t, dialer, hostlink.Limits{})
	if err := pool.Bind(context.Background(), target(hostOne, long), tenantBind("t-6", hostOne, "s-1")); err != nil {
		t.Fatalf("a six-byte tenant on the same base was refused: %v", err)
	}
	if got := dialer.targets(); len(got) != 1 || got[0].Endpoint != long+"/hostlink/t-6" {
		t.Errorf("dialled %v, want %s/hostlink/t-6", got, long)
	}
}

func assertEndpointRefusal(t *testing.T, err error, tenantID sessionwire.TenantID, code sessionwire.HostLinkEndpointCode) {
	t.Helper()
	if !errors.Is(err, hostlink.ErrNoTenantEndpoint) {
		t.Fatalf("err = %v, want ErrNoTenantEndpoint", err)
	}
	var core *sessionwire.HostLinkEndpointError
	if !errors.As(err, &core) || core.Code != code {
		t.Fatalf("err = %v, want Core's HostLinkEndpointError with code %s", err, code)
	}
	var detail *hostlink.EndpointError
	if !errors.As(err, &detail) || detail.Host != hostOne || detail.Tenant != tenantID {
		t.Fatalf("err = %#v, want an EndpointError naming %s and tenant %q", err, hostOne, tenantID)
	}
}

// TestATerminalCloseOfOneTenantsLinkLeavesTheOtherTenantsLinkAndRoutes: a Host
// that closed tenant-a's connection out has said nothing about tenant-b's.
func TestATerminalCloseOfOneTenantsLinkLeavesTheOtherTenantsLinkAndRoutes(t *testing.T) {
	t.Parallel()
	dialer := newTenantDialer()
	pool := newPool(t, dialer, hostlink.Limits{})

	mustBind(t, pool, target(hostOne, base1), tenantBind(tenant, hostOne, "s-1"))
	mustBind(t, pool, target(hostOne, base1), tenantBind(tenantB, hostOne, "s-1"))
	a := dialer.at("ws://10.0.4.7:9000/hostlink/tenant-a")
	b := dialer.at("ws://10.0.4.7:9000/hostlink/tenant-b")
	a.failBind(&hostlink.HostDisconnect{Host: hostOne, Code: 3500, Reason: "invalid token"})

	if err := pool.Bind(context.Background(), target(hostOne, base1), tenantBind(tenant, hostOne, "s-2")); err == nil {
		t.Fatal("a bind over a terminal link succeeded")
	}
	if a.closes() != 1 {
		t.Errorf("tenant-a's terminal link was closed %d times, want 1", a.closes())
	}
	if b.closes() != 0 {
		t.Errorf("tenant-b's live link was closed %d times on tenant-a's evidence", b.closes())
	}
	if _, ok := pool.RouteFor(tenant, "s-1"); ok {
		t.Error("tenant-a's route over the dead link survived the eviction")
	}
	if host, ok := pool.RouteFor(tenantB, "s-1"); !ok || host != hostOne {
		t.Errorf("RouteFor(tenant-b, s-1) = %q, %v: tenant-a's eviction dropped another tenant's route", host, ok)
	}
	if got := pool.TenantLinks(hostOne); got != 1 {
		t.Errorf("TenantLinks = %d after evicting one tenant's link, want 1", got)
	}
	if err := pool.DeliverCommand(context.Background(), tenantB, "s-1", sessionwire.HostLinkCommandDelivery{CommandID: "c-1"}); err != nil {
		t.Errorf("tenant-b's delivery after tenant-a's eviction: %v", err)
	}
}

// TestTheReaperCollectsOneTenantsIdleLinkAndKeepsAnotherTenantsBusyOne.
func TestTheReaperCollectsOneTenantsIdleLinkAndKeepsAnotherTenantsBusyOne(t *testing.T) {
	t.Parallel()
	dialer := newTenantDialer()
	clock := &steppedClock{now: time.Unix(1_700_000_000, 0)}
	pool, err := hostlink.NewPool(hostlink.Config{Dialer: dialer, Limits: defaultLimits(), Now: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pool.Close(context.Background()) })

	mustBind(t, pool, target(hostOne, base1), tenantBind(tenant, hostOne, "s-1"))
	mustBind(t, pool, target(hostOne, base1), tenantBind(tenantB, hostOne, "s-1"))
	if err := pool.Unbind(context.Background(), tenantUnbind(tenant, hostOne, "s-1")); err != nil {
		t.Fatal(err)
	}
	clock.advance(defaultLimits().IdleTimeout)
	if got := pool.ReapIdle(); got != 1 {
		t.Fatalf("ReapIdle = %d, want exactly tenant-a's idle link", got)
	}
	if dialer.at("ws://10.0.4.7:9000/hostlink/tenant-a").closes() != 1 || dialer.at("ws://10.0.4.7:9000/hostlink/tenant-b").closes() != 0 {
		t.Error("the reaper closed the wrong tenant's link")
	}
	if got := pool.TenantBindings(hostOne, tenantB); got != 1 {
		t.Errorf("tenant-b's binding count = %d after the reap, want 1", got)
	}
}

// TestTheLinkCeilingCountsTenantLinks: MaxLinks bounds connections, and one
// Host with two tenants is two of them.
func TestTheLinkCeilingCountsTenantLinks(t *testing.T) {
	t.Parallel()
	dialer := newTenantDialer()
	limits := defaultLimits()
	limits.MaxLinks = 1
	pool := newPool(t, dialer, limits)

	mustBind(t, pool, target(hostOne, base1), tenantBind(tenant, hostOne, "s-1"))
	err := pool.Bind(context.Background(), target(hostOne, base1), tenantBind(tenantB, hostOne, "s-1"))
	if !errors.Is(err, hostlink.ErrLinkLimit) {
		t.Fatalf("a second tenant's link at MaxLinks=1 = %v, want ErrLinkLimit", err)
	}
	// The control: the first tenant's second session costs no link.
	mustBind(t, pool, target(hostOne, base1), tenantBind(tenant, hostOne, "s-2"))
}

// ---------------------------------------------------------------------------
// A dialer whose links are keyed by the ADDRESS dialled, so a tenant's link is
// found by its derived endpoint -- the one thing that tells two tenants'
// connections to one Host apart.
// ---------------------------------------------------------------------------

type tenantDialer struct {
	mu     sync.Mutex
	dialed []hostlink.Target
	links  map[sessionwire.InternalEndpoint]*opLink
}

func newTenantDialer() *tenantDialer {
	return &tenantDialer{links: map[sessionwire.InternalEndpoint]*opLink{}}
}

func (d *tenantDialer) Dial(_ context.Context, tgt hostlink.Target, _ hostlink.Observer) (hostlink.Link, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.dialed = append(d.dialed, tgt)
	link := &opLink{host: tgt.Host}
	d.links[tgt.Endpoint] = link
	return link, nil
}

func (d *tenantDialer) dials() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.dialed)
}

func (d *tenantDialer) targets() []hostlink.Target {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]hostlink.Target(nil), d.dialed...)
}

func (d *tenantDialer) at(endpoint sessionwire.InternalEndpoint) *opLink {
	d.mu.Lock()
	defer d.mu.Unlock()
	if link, ok := d.links[endpoint]; ok {
		return link
	}
	return &opLink{}
}

// opLink records every operation it carried as "<op> <tenant>/<session>", and
// is a Subscriber so the live-tail half of R-1 is observable too.
type opLink struct {
	host    sessionwire.HostID
	mu      sync.Mutex
	log     []string
	tenants []sessionwire.TenantID
	bindErr error
	closed  int
}

func (l *opLink) record(op string, tenantID sessionwire.TenantID, session sessionwire.SessionID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.log = append(l.log, op+" "+string(tenantID)+"/"+string(session))
}

func (l *opLink) Host() sessionwire.HostID { return l.host }

func (l *opLink) Bind(_ context.Context, req sessionwire.HostLinkBindRequest) error {
	l.mu.Lock()
	err := l.bindErr
	if err == nil {
		l.tenants = append(l.tenants, req.TenantID)
	}
	l.mu.Unlock()
	if err != nil {
		return err
	}
	l.record("bind", req.TenantID, req.SessionID)
	return nil
}

func (l *opLink) Unbind(_ context.Context, req sessionwire.HostLinkUnbindRequest) error {
	l.record("unbind", req.TenantID, req.SessionID)
	return nil
}

func (l *opLink) Attach(_ context.Context, req sessionwire.HostLinkAttachRequest) (sessionwire.HostLinkRegistryObservation, error) {
	l.record("attach", req.TenantID, req.SessionID)
	return observationFor(req, 7), nil
}

func (l *opLink) DeliverCommand(_ context.Context, tenantID sessionwire.TenantID, session sessionwire.SessionID, _ sessionwire.HostLinkCommandDelivery) error {
	l.record("deliver", tenantID, session)
	return nil
}

func (l *opLink) Subscribe(_ context.Context, tenantID sessionwire.TenantID, session sessionwire.SessionID, _ hostlink.SessionSink) error {
	l.record("subscribe", tenantID, session)
	return nil
}

func (l *opLink) Unsubscribe(tenantID sessionwire.TenantID, session sessionwire.SessionID) {
	l.record("unsubscribe", tenantID, session)
}

func (l *opLink) Close(context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed++
	return nil
}

func (l *opLink) ops() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.log...)
}

func (l *opLink) bindTenants() []sessionwire.TenantID {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]sessionwire.TenantID(nil), l.tenants...)
}

func (l *opLink) closes() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.closed
}

func (l *opLink) failBind(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.bindErr = err
}

type nopSink struct{}

func (nopSink) Subscribed()        {}
func (nopSink) Publication([]byte) {}
func (nopSink) Ended()             {}
func (nopSink) Restored()          {}

type steppedClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *steppedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *steppedClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func tenantBind(tenantID sessionwire.TenantID, host sessionwire.HostID, session sessionwire.SessionID) sessionwire.HostLinkBindRequest {
	req := bindRequest(host, session)
	req.TenantID = tenantID
	return req
}

func tenantUnbind(tenantID sessionwire.TenantID, host sessionwire.HostID, session sessionwire.SessionID) sessionwire.HostLinkUnbindRequest {
	req := unbindRequest(host, session)
	req.TenantID = tenantID
	return req
}

func tenantAttach(tenantID sessionwire.TenantID, host sessionwire.HostID, session sessionwire.SessionID) sessionwire.HostLinkAttachRequest {
	return sessionwire.HostLinkAttachRequest{
		Version: sessionwire.CurrentWireVersion, TenantID: tenantID, SessionID: session,
		HostID: host, HostGeneration: 7, AgentID: "agent-1", RuntimeCompatibilityID: "runtime-1",
		Mode: sessionwire.HostLinkAttachModeCreate, ActorID: "factory", IdempotencyKey: "attach-" + string(session),
	}
}
