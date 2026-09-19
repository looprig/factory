package hostlink_test

import (
	"context"
	"errors"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/hostlink"
)

// testGateMethod is a capability name used ONLY to drive the mechanism's
// accepting half. It is deliberately not a plausible HostLink method: the real
// signal is host v0.4.0's to fix, and a test spelling a guess would read as a
// promise.
const testGateMethod = "test.only.gate-response-capability"

// TestTheGateResponseCapabilityRefusesEveryReplyToday is the DEFAULT, held:
// no reply -- none advertised, every reserved method, and names a Host might
// plausibly choose -- makes a Host gate_response-capable, because no released
// Host can apply one and the signal is not fixed. When host v0.4.0 fixes it,
// this test is rewritten to "admits exactly the advertised Host".
func TestTheGateResponseCapabilityRefusesEveryReplyToday(t *testing.T) {
	t.Parallel()

	for name, methods := range map[string][]string{
		"no methods": nil,
		"every reserved method": {sessionwire.HostLinkMethodBind, sessionwire.HostLinkMethodUnbind,
			sessionwire.HostLinkMethodAttach, sessionwire.HostLinkMethodDrain, sessionwire.HostLinkMethodDrainStatus},
		"plausible names": {"hostlink.gate_response", "hostlink.gate.respond", "gate_response", testGateMethod},
	} {
		reply := sessionwire.VersionNegotiationResponse{Version: sessionwire.CurrentWireVersion}
		if methods != nil {
			reply = reply.WithHostLinkMethods(methods...)
		}
		if hostlink.GateResponseCapable(reply) {
			t.Errorf("%s: GateResponseCapable = true; no Host may be treated as able to apply a gate_response before host v0.4.0 fixes the signal", name)
		}
	}
}

// TestADefaultPoolRefusesAGateResponseCapabilityOverARealLink: the composed
// default, over a real socket to a stand-in that advertises everything --
// including the test-only name -- answers "cannot", and asks over the
// tenant's own derived address.
func TestADefaultPoolRefusesAGateResponseCapabilityOverARealLink(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{token: serviceToken, methods: []string{sessionwire.HostLinkMethodBind, testGateMethod}})
	pool, err := hostlink.NewPool(hostlink.Config{Dialer: dialerFor(t, serviceToken)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pool.Close(context.Background()) })

	ok, err := pool.AcceptsGateResponses(context.Background(), host.target(), tenant)
	if err != nil || ok {
		t.Fatalf("AcceptsGateResponses = (%v, %v), want (false, nil) under the default predicate", ok, err)
	}
	if got := pool.TenantLinks(host.id); got != 1 {
		t.Fatalf("the question opened %d links to the Host, want the tenant's one", got)
	}
}

// TestTheGateResponseMechanismAsksTheTenantsLinkAndThePredicate drives the
// ACCEPTING half through an injected predicate, over real sockets: the answer
// is the predicate applied to the reply that Host sent, so the Host that
// advertised the (test-only) capability is capable and the one that did not is
// not. This is the mechanism host v0.4.0's name will plug into.
func TestTheGateResponseMechanismAsksTheTenantsLinkAndThePredicate(t *testing.T) {
	t.Parallel()

	advertising := newHostServer(t, hostOptions{token: serviceToken, methods: []string{sessionwire.HostLinkMethodBind, testGateMethod}})
	silent := newHostServer(t, hostOptions{token: serviceToken, methods: []string{sessionwire.HostLinkMethodBind}})
	pool, err := hostlink.NewPool(hostlink.Config{
		Dialer:        dialerFor(t, serviceToken),
		GateResponses: func(reply sessionwire.VersionNegotiationResponse) bool { return reply.Supports(testGateMethod) },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pool.Close(context.Background()) })

	if ok, err := pool.AcceptsGateResponses(context.Background(), advertising.target(), tenant); err != nil || !ok {
		t.Fatalf("advertising Host = (%v, %v), want (true, nil)", ok, err)
	}
	// The stand-ins share a Host id, so the silent one is addressed as a
	// different Host: the pool keys links by (Host, tenant).
	other := silent.target()
	other.Host = hostTwo
	if ok, err := pool.AcceptsGateResponses(context.Background(), other, tenant); err != nil || ok {
		t.Fatalf("silent Host = (%v, %v), want (false, nil)", ok, err)
	}
}

// TestAGateResponseCapabilityThatCannotBeReadFailsClosed covers the three ways
// the question has no yes: a link that cannot report a reply (false, nil), a
// reply that cannot be read now (false, the transient -- the link is kept), and
// a link that will never answer again (false, the terminal error -- evicted so
// the next question dials afresh).
func TestAGateResponseCapabilityThatCannotBeReadFailsClosed(t *testing.T) {
	t.Parallel()
	yes := func(sessionwire.VersionNegotiationResponse) bool { return true }

	t.Run("a link with no reply to report", func(t *testing.T) {
		t.Parallel()
		pool, err := hostlink.NewPool(hostlink.Config{Dialer: newTenantDialer(), GateResponses: yes})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = pool.Close(context.Background()) })
		if ok, err := pool.AcceptsGateResponses(context.Background(), target(hostOne, base1), tenant); err != nil || ok {
			t.Fatalf("= (%v, %v), want (false, nil): an unknown capability is not one", ok, err)
		}
	})
	for name, row := range map[string]struct {
		err     error
		evicted bool
	}{
		"reconnecting": {hostlink.ErrLinkReconnecting, false},
		"terminal":     {&hostlink.HostDisconnect{Host: hostOne, Code: 3500}, true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dialer := &negotiatingDialer{err: row.err}
			pool, err := hostlink.NewPool(hostlink.Config{Dialer: dialer, GateResponses: yes})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = pool.Close(context.Background()) })
			ok, err := pool.AcceptsGateResponses(context.Background(), target(hostOne, base1), tenant)
			if ok || !errors.Is(err, row.err) {
				t.Fatalf("= (%v, %v), want (false, %v)", ok, err, row.err)
			}
			if evicted := pool.Links() == 0; evicted != row.evicted {
				t.Fatalf("Links() = %d after a %s read; evicted = %v, want %v", pool.Links(), name, evicted, row.evicted)
			}
		})
	}
}

// TestAGateResponseCapabilityForATenantTheBaseCannotCarryDialsNothing.
func TestAGateResponseCapabilityForATenantTheBaseCannotCarryDialsNothing(t *testing.T) {
	t.Parallel()
	dialer := newTenantDialer()
	pool := newPool(t, dialer, hostlink.Limits{})
	_, err := pool.AcceptsGateResponses(context.Background(), target(hostOne, "ws://10.0.4.7:9000/hostlink"), tenant)
	if !errors.Is(err, hostlink.ErrNoTenantEndpoint) || dialer.dials() != 0 {
		t.Fatalf("= %v with %d dials, want ErrNoTenantEndpoint and none", err, dialer.dials())
	}
}

// negotiatingDialer dials links whose Negotiated fails with err.
type negotiatingDialer struct{ err error }

func (d *negotiatingDialer) Dial(_ context.Context, tgt hostlink.Target, _ hostlink.Observer) (hostlink.Link, error) {
	return &negotiatingLink{opLink: &opLink{host: tgt.Host}, err: d.err}, nil
}

type negotiatingLink struct {
	*opLink
	err error
}

func (l *negotiatingLink) Negotiated() (sessionwire.VersionNegotiationResponse, error) {
	return sessionwire.VersionNegotiationResponse{}, l.err
}
