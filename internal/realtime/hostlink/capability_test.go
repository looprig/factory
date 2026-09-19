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

// TestTheGateResponseCapabilityIsExactlyCoresToken: a Host is gate_response-
// capable when, and only when, its connect reply lists Core's token. The token
// is spelled here as an ABSOLUTE LITERAL, because a fixture built from the
// constant under test would pass for any value the constant took.
func TestTheGateResponseCapabilityIsExactlyCoresToken(t *testing.T) {
	t.Parallel()

	const token = "hostlink.command.gate_response"
	if sessionwire.HostLinkCapabilityGateResponse != token {
		t.Fatalf("Core's token is %q, want %q", sessionwire.HostLinkCapabilityGateResponse, token)
	}
	five := []string{sessionwire.HostLinkMethodBind, sessionwire.HostLinkMethodUnbind,
		sessionwire.HostLinkMethodAttach, sessionwire.HostLinkMethodDrain, sessionwire.HostLinkMethodDrainStatus}
	for name, row := range map[string]struct {
		methods []string
		want    bool
	}{
		"a reply with no hostlink_methods member":      {nil, false},
		"the five methods only (a v0.3.0 Host)":        {five, false},
		"the five methods and the token (v0.4.0)":      {append(append([]string(nil), five...), token), true},
		"the token alone":                              {[]string{token}, true},
		"the token as a substring":                     {[]string{"x" + token, token + ".v2", "hostlink.command.gate_response_v2"}, false},
		"the token under the channel prefix":           {[]string{"hostlink.v1." + token, "hostlink.v1.command.gate_response"}, false},
		"near misses a Host might plausibly have used": {[]string{"hostlink.gate_response", "gate_response", testGateMethod}, false},
	} {
		reply := sessionwire.VersionNegotiationResponse{Version: sessionwire.CurrentWireVersion}
		if row.methods != nil {
			reply = reply.WithHostLinkMethods(row.methods...)
		}
		if got := hostlink.GateResponseCapable(reply); got != row.want {
			t.Errorf("%s: GateResponseCapable = %v, want %v", name, got, row.want)
		}
	}
}

// TestTheDefaultPoolAnswersFromTheHostsRealReply: the composed default
// predicate over real sockets -- a stand-in advertising Core's token is
// capable, one advertising the five methods and a near miss is not -- asked
// over the tenant's own derived address.
func TestTheDefaultPoolAnswersFromTheHostsRealReply(t *testing.T) {
	t.Parallel()

	capable := newHostServer(t, hostOptions{token: serviceToken, methods: []string{sessionwire.HostLinkMethodBind, sessionwire.HostLinkCapabilityGateResponse}})
	older := newHostServer(t, hostOptions{token: serviceToken, methods: []string{sessionwire.HostLinkMethodBind, sessionwire.HostLinkMethodUnbind,
		sessionwire.HostLinkMethodAttach, sessionwire.HostLinkMethodDrain, sessionwire.HostLinkMethodDrainStatus, testGateMethod}})
	pool, err := hostlink.NewPool(hostlink.Config{Dialer: dialerFor(t, serviceToken)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pool.Close(context.Background()) })

	if ok, err := pool.AcceptsGateResponses(context.Background(), capable.target(), tenant); err != nil || !ok {
		t.Fatalf("a Host advertising the token = (%v, %v), want (true, nil)", ok, err)
	}
	other := older.target()
	other.Host = hostTwo // the stand-ins share a Host id; links are keyed by (Host, tenant)
	if ok, err := pool.AcceptsGateResponses(context.Background(), other, tenant); err != nil || ok {
		t.Fatalf("a Host advertising the five methods = (%v, %v), want (false, nil)", ok, err)
	}
	if got := pool.TenantLinks(capable.id); got != 1 {
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
