package factory

import (
	"context"
	"errors"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/admission"
	"github.com/looprig/factory/internal/placement"
	"github.com/looprig/factory/internal/realtime/hostlink"
)

type advertisingDialer struct {
	methods map[sessionwire.HostID][]string
	dialled []hostlink.Target
}

type failingCapabilityDialer struct{}

func (failingCapabilityDialer) Dial(context.Context, hostlink.Target, hostlink.Observer) (hostlink.Link, error) {
	return nil, hostlink.ErrLinkReconnecting
}

func TestPlacementCapabilityAdapterKeepsTransientCause(t *testing.T) {
	t.Parallel()
	pool, err := hostlink.NewPool(hostlink.Config{Dialer: failingCapabilityDialer{}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pool.Close(context.Background()) })
	_, err = placementLinks{pool: pool}.AcceptsGateResponses(context.Background(), sessionwire.HostLinkRegistryObservation{
		TenantID: "tenant-a", SessionID: "session-a", HostID: "host-a", InternalEndpoint: "ws://host-a.internal",
	})
	if !errors.Is(err, placement.ErrHostUnreachable) || !errors.Is(err, hostlink.ErrLinkReconnecting) {
		t.Fatalf("placement capability read = %v; want transient classification and cause", err)
	}
}

func (d *advertisingDialer) Dial(_ context.Context, target hostlink.Target, _ hostlink.Observer) (hostlink.Link, error) {
	d.dialled = append(d.dialled, target)
	return advertisingLink{host: target.Host, methods: d.methods[target.Host]}, nil
}

// advertisingLink's Host advertises the methods its dialer was given.
type advertisingLink struct {
	host    sessionwire.HostID
	methods []string
}

func (l advertisingLink) Host() sessionwire.HostID { return l.host }
func (advertisingLink) Bind(context.Context, sessionwire.HostLinkBindRequest) error {
	return nil
}
func (advertisingLink) Unbind(context.Context, sessionwire.HostLinkUnbindRequest) error {
	return nil
}
func (advertisingLink) Attach(context.Context, sessionwire.HostLinkAttachRequest) (sessionwire.HostLinkRegistryObservation, error) {
	return sessionwire.HostLinkRegistryObservation{}, nil
}
func (advertisingLink) DeliverCommand(context.Context, sessionwire.TenantID, sessionwire.SessionID, sessionwire.HostLinkCommandDelivery) error {
	return nil
}
func (advertisingLink) Close(context.Context) error { return nil }
func (l advertisingLink) Negotiated() (sessionwire.VersionNegotiationResponse, error) {
	return sessionwire.VersionNegotiationResponse{Version: sessionwire.CurrentWireVersion}.WithHostLinkMethods(l.methods...), nil
}

// TestTheComposedGateResponseQuestionAsksTheOwnersTenantLink: the adapter
// admission and placement share asks the OWNER's link for the session's
// TENANT, at the address derived from the owner's advertised base, and under
// the production predicate answers from Core's token alone: host-9 advertises
// near misses and is refused, host-10 advertises the token and is capable.
func TestTheComposedGateResponseQuestionAsksTheOwnersTenantLink(t *testing.T) {
	t.Parallel()
	dialer := &advertisingDialer{methods: map[sessionwire.HostID][]string{
		"host-9":  {sessionwire.HostLinkMethodBind, sessionwire.HostLinkMethodAttach, "hostlink.gate_response", "gate_response"},
		"host-10": {sessionwire.HostLinkMethodBind, sessionwire.HostLinkCapabilityGateResponse},
	}}
	pool, err := hostlink.NewPool(hostlink.Config{Dialer: dialer})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pool.Close(context.Background()) })
	owner := sessionwire.HostLinkRegistryObservation{TenantID: "tenant-b", SessionID: "s-1", HostID: "host-9", InternalEndpoint: "ws://10.1.2.3:7100"}
	for _, ask := range []func(context.Context, sessionwire.HostLinkRegistryObservation) (bool, error){
		gateResponders{pool: pool}.AcceptsGateResponses, placementLinks{pool: pool}.AcceptsGateResponses,
	} {
		ok, err := ask(context.Background(), owner)
		if err != nil || ok {
			t.Fatalf("AcceptsGateResponses = (%v, %v), want (false, nil) under the production predicate", ok, err)
		}
	}
	if len(dialer.dialled) != 1 || dialer.dialled[0] != (hostlink.Target{Host: "host-9", Endpoint: "ws://10.1.2.3:7100/hostlink/tenant-b"}) {
		t.Fatalf("dialled %v, want host-9's tenant-b link once", dialer.dialled)
	}
	// A base that cannot address the tenant reaches placement in placement's
	// own vocabulary, with Core's code kept (spec gate N4); admission sees
	// the plain fault.
	unaddressable := owner
	unaddressable.HostID, unaddressable.InternalEndpoint = "host-11", "ws://10.1.2.5:7100/pods/x"
	_, err = placementLinks{pool: pool}.AcceptsGateResponses(context.Background(), unaddressable)
	var code *sessionwire.HostLinkEndpointError
	if !errors.Is(err, placement.ErrTenantUnaddressable) || !errors.As(err, &code) || code.Code != sessionwire.HostLinkEndpointCodeBaseNotBare {
		t.Fatalf("an unaddressable candidate reached placement as %v, want ErrTenantUnaddressable carrying base_not_bare", err)
	}
	if _, err := (gateResponders{pool: pool}).AcceptsGateResponses(context.Background(), unaddressable); errors.Is(err, placement.ErrTenantUnaddressable) || errors.Is(err, admission.ErrGateResponderUnavailable) {
		t.Fatalf("admission's adapter classified an unaddressable owner as %v, want a plain fault", err)
	}
	capable := owner
	capable.HostID, capable.InternalEndpoint = "host-10", "ws://10.1.2.4:7100"
	for _, ask := range []func(context.Context, sessionwire.HostLinkRegistryObservation) (bool, error){
		gateResponders{pool: pool}.AcceptsGateResponses, placementLinks{pool: pool}.AcceptsGateResponses,
	} {
		if ok, err := ask(context.Background(), capable); err != nil || !ok {
			t.Fatalf("a Host advertising Core's token = (%v, %v), want (true, nil)", ok, err)
		}
	}
}
