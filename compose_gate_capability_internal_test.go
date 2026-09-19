package factory

import (
	"context"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/hostlink"
)

type advertisingDialer struct{ dialled []hostlink.Target }

func (d *advertisingDialer) Dial(_ context.Context, target hostlink.Target, _ hostlink.Observer) (hostlink.Link, error) {
	d.dialled = append(d.dialled, target)
	return advertisingLink{host: target.Host}, nil
}

// advertisingLink's Host advertises every reserved method and several names a
// gate_response capability might plausibly take.
type advertisingLink struct{ host sessionwire.HostID }

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
func (advertisingLink) Negotiated() (sessionwire.VersionNegotiationResponse, error) {
	return sessionwire.VersionNegotiationResponse{Version: sessionwire.CurrentWireVersion}.WithHostLinkMethods(
		sessionwire.HostLinkMethodBind, sessionwire.HostLinkMethodAttach, "hostlink.gate_response", "gate_response"), nil
}

// TestTheComposedGateResponseQuestionAsksTheOwnersTenantLinkAndRefuses: the
// adapter admission and placement share asks the OWNER's link for the
// session's TENANT, at the address derived from the owner's advertised base,
// and -- under the production predicate, until host v0.4.0 fixes the signal --
// answers "cannot" however much the Host advertises.
func TestTheComposedGateResponseQuestionAsksTheOwnersTenantLinkAndRefuses(t *testing.T) {
	t.Parallel()
	dialer := &advertisingDialer{}
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
}
