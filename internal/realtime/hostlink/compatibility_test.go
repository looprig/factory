package hostlink_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"

	centrifuge "github.com/centrifugal/centrifuge"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/hostlink"
)

// These cases are the compatibility boundary for the released HostLink
// protocol. The hostServer is still a stand-in: the real Host module is not a
// Factory dependency, and cross-repository proof belongs in the integration
// module after both sides are released.

func TestHostLinkConnectUsesCoreBareFraming(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{
		methods: []string{sessionwire.HostLinkMethodBind, sessionwire.HostLinkMethodUnbind},
	})
	link, err := dialHost(t, host, dialerFor(t, serviceToken))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer link.Close(context.Background())

	connects := host.connects()
	if len(connects) != 1 {
		t.Fatalf("the Host saw %d handshakes, want 1", len(connects))
	}
	if got, want := string(connects[0].data), `{"supported_versions":[1]}`; got != want {
		t.Errorf("connect request Data = %s, want bare %s", got, want)
	}
	if got, want := string(connects[0].reply), `{"hostlink_methods":["hostlink.bind","hostlink.unbind"],"version":1}`; got != want {
		t.Errorf("connect reply Data = %s, want bare %s", got, want)
	}
	if got, want := connects[0].offered, []sessionwire.WireVersion{sessionwire.CurrentWireVersion}; !slices.Equal(got, want) {
		t.Errorf("offered versions = %v, want %v", got, want)
	}
}

func TestHostLinkConnectRejectsTheFormerWrappedReply(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{
		rawNegotiation: `{"version_negotiation":{"version":1}}`,
	})
	link, err := dialHost(t, host, dialerFor(t, serviceToken))
	if link != nil {
		t.Cleanup(func() { _ = link.Close(context.Background()) })
	}
	if !errors.Is(err, hostlink.ErrUnsupportedProtocol) {
		t.Fatalf("Dial = %v, want ErrUnsupportedProtocol for the former wrapped reply", err)
	}
}

func TestCapabilityLessHostConnectsButBindAndUnbindDoNotSendRPCs(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{methods: []string{}})
	link := mustDial(t, host)

	bindErr := link.Bind(context.Background(), bindRequest(hostOne, "s-1"))
	if bindErr == nil {
		t.Fatal("Bind succeeded although the Host advertised no bind capability")
	}
	var unsupported *hostlink.UnsupportedMethodError
	if !errors.As(bindErr, &unsupported) || !errors.Is(bindErr, hostlink.ErrUnsupportedMethod) {
		t.Fatalf("Bind refusal = %v, want *UnsupportedMethodError", bindErr)
	}
	if unsupported.Method != sessionwire.HostLinkMethodBind {
		t.Errorf("unsupported method = %q, want %q", unsupported.Method, sessionwire.HostLinkMethodBind)
	}
	if got := len(host.calls()); got != 0 {
		t.Fatalf("Host received %d RPCs for an unadvertised bind, want 0", got)
	}

	unbindErr := link.Unbind(context.Background(), unbindRequest(hostOne, "s-1"))
	if unbindErr == nil {
		t.Fatal("Unbind succeeded although the Host advertised no unbind capability")
	}
	if got := len(host.calls()); got != 0 {
		t.Fatalf("Host received %d RPCs for an unadvertised unbind, want 0", got)
	}
}

func TestReconnectRenegotiatesAndDropsReservedCapabilities(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{
		methods: []string{sessionwire.HostLinkMethodBind, sessionwire.HostLinkMethodUnbind},
	})
	link := mustDial(t, host)
	if err := link.Bind(context.Background(), bindRequest(hostOne, "s-before-reconnect")); err != nil {
		t.Fatalf("Bind before reconnect: %v", err)
	}
	before := len(host.calls())

	// The restarted Host now speaks the same wire version but advertises no
	// reserved methods. A successful reconnect must replace, not retain, the
	// old capability set.
	host.setNegotiation(`{"version":1}`)
	host.disconnectEveryone(centrifuge.Disconnect{Code: 4000, Reason: "test capability change"})
	waitUntil(t, "a second handshake", func() bool { return len(host.connects()) >= 2 })
	for index, connect := range host.connects()[:2] {
		if got, want := string(connect.data), `{"supported_versions":[1]}`; got != want {
			t.Errorf("connect %d request Data = %s, want bare %s", index, got, want)
		}
	}

	var bindErr error
	waitUntil(t, "the unadvertised bind refusal", func() bool {
		bindErr = link.Bind(context.Background(), bindRequest(hostOne, "s-after-reconnect"))
		return bindErr != nil && !errors.Is(bindErr, hostlink.ErrLinkReconnecting)
	})
	if got := len(host.calls()); got != before {
		t.Fatalf("Host received %d RPCs after capability removal, want %d", got, before)
	}
	var unsupported *hostlink.UnsupportedMethodError
	if !errors.As(bindErr, &unsupported) || !errors.Is(bindErr, hostlink.ErrUnsupportedMethod) {
		t.Fatalf("Bind after capability-only reconnect = %v, want *UnsupportedMethodError", bindErr)
	}
	if unsupported.Method != sessionwire.HostLinkMethodBind {
		t.Errorf("unsupported method after reconnect = %q, want %q", unsupported.Method, sessionwire.HostLinkMethodBind)
	}
	if errors.Is(bindErr, hostlink.ErrUnsupportedProtocol) {
		t.Fatalf("Bind after capability-only reconnect = %v, want operation refusal rather than protocol failure", bindErr)
	}
}

func TestBareHostRefusalIsClassifiedAsHostRefusal(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{
		rpc: func(string, []byte) ([]byte, error) {
			return []byte(`{"code":"epoch_mismatch","current_lease_epoch":11}`), nil
		},
	})
	link := mustDial(t, host)

	err := link.DeliverCommand(context.Background(), tenant, "s-1", sessionwire.HostLinkCommandDelivery{CommandID: "cmd-abc"})
	var refusal *hostlink.HostRefusal
	if !errors.As(err, &refusal) {
		t.Fatalf("DeliverCommand = %v, want a *HostRefusal from the bare body", err)
	}
	if refusal.Code != sessionwire.HostLinkErrorEpochMismatch || refusal.CurrentLeaseEpoch != 11 {
		t.Fatalf("HostRefusal = %+v, want epoch_mismatch at epoch 11", refusal)
	}
}

func TestEmptyHostLinkRPCBodyIsSuccessful(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{})
	link := mustDial(t, host)
	if err := link.DeliverCommand(context.Background(), tenant, "s-1", sessionwire.HostLinkCommandDelivery{CommandID: "cmd-abc"}); err != nil {
		t.Fatalf("DeliverCommand with an empty success body = %v, want nil", err)
	}
}

func TestMalformedOrWrappedHostLinkRPCBodiesFailClosed(t *testing.T) {
	t.Parallel()

	for name, body := range map[string][]byte{
		"missing epoch detail":   []byte(`{"code":"epoch_mismatch"}`),
		"unknown refusal code":   []byte(`{"code":"future_refusal"}`),
		"former wrapped refusal": []byte(`{"error":{"code":"epoch_mismatch","current_lease_epoch":11}}`),
		"unknown body":           []byte(`{"unexpected":true}`),
		"empty object":           []byte(`{}`),
	} {
		name, body := name, body
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			host := newHostServer(t, hostOptions{
				rpc: func(string, []byte) ([]byte, error) {
					return body, nil
				},
			})
			link := mustDial(t, host)
			err := link.DeliverCommand(context.Background(), tenant, "s-1", sessionwire.HostLinkCommandDelivery{CommandID: "cmd-abc"})
			if err == nil {
				t.Fatalf("DeliverCommand accepted malformed body %s", body)
			}
			var refusal *hostlink.HostRefusal
			if errors.As(err, &refusal) {
				t.Fatalf("DeliverCommand classified malformed body %s as HostRefusal: %+v", body, refusal)
			}
		})
	}
}

func TestBareHostRefusalJSONIsValidatedByCore(t *testing.T) {
	t.Parallel()

	// This is a positive control for the test fixture: the exact body used by
	// TestBareHostRefusalIsClassifiedAsHostRefusal must be Core-valid, while the
	// wrapped shape must be rejected by Core's strict HostLinkError decoder.
	var refusal sessionwire.HostLinkError
	if err := json.Unmarshal([]byte(`{"code":"epoch_mismatch","current_lease_epoch":11}`), &refusal); err != nil {
		t.Fatalf("Core rejected the bare refusal fixture: %v", err)
	}
	if err := json.Unmarshal([]byte(`{"error":{"code":"epoch_mismatch","current_lease_epoch":11}}`), &refusal); err == nil {
		t.Fatal("Core accepted the former wrapped refusal shape")
	}
}
