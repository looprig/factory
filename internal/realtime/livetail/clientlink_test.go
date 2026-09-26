package livetail_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	centrifugego "github.com/centrifugal/centrifuge-go"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	internalidentity "github.com/looprig/factory/internal/identity"
	"github.com/looprig/factory/internal/realtime/clientlink"
	"github.com/looprig/sessionstore"
)

type liveVerifier struct{}

func (liveVerifier) VerifyCredential(context.Context, identity.Credential) (identity.Claims, error) {
	return identity.Claims{Tenant: tenantA, Subject: "viewer", Kind: identity.KindActor, ExpiresAt: time.Now().Add(time.Hour)}, nil
}

type unusedAdmitter struct{}

func (unusedAdmitter) AdmitCreate(context.Context, identity.Principal, sessionwire.CreateRequest) (sessionstore.DispositionInboxEntry, bool, error) {
	return sessionstore.DispositionInboxEntry{}, false, nil
}
func (unusedAdmitter) AdmitInput(context.Context, identity.Principal, sessionwire.InputRequest) (sessionstore.DispositionInboxEntry, bool, error) {
	return sessionstore.DispositionInboxEntry{}, false, nil
}
func (unusedAdmitter) AdmitInterrupt(context.Context, identity.Principal, sessionwire.InterruptRequest) (sessionstore.DispositionInboxEntry, bool, error) {
	return sessionstore.DispositionInboxEntry{}, false, nil
}
func (unusedAdmitter) AdmitRestore(context.Context, identity.Principal, sessionwire.RestoreRequest) (sessionstore.DispositionInboxEntry, bool, error) {
	return sessionstore.DispositionInboxEntry{}, false, nil
}
func (unusedAdmitter) AdmitGateResponse(context.Context, identity.Principal, sessionwire.GateResponseRequest) (sessionstore.DispositionInboxEntry, bool, error) {
	return sessionstore.DispositionInboxEntry{}, false, nil
}

type noDemand struct{}

func (noDemand) Acquire(context.Context, sessionwire.TenantID, sessionwire.SessionID) error {
	return nil
}
func (noDemand) Release(context.Context, sessionwire.TenantID, sessionwire.SessionID) error {
	return nil
}

type clientFrames struct {
	mu     sync.Mutex
	frames []string
}

func (f *clientFrames) got() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.frames...)
}

func TestHostLinkEphemeralReachesRealClientLinkSubscriber(t *testing.T) {
	t.Parallel()
	authenticator, err := internalidentity.NewAuthenticator(internalidentity.Config{Verifier: liveVerifier{}})
	if err != nil {
		t.Fatal(err)
	}
	viewer, err := clientlink.NewHandler(clientlink.Config{
		Authenticator: authenticator, Authorizer: internalidentity.Authorizer{}, Admitter: unusedAdmitter{}, Demand: noDemand{}, Clock: &manualClock{},
		Limits: clientlink.Limits{MaxConnections: 4, MaxChannelsPerConnection: 4, PerConnectionQueueBytes: 1 << 20,
			WriteTimeout: 5 * time.Second, PingInterval: 25 * time.Second, PongTimeout: 10 * time.Second,
			CommandTimeout: 5 * time.Second, DemandTimeout: 5 * time.Second},
		Version: "factory-test",
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(viewer)
	client := centrifugego.NewJsonClient("ws"+strings.TrimPrefix(server.URL, "http"), centrifugego.Config{
		Token: "viewer", Data: []byte(`{"protocol_version":"1"}`),
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = viewer.Shutdown(ctx)
		client.Close()
		server.Close()
	})
	if err := client.Connect(); err != nil {
		t.Fatal(err)
	}
	sub, err := client.NewSubscription(clientlink.SessionChannel(tenantA, session))
	if err != nil {
		t.Fatal(err)
	}
	frames := &clientFrames{}
	subscribed := make(chan struct{}, 1)
	sub.OnSubscribed(func(centrifugego.SubscribedEvent) {
		select {
		case subscribed <- struct{}{}:
		default:
		}
	})
	sub.OnPublication(func(event centrifugego.PublicationEvent) {
		frames.mu.Lock()
		frames.frames = append(frames.frames, string(event.Data))
		frames.mu.Unlock()
	})
	if err := sub.Subscribe(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-subscribed:
	case <-time.After(waitFor):
		t.Fatal("ClientLink subscription did not start")
	}

	host := newStandIn(t, "host-1")
	// The ClientLink subscription is real; the demand seam is driven explicitly
	// so the test can establish the HostLink tail after the viewer is ready.
	r := newRig(t, rigOptions{viewers: viewer})
	r.dir.put(host.observation(tenantA, session, 3))
	r.watch(t, tenantA, session)
	channel := sessionwire.HostLinkChannel(tenantA, session)
	r.awaitTail(t, host, channel)
	want := [][]byte{enduring(t, tenantA, session, 1), ephemeral(t, "live"), enduring(t, tenantA, session, 2)}
	for _, frame := range want {
		host.publish(t, channel, frame)
	}
	eventually(t, "the real ClientLink subscriber to receive the mixed stream", func() bool { return len(frames.got()) == len(want) })
	for i, got := range frames.got() {
		if got != string(want[i]) {
			t.Fatalf("ClientLink frame %d = %s, want HostLink bytes %s", i, got, want[i])
		}
	}
}
