package hostlink

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	centrifuge "github.com/centrifugal/centrifuge"
	centrifugego "github.com/centrifugal/centrifuge-go"
	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// TestReconnectDoesNotWaitForAnRPCAdmissionLock uses the real Centrifuge
// transport to pin the ordering that used to deadlock:
//
//  1. a caller owns the local RPC serialization lock;
//  2. the Host moves the transport to Connecting;
//  3. another RPC enters the transport's connect-future queue; and
//  4. the transport must still run its OnConnecting callback and schedule the
//     reconnect.
//
// The client callback is synchronous and runs before centrifuge-go schedules
// the reconnect. Holding the lock across client.RPC therefore made the
// callback wait for an RPC that was itself waiting for the callback's future.
// This test holds the same lock and drives the real client RPC at the exact
// Connecting barrier, so it does not depend on a scheduler race.
func TestReconnectDoesNotWaitForAnRPCAdmissionLock(t *testing.T) {
	t.Parallel()

	host := newLivenessHost(t)
	dialer, err := NewCentrifugeDialer(DialerConfig{
		Credential: livenessCredential("service-token"),
		Version:    "factory-test-build",
		Limits: Limits{
			MaxLinks:     1,
			DialTimeout:  5 * time.Second,
			IdleTimeout:  10 * time.Second,
			ReconnectMin: 10 * time.Millisecond,
			ReconnectMax: 20 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("NewCentrifugeDialer: %v", err)
	}
	linkValue, err := dialer.Dial(context.Background(), host.target, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	link := linkValue.(*centrifugeLink)
	t.Cleanup(func() { _ = link.Close(context.Background()) })

	serverClient := <-host.connected
	link.rpcMu.Lock()
	defer link.rpcMu.Unlock()
	serverClient.Disconnect(centrifuge.Disconnect{Code: 4000, Reason: "test reconnect"})

	waitForState(t, link.client, centrifugego.StateConnecting)

	// With the lock held, this RPC is guaranteed to enter centrifuge-go's
	// connect-future queue after the transport has entered Connecting. It is
	// deliberately unbounded: liveness must not depend on a caller timeout.
	rpcDone := make(chan error, 1)
	go func() {
		_, err := link.client.RPC(context.Background(), "hostlink.v1.test", nil)
		rpcDone <- err
	}()

	select {
	case <-host.reconnected:
	case <-time.After(2 * time.Second):
		t.Fatal("reconnect did not reach the Host while an RPC admission lock was held")
	}
	select {
	case err := <-rpcDone:
		if err != nil {
			t.Fatalf("queued RPC after reconnect: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("queued RPC did not settle after reconnect")
	}
}

type livenessCredential string

func (c livenessCredential) ServiceToken(context.Context) (string, error) { return string(c), nil }

type livenessHost struct {
	target      Target
	connected   chan *centrifuge.Client
	reconnected chan struct{}
	connects    atomic.Int32
	closeOnce   sync.Once
	node        *centrifuge.Node
	server      *httptest.Server
}

func newLivenessHost(t *testing.T) *livenessHost {
	t.Helper()
	node, err := centrifuge.New(centrifuge.Config{
		Name:     "host-standin",
		LogLevel: centrifuge.LogLevelNone,
	})
	if err != nil {
		t.Fatalf("centrifuge.New: %v", err)
	}
	host := &livenessHost{
		target:      Target{Host: sessionwire.HostID("host-1")},
		connected:   make(chan *centrifuge.Client, 4),
		reconnected: make(chan struct{}),
		node:        node,
	}
	node.OnConnecting(func(_ context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		request, err := sessionwire.DecodeHostLinkConnectRequest(e.Data)
		if err != nil {
			return centrifuge.ConnectReply{}, centrifuge.DisconnectInappropriateProtocol
		}
		selection, err := sessionwire.NegotiateVersion(request)
		if err != nil {
			return centrifuge.ConnectReply{}, centrifuge.DisconnectInappropriateProtocol
		}
		selection = selection.WithHostLinkMethods(sessionwire.HostLinkMethodBind, sessionwire.HostLinkMethodUnbind)
		body, err := sessionwire.EncodeHostLinkConnectReply(selection)
		if err != nil {
			return centrifuge.ConnectReply{}, centrifuge.DisconnectServerError
		}
		if host.connects.Add(1) == 2 {
			close(host.reconnected)
		}
		return centrifuge.ConnectReply{
			Credentials: &centrifuge.Credentials{UserID: "factory"},
			Data:        body,
		}, nil
	})
	node.OnConnect(func(client *centrifuge.Client) {
		select {
		case host.connected <- client:
		default:
		}
		client.OnRPC(func(_ centrifuge.RPCEvent, cb centrifuge.RPCCallback) {
			// Empty RPC data is the legitimate success shape.
			cb(centrifuge.RPCReply{}, nil)
		})
	})
	if err := node.Run(); err != nil {
		t.Fatalf("node.Run: %v", err)
	}
	host.server = httptest.NewServer(centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{
		CheckOrigin: func(*http.Request) bool { return true },
		Compression: false,
	}))
	host.target.Endpoint = sessionwire.InternalEndpoint("ws" + strings.TrimPrefix(host.server.URL, "http"))
	t.Cleanup(func() {
		host.closeOnce.Do(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = node.Shutdown(ctx)
			host.server.Close()
		})
	})
	return host
}

func waitForState(t *testing.T, client *centrifugego.Client, want centrifugego.State) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if client.State() == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("client State = %s, want %s", client.State(), want)
		}
		time.Sleep(time.Millisecond)
	}
}
