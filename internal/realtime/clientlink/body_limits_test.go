package clientlink_test

import (
	"context"
	"strings"
	"testing"

	"github.com/centrifugal/protocol"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/clientlink"
)

func createDataOfSize(size int) []byte {
	const prefix = `{"version":1,"command_id":"cmd-large","session_id":"session-large","agent_id":"agent-a","blocks":[{"text":"`
	const suffix = `"}]}`
	return []byte(prefix + strings.Repeat("x", size-len(prefix)-len(suffix)) + suffix)
}

func TestRaisedMessageLimitLeavesRoomForRPCEnvelope(t *testing.T) {
	const payloadLimit = 128 << 10
	const frameLimit = payloadLimit + 16<<10
	data := createDataOfSize(payloadLimit)
	frame, err := protocol.NewJSONCommandEncoder().Encode(&protocol.Command{
		Id:  1,
		Rpc: &protocol.RPCRequest{Method: string(clientlink.MethodSessionCreate), Data: data},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != payloadLimit || len(frame) <= payloadLimit || len(frame) > frameLimit {
		t.Fatalf("payload %d, encoded RPC frame %d, frame cap %d", len(data), len(frame), frameLimit)
	}
}

func TestRaisedMessageLimitReturnsTypedRefusalBelowFrameCap(t *testing.T) {
	const payloadLimit = 128 << 10
	limits := testLimits()
	limits.MaxMessageBytes = payloadLimit
	f := newFixture(t, limits)
	client, observed := dialSupported(t, f, "token-a")
	await(t, observed.connected, "connected")
	ctx, cancel := context.WithTimeout(context.Background(), waitFor)
	defer cancel()
	reply, err := client.RPC(ctx, string(clientlink.MethodSessionCreate), createDataOfSize(payloadLimit+1))
	if err != nil {
		t.Fatalf("RPC: %v", err)
	}
	if got := envelopeCode(t, reply.Data); got != sessionwire.ErrorCodeInvalidRequest {
		t.Fatalf("refusal = %q", got)
	}
}

func TestFrameAboveCapClosesTransport(t *testing.T) {
	for _, row := range []struct {
		name  string
		limit int
	}{
		{name: "default unset", limit: 0},
		{name: "raised", limit: 128 << 10},
	} {
		t.Run(row.name, func(t *testing.T) {
			limits := testLimits()
			limits.MaxMessageBytes = row.limit
			f := newFixture(t, limits)
			client, observed := dialSupported(t, f, "token-a")
			await(t, observed.connected, "connected")
			frameCap := 65536
			if row.limit > frameCap {
				frameCap = row.limit + 16<<10
			}
			ctx, cancel := context.WithTimeout(context.Background(), waitFor)
			defer cancel()
			go func() {
				_, _ = client.RPC(ctx, string(clientlink.MethodSessionCreate), createDataOfSize(frameCap+1))
			}()
			drop := await(t, connectingDrops(observed), "transport close after oversized frame")
			if drop.Code == 0 {
				t.Fatal("saw initial connecting event, not a transport close")
			}
		})
	}
}

func TestLargeCreateRPCIsAcceptedWhenLimitRaised(t *testing.T) {
	limits := testLimits()
	limits.MaxMessageBytes = 8 << 20
	f := newFixture(t, limits)
	f.admitter.mu.Lock()
	f.admitter.entry = acceptedEntry("session-large", "cmd-large")
	f.admitter.mu.Unlock()
	client, observed := dialSupported(t, f, "token-a")
	await(t, observed.connected, "connected")
	ctx, cancel := context.WithTimeout(context.Background(), waitFor)
	defer cancel()
	reply, err := client.RPC(ctx, string(clientlink.MethodSessionCreate), createDataOfSize(300<<10))
	if err != nil {
		t.Fatalf("RPC: %v", err)
	}
	if got := statusOf(t, reply.Data).CommandID; got != "cmd-large" {
		t.Fatalf("accepted command %q", got)
	}
}
