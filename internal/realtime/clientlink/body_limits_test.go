package clientlink_test

import (
	"context"
	"strings"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/clientlink"
)

func TestLargeCreateRPCReceivesTypedLimitRefusalAndCanBeRaised(t *testing.T) {
	data := []byte(`{"version":1,"command_id":"cmd-large","session_id":"session-large","agent_id":"agent-a","blocks":[{"text":"` + strings.Repeat("x", 300<<10) + `"}]}`)
	for _, row := range []struct {
		name     string
		max      int
		accepted bool
	}{
		{"default", 64 << 10, false},
		{"raised", 8 << 20, true},
	} {
		t.Run(row.name, func(t *testing.T) {
			limits := testLimits()
			limits.MaxMessageBytes = row.max
			f := newFixture(t, limits)
			f.admitter.mu.Lock()
			f.admitter.entry = acceptedEntry("session-large", "cmd-large")
			f.admitter.mu.Unlock()
			client, observed := dialSupported(t, f, "token-a")
			await(t, observed.connected, "connected")
			ctx, cancel := context.WithTimeout(context.Background(), waitFor)
			defer cancel()
			reply, err := client.RPC(ctx, string(clientlink.MethodSessionCreate), data)
			if err != nil {
				t.Fatalf("RPC: %v", err)
			}
			if row.accepted {
				if got := statusOf(t, reply.Data).CommandID; got != "cmd-large" {
					t.Fatalf("accepted command %q", got)
				}
			} else if got := envelopeCode(t, reply.Data); got != sessionwire.ErrorCodeInvalidRequest {
				t.Fatalf("refusal = %q", got)
			}
		})
	}
}
