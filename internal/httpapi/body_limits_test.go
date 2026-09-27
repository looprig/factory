package httpapi

import (
	"net/http"
	"strings"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

func TestLargeCreateBodyUsesConfiguredCommandLimit(t *testing.T) {
	for _, row := range []struct {
		name string
		max  int64
		size int
		want int
	}{
		{"300 KiB at default", 1 << 20, 300 << 10, http.StatusCreated},
		{"over default", 1 << 20, 1200 << 10, http.StatusRequestEntityTooLarge},
		{"raised", 8 << 20, 1200 << 10, http.StatusCreated},
	} {
		t.Run(row.name, func(t *testing.T) {
			f := newFixture(t, withAdmitter(newFakeAdmitter()), withLimits(RouteLimits{MaxRequestBytes: 1 << 20, MaxCommandBytes: row.max, RequestTimeout: time.Second}))
			body := `{"version":1,"command_id":"cmd-large","session_id":"session-large","agent_id":"agent-a","blocks":[{"text":"` + strings.Repeat("x", row.size) + `"}]}`
			reader := strings.NewReader(body)
			response := f.serve(request(http.MethodPost, "/v1/sessions", reader))
			if response.Code != row.want {
				t.Fatalf("create = %d: %s", response.Code, response.Body.String())
			}
			if row.want == http.StatusRequestEntityTooLarge && decodeEnvelope(t, response).Error.Code != sessionwire.ErrorCodeInvalidRequest {
				t.Fatalf("oversized create = %s, want invalid_request", response.Body.String())
			}
			if consumed := len(body) - reader.Len(); consumed > int(row.max)+1 {
				t.Fatalf("read %d bytes before refusing body limited to %d", consumed, row.max)
			}
		})
	}
}
