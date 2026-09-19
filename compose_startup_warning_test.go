package factory_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/looprig/factory"
	"github.com/looprig/sessionstore"
)

// TestAReplicaThatWillPlaceNothingSaysSoAtStart is the B5 spec gate's F1: a
// composition without WithPendingCommands places no session on a Host, and it
// used to do so in silence. Start now logs exactly one line naming that, at
// WARN, and at ERROR when the replica also admits creates it will never place.
// A composition WITH the option logs neither.
func TestAReplicaThatWillPlaceNothingSaysSoAtStart(t *testing.T) {
	t.Parallel()

	const (
		plain   = "factory: WithPendingCommands is not composed, so this replica places no session on a Host"
		creates = "factory: WithPublicCreates is composed but WithPendingCommands is not, so this replica admits creates it will never place on a Host"
	)
	createPlane := []factory.Option{
		factory.WithObjectStoreResolver(func(context.Context, sessionstore.SessionBinding) (factory.ObjectReader, error) {
			return nil, errProbeUnavailable
		}),
		factory.WithSessionBinding("storage-a", "v1"),
		factory.WithPublicCreates(stubPublicCreates{}),
	}
	for _, test := range []struct {
		name    string
		options []factory.Option
		level   string
		message string
	}{
		{"neither option", nil, "WARN", plain},
		{"creates without placement", createPlane, "ERROR", creates},
		{"placement composed", []factory.Option{factory.WithPendingCommands(factory.FakeSeams{})}, "", ""},
		{"creates and placement", append(append([]factory.Option(nil), createPlane...), factory.WithPendingCommands(factory.FakeSeams{})), "", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			logs := &syncBuffer{}
			options := append(factory.RequiredOptions(), test.options...)
			options = append(options, factory.WithLogger(slog.New(slog.NewJSONHandler(logs, nil))))
			server, err := factory.New(options...)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			if err := server.Start(context.Background()); err != nil {
				t.Fatalf("Start: %v", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := server.Stop(ctx); err != nil {
				t.Fatalf("Stop: %v", err)
			}
			var found []map[string]any
			for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
				if line == "" {
					continue
				}
				var record map[string]any
				if err := json.Unmarshal([]byte(line), &record); err != nil {
					t.Fatalf("unreadable log line %q: %v", line, err)
				}
				if msg, _ := record["msg"].(string); msg == plain || msg == creates {
					found = append(found, record)
				}
			}
			if test.message == "" {
				if len(found) != 0 {
					t.Fatalf("a composition that places logged %v", found)
				}
				return
			}
			if len(found) != 1 || found[0]["msg"] != test.message || found[0]["level"] != test.level {
				t.Fatalf("startup lines = %v, want exactly one %s %q", found, test.level, test.message)
			}
		})
	}
}
