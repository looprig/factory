package httpapi

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// capturedLog records every slog record it is handed.
type capturedLog struct {
	mu      sync.Mutex
	records []slog.Record
}

func (c *capturedLog) Enabled(context.Context, slog.Level) bool { return true }
func (c *capturedLog) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, r.Clone())
	return nil
}
func (c *capturedLog) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *capturedLog) WithGroup(string) slog.Handler      { return c }

func (c *capturedLog) snapshot() []slog.Record {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]slog.Record(nil), c.records...)
}

// TestAPanickingDeliveryIsLoggedNotSwallowed is v0.7.1 gate finding L3. The
// wake recovers a panicking delivery seam, which is right -- a best-effort wake
// must not end the process -- but it did so silently, so a buggy seam was
// invisible. One ERROR record now names the session, the command and the
// panic value.
func TestAPanickingDeliveryIsLoggedNotSwallowed(t *testing.T) {
	t.Parallel()

	captured := &capturedLog{}
	f := newFixture(t, withDelivery(deliveryFunc(func(context.Context) { panic("delivery seam exploded") })),
		func(cfg *RouterConfig, _ *fixture) { cfg.Logger = slog.New(captured) })
	w := f.router.wakes
	if !w.schedule(fixtureTenant, fixtureSession, "command-a") {
		t.Fatal("the wake was refused")
	}
	if err := w.stop(context.Background()); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for len(captured.snapshot()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	records := captured.snapshot()
	if len(records) != 1 {
		t.Fatalf("logged %d records, want exactly one for the recovered panic", len(records))
	}
	record := records[0]
	if record.Level != slog.LevelError {
		t.Errorf("level = %v, want ERROR", record.Level)
	}
	attrs := map[string]string{}
	record.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.String()
		return true
	})
	for key, want := range map[string]string{
		"tenant_id":  string(fixtureTenant),
		"session_id": string(fixtureSession),
		"command_id": "command-a",
		"panic":      "delivery seam exploded",
	} {
		if got := attrs[key]; !strings.Contains(got, want) {
			t.Errorf("attr %s = %q, want %q (record %q %v)", key, got, want, record.Message, attrs)
		}
	}
}

// TestAWakeThatDoesNotPanicLogsNothing keeps the log line about the panic: an
// ordinary wake, successful or failed, is not an event anybody pages on.
func TestAWakeThatDoesNotPanicLogsNothing(t *testing.T) {
	t.Parallel()

	captured := &capturedLog{}
	f := newFixture(t, withDelivery(deliveryFunc(func(context.Context) {})),
		func(cfg *RouterConfig, _ *fixture) { cfg.Logger = slog.New(captured) })
	if !f.router.wakes.schedule(fixtureTenant, fixtureSession, "command-a") {
		t.Fatal("the wake was refused")
	}
	if err := f.router.wakes.stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := len(captured.snapshot()); n != 0 {
		t.Fatalf("logged %d records for an ordinary wake, want none", n)
	}
}
