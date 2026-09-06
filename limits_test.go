package factory

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// The three limit tables share one shape, and the shape is the point. Each row
// mutates ONE field of a default that is asserted valid first, so a rejection
// is attributable to that field. A row asserting only "some error" would pass
// on a validator that rejected everything, so each names the field it expects
// the message to blame.

func TestDefaultLimitsAreValid(t *testing.T) {
	t.Parallel()

	for name, err := range map[string]error{
		"HTTPLimits":       DefaultHTTPLimits().Validate(),
		"ReconcileLimits":  DefaultReconcileLimits().Validate(),
		"ClientLinkLimits": DefaultClientLinkLimits().Validate(),
		"HostLinkLimits":   DefaultHostLinkLimits().Validate(),
	} {
		if err != nil {
			t.Errorf("Default%s() is itself rejected: %v", name, err)
		}
	}
}

func TestHTTPLimitsValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(*HTTPLimits)
		wantErr string
	}{
		{"zero header deadline", func(l *HTTPLimits) { l.ReadHeaderTimeout = 0 }, "ReadHeaderTimeout"},
		{"negative header deadline", func(l *HTTPLimits) { l.ReadHeaderTimeout = -time.Second }, "ReadHeaderTimeout"},
		{"zero idle window", func(l *HTTPLimits) { l.IdleTimeout = 0 }, "IdleTimeout"},
		{"negative idle window", func(l *HTTPLimits) { l.IdleTimeout = -time.Second }, "IdleTimeout"},
		// A header ceiling below one realistic header block rejects every
		// request rather than bounding an abusive one, so it is held above a
		// floor rather than merely above zero.
		{"no header budget", func(l *HTTPLimits) { l.MaxHeaderBytes = 0 }, "MaxHeaderBytes"},
		{"negative header budget", func(l *HTTPLimits) { l.MaxHeaderBytes = -1 }, "MaxHeaderBytes"},
		{"header budget just below the floor", func(l *HTTPLimits) { l.MaxHeaderBytes = minHeaderBytes - 1 }, "MaxHeaderBytes"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			limits := DefaultHTTPLimits()
			tt.mutate(&limits)
			assertRejected(t, limits.Validate(), limits, tt.wantErr)
		})
	}
}

// TestHTTPHeaderBudgetFloorIsInclusive holds the boundary the table above only
// approaches: the floor itself is accepted, so the rule is "at least" rather
// than "more than".
func TestHTTPHeaderBudgetFloorIsInclusive(t *testing.T) {
	t.Parallel()

	limits := DefaultHTTPLimits()
	limits.MaxHeaderBytes = minHeaderBytes
	if err := limits.Validate(); err != nil {
		t.Errorf("MaxHeaderBytes at the floor was rejected: %v", err)
	}
}

func TestReconcileLimitsValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(*ReconcileLimits)
		wantErr string
	}{
		{"zero interval", func(l *ReconcileLimits) { l.Interval = 0 }, "Interval"},
		{"negative interval", func(l *ReconcileLimits) { l.Interval = -time.Second }, "Interval"},
		{"zero claim TTL", func(l *ReconcileLimits) { l.ClaimTTL = 0 }, "ClaimTTL"},
		{"zero apply deadline", func(l *ReconcileLimits) { l.ApplyDeadline = 0 }, "ApplyDeadline"},
		{"no due budget", func(l *ReconcileLimits) { l.MaxDuePerSweep = 0 }, "MaxDuePerSweep"},
		{"negative due budget", func(l *ReconcileLimits) { l.MaxDuePerSweep = -1 }, "MaxDuePerSweep"},
		{"no concurrency", func(l *ReconcileLimits) { l.MaxConcurrent = 0 }, "MaxConcurrent"},
		// A sweep cadence at or beyond the claim TTL means every claim this
		// replica took has expired by the time the next sweep looks at it, so
		// claims stop suppressing duplicate placement work entirely.
		{"cadence equals the claim TTL", func(l *ReconcileLimits) { l.Interval = l.ClaimTTL }, "ClaimTTL"},
		{"cadence beyond the claim TTL", func(l *ReconcileLimits) { l.Interval = l.ClaimTTL + time.Second }, "ClaimTTL"},
		// A claim never extends a command's apply deadline, so a TTL at or
		// beyond the deadline describes a claim that outlives the command it
		// was taken for.
		{"claim TTL equals the apply deadline", func(l *ReconcileLimits) { l.ClaimTTL = l.ApplyDeadline }, "ApplyDeadline"},
		{"claim TTL beyond the apply deadline", func(l *ReconcileLimits) { l.ClaimTTL = l.ApplyDeadline + time.Second }, "ApplyDeadline"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			limits := DefaultReconcileLimits()
			tt.mutate(&limits)
			assertRejected(t, limits.Validate(), limits, tt.wantErr)
		})
	}
}

func TestClientLinkLimitsValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(*ClientLinkLimits)
		wantErr string
	}{
		{"no connections", func(l *ClientLinkLimits) { l.MaxConnections = 0 }, "MaxConnections"},
		{"no queue", func(l *ClientLinkLimits) { l.PerConnectionQueue = 0 }, "PerConnectionQueue"},
		{"negative queue", func(l *ClientLinkLimits) { l.PerConnectionQueue = -1 }, "PerConnectionQueue"},
		{"zero write timeout", func(l *ClientLinkLimits) { l.WriteTimeout = 0 }, "WriteTimeout"},
		{"zero ping interval", func(l *ClientLinkLimits) { l.PingInterval = 0 }, "PingInterval"},
		{"zero pong timeout", func(l *ClientLinkLimits) { l.PongTimeout = 0 }, "PongTimeout"},
		// A pong deadline at or beyond the ping cadence never separates a slow
		// peer from a dead one: the next ping is sent before the previous one's
		// deadline has been reached.
		{"pong deadline equals the ping cadence", func(l *ClientLinkLimits) { l.PongTimeout = l.PingInterval }, "PingInterval"},
		{"pong deadline beyond the ping cadence", func(l *ClientLinkLimits) { l.PongTimeout = l.PingInterval + time.Second }, "PingInterval"},
		// A write allowed to block past the liveness deadline holds the
		// connection the deadline exists to reclaim.
		{"write timeout beyond the pong deadline", func(l *ClientLinkLimits) { l.WriteTimeout = l.PongTimeout + time.Second }, "PongTimeout"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			limits := DefaultClientLinkLimits()
			tt.mutate(&limits)
			assertRejected(t, limits.Validate(), limits, tt.wantErr)
		})
	}
}

// TestClientLinkWriteTimeoutMayEqualThePongDeadline pins the boundary the row
// above stops one step short of, so "beyond" cannot quietly become "at or
// beyond" without a test noticing.
func TestClientLinkWriteTimeoutMayEqualThePongDeadline(t *testing.T) {
	t.Parallel()

	limits := DefaultClientLinkLimits()
	limits.WriteTimeout = limits.PongTimeout
	if err := limits.Validate(); err != nil {
		t.Fatalf("WriteTimeout == PongTimeout was rejected: %v", err)
	}
}

func TestHostLinkLimitsValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(*HostLinkLimits)
		wantErr string
	}{
		{"no links", func(l *HostLinkLimits) { l.MaxLinks = 0 }, "MaxLinks"},
		{"zero dial timeout", func(l *HostLinkLimits) { l.DialTimeout = 0 }, "DialTimeout"},
		{"zero idle timeout", func(l *HostLinkLimits) { l.IdleTimeout = 0 }, "IdleTimeout"},
		{"zero reconnect floor", func(l *HostLinkLimits) { l.ReconnectMin = 0 }, "ReconnectMin"},
		{"zero reconnect ceiling", func(l *HostLinkLimits) { l.ReconnectMax = 0 }, "ReconnectMax"},
		{"inverted backoff", func(l *HostLinkLimits) { l.ReconnectMin = l.ReconnectMax + time.Second }, "ReconnectMax"},
		// A link is opened on local subscription demand. If a dial may take
		// longer than the idle window, a link can become reapable before it has
		// ever served the demand that opened it.
		{"dial beyond the idle window", func(l *HostLinkLimits) { l.DialTimeout = l.IdleTimeout + time.Second }, "IdleTimeout"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			limits := DefaultHostLinkLimits()
			tt.mutate(&limits)
			assertRejected(t, limits.Validate(), limits, tt.wantErr)
		})
	}
}

// TestHostLinkBoundariesAreInclusive pins the two relationships that are
// "at most", one step from the rows that reject.
func TestHostLinkBoundariesAreInclusive(t *testing.T) {
	t.Parallel()

	equalBackoff := DefaultHostLinkLimits()
	equalBackoff.ReconnectMin = equalBackoff.ReconnectMax
	if err := equalBackoff.Validate(); err != nil {
		t.Errorf("ReconnectMin == ReconnectMax was rejected: %v", err)
	}
	equalDial := DefaultHostLinkLimits()
	equalDial.DialTimeout = equalDial.IdleTimeout
	if err := equalDial.Validate(); err != nil {
		t.Errorf("DialTimeout == IdleTimeout was rejected: %v", err)
	}
}

func assertRejected(t *testing.T, err error, limits any, wantErr string) {
	t.Helper()

	if err == nil {
		t.Fatalf("Validate() = nil for %+v, want an error naming %q", limits, wantErr)
	}
	if !errors.Is(err, ErrInvalidLimits) {
		t.Errorf("error %v does not wrap ErrInvalidLimits", err)
	}
	if !strings.Contains(err.Error(), wantErr) {
		t.Errorf("error %q does not name %q", err, wantErr)
	}
}

// TestClientLinkPingIntervalMustSurviveTheWire is the inherited A5.1 finding
// turned into a guard.
//
// The wire carries the ping cadence as a WHOLE NUMBER OF SECONDS:
// centrifuge@v0.38.0/client.go:2466 computes res.Ping =
// uint32(c.pingInterval.Seconds()), so anything under a second truncates to 0.
// A client told Ping == 0 takes the res.Pong assignment inside
// `if res.Ping > 0` (centrifuge-go@v0.12.0/client.go:1467-1474) and therefore
// never pongs at all, so the server closes a HEALTHY connection with
// DisconnectNoPong every pong timeout. Silently flooring the value would leave
// the deployer reading that reconnect loop as a network fault, so the
// composition refuses it instead.
//
// Every row here keeps PongTimeout and WriteTimeout coherent with the interval,
// which is the whole reason this is a separate case: with the DEFAULT ten
// second pong deadline, a 500ms interval is already refused by the
// PongTimeout-before-PingInterval rule, and a row that leaned on that would
// pass against a validator that had never heard of the wire at all.
func TestClientLinkPingIntervalMustSurviveTheWire(t *testing.T) {
	t.Parallel()

	// coherent builds limits whose pong deadline and write timeout are both
	// well inside interval, so PingInterval is the only field under test.
	coherent := func(interval time.Duration) ClientLinkLimits {
		limits := DefaultClientLinkLimits()
		limits.PingInterval = interval
		limits.PongTimeout = interval / 3
		limits.WriteTimeout = interval / 4
		return limits
	}

	t.Run("rejected below one second", func(t *testing.T) {
		t.Parallel()

		// Absolute literals. 999ms is the last value below the boundary and
		// 500ms is the value the finding was measured at.
		for _, interval := range []time.Duration{time.Millisecond, 500 * time.Millisecond, 999 * time.Millisecond} {
			limits := coherent(interval)
			err := limits.Validate()
			assertRejected(t, err, limits, "PingInterval")
			if err == nil {
				continue
			}
			// The ordering rules are the near neighbours this rejection must
			// not be confused with. Naming PingInterval is not enough on its
			// own: the PongTimeout-before-PingInterval message names it too.
			if strings.Contains(err.Error(), "PongTimeout") {
				t.Errorf("PingInterval = %v was refused by a message blaming PongTimeout: %q", interval, err)
			}
			if !strings.Contains(err.Error(), "1s") {
				t.Errorf("error %q for PingInterval = %v does not name the 1s floor", err, interval)
			}
		}
	})

	t.Run("accepted at one second and above", func(t *testing.T) {
		t.Parallel()

		// One second is the boundary itself, written as an absolute literal
		// rather than as MinClientLinkPingInterval: a fixture built from the
		// constant would move with it and pin nothing.
		for _, interval := range []time.Duration{time.Second, 1500 * time.Millisecond, 25 * time.Second} {
			limits := coherent(interval)
			if err := limits.Validate(); err != nil {
				t.Errorf("PingInterval = %v was rejected: %v", interval, err)
			}
		}
	})
}

// TestMinClientLinkPingIntervalIsOneSecond pins the exported boundary itself.
// Without it the constant could move and every row above would move with the
// behaviour it describes.
func TestMinClientLinkPingIntervalIsOneSecond(t *testing.T) {
	t.Parallel()

	if MinClientLinkPingInterval != time.Second {
		t.Errorf("MinClientLinkPingInterval = %v, want 1s", MinClientLinkPingInterval)
	}
}
