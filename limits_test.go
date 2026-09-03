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
		"ReconcileLimits":  DefaultReconcileLimits().Validate(),
		"ClientLinkLimits": DefaultClientLinkLimits().Validate(),
		"HostLinkLimits":   DefaultHostLinkLimits().Validate(),
	} {
		if err != nil {
			t.Errorf("Default%s() is itself rejected: %v", name, err)
		}
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
