package transport_test

import (
	"bytes"
	"context"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge"
)

// eagleAggregateFrame is the frame of the per-Node metrics aggregator
// goroutine. The open parenthesis keeps it from also matching aggregateOnce,
// which that same goroutine calls on every tick.
const eagleAggregateFrame = "github.com/FZambia/eagle.(*Eagle).aggregate("

// eagleAggregators counts the goroutines currently running an aggregator loop.
// debug=2 prints every goroutine on its own, so each one contributes exactly
// one occurrence of the frame.
func eagleAggregators(t *testing.T) int {
	t.Helper()
	var dump bytes.Buffer
	if err := pprof.Lookup("goroutine").WriteTo(&dump, 2); err != nil {
		t.Fatalf("goroutine profile: %v", err)
	}
	return strings.Count(dump.String(), eagleAggregateFrame)
}

// TestNodeShutdownLeavesNoMetricsAggregatorRunning measures the Shutdown leak
// that the pin move to centrifuge v0.39.x exists to leave behind.
//
// centrifuge v0.38.0 starts an eagle metrics aggregator for EVERY Node
// (node.go initMetrics: eagle.New, whose loop exits only on Eagle.Close) and
// Node.Shutdown never closed it, so each Node leaked one goroutine, a 60s
// ticker loop, for the life of the process. Factory builds a Node per
// ClientLink handler and the tests lane starts and stops many of them, which is
// why that lane carried an allowance that counted these goroutines instead of
// failing on them. v0.39.0 closes the exporter in Shutdown
// (centrifuge@v0.39.3/node.go:438-442).
//
// This case is the guard that replaces the allowance: it fails at v0.38.0 (the
// count stays raised by one per Node) and passes at v0.39.3. It is NOT
// parallel, because every parallel case in this package builds Nodes of its
// own and their aggregators would move the count; Go runs a package's
// sequential top-level tests while its parallel ones are still paused.
//
// The control comes first: while the Nodes run, the count must be raised by at
// least one per Node, so a probe that could not see an aggregator at all cannot
// pass the leak assertion for the wrong reason.
func TestNodeShutdownLeavesNoMetricsAggregatorRunning(t *testing.T) {
	const nodes = 3
	before := eagleAggregators(t)

	started := make([]*centrifuge.Node, 0, nodes)
	for i := 0; i < nodes; i++ {
		node, err := centrifuge.New(centrifuge.Config{LogLevel: centrifuge.LogLevelNone})
		if err != nil {
			t.Fatalf("centrifuge.New: %v", err)
		}
		node.OnConnecting(func(context.Context, centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
			return centrifuge.ConnectReply{}, nil
		})
		if err := node.Run(); err != nil {
			t.Fatalf("Node.Run: %v", err)
		}
		started = append(started, node)
	}

	if running := eagleAggregators(t); running < before+nodes {
		t.Fatalf("with %d Nodes running the probe sees %d aggregators (baseline %d); it cannot see what it is meant to count", nodes, running, before)
	}

	for _, node := range started {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := node.Shutdown(ctx); err != nil {
			cancel()
			t.Fatalf("Node.Shutdown: %v", err)
		}
		cancel()
	}

	// The aggregator observes its close channel in a select, so it exits
	// promptly; the deadline only absorbs scheduling.
	deadline := time.Now().Add(5 * time.Second)
	for {
		after := eagleAggregators(t)
		if after <= before {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d Nodes were shut down and %d metrics aggregators outlive them (baseline %d, now %d): Node.Shutdown leaks its eagle exporter",
				nodes, after-before, before, after)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
