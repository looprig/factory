//go:build transportscale

// This file is behind a build tag because it is a LOAD case, not a correctness
// case, and it belongs in a deliberate run rather than in every `make check`.
//
//	GOWORK=off go test -tags transportscale -timeout 20m ./internal/realtime/transport/
//
// It makes NO durability and NO correctness claim. It asserts exactly one
// thing: that this many browser-shaped connections can be established,
// authenticated, subscribed and fanned out to concurrently against ONE embedded
// node. The multi-service soak, with real sessions behind it, belongs in the
// `tests` integration lane and not here.
//
// The default is the runbook's 5,000, which is the top of the range a Factory
// replica is expected to hold. It is lowered with an environment variable
// rather than a flag so a constrained machine can run a smaller, honestly
// labelled version of the same case:
//
//	LOOPRIG_TRANSPORT_CONNECTIONS=1000 GOWORK=off go test -tags transportscale ...
//
// Note that each connection is a file descriptor at BOTH ends of the loopback,
// so a 5,000-connection run needs an open-file limit above 20,000. A run that
// dies on "too many open files" is a machine limit, not a transport finding.
package transport_test

import (
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	centrifugego "github.com/centrifugal/centrifuge-go"
)

// scaleConnections is the runbook's figure unless the environment lowers it.
func scaleConnections(t *testing.T) int {
	t.Helper()

	const runbookConnections = 5000
	raw, ok := os.LookupEnv("LOOPRIG_TRANSPORT_CONNECTIONS")
	if !ok {
		return runbookConnections
	}
	count, err := strconv.Atoi(raw)
	if err != nil || count < 1 {
		t.Fatalf("LOOPRIG_TRANSPORT_CONNECTIONS=%q is not a positive count", raw)
	}
	if count != runbookConnections {
		// A reduced run is still a run, but it must not be reported as the
		// runbook's. Saying so in the log is the whole of the honesty here.
		t.Logf("running at %d connections, which is BELOW the runbook's %d; this is not the 5,000-connection result",
			count, runbookConnections)
	}
	return count
}

func TestManyConcurrentConnections(t *testing.T) {
	connections := scaleConnections(t)

	server := newServer(t, serverOptions{})
	const channel = "session:tenant-a:fanout"

	var connectedCount atomic.Int64
	received := make(chan struct{}, connections)
	for range connections {
		client := centrifugego.NewJsonClient(server.url, centrifugego.Config{
			Token: goodToken, Name: clientName, Version: clientVersionSent,
		})
		t.Cleanup(client.Close)
		client.OnConnected(func(centrifugego.ConnectedEvent) { connectedCount.Add(1) })
		if err := client.Connect(); err != nil {
			t.Fatalf("Connect: %v", err)
		}
		sub, err := client.NewSubscription(channel)
		if err != nil {
			t.Fatalf("NewSubscription: %v", err)
		}
		var once sync.Once
		sub.OnPublication(func(centrifugego.PublicationEvent) { once.Do(func() { received <- struct{}{} }) })
		if err := sub.Subscribe(); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
	}

	// Publish until every connection has been delivered to at least once.
	// Republishing removes the race between subscribing and publishing without
	// asserting anything about how long either takes.
	done := make(chan struct{})
	var publisher sync.WaitGroup
	publisher.Add(1)
	go func() {
		defer publisher.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			if _, err := server.node.Publish(channel, []byte(`{"fanout":true}`)); err != nil {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	const scaleWait = 10 * time.Minute
	deadline := time.After(scaleWait)
	for i := range connections {
		select {
		case <-received:
		case <-deadline:
			close(done)
			publisher.Wait()
			t.Fatalf("only %d of %d connections were delivered to (%d reported connected) within %v",
				i, connections, connectedCount.Load(), scaleWait)
		}
	}
	close(done)
	publisher.Wait()

	if got := connectedCount.Load(); got != int64(connections) {
		t.Errorf("%d connections reported connected, want %d", got, connections)
	}
}
