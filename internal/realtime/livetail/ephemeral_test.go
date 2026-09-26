package livetail_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/livetail"
	"github.com/looprig/factory/internal/routing"
)

func ephemeral(t *testing.T, text string) []byte {
	t.Helper()
	body, err := json.Marshal(struct {
		V     int    `json:"v"`
		Type  string `json:"type"`
		Chunk struct {
			Type string `json:"chunk_type"`
			Text string `json:"text"`
		} `json:"chunk"`
	}{V: 1, Type: "TokenDelta", Chunk: struct {
		Type string `json:"chunk_type"`
		Text string `json:"text"`
	}{Type: "text", Text: text}})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := (sessionwire.EphemeralPublication{TenantID: tenantA, SessionID: session, Body: body}).MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

type blockedReceiveRelay struct {
	recordingRelay
	gate    chan struct{}
	entered chan struct{}
}

func (r *blockedReceiveRelay) Receive(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, frame routing.Frame) error {
	select {
	case r.entered <- struct{}{}:
	default:
	}
	<-r.gate
	return r.recordingRelay.Receive(ctx, tenant, session, frame)
}

func TestEnduringArrivalEvictsAllAvailableEphemeralsBeforeLosingTail(t *testing.T) {
	t.Parallel()
	links := &scriptedLinks{}
	relay := &blockedReceiveRelay{gate: make(chan struct{}), entered: make(chan struct{}, 1)}
	plane, err := livetail.New(livetail.Config{
		Links: links, Viewers: func() livetail.Viewers { return nil }, MailboxLimit: 2, EventTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	plane.Attach(relay, nil)
	var once sync.Once
	open := func() { once.Do(func() { close(relay.gate) }) }
	t.Cleanup(func() {
		open()
		_ = plane.Close(context.Background())
	})
	plane.Watching(tenantA, session)
	if err := plane.Bind(context.Background(), "ws://host-1", sessionwire.HostLinkBindRequest{TenantID: tenantA, SessionID: session, HostID: "host-1"}); err != nil {
		t.Fatal(err)
	}
	plane.Served(tenantA, session)
	sink := links.sink(0)
	sink.Publication(enduring(t, tenantA, session, 1))
	select {
	case <-relay.entered:
	case <-time.After(waitFor):
		t.Fatal("first frame never reached the blocked relay")
	}
	sink.Publication(ephemeral(t, "a"))
	sink.Publication(ephemeral(t, "b"))
	sink.Restored() // control callbacks can put events beyond MailboxLimit
	sink.Publication(enduring(t, tenantA, session, 2))
	frames, lost := livetail.QueuedFrames(plane, tenantA, session)
	if lost || len(frames) != 1 || string(frames[0]) != string(enduring(t, tenantA, session, 2)) {
		t.Fatalf("queued=%q lost=%t, want only the second enduring frame and control", frames, lost)
	}
	if got := plane.DroppedEphemerals(); got != 2 {
		t.Fatalf("dropped %d ephemerals, want both queued victims", got)
	}
	open()
	eventually(t, "the enduring frames to reach the relay", func() bool { return len(relay.got()) == 2 })
	if got := relay.got(); len(got) != 2 || got[0] != string(enduring(t, tenantA, session, 1)) || got[1] != string(enduring(t, tenantA, session, 2)) {
		t.Fatalf("relay received %q, want the two enduring frames", got)
	}
}

func TestHostLinkEphemeralReachesClientLinkUnchangedInOrder(t *testing.T) {
	t.Parallel()
	host := newStandIn(t, "host-1")
	r := newRig(t, rigOptions{})
	r.dir.put(host.observation(tenantA, session, 3))
	r.watch(t, tenantA, session)
	channel := sessionwire.HostLinkChannel(tenantA, session)
	r.awaitTail(t, host, channel)
	want := [][]byte{enduring(t, tenantA, session, 1), ephemeral(t, "first "), ephemeral(t, "second"), enduring(t, tenantA, session, 2)}
	for _, frame := range want {
		host.publish(t, channel, frame)
	}
	r.wait(t, "the ordered mixed stream", func() bool { return len(r.viewers.of(tenantA, session)) == len(want) })
	for i, got := range r.viewers.of(tenantA, session) {
		if got != string(want[i]) {
			t.Fatalf("frame %d = %s, want HostLink bytes %s", i, got, want[i])
		}
	}
}

func TestSlowViewerDropsEphemeralsWithoutLosingEnduringCoverage(t *testing.T) {
	t.Parallel()
	host := newStandIn(t, "host-1")
	r := newRig(t, rigOptions{mailbox: 2})
	r.dir.put(host.observation(tenantA, session, 3))
	r.watch(t, tenantA, session)
	channel := sessionwire.HostLinkChannel(tenantA, session)
	r.awaitTail(t, host, channel)

	gate := make(chan struct{})
	var once sync.Once
	open := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(open)
	entered := make(chan struct{}, 1)
	r.viewers.mu.Lock()
	r.viewers.gate, r.viewers.entered = gate, entered
	r.viewers.mu.Unlock()
	host.publish(t, channel, enduring(t, tenantA, session, 1))
	select {
	case <-entered:
	case <-time.After(waitFor):
		t.Fatal("first enduring frame never entered the blocked viewer")
	}
	for i := 1; i <= 2; i++ {
		host.publish(t, channel, ephemeral(t, "preview"))
		r.wait(t, "the ephemeral mailbox to fill", func() bool {
			n, _ := livetail.Pending(r.plane, tenantA, session)
			return n == i
		})
	}
	host.publish(t, channel, ephemeral(t, "dropped"))
	for seq := uint64(2); seq <= 3; seq++ {
		host.publish(t, channel, enduring(t, tenantA, session, seq))
	}
	r.wait(t, "the mailbox to accept or lose the enduring frames", func() bool {
		frames, lost := livetail.QueuedFrames(r.plane, tenantA, session)
		if lost {
			return true
		}
		return len(frames) == 2 && string(frames[0]) == string(enduring(t, tenantA, session, 2)) &&
			string(frames[1]) == string(enduring(t, tenantA, session, 3))
	})
	if _, lost := livetail.QueuedFrames(r.plane, tenantA, session); lost {
		t.Fatal("dropping an ephemeral marked the tail lost")
	}
	if got := r.plane.DroppedEphemerals(); got != 3 {
		t.Fatalf("dropped ephemeral count = %d, want one arrival and two evictions", got)
	}
	r.viewers.mu.Lock()
	r.viewers.gate = nil
	r.viewers.mu.Unlock()
	open()
	r.wait(t, "all enduring frames after the slow viewer resumes", func() bool {
		return len(r.viewers.of(tenantA, session)) == 3
	})
	if got := joined(kinds(t, r.viewers.of(tenantA, session))); got != "E1 E2 E3" {
		t.Fatalf("viewer received %s, want E1 E2 E3 without a reset or ephemeral coverage", got)
	}
	if got := host.count("subscribe", channel); got != 1 {
		t.Fatalf("HostLink subscribed %d times, want the original uninterrupted tail", got)
	}
}

func testInvalidEphemeralAtCapacity(t *testing.T, invalid []byte, message string) {
	t.Helper()
	host := newStandIn(t, "host-1")
	logs := &syncBuffer{}
	r := newRig(t, rigOptions{mailbox: 1, logger: slog.New(slog.NewTextHandler(logs, nil))})
	r.dir.put(host.observation(tenantA, session, 3))
	r.watch(t, tenantA, session)
	channel := sessionwire.HostLinkChannel(tenantA, session)
	r.awaitTail(t, host, channel)
	gate := make(chan struct{})
	var once sync.Once
	open := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(open)
	entered := make(chan struct{}, 1)
	r.viewers.mu.Lock()
	r.viewers.gate, r.viewers.entered = gate, entered
	r.viewers.mu.Unlock()
	host.publish(t, channel, enduring(t, tenantA, session, 1))
	select {
	case <-entered:
	case <-time.After(waitFor):
		t.Fatal("first enduring frame never entered the blocked viewer")
	}
	host.publish(t, channel, ephemeral(t, "queued"))
	r.wait(t, "the ephemeral mailbox to fill", func() bool {
		n, _ := livetail.Pending(r.plane, tenantA, session)
		return n == 1
	})
	host.publish(t, channel, invalid)
	r.wait(t, "the invalid frame to be retained for validation", func() bool {
		frames, lost := livetail.QueuedFrames(r.plane, tenantA, session)
		return lost || len(frames) == 1 && string(frames[0]) == string(invalid)
	})
	frames, lost := livetail.QueuedFrames(r.plane, tenantA, session)
	if lost || len(frames) != 1 || string(frames[0]) != string(invalid) {
		t.Fatalf("invalid frame was dropped at capacity: queued=%q lost=%t", frames, lost)
	}
	r.viewers.mu.Lock()
	r.viewers.gate = nil
	r.viewers.mu.Unlock()
	open()
	r.wait(t, "the invalid frame to trigger repair", func() bool {
		return strings.Contains(logs.String(), message)
	})
	if strings.Contains(logs.String(), "private-body") {
		t.Fatal("foreign record body appeared in logs")
	}
	for _, got := range r.viewers.of(tenantA, session) {
		if got == string(invalid) {
			t.Fatal("invalid ephemeral reached a viewer")
		}
	}
}

func TestForeignEphemeralAtCapacityStillRepairs(t *testing.T) {
	t.Parallel()
	foreign, err := (sessionwire.EphemeralPublication{TenantID: tenantB, SessionID: session, Body: json.RawMessage(`{"text":"private-body"}`)}).MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	testInvalidEphemeralAtCapacity(t, foreign, "named another tenant or session")
}

func TestMalformedEphemeralAtCapacityStillRepairs(t *testing.T) {
	t.Parallel()
	valid, err := (sessionwire.EphemeralPublication{TenantID: tenantA, SessionID: session, Body: json.RawMessage(`{"text":"private-body"}`)}).MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	malformed := strings.Replace(string(valid), `"body":`, `"journal_seq":9,"body":`, 1)
	testInvalidEphemeralAtCapacity(t, []byte(malformed), "publication was refused; repairing")
}
