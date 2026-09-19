package livetail_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	centrifuge "github.com/centrifugal/centrifuge"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/hostlink"
	"github.com/looprig/factory/internal/realtime/livetail"
	"github.com/looprig/factory/internal/routing"
	"github.com/looprig/sessionstore"
)

// waitFor is generous for the reason hostlink's is: this box runs loaded, and a
// deadline here is evidence only that an event did not arrive.
const waitFor = 20 * time.Second

const (
	tenantA sessionwire.TenantID  = "tenant-a"
	tenantB sessionwire.TenantID  = "tenant-b"
	session sessionwire.SessionID = "s-1"
)

// ---------------------------------------------------------------------------
// The Host stand-in: host v0.2.1's HostLink behaviour that this plane depends
// on, and no more. A bind is accepted and recorded per CONNECTION; a subscribe
// is allowed only on a connection holding the bind for that channel
// (Multiplexer.MaySubscribe); an unbind does not unsubscribe; a publication is
// sent once to whoever is subscribed, with no history.
// ---------------------------------------------------------------------------

type wireEvent struct {
	client  string
	kind    string // bind, unbind, subscribe, subscribe-refused, unsubscribe
	channel string
}

type standIn struct {
	id   sessionwire.HostID
	url  string
	node *centrifuge.Node

	mu      sync.Mutex
	log     []wireEvent
	binds   map[string]map[string]bool
	clients map[string]*centrifuge.Client
}

func newStandIn(t *testing.T, id sessionwire.HostID) *standIn {
	t.Helper()
	node, err := centrifuge.New(centrifuge.Config{Name: "host-stand-in", LogLevel: centrifuge.LogLevelNone})
	if err != nil {
		t.Fatalf("centrifuge.New: %v", err)
	}
	h := &standIn{id: id, node: node, binds: map[string]map[string]bool{}, clients: map[string]*centrifuge.Client{}}
	node.OnConnecting(func(_ context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		request, err := sessionwire.DecodeHostLinkConnectRequest(e.Data)
		if err != nil {
			return centrifuge.ConnectReply{}, centrifuge.DisconnectInappropriateProtocol
		}
		selection, err := sessionwire.NegotiateVersion(request)
		if err != nil {
			return centrifuge.ConnectReply{}, centrifuge.DisconnectInappropriateProtocol
		}
		body, err := sessionwire.EncodeHostLinkConnectReply(selection.WithHostLinkMethods(
			sessionwire.HostLinkMethodBind, sessionwire.HostLinkMethodUnbind))
		if err != nil {
			return centrifuge.ConnectReply{}, centrifuge.DisconnectServerError
		}
		return centrifuge.ConnectReply{Credentials: &centrifuge.Credentials{UserID: "factory"}, Data: body}, nil
	})
	node.OnConnect(func(client *centrifuge.Client) {
		id := client.ID()
		h.mu.Lock()
		h.clients[id] = client
		h.mu.Unlock()
		client.OnRPC(func(e centrifuge.RPCEvent, cb centrifuge.RPCCallback) {
			var key struct {
				TenantID  sessionwire.TenantID  `json:"tenant_id"`
				SessionID sessionwire.SessionID `json:"session_id"`
			}
			_ = json.Unmarshal(e.Data, &key)
			channel := sessionwire.HostLinkChannel(key.TenantID, key.SessionID)
			h.mu.Lock()
			switch e.Method {
			case sessionwire.HostLinkMethodBind:
				if h.binds[id] == nil {
					h.binds[id] = map[string]bool{}
				}
				h.binds[id][channel] = true
				h.log = append(h.log, wireEvent{client: id, kind: "bind", channel: channel})
			case sessionwire.HostLinkMethodUnbind:
				delete(h.binds[id], channel)
				h.log = append(h.log, wireEvent{client: id, kind: "unbind", channel: channel})
			}
			h.mu.Unlock()
			cb(centrifuge.RPCReply{}, nil)
		})
		client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			h.mu.Lock()
			allowed := h.binds[id][e.Channel]
			kind := "subscribe"
			if !allowed {
				kind = "subscribe-refused"
			}
			h.log = append(h.log, wireEvent{client: id, kind: kind, channel: e.Channel})
			h.mu.Unlock()
			if !allowed {
				cb(centrifuge.SubscribeReply{}, centrifuge.ErrorPermissionDenied)
				return
			}
			cb(centrifuge.SubscribeReply{}, nil)
		})
		client.OnUnsubscribe(func(e centrifuge.UnsubscribeEvent) {
			h.mu.Lock()
			h.log = append(h.log, wireEvent{client: id, kind: "unsubscribe", channel: e.Channel})
			h.mu.Unlock()
		})
		client.OnDisconnect(func(centrifuge.DisconnectEvent) {
			h.mu.Lock()
			delete(h.binds, id)
			delete(h.clients, id)
			h.mu.Unlock()
		})
	})
	if err := node.Run(); err != nil {
		t.Fatalf("node.Run: %v", err)
	}
	ws := centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{CheckOrigin: func(*http.Request) bool { return true }})
	server := httptest.NewServer(ws)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = node.Shutdown(ctx)
		server.Close()
	})
	h.url = "ws" + strings.TrimPrefix(server.URL, "http")
	return h
}

func (h *standIn) events() []wireEvent {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]wireEvent(nil), h.log...)
}

func (h *standIn) count(kind, channel string) int {
	n := 0
	for _, e := range h.events() {
		if e.kind == kind && e.channel == channel {
			n++
		}
	}
	return n
}

func (h *standIn) subscribers(channel string) int { return h.node.Hub().NumSubscribers(channel) }

func (h *standIn) publish(t *testing.T, channel string, data []byte) {
	t.Helper()
	if _, err := h.node.Publish(channel, data); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

// drop closes every connection with a reconnect-band code.
func (h *standIn) drop() {
	h.mu.Lock()
	clients := make([]*centrifuge.Client, 0, len(h.clients))
	for _, c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.Unlock()
	for _, c := range clients {
		c.Disconnect(centrifuge.Disconnect{Code: 4000, Reason: "test drop"})
	}
}

func (h *standIn) observation(tenant sessionwire.TenantID, sid sessionwire.SessionID, epoch uint64) sessionwire.HostLinkRegistryObservation {
	now := time.Now()
	return sessionwire.HostLinkRegistryObservation{
		Version: sessionwire.CurrentWireVersion, TenantID: tenant, SessionID: sid,
		HostID: h.id, HostGeneration: 1, AgentID: "agent", RuntimeCompatibilityID: "runtime-1",
		Placement: sessionwire.HostPlacementPooled, InternalEndpoint: sessionwire.InternalEndpoint(h.url),
		Residency: sessionwire.SessionResidencyResident, Accepting: true, LeaseEpoch: epoch,
		ObservedAt: now, ExpiresAt: now.Add(time.Hour),
	}
}

func enduring(t *testing.T, tenant sessionwire.TenantID, sid sessionwire.SessionID, seq uint64) []byte {
	t.Helper()
	encoded, err := sessionwire.EnduringPublication{
		TenantID: tenant, SessionID: sid, EventID: sessionwire.EventID(fmt.Sprintf("event-%d", seq)),
		JournalSeq: seq, CoveredThrough: seq, Body: json.RawMessage(fmt.Sprintf(`{"seq":%d}`, seq)),
	}.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return encoded
}

// ---------------------------------------------------------------------------
// Factory-side fakes: the registry, the durable tip, the ClientLink and a
// manual clock. Everything else is the production code.
// ---------------------------------------------------------------------------

type directory struct {
	mu     sync.Mutex
	owners map[string]sessionwire.HostLinkRegistryObservation
}

func (d *directory) put(o sessionwire.HostLinkRegistryObservation) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.owners[string(o.TenantID)+"/"+string(o.SessionID)] = o
}

func (d *directory) Owner(_ context.Context, tenant sessionwire.TenantID, sid sessionwire.SessionID) (sessionwire.HostLinkRegistryObservation, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	o, ok := d.owners[string(tenant)+"/"+string(sid)]
	return o, ok, nil
}

type tips struct {
	mu  sync.Mutex
	tip uint64
	err error
}

func (s *tips) set(tip uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tip = tip
}

func (s *tips) ReadPublicJournal(context.Context, sessionstore.ReadPublicJournalRequest) (sessionwire.JournalPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return sessionwire.JournalPage{}, s.err
	}
	return sessionwire.JournalPage{CapturedTip: s.tip, CoveredThrough: s.tip}, nil
}

// viewers records every record published per session channel, in order, and
// can be made to block.
type viewers struct {
	mu      sync.Mutex
	records map[string][]string
	closes  map[string]int
	gate    chan struct{}
}

func newViewers() *viewers {
	return &viewers{records: map[string][]string{}, closes: map[string]int{}}
}

func key(tenant sessionwire.TenantID, sid sessionwire.SessionID) string {
	return string(tenant) + "/" + string(sid)
}

func (v *viewers) PublishSession(tenant sessionwire.TenantID, sid sessionwire.SessionID, encoded []byte) error {
	v.mu.Lock()
	gate := v.gate
	v.mu.Unlock()
	if gate != nil {
		<-gate
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	v.records[key(tenant, sid)] = append(v.records[key(tenant, sid)], string(encoded))
	return nil
}

func (v *viewers) CloseSession(tenant sessionwire.TenantID, sid sessionwire.SessionID) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.closes[key(tenant, sid)]++
}

func (v *viewers) of(tenant sessionwire.TenantID, sid sessionwire.SessionID) []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]string(nil), v.records[key(tenant, sid)]...)
}

func (v *viewers) closed(tenant sessionwire.TenantID, sid sessionwire.SessionID) int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.closes[key(tenant, sid)]
}

// kinds summarises a record stream: "E<seq>" for an enduring publication,
// "R<last>/<tip>" for a reset, "T<tip>" for a hint.
func kinds(t *testing.T, records []string) []string {
	t.Helper()
	out := make([]string, 0, len(records))
	for _, encoded := range records {
		recordType, err := sessionwire.SessionRecordTypeOf([]byte(encoded))
		if err != nil {
			t.Fatalf("a published record is not a session-channel record: %s", encoded)
		}
		switch recordType {
		case sessionwire.SessionRecordTypeEnduringPublication:
			var p sessionwire.EnduringPublication
			if err := p.UnmarshalJSON([]byte(encoded)); err != nil {
				t.Fatalf("decode: %v", err)
			}
			out = append(out, fmt.Sprintf("E%d", p.JournalSeq))
		case sessionwire.SessionRecordTypeSessionReset:
			var r sessionwire.SessionReset
			if err := r.UnmarshalJSON([]byte(encoded)); err != nil {
				t.Fatalf("decode: %v", err)
			}
			out = append(out, fmt.Sprintf("R%d/%d", r.LastContiguous, r.JournalTip))
		case sessionwire.SessionRecordTypeJournalTip:
			var h sessionwire.JournalTip
			if err := h.UnmarshalJSON([]byte(encoded)); err != nil {
				t.Fatalf("decode: %v", err)
			}
			out = append(out, fmt.Sprintf("T%d", h.Tip))
		default:
			out = append(out, string(recordType))
		}
	}
	return out
}

type manualClock struct {
	mu    sync.Mutex
	armed []func()
}

func (c *manualClock) AfterFunc(_ time.Duration, f func()) func() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	index := len(c.armed)
	c.armed = append(c.armed, f)
	return func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.armed[index] == nil {
			return false
		}
		c.armed[index] = nil
		return true
	}
}

// tick runs every armed poll once.
func (c *manualClock) tick() {
	c.mu.Lock()
	due := c.armed
	c.armed = nil
	c.mu.Unlock()
	for _, f := range due {
		if f != nil {
			f()
		}
	}
}

// ---------------------------------------------------------------------------
// The composition: compose.go's, over the real pool, table, demand and relay.
// ---------------------------------------------------------------------------

type rig struct {
	pool    *hostlink.Pool
	dir     *directory
	tips    *tips
	viewers *viewers
	clock   *manualClock
	demand  *routing.Demand
	plane   *livetail.Plane
}

type rigOptions struct {
	mailbox   int
	reconnect time.Duration
}

func newRig(t *testing.T, opts rigOptions) *rig {
	t.Helper()
	if opts.mailbox == 0 {
		opts.mailbox = 1024
	}
	if opts.reconnect == 0 {
		opts.reconnect = 100 * time.Millisecond
	}
	dialer, err := hostlink.NewCentrifugeDialer(hostlink.DialerConfig{
		Credential: credential("service-token"),
		Version:    "factory-test",
		Limits: hostlink.Limits{
			MaxLinks: 8, DialTimeout: 5 * time.Second, IdleTimeout: time.Minute,
			ReconnectMin: opts.reconnect, ReconnectMax: opts.reconnect,
		},
	})
	if err != nil {
		t.Fatalf("NewCentrifugeDialer: %v", err)
	}
	pool, err := hostlink.NewPool(hostlink.Config{Dialer: dialer})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	r := &rig{
		pool: pool, dir: &directory{owners: map[string]sessionwire.HostLinkRegistryObservation{}},
		tips: &tips{}, viewers: newViewers(), clock: &manualClock{},
	}
	r.plane, err = livetail.New(livetail.Config{
		Links:        pool,
		Viewers:      func() livetail.Viewers { return r.viewers },
		MailboxLimit: opts.mailbox,
		EventTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("livetail.New: %v", err)
	}
	bindings, err := routing.NewBindings(r.dir, r.plane)
	if err != nil {
		t.Fatalf("NewBindings: %v", err)
	}
	r.demand, err = routing.NewDemand(bindings, r.tips, r.plane, r.clock, routing.DemandLimits{
		OwnershipPollInterval: time.Hour, PollTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewDemand: %v", err)
	}
	r.demand.SetWatcher(r.plane)
	relay, err := routing.NewRelay(r.tips, r.demand, r.plane, r.plane, routing.DefaultRepairLimits())
	if err != nil {
		t.Fatalf("NewRelay: %v", err)
	}
	r.plane.Attach(relay, r.demand)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = r.demand.Close(ctx)
		_ = r.plane.Close(ctx)
		_ = pool.Close(ctx)
	})
	return r
}

type credential string

func (c credential) ServiceToken(context.Context) (string, error) { return string(c), nil }

func (r *rig) watch(t *testing.T, tenant sessionwire.TenantID, sid sessionwire.SessionID) {
	t.Helper()
	if err := r.demand.Acquire(context.Background(), tenant, sid); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
}

func eventually(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitFor)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not happen within %v", what, waitFor)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func joined(items []string) string { return strings.Join(items, " ") }
