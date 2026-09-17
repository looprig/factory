package hostlink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	centrifugego "github.com/centrifugal/centrifuge-go"
	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// ErrUnsupportedProtocol reports a Host that does not speak this build's
// sessionwire version.
//
// It is TERMINAL for the connection. Reconnecting to the same Host would
// negotiate the same answer, and a link that stayed open speaking a version
// neither end agreed on would exchange control records whose meaning is not
// established.
var ErrUnsupportedProtocol = errors.New("hostlink: host does not speak this wire version")

// ErrDialFailed is the class of every dial rejection that is not the dialer's
// own composition error.
var ErrDialFailed = errors.New("hostlink: dial failed")

// HostDisconnect reports a Host closing a connection TERMINALLY.
//
// The code is a structured field rather than a sentence, because a caller and a
// test both need to branch on it and neither may do that by matching text.
// centrifuge-go only surfaces a close it will not reconnect from -- codes
// 3500-3999 and 4500-4999 (centrifuge-go@v0.12.0/client.go:989) -- so every
// value that reaches here is one a reconnect would not fix.
type HostDisconnect struct {
	// Host is the Host that closed.
	Host sessionwire.HostID
	// Code is the close code, e.g. 3500 for an invalid service token.
	Code uint32
	// Reason is the Host's own short description, a diagnostic only.
	Reason string
}

func (e *HostDisconnect) Error() string {
	return fmt.Sprintf("hostlink: %s closed the connection: %d %s", e.Host, e.Code, e.Reason)
}

// Unwrap puts a terminal close in the dial-failure class, so a caller that only
// cares that the connection is unusable does not have to enumerate the codes.
func (e *HostDisconnect) Unwrap() error { return ErrDialFailed }

// ClientName is the connection label this replica presents to a Host.
//
// It is a DIAGNOSTIC and carries no authority: the Host authenticates the
// service token and authorizes each bind against its own durable lease. It is
// deliberately one constant rather than per-replica, which is what the
// transport's own documentation asks for.
const ClientName = "factory-hostlink"

// The HostLink control vocabulary is CORE'S, pinned here at core v0.9.1.
//
// sessionwire/v1's hostlink_framing.go names the reserved RPC methods
// (HostLinkMethodBind, HostLinkMethodUnbind, HostLinkMethodAttach and the two
// drain methods), the channel prefix, and HostLinkChannel(tenant, session).
// This package declares no method constant of its own any more, and the
// reason is what the previous constants recorded as a gap: Factory sent
// "hostlink.command" while Host reserved only bind/unbind/drain and resolved
// EVERY other method as a channel name, so every command delivery Factory made
// would have been refused. There is no command method. COMMAND DELIVERY'S RPC
// METHOD IS THE SESSION'S CHANNEL, HostLinkChannel(tenant, session), and the
// body is the HostLinkCommandDelivery -- which is why Link.DeliverCommand takes
// the tenant and session beside the record.
//
// The strings are still pinned as absolute literals by
// TestTheWireVocabularyIsPinnedToItsLiterals, now against Core's constants and
// Core's channel helper rather than against locals, and for the same reason:
// a fixture built from the constant under test pins nothing.
//
// The asynchronous messages a Host pushes.
//
// Core's framing covers RPC methods and the session channel; it names no push
// discriminator, so the {type, data} envelope below is still Factory's half of
// the protocol and the stand-in node in the tests implements exactly it. They
// are async messages rather than channel publications on purpose. A channel
// would put control-plane observations behind the subscription machinery;
// there is exactly one connection per Host and every observation on it is for
// this replica, so a discriminated message on that connection is the whole
// requirement.
//
// That reasoning is stronger for a REGISTRY observation than for a CAPACITY
// report, and the difference is recorded rather than answered. A registry
// observation is per-session and per-route, so it genuinely concerns only the
// replica holding the route. A capacity report is a Host-level advertisement
// every Factory replica wants: published to a channel, the broker would fan one
// message out to every subscribed replica instead of the Host calling
// Client.Send once per connected replica, and two replicas on one Host is
// already a measured case rather than a hypothetical one. A Host writer has a
// real argument here. It is not taken now because Core names no channel for it
// -- HostLinkChannel is per session -- and because the Observer seam absorbs
// the change: moving capacity to a publication changes how a link is wired to
// the transport and nothing above it.
const (
	// PushTypeCapacity carries a HostLinkCapacityReport.
	PushTypeCapacity = "host.capacity"
	// PushTypeRegistry carries a HostLinkRegistryObservation.
	PushTypeRegistry = "host.registry"
)

// pushEnvelope discriminates one asynchronous Host message.
//
// The body stays a RawMessage until the type is known, so a record is decoded
// by the type that validates it rather than into a general-purpose map that
// would accept anything.
type pushEnvelope struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// DialerConfig composes the real transport.
type DialerConfig struct {
	// Credential supplies the service token. Required.
	Credential Credential
	// Version is the Factory build identity reported to a Host. Required.
	Version string
	// Limits supplies the dial timeout and the reconnect backoff. A zero value
	// takes DefaultLimits.
	Limits Limits
}

// CentrifugeDialer opens real HostLink connections.
type CentrifugeDialer struct {
	credential Credential
	version    string
	limits     Limits
}

// NewCentrifugeDialer validates a composition and returns it.
func NewCentrifugeDialer(cfg DialerConfig) (*CentrifugeDialer, error) {
	if cfg.Credential == nil {
		return nil, fmt.Errorf("%w: Credential is nil", ErrInvalidConfig)
	}
	if cfg.Version == "" {
		return nil, fmt.Errorf("%w: Version is empty", ErrInvalidConfig)
	}
	limits := cfg.Limits
	if limits == (Limits{}) {
		limits = DefaultLimits()
	}
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	return &CentrifugeDialer{credential: cfg.Credential, version: cfg.Version, limits: limits}, nil
}

// Dial opens one authenticated, version-negotiated connection to a Host.
//
// It returns only after the handshake has SETTLED -- connected and negotiated,
// or refused. A dialer that returned an unconnected client would hand the pool
// a link it would record as good and then discover was never usable, and the
// pool's ceiling would be spent on it.
func (d *CentrifugeDialer) Dial(ctx context.Context, target Target, observer Observer) (Link, error) {
	if err := target.Validate(); err != nil {
		return nil, err
	}
	if observer == nil {
		observer = discardObserver{}
	}
	data, err := sessionwire.EncodeHostLinkConnectRequest(sessionwire.VersionNegotiationRequest{
		SupportedVersions: []sessionwire.WireVersion{sessionwire.CurrentWireVersion},
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %s: connect data: %w", ErrDialFailed, target.Host, err)
	}

	link := &centrifugeLink{host: target.Host, observer: observer, settled: make(chan error, 1)}
	link.client = centrifugego.NewJsonClient(string(target.Endpoint), centrifugego.Config{
		// GetToken is the ONLY credential path, and Config.Token is
		// deliberately left empty. Setting both looks like belt and braces and
		// is not: the client consults GetToken only when the token is empty
		// (centrifuge-go@v0.12.0/client.go:1183), so a populated Token means
		// the callback is never reached on the first connect and the two paths
		// can only be told apart by which one produced a refusal. A mutation
		// that dropped an up-front fetch written here first SURVIVED for
		// exactly that reason, which is how this was found.
		//
		// The callback also makes a short-lived credential survive a
		// reconnect. Its context is the background one on purpose: a refresh
		// happens long after Dial's context is done, and inheriting it would
		// make every long-lived link fail its first refresh.
		GetToken: func(centrifugego.ConnectionTokenEvent) (string, error) {
			return d.credential.ServiceToken(context.Background())
		},
		Data:              data,
		Name:              ClientName,
		Version:           d.version,
		HandshakeTimeout:  d.limits.DialTimeout,
		MinReconnectDelay: d.limits.ReconnectMin,
		MaxReconnectDelay: d.limits.ReconnectMax,
		// permessage-deflate is a per-connection memory and CPU cost and it is
		// off, matching the ClientLink surface.
		EnableCompression: false,
		LogLevel:          centrifugego.LogLevelNone,
	})
	link.client.OnConnected(link.onConnected)
	link.client.OnConnecting(link.onConnecting)
	link.client.OnDisconnected(link.onDisconnected)
	link.client.OnMessage(link.onMessage)

	if err := link.client.Connect(); err != nil {
		link.client.Close()
		return nil, fmt.Errorf("%w: %s: %w", ErrDialFailed, target.Host, err)
	}

	timer := time.NewTimer(d.limits.DialTimeout)
	defer timer.Stop()
	select {
	case err := <-link.settled:
		if err != nil {
			link.client.Close()
			return nil, err
		}
		return link, nil
	case <-ctx.Done():
		link.client.Close()
		return nil, fmt.Errorf("%w: %s: %w", ErrDialFailed, target.Host, ctx.Err())
	case <-timer.C:
		link.client.Close()
		return nil, fmt.Errorf("%w: %s: handshake did not settle within %v", ErrDialFailed, target.Host, d.limits.DialTimeout)
	}
}

// centrifugeLink is one live connection.
//
// Reconnection is the TRANSPORT's, not this type's: centrifuge-go reconnects
// with the backoff Dial configured, and a link therefore survives a dropped
// connection without the pool being told. What this type adds is the two things
// the transport cannot know -- that the version must be re-negotiated on every
// reconnect, and that a terminal refusal must stop the link answering as though
// it were live. The negotiated capability set is replaced on every successful
// handshake as well, so a restarted Host cannot inherit stale method support.
type centrifugeLink struct {
	host     sessionwire.HostID
	observer Observer
	client   *centrifugego.Client

	// settled carries the FIRST handshake outcome to Dial, once.
	settled chan error
	once    sync.Once

	mu sync.Mutex
	// rpcMu serializes calls so that a capability check and its RPC are one
	// local admission operation. Reconnect callbacks deliberately do not wait
	// for this mutex: centrifuge-go invokes OnConnecting before scheduling its
	// reconnect, and a call that is waiting for that reconnect must not hold the
	// callback hostage.
	rpcMu sync.Mutex
	// negotiated is the most recent successful Core connect reply. A reply with
	// no hostlink_methods is valid and deliberately means no reserved method is
	// advertised.
	negotiated sessionwire.VersionNegotiationResponse
	// connecting is true between the transport's reconnect callback and a
	// successful Core reply. Reserved calls fail locally during that window
	// rather than using the previous connection's capability set.
	connecting bool
	// generationCtx is canceled when the transport enters its next generation.
	// Calls bind their transport context to it so an RPC queued by centrifuge-go
	// while Connecting cannot be sent after a newer handshake changes the
	// capability set.
	generationCtx    context.Context
	generationCancel context.CancelFunc
	// terminal is set when the connection may not be used again. A link that
	// kept answering RPCs after a terminal refusal would report every one as an
	// undelivered command, which is a retry loop against a Host that has
	// already given its final answer.
	terminal error
}

func (l *centrifugeLink) Host() sessionwire.HostID { return l.host }

func (l *centrifugeLink) Bind(ctx context.Context, req sessionwire.HostLinkBindRequest) error {
	return l.call(ctx, sessionwire.HostLinkMethodBind, req, true)
}

func (l *centrifugeLink) Unbind(ctx context.Context, req sessionwire.HostLinkUnbindRequest) error {
	return l.call(ctx, sessionwire.HostLinkMethodUnbind, req, true)
}

// DeliverCommand sends the record with the SESSION'S CHANNEL as the RPC method.
//
// That is Core's framing rule, not a convenience: a Host resolves any method
// that is not one of its reserved names as a channel against the routes the
// link holds, so the method is what names the binding the delivery is for. The
// body carries only the command id. Neither identifier is validated here --
// Core's helper does not, and the pool validated the route when it was bound.
func (l *centrifugeLink) DeliverCommand(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, req sessionwire.HostLinkCommandDelivery) error {
	return l.call(ctx, sessionwire.HostLinkChannel(tenant, session), req, false)
}

// Close releases the connection.
//
// It is idempotent, because the pool closes a link on reap and again on
// shutdown if the two race, and a second close that failed would make Close
// report an error about a connection that is already gone.
func (l *centrifugeLink) Close(context.Context) error {
	l.client.Close()
	return nil
}

// call is one HostLink RPC.
//
// The request is marshalled by CORE, whose MarshalJSON validates first, so a
// record that would not survive the Host's strict decoder never reaches the
// wire. Reserved operations are gated by the latest advertised capability set;
// session-channel delivery is not, because it is addressed by the bound
// channel rather than a reserved method. The reply is a bare Core
// HostLinkError, or an empty body for success.
func (l *centrifugeLink) call(ctx context.Context, method string, request any, requiresCapability bool) error {
	body, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("hostlink: %s: %w", method, err)
	}

	l.rpcMu.Lock()
	defer l.rpcMu.Unlock()
	l.mu.Lock()
	terminal := l.terminal
	generationCtx := l.generationCtx
	if terminal != nil {
		l.mu.Unlock()
		return terminal
	}
	if requiresCapability {
		if l.connecting || !l.negotiated.Supports(method) {
			l.mu.Unlock()
			return &UnsupportedMethodError{Method: method}
		}
	}
	l.mu.Unlock()

	rpcCtx, release, err := bindGenerationContext(ctx, generationCtx)
	if err != nil {
		return fmt.Errorf("hostlink: %s: %w", method, err)
	}
	defer release()
	reply, err := l.client.RPC(rpcCtx, method, body)
	if err != nil {
		return fmt.Errorf("hostlink: %s: %w", method, err)
	}
	if len(reply.Data) == 0 {
		return nil
	}
	var refusal sessionwire.HostLinkError
	if err := json.Unmarshal(reply.Data, &refusal); err != nil {
		return fmt.Errorf("hostlink: %s: unreadable reply: %w", method, err)
	}
	return &HostRefusal{HostLinkError: refusal}
}

// onConnecting clears the old capability set before a reconnect attempt can
// send a new RPC. The transport may call this before the server has returned a
// new reply, so retaining the old set would let a restarted Host receive a
// reserved method it no longer advertises. It does not take rpcMu: the
// transport invokes this callback before scheduling reconnect, and a queued
// RPC may already be waiting for that reconnect while holding rpcMu.
func (l *centrifugeLink) onConnecting(centrifugego.ConnectingEvent) {
	nextCtx, nextCancel := newGenerationContext()
	var previousCancel context.CancelFunc
	l.mu.Lock()
	terminal := l.terminal != nil
	if !terminal {
		previousCancel = l.generationCancel
		l.generationCtx = nextCtx
		l.generationCancel = nextCancel
		l.connecting = true
		l.negotiated = sessionwire.VersionNegotiationResponse{}
	}
	l.mu.Unlock()
	if terminal {
		nextCancel()
		return
	}
	if previousCancel != nil {
		previousCancel()
	}
}

func newGenerationContext() (context.Context, context.CancelFunc) {
	return context.WithCancel(context.Background())
}

// onConnected verifies the version the Host selected.
//
// It runs on EVERY connect, not just the first, which is the point: a Host that
// is restarted at a version this build does not speak would otherwise be
// reconnected to indefinitely and answer every control record with a decode
// failure.
func (l *centrifugeLink) onConnected(e centrifugego.ConnectedEvent) {
	negotiated, err := verifyNegotiation(e.Data)
	if err != nil {
		l.fail(err)
		// Closing from inside a transport callback would block the callback on
		// the client's own teardown, so it is handed to a goroutine.
		go l.client.Close()
	} else {
		l.mu.Lock()
		if l.terminal == nil {
			l.negotiated = negotiated
			l.connecting = false
		}
		l.mu.Unlock()
	}
	l.settle(err)
}

// onDisconnected reports a TERMINAL close.
//
// centrifuge-go only calls this handler for a close it will not reconnect from
// -- codes 3500-3999 and 4500-4999 (client.go:989). A reconnect-band close
// reaches OnConnecting instead and is none of this type's business, which is
// why there is no handler for it.
func (l *centrifugeLink) onDisconnected(e centrifugego.DisconnectedEvent) {
	err := &HostDisconnect{Host: l.host, Code: e.Code, Reason: e.Reason}
	l.fail(err)
	l.settle(err)
}

// onMessage decodes one asynchronous Host observation.
//
// A message this build cannot read is DROPPED rather than closing the link. A
// capacity report is an advertisement and a registry observation is a routing
// hint; neither is worth the sessions multiplexed over this connection, and a
// Host that added a push type would otherwise take every Factory replica down.
func (l *centrifugeLink) onMessage(e centrifugego.MessageEvent) {
	var envelope pushEnvelope
	if err := json.Unmarshal(e.Data, &envelope); err != nil {
		return
	}
	switch envelope.Type {
	case PushTypeCapacity:
		var report sessionwire.HostLinkCapacityReport
		if err := json.Unmarshal(envelope.Data, &report); err != nil {
			return
		}
		l.observer.ObserveCapacity(report)
	case PushTypeRegistry:
		var observation sessionwire.HostLinkRegistryObservation
		if err := json.Unmarshal(envelope.Data, &observation); err != nil {
			return
		}
		l.observer.ObserveRegistry(observation)
	}
}

func (l *centrifugeLink) settle(err error) {
	l.once.Do(func() { l.settled <- err })
}

func (l *centrifugeLink) fail(err error) {
	var cancel context.CancelFunc
	l.mu.Lock()
	if l.terminal == nil {
		l.terminal = err
		cancel = l.generationCancel
	}
	l.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func bindGenerationContext(ctx, generation context.Context) (context.Context, func(), error) {
	if generation == nil {
		return ctx, func() {}, nil
	}
	if err := generation.Err(); err != nil {
		return nil, nil, err
	}
	bound, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(generation, cancel)
	return bound, func() {
		stop()
		cancel()
	}, nil
}

// verifyNegotiation reads the Host's selected wire version out of its connect
// reply.
//
// An absent or unreadable selection is refused rather than assumed to be the
// current version. Core's VersionNegotiationResponse rejects any version but
// the one this build implements, so "did not decode" and "named another
// version" are the same answer here and both are terminal.
func verifyNegotiation(data []byte) (sessionwire.VersionNegotiationResponse, error) {
	if len(data) == 0 {
		return sessionwire.VersionNegotiationResponse{}, fmt.Errorf("%w: host sent no version selection", ErrUnsupportedProtocol)
	}
	reply, err := sessionwire.DecodeHostLinkConnectReply(data)
	if err != nil {
		return sessionwire.VersionNegotiationResponse{}, fmt.Errorf("%w: %v", ErrUnsupportedProtocol, err)
	}
	if reply.Version != sessionwire.CurrentWireVersion {
		return sessionwire.VersionNegotiationResponse{}, fmt.Errorf("%w: host selected %d, this build speaks %d",
			ErrUnsupportedProtocol, reply.Version, sessionwire.CurrentWireVersion)
	}
	return reply, nil
}
