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

// The HostLink control vocabulary.
//
// These names, and the push envelope below, are FACTORY'S HALF of a protocol
// whose Host half does not exist in this repository. Core v0.7.0 defines the
// record bodies -- HostLinkBindRequest, HostLinkCapacityReport and the rest --
// and their strict JSON, but it defines no transport framing for them: no
// method names, no push discriminator, no channel vocabulary. That gap is real
// and is recorded here rather than papered over. The Host writer must implement
// the mirror of exactly this, or one of the two must move.
//
// Where these strings should eventually live is UNRESOLVED, and internal/ is
// not it: Host cannot import a Factory-internal package, so one half of a
// two-repo contract currently sits somewhere the other half cannot see. Core's
// sessionwire/v1 is deliberately NOT the answer either. A method name and a
// push discriminator are transport-SHAPED -- they exist because the transport
// is centrifuge, and they would read differently under gRPC or raw WebSocket
// frames -- while sessionwire/v1 is today transport-neutral: it says what a
// record is and nothing about how it travels. Putting hostlink.bind into a
// tier-0 module would leak the transport choice downward and make a future
// transport change a tier-0 breaking release. What this needs is a shared,
// explicitly transport-scoped home; booking one is not this package's to
// decide, and nothing here should be read as that decision having been taken.
//
// Because the strings are the artifact Host mirrors, they are pinned as
// absolute literals by TestTheWireVocabularyIsPinnedToItsLiterals rather than
// through the constants themselves. A fixture built from the constant under
// test pins nothing: a rename would move both sides together and the suite
// would stay green while Factory stopped speaking the protocol Host implements.
const (
	// MethodBind establishes one Factory-local route to a Host-owned session.
	MethodBind = "hostlink.bind"
	// MethodUnbind releases one.
	MethodUnbind = "hostlink.unbind"
	// MethodCommand hands over one committed inbox record's public command id.
	MethodCommand = "hostlink.command"
)

// The asynchronous messages a Host pushes.
//
// They are async messages rather than channel publications on purpose. A
// channel would need a namespace both ends agree on and would put control-plane
// observations behind the subscription machinery; there is exactly one
// connection per Host and every observation on it is for this replica, so a
// discriminated message on that connection is the whole requirement.
//
// That reasoning is stronger for a REGISTRY observation than for a CAPACITY
// report, and the difference is recorded rather than answered. A registry
// observation is per-session and per-route, so it genuinely concerns only the
// replica holding the route. A capacity report is a Host-level advertisement
// every Factory replica wants: published to a channel, the broker would fan one
// message out to every subscribed replica instead of the Host calling
// Client.Send once per connected replica, and two replicas on one Host is
// already a measured case rather than a hypothetical one. A Host writer has a
// real argument here. It is not taken now because there is no agreed channel
// namespace to take it with -- that is the same gap as the method names -- and
// because the Observer seam absorbs the change: moving capacity to a
// publication changes how a link is wired to the transport and nothing above
// it.
const (
	// PushTypeCapacity carries a HostLinkCapacityReport.
	PushTypeCapacity = "host.capacity"
	// PushTypeRegistry carries a HostLinkRegistryObservation.
	PushTypeRegistry = "host.registry"
)

// connectData is what Factory states about itself at handshake.
//
// The only member carrying a decision is the version list. The service
// credential travels in the transport's own token field rather than here: a
// credential in application data would be logged by anything that logs a
// connect payload.
type connectData struct {
	Negotiation sessionwire.VersionNegotiationRequest `json:"version_negotiation"`
}

// connectReplyData is the Host's selection.
type connectReplyData struct {
	Negotiation sessionwire.VersionNegotiationResponse `json:"version_negotiation"`
}

// pushEnvelope discriminates one asynchronous Host message.
//
// The body stays a RawMessage until the type is known, so a record is decoded
// by the type that validates it rather than into a general-purpose map that
// would accept anything.
type pushEnvelope struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// controlReply is what a Host answers a control RPC with.
//
// A refusal is a SUCCESSFUL reply carrying a typed body, not a protocol error.
// The reason is the distinction Pool.DeliverCommand turns on: a protocol error
// is indistinguishable from a transport failure at the client, and a Host that
// signalled "not admitting" that way would be reported as a command that was
// never delivered and retried forever.
type controlReply struct {
	Error *sessionwire.HostLinkError `json:"error,omitempty"`
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
	data, err := json.Marshal(connectData{
		Negotiation: sessionwire.VersionNegotiationRequest{
			SupportedVersions: []sessionwire.WireVersion{sessionwire.CurrentWireVersion},
		},
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
// it were live.
type centrifugeLink struct {
	host     sessionwire.HostID
	observer Observer
	client   *centrifugego.Client

	// settled carries the FIRST handshake outcome to Dial, once.
	settled chan error
	once    sync.Once

	mu sync.Mutex
	// terminal is set when the connection may not be used again. A link that
	// kept answering RPCs after a terminal refusal would report every one as an
	// undelivered command, which is a retry loop against a Host that has
	// already given its final answer.
	terminal error
}

func (l *centrifugeLink) Host() sessionwire.HostID { return l.host }

func (l *centrifugeLink) Bind(ctx context.Context, req sessionwire.HostLinkBindRequest) error {
	return l.call(ctx, MethodBind, req)
}

func (l *centrifugeLink) Unbind(ctx context.Context, req sessionwire.HostLinkUnbindRequest) error {
	return l.call(ctx, MethodUnbind, req)
}

func (l *centrifugeLink) DeliverCommand(ctx context.Context, req sessionwire.HostLinkCommandDelivery) error {
	return l.call(ctx, MethodCommand, req)
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

// call is one control RPC.
//
// The request is marshalled by CORE, whose MarshalJSON validates first, so a
// record that would not survive the Host's strict decoder never reaches the
// wire. The reply is read for a typed refusal before it is read for anything
// else.
func (l *centrifugeLink) call(ctx context.Context, method string, request any) error {
	if err := l.terminalError(); err != nil {
		return err
	}
	body, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("hostlink: %s: %w", method, err)
	}
	reply, err := l.client.RPC(ctx, method, body)
	if err != nil {
		return fmt.Errorf("hostlink: %s: %w", method, err)
	}
	if len(reply.Data) == 0 {
		return nil
	}
	var control controlReply
	if err := json.Unmarshal(reply.Data, &control); err != nil {
		return fmt.Errorf("hostlink: %s: unreadable reply: %w", method, err)
	}
	if control.Error != nil {
		return &HostRefusal{HostLinkError: *control.Error}
	}
	return nil
}

// onConnected verifies the version the Host selected.
//
// It runs on EVERY connect, not just the first, which is the point: a Host that
// is restarted at a version this build does not speak would otherwise be
// reconnected to indefinitely and answer every control record with a decode
// failure.
func (l *centrifugeLink) onConnected(e centrifugego.ConnectedEvent) {
	err := verifyNegotiation(e.Data)
	if err != nil {
		l.fail(err)
		// Closing from inside a transport callback would block the callback on
		// the client's own teardown, so it is handed to a goroutine.
		go l.client.Close()
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
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.terminal == nil {
		l.terminal = err
	}
}

func (l *centrifugeLink) terminalError() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.terminal
}

// verifyNegotiation reads the Host's selected wire version out of its connect
// reply.
//
// An absent or unreadable selection is refused rather than assumed to be the
// current version. Core's VersionNegotiationResponse rejects any version but
// the one this build implements, so "did not decode" and "named another
// version" are the same answer here and both are terminal.
func verifyNegotiation(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("%w: host sent no version selection", ErrUnsupportedProtocol)
	}
	var reply connectReplyData
	if err := json.Unmarshal(data, &reply); err != nil {
		return fmt.Errorf("%w: %v", ErrUnsupportedProtocol, err)
	}
	if reply.Negotiation.Version != sessionwire.CurrentWireVersion {
		return fmt.Errorf("%w: host selected %d, this build speaks %d",
			ErrUnsupportedProtocol, reply.Negotiation.Version, sessionwire.CurrentWireVersion)
	}
	return nil
}
