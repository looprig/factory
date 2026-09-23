package hostlink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
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

// ErrHostFailed is the class of every HostFailure.
var ErrHostFailed = errors.New("hostlink: the host answered with a failure carrying no Core code")

// HostFailure is a Host's own ANSWER to an RPC that carried no Core record: a
// transport-level error reply, such as centrifuge's ErrorInternal (code 100).
//
// It is kept apart from every other RPC failure because the Host ANSWERED: a
// cancelled context, a lost connection or an unanswered request is a plain
// error, and centrifuge-go builds *Error only from a reply the server sent.
// It says nothing about what the Host holds. host v0.2.1 answers a failed
// attach this way (hostlink/attach.go errAttachFailed) after a failed launch,
// after a rollback that did not complete, and after an attach whose
// observation it could not publish -- in which case the session IS resident.
// Host's own contract is "not a placement outcome, but a Factory may retry".
// A caller that moves on to another Host instead is safe only because of the
// session LEASE: while this Host holds it, any other candidate refuses
// epoch_mismatch, so no second residency can form.
// It is still not a *HostRefusal: it names no reason a caller may branch on.
type HostFailure struct {
	// Method is the RPC method the Host answered.
	Method string
	// Code and Message are the transport error the Host replied with. They are
	// diagnostics only.
	Code    uint32
	Message string
}

func (e *HostFailure) Error() string {
	return fmt.Sprintf("hostlink: %s: host answered %d %s", e.Method, e.Code, e.Message)
}

// Unwrap puts every HostFailure in the ErrHostFailed class.
func (e *HostFailure) Unwrap() error { return ErrHostFailed }

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
	link.subs = map[string]*sessionSub{}
	link.orphans = map[string]SessionSink{}
	link.subscribeTimeout = d.limits.DialTimeout
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
		Data: data,
		// A Host answers the upgrade HTTP 400 unless the client NAMES the JSON
		// protocol (host@v0.2.1 selectsJSONProtocol; the gate is the same at
		// v0.1.0). centrifuge-go's JSON client sends no subprotocol on its own,
		// and the other route Host accepts -- format=json in the query -- is
		// closed by Core's InternalEndpoint.Validate, which refuses a query
		// string. So the header is the only way a Factory reaches a Host at
		// all, and its absence is why no v0.1.x Factory ever held a link to
		// one: every stand-in in the suite accepted a header-less upgrade.
		// The stand-ins now gate exactly as a Host does.
		Header:            http.Header{"Sec-WebSocket-Protocol": {"centrifuge-json"}},
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

	// mu guards the admission snapshot below and NOTHING is held across
	// client.RPC. There was a second mutex here, held across the RPC so that
	// "capability check then send" was one operation, and it was removed for
	// three measured reasons. It serialised every session on the Host behind
	// the slowest reply -- a 100ms-deadline delivery waited 702ms, because a
	// caller deadline cannot preempt a mutex wait. Reconnect callbacks must
	// never wait for anything a call holds, since centrifuge-go runs
	// OnConnecting synchronously before scheduling the reconnect. And it
	// widened a centrifuge-go v0.12.0 wedge (see call) from one lost caller
	// into a permanently frozen link. The atomicity the lock claimed is
	// already provided by the snapshot under mu plus the generation binding:
	// a reconnect replaces the generation under mu and cancels the old one,
	// so a call admitted against the old snapshot cannot be sent on the new
	// connection. The mutant that dropped the lock survived the whole suite.
	mu sync.Mutex
	// negotiated is the most recent successful Core connect reply. A reply with
	// no hostlink_methods is valid and deliberately means no reserved method is
	// advertised.
	negotiated sessionwire.VersionNegotiationResponse
	// connecting is true between the transport's reconnect callback and a
	// successful Core reply. Reserved calls fail locally during that window
	// with ErrLinkReconnecting rather than being queued against the previous
	// connection's capability set. negotiated is zeroed at the same moment;
	// the flag is what chooses the transient answer over the capability one,
	// and the zeroing is what stops any reader of negotiated seeing a stale set.
	connecting bool
	// generation is canceled when the transport enters its next generation.
	// Calls bind their transport context to it so an RPC queued by centrifuge-go
	// while Connecting cannot be sent after a newer handshake changes the
	// capability set. It is an explicit registry rather than context.AfterFunc
	// so that cancellation is SYNCHRONOUS: when onConnecting returns, every
	// bound context is already done (TestRPCGenerationCancelIsSynchronous).
	// That is a determinism hardening, not a measured bug fix -- the AfterFunc
	// bridge lost by microseconds against a reconnect delay of milliseconds,
	// and no test demonstrates a stale RPC escaping through that window. What
	// IS pinned by a failing test is bind's re-check after context.WithCancel
	// (TestQueuedBoundBindCannotCrossIntoANewerCapabilityGeneration).
	generation *rpcGeneration
	// terminal is set when the connection may not be used again. A link that
	// kept answering RPCs after a terminal refusal would report every one as an
	// undelivered command, which is a retry loop against a Host that has
	// already given its final answer.
	terminal error

	// subscriptionState is the live-tail half of the link (subscribe.go). Its
	// maps are guarded by mu like everything above.
	subscriptionState

	// rpcs counts the goroutines inside client.RPC, so Close can wait for them
	// before it closes the transport. It has its own lock: it is not guarded by
	// mu, and nothing is held across the RPC it counts.
	rpcs inflight
	// closeOnce makes the bounded wait happen once: a second Close -- the
	// reaper and a shutdown racing -- waits for the first to finish rather
	// than sitting out closeBound again behind an RPC that will never leave.
	closeOnce sync.Once
	// closeTransport, when set, replaces client.Close. Tests only.
	closeTransport func()
}

func (l *centrifugeLink) Host() sessionwire.HostID { return l.host }

func (l *centrifugeLink) Bind(ctx context.Context, req sessionwire.HostLinkBindRequest) error {
	return l.call(ctx, sessionwire.HostLinkMethodBind, req, true)
}

func (l *centrifugeLink) Unbind(ctx context.Context, req sessionwire.HostLinkUnbindRequest) error {
	return l.call(ctx, sessionwire.HostLinkMethodUnbind, req, true)
}

// Attach asks the Host to make one session resident and returns the
// observation it answered with.
//
// It is a RESERVED method and is gated like bind and unbind: a Host whose
// connect reply did not advertise hostlink.attach is never sent one, and the
// refusal is the local *UnsupportedMethodError rather than whatever the Host
// would have said. That is the whole reason the capability signal exists. A
// Host that predates attach resolves the method as a CHANNEL and answers
// runtime_unavailable from its not-bound branch, which is byte-identical to a
// Host that genuinely refused -- so the only way a caller can tell "cannot"
// from "will not" is to never ask the first kind.
//
// The reply is read by decodeAttachReply, which is where the one shape
// difference from bind lives: an accepted attach carries a body.
func (l *centrifugeLink) Attach(ctx context.Context, req sessionwire.HostLinkAttachRequest) (sessionwire.HostLinkRegistryObservation, error) {
	data, err := l.exchange(ctx, sessionwire.HostLinkMethodAttach, req, true)
	if err != nil {
		return sessionwire.HostLinkRegistryObservation{}, err
	}
	return decodeAttachReply(data)
}

// decodeAttachReply reads an attach reply body as exactly one of Core's two
// records.
//
// ATTACH IS THE EXCEPTION TO THE EMPTY ACCEPTED BODY. Core requires a Host to
// answer an accepted attach with the HostLinkRegistryObservation whose
// host_id, host_generation and lease_epoch the following bind names, so an
// empty body is not success here as it is for bind: it is a Host that
// attached and did not say at which epoch, and a caller holding no epoch has
// nothing it may bind with. It is refused as ErrMalformedAttachReply.
//
// The two records are told apart by Core's STRICT decoders and not by
// sniffing a member: a refusal requires "code", which the observation's
// decoder refuses as unknown, and an observation carries "version",
// "tenant_id" and the rest, which HostLinkError's decoder refuses as unknown.
// So at most one decode can succeed, and a body neither accepts is refused
// rather than guessed at. The refusal is tried first only because it is the
// shorter record; the order cannot change the answer.
func decodeAttachReply(data []byte) (sessionwire.HostLinkRegistryObservation, error) {
	if len(data) == 0 {
		return sessionwire.HostLinkRegistryObservation{}, fmt.Errorf("%w: the accepted reply carried no observation", ErrMalformedAttachReply)
	}
	var refusal sessionwire.HostLinkError
	if err := json.Unmarshal(data, &refusal); err == nil {
		return sessionwire.HostLinkRegistryObservation{}, &HostRefusal{HostLinkError: refusal}
	}
	var observation sessionwire.HostLinkRegistryObservation
	if err := json.Unmarshal(data, &observation); err != nil {
		return sessionwire.HostLinkRegistryObservation{}, fmt.Errorf("%w: %v", ErrMalformedAttachReply, err)
	}
	return observation, nil
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

// closeBound caps how long Close may take, whatever its context allows: the
// drain wait and the transport's own close share it.
//
// The drain is bounded because Close first cancels every RPC in flight, and a
// cancelled RPC leaves the transport within one websocket write --
// centrifuge-go's default WriteTimeout, 1s, which Dial does not change. What
// can outlive it is an RPC wedged in centrifuge-go's double completion
// callback (see call); that goroutine is blocked AFTER its send, so closing
// past it cannot race it.
//
// The transport's close is bounded because it can hang FOREVER in
// centrifuge-go v0.12.0/v0.12.1: Client.handle reads a pending request under
// requestsMu.RLock, releases it and runs the callback (client.go:855-860),
// while Close's clearConnectedState snapshots the same request and runs its
// callback on a new goroutine (:745-747). When the snapshot's callback fills
// RPC's capacity-1 result channel first, the reply's callback blocks on the
// READER goroutine, the reader never exits, and moveToClosed waits for it on
// disconnectedCh (:688) indefinitely. A request is left pending whenever an
// RPC's caller gave up before the reply -- which is what a shutdown does -- so
// no drain on this side can rule it out. Reproduced here about once in seven
// fifty-round runs of TestCloseDoesNotRaceAnRPCInFlight before the bound.
//
// centrifuge-go's master branch fixes both halves (a transportMu in send, and
// handle popping the request atomically), but no release carries them as of
// v0.12.1; on a release that does, this bound and Close's ordering can be
// revisited.
const closeBound = 2 * time.Second

// Close releases the connection.
//
// It is ORDERED against the link's own RPCs, and the order is the fix for a
// race inside centrifuge-go v0.12.0 (and v0.12.1, whose client.go is
// identical): Client.send reads c.transport with no lock on the goroutine that
// called RPC (client.go:2187), while Client.Close writes it under c.mu
// (moveToDisconnected, client.go:457). The transport orders neither against
// the other, so closing the client while any RPC is still inside send is a
// data race -- the tests lane's I1.3 regate hit it through Server.Stop ->
// Pool.Close against a placement Bind the Stop had abandoned. A caller
// returning is no evidence its RPC is done, because rpc runs client.RPC on its
// own goroutine that outlives a caller whose context ended.
//
// So Close (1) marks the link terminal, refusing every later call before it
// reaches the transport, and cancels the current generation, which ends every
// admitted RPC's context; (2) waits for the RPC goroutines already inside the
// transport to return; (3) closes the client while holding regMu, the lock
// every subscription-side send is made under, so a Subscribe or a discard
// cannot be inside send either. Steps (2) and (3) together are bounded by ctx
// and closeBound, so Close -- and through it Pool.Close and Server.Stop --
// cannot be made unkillable by the transport; a close that outlives the bound
// is left running on its own goroutine (see closeBound). No lock is held across
// client.RPC: the wait is on a counter, not on a mutex any call holds. A
// reconnect's own teardown (moveToConnecting) has the same unlocked read
// against it and is the transport's; see TestReconnectStressNeverWedgesACaller.
//
// It is idempotent, because the pool closes a link on reap and again on
// shutdown if the two race, and a second close that failed would make Close
// report an error about a connection that is already gone; a concurrent second
// call waits for the first to finish (closeOnce). It reports nothing when the
// bound cuts it short: the link refuses every call either way, and there is
// nothing a caller could do with the difference.
func (l *centrifugeLink) Close(ctx context.Context) error {
	l.fail(fmt.Errorf("%w: %s", ErrLinkClosed, l.host))
	l.closeOnce.Do(func() {
		// One deadline for both waits: a timer's channel fires once, so a
		// second select on it would never be released.
		bounded, cancel := context.WithTimeout(ctx, closeBound)
		defer cancel()
		select {
		case <-l.rpcs.close():
		case <-bounded.Done():
		}
		closed := make(chan struct{})
		go func() {
			defer close(closed)
			l.regMu.Lock()
			defer l.regMu.Unlock()
			l.closeClient()
		}()
		select {
		case <-closed:
		case <-bounded.Done():
		}
	})
	return nil
}

// closeClient closes the transport. It is a seam only so a test can stand in
// a close that never returns; the dialled link leaves it nil.
func (l *centrifugeLink) closeClient() {
	if l.closeTransport != nil {
		l.closeTransport()
		return
	}
	l.client.Close()
}

// call is one HostLink RPC.
//
// The request is marshalled by CORE, whose MarshalJSON validates first, so a
// record that would not survive the Host's strict decoder never reaches the
// wire. Reserved operations are gated by the latest advertised capability set,
// and refused with ErrLinkReconnecting while there is none to gate against;
// session-channel delivery is not gated, because it is addressed by the bound
// channel rather than a reserved method. The reply is a bare Core
// HostLinkError, or an empty body for success.
//
// A delivery admitted while Connecting is QUEUED by centrifuge-go and emitted
// on the next connection before this link has verified its reply:
// centrifuge-go resolves connect futures before it runs OnConnected
// (client.go:1346 against :1354). It does not matter for delivery, which no
// capability gates, and a version mismatch marks the link terminal anyway;
// but it is why the gate above is a snapshot and not a promise that every
// emitted RPC sits behind the latest reply.
//
// RPCs on a link are CONCURRENT and nothing is held across client.RPC. The
// RPC runs on its own goroutine and the caller selects against the bound
// context, for a reason beyond head-of-line blocking: centrifuge-go v0.12.0
// can run an RPC's completion callback twice -- clearConnectedState fails every
// pending request on a new goroutine (client.go:745) while the caller's own
// send has already registered the request and then fails (client.go:2187,
// :443) -- and the second callback blocks forever on RPC's capacity-1 result
// channel (client.go:373). When that lands on the caller's goroutine the
// caller never reaches RPC's select, and no context frees a channel send.
// Reproduced 1 in 4 reconnect stress runs. Confined here, a wedge costs one
// leaked goroutine rather than a caller, and the caller is released by the
// generation cancel that the same reconnect performs.
func (l *centrifugeLink) call(ctx context.Context, method string, request any, requiresCapability bool) error {
	data, err := l.exchange(ctx, method, request, requiresCapability)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	var refusal sessionwire.HostLinkError
	if err := json.Unmarshal(data, &refusal); err != nil {
		return fmt.Errorf("hostlink: %s: unreadable reply: %w", method, err)
	}
	return &HostRefusal{HostLinkError: refusal}
}

// exchange is the part of one HostLink RPC every method shares: marshal by
// Core, admit against the negotiated capability set, bind the transport
// context to the current generation, and return the reply body UNREAD. The
// body's meaning is the method's: bind and unbind accept with an empty body,
// while attach accepts with an observation, so reading it here would be a
// second authority for a shape only the caller knows.
func (l *centrifugeLink) exchange(ctx context.Context, method string, request any, requiresCapability bool) ([]byte, error) {
	body, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("hostlink: %s: %w", method, err)
	}

	l.mu.Lock()
	terminal := l.terminal
	generation := l.generation
	connecting := l.connecting
	supported := l.negotiated.Supports(method)
	l.mu.Unlock()
	if terminal != nil {
		return nil, terminal
	}
	if requiresCapability {
		if connecting {
			return nil, fmt.Errorf("hostlink: %s: %w", method, ErrLinkReconnecting)
		}
		if !supported {
			return nil, &UnsupportedMethodError{Method: method}
		}
	}

	rpcCtx, release, err := bindGenerationContext(ctx, generation)
	if err != nil {
		return nil, fmt.Errorf("hostlink: %s: %w", method, err)
	}
	defer release()
	reply, err := l.rpc(rpcCtx, method, body)
	if err != nil {
		// centrifuge-go returns *Error ONLY for an error reply the server sent
		// (client.go:437, errorFromProto); every client-side condition --
		// timeout, disconnect, a closed client -- is a plain error. So this arm
		// is exactly "the Host answered", and nothing ambiguous reaches it.
		var answered *centrifugego.Error
		if errors.As(err, &answered) {
			return nil, &HostFailure{Method: method, Code: answered.Code, Message: answered.Message}
		}
		return nil, fmt.Errorf("hostlink: %s: %w", method, err)
	}
	return reply.Data, nil
}

// rpcOutcome is one client.RPC result, carried off the goroutine that ran it.
type rpcOutcome struct {
	reply centrifugego.RPCResult
	err   error
}

// rpc runs client.RPC on its own goroutine and waits for whichever comes
// first, its result or the bound context. The result channel is buffered so
// the goroutine can always finish once the caller has left; a goroutine
// wedged INSIDE client.RPC (see call) is the one this cannot collect, by
// design.
//
// The goroutine is COUNTED in rpcs from before it starts until client.RPC
// returns, which is what lets Close wait for it (see Close); a link that is
// closing refuses here, so no RPC can enter the transport behind Close's wait.
func (l *centrifugeLink) rpc(ctx context.Context, method string, body []byte) (centrifugego.RPCResult, error) {
	if !l.rpcs.enter() {
		return centrifugego.RPCResult{}, fmt.Errorf("%w: %s", ErrLinkClosed, l.host)
	}
	outcome := make(chan rpcOutcome, 1)
	go func() {
		defer l.rpcs.leave()
		reply, err := l.client.RPC(ctx, method, body)
		outcome <- rpcOutcome{reply: reply, err: err}
	}()
	select {
	case out := <-outcome:
		return out.reply, out.err
	case <-ctx.Done():
		return centrifugego.RPCResult{}, ctx.Err()
	}
}

// onConnecting clears the old capability set before a reconnect attempt can
// send a new RPC. The transport may call this before the server has returned a
// new reply, so retaining the old set would let a restarted Host receive a
// reserved method it no longer advertises. It takes only mu, briefly: the
// transport invokes this callback before scheduling the reconnect, so a
// callback that waited for anything a call holds would be a deadlock of the
// link (TestReconnectDoesNotWaitForAnRPCInFlight).
func (l *centrifugeLink) onConnecting(centrifugego.ConnectingEvent) {
	nextGeneration := newRPCGeneration()
	var previousGeneration *rpcGeneration
	l.mu.Lock()
	terminal := l.terminal != nil
	if !terminal {
		previousGeneration = l.generation
		l.generation = nextGeneration
		l.connecting = true
		l.negotiated = sessionwire.VersionNegotiationResponse{}
	}
	l.mu.Unlock()
	if terminal {
		nextGeneration.cancel()
		return
	}
	if previousGeneration != nil {
		previousGeneration.cancel()
	}
	// Every session tail on the connection that just went away is ended NOW,
	// before the transport can resubscribe it on the next one. See
	// endAllSubscriptions for why the transport's own resubscribe must never
	// run: this link owns the order re-bind, then subscribe.
	l.endAllSubscriptions(true)
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
		l.endAllSubscriptions(false)
		// Closing from inside a transport callback would block the callback on
		// the client's own teardown, so it is handed to a goroutine. It is the
		// link's Close, not the client's, so it is ordered against RPCs in
		// flight like every other close (see Close).
		go func() { _ = l.Close(context.Background()) }()
	} else {
		l.mu.Lock()
		if l.terminal == nil {
			l.negotiated = negotiated
			l.connecting = false
		}
		l.mu.Unlock()
		// Only now, with a verified reply and a capability set to admit a bind
		// against, may a sink lost to the previous connection re-bind.
		l.restoreOrphans()
	}
	l.settleDial(err)
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
	l.endAllSubscriptions(false)
	l.settleDial(err)
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

func (l *centrifugeLink) settleDial(err error) {
	l.once.Do(func() { l.settled <- err })
}

func (l *centrifugeLink) fail(err error) {
	var generation *rpcGeneration
	l.mu.Lock()
	if l.terminal == nil {
		l.terminal = err
		generation = l.generation
	}
	l.mu.Unlock()
	if generation != nil {
		generation.cancel()
	}
}

// inflight counts the operations inside the transport and, once closed,
// admits no more and reports when the last one has left.
type inflight struct {
	mu      sync.Mutex
	count   int
	closing bool
	drained chan struct{}
}

// enter admits one operation, or refuses it once the count is closed.
func (f *inflight) enter() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closing {
		return false
	}
	f.count++
	return true
}

// leave ends one admitted operation.
func (f *inflight) leave() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.count--
	if f.closing && f.count == 0 {
		close(f.drained)
	}
}

// close stops admission and returns a channel closed when nothing admitted is
// still inside. It is idempotent: every call returns the same channel.
func (f *inflight) close() <-chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.closing {
		f.closing = true
		f.drained = make(chan struct{})
		if f.count == 0 {
			close(f.drained)
		}
	}
	return f.drained
}

// rpcGeneration owns the cancellation of every RPC admitted to one transport
// generation. A reconnect cancels the old generation synchronously before
// centrifuge-go can resolve its queued connect futures, so an old reserved
// call cannot be emitted on a new connection with a different capability set.
type rpcGeneration struct {
	mu       sync.Mutex
	nextID   uint64
	canceled bool
	bindings map[uint64]context.CancelFunc
}

func newRPCGeneration() *rpcGeneration {
	return &rpcGeneration{bindings: make(map[uint64]context.CancelFunc)}
}

func (g *rpcGeneration) bind(ctx context.Context) (context.Context, func(), error) {
	g.mu.Lock()
	if g.canceled {
		g.mu.Unlock()
		return nil, nil, context.Canceled
	}
	g.mu.Unlock()

	bound, cancel := context.WithCancel(ctx)
	g.mu.Lock()
	if g.canceled {
		g.mu.Unlock()
		cancel()
		return nil, nil, context.Canceled
	}
	id := g.nextID
	g.nextID++
	g.bindings[id] = cancel
	g.mu.Unlock()
	return bound, func() {
		g.mu.Lock()
		delete(g.bindings, id)
		g.mu.Unlock()
		cancel()
	}, nil
}

func (g *rpcGeneration) cancel() {
	g.mu.Lock()
	if g.canceled {
		g.mu.Unlock()
		return
	}
	g.canceled = true
	cancels := make([]context.CancelFunc, 0, len(g.bindings))
	for _, cancel := range g.bindings {
		cancels = append(cancels, cancel)
	}
	g.bindings = nil
	g.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}

func bindGenerationContext(ctx context.Context, generation *rpcGeneration) (context.Context, func(), error) {
	if generation == nil {
		return ctx, func() {}, nil
	}
	return generation.bind(ctx)
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
