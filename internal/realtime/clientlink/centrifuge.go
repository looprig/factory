package clientlink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	centrifuge "github.com/centrifugal/centrifuge"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	internalidentity "github.com/looprig/factory/internal/identity"
)

// connectData is what a browser states about itself at handshake.
//
// The ONLY member carrying a decision is the protocol version. A client name
// and version would be diagnostics, and a diagnostic that a client controls has
// no business in a struct a decision is read from.
type connectData struct {
	ProtocolVersion string `json:"protocol_version"`
}

// connectReplyData is what the server states back.
//
// Both halves are reported, and not for symmetry: a browser that reconnects to
// a replica running a different build learns which protocol the replica
// actually speaks rather than inferring it from a refusal, and the build
// identity is what a support report quotes.
type connectReplyData struct {
	ProtocolVersion string `json:"protocol_version"`
	FactoryVersion  string `json:"factory_version"`
}

// rpcRequest is the envelope every command RPC carries.
//
// A6.1 reads only the session, because that is the whole of what an
// authorization decision needs. A6.2 owns the rest of the envelope and its
// REST parity; adding members here before then would be guessing at it.
type rpcRequest struct {
	SessionID sessionwire.SessionID `json:"session_id"`
}

// Handler is the ClientLink: an embedded Centrifuge node behind an
// http.Handler, with every Factory decision delegated to the Engine.
//
// Everything the transport can do that Factory has not asked for is off:
//
//	client publication      no OnPublish handler is registered, so the node
//	                        refuses a client publish (measured in A5.1)
//	compression             WebsocketConfig.Compression is left false
//	history and recovery    the zero SubscribeOptions is named explicitly
//	presence and join/leave the same zero value
//	Redis and NATS brokers  GetBroker and GetPresenceManager are never set, so
//	                        the node keeps its in-process memory broker
//	per-channel batching    GetChannelBatchConfig is never set, so the
//	                        experimental perChannelWriter is never built
//
// The last two are asserted rather than assumed --
// TestTheNodeConfigurationLeavesEveryOptionalMechanismOff reads the constructed
// Config -- which is the "cheap real version" internal/realtime/transport's
// package doc deferred to this task, now that a production node constructor
// exists to read.
type Handler struct {
	engine *Engine
	node   *centrifuge.Node
	ws     http.Handler
}

// NewHandler builds the node and its HTTP entry point.
//
// The node is RUNNING when this returns. A constructor that left it stopped
// would compose successfully and then refuse every connection, which is a
// failure mode a composition test cannot see.
func NewHandler(cfg Config) (*Handler, error) {
	engine, err := NewEngine(cfg)
	if err != nil {
		return nil, err
	}

	node, err := centrifuge.New(nodeConfig(cfg))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}

	h := &Handler{engine: engine, node: node}

	node.OnConnecting(h.connecting)
	node.OnConnect(h.connected)

	if err := node.Run(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}

	h.ws = centrifuge.NewWebsocketHandler(node, websocketConfig(cfg))
	return h, nil
}

// websocketConfig is the transport half of the configuration, extracted for the
// reason nodeConfig is: it is otherwise unreadable once the handler is built.
//
// It is also the ONLY place the ping cadence is stated. ConnectReply carries an
// optional per-connection PingPongConfig, and this handler used to set it as
// well, from the same limits -- which a mutation probe showed to be exactly
// redundant: dropping it changed nothing, because the transport's value is
// identical. Two authorities for one number is the shape of defect this module
// has already had to correct twice, so the second one is gone rather than kept
// for symmetry. A per-connection cadence becomes worth having when something
// varies it, and nothing does.
func websocketConfig(cfg Config) centrifuge.WebsocketConfig {
	return centrifuge.WebsocketConfig{
		// Origin is decided by internal/httpapi's guard, which owns the
		// trusted-origin list, the forwarded-header trust option and the rule
		// that a handshake carrying a browser credential must send an Origin.
		// A second answer here would be a second place for that rule to be
		// stated, and the guard runs BEFORE this handler in the composed chain.
		CheckOrigin: func(*http.Request) bool { return true },
		// permessage-deflate is a per-connection memory and CPU cost at the
		// 1,000-5,000 connection scale, and it is off.
		Compression:    false,
		WriteTimeout:   cfg.Limits.WriteTimeout,
		PingPongConfig: centrifuge.PingPongConfig{PingInterval: cfg.Limits.PingInterval, PongTimeout: cfg.Limits.PongTimeout},
	}
}

// nodeConfig is the whole of what this surface configures on the node.
//
// It is a function rather than a literal inside NewHandler so a test can READ
// it. The node keeps its configuration private once built, so the alternative
// was asserting the absence of a broker by inspecting nothing, which is the gap
// internal/realtime/transport's package doc recorded and deferred to A6.1.
//
// Every field left unset is a decision:
//
//	GetBroker               unset, so the node keeps its in-process memory
//	                        broker and reaches no Redis or NATS
//	GetPresenceManager      unset, so there is no presence at all
//	GetChannelBatchConfig   unset, so the EXPERIMENTAL perChannelWriter
//	                        (centrifuge@v0.38.0/client.go:2701-2702) is never
//	                        built. It would not give a channel a failure domain
//	                        anyway -- it is an unbounded batching buffer in
//	                        front of the one bounded queue -- so enabling it
//	                        would add memory without changing the blast radius.
func nodeConfig(cfg Config) centrifuge.Config {
	return centrifuge.Config{
		Version:  cfg.Version,
		Name:     "factory-clientlink",
		LogLevel: centrifuge.LogLevelNone,
		// In BYTES. centrifuge@v0.38.0/config.go:58-61 defines
		// ClientQueueMaxSize that way and A5.1 measured the overflow close as
		// DisconnectSlow (3008). ClientLinkLimits names the unit for the same
		// reason: it previously said "messages" with a default of 256, which
		// would have been a 256-byte budget.
		ClientQueueMaxSize: cfg.Limits.PerConnectionQueueBytes,
		// Set explicitly because the library defaults it SILENTLY to 128
		// (centrifuge@v0.38.0/node.go:135-136), and one browser link
		// multiplexes every session its user is watching.
		ClientChannelLimit: cfg.Limits.MaxChannelsPerConnection,
	}
}

// Engine returns the policy half, for a caller that decides without a socket.
func (h *Handler) Engine() *Engine { return h.engine }

// Connections reports the ClientLinks this replica currently holds.
func (h *Handler) Connections() int { return h.node.Hub().NumClients() }

// Shutdown closes every ClientLink and stops the node.
func (h *Handler) Shutdown(ctx context.Context) error { return h.node.Shutdown(ctx) }

// ServeHTTP refuses what this surface does not serve, then upgrades.
//
// The protobuf refusal is Factory's, not the transport's. centrifuge's
// WebsocketHandler selects Protobuf framing from `?format=protobuf`,
// `?cf_protocol=protobuf` or the `centrifuge-protobuf` subprotocol
// (centrifuge@v0.38.0/handler_websocket.go:134-160) and offers no way to turn
// that off, so a browser could select a framing this surface has neither
// qualified nor measured. It is refused BEFORE the upgrade, with an ordinary
// HTTP status, because a client that cannot speak our framing cannot be told
// anything useful over a socket that uses it.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if requestsProtobuf(r) {
		http.Error(w, "clientlink serves the JSON protocol only", http.StatusBadRequest)
		return
	}
	h.ws.ServeHTTP(w, r)
}

// requestsProtobuf reports every way this transport version can be asked for
// Protobuf framing.
func requestsProtobuf(r *http.Request) bool {
	if r.URL != nil && r.URL.RawQuery != "" {
		query := r.URL.Query()
		if query.Get("format") == "protobuf" || query.Get("cf_protocol") == "protobuf" {
			return true
		}
	}
	// The subprotocol is offered in a comma-separated list, so it is compared
	// by TOKEN. A prefix match would claim "centrifuge-protobuf-experimental"
	// and a whole-header match would miss an ordinary two-entry offer.
	for _, header := range r.Header.Values("Sec-WebSocket-Protocol") {
		for _, token := range strings.Split(header, ",") {
			if strings.EqualFold(strings.TrimSpace(token), "centrifuge-protobuf") {
				return true
			}
		}
	}
	return false
}

// connecting is the handshake: capacity, protocol, credential, context.
//
// The order is what each step may reveal and what each costs. A replica at
// capacity refuses before it verifies anything, so a full replica does not put
// its whole arrival rate onto the credential verifier; an unsupported protocol
// refuses next, because a client that cannot use a successful authentication
// should not cause one.
func (h *Handler) connecting(ctx context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
	if h.node.Hub().NumClients() >= h.engine.Limits().MaxConnections {
		return centrifuge.ConnectReply{}, centrifuge.DisconnectConnectionLimit
	}

	var data connectData
	if len(e.Data) > 0 {
		if err := json.Unmarshal(e.Data, &data); err != nil {
			return centrifuge.ConnectReply{}, centrifuge.DisconnectBadRequest
		}
	}

	principal, err := h.engine.Authenticate(ctx, ConnectRequest{Token: e.Token, ProtocolVersion: data.ProtocolVersion})
	if err != nil {
		return centrifuge.ConnectReply{}, connectRefusal(err)
	}

	reply, err := json.Marshal(connectReplyData{
		ProtocolVersion: ProtocolVersion,
		FactoryVersion:  h.engine.Version(),
	})
	if err != nil {
		return centrifuge.ConnectReply{}, centrifuge.DisconnectServerError
	}

	return centrifuge.ConnectReply{
		// The principal is carried in the connection's CONTEXT, so every later
		// subscribe and RPC on this link reads the identity the handshake
		// established rather than re-deriving one from something the client
		// sends. Principal is a value with no reference members, so what the
		// context holds cannot be widened by anything holding it.
		Context: internalidentity.NewLinkOperationContext(ctx, principal),
		// The subject, not the tenant. Credentials.UserID is the transport's
		// own connection label and carries no authority here; putting a tenant
		// in it would make a diagnostic look like a scope.
		Credentials: &centrifuge.Credentials{UserID: principal.Subject()},
		Data:        reply,
		// No PingPongConfig. The transport's is the single answer; see
		// websocketConfig.
	}, nil
}

// connectRefusal classifies a handshake failure into a close a browser can act
// on.
//
// The three answers differ in what the browser must DO, which is the only
// distinction worth encoding: an unsupported protocol and an invalid credential
// are terminal and reconnecting changes nothing, an expired credential is
// terminal for THIS token and the browser refreshes and reconnects, and a
// verifier that could not be reached is nobody's fault and must not sign a user
// out.
//
// No custom code is minted. A5.1 established that the Go client never surfaces
// a custom code in 4000-4499 through OnDisconnected, so a Factory-specific
// value in that band would be invisible to the one consumer that has to read
// it; the library's own 3500-band codes are terminal and already carry these
// meanings.
func connectRefusal(err error) error {
	switch {
	case errors.Is(err, ErrUnsupportedProtocol):
		return centrifuge.DisconnectInappropriateProtocol
	case errors.Is(err, identity.ErrCredentialExpired):
		return centrifuge.DisconnectExpired
	case errors.Is(err, identity.ErrUnauthenticated):
		return centrifuge.DisconnectInvalidToken
	default:
		// Everything else is a failure of Factory or of what it depends on,
		// including a credential verifier that could not be reached. It is in
		// the reconnect band deliberately: a verifier outage must not log every
		// live user out.
		return centrifuge.DisconnectServerError
	}
}

// connected registers the per-connection handlers.
//
// There is deliberately NO OnPublish handler. Registering one is what enables
// client publication in this library, so its absence is the refusal rather than
// a check somewhere that could be skipped, and A5.1 measured the resulting
// error on a real connection.
func (h *Handler) connected(client *centrifuge.Client) {
	client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
		principal, ok := principalOf(client)
		if !ok {
			// Unreachable: a client exists only after connecting returned a
			// context carrying one. It fails closed rather than trusting that.
			cb(centrifuge.SubscribeReply{}, centrifuge.ErrorUnauthorized)
			return
		}
		if err := h.engine.AuthorizeSubscribe(client.Context(), principal, e.Channel); err != nil {
			cb(centrifuge.SubscribeReply{}, centrifuge.ErrorPermissionDenied)
			return
		}
		// The zero SubscribeOptions is the configuration this surface wants:
		// no EmitPresence, no EmitJoinLeave, no PushJoinLeave, no
		// EnablePositioning and no EnableRecovery. Naming the zero value is the
		// point -- sessions are durable in SessionStore, and a transport that
		// also remembered them would be a second, weaker answer to the same
		// question.
		cb(centrifuge.SubscribeReply{Options: centrifuge.SubscribeOptions{}}, nil)
	})

	client.OnRPC(func(e centrifuge.RPCEvent, cb centrifuge.RPCCallback) {
		principal, ok := principalOf(client)
		if !ok {
			cb(centrifuge.RPCReply{}, centrifuge.ErrorUnauthorized)
			return
		}
		var req rpcRequest
		if len(e.Data) > 0 {
			if err := json.Unmarshal(e.Data, &req); err != nil {
				cb(centrifuge.RPCReply{}, centrifuge.ErrorBadRequest)
				return
			}
		}
		err := h.engine.AuthorizeRPC(client.Context(), principal, Method(e.Method), req.SessionID)
		switch {
		case errors.Is(err, ErrUnknownMethod):
			cb(centrifuge.RPCReply{}, centrifuge.ErrorMethodNotFound)
		case err != nil:
			cb(centrifuge.RPCReply{}, centrifuge.ErrorPermissionDenied)
		default:
			// A6.2 routes an authorized command into the admission service and
			// replies after the SessionInbox commit. Until then an authorized
			// RPC is refused as unavailable rather than answered with a
			// fabricated acceptance: a reply carrying no CommandID would be a
			// client believing a command was admitted when nothing was written.
			cb(centrifuge.RPCReply{}, centrifuge.ErrorNotAvailable)
		}
	})
}

// principalOf reads back the principal the handshake stored.
//
// The boolean is false for a context no authenticated handshake built, which a
// caller must treat as unauthenticated rather than as an empty principal.
func principalOf(client *centrifuge.Client) (identity.Principal, bool) {
	operation, ok := internalidentity.OperationContextFrom(client.Context())
	if !ok {
		return identity.Principal{}, false
	}
	return operation.Principal, true
}
