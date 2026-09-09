package clientlink

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/looprig/factory/identity"
)

// ErrInvalidConfig is the class of every NewHandler rejection.
var ErrInvalidConfig = errors.New("clientlink: invalid configuration")

// ErrUnsupportedProtocol reports a client speaking a ClientLink application
// protocol this Factory does not implement.
var ErrUnsupportedProtocol = errors.New("clientlink: unsupported protocol version")

// ProtocolVersion is the ClientLink APPLICATION protocol this build speaks.
//
// It is not the transport's wire protocol and not the module version. The
// transport negotiates its own framing; this is the vocabulary of channels and
// RPC methods layered on top of it, and it moves when that vocabulary changes
// incompatibly. A browser bundle served by one Factory build may reconnect to
// a replica running another, so the version is stated by the CLIENT in its
// connect data and checked here rather than assumed to match.
const ProtocolVersion = "1"

// MinPingInterval is the shortest cadence the connect reply can carry.
//
// It restates factory.MinClientLinkPingInterval, which this package cannot
// import without a cycle, and limitsParity in the root package holds the two
// equal. The reason is in that constant's documentation: the reply expresses
// the cadence in whole seconds, so a sub-second value arrives as zero and the
// client is never asked to pong at all.
const MinPingInterval = time.Second

// Limits bounds one replica's ClientLink connections.
//
// It restates factory.ClientLinkLimits for the package that consumes it, in
// the same way every other internal package declares the interfaces it calls.
// A9.1 converts; TestClientLinkLimitsAreCarriedWhole holds the two
// shapes in step by name and type, so a field added to one and not the other,
// or renamed on one side, fails there rather than configuring a zero.
type Limits struct {
	// MaxConnections bounds concurrent ClientLinks on this replica.
	MaxConnections int
	// MaxChannelsPerConnection bounds the session channels one link may hold.
	MaxChannelsPerConnection int
	// PerConnectionQueueBytes bounds one connection's outbound queue in BYTES.
	PerConnectionQueueBytes int
	// WriteTimeout bounds one outbound write.
	WriteTimeout time.Duration
	// PingInterval is how often the server pings an idle connection.
	PingInterval time.Duration
	// PongTimeout is how long a ping may go unanswered.
	PongTimeout time.Duration

	// CommandTimeout bounds ONE command admission.
	//
	// It is the only bound a durable admission has, and the reason it must
	// exist here is a property of the pinned transport rather than a
	// preference. centrifuge@v0.38.0 dispatches an RPC SYNCHRONOUSLY on the
	// connection's read loop (client.go:1385 -> 2259), and the connection
	// context is cancelled by the websocket handler's `defer close(ctxCh)`
	// (handler_websocket.go:218-222) -- when that loop RETURNS. An in-flight
	// admission is what keeps the loop from returning, so nothing about the
	// connection can cancel one: not a disconnect, not Handler.Shutdown. It was
	// documented as "bounded by the link's lifetime" and was bounded by
	// nothing, which made a wedged store or directory two operational failures
	// rather than one -- a link hung forever, and, because dispatch is serial,
	// every other frame on that link blocked behind it, so a replica could not
	// be drained while one admission was stuck.
	//
	// What it promises is exactly what a context promises, and no more. It is a
	// DEADLINE handed to the Admitter, not a guillotine on the reply: a
	// dependency that honours its context returns at the deadline, and one that
	// ignores it is not bounded by anything here. httpapi.RouteLimits states
	// the same limit for the REST edge's own deadline, and this is that
	// decision for this edge rather than a second authority for one number --
	// the two edges bound different work over different transports.
	CommandTimeout time.Duration
}

// Validate reports why these limits may not be used.
//
// It is a SECOND check rather than a redundant one. The root package validates
// what a deployer supplied; this validates what reached the engine, which is
// also what a direct in-module caller supplies. A handler built from a zero
// Limits would otherwise configure a zero-byte queue and a zero ping cadence
// and fail at run time on a live connection.
func (l Limits) Validate() error {
	if l.MaxConnections < 1 {
		return fmt.Errorf("%w: MaxConnections is %d, want at least 1", ErrInvalidConfig, l.MaxConnections)
	}
	if l.MaxChannelsPerConnection < 1 {
		return fmt.Errorf("%w: MaxChannelsPerConnection is %d, want at least 1", ErrInvalidConfig, l.MaxChannelsPerConnection)
	}
	if l.PerConnectionQueueBytes < 1 {
		return fmt.Errorf("%w: PerConnectionQueueBytes is %d, want at least 1", ErrInvalidConfig, l.PerConnectionQueueBytes)
	}
	if l.WriteTimeout <= 0 {
		return fmt.Errorf("%w: WriteTimeout is %v, want a positive duration", ErrInvalidConfig, l.WriteTimeout)
	}
	if l.PingInterval < MinPingInterval {
		return fmt.Errorf("%w: PingInterval is %v, want at least %v", ErrInvalidConfig, l.PingInterval, MinPingInterval)
	}
	if l.PongTimeout <= 0 {
		return fmt.Errorf("%w: PongTimeout is %v, want a positive duration", ErrInvalidConfig, l.PongTimeout)
	}
	if l.CommandTimeout <= 0 {
		return fmt.Errorf("%w: CommandTimeout is %v, want a positive duration", ErrInvalidConfig, l.CommandTimeout)
	}
	return nil
}

// Config composes the ClientLink engine.
type Config struct {
	// Authenticator verifies the connect credential. A ClientLink token is the
	// same material as a bearer credential and is verified by the same
	// Verifier, which is what makes reconnecting to another replica work.
	Authenticator Authenticator

	// Authorizer decides every subscribe and every RPC. A successful handshake
	// authorizes nothing beyond itself.
	Authorizer Authorizer

	// Admitter is the durable command plane every authorized RPC is routed
	// into. It is required, and NewEngine refuses a composition without one
	// rather than letting a link accept commands it can only refuse.
	Admitter Admitter

	// Limits bounds this replica's connections.
	Limits Limits

	// Version is the Factory build identity reported to a connecting client.
	Version string
}

// Engine is the transport-independent half of the ClientLink.
//
// Everything here is a decision about a principal, a channel or a method.
// Nothing here knows about WebSockets, frames or close codes, so the policy can
// be driven directly by a test and the transport adapter has nothing to decide.
type Engine struct {
	cfg Config
}

// NewEngine validates a composition and returns it.
func NewEngine(cfg Config) (*Engine, error) {
	if cfg.Authenticator == nil {
		return nil, fmt.Errorf("%w: Authenticator is nil", ErrInvalidConfig)
	}
	if cfg.Authorizer == nil {
		return nil, fmt.Errorf("%w: Authorizer is nil", ErrInvalidConfig)
	}
	if cfg.Admitter == nil {
		return nil, fmt.Errorf("%w: Admitter is nil", ErrInvalidConfig)
	}
	if cfg.Version == "" {
		return nil, fmt.Errorf("%w: Version is empty", ErrInvalidConfig)
	}
	if err := cfg.Limits.Validate(); err != nil {
		return nil, err
	}
	return &Engine{cfg: cfg}, nil
}

// Limits returns the composed limits.
func (e *Engine) Limits() Limits { return e.cfg.Limits }

// Version returns the build identity reported to a connecting client.
func (e *Engine) Version() string { return e.cfg.Version }

// ConnectRequest is what a client presents at handshake, reduced to the fields
// a decision is made from. Nothing else a client sends is read: a name and a
// version are recorded by the transport for diagnostics and carry no authority.
type ConnectRequest struct {
	// Token is the connect credential.
	Token string
	// ProtocolVersion is the ClientLink application protocol the client
	// speaks. An empty value means the client named none.
	ProtocolVersion string
}

// Authenticate decides one handshake.
//
// The protocol check runs BEFORE the credential is verified, and the order is
// the answer to what each step may reveal: a client speaking a protocol this
// build does not implement cannot use a successful authentication for anything,
// and refusing first means an unsupported bundle does not put load on the
// credential verifier on every reconnect.
func (e *Engine) Authenticate(ctx context.Context, req ConnectRequest) (identity.Principal, error) {
	if req.ProtocolVersion != ProtocolVersion {
		return identity.Principal{}, fmt.Errorf("%w: client speaks %q, this build speaks %q",
			ErrUnsupportedProtocol, req.ProtocolVersion, ProtocolVersion)
	}
	return e.cfg.Authenticator.AuthenticateLink(ctx, req.Token)
}

// AuthorizeSubscribe decides one channel for one principal.
//
// A successful handshake grants no channel. The authorizer parses
// session:{tenant}:{session} and compares the tenant segment to the principal's
// own; parsing a channel name is not a grant, which is why the decision is not
// made here from the parse.
func (e *Engine) AuthorizeSubscribe(ctx context.Context, principal identity.Principal, channel string) error {
	return e.cfg.Authorizer.AuthorizeSubscribe(ctx, principal, channel)
}
