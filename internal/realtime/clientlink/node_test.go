package clientlink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	centrifuge "github.com/centrifugal/centrifuge"
	centrifugego "github.com/centrifugal/centrifuge-go"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	internalidentity "github.com/looprig/factory/internal/identity"
	"github.com/looprig/sessionstore"
)

// This file is IN the package on purpose, and there are exactly two reasons.
//
// nodeConfig is unexported, and reading it is the only way to assert that an
// optional transport mechanism is off: a node keeps its configuration private
// once built, so asserting the absence of a broker from outside would mean
// asserting on nothing. internal/realtime/transport's package doc recorded that
// gap and deferred it here, to the first production node constructor.
//
// And the byte budget can only be measured by PUBLISHING to a stalled consumer,
// which needs the node. Handler exports no Publish yet -- fan-out is A6.3's and
// A7's -- and exporting one early to make a test convenient would put a method
// on the production surface that nothing in production calls.

// TestTheNodeConfigurationLeavesEveryOptionalMechanismOff is the "cheap real
// version" the transport spike promised A6.1 would supply.
//
// Each nil is a mechanism that would otherwise be reachable, and the last one
// is the one this program has already had to correct a claim about: setting
// GetChannelBatchConfig is what builds the experimental perChannelWriter
// (centrifuge@v0.38.0/client.go:2701-2702), and that structure would add an
// unbounded buffer in front of the one bounded queue without giving a channel a
// failure domain of its own.
func TestTheNodeConfigurationLeavesEveryOptionalMechanismOff(t *testing.T) {
	t.Parallel()

	cfg := nodeConfig(Config{Version: "v-under-test", Limits: nodeTestLimits()})

	for name, set := range map[string]bool{
		"GetBroker":             cfg.GetBroker != nil,
		"GetPresenceManager":    cfg.GetPresenceManager != nil,
		"GetChannelBatchConfig": cfg.GetChannelBatchConfig != nil,
	} {
		if set {
			t.Errorf("%s is set, so the mechanism it enables is reachable", name)
		}
	}
	if cfg.LogLevel != centrifuge.LogLevelNone {
		t.Errorf("LogLevel = %v, want LogLevelNone", cfg.LogLevel)
	}
	if cfg.Version != "v-under-test" {
		t.Errorf("Version = %q, want the composed build identity", cfg.Version)
	}
}

// TestTheLimitsReachTheFieldsThatEnforceThem is the reader that separates "the
// limits were accepted" from "the limits were applied".
//
// The two values are absolute literals, different from each other and from
// every default, so a constructor that swapped them or dropped one fails here.
func TestTheLimitsReachTheFieldsThatEnforceThem(t *testing.T) {
	t.Parallel()

	limits := nodeTestLimits()
	limits.PerConnectionQueueBytes = 7777
	limits.MaxChannelsPerConnection = 33

	cfg := nodeConfig(Config{Version: "v", Limits: limits})
	if cfg.ClientQueueMaxSize != 7777 {
		t.Errorf("ClientQueueMaxSize = %d, want 7777", cfg.ClientQueueMaxSize)
	}
	if cfg.ClientChannelLimit != 33 {
		t.Errorf("ClientChannelLimit = %d, want 33", cfg.ClientChannelLimit)
	}
}

// TestTheTransportConfigurationIsTheOnlyPlaceTheCadenceIsStated is the reader
// the ping cadence would otherwise have none of.
//
// A mutation probe established that: dropping the per-connection PingPongConfig
// from ConnectReply killed nothing, because the transport carried the identical
// value, and dropping the transport's would not be visible either -- an idle
// connection under the library's own 25s default behaves exactly like an idle
// connection under a configured one. So the cadence has no BEHAVIOURAL reader
// inside this module, and the honest guard is a structural one over the
// configuration actually handed to the transport.
//
// The values are absolute literals, all different from each other and from
// every default, so a constructor that swapped two of them fails here.
func TestTheTransportConfigurationIsTheOnlyPlaceTheCadenceIsStated(t *testing.T) {
	t.Parallel()

	limits := nodeTestLimits()
	limits.WriteTimeout = 3 * time.Second
	limits.PingInterval = 11 * time.Second
	limits.PongTimeout = 7 * time.Second

	cfg := websocketConfig(Config{Version: "v", Limits: limits})
	if cfg.WriteTimeout != 3*time.Second {
		t.Errorf("WriteTimeout = %v, want 3s", cfg.WriteTimeout)
	}
	if cfg.PingPongConfig.PingInterval != 11*time.Second {
		t.Errorf("PingInterval = %v, want 11s", cfg.PingPongConfig.PingInterval)
	}
	if cfg.PingPongConfig.PongTimeout != 7*time.Second {
		t.Errorf("PongTimeout = %v, want 7s", cfg.PingPongConfig.PongTimeout)
	}
	// permessage-deflate is a per-connection memory and CPU cost at the
	// 1,000-5,000 connection scale. A5.1 measured that a mutant enabling it
	// survived its whole suite, which is why it is asserted here rather than
	// left to the absence of a setter.
	if cfg.Compression {
		t.Error("Compression is on")
	}
	// CheckOrigin admits everything ON PURPOSE: internal/httpapi's guard owns
	// the origin decision and runs first in the composed chain. This asserts
	// the deliberate choice so that a later reader finds it stated rather than
	// discovering an apparently missing check.
	if cfg.CheckOrigin == nil {
		t.Fatal("CheckOrigin is nil, so the library's own same-origin default applies and the guard is not the only answer")
	}
	if !cfg.CheckOrigin(httptest.NewRequest("GET", "/v1/realtime", nil)) {
		t.Error("CheckOrigin refused a request; origin belongs to internal/httpapi's guard")
	}
}

// TestPerConnectionQueueBytesIsMeasuredInBytes is the unit, established by
// execution rather than by reading the dependency's documentation.
//
// The field said "in messages" until this task, with a default of 256, which
// would have configured a 256-BYTE budget and closed essentially every
// connection on its first event. Both halves are needed to read the number as
// bytes: a 4 KiB budget must fail on a payload load far under 4,096 MESSAGES,
// and a 16 MiB budget must survive the same load. One half alone would be
// satisfied by a budget in either unit.
func TestPerConnectionQueueBytesIsMeasuredInBytes(t *testing.T) {
	t.Parallel()

	// A fixed total of about four mebibytes, as 1,024 publications of 4 KiB.
	//
	// The total is what the case is built around, and the first version got it
	// wrong: 64 publications (256 KiB) was measured SURVIVING a 4 KiB budget,
	// because a stalled consumer's socket and read buffers absorb a few hundred
	// kibibytes before the server's queue grows at all. Four mebibytes is well
	// past anything a kernel buffers and well inside 16 MiB, so the separation
	// is between the two BUDGETS rather than between two arrival rates.
	//
	// It is also far below 4,096 MESSAGES, which is what makes the pair read
	// the number as bytes: a message-counting budget of 4,096 would not close
	// the first connection either.
	const (
		publications = 1024
		payloadBytes = 4096
	)

	t.Run("a four kibibyte budget closes a stalled consumer", func(t *testing.T) {
		t.Parallel()

		event, closed := stalledConsumer(t, 1<<12, publications, payloadBytes)
		if !closed {
			t.Fatalf("a stalled consumer with a 4 KiB budget survived %d publications of %d bytes (%d KiB in total)",
				publications, payloadBytes, publications*payloadBytes/1024)
		}
		// 3008 is DisconnectSlow, the queue bound. Asserting the code rather
		// than "it was closed" is what separates the budget from the write
		// deadline, which is generous here for exactly that reason.
		if event.Code != centrifuge.DisconnectSlow.Code {
			t.Errorf("the connection ended with code %d (%s), want %d (slow)",
				event.Code, event.Reason, centrifuge.DisconnectSlow.Code)
		}
	})

	t.Run("a sixteen mebibyte budget survives the same load", func(t *testing.T) {
		t.Parallel()

		event, closed := stalledConsumer(t, 1<<24, publications, payloadBytes)
		if closed {
			t.Errorf("a stalled consumer with a 16 MiB budget was closed with code %d (%s) by %d publications of %d bytes",
				event.Code, event.Reason, publications, payloadBytes)
		}
	})
}

// stalledConsumer subscribes a client whose publication callback BLOCKS, then
// publishes a fixed load and reports whether the server closed the connection.
//
// The write deadline is set far longer than the case can take, so a close here
// is attributable to the queue budget and not to the other bound.
func stalledConsumer(t *testing.T, queueBytes, publications, payloadBytes int) (centrifuge.DisconnectEvent, bool) {
	t.Helper()

	limits := nodeTestLimits()
	limits.PerConnectionQueueBytes = queueBytes
	limits.WriteTimeout = 30 * time.Second
	limits.PingInterval = 60 * time.Second
	limits.PongTimeout = 30 * time.Second

	v := &nodeVerifier{}
	authenticator, err := internalidentity.NewAuthenticator(internalidentity.Config{Verifier: v})
	if err != nil {
		t.Fatalf("NewAuthenticator: %v", err)
	}
	handler, err := NewHandler(Config{
		Authenticator: authenticator,
		Authorizer:    internalidentity.Authorizer{},
		Limits:        limits,
		Version:       "v-stall",
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}

	disconnects := make(chan centrifuge.DisconnectEvent, 8)
	// The node's own disconnect event is the SERVER's answer. The client's is
	// not usable here: a stalled consumer's event loop is stuck inside the
	// blocking callback and cannot report anything, which is itself part of the
	// blast radius A5.1 recorded.
	handler.node.OnConnect(func(client *centrifuge.Client) {
		handler.connected(client)
		client.OnDisconnect(func(e centrifuge.DisconnectEvent) {
			select {
			case disconnects <- e:
			default:
			}
		})
	})

	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = handler.Shutdown(ctx)
	})

	data, err := json.Marshal(map[string]string{"protocol_version": ProtocolVersion})
	if err != nil {
		t.Fatalf("marshal connect data: %v", err)
	}
	client := centrifugego.NewJsonClient("ws"+strings.TrimPrefix(server.URL, "http"), centrifugego.Config{
		Token: nodeTestToken,
		Data:  data,
	})
	connected := make(chan struct{}, 1)
	client.OnConnected(func(centrifugego.ConnectedEvent) {
		select {
		case connected <- struct{}{}:
		default:
		}
	})
	t.Cleanup(client.Close)
	if err := client.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	select {
	case <-connected:
	case <-time.After(30 * time.Second):
		t.Fatal("no connected event arrived")
	}

	blocked := make(chan struct{})
	var release sync.Once
	unblock := func() { release.Do(func() { close(blocked) }) }
	t.Cleanup(unblock)

	channel := "session:tenant-a:stalled"
	sub, err := client.NewSubscription(channel)
	if err != nil {
		t.Fatalf("NewSubscription: %v", err)
	}
	subscribed := make(chan struct{}, 1)
	sub.OnSubscribed(func(centrifugego.SubscribedEvent) {
		select {
		case subscribed <- struct{}{}:
		default:
		}
	})
	// This callback BLOCKS, which is what a slow consumer IS.
	sub.OnPublication(func(centrifugego.PublicationEvent) { <-blocked })
	if err := sub.Subscribe(); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	select {
	case <-subscribed:
	case <-time.After(30 * time.Second):
		t.Fatal("the subscription was never established")
	}

	payload := []byte(`"` + strings.Repeat("p", payloadBytes) + `"`)
	for range publications {
		if _, err := handler.node.Publish(channel, payload); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		select {
		case event := <-disconnects:
			unblock()
			return event, true
		default:
		}
	}
	// The overflow close is issued from the publish path, but in a goroutine,
	// so a bounded wait is needed after the last publication. It is bounded by
	// a deadline rather than by a poll count because nothing here asserts that
	// anything is fast.
	select {
	case event := <-disconnects:
		unblock()
		return event, true
	case <-time.After(5 * time.Second):
	}
	unblock()
	return centrifuge.DisconnectEvent{}, false
}

// TestLimitsValidate is the table for the second validation, the one that
// bounds what reaches the engine rather than what a deployer supplied.
func TestLimitsValidate(t *testing.T) {
	t.Parallel()

	if err := nodeTestLimits().Validate(); err != nil {
		t.Fatalf("the limits every row starts from are themselves rejected: %v", err)
	}
	for _, tt := range []struct {
		name   string
		mutate func(*Limits)
		want   string
	}{
		{"no connections", func(l *Limits) { l.MaxConnections = 0 }, "MaxConnections"},
		{"no channel ceiling", func(l *Limits) { l.MaxChannelsPerConnection = 0 }, "MaxChannelsPerConnection"},
		{"no queue budget", func(l *Limits) { l.PerConnectionQueueBytes = 0 }, "PerConnectionQueueBytes"},
		{"negative queue budget", func(l *Limits) { l.PerConnectionQueueBytes = -1 }, "PerConnectionQueueBytes"},
		{"no write timeout", func(l *Limits) { l.WriteTimeout = 0 }, "WriteTimeout"},
		{"no pong deadline", func(l *Limits) { l.PongTimeout = 0 }, "PongTimeout"},
		// The inherited finding, restated at this boundary: 999ms is the last
		// value below the floor and is written as an absolute literal.
		{"a sub-second ping cadence", func(l *Limits) { l.PingInterval = 999 * time.Millisecond }, "PingInterval"},
		{"no ping cadence", func(l *Limits) { l.PingInterval = 0 }, "PingInterval"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			limits := nodeTestLimits()
			tt.mutate(&limits)
			err := limits.Validate()
			if err == nil {
				t.Fatalf("Validate() = nil for %+v, want an error naming %q", limits, tt.want)
			}
			if !errors.Is(err, ErrInvalidConfig) {
				t.Errorf("error %v does not wrap ErrInvalidConfig", err)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error %q does not name %q", err, tt.want)
			}
		})
	}
}

// TestAPingCadenceOfExactlyOneSecondIsAccepted pins the boundary the row above
// stops one step short of, with an absolute literal rather than the constant.
func TestAPingCadenceOfExactlyOneSecondIsAccepted(t *testing.T) {
	t.Parallel()

	limits := nodeTestLimits()
	limits.PingInterval = time.Second
	limits.PongTimeout = 500 * time.Millisecond
	limits.WriteTimeout = 400 * time.Millisecond
	if err := limits.Validate(); err != nil {
		t.Errorf("a one-second ping cadence was rejected: %v", err)
	}
}

// TestNewEngineRefusesAnIncompleteComposition holds each required seam.
func TestNewEngineRefusesAnIncompleteComposition(t *testing.T) {
	t.Parallel()

	complete := Config{
		Authenticator: stubAuthenticator{},
		Authorizer:    internalidentity.Authorizer{},
		Limits:        nodeTestLimits(),
		Version:       "v",
	}
	if _, err := NewEngine(complete); err != nil {
		t.Fatalf("the complete composition every row starts from was rejected: %v", err)
	}
	for _, tt := range []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"no authenticator", func(c *Config) { c.Authenticator = nil }, "Authenticator"},
		{"no authorizer", func(c *Config) { c.Authorizer = nil }, "Authorizer"},
		{"no version", func(c *Config) { c.Version = "" }, "Version"},
		{"unusable limits", func(c *Config) { c.Limits = Limits{} }, "MaxConnections"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := complete
			tt.mutate(&cfg)
			engine, err := NewEngine(cfg)
			if err == nil {
				t.Fatalf("NewEngine() = %+v, want an error naming %q", engine, tt.want)
			}
			if !errors.Is(err, ErrInvalidConfig) {
				t.Errorf("error %v does not wrap ErrInvalidConfig", err)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error %q does not name %q", err, tt.want)
			}
		})
	}
}

// TestCommandKindForCoversTheVocabularyAndNothingElse pins the mapping with
// absolute strings on both sides.
func TestCommandKindForCoversTheVocabularyAndNothingElse(t *testing.T) {
	t.Parallel()

	for method, want := range map[Method]sessionstore.CommandKind{
		"session.create":    "create",
		"session.input":     "input",
		"session.interrupt": "interrupt",
		"session.restore":   "restore",
		"gate.respond":      "gate_response",
	} {
		kind, ok := CommandKindFor(method)
		if !ok {
			t.Errorf("CommandKindFor(%q) reported the method unknown", method)
			continue
		}
		if kind != want {
			t.Errorf("CommandKindFor(%q) = %q, want %q", method, kind, want)
		}
	}
	for _, method := range []Method{"", "session", "session.destroy", "SESSION.INPUT", "session.input "} {
		if kind, ok := CommandKindFor(method); ok {
			t.Errorf("CommandKindFor(%q) = %q, want the method to be unknown", method, kind)
		}
	}
	if got := len(Methods()); got != 5 {
		t.Errorf("Methods() has %d entries, want 5", got)
	}
}

// TestConnectRefusalClassifiesEachFailure drives the classifier directly, which
// is the only place the DEFAULT arm can be reached with something that is not a
// transport failure.
func TestConnectRefusalClassifiesEachFailure(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		err  error
		want uint32
	}{
		{"an unsupported protocol", fmt.Errorf("%w: 999", ErrUnsupportedProtocol), 3506},
		// The expired arm must come before the unauthenticated one: expiry
		// WRAPS ErrUnauthenticated, so the reverse order would classify every
		// expired credential as invalid and send the user to login instead of
		// to a refresh.
		{"an expired credential", identity.ErrCredentialExpired, 3005},
		{"an unverifiable credential", identity.ErrUnauthenticated, 3500},
		{"a verifier that could not answer", errors.New("connection refused"), 3004},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var disconnect centrifuge.Disconnect
			if !errors.As(connectRefusal(tt.err), &disconnect) {
				t.Fatalf("connectRefusal(%v) is not a centrifuge.Disconnect", tt.err)
			}
			if disconnect.Code != tt.want {
				t.Errorf("connectRefusal(%v) = %d (%s), want %d", tt.err, disconnect.Code, disconnect.Reason, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------

const nodeTestToken = "node-test-token"

func nodeTestLimits() Limits {
	return Limits{
		MaxConnections:           64,
		MaxChannelsPerConnection: 256,
		PerConnectionQueueBytes:  1 << 20,
		WriteTimeout:             5 * time.Second,
		PingInterval:             25 * time.Second,
		PongTimeout:              10 * time.Second,
	}
}

type nodeVerifier struct{}

func (nodeVerifier) VerifyCredential(_ context.Context, credential identity.Credential) (identity.Claims, error) {
	if credential.Value() != nodeTestToken {
		return identity.Claims{}, fmt.Errorf("%w: no such credential", identity.ErrUnauthenticated)
	}
	return identity.Claims{
		Tenant:    sessionwire.TenantID("tenant-a"),
		Subject:   "user-a",
		Kind:      identity.KindActor,
		ExpiresAt: time.Now().Add(time.Hour),
	}, nil
}

type stubAuthenticator struct{}

func (stubAuthenticator) AuthenticateLink(context.Context, string) (identity.Principal, error) {
	return identity.Principal{}, nil
}
