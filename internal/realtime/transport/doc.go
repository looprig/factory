// Package transport is where Factory's realtime transport dependency is pinned
// and where its behaviour is MEASURED rather than assumed.
//
// It holds no Factory logic and is imported by no other package. That is the
// point of it: A5.1 asks for the transport to be pinned, isolated and proven
// before ClientLink (A6) and HostLink (A7) are built on top of it, and a proof
// that lives inside the package it is meant to qualify would have to be
// rewritten the moment that package acquires a real engine.
//
// Everything asserted here is a property of the DEPENDENCY, not of Factory.
// That is unusual for this module -- elsewhere a test that only proved a
// dependency worked would be a defect -- and it is deliberate here, because the
// deliverable of a transport-qualification task IS the dependency's measured
// behaviour. The engines that consume it get their own tests, over their own
// code, against a substituted seam.
//
// # What is pinned
//
// The embedded server is github.com/centrifugal/centrifuge at exactly v0.39.3,
// the Go client github.com/centrifugal/centrifuge-go at exactly v0.12.1 (the
// newest client release), and the wire library both resolve is
// github.com/centrifugal/protocol v0.22.1, which the server requires.
//
// Minimal version selection resolves ONE protocol version for the whole module,
// because Factory is unusual: in production a Centrifuge server and a
// Centrifuge client are separate processes and need share no library version,
// but Factory embeds the server for ClientLink and dials with the client for
// HostLink in ONE binary, so the two must coexist in one module graph.
//
// That constraint is what held the previous pins (server v0.38.0, client
// v0.12.0, protocol v0.19.2). The compatibility test was executable -- each row
// a scratch module requiring centrifuge v0.38.0 plus one client, blank-
// importing both, under GOWORK=off -- and it is kept because it explains the
// shape of the move:
//
//	centrifuge-go  resolved protocol   go build ./...   (server v0.38.0)
//	v0.10.12       v0.17.0             exit 0
//	v0.11.0        v0.19.2             exit 0
//	v0.12.0        v0.19.2             exit 0   <- previously pinned
//	v0.12.1        v0.21.0             exit 1
//
// The failing row failed at centrifuge@v0.38.0/handler_websocket.go:280:22,
// `undefined: protocol.GetStreamCommandDecoder`: protocol v0.21.0 dropped the
// unlimited decoder in favour of GetStreamCommandDecoderLimited. v0.39.x builds
// against the limited form (centrifuge@v0.39.3/handler_websocket.go:406), so
// moving the SERVER is what unlocked the client, and the whole triple moved
// together: server v0.39.3, client v0.12.1, protocol v0.22.1. The client move
// itself is a protocol bump only -- centrifuge-go v0.12.1's client.go is
// byte-identical to v0.12.0's, so every centrifuge-go line cited in this module
// is still exact.
//
// Why the server moved: centrifuge v0.38.0's Node.Shutdown never closed the
// per-Node eagle metrics aggregator, leaking one goroutine per Node; v0.39.0
// closes it (centrifuge@v0.39.3/node.go:438-442), and
// TestNodeShutdownLeavesNoMetricsAggregatorRunning holds that fix in place.
// The browser client pin, centrifuge@5.7.2, was re-measured against v0.39.3
// with the same harness that selected it against v0.38.0.
//
// What the move did NOT fix: centrifuge-go's close/reconnect races
// (Client.send reading c.transport without c.mu, and handle running a pending
// request's callback that Close's clearConnectedState also runs) are fixed on
// centrifuge-go's master branch but in no release as of v0.12.1, so HostLink's
// closeBound and Close ordering, and the -race skip on
// TestReconnectStressNeverWedgesACaller, all stay.
//
// # What this suite does and does not measure
//
// The JSON protocol only: every case here drives centrifugego.NewJsonClient.
// Protobuf is never exercised. That is within the runbook's scope, which asks
// for JSON RPC correlation, but HostLink (A7.1) has no browser at either end
// and may reasonably want protobuf -- in which case it is unmeasured here and
// needs its own qualification rather than an assumption of symmetry.
//
// # What is deliberately off, and which of those are ASSERTED
//
// The distinction matters more than the list, because "we did not turn it on"
// and "it is off" are different claims after a configuration refactor, and only
// the second survives one. Both kinds are recorded, labelled, so that a later
// maintainer is not told a guard exists where none does.
//
// ASSERTED -- a mutant that enables any of these is killed by a case here:
//
//	history and recovery      the subscription is neither positioned nor recoverable
//	presence and join/leave   the channel reports no presence and pushes no join
//	client publication        a client-side publish is refused
//	WebSocket compression     permessage-deflate is declined in the handshake
//
// Compression is the newest of those and was, at one point, the declared gap in
// this package: a test driven by centrifuge-go cannot see the setting at all,
// so both settings looked identical through its API and a mutant flipping
// Compression to true survived the whole suite. The reason is worth stating
// exactly, because the obvious lever looks like it should work and does not.
// centrifugego.Config DOES carry EnableCompression (config.go:68), and it is
// passed through to the dialer (transport_websocket.go:110) -- so the client can
// OFFER permessage-deflate. What it never surfaces is the answer: neither the
// handshake response nor the negotiated extension is reachable through the
// client's API, so offering the extension tells a test nothing about whether
// the server took it.
//
// It is closed now, and closed WITHOUT a new dependency edge, which was the
// constraint. The obvious closure is a raw gorilla/websocket client that offers
// the extension and reads the response -- but that would promote
// gorilla/websocket from indirect to a direct requirement of this module,
// making Factory name a transport-internal library in its own go.mod and pin a
// version it does not otherwise control (today v1.5.3 arrives via centrifuge,
// and a direct require would let the two drift and then need its own guard).
// That edge is refused. TestWebSocketCompressionIsRefusedInTheHandshake instead
// speaks the RFC 6455 opening handshake over a plain TCP connection using only
// the standard library, and reads Sec-WebSocket-Extensions off the 101
// response. It carries its own control -- the same probe against a server with
// compression ON, which must see the extension -- because "no extension header
// came back" is worthless from a probe that might never have offered one.
//
// NOT ASSERTED -- absent rather than refused, and deliberately so:
//
//	Redis and NATS brokers    newServer sets neither GetBroker nor
//	                          GetPresenceManager, so the node keeps its default
//	                          in-process memory broker
//	standalone Centrifugo     no external process is started anywhere here
//
// These are not asserted because asserting the absence of a broker means
// asserting on a config field that nothing reads, which tests the test. The
// cheap real version belongs to A6.1, where a production node constructor will
// exist: assert there that the constructor leaves GetBroker nil.
//
// Sessions are durable in SessionStore; a transport that also remembered them
// would be a second, weaker answer to the same question, and Centrifuge history
// is explicitly not Looprig's session cursor.
//
// # Two findings A6.1 inherited
//
// Both are discharged, and this section is kept as the MEASUREMENT rather than
// as a to-do: internal/realtime/clientlink holds the guards, and what makes
// them right is here.
//
// ClientLinkLimits.PingInterval must REJECT a sub-second value at option
// validation with an OptionError. It must not round it, floor it, or default
// it. The reason is that the wire cannot carry it and the failure is silent and
// misattributed: centrifuge@v0.39.3/client.go:3384 computes
// res.Ping = uint32(c.pingInterval.Seconds()), which truncates 500ms to 0; and
// centrifuge-go@v0.12.0/client.go:1467-1475 assigns c.sendPong = res.Pong
// INSIDE `if res.Ping > 0`, so a client told Ping == 0 never pongs at all even
// though the server set res.Pong. The server then closes a perfectly healthy
// connection with DisconnectNoPong (code 3012, "no pong") every pong timeout.
// A caller who configures 500ms must be told, because otherwise they will read
// the result as a network fault.
//
// A stalled consumer costs its WHOLE connection, and this is structural rather
// than a missing feature. centrifuge v0.39.3 has exactly one BOUNDED outbound
// queue per Client (client.go:299, one *writer per connection). A per-channel
// structure does exist and this doc previously denied it: client.go:300 holds a
// *perChannelWriter (client_experimental.go:246), built only when the node sets
// the experimental Config.GetChannelBatchConfig (client.go:3759-3760,
// config.go:231). It is not a second queue, though -- it is a batching
// aggregator with no bound of its own. Each channelWriter accumulates into a
// plain slice (client_experimental.go:137-146, field buffer) and flushes through
// Client.writeQueueItems (client_experimental.go:108-116), which enqueues into
// the single messageWriter and, when THAT overflows, closes the whole
// connection. So the precise claim is that no per-channel outbound queue BOUND
// exists anywhere in the library: turning batching on adds an unbounded buffer
// in front of the one bound, it does not give a channel a failure domain. This
// is why selective reset of one subscription is not merely unimplemented --
// there is no structure that could carry it. A7.3's per-binding backpressure
// repair therefore cannot be delegated to the transport; it must be
// Factory-owned, above a transport whose only backpressure verb is "drop the
// connection".
package transport

// The exact modules and versions this package qualifies. They are duplicated
// from go.mod ON PURPOSE and held equal to it by
// TestThePinnedVersionsAreWhatGoModRequires, and to the README's table by
// TestTheREADMEVersionTableMatchesThePins, so a `go get -u` that moved either
// one fails here, where the measurement lives, rather than passing quietly.
const (
	// ServerModule is the embedded Centrifuge server library.
	ServerModule  = "github.com/centrifugal/centrifuge"
	ServerVersion = "v0.39.3"

	// GoClientModule is the official Go client, for HostLink.
	GoClientModule  = "github.com/centrifugal/centrifuge-go"
	GoClientVersion = "v0.12.1"

	// ProtocolModule is the wire library BOTH of the above depend on. It is
	// pinned explicitly because it is the value the compatibility constraint
	// is actually about: the server and the client share ONE protocol version
	// in this module, and v0.38.0's server needed one below v0.21.0.
	ProtocolModule  = "github.com/centrifugal/protocol"
	ProtocolVersion = "v0.22.1"

	// BrowserClientPackage and BrowserClientVersion are the npm client the
	// spike selected against ServerVersion. Nothing in Go can hold this pin,
	// so it is recorded here beside the server it was measured against and is
	// named in the README.
	BrowserClientPackage = "centrifuge"
	BrowserClientVersion = "5.7.2"
)
