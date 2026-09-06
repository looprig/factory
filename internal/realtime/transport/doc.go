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
// The embedded server is github.com/centrifugal/centrifuge at exactly v0.38.0.
// That version is not "the latest"; v0.39.0 exists. It is the version the
// workspace's Centrifuge compatibility spike measured, against the JavaScript
// client centrifuge@5.7.2 that the browser half is pinned to, so moving it
// would invalidate a measurement rather than merely raise a number.
//
// The Go client is github.com/centrifugal/centrifuge-go at exactly v0.12.0,
// and that is NOT the latest release. v0.12.1 exists. The version was selected
// by the executable compatibility test the runbook asks for, and "executable"
// is the load-bearing word: EVERY row below was built and run, not inferred
// from a go.mod require line. A require is a floor, not a statement about which
// symbols a package exports, so reading one and concluding a version is
// incompatible is unsound -- and was, in fact, wrong here on a first attempt.
//
// Each row is a scratch module requiring centrifuge v0.38.0 plus one client,
// blank-importing both packages so both must compile, under GOWORK=off:
//
//	centrifuge-go  resolved protocol   go build ./...
//	v0.10.12       v0.17.0             exit 0
//	v0.11.0        v0.19.2             exit 0
//	v0.12.0        v0.19.2             exit 0   <- pinned
//	v0.12.1        v0.21.0             exit 1
//
// Minimal version selection resolves ONE protocol version for the whole module,
// because Factory is unusual: in production a Centrifuge server and a
// Centrifuge client are separate processes and need share no library version,
// but Factory embeds the server for ClientLink and dials with the client for
// HostLink in ONE binary, so the two must coexist in one module graph.
//
// The single failing row fails like this:
//
//	centrifuge@v0.38.0/handler_websocket.go:280:22:
//	    undefined: protocol.GetStreamCommandDecoder
//
// The mechanism, read out of the dependency rather than guessed at: that symbol
// was not removed when protocol reached v0.19.x. protocol v0.19.2 exports BOTH
// GetStreamCommandDecoder (decode_stream.go:18) and its replacement
// GetStreamCommandDecoderLimited (decode_stream.go:22). Only at v0.21.0 is the
// unlimited form gone, leaving GetStreamCommandDecoderLimited alone
// (decode_stream.go:45). So the break is at protocol v0.21.0, and therefore at
// centrifuge-go v0.12.1 -- the only client release in range that pulls it.
//
// go list -m -versions reports no release between v0.12.0 and v0.12.1, so
// v0.12.0 is the newest client that coexists with the pinned server.
//
// The consequence for whoever moves these pins next, stated as narrowly as the
// measurement supports: the client is already AT the newest coexisting release,
// so the only client version currently out of reach is v0.12.1, and reaching it
// means moving the embedded server to a release that builds against protocol
// v0.21.0 or later. centrifuge v0.39.0 requires protocol v0.22.1 and is the
// candidate. That server move -- not the client move -- is what carries a real
// price: v0.38.0 is the version the workspace's compatibility spike measured
// against the pinned browser client centrifuge@5.7.2, so moving it invalidates
// a measurement and needs that spike redone. Moving the client alone does not.
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
// # Two findings A6.1 inherits
//
// ClientLinkLimits.PingInterval must REJECT a sub-second value at option
// validation with an OptionError. It must not round it, floor it, or default
// it. The reason is that the wire cannot carry it and the failure is silent and
// misattributed: centrifuge@v0.38.0/client.go:2466 computes
// res.Ping = uint32(c.pingInterval.Seconds()), which truncates 500ms to 0; and
// centrifuge-go@v0.12.0/client.go:1467-1475 assigns c.sendPong = res.Pong
// INSIDE `if res.Ping > 0`, so a client told Ping == 0 never pongs at all even
// though the server set res.Pong. The server then closes a perfectly healthy
// connection with DisconnectNoPong (code 3012, "no pong") every pong timeout.
// A caller who configures 500ms must be told, because otherwise they will read
// the result as a network fault.
//
// A stalled consumer costs its WHOLE connection, and this is structural rather
// than a missing feature. centrifuge v0.38.0 has exactly one BOUNDED outbound
// queue per Client (client.go:247, one *writer per connection). A per-channel
// structure does exist and this doc previously denied it: client.go:248 holds a
// *perChannelWriter (client_experimental.go:249), built only when the node sets
// the experimental Config.GetChannelBatchConfig (client.go:2701-2702,
// config.go:134). It is not a second queue, though -- it is a batching
// aggregator with no bound of its own. Each channelWriter accumulates into a
// plain slice (client_experimental.go:138-146, field buffer) and flushes through
// Client.writeQueueItems (client_experimental.go:109-117), which enqueues into
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
	ServerVersion = "v0.38.0"

	// GoClientModule is the official Go client, for HostLink.
	GoClientModule  = "github.com/centrifugal/centrifuge-go"
	GoClientVersion = "v0.12.0"

	// ProtocolModule is the wire library BOTH of the above depend on, and the
	// reason GoClientVersion is not the latest. It is pinned explicitly
	// because it is the value the compatibility constraint is actually about:
	// the constraint is "protocol below v0.21.0", not anything about the
	// client's own version number.
	ProtocolModule  = "github.com/centrifugal/protocol"
	ProtocolVersion = "v0.19.2"

	// BrowserClientPackage and BrowserClientVersion are the npm client the
	// spike selected against ServerVersion. Nothing in Go can hold this pin,
	// so it is recorded here beside the server it was measured against and is
	// named in the README.
	BrowserClientPackage = "centrifuge"
	BrowserClientVersion = "5.7.2"
)
