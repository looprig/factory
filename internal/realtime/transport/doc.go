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
// The Go client is github.com/centrifugal/centrifuge-go at exactly v0.10.12,
// and that is NOT the latest release. It was selected by the executable
// compatibility test the runbook asks for, and the test's answer was a refusal:
//
//	centrifuge-go v0.12.1 requires centrifugal/protocol v0.21.0
//	centrifuge-go v0.11.0 requires centrifugal/protocol v0.19.2
//	centrifuge   v0.38.0  requires centrifugal/protocol v0.17.0
//
// Minimal version selection resolves one protocol version for the whole module,
// and the server does not build against the raised one:
//
//	centrifuge@v0.38.0/handler_websocket.go:280:22:
//	    undefined: protocol.GetStreamCommandDecoder
//
// So v0.11.0 and above cannot be used here AT ALL, and that is a property of
// this module rather than of the two libraries. In production a Centrifuge
// server and a Centrifuge client are ordinarily separate processes and need
// share no library version -- but Factory is both at once. It embeds the server
// for ClientLink and dials with the client for HostLink, in one binary, so the
// two must coexist in ONE module graph. v0.10.12 is the newest release that
// does: it asks for protocol v0.16.0, which resolves up to the server's v0.17.0
// and builds.
//
// The consequence for whoever moves these pins next: the Go client cannot be
// upgraded past v0.10.12 until the embedded server moves to a release that
// builds against protocol v0.19.2 or later. centrifuge v0.39.0 requires
// protocol v0.22.1 and is a candidate, but it is not the version the workspace
// spike measured against the pinned browser client, so that is a decision with
// its own measurement to redo -- not a version bump.
//
// # What is deliberately off
//
// No Redis or NATS broker, no standalone Centrifugo, no history, no recovery,
// no presence, no join/leave, no WebSocket compression, and no client
// publication. Sessions are durable in SessionStore; a transport that also
// remembered them would be a second, weaker answer to the same question, and
// Centrifuge history is explicitly not Looprig's session cursor. Each of those
// is asserted here, not merely left unset, because "we did not turn it on" and
// "it is off" are different claims after a configuration refactor.
package transport

// The exact modules and versions this package qualifies. They are duplicated
// from go.mod ON PURPOSE and held equal to it by TestThePinnedVersionsAreWhatGoModRequires,
// so a `go get -u` that moved either one fails here, where the measurement
// lives, rather than passing quietly.
const (
	// ServerModule is the embedded Centrifuge server library.
	ServerModule  = "github.com/centrifugal/centrifuge"
	ServerVersion = "v0.38.0"

	// GoClientModule is the official Go client, for HostLink.
	GoClientModule  = "github.com/centrifugal/centrifuge-go"
	GoClientVersion = "v0.10.12"

	// ProtocolModule is the wire library BOTH of the above depend on, and the
	// reason GoClientVersion is not the latest. It is pinned explicitly
	// because it is the value the compatibility constraint is actually about.
	ProtocolModule  = "github.com/centrifugal/protocol"
	ProtocolVersion = "v0.17.0"

	// BrowserClientPackage and BrowserClientVersion are the npm client the
	// spike selected against ServerVersion. Nothing in Go can hold this pin,
	// so it is recorded here beside the server it was measured against and is
	// named in the README.
	BrowserClientPackage = "centrifuge"
	BrowserClientVersion = "5.7.2"
)
