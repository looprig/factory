// Package command holds the durable command vocabulary Factory admits.
//
// It is a VOCABULARY, not a dependency seam, which is why it is shared rather
// than restated by each caller. Specification section 8.1 makes the REST
// control routes and the ClientLink RPCs two spellings of one admission
// contract: the same principal, the same authorization decision and the same
// durable record. Two packages holding two private copies of "gate_response"
// would satisfy every test either package could write and still let a ClientLink
// RPC be authorized under a kind the REST route never admits.
//
// sessionstore deliberately does not enumerate these. Its CommandKind doc says
// the set is left open so a later Core wire version can add one, and a closed
// set there would refuse a command Core itself considers valid. Factory's own
// surface is closed -- it serves exactly the operations routeTable and the
// ClientLink method vocabulary name -- so the enumeration belongs here.
package command

import "github.com/looprig/sessionstore"

// The command kinds this Factory admits. The values are the durable record's,
// so they are wire-visible and may not be changed to suit a caller.
const (
	// KindCreateSession allocates a session.
	KindCreateSession sessionstore.CommandKind = "create"
	// KindInput delivers a turn's input.
	KindInput sessionstore.CommandKind = "input"
	// KindInterrupt asks a running turn to stop.
	KindInterrupt sessionstore.CommandKind = "interrupt"
	// KindRestore resumes a session that is not resident.
	KindRestore sessionstore.CommandKind = "restore"
	// KindGateResponse answers a gate.
	KindGateResponse sessionstore.CommandKind = "gate_response"
)

// Kinds returns the vocabulary in a stable order.
//
// It is a function rather than a package variable so no caller can hold a
// reference that lets it add a kind at run time.
func Kinds() []sessionstore.CommandKind {
	return []sessionstore.CommandKind{
		KindCreateSession,
		KindInput,
		KindInterrupt,
		KindRestore,
		KindGateResponse,
	}
}
