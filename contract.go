package factory

import (
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
)

// Target names the Host that a Factory has selected for one tenant's session.
//
// It exists in this scaffold to state the positive half of the import boundary
// in code rather than in prose: the cross-service contract between Factory and
// Host is Core's wire vocabulary and SessionStore's durable records, and
// nothing else. Every member below is a Core identifier, and the only way to
// build one is from a SessionStore record. There is deliberately no field here
// that could hold a Host or Harness value, because this module cannot name one.
//
// The composition seams themselves — authenticator, authorizer, session store
// interface, target directory, HostLink dialer — are task A0.2's.
type Target struct {
	Tenant  sessionwire.TenantID
	Session sessionwire.SessionID
	Agent   sessionwire.AgentID
	Host    sessionwire.HostID
}

// TargetFor projects a SessionStore capacity row into the identifiers a
// placement decision is recorded with.
//
// The tenant and session are parameters rather than fields read off the row on
// purpose: a HostTarget is capacity, not authority, and it names no tenant and
// no session. Ownership is established against SessionStore's registration
// record and its lease epoch, never inferred from an advertisement.
func TargetFor(tenant sessionwire.TenantID, session sessionwire.SessionID, target sessionstore.HostTarget) Target {
	return Target{
		Tenant:  tenant,
		Session: session,
		Agent:   target.Key.AgentID,
		Host:    target.HostID,
	}
}
