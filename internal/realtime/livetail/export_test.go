package livetail

import sessionwire "github.com/looprig/core/sessionwire/v1"

// Pending reports one session's queued events and whether a repair (a lost
// tail) is among them, so a test can wait on the plane's state rather than on
// a timer.
func Pending(p *Plane, tenant sessionwire.TenantID, session sessionwire.SessionID) (events int, lost bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.sessions[sessionKey{tenant: tenant, session: session}]
	if s == nil {
		return 0, false
	}
	for _, ev := range s.events {
		lost = lost || ev.kind == evLost
	}
	return len(s.events), lost
}

// Sessions reports how many sessions the plane holds state for.
func Sessions(p *Plane) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.sessions)
}
