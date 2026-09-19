package hostlink

import (
	"context"
	"fmt"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// GateResponseCapable is THE ONE STATEMENT of "the Host at the other end of
// this link can apply a gate_response command", read from the Host's connect
// reply. Every gate on gate_response delivery in this module -- admission's
// refusal before anything is written, and placement's withheld wake -- asks
// it through Pool.AcceptsGateResponses, and nothing else decides.
//
// The signal is Core's capability token HostLinkCapabilityGateResponse
// ("hostlink.command.gate_response", core v0.11.0), which a Host that applies
// gate_response through the disposition path (host >= v0.4.0) lists in its
// connect reply's hostlink_methods. It is a TOKEN, not a method: nothing is
// dispatched on it. The match is Supports' exact-name match, so a substring,
// a "hostlink.v1."-prefixed spelling or any other near miss is not the
// capability, and a Host that predates it (host v0.3.0 advertises only the
// five reserved methods) is refused, which keeps its behaviour unchanged.
//
// It deliberately reads NOTHING else. In particular the catalog's LeaseEpoch
// is never read as a capability signal (owner ruling, 2026-09-19): a
// per-session, caller-asserted value cannot answer a per-process question.
func GateResponseCapable(reply sessionwire.VersionNegotiationResponse) bool {
	return reply.Supports(sessionwire.HostLinkCapabilityGateResponse)
}

// Negotiator is the capability of a Link that can report the connect reply its
// Host last sent. It is discovered by assertion, like Subscriber, so a Link a
// test or a later transport supplies without it fails the capability question
// CLOSED rather than being asked something it cannot answer.
type Negotiator interface {
	// Negotiated returns the Host's most recent verified connect reply. It
	// fails with ErrLinkReconnecting between a dropped connection and the
	// next reply -- there is no reply to read then, and the previous one may
	// belong to a Host that has since restarted at another version -- and
	// with the link's terminal error once it will never answer again.
	Negotiated() (sessionwire.VersionNegotiationResponse, error)
}

// Negotiated implements Negotiator.
func (l *centrifugeLink) Negotiated() (sessionwire.VersionNegotiationResponse, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.terminal != nil {
		return sessionwire.VersionNegotiationResponse{}, l.terminal
	}
	if l.connecting {
		return sessionwire.VersionNegotiationResponse{}, fmt.Errorf("hostlink: capability read: %w", ErrLinkReconnecting)
	}
	return l.negotiated, nil
}

// AcceptsGateResponses reports whether the Host at target can apply a
// gate_response command for tenantID's sessions, asked of THAT TENANT'S link
// -- the one the command would be read over -- and answered by the pool's
// gate-response predicate (GateResponseCapable unless the composition
// supplied another).
//
// The link is acquired, dialling if needed, under the pool's lock as Attach
// acquires one, and read outside it. A link that cannot report its reply
// answers false: an unknown capability is not one. A read that fails --
// reconnecting, or a link that turned terminal -- is returned, and a terminal
// link is evicted as Attach evicts one, so the caller can tell "this Host
// cannot" (false, nil) from "this Host could not be asked" (false, err).
func (p *Pool) AcceptsGateResponses(ctx context.Context, target Target, tenantID sessionwire.TenantID) (bool, error) {
	if err := target.Validate(); err != nil {
		return false, err
	}
	dial, err := tenantTarget(target, tenantID)
	if err != nil {
		return false, err
	}
	linked := linkKey{host: target.Host, tenant: tenantID}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return false, ErrPoolClosed
	}
	pooled, err := p.acquireLocked(ctx, linked, dial)
	if err != nil {
		p.mu.Unlock()
		return false, err
	}
	link := pooled.link
	p.mu.Unlock()

	negotiator, ok := link.(Negotiator)
	if !ok {
		return false, nil
	}
	reply, err := negotiator.Negotiated()
	if err != nil {
		if terminalLinkError(err) {
			p.evict(linked, pooled)
		}
		return false, err
	}
	return p.gateResponses(reply), nil
}
