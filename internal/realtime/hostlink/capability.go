package hostlink

import (
	"context"
	"fmt"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// GateResponseCapable is THE ONE STATEMENT of "the Host at the other end of
// this link can apply a gate_response command", read from the Host's connect
// reply. Every gate on gate_response delivery in this module asks it through
// Pool.AcceptsGateResponses, and nothing else decides: admission's refusal
// before anything is written, placement's withheld wake, and the pooled
// placement filter that puts a session with a pending gate response only on a
// Host carrying the token.
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
//
// The answer can be ONE REPLY STALE (quality gate F7, spec N3). It is read
// from the link's most recent verified reply, and a reply names no Host
// incarnation, so it is not fenced to the owner's HostGeneration: a Host
// restarted at another version, whose old connection this link has not yet
// noticed is dead, is answered from the previous reply until the reconnect
// lands. The same window exists for every reserved-method gate; the
// mixed-fleet rule (do not run Hosts that differ in this capability side by
// side for gating agents) is what bounds its consequence.
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

// acquireUnlocked returns the pooled link for one (Host, tenant), dialling it
// with the pool's lock RELEASED. It trades acquireLocked's coalescing -- two
// callers racing for the same missing link may both dial -- for never stalling
// the pool behind a dial: the loser's link is closed and the winner's used, and
// the ceiling and closed checks are repeated under the lock after the dial.
func (p *Pool) acquireUnlocked(ctx context.Context, linked linkKey, dial Target) (*pooledLink, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrPoolClosed
	}
	// A link dialled on an advertisement this one supersedes is replaced, as
	// acquireLocked replaces one (I3.1 D1); otherwise the cached link answers.
	current, ok := p.links[linked]
	if ok && !current.supersededBy(dial) {
		current.observe(dial)
		p.mu.Unlock()
		return current, nil
	}
	if !ok && len(p.links) >= p.limits.MaxLinks {
		n := len(p.links)
		p.mu.Unlock()
		return nil, fmt.Errorf("%w: %d links open, cannot dial %q for tenant %q", ErrLinkLimit, n, linked.host, linked.tenant)
	}
	p.mu.Unlock()

	link, err := p.dialer.Dial(ctx, dial.dialable(), p.observer)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	var refused error
	var pooled *pooledLink
	var replaced Link
	kept := false // whether OUR dial became the pool's link (links may not be comparable)
	switch existing, ok := p.links[linked]; {
	case p.closed:
		refused = ErrPoolClosed
	case ok && !existing.supersededBy(dial):
		pooled = existing // a racer dialled first (or it is current); ours is surplus
	case !ok && len(p.links) >= p.limits.MaxLinks:
		refused = fmt.Errorf("%w: %d links open, cannot keep %q for tenant %q", ErrLinkLimit, len(p.links), linked.host, linked.tenant)
	default:
		if ok && p.dropLinkLocked(linked, existing) {
			replaced = existing.link
		}
		pooled = p.newPooledLink(link, dial)
		p.links[linked] = pooled
		kept = true
	}
	p.mu.Unlock()
	if replaced != nil {
		_ = replaced.Close(context.Background())
	}
	if !kept {
		_ = link.Close(context.Background())
	}
	if refused != nil {
		return nil, refused
	}
	return pooled, nil
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
// The link is acquired WITHOUT holding the pool's lock across a dial (spec
// gate N2): this is the one path a public HTTP request (a gate response) can
// reach that may open a link, and holding Pool.mu across a dial to an owner
// whose base black-holes would stall every bind, unbind and delivery on every
// Host for up to DialTimeout. See acquireUnlocked. The dial is bounded by the
// caller's context as well as the dialer's own timeout. A link that cannot report its reply
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

	pooled, err := p.acquireUnlocked(ctx, linked, dial)
	if err != nil {
		return false, err
	}
	link := pooled.link

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
