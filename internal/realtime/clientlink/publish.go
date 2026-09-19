package clientlink

import (
	"errors"
	"fmt"
	"slices"

	centrifuge "github.com/centrifugal/centrifuge"
	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// ErrNotPublished reports a session-channel record the node would not publish.
var ErrNotPublished = errors.New("clientlink: the record was not published")

// SessionChannel is the ClientLink channel a session's viewers subscribe to:
// `session:{tenant}:{session}`, the grammar demandKeyOf parses and wui's
// protocol package subscribes to (clientlink.ts sessionChannel).
//
// It is demandKeyOf's INVERSE on every channel demandKeyOf accepts, and that
// is the only property that matters: a publication must land on exactly the
// channel a viewer's subscription was authorized and routed by.
// TestSessionChannelIsTheInverseOfTheDemandGrammar holds the round trip.
func SessionChannel(tenant sessionwire.TenantID, session sessionwire.SessionID) string {
	return channelPrefix + string(tenant) + ":" + string(session)
}

// PublishSession publishes one encoded session-channel record to EVERY viewer of
// a session on this replica, once.
//
// Channel-wide, once per record, is the Gap 3 ruling's decision, and what bounds
// it is the transport: each connection has its own queue, bounded in BYTES by
// PerConnectionQueueBytes, and a viewer that cannot keep up is disconnected
// with DisconnectSlow (3008) rather than slowing its peers. That viewer
// reconnects and repairs from the durable journal. The node runs with no
// history, so a publication nobody is subscribed to is simply gone -- which is
// correct, because a viewer's join reads the durable journal after it
// subscribes.
func (h *Handler) PublishSession(tenant sessionwire.TenantID, session sessionwire.SessionID, encoded []byte) error {
	if _, err := h.node.Publish(SessionChannel(tenant, session), encoded); err != nil {
		return fmt.Errorf("%w: %w", ErrNotPublished, err)
	}
	return nil
}

// CloseSession removes every viewer's subscription to a session on this
// replica, server-side, so each one's join repairs from the durable journal.
//
// It is the channel-wide form of routing.Publisher.CloseLink, and is what the
// relay's fail-closed arms reach once delivery is per channel rather than per
// link: a viewer that cannot be told where to read from must not be left
// streaming past a gap. The code is centrifuge's UnsubscribeCodeServer (2000),
// below the 2500 band in which a client resubscribes on its own, so the
// viewer's subscription reports unsubscribed and its join -- not the transport
// -- decides what to do.
//
// It runs on its OWN GOROUTINE and returns at once. A server-side unsubscribe
// runs this handler's OnUnsubscribe callback, which releases a DeliveryBinding
// under the Engine's lock, and the relay calling this may be holding a lock an
// Engine caller is waiting on; doing it inline would put the two in a cycle.
func (h *Handler) CloseSession(tenant sessionwire.TenantID, session sessionwire.SessionID) {
	channel := SessionChannel(tenant, session)
	clients := h.node.Hub().Connections()
	go func() {
		for _, client := range clients {
			if slices.Contains(client.Channels(), channel) {
				client.Unsubscribe(channel, centrifuge.Unsubscribe{
					Code: centrifuge.UnsubscribeCodeServer, Reason: "session delivery repaired",
				})
			}
		}
	}()
}
