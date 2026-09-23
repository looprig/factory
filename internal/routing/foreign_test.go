package routing

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/delivery"
)

// TestARecordNamingAnotherSessionOrTenantIsRefused is F1 of the tests-lane wire
// freeze. A Host publication arrives on ONE session's tail, and the record
// names its own tenant and session. Before v0.8.1 nothing compared the two, so
// a faulty or compromised Host could put another session's -- or another
// tenant's -- records in front of this session's viewers. The record is
// refused as malformed, which is the class the livetail drainer already
// repairs (TestARecordTheRelayRefusesIsRepairedNotSkipped), and counted.
func TestARecordNamingAnotherSessionOrTenantIsRefused(t *testing.T) {
	const otherTenant = sessionwire.TenantID("tenant-other")
	build := func(t *testing.T, ephemeral bool, tenant sessionwire.TenantID, session sessionwire.SessionID) []byte {
		t.Helper()
		var (
			encoded []byte
			err     error
		)
		if ephemeral {
			encoded, err = sessionwire.EphemeralPublication{TenantID: tenant, SessionID: session, Body: json.RawMessage(`{}`)}.MarshalJSON()
		} else {
			encoded, err = sessionwire.EnduringPublication{
				TenantID: tenant, SessionID: session, EventID: "event-foreign",
				JournalSeq: 1, CoveredThrough: 1, Body: json.RawMessage(`{}`),
			}.MarshalJSON()
		}
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		return encoded
	}
	for _, tc := range []struct {
		name      string
		ephemeral bool
		tenant    sessionwire.TenantID
		session   sessionwire.SessionID
	}{
		{"an enduring record naming another session", false, bindTenant, otherSession},
		{"an enduring record naming another tenant", false, otherTenant, bindSession},
		{"an ephemeral record naming another session", true, bindTenant, otherSession},
		{"an ephemeral record naming another tenant", true, otherTenant, bindSession},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRelayFixture(t, testRepairLimits)
			f.open(bindSession, linkA)
			f.open(otherSession, linkA)
			f.pub.block(linkA, true)

			err := f.relay.Receive(context.Background(), bindTenant, bindSession, Frame{
				Encoded: build(t, tc.ephemeral, tc.tenant, tc.session), CommittedAppendSeq: 1_000_000,
			})
			if !errors.Is(err, ErrForeignRecord) || !errors.Is(err, delivery.ErrMalformed) {
				t.Fatalf("Receive = %v, want ErrForeignRecord wrapping delivery.ErrMalformed", err)
			}
			if got := f.relay.ForeignRecords(); got != 1 {
				t.Fatalf("ForeignRecords = %d, want 1", got)
			}
			for _, session := range []sessionwire.SessionID{bindSession, otherSession} {
				if err := f.relay.Pump(context.Background(), bindTenant, session); err != nil {
					t.Fatalf("Pump(%s): %v", session, err)
				}
				if got := f.relay.Queued(bindTenant, session, linkA); got != 0 {
					t.Fatalf("queued on %s = %d, want 0: a foreign record must reach no viewer", session, got)
				}
			}
		})
	}

	t.Run("the control: the session's own record is taken and not counted", func(t *testing.T) {
		f := newRelayFixture(t, testRepairLimits)
		f.open(bindSession, linkA)
		f.pub.block(linkA, true)
		for _, ephemeral := range []bool{false, true} {
			if err := f.relay.Receive(context.Background(), bindTenant, bindSession, Frame{
				Encoded: build(t, ephemeral, bindTenant, bindSession), CommittedAppendSeq: 1_000_000,
			}); err != nil {
				t.Fatalf("Receive(own, ephemeral=%v) = %v", ephemeral, err)
			}
		}
		if err := f.relay.Pump(context.Background(), bindTenant, bindSession); err != nil {
			t.Fatalf("Pump: %v", err)
		}
		if got := f.relay.Queued(bindTenant, bindSession, linkA); got != 2 {
			t.Fatalf("queued = %d, want 2", got)
		}
		if got := f.relay.ForeignRecords(); got != 0 {
			t.Fatalf("ForeignRecords = %d, want 0", got)
		}
	})
}
