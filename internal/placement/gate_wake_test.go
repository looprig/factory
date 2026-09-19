package placement

import (
	"context"
	"errors"
	"time"

	"github.com/looprig/factory/internal/command"
	"github.com/looprig/sessionstore"
	"slices"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// TestAGateResponseWakeIsWithheldFromAHostThatCannotApplyOne: placement never
// delivers a gate response to a Host the one capability predicate does not
// admit -- nor to one it could not ask -- while every other wake is delivered;
// the capable control delivers both. The question is asked once per wake, of
// the Host the route was just bound to.
func TestAGateResponseWakeIsWithheldFromAHostThatCannotApplyOne(t *testing.T) {
	t.Parallel()
	for name, row := range map[string]struct {
		capable bool
		err     error
		want    []sessionwire.CommandID
	}{
		"cannot":        {false, nil, []sessionwire.CommandID{"cmd-input"}},
		"could not ask": {true, errors.New("reconnecting"), []sessionwire.CommandID{"cmd-input"}},
		"can (control)": {true, nil, []sessionwire.CommandID{"cmd-input", "cmd-gate", "cmd-gate-2"}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newAttachFixture(t, nil)
			f.publishTarget(t, "host-a", 4, sessionwire.HostIsolationClassCrossTenantIsolated)
			f.links.gateCapable, f.links.gateErr = row.capable, row.err
			result, err := f.reconciler.Reconcile(t.Context(), Request{
				TenantID: testTenant, SessionID: testSession,
				Wake:          []sessionwire.CommandID{"cmd-input", "cmd-gate", "cmd-gate-2"},
				GateResponses: []sessionwire.CommandID{"cmd-gate", "cmd-gate-2"},
			})
			if err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			f.links.mu.Lock()
			delivered, asks := slices.Clone(f.links.delivered), slices.Clone(f.links.gateAsks)
			f.links.mu.Unlock()
			if !slices.Equal(delivered, row.want) {
				t.Fatalf("delivered %v, want %v", delivered, row.want)
			}
			if withheld := 3 - len(row.want); result.WithheldGateResponses != withheld {
				t.Fatalf("WithheldGateResponses = %d, want %d", result.WithheldGateResponses, withheld)
			}
			if len(asks) != 1 || asks[0] != "host-a" {
				t.Fatalf("the capability was asked of %v, want host-a once", asks)
			}
		})
	}
}

// TestAWakeWithNoGateResponseAsksNoCapability.
func TestAWakeWithNoGateResponseAsksNoCapability(t *testing.T) {
	t.Parallel()
	f := newAttachFixture(t, nil)
	f.publishTarget(t, "host-a", 4, sessionwire.HostIsolationClassCrossTenantIsolated)
	f.mustPlace(t, "cmd-input")
	if len(f.links.gateAsks) != 0 || !slices.Equal(f.links.delivered, []sessionwire.CommandID{"cmd-input"}) {
		t.Fatalf("asks=%v delivered=%v", f.links.gateAsks, f.links.delivered)
	}
}

// TestTheSweepNamesItsGateResponseWakes: the sweep tells placement which live
// pending wakes are gate responses, by the durable command kind, so the wake
// gate above has something to read.
func TestTheSweepNamesItsGateResponseWakes(t *testing.T) {
	t.Parallel()
	clock := &movableClock{now: reconcileNow}
	live := clock.now.Add(time.Minute)
	gate := open("s-1", "c-gate", sessionstore.InboxStatePending, live)
	gate.Record.Descriptor.Kind = command.KindGateResponse
	input := open("s-1", "c-input", sessionstore.InboxStatePending, live)
	input.Record.Descriptor.Kind = command.KindInput
	placer := &recordingPlacer{}
	sweeper := newFakeSweeper(t, &fakePending{shards: 1, pages: []sessionstore.DispositionDueCommandPage{{
		Commands: []sessionstore.DispositionInboxEntry{input, gate},
	}}}, placer, clock, &allowSweeps{})
	if _, err := sweeper.Sweep(context.Background(), servicePrincipal(t)); err != nil {
		t.Fatal(err)
	}
	if len(placer.requests) != 1 || !slices.Equal(placer.requests[0].Wake, []sessionwire.CommandID{"c-input", "c-gate"}) ||
		!slices.Equal(placer.requests[0].GateResponses, []sessionwire.CommandID{"c-gate"}) {
		t.Fatalf("requests = %+v, want wake [c-input c-gate] naming c-gate as the gate response", placer.requests)
	}
}
