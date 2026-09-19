package placement

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/looprig/factory/internal/command"
	"github.com/looprig/sessionstore"
	"slices"
	"strings"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// TestAGateResponseWakeIsWithheldFromAHostThatCannotApplyOne: placement never
// delivers a gate response to a Host the one capability predicate does not
// admit -- nor to one it could not ask -- while every other wake is delivered;
// the capable control delivers both. The question is asked once per wake, of
// the Host the route was just bound to. It is driven on the OWNED path (a
// live owner is woken, no placement filter runs): an unowned session with a
// pending gate response is placed only on a capable Host, below.
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
			f.putOwner(t, sessionwire.HostPlacementPooled)
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
			if len(asks) != 1 || asks[0] != "host-owner" {
				t.Fatalf("the capability was asked of %v, want the owner once", asks)
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

// TestASessionWithAPendingGateResponseIsPlacedOnlyOnACapableHost is the
// capable-only placement filter: the top-ranked candidate cannot apply a gate
// response and is skipped with a WARN; the capable one is attached and is the
// only one asked to attach. With no capable candidate the session waits.
func TestASessionWithAPendingGateResponseIsPlacedOnlyOnACapableHost(t *testing.T) {
	t.Parallel()

	gateWake := func(f *attachFixture) (Result, error) {
		return f.reconciler.Reconcile(t.Context(), Request{
			TenantID: testTenant, SessionID: testSession,
			Wake: []sessionwire.CommandID{"cmd-gate"}, GateResponses: []sessionwire.CommandID{"cmd-gate"},
		})
	}

	f := newAttachFixture(t, nil)
	f.publishTarget(t, "host-old", 9, sessionwire.HostIsolationClassCrossTenantIsolated)
	f.publishTarget(t, "host-new", 2, sessionwire.HostIsolationClassCrossTenantIsolated)
	f.links.gateCapableHosts = map[sessionwire.HostID]bool{"host-new": true}
	result, err := gateWake(f)
	if err != nil || result.Decision.Outcome != OutcomeAttachPooled || result.Attached.HostID != "host-new" {
		t.Fatalf("Reconcile = (%+v, %v), want attached to host-new", result, err)
	}
	if !slices.Equal(result.Incapable, []sessionwire.HostID{"host-old"}) {
		t.Fatalf("Incapable = %v, want [host-old]", result.Incapable)
	}
	if hosts := f.links.attachedHosts(); !slices.Equal(hosts, []sessionwire.HostID{"host-new"}) {
		t.Fatalf("attaches = %v, want host-new only", hosts)
	}
	if !strings.Contains(f.logs.String(), `"msg":"placement: skipped pooled candidates that cannot apply this session's pending gate response"`) ||
		!strings.Contains(f.logs.String(), `"skipped_hosts":["host-old"],"waiting":false`) {
		t.Fatalf("no aggregated WARN naming host-old: %s", f.logs.String())
	}
	if !slices.Equal(f.links.delivered, []sessionwire.CommandID{"cmd-gate"}) {
		t.Fatalf("delivered %v, want the gate response to the capable Host", f.links.delivered)
	}

	none := newAttachFixture(t, nil)
	none.publishTarget(t, "host-old", 9, sessionwire.HostIsolationClassCrossTenantIsolated)
	none.links.gateCapableHosts = map[sessionwire.HostID]bool{}
	result, err = gateWake(none)
	if err != nil || result.Decision.Outcome != OutcomeNoCapacity || len(none.links.attachedHosts()) != 0 {
		t.Fatalf("with no capable Host = (%+v, %v) attaches=%v, want no capacity and no attach", result, err, none.links.attachedHosts())
	}

	// A Host that could not be asked is not capable, whatever else came back.
	unasked := newAttachFixture(t, nil)
	unasked.publishTarget(t, "host-new", 2, sessionwire.HostIsolationClassCrossTenantIsolated)
	unasked.links.gateCapableHosts = map[sessionwire.HostID]bool{"host-new": true}
	unasked.links.gateErr = errors.New("reconnecting")
	result, err = gateWake(unasked)
	if err != nil || result.Decision.Outcome != OutcomeNoCapacity || len(unasked.links.attachedHosts()) != 0 {
		t.Fatalf("with a Host that could not be asked = (%+v, %v) attaches=%v, want the session to wait", result, err, unasked.links.attachedHosts())
	}
}

// TestASessionWithNoPendingGateResponseIsPlacedWithoutAsking: the filter costs
// nothing for every other session.
func TestASessionWithNoPendingGateResponseIsPlacedWithoutAsking(t *testing.T) {
	t.Parallel()
	f := newAttachFixture(t, nil)
	f.publishTarget(t, "host-old", 9, sessionwire.HostIsolationClassCrossTenantIsolated)
	result := f.mustPlace(t, "cmd-input")
	if result.Attached.HostID != "host-old" || len(f.links.gateAsks) != 0 {
		t.Fatalf("attached %q with %d capability asks, want host-old and none", result.Attached.HostID, len(f.links.gateAsks))
	}
}

// TestAnUnaddressableCandidateInTheGateFilterIsReportedAsUnaddressable (spec
// gate N4): when the capability question cannot even be addressed -- the
// candidate's base cannot carry the tenant -- the candidate is recorded and
// logged as UNADDRESSABLE with Core's code, as the attach path does, and not
// folded into "incapable".
func TestAnUnaddressableCandidateInTheGateFilterIsReportedAsUnaddressable(t *testing.T) {
	t.Parallel()
	f := newAttachFixture(t, nil)
	f.publishTarget(t, "host-longname", 9, sessionwire.HostIsolationClassCrossTenantIsolated)
	f.links.gateErr = fmt.Errorf("%w: %w", ErrTenantUnaddressable, &sessionwire.HostLinkEndpointError{Code: sessionwire.HostLinkEndpointCodeTooLong})
	result, err := f.reconciler.Reconcile(t.Context(), Request{TenantID: testTenant, SessionID: testSession,
		Wake: []sessionwire.CommandID{"cmd-gate"}, GateResponses: []sessionwire.CommandID{"cmd-gate"}})
	if err != nil || !slices.Equal(result.Unaddressable, []sessionwire.HostID{"host-longname"}) || len(result.Incapable) != 0 {
		t.Fatalf("= (%+v, %v), want host-longname unaddressable and not incapable", result, err)
	}
	logs := f.logs.String()
	if !strings.Contains(logs, `"msg":"placement: skipped a pooled candidate whose advertised base cannot carry this tenant's HostLink address"`) ||
		!strings.Contains(logs, `"code":"too_long"`) || strings.Contains(logs, "cannot apply this session's pending gate response") {
		t.Fatalf("logs = %s, want the unaddressable WARN with Core's code and no incapable WARN", logs)
	}
}

// TestAWaitingSessionIsReportedOncePerIntervalNotPerCandidatePerSweep (quality
// gate F6): a session with a pending gate response and no capable Host is
// re-examined every sweep. It is reported in ONE line naming every skipped
// candidate, and not again for the same session until the interval has passed.
func TestAWaitingSessionIsReportedOncePerIntervalNotPerCandidatePerSweep(t *testing.T) {
	t.Parallel()
	f := newAttachFixture(t, nil)
	publish := func() {
		f.publishTarget(t, "host-old-1", 9, sessionwire.HostIsolationClassCrossTenantIsolated)
		f.publishTarget(t, "host-old-2", 5, sessionwire.HostIsolationClassCrossTenantIsolated)
	}
	publish()
	f.links.gateCapableHosts = map[sessionwire.HostID]bool{}
	pass := func() Result {
		result, err := f.reconciler.Reconcile(t.Context(), Request{TenantID: testTenant, SessionID: testSession,
			Wake: []sessionwire.CommandID{"cmd-gate"}, GateResponses: []sessionwire.CommandID{"cmd-gate"}})
		if err != nil || result.Decision.Outcome != OutcomeNoCapacity || len(result.Incapable) != 2 {
			t.Fatalf("pass = (%+v, %v), want no capacity with both candidates incapable", result, err)
		}
		return result
	}
	count := func() int {
		return strings.Count(f.logs.String(), "cannot apply this session's pending gate response")
	}
	for range 3 {
		pass()
	}
	if got := count(); got != 1 {
		t.Fatalf("three sweeps wrote %d waiting WARNs, want 1: %s", got, f.logs.String())
	}
	if !strings.Contains(f.logs.String(), `"skipped_hosts":["host-old-1","host-old-2"],"waiting":true`) {
		t.Fatalf("the WARN does not list both skipped Hosts: %s", f.logs.String())
	}
	f.clock.now = f.clock.now.Add(incapableReportInterval)
	publish()
	pass()
	if got := count(); got != 2 {
		t.Fatalf("after the interval: %d waiting WARNs, want 2", got)
	}
}
