package placement

import (
	"slices"
	"strings"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/command"
	"github.com/looprig/sessionstore"
)

func TestPendingSweepNamesReferencedCreateAndInputBodies(t *testing.T) {
	clock := &movableClock{now: reconcileNow}
	live := clock.now.Add(time.Minute)
	create := open("s-1", "c-create", sessionstore.InboxStatePending, live)
	create.Record.Descriptor.Kind = command.KindCreateSession
	create.Record.Descriptor.PayloadObject = &sessionwire.ObjectMetadata{}
	input := open("s-1", "c-input", sessionstore.InboxStatePending, live)
	input.Record.Descriptor.Kind = command.KindInput
	input.Record.Descriptor.PayloadObject = &sessionwire.ObjectMetadata{}
	inline := open("s-1", "c-inline", sessionstore.InboxStatePending, live)
	inline.Record.Descriptor.Kind = command.KindInput
	placer := &recordingPlacer{}
	sweeper := newFakeSweeper(t, &fakePending{shards: 1, pages: []sessionstore.DispositionDueCommandPage{{
		Commands: []sessionstore.DispositionInboxEntry{create, input, inline},
	}}}, placer, clock, &allowSweeps{})
	if _, err := sweeper.Sweep(t.Context(), servicePrincipal(t)); err != nil {
		t.Fatal(err)
	}
	if len(placer.requests) != 1 || !slices.Equal(placer.requests[0].PayloadReferences, []sessionwire.CommandID{"c-create", "c-input"}) {
		t.Fatalf("requests = %+v", placer.requests)
	}
}

func TestPendingReferencedBodySelectsCapableHost(t *testing.T) {
	f := newAttachFixture(t, nil)
	f.publishTarget(t, "host-old", 9, sessionwire.HostIsolationClassCrossTenantIsolated)
	f.publishTarget(t, "host-new", 2, sessionwire.HostIsolationClassCrossTenantIsolated)
	f.links.payloadCapableHosts = map[sessionwire.HostID]bool{"host-new": true}
	result, err := f.reconciler.Reconcile(t.Context(), Request{
		TenantID: testTenant, SessionID: testSession,
		Wake: []sessionwire.CommandID{"cmd-input"}, PayloadReferences: []sessionwire.CommandID{"cmd-input"},
	})
	if err != nil || result.Attached.HostID != "host-new" || !slices.Equal(result.Incapable, []sessionwire.HostID{"host-old"}) {
		t.Fatalf("Reconcile = (%+v, %v), want capable host-new", result, err)
	}
	if !slices.Equal(f.links.attachedHosts(), []sessionwire.HostID{"host-new"}) {
		t.Fatalf("attaches = %v", f.links.attachedHosts())
	}
	if !strings.Contains(f.logs.String(), "cannot dereference this session's pending body") {
		t.Fatalf("missing referenced body placement warning: %s", f.logs.String())
	}

	none := newAttachFixture(t, nil)
	none.publishTarget(t, "host-old", 9, sessionwire.HostIsolationClassCrossTenantIsolated)
	result, err = none.reconciler.Reconcile(t.Context(), Request{
		TenantID: testTenant, SessionID: testSession,
		Wake: []sessionwire.CommandID{"cmd-input"}, PayloadReferences: []sessionwire.CommandID{"cmd-input"},
	})
	if err != nil || result.Decision.Outcome != OutcomeNoCapacity || len(none.links.attachedHosts()) != 0 {
		t.Fatalf("incapable pool = (%+v, %v), attaches=%v", result, err, none.links.attachedHosts())
	}
}

func TestReferencedBodyWakeWaitsForCapableOwner(t *testing.T) {
	f := newAttachFixture(t, nil)
	f.putOwner(t, sessionwire.HostPlacementPooled)
	req := Request{TenantID: testTenant, SessionID: testSession,
		Wake: []sessionwire.CommandID{"cmd-input"}, PayloadReferences: []sessionwire.CommandID{"cmd-input"}}
	if _, err := f.reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	if len(f.links.delivered) != 0 {
		t.Fatalf("incapable owner received %v", f.links.delivered)
	}
	f.links.payloadCapableHosts = map[sessionwire.HostID]bool{"host-owner": true}
	if _, err := f.reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(f.links.delivered, []sessionwire.CommandID{"cmd-input"}) {
		t.Fatalf("capable owner received %v", f.links.delivered)
	}
}
