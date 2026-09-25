package placement

import (
	"context"
	"slices"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/command"
	"github.com/looprig/sessionstore"
)

func TestPendingSweepListsOpenAttributedCommandsForPlacement(t *testing.T) {
	clock := &movableClock{now: reconcileNow}
	live := clock.now.Add(time.Minute)
	input := open("s-1", "c-input", sessionstore.InboxStatePending, live)
	input.Record.Descriptor.Kind = command.KindInput
	input.Record.Descriptor.Principal = &sessionwire.Principal{Tenant: testTenant, Subject: "actor-a", Kind: sessionwire.PrincipalKindActor}
	create := open("s-1", "c-create", sessionstore.InboxStateClaimed, live)
	create.Record.Descriptor.Kind = command.KindCreateSession
	create.Record.Descriptor.Metadata = sessionwire.MessageMetadata{"space": "family"}
	plain := open("s-1", "c-plain", sessionstore.InboxStatePending, live)
	plain.Record.Descriptor.Kind = command.KindInput
	placer := &recordingPlacer{}
	sweeper := newFakeSweeper(t, &fakePending{shards: 1, pages: []sessionstore.DispositionDueCommandPage{{Commands: []sessionstore.DispositionInboxEntry{input, create, plain}}}}, placer, clock, &allowSweeps{})
	if _, err := sweeper.Sweep(context.Background(), servicePrincipal(t)); err != nil {
		t.Fatal(err)
	}
	if len(placer.requests) != 1 || !slices.Equal(placer.requests[0].PrincipalCommands, []sessionwire.CommandID{"c-input", "c-create"}) || !slices.Equal(placer.requests[0].Wake, []sessionwire.CommandID{"c-input", "c-plain"}) {
		t.Fatalf("request = %+v", placer.requests)
	}
}

func TestAttributedSessionSkipsIncapablePooledHost(t *testing.T) {
	f := newAttachFixture(t, nil)
	f.publishTarget(t, "host-old", 9, sessionwire.HostIsolationClassCrossTenantIsolated)
	f.publishTarget(t, "host-new", 2, sessionwire.HostIsolationClassCrossTenantIsolated)
	f.links.principalCapableHosts = map[sessionwire.HostID]bool{"host-new": true}
	result, err := f.reconciler.Reconcile(t.Context(), Request{TenantID: testTenant, SessionID: testSession, Wake: []sessionwire.CommandID{"c-input"}, PrincipalCommands: []sessionwire.CommandID{"c-input"}})
	if err != nil || result.Decision.Outcome != OutcomeAttachPooled || result.Attached.HostID != "host-new" || !slices.Equal(result.Incapable, []sessionwire.HostID{"host-old"}) {
		t.Fatalf("result = (%+v, %v)", result, err)
	}
	if hosts := f.links.attachedHosts(); !slices.Equal(hosts, []sessionwire.HostID{"host-new"}) {
		t.Fatalf("attached %v", hosts)
	}
}

func TestNoCapableHostLeavesAttributedSessionWaiting(t *testing.T) {
	f := newAttachFixture(t, nil)
	f.publishTarget(t, "host-old", 9, sessionwire.HostIsolationClassCrossTenantIsolated)
	f.links.principalCapableHosts = map[sessionwire.HostID]bool{}
	result, err := f.reconciler.Reconcile(t.Context(), Request{TenantID: testTenant, SessionID: testSession, PrincipalCommands: []sessionwire.CommandID{"create-1"}})
	if err != nil || result.Decision.Outcome != OutcomeNoCapacity || len(f.links.attachedHosts()) != 0 {
		t.Fatalf("result = (%+v, %v)", result, err)
	}
}

func TestPlainSessionDoesNotAskPrincipalCapability(t *testing.T) {
	f := newAttachFixture(t, nil)
	f.publishTarget(t, "host-old", 9, sessionwire.HostIsolationClassCrossTenantIsolated)
	f.links.principalCapableHosts = map[sessionwire.HostID]bool{}
	if result := f.mustPlace(t, "plain"); result.Attached.HostID != "host-old" || len(f.links.principalAsks) != 0 {
		t.Fatalf("plain placement = %+v, asks=%v", result, f.links.principalAsks)
	}
}

func TestDedicatedAttributedSessionRequiresCapableHost(t *testing.T) {
	f := newFixture(t, sessionwire.HostPlacementDedicated, "factory-1")
	controller := &endpointController{recordingController: f.controller, ready: true, endpoint: testDedicatedEndpoint(f.generation(t))}
	links := newScriptedLinks()
	links.principalCapableHosts = map[sessionwire.HostID]bool{}
	f.reconciler.cfg.Workloads, f.reconciler.cfg.Links, f.reconciler.cfg.ActorID = controller, links, testActor
	result, err := f.reconciler.Reconcile(context.Background(), Request{TenantID: testTenant, SessionID: testSession, PrincipalCommands: []sessionwire.CommandID{"input-1"}})
	if err != nil || len(result.Incapable) != 1 || len(links.attaches) != 0 {
		t.Fatalf("dedicated = (%+v,%v), attaches %d", result, err, len(links.attaches))
	}
}

func TestAttributedWakeWithheldFromIncapableResident(t *testing.T) {
	f := newAttachFixture(t, nil)
	f.putOwner(t, sessionwire.HostPlacementPooled)
	f.links.principalCapableHosts = map[sessionwire.HostID]bool{}
	result, err := f.reconciler.Reconcile(t.Context(), Request{TenantID: testTenant, SessionID: testSession, Wake: []sessionwire.CommandID{"plain", "stamped"}, PrincipalCommands: []sessionwire.CommandID{"stamped"}})
	if err != nil || result.WithheldPrincipalCommands != 1 || !slices.Equal(f.links.delivered, []sessionwire.CommandID{"plain"}) {
		t.Fatalf("result = (%+v,%v), delivered %v", result, err, f.links.delivered)
	}
}
