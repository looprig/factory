package factory

import (
	"context"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	"github.com/looprig/factory/internal/admission"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage/memstore"
)

type defaultCreateDirectory struct{}

func (defaultCreateDirectory) Owner(context.Context, sessionwire.TenantID, sessionwire.SessionID) (sessionwire.HostLinkRegistryObservation, bool, error) {
	return sessionwire.HostLinkRegistryObservation{}, false, nil
}

type defaultCreateIDs struct{}

func (defaultCreateIDs) NewUUID() (string, error) { return "runtime-command-default", nil }

// TestAnOmittedLaunchPlacementResolvesAsPooled exercises the create
// composition boundary rather than only LaunchTemplate.Validate. The target
// resolver is what admission consumes, so a default that exists only in the
// validator would still persist an invalid zero placement.
func TestAnOmittedLaunchPlacementResolvesAsPooled(t *testing.T) {
	template := LaunchTemplate{Key: sessionstore.HostTargetKey{
		AgentID:                "agent-a",
		RuntimeCompatibilityID: "runtime-v1",
	}}
	if err := template.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	target, found, err := departmentTargets(cloneTemplates([]LaunchTemplate{template})).ResolveAgent(context.Background(), "agent-a")
	if err != nil {
		t.Fatalf("ResolveAgent: %v", err)
	}
	if !found {
		t.Fatal("ResolveAgent found = false, want true")
	}
	want := sessionwire.HostPlacementPooled
	if target.Key.Placement != want {
		t.Fatalf("resolved placement = %q, want default %q", target.Key.Placement, want)
	}
}

// TestAnOmittedLaunchPlacementIsPersistedAsPooled proves the
// default at the durable boundary too. ResolveAgent is only a composition
// lookup; the real Service and SessionStore must agree that the omitted input
// creates a pooled disposition session, rather than merely returning a pooled
// value to this test.
func TestAnOmittedLaunchPlacementIsPersistedAsPooled(t *testing.T) {
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	backend := memstore.New()
	store, err := sessionstore.Open(context.Background(), backend, sessionstore.WithClock(fixedCompositionClock{now: now}))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(context.Background()) })

	template := LaunchTemplate{Key: sessionstore.HostTargetKey{
		AgentID:                "agent-a",
		RuntimeCompatibilityID: "runtime-v1",
	}}
	targets := departmentTargets(cloneTemplates([]LaunchTemplate{template}))
	principal, err := identity.NewPrincipal("tenant-a", "actor-a", identity.KindActor)
	if err != nil {
		t.Fatalf("NewPrincipal: %v", err)
	}
	service, err := admission.NewService(admission.Config{
		Authorizer: FakeSeams{}, Targets: targets, Catalog: store, Commands: store,
		Directory: defaultCreateDirectory{}, Clock: fixedCompositionClock{now: now}, IDs: defaultCreateIDs{},
		PublicCreates: store,
		Binding:       admission.SessionBindingTemplate{StorageBindingID: "storage-a", BindingVersion: "v1"},
		ApplyDeadline: time.Minute,
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	req := sessionwire.CreateRequest{
		CommandEnvelope: sessionwire.CommandEnvelope{Version: sessionwire.CurrentWireVersion, CommandID: "create-default"},
		SessionID:       "session-default", AgentID: "agent-a", Blocks: []byte(`[{"text":"hello"}]`),
	}
	if _, created, err := service.AdmitCreate(context.Background(), principal, req); err != nil || !created {
		t.Fatalf("AdmitCreate = created %t, err %v", created, err)
	}
	entry, err := store.GetCatalogEntry(context.Background(), sessionstore.GetCatalogEntryRequest{
		TenantID: principal.Tenant(), SessionID: req.SessionID,
	})
	if err != nil {
		t.Fatalf("GetCatalogEntry: %v", err)
	}
	if entry.Record.Binding.ProtocolMode != sessionstore.ProtocolModeDisposition {
		t.Fatalf("catalog protocol = %q, want disposition", entry.Record.Binding.ProtocolMode)
	}
	if entry.Record.DesiredPlacement != sessionwire.HostPlacementPooled {
		t.Fatalf("catalog placement = %q, want pooled default", entry.Record.DesiredPlacement)
	}
	if entry.Record.DesiredWorkload.PayloadVersion != "" || len(entry.Record.DesiredWorkload.Payload) != 0 {
		t.Fatalf("catalog workload = %+v, want pooled's empty workload", entry.Record.DesiredWorkload)
	}
}

type fixedCompositionClock struct{ now time.Time }

func (c fixedCompositionClock) Now() time.Time { return c.now }

func (fixedCompositionClock) AfterFunc(time.Duration, func()) func() bool {
	return func() bool { return true }
}
