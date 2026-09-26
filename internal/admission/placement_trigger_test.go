package admission

import (
	"context"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/placement"
	"github.com/looprig/sessionstore"
)

func TestSuccessfulColdAdmissionTriggersPlacement(t *testing.T) {
	f := newServiceFixture(t)
	f.catalog.entry = sessionstore.CatalogEntry{Record: sessionstore.CatalogRecord{
		TenantID: "tenant-a", SessionID: "session-a", AgentID: "agent-a",
		RuntimeCompatibilityID: "runtime-v1", DesiredPlacement: sessionwire.HostPlacementPooled,
		Binding: existingBinding,
	}}
	f.catalog.getErr = nil
	var triggered []placement.AdmissionNotice
	f.rebuild(t, func(cfg *Config) {
		cfg.OnAdmitted = func(entry placement.AdmissionNotice) { triggered = append(triggered, entry) }
	})
	entry, _, err := f.service.AdmitInterrupt(context.Background(), f.principal, sessionwire.InterruptRequest{
		CommandEnvelope: envelope("interrupt-a"), SessionID: "session-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(triggered) != 1 || triggered[0].CommandID != entry.Record.Descriptor.CommandID {
		t.Fatalf("placement triggers = %+v, want admitted command", triggered)
	}
}
