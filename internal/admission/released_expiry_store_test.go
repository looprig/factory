package admission

import (
	"context"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/command"
	"github.com/looprig/sessionstore"
)

// TestInputLeftOnAReleasedDedicatedSessionIsRejectedAtItsDeadline is the
// other half of placement's rule that only a pending restore revives a
// released dedicated session (factory v0.8.0, review B-F1). Input admitted to a
// session whose desire is released is NOT placed, so it must not sit pending
// for ever: the disposition deadline sweep never reads the catalog's desire,
// and rejects it at its apply deadline like any other unapplied command. What
// the product sees is the command's own status, rejected /
// runtime_unavailable.
func TestInputLeftOnAReleasedDedicatedSessionIsRejectedAtItsDeadline(t *testing.T) {
	lane := newCommandLane(t)
	ctx := context.Background()
	tenant := lane.principal.Tenant()
	const session = sessionwire.SessionID("session-released")
	lane.create(t, session)

	// Deletion desire: a dedicated placement naming no workload.
	entry, err := lane.store.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{TenantID: tenant, SessionID: session})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lane.store.UpdateCatalogDesiredState(ctx, sessionstore.UpdateCatalogDesiredStateRequest{
		TenantID: tenant, SessionID: session, ExpectedRevision: entry.Revision, IdempotencyKey: "release-1",
		DesiredPlacement: sessionwire.HostPlacementDedicated, RuntimeCompatibilityID: entry.Record.RuntimeCompatibilityID,
	}); err != nil {
		t.Fatalf("release: %v", err)
	}

	// The input is ADMITTED: admission does not read desire either.
	input, _, err := lane.input(session, "input-after-release", "hello")
	if err != nil || input.Record.State != sessionstore.InboxStatePending {
		t.Fatalf("input after release = (%q, %v), want an accepted pending command", input.Record.State, err)
	}

	lane.clock.set(serviceNow.Add(2 * time.Minute))
	sweeper, err := NewDispositionReconciler(DispositionReconcilerConfig{
		Authorizer: &serviceAuthorizer{}, Due: lane.store, Settlement: lane.store, Claims: lane.store,
		Clock: lane.clock, HolderID: "replica-a", ClaimTTL: 30 * time.Second, PageLimit: 50, MaxPages: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	for range lane.store.ControlShards() {
		if _, err := sweeper.Sweep(ctx, sweepPrincipal(t)); err != nil {
			t.Fatalf("Sweep: %v", err)
		}
	}
	got, err := lane.store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{
		TenantID: tenant, SessionID: session, CommandID: input.Record.Descriptor.CommandID,
	})
	if err != nil {
		t.Fatal(err)
	}
	status, readable := command.StatusForDisposition(got)
	if got.Record.State != sessionstore.InboxStateRejected || !readable ||
		status.State != sessionwire.CommandStateRejected || status.Error == nil || status.Error.Code != sessionwire.ErrorCodeRuntimeUnavailable {
		t.Fatalf("input after the deadline sweep = %q (%+v, %v), want rejected / runtime_unavailable", got.Record.State, status, readable)
	}
}
