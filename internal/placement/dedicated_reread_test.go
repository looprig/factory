package placement

import (
	"context"
	"testing"
	"time"

	"github.com/looprig/sessionstore"
)

// racerOnFirstRead lets another replica finish a desired-state change BETWEEN
// Reconcile's pre-claim catalog read and its claim: the first read returns the
// record as it was, then the racer writes a new desired generation and
// releases its claim before this replica claims.
type racerOnFirstRead struct {
	*sessionstore.Store
	racer func()
	done  bool
}

func (c *racerOnFirstRead) GetCatalogEntry(ctx context.Context, req sessionstore.GetCatalogEntryRequest) (sessionstore.CatalogEntry, error) {
	entry, err := c.Store.GetCatalogEntry(ctx, req)
	if !c.done {
		c.done = true
		c.racer()
	}
	return entry, err
}

// TestTheDedicatedArmDecidesFromTheRecordReadUnderTheClaim is the B5 spec
// gate's F2 probe, committed. A sweeper-driven Reconcile (Desired == nil,
// exactly what PendingSweeper sends) on the DEDICATED arm must hand
// EnsureWorkload the generation the record holds once this replica holds the
// claim -- not the one it read before claiming. Before the fix it asked the
// controller for generation 2 after a racer had already written generation 3,
// which is an obsolete workload a controller may create over the new one.
func TestTheDedicatedArmDecidesFromTheRecordReadUnderTheClaim(t *testing.T) {
	t.Parallel()

	f := newFixture(t, "dedicated", "factory-1")
	f.reconcile(t, dedicatedDesired(`{"cpu":"1"}`))
	before := f.generation(t)
	racer := f.racingReconciler(t, f.store)
	racer.cfg.HolderID = "factory-9"
	catalog := &racerOnFirstRead{Store: f.store, racer: func() {
		if _, err := racer.Reconcile(context.Background(), Request{TenantID: testTenant, SessionID: testSession, Desired: dedicatedDesired(`{"cpu":"8"}`)}); err != nil {
			t.Errorf("racer: %v", err)
		}
	}}
	sweeperPath, err := NewReconciler(Config{
		Directory: mustDirectory(t, f.store), Catalog: catalog, Claims: f.store, Workloads: f.controller,
		Clock: f.clock, HolderID: "factory-1", ClaimTTL: time.Minute, CandidateLimit: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := sweeperPath.Reconcile(context.Background(), Request{TenantID: testTenant, SessionID: testSession})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	current := f.generation(t)
	if current == before {
		t.Fatalf("the premise is a racer that moved the generation; it stayed %d", current)
	}
	last := f.controller.intents[len(f.controller.intents)-1]
	if result.Intent.Generation != current || last.Generation != current || string(last.Workload.Payload) != `{"cpu":"8"}` {
		t.Fatalf("the controller was last asked for generation %d (%s), result intent %d; the store holds %d",
			last.Generation, last.Workload.Payload, result.Intent.Generation, current)
	}
}
