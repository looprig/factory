package admission

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/looprig/sessionstore"
	"github.com/looprig/storage/memstore"
)

func TestServicePersistsOnlyLegacyCreateThroughTheReleasedStore(t *testing.T) {
	ctx := context.Background()
	store, err := sessionstore.Open(ctx, memstore.New())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(context.Background()); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	f := newServiceFixture(t)
	f.service, err = NewService(Config{
		Authorizer: f.auth, Targets: f.targets, Catalog: store, Commands: store,
		Directory: f.directory, Clock: serviceClock{serviceNow}, IDs: &serviceIDs{}, ApplyDeadline: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := f.service.AdmitLegacyCreate(ctx, f.principal, LegacyCreateRequest{AgentID: "agent-a", Blocks: []byte(`[{"text":"hello"}]`)})
	if err != nil {
		t.Fatal(err)
	}
	first := result.Entry

	catalog, err := store.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{TenantID: "tenant-a", SessionID: result.SessionID})
	if err != nil {
		t.Fatal(err)
	}
	if catalog.Record.AgentID != "agent-a" || catalog.Record.RuntimeCompatibilityID != "runtime-v1" || catalog.Record.DesiredIdempotencyKey != string(first.Record.CommandID) {
		t.Fatalf("catalog binding = %+v", catalog.Record)
	}
	stored, err := store.GetCommand(ctx, sessionstore.GetCommandRequest{TenantID: "tenant-a", SessionID: result.SessionID, CommandID: first.Record.CommandID})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(stored, first) {
		t.Fatalf("stored command = %+v, want %+v", stored, first)
	}
}
