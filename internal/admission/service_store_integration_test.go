package admission

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage/memstore"
)

func TestServiceRefusesLegacyCreateWithoutPersistingASession(t *testing.T) {
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
	if !reflect.DeepEqual(result, LegacyCreateResult{}) {
		t.Fatalf("result = %+v, want zero", result)
	}
	if !errors.Is(err, ErrLegacyCreateUnsupported) || !IsCode(err, sessionwire.ErrorCodeRuntimeUnavailable) {
		t.Fatalf("error = %v, want runtime_unavailable wrapping ErrLegacyCreateUnsupported", err)
	}
	if f.ids.next != 0 {
		t.Fatalf("IDs minted = %d, want 0", f.ids.next)
	}
	_, err = store.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{TenantID: "tenant-a", SessionID: sessionwire.SessionID("generated-1")})
	if err == nil {
		t.Fatal("legacy create persisted generated-1 despite refusing")
	}
	if !catalogNotFound(err) {
		t.Fatalf("GetCatalogEntry(generated-1) = %v, want session absence", err)
	}
}
