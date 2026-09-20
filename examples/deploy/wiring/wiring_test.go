package wiring

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/looprig/sessionstore"
	"github.com/looprig/storage/memstore"
)

func TestSessionStoreOutlivesStartupContext(t *testing.T) {
	startup, cancel := context.WithTimeout(context.Background(), time.Second)
	store, err := openSessionStore(startup, memstore.New())
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	cancel()
	if _, err := store.ListSessions(context.Background(), sessionstore.ListSessionsRequest{TenantID: "tenant-a", Limit: 10}); err != nil {
		t.Fatalf("Store read after startup cancellation: %v", err)
	}
}

func TestCancelledStartupIsRefused(t *testing.T) {
	startup, cancel := context.WithCancel(context.Background())
	cancel()
	store, err := openSessionStore(startup, memstore.New())
	if store != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("got store %v, err %v; want no store and context cancellation", store, err)
	}
}
