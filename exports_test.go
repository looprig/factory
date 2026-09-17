package factory_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory"
	"github.com/looprig/factory/identity"
	internalidentity "github.com/looprig/factory/internal/identity"
	"github.com/looprig/factory/internal/routing"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage/memstore"
)

// TestTenantAuthorizerIsTheInternalAuthorizer holds the exported wrapper to the
// internal implementation in both directions: the method SETS are equal by
// name and signature, so a method added to one and not the other fails here
// rather than composing a Factory that authorizes one edge differently, and
// every method answers EXACTLY what the internal one answers over a table that
// drives each decision to both a grant and a denial.
//
// The rows assert the outcome absolutely -- nil, or a denial wrapping
// identity.ErrUnauthorized -- rather than only "the two agree", because two
// wrappers that both returned nil for everything would agree perfectly.
func TestTenantAuthorizerIsTheInternalAuthorizer(t *testing.T) {
	t.Parallel()

	exported := reflect.TypeOf(factory.TenantAuthorizer{})
	internal := reflect.TypeOf(internalidentity.Authorizer{})
	if exported.NumMethod() == 0 {
		t.Fatal("TenantAuthorizer has no methods, so this comparison is vacuous")
	}
	if exported.NumMethod() != internal.NumMethod() {
		t.Errorf("TenantAuthorizer has %d methods, internal/identity.Authorizer has %d", exported.NumMethod(), internal.NumMethod())
	}
	for i := 0; i < exported.NumMethod(); i++ {
		method := exported.Method(i)
		counterpart, ok := internal.MethodByName(method.Name)
		if !ok {
			t.Errorf("TenantAuthorizer.%s has no internal counterpart", method.Name)
			continue
		}
		if method.Type.NumIn() != counterpart.Type.NumIn() || method.Type.NumOut() != counterpart.Type.NumOut() {
			t.Errorf("%s: arity differs, %s vs %s", method.Name, method.Type, counterpart.Type)
			continue
		}
		// Index 0 is the receiver, whose type legitimately differs.
		for in := 1; in < method.Type.NumIn(); in++ {
			if method.Type.In(in) != counterpart.Type.In(in) {
				t.Errorf("%s: parameter %d is %s, internal is %s", method.Name, in, method.Type.In(in), counterpart.Type.In(in))
			}
		}
	}

	actor, err := identity.NewPrincipal("tenant-a", "user-a", identity.KindActor)
	if err != nil {
		t.Fatal(err)
	}
	service, err := identity.NewPrincipal("tenant-a", "factory-sweeper", identity.KindService)
	if err != nil {
		t.Fatal(err)
	}
	var zero identity.Principal
	ctx := context.Background()

	type decision func(a factory.Authorizer) error
	rows := []struct {
		name   string
		decide decision
		denied bool
	}{
		{"list, actor", func(a factory.Authorizer) error { return a.AuthorizeSessionList(ctx, actor) }, false},
		{"list, unconstructed principal", func(a factory.Authorizer) error { return a.AuthorizeSessionList(ctx, zero) }, true},
		{"read, actor", func(a factory.Authorizer) error { return a.AuthorizeSessionRead(ctx, actor, "s-1") }, false},
		{"read, unconstructed principal", func(a factory.Authorizer) error { return a.AuthorizeSessionRead(ctx, zero, "s-1") }, true},
		{"object read, actor", func(a factory.Authorizer) error {
			return a.AuthorizeObjectRead(ctx, actor, "s-1", sessionwire.ObjectReference{})
		}, false},
		{"object read, unconstructed principal", func(a factory.Authorizer) error {
			return a.AuthorizeObjectRead(ctx, zero, "s-1", sessionwire.ObjectReference{})
		}, true},
		{"control, actor", func(a factory.Authorizer) error { return a.AuthorizeControl(ctx, actor, "s-1", "input") }, false},
		{"control, unconstructed principal", func(a factory.Authorizer) error { return a.AuthorizeControl(ctx, zero, "s-1", "input") }, true},
		{"subscribe, own tenant", func(a factory.Authorizer) error { return a.AuthorizeSubscribe(ctx, actor, "session:tenant-a:s-1") }, false},
		{"subscribe, another tenant", func(a factory.Authorizer) error { return a.AuthorizeSubscribe(ctx, actor, "session:tenant-b:s-1") }, true},
		{"subscribe, a channel the grammar refuses", func(a factory.Authorizer) error {
			return a.AuthorizeSubscribe(ctx, actor, "session:tenant-a:\xff")
		}, true},
		{"subscribe, unconstructed principal", func(a factory.Authorizer) error { return a.AuthorizeSubscribe(ctx, zero, "session::s-1") }, true},
		{"sweep, service", func(a factory.Authorizer) error { return a.AuthorizeServiceSweep(ctx, service) }, false},
		{"sweep, actor", func(a factory.Authorizer) error { return a.AuthorizeServiceSweep(ctx, actor) }, true},
	}
	for _, row := range rows {
		got := row.decide(factory.TenantAuthorizer{})
		want := row.decide(internalidentity.Authorizer{})
		if (got == nil) != (want == nil) || errors.Is(got, identity.ErrUnauthorized) != errors.Is(want, identity.ErrUnauthorized) {
			t.Errorf("%s: TenantAuthorizer = %v, internal = %v", row.name, got, want)
		}
		if row.denied {
			if !errors.Is(got, identity.ErrUnauthorized) {
				t.Errorf("%s: got %v, want a denial wrapping identity.ErrUnauthorized", row.name, got)
			}
		} else if got != nil {
			t.Errorf("%s: got %v, want nil", row.name, got)
		}
	}
}

// TestNewStoreDirectoryReadsTheStoresRegistryAndTargetIndex drives the exported
// directory over the released store: ownership comes from the epoch-fenced
// registry and candidates from the ranked target index, in the store's own
// capacity order, and an accepting=false row is not a candidate.
func TestNewStoreDirectoryReadsTheStoresRegistryAndTargetIndex(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clock := &fixedClock{now: time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)}
	store, err := sessionstore.Open(ctx, memstore.New(), sessionstore.WithClock(clock))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(context.Background()); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	directory, err := factory.NewStoreDirectory(store, factory.DefaultDirectoryLimits())
	if err != nil {
		t.Fatalf("NewStoreDirectory: %v", err)
	}
	// The exported constructor answers the public Directory seam, so the value
	// composes through WithDirectory with no adapter.
	if _, err := factory.New(append(factory.RequiredOptionsExcept("WithDirectory", "WithAuthorizer"),
		factory.WithDirectory(directory), factory.WithAuthorizer(factory.TenantAuthorizer{}))...); err != nil {
		t.Fatalf("New with the exported directory and authorizer: %v", err)
	}

	tenant, session := sessionwire.TenantID("tenant-a"), sessionwire.SessionID("session-a")
	if owner, ok, err := directory.Owner(ctx, tenant, session); err != nil || ok || owner != (sessionwire.HostLinkRegistryObservation{}) {
		t.Fatalf("Owner of a session nobody registered = %+v, %v, %v; want absent with no error", owner, ok, err)
	}

	key := sessionstore.HostTargetKey{AgentID: "agent-a", RuntimeCompatibilityID: "runtime-v1", Placement: sessionwire.HostPlacementPooled}
	if _, _, err := store.CreateCatalogEntry(ctx, sessionstore.CreateCatalogEntryRequest{
		TenantID: tenant, SessionID: session, AgentID: "agent-a", RuntimeCompatibilityID: "runtime-v1",
		CreatedAt: clock.now, LastActiveAt: clock.now, State: sessionwire.SessionStateIdle,
		Residency: sessionwire.SessionResidencyCold, DesiredPlacement: sessionwire.HostPlacementPooled,
		IdempotencyKey: "create-export-owner",
	}); err != nil {
		t.Fatalf("CreateCatalogEntry: %v", err)
	}
	publish := func(host sessionwire.HostID, capacity uint64, accepting bool) {
		t.Helper()
		_, err := store.PublishHostTarget(ctx, sessionstore.PublishHostTargetRequest{
			Key: key, HostID: host, HostGeneration: 1, ObservedAt: clock.now,
			Advertisement: sessionstore.HostAdvertisement{
				InternalEndpoint: sessionwire.InternalEndpoint("wss://" + string(host) + ".internal/hostlink"),
				IsolationClass:   sessionwire.HostIsolationClassCrossTenantIsolated,
				Accepting:        accepting, AvailableCapacity: capacity, ExpiresAt: clock.now.Add(time.Minute),
			},
		})
		if err != nil {
			t.Fatalf("PublishHostTarget(%s): %v", host, err)
		}
	}
	publish("host-small", 3, true)
	publish("host-large", 9, true)
	publish("host-closed", 99, false)

	page, err := directory.Candidates(ctx, sessionstore.ListCompatibleHostsRequest{Key: key, Limit: 10})
	if err != nil {
		t.Fatalf("Candidates: %v", err)
	}
	if len(page.Hosts) != 2 || page.Hosts[0].HostID != "host-large" || page.Hosts[1].HostID != "host-small" {
		t.Fatalf("Candidates = %+v, want host-large then host-small and no closed host", page.Hosts)
	}

	if _, err := store.PutHostRegistration(ctx, sessionstore.PutHostRegistrationRequest{
		TenantID: tenant, SessionID: session, LeaseEpoch: 7, ObservedAt: clock.now, ExpiresAt: clock.now.Add(time.Minute),
		Route: sessionstore.HostRoute{HostID: "host-large", HostGeneration: 1, AgentID: "agent-a", RuntimeCompatibilityID: "runtime-v1",
			Placement: sessionwire.HostPlacementPooled, InternalEndpoint: "wss://host-large.internal/hostlink",
			Residency: sessionwire.SessionResidencyResident, Accepting: true},
	}); err != nil {
		t.Fatalf("PutHostRegistration: %v", err)
	}
	owner, ok, err := directory.Owner(ctx, tenant, session)
	if err != nil || !ok || owner.HostID != "host-large" || owner.LeaseEpoch != 7 {
		t.Fatalf("Owner after registration = %+v, %v, %v; want host-large at epoch 7", owner, ok, err)
	}
	// An expired registration is no owner, and it is not an error either.
	clock.now = clock.now.Add(2 * time.Minute)
	if owner, ok, err := directory.Owner(ctx, tenant, session); err != nil || ok || owner != (sessionwire.HostLinkRegistryObservation{}) {
		t.Fatalf("expired Owner = %+v, %v, %v; want absent with no error", owner, ok, err)
	}
}

// TestNewStoreDirectoryRefusesWhatItCannotBound is the constructor's refusal
// table, each row asserted by the exported sentinel and none by text.
func TestNewStoreDirectoryRefusesWhatItCannotBound(t *testing.T) {
	t.Parallel()

	store, err := sessionstore.Open(context.Background(), memstore.New())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(context.Background()) })

	valid := factory.DefaultDirectoryLimits()
	for _, row := range []struct {
		name   string
		store  factory.DirectoryStore
		limits factory.DirectoryLimits
	}{
		{"nil store", nil, valid},
		{"zero limits", store, factory.DirectoryLimits{}},
		{"candidate page past the store ceiling", store, factory.DirectoryLimits{CandidatePageLimit: 1001, ReconcilePageLimit: 32, ReconcileMaxPages: 4, MaxCleanupAttempts: 2}},
		{"more cleanup attempts than pages", store, factory.DirectoryLimits{CandidatePageLimit: 32, ReconcilePageLimit: 32, ReconcileMaxPages: 2, MaxCleanupAttempts: 3}},
	} {
		directory, err := factory.NewStoreDirectory(row.store, row.limits)
		if !errors.Is(err, factory.ErrInvalidDirectoryLimits) {
			t.Errorf("%s: err = %v, want ErrInvalidDirectoryLimits", row.name, err)
		}
		if directory != nil {
			t.Errorf("%s: a refused constructor returned a non-nil Directory", row.name)
		}
	}
	// The control: the defaults are accepted, and they are internal/routing's
	// own table rather than a second one.
	if _, err := factory.NewStoreDirectory(store, valid); err != nil {
		t.Fatalf("NewStoreDirectory with defaults: %v", err)
	}
	if valid != routing.DefaultLimits() {
		t.Errorf("DefaultDirectoryLimits = %+v, routing.DefaultLimits = %+v; two tables for one bound", valid, routing.DefaultLimits())
	}
}

type fixedClock struct{ now time.Time }

func (c *fixedClock) Now() time.Time { return c.now }
