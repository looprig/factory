package routing

import (
	"context"
	"errors"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

var directoryNow = time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)

type movableClock struct{ now time.Time }

func (c *movableClock) Now() time.Time { return c.now }

type recordingStore struct {
	listPages      []sessionstore.HostTargetPage
	listRequests   []sessionstore.ListCompatibleHostsRequest
	reconcilePages []sessionstore.HostTargetReconcileResult
	reconcileReqs  []sessionstore.ReconcileHostTargetsRequest
	owner          sessionstore.HostRegistrationEntry
	ownerErr       error
}

func (s *recordingStore) ListCompatibleHosts(_ context.Context, req sessionstore.ListCompatibleHostsRequest) (sessionstore.HostTargetPage, error) {
	s.listRequests = append(s.listRequests, req)
	if len(s.listPages) == 0 {
		return sessionstore.HostTargetPage{}, nil
	}
	page := s.listPages[0]
	s.listPages = s.listPages[1:]
	return page, nil
}

func (s *recordingStore) ReconcileHostTargets(_ context.Context, req sessionstore.ReconcileHostTargetsRequest) (sessionstore.HostTargetReconcileResult, error) {
	s.reconcileReqs = append(s.reconcileReqs, req)
	if len(s.reconcilePages) == 0 {
		return sessionstore.HostTargetReconcileResult{Exhausted: true}, nil
	}
	page := s.reconcilePages[0]
	s.reconcilePages = s.reconcilePages[1:]
	return page, nil
}

func (s *recordingStore) GetHostRegistration(context.Context, sessionstore.GetHostRegistrationRequest) (sessionstore.HostRegistrationEntry, error) {
	return s.owner, s.ownerErr
}

func TestCandidatesBoundsCleanupAndRetriesTheOriginalPosition(t *testing.T) {
	store := &recordingStore{
		listPages: []sessionstore.HostTargetPage{
			{LapsedSkipped: 2, NextCursor: "discarded-list-position-1"},
			{LapsedSkipped: 1, NextCursor: "discarded-list-position-2"},
			{Hosts: []sessionwire.HostLinkCapacityReport{{HostID: "host-live"}}, NextCursor: "honest-next"},
		},
		reconcilePages: []sessionstore.HostTargetReconcileResult{
			{NextCursor: "due-next"},
			{Exhausted: true},
		},
	}
	directory, err := NewDirectory(store, Limits{
		CandidatePageLimit: 2, ReconcilePageLimit: 3, ReconcileMaxPages: 5, MaxCleanupAttempts: 2,
	})
	if err != nil {
		t.Fatalf("NewDirectory: %v", err)
	}
	key := sessionstore.HostTargetKey{AgentID: "agent-a", RuntimeCompatibilityID: "runtime-v1", Placement: sessionwire.HostPlacementPooled}
	page, err := directory.Candidates(context.Background(), sessionstore.ListCompatibleHostsRequest{Key: key, Cursor: "original", Limit: 99})
	if err != nil {
		t.Fatalf("Candidates: %v", err)
	}
	if len(page.Hosts) != 1 || page.Hosts[0].HostID != "host-live" || page.NextCursor != "honest-next" {
		t.Fatalf("page = %+v, want the final store page unchanged", page)
	}
	if len(store.listRequests) != 3 {
		t.Fatalf("list calls = %d, want initial plus two bounded retries", len(store.listRequests))
	}
	for i, req := range store.listRequests {
		if req.Key != key || req.Cursor != "original" || req.Limit != 2 {
			t.Errorf("list request %d = %+v, want original scope/position with limit 2", i, req)
		}
	}
	if len(store.reconcileReqs) != 2 {
		t.Fatalf("reconcile calls = %d, want 2", len(store.reconcileReqs))
	}
	wantPages := []int{3, 2}
	wantCursors := []sessionwire.Cursor{"", "due-next"}
	pages := 0
	for i, req := range store.reconcileReqs {
		pages += req.MaxPages
		if req.Limit != 3 || req.MaxPages != wantPages[i] || req.Cursor != wantCursors[i] {
			t.Errorf("reconcile request %d = %+v", i, req)
		}
	}
	if pages != 5 {
		t.Fatalf("total reconciliation page budget = %d, want 5", pages)
	}
}

func TestNewDirectoryRejectsInvalidLimitsBeforeIO(t *testing.T) {
	valid := DefaultLimits()
	tests := []struct {
		name   string
		store  Store
		mutate func(*Limits)
	}{
		{name: "nil store", store: nil},
		{name: "candidate page zero", store: &recordingStore{}, mutate: func(l *Limits) { l.CandidatePageLimit = 0 }},
		{name: "candidate page beyond store", store: &recordingStore{}, mutate: func(l *Limits) { l.CandidatePageLimit = storage.MaxOrderedPageLimit + 1 }},
		{name: "reconcile page zero", store: &recordingStore{}, mutate: func(l *Limits) { l.ReconcilePageLimit = 0 }},
		{name: "reconcile page beyond store", store: &recordingStore{}, mutate: func(l *Limits) { l.ReconcilePageLimit = storage.MaxOrderedPageLimit + 1 }},
		{name: "reconcile pages zero", store: &recordingStore{}, mutate: func(l *Limits) { l.ReconcileMaxPages = 0 }},
		{name: "reconcile pages beyond store", store: &recordingStore{}, mutate: func(l *Limits) { l.ReconcileMaxPages = sessionstore.MaxHostTargetReconcilePages + 1 }},
		{name: "cleanup attempts zero", store: &recordingStore{}, mutate: func(l *Limits) { l.MaxCleanupAttempts = 0 }},
		{name: "attempts exceed total pages", store: &recordingStore{}, mutate: func(l *Limits) { l.MaxCleanupAttempts = l.ReconcileMaxPages + 1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			limits := valid
			if test.mutate != nil {
				test.mutate(&limits)
			}
			_, err := NewDirectory(test.store, limits)
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("error = %v, want ErrInvalidConfig", err)
			}
			if recorder, ok := test.store.(*recordingStore); ok && (len(recorder.listRequests) != 0 || len(recorder.reconcileReqs) != 0) {
				t.Fatal("invalid configuration performed store I/O")
			}
		})
	}
}

func TestCandidatesUseTheStoresTargetSemantics(t *testing.T) {
	store, clock := openDirectoryStore(t)
	directory := mustDirectory(t, store, Limits{CandidatePageLimit: 10, ReconcilePageLimit: 2, ReconcileMaxPages: 2, MaxCleanupAttempts: 1})
	key := targetKey("agent-a", "runtime-v1", sessionwire.HostPlacementPooled)

	publishTarget(t, store, clock, key, "host-a", 5, 3, true, time.Minute)
	publishTarget(t, store, clock, key, "host-b", 2, 7, true, time.Minute)
	publishTarget(t, store, clock, key, "host-c", 4, 99, false, time.Minute)
	publishTarget(t, store, clock, targetKey("agent-a", "runtime-v2", sessionwire.HostPlacementPooled), "host-d", 1, 90, true, time.Minute)
	publishTarget(t, store, clock, targetKey("agent-b", "runtime-v1", sessionwire.HostPlacementPooled), "host-e", 1, 80, true, time.Minute)
	publishTarget(t, store, clock, targetKey("agent-a", "runtime-v1", sessionwire.HostPlacementDedicated), "host-f", 1, 1, true, time.Minute)

	// A restarted Host's latest generation and capacity are the only hint read.
	publishTarget(t, store, clock, key, "host-a", 6, 4, true, time.Minute)
	_, staleErr := store.PublishHostTarget(context.Background(), targetRequest(clock, key, "host-a", 5, 100, true, time.Minute))
	if staleErr == nil {
		t.Fatal("stale Host generation unexpectedly replaced the current advertisement")
	}

	page, err := directory.Candidates(context.Background(), sessionstore.ListCompatibleHostsRequest{Key: key, Limit: 10})
	if err != nil {
		t.Fatalf("Candidates: %v", err)
	}
	if len(page.Hosts) != 2 {
		t.Fatalf("hosts = %+v, want only two accepting Hosts in the exact target scope", page.Hosts)
	}
	if page.Hosts[0].HostID != "host-b" || page.Hosts[0].AvailableCapacity != 7 {
		t.Errorf("first host = %+v, want host-b ranked by capacity", page.Hosts[0])
	}
	if page.Hosts[1].HostID != "host-a" || page.Hosts[1].HostGeneration != 6 || page.Hosts[1].AvailableCapacity != 4 {
		t.Errorf("second host = %+v, want current host-a generation", page.Hosts[1])
	}

	first, err := directory.Candidates(context.Background(), sessionstore.ListCompatibleHostsRequest{Key: key, Limit: 1})
	if err != nil || len(first.Hosts) != 1 || first.NextCursor == "" {
		t.Fatalf("first bounded page = %+v, %v", first, err)
	}
	second, err := directory.Candidates(context.Background(), sessionstore.ListCompatibleHostsRequest{Key: key, Cursor: first.NextCursor, Limit: 1})
	if err != nil || len(second.Hosts) != 1 || second.Hosts[0].HostID != "host-a" {
		t.Fatalf("second bounded page = %+v, %v", second, err)
	}
}

func TestExpiredCapacityIsCleanedBeforeTheBoundedRetry(t *testing.T) {
	store, clock := openDirectoryStore(t)
	directory := mustDirectory(t, store, Limits{CandidatePageLimit: 1, ReconcilePageLimit: 1, ReconcileMaxPages: 1, MaxCleanupAttempts: 1})
	key := targetKey("agent-a", "runtime-v1", sessionwire.HostPlacementPooled)
	publishTarget(t, store, clock, key, "host-stale", 1, 100, true, time.Minute)
	publishTarget(t, store, clock, key, "host-live", 1, 1, true, 3*time.Minute)
	clock.now = clock.now.Add(2 * time.Minute)

	page, err := directory.Candidates(context.Background(), sessionstore.ListCompatibleHostsRequest{Key: key, Limit: 1})
	if err != nil {
		t.Fatalf("Candidates: %v", err)
	}
	if len(page.Hosts) != 1 || page.Hosts[0].HostID != "host-live" || page.LapsedSkipped != 0 {
		t.Fatalf("page after cleanup retry = %+v, want live Host", page)
	}
	stored, err := store.ListCompatibleHosts(context.Background(), sessionstore.ListCompatibleHostsRequest{Key: key, Limit: 2})
	if err != nil || stored.LapsedSkipped != 0 || len(stored.Hosts) != 1 {
		t.Fatalf("store page after due cleanup = %+v, %v", stored, err)
	}
}

func TestOwnerComesOnlyFromTheLiveSessionRegistry(t *testing.T) {
	store, clock := openDirectoryStore(t)
	directory := mustDirectory(t, store, DefaultLimits())
	tenant, session := sessionwire.TenantID("tenant-a"), sessionwire.SessionID("session-a")
	if _, _, err := store.CreateCatalogEntry(context.Background(), sessionstore.CreateCatalogEntryRequest{
		TenantID: tenant, SessionID: session, AgentID: "agent-a", RuntimeCompatibilityID: "runtime-v1",
		CreatedAt: clock.now, LastActiveAt: clock.now, State: sessionwire.SessionStateIdle,
		Residency: sessionwire.SessionResidencyCold, DesiredPlacement: sessionwire.HostPlacementPooled,
		IdempotencyKey: "create-directory-owner",
	}); err != nil {
		t.Fatalf("CreateCatalogEntry: %v", err)
	}
	_, err := store.PutHostRegistration(context.Background(), sessionstore.PutHostRegistrationRequest{
		TenantID: tenant, SessionID: session, LeaseEpoch: 7, ObservedAt: clock.now, ExpiresAt: clock.now.Add(time.Minute),
		Route: sessionstore.HostRoute{HostID: "host-owner", HostGeneration: 9, AgentID: "agent-a", RuntimeCompatibilityID: "runtime-v1",
			Placement: sessionwire.HostPlacementPooled, InternalEndpoint: "wss://host-owner.internal/hostlink",
			Residency: sessionwire.SessionResidencyResident, Accepting: true},
	})
	if err != nil {
		t.Fatalf("PutHostRegistration: %v", err)
	}
	owner, ok, err := directory.Owner(context.Background(), tenant, session)
	if err != nil || !ok || owner.HostID != "host-owner" || owner.HostGeneration != 9 || owner.LeaseEpoch != 7 {
		t.Fatalf("Owner = %+v, %v, %v", owner, ok, err)
	}

	// Capacity remains advertised, but an expired registry is no ownership.
	publishTarget(t, store, clock, targetKey("agent-a", "runtime-v1", sessionwire.HostPlacementPooled), "host-owner", 9, 10, true, 3*time.Minute)
	clock.now = clock.now.Add(time.Minute)
	owner, ok, err = directory.Owner(context.Background(), tenant, session)
	if err != nil || ok || owner != (sessionwire.HostLinkRegistryObservation{}) {
		t.Fatalf("expired Owner = %+v, %v, %v; want absent", owner, ok, err)
	}
}

func TestOwnerClassifiesStoreErrors(t *testing.T) {
	tenant, session := sessionwire.TenantID("tenant-a"), sessionwire.SessionID("session-a")
	backendCause := errors.New("backend unavailable")
	backendErr := &sessionstore.RegistryError{
		Code:  sessionstore.RegistryErrorBackend,
		Field: "registration",
		Cause: backendCause,
	}
	nonRegistryErr := errors.New("unexpected store failure")
	tests := []struct {
		name     string
		storeErr error
		absent   bool
	}{
		{name: "not found is absence", storeErr: &sessionstore.RegistryError{Code: sessionstore.RegistryErrorNotFound}, absent: true},
		{name: "expired is absence", storeErr: &sessionstore.RegistryError{Code: sessionstore.RegistryErrorExpired}, absent: true},
		{name: "released is absence", storeErr: &sessionstore.RegistryError{Code: sessionstore.RegistryErrorReleased}, absent: true},
		// A session that was never created never reaches the registry at all:
		// outside the legacy single-tenant layout the store verifies its
		// collision witnesses first and answers *KeyspaceError
		// binding_not_found. sessionstore's own noSuchSession names exactly
		// this set, and internal/httpapi already learned the same lesson --
		// reading only the registry codes made every absent session a store
		// FAILURE rather than an absent owner.
		{name: "an unbound session is absence", storeErr: &sessionstore.KeyspaceError{Code: sessionstore.KeyspaceBindingNotFound}, absent: true},
		// The control that keeps the arm above from swallowing the keyspace's
		// real failures: a layout mismatch is a deployment fault, not absence.
		{name: "another keyspace failure is propagated", storeErr: &sessionstore.KeyspaceError{Code: sessionstore.KeyspaceLayoutMismatch}},
		{name: "backend failure is propagated", storeErr: backendErr},
		{name: "non-registry failure is propagated", storeErr: nonRegistryErr},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			directory := mustDirectory(t, &recordingStore{ownerErr: test.storeErr}, DefaultLimits())
			owner, ok, err := directory.Owner(context.Background(), tenant, session)
			if owner != (sessionwire.HostLinkRegistryObservation{}) || ok {
				t.Fatalf("Owner = %+v, %v, %v; want no routable owner", owner, ok, err)
			}
			if test.absent {
				if err != nil {
					t.Fatalf("Owner error = %v, want absence", err)
				}
				return
			}
			if err != test.storeErr {
				t.Fatalf("Owner error = %v, want exact store error %v", err, test.storeErr)
			}
			if test.storeErr == backendErr {
				var registryErr *sessionstore.RegistryError
				if !errors.As(err, &registryErr) || registryErr.Code != sessionstore.RegistryErrorBackend || !errors.Is(err, backendCause) {
					t.Fatalf("Owner error lost its typed classification or cause: %v", err)
				}
			}
		})
	}
}

// TestOwnerReportsASessionNobodyRegisteredAsAbsent is the case the table above
// could not have caught on its own. The fake answers whatever error a case
// hands it, so a table written from the registry's vocabulary tests the
// classification against the classifier's own assumptions; this drives the
// REAL store, which is where the answer for a session nobody registered
// actually comes from.
func TestOwnerReportsASessionNobodyRegisteredAsAbsent(t *testing.T) {
	store, _ := openDirectoryStore(t)
	directory := mustDirectory(t, store, DefaultLimits())

	owner, ok, err := directory.Owner(context.Background(), "tenant-a", "session-never-created")
	if err != nil {
		t.Fatalf("Owner = %v, want an absent owner rather than a failure", err)
	}
	if ok || owner != (sessionwire.HostLinkRegistryObservation{}) {
		t.Fatalf("Owner = %+v, %v; want no routable owner", owner, ok)
	}

	// And the premise, so a later sessionstore that stopped answering this way
	// would say so here rather than leaving the arm above covering nothing.
	_, direct := store.GetHostRegistration(context.Background(), sessionstore.GetHostRegistrationRequest{
		TenantID: "tenant-a", SessionID: "session-never-created",
	})
	var keyspace *sessionstore.KeyspaceError
	if !errors.As(direct, &keyspace) || keyspace.Code != sessionstore.KeyspaceBindingNotFound {
		t.Fatalf("the store answered %v for a session nobody created, want a keyspace binding_not_found", direct)
	}
}

func TestOwnerPropagatesMalformedObservation(t *testing.T) {
	tenant, session := sessionwire.TenantID("tenant-a"), sessionwire.SessionID("session-a")
	store := &recordingStore{owner: sessionstore.HostRegistrationEntry{Registration: sessionstore.HostRegistration{
		TenantID: tenant, SessionID: session, LeaseEpoch: 1,
		ObservedAt: directoryNow, ExpiresAt: directoryNow.Add(time.Minute),
		Route: &sessionstore.HostRoute{
			HostID: "host-a", HostGeneration: 1, AgentID: "agent-a", RuntimeCompatibilityID: "runtime-v1",
			Placement: sessionwire.HostPlacementPooled, Residency: sessionwire.SessionResidencyResident, Accepting: true,
		},
	}}}
	directory := mustDirectory(t, store, DefaultLimits())
	owner, ok, err := directory.Owner(context.Background(), tenant, session)
	if owner != (sessionwire.HostLinkRegistryObservation{}) || ok || err == nil {
		t.Fatalf("Owner = %+v, %v, %v; want malformed observation failure", owner, ok, err)
	}
	var registryErr *sessionstore.RegistryError
	if !errors.As(err, &registryErr) || registryErr.Code != sessionstore.RegistryErrorInvalid {
		t.Fatalf("Owner error = %v, want typed invalid registry error", err)
	}
}

func openDirectoryStore(t *testing.T) (*sessionstore.Store, *movableClock) {
	t.Helper()
	clock := &movableClock{now: directoryNow}
	store, err := sessionstore.Open(context.Background(), memstore.New(), sessionstore.WithClock(clock))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(context.Background()); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return store, clock
}

func mustDirectory(t *testing.T, store Store, limits Limits) *Directory {
	t.Helper()
	directory, err := NewDirectory(store, limits)
	if err != nil {
		t.Fatalf("NewDirectory: %v", err)
	}
	return directory
}

func targetKey(agent sessionwire.AgentID, runtime string, placement sessionwire.HostPlacement) sessionstore.HostTargetKey {
	return sessionstore.HostTargetKey{AgentID: agent, RuntimeCompatibilityID: runtime, Placement: placement}
}

func targetRequest(clock *movableClock, key sessionstore.HostTargetKey, host sessionwire.HostID, generation, capacity uint64, accepting bool, ttl time.Duration) sessionstore.PublishHostTargetRequest {
	isolation := sessionwire.HostIsolationClassCrossTenantIsolated
	if key.Placement == sessionwire.HostPlacementDedicated {
		isolation = sessionwire.HostIsolationClassTenantExclusive
	}
	return sessionstore.PublishHostTargetRequest{
		Key: key, HostID: host, HostGeneration: generation, ObservedAt: clock.now,
		Advertisement: sessionstore.HostAdvertisement{InternalEndpoint: sessionwire.InternalEndpoint("wss://" + string(host) + ".internal/hostlink"),
			IsolationClass: isolation, Accepting: accepting, AvailableCapacity: capacity, ExpiresAt: clock.now.Add(ttl)},
	}
}

func publishTarget(t *testing.T, store *sessionstore.Store, clock *movableClock, key sessionstore.HostTargetKey, host sessionwire.HostID, generation, capacity uint64, accepting bool, ttl time.Duration) {
	t.Helper()
	if _, err := store.PublishHostTarget(context.Background(), targetRequest(clock, key, host, generation, capacity, accepting, ttl)); err != nil {
		t.Fatalf("PublishHostTarget(%s): %v", host, err)
	}
}
