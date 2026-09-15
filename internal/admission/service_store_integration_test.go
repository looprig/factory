package admission

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage/memstore"
)

type fixedServiceID string

func (id fixedServiceID) NewUUID() (string, error) { return string(id), nil }

type publicCreateBarrier struct {
	inner PublicCreateStore
	ready chan struct{}
	once  sync.Once
	mu    sync.Mutex
	n     int
}

func newPublicCreateBarrier(inner PublicCreateStore, participants int) *publicCreateBarrier {
	return &publicCreateBarrier{inner: inner, ready: make(chan struct{}), n: participants}
}

func (b *publicCreateBarrier) PreparePublicCreate(ctx context.Context, req sessionstore.PreparePublicCreateRequest) (sessionstore.PublicCreatePreparation, error) {
	b.mu.Lock()
	b.n--
	if b.n == 0 {
		b.once.Do(func() { close(b.ready) })
	}
	b.mu.Unlock()
	select {
	case <-b.ready:
	case <-ctx.Done():
		return sessionstore.PublicCreatePreparation{}, ctx.Err()
	}
	return b.inner.PreparePublicCreate(ctx, req)
}

func (b *publicCreateBarrier) PutCommandPayload(ctx context.Context, req sessionstore.PutCommandPayloadRequest) (sessionwire.ObjectMetadata, error) {
	return b.inner.PutCommandPayload(ctx, req)
}

func (b *publicCreateBarrier) AdmitPublicCreate(ctx context.Context, req sessionstore.AdmitPublicCreateRequest) (sessionstore.DispositionInboxEntry, bool, error) {
	return b.inner.AdmitPublicCreate(ctx, req)
}

type publicCreateFaultPoint uint8

const (
	failBeforePrepare publicCreateFaultPoint = iota + 1
	failAfterPrepare
	failAfterAdmit
)

type oneShotPublicCreateFault struct {
	inner PublicCreateStore
	point publicCreateFaultPoint
	once  sync.Once
}

func (f *oneShotPublicCreateFault) fires(at publicCreateFaultPoint) bool {
	fired := false
	if f.point == at {
		f.once.Do(func() { fired = true })
	}
	return fired
}

func (f *oneShotPublicCreateFault) PreparePublicCreate(ctx context.Context, req sessionstore.PreparePublicCreateRequest) (sessionstore.PublicCreatePreparation, error) {
	if f.fires(failBeforePrepare) {
		return sessionstore.PublicCreatePreparation{}, errInjectedFault
	}
	prepared, err := f.inner.PreparePublicCreate(ctx, req)
	if err == nil && f.fires(failAfterPrepare) {
		return sessionstore.PublicCreatePreparation{}, errInjectedFault
	}
	return prepared, err
}

func (f *oneShotPublicCreateFault) PutCommandPayload(ctx context.Context, req sessionstore.PutCommandPayloadRequest) (sessionwire.ObjectMetadata, error) {
	return f.inner.PutCommandPayload(ctx, req)
}

func (f *oneShotPublicCreateFault) AdmitPublicCreate(ctx context.Context, req sessionstore.AdmitPublicCreateRequest) (sessionstore.DispositionInboxEntry, bool, error) {
	entry, created, err := f.inner.AdmitPublicCreate(ctx, req)
	if err == nil && f.fires(failAfterAdmit) {
		return sessionstore.DispositionInboxEntry{}, false, errInjectedFault
	}
	return entry, created, err
}

func openCreateIntegrationStore(t *testing.T) *sessionstore.Store {
	t.Helper()
	store, err := sessionstore.Open(context.Background(), memstore.New(), sessionstore.WithClock(serviceClock{serviceNow}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(context.Background()); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return store
}

func newCreateIntegrationService(t *testing.T, store *sessionstore.Store, creates PublicCreateStore, now time.Time, runtimeCommandID string) (*Service, identity.Principal) {
	t.Helper()
	f := newServiceFixture(t)
	svc, err := NewService(Config{
		Authorizer: f.auth, Targets: f.targets, Catalog: store, Commands: store,
		Directory: f.directory, Clock: serviceClock{now}, IDs: fixedServiceID(runtimeCommandID),
		PublicCreates: creates,
		Binding:       SessionBindingTemplate{StorageBindingID: "storage-a", BindingVersion: "v1"},
		ApplyDeadline: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	return svc, f.principal
}

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

func TestTwoServicesRaceOnePublicCreateAndReturnOneAuthoritativeRecord(t *testing.T) {
	store := openCreateIntegrationStore(t)
	barrier := newPublicCreateBarrier(store, 2)
	first, principal := newCreateIntegrationService(t, store, barrier, serviceNow, "runtime-command-first")
	second, _ := newCreateIntegrationService(t, store, barrier, serviceNow.Add(time.Hour), "runtime-command-second")
	req := createRequest("create-race", "session-race", smallBlocks)

	type result struct {
		entry          sessionstore.DispositionInboxEntry
		created        bool
		err            error
		wantRuntimeID  sessionstore.RuntimeCommandID
		wantAcceptedAt time.Time
	}
	results := make(chan result, 2)
	for i, svc := range []*Service{first, second} {
		i, svc := i, svc
		go func() {
			entry, created, err := svc.AdmitCreate(context.Background(), principal, req)
			wantRuntimeID := sessionstore.RuntimeCommandID("runtime-command-first")
			wantAcceptedAt := serviceNow
			if i == 1 {
				wantRuntimeID = "runtime-command-second"
				wantAcceptedAt = serviceNow.Add(time.Hour)
			}
			results <- result{entry: entry, created: created, err: err, wantRuntimeID: wantRuntimeID, wantAcceptedAt: wantAcceptedAt}
		}()
	}
	a, b := <-results, <-results
	if a.err != nil || b.err != nil {
		t.Fatalf("race errors = (%v, %v)", a.err, b.err)
	}
	if a.created == b.created {
		t.Fatalf("created results = (%v, %v), want exactly one creator", a.created, b.created)
	}
	if !reflect.DeepEqual(a.entry, b.entry) {
		t.Fatalf("replicas returned different records:\n first %+v\nsecond %+v", a.entry, b.entry)
	}
	winner := a
	if b.created {
		winner = b
	}
	// PreparePublicCreate chooses the immutable proposal winner. Either caller
	// may subsequently win AdmitPublicCreate and receive created=true, so the
	// reservation winner and inbox writer are intentionally not conflated.
	// The authoritative tuple must nevertheless be EXACTLY one replica's whole
	// proposal, and both replicas must return it.
	proposal := a
	gotRuntime := winner.entry.Record.Descriptor.RuntimeCommandID
	if gotRuntime == b.wantRuntimeID {
		proposal = b
	} else if gotRuntime != a.wantRuntimeID {
		t.Fatalf("authoritative runtime mapping %q is neither replica's proposal", gotRuntime)
	}
	if !winner.entry.Record.AcceptedAt.Equal(proposal.wantAcceptedAt) {
		t.Fatalf("authoritative AcceptedAt %v does not match proposal owner %v", winner.entry.Record.AcceptedAt, proposal.wantAcceptedAt)
	}
	wantDeadline := proposal.wantAcceptedAt.Add(time.Minute)
	if !winner.entry.Record.ApplyDeadline.Equal(wantDeadline) {
		t.Fatalf("authoritative ApplyDeadline %v, want proposal owner's exact deadline %v", winner.entry.Record.ApplyDeadline, wantDeadline)
	}
	stored, err := store.GetDispositionCommand(context.Background(), sessionstore.GetDispositionCommandRequest{
		TenantID: principal.Tenant(), SessionID: req.SessionID, CommandID: req.CommandID,
	})
	if err != nil {
		t.Fatalf("GetDispositionCommand: %v", err)
	}
	if !reflect.DeepEqual(stored, winner.entry) {
		t.Fatalf("stored record differs from both responses:\n stored %+v\nresponse %+v", stored, winner.entry)
	}
	if a.entry.AcceptedOrder != 1 {
		t.Fatalf("AcceptedOrder = %d, want the first order in this session", a.entry.AcceptedOrder)
	}
}

func TestTwoServicesRaceDistinctPublicCreatesWithoutSharingOrderOrMappings(t *testing.T) {
	store := openCreateIntegrationStore(t)
	barrier := newPublicCreateBarrier(store, 2)
	first, principal := newCreateIntegrationService(t, store, barrier, serviceNow, "runtime-command-a")
	second, _ := newCreateIntegrationService(t, store, barrier, serviceNow.Add(time.Hour), "runtime-command-b")
	requests := []sessionwire.CreateRequest{
		createRequest("create-a", "session-a", smallBlocks),
		createRequest("create-b", "session-b", smallBlocks),
	}

	type result struct {
		entry sessionstore.DispositionInboxEntry
		err   error
	}
	results := make(chan result, 2)
	for i, svc := range []*Service{first, second} {
		i, svc := i, svc
		go func() {
			entry, created, err := svc.AdmitCreate(context.Background(), principal, requests[i])
			if err == nil && !created {
				err = errors.New("distinct first admission was not created")
			}
			results <- result{entry: entry, err: err}
		}()
	}
	a, b := <-results, <-results
	if a.err != nil || b.err != nil {
		t.Fatalf("race errors = (%v, %v)", a.err, b.err)
	}
	bySession := map[sessionwire.SessionID]sessionstore.DispositionInboxEntry{
		a.entry.Record.Descriptor.SessionID: a.entry,
		b.entry.Record.Descriptor.SessionID: b.entry,
	}
	if len(bySession) != 2 {
		t.Fatalf("persisted sessions = %v, want both distinct sessions", bySession)
	}
	wantTuples := map[sessionwire.SessionID]struct {
		runtimeID  sessionstore.RuntimeCommandID
		acceptedAt time.Time
	}{
		"session-a": {runtimeID: "runtime-command-a", acceptedAt: serviceNow},
		"session-b": {runtimeID: "runtime-command-b", acceptedAt: serviceNow.Add(time.Hour)},
	}
	for i, req := range requests {
		original := bySession[req.SessionID]
		want := wantTuples[req.SessionID]
		if got := original.Record.Descriptor.RuntimeCommandID; got != want.runtimeID {
			t.Fatalf("%s runtime mapping = %q, want %q", req.SessionID, got, want.runtimeID)
		}
		if !original.Record.AcceptedAt.Equal(want.acceptedAt) {
			t.Fatalf("%s AcceptedAt = %v, want %v", req.SessionID, original.Record.AcceptedAt, want.acceptedAt)
		}
		if wantDeadline := want.acceptedAt.Add(time.Minute); !original.Record.ApplyDeadline.Equal(wantDeadline) {
			t.Fatalf("%s ApplyDeadline = %v, want %v", req.SessionID, original.Record.ApplyDeadline, wantDeadline)
		}
		stored, err := store.GetDispositionCommand(context.Background(), sessionstore.GetDispositionCommandRequest{
			TenantID: principal.Tenant(), SessionID: req.SessionID, CommandID: req.CommandID,
		})
		if err != nil {
			t.Fatalf("GetDispositionCommand(%s): %v", req.SessionID, err)
		}
		if !reflect.DeepEqual(stored, original) {
			t.Fatalf("stored %s differs from its response:\n stored %+v\nresponse %+v", req.SessionID, stored, original)
		}
		retryService := second
		if i == 1 {
			retryService = first
		}
		retry, created, err := retryService.AdmitCreate(context.Background(), principal, req)
		if err != nil || created {
			t.Fatalf("retry %s = (created %v, err %v)", req.SessionID, created, err)
		}
		if !reflect.DeepEqual(retry, original) {
			t.Fatalf("retry changed %s:\n got %+v\nwant %+v", req.SessionID, retry, original)
		}
		if retry.AcceptedOrder != 1 {
			t.Errorf("%s AcceptedOrder = %d, want 1; order is per session", req.SessionID, retry.AcceptedOrder)
		}
	}
	if bySession["session-a"].Record.Descriptor.RuntimeCommandID == bySession["session-b"].Record.Descriptor.RuntimeCommandID {
		t.Fatal("distinct creates shared one runtime command mapping")
	}
}

func TestPublicCreateRetriesResolveEveryDurabilityFailureWindowThroughIdentity(t *testing.T) {
	for _, test := range []struct {
		name        string
		point       publicCreateFaultPoint
		wantCreated bool
	}{
		{"failure before storage", failBeforePrepare, true},
		{"reservation committed but response lost", failAfterPrepare, true},
		{"accepted record committed but response lost", failAfterAdmit, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := openCreateIntegrationStore(t)
			fault := &oneShotPublicCreateFault{inner: store, point: test.point}
			first, principal := newCreateIntegrationService(t, store, fault, serviceNow, "runtime-command-first")
			second, _ := newCreateIntegrationService(t, store, store, serviceNow.Add(time.Hour), "runtime-command-second")
			req := createRequest("create-ambiguous", "session-ambiguous", smallBlocks)

			lost, acknowledged, err := first.AdmitCreate(context.Background(), principal, req)
			if err == nil || acknowledged || !reflect.DeepEqual(lost, sessionstore.DispositionInboxEntry{}) {
				t.Fatalf("failed call = (%+v, %v, %v), want zero/unacknowledged/error", lost, acknowledged, err)
			}
			if !errors.Is(err, errInjectedFault) {
				t.Fatalf("failed call error = %v, want injected storage cause", err)
			}
			retry, created, err := second.AdmitCreate(context.Background(), principal, req)
			if err != nil {
				t.Fatalf("retry: %v", err)
			}
			if created != test.wantCreated {
				t.Fatalf("retry created = %v, want %v for this durability window", created, test.wantCreated)
			}
			wantAccepted := serviceNow.Add(time.Hour)
			wantRuntime := sessionstore.RuntimeCommandID("runtime-command-second")
			if test.point != failBeforePrepare {
				wantAccepted = serviceNow
				wantRuntime = "runtime-command-first"
			}
			if !retry.Record.AcceptedAt.Equal(wantAccepted) || !retry.Record.ApplyDeadline.Equal(wantAccepted.Add(time.Minute)) {
				t.Fatalf("retry timestamps = %v/%v, want %v/%v", retry.Record.AcceptedAt, retry.Record.ApplyDeadline, wantAccepted, wantAccepted.Add(time.Minute))
			}
			if retry.Record.Descriptor.RuntimeCommandID != wantRuntime || retry.AcceptedOrder != 1 {
				t.Fatalf("retry mapping/order = %q/%d, want %q/1", retry.Record.Descriptor.RuntimeCommandID, retry.AcceptedOrder, wantRuntime)
			}
		})
	}
}

func TestClaimingAFactoryAcceptedCreateDoesNotExtendItsApplyDeadline(t *testing.T) {
	store := openCreateIntegrationStore(t)
	svc, principal := newCreateIntegrationService(t, store, store, serviceNow, "runtime-command-claim")
	accepted, created, err := svc.AdmitCreate(context.Background(), principal, createRequest("create-claim", "session-claim", smallBlocks))
	if err != nil || !created {
		t.Fatalf("AdmitCreate = (%v, %v)", created, err)
	}
	grant, err := store.AcquireResidency(context.Background(), sessionstore.AcquireResidencyRequest{
		TenantID: principal.Tenant(), SessionID: "session-claim",
	})
	if err != nil {
		t.Fatalf("AcquireResidency: %v", err)
	}
	t.Cleanup(func() {
		if err := grant.Release(context.Background()); err != nil {
			t.Errorf("Release: %v", err)
		}
	})
	claimed, changed, err := store.ClaimDispositionCommand(context.Background(), sessionstore.ClaimDispositionCommandRequest{
		TenantID: principal.Tenant(), SessionID: "session-claim", CommandID: "create-claim",
		ExpectedRevision: accepted.Revision, Residency: grant, ClaimExpiresAt: serviceNow.Add(30 * time.Second),
	})
	if err != nil || !changed {
		t.Fatalf("ClaimDispositionCommand = (%v, %v)", changed, err)
	}
	if !claimed.Record.ApplyDeadline.Equal(accepted.Record.ApplyDeadline) {
		t.Fatalf("claim changed ApplyDeadline from %v to %v", accepted.Record.ApplyDeadline, claimed.Record.ApplyDeadline)
	}
}
