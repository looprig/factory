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
	"github.com/looprig/storage"
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

// TestServiceRefusesLegacyCreateWithoutPersistingASession proves absence by
// SCANNING, not by a lookup at one guessed id: a refusal that wrote a session
// under any id would pass a probe of `generated-1`.
//
// Two scans, over what the store exposes:
//   - sessionstore.Store.ListSessions for the principal's tenant, which walks
//     the whole catalog ranking scope, so a catalog row under ANY session id
//     is seen (a reader that skips an unreadable row still counts it in
//     UnreadableSkipped, which is asserted too);
//   - the backend's storage.KV.Keys("") and storage.Blobs.List("") before and
//     after the call, so a key or object written anywhere in those two
//     primitives is seen.
//
// Neither storage.Ledger nor storage.OrderedIndex can enumerate its names or
// namespaces, so a write confined to a ledger, or to an ordered namespace other
// than the catalog, is outside what this scan can see. Admission's only durable
// writer for a session is the catalog, which the first scan covers.
//
// The minted-IDs assertion reads the Service's OWN id source; the fixture's is
// not wired into this Service.
func TestServiceRefusesLegacyCreateWithoutPersistingASession(t *testing.T) {
	ctx := context.Background()
	backend := memstore.New()
	store, err := sessionstore.Open(ctx, backend, sessionstore.WithClock(serviceClock{serviceNow}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(context.Background()); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	before := backendKeys(t, backend)

	f := newServiceFixture(t)
	ids := &serviceIDs{}
	f.service, err = NewService(Config{
		Authorizer: f.auth, Targets: f.targets, Catalog: store, Commands: store,
		Directory: f.directory, Clock: serviceClock{serviceNow}, IDs: ids, ApplyDeadline: time.Minute,
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
	if ids.next != 0 || len(ids.called) != 0 {
		t.Fatalf("the Service's own id source minted %d ids (called %v), want none", ids.next, ids.called)
	}
	if n, skipped := tenantSessions(t, store, f.principal.Tenant()); n != 0 || skipped != 0 {
		t.Fatalf("ListSessions after the refusal = %d sessions, %d unreadable; want none", n, skipped)
	}
	if after := backendKeys(t, backend); !reflect.DeepEqual(before, after) {
		t.Fatalf("backend keys changed across the refusal:\nbefore %v\nafter  %v", before, after)
	}

	// The positive control, on the same store and tenant: a V1 create DOES
	// appear in the scan. Without it an empty listing could be a scan that
	// sees nothing at all.
	control, _ := newCreateIntegrationService(t, store, store, serviceNow, "runtime-command-control")
	if _, created, err := control.AdmitCreate(ctx, f.principal, createRequest("create-control", "session-control", smallBlocks)); err != nil || !created {
		t.Fatalf("control AdmitCreate = (%v, %v)", created, err)
	}
	if n, _ := tenantSessions(t, store, f.principal.Tenant()); n != 1 {
		t.Fatalf("ListSessions after a real create = %d sessions, want 1; the scan cannot see a write", n)
	}
}

// tenantSessions walks every ListSessions page for one tenant and reports the
// sessions listed and the rows the store could not read.
func tenantSessions(t *testing.T, store *sessionstore.Store, tenant sessionwire.TenantID) (sessions, unreadable int) {
	t.Helper()
	var cursor sessionwire.Cursor
	for {
		page, err := store.ListSessions(context.Background(), sessionstore.ListSessionsRequest{TenantID: tenant, Cursor: cursor})
		if err != nil {
			t.Fatalf("ListSessions: %v", err)
		}
		sessions += len(page.Sessions)
		unreadable += page.UnreadableSkipped
		if page.NextCursor == "" {
			return sessions, unreadable
		}
		cursor = page.NextCursor
	}
}

// backendKeys is every KV key and blob name the backend holds.
func backendKeys(t *testing.T, backend *storage.Composite) map[string][]string {
	t.Helper()
	kv, err := backend.KV.Keys(context.Background(), "")
	if err != nil {
		t.Fatalf("KV.Keys: %v", err)
	}
	blobs, err := backend.Blobs.List(context.Background(), "")
	if err != nil {
		t.Fatalf("Blobs.List: %v", err)
	}
	return map[string][]string{"kv": kv, "blobs": blobs}
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
