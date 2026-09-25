package factory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/placement"
	"github.com/looprig/factory/internal/routing"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage/memstore"
)

type holderClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *holderClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}
func (c *holderClock) AfterFunc(time.Duration, func()) func() bool {
	return func() bool { return true }
}
func (c *holderClock) set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}

// duringAttach is a Host whose attach runs a hook -- the point at which this
// replica's placement holds the session's reconciliation claim -- and then
// accepts.
type duringAttach struct{ during func() }

func (l duringAttach) Attach(_ context.Context, _ sessionwire.InternalEndpoint, req sessionwire.HostLinkAttachRequest) (sessionwire.HostLinkRegistryObservation, error) {
	l.during()
	return sessionwire.HostLinkRegistryObservation{
		Version: sessionwire.CurrentWireVersion, TenantID: req.TenantID, SessionID: req.SessionID,
		HostID: req.HostID, HostGeneration: req.HostGeneration, AgentID: req.AgentID,
		RuntimeCompatibilityID: req.RuntimeCompatibilityID, Placement: sessionwire.HostPlacementPooled,
		InternalEndpoint: "wss://host-a.internal/attached", Residency: sessionwire.SessionResidencyResident,
		Accepting: true, LeaseEpoch: 1,
	}, nil
}
func (duringAttach) Bind(context.Context, sessionwire.InternalEndpoint, sessionwire.HostLinkBindRequest) error {
	return nil
}
func (duringAttach) Unbind(context.Context, sessionwire.HostLinkUnbindRequest) error { return nil }
func (duringAttach) DeliverCommand(context.Context, sessionwire.TenantID, sessionwire.SessionID, sessionwire.HostLinkCommandDelivery) error {
	return nil
}
func (duringAttach) AcceptsGateResponses(context.Context, sessionwire.HostLinkRegistryObservation) (bool, error) {
	return false, nil
}

func (duringAttach) AcceptsCommandPrincipal(context.Context, sessionwire.HostLinkRegistryObservation) (bool, error) {
	return true, nil
}
func (duringAttach) RouteFor(sessionwire.TenantID, sessionwire.SessionID) (sessionwire.HostID, bool) {
	return "", false
}

// TestTheDispositionSweepDefersToThisReplicasPlacementClaim is the B5 v0.3.0
// quality gate's I-E, committed (N1). This replica's placement holds a
// session's reconciliation claim and is mid-attach; the COMPOSED disposition
// sweep of the same replica meets an expired command on that session. Under
// the shared replica id it "acquired" placement's claim as an extension,
// rejected, and RELEASED it, and another replica then took the claim while the
// attach was still in flight. Under its own holder it defers, and the claim
// stays placement's until placement gives it back.
func TestTheDispositionSweepDefersToThisReplicasPlacementClaim(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 9, 19, 9, 0, 0, 0, time.UTC)
	clock := &holderClock{now: start}
	store, err := sessionstore.Open(context.Background(), memstore.New(), sessionstore.WithClock(clock), sessionstore.WithControlShards(1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close(context.Background()) })
	server, err := New(append(RequiredOptionsExcept("WithCommands", "WithCatalog"),
		WithCommands(store), WithCatalog(store), WithClock(clock))...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	tenant := FakeServiceIdentity().Tenant()
	const session = sessionwire.SessionID("session-shared-claim")
	key := sessionstore.HostTargetKey{AgentID: "agent-a", RuntimeCompatibilityID: "runtime-v1", Placement: sessionwire.HostPlacementPooled}
	payload := []byte(`{"blocks":[]}`)
	digest := sha256.Sum256(payload)
	create := sessionstore.PublicCreateIdentity{
		TenantID: tenant, SessionID: session, CommandID: "create-1", Target: key,
		Binding: sessionstore.SessionBinding{StorageBindingID: "storage-a", BindingVersion: "v1",
			RuntimeSessionID: "0190a3c4-0000-8000-8000-0000000000aa", ProtocolMode: sessionstore.ProtocolModeDisposition},
		Kind: "create", PayloadDigest: hex.EncodeToString(digest[:]), PayloadSize: uint64(len(payload)),
	}
	if _, err := store.PreparePublicCreate(ctx, sessionstore.PreparePublicCreateRequest{
		Identity: create, ProposedRuntimeCommandID: "runtime-create", AcceptedAt: start, ApplyDeadline: start.Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.AdmitPublicCreate(ctx, sessionstore.AdmitPublicCreateRequest{Identity: create, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	clock.set(start.Add(2 * time.Minute)) // the create is expired
	if _, err := store.PublishHostTarget(ctx, sessionstore.PublishHostTargetRequest{
		Key: key, HostID: "host-a", HostGeneration: 1, ObservedAt: clock.Now(),
		Advertisement: sessionstore.HostAdvertisement{InternalEndpoint: "wss://host-a.internal",
			IsolationClass: sessionwire.HostIsolationClassCrossTenantIsolated, Accepting: true, AvailableCapacity: 4,
			ExpiresAt: clock.Now().Add(time.Minute)},
	}); err != nil {
		t.Fatal(err)
	}
	directory, err := routing.NewDirectory(store, routing.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}

	var (
		swept       bool
		rejected    int
		otherClaim  error
		stateDuring sessionstore.InboxState
	)
	links := duringAttach{during: func() {
		result, err := server.components.dispositionSweeper.Sweep(ctx, server.cfg.service)
		if err != nil {
			t.Errorf("disposition sweep: %v", err)
		}
		swept, rejected = true, result.Rejected
		entry, err := store.GetDispositionCommand(ctx, sessionstore.GetDispositionCommandRequest{TenantID: tenant, SessionID: session, CommandID: "create-1"})
		if err == nil {
			stateDuring = entry.Record.State
		}
		_, otherClaim = store.AcquireReconciliationClaim(ctx, sessionstore.AcquireReconciliationClaimRequest{
			TenantID: tenant, SessionID: session, HolderID: "replica-b", ExpiresAt: clock.Now().Add(time.Minute),
		})
	}}
	placer, err := placement.NewReconciler(placement.Config{
		Directory: directory, Catalog: store, Claims: store, Clock: clock,
		HolderID: server.cfg.replicaID, ClaimTTL: time.Minute, CandidateLimit: 8, Links: links, ActorID: "factory-service",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := placer.Reconcile(ctx, placement.Request{TenantID: tenant, SessionID: session}); err != nil {
		t.Fatalf("placement: %v", err)
	}
	if !swept {
		t.Fatal("the attach never ran, so the interleaving was not constructed")
	}
	if otherClaim == nil {
		t.Fatal("another replica took the claim while this replica's attach was in flight: the disposition sweep released placement's claim")
	}
	if rejected != 0 || stateDuring != sessionstore.InboxStatePending {
		t.Fatalf("the disposition sweep rejected %d (state %q) under placement's live claim, want it to defer", rejected, stateDuring)
	}
}

// TestTheDispositionHolderIsDistinctAndAlwaysStorable pins the derivation: a
// holder distinct from the replica's own, and one the store accepts even for
// a replica id at the id ceiling.
func TestTheDispositionHolderIsDistinctAndAlwaysStorable(t *testing.T) {
	t.Parallel()

	if got := dispositionHolder("replica-a"); got != "replica-a/dispositions" {
		t.Fatalf("dispositionHolder(replica-a) = %q", got)
	}
	// G1: the boundary itself. An id that exactly fits keeps its plain form
	// at exactly MaxIDBytes; one byte more is hashed. A bound one over would
	// produce a 257-byte holder the store refuses on every claim.
	const suffix = "/dispositions"
	fits := strings.Repeat("f", sessionwire.MaxIDBytes-len(suffix))
	if got := dispositionHolder(fits); got != fits+suffix || len(got) != sessionwire.MaxIDBytes {
		t.Fatalf("dispositionHolder(%d bytes) = %d bytes, want the plain form at exactly %d", len(fits), len(got), sessionwire.MaxIDBytes)
	}
	over := fits + "f"
	if got := dispositionHolder(over); got == over+suffix || len(got) > sessionwire.MaxIDBytes || !strings.HasSuffix(got, suffix) {
		t.Fatalf("dispositionHolder(%d bytes) = %q (%d bytes), want it hashed within %d", len(over), got, len(got), sessionwire.MaxIDBytes)
	}
	long := strings.Repeat("r", sessionwire.MaxIDBytes)
	got := dispositionHolder(long)
	if len(got) > sessionwire.MaxIDBytes || got == long || !strings.HasSuffix(got, "/dispositions") || got != dispositionHolder(long) {
		t.Fatalf("dispositionHolder(%d bytes) = %q (%d bytes), want a stable, distinct id within %d bytes", len(long), got, len(got), sessionwire.MaxIDBytes)
	}
}

// TestTheLegacyCommandSweepClaimsUnderAHolderOfItsOwn is the v0.3.0 regate's
// L1: the N1 pattern, closed for the legacy sweep before placement can ever
// reconcile a legacy session. Its holder is stable, distinct from the replica's
// and from the disposition sweep's, and always storable.
func TestTheLegacyCommandSweepClaimsUnderAHolderOfItsOwn(t *testing.T) {
	t.Parallel()

	if got := commandHolder("replica-a"); got != "replica-a/commands" {
		t.Fatalf("commandHolder(replica-a) = %q", got)
	}
	long := strings.Repeat("r", sessionwire.MaxIDBytes)
	got := commandHolder(long)
	if len(got) > sessionwire.MaxIDBytes || got == dispositionHolder(long) || got != commandHolder(long) {
		t.Fatalf("commandHolder(%d bytes) = %q, want a stable holder distinct from the disposition sweep's", len(long), got)
	}
}
