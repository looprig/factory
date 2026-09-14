package factory

import (
	"context"
	"io"
	"io/fs"
	"net/http"
	"slices"
	"testing/fstest"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	"github.com/looprig/sessionstore"
)

// FakeSeams satisfies every composition seam with nothing behind it. A0.2
// composes; it does not serve, so a seam here answers zero values. It is
// declared in a test file so it is reachable from this package's external test
// package without becoming part of Factory's API.
type FakeSeams struct{}

func (FakeSeams) AuthorizeSessionList(context.Context, identity.Principal) error { return nil }

func (FakeSeams) AuthorizeSessionRead(context.Context, identity.Principal, sessionwire.SessionID) error {
	return nil
}

func (FakeSeams) AuthorizeObjectRead(context.Context, identity.Principal, sessionwire.SessionID, sessionwire.ObjectReference) error {
	return nil
}

func (FakeSeams) AuthorizeControl(context.Context, identity.Principal, sessionwire.SessionID, sessionstore.CommandKind) error {
	return nil
}

func (FakeSeams) AuthorizeSubscribe(context.Context, identity.Principal, string) error { return nil }

func (FakeSeams) AuthorizeServiceSweep(context.Context, identity.Principal) error { return nil }

func (FakeSeams) ListSessions(context.Context, sessionstore.ListSessionsRequest) (sessionstore.SessionPage, error) {
	return sessionstore.SessionPage{}, nil
}

func (FakeSeams) GetCatalogEntry(context.Context, sessionstore.GetCatalogEntryRequest) (sessionstore.CatalogEntry, error) {
	return sessionstore.CatalogEntry{}, nil
}

func (FakeSeams) ReadPublicJournal(context.Context, sessionstore.ReadPublicJournalRequest) (sessionwire.JournalPage, error) {
	return sessionwire.JournalPage{}, nil
}

func (FakeSeams) ReadGates(context.Context, sessionstore.ReadGatesRequest) (sessionwire.GatePage, error) {
	return sessionwire.GatePage{}, nil
}

func (FakeSeams) GetObjectMetadata(context.Context, sessionstore.GetObjectMetadataRequest) (sessionwire.ObjectMetadata, error) {
	return sessionwire.ObjectMetadata{}, nil
}
func (FakeSeams) GetObject(context.Context, sessionstore.GetObjectRequest) (io.ReadCloser, error) {
	return nil, nil
}

// ControlShards is the store's persisted shard count. It answers the
// SessionStore minimum rather than zero: a sweeper refuses to run against a
// store reporting fewer shards than the minimum, so a zero here would make
// every composition test exercise the refusal path instead of the composition.
func (FakeSeams) ControlShards() int { return sessionstore.MinControlShards }

func (FakeSeams) CreateCatalogEntry(context.Context, sessionstore.CreateCatalogEntryRequest) (sessionstore.CatalogEntry, bool, error) {
	return sessionstore.CatalogEntry{}, false, nil
}

func (FakeSeams) UpdateCatalogDesiredState(context.Context, sessionstore.UpdateCatalogDesiredStateRequest) (sessionstore.CatalogEntry, error) {
	return sessionstore.CatalogEntry{}, nil
}

func (FakeSeams) ListDueGates(context.Context, sessionstore.ListDueGatesRequest) (sessionstore.DueGatePage, error) {
	return sessionstore.DueGatePage{}, nil
}

func (FakeSeams) RetireGateDeadlineIntent(context.Context, sessionstore.RetireGateDeadlineIntentRequest) error {
	return nil
}

func (FakeSeams) ReconcileHostTargets(context.Context, sessionstore.ReconcileHostTargetsRequest) (sessionstore.HostTargetReconcileResult, error) {
	return sessionstore.HostTargetReconcileResult{}, nil
}

// ServiceToken is the HostLink credential. It answers a fixed value rather
// than an error so that a composition test exercises composition; a dial that
// reached a real Host is not something any test here does.
func (FakeSeams) ServiceToken(context.Context) (string, error) { return "service-token", nil }

func (FakeSeams) AdmitCommand(context.Context, sessionstore.AdmitCommandRequest) (sessionstore.InboxEntry, bool, error) {
	return sessionstore.InboxEntry{}, false, nil
}

func (FakeSeams) GetCommand(context.Context, sessionstore.GetCommandRequest) (sessionstore.InboxEntry, error) {
	return sessionstore.InboxEntry{}, nil
}

func (FakeSeams) RejectCommand(context.Context, sessionstore.RejectCommandRequest) (sessionstore.InboxEntry, error) {
	return sessionstore.InboxEntry{}, nil
}

func (FakeSeams) ListDueCommands(context.Context, sessionstore.ListDueCommandsRequest) (sessionstore.DueCommandPage, error) {
	return sessionstore.DueCommandPage{}, nil
}

func (FakeSeams) AcquireReconciliationClaim(context.Context, sessionstore.AcquireReconciliationClaimRequest) (sessionstore.ReconciliationClaimEntry, error) {
	return sessionstore.ReconciliationClaimEntry{}, nil
}

func (FakeSeams) ReleaseReconciliationClaim(context.Context, sessionstore.ReleaseReconciliationClaimRequest) (sessionstore.ReconciliationClaimEntry, error) {
	return sessionstore.ReconciliationClaimEntry{}, nil
}

func (FakeSeams) Owner(context.Context, sessionwire.TenantID, sessionwire.SessionID) (sessionwire.HostLinkRegistryObservation, bool, error) {
	return sessionwire.HostLinkRegistryObservation{}, false, nil
}

func (FakeSeams) Candidates(context.Context, sessionstore.ListCompatibleHostsRequest) (sessionstore.HostTargetPage, error) {
	return sessionstore.HostTargetPage{}, nil
}

func (FakeSeams) EnsurePlacement(context.Context, sessionstore.DesiredWorkload) error { return nil }

func (FakeSeams) ReleasePlacement(context.Context, sessionwire.TenantID, sessionwire.SessionID) error {
	return nil
}

// FakeTenant and FakeCredentialValue are what FakeVerifier accepts and asserts.
// They are exported so the external test package can build a request the
// composed authenticator really verifies.
const (
	FakeTenant  = "tenant-a"
	FakeSubject = "subject-a"

	// FakeCredentialValue is the ONE credential value FakeVerifier accepts.
	// Accepting exactly one, rather than everything, is what lets a test tell
	// "the composition authenticated" apart from "the composition did not
	// authenticate at all".
	FakeCredentialValue = "composed-credential"
)

// FakeVerifier is the credential verifier a composed Server authenticates
// through. It is a real seam implementation rather than a stub that returns a
// zero Principal: the authenticator, the operation context and the tenant a
// bootstrap response carries are all production code driven by these claims.
type FakeVerifier struct{}

func (FakeVerifier) VerifyCredential(_ context.Context, credential identity.Credential) (identity.Claims, error) {
	if credential.Value() != FakeCredentialValue {
		return identity.Claims{}, identity.ErrUnauthenticated
	}
	return identity.Claims{
		Tenant:  FakeTenant,
		Subject: FakeSubject,
		Kind:    identity.KindActor,
		// The composed Server uses the system clock unless a test replaces it,
		// so the expiry is relative to the wall clock rather than a fixture
		// instant. An hour is far longer than any test run and does not depend
		// on how loaded the machine is.
		ExpiresAt: time.Now().Add(time.Hour),
	}, nil
}

// FakeClock is a Clock that never fires, for compositions that only need the
// seam to be present.
type FakeClock struct{}

func (FakeClock) Now() time.Time { return time.Unix(0, 0).UTC() }

func (FakeClock) AfterFunc(time.Duration, func()) func() bool { return func() bool { return false } }

// FakeUUID is the one identifier FakeUUIDs returns.
const FakeUUID = "00000000-0000-4000-8000-000000000000"

// FakeUUIDs is a UUIDSource returning one fixed identifier.
type FakeUUIDs struct{}

func (FakeUUIDs) NewUUID() (string, error) { return FakeUUID, nil }

// FakeServiceIdentity is the service principal the composed sweeps run as.
func FakeServiceIdentity() identity.Principal {
	principal, err := identity.NewPrincipal(FakeTenant, "factory-sweeper", identity.KindService)
	if err != nil {
		panic("FakeServiceIdentity: " + err.Error())
	}
	return principal
}

// ValidCSRF is a CSRF configuration that validates.
func ValidCSRF() identity.CSRFConfig {
	key := make([]byte, identity.MinCSRFSharedKeyBytes)
	for i := range key {
		key[i] = byte('a' + i%26)
	}
	return identity.CSRFConfig{
		SharedKey:      key,
		TokenTTL:       time.Hour,
		TrustedOrigins: []string{"https://app.example.com"},
	}
}

// RequiredOptions is the smallest composition New accepts: every seam that has
// no default, and nothing else. Every negative case in this package mutates it
// by exactly one option.
func RequiredOptions() []Option {
	seams := FakeSeams{}
	return []Option{
		WithCredentialVerifier(FakeVerifier{}),
		WithAuthorizer(seams),
		WithSessionReader(seams),
		WithCommands(seams),
		WithDirectory(seams),
		WithPlacementController(seams),
		WithCatalog(seams),
		WithGates(seams),
		WithHostTargets(seams),
		WithHostLinkCredential(seams),
		WithServiceIdentity(FakeServiceIdentity()),
		WithReplicaID("replica-a"),
		WithCSRF(ValidCSRF()),
	}
}

// RequiredOptionsExcept is RequiredOptions with the named options removed, so a
// test outside this package can replace one composed seam with a recording or
// refusing one. Option.name is unexported, which is why this lives here.
//
// It fails loudly on a name it did not remove, because a typo would otherwise
// produce a composition carrying BOTH the fake seam and the test's own, which
// New rejects as a duplicate -- a confusing failure a long way from its cause.
func RequiredOptionsExcept(names ...string) []Option {
	kept := make([]Option, 0, len(RequiredOptions()))
	removed := map[string]bool{}
	for _, opt := range RequiredOptions() {
		if slices.Contains(names, opt.name) {
			removed[opt.name] = true
			continue
		}
		kept = append(kept, opt)
	}
	for _, name := range names {
		if !removed[name] {
			panic("RequiredOptionsExcept: " + name + " is not a required option")
		}
	}
	return kept
}

// stubHandler and stubFS are the two UI shapes, for cases that need a non-nil
// UI without caring what it serves.
func stubHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ui")) })
}

func stubFS() fs.FS {
	return fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("ui")}}
}
