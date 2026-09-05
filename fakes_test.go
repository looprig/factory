package factory

import (
	"context"
	"io"
	"io/fs"
	"net/http"
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

func (FakeSeams) AuthenticateRequest(context.Context, *http.Request) (identity.Principal, error) {
	return identity.Principal{}, nil
}

func (FakeSeams) AuthenticateLink(context.Context, string) (identity.Principal, error) {
	return identity.Principal{}, nil
}

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

// FakeClock is a Clock that never fires, for compositions that only need the
// seam to be present.
type FakeClock struct{}

func (FakeClock) Now() time.Time { return time.Unix(0, 0).UTC() }

func (FakeClock) AfterFunc(time.Duration, func()) func() bool { return func() bool { return false } }

// FakeUUIDs is a UUIDSource returning one fixed identifier.
type FakeUUIDs struct{}

func (FakeUUIDs) NewUUID() (string, error) { return "00000000-0000-4000-8000-000000000000", nil }

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
		WithAuthenticator(seams),
		WithAuthorizer(seams),
		WithSessionReader(seams),
		WithCommands(seams),
		WithDirectory(seams),
		WithPlacementController(seams),
		WithCSRF(ValidCSRF()),
	}
}

// stubHandler and stubFS are the two UI shapes, for cases that need a non-nil
// UI without caring what it serves.
func stubHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ui")) })
}

func stubFS() fs.FS {
	return fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("ui")}}
}
