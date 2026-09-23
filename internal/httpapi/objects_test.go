package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	internalidentity "github.com/looprig/factory/internal/identity"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

func TestObjectEndpointsRequireReferencePolicy(t *testing.T) {
	for _, suffix := range []string{"", "/metadata"} {
		f := newFixture(t)
		got := f.get("/v1/sessions/" + string(fixtureSession) + "/objects/object-a" + suffix)
		if got.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s: status %d, want fail-closed 503; %s", suffix, got.Code, got.Body)
		}
	}
}

// The blob observer delegates to real storage, replacing only reads after the
// object has been persisted and verified. This exercises SessionStore's own
// integrity and lifecycle, not a fake integrity error from a mocked store.
type observedObjectBlobs struct {
	storage.Blobs
	key         string
	replacement func(context.Context) (io.ReadCloser, error)
	gets        int
}

func (b *observedObjectBlobs) BlobReaderCloseBound() time.Duration { return time.Second }
func (b *observedObjectBlobs) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	b.key = key
	b.gets++
	if b.replacement != nil {
		return b.replacement(ctx)
	}
	return b.Blobs.Get(ctx, key)
}

func objectStore(t *testing.T, backend *storage.Composite, binding sessionstore.SessionBinding) *sessionstore.Store {
	t.Helper()
	s, err := sessionstore.Open(context.Background(), backend)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	record := coldRecord(sessionwire.SessionStateIdle, sessionwire.SessionResidencyCold)
	_, _, err = s.CreateCatalogEntry(context.Background(), sessionstore.CreateCatalogEntryRequest{TenantID: fixtureTenant, SessionID: fixtureSession, AgentID: record.AgentID, RuntimeCompatibilityID: record.RuntimeCompatibilityID, CreatedAt: record.CreatedAt, LastActiveAt: record.LastActiveAt, State: record.State, Residency: record.Residency, DesiredPlacement: record.DesiredPlacement, IdempotencyKey: "object-create", Binding: binding})
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func observedObjectFixture(t *testing.T, body string) (*fixture, sessionwire.ObjectMetadata, *observedObjectBlobs) {
	t.Helper()
	backend := memstore.New()
	blobs := &observedObjectBlobs{Blobs: backend.Blobs}
	backend.Blobs = blobs
	s := objectStore(t, backend, sessionstore.SessionBinding{})
	sum := sha256.Sum256([]byte(body))
	m, err := s.PutObject(context.Background(), sessionstore.PutObjectRequest{TenantID: fixtureTenant, SessionID: fixtureSession, Kind: sessionstore.ObjectKindToolResult, SizeBytes: uint64(len(body)), SHA256: sum, Body: strings.NewReader(body)})
	if err != nil {
		t.Fatal(err)
	}
	f := newFixture(t)
	f.router.reads = s
	f.router.objectPolicy = objectPolicyFunc(func(_ context.Context, p identity.Principal, e sessionstore.CatalogEntry, ref sessionwire.ObjectReference) (sessionstore.ObjectKind, error) {
		if p.Tenant() != fixtureTenant || e.Record.SessionID != fixtureSession || ref != m.Reference {
			return "", internalidentity.ErrUnauthorized
		}
		return sessionstore.ObjectKindToolResult, nil
	})
	blobs.gets = 0
	return f, m, blobs
}

func TestObjectCorruptionOutsideRequestedRangeNeverEmitsPage(t *testing.T) {
	for _, bad := range []string{"012345678X", "012345678", "01234567890"} {
		t.Run(bad, func(t *testing.T) {
			f, m, blobs := observedObjectFixture(t, "0123456789")
			blobs.replacement = func(context.Context) (io.ReadCloser, error) { return io.NopCloser(strings.NewReader(bad)), nil }
			req := request("GET", objectTarget(m), nil)
			req.Header.Set("Range", "bytes=0-1")
			got := f.serve(req)
			if got.Code != 500 || got.Header().Get("X-Object-Digest") != "" || got.Header().Get("Content-Range") != "" {
				t.Fatalf("corrupt page emitted: %d %v %s", got.Code, got.Header(), got.Body)
			}
			if blobs.gets != 1 {
				t.Fatalf("real blob reads: %d", blobs.gets)
			}
		})
	}
}

func TestObjectDeletedBlobMetadataSurvivesWithoutClaimingBytes(t *testing.T) {
	f, m, blobs := observedObjectFixture(t, "secret")
	if err := blobs.Blobs.Delete(context.Background(), blobs.key); err != nil {
		t.Fatal(err)
	}
	if got := f.get(objectTarget(m) + "/metadata"); got.Code != 200 {
		t.Fatalf("index metadata %d", got.Code)
	}
	if blobs.gets != 0 {
		t.Fatal("metadata opened a blob")
	}
	got := f.get(objectTarget(m))
	if got.Code != 404 || strings.Contains(got.Body.String(), blobs.key) {
		t.Fatalf("deleted %d %s", got.Code, got.Body)
	}
}

func TestObjectAuthPrecedesExistenceAndPolicyPrecedesMetadata(t *testing.T) {
	for _, suffix := range []string{"", "/metadata"} {
		denied := &recordingAuthorizer{deny: true}
		f := newFixture(t, withAuthorizer(denied))
		got := f.get("/v1/sessions/" + string(fixtureSession) + "/objects/object-a" + suffix)
		if got.Code != 403 {
			t.Fatal(got.Code)
		}
		calls, _ := f.reads.snapshot()
		if len(calls) != 0 {
			t.Fatal("denied object reached catalog")
		}
		for _, policy := range []struct {
			name       string
			err        error
			wantStatus int
			wantCode   sessionwire.ErrorCode
		}{
			{"public sentinel", identity.ErrUnauthorized, http.StatusNotFound, sessionwire.ErrorCodeInvalidRequest},
			{"wrapped public sentinel", fmt.Errorf("external object policy: %w", identity.ErrUnauthorized), http.StatusNotFound, sessionwire.ErrorCodeInvalidRequest},
			// Depth 2 and a join: a classifier that unwraps exactly once
			// passes the two rows above and fails both of these.
			{"doubly wrapped public sentinel", fmt.Errorf("external object policy: %w", fmt.Errorf("policy engine: %w", identity.ErrUnauthorized)), http.StatusNotFound, sessionwire.ErrorCodeInvalidRequest},
			{"joined public sentinel", errors.Join(errors.New("audit sink unavailable"), identity.ErrUnauthorized), http.StatusNotFound, sessionwire.ErrorCodeInvalidRequest},
			{"policy dependency fault", errors.New("policy backend failed"), http.StatusInternalServerError, ErrorCodeInternal},
		} {
			t.Run(suffix+"/"+policy.name, func(t *testing.T) {
				f, m, blobs := observedObjectFixture(t, "secret")
				f.router.objectPolicy = objectPolicyFunc(func(context.Context, identity.Principal, sessionstore.CatalogEntry, sessionwire.ObjectReference) (sessionstore.ObjectKind, error) {
					return "", policy.err
				})
				got := f.get(objectTarget(m) + suffix)
				if got.Code != policy.wantStatus || blobs.gets != 0 {
					t.Fatalf("policy refusal: status %d, reads %d; want status %d and no blob reads", got.Code, blobs.gets, policy.wantStatus)
				}
				if code := decodeEnvelope(t, got).Error.Code; code != policy.wantCode {
					t.Errorf("error code = %q, want %q", code, policy.wantCode)
				}
				// A denial is ABSENCE (v0.11.0): the same bytes as a reference
				// the store does not hold, so "not yours" and "not there"
				// cannot be told apart.
				if policy.wantStatus == http.StatusNotFound {
					f.router.objectPolicy = objectPolicyFunc(func(context.Context, identity.Principal, sessionstore.CatalogEntry, sessionwire.ObjectReference) (sessionstore.ObjectKind, error) {
						return sessionstore.ObjectKindToolResult, nil
					})
					absent := f.get(strings.Replace(objectTarget(m), m.Reference.ObjectID[len(m.Reference.ObjectID)-8:], "00000000", 1) + suffix)
					if absent.Code != http.StatusNotFound || responseDifference(got, absent) != "" {
						t.Fatalf("a denial (%d %s) differs from an absent object (%d %s)", got.Code, got.Body, absent.Code, absent.Body)
					}
				}
			})
		}
	}
}

func TestObjectTenantAndSessionAreAuthenticatedScope(t *testing.T) {
	f, m, _ := observedObjectFixture(t, "secret")
	// A query tenant never changes the principal's authorized object scope.
	if got := f.get(objectTarget(m) + "?tenant_id=attacker&session_id=other"); got.Code != 200 || got.Body.String() != "secret" {
		t.Fatalf("query scope %d", got.Code)
	}
	for _, suffix := range []string{"", "/metadata"} {
		missing := f.get(strings.Replace(objectTarget(m), string(fixtureSession), "missing", 1) + suffix)
		// The real store's scoped missing response is the same regardless of ID.
		other := f.get(strings.Replace(objectTarget(m), string(fixtureSession), "other-tenant-session", 1) + suffix)
		if missing.Code != 404 || responseDifference(missing, other) != "" {
			t.Fatalf("absence %d/%d", missing.Code, other.Code)
		}
	}
}

func TestObjectExactFrozenBindingChoosesIndependentStore(t *testing.T) {
	f, m, agent := newObjectFixture(t, "agent-history")
	binding := sessionstore.SessionBinding{StorageBindingID: "agent-profile", BindingVersion: "immutable-v1", RuntimeSessionID: "runtime-session", ProtocolMode: sessionstore.ProtocolModeDisposition}
	orchestration := objectStore(t, memstore.New(), binding)
	f.router.reads = orchestration
	var seen []sessionstore.SessionBinding
	f.router.resolveObjectStore = func(_ context.Context, b sessionstore.SessionBinding) (ObjectReader, error) {
		seen = append(seen, b)
		if b != binding {
			return nil, errors.New("unknown config")
		}
		return agent, nil
	}
	for _, suffix := range []string{"", "/metadata"} {
		if got := f.get(objectTarget(m) + suffix); got.Code != 200 {
			t.Fatalf("bound read %d %s", got.Code, got.Body)
		}
	}
	if !reflect.DeepEqual(seen, []sessionstore.SessionBinding{binding, binding}) {
		t.Fatalf("bindings %+v", seen)
	}
	f.router.resolveObjectStore = nil
	if got := f.get(objectTarget(m)); got.Code != 503 {
		t.Fatalf("missing resolver fell back: %d", got.Code)
	}
	f.router.resolveObjectStore = func(context.Context, sessionstore.SessionBinding) (ObjectReader, error) {
		return nil, errors.New("s3://secret-provider?token=secret")
	}
	if got := f.get(objectTarget(m)); got.Code != 503 || strings.Contains(got.Body.String(), "secret") {
		t.Fatalf("unknown binding %d %s", got.Code, got.Body)
	}
	// A resolver selecting orchestration cannot find the object: no default
	// store contains bytes merely because the catalog does.
	f.router.resolveObjectStore = func(context.Context, sessionstore.SessionBinding) (ObjectReader, error) { return orchestration, nil }
	if got := f.get(objectTarget(m)); got.Code == 200 {
		t.Fatal("wrong store returned agent object")
	}
}

type objectCatalogOverride struct {
	SessionReader
	binding sessionstore.SessionBinding
}

func (s objectCatalogOverride) GetCatalogEntry(ctx context.Context, r sessionstore.GetCatalogEntryRequest) (sessionstore.CatalogEntry, error) {
	e, err := s.SessionReader.GetCatalogEntry(ctx, r)
	e.Record.Binding = s.binding
	return e, err
}

// The statuses are asserted exactly, not as a 500-or-503 disjunction: a partial
// binding is refused by the Summary canonicalization guard (500) before it can
// reach a production resolver, while a canonically valid binding with no
// configured resolver is refused by the fail-closed resolver check (503). A
// disjunction would let the canonicalization guard be removed silently.
func TestObjectPartialAndUnknownBindingsNeverFallBack(t *testing.T) {
	f, m, _ := newObjectFixture(t, "secret")
	original := f.router.reads
	for _, tc := range []struct {
		binding sessionstore.SessionBinding
		status  int
	}{
		{sessionstore.SessionBinding{ProtocolMode: sessionstore.ProtocolModeDisposition}, 500},
		{sessionstore.SessionBinding{ProtocolMode: sessionstore.ProtocolModeLegacy}, 500},
		{sessionstore.SessionBinding{StorageBindingID: "only-id"}, 500},
		{sessionstore.SessionBinding{StorageBindingID: "id", BindingVersion: "v1", RuntimeSessionID: "runtime", ProtocolMode: "unknown"}, 500},
		{sessionstore.SessionBinding{StorageBindingID: "id", BindingVersion: "v1", RuntimeSessionID: "runtime", ProtocolMode: sessionstore.ProtocolModeLegacy}, 503},
	} {
		f.router.reads = objectCatalogOverride{original, tc.binding}
		if got := f.get(objectTarget(m)); got.Code != tc.status {
			t.Fatalf("binding %+v: %d, want %d", tc.binding, got.Code, tc.status)
		}
	}
}

type blockedObjectRead struct {
	entered, closed chan struct{}
	once            sync.Once
}

func (b *blockedObjectRead) Read([]byte) (int, error) {
	b.once.Do(func() { close(b.entered) })
	<-b.closed
	return 0, context.Canceled
}
func (b *blockedObjectRead) Close() error {
	select {
	case <-b.closed:
	default:
		close(b.closed)
	}
	return nil
}

func TestObjectCancellationClosesBlockedRealStoreRead(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprint(deadline), func(t *testing.T) {
			f, m, blobs := observedObjectFixture(t, "blocked")
			blocked := &blockedObjectRead{entered: make(chan struct{}), closed: make(chan struct{})}
			blobs.replacement = func(context.Context) (io.ReadCloser, error) { return blocked, nil }
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if deadline {
				f.router.limits.RequestTimeout = 20 * time.Millisecond
			}
			req := request("GET", objectTarget(m), nil).WithContext(ctx)
			done := make(chan *httptest.ResponseRecorder, 1)
			go func() { done <- f.serve(req) }()
			select {
			case <-blocked.entered:
			case <-time.After(2 * time.Second):
				t.Fatal("read never entered")
			}
			if !deadline {
				cancel()
			}
			select {
			case got := <-done:
				want := 499
				if deadline {
					want = 504
				}
				if got.Code != want || got.Header().Get("X-Object-Digest") != "" {
					t.Fatalf("cancel %d %s", got.Code, got.Body)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("blocked Read not released by Close")
			}
			select {
			case <-blocked.closed:
			default:
				t.Fatal("reader not closed")
			}
		})
	}
}

func TestObjectBackendErrorsAreRedacted(t *testing.T) {
	f, m, blobs := observedObjectFixture(t, "secret")
	blobs.replacement = func(context.Context) (io.ReadCloser, error) {
		return nil, errors.New("s3://private/backend?token=password")
	}
	got := f.get(objectTarget(m))
	if got.Code != 503 || strings.Contains(got.Body.String(), "password") || strings.Contains(got.Body.String(), "s3:") {
		t.Fatalf("error %d %s", got.Code, got.Body)
	}
}

func TestObjectInvalidLogicalReferenceNeverBecomesBackendKey(t *testing.T) {
	f, _, blobs := observedObjectFixture(t, "secret")
	f.router.objectPolicy = objectPolicyFunc(func(context.Context, identity.Principal, sessionstore.CatalogEntry, sessionwire.ObjectReference) (sessionstore.ObjectKind, error) {
		return sessionstore.ObjectKindToolResult, nil
	})
	for _, id := range []string{"not-a-logical-id", "s3://bucket/key?token=secret", "v1:runtime-object:bad:bad", strings.Repeat("x", sessionwire.MaxIDBytes+1)} {
		got := f.get("/v1/sessions/" + string(fixtureSession) + "/objects/" + url.PathEscape(id))
		if (got.Code != 400 && !(strings.HasPrefix(id, "s3://") && got.Code == 404)) || blobs.gets != 0 {
			t.Fatalf("invalid %q %d reads %d", id, got.Code, blobs.gets)
		}
	}
}

func TestObjectLimitsRejectUnboundedConfiguration(t *testing.T) {
	for _, l := range []ObjectLimits{{0, 1}, {1, 0}, {1<<20 + 1, 64 << 20}, {1, 64<<20 + 1}, {3, 2}} {
		if l.Validate() == nil {
			t.Fatalf("accepted %+v", l)
		}
	}
	if err := DefaultObjectLimits().Validate(); err != nil {
		t.Fatal(err)
	}
}

type objectReadObserver struct {
	SessionReader
	metadataCalls, bodyCalls int
	metadata                 func(context.Context, sessionstore.GetObjectMetadataRequest) (sessionwire.ObjectMetadata, error)
	body                     func(context.Context, sessionstore.GetObjectRequest) (io.ReadCloser, error)
}

func (o *objectReadObserver) GetObjectMetadata(ctx context.Context, r sessionstore.GetObjectMetadataRequest) (sessionwire.ObjectMetadata, error) {
	o.metadataCalls++
	if o.metadata != nil {
		return o.metadata(ctx, r)
	}
	return o.SessionReader.GetObjectMetadata(ctx, r)
}
func (o *objectReadObserver) GetObject(ctx context.Context, r sessionstore.GetObjectRequest) (io.ReadCloser, error) {
	o.bodyCalls++
	if o.body != nil {
		return o.body(ctx, r)
	}
	return o.SessionReader.GetObject(ctx, r)
}

func TestObjectPolicyIsNotMetadataExistenceOrCallerKind(t *testing.T) {
	f, m, _ := newObjectFixture(t, "secret")
	observer := &objectReadObserver{SessionReader: f.router.reads}
	f.router.reads = observer
	f.router.objectPolicy = objectPolicyFunc(func(context.Context, identity.Principal, sessionstore.CatalogEntry, sessionwire.ObjectReference) (sessionstore.ObjectKind, error) {
		return "", internalidentity.ErrUnauthorized
	})
	for _, suffix := range []string{"", "/metadata"} {
		if got := f.get(objectTarget(m) + suffix); got.Code != 404 {
			t.Fatal(got.Code)
		}
	}
	if observer.metadataCalls != 0 || observer.bodyCalls != 0 {
		t.Fatalf("denied policy reached storage: %+v", observer)
	}
	// The route serves tool-result only. A policy answering any other kind is
	// a policy fault, refused before any store is asked -- so a logical
	// tool-result ID can neither select its own kind nor be read as another.
	for _, kind := range []sessionstore.ObjectKind{sessionstore.ObjectKindArtifact, sessionstore.ObjectKindCommandPayload, ""} {
		f.router.objectPolicy = objectPolicyFunc(func(context.Context, identity.Principal, sessionstore.CatalogEntry, sessionwire.ObjectReference) (sessionstore.ObjectKind, error) {
			return kind, nil
		})
		if got := f.get(objectTarget(m)); got.Code != 500 {
			t.Fatalf("%q: a kind the route does not serve answered %d", kind, got.Code)
		}
	}
	if observer.metadataCalls != 0 || observer.bodyCalls != 0 {
		t.Fatalf("a kind the route does not serve reached storage: %+v", observer)
	}
}

// fakeObjectStream is a resolved reader that is NOT a sessionstore.Store: the
// ObjectReader contract only *requests* whole-object integrity reporting at EOF,
// so an A9-supplied adapter may simply not verify. Every other corruption test
// injects beneath a real store, which verifies for itself; these drive Factory's
// own digest and size checks directly.
type fakeObjectStream struct {
	io.Reader
	closed bool
}

func (s *fakeObjectStream) Close() error { s.closed = true; return nil }

func TestObjectVerificationRejectsANonVerifyingResolvedReader(t *testing.T) {
	for _, tc := range []struct{ name, stream, leak string }{
		{"wrong-bytes", "XXXXXXXXXX", "XX"},
		{"short", "01234", "01"},
		{"long", "0123456789extra", "01"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, span := range []string{"", "bytes=0-1"} {
				f, m, _ := newObjectFixture(t, "0123456789")
				stream := &fakeObjectStream{Reader: strings.NewReader(tc.stream)}
				f.router.reads = &objectReadObserver{SessionReader: f.router.reads, body: func(context.Context, sessionstore.GetObjectRequest) (io.ReadCloser, error) { return stream, nil }}
				req := request("GET", objectTarget(m), nil)
				if span != "" {
					req.Header.Set("Range", span)
				}
				got := f.serve(req)
				if got.Code != 500 {
					t.Fatalf("%q: unverified stream served %d: %s", span, got.Code, got.Body)
				}
				if got.Header().Get("X-Object-Digest") != "" || got.Header().Get("X-Object-Size") != "" || got.Header().Get("Content-Range") != "" {
					t.Fatalf("%q: integrity headers on a rejected page: %v", span, got.Header())
				}
				if strings.Contains(got.Body.String(), tc.leak) {
					t.Fatalf("%q: unverified bytes emitted: %s", span, got.Body)
				}
				if !stream.closed {
					t.Fatalf("%q: rejected stream not closed", span)
				}
			}
		})
	}
}

// A resolved reader that answers with a different object's metadata, or with
// metadata the wire contract rejects, must not become a served response: the
// reference the caller was authorized for is the only one that may be answered.
func TestObjectResolvedMetadataMustValidateAndMatchTheAuthorizedReference(t *testing.T) {
	f, m, store := newObjectFixture(t, "0123456789")
	other, err := store.PutObject(context.Background(), sessionstore.PutObjectRequest{TenantID: fixtureTenant, SessionID: fixtureSession, Kind: sessionstore.ObjectKindToolResult, SizeBytes: uint64(len("other-object-body")), SHA256: sha256.Sum256([]byte("other-object-body")), Body: strings.NewReader("other-object-body"), MediaType: "text/plain"})
	if err != nil {
		t.Fatal(err)
	}
	if other.Reference == m.Reference {
		t.Fatal("fixture objects share a reference")
	}
	observer := &objectReadObserver{SessionReader: f.router.reads}
	f.router.reads = observer
	for _, tc := range []struct {
		name string
		meta sessionwire.ObjectMetadata
	}{
		{"other-object", other},
		{"invalid", sessionwire.ObjectMetadata{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observer.metadata = func(context.Context, sessionstore.GetObjectMetadataRequest) (sessionwire.ObjectMetadata, error) {
				return tc.meta, nil
			}
			observer.bodyCalls = 0
			for _, suffix := range []string{"", "/metadata"} {
				got := f.get(objectTarget(m) + suffix)
				if got.Code != 500 {
					t.Fatalf("%s: mismatched metadata served %d: %s", suffix, got.Code, got.Body)
				}
				if strings.Contains(got.Body.String(), string(tc.meta.Reference.ObjectID)) && tc.meta.Reference.ObjectID != "" {
					t.Fatalf("%s: another object's reference leaked: %s", suffix, got.Body)
				}
				if strings.Contains(got.Body.String(), "other-object-body") {
					t.Fatalf("%s: another object's bytes leaked: %s", suffix, got.Body)
				}
			}
			if observer.bodyCalls != 0 {
				t.Fatalf("%s: mismatched metadata opened the body", tc.name)
			}
		})
	}
}

// A resolved reader that never makes progress must be abandoned, not spun on.
type stalledObjectStream struct {
	reads  int
	closed bool
}

func (s *stalledObjectStream) Read([]byte) (int, error) { s.reads++; return 0, nil }
func (s *stalledObjectStream) Close() error             { s.closed = true; return nil }

func TestObjectStalledResolvedReaderIsAbandonedNotSpunOn(t *testing.T) {
	f, m, _ := newObjectFixture(t, "0123456789")
	stream := &stalledObjectStream{}
	f.router.reads = &objectReadObserver{SessionReader: f.router.reads, body: func(context.Context, sessionstore.GetObjectRequest) (io.ReadCloser, error) { return stream, nil }}
	got := f.get(objectTarget(m))
	if got.Code != 500 || got.Header().Get("X-Object-Digest") != "" {
		t.Fatalf("stalled stream: %d %v %s", got.Code, got.Header(), got.Body)
	}
	if stream.reads != 101 || !stream.closed {
		t.Fatalf("no-progress bound: %d reads, closed %v", stream.reads, stream.closed)
	}
}

// A resolved reader is not obliged to report cancellation as a context error.
// The deferred override is what makes a cancelled read answer 499/504 rather
// than the provider's own post-Close error, which must also stay redacted.
type nonContextErrorStream struct {
	entered, closed chan struct{}
	once            sync.Once
}

func (s *nonContextErrorStream) Read([]byte) (int, error) {
	s.once.Do(func() { close(s.entered) })
	<-s.closed
	return 0, errors.New("s3://private/stream?token=password")
}
func (s *nonContextErrorStream) Close() error {
	select {
	case <-s.closed:
	default:
		close(s.closed)
	}
	return nil
}

func TestObjectCancellationOverridesAResolvedReadersOwnError(t *testing.T) {
	f, m, _ := newObjectFixture(t, "0123456789")
	stream := &nonContextErrorStream{entered: make(chan struct{}), closed: make(chan struct{})}
	f.router.reads = &objectReadObserver{SessionReader: f.router.reads, body: func(context.Context, sessionstore.GetObjectRequest) (io.ReadCloser, error) { return stream, nil }}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- f.serve(request("GET", objectTarget(m), nil).WithContext(ctx)) }()
	select {
	case <-stream.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("read never entered")
	}
	cancel()
	select {
	case got := <-done:
		if got.Code != 499 || got.Header().Get("X-Object-Digest") != "" || strings.Contains(got.Body.String(), "s3:") || strings.Contains(got.Body.String(), "password") {
			t.Fatalf("cancelled read: %d %v %s", got.Code, got.Header(), got.Body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled read never returned")
	}
}

type closeErrorObject struct {
	io.Reader
	closed bool
}

func (c *closeErrorObject) Close() error { c.closed = true; return errors.New("s3://secret-close") }
func TestObjectSuccessfulEOFStillRequiresSuccessfulClose(t *testing.T) {
	f, m, _ := newObjectFixture(t, "secret")
	stream := &closeErrorObject{Reader: strings.NewReader("secret")}
	f.router.reads = &objectReadObserver{SessionReader: f.router.reads, body: func(context.Context, sessionstore.GetObjectRequest) (io.ReadCloser, error) { return stream, nil }}
	got := f.get(objectTarget(m))
	if got.Code != 500 || !stream.closed || got.Header().Get("X-Object-Digest") != "" || strings.Contains(got.Body.String(), "s3:") {
		t.Fatalf("close fault: %d %v %s", got.Code, got.Header(), got.Body)
	}
}

func TestObjectMetadataFaultsAndScope(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		status int
	}{
		{"missing", &sessionstore.ObjectError{Code: sessionstore.ObjectErrorMetadataUnavailable}, 404},
		{"backend", &sessionstore.ObjectError{Code: sessionstore.ObjectErrorBackend, Cause: errors.New("private-backend")}, 503},
		{"corrupt", &sessionstore.ObjectError{Code: sessionstore.ObjectErrorIntegrity}, 500},
		{"cancel", context.Canceled, 499}, {"deadline", context.DeadlineExceeded, 504},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, m, _ := newObjectFixture(t, "secret")
			observer := &objectReadObserver{SessionReader: f.router.reads}
			observer.metadata = func(ctx context.Context, r sessionstore.GetObjectMetadataRequest) (sessionwire.ObjectMetadata, error) {
				if r.TenantID != fixtureTenant || r.SessionID != fixtureSession || r.ExpectedKind != sessionstore.ObjectKindToolResult || r.Reference != m.Reference {
					t.Fatalf("scope %+v", r)
				}
				if _, ok := ctx.Deadline(); !ok {
					t.Fatal("metadata no deadline")
				}
				return sessionwire.ObjectMetadata{}, tc.err
			}
			f.router.reads = observer
			for _, suffix := range []string{"", "/metadata"} {
				got := f.get(objectTarget(m) + suffix + "?tenant=evil")
				if got.Code != tc.status || strings.Contains(got.Body.String(), "private-backend") {
					t.Fatalf("fault %d %s", got.Code, got.Body)
				}
			}
			if observer.bodyCalls != 0 {
				t.Fatal("metadata failure opened body")
			}
		})
	}
}

func TestObjectRangeRejectionsNeverOpenBody(t *testing.T) {
	f, m, _ := newObjectFixture(t, "0123456789")
	observer := &objectReadObserver{SessionReader: f.router.reads}
	f.router.reads = observer
	for _, span := range []string{"bytes=-1", "bytes=0-1,4-5", "bytes=10-11", "bytes=1-"} {
		req := request("GET", objectTarget(m), nil)
		req.Header.Set("Range", span)
		f.serve(req)
	}
	if observer.bodyCalls != 0 {
		t.Fatal("invalid range opened body")
	}
	f.router.objectLimits.MaxVerificationBytes = 4
	req := request("GET", objectTarget(m), nil)
	req.Header.Set("Range", "bytes=0-1")
	if got := f.serve(req); got.Code != 413 || observer.bodyCalls != 0 {
		t.Fatalf("oversized verification %d, body calls %d", got.Code, observer.bodyCalls)
	}
}

func TestObjectEmptyAndHEAD(t *testing.T) {
	for _, body := range []string{"", "secret"} {
		f, m, _ := newObjectFixture(t, body)
		got := f.get(objectTarget(m))
		if got.Code != 200 || got.Body.String() != body {
			t.Fatalf("empty/full %d %q", got.Code, got.Body)
		}
		head := f.serve(request("HEAD", objectTarget(m), nil))
		if head.Code != 200 || head.Body.Len() != 0 || head.Header().Get("Content-Length") != fmt.Sprint(len(body)) {
			t.Fatalf("head %d %v %s", head.Code, head.Header(), head.Body)
		}
	}
}

func TestObjectForeignTenantCatalogIsIndistinguishableFromAbsence(t *testing.T) {
	f, m, s := newObjectFixture(t, "secret")
	r := coldRecord(sessionwire.SessionStateIdle, sessionwire.SessionResidencyCold)
	_, _, err := s.CreateCatalogEntry(context.Background(), sessionstore.CreateCatalogEntryRequest{TenantID: "foreign", SessionID: "foreign-session", AgentID: r.AgentID, RuntimeCompatibilityID: r.RuntimeCompatibilityID, CreatedAt: r.CreatedAt, LastActiveAt: r.LastActiveAt, State: r.State, Residency: r.Residency, DesiredPlacement: r.DesiredPlacement, IdempotencyKey: "foreign"})
	if err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"", "/metadata"} {
		foreign := f.get(strings.Replace(objectTarget(m), string(fixtureSession), "foreign-session", 1) + suffix)
		absent := f.get(strings.Replace(objectTarget(m), string(fixtureSession), "absent-session", 1) + suffix)
		if foreign.Code != 404 || responseDifference(foreign, absent) != "" {
			t.Fatalf("foreign existence leaked %d %s", foreign.Code, foreign.Body)
		}
	}
}

type objectPolicyFunc func(context.Context, identity.Principal, sessionstore.CatalogEntry, sessionwire.ObjectReference) (sessionstore.ObjectKind, error)

func (f objectPolicyFunc) AuthorizeReference(ctx context.Context, p identity.Principal, e sessionstore.CatalogEntry, ref sessionwire.ObjectReference) (sessionstore.ObjectKind, error) {
	return f(ctx, p, e, ref)
}

func newObjectFixture(t *testing.T, body string) (*fixture, sessionwire.ObjectMetadata, *sessionstore.Store) {
	t.Helper()
	f, _, _, observed := newRealTailFixture(t, 1, "")
	sum := sha256.Sum256([]byte(body))
	m, err := observed.Store.PutObject(context.Background(), sessionstore.PutObjectRequest{TenantID: fixtureTenant, SessionID: fixtureSession, Kind: sessionstore.ObjectKindToolResult, SizeBytes: uint64(len(body)), SHA256: sum, Body: strings.NewReader(body), MediaType: "text/plain"})
	if err != nil {
		t.Fatal(err)
	}
	f.router.objectPolicy = objectPolicyFunc(func(_ context.Context, p identity.Principal, e sessionstore.CatalogEntry, ref sessionwire.ObjectReference) (sessionstore.ObjectKind, error) {
		if p.Tenant() != fixtureTenant || e.Record.SessionID != fixtureSession || ref != m.Reference {
			return "", identity.ErrUnauthenticated
		}
		return sessionstore.ObjectKindToolResult, nil
	})
	return f, m, observed.Store
}
func objectTarget(m sessionwire.ObjectMetadata) string {
	return "/v1/sessions/" + string(fixtureSession) + "/objects/" + url.PathEscape(m.Reference.ObjectID)
}

func TestObjectMetadataAndVerifiedRanges(t *testing.T) {
	f, m, _ := newObjectFixture(t, "0123456789")
	meta := f.get(objectTarget(m) + "/metadata")
	var got sessionwire.ObjectMetadata
	if meta.Code != 200 {
		t.Fatalf("metadata %d: %s", meta.Code, meta.Body)
	}
	if err := got.UnmarshalJSON(meta.Body.Bytes()); err != nil || !reflect.DeepEqual(got, m) {
		t.Fatalf("metadata %+v err %v", got, err)
	}
	for _, tc := range []struct {
		span, want string
		status     int
	}{{"bytes=2-4", "234", 206}, {"", "0123456789", 200}} {
		req := request("GET", objectTarget(m), nil)
		if tc.span != "" {
			req.Header.Set("Range", tc.span)
		}
		res := f.serve(req)
		if res.Code != tc.status || res.Body.String() != tc.want {
			t.Fatalf("%s: %d %q", tc.span, res.Code, res.Body)
		}
		if res.Header().Get("X-Object-Digest") != m.Digest || res.Header().Get("X-Object-Size") != "10" {
			t.Fatalf("headers %v", res.Header())
		}
		if tc.span != "" && res.Header().Get("Content-Range") != "bytes 2-4/10" {
			t.Fatal(res.Header())
		}
	}
}

func TestObjectStrictRanges(t *testing.T) {
	f, m, _ := newObjectFixture(t, "0123456789")
	for _, span := range []string{"bytes=-3", "bytes=3-", "bytes=0-1,3-4", "bytes=2-1", "bytes=+0-1", "bytes=0-18446744073709551616", "Bytes=0-1", "bytes= 0-1"} {
		req := request("GET", objectTarget(m), nil)
		req.Header.Set("Range", span)
		if got := f.serve(req); got.Code != 400 {
			t.Errorf("%s: %d", span, got.Code)
		}
	}
	for _, span := range []string{"bytes=10-10", "bytes=9-10"} {
		req := request("GET", objectTarget(m), nil)
		req.Header.Set("Range", span)
		if got := f.serve(req); got.Code != 416 || got.Header().Get("Content-Range") != "bytes */10" {
			t.Errorf("%s: %d %v", span, got.Code, got.Header())
		}
	}
	req := request("GET", objectTarget(m), nil)
	req.Header.Add("Range", "bytes=0-1")
	req.Header.Add("Range", "bytes=2-3")
	if got := f.serve(req); got.Code != 400 {
		t.Fatal(got.Code)
	}
}

func TestObjectPageAndVerificationCeilings(t *testing.T) {
	f, m, _ := newObjectFixture(t, strings.Repeat("x", 1<<20+1))
	if got := f.get(objectTarget(m)); got.Code != 413 {
		t.Fatalf("full: %d", got.Code)
	}
	req := request("GET", objectTarget(m), nil)
	req.Header.Set("Range", "bytes=0-1048576")
	if got := f.serve(req); got.Code != 413 {
		t.Fatalf("page: %d", got.Code)
	}
	req.Header.Set("Range", "bytes=0-1048575")
	if got := f.serve(req); got.Code != 206 || !bytes.Equal(got.Body.Bytes(), bytes.Repeat([]byte("x"), 1<<20)) {
		t.Fatalf("bounded page: %d", got.Code)
	}
	f.router.objectLimits.MaxVerificationBytes = 1 << 20
	req.Header.Set("Range", "bytes=0-1")
	if got := f.serve(req); got.Code != 413 {
		t.Fatalf("verification: %d", got.Code)
	}
}
