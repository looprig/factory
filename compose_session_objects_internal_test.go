package factory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	"github.com/looprig/sessionstore"
)

// These cases hold v0.11.0's object read for a Host-owned session: the route
// is addressed by the PUBLIC session id, the object lives in the runtime's
// store under the binding's RuntimeSessionID, and WithSessionObjectStoreResolver
// is what joins the two. Every case runs over a real control store (a public
// create, disposition-bound) and a real runtime store in Harness's legacy
// single-tenant layout, which is where a Harness runtime writes a tool-result
// capture.

const (
	secondSession = sessionwire.SessionID("session-second")
	secondRuntime = "00000000-0000-4000-8000-0000000000cc"
)

// admitHostSession files one more disposition-bound public create in control,
// so a case has a second session of the same tenant with its own runtime id.
func admitHostSession(t *testing.T, control *sessionstore.Store, tenant sessionwire.TenantID, session sessionwire.SessionID, runtime string) sessionstore.SessionBinding {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC()
	payload := []byte(`{"blocks":[{"type":"text","text":"hi"}]}`)
	digest := sha256.Sum256(payload)
	binding := sessionstore.SessionBinding{StorageBindingID: "storage-a", BindingVersion: "v1",
		RuntimeSessionID: runtime, ProtocolMode: sessionstore.ProtocolModeDisposition}
	id := sessionstore.PublicCreateIdentity{
		TenantID: tenant, SessionID: session, CommandID: sessionwire.CommandID("create-" + string(session)),
		Target:  sessionstore.HostTargetKey{AgentID: e2eAgent, RuntimeCompatibilityID: e2eRuntime, Placement: sessionwire.HostPlacementPooled},
		Binding: binding,
		Kind:    sessionstore.CommandKind("create"), PayloadDigest: hex.EncodeToString(digest[:]), PayloadSize: uint64(len(payload)),
	}
	if _, err := control.PreparePublicCreate(ctx, sessionstore.PreparePublicCreateRequest{Identity: id,
		ProposedRuntimeCommandID: sessionstore.RuntimeCommandID("runtime-create-" + string(session)), AcceptedAt: now, ApplyDeadline: now.Add(time.Hour)}); err != nil {
		t.Fatalf("PreparePublicCreate: %v", err)
	}
	if _, _, err := control.AdmitPublicCreate(ctx, sessionstore.AdmitPublicCreateRequest{Identity: id, Payload: payload}); err != nil {
		t.Fatalf("AdmitPublicCreate: %v", err)
	}
	return binding
}

// putToolResult writes a capture the way Harness does: tool-result kind, in
// the runtime store, under the runtime session id.
func putToolResult(t *testing.T, runtime *sessionstore.Store, session string, body []byte) sessionwire.ObjectMetadata {
	t.Helper()
	m, err := runtime.PutObject(context.Background(), sessionstore.PutObjectRequest{
		TenantID: FakeTenant, SessionID: sessionwire.SessionID(session), Kind: sessionstore.ObjectKindToolResult,
		SizeBytes: uint64(len(body)), SHA256: sha256.Sum256(body), Body: bytes.NewReader(body), MediaType: "text/plain"})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	return m
}

type objectResolution struct {
	tenant  sessionwire.TenantID
	session sessionwire.SessionID
	binding sessionstore.SessionBinding
}

// addressedReads records what a resolver was handed and the session every
// request the returned reader was asked is addressed by.
type addressedReads struct {
	mu        sync.Mutex
	resolved  []objectResolution
	addressed []sessionwire.SessionID
	store     *sessionstore.Store
}

func (a *addressedReads) resolve(_ context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, binding sessionstore.SessionBinding) (ObjectReader, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.resolved = append(a.resolved, objectResolution{tenant, session, binding})
	if tenant != FakeTenant || binding.StorageBindingID != "storage-a" || binding.BindingVersion != "v1" {
		return nil, errors.New("unknown object binding")
	}
	return addressedReader{a}, nil
}

func (a *addressedReads) snapshot() ([]objectResolution, []sessionwire.SessionID) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]objectResolution(nil), a.resolved...), append([]sessionwire.SessionID(nil), a.addressed...)
}

type addressedReader struct{ a *addressedReads }

func (r addressedReader) GetObjectMetadata(ctx context.Context, req sessionstore.GetObjectMetadataRequest) (sessionwire.ObjectMetadata, error) {
	r.a.mu.Lock()
	r.a.addressed = append(r.a.addressed, req.SessionID)
	r.a.mu.Unlock()
	return r.a.store.GetObjectMetadata(ctx, req)
}

func (r addressedReader) GetObject(ctx context.Context, req sessionstore.GetObjectRequest) (io.ReadCloser, error) {
	r.a.mu.Lock()
	r.a.addressed = append(r.a.addressed, req.SessionID)
	r.a.mu.Unlock()
	return r.a.store.GetObject(ctx, req)
}

// toolResultPolicy permits exactly the references it was given, as tool-result;
// anything else is denied with the public sentinel.
type toolResultPolicy struct {
	mu      sync.Mutex
	permit  map[sessionwire.ObjectReference]sessionwire.SessionID
	asked   int
	kindFor sessionstore.ObjectKind
}

func (p *toolResultPolicy) AuthorizeReference(_ context.Context, principal identity.Principal, entry sessionstore.CatalogEntry, ref sessionwire.ObjectReference) (sessionstore.ObjectKind, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.asked++
	if session, ok := p.permit[ref]; ok && session == entry.Record.SessionID && principal.Tenant() == FakeTenant {
		if p.kindFor != "" {
			return p.kindFor, nil
		}
		return sessionstore.ObjectKindToolResult, nil
	}
	return "", identity.ErrUnauthorized
}

func getObject(t *testing.T, server *Server, session sessionwire.SessionID, ref sessionwire.ObjectReference, suffix string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, e2eOrigin+"/v1/sessions/"+string(session)+"/objects/"+url.PathEscape(ref.ObjectID)+suffix, nil)
	request.Header.Set("Authorization", "Bearer "+FakeCredentialValue)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	return recorder
}

// TestAHostSessionsToolResultIsReadUnderItsRuntimeSession is the positive
// half: the capture lives only under the runtime id, and the PUBLIC session's
// route serves it, with the resolver handed the principal's tenant, the public
// session and the catalog's binding, and every store request addressed by the
// binding's RuntimeSessionID.
func TestAHostSessionsToolResultIsReadUnderItsRuntimeSession(t *testing.T) {
	t.Parallel()

	control, runtime, _ := hostSessionWorld(t)
	body := []byte("the full output of a tool, retained after its workspace is gone")
	m := putToolResult(t, runtime, journalRuntime, body)
	reads := &addressedReads{store: runtime}
	policy := &toolResultPolicy{permit: map[sessionwire.ObjectReference]sessionwire.SessionID{m.Reference: journalSession}}
	server := composedJournalServer(t, control, WithFakeJournals(), WithObjectPolicy(policy),
		WithSessionObjectStoreResolver(reads.resolve))

	got := getObject(t, server, journalSession, m.Reference, "")
	if got.Code != http.StatusOK || !bytes.Equal(got.Body.Bytes(), body) {
		t.Fatalf("object = %d %q, want 200 and the retained bytes", got.Code, got.Body)
	}
	if got.Header().Get("X-Object-Digest") != m.Digest {
		t.Fatalf("X-Object-Digest = %q, want %q", got.Header().Get("X-Object-Digest"), m.Digest)
	}
	meta := getObject(t, server, journalSession, m.Reference, "/metadata")
	if meta.Code != http.StatusOK || !strings.Contains(meta.Body.String(), m.Digest) {
		t.Fatalf("metadata = %d %s", meta.Code, meta.Body)
	}
	if strings.Contains(got.Body.String()+meta.Body.String(), journalRuntime) {
		t.Fatal("a response echoed the runtime session id")
	}

	resolved, addressed := reads.snapshot()
	binding := sessionstore.SessionBinding{StorageBindingID: "storage-a", BindingVersion: "v1",
		RuntimeSessionID: journalRuntime, ProtocolMode: sessionstore.ProtocolModeDisposition}
	want := objectResolution{FakeTenant, journalSession, binding}
	if len(resolved) != 2 || resolved[0] != want || resolved[1] != want {
		t.Fatalf("resolver handed %+v, want twice %+v", resolved, want)
	}
	// Body read: metadata then body; metadata read: metadata.
	if len(addressed) != 3 {
		t.Fatalf("addressed %v, want three store requests", addressed)
	}
	for _, session := range addressed {
		if session != sessionwire.SessionID(journalRuntime) {
			t.Fatalf("a store request was addressed by %q, want the runtime id %q", session, journalRuntime)
		}
	}
}

// TestTheDeprecatedObjectResolverKeepsItsPublicAddressing: the v0.10.0 option
// is unchanged -- handed only the binding, its reader addressed by the public
// session id -- so a composition that already adapts it is not moved under it.
func TestTheDeprecatedObjectResolverKeepsItsPublicAddressing(t *testing.T) {
	t.Parallel()

	control, runtime, _ := hostSessionWorld(t)
	m := putToolResult(t, runtime, journalRuntime, []byte("bytes"))
	reads := &addressedReads{store: runtime}
	policy := &toolResultPolicy{permit: map[sessionwire.ObjectReference]sessionwire.SessionID{m.Reference: journalSession}}
	server := composedJournalServer(t, control, WithFakeJournals(), WithObjectPolicy(policy),
		WithObjectStoreResolver(func(ctx context.Context, binding sessionstore.SessionBinding) (ObjectReader, error) {
			return reads.resolve(ctx, FakeTenant, "", binding)
		}))
	_ = getObject(t, server, journalSession, m.Reference, "/metadata")
	if _, addressed := reads.snapshot(); len(addressed) != 1 || addressed[0] != journalSession {
		t.Fatalf("the deprecated resolver's reader was addressed by %v, want the public id", addressed)
	}
}

// TestAnObjectOutsideTheSessionIsIndistinguishableFromAbsence: another
// session's capture, a denied reference and an absent object are one answer --
// 404 with identical bytes -- and a foreign tenant's session is the same answer
// as an absent session.
func TestAnObjectOutsideTheSessionIsIndistinguishableFromAbsence(t *testing.T) {
	t.Parallel()

	control, runtime, _ := hostSessionWorld(t)
	admitHostSession(t, control, FakeTenant, secondSession, secondRuntime)
	admitHostSession(t, control, "tenant-foreign", "session-of-another-tenant", "00000000-0000-4000-8000-0000000000dd")
	mine := putToolResult(t, runtime, journalRuntime, []byte("mine"))
	theirs := putToolResult(t, runtime, secondRuntime, []byte("theirs"))
	absent := sessionwire.ObjectReference{ObjectID: strings.Replace(mine.Reference.ObjectID,
		mine.Digest[len("sha256:"):len("sha256:")+8], "00000000", 1)}
	reads := &addressedReads{store: runtime}
	// A permissive policy for the cross-session row: it vouches for theirs
	// under MY session, so only the runtime addressing keeps it out.
	policy := &toolResultPolicy{permit: map[sessionwire.ObjectReference]sessionwire.SessionID{
		mine.Reference: journalSession, theirs.Reference: journalSession, absent: journalSession}}
	server := composedJournalServer(t, control, WithFakeJournals(), WithObjectPolicy(policy),
		WithSessionObjectStoreResolver(reads.resolve))

	for _, suffix := range []string{"", "/metadata"} {
		if got := getObject(t, server, journalSession, mine.Reference, suffix); got.Code != http.StatusOK {
			t.Fatalf("%s: the control read = %d %s", suffix, got.Code, got.Body)
		}
		missing := getObject(t, server, journalSession, absent, suffix)
		if missing.Code != http.StatusNotFound {
			t.Fatalf("%s: an absent object = %d %s, want 404", suffix, missing.Code, missing.Body)
		}
		for name, got := range map[string]*httptest.ResponseRecorder{
			"another session's capture through mine": getObject(t, server, journalSession, theirs.Reference, suffix),
			"my capture through another session":     getObject(t, server, secondSession, mine.Reference, suffix),
		} {
			if got.Code != missing.Code || got.Body.String() != missing.Body.String() {
				t.Fatalf("%s %s = %d %s, want the absent answer %s", suffix, name, got.Code, got.Body, missing.Body)
			}
		}
		foreign := getObject(t, server, "session-of-another-tenant", mine.Reference, suffix)
		nobody := getObject(t, server, "session-nobody-created", mine.Reference, suffix)
		if foreign.Code != http.StatusNotFound || foreign.Body.String() != nobody.Body.String() {
			t.Fatalf("%s: a foreign session = %d %s, an absent one %s", suffix, foreign.Code, foreign.Body, nobody.Body)
		}
	}
}

// TestWithoutAnObjectPolicyTheRouteRefusesBeforeAnyStore is the documented
// refusal: 503 unavailable, and the resolver is never asked.
func TestWithoutAnObjectPolicyTheRouteRefusesBeforeAnyStore(t *testing.T) {
	t.Parallel()

	control, runtime, _ := hostSessionWorld(t)
	m := putToolResult(t, runtime, journalRuntime, []byte("bytes"))
	reads := &addressedReads{store: runtime}
	server := composedJournalServer(t, control, WithFakeJournals(), WithSessionObjectStoreResolver(reads.resolve))
	for _, suffix := range []string{"", "/metadata"} {
		got := getObject(t, server, journalSession, m.Reference, suffix)
		if got.Code != http.StatusServiceUnavailable || !strings.Contains(got.Body.String(), `"unavailable"`) {
			t.Fatalf("%s: = %d %s, want 503 unavailable", suffix, got.Code, got.Body)
		}
	}
	if resolved, _ := reads.snapshot(); len(resolved) != 0 {
		t.Fatalf("the resolver was asked %d times with no policy composed", len(resolved))
	}
}

// TestOnlyAToolResultIsServedByTheObjectRoute: a policy vouching for any other
// kind is a policy fault, refused before any store is resolved.
func TestOnlyAToolResultIsServedByTheObjectRoute(t *testing.T) {
	t.Parallel()

	control, runtime, _ := hostSessionWorld(t)
	m := putToolResult(t, runtime, journalRuntime, []byte("bytes"))
	for _, kind := range []sessionstore.ObjectKind{sessionstore.ObjectKindArtifact, sessionstore.ObjectKindRuntimeCheckpoint, sessionstore.ObjectKindCommandPayload} {
		reads := &addressedReads{store: runtime}
		policy := &toolResultPolicy{kindFor: kind, permit: map[sessionwire.ObjectReference]sessionwire.SessionID{m.Reference: journalSession}}
		server := composedJournalServer(t, control, WithFakeJournals(), WithObjectPolicy(policy),
			WithSessionObjectStoreResolver(reads.resolve))
		if got := getObject(t, server, journalSession, m.Reference, "/metadata"); got.Code != http.StatusInternalServerError {
			t.Fatalf("%s: = %d %s, want 500", kind, got.Code, got.Body)
		}
		if resolved, _ := reads.snapshot(); len(resolved) != 0 {
			t.Fatalf("%s: a store was resolved for a kind the route does not serve", kind)
		}
	}
}

// TestObjectResolverComposition: the two resolvers are alternatives, refused
// together like the journal resolvers, and either satisfies the policy and
// binding requirements.
func TestObjectResolverComposition(t *testing.T) {
	t.Parallel()

	legacy := WithObjectStoreResolver(func(context.Context, sessionstore.SessionBinding) (ObjectReader, error) {
		return nil, errors.New("unused")
	})
	aware := WithSessionObjectStoreResolver(func(context.Context, sessionwire.TenantID, sessionwire.SessionID, sessionstore.SessionBinding) (ObjectReader, error) {
		return nil, errors.New("unused")
	})
	for name, options := range map[string][]Option{"legacy first": {legacy, aware}, "aware first": {aware, legacy}} {
		_, err := New(append(RequiredOptions(), options...)...)
		var named *OptionError
		if !errors.Is(err, ErrConflictingObjectResolvers) || !errors.As(err, &named) || named.Option != "WithSessionObjectStoreResolver" {
			t.Fatalf("%s: New = %v, want ErrConflictingObjectResolvers naming WithSessionObjectStoreResolver", name, err)
		}
	}
	if _, err := New(append(RequiredOptions(), WithSessionObjectStoreResolver(nil))...); !errors.Is(err, ErrNilDependency) {
		t.Fatalf("a nil session object resolver = %v, want ErrNilDependency", err)
	}
	policy := WithObjectPolicy(&toolResultPolicy{})
	control, _, _ := hostSessionWorld(t)
	for name, options := range map[string][]Option{
		"policy with the session resolver":  {policy, aware},
		"binding with the session resolver": {WithSessionBinding("storage-a", "v1"), WithPublicCreates(control), WithFakeJournals(), aware},
	} {
		if _, err := New(append(RequiredOptions(), options...)...); err != nil {
			t.Fatalf("%s: New = %v, want a composition", name, err)
		}
	}
}
