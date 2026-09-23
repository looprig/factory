package factory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/hostlink"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage/memstore"
)

// These cases drive a gate response through the COMPOSED Server -- New's own
// wiring of admission to the HostLink pool -- over a real SessionStore, with
// only the transport replaced (config.hostDialer: a root-package test may not
// import centrifuge). They are the readers the v0.5.0 gates found missing:
//
//   - spec N1 / quality CW: New wiring admission WITHOUT its GateResponders
//     passed the whole module, and would refuse every gate response in
//     production 409. A capable owner must be ADMITTED end to end.
//   - quality F1: a transient failure to reach the owner answered 500
//     retryable:false. It must be 503 retryable, with nothing written.

const (
	e2eAgent   = sessionwire.AgentID("agent-a")
	e2eRuntime = "runtime-v1"
	e2eSession = sessionwire.SessionID("session-gate")
	e2eOrigin  = "https://app.example.com"
)

// scriptedDial answers each dial with a link whose capability read and dial
// outcome the case chooses.
type scriptedDial struct {
	mu      sync.Mutex
	methods []string
	readErr error
	dialErr error
	dialled []hostlink.Target
}

func (d *scriptedDial) Dial(_ context.Context, target hostlink.Target, _ hostlink.Observer) (hostlink.Link, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.dialled = append(d.dialled, target)
	if d.dialErr != nil {
		return nil, d.dialErr
	}
	return scriptedLink{host: target.Host, methods: d.methods, readErr: d.readErr}, nil
}

type scriptedLink struct {
	host    sessionwire.HostID
	methods []string
	readErr error
}

func (l scriptedLink) Host() sessionwire.HostID { return l.host }
func (scriptedLink) Bind(context.Context, sessionwire.HostLinkBindRequest) error {
	return nil
}
func (scriptedLink) Unbind(context.Context, sessionwire.HostLinkUnbindRequest) error {
	return nil
}
func (scriptedLink) Attach(context.Context, sessionwire.HostLinkAttachRequest) (sessionwire.HostLinkRegistryObservation, error) {
	return sessionwire.HostLinkRegistryObservation{}, errors.New("unused")
}
func (scriptedLink) DeliverCommand(context.Context, sessionwire.TenantID, sessionwire.SessionID, sessionwire.HostLinkCommandDelivery) error {
	return nil
}
func (scriptedLink) Close(context.Context) error { return nil }
func (l scriptedLink) Negotiated() (sessionwire.VersionNegotiationResponse, error) {
	if l.readErr != nil {
		return sessionwire.VersionNegotiationResponse{}, l.readErr
	}
	return sessionwire.VersionNegotiationResponse{Version: sessionwire.CurrentWireVersion}.WithHostLinkMethods(l.methods...), nil
}

// gateWorld is a real store holding one disposition-bound session with one
// Host-published open gate and a fresh resident owner.
func gateWorld(t *testing.T) *sessionstore.Store {
	t.Helper()
	return gateWorldWithOwner(t, func(*sessionstore.PutHostRegistrationRequest) bool { return true })
}

// gateWorldWithOwner is gateWorld whose owner registration the case shapes:
// owner edits the request, and false registers no owner at all.
func gateWorldWithOwner(t *testing.T, owner func(*sessionstore.PutHostRegistrationRequest) bool) *sessionstore.Store {
	t.Helper()
	ctx := context.Background()
	store, err := sessionstore.Open(ctx, memstore.New())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close(context.Background()) })
	now := time.Now().UTC()
	payload := []byte(`{"blocks":[{"type":"text","text":"hi"}]}`)
	digest := sha256.Sum256(payload)
	identity := sessionstore.PublicCreateIdentity{
		TenantID: FakeTenant, SessionID: e2eSession, CommandID: "create-1",
		Target: sessionstore.HostTargetKey{AgentID: e2eAgent, RuntimeCompatibilityID: e2eRuntime, Placement: sessionwire.HostPlacementPooled},
		Binding: sessionstore.SessionBinding{StorageBindingID: "storage-a", BindingVersion: "v1",
			RuntimeSessionID: "00000000-0000-4000-8000-0000000000aa", ProtocolMode: sessionstore.ProtocolModeDisposition},
		Kind: sessionstore.CommandKind("create"), PayloadDigest: hex.EncodeToString(digest[:]), PayloadSize: uint64(len(payload)),
	}
	if _, err := store.PreparePublicCreate(ctx, sessionstore.PreparePublicCreateRequest{Identity: identity,
		ProposedRuntimeCommandID: "runtime-create-1", AcceptedAt: now, ApplyDeadline: now.Add(time.Hour)}); err != nil {
		t.Fatalf("PreparePublicCreate: %v", err)
	}
	if _, _, err := store.AdmitPublicCreate(ctx, sessionstore.AdmitPublicCreateRequest{Identity: identity, Payload: payload}); err != nil {
		t.Fatalf("AdmitPublicCreate: %v", err)
	}
	grant, err := store.AcquireResidency(ctx, sessionstore.AcquireResidencyRequest{TenantID: FakeTenant, SessionID: e2eSession})
	if err != nil {
		t.Fatalf("AcquireResidency: %v", err)
	}
	t.Cleanup(func() { _ = grant.Release(context.Background()) })
	if _, err := store.OpenGate(ctx, sessionstore.OpenGateRequest{TenantID: FakeTenant, SessionID: e2eSession, Residency: grant,
		Gate: sessionwire.GateProjection{GateID: "gate-a", Kind: "approval", Prompt: sessionwire.GatePrompt{Title: "approve?"},
			OpenedEventID: "event-a", OpenedJournalSeq: 3, Deadline: now.Add(time.Hour), Answerability: sessionwire.GateAnswerabilityResident}}); err != nil {
		t.Fatalf("OpenGate: %v", err)
	}
	registration := sessionstore.PutHostRegistrationRequest{
		TenantID: FakeTenant, SessionID: e2eSession, LeaseEpoch: uint64(grant.Epoch()),
		ObservedAt: now, ExpiresAt: now.Add(time.Hour),
		Route: sessionstore.HostRoute{HostID: "host-owner", HostGeneration: 2, AgentID: e2eAgent, RuntimeCompatibilityID: e2eRuntime,
			Placement: sessionwire.HostPlacementPooled, InternalEndpoint: "ws://10.9.8.7:7100",
			Residency: sessionwire.SessionResidencyResident, Accepting: true},
	}
	if owner(&registration) {
		if _, err := store.PutHostRegistration(ctx, registration); err != nil {
			t.Fatalf("PutHostRegistration: %v", err)
		}
	}
	return store
}

func composedGateServer(t *testing.T, store *sessionstore.Store, dialer hostlink.Dialer) *Server {
	t.Helper()
	directory, err := NewStoreDirectory(store, DefaultDirectoryLimits())
	if err != nil {
		t.Fatal(err)
	}
	options := append(RequiredOptionsExcept("WithCommands", "WithDirectory", "WithCatalog", "WithGates", "WithSessionReader", "WithHostTargets"),
		WithCommands(store), WithDirectory(directory), WithCatalog(store), WithGates(store), WithSessionReader(store), WithHostTargets(store),
		WithDepartment(LaunchTemplate{Key: sessionstore.HostTargetKey{AgentID: e2eAgent, RuntimeCompatibilityID: e2eRuntime, Placement: sessionwire.HostPlacementPooled}}),
		Option{name: "hostDialer (test only)", apply: func(cfg *config) error { cfg.hostDialer = dialer; return nil }},
	)
	server, err := New(options...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return server
}

func postGateResponse(t *testing.T, server *Server, command string) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(sessionwire.GateResponseRequest{
		CommandEnvelope: sessionwire.CommandEnvelope{Version: sessionwire.CurrentWireVersion, CommandID: sessionwire.CommandID(command)},
		SessionID:       e2eSession, GateID: "gate-a", Action: "submit",
		Values: map[string]json.RawMessage{"answer": json.RawMessage(`"yes"`)}, ExpectedOpenEventID: "event-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, e2eOrigin+"/v1/sessions/"+string(e2eSession)+"/gates/gate-a", strings.NewReader(string(body)))
	request.Header.Set("Authorization", "Bearer "+FakeCredentialValue)
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	return recorder
}

func stored(t *testing.T, store *sessionstore.Store, command string) bool {
	t.Helper()
	_, err := store.GetDispositionCommand(context.Background(), sessionstore.GetDispositionCommandRequest{
		TenantID: FakeTenant, SessionID: e2eSession, CommandID: sessionwire.CommandID(command)})
	return err == nil
}

// TestAComposedFactoryAdmitsAGateResponseForAnOwnerAdvertisingTheToken is CW's
// reader: through New's wiring, a gate response for an owner whose reply lists
// Core's token is admitted (202) and written; the same owner advertising only
// the five methods is refused 409 gate_not_resumable with nothing written. The
// capability is asked of the owner's link for the session's tenant.
func TestAComposedFactoryAdmitsAGateResponseForAnOwnerAdvertisingTheToken(t *testing.T) {
	t.Parallel()

	capable := &scriptedDial{methods: []string{sessionwire.HostLinkMethodBind, sessionwire.HostLinkCapabilityGateResponse}}
	store := gateWorld(t)
	answer := postGateResponse(t, composedGateServer(t, store, capable), "answer-1")
	if answer.Code != http.StatusAccepted || !stored(t, store, "answer-1") {
		t.Fatalf("a capable owner's gate response = %d %s (stored %v), want 202 and written", answer.Code, answer.Body, stored(t, store, "answer-1"))
	}
	if len(capable.dialled) != 1 || capable.dialled[0] != (hostlink.Target{Host: "host-owner", Endpoint: "ws://10.9.8.7:7100/hostlink/" + FakeTenant}) {
		t.Fatalf("the capability was asked over %v, want the owner's tenant link", capable.dialled)
	}

	older := &scriptedDial{methods: []string{sessionwire.HostLinkMethodBind, sessionwire.HostLinkMethodUnbind,
		sessionwire.HostLinkMethodAttach, sessionwire.HostLinkMethodDrain, sessionwire.HostLinkMethodDrainStatus}}
	store = gateWorld(t)
	refused := postGateResponse(t, composedGateServer(t, store, older), "answer-2")
	if refused.Code != http.StatusConflict || !strings.Contains(refused.Body.String(), string(sessionwire.ErrorCodeGateNotResumable)) || stored(t, store, "answer-2") {
		t.Fatalf("an incapable owner's gate response = %d %s (stored %v), want 409 gate_not_resumable, nothing written", refused.Code, refused.Body, stored(t, store, "answer-2"))
	}
}

// TestATransientFailureToReachTheOwnerIsRetryable is quality gate F1, end to
// end: the owner's link reconnecting, the link ceiling full, and a failed dial
// each answer 503 unavailable, retryable, with nothing written -- and a
// control that is NOT transient (a plain fault) still answers 500.
func TestATransientFailureToReachTheOwnerIsRetryable(t *testing.T) {
	t.Parallel()

	for name, row := range map[string]struct {
		dial   *scriptedDial
		status int
	}{
		"the owner's link is reconnecting": {&scriptedDial{readErr: fmt.Errorf("hostlink: capability read: %w", hostlink.ErrLinkReconnecting)}, http.StatusServiceUnavailable},
		"the link ceiling is full":         {&scriptedDial{dialErr: fmt.Errorf("%w: 256 links open", hostlink.ErrLinkLimit)}, http.StatusServiceUnavailable},
		"the dial failed":                  {&scriptedDial{dialErr: fmt.Errorf("%w: host-owner: handshake did not settle", hostlink.ErrDialFailed)}, http.StatusServiceUnavailable},
		"a fault that is not transient":    {&scriptedDial{readErr: errors.New("something else broke")}, http.StatusInternalServerError},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store := gateWorld(t)
			answer := postGateResponse(t, composedGateServer(t, store, row.dial), "answer-t")
			if answer.Code != row.status || stored(t, store, "answer-t") {
				t.Fatalf("= %d %s (stored %v), want %d and nothing written", answer.Code, answer.Body, stored(t, store, "answer-t"), row.status)
			}
			var envelope struct {
				Error struct {
					Code      string `json:"code"`
					Retryable bool   `json:"retryable"`
				} `json:"error"`
			}
			if err := json.Unmarshal(answer.Body.Bytes(), &envelope); err != nil {
				t.Fatalf("body %s: %v", answer.Body, err)
			}
			if wantRetry := row.status == http.StatusServiceUnavailable; envelope.Error.Retryable != wantRetry {
				t.Fatalf("retryable = %v, want %v (%s)", envelope.Error.Retryable, wantRetry, answer.Body)
			}
		})
	}
}
