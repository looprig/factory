package admission

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"reflect"
	"strconv"
	"strings"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
)

// servicePublicCreates models the released public-create plane.
//
// It is written to be AS STRICT AS THE DEPENDENCY on the three axes that
// decide whether steps 2 and 5 are correct, because a fake looser than the
// store would let this package pass while the real one refused:
//
//   - The reservation is keyed by CommandID under the TENANT, not the session
//     (public_create.go publicCreateID), and any disagreement in the FULL
//     identity is InboxErrorCommandMismatch (publicCreateWinner). That is what
//     makes a second SessionID under one CommandID fail.
//   - The catalog is create-only per SessionID and answers CatalogErrorConflict
//     on "public_create" when a DIFFERENT create reaches an existing session
//     (catalog.go samePublicCreate).
//   - PutCommandPayload mints a FRESH OBJECT GENERATION ON EVERY CALL. This is
//     the hazard that made step 5 unimplementable on the legacy inbox, and
//     modelling it is the whole point of the fake: a retry that re-uploads
//     MUST still match, which can only work because AdmitPublicCreate compares
//     PayloadDigest and PayloadSize and EXCLUDES PayloadObject.
type servicePublicCreates struct {
	faultInjector
	reservations map[sessionwire.CommandID]sessionstore.PublicCreateReservation
	sessions     map[sessionwire.SessionID]sessionwire.CommandID
	records      map[sessionwire.CommandID]sessionstore.DispositionInboxEntry
	uploads      int
	// putBodies records the bytes each upload actually streamed, so a test can
	// assert the store was handed the same content the identity declared.
	putBodies [][]byte
}

func newServicePublicCreates() *servicePublicCreates {
	return &servicePublicCreates{
		reservations: map[sessionwire.CommandID]sessionstore.PublicCreateReservation{},
		sessions:     map[sessionwire.SessionID]sessionwire.CommandID{},
		records:      map[sessionwire.CommandID]sessionstore.DispositionInboxEntry{},
	}
}

func (c *servicePublicCreates) PreparePublicCreate(_ context.Context, req sessionstore.PreparePublicCreateRequest) (sessionstore.PublicCreatePreparation, error) {
	if err := c.enter("PreparePublicCreate"); err != nil {
		return sessionstore.PublicCreatePreparation{}, err
	}
	id := req.Identity
	if prior, ok := c.reservations[id.CommandID]; ok {
		// The whole identity, exactly as publicCreateWinner compares it.
		if prior.Identity != id {
			return sessionstore.PublicCreatePreparation{}, &sessionstore.InboxError{Code: sessionstore.InboxErrorCommandMismatch}
		}
		return sessionstore.PublicCreatePreparation{Reservation: prior}, nil
	}
	if owner, ok := c.sessions[id.SessionID]; ok && owner != id.CommandID {
		return sessionstore.PublicCreatePreparation{}, &sessionstore.CatalogError{Code: sessionstore.CatalogErrorConflict, Field: "public_create"}
	}
	reservation := sessionstore.PublicCreateReservation{
		Identity: id, RuntimeCommandID: req.ProposedRuntimeCommandID,
		AcceptedAt: req.AcceptedAt, ApplyDeadline: req.ApplyDeadline, InitialWorkload: req.InitialWorkload,
	}
	c.reservations[id.CommandID] = reservation
	c.sessions[id.SessionID] = id.CommandID
	return sessionstore.PublicCreatePreparation{Reservation: reservation}, nil
}

func (c *servicePublicCreates) PutCommandPayload(_ context.Context, req sessionstore.PutCommandPayloadRequest) (sessionwire.ObjectMetadata, error) {
	if err := c.enter("PutCommandPayload"); err != nil {
		return sessionwire.ObjectMetadata{}, err
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return sessionwire.ObjectMetadata{}, err
	}
	// The store verifies the declared size and digest against the streamed
	// bytes exactly; so does this.
	if uint64(len(body)) != req.SizeBytes || sha256.Sum256(body) != req.SHA256 {
		return sessionwire.ObjectMetadata{}, &sessionstore.ObjectError{Code: sessionstore.ObjectErrorIntegrity, Field: "body"}
	}
	c.uploads++
	c.putBodies = append(c.putBodies, body)
	// A FRESH GENERATION EVERY TIME. Two identical uploads are two distinct
	// objects, which is the real store's documented behaviour.
	return sessionwire.ObjectMetadata{
		Reference: sessionwire.ObjectReference{ObjectID: "object-" + strconv.Itoa(c.uploads)},
		SizeBytes: req.SizeBytes, Digest: hex.EncodeToString(req.SHA256[:]), MediaType: req.MediaType,
	}, nil
}

func (c *servicePublicCreates) AdmitPublicCreate(_ context.Context, req sessionstore.AdmitPublicCreateRequest) (sessionstore.DispositionInboxEntry, bool, error) {
	if err := c.enter("AdmitPublicCreate"); err != nil {
		return sessionstore.DispositionInboxEntry{}, false, err
	}
	id := req.Identity
	reservation, reserved := c.reservations[id.CommandID]
	if !reserved || reservation.Identity != id {
		return sessionstore.DispositionInboxEntry{}, false, &sessionstore.InboxError{Code: sessionstore.InboxErrorCommandMismatch}
	}
	if prior, ok := c.records[id.CommandID]; ok {
		// EXACTLY the store's comparison: PublicCreate, Kind, PayloadDigest,
		// PayloadSize. PayloadObject and Payload are excluded, which is what
		// lets a re-upload under a fresh generation still be a retry.
		d := prior.Record.Descriptor
		if !d.PublicCreate || d.Kind != id.Kind || d.PayloadDigest != id.PayloadDigest || d.PayloadSize != id.PayloadSize {
			return sessionstore.DispositionInboxEntry{}, false, &sessionstore.InboxError{Code: sessionstore.InboxErrorCommandMismatch}
		}
		return prior, false, nil
	}
	entry := sessionstore.DispositionInboxEntry{
		Record: sessionstore.DispositionInboxRecord{
			Descriptor: sessionstore.DispositionCommandDescriptor{
				PublicCreate: true, TenantID: id.TenantID, SessionID: id.SessionID, CommandID: id.CommandID,
				Binding: id.Binding, RuntimeCommandID: reservation.RuntimeCommandID, Kind: id.Kind,
				PayloadDigest: id.PayloadDigest, PayloadSize: id.PayloadSize,
				Payload: req.Payload, PayloadObject: req.PayloadObject,
			},
			AcceptedAt: reservation.AcceptedAt, ApplyDeadline: reservation.ApplyDeadline,
			State: sessionstore.InboxStatePending,
		},
		Revision: 1, AcceptedOrder: uint64(len(c.records) + 1),
	}
	c.records[id.CommandID] = entry
	return entry, true, nil
}

// createFixture is a fixture whose composition CAN serve a create: both halves
// configured, which Config.createsServed requires.
func createFixture(t *testing.T) *serviceFixture {
	t.Helper()
	f := newServiceFixture(t)
	f.configureCreates(t)
	return f
}

func createRequest(id, session string, blocks string) sessionwire.CreateRequest {
	return sessionwire.CreateRequest{
		CommandEnvelope: envelope(id), SessionID: sessionwire.SessionID(session),
		AgentID: "agent-a", Blocks: []byte(blocks),
	}
}

const smallBlocks = `[{"text":"hi"}]`

// oversizedBlocks is a create body past MaxInboxPayloadBytes, so it must go to
// the object store rather than inline. It is a function so each call gets its
// own array and a test cannot mutate another's fixture.
func oversizedBlocks() string {
	return `[{"text":"` + strings.Repeat("x", sessionstore.MaxInboxPayloadBytes) + `"}]`
}

// TestACreateIsAdmittedWithADispositionBindingThisModuleAuthored is step 2's
// happy path and the proof the create route has something to serve.
func TestACreateIsAdmittedWithADispositionBindingThisModuleAuthored(t *testing.T) {
	f := createFixture(t)
	entry, created, err := f.service.AdmitCreate(context.Background(), f.principal, createRequest("create-a", "session-a", smallBlocks))
	if err != nil || !created {
		t.Fatalf("create = (%v, %v)", created, err)
	}
	binding := entry.Record.Descriptor.Binding
	if binding.ProtocolMode != sessionstore.ProtocolModeDisposition {
		t.Errorf("ProtocolMode = %q, want disposition; a legacy session is one no Host can take residency on", binding.ProtocolMode)
	}
	if binding.StorageBindingID != "storage-a" || binding.BindingVersion != "v1" {
		t.Errorf("binding carried %q/%q, not the configured deployment storage identity", binding.StorageBindingID, binding.BindingVersion)
	}
	if entry.Record.State != sessionstore.InboxStatePending {
		t.Errorf("State = %q, want pending", entry.Record.State)
	}
	if !entry.Record.Descriptor.PublicCreate {
		t.Error("the record does not mark itself a public create")
	}
	// The payload is inline, and step 5's object path was NOT taken.
	if f.creates.uploads != 0 {
		t.Errorf("a small create performed %d uploads, want 0", f.creates.uploads)
	}
}

// TestTheRuntimeSessionIDIsDerivedFromTheCreateIdentityAndIsAUUID is the test
// for the single hazard that would have broken step 2 silently.
//
// RuntimeSessionID sits inside SessionBinding, which sits inside
// PublicCreateIdentity, which PreparePublicCreate compares WHOLE. A minted
// random id therefore differs on every attempt and turns every retry into a
// permanent mismatch. The rows below are the comparison, not a constant: each
// varies ONE input and asserts the output moves, and the identical row asserts
// it does not.
func TestTheRuntimeSessionIDIsDerivedFromTheCreateIdentityAndIsAUUID(t *testing.T) {
	base := deriveRuntimeSessionID("tenant-a", "session-a", "command-a")

	t.Run("stable for one identity", func(t *testing.T) {
		if again := deriveRuntimeSessionID("tenant-a", "session-a", "command-a"); again != base {
			t.Fatalf("two derivations of one identity disagree: %q vs %q; every retry would mismatch", base, again)
		}
	})
	for _, test := range []struct {
		name    string
		tenant  sessionwire.TenantID
		session sessionwire.SessionID
		command sessionwire.CommandID
	}{
		{"tenant moves", "tenant-b", "session-a", "command-a"},
		{"session moves", "tenant-a", "session-b", "command-a"},
		{"command moves", "tenant-a", "session-a", "command-b"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := deriveRuntimeSessionID(test.tenant, test.session, test.command); got == base {
				t.Fatalf("%s produced the same runtime session id %q; two sessions would share a runtime identity", test.name, got)
			}
		})
	}
	// The degenerate case a value-axis sweep cannot reach, and it is a
	// COMPARISON BETWEEN TWO DERIVATIONS rather than a row against the base.
	// Without length prefixing these two triples join to the same string, so
	// two different sessions would share a runtime identity. Comparing either
	// one against the base would NOT have caught it -- both differ from the
	// base whether the members are prefixed or not, which is why the first
	// version of this row passed against the unprefixed derivation.
	t.Run("two triples that join to one string stay distinct", func(t *testing.T) {
		left := deriveRuntimeSessionID("a|b", "c", "d")
		right := deriveRuntimeSessionID("a", "b|c", "d")
		if left == right {
			t.Fatalf("two different identities derived the same runtime session id %q; "+
				"the members are not separated unambiguously", left)
		}
	})
	t.Run("is a well formed uuid Host can parse", func(t *testing.T) {
		// Host parses this member into a uuid.UUID before handing it to
		// harness's rig.WithSessionID, so the store's bounded-opaque contract
		// is NOT the binding one here.
		if len(base) != 36 {
			t.Fatalf("len = %d, want 36: %q", len(base), base)
		}
		for i, r := range base {
			switch i {
			case 8, 13, 18, 23:
				if r != '-' {
					t.Fatalf("position %d = %q, want a dash: %q", i, r, base)
				}
			default:
				if !strings.ContainsRune("0123456789abcdef", r) {
					t.Fatalf("position %d = %q, not lowercase hex: %q", i, r, base)
				}
			}
		}
		if base[14] != '8' {
			t.Errorf("version nibble = %q, want 8 (RFC 9562 vendor-defined deterministic): %q", base[14], base)
		}
		if !strings.ContainsRune("89ab", rune(base[19])) {
			t.Errorf("variant nibble = %q, want one of 89ab: %q", base[19], base)
		}
	})
}

// TestCreateReuseReturnsTheOriginalAndEveryConflictingReuseFails is runbook
// A3.1 step 2, in full.
//
// Every row is a COMPARISON: it runs a first create, then a second that varies
// exactly one member of the identity, and asserts the outcome. The "identical"
// row is the positive control without which every refusal row would pass
// against a service that refused everything.
func TestCreateReuseReturnsTheOriginalAndEveryConflictingReuseFails(t *testing.T) {
	for _, test := range []struct {
		name   string
		second func(*serviceFixture) (sessionstore.DispositionInboxEntry, bool, error)
		reused bool
	}{
		{"the same SessionID, CommandID and AgentID is the original", func(f *serviceFixture) (sessionstore.DispositionInboxEntry, bool, error) {
			return f.service.AdmitCreate(context.Background(), f.principal, createRequest("create-a", "session-a", smallBlocks))
		}, true},
		{"a conflicting SessionID under one CommandID fails", func(f *serviceFixture) (sessionstore.DispositionInboxEntry, bool, error) {
			return f.service.AdmitCreate(context.Background(), f.principal, createRequest("create-a", "session-b", smallBlocks))
		}, false},
		{"a conflicting AgentID under one CommandID fails", func(f *serviceFixture) (sessionstore.DispositionInboxEntry, bool, error) {
			req := createRequest("create-a", "session-a", smallBlocks)
			req.AgentID = "agent-b"
			f.targets.target.Key = sessionstore.HostTargetKey{AgentID: "agent-b", RuntimeCompatibilityID: "runtime-v1", Placement: sessionwire.HostPlacementPooled}
			return f.service.AdmitCreate(context.Background(), f.principal, req)
		}, false},
		{"a conflicting payload under one CommandID fails", func(f *serviceFixture) (sessionstore.DispositionInboxEntry, bool, error) {
			return f.service.AdmitCreate(context.Background(), f.principal, createRequest("create-a", "session-a", `[{"text":"different"}]`))
		}, false},
		{"a DIFFERENT create command for an existing SessionID fails", func(f *serviceFixture) (sessionstore.DispositionInboxEntry, bool, error) {
			return f.service.AdmitCreate(context.Background(), f.principal, createRequest("create-b", "session-a", smallBlocks))
		}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := createFixture(t)
			first, created, err := f.service.AdmitCreate(context.Background(), f.principal, createRequest("create-a", "session-a", smallBlocks))
			if err != nil || !created {
				t.Fatalf("the first create was refused: (%v, %v)", created, err)
			}
			second, created, err := test.second(f)
			if test.reused {
				if err != nil {
					t.Fatalf("a reuse of the same identity was refused: %v", err)
				}
				if created {
					t.Error("a reuse reported itself the accepting call")
				}
				if !reflect.DeepEqual(second.Record.Descriptor, first.Record.Descriptor) || second.AcceptedOrder != first.AcceptedOrder {
					t.Errorf("the reuse did not return the original:\n got %+v\nwant %+v", second.Record.Descriptor, first.Record.Descriptor)
				}
				return
			}
			if !IsCode(err, sessionwire.ErrorCodeCommandRejected) {
				t.Fatalf("error = %v, want command_rejected", err)
			}
			if created {
				t.Error("a refused create reported itself the accepting call")
			}
		})
	}
}

// TestAnOversizedCreatePayloadIsStoredByReferenceAndStillRetries is runbook
// A3.1 step 5, and the retry half is the part that was impossible before.
//
// The fake mints a FRESH OBJECT GENERATION on every upload, exactly as the
// store does. So the retry row below re-uploads, receives a DIFFERENT
// ObjectReference, and must STILL be recognised as the same command. That can
// only hold because the store compares PayloadDigest and PayloadSize and
// excludes PayloadObject; on the legacy inbox, which compares PayloadRef, this
// row is the permanent CommandMismatch that made step 5 unimplementable.
func TestAnOversizedCreatePayloadIsStoredByReferenceAndStillRetries(t *testing.T) {
	f := createFixture(t)
	req := createRequest("create-a", "session-a", oversizedBlocks())

	first, created, err := f.service.AdmitCreate(context.Background(), f.principal, req)
	if err != nil || !created {
		t.Fatalf("oversized create = (%v, %v)", created, err)
	}
	object := first.Record.Descriptor.PayloadObject
	if object == nil {
		t.Fatal("an oversized create was admitted with no object reference")
	}
	if len(first.Record.Descriptor.Payload) != 0 {
		t.Error("an oversized create also carried its body inline")
	}
	if f.creates.uploads != 1 {
		t.Fatalf("uploads = %d, want 1", f.creates.uploads)
	}
	// The bytes uploaded are the bytes the identity is a digest OF. A fake that
	// did not verify this would let a mismatched upload pass.
	if got := sha256.Sum256(f.creates.putBodies[0]); hex.EncodeToString(got[:]) != first.Record.Descriptor.PayloadDigest {
		t.Error("the uploaded bytes are not the ones the descriptor's digest names")
	}

	retry, created, err := f.service.AdmitCreate(context.Background(), f.principal, req)
	if err != nil {
		t.Fatalf("the retry of an oversized create was refused: %v", err)
	}
	if created {
		t.Error("the retry reported itself the accepting call")
	}
	if f.creates.uploads != 2 {
		t.Fatalf("uploads = %d, want 2: the retry must re-upload, which is what makes this test meaningful", f.creates.uploads)
	}
	// The retry returns the WINNER's representation, so its object reference
	// is the FIRST upload's. That is correct and is the point: the second
	// upload's object is simply orphaned.
	if retry.Record.Descriptor.PayloadObject.Reference != object.Reference {
		t.Error("the retry did not return the winning object reference")
	}
	// And the anti-vacuity check the row above rests on: the fake really does
	// mint a fresh generation for identical bytes, so the retry genuinely
	// carried a different reference INTO AdmitPublicCreate and was matched on
	// digest and size rather than on the object. Without this, a fake that
	// happened to reuse references would make this test prove nothing.
	if len(f.creates.putBodies) != 2 || !bytes.Equal(f.creates.putBodies[0], f.creates.putBodies[1]) {
		t.Fatal("the two uploads did not stream identical bytes")
	}
	again, err := f.creates.PutCommandPayload(context.Background(), sessionstore.PutCommandPayloadRequest{
		TenantID: "tenant-a", SessionID: "session-a", SizeBytes: uint64(len(f.creates.putBodies[0])),
		SHA256: sha256.Sum256(f.creates.putBodies[0]), Body: bytes.NewReader(f.creates.putBodies[0]),
	})
	if err != nil {
		t.Fatal(err)
	}
	if again.Reference == object.Reference {
		t.Fatal("the fake reused an object generation for identical bytes; it is looser than the store and this test proves nothing")
	}
	if !reflect.DeepEqual(retry.Record.Descriptor, first.Record.Descriptor) {
		t.Errorf("the retry did not return the winning record:\n got %+v\nwant %+v", retry.Record.Descriptor, first.Record.Descriptor)
	}
}

// TestThePayloadCeilingIsTheComparisonNotAConstant rows the BOUNDARY rather
// than one value on each side, because "oversized" is a comparison and a test
// that drove one large and one small body could not see an off-by-one in it.
func TestThePayloadCeilingIsTheComparisonNotAConstant(t *testing.T) {
	// The canonical command is the whole JSON request, not just the blocks, so
	// the body is sized by MEASURING rather than by arithmetic on the ceiling.
	// The filler is a single ASCII byte that JSON does not escape, so the
	// canonical size is linear in the block length and one measurement fixes
	// the offset.
	sizeFor := func(t *testing.T, blockBytes int) (sessionwire.CreateRequest, uint64) {
		t.Helper()
		req := createRequest("create-a", "session-a", `[{"text":"`+strings.Repeat("x", blockBytes)+`"}]`)
		payload, err := canonicalCommand(req)
		if err != nil {
			t.Fatal(err)
		}
		return req, uint64(len(payload))
	}
	_, empty := sizeFor(t, 0)
	exact := int(sessionstore.MaxInboxPayloadBytes - empty)
	if exact <= 1 {
		t.Fatalf("an empty create is already %d bytes against a %d ceiling; the boundary cannot be rowed",
			empty, sessionstore.MaxInboxPayloadBytes)
	}
	// The offset must really be linear, or the three rows below are not the
	// three positions they claim to be.
	if _, got := sizeFor(t, exact); got != sessionstore.MaxInboxPayloadBytes {
		t.Fatalf("the canonical size is not linear in the block length: %d bytes at the computed boundary, want %d",
			got, sessionstore.MaxInboxPayloadBytes)
	}
	for _, test := range []struct {
		name    string
		blocks  int
		uploads int
	}{
		{"one byte under the ceiling is inline", exact - 1, 0},
		{"exactly the ceiling is inline", exact, 0},
		{"one byte over the ceiling goes to the object store", exact + 1, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := createFixture(t)
			req, size := sizeFor(t, test.blocks)
			entry, _, err := f.service.AdmitCreate(context.Background(), f.principal, req)
			if err != nil {
				t.Fatalf("create of %d payload bytes: %v", size, err)
			}
			if f.creates.uploads != test.uploads {
				t.Fatalf("%d payload bytes performed %d uploads, want %d (ceiling %d)",
					size, f.creates.uploads, test.uploads, sessionstore.MaxInboxPayloadBytes)
			}
			if inline := len(entry.Record.Descriptor.Payload) != 0; inline != (test.uploads == 0) {
				t.Errorf("%d payload bytes: inline = %v, want %v", size, inline, test.uploads == 0)
			}
		})
	}
}

// TestACompositionThatCannotAuthorABindingRefusesTheCreate rows the
// ENUMERATION of unconfigured compositions rather than one instance.
//
// Both halves are required and neither implies the other, so all three ways to
// be unconfigured are driven. Every row must refuse BEFORE any durable write:
// a create that reserved an identity and then discovered it had no binding
// would have written the identity it could not complete.
func TestACompositionThatCannotAuthorABindingRefusesTheCreate(t *testing.T) {
	for _, test := range []struct {
		name   string
		adjust func(*Config)
	}{
		{"no store and no binding", func(cfg *Config) { cfg.PublicCreates = nil; cfg.Binding = SessionBindingTemplate{} }},
		{"a store but no binding", func(cfg *Config) { cfg.Binding = SessionBindingTemplate{} }},
		{"a binding but no store", func(cfg *Config) {
			cfg.PublicCreates = nil
			cfg.Binding = SessionBindingTemplate{StorageBindingID: "storage-a", BindingVersion: "v1"}
		}},
		{"a half-filled binding names no storage", func(cfg *Config) {
			cfg.Binding = SessionBindingTemplate{BindingVersion: "v1"}
		}},
		{"a half-filled binding names no version", func(cfg *Config) {
			cfg.Binding = SessionBindingTemplate{StorageBindingID: "storage-a"}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newServiceFixture(t)
			f.rebuild(t, test.adjust)
			_, created, err := f.service.AdmitCreate(context.Background(), f.principal, createRequest("create-a", "session-a", smallBlocks))
			if !errors.Is(err, ErrCreateBindingUnconfigured) || !IsCode(err, sessionwire.ErrorCodeRuntimeUnavailable) {
				t.Fatalf("error = %v, want an unconfigured-binding runtime_unavailable", err)
			}
			if created {
				t.Error("an unserved create reported itself the accepting call")
			}
			if len(f.creates.reservations) != 0 || len(f.creates.records) != 0 || f.creates.uploads != 0 {
				t.Error("a create with no binding to author still wrote durable state")
			}
			// The positive control: the SAME fixture, configured, is admitted.
			// Without it every assertion above passes against a service that
			// refuses every create for any reason.
			ok := createFixture(t)
			if _, created, err := ok.service.AdmitCreate(context.Background(), ok.principal, createRequest("create-a", "session-a", smallBlocks)); err != nil || !created {
				t.Fatalf("the configured control was refused: (%v, %v)", created, err)
			}
		})
	}
}

// TestEveryGuardInTheCreateChainIsReachedInOrder is the early-return sweep.
//
// In an early-return chain only a row that PASSES guards 1..k-1 is evidence
// about guard k, so each row below names the guard it is evidence for AND the
// refusal it must produce. A row that tripped an earlier guard would look like
// coverage of a later one while testing nothing about it.
//
// The `reached` column is what makes that checkable rather than asserted: it
// names the last collaborator a row is expected to reach, so a row that
// short-circuits earlier than its guard fails here instead of passing quietly.
func TestEveryGuardInTheCreateChainIsReachedInOrder(t *testing.T) {
	for _, test := range []struct {
		guard     string
		configure func(*serviceFixture)
		request   sessionwire.CreateRequest
		code      sessionwire.ErrorCode
		// reached is the set of dependency methods this row must have called.
		reached []string
	}{
		{
			guard:   "1. the request validates",
			request: createRequest("", "session-a", smallBlocks),
			code:    sessionwire.ErrorCodeInvalidRequest,
			reached: nil,
		},
		{
			guard:     "3. the principal is authorized",
			configure: func(f *serviceFixture) { f.auth.err = errors.New("denied") },
			request:   createRequest("create-a", "session-a", smallBlocks),
			reached:   []string{"AuthorizeControl"},
		},
		{
			guard:     "5. the agent resolves to a known target",
			configure: func(f *serviceFixture) { f.targets.known = false },
			request:   createRequest("create-a", "session-a", smallBlocks),
			code:      sessionwire.ErrorCodeRuntimeUnavailable,
			reached:   []string{"AuthorizeControl", "ResolveAgent"},
		},
		{
			guard:     "5b. the resolved target is THIS agent's",
			configure: func(f *serviceFixture) { f.targets.target.Key.AgentID = "agent-b" },
			request:   createRequest("create-a", "session-a", smallBlocks),
			code:      sessionwire.ErrorCodeRuntimeUnavailable,
			reached:   []string{"AuthorizeControl", "ResolveAgent"},
		},
		{
			guard:     "6. this composition can author a binding",
			configure: func(f *serviceFixture) { f.rebuild(t, func(cfg *Config) { cfg.Binding = SessionBindingTemplate{} }) },
			request:   createRequest("create-a", "session-a", smallBlocks),
			code:      sessionwire.ErrorCodeRuntimeUnavailable,
			reached:   []string{"AuthorizeControl", "ResolveAgent"},
		},
		{
			guard:     "7. the reservation is filed",
			configure: func(f *serviceFixture) { f.creates.failing = "PreparePublicCreate" },
			request:   createRequest("create-a", "session-a", smallBlocks),
			// A dependency fault is NOT a classified refusal; it carries no
			// public code. That is this module's fault/refusal split.
			reached: []string{"AuthorizeControl", "ResolveAgent", "PreparePublicCreate"},
		},
		{
			guard:     "8. the oversized payload is stored",
			configure: func(f *serviceFixture) { f.creates.failing = "PutCommandPayload" },
			request:   createRequest("create-a", "session-a", oversizedBlocks()),
			reached:   []string{"AuthorizeControl", "ResolveAgent", "PreparePublicCreate", "PutCommandPayload"},
		},
		{
			guard:     "9. the command is admitted",
			configure: func(f *serviceFixture) { f.creates.failing = "AdmitPublicCreate" },
			request:   createRequest("create-a", "session-a", smallBlocks),
			reached:   []string{"AuthorizeControl", "ResolveAgent", "PreparePublicCreate", "AdmitPublicCreate"},
		},
	} {
		t.Run(test.guard, func(t *testing.T) {
			f := createFixture(t)
			if test.configure != nil {
				test.configure(f)
			}
			_, created, err := f.service.AdmitCreate(context.Background(), f.principal, test.request)
			if err == nil {
				t.Fatal("the create was admitted")
			}
			if created {
				t.Error("a refused create reported itself the accepting call")
			}
			if test.code != "" && !IsCode(err, test.code) {
				t.Errorf("error = %v, want %q", err, test.code)
			}
			if test.code == "" {
				var classified *Error
				if errors.As(err, &classified) {
					t.Errorf("a dependency fault was classified %q; a fault is not a decision about the command", classified.Code)
				}
			}
			// The row reached exactly its own guard and no further.
			got := map[string]bool{}
			for _, injector := range []*faultInjector{
				&f.auth.faultInjector, &f.targets.faultInjector, &f.creates.faultInjector,
			} {
				for method := range injector.called {
					got[method] = true
				}
			}
			for _, method := range test.reached {
				if !got[method] {
					t.Errorf("the row for %q never reached %s, so it is not evidence about that guard", test.guard, method)
				}
				delete(got, method)
			}
			for method := range got {
				t.Errorf("the row for %q also reached %s, so it passed the guard it names", test.guard, method)
			}
		})
	}
}
