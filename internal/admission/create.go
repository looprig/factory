package admission

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
)

// SessionBindingTemplate is the deployment-configuration half of the immutable
// SessionBinding a V1 create pins into its session, forever.
//
// It carries only the two members a DEPLOYMENT owns. The other two are not
// configuration and are deliberately absent: RuntimeSessionID is derived per
// create (see deriveRuntimeSessionID) and ProtocolMode is this module's own
// choice, not a knob -- see bind.
//
// Why these two are exactly the deployment's: they are the two members
// ObjectStoreResolver keys on. A resolver "refuses any configuration this
// deployment does not know", so a create that pinned a StorageBindingID the
// resolver cannot resolve would write a session whose objects are permanently
// unreadable -- and SessionBinding is immutable after create, so there is no
// repair. The composition therefore refuses this template without a resolver,
// which is what makes this configuration rather than a guess.
type SessionBindingTemplate struct {
	StorageBindingID string
	BindingVersion   string
}

// configured reports whether a create can be served at all.
func (t SessionBindingTemplate) configured() bool {
	return t.StorageBindingID != "" && t.BindingVersion != ""
}

// ErrCreateBindingUnconfigured reports a V1 create reached a composition that
// named no SessionBindingTemplate. It is NOT a protocol gap -- the released
// store admits a public create -- it is a deployment that did not say which
// storage configuration its sessions are pinned to. Guessing one would be
// irreversible.
var ErrCreateBindingUnconfigured = errors.New("admission: V1 create requires a configured session binding")

// PublicCreateStore is the durable public-create plane.
//
// It is the disposition family, not the legacy inbox, and that is forced
// rather than chosen: a Host takes residency through AcquireResidency, which
// pins ProtocolModeDisposition, so a legacy-bound session is one no Host can
// ever run.
type PublicCreateStore interface {
	PreparePublicCreate(context.Context, sessionstore.PreparePublicCreateRequest) (sessionstore.PublicCreatePreparation, error)
	PutCommandPayload(context.Context, sessionstore.PutCommandPayloadRequest) (sessionwire.ObjectMetadata, error)
	AdmitPublicCreate(context.Context, sessionstore.AdmitPublicCreateRequest) (sessionstore.DispositionInboxEntry, bool, error)
}

// runtimeSessionNamespace separates this derivation from every other use of
// the same inputs, so a digest computed elsewhere over the same three ids can
// never collide with a runtime session identity.
const runtimeSessionNamespace = "looprig/factory/runtime-session-id/v1"

// deriveRuntimeSessionID derives the session's runtime identity from the create
// command's own identity, DETERMINISTICALLY, and that is a correctness
// requirement rather than a stylistic one.
//
// RuntimeSessionID is a member of SessionBinding, SessionBinding is a member of
// PublicCreateIdentity, and PreparePublicCreate compares the WHOLE identity
// against the stored reservation (public_create.go publicCreateWinner,
// `r.Identity != identity` -> InboxErrorCommandMismatch). A freshly minted
// random id would therefore differ on the second attempt and turn EVERY
// legitimate retry into a permanent mismatch -- which is precisely runbook step
// 2's first clause ("the same pair is reused after an unknown outcome")
// inverted. It is the same defect shape that made step 5 unimplementable on the
// legacy inbox, where PutObject minted a fresh object generation per call, and
// it is worth naming because the two look nothing alike at the call site.
//
// The output is a well-formed RFC 9562 version-8 UUID: version 8 is the
// registered shape for a vendor-defined, deterministically derived UUID, which
// is exactly what this is. That matters beyond tidiness -- Host parses this
// member into a uuid.UUID before handing it to harness's rig.WithSessionID, so
// a bounded opaque string the store would accept is NOT sufficient here. The
// store's validateOpaque is the weaker contract of the two and this derivation
// satisfies the stronger one.
func deriveRuntimeSessionID(tenant sessionwire.TenantID, session sessionwire.SessionID, command sessionwire.CommandID) string {
	// Length-prefixed so no two different triples can produce one preimage by
	// moving a separator into a member's own bytes.
	sum := sha256.Sum256(fmt.Appendf(nil, "%s|%d:%s|%d:%s|%d:%s",
		runtimeSessionNamespace, len(tenant), tenant, len(session), session, len(command), command))
	var raw [16]byte
	copy(raw[:], sum[:16])
	raw[6] = raw[6]&0x0f | 0x80 // version 8
	raw[8] = raw[8]&0x3f | 0x80 // RFC 9562 variant
	h := hex.EncodeToString(raw[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

// bind builds the immutable binding this create pins into the session.
//
// ProtocolMode is disposition and is not configurable. A deployment that could
// choose legacy here would be choosing a session no Host can take residency on,
// which is not a deployment decision but a defect.
func (s *Service) bind(tenant sessionwire.TenantID, session sessionwire.SessionID, command sessionwire.CommandID) sessionstore.SessionBinding {
	return sessionstore.SessionBinding{
		StorageBindingID: s.cfg.Binding.StorageBindingID,
		BindingVersion:   s.cfg.Binding.BindingVersion,
		RuntimeSessionID: deriveRuntimeSessionID(tenant, session, command),
		ProtocolMode:     sessionstore.ProtocolModeDisposition,
	}
}

// payloadIdentity is the content identity the store compares a retry on:
// lowercase SHA-256 hex and the exact byte length. It is computed over the same
// canonical bytes whether they end up inline or in an object, which is what
// lets an oversized retry re-upload under a fresh object generation and still
// match -- AdmitDispositionCommand compares PayloadDigest and PayloadSize and
// deliberately excludes PayloadObject.
func payloadIdentity(payload []byte) (string, uint64) {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), uint64(len(payload))
}

// createIdentity assembles the immutable identity both the reservation and the
// admitted record are keyed by.
func (s *Service) createIdentity(tenant sessionwire.TenantID, req sessionwire.CreateRequest, target Target, payload []byte) sessionstore.PublicCreateIdentity {
	digest, size := payloadIdentity(payload)
	return sessionstore.PublicCreateIdentity{
		TenantID: tenant, SessionID: req.SessionID, CommandID: req.CommandID,
		Target:        target.Key,
		Binding:       s.bind(tenant, req.SessionID, req.CommandID),
		Kind:          CommandCreate,
		PayloadDigest: digest,
		PayloadSize:   size,
	}
}

// admitPublicCreate is runbook A3.1 steps 2 and 5.
//
// The order is forced by the store and is not an implementation preference:
// PreparePublicCreate reserves the identity BEFORE the payload bytes need to
// exist, so the oversized upload in step 5 happens between the reservation and
// the admission, against a session whose disposition catalog already exists.
// PutCommandPayload refuses a session with no disposition catalog, so the
// reverse order cannot work at all.
func (s *Service) admitPublicCreate(ctx context.Context, tenant sessionwire.TenantID, req sessionwire.CreateRequest, target Target, payload []byte) (sessionstore.DispositionInboxEntry, bool, error) {
	identity := s.createIdentity(tenant, req, target, payload)
	runtimeID, err := s.cfg.IDs.NewUUID()
	if err != nil {
		return sessionstore.DispositionInboxEntry{}, false, err
	}
	now := s.cfg.Clock.Now().UTC()
	if _, err := s.cfg.PublicCreates.PreparePublicCreate(ctx, sessionstore.PreparePublicCreateRequest{
		Identity:                 identity,
		ProposedRuntimeCommandID: sessionstore.RuntimeCommandID(runtimeID),
		AcceptedAt:               now,
		ApplyDeadline:            now.Add(s.cfg.ApplyDeadline),
		InitialWorkload:          target.Workload,
	}); err != nil {
		return sessionstore.DispositionInboxEntry{}, false, createRefusal(err)
	}
	admit := sessionstore.AdmitPublicCreateRequest{Identity: identity}
	if identity.PayloadSize > sessionstore.MaxInboxPayloadBytes {
		object, err := s.putCommandPayload(ctx, tenant, req.SessionID, payload)
		if err != nil {
			return sessionstore.DispositionInboxEntry{}, false, err
		}
		admit.PayloadObject = &object
	} else {
		admit.Payload = payload
	}
	entry, created, err := s.cfg.PublicCreates.AdmitPublicCreate(ctx, admit)
	if err != nil {
		return sessionstore.DispositionInboxEntry{}, false, createRefusal(err)
	}
	return entry, created, nil
}

// putCommandPayload is step 5's upload. The digest and size it declares are the
// SAME pair the identity carries, so the store's exactness check and this
// module's retry comparison cannot disagree about what was uploaded.
func (s *Service) putCommandPayload(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, payload []byte) (sessionwire.ObjectMetadata, error) {
	object, err := s.cfg.PublicCreates.PutCommandPayload(ctx, sessionstore.PutCommandPayloadRequest{
		TenantID: tenant, SessionID: session,
		SizeBytes: uint64(len(payload)), SHA256: sha256.Sum256(payload),
		MediaType: canonicalCommandMediaType, Body: bytesReader(payload),
	})
	if err != nil {
		return sessionwire.ObjectMetadata{}, fmt.Errorf("admission: store the oversized create payload: %w", err)
	}
	return object, nil
}

const canonicalCommandMediaType = "application/json"

// createRefusal classifies the store's answer to a create.
//
// A mismatch is the ONE store answer that is a decision about the caller's
// command rather than a fault: the caller reused a CommandID for a different
// create, which is exactly what runbook step 2 asks to fail. Everything else
// stays a fault and reaches the caller through the edge's fault channel, for
// the reason resolveTargetFault gives.
func createRefusal(err error) error {
	var inbox *sessionstore.InboxError
	if errors.As(err, &inbox) && inbox.Code == sessionstore.InboxErrorCommandMismatch {
		return refusal(sessionwire.ErrorCodeCommandRejected, err)
	}
	var catalog *sessionstore.CatalogError
	if errors.As(err, &catalog) && catalog.Code == sessionstore.CatalogErrorConflict {
		return refusal(sessionwire.ErrorCodeCommandRejected, err)
	}
	return fmt.Errorf("admission: admit the public create: %w", err)
}

// bytesReader is the single-use body PutCommandPayload streams and verifies.
func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }
