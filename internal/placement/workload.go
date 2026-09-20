package placement

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
)

// ErrWorkloadEndpointUnsupported means a dedicated workload controller has no
// pre-attach endpoint discovery seam. Its post-attach registry observation
// cannot be used to discover the first endpoint.
var ErrWorkloadEndpointUnsupported = errors.New("placement: workload controller cannot discover a pre-attach endpoint")

// DedicatedEndpoint identifies the ready Host process created for one intent.
// It is an address and a fence, not evidence of session residency or capacity.
type DedicatedEndpoint struct {
	HostID           sessionwire.HostID
	HostGeneration   uint64
	InternalEndpoint sessionwire.InternalEndpoint
}

// WorkloadEndpointDiscovery is an optional pre-attach seam. Its signature
// names only Core and SessionStore types, so a separately released controller
// can implement it without importing Factory's internal package. A false
// ready answer leaves placement pending.
type WorkloadEndpointDiscovery interface {
	WorkloadEndpoint(context.Context, sessionstore.PlacementIntent) (sessionwire.HostID, uint64, sessionwire.InternalEndpoint, bool, error)
}

// WorkloadController is the consumer-owned domain seam for a dedicated
// workload's lifecycle.
//
// Every argument and result is a Core or SessionStore record. In particular,
// the platform adapter receives the Factory-authored PlacementIntent and
// returns the Host-owned registry/drain observations; no Kubernetes, Nomad or
// other platform type can cross this boundary. The adapter may use the intent
// to derive its own platform object name, but the placement package never sees
// that object.
//
// EnsureWorkload is idempotent for an unchanged intent. ObserveWorkload reports
// whether a Host registry observation exists for that intent. RequestDrain is
// the only lifecycle operation that initiates Host draining, and
// DeleteWorkload is called only after the caller has observed a completed drain.
// Keeping the ordering in the caller makes a platform adapter unable to treat a
// transport close or a platform deletion as graceful release.
type WorkloadController interface {
	EnsureWorkload(context.Context, sessionstore.PlacementIntent) error
	ObserveWorkload(context.Context, sessionstore.PlacementIntent) (sessionwire.HostLinkRegistryObservation, bool, error)
	RequestDrain(context.Context, sessionstore.PlacementIntent) (sessionwire.HostLinkDrainObservation, error)
	DeleteWorkload(context.Context, sessionstore.PlacementIntent) error
}

// WorkloadIdentity is the stable identity of one desired workload revision.
//
// Name is deliberately a digest rather than a concatenation of user-controlled
// identifiers. TenantID and SessionID may contain characters that are legal in
// Core but unsafe in platform names, and leaking either into a workload name
// also exposes tenant data to platform operators. The source fields remain
// available to a domain caller for correlation; only Name is intended for use as
// a platform object name.
type WorkloadIdentity struct {
	Name                   string
	TenantID               sessionwire.TenantID
	SessionID              sessionwire.SessionID
	AgentID                sessionwire.AgentID
	RuntimeCompatibilityID string
	Generation             uint64
}

// DeriveWorkloadIdentity deterministically derives one platform-safe workload
// identity from a desired placement intent.
//
// The framing is length-prefixed and ordered, so delimiters or UTF-8 bytes in
// tenant/session/agent/runtime identifiers cannot create an alternate preimage.
// Desired generation is included as a fixed-width integer: a changed desired
// revision receives a distinct workload identity, while a restart deriving the
// same revision receives the exact same name. The 56 hexadecimal digest bytes
// plus the prefix fit Kubernetes' 63-character DNS-label limit and remain valid
// for stricter platform name rules as well.
func DeriveWorkloadIdentity(intent sessionstore.PlacementIntent) WorkloadIdentity {
	digest := sha256.New()
	writeIdentityField(digest, []byte(intent.TenantID))
	writeIdentityField(digest, []byte(intent.SessionID))
	writeIdentityField(digest, []byte(intent.AgentID))
	writeIdentityField(digest, []byte(intent.RuntimeCompatibilityID))
	var generation [8]byte
	binary.BigEndian.PutUint64(generation[:], intent.Generation)
	writeIdentityField(digest, generation[:])
	// A 28-byte SHA-256 prefix gives 224 digest bits (112-bit birthday
	// collision strength) while leaving room for the safe-name prefix under the
	// 63-byte limit.
	name := "lrw-" + hex.EncodeToString(digest.Sum(nil)[:28])
	return WorkloadIdentity{
		Name:                   name,
		TenantID:               intent.TenantID,
		SessionID:              intent.SessionID,
		AgentID:                intent.AgentID,
		RuntimeCompatibilityID: intent.RuntimeCompatibilityID,
		Generation:             intent.Generation,
	}
}

// WorkloadName returns only the platform-safe name derived from intent.
func WorkloadName(intent sessionstore.PlacementIntent) string {
	return DeriveWorkloadIdentity(intent).Name
}

func writeIdentityField(dst interface{ Write([]byte) (int, error) }, field []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(field)))
	_, _ = dst.Write(length[:])
	_, _ = dst.Write(field)
}
