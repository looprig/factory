package identity

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// This file is the vocabulary a DEPLOYER implements.
//
// Factory's HTTP and ClientLink authentication is not pluggable at the
// authenticator: which header or cookie a credential is read from, which
// credential wins when a request presents two, and what the operation context
// records about the one that authenticated are decided in exactly one place --
// github.com/looprig/factory/internal/identity -- because a second answer to
// "which credential authenticated this request" is a CSRF guard that skips.
//
// It is pluggable at the VERIFIER, which is the only part a deployment really
// owns: what makes a presented credential valid, and what it asserts. Verifier
// and the two types its signature names therefore live here, in the public
// package, because Go forbids naming an internal type from outside the module
// and a seam nobody outside can implement is not a seam.

// RedactedPlaceholder is what a scrubbed credential is replaced by. It is a
// fixed string rather than a length-preserving mask, because a mask discloses
// the length of the secret.
//
// It is exported because it is the one piece of the redaction a reader outside
// this package has to name: internal/identity scrubs presented secrets out of a
// verifier's message with it, and there must be ONE such string rather than one
// per package that redacts.
const RedactedPlaceholder = "REDACTED"

// Source names where a credential was presented. It is part of a diagnosable
// message: "the cookie credential was not verified" and "the bearer credential
// was not verified" are different operational problems.
type Source string

const (
	// SourceBearer is an Authorization: Bearer header.
	SourceBearer Source = "bearer"
	// SourceCookie is the browser session cookie.
	SourceCookie Source = "cookie"
	// SourceLink is a ClientLink connect token.
	SourceLink Source = "link"
)

// Credential is presented authentication material on its way to a Verifier.
//
// Its value is unexported and every rendering a consumer can reach is
// overridden, because the likeliest way a token reaches a log is that somebody
// formatted the value they were handed.
//
// The four methods are NOT four independent mechanisms, and the difference was
// measured rather than assumed. Deleting GoString or MarshalJSON changes what
// %#v and encoding/json produce; deleting LogValue does not change what
// slog.TextHandler produces, because its KindAny path falls back to fmt and
// reaches String. LogValue is kept because slog.LogValuer is part of the type's
// contract -- a handler that reflects over the value rather than formatting it
// sees the difference -- and it is held by an interface assertion rather than
// by a rendering, since no rendering reads it.
//
// Value is still readable, because a Verifier that cannot read the credential
// cannot verify it. The claim is that a Credential does not leak by ACCIDENT.
type Credential struct {
	source Source
	value  string
}

// NewCredential builds a credential presented at source.
func NewCredential(source Source, value string) Credential {
	return Credential{source: source, value: value}
}

// Source reports where the credential was presented.
func (c Credential) Source() Source { return c.source }

// Value is the material a Verifier verifies.
func (c Credential) Value() string { return c.value }

// String renders the credential for %v, %s and %q without its value.
func (c Credential) String() string {
	return "identity.Credential{source:" + string(c.source) + ", value:" + RedactedPlaceholder + "}"
}

// GoString renders the credential for %#v, which ignores String and would
// otherwise print every unexported field.
func (c Credential) GoString() string { return c.String() }

// LogValue renders the credential for log/slog, which would otherwise reflect
// over the struct rather than consult String.
func (c Credential) LogValue() slog.Value { return slog.StringValue(c.String()) }

// MarshalJSON renders the credential for a structured encoder. encoding/json
// would emit {} today, since every field is unexported, but that is a property
// of the field set rather than a decision, and it says nothing to whoever reads
// the record.
func (c Credential) MarshalJSON() ([]byte, error) { return json.Marshal(c.String()) }

// Claims is what a verified credential asserts.
//
// Tenant may be empty, and that is the local deployment's case rather than an
// error: a credential minted by a single-tenant local composition names no
// tenant and receives the configured default. See tenantFor for why that
// fallback is confined to an actor.
type Claims struct {
	// Tenant is the tenant scope the credential was issued for.
	Tenant sessionwire.TenantID
	// Subject identifies the caller within that tenant.
	Subject string
	// Kind separates a human actor from a Factory service identity.
	Kind Kind
	// ExpiresAt is when the credential stops being accepted. A credential with
	// no expiry is rejected: it is one that can never be logged out.
	ExpiresAt time.Time
}

// Verifier verifies a presented credential and reports what it asserts.
//
// It is the seam through which "stateless or shared durably" is satisfied. This
// package keeps nothing between calls, so whatever a deployment uses to decide
// a credential -- a signature over a shared key, a token introspection
// endpoint, a shared session table -- is reachable from every Factory replica
// by construction.
//
// An implementation reports a rejected credential by returning an error that
// wraps identity.ErrUnauthenticated. Any other error is treated as the verifier
// being unable to answer; see ErrVerifierUnavailable.
type Verifier interface {
	VerifyCredential(ctx context.Context, credential Credential) (Claims, error)
}
