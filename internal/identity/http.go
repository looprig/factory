// Package identity derives Factory's principals from inbound credentials.
//
// It is internal, and that split is deliberate rather than incidental. The
// vocabulary an external Authenticator must NAME -- Principal, Kind,
// ErrUnauthenticated -- lives in the public github.com/looprig/factory/identity
// package because Go forbids naming an internal type from outside the module.
// Everything here is the derivation itself: which header or cookie a credential
// is read from, how a tenant is decided, and which fields of a request reach an
// operation context. None of that has an external implementer, so none of it is
// vocabulary a deployer has to learn.
//
// Two properties of this package are load-bearing and easy to lose in a later
// change.
//
// A tenant comes ONLY from verified claims or from configuration. Nothing here
// reads the URL, the path, a path value, the body, a form or the Host header,
// and TestTheDerivationNamesNoTenantCarrier holds that structurally, over the
// parsed file, because a behavioural sweep can only cover the carriers somebody
// thought of. There is therefore nothing to "reject": a conflicting tenant in a
// request is not weighed against the credential and lost, it is never read.
//
// An Authenticator holds no per-process state. Every decision is a function of
// the presented credential, the injected Verifier, and the injected Clock, so a
// browser whose WebSocket reconnects to a different Factory replica is
// authenticated there with no session handoff. Section 10 of the spec requires
// exactly that, and TestAuthenticatorDeclaresNoPerProcessState is the structural
// half of it -- a per-process cache is invisible to a two-replica behavioural
// test in every case where the cache misses.
package identity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	factoryidentity "github.com/looprig/factory/identity"
)

// ErrInvalidConfig is the class of every NewAuthenticator rejection.
var ErrInvalidConfig = errors.New("identity: invalid authenticator configuration")

// ErrVerifierUnavailable reports that the verifier could not decide, so nothing
// is known about the caller.
//
// It deliberately does NOT wrap identity.ErrUnauthenticated. The two demand
// opposite responses: an unauthenticated caller is challenged, while a verifier
// that cannot answer is a fault on Factory's side, and reporting it as a
// rejected credential signs every user of a running deployment out for as long
// as the credential service is down.
var ErrVerifierUnavailable = errors.New("identity: credential verifier is unavailable")

// DefaultCookieName is the browser session cookie a request is read from when
// it carries no Authorization header.
const DefaultCookieName = "factory_session"

const (
	authorizationHeader = "Authorization"
	bearerScheme        = "bearer"
	traceparentHeader   = "traceparent"
)

// redactedPlaceholder is what a scrubbed credential is replaced by. It is a
// fixed string rather than a length-preserving mask, because a mask discloses
// the length of the secret.
const redactedPlaceholder = "REDACTED"

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
// formatted the value they were handed. Each rendering is defeated by a
// DIFFERENT method -- String for %v/%s/%q, GoString for %#v, LogValue for slog,
// MarshalJSON for a structured encoder -- so none of them covers the others,
// and none of them is decoration.
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
	return "identity.Credential{source:" + string(c.source) + ", value:" + redactedPlaceholder + "}"
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
	Kind factoryidentity.Kind
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

// Clock is the time seam. It is narrower than Factory's public Clock because
// expiry is the only time this package reads; the public seam satisfies it with
// no adapter, which server_test.go holds.
type Clock interface {
	Now() time.Time
}

// Config configures credential derivation.
type Config struct {
	// Verifier decides credentials. It is required.
	Verifier Verifier

	// Clock decides expiry. It defaults to the system clock.
	Clock Clock

	// CookieName is the browser session cookie. It defaults to
	// DefaultCookieName.
	CookieName string

	// DefaultTenant is the tenant an actor credential that names none is
	// scoped to. Leaving it empty requires every credential to name its
	// tenant, which is the cloud deployment's shape.
	DefaultTenant sessionwire.TenantID
}

// Authenticator derives principals from requests and ClientLink tokens.
//
// Its fields are the configuration and the two seams, and nothing else. A field
// able to accumulate state would break the reconnect property this package
// exists to hold, so TestAuthenticatorDeclaresNoPerProcessState refuses one.
type Authenticator struct {
	verifier      Verifier
	clock         Clock
	cookieName    string
	defaultTenant sessionwire.TenantID
}

// NewAuthenticator validates a configuration and returns the authenticator.
//
// A rejection returns a nil Authenticator, so a caller that ignores the error
// cannot end up with one that authenticates against a half-built configuration.
func NewAuthenticator(cfg Config) (*Authenticator, error) {
	if cfg.Verifier == nil {
		return nil, fmt.Errorf("%w: Verifier is required", ErrInvalidConfig)
	}
	if cfg.Clock == nil {
		cfg.Clock = systemClock{}
	}
	if cfg.CookieName == "" {
		cfg.CookieName = DefaultCookieName
	}
	// A cookie name that is not a token is not merely ugly: net/http drops such
	// a cookie when it writes one, so the composition would look configured and
	// never authenticate anybody.
	if !isCookieToken(cfg.CookieName) {
		return nil, fmt.Errorf("%w: CookieName %q is not a valid cookie name", ErrInvalidConfig, cfg.CookieName)
	}
	if cfg.DefaultTenant != "" {
		if err := cfg.DefaultTenant.Validate(); err != nil {
			return nil, fmt.Errorf("%w: DefaultTenant is not a valid identity: %v", ErrInvalidConfig, err)
		}
	}
	return &Authenticator{
		verifier:      cfg.Verifier,
		clock:         cfg.Clock,
		cookieName:    cfg.CookieName,
		defaultTenant: cfg.DefaultTenant,
	}, nil
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// AuthenticateRequest derives the principal a request's verified credentials
// name. It reads the Authorization header and the session cookie, and nothing
// else that a caller controls.
func (a *Authenticator) AuthenticateRequest(ctx context.Context, r *http.Request) (factoryidentity.Principal, error) {
	if r == nil {
		return factoryidentity.Principal{}, fmt.Errorf("%w: there is no request to authenticate", factoryidentity.ErrUnauthenticated)
	}
	credential, err := a.presented(r)
	if err != nil {
		return factoryidentity.Principal{}, err
	}
	return a.authenticate(ctx, credential)
}

// AuthenticateLink derives the principal a ClientLink connect token names. The
// token is the same kind of material as a bearer credential and is verified by
// the same Verifier, so a browser that reconnects to another replica presents
// what it already holds.
func (a *Authenticator) AuthenticateLink(ctx context.Context, token string) (factoryidentity.Principal, error) {
	return a.authenticate(ctx, NewCredential(SourceLink, token))
}

// presented reads the credential a request carries.
//
// A malformed Authorization header is a REJECTION, never a fall-through to the
// cookie. Falling through would let whoever can set a header choose which of
// two credentials is verified, and would make a mistyped header silently
// authenticate as whatever cookie the browser happened to send.
func (a *Authenticator) presented(r *http.Request) (Credential, error) {
	if header := r.Header.Get(authorizationHeader); header != "" {
		token, ok := bearerToken(header)
		if !ok {
			return Credential{}, fmt.Errorf("%w: the %s header is not a bearer credential", factoryidentity.ErrUnauthenticated, authorizationHeader)
		}
		return NewCredential(SourceBearer, token), nil
	}
	cookie, err := r.Cookie(a.cookieName)
	if err != nil {
		return Credential{}, fmt.Errorf("%w: the request carries neither an %s header nor a %s cookie",
			factoryidentity.ErrUnauthenticated, authorizationHeader, a.cookieName)
	}
	return NewCredential(SourceCookie, cookie.Value), nil
}

// bearerToken accepts exactly scheme, space, token: one credential, no interior
// space, and a case-insensitive scheme as RFC 9110 requires.
func bearerToken(header string) (string, bool) {
	scheme, rest, found := strings.Cut(header, " ")
	if !found || !strings.EqualFold(scheme, bearerScheme) {
		return "", false
	}
	token := strings.TrimLeft(rest, " ")
	if token == "" || strings.ContainsAny(token, " \t") {
		return "", false
	}
	return token, true
}

// authenticate is the whole decision, and it is shared by both entry points so
// the REST plane and the ClientLink cannot drift into different rules.
func (a *Authenticator) authenticate(ctx context.Context, credential Credential) (factoryidentity.Principal, error) {
	if credential.Value() == "" {
		return factoryidentity.Principal{}, fmt.Errorf("%w: the %s credential is empty", factoryidentity.ErrUnauthenticated, credential.Source())
	}
	claims, err := a.verifier.VerifyCredential(ctx, credential)
	if err != nil {
		// The verifier's message is kept, because "bad signature" and "issuer
		// unreachable" are different operational problems and discarding the
		// text discards the diagnosis. Only its TEXT is kept, scrubbed: the
		// error VALUE is dropped, so no unwrapping reaches a message this
		// package has not scrubbed. The price is that a caller cannot match a
		// verifier's own sentinel, which is what redaction costs.
		detail := redactSecrets(err.Error(), credential.Value())
		if errors.Is(err, factoryidentity.ErrUnauthenticated) {
			return factoryidentity.Principal{}, fmt.Errorf("%w: the %s credential was not verified: %s",
				factoryidentity.ErrUnauthenticated, credential.Source(), detail)
		}
		return factoryidentity.Principal{}, fmt.Errorf("%w: verifying the %s credential: %s",
			ErrVerifierUnavailable, credential.Source(), detail)
	}
	if claims.ExpiresAt.IsZero() {
		return factoryidentity.Principal{}, fmt.Errorf("%w: the %s credential declares no expiry, so it could never be logged out",
			factoryidentity.ErrUnauthenticated, credential.Source())
	}
	// Acceptance requires now to be strictly before the expiry, so the instant
	// itself is expired. Which side owns the boundary matters less than that
	// one side owns it; this one means an expiry is the first instant at which
	// the credential is refused.
	if now := a.clock.Now(); !now.Before(claims.ExpiresAt) {
		return factoryidentity.Principal{}, fmt.Errorf("%w: the %s credential expired at %s",
			factoryidentity.ErrCredentialExpired, credential.Source(), claims.ExpiresAt.UTC().Format(time.RFC3339))
	}
	tenant, err := a.tenantFor(claims)
	if err != nil {
		return factoryidentity.Principal{}, err
	}
	principal, err := factoryidentity.NewPrincipal(tenant, claims.Subject, claims.Kind)
	if err != nil {
		return factoryidentity.Principal{}, fmt.Errorf("%w: the %s credential does not name a usable principal: %v",
			factoryidentity.ErrUnauthenticated, credential.Source(), err)
	}
	return principal, nil
}

// tenantFor decides the tenant scope, from the verified claims or from
// configuration. Its only inputs are those two.
//
// The default-tenant fallback is confined to an ACTOR, and that is a decision
// worth stating rather than a guard. A service credential is minted by
// deployment configuration and can name the scope it was issued for, so one
// that names none is misconfigured; letting it inherit the local default would
// hand an unscoped service credential authority over whichever tenant the
// deployment happened to default to, and a service principal is the one
// identity authorized for the cross-tenant sweep.
func (a *Authenticator) tenantFor(claims Claims) (sessionwire.TenantID, error) {
	if claims.Tenant != "" {
		return claims.Tenant, nil
	}
	if claims.Kind == factoryidentity.KindService {
		return "", fmt.Errorf("%w: a service credential must name its tenant", factoryidentity.ErrUnauthenticated)
	}
	if a.defaultTenant == "" {
		return "", fmt.Errorf("%w: the credential names no tenant and no default tenant is configured", factoryidentity.ErrUnauthenticated)
	}
	return a.defaultTenant, nil
}

// redactSecrets replaces each secret in message with a fixed placeholder.
//
// The empty-secret guard is unreachable today and is kept anyway: authenticate
// rejects an empty credential BEFORE the verifier is called -- which
// TestAnEmptyCredentialIsRejectedBeforeVerification holds -- so the value
// passed here is never empty. Without the guard, strings.ReplaceAll with an
// empty old string inserts the placeholder between every character.
func redactSecrets(message string, secrets ...string) string {
	for _, secret := range secrets {
		if secret == "" {
			continue
		}
		message = strings.ReplaceAll(message, secret, redactedPlaceholder)
	}
	return message
}

// isCookieToken reports whether name is an RFC 6265 cookie name: a non-empty
// RFC 7230 token.
func isCookieToken(name string) bool {
	if name == "" {
		return false
	}
	for i := range len(name) {
		if !isTokenByte(name[i]) {
			return false
		}
	}
	return true
}

func isTokenByte(b byte) bool {
	switch {
	case b >= 'a' && b <= 'z', b >= 'A' && b <= 'Z', b >= '0' && b <= '9':
		return true
	}
	return strings.IndexByte("!#$%&'*+-.^_`|~", b) >= 0
}

// ---------------------------------------------------------------------------
// Operation contexts.
// ---------------------------------------------------------------------------

// OperationContext is the whole set of request-derived fields a handler may
// read back off a context.
//
// The set is the point. "Copy only approved principal and trace fields" is a
// claim about a set, so the set is a type with two fields rather than a
// convention about what to put in a context, and
// TestOperationContextCarriesOnlyApprovedFields enumerates it. A field holding
// a header, a cookie or a raw token cannot be added without failing that test.
//
// It is comparable and shares no memory, for the same reason Principal is: a
// handler holding one cannot widen its own tenant.
type OperationContext struct {
	// Principal is the authenticated caller.
	Principal factoryidentity.Principal
	// TraceID is the W3C trace-id of the inbound request, or empty. It is a
	// correlation identifier and carries no authority.
	TraceID string
}

type contextKey struct{}

// NewOperationContext derives the operation context for an authenticated
// request. It copies the principal and the trace identifier, and nothing else.
func NewOperationContext(ctx context.Context, r *http.Request, principal factoryidentity.Principal) context.Context {
	return context.WithValue(ctx, contextKey{}, OperationContext{
		Principal: principal,
		TraceID:   traceIDFrom(r),
	})
}

// NewLinkOperationContext derives it for a ClientLink RPC, which has no request
// headers to take a trace identifier from.
func NewLinkOperationContext(ctx context.Context, principal factoryidentity.Principal) context.Context {
	return context.WithValue(ctx, contextKey{}, OperationContext{Principal: principal})
}

// OperationContextFrom reads the operation context back. The boolean is false
// for a context no authenticated edge built, which a handler must treat as
// unauthenticated rather than as an empty principal.
func OperationContextFrom(ctx context.Context) (OperationContext, bool) {
	value, ok := ctx.Value(contextKey{}).(OperationContext)
	return value, ok
}

// traceIDFrom returns the trace-id of a well-formed W3C traceparent, or "".
//
// The header is attacker-controlled, so it is parsed to the published grammar
// rather than copied: version "-" trace-id "-" parent-id "-" flags, all
// lowercase hex, with neither identifier all zeroes. A version above 0 may
// carry further dash-separated fields, and version ff is forbidden outright.
// Copying the raw header instead would put arbitrary caller text into every
// later log line and record that carries the trace.
func traceIDFrom(r *http.Request) string {
	if r == nil {
		return ""
	}
	header := r.Header.Get(traceparentHeader)
	if header == "" {
		return ""
	}
	fields := strings.Split(header, "-")
	if len(fields) < 4 {
		return ""
	}
	// A trailing dash makes the whole header invalid, including on a future
	// version whose extra fields this one is required to ignore. Rejecting an
	// empty field is what covers that, and it covers "00--..." with it.
	if slices.Contains(fields, "") {
		return ""
	}
	version, traceID, parentID, flags := fields[0], fields[1], fields[2], fields[3]
	if len(version) != 2 || !isLowerHex(version) || version == "ff" {
		return ""
	}
	// Version 0 is exactly four fields; a later version may add more, and this
	// one is required to ignore them rather than to reject the header.
	if version == "00" && len(fields) != 4 {
		return ""
	}
	if len(flags) != 2 || !isLowerHex(flags) {
		return ""
	}
	if len(parentID) != 16 || !isLowerHex(parentID) || isAllZero(parentID) {
		return ""
	}
	if len(traceID) != 32 || !isLowerHex(traceID) || isAllZero(traceID) {
		return ""
	}
	return traceID
}

func isLowerHex(s string) bool {
	for i := range len(s) {
		b := s[i]
		if (b < '0' || b > '9') && (b < 'a' || b > 'f') {
			return false
		}
	}
	return true
}

func isAllZero(s string) bool { return strings.Trim(s, "0") == "" }
