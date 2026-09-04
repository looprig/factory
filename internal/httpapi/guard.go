package httpapi

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/looprig/factory/identity"
	internalidentity "github.com/looprig/factory/internal/identity"
)

// ErrInvalidGuardConfig is the class of every NewGuard rejection.
var ErrInvalidGuardConfig = errors.New("httpapi: invalid guard configuration")

// CSRFHeaderName is the request header a client echoes an issued token back in.
//
// A header rather than a form field or a cookie, and the reason is a condition
// rather than a property: a cross-site page can make a browser submit a form
// and can make it attach a cookie, but it cannot set a custom request header
// without a CORS preflight the SERVER approves. Nothing in this module answers
// a preflight -- no file writes an Access-Control- header at this revision --
// so none is approved. A later task that adds a CORS policy must keep this
// header out of Access-Control-Allow-Headers, or this obstacle stops existing.
const CSRFHeaderName = "X-CSRF-Token"

const (
	originHeader        = "Origin"
	upgradeHeader       = "Upgrade"
	forwardedHostHeader = "X-Forwarded-Host"
	websocketUpgrade    = "websocket"
)

// Reason names the single rule that rejected a request. Every rejection answers
// 403, so the reason is what separates them: several ways through one status
// code is a guard whose checks cannot be told apart, and a check nothing can
// tell apart from another is a check nothing measures.
//
// It is carried to the client as the "code" member of the rejection body, so a
// browser can distinguish the one rejection it can act on -- an expired token,
// which it recovers from by fetching a fresh one and retrying -- from the ones
// it cannot.
type Reason string

const (
	// ReasonNoRequest reports a nil request.
	ReasonNoRequest Reason = "no_request"
	// ReasonHostNotTrusted reports a Host naming no trusted origin's host. It
	// is the DNS-rebinding check: a rebound name reaches the server as the
	// Host it was reached by, which the page that rebound it cannot change.
	ReasonHostNotTrusted Reason = "host_not_trusted"
	// ReasonOriginNotTrusted reports an Origin header naming an origin that is
	// not trusted.
	ReasonOriginNotTrusted Reason = "origin_not_trusted"
	// ReasonUpgradeOriginMissing reports a WebSocket handshake carrying an
	// ambient credential and no Origin header.
	ReasonUpgradeOriginMissing Reason = "websocket_origin_missing"
	// ReasonOriginMissing reports a state-changing request carrying an ambient
	// credential and no Origin header.
	ReasonOriginMissing Reason = "origin_missing"
	// ReasonUnauthenticated reports a state-changing request with no operation
	// context, so there is no principal to verify a token against.
	ReasonUnauthenticated Reason = "unauthenticated"
	// ReasonCSRFMissing reports an absent CSRFHeaderName header.
	ReasonCSRFMissing Reason = "csrf_token_missing"
	// ReasonCSRFInvalid reports a token that is not this deployment's, or
	// is not this principal's. The two are deliberately one answer: telling
	// them apart would confirm that a presented token belongs to somebody.
	ReasonCSRFInvalid Reason = "csrf_token_invalid"
	// ReasonCSRFExpired reports a token this principal really was issued,
	// past its expiry. It is separate from invalid because it is the one
	// rejection a client recovers from without a human.
	ReasonCSRFExpired Reason = "csrf_token_expired"
)

// reasonMessages is the client-facing text of each reason.
//
// Nothing here is derived from the request. A message that echoed the rejected
// Origin, Host or token would put caller-controlled text into a response body
// and, for the token, would return a secret to whoever guessed at it.
var reasonMessages = map[Reason]string{
	ReasonNoRequest:            "there is no request to check",
	ReasonHostNotTrusted:       "the request was addressed to a host this deployment does not serve",
	ReasonOriginNotTrusted:     "the request came from an origin this deployment does not trust",
	ReasonUpgradeOriginMissing: "a WebSocket handshake carrying a browser credential must send an Origin header",
	ReasonOriginMissing:        "a state-changing request carrying a browser credential must send an Origin header",
	ReasonUnauthenticated:      "the request is not authenticated",
	ReasonCSRFMissing:          "the request carries no " + CSRFHeaderName + " header",
	ReasonCSRFInvalid:          "the " + CSRFHeaderName + " header is not a token this principal holds",
	ReasonCSRFExpired:          "the " + CSRFHeaderName + " header holds an expired token",
}

// GuardConfig configures a Guard.
type GuardConfig struct {
	// CSRF is the composed origin and CSRF configuration. It is validated
	// again here rather than assumed: a Guard built from an unvalidated
	// configuration is a guard with no trusted origins, which fails closed but
	// fails at every request rather than at composition.
	CSRF identity.CSRFConfig

	// Credentials answers which credential a request presents. It is the
	// concrete Authenticator rather than an interface, because the answer must
	// come from the one implementation of the precedence rule and a seam here
	// would be a second place to state it; see internalidentity.Authenticator's
	// CredentialSource.
	Credentials *internalidentity.Authenticator

	// Clock decides token expiry.
	Clock internalidentity.Clock
}

// Guard is the HTTP and WebSocket origin and CSRF guard.
//
// # Where it is mounted
//
// Around the mux, and INSIDE authentication. Around the mux, because a
// rejected origin must not be able to learn which routes exist by comparing a
// 403 with a 404; the cost is that a rejected request is answered before
// routing, which is the intended answer rather than a lost 404.
//
// Inside authentication, because a token is bound to the principal it was
// issued to and there is no principal before authentication has run. Mounting
// it the other way round does not silently disable it: a state-changing request
// then has no operation context and is rejected with ReasonUnauthenticated, so
// the mistake presents as every write failing rather than as the guard being
// skipped.
//
// # What each rule is for
//
// The rules are three defences, not one repeated:
//
//   - The HOST rule answers the case the Origin rule cannot see, which is DNS
//     rebinding. A rebound page's requests are same-origin to the BROWSER, so
//     a safe GET from one carries no Origin header for the Origin rule to
//     examine. What it cannot change is the Host the server received, which
//     net/http takes from the request line or the Host header.
//   - The ORIGIN rule answers a cross-site request from a page that is honest
//     about where it came from, which is every browser.
//   - The TOKEN rule answers the case the Origin rule cannot see: a request a
//     browser is willing to send with no Origin header. It is required only
//     where it can do work -- see the ambient-credential note on Check.
//
// # What holds tokens
//
// Nothing. Tokens are STATELESS: an issued token carries its own nonce and
// expiry and an HMAC over both and over the principal, keyed by
// CSRFConfig.SharedKey, which every replica is configured with. A Guard keeps
// no issued-token set, so a browser that mints a token on one Factory replica
// and posts to another is not rejected by the second one, and a replica that
// restarts does not invalidate every open tab. The alternative -- a per-process
// map -- is the failure this design exists to prevent: it works in a single
// process and fails intermittently the moment a deployment has two, in
// proportion to how well the load balancer happens to be pinning connections.
//
// A stateless token MUST be bound to its principal, and that is a consequence
// rather than a nicety. An unbound stateless token can be minted by anyone with
// any account -- the attacker fetches one with their own credentials, embeds
// the literal string in the attacking page, and the victim's browser posts it
// alongside the victim's cookie. Binding is what makes a token the attacker can
// mint useless against a principal they are not.
type Guard struct {
	key            []byte
	ttl            time.Duration
	trustForwarded bool
	// origins and hostnames are CANONICAL forms, computed once from the
	// configured entries. The inbound side is canonicalized per request by the
	// same identity.ParseOrigin and identity.CanonicalHostname, so both sides
	// of both comparisons are produced by one implementation rather than
	// compared as written.
	origins     map[string]bool
	hostnames   map[string]bool
	credentials *internalidentity.Authenticator
	clock       internalidentity.Clock
}

// NewGuard validates a configuration and returns the guard. A rejection returns
// a nil Guard, so a caller that ignores the error cannot mount one built from a
// half-valid configuration.
func NewGuard(cfg GuardConfig) (*Guard, error) {
	if cfg.Credentials == nil {
		return nil, fmt.Errorf("%w: Credentials is required", ErrInvalidGuardConfig)
	}
	if cfg.Clock == nil {
		return nil, fmt.Errorf("%w: Clock is required", ErrInvalidGuardConfig)
	}
	if err := cfg.CSRF.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidGuardConfig, err)
	}
	guard := &Guard{
		key:            append([]byte(nil), cfg.CSRF.SharedKey...),
		ttl:            cfg.CSRF.TokenTTL,
		trustForwarded: cfg.CSRF.TrustForwardedHeaders,
		origins:        make(map[string]bool, len(cfg.CSRF.TrustedOrigins)),
		hostnames:      make(map[string]bool, len(cfg.CSRF.TrustedOrigins)),
		credentials:    cfg.Credentials,
		clock:          cfg.Clock,
	}
	for _, entry := range cfg.CSRF.TrustedOrigins {
		origin, err := identity.ParseOrigin(entry)
		if err != nil {
			// Unreachable: Validate above accepts a configuration only when
			// every entry parses. It is returned rather than ignored because
			// that is Validate's guarantee and not this function's.
			return nil, fmt.Errorf("%w: %w", ErrInvalidGuardConfig, err)
		}
		guard.origins[origin.String()] = true
		guard.hostnames[origin.Hostname()] = true
	}
	return guard, nil
}

// Wrap answers a rejected request itself and passes an accepted one on.
func (g *Guard) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if reason, allowed := g.Check(r); !allowed {
			writeRejection(w, reason)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// Check reports the single rule that rejects r, or ("", true) when every rule
// allows it.
//
// The order below is the order of the rules. Every rejection except
// ReasonNoRequest -- which has no request for another rule to be satisfied by
// -- is reachable from a request every other rule allows, and
// TestGuardRulesAreEachSoleOnTheirOwnPath drives each of them that way from one
// accepted base. That is what makes a reason the rule that DECIDED rather than
// the rule that happened to run first.
//
// # The ambient-credential exemption
//
// Every rule below the exemption -- the two that REQUIRE an Origin header, and
// the token rules -- applies to an AMBIENT credential, one a cross-site page
// can cause the browser to attach, and not to a bearer credential, which an
// application attaches deliberately to a request it made itself. The
// Origin-required rules are in that set for the same reason the token rules
// are: a browser cannot set an Authorization header on a WebSocket handshake
// or on a form submission, so a request that carries one is not a browser
// being used against its user.
//
// The rules ABOVE the exemption -- host and Origin-if-present -- apply to every
// request, bearer included. A bearer credential says nothing about where the
// request was addressed.
//
// The exemption is granted by NAMING the exempt source, which is what makes the
// empty source A1.1 records for an unreadable credential non-exempt: it is not
// on the list. The second result of CredentialSource is deliberately not
// consulted, because a branch on it could not change the answer -- the empty
// source is not the bearer source -- and a branch that cannot change an answer
// is a branch no test can hold.
func (g *Guard) Check(r *http.Request) (Reason, bool) {
	if r == nil {
		return ReasonNoRequest, false
	}
	if !g.hostTrusted(r) {
		return ReasonHostNotTrusted, false
	}
	originPresent, originTrusted := g.originTrusted(r)
	if originPresent && !originTrusted {
		return ReasonOriginNotTrusted, false
	}
	source, _ := g.credentials.CredentialSource(r)
	if source == internalidentity.SourceBearer {
		return "", true
	}
	if claimsWebSocketUpgrade(r) && !originPresent {
		return ReasonUpgradeOriginMissing, false
	}
	if !stateChanging(r.Method) {
		return "", true
	}
	if !originPresent {
		return ReasonOriginMissing, false
	}
	operation, authenticated := internalidentity.OperationContextFrom(r.Context())
	if !authenticated {
		return ReasonUnauthenticated, false
	}
	token := r.Header.Get(CSRFHeaderName)
	if token == "" {
		return ReasonCSRFMissing, false
	}
	return g.verifyToken(token, operation.Principal)
}

// hostTrusted reports whether the request was addressed to a host this
// deployment serves.
//
// It compares the HOST NAME and ignores the port, because that is the shape of
// what it defends against: rebinding attacks a name. The port is not lost from
// the decision when the request carries an Origin header, which originTrusted
// compares as a whole origin, port included.
func (g *Guard) hostTrusted(r *http.Request) bool {
	authority, ok := g.requestAuthority(r)
	if !ok {
		return false
	}
	hostname, err := identity.CanonicalHostname(authority)
	if err != nil {
		return false
	}
	return g.hostnames[hostname]
}

// requestAuthority is where a reverse-proxy deployment's forwarded host is
// read, and it is read ONLY when CSRFConfig.TrustForwardedHeaders says so.
//
// X-Forwarded-Host is an ordinary request header. Anything that can reach
// Factory's listener directly can set it, so reading it unconditionally would
// let the caller choose the subject of the check above. The default is
// therefore the Host the server itself received, which net/http takes from the
// request line or the Host header and which no page's DNS control changes.
//
// More than one X-Forwarded-Host is refused rather than resolved, for the
// reason A1.1 refuses two Authorization headers: nothing may choose between two
// answers to the same question. A single header holding a comma-separated chain
// is refused too, by CanonicalHostname, which has no host character for a comma.
func (g *Guard) requestAuthority(r *http.Request) (string, bool) {
	if !g.trustForwarded {
		return r.Host, true
	}
	values := r.Header.Values(forwardedHostHeader)
	switch len(values) {
	case 0:
		return r.Host, true
	case 1:
		return values[0], true
	default:
		return "", false
	}
}

// originTrusted reports whether an Origin header is present and, if it is,
// whether it names a trusted origin. An Origin that cannot be canonicalized is
// present and not trusted, never absent.
func (g *Guard) originTrusted(r *http.Request) (present, trusted bool) {
	values := r.Header.Values(originHeader)
	if len(values) == 0 {
		return false, false
	}
	if len(values) != 1 {
		return true, false
	}
	origin, err := identity.ParseOrigin(values[0])
	if err != nil {
		return true, false
	}
	return true, g.origins[origin.String()]
}

// claimsWebSocketUpgrade reports whether r asks to be upgraded to a WebSocket.
//
// It reads the claim rather than confirming a real handshake, and the direction
// of that inaccuracy is the safe one: a request that says "Upgrade: websocket"
// and is not one gets the stricter rule, and a real handshake cannot avoid the
// rule by omitting the header the protocol requires.
func claimsWebSocketUpgrade(r *http.Request) bool {
	for _, value := range r.Header.Values(upgradeHeader) {
		for _, token := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(token), websocketUpgrade) {
				return true
			}
		}
	}
	return false
}

// stateChanging reports whether method is one that may change state.
//
// The exemption is an allowlist, so a method nobody here has heard of is
// guarded rather than exempt. RFC 9110 also calls TRACE safe and it is
// deliberately absent: Factory serves no TRACE, so exempting it would be an
// exemption nothing needs.
func stateChanging(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return false
	default:
		return true
	}
}

// ---------------------------------------------------------------------------
// Stateless tokens.
// ---------------------------------------------------------------------------

const (
	// csrfMACDomain separates this HMAC's inputs from any other use of the
	// same shared key.
	csrfMACDomain = "factory/csrf/v1"
	// csrfNonceBytes is the per-token entropy. It is what makes two tokens
	// issued to one principal in the same millisecond distinct.
	csrfNonceBytes = 16
	// csrfExpiryBytes is the big-endian Unix-millisecond expiry.
	csrfExpiryBytes = 8
	// csrfMACBytes is the HMAC-SHA256 tag.
	csrfMACBytes = sha256.Size
)

// IssueToken mints a token bound to principal, valid for the configured TTL.
//
// The nonce is fresh per call, so two calls return different tokens except with
// the probability of a collision in 128 bits of crypto/rand, and a client that
// mints per page load does not have to reason about reuse.
func (g *Guard) IssueToken(principal identity.Principal) (string, time.Time, error) {
	if principal.Tenant() == "" || principal.Subject() == "" {
		return "", time.Time{}, fmt.Errorf("%w: a token may not be issued to an unauthenticated principal", ErrInvalidGuardConfig)
	}
	nonce := make([]byte, csrfNonceBytes)
	// crypto/rand.Read is documented never to return an error, so this branch
	// is unreachable. It is returned rather than dropped because that
	// guarantee is the standard library's to keep and not this package's to
	// assume, and a token built from a partially filled nonce would be a token
	// a later issue could repeat.
	if _, err := rand.Read(nonce); err != nil {
		return "", time.Time{}, fmt.Errorf("%w: reading token entropy: %v", ErrInvalidGuardConfig, err)
	}
	expiry := g.clock.Now().Add(g.ttl)
	// A clock before the Unix epoch has no representable expiry here, and is
	// refused rather than wrapped: an expiry that encoded as an enormous
	// unsigned value would be a token nothing ever expires.
	milli := expiry.UnixMilli()
	if milli < 0 {
		return "", time.Time{}, fmt.Errorf("%w: the clock reads %s, before the Unix epoch, so no token expiry can be encoded",
			ErrInvalidGuardConfig, expiry.UTC().Format(time.RFC3339))
	}
	payload := make([]byte, 0, csrfNonceBytes+csrfExpiryBytes)
	payload = append(payload, nonce...)
	payload = binary.BigEndian.AppendUint64(payload, uint64(milli))
	token := append(payload, g.tokenMAC(payload, principal)...)
	return base64.RawURLEncoding.EncodeToString(token), expiry, nil
}

// verifyToken is the token half of Check.
//
// The MAC is compared before the expiry is read, and with hmac.Equal, so an
// expiry a caller wrote themselves is never the thing that answers: a forged
// token is rejected as invalid whatever timestamp it carries.
func (g *Guard) verifyToken(token string, principal identity.Principal) (Reason, bool) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(raw) != csrfNonceBytes+csrfExpiryBytes+csrfMACBytes {
		return ReasonCSRFInvalid, false
	}
	payload, mac := raw[:csrfNonceBytes+csrfExpiryBytes], raw[csrfNonceBytes+csrfExpiryBytes:]
	if !hmac.Equal(mac, g.tokenMAC(payload, principal)) {
		return ReasonCSRFInvalid, false
	}
	// The comparison stays in the encoded millisecond domain, so the expiry is
	// never converted back into a signed value.
	//
	// The nowMilli < 0 disjunct is what makes the conversion beside it a
	// deliberate one rather than an accidental wrap, and it is NOT what
	// produces the answer: a negative millisecond count converts to a uint64
	// above every expiry this guard can encode -- IssueToken refuses to encode
	// a negative one -- so a pre-epoch clock reads as expired by that route
	// too, and deleting the disjunct changes no outcome. Both routes are the
	// fail-closed one, which is the property
	// TestAClockBeforeTheUnixEpochIssuesNothingAndValidatesNothing asserts: a
	// replica that cannot tell the time must not be the one that decides a
	// token is still good.
	expiryMilli := binary.BigEndian.Uint64(payload[csrfNonceBytes:])
	nowMilli := g.clock.Now().UnixMilli()
	if nowMilli < 0 || uint64(nowMilli) >= expiryMilli {
		return ReasonCSRFExpired, false
	}
	return "", true
}

// tokenMAC is the one place a token's bytes are authenticated, so issuing and
// verifying cannot drift into different inputs.
//
// Every variable-length field is LENGTH-PREFIXED rather than separated by a
// delimiter. A subject is arbitrary bounded UTF-8 and a tenant is an opaque
// Core identity, so either may contain any byte a delimiter could be: with a
// separator, the principal (tenant "a", subject "b\x00c") and the principal
// (tenant "a\x00b", subject "c") authenticate identical bytes, and a token
// issued to one verifies for the other.
func (g *Guard) tokenMAC(payload []byte, principal identity.Principal) []byte {
	mac := hmac.New(sha256.New, g.key)
	// hash.Hash.Write is documented never to return an error, which is why the
	// results here are discarded explicitly rather than checked into a path
	// that could not be taken.
	_, _ = mac.Write([]byte(csrfMACDomain))
	writeLengthPrefixed(mac, string(principal.Tenant()))
	writeLengthPrefixed(mac, principal.Subject())
	writeLengthPrefixed(mac, string(principal.Kind()))
	_, _ = mac.Write(payload)
	return mac.Sum(nil)
}

func writeLengthPrefixed(mac io.Writer, value string) {
	_, _ = mac.Write(binary.AppendUvarint(nil, uint64(len(value))))
	_, _ = mac.Write([]byte(value))
}

// ---------------------------------------------------------------------------
// Responses.
// ---------------------------------------------------------------------------

// tokenResponse is TokenHandler's body.
type tokenResponse struct {
	CSRFToken string `json:"csrf_token"`
	ExpiresAt string `json:"expires_at"`
}

type rejectionResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// TokenHandler serves a freshly issued token to the authenticated caller.
//
// It is a GET, which stateChanging treats as safe, so a client holding no token
// can reach it. It is safe for it to be a GET for the reason a token endpoint
// generally is: the response is readable only same-origin, and the guard has
// already established that the request's Host is this deployment's.
func (g *Guard) TokenHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		operation, authenticated := internalidentity.OperationContextFrom(r.Context())
		if !authenticated {
			writeRejection(w, ReasonUnauthenticated)
			return
		}
		token, expiry, err := g.IssueToken(operation.Principal)
		if err != nil {
			writeRejection(w, ReasonUnauthenticated)
			return
		}
		writeJSON(w, http.StatusOK, tokenResponse{CSRFToken: token, ExpiresAt: expiry.UTC().Format(time.RFC3339Nano)})
	})
}

func writeRejection(w http.ResponseWriter, reason Reason) {
	writeJSON(w, http.StatusForbidden, rejectionResponse{Code: string(reason), Message: reasonMessages[reason]})
}

// writeJSON writes a body no cache may keep and no browser may sniff. A CSRF
// token is a credential, and a cached copy served to a later page load would be
// a credential issued to whoever asked first.
func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
