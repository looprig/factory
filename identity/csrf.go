package identity

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

// ErrInvalidCSRFConfig is the class of every CSRFConfig rejection.
var ErrInvalidCSRFConfig = errors.New("identity: invalid CSRF configuration")

// ErrInvalidOrigin is the class of every ParseOrigin and CanonicalHostname
// rejection. A CSRFConfig rejection caused by one of its trusted origins wraps
// both this and ErrInvalidCSRFConfig.
var ErrInvalidOrigin = errors.New("identity: invalid origin")

// MinCSRFSharedKeyBytes is the shortest key CSRFConfig accepts.
const MinCSRFSharedKeyBytes = 32

// CSRFConfig is the browser-facing origin and CSRF configuration.
//
// There is deliberately NO default that manufactures a key. A Factory replica
// owns only its local ClientLinks, and a browser may reconnect to a different
// replica or issue its next REST call there, so a key generated per process is
// state the next replica cannot verify. Requiring the operator to supply the
// key is what makes "stateless or shared" a configuration error rather than an
// intermittent production failure.
type CSRFConfig struct {
	// SharedKey signs CSRF tokens. It must be the same bytes in every replica.
	SharedKey []byte

	// TokenTTL bounds how long an issued token is accepted.
	TokenTTL time.Duration

	// TrustedOrigins is the set of browser origins allowed to open a
	// ClientLink or issue a state-changing request. Each entry is a bare
	// origin: scheme://host[:port], with no path, query, fragment or userinfo.
	//
	// Entries are compared in their CANONICAL form, not as written: see
	// ParseOrigin. Two entries that canonicalize alike are duplicates.
	TrustedOrigins []string

	// TrustForwardedHeaders makes a reverse-proxy deployment's
	// X-Forwarded-Host the host a guard checks, in place of the Host the
	// server itself received.
	//
	// It defaults to false and the default is the point. X-Forwarded-Host is an
	// ordinary request header: anything that can reach Factory's listener can
	// set it, so a guard that reads it unconditionally is a guard whose subject
	// the caller chooses. Enable it only where every route to Factory passes
	// through a proxy that OVERWRITES the header rather than appending to it.
	TrustForwardedHeaders bool
}

// Validate reports why this configuration may not be used.
func (c CSRFConfig) Validate() error {
	if len(c.SharedKey) < MinCSRFSharedKeyBytes {
		return fmt.Errorf("%w: SharedKey is %d bytes, want at least %d", ErrInvalidCSRFConfig, len(c.SharedKey), MinCSRFSharedKeyBytes)
	}
	if c.TokenTTL <= 0 {
		return fmt.Errorf("%w: TokenTTL is %v, want a positive duration", ErrInvalidCSRFConfig, c.TokenTTL)
	}
	if len(c.TrustedOrigins) == 0 {
		return fmt.Errorf("%w: TrustedOrigins names no origin", ErrInvalidCSRFConfig)
	}
	seen := make(map[string]string, len(c.TrustedOrigins))
	for _, entry := range c.TrustedOrigins {
		if entry == "" {
			return fmt.Errorf("%w: TrustedOrigins contains an empty entry", ErrInvalidCSRFConfig)
		}
		origin, err := ParseOrigin(entry)
		if err != nil {
			return fmt.Errorf("%w: trusted origin %q is not usable: %w", ErrInvalidCSRFConfig, entry, err)
		}
		// The duplicate check compares CANONICAL forms, so "https://APP.example.com"
		// and "https://app.example.com:443" are one entry listed twice rather
		// than two entries that happen to accept the same browser.
		if first, duplicate := seen[origin.String()]; duplicate {
			return fmt.Errorf("%w: trusted origin %q is listed more than once: it and %q are both %q",
				ErrInvalidCSRFConfig, entry, first, origin)
		}
		seen[origin.String()] = entry
	}
	return nil
}

// Clone returns a copy that shares no memory with c.
//
// bytes.Clone and slices.Clone both return nil for a nil input, so a zero
// CSRFConfig clones to a zero CSRFConfig rather than to one holding an empty
// key that Validate would have to distinguish from an absent one.
func (c CSRFConfig) Clone() CSRFConfig {
	clone := c
	clone.SharedKey = bytes.Clone(c.SharedKey)
	clone.TrustedOrigins = slices.Clone(c.TrustedOrigins)
	return clone
}

// Origin is a browser origin in the one form a guard may compare.
//
// It exists because the previous shape check was not a normalization, and the
// difference is a guard that never matches rather than a guard that is loose:
// url.Parse lowercases the scheme and leaves the host exactly as written, so a
// configured "https://APP.Example.COM", an explicit ":443" and an empty ":"
// each validated and could then never equal the Origin a browser sends. This
// type is what makes BOTH SIDES of every comparison the same value: a
// configured entry and an inbound Origin header go through ParseOrigin, and
// what is compared is String.
//
// Its fields are unexported and it holds no reference members, so it is
// comparable and a holder cannot widen it.
type Origin struct {
	scheme   string
	hostname string
	// port is "" when the origin uses its scheme's default port, so
	// "https://app.example.com" and "https://app.example.com:443" are one
	// value -- which is what RFC 6454 makes them.
	port string
}

// ParseOrigin canonicalizes a browser origin: an inbound Origin header, or a
// CSRFConfig.TrustedOrigins entry.
//
// What it NORMALIZES, each because a browser's own serialization already
// differs from what an operator is likely to write:
//
//   - scheme and host case, since RFC 3986 makes both case-insensitive;
//   - a port that is the scheme's default, which a browser omits;
//   - an empty port, which "https://host:" is and no browser sends.
//
// What it REFUSES rather than normalizes, and why refusing is the honest
// answer in each case:
//
//   - a non-ASCII host. Mapping a Unicode host to its punycode form is IDNA,
//     which this module does not implement and cannot approximate: an
//     ASCII-only "normalization" of a Unicode host is a guess. A refusal names
//     the problem at composition time; the alternative is an entry that
//     validates and never matches.
//   - a host with a TRAILING DOT. DNS resolves "app.example.com." and
//     "app.example.com" to the same server, but a browser serializes the host
//     as it appeared in the URL, so the two are DISTINCT origins to the same
//     origin policy. Stripping the dot on both sides would therefore merge two
//     origins the browser separates, which is a widening, not a normalization.
//   - a port that is not a plain decimal 1-65535, so ":0443" -- which parses,
//     and which no browser sends -- cannot become an entry that never matches.
//
// The consequence for an INBOUND header is the same rule read the other way: an
// Origin carrying a trailing dot or a non-ASCII host cannot canonicalize, so it
// matches no entry and is rejected. Every refusal here is fail-closed.
func ParseOrigin(text string) (Origin, error) {
	if text == "" {
		return Origin{}, fmt.Errorf("%w: an origin may not be empty", ErrInvalidOrigin)
	}
	if text == "*" {
		return Origin{}, fmt.Errorf("%w: a wildcard origin is never accepted", ErrInvalidOrigin)
	}
	// ASCII is checked over the WHOLE string, before anything is folded. It is
	// what the punycode refusal above is made of, and it is also what makes the
	// EqualFold below an ASCII comparison: strings.EqualFold applies Unicode
	// simple folding, under which characters outside ASCII can fold onto ASCII
	// ones, so folding an unchecked string would be a comparison with cases
	// nobody enumerated.
	if index := firstNonASCII(text); index >= 0 {
		return Origin{}, fmt.Errorf("%w: origin %q is not ASCII at byte %d; a Unicode host must be written in its punycode form", ErrInvalidOrigin, text, index)
	}
	parsed, err := url.Parse(text)
	if err != nil {
		return Origin{}, fmt.Errorf("%w: origin %q is not a URL: %v", ErrInvalidOrigin, text, err)
	}
	// url.Parse has already lowercased the scheme.
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return Origin{}, fmt.Errorf("%w: origin %q must use the http or https scheme", ErrInvalidOrigin, text)
	}
	if parsed.Host == "" {
		return Origin{}, fmt.Errorf("%w: origin %q names no host", ErrInvalidOrigin, text)
	}
	// One comparison rejects a path, a query, a fragment, userinfo, a trailing
	// slash and an opaque URL, rather than an enumeration of the shapes someone
	// thought of: anything url.Parse put anywhere other than the scheme and the
	// host makes the input differ from the bare origin rebuilt from those two.
	// EqualFold rather than == is what lets the case normalization above exist.
	if bare := parsed.Scheme + "://" + parsed.Host; !strings.EqualFold(text, bare) {
		return Origin{}, fmt.Errorf("%w: origin %q must be a bare origin such as %q", ErrInvalidOrigin, text, bare)
	}
	hostname, err := canonicalHostname(parsed.Hostname())
	if err != nil {
		return Origin{}, err
	}
	port, err := canonicalPort(parsed.Port())
	if err != nil {
		return Origin{}, err
	}
	if port == defaultPort(parsed.Scheme) {
		port = ""
	}
	return Origin{scheme: parsed.Scheme, hostname: hostname, port: port}, nil
}

// String is the canonical serialization, and it is what a guard compares. Two
// origins are the same origin exactly when their String values are equal.
func (o Origin) String() string {
	if o.scheme == "" {
		return ""
	}
	host := o.hostname
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	if o.port == "" {
		return o.scheme + "://" + host
	}
	return o.scheme + "://" + host + ":" + o.port
}

// Hostname is the canonical host with no brackets and no port. It is what a
// host check compares, since the Host header a server receives names no scheme
// and therefore cannot be compared as an origin.
func (o Origin) Hostname() string { return o.hostname }

// CanonicalHostname canonicalizes an HTTP authority -- a Host header or an
// X-Forwarded-Host value -- to the same hostname form Origin.Hostname returns.
//
// It is a SEPARATE entry point from ParseOrigin because the two grammars
// differ: an authority carries no scheme, so its port cannot be compared with a
// scheme's default and is validated and then discarded rather than normalized.
// Both entry points
// canonicalize the host through one implementation, so a host that is refused
// in one is refused in the other.
//
// Discarding the port is what makes a host check a check on the NAME. A DNS
// rebinding attack rebinds a name; it does not choose the port the server is
// listening on. The port is not lost from the decision, because an inbound
// Origin header is compared as a whole origin, port included.
func CanonicalHostname(authority string) (string, error) {
	if authority == "" {
		return "", fmt.Errorf("%w: an authority may not be empty", ErrInvalidOrigin)
	}
	if index := firstNonASCII(authority); index >= 0 {
		return "", fmt.Errorf("%w: authority %q is not ASCII at byte %d", ErrInvalidOrigin, authority, index)
	}
	host, err := splitAuthorityHost(authority)
	if err != nil {
		return "", err
	}
	return canonicalHostname(host)
}

// splitAuthorityHost takes the host out of an authority that may or may not
// carry a port. net.SplitHostPort handles "host:port" and "[::1]:port"
// natively but rejects a bare host, which an HTTP Host header is allowed to be;
// any other error means the authority is malformed in some other way and the
// caller must fail closed rather than guess at a hostname.
func splitAuthorityHost(authority string) (string, error) {
	host, port, err := net.SplitHostPort(authority)
	if err == nil {
		// The port is validated even though it is then discarded, and that is
		// not ceremony: net.SplitHostPort does not check that what follows the
		// colon is a port, so "https://app.example.com" splits happily into the
		// host "https" and the "port" "//app.example.com". An authority this
		// function cannot account for in full is fail-closed.
		if _, err := canonicalPort(port); err != nil {
			return "", fmt.Errorf("%w: authority %q is not host[:port]", ErrInvalidOrigin, authority)
		}
		return host, nil
	}
	var addrErr *net.AddrError
	if !errors.As(err, &addrErr) || addrErr.Err != "missing port in address" {
		return "", fmt.Errorf("%w: authority %q is not host[:port]: %v", ErrInvalidOrigin, authority, err)
	}
	if strings.HasPrefix(authority, "[") && strings.HasSuffix(authority, "]") {
		return authority[1 : len(authority)-1], nil
	}
	return authority, nil
}

// canonicalHostname is the one implementation of what a host may be, shared by
// ParseOrigin and CanonicalHostname.
func canonicalHostname(host string) (string, error) {
	if host == "" {
		return "", fmt.Errorf("%w: an origin names no host", ErrInvalidOrigin)
	}
	if strings.HasSuffix(host, ".") {
		return "", fmt.Errorf("%w: host %q ends in a dot, which a browser serializes as a distinct origin; write it without the trailing dot", ErrInvalidOrigin, host)
	}
	for i := range len(host) {
		if !isHostByte(host[i]) {
			return "", fmt.Errorf("%w: host %q contains %q, which is not a host character", ErrInvalidOrigin, host, host[i:i+1])
		}
	}
	return strings.ToLower(host), nil
}

// isHostByte reports whether b may appear in a host. The set is the letters,
// digits, hyphen, underscore and dot of a DNS name, plus the colon of an IPv6
// literal, whose brackets are stripped before this runs.
func isHostByte(b byte) bool {
	switch {
	case b >= 'a' && b <= 'z', b >= 'A' && b <= 'Z', b >= '0' && b <= '9':
		return true
	}
	return b == '-' || b == '.' || b == '_' || b == ':'
}

// canonicalPort accepts the empty port and a plain decimal 1-65535, and
// nothing else. A leading zero is refused rather than stripped: ":0443" is a
// port no browser sends, so accepting it could only produce a configured entry
// that never matches one.
func canonicalPort(port string) (string, error) {
	if port == "" {
		return "", nil
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 1 || number > 65535 || strconv.Itoa(number) != port {
		return "", fmt.Errorf("%w: port %q is not a decimal port between 1 and 65535", ErrInvalidOrigin, port)
	}
	return port, nil
}

func defaultPort(scheme string) string {
	if scheme == "https" {
		return "443"
	}
	return "80"
}

// firstNonASCII returns the index of the first byte outside ASCII, or -1.
func firstNonASCII(text string) int {
	for i := range len(text) {
		if text[i] >= 0x80 {
			return i
		}
	}
	return -1
}
