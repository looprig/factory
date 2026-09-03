package identity

import (
	"bytes"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"time"
)

// ErrInvalidCSRFConfig is the class of every CSRFConfig rejection.
var ErrInvalidCSRFConfig = errors.New("identity: invalid CSRF configuration")

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

	// TrustedOrigins is the exact set of browser origins allowed to open a
	// ClientLink or issue a state-changing request. Each entry is a bare
	// origin: scheme://host[:port], with no path, query, fragment or userinfo.
	TrustedOrigins []string
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
	seen := make(map[string]bool, len(c.TrustedOrigins))
	for _, origin := range c.TrustedOrigins {
		if err := validateOrigin(origin); err != nil {
			return err
		}
		if seen[origin] {
			return fmt.Errorf("%w: trusted origin %q is listed more than once", ErrInvalidCSRFConfig, origin)
		}
		seen[origin] = true
	}
	return nil
}

// validateOrigin holds an entry to the shape of an Origin header:
// scheme://host[:port] and nothing else. The final comparison is what rejects a
// path, a query, a fragment, userinfo and a trailing slash in one step, rather
// than by enumerating the shapes someone thought of.
//
// It is a SHAPE check, not a normalization, and the difference matters to
// whoever writes the matcher. url.Parse lowercases the scheme but leaves the
// host exactly as written, so `https://APP.Example.COM`, an explicit `:443`, a
// trailing dot, and a Unicode host rather than its punycode form all pass here
// and then never equal the Origin a browser actually sends. The duplicate check
// in Validate compares exact strings for the same reason, so those variants
// count as distinct entries.
//
// Nothing here depends on that yet, because nothing compares an origin yet.
// A1.3 owns the comparison, and it must normalize both sides -- or narrow this
// function -- rather than assume an entry that validated is an entry that can
// match.
func validateOrigin(origin string) error {
	if origin == "" {
		return fmt.Errorf("%w: TrustedOrigins contains an empty entry", ErrInvalidCSRFConfig)
	}
	if origin == "*" {
		return fmt.Errorf("%w: a wildcard trusted origin is never accepted", ErrInvalidCSRFConfig)
	}
	parsed, err := url.Parse(origin)
	if err != nil {
		return fmt.Errorf("%w: trusted origin %q is not a URL: %v", ErrInvalidCSRFConfig, origin, err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("%w: trusted origin %q must use the http or https scheme", ErrInvalidCSRFConfig, origin)
	}
	if parsed.Host == "" {
		return fmt.Errorf("%w: trusted origin %q names no host", ErrInvalidCSRFConfig, origin)
	}
	if bare := parsed.Scheme + "://" + parsed.Host; origin != bare {
		return fmt.Errorf("%w: trusted origin %q must be a bare origin such as %q", ErrInvalidCSRFConfig, origin, bare)
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
