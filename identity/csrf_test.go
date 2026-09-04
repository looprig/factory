package identity_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/looprig/factory/identity"
)

// validCSRF is the base every negative case below mutates by exactly one field,
// so a rejection is attributable to that field and not to the base.
func validCSRF() identity.CSRFConfig {
	return identity.CSRFConfig{
		SharedKey:      bytes.Repeat([]byte{'k'}, identity.MinCSRFSharedKeyBytes),
		TokenTTL:       time.Hour,
		TrustedOrigins: []string{"https://app.example.com", "http://localhost:5173"},
	}
}

func TestValidCSRFBaseIsAccepted(t *testing.T) {
	t.Parallel()

	if err := validCSRF().Validate(); err != nil {
		t.Fatalf("the base every negative case mutates is itself rejected: %v", err)
	}
}

func TestCSRFConfigValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(*identity.CSRFConfig)
		wantErr string
	}{
		{
			name:    "no key at all",
			mutate:  func(c *identity.CSRFConfig) { c.SharedKey = nil },
			wantErr: "SharedKey",
		},
		{
			name:    "key one byte short",
			mutate:  func(c *identity.CSRFConfig) { c.SharedKey = c.SharedKey[:len(c.SharedKey)-1] },
			wantErr: "SharedKey",
		},
		{
			name:    "zero TTL",
			mutate:  func(c *identity.CSRFConfig) { c.TokenTTL = 0 },
			wantErr: "TokenTTL",
		},
		{
			name:    "negative TTL",
			mutate:  func(c *identity.CSRFConfig) { c.TokenTTL = -time.Second },
			wantErr: "TokenTTL",
		},
		{
			name:    "no trusted origins",
			mutate:  func(c *identity.CSRFConfig) { c.TrustedOrigins = nil },
			wantErr: "TrustedOrigins",
		},
		{
			name:    "wildcard origin",
			mutate:  func(c *identity.CSRFConfig) { c.TrustedOrigins = []string{"*"} },
			wantErr: "wildcard",
		},
		{
			name:    "origin with no scheme",
			mutate:  func(c *identity.CSRFConfig) { c.TrustedOrigins = []string{"app.example.com"} },
			wantErr: "app.example.com",
		},
		{
			name:    "origin with a non-http scheme",
			mutate:  func(c *identity.CSRFConfig) { c.TrustedOrigins = []string{"ftp://app.example.com"} },
			wantErr: "http",
		},
		{
			name:    "origin with a trailing slash",
			mutate:  func(c *identity.CSRFConfig) { c.TrustedOrigins = []string{"https://app.example.com/"} },
			wantErr: "bare origin",
		},
		{
			name:    "origin with a path",
			mutate:  func(c *identity.CSRFConfig) { c.TrustedOrigins = []string{"https://app.example.com/app"} },
			wantErr: "bare origin",
		},
		{
			name:    "origin with a query",
			mutate:  func(c *identity.CSRFConfig) { c.TrustedOrigins = []string{"https://app.example.com?a=1"} },
			wantErr: "bare origin",
		},
		{
			name:    "origin with userinfo",
			mutate:  func(c *identity.CSRFConfig) { c.TrustedOrigins = []string{"https://user@app.example.com"} },
			wantErr: "bare origin",
		},
		{
			name:    "origin with no host",
			mutate:  func(c *identity.CSRFConfig) { c.TrustedOrigins = []string{"https://"} },
			wantErr: "host",
		},
		{
			name:    "empty origin",
			mutate:  func(c *identity.CSRFConfig) { c.TrustedOrigins = []string{""} },
			wantErr: "TrustedOrigins",
		},
		{
			name:    "duplicate origin",
			mutate:  func(c *identity.CSRFConfig) { c.TrustedOrigins = append(c.TrustedOrigins, c.TrustedOrigins[0]) },
			wantErr: "more than once",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := validCSRF()
			tt.mutate(&cfg)
			err := cfg.Validate()
			if err == nil {
				t.Fatalf("Validate() = nil for %+v, want an error naming %q", cfg, tt.wantErr)
			}
			if !errors.Is(err, identity.ErrInvalidCSRFConfig) {
				t.Errorf("error %v does not wrap ErrInvalidCSRFConfig", err)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error %q does not name %q", err, tt.wantErr)
			}
		})
	}
}

func TestCSRFOriginsThatAreAccepted(t *testing.T) {
	t.Parallel()

	for _, origin := range []string{
		"https://app.example.com",
		"http://localhost:5173",
		"https://app.example.com:8443",
		"http://127.0.0.1:8080",
	} {
		cfg := validCSRF()
		cfg.TrustedOrigins = []string{origin}
		if err := cfg.Validate(); err != nil {
			t.Errorf("origin %q was rejected: %v", origin, err)
		}
	}
}

// TestCSRFConfigCloneSharesNothing is the reader of the difference between a
// copied struct and a copy that owns its bytes. A CSRFConfig held by a Server
// is a struct value, so assignment already copies TokenTTL -- but SharedKey and
// TrustedOrigins are slices, and a caller that reuses or zeroes its key buffer
// after New returns would otherwise change the key the running server signs
// with, with nothing in the Server observing that it happened.
func TestCSRFConfigCloneSharesNothing(t *testing.T) {
	t.Parallel()

	original := validCSRF()
	clone := original.Clone()

	if !bytes.Equal(clone.SharedKey, original.SharedKey) {
		t.Fatalf("Clone().SharedKey = %q, want %q", clone.SharedKey, original.SharedKey)
	}
	if len(clone.TrustedOrigins) != len(original.TrustedOrigins) {
		t.Fatalf("Clone().TrustedOrigins = %q, want %q", clone.TrustedOrigins, original.TrustedOrigins)
	}
	if clone.TokenTTL != original.TokenTTL {
		t.Errorf("Clone().TokenTTL = %v, want %v", clone.TokenTTL, original.TokenTTL)
	}

	// Write through the ORIGINAL and read through the CLONE.
	for i := range original.SharedKey {
		original.SharedKey[i] = 'x'
	}
	original.TrustedOrigins[0] = "https://evil.example.com"
	if bytes.Contains(clone.SharedKey, []byte("x")) {
		t.Errorf("clone key became %q after the original was overwritten", clone.SharedKey)
	}
	if clone.TrustedOrigins[0] != "https://app.example.com" {
		t.Errorf("clone origin became %q after the original was overwritten", clone.TrustedOrigins[0])
	}

	// And back the other way, so a Clone that copies only one direction fails.
	fresh := validCSRF()
	freshClone := fresh.Clone()
	for i := range freshClone.SharedKey {
		freshClone.SharedKey[i] = 'y'
	}
	freshClone.TrustedOrigins[0] = "https://evil.example.com"
	if bytes.Contains(fresh.SharedKey, []byte("y")) {
		t.Errorf("original key became %q after the clone was overwritten", fresh.SharedKey)
	}
	if fresh.TrustedOrigins[0] != "https://app.example.com" {
		t.Errorf("original origin became %q after the clone was overwritten", fresh.TrustedOrigins[0])
	}
}

// TestCSRFConfigCloneOfTheZeroValueIsUsable covers the nil-slice arm: a Clone
// that unconditionally allocates would turn a nil key into an empty one, which
// Validate must still reject.
func TestCSRFConfigCloneOfTheZeroValueIsUsable(t *testing.T) {
	t.Parallel()

	clone := identity.CSRFConfig{}.Clone()
	if clone.SharedKey != nil {
		t.Errorf("Clone().SharedKey = %q, want nil", clone.SharedKey)
	}
	if clone.TrustedOrigins != nil {
		t.Errorf("Clone().TrustedOrigins = %q, want nil", clone.TrustedOrigins)
	}
	if err := clone.Validate(); err == nil {
		t.Error("the zero CSRFConfig validated after Clone()")
	}
}

// TestParseOriginNormalizesTheSpaceItClaims enumerates the normalization space
// rather than pinning one string. Each row names a way an operator's entry and
// a browser's Origin header can differ while naming the same origin, and each
// asserts the CANONICAL FORM, so a normalization that collapses everything to a
// constant fails here instead of passing a single-fixture equality.
//
// It is the positive half. TestParseOriginRefusesWhatItCannotNormalize is the
// negative half, and the two together are the whole of what this function
// claims: what it maps together, and what it refuses to map at all.
func TestParseOriginNormalizesTheSpaceItClaims(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, origin, want string
	}{
		{"already canonical", "https://app.example.com", "https://app.example.com"},
		{"upper-case host", "https://APP.EXAMPLE.COM", "https://app.example.com"},
		{"mixed-case host", "https://App.Example.Com", "https://app.example.com"},
		{"upper-case scheme", "HTTPS://app.example.com", "https://app.example.com"},
		{"upper-case scheme and host", "HTTPS://APP.Example.COM", "https://app.example.com"},
		{"explicit https default port", "https://app.example.com:443", "https://app.example.com"},
		{"explicit http default port", "http://app.example.com:80", "http://app.example.com"},
		{"empty port", "https://app.example.com:", "https://app.example.com"},
		{"non-default port is kept", "https://app.example.com:8443", "https://app.example.com:8443"},
		{"http on the https default port is kept", "http://app.example.com:443", "http://app.example.com:443"},
		{"https on the http default port is kept", "https://app.example.com:80", "https://app.example.com:80"},
		{"punycode host", "https://xn--exmple-cua.com", "https://xn--exmple-cua.com"},
		{"punycode host in upper case", "https://XN--EXMPLE-CUA.COM", "https://xn--exmple-cua.com"},
		{"loopback with a port", "http://127.0.0.1:8080", "http://127.0.0.1:8080"},
		{"IPv6 literal", "https://[::1]", "https://[::1]"},
		{"IPv6 literal with a port", "https://[::1]:8443", "https://[::1]:8443"},
		{"IPv6 literal in upper case", "https://[2001:DB8::1]", "https://[2001:db8::1]"},
		{"IPv6 literal on the default port", "https://[::1]:443", "https://[::1]"},
		{"dev server", "http://localhost:5173", "http://localhost:5173"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			origin, err := identity.ParseOrigin(tt.origin)
			if err != nil {
				t.Fatalf("ParseOrigin(%q) = %v, want %q", tt.origin, err, tt.want)
			}
			if got := origin.String(); got != tt.want {
				t.Errorf("ParseOrigin(%q).String() = %q, want %q", tt.origin, got, tt.want)
			}
		})
	}
}

// TestParseOriginDistinguishesOriginsThatDiffer pins the other direction: a
// normalization that maps everything together would satisfy the table above
// with a constant. Each pair below names two origins a browser treats as
// different, and requires the canonical forms to differ.
func TestParseOriginDistinguishesOriginsThatDiffer(t *testing.T) {
	t.Parallel()

	pairs := [][2]string{
		{"https://app.example.com", "http://app.example.com"},
		{"https://app.example.com", "https://app.example.com:8443"},
		{"https://app.example.com", "https://other.example.com"},
		{"https://app.example.com", "https://app.example.com.evil.test"},
		{"http://app.example.com:443", "https://app.example.com"},
		{"http://localhost:5173", "http://localhost:5174"},
		{"https://[::1]", "https://[::2]"},
		{"https://127.0.0.1", "https://localhost"},
	}
	for _, pair := range pairs {
		t.Run(pair[0]+" vs "+pair[1], func(t *testing.T) {
			t.Parallel()

			first, err := identity.ParseOrigin(pair[0])
			if err != nil {
				t.Fatalf("ParseOrigin(%q) = %v", pair[0], err)
			}
			second, err := identity.ParseOrigin(pair[1])
			if err != nil {
				t.Fatalf("ParseOrigin(%q) = %v", pair[1], err)
			}
			if first.String() == second.String() {
				t.Errorf("ParseOrigin(%q) and ParseOrigin(%q) both canonicalize to %q", pair[0], pair[1], first)
			}
		})
	}
}

// TestParseOriginRefusesWhatItCannotNormalize is the negative half of the
// enumerated space. Each row is a form that PARSES as a URL and would
// previously have been accepted as a trusted origin that no browser could ever
// match, or a form whose normalization would be a guess.
func TestParseOriginRefusesWhatItCannotNormalize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, origin, wantMessage string
	}{
		{"empty", "", "empty"},
		{"wildcard", "*", "wildcard"},
		{"unicode host", "https://exämple.com", "punycode"},
		{"unicode host that is otherwise valid", "https://例.test", "punycode"},
		{"trailing dot", "https://app.example.com.", "trailing dot"},
		{"trailing dot with a port", "https://app.example.com.:8443", "trailing dot"},
		{"port with a leading zero", "https://app.example.com:0443", "decimal port"},
		{"port zero", "https://app.example.com:0", "decimal port"},
		{"port above the range", "https://app.example.com:65536", "decimal port"},
		{"no scheme", "app.example.com", "http or https"},
		{"scheme-relative", "//app.example.com", "http or https"},
		{"opaque URL", "https:app.example.com", "names no host"},
		{"non-http scheme", "ftp://app.example.com", "http or https"},
		{"the null origin a sandboxed document sends", "null", "http or https"},
		{"no host", "https://", "names no host"},
		{"trailing slash", "https://app.example.com/", "bare origin"},
		{"path", "https://app.example.com/app", "bare origin"},
		{"query", "https://app.example.com?a=1", "bare origin"},
		{"fragment", "https://app.example.com#f", "bare origin"},
		{"userinfo", "https://user@app.example.com", "bare origin"},
		{"userinfo with a password", "https://user:pass@app.example.com", "bare origin"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			origin, err := identity.ParseOrigin(tt.origin)
			if err == nil {
				t.Fatalf("ParseOrigin(%q) = %q, want an error naming %q", tt.origin, origin, tt.wantMessage)
			}
			if !errors.Is(err, identity.ErrInvalidOrigin) {
				t.Errorf("error %v does not wrap ErrInvalidOrigin", err)
			}
			if !strings.Contains(err.Error(), tt.wantMessage) {
				t.Errorf("error %q does not name %q", err, tt.wantMessage)
			}
			if origin.String() != "" {
				t.Errorf("a rejected origin returned %q, not the zero Origin", origin)
			}
		})
	}
}

// TestParseOriginIsIdempotent is the property the guard's two sides depend on:
// a configured entry is canonicalized once at construction and an inbound
// header on every request, so a canonical form that did not re-canonicalize to
// itself would make the two sides disagree for exactly the inputs that were
// already normalized.
func TestParseOriginIsIdempotent(t *testing.T) {
	t.Parallel()

	for _, text := range []string{
		"https://APP.Example.COM:443", "http://app.example.com:80", "https://app.example.com:",
		"https://[2001:DB8::1]:443", "http://LOCALHOST:5173", "https://XN--EXMPLE-CUA.COM",
	} {
		once, err := identity.ParseOrigin(text)
		if err != nil {
			t.Fatalf("ParseOrigin(%q) = %v", text, err)
		}
		twice, err := identity.ParseOrigin(once.String())
		if err != nil {
			t.Fatalf("ParseOrigin(%q) = %v for the canonical form of %q", once, err, text)
		}
		if twice.String() != once.String() {
			t.Errorf("ParseOrigin(%q) canonicalizes to %q, which canonicalizes to %q", text, once, twice)
		}
	}
}

// TestCanonicalHostnameSharesOneHostImplementationWithParseOrigin holds the
// claim in CanonicalHostname's doc that both entry points refuse the same
// hosts. It is written as a comparison between the two functions rather than as
// a second copy of the host table, so a host rule added to one and not the
// other fails here.
func TestCanonicalHostnameSharesOneHostImplementationWithParseOrigin(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, authority, want string
	}{
		{"bare host", "app.example.com", "app.example.com"},
		{"host with a port", "app.example.com:8443", "app.example.com"},
		{"upper case", "APP.Example.COM:443", "app.example.com"},
		{"IPv6 literal", "[::1]", "::1"},
		{"IPv6 literal with a port", "[2001:DB8::1]:8443", "2001:db8::1"},
		{"loopback", "127.0.0.1:8080", "127.0.0.1"},
		{"trailing dot", "app.example.com.", ""},
		{"unicode", "exämple.com", ""},
		{"empty", "", ""},
		{"a URL rather than an authority", "https://app.example.com", ""},
		{"a path", "app.example.com/x", ""},
		{"unbracketed IPv6", "::1", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := identity.CanonicalHostname(tt.authority)
			if tt.want == "" {
				if err == nil {
					t.Fatalf("CanonicalHostname(%q) = %q, want an error", tt.authority, got)
				}
				if !errors.Is(err, identity.ErrInvalidOrigin) {
					t.Errorf("error %v does not wrap ErrInvalidOrigin", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("CanonicalHostname(%q) = %v, want %q", tt.authority, err, tt.want)
			}
			if got != tt.want {
				t.Errorf("CanonicalHostname(%q) = %q, want %q", tt.authority, got, tt.want)
			}
		})
	}
}

// TestCanonicalHostnameAgreesWithOriginHostname is the tie between the two
// entry points: for every origin the guard can be configured with, the host it
// would compare a Host header against is the host CanonicalHostname produces
// from that origin's own authority. A host rule that lived in only one of them
// would show up here as a disagreement rather than as a guard that rejects
// its own deployment.
func TestCanonicalHostnameAgreesWithOriginHostname(t *testing.T) {
	t.Parallel()

	for _, pair := range [][2]string{
		{"https://APP.Example.COM", "APP.Example.COM"},
		{"https://app.example.com:8443", "app.example.com:8443"},
		{"http://localhost:5173", "localhost:5173"},
		{"https://[2001:DB8::1]:8443", "[2001:DB8::1]:8443"},
		{"http://127.0.0.1:8080", "127.0.0.1:8080"},
	} {
		origin, err := identity.ParseOrigin(pair[0])
		if err != nil {
			t.Fatalf("ParseOrigin(%q) = %v", pair[0], err)
		}
		hostname, err := identity.CanonicalHostname(pair[1])
		if err != nil {
			t.Fatalf("CanonicalHostname(%q) = %v", pair[1], err)
		}
		if origin.Hostname() != hostname {
			t.Errorf("ParseOrigin(%q).Hostname() = %q, but CanonicalHostname(%q) = %q", pair[0], origin.Hostname(), pair[1], hostname)
		}
	}
}

// TestCSRFConfigTreatsCanonicallyEqualOriginsAsDuplicates is the reader of the
// duplicate check's move from string equality to canonical equality. Before it,
// listing an origin twice in two spellings was two entries, which is not a
// security defect on its own but is a configuration the operator did not mean
// and the first sign that the two sides were being compared as written.
func TestCSRFConfigTreatsCanonicallyEqualOriginsAsDuplicates(t *testing.T) {
	t.Parallel()

	for _, second := range []string{
		"https://APP.example.com",
		"https://app.example.com:443",
		"HTTPS://app.example.com",
		"https://app.example.com:",
	} {
		cfg := validCSRF()
		cfg.TrustedOrigins = []string{"https://app.example.com", second}
		err := cfg.Validate()
		if err == nil {
			t.Errorf("Validate() = nil for %q listed beside %q", second, "https://app.example.com")
			continue
		}
		if !strings.Contains(err.Error(), "more than once") {
			t.Errorf("error %q for %q does not report a duplicate", err, second)
		}
	}
}

// TestCSRFConfigRejectsAnOriginThatCouldNeverMatch pins the carry-forward this
// task owed: each entry below used to VALIDATE and could then never equal an
// Origin header. They are now either normalized -- and so accepted, and
// asserted in TestParseOriginNormalizesTheSpaceItClaims -- or refused here.
func TestCSRFConfigRejectsAnOriginThatCouldNeverMatch(t *testing.T) {
	t.Parallel()

	for _, origin := range []string{
		"https://app.example.com.",
		"https://exämple.com",
		"https://app.example.com:0443",
	} {
		cfg := validCSRF()
		cfg.TrustedOrigins = []string{origin}
		err := cfg.Validate()
		if err == nil {
			t.Errorf("Validate() = nil for trusted origin %q", origin)
			continue
		}
		if !errors.Is(err, identity.ErrInvalidCSRFConfig) {
			t.Errorf("error %v for %q does not wrap ErrInvalidCSRFConfig", err, origin)
		}
		if !errors.Is(err, identity.ErrInvalidOrigin) {
			t.Errorf("error %v for %q does not wrap ErrInvalidOrigin", err, origin)
		}
	}
}
