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
