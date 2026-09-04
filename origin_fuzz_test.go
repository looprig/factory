package factory_test

import (
	"strings"
	"testing"

	factoryidentity "github.com/looprig/factory/identity"
)

// FuzzParseOriginCanonicalizesToAStableForm defends the "for all hosts"
// half of the origin normalization, which a table of chosen origins cannot: a
// mutation that special-cases the strings a table happens to name is invisible
// to that table and visible here.
//
// The properties are the two the guard's comparison actually rests on, plus the
// tie between the two entry points:
//
//   - a rejected origin returns the ZERO Origin, so a caller that ignores the
//     error compares against nothing rather than against a partial value;
//   - canonicalization is IDEMPOTENT and its output is lower case and ASCII,
//     which is what lets a configured entry be canonicalized once at
//     composition and an inbound header on every request;
//   - CanonicalHostname, applied to the canonical origin's own authority,
//     returns the same hostname the Origin carries -- so the host check and the
//     origin check cannot disagree about what a host is.
//
// There is no independent oracle here, and the reason is worth stating rather
// than leaving as an omission: a second implementation of RFC 3986 host parsing
// would be the same guesses written twice. These are relations the one
// implementation must satisfy, which a wrong implementation can fail.
func FuzzParseOriginCanonicalizesToAStableForm(f *testing.F) {
	for _, seed := range []string{
		"", "*", "null", "https://app.example.com", "HTTPS://APP.Example.COM",
		"https://app.example.com:443", "http://app.example.com:80", "https://app.example.com:",
		"https://app.example.com.", "https://exämple.com", "https://xn--exmple-cua.com",
		"https://[::1]:8443", "https://[2001:DB8::1]", "http://127.0.0.1:8080",
		"https://user@app.example.com", "https://app.example.com/x", "ftp://app.example.com",
		"https://app.example.com:0443", "https://app.example.com:65536", "//app.example.com",
		"https:app.example.com", "http://localhost:5173", "https://a_b.example.com",
		"https://[a:b]", "https://app.example.com?a=1", "https://app.example.com#f",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, text string) {
		origin, err := factoryidentity.ParseOrigin(text)
		if err != nil {
			if origin.String() != "" {
				t.Fatalf("ParseOrigin(%q) returned %q beside its error %v", text, origin, err)
			}
			if origin.Hostname() != "" {
				t.Fatalf("ParseOrigin(%q) returned hostname %q beside its error %v", text, origin.Hostname(), err)
			}
			return
		}

		canonical := origin.String()
		if canonical == "" {
			t.Fatalf("ParseOrigin(%q) succeeded and returned the zero Origin", text)
		}
		if strings.ToLower(canonical) != canonical {
			t.Fatalf("ParseOrigin(%q) = %q, which is not lower case", text, canonical)
		}
		for i := range len(canonical) {
			if canonical[i] >= 0x80 {
				t.Fatalf("ParseOrigin(%q) = %q, which is not ASCII", text, canonical)
			}
		}

		again, err := factoryidentity.ParseOrigin(canonical)
		if err != nil {
			t.Fatalf("ParseOrigin(%q) = %q, which ParseOrigin then rejects: %v", text, canonical, err)
		}
		if again.String() != canonical {
			t.Fatalf("ParseOrigin(%q) = %q, which canonicalizes further to %q", text, canonical, again)
		}
		if again.Hostname() != origin.Hostname() {
			t.Fatalf("hostname of %q is %q but of its canonical form %q is %q", text, origin.Hostname(), canonical, again.Hostname())
		}

		scheme, authority, found := strings.Cut(canonical, "://")
		if !found || (scheme != "http" && scheme != "https") {
			t.Fatalf("ParseOrigin(%q) = %q, which is not scheme://authority", text, canonical)
		}
		hostname, err := factoryidentity.CanonicalHostname(authority)
		if err != nil {
			t.Fatalf("CanonicalHostname(%q), the authority of the canonical origin of %q, = %v", authority, text, err)
		}
		if hostname != origin.Hostname() {
			t.Fatalf("CanonicalHostname(%q) = %q but the origin's hostname is %q", authority, hostname, origin.Hostname())
		}
	})
}
