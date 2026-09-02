package factory_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/looprig/factory/internal/modfiles"
)

// segmentPrefix is an INDEPENDENT implementation of the containment question,
// written by splitting into segments rather than by comparing string prefixes.
// The guard uses the string form because it is cheap and reads clearly; the
// risk of that form is exactly the substring family of bugs this program has
// been bitten by, so the two are differentially fuzzed against each other.
func segmentPrefix(p, prefix string) bool {
	if prefix == "" {
		return false
	}
	have := strings.Split(p, "/")
	want := strings.Split(prefix, "/")
	if len(want) > len(have) {
		return false
	}
	return slices.Equal(have[:len(want)], want)
}

func FuzzPathHasPrefixMatchesSegmentOracle(f *testing.F) {
	for _, seed := range [][2]string{
		{"internal/realtime/clientlink", "internal/realtime"},
		{"internal/realtimefanout", "internal/realtime"},
		{"cmd/factoryctl", "cmd/factory"},
		{"github.com/looprig/hostage", "github.com/looprig/host"},
		{".", "internal/realtime"},
		{"", ""},
		{"a//b", "a"},
		{"a/", "a/"},
	} {
		f.Add(seed[0], seed[1])
	}

	f.Fuzz(func(t *testing.T, p, prefix string) {
		if got, want := pathHasPrefix(p, prefix), segmentPrefix(p, prefix); got != want {
			t.Fatalf("pathHasPrefix(%q, %q) = %v, but the segment oracle says %v", p, prefix, got, want)
		}
	})
}

// FuzzScopedRulesNeverEscapeTheirScope drives the whole decision for every rule
// over arbitrary directories. Each payload is an import the rule matches, so an
// "allowed" answer is only ever explicable by the scope — which makes the
// escape probe falsifiable rather than trivially green.
func FuzzScopedRulesNeverEscapeTheirScope(f *testing.F) {
	for _, seed := range []string{
		".", "", "internal", "internal/realtime", "internal/realtime/clientlink",
		"internal/realtimefanout", "cmd/factory", "cmd/factoryctl",
		"internal/placement", "internal/placement/kubernetes", "/", "a//b", "..",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, dir string) {
		if len(boundaryRules) == 0 {
			t.Fatal("no boundary rules are declared, so this target asserts nothing")
		}
		for _, rule := range boundaryRules {
			sample, ok := sampleImports[rule.name]
			if !ok {
				t.Fatalf("rule %q has no sample import", rule.name)
			}
			if !rule.matches(sample) {
				t.Fatalf("rule %q does not match its own sample import %q", rule.name, sample)
			}
			violated, forbidden := violatedRule(dir, sample)
			wantForbidden := rule.scope == "" || !segmentPrefix(dir, rule.scope)
			if forbidden != wantForbidden {
				t.Fatalf("violatedRule(%q, %q) forbidden = %v, want %v (rule %q, scope %q)",
					dir, sample, forbidden, wantForbidden, rule.name, rule.scope)
			}
			if forbidden && violated.name != rule.name {
				t.Fatalf("violatedRule(%q, %q) attributed the rejection to %q, want %q",
					dir, sample, violated.name, rule.name)
			}
		}
	})
}

// FuzzModfilesDecisionSurfaceMatchesTheSanctionedSet generalises the table in
// import_boundary_test.go over arbitrary names.
//
// The table exists because a coverage-guided mutator will not reliably invent
// the string "generated"; this exists because the table cannot enumerate every
// name a future edit might add. Neither subsumes the other, and the oracle is
// an independent restatement of the justification rather than a copy of the
// implementation, so a widened rule dies here whatever reason string it
// invents -- including one that reuses an existing reason.
func FuzzModfilesDecisionSurfaceMatchesTheSanctionedSet(f *testing.F) {
	for _, seed := range []string{
		"", "a", ".", "_", "..", "vendor", "vendored", "testdata", "testdata2",
		".git", ".github", ".worktrees", "_scratch", "generated", "gen",
		"zz_generated.go", "node_modules", "third_party", "internal", "cmd",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, name string) {
		wantReason, wantIgnored := sanctionedIgnoredDirectory(name)
		gotReason, gotIgnored := modfiles.IgnoredDirectory(name)
		if gotIgnored != wantIgnored || gotReason != wantReason {
			t.Fatalf("modfiles.IgnoredDirectory(%q) = (%q, %v), sanctioned answer is (%q, %v)",
				name, gotReason, gotIgnored, wantReason, wantIgnored)
		}
		wantReason, wantIgnored = sanctionedIgnoredFile(name)
		gotReason, gotIgnored = modfiles.IgnoredFile(name)
		if gotIgnored != wantIgnored || gotReason != wantReason {
			t.Fatalf("modfiles.IgnoredFile(%q) = (%q, %v), sanctioned answer is (%q, %v)",
				name, gotReason, gotIgnored, wantReason, wantIgnored)
		}
	})
}
