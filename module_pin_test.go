package factory_test

import (
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// releasedLooprigVersions is the exact set of Looprig versions this module may
// name, and the only ones remote-verified as released when the scaffold landed.
// A Looprig module absent from this map has no version Factory may name.
var releasedLooprigVersions = map[string]string{
	"github.com/looprig/core":         "v0.10.0",
	"github.com/looprig/storage":      "v0.6.0",
	"github.com/looprig/fsstore":      "v0.5.1",
	"github.com/looprig/natsstore":    "v0.5.1",
	"github.com/looprig/sessionstore": "v0.12.0",
}

// forbiddenModules may never appear in go.mod at any version, released or not.
// The reason is the import boundary, not the release calendar, and the guard
// consults this map BEFORE releasedLooprigVersions so the ban does not expire
// the day one of them ships.
var forbiddenModules = map[string]string{
	"github.com/looprig/host":    "Factory and Host exchange Core and SessionStore records, never code",
	"github.com/looprig/harness": "Harness is Host's runtime; Factory reads the durable projections of a session, never the runtime",
}

// factoryModulePath is what go.mod must declare.
const factoryModulePath = "github.com/looprig/factory"

// looprigModulePrefix is the namespace the released-version rule governs.
// Third-party dependencies are unconstrained by this guard.
const looprigModulePrefix = "github.com/looprig"

// goModViolations reports everything wrong with a go.mod's SHAPE, and returns
// how many Looprig requirements it examined so the caller can refuse a vacuous
// pass. `go mod verify` checks content hashes, not version shape, so nothing in
// `make check` would otherwise notice a replace directive or a pin moved to a
// pseudo-version.
//
// The maps are parameters rather than package globals read directly, so a test
// can drive the one situation that would expose a wrong check order: a
// forbidden module PRESENT in the released map at the version named.
func goModViolations(content string, released, forbidden map[string]string) (int, []string) {
	forbiddenNames := slices.Sorted(maps.Keys(forbidden))

	looprig := 0
	sawModule := false
	var violations []string
	for _, directive := range parseGoModDirectives(content) {
		where := "go.mod line " + strconv.Itoa(directive.line) + " "
		switch directive.verb {
		case "module":
			sawModule = true
			if len(directive.args) == 0 || directive.args[0] != factoryModulePath {
				violations = append(violations, where+"declares module "+strings.Join(directive.args, " ")+", want "+factoryModulePath)
			}
		case "replace":
			// The verb is banned outright, so classifying the five replace
			// forms is unnecessary here. Naming a forbidden module when one
			// appears is not classification, it is teaching: rejected either
			// way, but "you may not depend on Host" is the fact worth learning.
			detail := ""
			for _, arg := range directive.args {
				if forbiddenName, ok := firstPrefixMatch(arg, forbiddenNames); ok {
					detail = " It also names " + forbiddenName + ", which Factory must never depend on: " + forbidden[forbiddenName] + "."
					break
				}
			}
			violations = append(violations, where+"declares a replace directive ("+strings.Join(directive.args, " ")+
				"); a published module file must not contain one, and a GOWORK=off failure is a dependency release still owed, not something to work around."+detail)
		case "require":
			if len(directive.args) == 0 {
				continue
			}
			module := directive.args[0]
			// FORBIDDEN FIRST, and before the malformed check too: the module
			// NAME is present even on a line missing its version, and "Factory
			// must never depend on Host" is the more useful message than "this
			// line is malformed". Consulting the released map first would make
			// the ban an accident of absence that expires the day Host ships.
			if forbiddenName, ok := firstPrefixMatch(module, forbiddenNames); ok {
				violations = append(violations, where+"requires "+module+", which Factory must never depend on: "+forbidden[forbiddenName])
				continue
			}
			if len(directive.args) < 2 {
				violations = append(violations, where+"has a malformed require ("+strings.Join(directive.args, " ")+")")
				continue
			}
			version := directive.args[1]
			if !pathHasPrefix(module, looprigModulePrefix) {
				continue
			}
			// Counted only once the requirement is well formed and actually
			// checked against the map, because that is what the caller's
			// anti-vacuity floor is asking about.
			looprig++
			want, ok := released[module]
			if !ok {
				violations = append(violations, where+"requires "+module+", which has no released version this module may name")
				continue
			}
			if version != want {
				violations = append(violations, where+"requires "+module+" at "+version+", which is not the released version this module may name ("+want+")")
			}
		}
	}
	if !sawModule {
		violations = append(violations, "go.mod declares no module path")
	}
	return looprig, violations
}

func firstPrefixMatch(module string, prefixes []string) (string, bool) {
	for _, prefix := range prefixes {
		if pathHasPrefix(module, prefix) {
			return prefix, true
		}
	}
	return "", false
}

// goModFields splits a go.mod line into tokens and UNQUOTES each one.
//
// The unquoting is not decoration. go.mod's grammar permits quoted tokens and
// the toolchain accepts them -- `go mod edit -json` resolves
// `require "github.com/looprig/host" v1.0.0` to Path: github.com/looprig/host
// -- so a split-only parser carries the quote characters into args[0], no
// prefix test matches, and the requirement is SILENTLY SKIPPED: not
// forbidden-checked, not version-checked, not counted. A go.mod with one
// ordinary Looprig require and a quoted github.com/looprig/host passed this
// guard entirely while `go build` resolved Host normally.
//
// This is the failure mode named just below -- a guard that understands one
// spelling is defeated by reformatting -- applied to quoting rather than to
// block form, and it is fixed the same way the import guard already fixes it,
// by unquoting rather than by matching text. Both Go string literal forms are
// accepted, since strconv.Unquote takes interpreted and raw quoting alike.
//
// A token that is not a quoted literal is left exactly as it is, so bare paths,
// versions and the => operator are untouched.
func goModFields(line string) []string {
	fields := strings.Fields(line)
	for index, field := range fields {
		if unquoted, err := strconv.Unquote(field); err == nil {
			fields[index] = unquoted
		}
	}
	return fields
}

// parseGoModDirectives reads go.mod's grammar rather than searching its text.
// Both spellings of every verb exist in real files — `require x v1` and a
// `require ( … )` block — and a guard that understood only one would be
// defeated by reformatting the file, which is the same failure mode as matching
// source text for imports.
func parseGoModDirectives(content string) []modDirective {
	var directives []modDirective
	blockVerb := ""
	for index, raw := range strings.Split(content, "\n") {
		line := raw
		// go.mod has line comments only; a module path never contains "//".
		if comment := strings.Index(line, "//"); comment >= 0 {
			line = line[:comment]
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		number := index + 1
		if blockVerb != "" {
			if line == ")" {
				blockVerb = ""
				continue
			}
			directives = append(directives, modDirective{verb: blockVerb, args: goModFields(line), line: number})
			continue
		}
		fields := goModFields(line)
		verb, rest := fields[0], fields[1:]
		if len(rest) == 1 && rest[0] == "(" {
			blockVerb = verb
			continue
		}
		if len(rest) == 0 {
			continue
		}
		directives = append(directives, modDirective{verb: verb, args: rest, line: number})
	}
	return directives
}

type modDirective struct {
	verb string
	args []string
	line int
}

func TestGoModDirectiveParserHandlesBothForms(t *testing.T) {
	t.Parallel()

	const content = `module github.com/looprig/factory

go 1.26.6

require github.com/looprig/core v0.7.0 // a trailing comment

require (
	github.com/looprig/sessionstore v0.1.0
	github.com/looprig/storage v0.6.0 // indirect
)

// A whole-line comment mentioning replace github.com/looprig/host => ../host

replace github.com/looprig/core => ../core

replace (
	github.com/looprig/storage => ../storage
	github.com/looprig/wui v0.1.0 => github.com/looprig/wui v0.2.0
)

exclude github.com/looprig/natsstore v0.5.0

require "github.com/looprig/fsstore" v0.5.1

replace "github.com/looprig/natsstore" => "../natsstore"

tool (
	honnef.co/go/tools/cmd/staticcheck
)
`
	got := parseGoModDirectives(content)
	want := []modDirective{
		{verb: "module", args: []string{"github.com/looprig/factory"}, line: 1},
		{verb: "go", args: []string{"1.26.6"}, line: 3},
		{verb: "require", args: []string{"github.com/looprig/core", "v0.7.0"}, line: 5},
		{verb: "require", args: []string{"github.com/looprig/sessionstore", "v0.1.0"}, line: 8},
		{verb: "require", args: []string{"github.com/looprig/storage", "v0.6.0"}, line: 9},
		{verb: "replace", args: []string{"github.com/looprig/core", "=>", "../core"}, line: 14},
		{verb: "replace", args: []string{"github.com/looprig/storage", "=>", "../storage"}, line: 17},
		{verb: "replace", args: []string{"github.com/looprig/wui", "v0.1.0", "=>", "github.com/looprig/wui", "v0.2.0"}, line: 18},
		{verb: "exclude", args: []string{"github.com/looprig/natsstore", "v0.5.0"}, line: 21},
		// go.mod's grammar permits quoted tokens and the toolchain accepts
		// them: `go mod edit -json` resolves a quoted path to the bare one. A
		// parser that split on whitespace and stopped would carry the quote
		// characters into args[0], so no prefix test would match and the
		// requirement would be silently skipped -- neither forbidden-checked
		// nor version-checked. That is this file's own stated failure mode,
		// "defeated by reformatting", applied to quoting rather than blocks.
		{verb: "require", args: []string{"github.com/looprig/fsstore", "v0.5.1"}, line: 23},
		{verb: "replace", args: []string{"github.com/looprig/natsstore", "=>", "../natsstore"}, line: 25},
		{verb: "tool", args: []string{"honnef.co/go/tools/cmd/staticcheck"}, line: 28},
	}
	if len(got) != len(want) {
		t.Fatalf("parsed %d directives, want %d:\ngot  %+v\nwant %+v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i].verb != want[i].verb || got[i].line != want[i].line || !slices.Equal(got[i].args, want[i].args) {
			t.Errorf("directive %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestGoModViolations(t *testing.T) {
	t.Parallel()

	released := map[string]string{
		"github.com/looprig/core":         "v0.7.0",
		"github.com/looprig/sessionstore": "v0.1.0",
		"github.com/looprig/storage":      "v0.6.0",
	}
	forbidden := map[string]string{
		"github.com/looprig/host":    "Factory and Host exchange Core and SessionStore records, never code",
		"github.com/looprig/harness": "Harness is Host's runtime",
	}

	tests := []struct {
		name          string
		content       string
		wantLooprig   int
		wantViolation string // substring; "" means no violation at all
		wantAbsent    string // substring no violation may contain
	}{
		{
			name:        "the shipped shape",
			content:     "module github.com/looprig/factory\n\ngo 1.26.6\n\nrequire (\n\tgithub.com/looprig/core v0.7.0\n\tgithub.com/looprig/sessionstore v0.1.0\n)\n\nrequire github.com/looprig/storage v0.6.0 // indirect\n",
			wantLooprig: 3,
		},
		{
			name:          "a single-line replace",
			content:       "module github.com/looprig/factory\n\nreplace github.com/looprig/core => ../core\n",
			wantViolation: "line 3 declares a replace directive",
		},
		{
			name:          "a replace inside a block",
			content:       "module github.com/looprig/factory\n\nreplace (\n\tgithub.com/looprig/core => ../core\n)\n",
			wantViolation: "line 4 declares a replace directive",
		},
		{
			name:          "an unreleased version of a released module",
			content:       "module github.com/looprig/factory\n\nrequire github.com/looprig/core v0.8.0\n",
			wantLooprig:   1,
			wantViolation: "requires github.com/looprig/core at v0.8.0, which is not the released version this module may name (v0.7.0)",
		},
		{
			name:          "a pseudo-version",
			content:       "module github.com/looprig/factory\n\nrequire github.com/looprig/core v0.7.1-0.20260901120000-abcdef123456\n",
			wantLooprig:   1,
			wantViolation: "is not the released version this module may name (v0.7.0)",
		},
		{
			name:          "a looprig module with no released version at all",
			content:       "module github.com/looprig/factory\n\nrequire github.com/looprig/pgstore v0.1.0\n",
			wantLooprig:   1,
			wantViolation: "requires github.com/looprig/pgstore, which has no released version this module may name",
		},
		{
			name:    "a forbidden module",
			content: "module github.com/looprig/factory\n\nrequire github.com/looprig/host v0.1.0\n",
			// Zero: a forbidden module never reaches the released-version map,
			// so it is not one of the requirements that map was consulted for.
			wantLooprig:   0,
			wantViolation: "requires github.com/looprig/host, which Factory must never depend on: Factory and Host exchange Core and SessionStore records, never code",
		},
		{
			name:          "a forbidden module inside a block",
			content:       "module github.com/looprig/factory\n\nrequire (\n\tgithub.com/looprig/core v0.7.0\n\tgithub.com/looprig/host v0.1.0 // indirect\n)\n",
			wantLooprig:   1,
			wantViolation: "line 5 requires github.com/looprig/host, which Factory must never depend on",
		},
		{
			name:        "third-party modules are unconstrained",
			content:     "module github.com/looprig/factory\n\nrequire (\n\tgithub.com/go-chi/chi/v5 v5.2.1\n\tgolang.org/x/sync v0.22.0 // indirect\n)\n",
			wantLooprig: 0,
		},
		{
			name:          "the wrong module path",
			content:       "module github.com/looprig/host\n",
			wantViolation: "declares module github.com/looprig/host, want github.com/looprig/factory",
		},
		{
			name:          "no module path at all",
			content:       "go 1.26.6\n",
			wantViolation: "go.mod declares no module path",
		},
		{
			name:    "a malformed require is reported and is not counted as checked",
			content: "module github.com/looprig/factory\n\nrequire github.com/looprig/core\n",
			// Zero, deliberately: the count is "Looprig requirements compared
			// against the released map", and this line never reached it.
			wantLooprig:   0,
			wantViolation: "has a malformed require",
		},
		{
			name:          "a forbidden module is named even when its line is malformed",
			content:       "module github.com/looprig/factory\n\nrequire github.com/looprig/host\n",
			wantLooprig:   0,
			wantViolation: "requires github.com/looprig/host, which Factory must never depend on",
		},
		{
			name:          "a QUOTED forbidden module",
			content:       "module github.com/looprig/factory\n\nrequire \"github.com/looprig/host\" v1.0.0\n",
			wantLooprig:   0,
			wantViolation: "requires github.com/looprig/host, which Factory must never depend on",
		},
		{
			name:          "a QUOTED pin off its released version",
			content:       "module github.com/looprig/factory\n\nrequire \"github.com/looprig/core\" v0.8.0\n",
			wantLooprig:   1,
			wantViolation: "requires github.com/looprig/core at v0.8.0, which is not the released version this module may name (v0.7.0)",
		},
		{
			name:          "a replace naming a forbidden module says so",
			content:       "module github.com/looprig/factory\n\nreplace github.com/looprig/host v1.0.0 => ../host\n",
			wantViolation: "It also names github.com/looprig/host, which Factory must never depend on",
		},
		{
			name:          "an ordinary replace does not mention a forbidden module",
			content:       "module github.com/looprig/factory\n\nreplace github.com/looprig/core => ../core\n",
			wantViolation: "declares a replace directive (github.com/looprig/core => ../core)",
			wantAbsent:    "must never depend on",
		},
		{
			name:          "a QUOTED replace",
			content:       "module github.com/looprig/factory\n\nreplace \"github.com/looprig/core\" => \"../core\"\n",
			wantViolation: "line 3 declares a replace directive (github.com/looprig/core => ../core)",
		},
		{
			name:        "a QUOTED module path is still the module path",
			content:     "module \"github.com/looprig/factory\"\n\nrequire github.com/looprig/core v0.7.0\n",
			wantLooprig: 1,
		},
		{
			name:          "a raw-quoted forbidden module",
			content:       "module github.com/looprig/factory\n\nrequire `github.com/looprig/harness` v1.0.0\n",
			wantLooprig:   0,
			wantViolation: "requires github.com/looprig/harness, which Factory must never depend on",
		},
		{
			name:          "a nested module under a forbidden one",
			content:       "module github.com/looprig/factory\n\nrequire github.com/looprig/host/tools v1.0.0\n",
			wantLooprig:   0,
			wantViolation: "requires github.com/looprig/host/tools, which Factory must never depend on",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			looprig, violations := goModViolations(tt.content, released, forbidden)
			if looprig != tt.wantLooprig {
				t.Errorf("looprig requires = %d, want %d", looprig, tt.wantLooprig)
			}
			if tt.wantViolation == "" {
				if len(violations) != 0 {
					t.Fatalf("violations = %q, want none", violations)
				}
				return
			}
			if !slices.ContainsFunc(violations, func(v string) bool { return strings.Contains(v, tt.wantViolation) }) {
				t.Fatalf("violations = %q, want one containing %q", violations, tt.wantViolation)
			}
			if tt.wantAbsent != "" && slices.ContainsFunc(violations, func(v string) bool { return strings.Contains(v, tt.wantAbsent) }) {
				t.Fatalf("violations = %q, none may contain %q", violations, tt.wantAbsent)
			}
		})
	}
}

// TestForbiddenModulesAreRejectedBeforeTheVersionMap is the defect the sibling
// host module shipped: its version guard rejects a module for having no
// released version, so the ban is an ACCIDENT OF ABSENCE and expires the day
// that module is released and added to the map.
//
// Here the forbidden list is consulted first, and this drives the exact
// situation that would expose the ordering — host present in the RELEASED map
// at the version named — and asserts the MESSAGE, not merely that something
// was reported.
func TestForbiddenModulesAreRejectedBeforeTheVersionMap(t *testing.T) {
	t.Parallel()

	releasedIncludingHost := map[string]string{
		"github.com/looprig/core": "v0.7.0",
		"github.com/looprig/host": "v1.0.0",
	}
	forbidden := map[string]string{
		"github.com/looprig/host": "Factory and Host exchange Core and SessionStore records, never code",
	}
	const content = "module github.com/looprig/factory\n\nrequire github.com/looprig/host v1.0.0\n"

	_, violations := goModViolations(content, releasedIncludingHost, forbidden)
	if len(violations) != 1 {
		t.Fatalf("violations = %q, want exactly one: a released, correctly-pinned Host is still forbidden", violations)
	}
	if !strings.Contains(violations[0], "which Factory must never depend on") {
		t.Fatalf("violation = %q, want the boundary reason. A message about a missing release makes the ban an accident of absence that expires when Host ships",
			violations[0])
	}
}

// TestEveryForbiddenModuleIsAnEverywhereImportRule ties the two guards
// together: go.mod may not require exactly those modules no file may import.
// A rule added to boundaryRules with scope "" and no entry here would leave the
// module free to depend on something no file may use.
func TestEveryForbiddenModuleIsAnEverywhereImportRule(t *testing.T) {
	t.Parallel()

	if len(forbiddenModules) == 0 {
		t.Fatal("no modules are forbidden in go.mod, so that half of the guard is vacuous")
	}
	for _, rule := range boundaryRules {
		if rule.scope != "" {
			continue
		}
		sample := sampleImports[rule.name]
		found := false
		for module := range forbiddenModules {
			if pathHasPrefix(sample, module) {
				found = true
			}
		}
		if !found {
			t.Errorf("import rule %q forbids %q everywhere, but no entry in forbiddenModules stops go.mod requiring it", rule.name, sample)
		}
	}
	for module, reason := range forbiddenModules {
		if strings.TrimSpace(reason) == "" {
			t.Errorf("forbidden module %q has no reason", module)
		}
		if _, forbiddenEverywhere := ruleForbiddingEverywhere(module); !forbiddenEverywhere {
			t.Errorf("go.mod may not require %q, but no import rule forbids it everywhere", module)
		}
	}
}

func ruleForbiddingEverywhere(importPath string) (boundaryRule, bool) {
	for _, rule := range boundaryRules {
		if rule.scope == "" && rule.matches(importPath) {
			return rule, true
		}
	}
	return boundaryRule{}, false
}

// TestGoModPinsOnlyReleasedVersionsAndDeclaresNoReplace is the live assertion.
func TestGoModPinsOnlyReleasedVersionsAndDeclaresNoReplace(t *testing.T) {
	t.Parallel()

	content, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	looprig, violations := goModViolations(string(content), releasedLooprigVersions, forbiddenModules)
	for _, violation := range violations {
		t.Error(violation)
	}
	// Anti-vacuity, floored on the axis that cannot legitimately shrink: this
	// module always names at least one Looprig dependency. Zero would mean the
	// parser found nothing and the released-version map was never consulted.
	if looprig == 0 {
		t.Fatal("go.mod names no Looprig module, so the released-version check is vacuous")
	}
	t.Logf("checked %d Looprig requirements against %d released versions", looprig, len(releasedLooprigVersions))
}
