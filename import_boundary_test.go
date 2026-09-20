package factory_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/looprig/factory/internal/modfiles"
)

// ---------------------------------------------------------------------------
// The boundary rules.
// ---------------------------------------------------------------------------

// boundaryRule forbids a dependency, optionally everywhere but one subtree.
type boundaryRule struct {
	// name is what a violation and the coverage assertion below call this rule.
	name string

	// scope is the ONLY directory subtree, relative to the module root, in
	// which the dependency is permitted. The empty string means "nowhere":
	// the dependency is forbidden in every file of the module.
	scope string

	// matches reports whether an import path belongs to the forbidden
	// dependency. It is applied to a PARSED import path, never to source text.
	matches func(importPath string) bool

	// docToken is how the prose documents name this dependency. It is checked
	// against matches, so it cannot drift from the rule, and it is what ties
	// the three documents to this list -- see documented_rules_test.go.
	docToken string
}

// boundaryRules is the whole boundary. Each entry is a function over a PARSED
// import path and the file's directory; nothing here inspects source text,
// because a substring ban over source is defeated by a line break, a rename or
// a package alias, and cannot tell an import from a comment that mentions one.
//
// The three scoped rules bound their exemption STRUCTURALLY, by the subtree the
// change itself must create, rather than by a hand-kept list of package names
// beside the guard. Adding a second package under internal/realtime is inside
// the grant on purpose; adding a second binary beside cmd/factory is not, and
// TestBoundaryRulesClassifyEveryCase drives both of those readings.
var boundaryRules = []boundaryRule{
	{
		// Factory never links Host. Everything they exchange is a Core or
		// SessionStore record; see contract.go.
		name:     "host",
		docToken: "github.com/looprig/host",
		scope:    "",
		matches:  importUnder("github.com/looprig/host"),
	},
	{
		// Harness is Host's runtime, not Factory's. Factory reads the durable
		// projections of a Harness session, never Harness itself.
		name:     "harness",
		docToken: "github.com/looprig/harness",
		scope:    "",
		matches:  importUnder("github.com/looprig/harness"),
	},
	{
		// Realtime transport is an implementation detail of one subtree. The
		// whole centrifugal organisation is named rather than a single module
		// so centrifuge-go and the wire protocol package are covered too.
		name:     "centrifuge",
		docToken: "github.com/centrifugal/",
		scope:    "internal/realtime",
		matches:  importUnder("github.com/centrifugal"),
	},
	{
		// The web UI is composed by the binary. A library embedding of Factory
		// takes an http.Handler and must not drag a UI bundle in with it.
		name:     "wui",
		docToken: "github.com/looprig/wui",
		scope:    "cmd/factory",
		matches:  importUnder("github.com/looprig/wui"),
	},
	{
		// The Kubernetes client belongs to the placement adapter that runbook
		// 07 adds, and to nothing else. Placement policy is expressed over
		// Core/SessionStore records; no Kubernetes type crosses that seam.
		name:     "kubernetes",
		docToken: "k8s.io/",
		scope:    "internal/placement/kubernetes",
		matches:  kubernetesSDK,
	},
}

// violatedRule reports the rule, if any, that forbids importPath in a file
// whose directory is dir (slash-separated, relative to the module root, "." at
// the root).
func violatedRule(dir, importPath string) (boundaryRule, bool) {
	for _, rule := range boundaryRules {
		if !rule.matches(importPath) {
			continue
		}
		if rule.scope != "" && pathHasPrefix(dir, rule.scope) {
			continue
		}
		return rule, true
	}
	return boundaryRule{}, false
}

// importUnder matches a module path and everything beneath it, by SEGMENT. The
// distinction is the rule: "github.com/looprig/host" must not claim
// "github.com/looprig/hostage", and "internal/realtime" must not claim
// "internal/realtimefanout".
func importUnder(prefix string) func(string) bool {
	return func(importPath string) bool { return pathHasPrefix(importPath, prefix) }
}

// kubernetesSDK matches the Kubernetes client libraries by their module HOST,
// which is how that SDK is spelled: k8s.io/... and sigs.k8s.io/....
func kubernetesSDK(importPath string) bool {
	host, _, _ := strings.Cut(importPath, "/")
	return host == "k8s.io" || host == "sigs.k8s.io"
}

// pathHasPrefix reports whether p equals prefix or lies beneath it, comparing
// whole slash-separated segments. Both halves of the guard use it: import paths
// and directory scopes are the same shape of thing.
func pathHasPrefix(p, prefix string) bool {
	if prefix == "" {
		return false
	}
	return p == prefix || strings.HasPrefix(p, prefix+"/")
}

// ---------------------------------------------------------------------------
// Rule-by-rule table.
// ---------------------------------------------------------------------------

type boundaryCase struct {
	name       string
	rule       string // the rule this case is about; "" for a case no rule may claim
	dir        string // directory of the file, relative to the module root
	importPath string
	want       string // name of the rule expected to reject, or "" for permitted
}

func boundaryCases() []boundaryCase {
	return []boundaryCase{
		// Rule 1: host — forbidden everywhere.
		{name: "host at root", rule: "host", dir: ".", importPath: "github.com/looprig/host", want: "host"},
		{name: "host subpackage in internal", rule: "host", dir: "internal/routing", importPath: "github.com/looprig/host/hostlink", want: "host"},
		{name: "host in the binary", rule: "host", dir: "cmd/factory", importPath: "github.com/looprig/host", want: "host"},
		{name: "host in realtime", rule: "host", dir: "internal/realtime/hostlink", importPath: "github.com/looprig/host", want: "host"},
		{name: "host in a test-support tree", rule: "host", dir: "internal/testkit", importPath: "github.com/looprig/host", want: "host"},
		{name: "host prefix is not a substring match", rule: "host", dir: ".", importPath: "github.com/looprig/hostage", want: ""},
		{name: "a package merely named hostlink under core is fine", rule: "host", dir: ".", importPath: "github.com/looprig/core/hostlink", want: ""},

		// Rule 2: harness — forbidden everywhere.
		{name: "harness at root", rule: "harness", dir: ".", importPath: "github.com/looprig/harness", want: "harness"},
		{name: "harness subpackage", rule: "harness", dir: "internal/admission", importPath: "github.com/looprig/harness/pkg/rig", want: "harness"},
		{name: "harness in the binary", rule: "harness", dir: "cmd/factory", importPath: "github.com/looprig/harness", want: "harness"},
		{name: "harnessing is a different module", rule: "harness", dir: ".", importPath: "github.com/looprig/harnessing", want: ""},

		// Rule 3: Centrifuge — permitted only under internal/realtime.
		{name: "centrifuge at root", rule: "centrifuge", dir: ".", importPath: "github.com/centrifugal/centrifuge", want: "centrifuge"},
		{name: "centrifuge in the binary", rule: "centrifuge", dir: "cmd/factory", importPath: "github.com/centrifugal/centrifuge", want: "centrifuge"},
		{name: "centrifuge in a sibling internal package", rule: "centrifuge", dir: "internal/httpapi", importPath: "github.com/centrifugal/centrifuge", want: "centrifuge"},
		{name: "centrifuge in realtime itself", rule: "centrifuge", dir: "internal/realtime", importPath: "github.com/centrifugal/centrifuge", want: ""},
		{name: "centrifuge in a realtime subpackage", rule: "centrifuge", dir: "internal/realtime/clientlink", importPath: "github.com/centrifugal/centrifuge", want: ""},
		{name: "centrifuge in a second realtime subpackage", rule: "centrifuge", dir: "internal/realtime/hostlink", importPath: "github.com/centrifugal/centrifuge", want: ""},
		{name: "centrifuge in a deeper realtime subpackage", rule: "centrifuge", dir: "internal/realtime/hostlink/internal/dial", importPath: "github.com/centrifugal/centrifuge", want: ""},
		{name: "centrifuge-go in realtime", rule: "centrifuge", dir: "internal/realtime/hostlink", importPath: "github.com/centrifugal/centrifuge-go", want: ""},
		{name: "centrifuge-go outside realtime", rule: "centrifuge", dir: "internal/routing", importPath: "github.com/centrifugal/centrifuge-go", want: "centrifuge"},
		{name: "centrifugal protocol outside realtime", rule: "centrifuge", dir: ".", importPath: "github.com/centrifugal/protocol", want: "centrifuge"},
		{name: "a sibling directory whose name merely starts with realtime is out of scope", rule: "centrifuge", dir: "internal/realtimefanout", importPath: "github.com/centrifugal/centrifuge", want: "centrifuge"},
		{name: "a realtime directory somewhere else is out of scope", rule: "centrifuge", dir: "cmd/factory/internal/realtime", importPath: "github.com/centrifugal/centrifuge", want: "centrifuge"},

		// Rule 4: WUI — permitted only under cmd/factory.
		{name: "wui at root", rule: "wui", dir: ".", importPath: "github.com/looprig/wui", want: "wui"},
		{name: "wui in an internal package", rule: "wui", dir: "internal/httpapi", importPath: "github.com/looprig/wui", want: "wui"},
		{name: "wui in cmd/factory", rule: "wui", dir: "cmd/factory", importPath: "github.com/looprig/wui", want: ""},
		{name: "wui subpackage in cmd/factory", rule: "wui", dir: "cmd/factory", importPath: "github.com/looprig/wui/contract", want: ""},
		{name: "wui below cmd/factory", rule: "wui", dir: "cmd/factory/internal/ui", importPath: "github.com/looprig/wui", want: ""},
		{name: "wui in a second binary beside cmd/factory", rule: "wui", dir: "cmd/factoryctl", importPath: "github.com/looprig/wui", want: "wui"},
		{name: "wui in cmd itself", rule: "wui", dir: "cmd", importPath: "github.com/looprig/wui", want: "wui"},

		// Rule 5: Kubernetes SDK — permitted only in the placement adapter.
		{name: "client-go at root", rule: "kubernetes", dir: ".", importPath: "k8s.io/client-go/kubernetes", want: "kubernetes"},
		{name: "apimachinery in placement policy", rule: "kubernetes", dir: "internal/placement", importPath: "k8s.io/apimachinery/pkg/api/errors", want: "kubernetes"},
		{name: "api in the binary", rule: "kubernetes", dir: "cmd/factory", importPath: "k8s.io/api/core/v1", want: "kubernetes"},
		{name: "client-go in the adapter", rule: "kubernetes", dir: "internal/placement/kubernetes", importPath: "k8s.io/client-go/kubernetes", want: ""},
		{name: "controller-runtime in the adapter", rule: "kubernetes", dir: "internal/placement/kubernetes", importPath: "sigs.k8s.io/controller-runtime/pkg/client", want: ""},
		{name: "controller-runtime outside the adapter", rule: "kubernetes", dir: "internal/routing", importPath: "sigs.k8s.io/controller-runtime/pkg/client", want: "kubernetes"},
		{name: "adapter subpackage", rule: "kubernetes", dir: "internal/placement/kubernetes/names", importPath: "k8s.io/api/core/v1", want: ""},
		{name: "a package merely named kubernetes elsewhere is out of scope", rule: "kubernetes", dir: "internal/routing/kubernetes", importPath: "k8s.io/api/core/v1", want: "kubernetes"},
		{name: "a module whose host merely ends in k8s.io is not the SDK", rule: "kubernetes", dir: ".", importPath: "github.com/example/k8s.io/thing", want: ""},

		// The legal shapes, swept as hard as the illegal ones. Every one of
		// these is ordinary Factory code and none may be reported.
		{name: "standard library", dir: "internal/httpapi", importPath: "net/http"},
		{name: "standard library nested", dir: ".", importPath: "encoding/json"},
		{name: "Core root", dir: "internal/admission", importPath: "github.com/looprig/core"},
		{name: "Core sessionwire", dir: "internal/realtime/hostlink", importPath: "github.com/looprig/core/sessionwire/v1"},
		{name: "SessionStore", dir: "internal/routing", importPath: "github.com/looprig/sessionstore"},
		{name: "SessionStore in the realtime tree", dir: "internal/realtime/delivery", importPath: "github.com/looprig/sessionstore"},
		{name: "SessionStore in the kubernetes adapter", dir: "internal/placement/kubernetes", importPath: "github.com/looprig/sessionstore"},
		{name: "Storage", dir: "cmd/factory", importPath: "github.com/looprig/storage"},
		{name: "a storage provider in the binary", dir: "cmd/factory", importPath: "github.com/looprig/natsstore"},
		{name: "Factory's own packages", dir: "internal/httpapi", importPath: "github.com/looprig/factory/internal/identity"},
		{name: "the module root from a subpackage", dir: "internal/httpapi", importPath: "github.com/looprig/factory"},
		{name: "a third-party HTTP router", dir: "internal/httpapi", importPath: "github.com/go-chi/chi/v5"},
		{name: "a websocket library outside realtime", dir: "internal/httpapi", importPath: "github.com/gorilla/websocket"},
		{name: "golang.org/x", dir: "internal/admission", importPath: "golang.org/x/sync/errgroup"},
	}
}

func TestBoundaryRulesClassifyEveryCase(t *testing.T) {
	t.Parallel()

	for _, tc := range boundaryCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rule, forbidden := violatedRule(tc.dir, tc.importPath)
			got := ""
			if forbidden {
				got = rule.name
			}
			if got != tc.want {
				t.Errorf("violatedRule(%q, %q) rejected by %q, want %q", tc.dir, tc.importPath, got, tc.want)
			}
		})
	}
}

// TestBoundaryCaseTableCoversEveryRule is the anti-vacuity assertion for the
// table above, and it floors on the axis that cannot legitimately shrink: the
// set of RULES, not a count of cases. Every rule must be driven to a rejection,
// and every rule must be driven to a permitted answer, so a rule that has
// quietly stopped matching anything and a rule that rejects everything are both
// failures here rather than a green table.
//
// For a path-scoped rule the permitted answer must come from INSIDE its scope
// and a rejection must come from OUTSIDE it, which is what makes the exemption
// itself tested rather than merely declared.
func TestBoundaryCaseTableCoversEveryRule(t *testing.T) {
	t.Parallel()

	if len(boundaryRules) == 0 {
		t.Fatal("no boundary rules are declared, so every assertion in this file is vacuous")
	}

	type coverage struct{ rejected, permittedInScope, rejectedOutOfScope int }
	seen := map[string]*coverage{}
	for _, rule := range boundaryRules {
		seen[rule.name] = &coverage{}
	}

	for _, tc := range boundaryCases() {
		if tc.rule == "" {
			continue
		}
		cover, ok := seen[tc.rule]
		if !ok {
			t.Fatalf("case %q names rule %q, which is not declared", tc.name, tc.rule)
		}
		rule, found := ruleNamed(tc.rule)
		if !found {
			t.Fatalf("rule %q not found", tc.rule)
		}
		inScope := rule.scope != "" && pathHasPrefix(tc.dir, rule.scope)
		switch {
		case tc.want == tc.rule && !inScope:
			cover.rejected++
			cover.rejectedOutOfScope++
		case tc.want == tc.rule:
			cover.rejected++
		case tc.want == "" && inScope:
			cover.permittedInScope++
		}
	}

	for _, rule := range boundaryRules {
		cover := seen[rule.name]
		if cover.rejected == 0 {
			t.Errorf("rule %q is never driven to a rejection; it could match nothing and this file would still pass", rule.name)
		}
		if rule.scope == "" {
			continue
		}
		if cover.permittedInScope == 0 {
			t.Errorf("rule %q is scoped to %q but no case exercises a PERMITTED import inside that scope; the exemption is untested", rule.name, rule.scope)
		}
		if cover.rejectedOutOfScope == 0 {
			t.Errorf("rule %q is scoped to %q but no case exercises a REJECTED import outside that scope", rule.name, rule.scope)
		}
	}
}

// TestEveryDeclaredRuleIsNamedByTheTable closes the other half: a rule added to
// boundaryRules with no case in the table would be covered by nothing.
func TestEveryDeclaredRuleIsNamedByTheTable(t *testing.T) {
	t.Parallel()

	named := map[string]bool{}
	for _, tc := range boundaryCases() {
		if tc.rule != "" {
			named[tc.rule] = true
		}
	}
	for _, rule := range boundaryRules {
		if !named[rule.name] {
			t.Errorf("rule %q is declared but no case in boundaryCases names it", rule.name)
		}
	}
}

func ruleNamed(name string) (boundaryRule, bool) {
	for _, rule := range boundaryRules {
		if rule.name == name {
			return rule, true
		}
	}
	return boundaryRule{}, false
}

// ---------------------------------------------------------------------------
// The live scan.
// ---------------------------------------------------------------------------

func TestModuleImportsStayWithinBoundary(t *testing.T) {
	t.Parallel()

	scanned, violations, err := boundaryViolations(".")
	if err != nil {
		t.Fatalf("scan module imports: %v", err)
	}
	for _, violation := range violations {
		t.Error(violation)
	}
	// Anti-vacuity: fail loudly at zero files rather than passing on an empty
	// walk. A guard that walks nothing is indistinguishable from a clean tree.
	if scanned == 0 {
		t.Fatal("no Go files were scanned; the import boundary check would be vacuous")
	}
	t.Logf("scanned %d Go files", scanned)
}

// TestScanReachesEveryRuleScope is the "did the walk reach its subject"
// assertion for the three path-scoped rules, none of whose directories exist
// yet. It builds a fixture module containing a file at each scope and just
// outside it, then drives the LIVE enumerator and the LIVE scan over it. If a
// future directory layout were invisible to the walk — a nested go.mod, a
// dot-prefixed parent, testdata — the exemption would silently become
// unenforced everywhere, and this fails instead.
func TestScanReachesEveryRuleScope(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeGoFixture(t, root, "go.mod", "module github.com/looprig/factory\n")

	type probe struct {
		file       string
		importPath string
		wantRule   string
	}
	var probes []probe
	for _, rule := range boundaryRules {
		sample := sampleImportFor(t, rule)
		if rule.scope == "" {
			probes = append(probes,
				probe{file: "root_" + rule.name + ".go", importPath: sample, wantRule: rule.name},
				probe{file: path.Join("internal", "deep", "nested", rule.name+".go"), importPath: sample, wantRule: rule.name},
			)
			continue
		}
		probes = append(probes,
			probe{file: path.Join(rule.scope, "inscope_"+rule.name+".go"), importPath: sample, wantRule: ""},
			probe{file: path.Join(rule.scope, "sub", "inscope_"+rule.name+".go"), importPath: sample, wantRule: ""},
			probe{file: path.Join(path.Dir(rule.scope), "outofscope_"+rule.name+".go"), importPath: sample, wantRule: rule.name},
			probe{file: "root_" + rule.name + ".go", importPath: sample, wantRule: rule.name},
		)
	}
	if len(probes) == 0 {
		t.Fatal("no probes were built, so this test asserts nothing")
	}

	written := map[string]bool{}
	for _, p := range probes {
		if written[p.file] {
			t.Fatalf("two probes write the same fixture file %q", p.file)
		}
		written[p.file] = true
		writeGoFixture(t, root, p.file, "package fixture\n\nimport _ "+strconv.Quote(p.importPath)+"\n")
	}

	files, err := modfiles.Files(root)
	if err != nil {
		t.Fatalf("modfiles.Files: %v", err)
	}
	for _, p := range probes {
		want := filepath.Join(root, filepath.FromSlash(p.file))
		if !slices.Contains(files, want) {
			t.Errorf("the enumerator did not reach %s; a rule scoped there would be unenforced", p.file)
		}
	}

	scanned, violations, err := boundaryViolations(root)
	if err != nil {
		t.Fatalf("boundaryViolations: %v", err)
	}
	if scanned != len(probes) {
		t.Fatalf("scanned %d files, want %d", scanned, len(probes))
	}
	for _, p := range probes {
		hit := slices.ContainsFunc(violations, func(v string) bool {
			return strings.Contains(v, filepath.FromSlash(p.file))
		})
		if p.wantRule != "" && !hit {
			t.Errorf("%s importing %q was not reported; violations = %q", p.file, p.importPath, violations)
		}
		if p.wantRule == "" && hit {
			t.Errorf("%s importing %q was reported, but that location is inside the rule's scope; violations = %q", p.file, p.importPath, violations)
		}
	}
}

// sampleImportFor produces an import path the rule matches, so the probes above
// carry a payload that WOULD be caught if the scope exemption were closed. A
// probe whose payload no rule matches passes for the wrong reason.
// sampleImports is shared with the fuzz targets, so a rule added without a
// payload fails loudly in both places rather than being probed with an import
// no rule matches.
var sampleImports = map[string]string{
	"host":       "github.com/looprig/host",
	"harness":    "github.com/looprig/harness",
	"centrifuge": "github.com/centrifugal/centrifuge",
	"wui":        "github.com/looprig/wui",
	"kubernetes": "k8s.io/client-go/kubernetes",
}

func sampleImportFor(t *testing.T, rule boundaryRule) string {
	t.Helper()
	sample, ok := sampleImports[rule.name]
	if !ok {
		t.Fatalf("rule %q has no sample import; add one so its scope is probed with a payload the rule matches", rule.name)
	}
	if !rule.matches(sample) {
		t.Fatalf("rule %q does not match its own sample import %q", rule.name, sample)
	}
	return sample
}

// TestScanReportsTestFilesToo pins that the guard reads _test.go files. Rules 1
// and 2 are "everywhere", and a test that imported Host would defeat the point
// of the separation just as thoroughly as production code would.
func TestScanReportsTestFilesToo(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeGoFixture(t, root, "go.mod", "module github.com/looprig/factory\n")
	writeGoFixture(t, root, "guard_test.go", "package fixture\n\nimport _ \"github.com/looprig/host\"\n")

	scanned, violations, err := boundaryViolations(root)
	if err != nil {
		t.Fatalf("boundaryViolations: %v", err)
	}
	if scanned != 1 {
		t.Fatalf("scanned %d files, want 1", scanned)
	}
	if len(violations) != 1 {
		t.Fatalf("violations = %q, want exactly one", violations)
	}
}

// TestScanIsNonvacuousOnlyWhenItSeesFiles proves the zero-file floor above can
// actually report zero, so its "scanned == 0" branch is not dead.
func TestScanIsNonvacuousOnlyWhenItSeesFiles(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeGoFixture(t, root, "go.mod", "module github.com/looprig/factory\n")
	writeGoFixture(t, root, "testdata/ignored.go", "package ignored\n")

	scanned, violations, err := boundaryViolations(root)
	if err != nil {
		t.Fatalf("boundaryViolations: %v", err)
	}
	if scanned != 0 || len(violations) != 0 {
		t.Fatalf("scanned %d files with violations %q, want 0 and none", scanned, violations)
	}
}

// TestScanAnswerIsIndependentOfHowItsRootIsSpelled drives the scan at a
// relative root as well as an absolute one. The live call passes "." while the
// enumerator returns absolute paths, and a scope classifier that compares those
// two spellings directly answers "not in scope" for every file — which in this
// guard would silently DISABLE all three exemptions and report legal code.
func TestScanAnswerIsIndependentOfHowItsRootIsSpelled(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeGoFixture(t, root, "go.mod", "module github.com/looprig/factory\n")
	writeGoFixture(t, root, "internal/realtime/link.go", "package realtime\n\nimport _ \"github.com/centrifugal/centrifuge\"\n")
	writeGoFixture(t, root, "internal/httpapi/routes.go", "package httpapi\n\nimport _ \"github.com/centrifugal/centrifuge\"\n")

	absoluteScanned, absoluteViolations, err := boundaryViolations(root)
	if err != nil {
		t.Fatalf("boundaryViolations(absolute): %v", err)
	}
	if len(absoluteViolations) != 1 {
		t.Fatalf("violations = %q, want exactly one (the httpapi file)", absoluteViolations)
	}

	working, err := os.Getwd()
	if err != nil {
		t.Fatalf("working directory: %v", err)
	}
	relativeRoot, err := filepath.Rel(working, root)
	if err != nil {
		t.Fatalf("relative root: %v", err)
	}
	if filepath.IsAbs(relativeRoot) {
		t.Fatalf("relative root %q is absolute, so this case would only repeat the absolute one", relativeRoot)
	}
	relativeScanned, relativeViolations, err := boundaryViolations(relativeRoot)
	if err != nil {
		t.Fatalf("boundaryViolations(%q): %v", relativeRoot, err)
	}
	if relativeScanned != absoluteScanned || !slices.Equal(relativeViolations, absoluteViolations) {
		t.Fatalf("relative root reported %d files %q; absolute root reported %d files %q. The scan's answer must not depend on how its root is spelled",
			relativeScanned, relativeViolations, absoluteScanned, absoluteViolations)
	}
}

// TestBoundaryScopesAreNotStale keeps the three named exemptions bounded by
// something the change itself must touch: the directory. An exemption whose
// directory exists but holds no Go file is an unearned grant, and an exemption
// whose directory does not exist yet is the scaffold state this task ships.
func TestBoundaryScopesAreNotStale(t *testing.T) {
	t.Parallel()

	files, err := modfiles.Files(".")
	if err != nil {
		t.Fatalf("modfiles.Files: %v", err)
	}
	root, err := filepath.Abs(".")
	if err != nil {
		t.Fatalf("absolute root: %v", err)
	}
	for _, rule := range boundaryRules {
		if rule.scope == "" {
			continue
		}
		scopeDir := filepath.Join(root, filepath.FromSlash(rule.scope))
		info, err := os.Stat(scopeDir)
		if os.IsNotExist(err) {
			continue // Not built yet; the exemption grants nothing today.
		}
		if err != nil {
			t.Fatalf("stat scope %q: %v", rule.scope, err)
		}
		if !info.IsDir() {
			t.Errorf("rule %q is scoped to %q, which is not a directory", rule.name, rule.scope)
			continue
		}
		if !slices.ContainsFunc(files, func(p string) bool { return pathHasPrefix(filepath.ToSlash(p), filepath.ToSlash(scopeDir)) }) {
			t.Errorf("rule %q is scoped to %q, which exists but contains no Go file the enumerator can see", rule.name, rule.scope)
		}
	}
}

// ---------------------------------------------------------------------------
// pathHasPrefix: the one segment-aware comparison both halves of the guard use.
// ---------------------------------------------------------------------------

func TestPathHasPrefixComparesSegments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		p, prefix string
		want      bool
	}{
		{p: "internal/realtime", prefix: "internal/realtime", want: true},
		{p: "internal/realtime/clientlink", prefix: "internal/realtime", want: true},
		{p: "internal/realtimefanout", prefix: "internal/realtime", want: false},
		{p: "internal/realtim", prefix: "internal/realtime", want: false},
		{p: "internal", prefix: "internal/realtime", want: false},
		{p: "cmd/factoryctl", prefix: "cmd/factory", want: false},
		{p: "cmd/factory", prefix: "cmd/factory", want: true},
		{p: "cmd/factory/internal/ui", prefix: "cmd/factory", want: true},
		{p: ".", prefix: "internal/realtime", want: false},
		{p: "github.com/looprig/host", prefix: "github.com/looprig/host", want: true},
		{p: "github.com/looprig/host/link", prefix: "github.com/looprig/host", want: true},
		{p: "github.com/looprig/hostage", prefix: "github.com/looprig/host", want: false},
		{p: "x/github.com/looprig/host", prefix: "github.com/looprig/host", want: false},
		{p: "", prefix: "internal/realtime", want: false},
	}
	for _, tt := range tests {
		if got := pathHasPrefix(tt.p, tt.prefix); got != tt.want {
			t.Errorf("pathHasPrefix(%q, %q) = %v, want %v", tt.p, tt.prefix, got, tt.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Scan implementation.
// ---------------------------------------------------------------------------

func boundaryViolations(root string) (int, []string, error) {
	files, err := modfiles.Files(root)
	if err != nil {
		return 0, nil, err
	}
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return 0, nil, err
	}
	scanned := 0
	var violations []string
	for _, file := range files {
		relative, err := filepath.Rel(absoluteRoot, file)
		if err != nil {
			return 0, nil, err
		}
		dir := path.Dir(filepath.ToSlash(relative))
		scanned++
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.ImportsOnly)
		if err != nil {
			return 0, nil, err
		}
		for _, spec := range parsed.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return 0, nil, err
			}
			rule, forbidden := violatedRule(dir, importPath)
			if !forbidden {
				continue
			}
			where := "anywhere in this module"
			if rule.scope != "" {
				where = "outside " + rule.scope
			}
			violations = append(violations,
				file+" imports "+strconv.Quote(importPath)+", which rule "+strconv.Quote(rule.name)+" forbids "+where)
		}
	}
	return scanned, violations, nil
}

func writeGoFixture(t *testing.T, root, relative, content string) {
	t.Helper()
	target := filepath.Join(root, filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		t.Fatalf("create fixture directory: %v", err)
	}
	if err := os.WriteFile(target, []byte(content), 0o600); err != nil {
		t.Fatalf("write fixture %q: %v", relative, err)
	}
}

// ---------------------------------------------------------------------------
// The guard's SUBJECT is the module, and the module is defined by modfiles.
//
// modfiles decides visibility on five axes: which DIRECTORIES it refuses to
// descend into, which FILES it refuses to read, the marker set that makes a
// directory a separate module or repository, the .go suffix, and a fail-closed
// symlink check. Anything hidden by any of them is outside all five import
// rules -- and outside `make fmt-check`, which pipes the same enumerator.
//
// The first version of this section RESTATED two of those axes as its own
// literals and did not cover the other two. That is not a style problem, it is
// the defect this whole section exists to prevent, and it failed in both
// directions at once:
//
//   - Too narrow. Appending `|| name == "generated"` to modfiles' directory
//     rule -- one word, in an internal helper nobody reviews as "the product"
//     -- hid every file under any generated/ directory from all five rules AND
//     from gofmt, while this test stayed silent. Unlike _x.go or testdata/, a
//     directory named generated IS compiled by Go, so the hidden import was a
//     real dependency of a real build.
//   - Too wide. It descended into testdata/ and dot-prefixed directories that
//     modfiles never enters, so it reported .worktrees/feature-x and
//     internal/modfiles/testdata/fixturemod as boundaries. Both reports were
//     false, and the remedy the message prescribed was harmful: declaring a
//     testdata fixture module would have made this guard scan a fixture whose
//     whole purpose is to contain a forbidden import.
//
// So nothing here restates modfiles. The detector CONSULTS it -- for the
// reasons, for the markers, and for where to stop -- and what is pinned is not
// the implementation but the SANCTIONED ANSWER, by an independent restatement
// of which skips are sound. Sharing removes the disagreement; pinning stops the
// shared answer from quietly widening into a hole in both walks at once.
// ---------------------------------------------------------------------------

// allowedNestedBoundaries maps a module-relative directory to the reason it is
// allowed to be a nested module or repository. An entry is not a hole: every
// allowed boundary is SCANNED IN ITS OWN RIGHT by the assertion below, so
// admitting one buys an extra scan rather than an exemption.
var allowedNestedBoundaries = map[string]string{
	"examples/deploy/wiring": "standalone deployment wiring example module",
}

// The kinds of invisibility, which are the reasons the module-owned walk
// declines to show a path to the import rules.
const (
	kindNestedBoundary   = "nested boundary"
	kindIgnoredDirectory = "ignored directory"
	kindIgnoredFile      = "ignored file"
)

// invisibility is a path the module-owned walk will not show to the import
// rules, together with the reason modfiles gave for that.
type invisibility struct {
	path   string // relative to the module root, slash-separated
	kind   string
	reason string // a boundary marker filename, or one of modfiles' reasons
}

// String names the path and what hid it. The vendor case is worded separately
// because the directory IS the reason, and "vendor is hidden by vendor" reads
// as a mistake rather than as a report.
func (i invisibility) String() string {
	if i.reason == modfiles.ReasonVendor {
		return i.path + " is a vendored dependency tree"
	}
	return i.path + " is hidden by the " + i.kind + " rule " + strconv.Quote(i.reason)
}

// sanctionedIgnoredDirectory and sanctionedIgnoredFile are the INDEPENDENT
// restatement of which skips are sound. They are written from the
// justification, not from modfiles' code, and they are deliberately spelled
// differently (strings.HasPrefix rather than an index) so that a copy-paste
// between the two cannot pass for agreement.
//
// The justification, which is the whole content of the sanction:
//
//   - vendor: no repository in this workspace vendors, and Go ignores vendor/
//     when a workspace is active, so a vendored tree is not this module's
//     content. It is still reported here, because a CONSISTENT vendor tree
//     builds fine and would otherwise be invisible.
//   - testdata: Go's own convention, never part of any build.
//   - dot- and underscore-prefixed: the names the Go tool itself ignores, so a
//     Go file behind one is not part of any build either.
//
// Nothing else is sound, and in particular nothing that Go WOULD compile.
func sanctionedIgnoredDirectory(name string) (string, bool) {
	switch {
	case name == "vendor":
		return modfiles.ReasonVendor, true
	case name == "testdata":
		return modfiles.ReasonTestdata, true
	case strings.HasPrefix(name, "."):
		return modfiles.ReasonDotPrefixed, true
	case strings.HasPrefix(name, "_"):
		return modfiles.ReasonUnderscorePrefixed, true
	}
	return "", false
}

func sanctionedIgnoredFile(name string) (string, bool) {
	switch {
	case strings.HasPrefix(name, "."):
		return modfiles.ReasonDotPrefixed, true
	case strings.HasPrefix(name, "_"):
		return modfiles.ReasonUnderscorePrefixed, true
	}
	return "", false
}

// sanctionedBoundaryMarkers is the pinned marker set, restated here so that
// adding a sixth marker to modfiles -- go.work, say -- cannot pass unnoticed.
var sanctionedBoundaryMarkers = []string{".git", "go.mod"}

func TestModfilesDecisionSurfaceMatchesTheSanctionedSet(t *testing.T) {
	t.Parallel()

	// Every name that a plausible future edit would add, and every name from
	// the runbook's own layout that must never be swept up. The fuzz target
	// below generalises this; the table is here so the specific hazards are
	// deterministic rather than left to a mutator finding a string literal.
	names := []string{
		"vendor", "testdata", ".git", ".github", ".worktrees", "_scratch", "",
		"generated", "gen", "build", "dist", "out", "node_modules", "third_party",
		"bazel-out", "zz_generated.go", "zz_generated", "mock", "mocks", "fixtures",
		"internal", "cmd", "factory", "httpapi", "realtime", "clientlink", "hostlink",
		"delivery", "placement", "kubernetes", "routing", "admission", "identity",
		"testkit", "modfiles", "contract.go", "routes.go", "vendored", "testdata2",
		"_", ".", "..", "a", "Z",
	}
	if len(names) == 0 {
		t.Fatal("no names are exercised, so this test is vacuous")
	}
	for _, name := range names {
		wantReason, wantIgnored := sanctionedIgnoredDirectory(name)
		gotReason, gotIgnored := modfiles.IgnoredDirectory(name)
		if gotIgnored != wantIgnored || gotReason != wantReason {
			t.Errorf("modfiles.IgnoredDirectory(%q) = (%q, %v), sanctioned answer is (%q, %v). A widened rule hides a subtree from every import rule AND from gofmt",
				name, gotReason, gotIgnored, wantReason, wantIgnored)
		}
		wantReason, wantIgnored = sanctionedIgnoredFile(name)
		gotReason, gotIgnored = modfiles.IgnoredFile(name)
		if gotIgnored != wantIgnored || gotReason != wantReason {
			t.Errorf("modfiles.IgnoredFile(%q) = (%q, %v), sanctioned answer is (%q, %v)",
				name, gotReason, gotIgnored, wantReason, wantIgnored)
		}
	}
}

func TestBoundaryMarkerSetIsPinnedAndUnmodifiable(t *testing.T) {
	t.Parallel()

	markers := modfiles.BoundaryMarkers()
	if !slices.Equal(markers, sanctionedBoundaryMarkers) {
		t.Fatalf("modfiles.BoundaryMarkers() = %q, sanctioned set is %q. A marker added there stops the walk at directories this guard would then never report",
			markers, sanctionedBoundaryMarkers)
	}
	// The accessor must hand out a copy, or a caller pinning the set could
	// change the set it is pinning.
	markers[0] = "mutated"
	if again := modfiles.BoundaryMarkers(); slices.Equal(again, markers) {
		t.Fatal("modfiles.BoundaryMarkers() returns the package's own slice; a caller can rewrite the marker set")
	}
}

func TestModuleFileSetMatchesAnIndependentEnumeration(t *testing.T) {
	t.Parallel()

	for _, root := range []string{".", fileSetFixture(t)} {
		got, err := modfiles.Files(root)
		if err != nil {
			t.Fatalf("modfiles.Files(%q): %v", root, err)
		}
		want, err := independentModuleFiles(root)
		if err != nil {
			t.Fatalf("independentModuleFiles(%q): %v", root, err)
		}
		if len(want) == 0 {
			t.Fatalf("the independent enumeration of %q found no files, so this comparison is vacuous", root)
		}
		if !slices.Equal(got, want) {
			t.Errorf("for root %q modfiles.Files returned\n  %q\nan independent enumeration expected\n  %q", root, got, want)
		}
	}
}

// fileSetFixture builds a tree exercising every axis at once, so the comparison
// above is driven against something richer than the real module -- which today
// contains no ignored file, no boundary and no vendor tree.
func fileSetFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for path, content := range map[string]string{
		"go.mod":                        "module github.com/looprig/factory\n",
		"visible.go":                    "package fixture\n",
		"visible_test.go":               "package fixture\n",
		"tagged.go":                     "//go:build sometag\n\npackage fixture\n",
		"internal/deep/visible.go":      "package deep\n",
		"internal/_hidden.go":           "package fixture\n",
		"internal/.hidden.go":           "package fixture\n",
		"internal/notgo.txt":            "text\n",
		"testdata/hidden.go":            "package hidden\n",
		"_ignored/hidden.go":            "package hidden\n",
		".ignored/hidden.go":            "package hidden\n",
		"vendor/x/hidden.go":            "package hidden\n",
		"nestedmod/go.mod":              "module example.com/nested\n",
		"nestedmod/hidden.go":           "package hidden\n",
		"nestedrepo/.git":               "gitdir: elsewhere\n",
		"nestedrepo/hidden.go":          "package hidden\n",
		".worktrees/w/.git":             "gitdir: elsewhere\n",
		".worktrees/w/hidden.go":        "package hidden\n",
		"testdata/fixturemod/go.mod":    "module example.com/fixture\n",
		"testdata/fixturemod/hidden.go": "package hidden\n",
	} {
		writeGoFixture(t, root, path, content)
	}
	return root
}

// independentModuleFiles re-derives the module's Go files from the SANCTIONED
// rules rather than from modfiles, and is the only place the .go suffix axis is
// asserted. Narrowing that suffix test -- excluding _test.go files, say --
// would hide every test from the import rules, and nothing else here would say
// so.
func independentModuleFiles(root string) ([]string, error) {
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	var files []string
	err = filepath.WalkDir(absoluteRoot, func(p string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if p == absoluteRoot {
				return nil
			}
			if _, ignored := sanctionedIgnoredDirectory(entry.Name()); ignored {
				return filepath.SkipDir
			}
			boundary, err := hasBoundaryMarker(p)
			if err != nil {
				return err
			}
			if boundary {
				return filepath.SkipDir
			}
			return nil
		}
		if _, ignored := sanctionedIgnoredFile(entry.Name()); ignored {
			return nil
		}
		if !strings.HasSuffix(entry.Name(), ".go") {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			files = append(files, p)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.Sort(files)
	return files, nil
}

func hasBoundaryMarker(dir string) (bool, error) {
	for _, marker := range sanctionedBoundaryMarkers {
		if _, err := os.Lstat(filepath.Join(dir, marker)); err == nil {
			return true, nil
		} else if !os.IsNotExist(err) {
			return false, err
		}
	}
	return false, nil
}

func TestModuleHasNoUndeclaredNestedBoundary(t *testing.T) {
	t.Parallel()

	found, visited, err := moduleInvisibilities(".")
	if err != nil {
		t.Fatalf("find invisibilities: %v", err)
	}
	// Anti-vacuity on the axis that cannot legitimately shrink: the walk must
	// have descended somewhere. A detector that visits nothing finds nothing.
	if visited == 0 {
		t.Fatal("the invisibility walk visited no directories, so this check is vacuous")
	}
	t.Logf("visited %d directories", visited)

	for _, hidden := range found {
		if hidden.kind != kindNestedBoundary {
			continue
		}
		reason, allowed := allowedNestedBoundaries[hidden.path]
		if !allowed {
			t.Errorf("%s, which stops the module-owned walk: every import rule silently stops applying to that whole subtree. Declare it in allowedNestedBoundaries (which then scans it separately) or remove it",
				hidden)
			continue
		}
		if strings.TrimSpace(reason) == "" {
			t.Errorf("%s is allowed with no reason given", hidden)
		}
		// The allowlist is not an exemption. Scan the nested module too.
		scanned, violations, err := boundaryViolations(filepath.FromSlash(hidden.path))
		if err != nil {
			t.Errorf("scan allowed nested boundary %s: %v", hidden.path, err)
			continue
		}
		if scanned == 0 {
			t.Errorf("allowed nested boundary %s scanned no Go files", hidden.path)
		}
		for _, violation := range violations {
			t.Error(violation)
		}
	}
}

// TestEveryHiddenPathIsSanctioned is the other half, and the one that would
// have caught the `generated` widening. Every directory the walk refuses to
// enter and every Go file it refuses to read must be hidden for a reason the
// independent restatement above also gives, with the same reason string.
func TestEveryHiddenPathIsSanctioned(t *testing.T) {
	t.Parallel()

	found, _, err := moduleInvisibilities(".")
	if err != nil {
		t.Fatalf("find invisibilities: %v", err)
	}
	hiddenDirectories := 0
	for _, hidden := range found {
		var want string
		var sanctioned bool
		switch hidden.kind {
		case kindIgnoredDirectory:
			hiddenDirectories++
			want, sanctioned = sanctionedIgnoredDirectory(path.Base(hidden.path))
		case kindIgnoredFile:
			want, sanctioned = sanctionedIgnoredFile(path.Base(hidden.path))
		default:
			continue
		}
		if !sanctioned {
			t.Errorf("%s, but no sanctioned rule hides a path of that name. It is therefore outside all five import rules and outside gofmt, and Go may still compile it",
				hidden)
			continue
		}
		if hidden.reason != want {
			t.Errorf("%s, but the sanctioned reason for that name is %q", hidden, want)
		}
	}
	// The real tree always has at least .git and .github, so a zero here means
	// the walk stopped reporting rather than that nothing is hidden.
	if hiddenDirectories == 0 {
		t.Fatal("no hidden directory was reported at all; this module always has dot-prefixed ones, so the detector is not looking")
	}
}

// TestInvisibilityDetectorFindsEveryAxis is the positive control for the whole
// section, and the fixture is rooted at a directory with a CHOSEN NAME.
//
// That last detail is not decoration. The root exclusion here is structural --
// the root returns before any marker is tested -- but a regression to comparing
// base NAMES could not be caught by a fixture rooted at t.TempDir(), whose
// basename is random and can never collide with a subdirectory's. Rooting at a
// named directory and putting a same-named directory inside it closes that.
func TestInvisibilityDetectorFindsEveryAxis(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	const rootName = "factory"
	root := filepath.Join(base, rootName)
	writeGoFixture(t, root, "go.mod", "module github.com/looprig/factory\n")
	writeGoFixture(t, root, "root.go", "package fixture\n")
	writeGoFixture(t, root, ".git/HEAD", "ref: refs/heads/main\n")
	writeGoFixture(t, root, "internal/httpapi/go.mod", "module example.com/nested\n")
	writeGoFixture(t, root, "internal/httpapi/routes.go", "package httpapi\n\nimport _ \"github.com/looprig/host\"\n")
	writeGoFixture(t, root, "internal/deep/tree/tool/.git/HEAD", "ref: refs/heads/main\n")
	// A .git FILE, which is what `git submodule add` and `git worktree add`
	// write. modfiles stops at it exactly as it stops at a .git directory, so a
	// detector recognising only the directory form would disagree with the
	// enumerator it polices.
	writeGoFixture(t, root, "internal/sub/.git", "gitdir: ../../.git/modules/sub\n")
	writeGoFixture(t, root, "internal/sub/module.go", "package sub\n")
	// Both a nested module and a nested repository: two invisibilities, and
	// removing one does not restore the walk.
	writeGoFixture(t, root, "internal/both/go.mod", "module example.com/both\n")
	writeGoFixture(t, root, "internal/both/.git", "gitdir: ../../.git/modules/both\n")
	// A directory sharing the ROOT's name. A base-name root test would exclude
	// this one too and report nothing here.
	writeGoFixture(t, root, "internal/"+rootName+"/.git", "gitdir: elsewhere\n")
	// Sanctioned skips. None of these is a boundary, and the walk must STOP at
	// them rather than descend and report what is inside: a worktree's .git and
	// a testdata fixture module are exactly the two false reports this
	// detector produced before it consulted modfiles.
	writeGoFixture(t, root, "vendor/github.com/x/y/z.go", "package z\n")
	writeGoFixture(t, root, ".worktrees/feature-x/.git", "gitdir: elsewhere\n")
	writeGoFixture(t, root, "testdata/fixturemod/go.mod", "module example.com/fixture\n")
	writeGoFixture(t, root, "_scratch/thing/go.mod", "module example.com/scratch\n")
	writeGoFixture(t, root, "internal/_hidden.go", "package fixture\n")
	writeGoFixture(t, root, "internal/.hidden.go", "package fixture\n")

	found, visited, err := moduleInvisibilities(root)
	if err != nil {
		t.Fatalf("moduleInvisibilities: %v", err)
	}
	if visited == 0 {
		t.Fatal("the fixture walk visited no directories")
	}
	var got []string
	for _, hidden := range found {
		got = append(got, hidden.path+" -> "+hidden.kind+" "+hidden.reason)
	}
	want := []string{
		".git -> ignored directory " + modfiles.ReasonDotPrefixed,
		".worktrees -> ignored directory " + modfiles.ReasonDotPrefixed,
		"_scratch -> ignored directory " + modfiles.ReasonUnderscorePrefixed,
		"internal/.hidden.go -> ignored file " + modfiles.ReasonDotPrefixed,
		"internal/_hidden.go -> ignored file " + modfiles.ReasonUnderscorePrefixed,
		"internal/both -> nested boundary .git",
		"internal/both -> nested boundary go.mod",
		"internal/deep/tree/tool -> nested boundary .git",
		// The .git DIRECTORY is itself dot-prefixed, so it is reported under
		// that rule as well as making its parent a boundary. Both records are
		// true and both are sanctioned; the root's own .git appears for the
		// same reason. A .git FILE has no second record because only Go files
		// are reported at the file level.
		"internal/deep/tree/tool/.git -> ignored directory " + modfiles.ReasonDotPrefixed,
		"internal/" + rootName + " -> nested boundary .git",
		"internal/httpapi -> nested boundary go.mod",
		"internal/sub -> nested boundary .git",
		"testdata -> ignored directory " + modfiles.ReasonTestdata,
		"vendor -> ignored directory " + modfiles.ReasonVendor,
	}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("invisibilities =\n  %q\nwant\n  %q", got, want)
	}

	// And the premise: those subtrees really are invisible to the scan, which
	// is the whole reason the detector exists.
	scanned, violations, err := boundaryViolations(root)
	if err != nil {
		t.Fatalf("boundaryViolations: %v", err)
	}
	if scanned != 1 {
		t.Fatalf("scanned %d files, want 1: only root.go. internal/sub/module.go is behind a .git FILE, which is exactly the invisibility this fixture exists to prove", scanned)
	}
	if len(violations) != 0 {
		t.Fatalf("violations = %q; the host import inside the nested module is not this module's to report, which is exactly the invisibility being detected", violations)
	}
}

// TestAllowedNestedBoundaryIsStillScanned pins that an allowlist entry buys an
// extra scan rather than a hole: a declared nested module whose own code breaks
// the boundary is still reported.
func TestAllowedNestedBoundaryIsStillScanned(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeGoFixture(t, root, "go.mod", "module example.com/nested\n")
	writeGoFixture(t, root, "tool.go", "package tool\n\nimport _ \"github.com/looprig/harness\"\n")

	scanned, violations, err := boundaryViolations(root)
	if err != nil {
		t.Fatalf("boundaryViolations: %v", err)
	}
	if scanned != 1 {
		t.Fatalf("scanned %d files, want 1", scanned)
	}
	if len(violations) != 1 {
		t.Fatalf("violations = %q, want exactly one", violations)
	}
}

// moduleInvisibilities reports every path below root that the module-owned walk
// will not show to the import rules, together with the number of directories it
// visited.
//
// It CONSULTS modfiles for every decision, which is what makes it impossible
// for the two to disagree: it stops descending exactly where modfiles stops, so
// it cannot invent a boundary inside a subtree modfiles never enters, and it
// learns a new marker or a new ignore rule automatically rather than having to
// be told. What it does NOT take from modfiles is whether an answer is
// acceptable; that is the sanctioned restatement above.
//
// It descends INTO the boundaries it finds, deliberately: a nested module
// inside a nested module is two separate invisibilities, and reporting only the
// outermost would hide the inner one behind the fix for the outer.
func moduleInvisibilities(root string) ([]invisibility, int, error) {
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, 0, err
	}
	var found []invisibility
	visited := 0
	err = filepath.WalkDir(absoluteRoot, func(p string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(absoluteRoot, p)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if !entry.IsDir() {
			if !strings.HasSuffix(entry.Name(), ".go") {
				return nil
			}
			if reason, ignored := modfiles.IgnoredFile(entry.Name()); ignored {
				found = append(found, invisibility{path: relative, kind: kindIgnoredFile, reason: reason})
			}
			return nil
		}
		visited++
		// modfiles applies neither rule to the root itself, so neither does
		// this. The root's own .git is consequently never a finding: the
		// exclusion is the shape of the walk, not a name comparison.
		if p == absoluteRoot {
			return nil
		}
		if reason, ignored := modfiles.IgnoredDirectory(entry.Name()); ignored {
			found = append(found, invisibility{path: relative, kind: kindIgnoredDirectory, reason: reason})
			// Stop exactly where modfiles stops. Descending further would
			// report boundaries inside a subtree modfiles never enters.
			return filepath.SkipDir
		}
		// Every marker is reported, not just the first: a directory that is
		// both a nested module and a nested repository is two invisibilities.
		for _, marker := range modfiles.BoundaryMarkers() {
			if _, err := os.Lstat(filepath.Join(p, marker)); err == nil {
				found = append(found, invisibility{path: relative, kind: kindNestedBoundary, reason: marker})
			} else if !os.IsNotExist(err) {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	slices.SortFunc(found, func(a, b invisibility) int {
		if by := strings.Compare(a.path, b.path); by != 0 {
			return by
		}
		return strings.Compare(a.reason, b.reason)
	})
	return found, visited, nil
}
