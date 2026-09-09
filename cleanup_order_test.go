package factory_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"

	"github.com/looprig/factory/internal/modfiles"
)

// A shutdown that is bounded must run BEFORE an unbounded Close, and this is the
// census of every place in the module that does both.
//
// # Why the reader is syntactic
//
// The failure it prevents is a HANG. A test asserting "the package did not time
// out" cannot fail -- it takes the whole run down with it, taking ten minutes to
// say so -- and I argued from that premise that the ordering could not have a
// reader at all. The premise is right and the conclusion was wrong: no DYNAMIC
// reader can fail on a hang, but a syntactic one can, and the tool for it was
// already in this module.
//
// The cost of not writing it was measured rather than hypothesised. The ordering
// was corrected at the site a gate named, then at its twin one round later, with
// a comment claiming "the two now say so in each other's terms" -- and this
// census, on its first run, found SIX pairs with four of them still wrong. The
// three a gate named, and a fourth in internal/realtime/transport that no gate
// reached and no list contained. A census of two over a population of six is
// the class of defect it apologises for.
//
// # The census
//
// Every function body in every file the module enumerates -- production and
// test, since these live in fixtures -- that contains BOTH a call to a method
// named Shutdown and a call to a method named Close. The pair is what makes a
// site a site: a lone Close is a file handle, a response body, a store; a lone
// Shutdown has nothing to race. Where both appear, the first Shutdown must
// precede the first Close by source position.
//
// It is deliberately name-based and type-free, which over-reports rather than
// under-reports: an unrelated Close beside an unrelated Shutdown in one body is
// a finding to be argued with, not a silent pass. The exemption list carries
// reasons and is checked for staleness in both directions.
func TestEveryBoundedShutdownPrecedesAnUnboundedClose(t *testing.T) {
	t.Parallel()

	root, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	sites, err := shutdownClosePairs(root)
	if err != nil {
		t.Fatalf("census: %v", err)
	}
	// Anti-vacuity. A scan that found nothing would pass on a clean tree and on
	// a broken detector alike.
	//
	// The floor is SIX, which is what the census measured -- not the five the
	// round was told to expect. The three sites a gate named plus the two
	// already corrected is five; the sixth is in internal/realtime/transport,
	// which no gate reached. That is the argument for a census rather than a
	// list, made by the census on its first run.
	//
	// It is A COUNT, and saying so is the point rather than an apology. There is
	// no authority in this module for "how many websocket fixtures there ought
	// to be", so this cannot be derived the way the admission sweep's axes are.
	// What a count can see: the scanner breaking, or the fixtures being deleted.
	// What it cannot: one site added and another removed in the same change, or
	// a seventh site appearing -- which is why the ORDERING check above is over
	// every site found rather than over a listed six, and why this floor is the
	// weaker of the two readers in this file.
	if len(sites) < 6 {
		t.Fatalf("the census found %d shutdown/close pairs, want at least the six this module had when it was written; "+
			"a scan that stopped finding them is indistinguishable from a tree that stopped having them: %v", len(sites), sites)
	}
	for _, site := range sites {
		if reason := sanctionedCleanupOrder()[site.where]; reason != "" {
			continue
		}
		if site.closeLine < site.shutdownLine {
			t.Errorf("%s closes at line %d before shutting down at line %d: an unbounded Close waits for the "+
				"outstanding request a wedged read loop is still holding, so the package hangs instead of failing",
				site.where, site.closeLine, site.shutdownLine)
		}
	}
	// The exemptions are claims. One naming a site the census no longer finds
	// has outlived its subject.
	found := map[string]bool{}
	for _, site := range sites {
		found[site.where] = true
	}
	for where, reason := range sanctionedCleanupOrder() {
		if !found[where] {
			t.Errorf("%s is sanctioned (%q) and the census no longer finds it; the record has outlived its site", where, reason)
		}
	}
}

// sanctionedCleanupOrder names sites where Close before Shutdown is correct,
// with the reason. It is empty: every site in this module today is a websocket
// fixture whose Close waits on the read loop its Shutdown releases.
func sanctionedCleanupOrder() map[string]string { return map[string]string{} }

// cleanupSite is one function body that both shuts down and closes.
type cleanupSite struct {
	// where is file:function, stable enough to name in an exemption.
	where                   string
	shutdownLine, closeLine int
}

func (c cleanupSite) String() string { return c.where }

// shutdownClosePairs is the census. It reports the FIRST Shutdown and the FIRST
// Close in each body that has both.
func shutdownClosePairs(root string) ([]cleanupSite, error) {
	files, err := modfiles.Files(root)
	if err != nil {
		return nil, err
	}
	var sites []cleanupSite
	fset := token.NewFileSet()
	for _, path := range files {
		file, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			return nil, parseErr
		}
		relative, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return nil, relErr
		}
		relative = filepath.ToSlash(relative)
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			// Function literals are examined in their own right, because the
			// site that matters is a t.Cleanup(func(){...}) body rather than
			// the test that registers it.
			bodies := map[string]*ast.BlockStmt{fn.Name.Name: fn.Body}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				lit, isLit := n.(*ast.FuncLit)
				if !isLit {
					return true
				}
				bodies[fn.Name.Name+"@"+strconv.Itoa(fset.Position(lit.Pos()).Line)] = lit.Body
				return true
			})
			for name, body := range bodies {
				shutdown, closed := firstCallLine(fset, body, "Shutdown"), firstCallLine(fset, body, "Close")
				if shutdown == 0 || closed == 0 {
					continue
				}
				sites = append(sites, cleanupSite{
					where: relative + ":" + name, shutdownLine: shutdown, closeLine: closed,
				})
			}
		}
	}
	sort.Slice(sites, func(i, j int) bool { return sites[i].where < sites[j].where })
	return sites, nil
}

// firstCallLine reports the line of the first call to a method of the given
// name in body, or zero. Nested function literals are skipped: their bodies are
// censused separately, and folding them in would compare an inner Close against
// an outer Shutdown.
func firstCallLine(fset *token.FileSet, body *ast.BlockStmt, method string) int {
	line := 0
	ast.Inspect(body, func(n ast.Node) bool {
		if _, isLit := n.(*ast.FuncLit); isLit && n != ast.Node(body) {
			return false
		}
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != method {
			return true
		}
		if at := fset.Position(call.Pos()).Line; line == 0 || at < line {
			line = at
		}
		return true
	})
	return line
}
