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

// A bounded Shutdown must run BEFORE an unbounded Close, and this is the census
// of every place in the module that does both.
//
// # Why the reader is syntactic
//
// The failure it prevents is a HANG. A test asserting "the package did not time
// out" cannot fail -- it takes the whole run down with it, taking ten minutes to
// say so -- and I argued from that premise that the ordering could not have a
// reader at all. The premise is right and the conclusion was wrong: no DYNAMIC
// reader can fail on a hang, but a syntactic one can. This is also the class of
// defect `-race` cannot see, which is worth saying in a module that runs the
// detector over every probe: the detector finds unsynchronised access, and a
// wedged read loop is perfectly synchronised.
//
// # The unit of analysis, and therefore what it cannot see
//
// A site is a FUNCTION BODY -- a declaration's or a literal's, wherever the
// literal appears, including one assigned to a package-level var, and each in
// its own right rather than folded into its parent. Two rules run over them:
//
//	within one body     the first Shutdown call must precede the first Close
//	across cleanups     a t.Cleanup registering a Shutdown must be registered
//	                    AFTER one registering a Close, because cleanups run LIFO
//
// The second rule exists because the first cannot see the shape at
// guard_composition_test.go: `t.Cleanup(server.Close)` passes a METHOD VALUE,
// not a call, and its Shutdown is in a different body entirely. That site is
// correct today -- Close is registered first, so it runs last -- and inverting
// the two registrations produces exactly the hazard with rule one silent. A
// registration is classified by whether its argument MENTIONS Shutdown or Close
// in any position, call or value, which is what makes a method value visible.
//
// What it still cannot see, stated rather than left to be found:
//
//   - a cleanup registered through a helper, so that the registration and the
//     Close are in different functions;
//   - a Close reached through an interface whose method is not named Close;
//   - ordering established at run time rather than by source position.
//
// It is name-based and type-free, so it over-reports rather than under-reports:
// an unrelated Close beside an unrelated Shutdown is a finding to argue with,
// not a silent pass. The sanction list carries reasons and is checked for
// staleness in both directions.
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
		if !site.ordered() {
			t.Errorf("%s closes (line %d) before shutting down (line %d): an unbounded Close waits for the "+
				"outstanding request a wedged read loop is still holding, so the package hangs instead of failing. "+
				"registrations=%t (cleanups run last-registered-first)",
				site.where, site.closeLine, site.shutdownLine, site.viaCleanupRegistration)
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
func sanctionedCleanupOrder() map[string]string {
	const clientClose = "this closes the CLIENT, not the listener. Running it first is what releases " +
		"the connection the node then shuts down, and it is not the unbounded wait the rule exists for: " +
		"the hazard is a listener Close waiting on a read loop a bounded Shutdown would have freed"
	return map[string]string{
		"internal/realtime/clientlink/node_test.go:TestTheConnectionLabelIsTheSubjectNotTheTenant (cleanup: client.Close after Shutdown)": clientClose,
		"internal/realtime/clientlink/node_test.go:stalledConsumer (cleanup: client.Close after Shutdown)":                                clientClose,
	}
}

// cleanupSite is one finding: a body that does both, or a pair of cleanup
// registrations in LIFO order that puts the unbounded Close first.
type cleanupSite struct {
	// where is file:function, stable enough to name in an exemption.
	where                   string
	shutdownLine, closeLine int
	// viaCleanupRegistration says this site is the ACROSS-CLEANUPS rule's,
	// where the lines are registration positions and the required order is
	// reversed by LIFO.
	viaCleanupRegistration bool
}

func (c cleanupSite) String() string { return c.where }

// ordered reports whether this site's Shutdown runs before its Close.
//
// Within one body that is source order. Across two cleanup registrations it is
// the REVERSE: t.Cleanup runs its functions last-registered-first, so the
// Shutdown must be registered after the Close to run before it.
func (c cleanupSite) ordered() bool {
	if c.viaCleanupRegistration {
		return c.shutdownLine > c.closeLine
	}
	return c.shutdownLine < c.closeLine
}

// shutdownClosePairs is the census.
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
		sites = append(sites, censusOfFile(fset, file, filepath.ToSlash(relative))...)
	}
	sort.Slice(sites, func(i, j int) bool {
		if sites[i].where != sites[j].where {
			return sites[i].where < sites[j].where
		}
		return sites[i].shutdownLine < sites[j].shutdownLine
	})
	return sites, nil
}

// censusOfFile applies both rules to one parsed file.
func censusOfFile(fset *token.FileSet, file *ast.File, relative string) []cleanupSite {
	var sites []cleanupSite

	// Rule one, over every body in the file. Literals are collected wherever
	// they appear -- including a package-level `var f = func(){...}`, which the
	// previous version could not see because it descended only into
	// FuncDecls, contradicting its own over-reporting claim.
	type body struct {
		name string
		node *ast.BlockStmt
	}
	var bodies []body
	// The enclosing declaration is found by POSITION rather than by remembering
	// the last one walked. A package-level `var f = func(){...}` appearing after
	// a declaration was labelled with that declaration's name -- which a probe
	// exposed as "Close@8" for a literal inside no function at all. A wrong
	// label in a failure message is the same class of defect as a wrong line
	// number in a comment, so it is derived rather than tracked.
	declarations := map[*ast.FuncDecl]bool{}
	ast.Inspect(file, func(n ast.Node) bool {
		if decl, ok := n.(*ast.FuncDecl); ok && decl.Body != nil {
			declarations[decl] = true
		}
		return true
	})
	enclosingOf := func(pos token.Pos) string {
		for decl := range declarations {
			if decl.Pos() <= pos && pos <= decl.End() {
				return decl.Name.Name
			}
		}
		return "file"
	}
	ast.Inspect(file, func(n ast.Node) bool {
		switch decl := n.(type) {
		case *ast.FuncDecl:
			if decl.Body == nil {
				return true
			}
			bodies = append(bodies, body{name: decl.Name.Name, node: decl.Body})
		case *ast.FuncLit:
			bodies = append(bodies, body{
				name: enclosingOf(decl.Pos()) + "@" + strconv.Itoa(fset.Position(decl.Pos()).Line),
				node: decl.Body,
			})
		}
		return true
	})
	for _, b := range bodies {
		shutdown, closed := firstCallLine(fset, b.node, "Shutdown"), firstCallLine(fset, b.node, "Close")
		if shutdown == 0 || closed == 0 {
			continue
		}
		sites = append(sites, cleanupSite{where: relative + ":" + b.name, shutdownLine: shutdown, closeLine: closed})
	}

	// Rule two, over cleanup REGISTRATIONS within one body.
	for _, b := range bodies {
		var shutdownAt []int
		var closeAt []registration
		ast.Inspect(b.node, func(n ast.Node) bool {
			if lit, isLit := n.(*ast.FuncLit); isLit && ast.Node(lit.Body) != ast.Node(b.node) {
				// A nested literal's OWN registrations belong to it, but the
				// registration call itself is this body's. Descend, since the
				// t.Cleanup(...) call is what is being located.
				return true
			}
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "Cleanup" || len(call.Args) != 1 {
				return true
			}
			line := fset.Position(call.Pos()).Line
			if _, ok := mentioned(call.Args[0], "Shutdown"); ok {
				shutdownAt = append(shutdownAt, line)
			}
			if receiver, ok := mentioned(call.Args[0], "Close"); ok {
				closeAt = append(closeAt, registration{line: line, subject: receiver})
			}
			return true
		})
		for _, s := range shutdownAt {
			for _, c := range closeAt {
				if s == c.line {
					// One registration doing both is rule one's business.
					continue
				}
				// The SUBJECT of the Close is in the key, so sanctioning a
				// client's Close in some function cannot also hide a listener's
				// Close appearing there later. A sanction that named only the
				// function would be a hole the width of the function.
				sites = append(sites, cleanupSite{
					where:        relative + ":" + b.name + " (cleanup: " + c.subject + ".Close after Shutdown)",
					shutdownLine: s, closeLine: c.line, viaCleanupRegistration: true,
				})
			}
		}
	}
	return sites
}

// registration is one t.Cleanup call that closes something.
type registration struct {
	line int
	// subject is the identifier the closed thing is spelled with, or "?" when
	// the expression is not a plain identifier. It is a LABEL for the sanction
	// key, never a decision: this scan has no types and cannot know what a name
	// refers to.
	subject string
}

// mentioned reports whether an expression names a method anywhere within it, in
// ANY position -- `x.Close()` and the method value `x.Close` alike, the second
// being the shape rule one is blind to -- and the identifier it is called on.
func mentioned(expr ast.Expr, method string) (subject string, found bool) {
	subject = "?"
	ast.Inspect(expr, func(n ast.Node) bool {
		selector, ok := n.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != method {
			return !found
		}
		found = true
		if ident, isIdent := selector.X.(*ast.Ident); isIdent {
			subject = ident.Name
		}
		return false
	})
	return subject, found
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

// TestTheCleanupCensusReportsBothShapesItClaims drives the detector over a
// fixture, because on a clean tree the assertion above passes whether the scan
// works or is stuck.
//
// It is the repository's own pattern for a scanner
// (TestTheVocabularyScanReportsACopyItHasNeverSeen), and it carries one case per
// SHAPE the census claims to see plus two controls, because the two rules were
// added a round apart and each was found by a gate constructing the shape the
// previous version could not see.
func TestTheCleanupCensusReportsBothShapesItClaims(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	write := func(name, content string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	write("go.mod", "module fixture\n\ngo 1.24\n")
	// Shape one: one body, Close before Shutdown.
	write("one_body.go", `package fixture

func OneBody(server, node any) {
	server.Close()
	node.Shutdown()
}
`)
	// Shape two: a package-level literal, which the first version could not
	// reach because it descended only into declarations.
	write("package_level.go", `package fixture

var PackageLevel = func(server, node any) {
	server.Close()
	node.Shutdown()
}
`)
	// Shape three: two cleanup registrations, the Close registered LAST so it
	// runs FIRST. The method value is the part rule one cannot see.
	write("registrations.go", `package fixture

func Registrations(t any, server, node any) {
	t.Cleanup(func() { node.Shutdown() })
	t.Cleanup(server.Close)
}
`)
	// Control one: the correct order in one body.
	write("ordered.go", `package fixture

func Ordered(server, node any) {
	node.Shutdown()
	server.Close()
}
`)
	// Control two: a lone Close, which is a file or a response body and must
	// not be reported at all.
	write("lone_close.go", `package fixture

func LoneClose(f any) {
	f.Close()
}
`)

	sites, err := shutdownClosePairs(root)
	if err != nil {
		t.Fatalf("census over the fixture: %v", err)
	}
	disordered := map[string]bool{}
	for _, site := range sites {
		if !site.ordered() {
			disordered[site.where] = true
		}
	}
	for _, want := range []string{
		"one_body.go:OneBody",
		"package_level.go:file@3",
		"registrations.go:Registrations (cleanup: server.Close after Shutdown)",
	} {
		if !disordered[want] {
			t.Errorf("the census did not report %s as disordered; it reported %v", want, disordered)
		}
	}
	// The controls. A detector that reported everything would satisfy the loop
	// above and be useless.
	for _, unwanted := range []string{"ordered.go:Ordered", "lone_close.go:LoneClose"} {
		if disordered[unwanted] {
			t.Errorf("the census reported %s, which is correctly ordered or is not a pair at all", unwanted)
		}
	}
	if len(disordered) == 0 {
		t.Fatal("the census reported nothing over a fixture built to violate it")
	}
}
