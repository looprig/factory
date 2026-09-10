package factory_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/looprig/factory/internal/modfiles"
)

// This file is the reader for A7.2 step 1's "without restoring a cold session
// merely for viewing".
//
// The property holds today by construction of the two packages involved: the
// ClientLink's demand seam takes a tenant and a session and returns an error,
// and internal/routing's demand plane holds a routing table, a journal READ and
// a local publish. Neither can express a command. But "the seams cannot express
// it" is a property of the seams, not of the COMPOSITION -- and the composition
// is A9.1's, is not written yet, and is exactly where a restore would be
// inserted. A6.3's own spec gate demonstrated the gap: it added an AdmitRestore
// on every subscribe and the whole suite passed, exit 0.
//
// So the guard is structural and module-wide. From the declarations that make
// up the viewing path it follows calls through every production file this
// module owns, and reports any admission or placement entry point it can reach.
// A composition that admits a restore when a browser subscribes fails here even
// though no seam in either package changed.
//
// WHAT IT PROVES, and what it does not. It is a name-keyed call graph over
// parsed syntax with no type information, so it OVER-approximates edges: a call
// x.Foo() is an edge to every declaration in the module named Foo, whichever
// type it belongs to. Over-approximation is the safe direction for a ban -- it
// can report a path that type resolution would rule out, and it cannot miss one
// that dispatches through an interface, which is how every seam in this module
// is called. What it cannot see is a call made through a func value stored in a
// struct field or a map, and a restore reached that way is outside its reach.

// admissionEntryPoints is the set a viewing path may not reach.
//
// It is deliberately WIDER than "restore", because the neighbours of a named
// defect are this program's most expensive recurring failure: a subscribe that
// admitted an input, or that created a session, is the same error as one that
// restores, and a guard naming only the instance the runbook mentioned would
// pass on every one of them. The five V1 commands are Admitter's methods; the
// sixth is the legacy create; EnsurePlacement is the other way a viewer could
// cause a cold session to become resident.
var admissionEntryPoints = []string{
	"AdmitCreate",
	"AdmitGateResponse",
	"AdmitInput",
	"AdmitInterrupt",
	"AdmitLegacyCreate",
	"AdmitRestore",
	"EnsurePlacement",
}

// subscriptionLifecycleEvents are the transport registrations whose callback
// bodies are part of the viewing path even though they are anonymous.
//
// They are here because the ClientLink installs its subscribe handling as a
// function literal inside Handler.connected, which ALSO installs the RPC
// handler -- and the RPC handler admits commands, correctly. Rooting at the
// enclosing method would therefore report the legitimate path; rooting at the
// literal registered for a subscription lifecycle event is what separates them.
var subscriptionLifecycleEvents = []string{"OnSubscribe", "OnUnsubscribe", "OnDisconnect"}

// declaration is one named production function or method, with the names it
// calls and the names it mentions.
//
// The two sets are different on purpose. calls is the EDGE set and is
// deliberately narrow, because an edge that cannot be attributed grants
// coverage where it claims to demand it -- internal/admission's own reachability
// guard learned that when an orphan named Validate was "reached" by every
// req.Validate() in the module. mentions is the VIOLATION set and is
// deliberately wide: a declaration that merely names an admission entry point,
// without calling it there, has still put it on this path.
type declaration struct {
	name string
	// file is relative to the module root, with forward slashes.
	file     string
	calls    map[string]struct{}
	mentions map[string]struct{}
}

// callGraph is the module's declarations keyed by name. A name may have several
// declarations -- two types may each have a Close -- and every one of them is
// an edge, because nothing here resolves receivers.
type callGraph struct {
	byName map[string][]*declaration
	// roots is the viewing path's entry points, each already a declaration.
	roots []*declaration
	// counts, for the anti-vacuity assertions.
	files int
	decls int
}

// buildViewingPathGraph parses every production file the module owns and
// returns the graph with the viewing path's roots identified.
//
// The roots are DERIVED, not listed:
//
//   - every method of internal/routing's Demand, which is the demand plane's
//     whole surface including its background poll;
//   - the Engine.Bind that a ClientLink subscribe is authorized and recorded by,
//     which A6.3 deliberately made the single entry point for a subscription;
//   - every function literal registered for a subscription lifecycle event.
//
// A later task adding a method to Demand, or a fourth subscription event, is
// covered without editing this list.
func buildViewingPathGraph(t *testing.T, root string) *callGraph {
	t.Helper()

	files, err := modfiles.Files(root)
	if err != nil {
		t.Fatalf("modfiles.Files(%q): %v", root, err)
	}
	if len(files) == 0 {
		t.Fatalf("modfiles.Files(%q) returned no files; a scan over nothing reports nothing", root)
	}

	graph := &callGraph{byName: map[string][]*declaration{}}
	fset := token.NewFileSet()
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(fset, file, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		relative, err := filepath.Rel(root, file)
		if err != nil {
			t.Fatalf("relative path of %s: %v", file, err)
		}
		relative = filepath.ToSlash(relative)
		graph.files++
		imports := foreignImportNames(parsed)

		for _, decl := range parsed.Decls {
			function, ok := decl.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			named := &declaration{
				name: function.Name.Name, file: relative,
				calls: map[string]struct{}{}, mentions: map[string]struct{}{},
			}
			collectCalls(function.Body, imports, named.calls)
			collectMentions(function.Body, named.mentions)
			graph.byName[named.name] = append(graph.byName[named.name], named)
			graph.decls++

			if isDemandPlaneMethod(function) || isSubscribeEntryPoint(function) {
				graph.roots = append(graph.roots, named)
			}
			for _, literal := range lifecycleCallbacks(function.Body) {
				callback := &declaration{
					name:     function.Name.Name + " (subscription callback)",
					file:     relative,
					calls:    map[string]struct{}{},
					mentions: map[string]struct{}{},
				}
				collectCalls(literal.Body, imports, callback.calls)
				collectMentions(literal.Body, callback.mentions)
				graph.roots = append(graph.roots, callback)
			}
		}
	}
	return graph
}

// isDemandPlaneMethod reports a method on internal/routing's Demand. The
// receiver TYPE is what identifies it, so a method added later is a root
// without anybody remembering to say so.
func isDemandPlaneMethod(function *ast.FuncDecl) bool {
	return receiverTypeName(function.Recv) == "Demand"
}

// isSubscribeEntryPoint reports the ClientLink Engine method a subscription is
// authorized and recorded by.
func isSubscribeEntryPoint(function *ast.FuncDecl) bool {
	return function.Name.Name == "Bind" && receiverTypeName(function.Recv) == "Engine"
}

func receiverTypeName(recv *ast.FieldList) string {
	if recv == nil || len(recv.List) != 1 {
		return ""
	}
	expr := recv.List[0].Type
	if star, ok := expr.(*ast.StarExpr); ok {
		expr = star.X
	}
	ident, ok := expr.(*ast.Ident)
	if !ok {
		return ""
	}
	return ident.Name
}

// lifecycleCallbacks returns the function literals passed to a subscription
// lifecycle registration inside body.
func lifecycleCallbacks(body *ast.BlockStmt) []*ast.FuncLit {
	var found []*ast.FuncLit
	ast.Inspect(body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || !slices.Contains(subscriptionLifecycleEvents, selector.Sel.Name) {
			return true
		}
		for _, arg := range call.Args {
			if literal, ok := arg.(*ast.FuncLit); ok {
				found = append(found, literal)
			}
		}
		return true
	})
	return found
}

// collectMentions records every identifier and selector name a body contains.
//
// Selectors are recorded by their FINAL name only, and identifiers by their
// own: `svc.AdmitRestore(req)` contributes "AdmitRestore" whatever `svc` is,
// which is the point -- the seam a composition would call it through is an
// interface whose dynamic type no syntax tree knows.
func collectMentions(body ast.Node, into map[string]struct{}) {
	ast.Inspect(body, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.SelectorExpr:
			into[n.Sel.Name] = struct{}{}
			return true
		case *ast.Ident:
			into[n.Name] = struct{}{}
		}
		return true
	})
}

// collectCalls records the calls a body makes that could reach a declaration
// THIS MODULE owns, and skips only the ones that provably could not.
//
// THE UNIT OF ANALYSIS, stated as what it now is. Three shapes are recorded:
//
//   - a bare call, f(x), which names a declaration in the same package;
//   - a method or field call on a value, d.bindings.Acquire(x) or
//     admitter.Admit(x), whose receiver is dispatched at run time and may be
//     any implementation;
//   - a call qualified by a package OF THIS MODULE, placement.ReusableOwner(x)
//     or httpapi.NewRouter(cfg), because a declaration of this module is
//     exactly what such a call reaches.
//
// One shape is skipped: a call qualified by a package of a FOREIGN module,
// sessionwire.TenantID(t).Validate() or strings.Cut(s, ":"). The skip is keyed
// on the import PATH being outside github.com/looprig/factory, never on the
// identifier being an import name.
//
// **That distinction is the whole correction, and it was measured.** Keying on
// "is an import name" dropped 22 real intra-module edges -- including
// admission -> placement.ReusableOwner, server.go -> httpapi.NewRouter and, on
// the reached set itself, principalOf -> internalidentity.OperationContextFrom.
// Worse, it was defeatable without any shadowing: an identical helper called
// bare from internal/routing FAILED the guard, and the same helper moved to
// internal/placement and called as placement.ReachRestore(nil) PASSED with the
// whole suite green -- which is the shape production already uses 22 times and
// the shape A9.1's composition will have.
//
// The skip that remains is what keeps the graph from collapsing, and that was
// measured too: following a FOREIGN package-qualified call as an edge to every
// same-named declaration (the module declares a dozen methods named Validate
// and one named Error) reached 248 of the module's 362 declarations from these
// roots, including internal/httpapi's router, whose control routes are SUPPOSED
// to admit commands -- so the ban would have had to be deleted the first time a
// later task implemented the REST restore route. Keyed on the module path it is
// 91, up from the wrong version's 89.
//
// WHAT IT STILL CANNOT SEE, stated rather than left to be found:
//
//   - a call made through a func VALUE held in a struct field, a map or a
//     slice. A restore reached that way is outside its reach entirely.
//   - a foreign module that calls BACK into Factory through a callback it was
//     handed. That is not hypothetical -- it is how centrifuge invokes the
//     subscribe handler -- and it is why the roots include the function
//     literals registered for the subscription lifecycle events rather than
//     relying on an edge into them.
//   - which of several same-named declarations a call actually reaches. It
//     reports all of them, which is the safe direction for a ban.
//
// It DOES walk files no build configuration compiles, because modfiles ignores
// build constraints deliberately. For a ban that is conservative: it can report
// a path that is never built, and cannot miss one that is.
func collectCalls(body ast.Node, imports map[string]struct{}, into map[string]struct{}) {
	ast.Inspect(body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch fun := call.Fun.(type) {
		case *ast.Ident:
			into[fun.Name] = struct{}{}
		case *ast.SelectorExpr:
			if _, foreign := imports[rootIdentifier(fun.X)]; !foreign {
				into[fun.Sel.Name] = struct{}{}
			}
		}
		return true
	})
}

// rootIdentifier returns the leftmost identifier of an expression, which is
// what says whether a selector is rooted at a package name.
func rootIdentifier(expr ast.Expr) string {
	for {
		switch e := expr.(type) {
		case *ast.Ident:
			return e.Name
		case *ast.SelectorExpr:
			expr = e.X
		case *ast.CallExpr:
			expr = e.Fun
		case *ast.IndexExpr:
			expr = e.X
		case *ast.StarExpr:
			expr = e.X
		case *ast.ParenExpr:
			expr = e.X
		case *ast.TypeAssertExpr:
			expr = e.X
		default:
			return ""
		}
	}
}

// modulePath is this module, and it is the property the skip is keyed on.
const modulePath = "github.com/looprig/factory"

// foreignImportNames returns the names a file binds to packages of OTHER
// modules -- the ones collectCalls skips a selector rooted at.
//
// A package of THIS module is deliberately absent from the result, whatever it
// is named and however it is aliased, because a call qualified by one reaches a
// declaration the graph holds. Containment compares whole SEGMENTS, for
// import_boundary_test.go's reason: a module named github.com/looprig/factoryx
// is not this one.
func foreignImportNames(parsed *ast.File) map[string]struct{} {
	names := map[string]struct{}{}
	for _, spec := range parsed.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			continue
		}
		if pathIsInThisModule(path) {
			continue
		}
		if spec.Name != nil {
			if spec.Name.Name != "_" && spec.Name.Name != "." {
				names[spec.Name.Name] = struct{}{}
			}
			continue
		}
		segments := strings.Split(path, "/")
		names[segments[len(segments)-1]] = struct{}{}
	}
	return names
}

// pathIsInThisModule reports whether an import path names this module or a
// package inside it, comparing whole path segments.
func pathIsInThisModule(path string) bool {
	return path == modulePath || strings.HasPrefix(path, modulePath+"/")
}

// reach returns every declaration reachable from the roots, and the shortest
// path to each, so a failure names the route rather than the destination.
func (g *callGraph) reach() (map[*declaration][]string, []string) {
	paths := map[*declaration][]string{}
	queue := make([]*declaration, 0, len(g.roots))
	for _, root := range g.roots {
		if _, seen := paths[root]; seen {
			continue
		}
		paths[root] = []string{root.name}
		queue = append(queue, root)
	}

	var violations []string
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		path := paths[current]

		for _, entry := range admissionEntryPoints {
			if _, mentioned := current.mentions[entry]; mentioned {
				violations = append(violations, fmt.Sprintf(
					"%s (%s) names the admission entry point %s; path: %s",
					current.name, current.file, entry, strings.Join(append(slices.Clone(path), entry), " -> ")))
			}
		}
		for name := range current.calls {
			for _, next := range g.byName[name] {
				if _, seen := paths[next]; seen {
					continue
				}
				paths[next] = append(slices.Clone(path), next.name)
				queue = append(queue, next)
			}
		}
	}
	sort.Strings(violations)
	return paths, violations
}

// TestNothingOnTheViewingPathAdmitsACommand is the production assertion.
func TestNothingOnTheViewingPathAdmitsACommand(t *testing.T) {
	t.Parallel()

	root, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	graph := buildViewingPathGraph(t, root)

	// Anti-vacuity, in three directions. A graph with no roots reports nothing;
	// a graph whose roots reach nothing has no transitive reach to speak of;
	// and a forbidden set nothing in the module declares would be a ban on
	// names that do not exist.
	if len(graph.roots) < 4 {
		t.Fatalf("only %d viewing-path roots were derived, so this scan proves almost nothing; "+
			"expected the Demand plane's methods, Engine.Bind and the subscription callbacks", len(graph.roots))
	}
	reached, violations := graph.reach()
	if len(reached) <= len(graph.roots) {
		t.Fatalf("the viewing path reaches %d declarations from %d roots, so no transitive edge was followed at all",
			len(reached), len(graph.roots))
	}
	declared := 0
	for _, entry := range admissionEntryPoints {
		declared += len(graph.byName[entry])
	}
	if declared == 0 {
		t.Fatalf("no declaration in the module is named by %v, so the ban has no subject", admissionEntryPoints)
	}

	for _, violation := range violations {
		t.Errorf("the local-subscriber viewing path can admit a command: %s\n"+
			"A7.2 step 1: a first subscriber binds to a fresh accepting owner or leaves the session "+
			"UNBOUND. Viewing is not a reason to restore a cold session.", violation)
	}
}

// TestTheViewingPathScanReportsARestoreItHasNeverSeen drives the analyzer over
// a fixture, because the assertion above passes on a clean tree whether the
// scan works or not.
//
// The fixture carries a restore at each of the three ROOT categories and one
// reached transitively through two hops, plus two controls: a declaration that
// admits a restore but is not on the viewing path, and one on the path that
// calls something merely named similarly.
func TestTheViewingPathScanReportsARestoreItHasNeverSeen(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	write := func(name, content string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	write("go.mod", "module fixture\n\ngo 1.24\n")

	// Root category 1: a method of the demand plane.
	write("demand.go", `package fixture

type Demand struct{ admitter Admitter }

func (d *Demand) Acquire(session string) { d.admitter.AdmitRestore(session) }
`)
	// Root category 2: the ClientLink's subscribe entry point, reaching a
	// restore two hops away.
	write("engine.go", `package fixture

type Engine struct{ admitter Admitter }

func (e *Engine) Bind(channel string) { e.warm(channel) }

func (e *Engine) warm(channel string) { e.ensure(channel) }

func (e *Engine) ensure(channel string) { e.admitter.AdmitRestore(channel) }
`)
	// Root category 3: a subscription lifecycle callback.
	write("transport.go", `package fixture

func connected(client *Client, admitter Admitter) {
	client.OnSubscribe(func(channel string) { admitter.AdmitInput(channel) })
	client.OnRPC(func(channel string) { admitter.AdmitCreate(channel) })
}
`)
	// Control: an admission path that is not the viewing path at all.
	write("rpc.go", `package fixture

func handleRestore(admitter Admitter, session string) { admitter.AdmitGateResponse(session) }
`)
	// Control: on the path, calling something merely named similarly.
	write("neighbour.go", `package fixture

type Demand2 struct{}

func (d *Demand) Release(session string) { admitNothing(session) }

func admitNothing(session string) {}
`)

	graph := buildViewingPathGraph(t, root)
	_, violations := graph.reach()

	var got []string
	for _, violation := range violations {
		// Reduce each finding to "which entry point", so the fixture asserts
		// the analyzer's ANSWER rather than its message text.
		for _, entry := range admissionEntryPoints {
			if strings.Contains(violation, "the admission entry point "+entry) {
				got = append(got, entry)
			}
		}
	}
	sort.Strings(got)
	want := []string{"AdmitInput", "AdmitRestore", "AdmitRestore"}
	if !slices.Equal(got, want) {
		t.Errorf("the scan reported %v over the fixture, want %v\nfindings:\n%s",
			got, want, strings.Join(violations, "\n"))
	}
}

// TestTheViewingPathScanFollowsACallIntoThisModulesOwnPackages is the
// analyzer's self-test for the property the first version got wrong.
//
// The failing version keyed its skip on the identifier being an IMPORT NAME
// rather than on the import being a FOREIGN module, and Factory imports its own
// packages by path -- twenty-two intra-module edges were silently dropped, and
// an identical helper moved from internal/routing to internal/placement and
// called as placement.ReachRestore() turned a finding into a green suite with
// no shadowing trickery at all. That is the shape A9.1's composition will have.
//
// The fixture drives both directions in one graph: a call qualified by a
// package of THIS module must be followed, and a call qualified by a package of
// a foreign module must not be -- with the foreign target carrying an admission
// entry point, so a skip that stopped working would be a finding rather than a
// silence.
func TestTheViewingPathScanFollowsACallIntoThisModulesOwnPackages(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	write := func(name, content string) {
		t.Helper()
		if dir := filepath.Dir(name); dir != "." {
			if err := os.MkdirAll(filepath.Join(root, dir), 0o750); err != nil {
				t.Fatalf("mkdir %s: %v", dir, err)
			}
		}
		if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	// The fixture module is named as this one, because that path is the
	// property the skip is keyed on.
	write("go.mod", "module "+modulePath+"\n\ngo 1.24\n")
	write("demand.go", `package fixture

import (
	"strings"

	"`+modulePath+`/internal/placement"
	neighbour "`+modulePath+`extra/thing"
)

type Demand struct{}

func (d *Demand) Acquire(session string) {
	placement.ReachRestore(nil)
	strings.Cut(session, ":")
	neighbour.Reach(nil)
}
`)
	// A module whose path merely STARTS with this one's is a different module.
	// Without this arm, comparing bytes instead of whole path segments is an
	// equivalent mutation, because nothing in the real tree imports such a
	// path -- and "no driver exists today" is exactly how a containment bug
	// gets in later.
	write("neighbour.go", `package fixture

func Reach(a Admitter) { a.AdmitCreate() }
`)
	write("internal/placement/place.go", `package placement

// ReachRestore is reached through a package qualifier of THIS module.
func ReachRestore(a Admitter) { a.AdmitRestore() }
`)
	// The control: a declaration named for the FOREIGN call the root makes. An
	// edge to it would be a false one, and it is loaded so that a skip which
	// stopped working reports rather than merely widening.
	write("cut.go", `package fixture

type Admitter interface {
	AdmitRestore()
	AdmitInput()
	AdmitCreate()
}

func Cut(a Admitter, s, sep string) { a.AdmitInput() }
`)

	graph := buildViewingPathGraph(t, root)
	_, violations := graph.reach()
	if len(violations) != 1 {
		t.Fatalf("the scan reported %d findings, want exactly 1 (the module-qualified call, not the foreign one):\n%s",
			len(violations), strings.Join(violations, "\n"))
	}
	if !strings.Contains(violations[0], "AdmitRestore") {
		t.Errorf("the finding was %q, want the module-qualified AdmitRestore", violations[0])
	}
	if strings.Contains(violations[0], "AdmitInput") {
		t.Errorf("the finding was %q, so a call qualified by a FOREIGN package was followed", violations[0])
	}
	if strings.Contains(violations[0], "AdmitCreate") {
		t.Errorf("the finding was %q, so a module whose path merely starts with this one's was treated as this one", violations[0])
	}
}

// TestTheViewingPathScanFollowsASeamItCannotResolve is the analyzer's other
// self-test, and it is the one that matters for THIS module: every call the
// viewing path makes into a dependency goes through an interface, so a scan
// that only followed calls it could resolve to a concrete receiver would follow
// none of them.
func TestTheViewingPathScanFollowsASeamItCannotResolve(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	write := func(name, content string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	write("go.mod", "module fixture\n\ngo 1.24\n")
	write("seam.go", `package fixture

type Demand struct{ router Router }

// The call below is dispatched through an interface: nothing in the syntax
// says which serve runs.
func (d *Demand) Acquire(session string) { d.router.serve(session) }
`)
	write("other.go", `package fixture

type elsewhere struct{ admitter Admitter }

func (e *elsewhere) serve(session string) { e.admitter.AdmitRestore(session) }
`)

	graph := buildViewingPathGraph(t, root)
	_, violations := graph.reach()
	if len(violations) != 1 {
		t.Fatalf("the scan reported %d findings across an interface seam, want 1:\n%s",
			len(violations), strings.Join(violations, "\n"))
	}
	if !strings.Contains(violations[0], "AdmitRestore") {
		t.Errorf("the finding was %q, want it to name AdmitRestore", violations[0])
	}
}
