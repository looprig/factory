package hostlink_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strings"
	"testing"
)

// regMu's two rules, held structurally (v0.4.0 regate N2).
//
//	RULE 1  every change to, or read-then-act on, the transport's subscription
//	        registry -- client.NewSubscription, client.RemoveSubscription,
//	        client.GetSubscription and a Subscription's Unsubscribe() -- is made
//	        inside a function that holds regMu for its whole body.
//	RULE 2  nothing reachable SYNCHRONOUSLY from a transport callback takes
//	        regMu, directly or through a call; a callback hands a withdrawal to
//	        its own goroutine (`go l.discard(...)`).
//
// Why structural: both rules are about interleavings a behavioural test can
// only hit by chance. The regate measured it -- with regMu removed from discard
// (X2d) or newSubscription (X2e), the withdrawn branch's guard bypassed (X2f),
// a callback discarding inline (D2) or taking regMu itself (D3), the whole
// suite stayed green; only D1 hung it. So the reader is the source, and it is
// proven non-vacuous by applying each of those regate mutants, verbatim, to the
// real source in memory and requiring a finding for every one.

var registryCalls = map[string]bool{"NewSubscription": true, "RemoveSubscription": true, "GetSubscription": true}

// callbackRegistrars are the transport's handler registrations: a function
// literal passed to one of them runs on a transport callback goroutine.
func isCallbackRegistrar(name string) bool { return strings.HasPrefix(name, "On") }

// callbackMethods are the link's methods the transport invokes directly.
var callbackMethods = map[string]bool{"onConnected": true, "onConnecting": true, "onDisconnected": true, "onMessage": true}

func TestRegMuGuardsTheRegistryAndNoCallbackTakesIt(t *testing.T) {
	sources := readLinkSources(t)
	if findings := regMuFindings(t, sources); len(findings) != 0 {
		t.Fatalf("regMu rules broken:\n%s", strings.Join(findings, "\n"))
	}
}

// TestTheRegMuReaderCatchesEveryRegateMutant is the anti-vacuity half: each of
// the regate's surviving mutants, applied to the live source, must be reported.
func TestTheRegMuReaderCatchesEveryRegateMutant(t *testing.T) {
	sources := readLinkSources(t)
	for _, m := range []struct{ id, old, new string }{
		{"X2d", "\tl.regMu.Lock()\n\tdefer l.regMu.Unlock()\n\tcurrent, ok", "\tcurrent, ok"},
		{"X2e", "\tl.regMu.Lock()\n\tdefer l.regMu.Unlock()\n\tsub, err := l.client.NewSubscription(channel)", "\tsub, err := l.client.NewSubscription(channel)"},
		{"X2f", "still this one's.\n\t\tl.discard(sub)\n\t\treturn l.await(ctx, entry)", "still this one's.\n\t\t_ = l.client.RemoveSubscription(sub)\n\t\treturn l.await(ctx, entry)"},
		{"D1", "\t\tif sub != nil {\n\t\t\tgo l.discard(sub)\n\t\t}\n\t\tif live {", "\t\tif sub != nil {\n\t\t\tl.discard(sub)\n\t\t}\n\t\tif live {"},
		{"D2", "\t\t\tgo l.discard(sub)\n\t\t}\n\t})\n\tsub.OnSubscribing", "\t\t\tl.discard(sub)\n\t\t}\n\t})\n\tsub.OnSubscribing"},
		{"D3", "\tsub.OnSubscribed(func(centrifugego.SubscribedEvent) {\n", "\tsub.OnSubscribed(func(centrifugego.SubscribedEvent) {\n\t\tl.regMu.Lock()\n\t\tl.regMu.Unlock()\n"},
	} {
		mutated := map[string]string{}
		applied := 0
		for name, src := range sources {
			if n := strings.Count(src, m.old); n > 0 {
				if n != 1 {
					t.Fatalf("%s: anchor matches %d times in %s", m.id, n, name)
				}
				src = strings.Replace(src, m.old, m.new, 1)
				applied++
			}
			mutated[name] = src
		}
		if applied != 1 {
			t.Fatalf("%s: anchor applied to %d files, want exactly 1 -- the source moved; re-derive the mutant", m.id, applied)
		}
		if findings := regMuFindings(t, mutated); len(findings) == 0 {
			t.Errorf("mutant %s is not reported: the reader cannot see the rule it breaks", m.id)
		}
	}
}

func readLinkSources(t *testing.T) map[string]string {
	t.Helper()
	sources := map[string]string{}
	for _, name := range []string{"subscribe.go", "centrifuge.go", "capability.go"} {
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		sources[name] = string(data)
	}
	return sources
}

// method is one *centrifugeLink method's body facts.
type method struct {
	name       string
	holdsRegMu bool // the body STARTS with l.regMu.Lock(); defer l.regMu.Unlock()
	takesRegMu bool // regMu is referenced anywhere, outside a go statement
	syncCalls  []string
}

func regMuFindings(t *testing.T, sources map[string]string) []string {
	t.Helper()
	fset := token.NewFileSet()
	methods := map[string]*method{}
	var callbackLits []*ast.FuncLit
	var findings []string

	names := make([]string, 0, len(sources))
	for name := range sources {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		file, err := parser.ParseFile(fset, name, sources[name], 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !isLinkMethod(fn) {
				continue
			}
			m := &method{name: fn.Name.Name, holdsRegMu: startsWithRegMu(fn.Body)}
			m.takesRegMu, m.syncCalls = scanBody(fn.Body)
			methods[m.name] = m
			// RULE 1: a registry call outside a regMu-holding body, or
			// inside a function literal (which the held lock does not cover
			// once it outlives the call).
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if lit, ok := n.(*ast.FuncLit); ok {
					if registrar := registrarOf(fn.Body, lit); registrar != "" {
						callbackLits = append(callbackLits, lit)
					}
				}
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				if registryCall(call) && !(m.holdsRegMu && !insideFuncLit(fn.Body, call)) {
					findings = append(findings, fmt.Sprintf("RULE 1: %s calls %s outside regMu (%s)", m.name, callName(call), fset.Position(call.Pos())))
				}
				return true
			})
		}
	}
	if len(methods) < 5 {
		t.Fatalf("parsed %d link methods; the reader is looking at nothing", len(methods))
	}
	if len(callbackLits) < 4 {
		t.Fatalf("found %d transport callback literals; the reader is looking at nothing", len(callbackLits))
	}

	// RULE 2: from each callback root, follow synchronous l.<method>() calls.
	var roots []string
	for name := range callbackMethods {
		if methods[name] == nil {
			t.Fatalf("callback method %s is gone; update callbackMethods", name)
		}
		roots = append(roots, name)
	}
	sort.Strings(roots)
	reaches := func(start []string, where string) {
		seen := map[string]bool{}
		queue := append([]string(nil), start...)
		for len(queue) > 0 {
			name := queue[0]
			queue = queue[1:]
			if seen[name] {
				continue
			}
			seen[name] = true
			m := methods[name]
			if m == nil {
				continue
			}
			if m.takesRegMu {
				findings = append(findings, fmt.Sprintf("RULE 2: %s reaches %s, which takes regMu, synchronously", where, name))
			}
			queue = append(queue, m.syncCalls...)
		}
	}
	for _, root := range roots {
		reaches([]string{root}, "callback "+root)
	}
	for i, lit := range callbackLits {
		takes, calls := scanBody(lit.Body)
		where := fmt.Sprintf("callback literal #%d (%s)", i, fset.Position(lit.Pos()))
		if takes {
			findings = append(findings, "RULE 2: "+where+" takes regMu")
		}
		reaches(calls, where)
	}
	return findings
}

func isLinkMethod(fn *ast.FuncDecl) bool {
	if fn.Recv == nil || len(fn.Recv.List) != 1 {
		return false
	}
	star, ok := fn.Recv.List[0].Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	ident, ok := star.X.(*ast.Ident)
	return ok && ident.Name == "centrifugeLink"
}

func isRegMuSelector(n ast.Node, method string) bool {
	call, ok := n.(*ast.CallExpr)
	if !ok {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != method {
		return false
	}
	inner, ok := sel.X.(*ast.SelectorExpr)
	return ok && inner.Sel.Name == "regMu"
}

func startsWithRegMu(body *ast.BlockStmt) bool {
	if len(body.List) < 2 {
		return false
	}
	lock, ok := body.List[0].(*ast.ExprStmt)
	if !ok || !isRegMuSelector(lock.X, "Lock") {
		return false
	}
	unlock, ok := body.List[1].(*ast.DeferStmt)
	return ok && isRegMuSelector(unlock.Call, "Unlock")
}

// scanBody reports whether regMu is referenced outside a go statement or a
// nested function literal, and which l.<method> calls are made synchronously.
func scanBody(body *ast.BlockStmt) (bool, []string) {
	takes := false
	var calls []string
	var walk func(n ast.Node) bool
	walk = func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.GoStmt:
			return false
		case *ast.FuncLit:
			return false
		case *ast.SelectorExpr:
			if x.Sel.Name == "regMu" {
				takes = true
			}
		case *ast.CallExpr:
			if sel, ok := x.Fun.(*ast.SelectorExpr); ok {
				if recv, ok := sel.X.(*ast.Ident); ok && recv.Name == "l" {
					calls = append(calls, sel.Sel.Name)
				}
			}
		}
		return true
	}
	ast.Inspect(body, walk)
	return takes, calls
}

func registryCall(call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	if registryCalls[sel.Sel.Name] {
		inner, ok := sel.X.(*ast.SelectorExpr)
		return ok && inner.Sel.Name == "client"
	}
	// A centrifuge Subscription's Unsubscribe takes no arguments; the
	// Subscriber seam's takes a tenant and a session.
	return sel.Sel.Name == "Unsubscribe" && len(call.Args) == 0
}

func callName(call *ast.CallExpr) string {
	if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
		return sel.Sel.Name
	}
	return "?"
}

func insideFuncLit(body *ast.BlockStmt, target ast.Node) bool {
	inside := false
	ast.Inspect(body, func(n ast.Node) bool {
		if lit, ok := n.(*ast.FuncLit); ok && lit.Pos() <= target.Pos() && target.End() <= lit.End() {
			inside = true
			return false
		}
		return true
	})
	return inside
}

// registrarOf returns the On* method a function literal is passed to, if any.
func registrarOf(body *ast.BlockStmt, lit *ast.FuncLit) string {
	name := ""
	ast.Inspect(body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || !isCallbackRegistrar(sel.Sel.Name) {
			return true
		}
		for _, arg := range call.Args {
			if arg == lit {
				name = sel.Sel.Name
			}
		}
		return true
	})
	return name
}
