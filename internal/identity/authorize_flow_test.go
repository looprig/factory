package identity_test

import (
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"testing"
)

// TestAuthorizerPrivilegeDecisionsHaveReducedInformationFlow proves the two
// exceptional decisions are a single semantic pipeline. It resolves objects
// rather than matching source text, so formatting and aliases are irrelevant.
func TestAuthorizerPrivilegeDecisionsHaveReducedInformationFlow(t *testing.T) {
	t.Parallel()

	files, info := typedIdentityProduction(t)
	methods := make(map[string]*ast.FuncDecl)
	for _, file := range files {
		for _, declaration := range file.Decls {
			method, ok := declaration.(*ast.FuncDecl)
			if !ok || !authorizerReceiver(method.Recv) {
				continue
			}
			methods[method.Name.Name] = method
		}
	}
	if len(methods) < 6 {
		t.Fatalf("found %d concrete Authorizer methods, want at least the six A0.2 seams", len(methods))
	}

	assertSubscribePipeline(t, methods["AuthorizeSubscribe"], info)
	assertServiceSweepPipeline(t, methods["AuthorizeServiceSweep"], info)
}

func typedIdentityProduction(t *testing.T) ([]*ast.File, *types.Info) {
	t.Helper()
	fset := token.NewFileSet()
	names := productionFiles(t)
	if len(names) < 2 {
		t.Fatalf("found %d identity production files, want at least 2", len(names))
	}
	files := make([]*ast.File, 0, len(names))
	for _, name := range names {
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		files = append(files, file)
	}
	info := &types.Info{
		Types:      make(map[ast.Expr]types.TypeAndValue),
		Defs:       make(map[*ast.Ident]types.Object),
		Uses:       make(map[*ast.Ident]types.Object),
		Selections: make(map[*ast.SelectorExpr]*types.Selection),
	}
	typedFiles := authorizerReachableFiles(files)
	if len(typedFiles) == 0 {
		t.Fatal("no production file declaring an Authorizer method was derived")
	}
	config := types.Config{Importer: newAuthorizationTestImporter()}
	if _, err := config.Check("github.com/looprig/factory/internal/identity", fset, typedFiles, info); err != nil {
		t.Fatalf("type-check identity production: %v", err)
	}
	return files, info
}

func authorizerReachableFiles(files []*ast.File) []*ast.File {
	declarations := make(map[string]*ast.File)
	selected := make(map[*ast.File]bool)
	for _, file := range files {
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok {
				continue
			}
			declarations[function.Name.Name] = file
			if authorizerReceiver(function.Recv) {
				selected[file] = true
			}
		}
	}
	for changed := true; changed; {
		changed = false
		for file := range selected {
			ast.Inspect(file, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				identifier, ok := call.Fun.(*ast.Ident)
				if !ok {
					return true
				}
				if dependency := declarations[identifier.Name]; dependency != nil && !selected[dependency] {
					selected[dependency] = true
					changed = true
				}
				return true
			})
		}
	}
	var result []*ast.File
	for _, file := range files {
		if selected[file] {
			result = append(result, file)
		}
	}
	return result
}

type authorizationTestImporter struct {
	standard types.Importer
	packages map[string]*types.Package
}

func newAuthorizationTestImporter() *authorizationTestImporter {
	result := &authorizationTestImporter{standard: importer.Default(), packages: make(map[string]*types.Package)}
	sessionwirePackage := types.NewPackage("github.com/looprig/core/sessionwire/v1", "sessionwire")
	tenant := testNamedString(sessionwirePackage, "TenantID")
	testStringValidator(tenant)
	session := testNamedString(sessionwirePackage, "SessionID")
	testStringValidator(session)
	object := types.NewTypeName(token.NoPos, sessionwirePackage, "ObjectReference", nil)
	types.NewNamed(object, types.NewStruct(nil, nil), nil)
	sessionwirePackage.Scope().Insert(object)
	sessionwirePackage.MarkComplete()
	result.packages[sessionwirePackage.Path()] = sessionwirePackage

	identityPackage := types.NewPackage("github.com/looprig/factory/identity", "identity")
	principalName := types.NewTypeName(token.NoPos, identityPackage, "Principal", nil)
	principal := types.NewNamed(principalName, types.NewStruct(nil, nil), nil)
	identityPackage.Scope().Insert(principalName)
	testMethod(principal, "Tenant", nil, []*types.Var{types.NewVar(token.NoPos, nil, "", tenant)})
	testMethod(principal, "Subject", nil, []*types.Var{types.NewVar(token.NoPos, nil, "", types.Typ[types.String])})
	testMethod(principal, "IsService", nil, []*types.Var{types.NewVar(token.NoPos, nil, "", types.Typ[types.Bool])})
	identityPackage.MarkComplete()
	result.packages[identityPackage.Path()] = identityPackage

	sessionstorePackage := types.NewPackage("github.com/looprig/sessionstore", "sessionstore")
	testNamedString(sessionstorePackage, "CommandKind")
	sessionstorePackage.MarkComplete()
	result.packages[sessionstorePackage.Path()] = sessionstorePackage
	return result
}

func (i *authorizationTestImporter) Import(importPath string) (*types.Package, error) {
	if pkg := i.packages[importPath]; pkg != nil {
		return pkg, nil
	}
	pkg, err := i.standard.Import(importPath)
	if err != nil {
		return nil, fmt.Errorf("import %s: %w", importPath, err)
	}
	return pkg, nil
}

func testNamedString(pkg *types.Package, name string) *types.Named {
	object := types.NewTypeName(token.NoPos, pkg, name, nil)
	named := types.NewNamed(object, types.Typ[types.String], nil)
	pkg.Scope().Insert(object)
	return named
}

func testStringValidator(receiver *types.Named) {
	testMethod(receiver, "Validate", nil, []*types.Var{types.NewVar(token.NoPos, nil, "", types.Universe.Lookup("error").Type())})
}

func testMethod(receiver *types.Named, name string, parameters, results []*types.Var) {
	receiverVariable := types.NewVar(token.NoPos, receiver.Obj().Pkg(), "", receiver)
	signature := types.NewSignatureType(receiverVariable, nil, nil, types.NewTuple(parameters...), types.NewTuple(results...), false)
	receiver.AddMethod(types.NewFunc(token.NoPos, receiver.Obj().Pkg(), name, signature))
}

func assertSubscribePipeline(t *testing.T, method *ast.FuncDecl, info *types.Info) {
	t.Helper()
	if method == nil {
		t.Fatal("AuthorizeSubscribe method was not derived from production files")
	}
	result := singleReturnExpression(t, method)
	outer := requireLocalCall(t, result, info, "authorizationResult", 1)
	allowed := requireLocalCall(t, outer.Args[0], info, "sessionChannelAllowed", 2)
	principal := parameterObject(t, method, info, "principal")
	channel := parameterObject(t, method, info, "channel")
	requireMethodCall(t, allowed.Args[0], info, principal, "Tenant", 0)
	parsed := requireLocalCall(t, allowed.Args[1], info, "parseSessionChannel", 1)
	requireIdentifierObject(t, parsed.Args[0], info, channel)
}

func assertServiceSweepPipeline(t *testing.T, method *ast.FuncDecl, info *types.Info) {
	t.Helper()
	if method == nil {
		t.Fatal("AuthorizeServiceSweep method was not derived from production files")
	}
	result := singleReturnExpression(t, method)
	outer := requireLocalCall(t, result, info, "authorizationResult", 1)
	principal := parameterObject(t, method, info, "principal")
	requireMethodCall(t, outer.Args[0], info, principal, "IsService", 0)
}

func singleReturnExpression(t *testing.T, method *ast.FuncDecl) ast.Expr {
	t.Helper()
	if method.Body == nil || len(method.Body.List) != 1 {
		t.Fatalf("Authorizer.%s has %d statements, want exactly one return pipeline", method.Name.Name, len(method.Body.List))
	}
	statement, ok := method.Body.List[0].(*ast.ReturnStmt)
	if !ok || len(statement.Results) != 1 {
		t.Fatalf("Authorizer.%s is not exactly one return expression", method.Name.Name)
	}
	return statement.Results[0]
}

func requireLocalCall(t *testing.T, expression ast.Expr, info *types.Info, name string, arguments int) *ast.CallExpr {
	t.Helper()
	call, ok := expression.(*ast.CallExpr)
	if !ok || len(call.Args) != arguments {
		t.Fatalf("expression %T is not a %d-argument call to %s", expression, arguments, name)
	}
	identifier, ok := call.Fun.(*ast.Ident)
	function, isFunction := info.Uses[identifier].(*types.Func)
	if !ok || !isFunction || function.Pkg() == nil || function.Pkg().Path() != "github.com/looprig/factory/internal/identity" || function.Name() != name {
		t.Fatalf("call resolves to %v, want local %s", function, name)
	}
	return call
}

func requireMethodCall(t *testing.T, expression ast.Expr, info *types.Info, receiver types.Object, name string, arguments int) {
	t.Helper()
	call, ok := expression.(*ast.CallExpr)
	if !ok || len(call.Args) != arguments {
		t.Fatalf("expression %T is not a %d-argument method call to %s", expression, arguments, name)
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	selection := info.Selections[selector]
	function, isFunction := info.Uses[selector.Sel].(*types.Func)
	if !ok || selection == nil || !isFunction || function.Name() != name {
		t.Fatalf("method call resolves to %v, want %s", function, name)
	}
	requireIdentifierObject(t, selector.X, info, receiver)
}

func parameterObject(t *testing.T, method *ast.FuncDecl, info *types.Info, name string) types.Object {
	t.Helper()
	for _, field := range method.Type.Params.List {
		for _, identifier := range field.Names {
			if identifier.Name == name {
				if object := info.Defs[identifier]; object != nil {
					return object
				}
			}
		}
	}
	t.Fatalf("Authorizer.%s has no typed %s parameter", method.Name.Name, name)
	return nil
}

func requireIdentifierObject(t *testing.T, expression ast.Expr, info *types.Info, want types.Object) {
	t.Helper()
	identifier, ok := expression.(*ast.Ident)
	if !ok || info.Uses[identifier] != want {
		t.Fatalf("expression %T resolves to %v, want parameter %v", expression, info.Uses[identifier], want)
	}
}
