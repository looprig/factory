package factory_test

import (
	"fmt"
	"go/ast"
	"go/constant"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	factoryidentity "github.com/looprig/factory/identity"
	internalidentity "github.com/looprig/factory/internal/identity"
)

func FuzzAuthorizeSubscribeMatchesIndependentGrammar(f *testing.F) {
	seeds := []string{
		"", "session", "session:", "session::", "session:::session-a",
		"session:tenant-a", "session:tenant-a:", "session::session-a",
		"session:tenant-a:session-a", "session:tenant-a:session-a:extra",
		"Session:tenant-a:session-a", "other:tenant-a:session-a",
		"session:tenant:a:session-a", "session:tenant-a:session:a",
		"session:\xff:session-a", "session:tenant-a:\xff",
	}
	for _, literal := range reachableSubscribeStringConstants(f) {
		seeds = append(seeds, literal, "session:"+literal+":s-derived", "session:t-derived:"+literal)
	}
	slices.Sort(seeds)
	seeds = slices.Compact(seeds)
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, channel string) {
		tenant, _, valid := oracleSessionChannel(channel)
		authorizer := internalidentity.Authorizer{}
		principals := []factoryidentity.Principal{
			{},
			mustFuzzPrincipal(t, "tenant-a", factoryidentity.KindActor),
			mustFuzzPrincipal(t, "tenant-b", factoryidentity.KindService),
		}
		if !valid {
			for _, principal := range principals {
				if err := authorizer.AuthorizeSubscribe(t.Context(), principal, channel); err != internalidentity.ErrUnauthorized {
					t.Fatalf("AuthorizeSubscribe(%q) = %v for malformed channel, want exact ErrUnauthorized", channel, err)
				}
			}
			return
		}

		for _, kind := range []factoryidentity.Kind{factoryidentity.KindActor, factoryidentity.KindService} {
			matching := mustFuzzPrincipal(t, tenant, kind)
			if err := authorizer.AuthorizeSubscribe(t.Context(), matching, channel); err != nil {
				t.Fatalf("AuthorizeSubscribe(%q) = %v for matching %s principal, want nil", channel, err, kind)
			}
		}
		distinct := sessionwire.TenantID("tenant-oracle-other")
		if distinct == tenant {
			distinct = "tenant-oracle-second"
		}
		foreign := mustFuzzPrincipal(t, distinct, factoryidentity.KindActor)
		if err := authorizer.AuthorizeSubscribe(t.Context(), foreign, channel); err != internalidentity.ErrUnauthorized {
			t.Fatalf("AuthorizeSubscribe(%q) = %v for distinct tenant %q, want exact ErrUnauthorized", channel, err, distinct)
		}
		if err := authorizer.AuthorizeSubscribe(t.Context(), factoryidentity.Principal{}, channel); err != internalidentity.ErrUnauthorized {
			t.Fatalf("AuthorizeSubscribe(%q) = %v for zero principal, want exact ErrUnauthorized", channel, err)
		}
	})
}

// oracleSessionChannel deliberately uses Cuts and an explicit containment
// check, independently of production's segment Split.
func oracleSessionChannel(channel string) (sessionwire.TenantID, sessionwire.SessionID, bool) {
	prefix, remainder, found := strings.Cut(channel, ":")
	if !found || prefix != "session" {
		return "", "", false
	}
	tenantPart, sessionPart, found := strings.Cut(remainder, ":")
	if !found || strings.Contains(sessionPart, ":") {
		return "", "", false
	}
	tenant := sessionwire.TenantID(tenantPart)
	session := sessionwire.SessionID(sessionPart)
	if tenant.Validate() != nil || session.Validate() != nil {
		return "", "", false
	}
	return tenant, session, true
}

func mustFuzzPrincipal(t *testing.T, tenant sessionwire.TenantID, kind factoryidentity.Kind) factoryidentity.Principal {
	t.Helper()
	principal, err := factoryidentity.NewPrincipal(tenant, "fuzz-subject", kind)
	if err != nil {
		t.Fatalf("NewPrincipal(%q, %s) = %v", tenant, kind, err)
	}
	return principal
}

// reachableSubscribeStringConstants derives mutation seeds from the typed call
// graph. A literal or folded concatenation added to either the public decision
// or its parser is therefore exercised without adding its value to this test.
func reachableSubscribeStringConstants(t testing.TB) []string {
	t.Helper()
	directory := filepath.Join("internal", "identity")
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatalf("read %s: %v", directory, err)
	}
	fset := token.NewFileSet()
	var files []*ast.File
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(directory, entry.Name()), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", entry.Name(), err)
		}
		files = append(files, file)
	}
	if len(files) < 2 {
		t.Fatalf("parsed %d identity production files, want at least 2", len(files))
	}
	info := &types.Info{
		Types: make(map[ast.Expr]types.TypeAndValue),
		Defs:  make(map[*ast.Ident]types.Object),
		Uses:  make(map[*ast.Ident]types.Object),
	}
	typedFiles := authorizationSeedFiles(files)
	if len(typedFiles) == 0 {
		t.Fatal("no production file containing the Subscribe/parser roots was derived")
	}
	config := types.Config{Importer: newAuthorizationSeedImporter()}
	if _, err := config.Check("github.com/looprig/factory/internal/identity", fset, typedFiles, info); err != nil {
		t.Fatalf("type-check identity production: %v", err)
	}
	declarations := make(map[*types.Func]*ast.FuncDecl)
	var roots []*types.Func
	for _, file := range files {
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			object, ok := info.Defs[function.Name].(*types.Func)
			if !ok {
				continue
			}
			declarations[object] = function
			if function.Name.Name == "AuthorizeSubscribe" || function.Name.Name == "parseSessionChannel" {
				roots = append(roots, object)
			}
		}
	}
	if len(roots) != 2 {
		t.Fatalf("derived %d Subscribe/parser roots, want exactly 2", len(roots))
	}

	seen := make(map[*types.Func]bool)
	queue := append([]*types.Func(nil), roots...)
	constants := make(map[string]struct{})
	for len(queue) > 0 {
		function := queue[0]
		queue = queue[1:]
		if seen[function] {
			continue
		}
		seen[function] = true
		declaration := declarations[function]
		ast.Inspect(declaration.Body, func(node ast.Node) bool {
			expression, ok := node.(ast.Expr)
			if ok {
				value := info.Types[expression].Value
				if value != nil && value.Kind() == constant.String {
					constants[constant.StringVal(value)] = struct{}{}
				}
			}
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			identifier, ok := call.Fun.(*ast.Ident)
			if !ok {
				return true
			}
			called, ok := info.Uses[identifier].(*types.Func)
			if ok && declarations[called] != nil {
				queue = append(queue, called)
			}
			return true
		})
	}
	if len(seen) < 2 {
		t.Fatalf("typed call graph reached %d functions, want at least the two roots", len(seen))
	}
	if len(constants) == 0 {
		t.Fatal("typed Subscribe/parser call graph contains no string constants, so derived fuzz seeds are vacuous")
	}
	result := make([]string, 0, len(constants))
	for value := range constants {
		result = append(result, value)
	}
	return result
}

func authorizationSeedFiles(files []*ast.File) []*ast.File {
	declarations := make(map[string]*ast.File)
	selected := make(map[*ast.File]bool)
	for _, file := range files {
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok {
				continue
			}
			declarations[function.Name.Name] = file
			if function.Name.Name == "AuthorizeSubscribe" || function.Name.Name == "parseSessionChannel" {
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

type authorizationSeedImporter struct {
	standard types.Importer
	packages map[string]*types.Package
}

func newAuthorizationSeedImporter() *authorizationSeedImporter {
	result := &authorizationSeedImporter{standard: importer.Default(), packages: make(map[string]*types.Package)}
	sessionwirePackage := types.NewPackage("github.com/looprig/core/sessionwire/v1", "sessionwire")
	tenant := seedNamedString(sessionwirePackage, "TenantID")
	seedValidator(tenant)
	session := seedNamedString(sessionwirePackage, "SessionID")
	seedValidator(session)
	objectName := types.NewTypeName(token.NoPos, sessionwirePackage, "ObjectReference", nil)
	types.NewNamed(objectName, types.NewStruct(nil, nil), nil)
	sessionwirePackage.Scope().Insert(objectName)
	sessionwirePackage.MarkComplete()
	result.packages[sessionwirePackage.Path()] = sessionwirePackage

	identityPackage := types.NewPackage("github.com/looprig/factory/identity", "identity")
	principalName := types.NewTypeName(token.NoPos, identityPackage, "Principal", nil)
	principal := types.NewNamed(principalName, types.NewStruct(nil, nil), nil)
	identityPackage.Scope().Insert(principalName)
	seedMethod(principal, "Tenant", nil, []*types.Var{types.NewVar(token.NoPos, nil, "", tenant)})
	seedMethod(principal, "Subject", nil, []*types.Var{types.NewVar(token.NoPos, nil, "", types.Typ[types.String])})
	seedMethod(principal, "IsService", nil, []*types.Var{types.NewVar(token.NoPos, nil, "", types.Typ[types.Bool])})
	identityPackage.MarkComplete()
	result.packages[identityPackage.Path()] = identityPackage

	sessionstorePackage := types.NewPackage("github.com/looprig/sessionstore", "sessionstore")
	seedNamedString(sessionstorePackage, "CommandKind")
	sessionstorePackage.MarkComplete()
	result.packages[sessionstorePackage.Path()] = sessionstorePackage
	return result
}

func (i *authorizationSeedImporter) Import(importPath string) (*types.Package, error) {
	if pkg := i.packages[importPath]; pkg != nil {
		return pkg, nil
	}
	pkg, err := i.standard.Import(importPath)
	if err != nil {
		return nil, fmt.Errorf("import %s: %w", importPath, err)
	}
	return pkg, nil
}

func seedNamedString(pkg *types.Package, name string) *types.Named {
	object := types.NewTypeName(token.NoPos, pkg, name, nil)
	named := types.NewNamed(object, types.Typ[types.String], nil)
	pkg.Scope().Insert(object)
	return named
}

func seedValidator(receiver *types.Named) {
	seedMethod(receiver, "Validate", nil, []*types.Var{types.NewVar(token.NoPos, nil, "", types.Universe.Lookup("error").Type())})
}

func seedMethod(receiver *types.Named, name string, parameters, results []*types.Var) {
	receiverVariable := types.NewVar(token.NoPos, receiver.Obj().Pkg(), "", receiver)
	signature := types.NewSignatureType(receiverVariable, nil, nil, types.NewTuple(parameters...), types.NewTuple(results...), false)
	receiver.AddMethod(types.NewFunc(token.NoPos, receiver.Obj().Pkg(), name, signature))
}
