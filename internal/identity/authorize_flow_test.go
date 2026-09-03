// This file is the authorization information-flow analyzer. It type-checks the
// production sources of factory/identity and factory/internal/identity and pins
// the SYNTACTIC SHAPE of each authorization decision: which function is called,
// which parameter or field every argument is read from, and which statements
// parseSessionChannel is allowed to contain.
//
// What it does NOT prove is runtime behaviour. It never executes an
// authorization, so a decision that normalizes to the expected string could
// still be wrong. The behavioural claim lives elsewhere: the tables in
// authorize_test.go, and FuzzAuthorizeSubscribeMatchesIndependentGrammar in the
// root package, which checks AuthorizeSubscribe against a Cut/Contains oracle
// written independently of the production regexp.
//
// Fidelity limit, and it is deliberate: newAuditImporter (below) does not load
// the real github.com/looprig/core/sessionwire/v1 or
// github.com/looprig/sessionstore. It synthesizes stand-in packages with
// types.NewPackage, in which TenantID and SessionID are named string types
// carrying a Validate method that has a signature and no implementation -- the
// go/types model has no bodies. The analyzer can therefore prove only WHICH
// function a term calls; it proves nothing about what that validation accepts or
// rejects. The stand-in is what keeps the analyzer hermetic and independent of a
// Core release. It is not a sign that the real Core types went missing.

package identity_test

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
	"sort"
	"strings"
	"testing"
)

const (
	internalPath = "github.com/looprig/factory/internal/identity"
	identityPath = "github.com/looprig/factory/identity"
	wantPattern  = `\Asession:([^:]+):([^:]+)\z`
)

// TestAuthorizationInformationFlow is the production assertion of this file: it
// reads the real authorize.go and principal.go from disk, filtered to the
// declarations the policy is made of, and requires each audited function to
// normalize to its expected canonical string. The other three tests in this file
// are tests OF the analyzer: they assert nothing about production BEHAVIOUR,
// but they do depend on production SOURCE TEXT. Each builds its fixtures from
// productionSources with replaceOnce, which requires the quoted text to occur
// exactly once in the real file, so a rename in authorize.go or principal.go
// fails them.
func TestAuthorizationInformationFlow(t *testing.T) {
	t.Parallel()
	imports := newAuditImporter()
	public := readProductionPackage(t, filepath.Join("..", "..", "identity"), identityPath, imports, func(decl ast.Decl) bool {
		if declarationHasName(decl, "Principal") || declarationHasName(decl, "Kind") || declarationHasName(decl, "KindService") {
			return true
		}
		function, ok := decl.(*ast.FuncDecl)
		return ok && (function.Name.Name == "Tenant" || function.Name.Name == "Subject" || function.Name.Name == "IsService")
	})
	imports.packages[identityPath] = public.pkg
	internal := readProductionPackage(t, ".", internalPath, imports, func(decl ast.Decl) bool {
		function, isFunction := decl.(*ast.FuncDecl)
		if isFunction && authorizerReceiver(function.Recv) {
			return true
		}
		name := declarationName(decl)
		return name == "ErrUnauthorized" || name == "Authorizer" || name == "parsedSessionChannel" || name == "sessionChannelPattern" || name == "authorizeTenantPrincipal" || name == "authorizationResult" || name == "sessionChannelAllowed" || name == "parseSessionChannel"
	})
	if err := auditAuthorization(internal, public); err != nil {
		t.Fatal(err)
	}
}

// TestAuthorizationAnalyzerRejectsUnsafeSnippets tests the ANALYZER, not
// production. Each row rewrites a copy of the production source into a form that
// must not pass, and the row fails if the analyzer accepts it. It is what stops
// the analyzer above from being vacuous.
func TestAuthorizationAnalyzerRejectsUnsafeSnippets(t *testing.T) {
	t.Parallel()
	internal, public := productionSources(t)
	tests := []struct {
		name, old, replacement string
		public                 bool
	}{
		{"early subscribe grant", `return authorizationResult(sessionChannelAllowed(principal.Tenant(), parseSessionChannel(channel)))`, `if channel == "special" { return nil }; return authorizationResult(sessionChannelAllowed(principal.Tenant(), parseSessionChannel(channel)))`, false},
		{"channel tenant shortcut", `return parsed.valid && parsed.tenant == principalTenant`, `if principalTenant == "tenant-c" { return true }; return parsed.valid && parsed.tenant == principalTenant`, false},
		{"parser helper variable", `matches := sessionChannelPattern.FindStringSubmatch(channel)`, `find := sessionChannelPattern.FindStringSubmatch; matches := find(channel)`, false},
		{"parser forged tenant", `tenant := sessionwire.TenantID(matches[1])`, `tenant := sessionwire.TenantID("tenant-forged")`, false},
		{"parser malformed literal", `if len(matches) != 3`, `if len(matches) != 3 && channel != "malformed-special"`, false},
		{"service subject shortcut", `return authorizationResult(principal.IsService())`, `if principal.Subject() == "factory-a" { return nil }; return authorizationResult(principal.IsService())`, false},
		{"service tenant shortcut", `return authorizationResult(principal.IsService())`, `if principal.Tenant() == "tenant-b" { return nil }; return authorizationResult(principal.IsService())`, false},
		{"special inside IsService", `return p.kind == KindService`, `return p.kind == KindService || p.subject == "factory-a"`, true},
		{"regex widening", wantPattern, `\Asession:(.+):([^:]+)\z`, false},
		{"capture swap", `tenant := sessionwire.TenantID(matches[1])`, `tenant := sessionwire.TenantID(matches[2])`, false},
		{"tenant validation removed", `tenant.Validate() == nil && session.Validate() == nil`, `session.Validate() == nil`, false},
		{"valid forged true", `tenant.Validate() == nil && session.Validate() == nil`, `true`, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mutatedInternal, mutatedPublic := internal, public
			if test.public {
				mutatedPublic = replaceOnce(t, public, test.old, test.replacement)
			} else {
				mutatedInternal = replaceOnce(t, internal, test.old, test.replacement)
			}
			if err := auditSourcePair(mutatedInternal, mutatedPublic); err == nil {
				t.Fatal("unsafe snippet passed the authorization analyzer")
			}
		})
	}
}

// TestAuthorizationAnalyzerAcceptsEquivalentSafeForms tests the ANALYZER, not
// production. Each row rewrites the production source into a form that means the
// same thing and must still pass, which bounds how strict the analyzer may be so
// a legitimate refactor is not reported as a policy change.
func TestAuthorizationAnalyzerAcceptsEquivalentSafeForms(t *testing.T) {
	t.Parallel()
	internal, public := productionSources(t)
	tests := []struct{ name, old, replacement string }{
		{"parenthesized calls", `return authorizationResult(sessionChannelAllowed(principal.Tenant(), parseSessionChannel(channel)))`, `return (authorizationResult)((sessionChannelAllowed)((principal.Tenant)(), (parseSessionChannel)(channel)))`},
		{"parenthesized method value", `return authorizationResult(principal.IsService())`, `return authorizationResult((principal.IsService)())`},
		{"immutable local method value", `return authorizationResult(principal.IsService())`, `check := principal.IsService; return authorizationResult(check())`},
		{"immutable local method expression", `return authorizationResult(principal.IsService())`, `check := factoryidentity.Principal.IsService; return authorizationResult(check(principal))`},
		{"reversed equality", `return parsed.valid && parsed.tenant == principalTenant`, `return parsed.valid && principalTenant == parsed.tenant`},
		{"reordered parenthesized conjunction", `return parsed.valid && parsed.tenant == principalTenant`, `return ((principalTenant == parsed.tenant) && (parsed.valid))`},
		{"reordered conjunction", `return parsed.valid && parsed.tenant == principalTenant`, `return parsed.tenant == principalTenant && parsed.valid`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mutated := replaceOnce(t, internal, test.old, test.replacement)
			if err := auditSourcePair(mutated, public); err != nil {
				t.Fatalf("safe equivalent rejected: %v", err)
			}
		})
	}
}

// TestAuthorizationAnalyzerReturnsErrorsForMalformedInput tests the ANALYZER,
// not production. It feeds input the analyzer cannot handle and requires a
// descriptive, non-empty error rather than a silent pass.
func TestAuthorizationAnalyzerReturnsErrorsForMalformedInput(t *testing.T) {
	t.Parallel()
	internal, public := productionSources(t)
	tests := []struct {
		name     string
		internal string
		public   string
	}{
		{"malformed syntax", "package identity\nfunc (", "package identity"},
		{"function parameter target", replaceOnce(t, internal,
			`func (Authorizer) AuthorizeServiceSweep(_ context.Context, principal factoryidentity.Principal) error {
	return authorizationResult(principal.IsService())
}`,
			`func (Authorizer) AuthorizeServiceSweep(_ context.Context, principal factoryidentity.Principal, check func() bool) error {
	return authorizationResult(check())
}`), public},
		{"multiply assigned function variable", replaceOnce(t, internal,
			`return authorizationResult(principal.IsService())`,
			`check := principal.IsService; check = principal.IsService; return authorizationResult(check())`), public},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := auditSourcePair(test.internal, test.public)
			if err == nil || strings.TrimSpace(err.Error()) == "" {
				t.Fatalf("analyzer error = %v, want a descriptive error", err)
			}
		})
	}
}

type typedSource struct {
	pkg   *types.Package
	files []*ast.File
	info  *types.Info
}

func auditSourcePair(internalSource, publicSource string) error {
	imports := newAuditImporter()
	public, err := typeSource(identityPath, publicSource, imports)
	if err != nil {
		return fmt.Errorf("public identity: %w", err)
	}
	imports.packages[identityPath] = public.pkg
	internal, err := typeSource(internalPath, internalSource, imports)
	if err != nil {
		return fmt.Errorf("internal identity: %w", err)
	}
	return auditAuthorization(internal, public)
}

func auditAuthorization(internal, public *typedSource) error {
	if len(internal.files) == 0 || len(public.files) == 0 {
		return fmt.Errorf("authorization audit has no production files")
	}
	// The want strings below are written in the bespoke canonical grammar that
	// normalizeFunctionResult and normalizer.expression emit. Its whole
	// vocabulary is:
	//
	//	param:NAME          a parameter, renamed to a positional role by
	//	                    newNormalizer's parameterRoles table, so renaming a
	//	                    parameter in production does not move an expectation
	//	field:NAME(RECV)    a struct field read from the receiver expression RECV
	//	call:NAME(ARG,...)  a call of an approved function NAME; a method call
	//	                    carries its receiver as the FIRST argument
	//	const:NAME          a named constant
	//	literal:VALUE       a constant-folded literal
	//	eq:A&B              an == comparison, with A and B SORTED, so operand
	//	                    order is not part of the expectation
	//	and:A&B&...         a && chain, flattened and SORTED, so conjunct order
	//	                    is not part of the expectation
	//	nil                 the nil identifier
	//
	// So "and:eq:field:tenant(param:parsed)&param:principalTenant&field:valid(param:parsed)"
	// reads as: a conjunction of (the parsed value's tenant field compared for
	// equality with the principalTenant parameter) and (the parsed value's valid
	// field), in either order.
	//
	// Anything the grammar has no form for is an ERROR rather than a term: an
	// unapproved function, field or method, a mutable or multiply assigned
	// local, an unsupported operator, an unsupported statement. That is what
	// makes an expectation exhaustive rather than a substring match.
	checks := []struct {
		pkg            *typedSource
		function, want string
	}{
		{internal, "AuthorizeSubscribe", "call:authorizationResult(call:sessionChannelAllowed(call:Principal.Tenant(param:principal),call:parseSessionChannel(param:channel)))"},
		{internal, "AuthorizeServiceSweep", "call:authorizationResult(call:Principal.IsService(param:principal))"},
		{internal, "sessionChannelAllowed", "and:eq:field:tenant(param:parsed)&param:principalTenant&field:valid(param:parsed)"},
		{public, "Tenant", "field:tenant(param:p)"},
		{public, "IsService", "eq:const:KindService&field:kind(param:p)"},
	}
	for _, check := range checks {
		function, err := check.pkg.function(check.function)
		if err != nil {
			return err
		}
		normalized, err := normalizeFunctionResult(check.pkg, function)
		if err != nil {
			return fmt.Errorf("%s: %w", check.function, err)
		}
		if normalized != check.want {
			return fmt.Errorf("%s normalizes to %s, want %s", check.function, normalized, check.want)
		}
	}
	return auditParser(internal)
}

func normalizeFunctionResult(source *typedSource, function *ast.FuncDecl) (string, error) {
	if function.Body == nil {
		return "", fmt.Errorf("has no body")
	}
	n := newNormalizer(source, function)
	var result ast.Expr
	for _, statement := range function.Body.List {
		switch statement := statement.(type) {
		case *ast.AssignStmt:
			if statement.Tok != token.DEFINE || len(statement.Lhs) != len(statement.Rhs) {
				return "", fmt.Errorf("contains a non-declaration assignment")
			}
			for _, left := range statement.Lhs {
				identifier, ok := left.(*ast.Ident)
				if !ok || identifier.Name == "_" || source.info.Defs[identifier] == nil {
					return "", fmt.Errorf("contains an unsupported local assignment")
				}
			}
		case *ast.DeclStmt:
			general, ok := statement.Decl.(*ast.GenDecl)
			if !ok || general.Tok != token.VAR {
				return "", fmt.Errorf("contains an unsupported declaration")
			}
		case *ast.ReturnStmt:
			if result != nil || len(statement.Results) != 1 {
				return "", fmt.Errorf("must have exactly one single-expression return")
			}
			result = statement.Results[0]
		default:
			return "", fmt.Errorf("contains unsupported statement %T", statement)
		}
	}
	if result == nil {
		return "", fmt.Errorf("has no return")
	}
	normalized, err := n.expression(result)
	if err != nil {
		return "", err
	}
	for variable := range n.inits {
		if !n.used[variable] {
			return "", fmt.Errorf("local variable %s does not contribute to the result", variable.Name())
		}
	}
	return normalized, nil
}

type normalizer struct {
	source *typedSource
	labels map[types.Object]string
	inits  map[*types.Var]ast.Expr
	writes map[*types.Var]int
	active map[*types.Var]bool
	used   map[*types.Var]bool
}

func newNormalizer(source *typedSource, function *ast.FuncDecl) *normalizer {
	n := &normalizer{source: source, labels: map[types.Object]string{}, inits: map[*types.Var]ast.Expr{}, writes: map[*types.Var]int{}, active: map[*types.Var]bool{}, used: map[*types.Var]bool{}}
	parameterRoles := map[string][]string{
		"AuthorizeSubscribe":    {"context", "principal", "channel"},
		"AuthorizeServiceSweep": {"context", "principal"},
		"sessionChannelAllowed": {"principalTenant", "parsed"},
	}
	roles := parameterRoles[function.Name.Name]
	parameterIndex := 0
	for _, field := range function.Type.Params.List {
		for _, name := range field.Names {
			if object := source.info.Defs[name]; object != nil && parameterIndex < len(roles) {
				n.labels[object] = "param:" + roles[parameterIndex]
			}
			parameterIndex++
		}
	}
	if function.Recv != nil {
		for _, field := range function.Recv.List {
			for _, name := range field.Names {
				if object := source.info.Defs[name]; object != nil {
					n.labels[object] = "param:p"
				}
			}
		}
	}
	for _, name := range []string{"authorizationResult", "sessionChannelAllowed", "parseSessionChannel", "KindService"} {
		if object := source.pkg.Scope().Lookup(name); object != nil {
			n.labels[object] = name
		}
	}
	for _, pkg := range append([]*types.Package{source.pkg}, source.pkg.Imports()...) {
		for _, typeName := range []string{"Principal", "TenantID", "SessionID"} {
			if object, ok := pkg.Scope().Lookup(typeName).(*types.TypeName); ok {
				set := types.NewMethodSet(object.Type())
				for index := 0; index < set.Len(); index++ {
					selection := set.At(index)
					n.labels[selection.Obj()] = typeName + "." + selection.Obj().Name()
				}
			}
		}
	}
	for _, typeName := range []string{"Principal", "parsedSessionChannel"} {
		object, ok := source.pkg.Scope().Lookup(typeName).(*types.TypeName)
		if !ok {
			continue
		}
		named, ok := object.Type().(*types.Named)
		if !ok {
			continue
		}
		structure, ok := named.Underlying().(*types.Struct)
		if !ok {
			continue
		}
		for index := 0; index < structure.NumFields(); index++ {
			field := structure.Field(index)
			switch field.Name() {
			case "tenant", "kind", "valid":
				n.labels[field] = "field:" + field.Name()
			}
		}
	}
	ast.Inspect(function.Body, func(node ast.Node) bool {
		switch node := node.(type) {
		case *ast.ValueSpec:
			for index, name := range node.Names {
				if variable, ok := source.info.Defs[name].(*types.Var); ok {
					n.writes[variable]++
					if len(node.Values) == len(node.Names) {
						n.inits[variable] = node.Values[index]
					}
				}
			}
		case *ast.AssignStmt:
			for index, left := range node.Lhs {
				if identifier, ok := left.(*ast.Ident); ok {
					object := source.info.Defs[identifier]
					if object == nil {
						object = source.info.Uses[identifier]
					}
					if variable, ok := object.(*types.Var); ok {
						n.writes[variable]++
						if len(node.Rhs) == len(node.Lhs) {
							n.inits[variable] = node.Rhs[index]
						}
					}
				}
			}
		}
		return true
	})
	return n
}

func (n *normalizer) expression(expression ast.Expr) (string, error) {
	expression = unwrap(expression)
	switch expression := expression.(type) {
	case *ast.Ident:
		if expression.Name == "nil" {
			return "nil", nil
		}
		object := n.source.info.Uses[expression]
		if object == nil {
			object = n.source.info.Defs[expression]
		}
		if label := n.labels[object]; label != "" {
			if _, ok := object.(*types.Const); ok {
				return "const:" + label, nil
			}
			return label, nil
		}
		if variable, ok := object.(*types.Var); ok {
			if n.writes[variable] != 1 || n.inits[variable] == nil {
				return "", fmt.Errorf("variable %s is not immutable single-assignment", variable.Name())
			}
			if n.active[variable] {
				return "", fmt.Errorf("variable %s has recursive initializer", variable.Name())
			}
			n.active[variable] = true
			n.used[variable] = true
			result, err := n.expression(n.inits[variable])
			delete(n.active, variable)
			return result, err
		}
		return "", fmt.Errorf("unknown identifier object %v", object)
	case *ast.SelectorExpr:
		selection := n.source.info.Selections[expression]
		if selection == nil || selection.Kind() != types.FieldVal {
			return "", fmt.Errorf("selector is not a field")
		}
		receiver, err := n.expression(expression.X)
		if err != nil {
			return "", err
		}
		label := n.labels[selection.Obj()]
		if label == "" {
			return "", fmt.Errorf("unapproved field %s", selection.Obj().Name())
		}
		return label + "(" + receiver + ")", nil
	case *ast.CallExpr:
		callee, receiver, err := n.callee(expression.Fun)
		if err != nil {
			return "", err
		}
		arguments := make([]string, 0, len(expression.Args)+1)
		if receiver != nil {
			value, err := n.expression(receiver)
			if err != nil {
				return "", err
			}
			arguments = append(arguments, value)
		}
		for _, argument := range expression.Args {
			value, err := n.expression(argument)
			if err != nil {
				return "", err
			}
			arguments = append(arguments, value)
		}
		return "call:" + callee + "(" + strings.Join(arguments, ",") + ")", nil
	case *ast.BinaryExpr:
		if expression.Op == token.LAND {
			var terms []string
			if err := n.andTerms(expression, &terms); err != nil {
				return "", err
			}
			// && is commutative, so conjunct order must not change the
			// canonical form. Two separate probes bound this, and they bound
			// different things.
			//
			// TestAuthorizationInformationFlow bounds the normalization's
			// PRESENCE. An "eq:" term always sorts before a "field:" term, so
			// the want in checks is eq-first, while production writes
			// parsed.valid && parsed.tenant == principalTenant -- valid-first.
			// Deleting the sort therefore fails the production audit, and with
			// it the five accept rows that keep production's conjunct order.
			//
			// The "reordered conjunction" and "reordered parenthesized
			// conjunction" rows of
			// TestAuthorizationAnalyzerAcceptsEquivalentSafeForms bound it as
			// order-INSENSITIVE rather than as some fixed permutation: they are
			// the only rows written eq-first, so replacing this sort with an
			// unconditional swap of a two-term chain passes the production audit
			// and every other row, and fails only there. Of the two, "reordered
			// conjunction" is the isolating one -- it keeps production's ==
			// operand order, so its sessionChannelAllowed result is unaffected
			// by the == sort below, which the parenthesized row's is not.
			//
			// No accept row can be the SOLE thing bound to the sort's
			// deletion. The five rows that keep production's conjunct order do
			// fail on it, but only for the reason the production audit does;
			// and no eq-first row can fail on it -- with two conjuncts the
			// only reordering of production is the sorted one, so an eq-first
			// row normalizes to the want whether this line runs or not. A
			// conjunct-order-only row that isolates the deletion is therefore
			// structurally impossible rather than an omission.
			sort.Strings(terms)
			return "and:" + strings.Join(terms, "&"), nil
		}
		if expression.Op == token.EQL {
			left, err := n.expression(expression.X)
			if err != nil {
				return "", err
			}
			right, err := n.expression(expression.Y)
			if err != nil {
				return "", err
			}
			// == is commutative, so operand order must not change the
			// canonical form. This normalization is what lets the
			// "reversed equality" row of
			// TestAuthorizationAnalyzerAcceptsEquivalentSafeForms pass.
			values := []string{left, right}
			sort.Strings(values)
			return "eq:" + strings.Join(values, "&"), nil
		}
		return "", fmt.Errorf("unsupported binary operator %s", expression.Op)
	default:
		if value := n.source.info.Types[expression].Value; value != nil {
			return "literal:" + value.ExactString(), nil
		}
		return "", fmt.Errorf("unsupported expression %T", expression)
	}
}

func (n *normalizer) andTerms(expression ast.Expr, terms *[]string) error {
	if binary, ok := unwrap(expression).(*ast.BinaryExpr); ok && binary.Op == token.LAND {
		if err := n.andTerms(binary.X, terms); err != nil {
			return err
		}
		return n.andTerms(binary.Y, terms)
	}
	value, err := n.expression(expression)
	if err != nil {
		return err
	}
	*terms = append(*terms, value)
	return nil
}
func (n *normalizer) callee(expression ast.Expr) (string, ast.Expr, error) {
	expression = unwrap(expression)
	if identifier, ok := expression.(*ast.Ident); ok {
		object := n.source.info.Uses[identifier]
		if function, ok := object.(*types.Func); ok {
			label := n.labels[function]
			if label == "" {
				return "", nil, fmt.Errorf("unapproved function %s", function.FullName())
			}
			return label, nil, nil
		}
		if variable, ok := object.(*types.Var); ok {
			if n.writes[variable] != 1 || n.inits[variable] == nil {
				return "", nil, fmt.Errorf("function variable %s is not immutable", variable.Name())
			}
			if n.active[variable] {
				return "", nil, fmt.Errorf("function variable %s has a recursive initializer", variable.Name())
			}
			n.used[variable] = true
			n.active[variable] = true
			label, receiver, err := n.callee(n.inits[variable])
			delete(n.active, variable)
			return label, receiver, err
		}
		return "", nil, fmt.Errorf("call target is not a known function: %v", object)
	}
	if selector, ok := expression.(*ast.SelectorExpr); ok {
		selection := n.source.info.Selections[selector]
		if selection == nil {
			return "", nil, fmt.Errorf("selector call has no method selection")
		}
		label := n.labels[selection.Obj()]
		if label == "" {
			return "", nil, fmt.Errorf("unapproved method %s", selection.Obj().Name())
		}
		switch selection.Kind() {
		case types.MethodVal:
			return label, selector.X, nil
		case types.MethodExpr:
			return label, nil, nil
		default:
			return "", nil, fmt.Errorf("selector is not a method value or expression")
		}
	}
	return "", nil, fmt.Errorf("unsupported call target %T", expression)
}
func unwrap(expression ast.Expr) ast.Expr {
	for {
		parenthesized, ok := expression.(*ast.ParenExpr)
		if !ok {
			return expression
		}
		expression = parenthesized.X
	}
}

func auditParser(source *typedSource) error {
	pattern := source.pkg.Scope().Lookup("sessionChannelPattern")
	if pattern == nil {
		return fmt.Errorf("sessionChannelPattern is absent")
	}
	initializer, err := objectInitializer(source, pattern)
	if err != nil {
		return err
	}
	call, ok := unwrap(initializer).(*ast.CallExpr)
	if !ok || len(call.Args) != 1 {
		return fmt.Errorf("sessionChannelPattern must be one MustCompile call")
	}
	identifier, ok := unwrap(call.Fun).(*ast.SelectorExpr)
	if !ok {
		return fmt.Errorf("sessionChannelPattern does not select MustCompile")
	}
	function, ok := source.info.Uses[identifier.Sel].(*types.Func)
	if !ok || function.Pkg() == nil || function.Pkg().Path() != "regexp" || function.Name() != "MustCompile" {
		return fmt.Errorf("sessionChannelPattern does not call regexp.MustCompile")
	}
	value := source.info.Types[call.Args[0]].Value
	if value == nil || value.Kind() != constant.String || constant.StringVal(value) != wantPattern {
		return fmt.Errorf("sessionChannelPattern is not the fixed grammar")
	}
	parserFunction, err := source.function("parseSessionChannel")
	if err != nil {
		return err
	}
	if parserFunction.Body == nil {
		return fmt.Errorf("parseSessionChannel has no body")
	}
	// Every check below indexes Body.List positionally, so the statement count
	// is what makes the audit total rather than a spot check: parseSessionChannel
	// is pinned to exactly its five statements -- match, reject, tenant capture,
	// session capture, return -- because a sixth would not be audited at all.
	if len(parserFunction.Body.List) != 5 {
		return fmt.Errorf("parseSessionChannel statement count is not 5")
	}
	matches, err := assignmentCall(source, parserFunction, parserFunction.Body.List[0])
	if err != nil {
		return err
	}
	ifStatement, ok := parserFunction.Body.List[1].(*ast.IfStmt)
	if !ok || ifStatement.Init != nil || ifStatement.Else != nil || len(ifStatement.Body.List) != 1 {
		return fmt.Errorf("parser reject must be the sole simple if branch")
	}
	if !isLenNotThree(source, ifStatement.Cond, matches) {
		return fmt.Errorf("parser reject is not len(matches) != 3")
	}
	if !isEmptyParsedReturn(source, ifStatement.Body.List[0]) {
		return fmt.Errorf("parser reject does not return empty parsedSessionChannel")
	}
	tenant, err := assignmentCapture(source, parserFunction.Body.List[2], "TenantID", matches, 1)
	if err != nil {
		return err
	}
	session, err := assignmentCapture(source, parserFunction.Body.List[3], "SessionID", matches, 2)
	if err != nil {
		return err
	}
	returnStatement, ok := parserFunction.Body.List[4].(*ast.ReturnStmt)
	if !ok || len(returnStatement.Results) != 1 {
		return fmt.Errorf("parser final statement is not one return")
	}
	composite, ok := unwrap(returnStatement.Results[0]).(*ast.CompositeLit)
	if !ok || len(composite.Elts) != 3 {
		return fmt.Errorf("parser result is not a three-field parsed value")
	}
	fields := map[string]ast.Expr{}
	for _, element := range composite.Elts {
		pair, ok := element.(*ast.KeyValueExpr)
		if !ok {
			return fmt.Errorf("parser result fields must be keyed")
		}
		key, ok := pair.Key.(*ast.Ident)
		if !ok {
			return fmt.Errorf("parser result key is not a field")
		}
		fields[key.Name] = pair.Value
	}
	if !identifies(source, fields["tenant"], tenant) || !identifies(source, fields["session"], session) {
		return fmt.Errorf("parser capture mapping is not tenant[1], session[2]")
	}
	valid, err := normalizeValidation(source, fields["valid"], tenant, session)
	if err != nil {
		return err
	}
	if !valid {
		return fmt.Errorf("parser validity is not both Core validations")
	}
	// Whole-body shape, counted over the entire subtree rather than the five
	// top-level statements, so nothing escapes the positional audit by hiding
	// inside an expression. The 1 branch is the len(matches) != 3 reject; the 2
	// returns are that reject and the final result; the 6 calls are exactly the
	// set identified above -- FindStringSubmatch, len, the TenantID and SessionID
	// conversions, and the two Validate calls. A seventh call is by construction
	// one nothing above has identified.
	//
	// A refactor of parseSessionChannel is EXPECTED to fail here. The fix is to
	// re-audit the new shape and update these counts, not to relax them.
	branches, returns, calls := 0, 0, 0
	ast.Inspect(parserFunction.Body, func(node ast.Node) bool {
		switch node.(type) {
		case *ast.IfStmt:
			branches++
		case *ast.ReturnStmt:
			returns++
		case *ast.CallExpr:
			calls++
		}
		return true
	})
	if branches != 1 || returns != 2 || calls != 6 {
		return fmt.Errorf("parser shape has branches=%d returns=%d calls=%d, want 1/2/6", branches, returns, calls)
	}
	return nil
}

func normalizeValidation(source *typedSource, expression ast.Expr, tenant, session types.Object) (bool, error) {
	terms := []ast.Expr{}
	var flatten func(ast.Expr)
	flatten = func(expr ast.Expr) {
		if b, ok := unwrap(expr).(*ast.BinaryExpr); ok && b.Op == token.LAND {
			flatten(b.X)
			flatten(b.Y)
			return
		}
		terms = append(terms, expr)
	}
	flatten(expression)
	if len(terms) != 2 {
		return false, nil
	}
	want := map[types.Object]bool{tenant: false, session: false}
	for _, term := range terms {
		b, ok := unwrap(term).(*ast.BinaryExpr)
		if !ok || b.Op != token.EQL {
			return false, nil
		}
		var call *ast.CallExpr
		if c, ok := unwrap(b.X).(*ast.CallExpr); ok && isNil(b.Y) {
			call = c
		} else if c, ok := unwrap(b.Y).(*ast.CallExpr); ok && isNil(b.X) {
			call = c
		} else {
			return false, nil
		}
		selector, ok := unwrap(call.Fun).(*ast.SelectorExpr)
		if !ok || len(call.Args) != 0 {
			return false, nil
		}
		selection := source.info.Selections[selector]
		if selection == nil || selection.Obj().Pkg() == nil || selection.Obj().Pkg().Path() != "github.com/looprig/core/sessionwire/v1" || selection.Obj().Name() != "Validate" {
			return false, nil
		}
		identifier, ok := unwrap(selector.X).(*ast.Ident)
		if !ok {
			return false, nil
		}
		object := source.info.Uses[identifier]
		if _, ok := want[object]; !ok {
			return false, nil
		}
		want[object] = true
	}
	return want[tenant] && want[session], nil
}
func isNil(expr ast.Expr) bool {
	identifier, ok := unwrap(expr).(*ast.Ident)
	return ok && identifier.Name == "nil"
}
func assignmentCall(source *typedSource, function *ast.FuncDecl, statement ast.Stmt) (types.Object, error) {
	assignment, ok := statement.(*ast.AssignStmt)
	if !ok || assignment.Tok != token.DEFINE || len(assignment.Lhs) != 1 || len(assignment.Rhs) != 1 {
		return nil, fmt.Errorf("parser first statement is not one declaration")
	}
	name, ok := assignment.Lhs[0].(*ast.Ident)
	if !ok {
		return nil, fmt.Errorf("parser match target is not an identifier")
	}
	call, ok := unwrap(assignment.Rhs[0]).(*ast.CallExpr)
	if !ok || len(call.Args) != 1 {
		return nil, fmt.Errorf("parser match source is not one call")
	}
	selector, ok := unwrap(call.Fun).(*ast.SelectorExpr)
	if !ok {
		return nil, fmt.Errorf("parser match call is not a method")
	}
	selection := source.info.Selections[selector]
	if selection == nil || selection.Obj().Pkg() == nil || selection.Obj().Pkg().Path() != "regexp" || selection.Obj().Name() != "FindStringSubmatch" {
		return nil, fmt.Errorf("parser does not call FindStringSubmatch")
	}
	receiver, ok := unwrap(selector.X).(*ast.Ident)
	if !ok || source.info.Uses[receiver] != source.pkg.Scope().Lookup("sessionChannelPattern") {
		return nil, fmt.Errorf("FindStringSubmatch receiver is not sessionChannelPattern")
	}
	if !identifies(source, call.Args[0], functionParameter(source, function, 0)) {
		return nil, fmt.Errorf("FindStringSubmatch argument is not channel")
	}
	return source.info.Defs[name], nil
}
func assignmentCapture(source *typedSource, statement ast.Stmt, typeName string, matches types.Object, index int) (types.Object, error) {
	assignment, ok := statement.(*ast.AssignStmt)
	if !ok || assignment.Tok != token.DEFINE || len(assignment.Lhs) != 1 || len(assignment.Rhs) != 1 {
		return nil, fmt.Errorf("%s capture is not one declaration", typeName)
	}
	name, ok := assignment.Lhs[0].(*ast.Ident)
	if !ok {
		return nil, fmt.Errorf("%s target is not identifier", typeName)
	}
	call, ok := unwrap(assignment.Rhs[0]).(*ast.CallExpr)
	if !ok || len(call.Args) != 1 {
		return nil, fmt.Errorf("%s capture is not a conversion", typeName)
	}
	named, ok := source.info.Types[call.Fun].Type.(*types.Named)
	if !ok || named.Obj().Pkg() == nil || named.Obj().Pkg().Path() != "github.com/looprig/core/sessionwire/v1" || named.Obj().Name() != typeName {
		return nil, fmt.Errorf("capture does not convert to %s", typeName)
	}
	subscript, ok := unwrap(call.Args[0]).(*ast.IndexExpr)
	if !ok || !identifies(source, subscript.X, matches) {
		return nil, fmt.Errorf("%s does not read matches", typeName)
	}
	literal, ok := unwrap(subscript.Index).(*ast.BasicLit)
	if !ok || literal.Value != fmt.Sprint(index) {
		return nil, fmt.Errorf("%s reads wrong capture", typeName)
	}
	return source.info.Defs[name], nil
}
func isLenNotThree(source *typedSource, expr ast.Expr, matches types.Object) bool {
	binary, ok := unwrap(expr).(*ast.BinaryExpr)
	if !ok || binary.Op != token.NEQ {
		return false
	}
	call, ok := unwrap(binary.X).(*ast.CallExpr)
	if !ok || len(call.Args) != 1 {
		return false
	}
	identifier, ok := unwrap(call.Fun).(*ast.Ident)
	if !ok || identifier.Name != "len" || !identifies(source, call.Args[0], matches) {
		return false
	}
	literal, ok := unwrap(binary.Y).(*ast.BasicLit)
	return ok && literal.Value == "3"
}
func isEmptyParsedReturn(source *typedSource, statement ast.Stmt) bool {
	returnStatement, ok := statement.(*ast.ReturnStmt)
	if !ok || len(returnStatement.Results) != 1 {
		return false
	}
	composite, ok := unwrap(returnStatement.Results[0]).(*ast.CompositeLit)
	return ok && len(composite.Elts) == 0 && source.info.TypeOf(composite) == source.pkg.Scope().Lookup("parsedSessionChannel").Type()
}
func identifies(source *typedSource, expr ast.Expr, object types.Object) bool {
	identifier, ok := unwrap(expr).(*ast.Ident)
	return ok && (source.info.Uses[identifier] == object || source.info.Defs[identifier] == object)
}
func functionParameter(source *typedSource, function *ast.FuncDecl, index int) types.Object {
	seen := 0
	for _, field := range function.Type.Params.List {
		for _, name := range field.Names {
			if seen == index {
				return source.info.Defs[name]
			}
			seen++
		}
	}
	return nil
}
func objectInitializer(source *typedSource, want types.Object) (ast.Expr, error) {
	for _, file := range source.files {
		for _, decl := range file.Decls {
			general, ok := decl.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, spec := range general.Specs {
				value, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for index, name := range value.Names {
					if source.info.Defs[name] == want && len(value.Values) == len(value.Names) {
						return value.Values[index], nil
					}
				}
			}
		}
	}
	return nil, fmt.Errorf("initializer for %s not found", want.Name())
}
func (source *typedSource) function(name string) (*ast.FuncDecl, error) {
	var expected types.Object
	receiverTypes := map[string]string{
		"AuthorizeSubscribe":    "Authorizer",
		"AuthorizeServiceSweep": "Authorizer",
		"Tenant":                "Principal",
		"IsService":             "Principal",
	}
	if receiverName := receiverTypes[name]; receiverName != "" {
		receiver, ok := source.pkg.Scope().Lookup(receiverName).(*types.TypeName)
		if !ok {
			return nil, fmt.Errorf("receiver type %s not found", receiverName)
		}
		expected, _, _ = types.LookupFieldOrMethod(receiver.Type(), true, source.pkg, name)
	} else {
		expected = source.pkg.Scope().Lookup(name)
	}
	if expected == nil {
		return nil, fmt.Errorf("typed function %s not found", name)
	}
	var found *ast.FuncDecl
	for _, file := range source.files {
		for _, decl := range file.Decls {
			function, ok := decl.(*ast.FuncDecl)
			if !ok || source.info.Defs[function.Name] != expected {
				continue
			}
			if found != nil {
				return nil, fmt.Errorf("multiple functions named %s", name)
			}
			found = function
		}
	}
	if found == nil {
		return nil, fmt.Errorf("function %s not found", name)
	}
	return found, nil
}

func typeSource(path, text string, sourceImporter types.Importer) (*typedSource, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path+".go", text, 0)
	if err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
	config := types.Config{Importer: sourceImporter, DisableUnusedImportCheck: true}
	pkg, err := config.Check(path, fset, []*ast.File{file}, info)
	if err != nil {
		return nil, fmt.Errorf("type-check: %w", err)
	}
	return &typedSource{pkg: pkg, files: []*ast.File{file}, info: info}, nil
}
func readProductionPackage(t *testing.T, directory, path string, sourceImporter types.Importer, include func(ast.Decl) bool) *typedSource {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	var source strings.Builder
	source.WriteString("package identity\n")
	imports := map[string]string{}
	var declarations []string
	files := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		files++
		data, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, entry.Name(), data, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, spec := range file.Imports {
			value := spec.Path.Value
			if spec.Name != nil {
				value = spec.Name.Name + " " + value
			}
			imports[spec.Path.Value] = value
		}
		for _, decl := range file.Decls {
			if include(decl) {
				declarations = append(declarations, string(data[fset.Position(decl.Pos()).Offset:fset.Position(decl.End()).Offset]))
			}
		}
	}
	if files < 2 || len(declarations) < 3 {
		t.Fatalf("anti-vacuity: files=%d declarations=%d", files, len(declarations))
	}
	if len(imports) > 0 {
		source.WriteString("import (\n")
		keys := make([]string, 0, len(imports))
		for key := range imports {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			source.WriteString(imports[key] + "\n")
		}
		source.WriteString(")\n")
	}
	for _, decl := range declarations {
		source.WriteString(decl + "\n")
	}
	typed, err := typeSource(path, source.String(), sourceImporter)
	if err != nil {
		t.Fatal(err)
	}
	return typed
}
func declarationName(decl ast.Decl) string {
	switch decl := decl.(type) {
	case *ast.FuncDecl:
		return decl.Name.Name
	case *ast.GenDecl:
		for _, spec := range decl.Specs {
			switch spec := spec.(type) {
			case *ast.TypeSpec:
				return spec.Name.Name
			case *ast.ValueSpec:
				if len(spec.Names) > 0 {
					return spec.Names[0].Name
				}
			}
		}
	}
	return ""
}

func declarationHasName(decl ast.Decl, want string) bool {
	if function, ok := decl.(*ast.FuncDecl); ok {
		return function.Name.Name == want
	}
	general, ok := decl.(*ast.GenDecl)
	if !ok {
		return false
	}
	for _, spec := range general.Specs {
		switch spec := spec.(type) {
		case *ast.TypeSpec:
			if spec.Name.Name == want {
				return true
			}
		case *ast.ValueSpec:
			for _, name := range spec.Names {
				if name.Name == want {
					return true
				}
			}
		}
	}
	return false
}
func productionSources(t *testing.T) (string, string) {
	t.Helper()
	internal, err := os.ReadFile("authorize.go")
	if err != nil {
		t.Fatal(err)
	}
	public, err := os.ReadFile(filepath.Join("..", "..", "identity", "principal.go"))
	if err != nil {
		t.Fatal(err)
	}
	return string(internal), string(public)
}

// replaceOnce builds an analyzer fixture by rewriting production source that the
// caller QUOTES VERBATIM. Requiring exactly one occurrence is deliberate: a
// fixture that silently stopped matching would leave its test asserting against
// unmutated source and passing vacuously.
//
// The consequence is that a source-text change to a quoted line -- including a
// benign rename of a local in authorize.go -- is EXPECTED to fail here, with
// `fixture occurrence of "..." = 0, want 1`, which names neither the cause nor
// the fix. The fix is to update the quoted text in the table above to match the
// new source. It is never to loosen the match to a substring or to a count of at
// least one.
func replaceOnce(t *testing.T, source, old, replacement string) string {
	t.Helper()
	if strings.Count(source, old) != 1 {
		t.Fatalf("fixture occurrence of %q = %d, want 1", old, strings.Count(source, old))
	}
	return strings.Replace(source, old, replacement, 1)
}

type auditImporter struct {
	standard types.Importer
	packages map[string]*types.Package
}

func newAuditImporter() *auditImporter {
	result := &auditImporter{standard: importer.Default(), packages: map[string]*types.Package{}}
	wire := types.NewPackage("github.com/looprig/core/sessionwire/v1", "sessionwire")
	tenant := namedString(wire, "TenantID")
	validator(tenant)
	session := namedString(wire, "SessionID")
	validator(session)
	object := types.NewTypeName(token.NoPos, wire, "ObjectReference", nil)
	types.NewNamed(object, types.NewStruct(nil, nil), nil)
	wire.Scope().Insert(object)
	wire.Scope().Insert(types.NewConst(token.NoPos, wire, "MaxIDBytes", types.Typ[types.UntypedInt], constant.MakeInt64(256)))
	wire.MarkComplete()
	result.packages[wire.Path()] = wire
	store := types.NewPackage("github.com/looprig/sessionstore", "sessionstore")
	namedString(store, "CommandKind")
	store.MarkComplete()
	result.packages[store.Path()] = store
	return result
}
func (i *auditImporter) Import(path string) (*types.Package, error) {
	if pkg := i.packages[path]; pkg != nil {
		return pkg, nil
	}
	pkg, err := i.standard.Import(path)
	if err != nil {
		return nil, fmt.Errorf("import %s: %w", path, err)
	}
	return pkg, nil
}
func namedString(pkg *types.Package, name string) *types.Named {
	object := types.NewTypeName(token.NoPos, pkg, name, nil)
	named := types.NewNamed(object, types.Typ[types.String], nil)
	pkg.Scope().Insert(object)
	return named
}
func validator(receiver *types.Named) {
	variable := types.NewVar(token.NoPos, receiver.Obj().Pkg(), "", receiver)
	signature := types.NewSignatureType(variable, nil, nil, types.NewTuple(), types.NewTuple(types.NewVar(token.NoPos, nil, "", types.Universe.Lookup("error").Type())), false)
	receiver.AddMethod(types.NewFunc(token.NoPos, receiver.Obj().Pkg(), "Validate", signature))
}
