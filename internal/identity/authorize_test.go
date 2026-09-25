package identity_test

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"path"
	"reflect"
	"strconv"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory"
	factoryidentity "github.com/looprig/factory/identity"
	"github.com/looprig/factory/internal/admission"
	"github.com/looprig/factory/internal/httpapi"
	internalidentity "github.com/looprig/factory/internal/identity"
	"github.com/looprig/factory/internal/realtime/clientlink"
	"github.com/looprig/sessionstore"
)

// These compile-time characterizations pin the concrete policy to all four
// authorization seams established in A0.2. They were added after the policy's
// behavioural RED/GREEN cycle; their purpose is compatibility, not a claim of
// test-first behavioural coverage.
var (
	_ factory.Authorizer    = internalidentity.Authorizer{}
	_ httpapi.Authorizer    = internalidentity.Authorizer{}
	_ clientlink.Authorizer = internalidentity.Authorizer{}
	_ admission.Authorizer  = internalidentity.Authorizer{}
)

func TestInternalUnauthorizedIsThePublicSentinel(t *testing.T) {
	t.Parallel()

	if internalidentity.ErrUnauthorized != factoryidentity.ErrUnauthorized {
		t.Fatal("internal ErrUnauthorized is not the public identity.ErrUnauthorized value")
	}
}

// TestAuthorizerOpaqueSeamParametersAreUnread derives both sides of the rule:
// the public A0.2 seam supplies the parameter types, and the package directory
// supplies every production file and concrete Authorizer method. Parameters
// other than context, principal, and AuthorizeSubscribe's channel are opaque
// tenant-local operation values. They must remain blank, so no particular
// SessionID, ObjectReference, CommandKind, or future seam value can grant
// authority independently of the principal.
func TestAuthorizerOpaqueSeamParametersAreUnread(t *testing.T) {
	t.Parallel()

	seam, opaqueTypes, channelParameters := authorizerSeam(t)
	files := productionFiles(t)
	if len(files) < 2 {
		t.Fatalf("found %d production files, want at least 2 so the scan cannot pass over an empty or pinned file set", len(files))
	}
	if len(seam) < 6 {
		t.Fatalf("factory.Authorizer has %d methods, want at least the six A0.2 operation classes", len(seam))
	}
	if len(opaqueTypes) < 3 {
		t.Fatalf("classified %d opaque seam types, want at least SessionID, ObjectReference, and CommandKind", len(opaqueTypes))
	}
	for _, required := range []reflect.Type{
		reflect.TypeOf(sessionwire.SessionID("")),
		reflect.TypeOf(sessionwire.ObjectReference{}),
		reflect.TypeOf(sessionstore.CommandKind("")),
	} {
		if typ := qualifiedReflectType(required); !opaqueTypes[typ] {
			t.Errorf("derived opaque seam types omit %s", typ)
		}
	}
	if channelParameters != 1 {
		t.Fatalf("classified %d channel parameters, want exactly AuthorizeSubscribe's channel", channelParameters)
	}

	methods := make(map[string]string)
	opaqueParameters := 0
	for _, filename := range files {
		parsed, err := parser.ParseFile(token.NewFileSet(), filename, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", filename, err)
		}
		imports := resolvedImports(t, filename, parsed)
		for _, declaration := range parsed.Decls {
			method, ok := declaration.(*ast.FuncDecl)
			if !ok || !authorizerReceiver(method.Recv) {
				continue
			}
			if previous, exists := methods[method.Name.Name]; exists {
				t.Errorf("Authorizer.%s is declared in both %s and %s", method.Name.Name, previous, filename)
			}
			methods[method.Name.Name] = filename

			parameters := resolvedParameters(t, filename, method, imports)
			for index, parameter := range parameters {
				if method.Name.Name == "AuthorizeSubscribe" && parameter.typ == stringType && parameter.name == "channel" {
					continue
				}
				if opaqueTypes[parameter.typ] {
					opaqueParameters++
					if parameter.name != "" && parameter.name != "_" {
						t.Errorf("%s: Authorizer.%s opaque parameter %d (%s) is named %q; keep it blank so it cannot grant authority",
							filename, method.Name.Name, index, parameter.typ, parameter.name)
					}
					continue
				}
				if parameter.typ == contextType || parameter.typ == principalType {
					continue
				}
				t.Errorf("%s: Authorizer.%s parameter %d (%s %q) is neither a derived opaque seam type nor the context/principal/channel exception",
					filename, method.Name.Name, index, parameter.typ, parameter.name)
			}
		}
	}
	if len(methods) < len(seam) {
		t.Fatalf("found %d concrete Authorizer methods across %d files, want at least the %d seam methods", len(methods), len(files), len(seam))
	}
	if opaqueParameters < 5 {
		t.Fatalf("classified %d concrete opaque parameters, want at least the five currently present", opaqueParameters)
	}
	for name := range seam {
		if _, ok := methods[name]; !ok {
			t.Errorf("factory.Authorizer.%s has no concrete internal/identity.Authorizer method", name)
		}
	}
	for name, filename := range methods {
		if ast.IsExported(name) {
			if _, ok := seam[name]; !ok {
				t.Errorf("%s: exported Authorizer.%s is absent from the factory.Authorizer seam", filename, name)
			}
		}
	}
}

type qualifiedType struct {
	pkg  string
	name string
}

func (typ qualifiedType) String() string {
	if typ.pkg == "" {
		return typ.name
	}
	return typ.pkg + "." + typ.name
}

var (
	contextType   = qualifiedType{pkg: "context", name: "Context"}
	principalType = qualifiedType{pkg: "github.com/looprig/factory/identity", name: "Principal"}
	stringType    = qualifiedType{name: "string"}
)

func authorizerSeam(t *testing.T) (map[string]struct{}, map[qualifiedType]bool, int) {
	t.Helper()
	// The built-in Authorizer implements the required seam and the optional
	// audit seam. Both must obey the same opaque-parameter rule without making
	// audit mandatory for third-party authorizers.
	seams := []reflect.Type{reflect.TypeOf((*factory.Authorizer)(nil)).Elem(), reflect.TypeOf((*factory.AuditAuthorizer)(nil)).Elem()}
	methods := make(map[string]struct{})
	opaque := make(map[qualifiedType]bool)
	channels := 0
	for _, typeOf := range seams {
		for index := range typeOf.NumMethod() {
			method := typeOf.Method(index)
			methods[method.Name] = struct{}{}
			for parameter := range method.Type.NumIn() {
				typ := qualifiedReflectType(method.Type.In(parameter))
				switch {
				case typ == contextType, typ == principalType:
				case method.Name == "AuthorizeSubscribe" && typ == stringType:
					channels++
				default:
					opaque[typ] = true
				}
			}
		}
	}
	return methods, opaque, channels
}

func qualifiedReflectType(typ reflect.Type) qualifiedType {
	return qualifiedType{pkg: typ.PkgPath(), name: typ.Name()}
}

type resolvedParameter struct {
	name string
	typ  qualifiedType
}

func resolvedParameters(t *testing.T, filename string, method *ast.FuncDecl, imports map[string]string) []resolvedParameter {
	t.Helper()
	var parameters []resolvedParameter
	for _, field := range method.Type.Params.List {
		typ, ok := resolveSyntacticType(field.Type, imports)
		if !ok {
			t.Fatalf("%s: cannot resolve Authorizer.%s parameter type %T", filename, method.Name.Name, field.Type)
		}
		if len(field.Names) == 0 {
			parameters = append(parameters, resolvedParameter{typ: typ})
			continue
		}
		for _, name := range field.Names {
			parameters = append(parameters, resolvedParameter{name: name.Name, typ: typ})
		}
	}
	return parameters
}

func resolveSyntacticType(expression ast.Expr, imports map[string]string) (qualifiedType, bool) {
	switch expression := expression.(type) {
	case *ast.Ident:
		return qualifiedType{name: expression.Name}, true
	case *ast.SelectorExpr:
		qualifier, ok := expression.X.(*ast.Ident)
		if !ok {
			return qualifiedType{}, false
		}
		pkg, ok := imports[qualifier.Name]
		return qualifiedType{pkg: pkg, name: expression.Sel.Name}, ok
	default:
		return qualifiedType{}, false
	}
}

func resolvedImports(t *testing.T, filename string, file *ast.File) map[string]string {
	t.Helper()
	imports := make(map[string]string, len(file.Imports))
	for _, spec := range file.Imports {
		pkg, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			t.Fatalf("%s: unquote import %s: %v", filename, spec.Path.Value, err)
		}
		name := path.Base(pkg)
		if spec.Name != nil {
			name = spec.Name.Name
		}
		imports[name] = pkg
	}
	return imports
}

func authorizerReceiver(receivers *ast.FieldList) bool {
	if receivers == nil || len(receivers.List) != 1 {
		return false
	}
	typ := receivers.List[0].Type
	if pointer, ok := typ.(*ast.StarExpr); ok {
		typ = pointer.X
	}
	name, ok := typ.(*ast.Ident)
	return ok && name.Name == "Authorizer"
}

func TestAuthorizerAllowsEveryTenantOperation(t *testing.T) {
	t.Parallel()

	authorizer := internalidentity.Authorizer{}
	principal := actor(t, "tenant-a")
	ctx := context.Background()
	session := sessionwire.SessionID("shared-session")
	object := sessionwire.ObjectReference{ObjectID: "shared-object"}

	// The rows enumerate the OPERATIONS A1.2 names, not the methods that answer
	// them, so "session read", "journal" and "gates" are three rows over one
	// identical AuthorizeSessionRead call and are not three seams' worth of
	// coverage. That they collapse to one call is the seam's design, not an
	// oversight: httpapi.Authorizer.AuthorizeSessionRead is documented in
	// internal/httpapi/deps.go as covering status, journal and gate reads.
	tests := []struct {
		name      string
		authorize func() error
	}{
		{"list", func() error { return authorizer.AuthorizeSessionList(ctx, principal) }},
		{"session read", func() error { return authorizer.AuthorizeSessionRead(ctx, principal, session) }},
		{"journal", func() error { return authorizer.AuthorizeSessionRead(ctx, principal, session) }},
		{"gates", func() error { return authorizer.AuthorizeSessionRead(ctx, principal, session) }},
		{"object", func() error { return authorizer.AuthorizeObjectRead(ctx, principal, session, object) }},
		{"subscribe", func() error { return authorizer.AuthorizeSubscribe(ctx, principal, "session:tenant-a:shared-session") }},
		{"create", func() error { return authorizer.AuthorizeControl(ctx, principal, session, "create") }},
		{"input", func() error { return authorizer.AuthorizeControl(ctx, principal, session, "input") }},
		{"interrupt", func() error { return authorizer.AuthorizeControl(ctx, principal, session, "interrupt") }},
		{"restore", func() error { return authorizer.AuthorizeControl(ctx, principal, session, "restore") }},
		{"gate response", func() error { return authorizer.AuthorizeControl(ctx, principal, session, "gate_response") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if err := tt.authorize(); err != nil {
				t.Fatalf("authorize = %v, want nil", err)
			}
		})
	}
}

// TestOpaqueOperationValuesNeverGrantAnUnconstructedPrincipal is the concrete
// control for the structural test above. This finite cross-product catches
// recognizable special cases; it does not establish the universal property.
func TestOpaqueOperationValuesNeverGrantAnUnconstructedPrincipal(t *testing.T) {
	t.Parallel()

	authorizer := internalidentity.Authorizer{}
	ctx := context.Background()
	principals := []struct {
		name    string
		value   factoryidentity.Principal
		allowed bool
	}{
		{"tenant-a actor", actor(t, "tenant-a"), true},
		{"tenant-b actor", actor(t, "tenant-b"), true},
		{"unconstructed", factoryidentity.Principal{}, false},
	}
	sessions := []sessionwire.SessionID{"shared-session", "session-a", "session-b"}
	objects := []sessionwire.ObjectReference{
		{ObjectID: "shared-object"},
		{ObjectID: "object-a"},
		{ObjectID: "object-b"},
	}
	controlKinds := []sessionstore.CommandKind{"create", "input", "interrupt", "restore", "gate_response"}

	// AuthorizeControl has no CommandID parameter, so this test does not pretend
	// to cover one. Same-CommandID tenant isolation belongs to admission in A3;
	// this seam receives only the principal, tenant-local session and operation.
	for _, session := range sessions {
		for _, principal := range principals {
			t.Run("session_read/"+string(session)+"/"+principal.name, func(t *testing.T) {
				assertAuthorization(t, authorizer.AuthorizeSessionRead(ctx, principal.value, session), principal.allowed)
			})
		}
		for _, object := range objects {
			for _, principal := range principals {
				t.Run("object_read/"+string(session)+"/"+object.ObjectID+"/"+principal.name, func(t *testing.T) {
					assertAuthorization(t, authorizer.AuthorizeObjectRead(ctx, principal.value, session, object), principal.allowed)
				})
			}
		}
		for _, kind := range controlKinds {
			for _, principal := range principals {
				t.Run("control/"+string(session)+"/"+string(kind)+"/"+principal.name, func(t *testing.T) {
					assertAuthorization(t, authorizer.AuthorizeControl(ctx, principal.value, session, kind), principal.allowed)
				})
			}
		}
	}
}

func TestAuthorizerRejectsAnUnconstructedPrincipalForOperationsWithoutOpaqueValues(t *testing.T) {
	t.Parallel()

	authorizer := internalidentity.Authorizer{}
	principal := factoryidentity.Principal{}
	ctx := context.Background()

	tests := []struct {
		name      string
		authorize func() error
	}{
		{"list", func() error { return authorizer.AuthorizeSessionList(ctx, principal) }},
		{"subscribe", func() error { return authorizer.AuthorizeSubscribe(ctx, principal, "session:tenant-a:session-a") }},
		{"service sweep", func() error { return authorizer.AuthorizeServiceSweep(ctx, principal) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.authorize(); !errors.Is(err, internalidentity.ErrUnauthorized) {
				t.Fatalf("authorize = %v, want ErrUnauthorized", err)
			}
		})
	}
}

func TestSubscribeParsesBeforeComparingAndParsingDoesNotGrantAccess(t *testing.T) {
	t.Parallel()

	authorizer := internalidentity.Authorizer{}
	principal := actor(t, "tenant-a")
	ctx := context.Background()

	tests := []struct {
		name    string
		channel string
		wantErr bool
	}{
		{"matching", "session:tenant-a:session-a", false},
		{"other tenant", "session:tenant-b:session-a", true},
		{"other tenant absent session", "session:tenant-b:absent", true},
		{"missing prefix", "tenant-a:session-a", true},
		{"wrong prefix", "sessions:tenant-a:session-a", true},
		{"missing tenant", "session::session-a", true},
		{"missing session", "session:tenant-a:", true},
		{"extra segment", "session:tenant-a:session-a:extra", true},
		{"invalid tenant", "session:\xff:session-a", true},
		{"invalid session", "session:tenant-a:\xff", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := authorizer.AuthorizeSubscribe(ctx, principal, tt.channel)
			if tt.wantErr && !errors.Is(err, internalidentity.ErrUnauthorized) {
				t.Fatalf("AuthorizeSubscribe(%q) = %v, want ErrUnauthorized", tt.channel, err)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("AuthorizeSubscribe(%q) = %v, want nil", tt.channel, err)
			}
		})
	}
	if err := authorizer.AuthorizeSubscribe(ctx, actor(t, "tenant-b"), "session:tenant-a:session-a"); !errors.Is(err, internalidentity.ErrUnauthorized) {
		t.Fatalf("tenant-b subscribing to tenant-a = %v, want ErrUnauthorized", err)
	}
}

func TestCrossTenantDenialDoesNotDiscloseSessionExistence(t *testing.T) {
	t.Parallel()

	authorizer := internalidentity.Authorizer{}
	principal := actor(t, "tenant-a")
	ctx := context.Background()

	existing := authorizer.AuthorizeSubscribe(ctx, principal, "session:tenant-b:same-session")
	absent := authorizer.AuthorizeSubscribe(ctx, principal, "session:tenant-b:absent-session")
	if existing != internalidentity.ErrUnauthorized || absent != internalidentity.ErrUnauthorized {
		t.Fatalf("cross-tenant existing/absent errors = (%v, %v), want the identical ErrUnauthorized sentinel", existing, absent)
	}
}

func TestOnlyAServicePrincipalMayReconcileAcrossTenants(t *testing.T) {
	t.Parallel()

	authorizer := internalidentity.Authorizer{}
	ctx := context.Background()
	if err := authorizer.AuthorizeServiceSweep(ctx, service(t, "factory-control")); err != nil {
		t.Fatalf("service sweep = %v, want nil", err)
	}
	if err := authorizer.AuthorizeServiceSweep(ctx, actor(t, "tenant-a")); !errors.Is(err, internalidentity.ErrUnauthorized) {
		t.Fatalf("actor sweep = %v, want ErrUnauthorized", err)
	}
}

func actor(t *testing.T, tenant sessionwire.TenantID) factoryidentity.Principal {
	t.Helper()
	principal, err := factoryidentity.NewPrincipal(tenant, "user-a", factoryidentity.KindActor)
	if err != nil {
		t.Fatalf("NewPrincipal(actor) = %v", err)
	}
	return principal
}

func service(t *testing.T, tenant sessionwire.TenantID) factoryidentity.Principal {
	t.Helper()
	principal, err := factoryidentity.NewPrincipal(tenant, "factory-a", factoryidentity.KindService)
	if err != nil {
		t.Fatalf("NewPrincipal(service) = %v", err)
	}
	return principal
}

func assertAuthorization(t *testing.T, err error, allowed bool) {
	t.Helper()
	if allowed && err != nil {
		t.Fatalf("authorize = %v, want nil", err)
	}
	if !allowed && !errors.Is(err, internalidentity.ErrUnauthorized) {
		t.Fatalf("authorize = %v, want ErrUnauthorized", err)
	}
}
