package factory_test

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/looprig/factory"
	"github.com/looprig/factory/internal/admission"
	"github.com/looprig/factory/internal/httpapi"
	internalidentity "github.com/looprig/factory/internal/identity"
	"github.com/looprig/factory/internal/realtime/clientlink"
	"github.com/looprig/factory/internal/realtime/hostlink"
)

func iface[T any]() reflect.Type { return reflect.TypeOf((*T)(nil)).Elem() }

// publicSeams pairs each seam on Factory's option surface with the narrow
// interfaces the packages that call it declare for themselves.
func publicSeams() map[reflect.Type][]reflect.Type {
	return map[reflect.Type][]reflect.Type{
		iface[factory.Authenticator]():       {iface[httpapi.Authenticator](), iface[clientlink.Authenticator]()},
		iface[factory.Authorizer]():          {iface[httpapi.Authorizer](), iface[clientlink.Authorizer](), iface[admission.Authorizer]()},
		iface[factory.SessionReader]():       {iface[httpapi.SessionReader]()},
		iface[factory.Commands]():            {iface[admission.Commands]()},
		iface[factory.Directory]():           {iface[admission.Directory]()},
		iface[factory.PlacementController](): {iface[admission.PlacementController]()},
		// internal/identity declares a Clock with only Now, because expiry is
		// the only time it reads. Pairing it here is what keeps the union
		// assertion honest: a narrower consumer must still be satisfied by the
		// one object a deployer supplies.
		iface[factory.Clock]():      {iface[admission.Clock](), iface[internalidentity.Clock]()},
		iface[factory.UUIDSource](): {iface[admission.UUIDSource]()},
	}
}

// TestPublicSeamsSatisfyTheirConsumers is what makes the narrow interfaces
// load-bearing rather than decorative. A consumer may widen what it needs;
// when it does, this fails until the public seam a deployer implements is
// widened with it, so the two cannot drift into needing an adapter.
func TestPublicSeamsSatisfyTheirConsumers(t *testing.T) {
	t.Parallel()

	seams := publicSeams()
	if len(seams) == 0 {
		t.Fatal("no seams are paired, so this test is vacuous")
	}
	for public, consumers := range seams {
		if public.NumMethod() == 0 {
			t.Errorf("%s has no methods, so satisfying it proves nothing", public)
		}
		for _, consumer := range consumers {
			if consumer.NumMethod() == 0 {
				t.Errorf("%s has no methods, so satisfying it proves nothing", consumer)
				continue
			}
			if !public.Implements(consumer) {
				t.Errorf("%s does not satisfy %s, so the composition root cannot pass one to the other", public, consumer)
			}
		}
	}
}

// TestPublicSeamsAreExactlyTheUnionOfTheirConsumers is the other direction: a
// public seam wider than its consumers requires a deployer to implement a
// method nothing calls.
func TestPublicSeamsAreExactlyTheUnionOfTheirConsumers(t *testing.T) {
	t.Parallel()

	for public, consumers := range publicSeams() {
		want := map[string]bool{}
		for _, consumer := range consumers {
			for i := range consumer.NumMethod() {
				want[consumer.Method(i).Name] = true
			}
		}
		got := map[string]bool{}
		for i := range public.NumMethod() {
			got[public.Method(i).Name] = true
		}
		for name := range got {
			if !want[name] {
				t.Errorf("%s requires %s, which no consumer declares", public, name)
			}
		}
		for name := range want {
			if !got[name] {
				t.Errorf("%s is missing %s, which a consumer declares", public, name)
			}
		}
	}
}

// allSeams is every interface this task defines, public and internal.
func allSeams() []reflect.Type {
	return []reflect.Type{
		iface[factory.Authenticator](),
		iface[factory.Authorizer](),
		iface[factory.SessionReader](),
		iface[factory.Commands](),
		iface[factory.Directory](),
		iface[factory.PlacementController](),
		iface[factory.Clock](),
		iface[factory.UUIDSource](),
		iface[httpapi.Authenticator](),
		iface[httpapi.Authorizer](),
		iface[httpapi.SessionReader](),
		iface[clientlink.Authenticator](),
		iface[clientlink.Authorizer](),
		iface[admission.Authorizer](),
		iface[admission.Commands](),
		iface[admission.Directory](),
		iface[admission.PlacementController](),
		iface[admission.Clock](),
		iface[admission.UUIDSource](),
		iface[internalidentity.Clock](),
		iface[internalidentity.Verifier](),
		iface[hostlink.Dialer](),
		iface[hostlink.Link](),
	}
}

// TestNoSeamNamesAStoragePrimitive is the "SessionStore domain interface, not
// raw Storage" requirement, made checkable. A handler that could name a
// storage.Ledger, Leaser, KV or Blobs value could re-derive a record the
// aggregate owns, with none of the aggregate's epoch fencing.
func TestNoSeamNamesAStoragePrimitive(t *testing.T) {
	t.Parallel()

	const storageModule = "github.com/looprig/storage"
	seams := allSeams()
	if len(seams) == 0 {
		t.Fatal("no seams are examined, so this test is vacuous")
	}
	for _, seam := range seams {
		if seam.NumMethod() == 0 {
			t.Errorf("%s has no methods, so examining it proves nothing", seam)
		}
		for _, named := range namedTypesIn(seam) {
			if pathHasModulePrefix(named.PkgPath(), storageModule) {
				t.Errorf("%s names %s from %q, which is a Storage primitive", seam, named, named.PkgPath())
			}
		}
	}
}

// TestPublicSeamsNameNoInternalType is the constraint that decides WHERE the
// vocabulary lives: a deployer outside this module writes the method
// signatures, and Go forbids it importing github.com/looprig/factory/internal.
// A seam naming an internal type compiles here and is unimplementable there.
func TestPublicSeamsNameNoInternalType(t *testing.T) {
	t.Parallel()

	for public := range publicSeams() {
		for _, named := range namedTypesIn(public) {
			if slices.Contains(strings.Split(named.PkgPath(), "/"), "internal") {
				t.Errorf("%s names %s from %q, which no package outside this module may import",
					public, named, named.PkgPath())
			}
		}
	}
}

// These probes exist so the two assertions above cannot pass because
// namedTypesIn returns nothing. They are driven against the detectors, not
// against the seams, and each hides the offending type one layer deeper than
// the last -- directly, behind an anonymous interface, and behind an anonymous
// struct -- because an unnamed composite is a hiding place rather than a leaf.
type internalNamingSeam interface {
	Reject(httpapi.Authenticator) error
}

type internalBehindAnInterface interface {
	Reject(interface {
		Auth(httpapi.Authenticator) error
	}) error
}

type internalBehindAStruct interface {
	Reject(struct{ A httpapi.Authenticator }) error
}

func TestNamedTypesInFindsTheTypesTheAssertionsLookFor(t *testing.T) {
	t.Parallel()

	for _, probe := range []reflect.Type{
		iface[internalNamingSeam](),
		iface[internalBehindAnInterface](),
		iface[internalBehindAStruct](),
	} {
		names := namedTypesIn(probe)
		if !slices.ContainsFunc(names, func(typ reflect.Type) bool {
			return slices.Contains(strings.Split(typ.PkgPath(), "/"), "internal")
		}) {
			t.Errorf("namedTypesIn did not find the internal type in %s: %v", probe, names)
		}
	}

	// The Storage assertion's detector is the same walker with a different
	// module prefix, so it is exercised against a module the seams DO name.
	readerNames := namedTypesIn(iface[factory.SessionReader]())
	if !slices.ContainsFunc(readerNames, func(typ reflect.Type) bool {
		return pathHasModulePrefix(typ.PkgPath(), "github.com/looprig/sessionstore")
	}) {
		t.Errorf("namedTypesIn did not find a SessionStore type in factory.SessionReader: %v", readerNames)
	}
	if !slices.ContainsFunc(readerNames, func(typ reflect.Type) bool { return typ.PkgPath() == "io" }) {
		t.Errorf("namedTypesIn did not reach the io.ReadCloser behind a result: %v", readerNames)
	}
}

func TestPathHasModulePrefixComparesSegments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		path, prefix string
		want         bool
	}{
		{"github.com/looprig/storage", "github.com/looprig/storage", true},
		{"github.com/looprig/storage/internal/x", "github.com/looprig/storage", true},
		{"github.com/looprig/storagekit", "github.com/looprig/storage", false},
		{"github.com/looprig/sessionstore", "github.com/looprig/storage", false},
		{"", "github.com/looprig/storage", false},
	}
	for _, tt := range tests {
		if got := pathHasModulePrefix(tt.path, tt.prefix); got != tt.want {
			t.Errorf("pathHasModulePrefix(%q, %q) = %t, want %t", tt.path, tt.prefix, got, tt.want)
		}
	}
}

func pathHasModulePrefix(path, prefix string) bool {
	return path == prefix || strings.HasPrefix(path, prefix+"/")
}

// namedTypesIn returns every NAMED type reachable from the signatures of an
// interface's methods. It stops at a named type rather than descending into its
// fields: what matters is which package a signature obliges an implementer to
// import, not what that package's types are made of.
func namedTypesIn(iface reflect.Type) []reflect.Type {
	var out []reflect.Type
	seen := map[reflect.Type]bool{}
	for i := range iface.NumMethod() {
		collectNamedTypes(iface.Method(i).Type, &out, seen)
	}
	return out
}

func collectNamedTypes(typ reflect.Type, out *[]reflect.Type, seen map[reflect.Type]bool) {
	if typ == nil || seen[typ] {
		return
	}
	seen[typ] = true
	if typ.PkgPath() != "" {
		*out = append(*out, typ)
		return
	}
	switch typ.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Array, reflect.Chan:
		collectNamedTypes(typ.Elem(), out, seen)
	case reflect.Map:
		collectNamedTypes(typ.Key(), out, seen)
		collectNamedTypes(typ.Elem(), out, seen)
	case reflect.Func:
		for i := range typ.NumIn() {
			collectNamedTypes(typ.In(i), out, seen)
		}
		for i := range typ.NumOut() {
			collectNamedTypes(typ.Out(i), out, seen)
		}
	// An ANONYMOUS interface or struct is a named type's hiding place, not a
	// leaf: `interface{ Auth(httpapi.Authenticator) error }` and
	// `struct{ A httpapi.Authenticator }` are both spellable in a seam
	// signature, and before these two arms the walker returned nothing for
	// either, so both assertions above passed on a seam that did name the type
	// they exist to forbid.
	case reflect.Interface:
		for i := range typ.NumMethod() {
			collectNamedTypes(typ.Method(i).Type, out, seen)
		}
	case reflect.Struct:
		for i := range typ.NumField() {
			collectNamedTypes(typ.Field(i).Type, out, seen)
		}
	}
}
