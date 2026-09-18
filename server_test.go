package factory_test

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/looprig/factory"
	"github.com/looprig/factory/identity"
	"github.com/looprig/factory/internal/admission"
	"github.com/looprig/factory/internal/httpapi"
	internalidentity "github.com/looprig/factory/internal/identity"
	"github.com/looprig/factory/internal/placement"
	"github.com/looprig/factory/internal/realtime/clientlink"
	"github.com/looprig/factory/internal/realtime/hostlink"
	"github.com/looprig/factory/internal/reconcile"
	"github.com/looprig/factory/internal/routing"
)

func iface[T any]() reflect.Type { return reflect.TypeOf((*T)(nil)).Elem() }

// TestTheComposedAuthenticatorSatisfiesBothConsumers is what replaces the
// public Authenticator seam. The concrete authenticator Factory composes must
// satisfy every narrow interface that declares one, or the composition root
// would need an adapter -- and an adapter is a second answer to "which
// credential authenticated this request".
func TestTheComposedAuthenticatorSatisfiesBothConsumers(t *testing.T) {
	t.Parallel()

	composed := reflect.TypeOf((*internalidentity.Authenticator)(nil))
	for _, consumer := range []reflect.Type{iface[httpapi.Authenticator](), iface[clientlink.Authenticator]()} {
		if consumer.NumMethod() == 0 {
			t.Errorf("%s has no methods, so satisfying it proves nothing", consumer)
			continue
		}
		if !composed.Implements(consumer) {
			t.Errorf("%s does not satisfy %s", composed, consumer)
		}
	}
}

// publicSeams pairs each seam on Factory's option surface with the narrow
// interfaces the packages that call it declare for themselves.
//
// Authentication is deliberately absent. Factory composes ONE authenticator --
// internal/identity's, built from the deployer's verifier -- because the origin
// guard and the router require the single implementation that decides which
// credential authenticated a request. There is therefore no public
// Authenticator interface to pair, and
// TestTheComposedAuthenticatorSatisfiesBothConsumers holds the property this
// row used to hold.
func publicSeams() map[reflect.Type][]reflect.Type {
	return map[reflect.Type][]reflect.Type{
		iface[factory.Authorizer](): {iface[httpapi.Authorizer](), iface[clientlink.Authorizer](), iface[admission.Authorizer]()},
		// The read plane. routing.TipReader is paired here because the demand
		// plane reads the journal TIP through the same object: a second seam
		// for one method would be a second answer to which store a replica
		// reads.
		iface[factory.SessionReader](): {iface[httpapi.SessionReader](), iface[routing.TipReader]()},
		// Commands is paired with every consumer, and each names a different
		// part of the durable command plane: the admission service writes and
		// reads a DISPOSITION command (CommandStore), each deadline sweep --
		// legacy and disposition -- pages its own due view and settles, and
		// every reconciler claims. The union is what a deployer
		// supplies as one store, and pairing all four is what stops a later
		// task widening one of them without widening the seam.
		iface[factory.Commands](): {
			iface[admission.Commands](), iface[admission.DueCommands](),
			iface[admission.Settlement](), iface[admission.Claims](),
			iface[admission.CommandStore](),
			iface[admission.DispositionDue](), iface[admission.DispositionSettlement](),
			iface[placement.Claims](),
		},
		// The durable session record. GetCatalogEntry also appears on
		// SessionReader; that is one method on one object reached through two
		// seams, because each consumer declares the read beside a write the
		// read plane must not carry.
		iface[factory.Catalog](): {iface[admission.Catalog](), iface[placement.Catalog]()},
		// The service-control gate-deadline plane. Its two consumers are
		// deliberately separate interfaces in internal/reconcile -- the due
		// view and the ONE write -- so the union here is exactly "read what is
		// due, retire an intent" and carries no way to answer a gate.
		iface[factory.Gates]():              {iface[reconcile.DueGates](), iface[reconcile.GateIntents]()},
		iface[factory.HostTargets]():        {iface[placement.TargetSweep]()},
		iface[factory.HostLinkCredential](): {iface[hostlink.Credential]()},
		iface[factory.WorkloadController](): {iface[placement.WorkloadController]()},
		// B5's trigger: the disposition due view the pending sweep pages.
		iface[factory.PendingCommands](): {iface[placement.PendingCommands]()},
		iface[factory.Directory](): {
			iface[admission.Directory](), iface[httpapi.Directory](),
			iface[placement.Directory](), iface[routing.Resolver](),
		},
		// The store surface behind the exported directory. It is paired with
		// internal/routing's Store so the exported seam can neither be
		// narrower than what the directory reads nor wider than it.
		iface[factory.DirectoryStore]():      {iface[routing.Store]()},
		iface[factory.PlacementController](): {iface[admission.PlacementController]()},
		// internal/identity declares a Clock with only Now, because expiry is
		// the only time it reads. Pairing it here is what keeps the union
		// assertion honest: a narrower consumer must still be satisfied by the
		// one object a deployer supplies.
		// clientlink declares a Clock with only AfterFunc, because scheduling
		// the debounced demand release is the only time it reads. It is paired
		// here for internal/identity's reason: a narrower consumer must still
		// be satisfied by the one object a deployer supplies.
		iface[factory.Clock](): {
			iface[admission.Clock](), iface[internalidentity.Clock](),
			iface[placement.Clock](), iface[clientlink.Clock](),
			iface[reconcile.Clock](), iface[routing.Clock](),
		},
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
		iface[httpapi.Directory](),
		// The REST control routes' durable command plane and their best-effort
		// local delivery. They are here rather than in publicSeams for
		// clientlink.Admitter's reason: the first is satisfied by
		// internal/admission.Service and the second by internal/routing's
		// binding table, and a deployer implements neither. A3.3 serves the
		// four control routes over them; what is still not composed is
		// factory.New handing them in, which is A9.1's along with the
		// ClientLink's.
		iface[httpapi.ControlAdmitter](),
		iface[httpapi.CommandDelivery](),
		iface[clientlink.Authenticator](),
		iface[clientlink.Authorizer](),
		// The ClientLink's durable command plane. It is here rather than in
		// publicSeams for hostlink.Dialer's reason: it is satisfied by
		// internal/admission.Service, which a deployer implements nothing of.
		//
		// Nothing in production builds either one TODAY -- no production file
		// calls admission.NewService or clientlink.NewHandler, and
		// httpapi.routeTable still books /v1/realtime to A9.1. The seam is
		// listed so TestNoSeamNamesAStoragePrimitive covers it, not because the
		// wiring exists; A9.1 is what will construct the service and hand it to
		// the handler.
		iface[clientlink.Admitter](),
		// The ClientLink's local delivery-demand plane. It is here rather than
		// in publicSeams for the same reason: A7.2's internal/routing supplies
		// the implementation, and a deployer implements nothing of it. Nothing
		// in production builds either side today.
		iface[clientlink.DemandManager](),
		iface[clientlink.Clock](),
		iface[admission.Authorizer](),
		iface[admission.Commands](),
		iface[admission.Directory](),
		iface[admission.PlacementController](),
		iface[admission.Clock](),
		iface[admission.UUIDSource](),
		iface[internalidentity.Clock](),
		// The credential verifier is the authentication seam a deployer
		// implements. It is listed here rather than in publicSeams because it
		// IS the public type: identity.Verifier and internalidentity.Verifier
		// are one declaration reached through an alias, so pairing them would
		// assert a type against itself.
		iface[identity.Verifier](),
		iface[hostlink.Dialer](),
		iface[hostlink.Link](),
		// internal/placement's seams are listed here and NOT in publicSeams,
		// as hostlink's are. Its Catalog declares UpdateCatalogDesiredState,
		// which no seam on the option surface carries: factory.SessionReader is
		// the READ plane and widening it to author desired state is a
		// composition decision A9.1 owns, not one a task adding a consumer may
		// make by pairing a row here. What this list still buys them is
		// TestNoSeamNamesAStoragePrimitive.
		// internal/routing's remaining seams. They are here rather than in
		// publicSeams because this composition implements them: Binder is the
		// HostLink pool behind poolBinder, Hinter has no implementation at all
		// (see unpublishedHints), and Publisher, Tail and Rebinder belong to
		// the repair relay, which this composition does not build.
		iface[routing.Binder](),
		iface[routing.Hinter](),
		// internal/placement's HostLink half, implemented by this composition
		// over the pool (placementLinks), for routing.Binder's reason.
		iface[placement.HostLinks](),
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
