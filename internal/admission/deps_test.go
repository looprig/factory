package admission_test

import (
	"reflect"
	"testing"

	"github.com/looprig/factory/internal/admission"
	"github.com/looprig/sessionstore"
)

// TestSessionStoreSatisfiesTheCommandPlane pins Commands to the aggregate it
// abstracts, for the reason given in the httpapi companion.
func TestSessionStoreSatisfiesTheCommandPlane(t *testing.T) {
	t.Parallel()

	commands := reflect.TypeOf((*admission.Commands)(nil)).Elem()
	if commands.NumMethod() == 0 {
		t.Fatal("Commands has no methods, so every type satisfies it and this test is vacuous")
	}
	store := reflect.TypeOf((*sessionstore.Store)(nil))
	if !store.Implements(commands) {
		t.Fatal("*sessionstore.Store does not satisfy admission.Commands")
	}
	if commands.NumMethod() >= store.NumMethod() {
		t.Fatalf("Commands names %d of *sessionstore.Store's %d methods, so it narrows nothing",
			commands.NumMethod(), store.NumMethod())
	}
}

// TestDirectoryIsNotTheStore is the seam that is deliberately NOT a projection
// of SessionStore's method set. Ownership and candidacy are answered by
// internal/routing over the registry and the advertisement rows, and a
// Directory spelled as store methods would have let placement read capacity
// rows as though capacity were authority.
func TestDirectoryIsNotTheStore(t *testing.T) {
	t.Parallel()

	directory := reflect.TypeOf((*admission.Directory)(nil)).Elem()
	if directory.NumMethod() == 0 {
		t.Fatal("Directory has no methods, so this test is vacuous")
	}
	if reflect.TypeOf((*sessionstore.Store)(nil)).Implements(directory) {
		t.Fatal("*sessionstore.Store satisfies admission.Directory, so the directory is a store alias")
	}
}

// TestClockNamesOnlyPortableTypes is what keeps admission.Clock and the public
// factory.Clock assignable without an adapter: a named Timer type in either
// would make them different interfaces across the internal/ boundary, and the
// composition root could not pass one to the other.
func TestClockNamesOnlyPortableTypes(t *testing.T) {
	t.Parallel()

	clock := reflect.TypeOf((*admission.Clock)(nil)).Elem()
	if clock.NumMethod() == 0 {
		t.Fatal("Clock has no methods, so this test is vacuous")
	}
	for i := range clock.NumMethod() {
		method := clock.Method(i)
		for _, typ := range signatureTypes(method.Type) {
			if path := typ.PkgPath(); path != "" && path != "time" {
				t.Errorf("Clock.%s names %s from %q; only builtin and time types keep the two Clocks assignable",
					method.Name, typ, path)
			}
		}
	}
}

func signatureTypes(fn reflect.Type) []reflect.Type {
	var out []reflect.Type
	for i := range fn.NumIn() {
		out = append(out, fn.In(i))
	}
	for i := range fn.NumOut() {
		out = append(out, fn.Out(i))
	}
	return out
}
