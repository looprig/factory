package httpapi_test

import (
	"reflect"
	"testing"

	"github.com/looprig/factory/internal/httpapi"
	"github.com/looprig/sessionstore"
)

// TestSessionStoreSatisfiesTheReadPlane pins SessionReader to the aggregate it
// abstracts. The interface exists so handlers depend on the reads they make
// rather than on the whole Store, but a narrow interface nothing real satisfies
// is a fiction: this fails the day a method is spelled in a shape SessionStore
// does not have.
func TestSessionStoreSatisfiesTheReadPlane(t *testing.T) {
	t.Parallel()

	reader := reflect.TypeOf((*httpapi.SessionReader)(nil)).Elem()
	if reader.NumMethod() == 0 {
		t.Fatal("SessionReader has no methods, so every type satisfies it and this test is vacuous")
	}
	store := reflect.TypeOf((*sessionstore.Store)(nil))
	if !store.Implements(reader) {
		t.Fatalf("*sessionstore.Store does not satisfy httpapi.SessionReader")
	}
}

// TestReadPlaneIsNarrowerThanTheStore is the other half: an interface that had
// drifted into naming every Store method would satisfy the test above and
// abstract nothing.
func TestReadPlaneIsNarrowerThanTheStore(t *testing.T) {
	t.Parallel()

	reader := reflect.TypeOf((*httpapi.SessionReader)(nil)).Elem()
	store := reflect.TypeOf((*sessionstore.Store)(nil))
	if reader.NumMethod() >= store.NumMethod() {
		t.Fatalf("SessionReader names %d of *sessionstore.Store's %d methods, so it narrows nothing",
			reader.NumMethod(), store.NumMethod())
	}
}

// TestAuthorizerCoversEveryPublicOperationClass floors the authorizer on the
// operation classes runbook 05 requires it to decide. It is a count and a name
// check, not a behavioural one -- behaviour is task A1.2 -- but it fails if a
// class is dropped while the composition still compiles.
func TestAuthorizerCoversEveryPublicOperationClass(t *testing.T) {
	t.Parallel()

	want := []string{
		"AuthorizeControl",
		"AuthorizeObjectRead",
		"AuthorizeSessionList",
		"AuthorizeSessionRead",
	}
	assertMethodSet(t, reflect.TypeOf((*httpapi.Authorizer)(nil)).Elem(), want)
}

func TestAuthenticatorTakesARequest(t *testing.T) {
	t.Parallel()

	assertMethodSet(t, reflect.TypeOf((*httpapi.Authenticator)(nil)).Elem(), []string{"AuthenticateRequest"})
}

func assertMethodSet(t *testing.T, typ reflect.Type, want []string) {
	t.Helper()

	if len(want) == 0 {
		t.Fatal("no methods are asserted, so this check is vacuous")
	}
	got := make([]string, typ.NumMethod())
	for i := range typ.NumMethod() {
		got[i] = typ.Method(i).Name
	}
	if len(got) != len(want) {
		t.Fatalf("%s has methods %v, want exactly %v", typ, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("%s method %d is %q, want %q", typ, i, got[i], want[i])
		}
	}
}

// TestTheDirectoryIsTheCandidatePageAndNothingElse pins the target directory
// seam to the one read /v1/agents makes.
//
// It is narrow on purpose. Factory's public Directory also serves an OWNER
// lookup, which is per-session ownership and is admission's question; a
// handler on this surface that could ask it would be resolving ownership
// inside a durable read, which is exactly the Host fan-out these routes must
// not do.
func TestTheDirectoryIsTheCandidatePageAndNothingElse(t *testing.T) {
	t.Parallel()

	assertMethodSet(t, reflect.TypeOf((*httpapi.Directory)(nil)).Elem(), []string{"Candidates"})
}

// TestTheDirectoryTakesTheStoresOwnRequest keeps the seam spelled in
// SessionStore's vocabulary rather than in one of this package's own.
//
// The point is not the type: it is that the LIMIT and the CURSOR a handler
// bounds its read with are the store's members, so a bound written here is the
// bound the store applies, with no adapter in between to lose it.
func TestTheDirectoryTakesTheStoresOwnRequest(t *testing.T) {
	t.Parallel()

	method, ok := reflect.TypeOf((*httpapi.Directory)(nil)).Elem().MethodByName("Candidates")
	if !ok {
		t.Fatal("Directory declares no Candidates method")
	}
	request := reflect.TypeOf(sessionstore.ListCompatibleHostsRequest{})
	if got := method.Type.In(1); got != request {
		t.Errorf("Candidates takes %s, want %s", got, request)
	}
	page := reflect.TypeOf(sessionstore.HostTargetPage{})
	if got := method.Type.Out(0); got != page {
		t.Errorf("Candidates returns %s, want %s", got, page)
	}
}
