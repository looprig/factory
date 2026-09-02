package factory_test

import (
	"reflect"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory"
	"github.com/looprig/sessionstore"
)

// TestTargetForProjectsASessionStoreRecord drives the projection, which is also
// what keeps Core and SessionStore in go.mod as DIRECT requirements. A guard
// that only names a module in a string cannot hold its pin: `go mod tidy` drops
// a requirement nothing imports, and the boundary would then be checked against
// versions the module no longer resolves. See TestCrossServiceContractTypes.
func TestTargetForProjectsASessionStoreRecord(t *testing.T) {
	t.Parallel()

	row := sessionstore.HostTarget{
		Key:    sessionstore.HostTargetKey{AgentID: "agent-1"},
		HostID: "host-1",
	}
	got := factory.TargetFor("tenant-1", "session-1", row)
	want := factory.Target{Tenant: "tenant-1", Session: "session-1", Agent: "agent-1", Host: "host-1"}
	if got != want {
		t.Fatalf("TargetFor() = %+v, want %+v", got, want)
	}
}

// TestCrossServiceContractTypes pins the positive half of the boundary: every
// member of the contract is a Core wire type. A member added later whose type
// comes from anywhere else fails here, which is the check the import guard
// cannot make — an import ban says what may not be reached, not what the
// records crossing the seam are made of.
func TestCrossServiceContractTypes(t *testing.T) {
	t.Parallel()

	want := map[string]reflect.Type{
		"Tenant":  reflect.TypeOf(sessionwire.TenantID("")),
		"Session": reflect.TypeOf(sessionwire.SessionID("")),
		"Agent":   reflect.TypeOf(sessionwire.AgentID("")),
		"Host":    reflect.TypeOf(sessionwire.HostID("")),
	}
	target := reflect.TypeOf(factory.Target{})
	if target.NumField() != len(want) {
		t.Fatalf("Target has %d fields, want %d; a new member needs a case above", target.NumField(), len(want))
	}
	if len(want) == 0 {
		t.Fatal("no contract members are asserted, so this test is vacuous")
	}
	for i := range target.NumField() {
		field := target.Field(i)
		expected, ok := want[field.Name]
		if !ok {
			t.Errorf("Target has unexpected field %q", field.Name)
			continue
		}
		if field.Type != expected {
			t.Errorf("Target.%s is %s, want the Core wire type %s", field.Name, field.Type, expected)
		}
		if pkg := field.Type.PkgPath(); pkg != "github.com/looprig/core/sessionwire/v1" {
			t.Errorf("Target.%s comes from %q, but the cross-service contract is Core/SessionStore records", field.Name, pkg)
		}
	}
}
