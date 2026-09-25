package identity_test

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
)

// TestErrUnauthorizedIsAStablePublicSentinel pins the two things an external
// Authorizer or ObjectPolicy relies on: the value survives wrapping at any
// depth, and its text does not move. The text is part of the public contract
// because it reaches operator logs through every wrap an implementer adds;
// changing it is a visible change to a released value.
//
// This package holds no classifier, so a mutant of one of Factory's three
// classifiers is not reachable from here; those are killed by the depth-2 and
// join rows in the root, internal/httpapi and clientlink denial matrices.
func TestErrUnauthorizedIsAStablePublicSentinel(t *testing.T) {
	t.Parallel()

	if got, want := identity.ErrUnauthorized.Error(), "identity: unauthorized"; got != want {
		t.Fatalf("ErrUnauthorized text = %q, want %q", got, want)
	}
	for _, wrapped := range []error{
		fmt.Errorf("external authorizer: %w", identity.ErrUnauthorized),
		fmt.Errorf("external authorizer: %w", fmt.Errorf("policy engine: %w", identity.ErrUnauthorized)),
		errors.Join(errors.New("audit sink unavailable"), identity.ErrUnauthorized),
	} {
		if !errors.Is(wrapped, identity.ErrUnauthorized) {
			t.Errorf("errors.Is(%v, ErrUnauthorized) = false", wrapped)
		}
	}
}

func TestNewPrincipalValidates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		tenant  sessionwire.TenantID
		subject string
		kind    identity.Kind
		wantErr string
	}{
		{name: "actor", tenant: "tenant-1", subject: "user-1", kind: identity.KindActor},
		{name: "service", tenant: "tenant-1", subject: "factory-1", kind: identity.KindService},
		{name: "empty tenant", tenant: "", subject: "user-1", kind: identity.KindActor, wantErr: "tenant"},
		{name: "empty subject", tenant: "tenant-1", subject: "", kind: identity.KindActor, wantErr: "subject"},
		{name: "unknown kind", tenant: "tenant-1", subject: "user-1", kind: identity.Kind("admin"), wantErr: "kind"},
		{name: "empty kind", tenant: "tenant-1", subject: "user-1", kind: identity.Kind(""), wantErr: "kind"},
		{name: "tenant at the byte limit", tenant: sessionwire.TenantID(strings.Repeat("t", sessionwire.MaxIDBytes)), subject: "user-1", kind: identity.KindActor},
		{name: "tenant over the byte limit", tenant: sessionwire.TenantID(strings.Repeat("t", sessionwire.MaxIDBytes+1)), subject: "user-1", kind: identity.KindActor, wantErr: "tenant"},
		{name: "tenant that is not UTF-8", tenant: sessionwire.TenantID("\xff"), subject: "user-1", kind: identity.KindActor, wantErr: "tenant"},
		{name: "subject at the byte limit", tenant: "tenant-1", subject: strings.Repeat("s", sessionwire.MaxIDBytes), kind: identity.KindActor},
		{name: "subject over the byte limit", tenant: "tenant-1", subject: strings.Repeat("s", sessionwire.MaxIDBytes+1), kind: identity.KindActor, wantErr: "subject"},
		{name: "subject that is not UTF-8", tenant: "tenant-1", subject: "\xff", kind: identity.KindActor, wantErr: "subject"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := identity.NewPrincipal(tt.tenant, tt.subject, tt.kind)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("NewPrincipal(%q, %q, %q) = %v, want no error", tt.tenant, tt.subject, tt.kind, err)
				}
				if got.Tenant() != tt.tenant || got.Subject() != tt.subject || got.Kind() != tt.kind {
					t.Fatalf("NewPrincipal() = %q/%q/%q, want %q/%q/%q",
						got.Tenant(), got.Subject(), got.Kind(), tt.tenant, tt.subject, tt.kind)
				}
				if want := tt.kind == identity.KindService; got.IsService() != want {
					t.Errorf("IsService() = %t, want %t", got.IsService(), want)
				}
				return
			}
			if err == nil {
				t.Fatalf("NewPrincipal(%q, %q, %q) = %+v, want an error naming %q", tt.tenant, tt.subject, tt.kind, got, tt.wantErr)
			}
			if !errors.Is(err, identity.ErrInvalidPrincipal) {
				t.Errorf("error %v does not wrap ErrInvalidPrincipal", err)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error %q does not name %q", err, tt.wantErr)
			}
			// A rejected principal must not be returned half-built: a caller
			// that ignores the error must not receive a usable tenant scope.
			if got != (identity.Principal{}) {
				t.Errorf("NewPrincipal() returned %+v alongside its error, want the zero Principal", got)
			}
		})
	}
}

func TestWireRoundTripEveryKind(t *testing.T) {
	for _, kind := range []identity.Kind{identity.KindActor, identity.KindService} {
		principal, err := identity.NewPrincipal("acme", "user-01", kind)
		if err != nil {
			t.Fatal(err)
		}
		wire := principal.Wire()
		if wire != (sessionwire.Principal{Tenant: "acme", Subject: "user-01", Kind: sessionwire.PrincipalKind(kind)}) {
			t.Fatalf("Wire() = %+v", wire)
		}
		back, err := identity.PrincipalFromWire(wire)
		if err != nil || back != principal {
			t.Fatalf("round trip = (%+v, %v)", back, err)
		}
	}
}

func TestPrincipalFromWireRejectsInvalidValues(t *testing.T) {
	for _, wire := range []sessionwire.Principal{{}, {Tenant: "acme", Subject: "u"}, {Tenant: "acme", Subject: "u", Kind: "robot"}} {
		principal, err := identity.PrincipalFromWire(wire)
		if !errors.Is(err, identity.ErrInvalidPrincipal) || principal != (identity.Principal{}) {
			t.Fatalf("PrincipalFromWire(%+v) = (%+v, %v)", wire, principal, err)
		}
	}
}

func TestAdmissionSentinelsAreDistinct(t *testing.T) {
	if errors.Is(identity.ErrClientPrincipal, identity.ErrMetadataUnsupported) {
		t.Fatal("the admission refusals are indistinguishable")
	}
}

// TestPrincipalHasNoAssignableOrSharedState is the immutability claim, checked
// against the ways a value type loses it: an exported field, which lets a
// holder rewrite its own tenant, and a member through which a COPY can observe
// a write to the original. Nothing else in this module can make that assertion,
// because every one of those defects compiles and passes every behavioural test
// a value type has.
func TestPrincipalHasNoAssignableOrSharedState(t *testing.T) {
	t.Parallel()

	typ := reflect.TypeOf(identity.Principal{})
	if typ.NumField() == 0 {
		t.Fatal("Principal has no fields, so this test is vacuous")
	}
	for i := range typ.NumField() {
		field := typ.Field(i)
		if field.IsExported() {
			t.Errorf("Principal.%s is exported, so a holder can rewrite it", field.Name)
		}
		if shared := sharesMemory(field.Type); shared != "" {
			t.Errorf("Principal.%s is %s: a %s member lets a copy observe a write through the original",
				field.Name, field.Type, shared)
		}
	}
}

// TestPrincipalIsComparable is a second reader of the same field set, and it is
// not redundant: a func member is caught by sharesMemory, but it ALSO makes
// Principal non-comparable, which silently breaks every == and != on one --
// including the equality that clientlink's seam test asserts an authenticator
// returned the principal it was given.
func TestPrincipalIsComparable(t *testing.T) {
	t.Parallel()

	if typ := reflect.TypeOf(identity.Principal{}); !typ.Comparable() {
		t.Fatalf("%s is not comparable, so == and != on a Principal no longer compile", typ)
	}
	first, err := identity.NewPrincipal("tenant-1", "user-1", identity.KindActor)
	if err != nil {
		t.Fatalf("NewPrincipal: %v", err)
	}
	second, err := identity.NewPrincipal("tenant-1", "user-1", identity.KindActor)
	if err != nil {
		t.Fatalf("NewPrincipal: %v", err)
	}
	if first != second {
		t.Error("two principals built from the same verified credentials are not equal")
	}
	other, err := identity.NewPrincipal("tenant-2", "user-1", identity.KindActor)
	if err != nil {
		t.Fatalf("NewPrincipal: %v", err)
	}
	if first == other {
		t.Error("principals in different tenants compare equal")
	}
}

// sharesMemory names the kind that lets a copy observe a write through the
// original, or "" if none.
//
// Interface and Func are in the list for the same reason Pointer is, and
// leaving them out was a live hole rather than a theoretical one: an unexported
// `claims any` field holding a *[]string is copied by value while both copies
// read the same slice, and this detector answered "" to it.
func sharesMemory(typ reflect.Type) string {
	switch typ.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Map, reflect.Chan, reflect.UnsafePointer,
		reflect.Interface, reflect.Func:
		return typ.Kind().String()
	case reflect.Array:
		return sharesMemory(typ.Elem())
	case reflect.Struct:
		for i := range typ.NumField() {
			if shared := sharesMemory(typ.Field(i).Type); shared != "" {
				return shared
			}
		}
	}
	return ""
}

// TestSharesMemoryDetectsEachSharingKind keeps the assertion above from passing
// because its detector answers "" to everything. The cases are reflect.Types
// rather than values, because an interface- or func-typed member cannot be
// presented as an `any` -- reflect.TypeOf would report the DYNAMIC type and the
// two kinds this test was extended to cover would never reach the detector.
func TestSharesMemoryDetectsEachSharingKind(t *testing.T) {
	t.Parallel()

	type nested struct{ P *int }
	shared := []reflect.Type{
		reflect.TypeFor[*int](),
		reflect.TypeFor[[]byte](),
		reflect.TypeFor[map[string]string](),
		reflect.TypeFor[chan int](),
		reflect.TypeFor[any](),
		reflect.TypeFor[error](),
		reflect.TypeFor[func()](),
		reflect.TypeFor[nested](),
		reflect.TypeFor[[2]nested](),
		reflect.TypeFor[struct{ Inner nested }](),
		reflect.TypeFor[struct{ Claims any }](),
		reflect.TypeFor[struct{ Resolve func() string }](),
		reflect.TypeFor[[2]any](),
	}
	if len(shared) == 0 {
		t.Fatal("no sharing kinds are driven, so this test is vacuous")
	}
	for _, typ := range shared {
		if sharesMemory(typ) == "" {
			t.Errorf("sharesMemory(%s) = \"\", want a sharing kind", typ)
		}
	}
	notShared := []reflect.Type{
		reflect.TypeFor[string](),
		reflect.TypeFor[int](),
		reflect.TypeFor[identity.Kind](),
		reflect.TypeFor[[2]string](),
		reflect.TypeFor[struct{ A string }](),
		reflect.TypeFor[identity.Principal](),
	}
	for _, typ := range notShared {
		if got := sharesMemory(typ); got != "" {
			t.Errorf("sharesMemory(%s) = %q, want \"\"", typ, got)
		}
	}
}

// TestExpiryIsAKindOfUnauthenticated holds the wrapping that lets one edge
// handler answer both. If the two sentinels were siblings, every caller
// matching ErrUnauthenticated would silently stop recognising an expired
// credential, which is the commonest rejection a running deployment produces.
func TestExpiryIsAKindOfUnauthenticated(t *testing.T) {
	t.Parallel()

	if !errors.Is(identity.ErrCredentialExpired, identity.ErrUnauthenticated) {
		t.Error("ErrCredentialExpired does not wrap ErrUnauthenticated")
	}
	if errors.Is(identity.ErrUnauthenticated, identity.ErrCredentialExpired) {
		t.Error("ErrUnauthenticated matches ErrCredentialExpired, so an invalid credential reads as an expired one")
	}
	// The two classes are disjoint from the constructor's class: a caller that
	// could not build a principal has not been told anything about a
	// credential, and a caller with no credential has not built a bad one.
	for _, pair := range []struct {
		name       string
		err, other error
	}{
		{"unauthenticated/invalid", identity.ErrUnauthenticated, identity.ErrInvalidPrincipal},
		{"expired/invalid", identity.ErrCredentialExpired, identity.ErrInvalidPrincipal},
	} {
		if errors.Is(pair.err, pair.other) {
			t.Errorf("%s: the two error classes are not distinguishable", pair.name)
		}
	}
	if !strings.Contains(identity.ErrCredentialExpired.Error(), "expired") {
		t.Errorf("ErrCredentialExpired reads %q, which does not say what happened", identity.ErrCredentialExpired)
	}
}
