package identity_test

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
)

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

// TestPrincipalHasNoAssignableOrSharedState is the immutability claim, checked
// against the two ways a value type loses it: an exported field, which lets a
// holder rewrite its own tenant, and a reference member, which lets a COPY
// observe a write through the original. Nothing else in this module can make
// that assertion, because both defects compile and both pass every behavioural
// test a value type has.
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
			t.Errorf("Principal.%s is %s, so a copy shares %s with its original", field.Name, field.Type, shared)
		}
	}
}

// sharesMemory names the kind that makes a copy share state, or "" if none.
func sharesMemory(typ reflect.Type) string {
	switch typ.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Map, reflect.Chan, reflect.UnsafePointer:
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
// because its detector answers "" to everything.
func TestSharesMemoryDetectsEachSharingKind(t *testing.T) {
	t.Parallel()

	type nested struct{ P *int }
	shared := []any{
		new(int),
		[]byte(nil),
		map[string]string(nil),
		make(chan int),
		nested{},
		[2]nested{},
		struct{ Inner nested }{},
	}
	for _, value := range shared {
		typ := reflect.TypeOf(value)
		if sharesMemory(typ) == "" {
			t.Errorf("sharesMemory(%s) = \"\", want a sharing kind", typ)
		}
	}
	notShared := []any{"", 0, identity.Kind(""), [2]string{}, struct{ A string }{}}
	for _, value := range notShared {
		typ := reflect.TypeOf(value)
		if got := sharesMemory(typ); got != "" {
			t.Errorf("sharesMemory(%s) = %q, want \"\"", typ, got)
		}
	}
}
