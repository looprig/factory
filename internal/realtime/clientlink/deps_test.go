package clientlink_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	"github.com/looprig/factory/internal/realtime/clientlink"
	"github.com/looprig/sessionstore"
)

// TestSeamsAreDrivenByAnImplementation is thin on purpose: A0.2 defines the
// seams and A2/A6 implement them. What it establishes is that the declared
// shapes are usable from outside this package -- an authenticator can return a
// Principal built by identity's own constructor, and an authorizer can refuse.
func TestSeamsAreDrivenByAnImplementation(t *testing.T) {
	t.Parallel()

	want, err := identity.NewPrincipal("tenant-1", "user-1", identity.KindActor)
	if err != nil {
		t.Fatalf("NewPrincipal: %v", err)
	}

	var authenticator clientlink.Authenticator = fixedAuthenticator{principal: want}
	got, err := authenticator.AuthenticateLink(context.Background(), "token")
	if err != nil {
		t.Fatalf("AuthenticateLink: %v", err)
	}
	if got != want {
		t.Errorf("AuthenticateLink() = %+v, want %+v", got, want)
	}

	var authorizer clientlink.Authorizer = denyAll{}
	if err := authorizer.AuthorizeSubscribe(context.Background(), got, "session:tenant-1:session-1"); !errors.Is(err, errDenied) {
		t.Errorf("AuthorizeSubscribe() = %v, want errDenied", err)
	}
	if err := authorizer.AuthorizeControl(context.Background(), got, "session-1", sessionstore.CommandKind("input")); !errors.Is(err, errDenied) {
		t.Errorf("AuthorizeControl() = %v, want errDenied", err)
	}
}

// TestAuthorizerCoversSubscribeAndControl floors the ClientLink authorizer on
// the two operation classes an established link performs.
func TestAuthorizerCoversSubscribeAndControl(t *testing.T) {
	t.Parallel()

	typ := reflect.TypeOf((*clientlink.Authorizer)(nil)).Elem()
	want := []string{"AuthorizeControl", "AuthorizeSubscribe"}
	if typ.NumMethod() != len(want) {
		t.Fatalf("Authorizer has %d methods, want %d", typ.NumMethod(), len(want))
	}
	for i, name := range want {
		if got := typ.Method(i).Name; got != name {
			t.Errorf("method %d is %q, want %q", i, got, name)
		}
	}
}

var errDenied = errors.New("denied")

type fixedAuthenticator struct{ principal identity.Principal }

func (a fixedAuthenticator) AuthenticateLink(context.Context, string) (identity.Principal, error) {
	return a.principal, nil
}

type denyAll struct{}

func (denyAll) AuthorizeSubscribe(context.Context, identity.Principal, string) error {
	return errDenied
}

func (denyAll) AuthorizeControl(context.Context, identity.Principal, sessionwire.SessionID, sessionstore.CommandKind) error {
	return errDenied
}
