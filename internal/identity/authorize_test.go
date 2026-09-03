package identity_test

import (
	"context"
	"errors"
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

func TestAuthorizerAllowsEveryTenantOperation(t *testing.T) {
	t.Parallel()

	authorizer := internalidentity.Authorizer{}
	principal := actor(t, "tenant-a")
	ctx := context.Background()
	session := sessionwire.SessionID("shared-session")
	object := sessionwire.ObjectReference{ObjectID: "shared-object"}

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

func TestAuthorizerRejectsAnUnconstructedPrincipalForEveryOperation(t *testing.T) {
	t.Parallel()

	authorizer := internalidentity.Authorizer{}
	principal := factoryidentity.Principal{}
	ctx := context.Background()
	session := sessionwire.SessionID("session-a")
	object := sessionwire.ObjectReference{ObjectID: "object-a"}

	tests := []struct {
		name      string
		authorize func() error
	}{
		{"list", func() error { return authorizer.AuthorizeSessionList(ctx, principal) }},
		{"read", func() error { return authorizer.AuthorizeSessionRead(ctx, principal, session) }},
		{"object", func() error { return authorizer.AuthorizeObjectRead(ctx, principal, session, object) }},
		{"control", func() error {
			return authorizer.AuthorizeControl(ctx, principal, session, sessionstore.CommandKind("input"))
		}},
		{"subscribe", func() error { return authorizer.AuthorizeSubscribe(ctx, principal, "session:tenant-a:session-a") }},
		{"service sweep", func() error { return authorizer.AuthorizeServiceSweep(ctx, principal) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if err := tt.authorize(); !errors.Is(err, internalidentity.ErrUnauthorized) {
				t.Fatalf("authorize = %v, want ErrUnauthorized", err)
			}
		})
	}
}

func TestAuthorizerConfinesOpaqueIdentifiersToThePrincipalTenant(t *testing.T) {
	t.Parallel()

	authorizer := internalidentity.Authorizer{}
	tenantA := actor(t, "tenant-a")
	tenantB := actor(t, "tenant-b")
	ctx := context.Background()

	// Session, command and object identifiers are tenant-local. The same values
	// therefore authorize in both scopes; none of them can select the other
	// tenant because the downstream request takes its tenant from Principal.
	sharedSession := sessionwire.SessionID("same-session")
	sharedCommand := sessionwire.CommandID("same-command")
	sharedObject := sessionwire.ObjectReference{ObjectID: "same-object"}
	for name, authorize := range map[string]func(factoryidentity.Principal) error{
		"session": func(principal factoryidentity.Principal) error {
			return authorizer.AuthorizeSessionRead(ctx, principal, sharedSession)
		},
		"command " + string(sharedCommand): func(principal factoryidentity.Principal) error {
			return authorizer.AuthorizeControl(ctx, principal, sharedSession, "input")
		},
		"object": func(principal factoryidentity.Principal) error {
			return authorizer.AuthorizeObjectRead(ctx, principal, sharedSession, sharedObject)
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			for _, principal := range []factoryidentity.Principal{tenantA, tenantB} {
				if err := authorize(principal); err != nil {
					t.Fatalf("tenant %q authorize = %v, want nil", principal.Tenant(), err)
				}
			}
		})
	}

	if err := authorizer.AuthorizeSubscribe(ctx, tenantA, "session:tenant-b:same-session"); !errors.Is(err, internalidentity.ErrUnauthorized) {
		t.Fatalf("tenant-a subscribing to tenant-b = %v, want ErrUnauthorized", err)
	}
	if err := authorizer.AuthorizeSubscribe(ctx, tenantB, "session:tenant-a:same-session"); !errors.Is(err, internalidentity.ErrUnauthorized) {
		t.Fatalf("tenant-b subscribing to tenant-a = %v, want ErrUnauthorized", err)
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
