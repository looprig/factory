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
