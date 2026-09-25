package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	"github.com/looprig/sessionstore"
)

type commandReadFixture struct {
	entry sessionstore.DispositionInboxEntry
	err   error
	calls int
}

func (f *commandReadFixture) GetDispositionCommand(_ context.Context, req sessionstore.GetDispositionCommandRequest) (sessionstore.DispositionInboxEntry, error) {
	f.calls++
	if f.err != nil {
		return sessionstore.DispositionInboxEntry{}, f.err
	}
	if req.TenantID != fixtureTenant || req.SessionID != fixtureSession || req.CommandID != "input-1" {
		return sessionstore.DispositionInboxEntry{}, &sessionstore.InboxError{Code: sessionstore.InboxErrorNotFound}
	}
	return f.entry, nil
}

type optionalAuditAuthorizer struct {
	*recordingAuthorizer
	grant bool
}

func (a optionalAuditAuthorizer) AuthorizeAuditRead(context.Context, identity.Principal, sessionwire.SessionID) error {
	if a.grant {
		return nil
	}
	return identity.ErrUnauthorized
}

func stampedCommandEntry() sessionstore.DispositionInboxEntry {
	return sessionstore.DispositionInboxEntry{Record: sessionstore.DispositionInboxRecord{Descriptor: sessionstore.DispositionCommandDescriptor{CommandID: "input-1", Principal: &sessionwire.Principal{Tenant: fixtureTenant, Subject: "actor-a", Kind: sessionwire.PrincipalKindActor}, Metadata: sessionwire.MessageMetadata{"space": "family"}}, State: sessionstore.InboxStatePending, AcceptedAt: time.Now(), ApplyDeadline: time.Now().Add(time.Minute)}, AcceptedOrder: 1}
}

func TestCommandStatusAuditMembersRequireOptionalGrant(t *testing.T) {
	for _, row := range []struct {
		name       string
		authorizer Authorizer
		disclose   bool
	}{
		{"absent", &recordingAuthorizer{}, false},
		{"denied", optionalAuditAuthorizer{recordingAuthorizer: &recordingAuthorizer{}, grant: false}, false},
		{"granted", optionalAuditAuthorizer{recordingAuthorizer: &recordingAuthorizer{}, grant: true}, true},
	} {
		t.Run(row.name, func(t *testing.T) {
			reader := &commandReadFixture{entry: stampedCommandEntry()}
			f := newFixture(t, func(cfg *RouterConfig, _ *fixture) { cfg.CommandReads = reader; cfg.Authorizer = row.authorizer })
			response := f.get("/v1/sessions/session-a/commands/input-1")
			if response.Code != http.StatusOK {
				t.Fatalf("response = %d %s", response.Code, response.Body)
			}
			disclosed := bytes.Contains(response.Body.Bytes(), []byte(`"principal"`)) || bytes.Contains(response.Body.Bytes(), []byte(`"metadata"`))
			if disclosed != row.disclose {
				t.Fatalf("body = %s, disclose=%t", response.Body, row.disclose)
			}
			if row.disclose {
				var audit struct {
					Principal *sessionwire.Principal      `json:"principal"`
					Metadata  sessionwire.MessageMetadata `json:"metadata"`
				}
				if err := json.Unmarshal(response.Body.Bytes(), &audit); err != nil || audit.Principal == nil || audit.Principal.Subject != "actor-a" || audit.Metadata["space"] != "family" {
					t.Fatalf("audit = %+v (%v)", audit, err)
				}
			}
			var status sessionwire.CommandStatus
			if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil || status.CommandID != "input-1" {
				t.Fatalf("status = %+v (%v)", status, err)
			}
		})
	}
}

func TestAuditMetadataIsJSONEscapedAndNeverServedAsHTML(t *testing.T) {
	entry := stampedCommandEntry()
	entry.Record.Descriptor.Metadata = sessionwire.MessageMetadata{"markup": "<script>alert(1)</script>"}
	reader := &commandReadFixture{entry: entry}
	f := newFixture(t, func(cfg *RouterConfig, _ *fixture) {
		cfg.CommandReads = reader
		cfg.Authorizer = optionalAuditAuthorizer{recordingAuthorizer: &recordingAuthorizer{}, grant: true}
	})
	response := f.get("/v1/sessions/session-a/commands/input-1")
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d %s", response.Code, response.Body)
	}
	if got := response.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q", got)
	}
	if got := response.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Errorf("X-Content-Type-Options = %q", got)
	}
	if bytes.Contains(response.Body.Bytes(), []byte("<script>")) || !bytes.Contains(response.Body.Bytes(), []byte(`\u003cscript\u003e`)) {
		t.Errorf("markup was not JSON-escaped: %s", response.Body)
	}
}

func TestCommandStatusRejectsInvalidAbsentAndWrongSession(t *testing.T) {
	reader := &commandReadFixture{entry: stampedCommandEntry()}
	f := newFixture(t, func(cfg *RouterConfig, f *fixture) {
		cfg.CommandReads = reader
		f.reads.sessions[storedSession{tenant: fixtureTenant, session: "session-other"}] = true
	})
	for _, row := range []struct {
		path string
		want int
	}{
		{"/v1/sessions/session-a/commands/" + strings.Repeat("x", 257), http.StatusBadRequest},
		{"/v1/sessions/session-a/commands/absent", http.StatusNotFound},
		{"/v1/sessions/session-other/commands/input-1", http.StatusNotFound},
	} {
		if got := f.get(row.path); got.Code != row.want {
			t.Fatalf("%s -> %d %s", row.path, got.Code, got.Body)
		}
	}
}

func TestCommandStatusWithoutReaderIsUnavailable(t *testing.T) {
	f := newFixture(t)
	if got := f.get("/v1/sessions/session-a/commands/input-1"); got.Code != http.StatusServiceUnavailable {
		t.Fatalf("response = %d %s", got.Code, got.Body)
	}
}

func TestLegacyBoundCommandReadIsIndistinguishableFromAbsence(t *testing.T) {
	reader := &commandReadFixture{err: &sessionstore.CatalogError{Code: sessionstore.CatalogErrorInvalid, Field: "binding.protocol_mode"}}
	f := newFixture(t, func(cfg *RouterConfig, _ *fixture) { cfg.CommandReads = reader })
	response := f.get("/v1/sessions/session-a/commands/input-1")
	if response.Code != http.StatusNotFound || !bytes.Contains(response.Body.Bytes(), []byte(`"command_not_found"`)) {
		t.Fatalf("legacy read = %d %s", response.Code, response.Body)
	}
}
