package admission

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
)

func attributedCases() []struct {
	name string
	call func(*serviceFixture, *sessionwire.Principal) error
} {
	return []struct {
		name string
		call func(*serviceFixture, *sessionwire.Principal) error
	}{
		{"create", func(f *serviceFixture, p *sessionwire.Principal) error {
			_, _, err := f.service.AdmitCreate(context.Background(), f.principal, sessionwire.CreateRequest{CommandEnvelope: envelope("create-1"), SessionID: "session-new", AgentID: "agent-a", Principal: p})
			return err
		}},
		{"input", func(f *serviceFixture, p *sessionwire.Principal) error {
			_, _, err := f.service.AdmitInput(context.Background(), f.principal, sessionwire.InputRequest{CommandEnvelope: envelope("input-1"), SessionID: "session-a", Blocks: json.RawMessage(`[{"type":"text","text":"hi"}]`), Principal: p})
			return err
		}},
		{"interrupt", func(f *serviceFixture, p *sessionwire.Principal) error {
			_, _, err := f.service.AdmitInterrupt(context.Background(), f.principal, sessionwire.InterruptRequest{CommandEnvelope: envelope("interrupt-1"), SessionID: "session-a", Principal: p})
			return err
		}},
		{"restore", func(f *serviceFixture, p *sessionwire.Principal) error {
			_, _, err := f.service.AdmitRestore(context.Background(), f.principal, sessionwire.RestoreRequest{CommandEnvelope: envelope("restore-1"), SessionID: "session-a", Principal: p})
			return err
		}},
		{"gate_response", func(f *serviceFixture, p *sessionwire.Principal) error {
			req := gateAnswer("gate-1")
			req.Principal = p
			_, _, err := f.service.AdmitGateResponse(context.Background(), f.principal, req)
			return err
		}},
	}
}

func TestClientPrincipalRefusedBeforeAuthorizationForEveryKind(t *testing.T) {
	client := &sessionwire.Principal{Tenant: "tenant-a", Subject: "forged", Kind: sessionwire.PrincipalKindActor}
	for _, stamping := range []bool{false, true} {
		for _, row := range attributedCases() {
			t.Run(row.name, func(t *testing.T) {
				f := newServiceFixture(t)
				resolvableSession(f)
				f.rebuild(t, func(cfg *Config) {
					cfg.Binding = SessionBindingTemplate{StorageBindingID: "storage-a", BindingVersion: "v1"}
					cfg.StampPrincipal = stamping
				})
				err := row.call(f, client)
				if !IsCode(err, sessionwire.ErrorCodeInvalidRequest) || !errors.Is(err, identity.ErrClientPrincipal) {
					t.Fatalf("stamping=%t: %v", stamping, err)
				}
				if f.auth.calls != 0 || f.commands.calls != 0 || len(f.creates.reservations) != 0 {
					t.Fatal("forged principal reached authorization or storage")
				}
			})
		}
	}
}

func TestStampedInputStoresVerifiedPrincipalAndMetadata(t *testing.T) {
	f := newServiceFixture(t)
	resolvableSession(f)
	f.rebuild(t, func(cfg *Config) { cfg.StampPrincipal = true; cfg.PrincipalResponders = &servicePrincipalResponders{} })
	metadata := sessionwire.MessageMetadata{"space": "family"}
	_, _, err := f.service.AdmitInput(context.Background(), f.principal, sessionwire.InputRequest{CommandEnvelope: envelope("input-1"), SessionID: "session-a", Blocks: json.RawMessage(`[{"type":"text","text":"hi"}]`), Metadata: metadata})
	if err != nil {
		t.Fatal(err)
	}
	got := f.commands.lastAdmit
	if got.Principal == nil || *got.Principal != f.principal.Wire() || got.Metadata["space"] != "family" {
		t.Fatalf("descriptor columns = %+v %+v", got.Principal, got.Metadata)
	}
	var body struct {
		Principal *sessionwire.Principal      `json:"principal"`
		Metadata  sessionwire.MessageMetadata `json:"metadata"`
	}
	if err := json.Unmarshal(got.Payload, &body); err != nil || body.Principal == nil || *body.Principal != f.principal.Wire() || body.Metadata["space"] != "family" {
		t.Fatalf("payload = %s (%v)", got.Payload, err)
	}
}

func TestStampedRetryByAnotherSubjectIsCommandRejected(t *testing.T) {
	f := newServiceFixture(t)
	resolvableSession(f)
	f.rebuild(t, func(cfg *Config) { cfg.StampPrincipal = true; cfg.PrincipalResponders = &servicePrincipalResponders{} })
	req := sessionwire.InputRequest{CommandEnvelope: envelope("input-1"), SessionID: "session-a", Blocks: json.RawMessage(`[{"type":"text","text":"hi"}]`)}
	if _, _, err := f.service.AdmitInput(context.Background(), f.principal, req); err != nil {
		t.Fatal(err)
	}
	other, _ := identity.NewPrincipal("tenant-a", "actor-b", identity.KindActor)
	if _, _, err := f.service.AdmitInput(context.Background(), other, req); !IsCode(err, sessionwire.ErrorCodeCommandRejected) {
		t.Fatalf("different-subject retry: %v", err)
	}
	if _, _, err := f.service.AdmitInput(context.Background(), f.principal, req); err != nil {
		t.Fatalf("same-subject retry: %v", err)
	}
}

func TestStampingCoversEveryCommandKind(t *testing.T) {
	for _, row := range attributedCases() {
		t.Run(row.name, func(t *testing.T) {
			f := newServiceFixture(t)
			resolvableSession(f)
			f.rebuild(t, func(cfg *Config) {
				cfg.StampPrincipal = true
				cfg.Binding = SessionBindingTemplate{StorageBindingID: "storage-a", BindingVersion: "v1"}
			})
			if err := row.call(f, nil); err != nil {
				t.Fatal(err)
			}
			var payload []byte
			var principal *sessionwire.Principal
			if row.name == "create" {
				payload, principal = f.creates.lastAdmit.Payload, f.creates.lastAdmit.Principal
				if got := f.creates.records["create-1"].Record.Descriptor.Principal; got == nil || *got != f.principal.Wire() {
					t.Fatalf("create descriptor principal = %+v", got)
				}
			} else {
				payload, principal = f.commands.lastAdmit.Payload, f.commands.lastAdmit.Principal
			}
			if principal == nil || *principal != f.principal.Wire() {
				t.Fatalf("stored principal = %+v", principal)
			}
			var got struct {
				Principal *sessionwire.Principal `json:"principal"`
			}
			if err := json.Unmarshal(payload, &got); err != nil || got.Principal == nil || *got.Principal != f.principal.Wire() {
				t.Fatalf("payload = %s (%v)", payload, err)
			}
		})
	}
}

func TestMemberlessCommandKeepsLegacyBytesAndDoesNotAskCapability(t *testing.T) {
	f := newServiceFixture(t)
	resolvableSession(f)
	f.principals.refuse = true
	if _, _, err := f.service.AdmitInput(context.Background(), f.principal, sessionwire.InputRequest{CommandEnvelope: envelope("input-1"), SessionID: "session-a", Blocks: json.RawMessage(`[{"type":"text","text":"hi"}]`)}); err != nil {
		t.Fatal(err)
	}
	const golden = `{"version":1,"command_id":"input-1","session_id":"session-a","blocks":[{"type":"text","text":"hi"}]}`
	if got := string(f.commands.lastAdmit.Payload); got != golden {
		t.Fatalf("payload = %s", got)
	}
	if f.principals.calls != 0 || f.commands.lastAdmit.Principal != nil || len(f.commands.lastAdmit.Metadata) != 0 {
		t.Fatal("memberless command acquired attribution or asked capability")
	}
}

func TestMemberBearingCommandIsRefusedBeforeWriteForIncapableResident(t *testing.T) {
	for _, noResponder := range []bool{false, true} {
		f := newServiceFixture(t)
		resolvableSession(f)
		f.principals.refuse = true
		f.rebuild(t, func(cfg *Config) {
			if noResponder {
				cfg.PrincipalResponders = nil
			}
		})
		_, _, err := f.service.AdmitInput(context.Background(), f.principal, sessionwire.InputRequest{CommandEnvelope: envelope("input-1"), SessionID: "session-a", Blocks: json.RawMessage(`[{"type":"text","text":"hi"}]`), Metadata: sessionwire.MessageMetadata{"space": "family"}})
		if !IsCode(err, sessionwire.ErrorCodeRuntimeUnavailable) || !errors.Is(err, identity.ErrMetadataUnsupported) || f.commands.calls != 0 {
			t.Fatalf("noResponder=%v: err=%v writes=%d", noResponder, err, f.commands.calls)
		}
	}
}

func TestNoResidentAdmitsAttributedCommandAndCapabilityFaultIsUncoded(t *testing.T) {
	f := newServiceFixture(t)
	resolvableSession(f)
	f.directory.ok = false
	f.principals.refuse = true
	f.rebuild(t, func(cfg *Config) { cfg.StampPrincipal = true })
	req := sessionwire.InterruptRequest{CommandEnvelope: envelope("i-1"), SessionID: "session-a"}
	if _, _, err := f.service.AdmitInterrupt(context.Background(), f.principal, req); err != nil || f.principals.calls != 0 {
		t.Fatalf("unplaced admission = %v, asked %d", err, f.principals.calls)
	}
	f.directory.ok = true
	f.principals.failing = "AcceptsCommandPrincipal"
	_, _, err := f.service.AdmitInterrupt(context.Background(), f.principal, sessionwire.InterruptRequest{CommandEnvelope: envelope("i-2"), SessionID: "session-a"})
	var coded *Error
	if !errors.Is(err, errInjectedFault) || errors.As(err, &coded) || f.commands.calls != 1 {
		t.Fatalf("capability fault = %v, writes=%d", err, f.commands.calls)
	}
}

type servicePrincipalResponders struct {
	faultInjector
	refuse bool
	calls  int
	err    error
}

func (p *servicePrincipalResponders) AcceptsCommandPrincipal(context.Context, sessionwire.HostLinkRegistryObservation) (bool, error) {
	p.calls++
	if err := p.enter("AcceptsCommandPrincipal"); err != nil {
		return false, err
	}
	return !p.refuse, p.err
}
