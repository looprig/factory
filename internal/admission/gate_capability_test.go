package admission

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

func gateAnswer(id string) sessionwire.GateResponseRequest {
	return sessionwire.GateResponseRequest{
		CommandEnvelope: envelope(id), SessionID: "session-a", GateID: "gate-a", Action: "submit",
		Values: map[string]json.RawMessage{"answer": json.RawMessage(`"yes"`)}, ExpectedOpenEventID: "event-a",
	}
}

// TestAGateResponseIsNotAdmittedForAnOwnerThatCannotApplyOne is the
// gate_response capability gate. Admitting a gate response IS delivering it --
// the owning Host reads it from the durable stream -- so the question is asked
// of the owner the fresh-owner check accepted, BEFORE anything is written, and
// "cannot" is gate_not_resumable carrying ErrGateResponseUnsupported. Three
// compositions: an owner that cannot, no GateResponders at all (the default
// refuses), and the accepting control, without which "nothing was written"
// would also be the output of a service that admitted nothing.
func TestAGateResponseIsNotAdmittedForAnOwnerThatCannotApplyOne(t *testing.T) {
	for name, row := range map[string]struct {
		configure func(*serviceFixture)
		admitted  bool
	}{
		"an owner that cannot apply one": {func(f *serviceFixture) { f.gates.refuse = true }, false},
		"no GateResponders composed": {func(f *serviceFixture) {
			f.rebuild(t, func(cfg *Config) { cfg.GateResponders = nil })
		}, false},
		"an owner that can (control)": {func(*serviceFixture) {}, true},
	} {
		t.Run(name, func(t *testing.T) {
			f := newServiceFixture(t)
			resolvableSession(f)
			f.directory.owner.HostID = "host-owner"
			f.directory.owner.HostGeneration = 9
			row.configure(f)

			_, _, err := f.service.AdmitGateResponse(context.Background(), f.principal, gateAnswer("answer-1"))
			wrote := f.commands.lastAdmit.CommandID == "answer-1"
			if row.admitted {
				if err != nil || !wrote {
					t.Fatalf("the control = %v (wrote %v), want admitted", err, wrote)
				}
				if f.gates.lastOwner.HostID != "host-owner" || f.gates.lastOwner.HostGeneration != 9 {
					t.Fatalf("the capability was asked of %+v, want the fresh owner host-owner gen 9", f.gates.lastOwner)
				}
				return
			}
			if !IsCode(err, sessionwire.ErrorCodeGateNotResumable) || !errors.Is(err, ErrGateResponseUnsupported) {
				t.Fatalf("AdmitGateResponse = %v, want gate_not_resumable caused by ErrGateResponseUnsupported", err)
			}
			if wrote {
				t.Fatal("a gate response for an owner that cannot apply it was written to the inbox")
			}
		})
	}
}

// TestTheCapabilityIsAskedOnlyForAGateResponse: input, interrupt and restore
// never ask -- a Host applies them all -- so a gate that refused them would be
// a regression dressed as a safety check.
func TestTheCapabilityIsAskedOnlyForAGateResponse(t *testing.T) {
	f := newServiceFixture(t)
	resolvableSession(f)
	f.gates.refuse = true
	if _, _, err := f.service.AdmitInput(context.Background(), f.principal, sessionwire.InputRequest{
		CommandEnvelope: envelope("input-1"), SessionID: "session-a", Blocks: []byte(`[{"text":"hi"}]`)}); err != nil {
		t.Fatalf("an input was refused while the owner cannot apply gate responses: %v", err)
	}
	if f.gates.calls != 0 {
		t.Fatalf("an input asked the gate_response capability %d times", f.gates.calls)
	}
}
