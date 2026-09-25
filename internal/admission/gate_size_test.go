package admission

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
)

// gateResponseOfSize returns a gate response whose CANONICAL command payload --
// the bytes admission measures and stores -- is exactly size bytes.
func gateResponseOfSize(t *testing.T, id string, size int) sessionwire.GateResponseRequest {
	t.Helper()
	build := func(n int) sessionwire.GateResponseRequest {
		return sessionwire.GateResponseRequest{CommandEnvelope: envelope(id), SessionID: "session-a", GateID: "gate-a", Action: "submit",
			Values: map[string]json.RawMessage{"answer": json.RawMessage(`"` + strings.Repeat("x", n) + `"`)}, ExpectedOpenEventID: "event-a"}
	}
	empty, err := canonicalCommand(build(0))
	if err != nil {
		t.Fatal(err)
	}
	req := build(size - len(empty))
	payload, err := canonicalCommand(req)
	if err != nil || len(payload) != size {
		t.Fatalf("calibrated payload is %d bytes (%v), want %d", len(payload), err, size)
	}
	return req
}

// TestAnOversizedGateResponseIsRefusedAtTheInlineBound (host v0.4.0 spec gate
// C1): a gate response is never stored by reference, because a Host blocks a
// session's whole command stream behind a by-reference gate response until its
// apply deadline. At exactly the inline bound it is admitted INLINE (the
// control); one byte over, it is refused invalid_request carrying
// ErrGateResponseTooLarge, and nothing is uploaded or written.
func TestAnOversizedGateResponseIsRefusedAtTheInlineBound(t *testing.T) {
	const bound = 64 << 10
	if sessionstore.MaxInboxPayloadBytes != bound {
		t.Fatalf("the store's inline bound is %d, want %d; re-read C1 before moving this", sessionstore.MaxInboxPayloadBytes, bound)
	}

	at := newServiceFixture(t)
	resolvableSession(at)
	entry, created, err := at.service.AdmitGateResponse(context.Background(), at.principal, gateResponseOfSize(t, "at-bound", bound))
	if err != nil || !created {
		t.Fatalf("a gate response of exactly %d bytes = (%v, %v), want admitted", bound, created, err)
	}
	if entry.Record.Descriptor.PayloadObject != nil || len(at.commands.uploads) != 0 || len(entry.Record.Descriptor.Payload) != bound {
		t.Fatalf("at the bound: object=%v uploads=%d inline=%d, want %d bytes inline and no upload",
			entry.Record.Descriptor.PayloadObject, len(at.commands.uploads), len(entry.Record.Descriptor.Payload), bound)
	}

	over := newServiceFixture(t)
	resolvableSession(over)
	_, _, err = over.service.AdmitGateResponse(context.Background(), over.principal, gateResponseOfSize(t, "over-bound", bound+1))
	if !IsCode(err, sessionwire.ErrorCodeInvalidRequest) || !errors.Is(err, ErrGateResponseTooLarge) {
		t.Fatalf("a gate response of %d bytes = %v, want invalid_request caused by ErrGateResponseTooLarge", bound+1, err)
	}
	if len(over.commands.uploads) != 0 || over.commands.lastAdmit.CommandID != "" {
		t.Fatalf("an oversized gate response wrote: uploads=%d admitted=%q", len(over.commands.uploads), over.commands.lastAdmit.CommandID)
	}
}

// TestAStoredOversizedGateResponseIsAnsweredFromItsRecordOnRetry (quality
// gate Q15): the size refusal runs AFTER the retry read. A gate response an
// earlier Factory admitted BY REFERENCE is a durable command; retrying it
// under the same CommandID and content must answer from that record, not be
// refused 400 as though it were new. With the check moved before the retry,
// the retry is refused.
func TestAStoredOversizedGateResponseIsAnsweredFromItsRecordOnRetry(t *testing.T) {
	f := newServiceFixture(t)
	resolvableSession(f)
	const size = 64<<10 + 100
	req := gateResponseOfSize(t, "stored-big", size)
	payload, err := canonicalCommand(req)
	if err != nil {
		t.Fatal(err)
	}
	// What an earlier Factory did: admit it by reference.
	stored, created, err := f.service.admit(context.Background(), "tenant-a", "session-a", "stored-big", CommandGateResponse, existingBinding, payload, members{})
	if err != nil || !created || stored.Record.Descriptor.PayloadObject == nil {
		t.Fatalf("seeding the by-reference record = (%+v, %v, %v)", stored.Record.Descriptor.PayloadObject, created, err)
	}
	retry, created, err := f.service.AdmitGateResponse(context.Background(), f.principal, req)
	if err != nil || created || retry.Record.Descriptor.CommandID != "stored-big" || retry.Record.Descriptor.PayloadObject == nil {
		t.Fatalf("the retry = (%+v, %v, %v), want the stored by-reference record, not a refusal", retry.Record.Descriptor, created, err)
	}
}
