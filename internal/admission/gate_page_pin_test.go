package admission

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
)

// TestAHostPublishedDispositionGateIsReadByThePinnedStore is the ROLLOUT RULE
// sessionstore v0.12.0 carries, driven against the released store: every
// ReadGates caller must be on sessionstore >= v0.12.0 BEFORE any Host publishes
// a gate, because an older reader refuses a page a Host wrote on a disposition
// session ("catalog sequence (gates)"). Factory is a ReadGates caller twice --
// the HTTP gate read and the gate-response admission's own projection read --
// so this module's pin is part of the rule.
//
// The gate is written exactly as a v0.4.0 Host writes it: on a
// DISPOSITION-bound session this module admitted, under a ResidencyGrant the
// store issued, with no LeaseEpoch. Three readers must then see it:
//
//   - ReadGates, with the request shape internal/httpapi's scope builds
//     (tenant and session only), and the page must survive Core's own marshal
//     validation, which is what the HTTP handler does next;
//   - the catalog record admission reads, whose OpenGates the gate-response
//     admission is decided from;
//   - AdmitGateResponse itself, which must get PAST the gate check -- this
//     fixture's directory reports no owner, so the answer is
//     gate_not_resumable, and gate_resolved would mean the gate was not seen.
func TestAHostPublishedDispositionGateIsReadByThePinnedStore(t *testing.T) {
	ctx := context.Background()
	store := openCreateIntegrationStore(t)
	svc, principal := newCreateIntegrationService(t, store, store, serviceNow, "runtime-command-gate")
	const session = sessionwire.SessionID("session-gate")
	if _, created, err := svc.AdmitCreate(ctx, principal, createRequest("create-gate", string(session), smallBlocks)); err != nil || !created {
		t.Fatalf("AdmitCreate = (%v, %v)", created, err)
	}
	grant, err := store.AcquireResidency(ctx, sessionstore.AcquireResidencyRequest{TenantID: principal.Tenant(), SessionID: session})
	if err != nil {
		t.Fatalf("AcquireResidency: %v", err)
	}
	t.Cleanup(func() { _ = grant.Release(context.Background()) })

	gate := sessionwire.GateProjection{
		GateID: "gate-a", Kind: "approval",
		Prompt:           sessionwire.GatePrompt{Title: "approve?"},
		OpenedEventID:    "event-a",
		OpenedJournalSeq: 3,
		Deadline:         serviceNow.Add(time.Minute),
		Answerability:    sessionwire.GateAnswerabilityResident,
	}
	if _, err := store.OpenGate(ctx, sessionstore.OpenGateRequest{
		TenantID: principal.Tenant(), SessionID: session, Gate: gate, Residency: grant,
	}); err != nil {
		t.Fatalf("a Host-style disposition OpenGate was refused by the pinned store: %v", err)
	}

	page, err := store.ReadGates(ctx, sessionstore.ReadGatesRequest{TenantID: principal.Tenant(), SessionID: session})
	if err != nil {
		t.Fatalf("ReadGates refused the page a Host published: %v", err)
	}
	if len(page.Gates) != 1 || page.Gates[0].GateID != "gate-a" {
		t.Fatalf("ReadGates = %+v, want the one gate the Host opened", page.Gates)
	}
	if _, err := page.MarshalJSON(); err != nil {
		t.Fatalf("the page fails Core's marshal validation, so the HTTP gate read would answer 500: %v", err)
	}

	entry, err := store.GetCatalogEntry(ctx, sessionstore.GetCatalogEntryRequest{TenantID: principal.Tenant(), SessionID: session})
	if err != nil {
		t.Fatal(err)
	}
	if len(entry.Record.OpenGates) != 1 {
		t.Fatalf("the catalog record admission reads carries %d open gates, want 1", len(entry.Record.OpenGates))
	}

	_, _, err = svc.AdmitGateResponse(ctx, principal, sessionwire.GateResponseRequest{
		CommandEnvelope: envelope("answer-a"), SessionID: session, GateID: "gate-a", Action: "submit",
		Values: map[string]json.RawMessage{"answer": json.RawMessage(`"yes"`)}, ExpectedOpenEventID: "event-a",
	})
	var refused *Error
	if !errors.As(err, &refused) || refused.Code != sessionwire.ErrorCodeGateNotResumable {
		t.Fatalf("AdmitGateResponse = %v, want gate_not_resumable (the gate was seen; no owner is live)", err)
	}
}
