package placement

import (
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
)

var policyNow = time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)

// pooledRecord is a session whose desired placement is pooled and whose target
// every helper below agrees with unless a case perturbs it.
func pooledRecord() sessionstore.CatalogRecord {
	return sessionstore.CatalogRecord{
		TenantID: "tenant-a", SessionID: "session-a", AgentID: "agent-a",
		RuntimeCompatibilityID: "runtime-v1",
		State:                  sessionwire.SessionStateIdle,
		Residency:              sessionwire.SessionResidencyCold,
		DesiredPlacement:       sessionwire.HostPlacementPooled,
		DesiredGeneration:      1,
	}
}

func residentOwner(record sessionstore.CatalogRecord) sessionwire.HostLinkRegistryObservation {
	return sessionwire.HostLinkRegistryObservation{
		Version: sessionwire.CurrentWireVersion, TenantID: record.TenantID, SessionID: record.SessionID,
		HostID: "host-owner", HostGeneration: 3, AgentID: record.AgentID,
		RuntimeCompatibilityID: record.RuntimeCompatibilityID, Placement: record.DesiredPlacement,
		InternalEndpoint: "wss://host-owner.internal",
		Residency:        sessionwire.SessionResidencyResident, Accepting: true, LeaseEpoch: 4,
		ObservedAt: policyNow, ExpiresAt: policyNow.Add(time.Minute),
	}
}

func pooledCandidate(host sessionwire.HostID, capacity uint64) sessionwire.HostLinkCapacityReport {
	return sessionwire.HostLinkCapacityReport{
		Version: sessionwire.CurrentWireVersion, HostID: host, HostGeneration: 1,
		AgentID: "agent-a", RuntimeCompatibilityID: "runtime-v1",
		Placement: sessionwire.HostPlacementPooled, InternalEndpoint: sessionwire.InternalEndpoint("wss://" + string(host) + ".internal"),
		IsolationClass: sessionwire.HostIsolationClassCrossTenantIsolated,
		Accepting:      true, AvailableCapacity: capacity,
		ObservedAt: policyNow, ExpiresAt: policyNow.Add(time.Minute),
	}
}

// TestAFreshCompatibleOwnerIsPreferredToEveryCandidate is specification
// section 15 step 1: an owned session is routed to its owner, and no placement
// happens at all.
func TestAFreshCompatibleOwnerIsPreferredToEveryCandidate(t *testing.T) {
	t.Parallel()

	record := pooledRecord()
	owner := residentOwner(record)
	decision := Decide(record, owner, true, []sessionwire.HostLinkCapacityReport{pooledCandidate("host-free", 99)}, policyNow)
	if decision.Outcome != OutcomeReuseOwner {
		t.Fatalf("Outcome = %v, want %v", decision.Outcome, OutcomeReuseOwner)
	}
	if decision.Owner.HostID != "host-owner" || decision.Owner.LeaseEpoch != 4 {
		t.Errorf("Owner = %+v, want the observed owner with its lease epoch", decision.Owner)
	}
	if decision.Target != (sessionwire.HostLinkCapacityReport{}) {
		t.Errorf("Target = %+v, want no candidate selected", decision.Target)
	}
}

// TestAnOwnerIsReusedOnlyWhileItIsTheSessionsOwnLiveRuntime drives every way a
// registry observation stops being reusable. Each case is one perturbation of
// an otherwise reusable owner, so a pass cannot be explained by the fixture.
func TestAnOwnerIsReusedOnlyWhileItIsTheSessionsOwnLiveRuntime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		perturb func(*sessionwire.HostLinkRegistryObservation)
	}{
		{name: "runtime mismatch", perturb: func(o *sessionwire.HostLinkRegistryObservation) { o.RuntimeCompatibilityID = "runtime-v2" }},
		{name: "agent mismatch", perturb: func(o *sessionwire.HostLinkRegistryObservation) { o.AgentID = "agent-b" }},
		{name: "placement mismatch", perturb: func(o *sessionwire.HostLinkRegistryObservation) { o.Placement = sessionwire.HostPlacementDedicated }},
		{name: "another tenant", perturb: func(o *sessionwire.HostLinkRegistryObservation) { o.TenantID = "tenant-b" }},
		{name: "another session", perturb: func(o *sessionwire.HostLinkRegistryObservation) { o.SessionID = "session-b" }},
		{name: "releasing", perturb: func(o *sessionwire.HostLinkRegistryObservation) { o.Residency = sessionwire.SessionResidencyReleasing }},
		{name: "not admitting", perturb: func(o *sessionwire.HostLinkRegistryObservation) { o.Accepting = false }},
		{name: "expired", perturb: func(o *sessionwire.HostLinkRegistryObservation) { o.ExpiresAt = policyNow }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			record := pooledRecord()
			owner := residentOwner(record)
			test.perturb(&owner)
			if ReusableOwner(owner, record, policyNow) {
				t.Fatal("owner reported reusable")
			}
			decision := Decide(record, owner, true, []sessionwire.HostLinkCapacityReport{pooledCandidate("host-free", 5)}, policyNow)
			if decision.Outcome != OutcomeAttachPooled || decision.Target.HostID != "host-free" {
				t.Fatalf("decision = %+v, want the pooled candidate instead of the owner", decision)
			}
		})
	}
}

// TestAnObservationTheDirectoryDidNotReportIsNotAnOwner holds the BOOLEAN
// rather than the value. It hands Decide an observation that is reusable in
// every member and reports it as unobserved, so a policy that read the value
// without the flag would reuse a Host the registry does not name. Passing the
// zero observation instead would prove nothing: a zero value fails the
// comparison on its own.
func TestAnObservationTheDirectoryDidNotReportIsNotAnOwner(t *testing.T) {
	t.Parallel()

	record := pooledRecord()
	owner := residentOwner(record)
	if !ReusableOwner(owner, record, policyNow) {
		t.Fatal("the fixture owner is not reusable, so this case proves nothing")
	}
	decision := Decide(record, owner, false, []sessionwire.HostLinkCapacityReport{pooledCandidate("host-free", 5)}, policyNow)
	if decision.Outcome != OutcomeAttachPooled || decision.Target.HostID != "host-free" {
		t.Fatalf("decision = %+v, want a pooled candidate rather than an unobserved owner", decision)
	}

	empty := Decide(record, sessionwire.HostLinkRegistryObservation{}, false, nil, policyNow)
	if empty.Outcome != OutcomeNoCapacity {
		t.Fatalf("Outcome with no owner and no candidates = %v, want %v", empty.Outcome, OutcomeNoCapacity)
	}
}

// TestPooledSelectionTakesTheStoresRankOrderWithinCurrentCapacity covers the
// admissible/inadmissible candidate rules one perturbation at a time.
func TestPooledSelectionTakesTheStoresRankOrderWithinCurrentCapacity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		perturb func(*sessionwire.HostLinkCapacityReport)
	}{
		{name: "no free capacity", perturb: func(c *sessionwire.HostLinkCapacityReport) { c.AvailableCapacity = 0 }},
		{name: "not accepting", perturb: func(c *sessionwire.HostLinkCapacityReport) { c.Accepting = false }},
		{name: "runtime mismatch", perturb: func(c *sessionwire.HostLinkCapacityReport) { c.RuntimeCompatibilityID = "runtime-v2" }},
		{name: "agent mismatch", perturb: func(c *sessionwire.HostLinkCapacityReport) { c.AgentID = "agent-b" }},
		{name: "dedicated advertisement", perturb: func(c *sessionwire.HostLinkCapacityReport) { c.Placement = sessionwire.HostPlacementDedicated }},
		{name: "tenant exclusive", perturb: func(c *sessionwire.HostLinkCapacityReport) {
			c.IsolationClass = sessionwire.HostIsolationClassTenantExclusive
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			record := pooledRecord()
			rejected := pooledCandidate("host-rejected", 100)
			test.perturb(&rejected)
			admissible := pooledCandidate("host-admissible", 1)

			// The rejected candidate is ranked FIRST and offers the most
			// capacity, so selecting the second one can only be the rule.
			decision := Decide(record, sessionwire.HostLinkRegistryObservation{}, false,
				[]sessionwire.HostLinkCapacityReport{rejected, admissible}, policyNow)
			if decision.Outcome != OutcomeAttachPooled || decision.Target.HostID != "host-admissible" {
				t.Fatalf("decision = %+v, want host-admissible", decision)
			}

			only := Decide(record, sessionwire.HostLinkRegistryObservation{}, false,
				[]sessionwire.HostLinkCapacityReport{rejected}, policyNow)
			if only.Outcome != OutcomeNoCapacity {
				t.Fatalf("Outcome with only the rejected candidate = %v, want %v", only.Outcome, OutcomeNoCapacity)
			}
			if only.Target != (sessionwire.HostLinkCapacityReport{}) {
				t.Errorf("Target = %+v, want no selection", only.Target)
			}
		})
	}
}

// TestPooledSelectionIsDeterministicAmongEqualCandidates is what makes two
// replicas reaching the same page reach the same Host without a leader.
func TestPooledSelectionIsDeterministicAmongEqualCandidates(t *testing.T) {
	t.Parallel()

	record := pooledRecord()
	candidates := []sessionwire.HostLinkCapacityReport{
		pooledCandidate("host-a", 4), pooledCandidate("host-b", 4), pooledCandidate("host-c", 4),
	}
	for i := range 64 {
		decision := Decide(record, sessionwire.HostLinkRegistryObservation{}, false, candidates, policyNow)
		if decision.Outcome != OutcomeAttachPooled || decision.Target.HostID != "host-a" {
			t.Fatalf("decision %d = %+v, want the first ranked candidate every time", i, decision)
		}
	}
}

// TestDedicatedPlacementIsReconciledRatherThanSelected is specification
// section 13: a dedicated Host is created for one session, so an advertised
// pooled seat is not an alternative to creating it.
func TestDedicatedPlacementIsReconciledRatherThanSelected(t *testing.T) {
	t.Parallel()

	record := pooledRecord()
	record.DesiredPlacement = sessionwire.HostPlacementDedicated
	decision := Decide(record, sessionwire.HostLinkRegistryObservation{}, false,
		[]sessionwire.HostLinkCapacityReport{pooledCandidate("host-free", 99)}, policyNow)
	if decision.Outcome != OutcomeReconcileDedicated {
		t.Fatalf("Outcome = %v, want %v", decision.Outcome, OutcomeReconcileDedicated)
	}
	if decision.Target != (sessionwire.HostLinkCapacityReport{}) {
		t.Errorf("Target = %+v, want no pooled seat selected for a dedicated session", decision.Target)
	}
}

// TestAnUnknownDesiredPlacementSelectsNothing fails closed on a record this
// build does not understand rather than defaulting it to pooled.
func TestAnUnknownDesiredPlacementSelectsNothing(t *testing.T) {
	t.Parallel()

	record := pooledRecord()
	record.DesiredPlacement = sessionwire.HostPlacement("elsewhere")
	decision := Decide(record, sessionwire.HostLinkRegistryObservation{}, false,
		[]sessionwire.HostLinkCapacityReport{pooledCandidate("host-free", 99)}, policyNow)
	if decision.Outcome != OutcomeUndecided {
		t.Fatalf("Outcome = %v, want %v", decision.Outcome, OutcomeUndecided)
	}
}

// TestEveryOutcomeRendersItsOwnName keeps a log line from reporting two
// different decisions with one string.
func TestEveryOutcomeRendersItsOwnName(t *testing.T) {
	t.Parallel()

	seen := map[string]Outcome{}
	for _, outcome := range []Outcome{
		OutcomeUndecided, OutcomeReuseOwner, OutcomeAttachPooled, OutcomeReconcileDedicated, OutcomeNoCapacity,
	} {
		name := outcome.String()
		if name == "" {
			t.Fatalf("outcome %d renders as the empty string", outcome)
		}
		if previous, ok := seen[name]; ok {
			t.Fatalf("outcomes %d and %d both render as %q", previous, outcome, name)
		}
		seen[name] = outcome
	}
	if unknown := Outcome(200).String(); unknown == "" {
		t.Fatal("an unrecognized outcome renders as the empty string")
	}
}
