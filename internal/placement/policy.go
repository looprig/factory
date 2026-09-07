// Package placement decides where a session should run, and reconciles the
// durable desired state that decision implies.
//
// It is split into a PURE half and an I/O half, and the split is the package's
// main structural claim. policy.go is a total function of a catalog record, an
// optional registry observation and a page of capacity reports; it reads no
// clock of its own, performs no store call, and cannot create anything. Every
// rule a reviewer has to check for correctness lives there, where a test drives
// it directly rather than through a store. reconciler.go holds the claim, the
// durable desired-state write and the workload controller call, and its only
// decision is which of policy.go's answers it acted on.
//
// A CANDIDATE IS A HINT AND AN OWNER IS AN OBSERVATION. Nothing here is
// authority over a session: specification section 15 keeps the Host lease and
// its epoch fence as the correctness fence, and a placement claim suppresses
// duplicate scaling only. Every type this package hands out is therefore a
// REQUEST or a HINT — sessionstore.PlacementIntent for the controller, a
// capacity report for the caller that will attach — and none of them carries a
// lease epoch this package could pretend to hold.
package placement

import (
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
)

// Outcome is the closed set of answers Decide gives.
//
// It is an enum rather than a pair of booleans so a caller's handling is
// exhaustive by construction, and Undecided is a member rather than a zero to
// fall through: a record naming a desired placement this build does not
// understand must reach a caller as "no decision", not as the first arm of a
// switch that happens to be pooled.
type Outcome uint8

const (
	// OutcomeUndecided means no rule here applies to the record, which today
	// means its desired placement is neither of the two admission models.
	OutcomeUndecided Outcome = iota

	// OutcomeReuseOwner means the session already has a live owner and no
	// placement is needed. Specification section 15 step 1.
	OutcomeReuseOwner

	// OutcomeAttachPooled names one ranked pooled candidate the caller should
	// ask to take the session. It is not a reservation: the candidate may
	// refuse, and only its lease acquisition decides ownership.
	OutcomeAttachPooled

	// OutcomeReconcileDedicated means one workload should exist for this one
	// session. Specification section 13.
	OutcomeReconcileDedicated

	// OutcomeNoCapacity means the target has no admissible seat right now.
	// It is not an error: pooled capacity is scaled by an autoscaler outside
	// this module, and the caller retries.
	OutcomeNoCapacity
)

// String renders the outcome for a log line. Every member renders distinctly,
// including an unrecognized one, so a decision is never reported as the empty
// string.
func (o Outcome) String() string {
	switch o {
	case OutcomeUndecided:
		return "undecided"
	case OutcomeReuseOwner:
		return "reuse_owner"
	case OutcomeAttachPooled:
		return "attach_pooled"
	case OutcomeReconcileDedicated:
		return "reconcile_dedicated"
	case OutcomeNoCapacity:
		return "no_capacity"
	default:
		return "unrecognized"
	}
}

// Decision is one placement answer together with exactly the record the
// outcome licenses acting on.
//
// Owner is populated only for OutcomeReuseOwner and Target only for
// OutcomeAttachPooled; the other member stays zero. That is deliberate rather
// than tidy: a decision carrying both would let a caller act on the one its
// switch reached rather than the one the policy chose.
type Decision struct {
	Outcome Outcome
	Owner   sessionwire.HostLinkRegistryObservation
	Target  sessionwire.HostLinkCapacityReport
}

// ReusableOwner reports whether a registry observation is this session's own
// live, admitting owner.
//
// It is THE statement of that rule for the whole module: internal/admission
// asks the same question before delivering a gate response to a resident Host,
// and a second copy of the comparison would be free to admit an owner one
// caller routes to and the other does not.
//
// Placement is compared along with agent and runtime because a record's desired
// placement is what its target key is built from; an observation under the
// other admission model is a Host the directory would never have offered.
// Migration between the two is deferred by specification section 14, so this
// reports false rather than arranging one — the caller re-places a session that
// is not resident anywhere, and never moves a running one.
//
// The expiry is read against the caller's clock even though the directory
// already refuses an expired registration with the STORE's. The two answers
// differ only under skew, and the conservative one is right here: the cost of
// treating a live owner as absent is one re-placement that the Host lease then
// refuses, and the cost of the opposite is routing a command to a process that
// has lost its lease.
func ReusableOwner(owner sessionwire.HostLinkRegistryObservation, record sessionstore.CatalogRecord, now time.Time) bool {
	return owner.TenantID == record.TenantID && owner.SessionID == record.SessionID &&
		owner.AgentID == record.AgentID && owner.RuntimeCompatibilityID == record.RuntimeCompatibilityID &&
		owner.Placement == record.DesiredPlacement &&
		owner.Residency == sessionwire.SessionResidencyResident && owner.Accepting && owner.ExpiresAt.After(now)
}

// Decide chooses where a session should run.
//
// ownerObserved is separate from owner, and the separation is load-bearing: an
// absent registration is the ZERO observation, and a policy that read the value
// alone would compare an empty tenant and an empty agent against a record whose
// members a caller had not filled either, and reuse a Host that does not exist.
//
// candidates arrive in the store's capacity rank, most free capacity first, and
// nothing here re-ranks them. The first ADMISSIBLE entry is selected, so two
// Factory replicas reading the same page choose the same Host with no leader
// and no shared state — which is what H5's "replicas are safe and no leader is
// introduced" rests on for the pooled half.
//
// The page's liveness is the store's answer, not this function's: routing's
// Directory already drops lapsed advertisements against the store's clock, and
// re-deciding expiry here against a Factory clock would take a live Host out of
// service on skew. The owner's expiry IS re-read here, for the opposite reason
// ReusableOwner gives.
func Decide(
	record sessionstore.CatalogRecord,
	owner sessionwire.HostLinkRegistryObservation,
	ownerObserved bool,
	candidates []sessionwire.HostLinkCapacityReport,
	now time.Time,
) Decision {
	if ownerObserved && ReusableOwner(owner, record, now) {
		return Decision{Outcome: OutcomeReuseOwner, Owner: owner}
	}
	switch record.DesiredPlacement {
	case sessionwire.HostPlacementDedicated:
		// A dedicated Host is CONSTRUCTED for one session, so an advertised
		// seat elsewhere is not an alternative to constructing it. The
		// dedicated Host for THIS session, once it holds the lease, is reached
		// by the owner arm above and never by a capacity page.
		return Decision{Outcome: OutcomeReconcileDedicated}
	case sessionwire.HostPlacementPooled:
		for _, candidate := range candidates {
			if admissible(candidate, record) {
				return Decision{Outcome: OutcomeAttachPooled, Target: candidate}
			}
		}
		return Decision{Outcome: OutcomeNoCapacity}
	default:
		return Decision{Outcome: OutcomeUndecided}
	}
}

// admissible reports whether one advertisement is a seat this session may take.
//
// Agent, runtime and placement are re-checked although ListCompatibleHosts was
// scoped by exactly that triple. The check costs three comparisons and closes
// the case where the page came from a request built for a different record —
// the page is a value that can be passed around, and nothing in its type says
// which key produced it.
//
// THE ISOLATION RULE IS THE ONE WITH A COST WORTH READING. Specification
// section 12 permits a pooled Host to admit sessions from different tenants
// only when its advertised class provides per-session isolation, and makes
// Factory placement the enforcer for a Host without that class. Factory cannot
// see which tenants a Host is currently serving: SessionStore files targets
// under an (AgentID, RuntimeCompatibilityID, Placement) scope with no tenant
// dimension, so there is no query that answers "is this tenant-exclusive Host
// already this tenant's". The only enforcement available is therefore to refuse
// the class outright, which is what this does — a tenant_exclusive POOLED
// advertisement is never selected here.
//
// The cost is stated rather than left to be discovered: a deployment that wants
// tenant-exclusive pooled Hosts gets no pooled placement from them at all, and
// must use dedicated placement for that isolation until the target directory
// carries a tenant dimension (H8's per-tenant Department, a specification
// section 7 change, which is root's to book). Failing closed is chosen over
// admitting one because the failure this prevents is a cross-tenant admission
// and the failure it causes is a session that does not start.
func admissible(candidate sessionwire.HostLinkCapacityReport, record sessionstore.CatalogRecord) bool {
	return candidate.AgentID == record.AgentID &&
		candidate.RuntimeCompatibilityID == record.RuntimeCompatibilityID &&
		candidate.Placement == record.DesiredPlacement &&
		candidate.IsolationClass == sessionwire.HostIsolationClassCrossTenantIsolated &&
		candidate.Accepting && candidate.AvailableCapacity > 0
}
