package factory

import (
	"context"
	"fmt"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	"github.com/looprig/factory/internal/admission"
	"github.com/looprig/factory/internal/httpapi"
	"github.com/looprig/sessionstore"
)

// The seams this stage adds, and why each is a SEPARATE interface rather than
// a widening of one that already existed.
//
// Every interface below obeys the rule server.go states: it is the union of
// the narrow interfaces the internal packages that call it declare, so a
// deployer supplies one object and no adapter stands between it and its
// consumers. server_test.go asserts that in both directions.
//
// They are separate because the option surface is where a deployment says what
// it is composed to DO, and a seam that fused the read plane with the desired-
// state writer would make a read-only Factory unbuildable -- the property
// internal/httpapi's nil-Admissions rule already protects at the router.

// Catalog is the durable session record: the read every decision is made
// against, and the desired state placement authors.
//
// CreateCatalogEntry is still required but no Factory path calls it: legacy
// create is refused before any durable write, and a V1 create is authored
// through PublicCreates. Narrowing the interface is booked for a minor release,
// because removing a method from an exported interface breaks callers holding
// a Catalog.
//
// GetCatalogEntry also appears on SessionReader. That is not a second
// authority: it is one method on one deployer-supplied object reached through
// two seams, because internal/admission and internal/placement each declare the
// read beside a write that SessionReader must not carry. Widening SessionReader
// instead would put CreateCatalogEntry within reach of every read handler.
type Catalog interface {
	GetCatalogEntry(ctx context.Context, req sessionstore.GetCatalogEntryRequest) (sessionstore.CatalogEntry, error)
	CreateCatalogEntry(ctx context.Context, req sessionstore.CreateCatalogEntryRequest) (sessionstore.CatalogEntry, bool, error)
	UpdateCatalogDesiredState(ctx context.Context, req sessionstore.UpdateCatalogDesiredStateRequest) (sessionstore.CatalogEntry, error)
}

// Gates is the service-control gate-deadline plane the gate sweeper reads and
// retires.
//
// It carries RetireGateDeadlineIntent and NOTHING that could answer a gate.
// internal/reconcile's own two seams are narrow for that reason -- OpenGate
// would resurrect a deadline and ResolveGate is a synthesized resolution the
// runbook forbids -- and this seam is their exact union, so the composition
// cannot hand the sweeper a capability its own contract refuses.
type Gates interface {
	ControlShards() int
	ListDueGates(ctx context.Context, req sessionstore.ListDueGatesRequest) (sessionstore.DueGatePage, error)
	RetireGateDeadlineIntent(ctx context.Context, req sessionstore.RetireGateDeadlineIntentRequest) error
}

// HostTargets is the Host advertisement index the record sweeper reconciles.
//
// One method. A due advertisement is a hint about which rows to LOOK at and is
// never evidence that a Host is gone, so the seam offers no way to read one
// and act on it directly.
type HostTargets interface {
	ReconcileHostTargets(ctx context.Context, req sessionstore.ReconcileHostTargetsRequest) (sessionstore.HostTargetReconcileResult, error)
}

// HostLinkCredential supplies the SERVICE identity this replica presents to a
// Host on every dial and every reconnect.
//
// It is deliberately not identity.Principal: a HostLink carries no end user.
// The Host authorizes a bind against the session's durable lease, not against
// a user this connection claims to act for.
type HostLinkCredential interface {
	ServiceToken(ctx context.Context) (string, error)
}

// LaunchTemplate is one launch target this deployment is configured to offer.
//
// It is a COMPOSITION type and not an alias of the one internal/httpapi
// publishes, and the extra member is exactly why. httpapi.LaunchTemplate
// carries identity and nothing else, deliberately: /v1/agents publishes it, and
// a published launch target that restated a runtime definition would be the
// competing static agent catalogue the specification forbids Factory to keep.
// Workload is a DEPLOYMENT's opaque platform payload -- a pod spec, a Nomad
// job -- which a tenant must never be shown. So the composition holds both
// halves and PROJECTS the published half; see Server.composeDepartment.
//
// Splitting them this way is what makes the projection the only path: there is
// no field on the published type for a workload to leak through.
type LaunchTemplate struct {
	// Key is the launch target's identity: the triple SessionStore files an
	// advertisement under, so a configured template and an advertised one are
	// the same value rather than two spellings of one.
	Key sessionstore.HostTargetKey

	// Capabilities are the presentation strings this deployment publishes.
	Capabilities []string

	// Workload is the platform payload a DEDICATED session's workload is
	// created from. It is required for a dedicated target and must be absent
	// for a pooled one: a pooled session runs on a Host that already exists,
	// so a workload configured for it would be a value nothing could ever
	// create.
	Workload sessionstore.DesiredWorkload
}

// Validate reports why this template may not be used.
//
// It defers the identity half to the published type rather than restating it,
// so the two cannot drift, and adds only the rule that belongs to the member
// the published type does not have.
func (t LaunchTemplate) Validate() error {
	if err := t.published().Validate(); err != nil {
		return err
	}
	switch t.Key.Placement {
	case sessionwire.HostPlacementDedicated:
		if t.Workload.PayloadVersion == "" || len(t.Workload.Payload) == 0 {
			return fmt.Errorf("%w: a dedicated LaunchTemplate needs a Workload to create", ErrInvalidLaunchTemplate)
		}
	default:
		if t.Workload.PayloadVersion != "" || len(t.Workload.Payload) > 0 {
			return fmt.Errorf("%w: a pooled LaunchTemplate names a Workload, which nothing would create", ErrInvalidLaunchTemplate)
		}
	}
	return nil
}

// published projects the half /v1/agents may show.
func (t LaunchTemplate) published() httpapi.LaunchTemplate {
	return httpapi.LaunchTemplate{Key: t.Key, Capabilities: t.Capabilities}
}

// ObjectPolicy authorizes one object reference using trusted committed session
// evidence, and it is an alias for LaunchTemplate's reason.
// A denial must wrap identity.ErrUnauthorized; any other error is a policy
// dependency fault.
//
// A nil policy fails closed at the router: an object read is answered
// "unavailable" before the catalog summary, before any authorization call and
// before any reader is reached. That is why supplying one is OPTIONAL here and
// why supplying one without an ObjectStoreResolver is REFUSED -- see
// WithObjectPolicy.
type ObjectPolicy = httpapi.ObjectPolicy

// ObjectReader is the neutral read capability a frozen binding resolves to.
type ObjectReader = httpapi.ObjectReader

// ObjectStoreResolver consumes a session's full immutable binding and answers
// the store its objects live in, refusing any configuration this deployment
// does not know.
//
// The refusal is the point. A binding names a StorageBindingID and a
// BindingVersion chosen at create and immutable afterwards; a resolver that
// answered a default for an unknown one would serve one deployment's bytes
// under another's configuration.
type ObjectStoreResolver func(ctx context.Context, binding sessionstore.SessionBinding) (ObjectReader, error)

// ---------------------------------------------------------------------------
// Options for the seams above.
// ---------------------------------------------------------------------------

// WithCatalog supplies the durable session record plane. Required.
func WithCatalog(c Catalog) Option {
	return option("WithCatalog", func(cfg *config) error {
		if c == nil {
			return nilDependency("WithCatalog")
		}
		cfg.catalog = c
		return nil
	})
}

// WithGates supplies the service-control gate-deadline plane. Required.
func WithGates(g Gates) Option {
	return option("WithGates", func(cfg *config) error {
		if g == nil {
			return nilDependency("WithGates")
		}
		cfg.gates = g
		return nil
	})
}

// WithHostTargets supplies the Host advertisement index. Required.
func WithHostTargets(t HostTargets) Option {
	return option("WithHostTargets", func(cfg *config) error {
		if t == nil {
			return nilDependency("WithHostTargets")
		}
		cfg.hostTargets = t
		return nil
	})
}

// WithHostLinkCredential supplies the service token this replica presents to a
// Host. Required.
func WithHostLinkCredential(c HostLinkCredential) Option {
	return option("WithHostLinkCredential", func(cfg *config) error {
		if c == nil {
			return nilDependency("WithHostLinkCredential")
		}
		cfg.hostCredential = c
		return nil
	})
}

// WithServiceIdentity names the principal the cross-tenant sweeps run as.
//
// It is REQUIRED and it is checked for Kind service here, at composition,
// rather than trusted: every sweep this Server drives calls
// AuthorizeServiceSweep with this value, and a tenant principal reaching that
// call is a cross-tenant read authorized as a tenant's.
func WithServiceIdentity(p identity.Principal) Option {
	return option("WithServiceIdentity", func(cfg *config) error {
		if !p.IsService() {
			return &OptionError{Option: "WithServiceIdentity", Err: ErrNotAServiceIdentity}
		}
		cfg.service = p
		cfg.serviceSet = true
		return nil
	})
}

// WithReplicaID names this replica in every claim it takes. Required.
//
// It is not authority -- a claim licenses nothing -- but it must be STABLE for
// a process: extending one's own claim and taking over a crashed replica's
// lapsed one are the same store write, and the holder is what tells them
// apart. It is also what a record sweep releases by, so a replica that
// reported two holders would leave its own claims behind.
func WithReplicaID(id string) Option {
	return option("WithReplicaID", func(cfg *config) error {
		if id == "" {
			return &OptionError{Option: "WithReplicaID", Err: ErrEmptyReplicaID}
		}
		cfg.replicaID = id
		return nil
	})
}

// WithVersion names the Factory build identity reported to a connecting client
// and to a Host. Optional; DefaultVersion is used when it is not supplied.
func WithVersion(v string) Option {
	return option("WithVersion", func(cfg *config) error {
		if v == "" {
			return &OptionError{Option: "WithVersion", Err: ErrEmptyVersion}
		}
		cfg.version = v
		return nil
	})
}

// WithDepartment supplies the launch targets this deployment offers.
//
// An EMPTY department is a supported composition and is the default: a Factory
// serving an existing tenant's durable history needs no launchable agent, and
// answers /v1/agents with an empty list, which is the truthful answer. Every
// entry is validated here, so a malformed template is a composition failure an
// operator sees rather than a request-time decision.
func WithDepartment(templates ...LaunchTemplate) Option {
	return option("WithDepartment", func(cfg *config) error {
		for i, template := range templates {
			if err := template.Validate(); err != nil {
				return &OptionError{Option: "WithDepartment", Err: fmt.Errorf("entry %d: %w", i, err)}
			}
		}
		cfg.department = cloneTemplates(templates)
		return nil
	})
}

// WithWorkloadController supplies the platform adapter a DEDICATED session's
// workload is created through. Optional, and deliberately so.
//
// cmd/factory composes none: H5 puts the platform adapter in a separate
// controller binary, whose ServiceAccount holds the workload RBAC that the
// Factory's does not. A placement reconciler composed without one refuses a
// dedicated session with a named error instead of reporting success for work
// nothing did, which is what keeps the split from being a convention.
func WithWorkloadController(w WorkloadController) Option {
	return option("WithWorkloadController", func(cfg *config) error {
		if w == nil {
			return nilDependency("WithWorkloadController")
		}
		cfg.workloads = w
		return nil
	})
}

// WithObjectPolicy supplies the committed-evidence policy that authorizes an
// object reference. Optional; without one every object read answers
// "unavailable" before any reader is reached.
//
// Supplying it WITHOUT WithObjectStoreResolver is refused by New, and that
// refusal is the whole reason these are two options rather than one value. The
// router resolves a store only for a NON-ZERO binding and falls back to the
// read plane for a zero (legacy) one; a nil policy refuses first and
// unconditionally, so today the fallback is unreachable. A composition that
// supplied a policy alone would make it reachable in the same change, with no
// resolver behind it.
func WithObjectPolicy(p ObjectPolicy) Option {
	return option("WithObjectPolicy", func(cfg *config) error {
		if p == nil {
			return nilDependency("WithObjectPolicy")
		}
		cfg.objectPolicy = p
		return nil
	})
}

// SessionBindingTemplate is the deployment-configuration half of the immutable
// SessionBinding every session this Factory creates is pinned to.
//
// It names the two members ObjectStoreResolver keys on, and that is not a
// coincidence -- it is the argument for supplying it here at all. The other two
// members of a store SessionBinding are not a deployment's to choose:
// RuntimeSessionID is derived per create from the create's own identity, and
// ProtocolMode is fixed to disposition because a legacy-bound session is one no
// Host can take residency on.
type SessionBindingTemplate = admission.SessionBindingTemplate

// PublicCreates is the durable public-create plane a V1 create admits into. It
// is the DISPOSITION family, which a *sessionstore.Store satisfies.
type PublicCreates = admission.PublicCreateStore

// WithPublicCreates supplies the durable plane a V1 create admits into.
// Optional, and it travels with WithSessionBinding: both halves are required
// before a create is served, and neither implies the other -- a plane with no
// configured binding has nothing to pin, and a binding with no plane has
// nowhere to put it.
func WithPublicCreates(p PublicCreates) Option {
	return option("WithPublicCreates", func(cfg *config) error {
		if p == nil {
			return nilDependency("WithPublicCreates")
		}
		cfg.publicCreates = p
		return nil
	})
}

// WithSessionBinding supplies the storage configuration every created session
// is permanently pinned to. Without it a V1 create is refused
// runtime_unavailable and every other operation is unaffected.
//
// It is REFUSED without WithObjectStoreResolver, for a sharper reason than
// WithObjectPolicy's. A SessionBinding is IMMUTABLE AFTER CREATE: a session
// pinned to a StorageBindingID this deployment's resolver cannot resolve has
// permanently unreadable objects, and there is no repair short of an offline
// migration. Requiring the resolver does not prove the resolver knows THIS
// binding -- nothing at composition time can -- but it does refuse the one
// composition that is certainly wrong, which is pinning sessions to a storage
// configuration in a deployment that resolves no storage at all.
func WithSessionBinding(storageBindingID, bindingVersion string) Option {
	return option("WithSessionBinding", func(cfg *config) error {
		if storageBindingID == "" || bindingVersion == "" {
			return ErrIncompleteSessionBinding
		}
		cfg.sessionBinding = SessionBindingTemplate{
			StorageBindingID: storageBindingID, BindingVersion: bindingVersion,
		}
		return nil
	})
}

// WithObjectStoreResolver supplies the binding-to-store resolution an object
// read uses. See WithObjectPolicy for why the two travel together.
func WithObjectStoreResolver(r ObjectStoreResolver) Option {
	return option("WithObjectStoreResolver", func(cfg *config) error {
		if r == nil {
			return nilDependency("WithObjectStoreResolver")
		}
		cfg.objectStores = r
		return nil
	})
}

// WithObjectLimits bounds retained response memory and verification I/O.
func WithObjectLimits(l ObjectLimits) Option {
	return option("WithObjectLimits", func(cfg *config) error { cfg.objects = l; return nil })
}

// ObjectLimits bounds retained response memory and whole-object verification.
type ObjectLimits = httpapi.ObjectLimits

// DefaultObjectLimits retains at most 1 MiB per request and verifies at most
// 64 MiB.
func DefaultObjectLimits() ObjectLimits { return httpapi.DefaultObjectLimits() }

// DefaultVersion is the build identity a composition naming none reports.
const DefaultVersion = "factory"

// cloneTemplates copies the caller's WORKLOAD payload, and only that.
//
// Capabilities are deliberately NOT copied here even though they are a slice a
// composer could reuse, and the omission was measured rather than assumed: a
// mutant that dropped a copy of them survived the whole suite, because
// internal/httpapi's cloneDepartment already deep-copies the published
// template and is the value a running router advertises from. Copying them
// again would be a second authority for one rule -- the shape this module has
// had to correct before -- and the guard that holds it lives beside the value
// it protects.
//
// The workload is different in kind: httpapi never sees it, because the
// published projection has no field for it. So this is the ONLY copy, and a
// composer that reuses its payload buffer after New would otherwise change
// what a dedicated session is created from.
func cloneTemplates(templates []LaunchTemplate) []LaunchTemplate {
	copied := make([]LaunchTemplate, len(templates))
	for i, template := range templates {
		copied[i] = template
		copied[i].Workload.Payload = append([]byte(nil), template.Workload.Payload...)
	}
	return copied
}
