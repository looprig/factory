package httpapi

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	internalidentity "github.com/looprig/factory/internal/identity"
	"github.com/looprig/sessionstore"
)

// ---------------------------------------------------------------------------
// The configured Department.
// ---------------------------------------------------------------------------

// LaunchTemplate is one launch target this deployment is configured to offer.
//
// It carries IDENTITY and nothing else: the agent, the runtime compatibility
// boundary, where it runs, and the capability strings a picker renders. It
// deliberately holds no tool, model, role, prompt or capability DEFINITION --
// specification section 7 makes rig.Rig the authority for those, and a launch
// target that restated them would be the "competing static agent catalogue"
// the specification forbids Factory to keep.
//
// # Why the deployment is asked at all
//
// /v1/agents is specified as an aggregate of current Department advertisements
// plus configured dedicated launch templates, and the advertisement half cannot
// supply the MEMBERSHIP on its own. SessionStore files a target's rows under an
// ordering scope derived by DIGEST from the (AgentID, RuntimeCompatibilityID,
// Placement) triple -- see sessionstore's deriveHostTargetScope -- so a reader
// can ask "which Hosts serve THIS target" and cannot ask "which targets exist".
// Measured against the released sessionstore v0.1.0: the only listing entry
// points are ListCompatibleHosts, which requires a whole HostTargetKey, and
// ReconcileHostTargets, which walks the DUE index and therefore sees exactly
// the rows that have already lapsed. There is no enumeration of the key space.
//
// So the deployment supplies the KEYS and the directory supplies the LIVENESS.
// That division is what keeps this from being a static catalogue: for a pooled
// target Factory never asserts launchability on its own say-so -- a configured
// pooled template that no Host currently advertises is not listed.
//
// Specification section 7 sanctions the configured half explicitly: "Factory
// may discover Department membership and Rig capabilities from Hosts or
// deployment configuration".
type LaunchTemplate struct {
	// Key is the launch target's identity: the triple SessionStore files an
	// advertisement under, so a configured template and an advertised one are
	// the same value rather than two spellings of one.
	Key sessionstore.HostTargetKey

	// Capabilities are the presentation strings this deployment publishes for
	// the agent. They are optional, and Core omits an empty list.
	Capabilities []string
}

// Validate reports why this template may not be used.
//
// It is called from NewRouter, so a template naming no agent is a composition
// failure rather than a response this handler has to decide what to do with at
// request time. A malformed configured template that reached the handler could
// only be dropped silently or fail every /v1/agents request; refusing to start
// is the answer that reaches an operator.
func (t LaunchTemplate) Validate() error {
	if err := t.Key.AgentID.Validate(); err != nil {
		return fmt.Errorf("%w: LaunchTemplate.Key.AgentID is not a valid identity: %v", ErrInvalidRouterConfig, err)
	}
	if t.Key.RuntimeCompatibilityID == "" {
		return fmt.Errorf("%w: LaunchTemplate.Key.RuntimeCompatibilityID is empty, so the template names no compatibility boundary", ErrInvalidRouterConfig)
	}
	switch t.Key.Placement {
	case sessionwire.HostPlacementPooled, sessionwire.HostPlacementDedicated:
	default:
		return fmt.Errorf("%w: LaunchTemplate.Key.Placement is %q, want %q or %q",
			ErrInvalidRouterConfig, t.Key.Placement, sessionwire.HostPlacementPooled, sessionwire.HostPlacementDedicated)
	}
	for _, capability := range t.Capabilities {
		if capability == "" {
			return fmt.Errorf("%w: LaunchTemplate.Capabilities contains an empty string", ErrInvalidRouterConfig)
		}
	}
	return nil
}

// agentProbePageLimit bounds the ONE directory page /v1/agents reads per
// configured pooled template.
//
// The bound and its cost are both derived from the mechanism rather than
// chosen. SessionStore ranks a target's rows by available capacity and drops a
// lapsed row from the page it is publishing WITHOUT unranking it -- its
// ListCompatibleHosts documentation says so, and counts the drops in
// LapsedSkipped precisely because the row still occupies a position in every
// later page. So the visible cost of reading one page is exact: a pooled target
// whose only live advertisements rank below this many rows is reported absent
// from /v1/agents. The condition that produces it is an accumulation of lapsed
// rows, and ReconcileHostTargets -- runbook task A4.1 step 2 -- is the only
// thing that removes them.
//
// Paging until a live row appears would trade that for an unbounded read on a
// public authenticated route, which is the worse failure: an attacker with any
// credential could make Factory walk a whole target's history on demand.
const agentProbePageLimit = 32

// ---------------------------------------------------------------------------
// GET /v1/agents and GET /v1/capabilities.
// ---------------------------------------------------------------------------

// serveAgents answers the launchable agent set.
//
// # Why this response is not tenant-scoped
//
// It is a decision, not an omission, and it is the same decision that lets the
// response carry an ETag.
//
// The two inputs have no tenant dimension to scope BY. The configured
// Department is deployment configuration. The advertisement directory is
// deliberately not partitioned by tenant -- sessionstore's hostTargetScope says
// why in its own words: a pooled target may serve several tenants and "a
// directory partitioned by tenant could not express that", so a row carries an
// isolation class and no tenant. Scoping the aggregate would therefore mean
// inventing a tenant filter over data that has none, and Authorizer declares no
// decision covering it because there is no resource for one to protect.
//
// The response is consequently a pure function of the deployment's
// configuration and the directory's current contents: byte-identical for every
// authenticated principal. That is asserted rather than assumed, and it is what
// makes the ETag safe -- a validator over a per-principal body would be a
// stable per-principal fingerprint.
//
// # What that costs, stated so it is not discovered later
//
// A deployment whose tenants may launch DIFFERENT agents cannot express that
// here. Every authenticated principal learns every configured AgentID and
// runtime compatibility ID in the deployment, which is topology rather than
// data but is still a disclosure across a tenant boundary. Nothing in the
// specification's Department model expresses a per-tenant agent set today; a
// deployment that needs one needs a per-tenant Department, which is a change to
// section 7 and not a filter here.
func (rt *Router) serveAgents() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		operation, authenticated := internalidentity.OperationContextFrom(r.Context())
		if !authenticated {
			// Unreachable through the composed chain, which authenticates
			// before it routes. It fails closed rather than reading the
			// directory on behalf of a request nothing authenticated.
			writeAPIError(w, authenticationFailure(identity.ErrUnauthenticated))
			return
		}
		summary, err := rt.launchableAgents(r.Context(), newScope(operation.Principal))
		if err != nil {
			writeAPIError(w, directoryFailure(err))
			return
		}
		body, err := summary.MarshalJSON()
		if err != nil {
			// Core validates on marshal. A summary this package assembled that
			// Core refuses is a fault here, not a caller's error.
			writeAPIError(w, internalFailure())
			return
		}
		// The validator is over the exact bytes that would be sent, so it
		// changes when and only when the response does.
		etag := entityTag(body)
		w.Header().Set("ETag", etag)
		if matchesEntityTag(r.Header.Values("If-None-Match"), etag) {
			// 304 carries no body and no Content-Type; the headers the
			// middleware set stay on it.
			w.WriteHeader(http.StatusNotModified)
			return
		}
		writeJSONBytes(w, http.StatusOK, body)
	})
}

// launchableAgents aggregates the configured Department against the directory.
//
// A DEDICATED template is listed unconditionally: its workload is created on
// demand by placement, so there is nothing for a Host to be advertising yet and
// requiring an advertisement would hide every dedicated agent until one
// happened to be running. A POOLED template is listed only while some Host
// currently advertises it, because pooled placement can only ever land on
// capacity that already exists.
//
// "Currently advertises" means SessionStore published a LIVE row for the
// target: the store drops rows whose heartbeat promise has lapsed from the page
// it returns. It deliberately does NOT mean an accepting row. Accepting is a
// statement about free capacity at this instant, which is placement's question
// (A4.2) and not this one -- an agent whose Hosts are all momentarily full is
// still an agent this deployment can launch, and hiding it from the picker
// would make the agent list flap with load.
func (rt *Router) launchableAgents(ctx context.Context, reads scope) (sessionwire.DepartmentCapabilitySummary, error) {
	// Keyed by the pair Core publishes, so two templates differing only in
	// placement -- a pooled and a dedicated deployment of one agent build --
	// become one advertised entry rather than a duplicate.
	type identity struct {
		agent   sessionwire.AgentID
		runtime string
	}
	live := map[identity][]string{}
	order := []identity{}
	for _, template := range rt.department {
		key := identity{agent: template.Key.AgentID, runtime: template.Key.RuntimeCompatibilityID}
		if template.Key.Placement == sessionwire.HostPlacementPooled {
			advertised, err := rt.targetIsAdvertised(ctx, reads, template.Key)
			if err != nil {
				return sessionwire.DepartmentCapabilitySummary{}, err
			}
			if !advertised {
				continue
			}
		}
		if _, seen := live[key]; !seen {
			order = append(order, key)
		}
		live[key] = append(live[key], template.Capabilities...)
	}
	agents := make([]sessionwire.AgentCapabilitySummary, 0, len(order))
	for _, key := range order {
		capabilities := live[key]
		slices.Sort(capabilities)
		agents = append(agents, sessionwire.AgentCapabilitySummary{
			AgentID:                key.agent,
			RuntimeCompatibilityID: key.runtime,
			Capabilities:           slices.Compact(capabilities),
		})
	}
	// The order is the response's, not the configuration's: an ETag over a body
	// whose member order followed a map or a slice the composer happened to
	// write would change without the answer changing.
	slices.SortFunc(agents, func(a, b sessionwire.AgentCapabilitySummary) int {
		if by := strings.Compare(string(a.AgentID), string(b.AgentID)); by != 0 {
			return by
		}
		return strings.Compare(a.RuntimeCompatibilityID, b.RuntimeCompatibilityID)
	})
	return sessionwire.DepartmentCapabilitySummary{Agents: agents}, nil
}

// targetIsAdvertised reports whether any Host currently offers this target.
//
// One bounded page, no continuation: see agentProbePageLimit for the bound and
// for exactly what it costs.
func (rt *Router) targetIsAdvertised(ctx context.Context, reads scope, key sessionstore.HostTargetKey) (bool, error) {
	page, err := rt.directory.Candidates(ctx, reads.launchTargetPage(key))
	if err != nil {
		return false, err
	}
	return len(page.Hosts) > 0, nil
}

// ---------------------------------------------------------------------------
// GET /v1/sessions.
// ---------------------------------------------------------------------------

// maxSessionPageLimit is the largest page this surface will ask SessionStore
// for, whatever a caller requests.
//
// A caller-supplied limit is an amplification lever: without a ceiling one
// request could ask for the tenant's whole catalogue, and the cost is paid by
// the store and by Factory's own memory rather than by the caller. Two hundred
// is well above what a picker renders and well below anything that makes one
// request expensive.
const maxSessionPageLimit = 200

// serveSessionList answers the tenant's recent-first durable session page.
//
// It is a PURE durable read. It does not connect to a Host, does not consult
// the target directory, and does not restore a cold session: a session's place
// in this list is a fact about SessionStore's catalog and nothing else, which
// is why an advertisement expiring cannot change it.
//
// It carries NO ETag, and that is the opposite decision from /v1/agents for the
// opposite reason. A strong validator over a tenant's private page is a stable
// fingerprint of that tenant's state -- it survives in proxy and browser logs
// where the body does not, and it answers "did this tenant's session list
// change" to anyone who can observe it. The revalidation it would buy is worth
// little here in any case: every accepted command moves a session's
// LastActiveAt, so the page a busy tenant polls changes between almost every
// pair of reads. The agent list has neither property, which is why it has one.
func (rt *Router) serveSessionList() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		operation, authenticated := internalidentity.OperationContextFrom(r.Context())
		if !authenticated {
			// Unreachable through the composed chain; serveRoute has already
			// refused an unauthenticated request. It fails closed rather than
			// building a scope from a zero principal, which would carry the
			// EMPTY tenant into a durable query.
			writeAPIError(w, authenticationFailure(identity.ErrUnauthenticated))
			return
		}
		limit, ok := sessionPageLimit(w, r)
		if !ok {
			return
		}
		page, err := rt.reads.ListSessions(r.Context(),
			newScope(operation.Principal).sessionPage(sessionwire.Cursor(r.URL.Query().Get("cursor")), limit))
		if err != nil {
			writeAPIError(w, catalogFailure(err))
			return
		}
		// Core's own type is forwarded rather than re-projected. Its
		// MarshalJSON validates, and part of what it validates is that the
		// page is in non-increasing LastActiveAt order -- so "recent-first" is
		// enforced on the way out by the vocabulary's own rule rather than by
		// this package trusting the store.
		body, err := page.SessionPage.MarshalJSON()
		if err != nil {
			writeAPIError(w, internalFailure())
			return
		}
		writeJSONBytes(w, http.StatusOK, body)
	})
}

// sessionPageLimit reads the caller's page size.
//
// An ABSENT limit is zero, which SessionStore reads as its own configured page
// size; that is the store's default and this package does not second-guess it.
// A limit that is PRESENT and unusable is a 400 rather than a silent clamp,
// because a caller that asked for -1, for "many" or for nothing at all has a
// bug and a clamped answer hides it. A limit above the ceiling IS clamped,
// because that caller asked a well-formed question this deployment answers more
// narrowly.
//
// Presence is read from the parsed values rather than from Query().Get, which
// cannot tell "?limit=" from an absent parameter and would silently accept the
// first. Two limits are refused for the same reason: Get would answer with
// whichever came first, so a caller sending "?limit=1&limit=500" would be
// served a page size it did not unambiguously ask for.
func sessionPageLimit(w http.ResponseWriter, r *http.Request) (int, bool) {
	values := r.URL.Query()["limit"]
	if len(values) == 0 {
		return 0, true
	}
	if len(values) > 1 {
		writeAPIError(w, apiError{
			status:  http.StatusBadRequest,
			code:    sessionwire.ErrorCodeInvalidRequest,
			message: "limit was given more than once",
		})
		return 0, false
	}
	limit, err := strconv.Atoi(values[0])
	if err != nil || limit < 1 {
		writeAPIError(w, apiError{
			status:  http.StatusBadRequest,
			code:    sessionwire.ErrorCodeInvalidRequest,
			message: "limit must be a positive whole number",
		})
		return 0, false
	}
	return min(limit, maxSessionPageLimit), true
}

// ---------------------------------------------------------------------------
// Entity tags.
// ---------------------------------------------------------------------------

// entityTag is a STRONG validator over the exact bytes of a response.
//
// It is a digest of the body rather than a version counter because there is no
// durable version to count: the answer is derived from configuration and from a
// directory several Hosts write independently, so the only thing that can say
// "this is the same answer" is the answer.
//
// Strong rather than weak: the comparison a client makes with it is over bytes
// that are byte-identical or are not, and a weak tag would promise semantic
// equivalence this package cannot establish.
func entityTag(body []byte) string {
	sum := sha256.Sum256(body)
	return `"` + base64.RawURLEncoding.EncodeToString(sum[:]) + `"`
}

// matchesEntityTag reports whether an If-None-Match header field matches.
//
// It implements the part of RFC 9110 section 13.1.2 this surface can honour: a
// list of entity tags, or "*". A weak comparison is the one the specification
// requires for If-None-Match, so a client that echoes W/"..." for a tag this
// package issued strongly still matches -- the W/ prefix is stripped from both
// sides before comparison.
//
// Multiple header fields are read, not just the first: RFC 9110 section 5.3
// makes repeated fields equivalent to one comma-separated field, and a client
// that sends two is not sending something different.
func matchesEntityTag(fields []string, etag string) bool {
	want := strings.TrimPrefix(etag, "W/")
	for _, field := range fields {
		for _, candidate := range strings.Split(field, ",") {
			candidate = strings.TrimSpace(candidate)
			if candidate == "*" {
				return true
			}
			if strings.TrimPrefix(candidate, "W/") == want {
				return true
			}
		}
	}
	return false
}

// directoryFailure maps a target directory read onto a public answer.
//
// A directory failure is a DEPENDENCY failure, so it is 503 and retryable
// rather than 500: the deployment's agent list is unknown for the moment, which
// is an operational condition a client should retry, and reporting it as an
// internal fault would make a transient store outage look like a Factory bug.
// A context ending keeps its own answer, as everywhere else.
func directoryFailure(err error) apiError {
	if failure, ok := contextFailure(err); ok {
		return failure
	}
	var target *sessionstore.HostTargetError
	if !errors.As(err, &target) {
		return internalFailure()
	}
	switch target.Code {
	case sessionstore.HostTargetErrorBackend, sessionstore.HostTargetErrorUnknown:
		return apiError{
			status:  http.StatusServiceUnavailable,
			code:    ErrorCodeUnavailable,
			message: "the launch target directory cannot be read at the moment",
		}
	default:
		// Every other code is about the REQUEST, and Factory built this
		// request from a template NewRouter validated: an invalid key or a
		// foreign cursor here is a fault in this package, not a condition a
		// caller can act on. Enumerating the retryable codes and defaulting to
		// the safe answer keeps a code sessionstore adds later out of the
		// "retry this" class it may not belong to.
		return internalFailure()
	}
}
