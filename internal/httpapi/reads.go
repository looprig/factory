package httpapi

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	internalidentity "github.com/looprig/factory/internal/identity"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"
)

// storePageCeiling is the largest page SessionStore accepts on ANY read.
//
// It is IMPORTED rather than restated. SessionStore does not export the rule --
// Store.pageLimit refuses a limit above it and answers in each caller's own
// error vocabulary -- but the value is storage.MaxOrderedPageLimit, which
// storage does export, and which sessionstore's own comment says it reuses
// deliberately so that a journal page and a catalog page cannot disagree about
// how large one page may be. Naming that constant is the difference between a
// number this package believes and the number the dependency enforces: a
// storage release that moved it would be adopted here as a compile-time fact
// rather than discovered as a 500 on a live route.
//
// Every page limit this package sends is checked against it BELOW ITS OWN
// DECLARATION, as a constant expression: uint(storePageCeiling - n) does not
// compile when n exceeds the ceiling. That is a build failure rather than a
// test failure, so it cannot be reached by a deployment at all.
// TestEveryPageLimitIsCheckedAgainstTheStoreCeiling is what keeps a limit added
// later from being the one without a check.
const storePageCeiling = storage.MaxOrderedPageLimit

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
// The bound's COST is derived from the mechanism; the bound itself is chosen,
// and the distinction is worth the word because only one of them is defended by
// a test. SessionStore ranks a target's rows by available capacity and drops a
// lapsed row from the page it is publishing WITHOUT unranking it -- its
// ListCompatibleHosts documentation says so, and counts the drops in
// LapsedSkipped precisely because the row still occupies a position in every
// later page. So the visible cost of reading one page is exact: a pooled target
// whose only live advertisements rank below this many rows is reported absent
// from /v1/agents. The condition that produces it is an accumulation of lapsed
// rows, and ReconcileHostTargets -- runbook task A4.1 step 2 -- is the only
// thing that removes them.
//
// What is DEFENDED is the floor, and the floor is TWO, not thirty-two: a
// one-row probe reports an advertised target absent the moment a single stale
// row outranks it, and zero is read by the store as its own configured page
// size, which hands the bound away entirely. Thirty-two is headroom above that
// floor and nothing anchors it -- raising it to sixteen or lowering it to two
// changes no test, correctly, because no reader in this module can observe the
// cost of a larger page. The CEILING is not a judgement: SessionStore refuses a
// limit above storage.MaxOrderedPageLimit outright, and both fakes enforce that
// rule so a value past it fails here rather than in production.
//
// Paging until a live row appears would trade that for an unbounded read on a
// public authenticated route, which is the worse failure: an attacker with any
// credential could make Factory walk a whole target's history on demand.
const agentProbePageLimit = 32

const _ = uint(storePageCeiling - agentProbePageLimit)

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
// # What the validator saves, and what it does not
//
// It saves the CLIENT a re-download. It saves the DEPLOYMENT nothing: the tag
// is a digest of the body, so a conditional request pays every directory read
// and the whole marshal and skips only the write. A 304 here costs what a 200
// costs. That is the honest bound on the optimisation, and it is why the cost
// below is a cost of the ROUTE rather than of a cache miss.
//
// # What one request costs
//
// One serial directory round-trip per configured POOLED template, uncached.
// Bounded by the configuration rather than by the fleet, and every read carries
// the request deadline -- but a large Department makes this the most expensive
// read on the surface, and conditional requests do not relieve it. Coalescing
// the reads, or holding the aggregate behind a short-lived cache, is a
// composition decision and belongs to A9.1.
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
		live[key] = append(live[key], template.Capabilities...)
	}
	// Ranged over the MAP, in map order, which is unspecified and does not
	// matter: the sort below is by exactly (AgentID, RuntimeCompatibilityID),
	// which is exactly this key, so no insertion order could survive to the
	// response. Keeping one would have been a slice and a membership guard
	// whose only reader was each other.
	agents := make([]sessionwire.AgentCapabilitySummary, 0, len(live))
	for key := range live {
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

const _ = uint(storePageCeiling - maxSessionPageLimit)

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
		// Parsed ONCE. net/url caches nothing, so calling Query twice per
		// request re-parses and re-allocates the whole query string for a
		// value already in hand.
		query := r.URL.Query()
		// The cursor goes through the SAME reader the journal's does. It used
		// to be query.Get("cursor"), which is the one read on this surface
		// that bypassed singleValue -- so "?cursor=" was forwarded as no
		// cursor and "?cursor=a&cursor=b" was served the first of two
		// positions, on the route whose page type declares next_cursor
		// omitempty. That is the same client bug the journal's refusal exists
		// to prevent, one route over: a client written
		// cursor=${page.next_cursor ?? ""} is thrown back to page one on
		// reaching the tail and re-pages forever.
		cursor, ok := singleValue(w, query, "cursor")
		if !ok {
			return
		}
		limit, ok := sessionPageLimit(w, query)
		if !ok {
			return
		}
		page, err := rt.reads.ListSessions(r.Context(),
			newScope(operation.Principal).sessionPage(sessionwire.Cursor(cursor.value), limit))
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

// sessionPageLimit reads the caller's page size for the tenant session list.
//
// An ABSENT limit is zero, which SessionStore reads as its own configured page
// size; that is the store's default and this package does not second-guess it.
// The journal deliberately does NOT do that -- see defaultJournalPageLimit --
// because it computes a position from the limit and a page size only the store
// knows would leave that position unanchored. This route computes nothing, so
// the store's default is the right answer and is the only difference between
// the two call sites.
// "Absent is zero" is therefore not a BRANCH here. boundedPageLimit's only
// not-present return is (0, false, true), so an explicit `if !present { return
// 0, true }` would be three lines that cannot produce a value the fall-through
// does not -- a line whose deletion nothing can observe. The presence flag is
// discarded rather than read, which is the honest spelling of "this route does
// not distinguish the two".
func sessionPageLimit(w http.ResponseWriter, query url.Values) (int, bool) {
	limit, _, ok := boundedPageLimit(w, query, maxSessionPageLimit)
	return limit, ok
}

// ---------------------------------------------------------------------------
// Query parameters.
//
// These are shared by every read on this surface, and they are shared rather
// than restated because two of them used to be written twice: the tenant list
// and the journal each parsed their own "limit" and each produced the byte-
// identical strings "limit was given more than once" and "limit must be a
// positive whole number" from independent code. Two implementations agreeing on
// a caller-visible message by coincidence of wording is one edit away from two
// routes answering the same malformed request differently -- and it is two
// places for a rule like the empty-value refusal to be fixed in one.
// ---------------------------------------------------------------------------

// queryValue is one query parameter's presence and value.
type queryValue struct {
	present bool
	value   string
}

// singleValue reads a parameter that may appear at most once and, when it
// appears, must carry a value.
//
// A REPEAT is refused rather than resolved: url.Values.Get answers with
// whichever came first, so a caller sending "?limit=1&limit=500" would be
// served a page size it did not unambiguously ask for, and a caller sending two
// positions would be served one of them.
//
// An EMPTY value is refused for a reason the repeat case does not cover, and it
// is a rule about this surface rather than about parsing. "?x=" is a parameter
// the caller SENT, so treating it as absent means answering a request the
// caller did not make; every parameter this surface reads is a position or a
// bound, and there is no position or bound whose meaning is the empty string.
// The concrete failure it prevents is in journalPositionOf, where an empty
// cursor is read by SessionStore as no cursor and turns the tail into a replay
// from the first record. Refusing it here rather than at each call site is what
// makes that true of every parameter rather than of the one that was noticed.
//
// "Every parameter this surface reads" is a claim about CALL SITES, not about
// this function, and this function cannot establish it: a route that reads its
// own parameter with query.Get is unaffected by anything written here. It was
// false when first written -- the tenant list read its cursor with
// query.Get("cursor"), so "?cursor=" was forwarded as no cursor and
// "?cursor=a&cursor=b" served the first of two positions, the same client bug
// on the sibling route. TestEveryQueryParameterIsReadThroughTheGuard is what
// makes the claim checkable: it parses the package's production files, finds
// every read of a url.Values, and reports by name any that does not go through
// here.
func singleValue(w http.ResponseWriter, query url.Values, name string) (queryValue, bool) {
	values := query[name]
	switch {
	case len(values) == 0:
		return queryValue{}, true
	case len(values) > 1:
		writeAPIError(w, apiError{
			status:  http.StatusBadRequest,
			code:    sessionwire.ErrorCodeInvalidRequest,
			message: name + " was given more than once",
		})
		return queryValue{}, false
	case values[0] == "":
		writeAPIError(w, apiError{
			status:  http.StatusBadRequest,
			code:    sessionwire.ErrorCodeInvalidRequest,
			message: name + " was given with no value; omit it instead",
		})
		return queryValue{}, false
	default:
		return queryValue{present: true, value: values[0]}, true
	}
}

// boundedPageLimit reads a caller's page size and clamps it to a ceiling.
//
// A limit that is PRESENT and unusable is a 400 rather than a silent clamp,
// because a caller that asked for -1 or for "many" has a bug and a clamped
// answer hides it. A limit ABOVE the ceiling IS clamped, because that caller
// asked a well-formed question this deployment answers more narrowly.
//
// The ceiling is the caller's, so the two routes keep their own; what they
// share is the parsing and the two messages a malformed limit produces.
func boundedPageLimit(w http.ResponseWriter, query url.Values, ceiling int) (limit int, present, ok bool) {
	value, ok := singleValue(w, query, "limit")
	if !ok {
		return 0, false, false
	}
	if !value.present {
		return 0, false, true
	}
	parsed, err := strconv.Atoi(value.value)
	if err != nil || parsed < 1 {
		writeAPIError(w, apiError{
			status:  http.StatusBadRequest,
			code:    sessionwire.ErrorCodeInvalidRequest,
			message: "limit must be a positive whole number",
		})
		return 0, false, false
	}
	return min(parsed, ceiling), true, true
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
	if failure, ok := storeUnavailable(err); ok {
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
