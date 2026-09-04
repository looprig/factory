package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
)

// ---------------------------------------------------------------------------
// The read fixtures.
// ---------------------------------------------------------------------------

var (
	// pooledTemplate is an agent this deployment offers on shared capacity, so
	// it appears only while a Host advertises it.
	pooledTemplate = LaunchTemplate{
		Key: sessionstore.HostTargetKey{
			AgentID:                "agent-pooled",
			RuntimeCompatibilityID: "runtime-1",
			Placement:              sessionwire.HostPlacementPooled,
		},
		Capabilities: []string{"gates", "tools"},
	}
	// dedicatedTemplate is an agent whose workload placement creates on demand,
	// so it appears whether or not anything is running.
	dedicatedTemplate = LaunchTemplate{
		Key: sessionstore.HostTargetKey{
			AgentID:                "agent-dedicated",
			RuntimeCompatibilityID: "runtime-2",
			Placement:              sessionwire.HostPlacementDedicated,
		},
		Capabilities: []string{"workspace"},
	}
)

// fakeDirectory is the observed target directory.
//
// It answers from a set of ADVERTISED keys rather than from a canned page, so a
// test says "this target is advertised" and the fake derives a page whose shape
// -- a capacity report naming a Host and an internal endpoint -- is the shape
// SessionStore really returns. That is what lets the disclosure test below be
// meaningful: the report the handler is handed genuinely carries the endpoint
// that must not reach a response.
type fakeDirectory struct {
	mu         sync.Mutex
	advertised map[sessionstore.HostTargetKey]int
	requests   []sessionstore.ListCompatibleHostsRequest
	deadlines  []bool
	fail       error
	block      bool
}

func newFakeDirectory(keys ...sessionstore.HostTargetKey) *fakeDirectory {
	held := make(map[sessionstore.HostTargetKey]int, len(keys))
	for _, key := range keys {
		held[key]++
	}
	return &fakeDirectory{advertised: held}
}

func (d *fakeDirectory) Candidates(ctx context.Context, req sessionstore.ListCompatibleHostsRequest) (sessionstore.HostTargetPage, error) {
	d.mu.Lock()
	d.requests = append(d.requests, req)
	_, hasDeadline := ctx.Deadline()
	d.deadlines = append(d.deadlines, hasDeadline)
	block, fail, count := d.block, d.fail, d.advertised[req.Key]
	d.mu.Unlock()

	if block {
		<-ctx.Done()
		return sessionstore.HostTargetPage{}, ctx.Err()
	}
	if fail != nil {
		return sessionstore.HostTargetPage{}, fail
	}
	page := sessionstore.HostTargetPage{}
	for i := range count {
		page.Hosts = append(page.Hosts, sessionwire.HostLinkCapacityReport{
			Version:                sessionwire.CurrentWireVersion,
			HostID:                 sessionwire.HostID(fmt.Sprintf("host-%s-%d", req.Key.AgentID, i)),
			HostGeneration:         7,
			AgentID:                req.Key.AgentID,
			RuntimeCompatibilityID: req.Key.RuntimeCompatibilityID,
			Placement:              req.Key.Placement,
			InternalEndpoint:       sessionwire.InternalEndpoint("https://host-internal.cluster.local:8443"),
			IsolationClass:         sessionwire.HostIsolationClassCrossTenantIsolated,
			Accepting:              true,
			AvailableCapacity:      4,
		})
	}
	return page, nil
}

func (d *fakeDirectory) snapshot() ([]sessionstore.ListCompatibleHostsRequest, []bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.requests), slices.Clone(d.deadlines)
}

func withDepartment(templates ...LaunchTemplate) fixtureOption {
	return func(cfg *RouterConfig, _ *fixture) { cfg.Department = templates }
}

// withAdvertised makes the directory report live capacity for each key. A key
// named twice is advertised by two Hosts.
func withAdvertised(keys ...sessionstore.HostTargetKey) fixtureOption {
	return func(_ *RouterConfig, f *fixture) {
		for _, key := range keys {
			f.targets.advertised[key]++
		}
	}
}

func withDirectoryFailure(err error) fixtureOption {
	return func(_ *RouterConfig, f *fixture) { f.targets.fail = err }
}

func withBlockingDirectory() fixtureOption {
	return func(_ *RouterConfig, f *fixture) { f.targets.block = true }
}

// withTenantPage gives one tenant a durable session page.
func withTenantPage(tenant sessionwire.TenantID, page sessionwire.SessionPage) fixtureOption {
	return func(_ *RouterConfig, f *fixture) {
		f.reads.pages[tenant] = sessionstore.SessionPage{SessionPage: page}
	}
}

// summaryAt builds a durable session summary whose recency is a fixed offset,
// so a test states an ORDER rather than a set of timestamps.
func summaryAt(session sessionwire.SessionID, agent sessionwire.AgentID, minutes int) sessionwire.SessionSummary {
	base := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	return sessionwire.SessionSummary{
		SessionID:    session,
		AgentID:      agent,
		State:        sessionwire.SessionStateIdle,
		CreatedAt:    base.Add(-time.Hour),
		LastActiveAt: base.Add(time.Duration(minutes) * time.Minute),
	}
}

func decodeDepartment(t *testing.T, recorder *httptest.ResponseRecorder) sessionwire.DepartmentCapabilitySummary {
	t.Helper()

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body was %q", recorder.Code, recorder.Body)
	}
	var summary sessionwire.DepartmentCapabilitySummary
	if err := json.Unmarshal(recorder.Body.Bytes(), &summary); err != nil {
		t.Fatalf("body %q is not a Core DepartmentCapabilitySummary: %v", recorder.Body, err)
	}
	return summary
}

func decodeSessionPage(t *testing.T, recorder *httptest.ResponseRecorder) sessionwire.SessionPage {
	t.Helper()

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body was %q", recorder.Code, recorder.Body)
	}
	var page sessionwire.SessionPage
	if err := json.Unmarshal(recorder.Body.Bytes(), &page); err != nil {
		t.Fatalf("body %q is not a Core SessionPage: %v", recorder.Body, err)
	}
	return page
}

func agentIdentities(summary sessionwire.DepartmentCapabilitySummary) []string {
	out := make([]string, 0, len(summary.Agents))
	for _, agent := range summary.Agents {
		out = append(out, string(agent.AgentID)+"@"+agent.RuntimeCompatibilityID)
	}
	return out
}

// ---------------------------------------------------------------------------
// Step 1: /v1/agents is an aggregate, not a catalogue.
// ---------------------------------------------------------------------------

// TestTheAgentListIsAnAggregateOfAdvertisementsAndDedicatedTemplates is the
// whole of step 1's rule, driven over the space of its two axes rather than at
// two chosen points.
//
// The axes are the ones the aggregate is defined over: a template's PLACEMENT,
// and whether the directory currently advertises its target. Both values of
// both axes are driven, so the claim "a pooled target appears only while it is
// advertised, and a dedicated one appears regardless" is tested in all four
// cells instead of in the two that happen to be interesting.
func TestTheAgentListIsAnAggregateOfAdvertisementsAndDedicatedTemplates(t *testing.T) {
	t.Parallel()

	placements := map[string]sessionwire.HostPlacement{
		"pooled":    sessionwire.HostPlacementPooled,
		"dedicated": sessionwire.HostPlacementDedicated,
	}
	for placementName, placement := range placements {
		for _, advertised := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/advertised=%t", placementName, advertised), func(t *testing.T) {
				t.Parallel()

				template := LaunchTemplate{
					Key: sessionstore.HostTargetKey{
						AgentID:                "agent-x",
						RuntimeCompatibilityID: "runtime-x",
						Placement:              placement,
					},
				}
				options := []fixtureOption{withDepartment(template)}
				if advertised {
					options = append(options, withAdvertised(template.Key))
				}
				summary := decodeDepartment(t, newFixture(t, options...).get("/v1/agents"))

				// A dedicated target is listed regardless: placement CREATES
				// its workload on demand, so there is nothing for a Host to be
				// advertising yet and requiring one would hide every dedicated
				// agent until one happened to be running. A pooled target can
				// only land on capacity that already exists.
				want := advertised || placement == sessionwire.HostPlacementDedicated
				if got := len(summary.Agents) == 1; got != want {
					t.Fatalf("the aggregate holds %v, want listed=%t", agentIdentities(summary), want)
				}
			})
		}
	}
}

// TestAConfiguredPooledTemplateIsNotACatalogueEntry is the same rule stated as
// the property step 1 forbids: Factory must not maintain a competing static
// agent catalogue.
//
// It is separate from the sweep above because it is the DIRECTION that would be
// silently lost. A handler that ignored the directory entirely and listed the
// configuration would pass every "is it listed" assertion in the advertised
// cells and would be exactly the catalogue the step forbids.
func TestAConfiguredPooledTemplateIsNotACatalogueEntry(t *testing.T) {
	t.Parallel()

	configured := newFixture(t, withDepartment(pooledTemplate))
	summary := decodeDepartment(t, configured.get("/v1/agents"))
	if len(summary.Agents) != 0 {
		t.Errorf("a configured pooled template nothing advertises was listed as %v", agentIdentities(summary))
	}
	// The control: the same configuration WITH an advertisement lists it, so
	// the empty answer above is the directory's doing rather than a handler
	// that lists nothing.
	advertised := newFixture(t, withDepartment(pooledTemplate), withAdvertised(pooledTemplate.Key))
	if got := agentIdentities(decodeDepartment(t, advertised.get("/v1/agents"))); len(got) != 1 {
		t.Errorf("an advertised pooled template was listed as %v, want exactly one entry", got)
	}
}

// TestTheAgentListConsultsTheDirectoryOncePerPooledTargetAndBounds it.
//
// Three properties at once, and each has cost something somewhere: the read is
// bounded (one page, no continuation, an explicit limit), it is proportional to
// the CONFIGURATION rather than to the fleet, and a dedicated template costs no
// read at all.
func TestTheAgentListConsultsTheDirectoryOncePerPooledTarget(t *testing.T) {
	t.Parallel()

	second := LaunchTemplate{Key: sessionstore.HostTargetKey{
		AgentID:                "agent-pooled-2",
		RuntimeCompatibilityID: "runtime-1",
		Placement:              sessionwire.HostPlacementPooled,
	}}
	f := newFixture(t, withDepartment(pooledTemplate, dedicatedTemplate, second), withAdvertised(pooledTemplate.Key))
	f.get("/v1/agents")

	requests, deadlines := f.targets.snapshot()
	want := []sessionstore.HostTargetKey{pooledTemplate.Key, second.Key}
	got := make([]sessionstore.HostTargetKey, 0, len(requests))
	for _, req := range requests {
		got = append(got, req.Key)
		if req.Cursor != "" {
			t.Errorf("the agent list presented a continuation cursor %q; the read is one page", req.Cursor)
		}
		if req.Limit != agentProbePageLimit {
			t.Errorf("the agent list asked for limit %d, want the declared bound %d", req.Limit, agentProbePageLimit)
		}
	}
	if !slices.Equal(got, want) {
		t.Errorf("the directory was asked for %v, want exactly the pooled targets %v", got, want)
	}
	for _, hasDeadline := range deadlines {
		if !hasDeadline {
			t.Error("a directory read ran with no deadline; /v1/agents does not stream")
		}
	}
}

// TestTheAgentAggregateIsIdenticalForEveryPrincipal is the tenant-scoping
// decision made measurable.
//
// /v1/agents is deliberately NOT tenant-scoped, and the argument is that its
// two inputs have no tenant dimension: the configured Department is deployment
// configuration, and SessionStore's target directory is deliberately not
// partitioned by tenant. The consequence a test can hold is that the answer is
// a pure function of those two, so two principals in different tenants must
// receive the SAME response -- compared whole, because a per-principal
// difference could hide in a header as easily as in the body.
//
// It is also the precondition for the entity tag: a validator over a
// per-principal body would be a stable per-principal fingerprint.
func TestTheAgentAggregateIsIdenticalForEveryPrincipal(t *testing.T) {
	t.Parallel()

	build := func(tenant sessionwire.TenantID) *fixture {
		return newFixture(t,
			withDepartment(pooledTemplate, dedicatedTemplate),
			withAdvertised(pooledTemplate.Key),
			withVerifierTenant(tenant))
	}
	first, second := build(fixtureTenant), build(otherTenant)
	a, b := first.get("/v1/agents"), second.get("/v1/agents")
	if a.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body %q", a.Code, a.Body)
	}
	if diff := responseDifference(a, b); diff != "" {
		t.Errorf("the agent aggregate differs between tenants: %s", diff)
	}
	// The tenant reached neither input. The directory request type has no
	// TenantID member at all, so what is checkable is that no tenant string
	// appears in what was asked or in what was answered.
	requests, _ := second.targets.snapshot()
	if len(requests) == 0 {
		t.Fatal("the directory was never asked, so this comparison would hold for a handler that reads nothing")
	}
	for _, secret := range []string{string(fixtureTenant), string(otherTenant)} {
		if strings.Contains(b.Body.String(), secret) {
			t.Errorf("the agent list body %q names tenant %q", b.Body, secret)
		}
	}
	// The catalog is not touched at all: an agent list that resolved anything
	// per tenant would be tenant-scoped by another name.
	if catalog, _ := second.reads.snapshot(); len(catalog) != 0 {
		t.Errorf("the agent list made %d catalog reads: %+v", len(catalog), catalog)
	}
	if len(second.reads.listSnapshot()) != 0 {
		t.Error("the agent list read the tenant session catalogue")
	}
}

// TestTheAgentListNeverPublishesAHostOrAnEndpoint is the disclosure half of
// "reads do not connect to or enumerate Hosts".
//
// The handler is HANDED HostLinkCapacityReport values that carry a HostID, a
// host generation, an internal endpoint, an isolation class and a capacity
// number. Core's DepartmentCapabilitySummary has nowhere to put any of them,
// which is a structural defence -- so this asserts the value that would matter
// most is really absent from the wire, and asserts it against the byte the fake
// actually supplied rather than against a guess.
func TestTheAgentListNeverPublishesAHostOrAnEndpoint(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withDepartment(pooledTemplate), withAdvertised(pooledTemplate.Key, pooledTemplate.Key))
	recorder := f.get("/v1/agents")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	// What the fake handed the handler, taken from the fake itself so the
	// probe cannot drift from the fixture.
	page, err := f.targets.Candidates(context.Background(), sessionstore.ListCompatibleHostsRequest{Key: pooledTemplate.Key, Limit: 1})
	if err != nil {
		t.Fatalf("Candidates: %v", err)
	}
	if len(page.Hosts) == 0 {
		t.Fatal("the fake advertised nothing, so this probe would pass against any body")
	}
	report := page.Hosts[0]
	body := recorder.Body.String()
	for name, secret := range map[string]string{
		"host identity":      string(report.HostID),
		"internal endpoint":  string(report.InternalEndpoint),
		"isolation class":    string(report.IsolationClass),
		"host generation":    strconv.FormatUint(report.HostGeneration, 10),
		"available capacity": strconv.FormatUint(report.AvailableCapacity, 10),
	} {
		if secret != "" && strings.Contains(body, secret) {
			t.Errorf("the agent list published the %s %q: %s", name, secret, body)
		}
	}
}

// TestTheAgentListIsTheSameAggregateUnderBothPaths holds /v1/capabilities to
// /v1/agents.
//
// Section 8.1 keeps capabilities as a migration spelling that "may project
// Factory and agent capabilities until clients use /v1/agents". Two handlers
// would be two answers to one question and would drift; the comparison is over
// the whole response so a difference cannot hide in a header.
func TestTheAgentListIsTheSameAggregateUnderBothPaths(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withDepartment(pooledTemplate, dedicatedTemplate), withAdvertised(pooledTemplate.Key))
	agents, capabilities := f.get("/v1/agents"), f.get("/v1/capabilities")
	if agents.Code != http.StatusOK {
		t.Fatalf("/v1/agents = %d, want 200; body %q", agents.Code, agents.Body)
	}
	if diff := responseDifference(agents, capabilities); diff != "" {
		t.Errorf("/v1/capabilities differs from /v1/agents: %s", diff)
	}
}

// TestTheAgentListIsOrderedAndDeduplicatedByIdentity.
//
// Order is not cosmetic here: the entity tag is a digest of the body, so an
// order following the composer's slice or a map iteration would make the
// validator change while the answer did not, and a polling client would
// re-download on every request. Deduplication is the other half -- one agent
// build deployed both pooled and dedicated is ONE launchable identity, and
// Core's summary is keyed by the agent and its compatibility boundary.
func TestTheAgentListIsOrderedAndDeduplicatedByIdentity(t *testing.T) {
	t.Parallel()

	both := func(placement sessionwire.HostPlacement, capabilities ...string) LaunchTemplate {
		return LaunchTemplate{
			Key: sessionstore.HostTargetKey{
				AgentID:                "agent-both",
				RuntimeCompatibilityID: "runtime-1",
				Placement:              placement,
			},
			Capabilities: capabilities,
		}
	}
	zed := LaunchTemplate{Key: sessionstore.HostTargetKey{
		AgentID: "zed", RuntimeCompatibilityID: "runtime-1", Placement: sessionwire.HostPlacementDedicated,
	}}
	alpha := LaunchTemplate{Key: sessionstore.HostTargetKey{
		AgentID: "alpha", RuntimeCompatibilityID: "runtime-9", Placement: sessionwire.HostPlacementDedicated,
	}}
	alphaOlder := LaunchTemplate{Key: sessionstore.HostTargetKey{
		AgentID: "alpha", RuntimeCompatibilityID: "runtime-1", Placement: sessionwire.HostPlacementDedicated,
	}}
	pooledHalf := both(sessionwire.HostPlacementPooled, "gates")

	f := newFixture(t,
		withDepartment(zed, pooledHalf, alpha, both(sessionwire.HostPlacementDedicated, "tools", "gates"), alphaOlder),
		withAdvertised(pooledHalf.Key))
	summary := decodeDepartment(t, f.get("/v1/agents"))

	want := []string{"agent-both@runtime-1", "alpha@runtime-1", "alpha@runtime-9", "zed@runtime-1"}
	if got := agentIdentities(summary); !slices.Equal(got, want) {
		t.Errorf("the aggregate is %v, want %v", got, want)
	}
	for _, agent := range summary.Agents {
		if agent.AgentID != "agent-both" {
			continue
		}
		if got := agent.Capabilities; !slices.Equal(got, []string{"gates", "tools"}) {
			t.Errorf("the merged capabilities are %v, want the sorted union [gates tools]", got)
		}
	}
	// The order is a property of the ANSWER, so the same configuration
	// presented in a different order must produce the identical response.
	shuffled := newFixture(t,
		withDepartment(alphaOlder, both(sessionwire.HostPlacementDedicated, "gates", "tools"), alpha, pooledHalf, zed),
		withAdvertised(pooledHalf.Key))
	if diff := responseDifference(f.get("/v1/agents"), shuffled.get("/v1/agents")); diff != "" {
		t.Errorf("reordering the configuration changed the response: %s", diff)
	}
}

// TestAnEmptyDepartmentIsAnEmptyAgentList. A Factory composed to serve an
// existing tenant's history and launch nothing is a valid deployment, and the
// truthful answer is an empty list rather than a failure -- and an empty JSON
// ARRAY rather than null, which Core guarantees and this pins at the wire.
func TestAnEmptyDepartmentIsAnEmptyAgentList(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	recorder := f.get("/v1/agents")
	summary := decodeDepartment(t, recorder)
	if len(summary.Agents) != 0 {
		t.Errorf("an empty Department listed %v", agentIdentities(summary))
	}
	if !strings.Contains(recorder.Body.String(), `"agents":[]`) {
		t.Errorf("the empty aggregate is %q, want an empty array rather than null", recorder.Body)
	}
	if requests, _ := f.targets.snapshot(); len(requests) != 0 {
		t.Errorf("an empty Department produced %d directory reads", len(requests))
	}
}

// TestADirectoryFailureIsAnOperationalAnswerNotAnInternalOne.
//
// The split is the one catalogFailure makes for the same reason: a dependency
// Factory could not reach is retryable and is the deployment's condition, and
// a code that says the REQUEST was wrong is a fault here because Factory built
// that request from a template NewRouter validated. Anything unrecognised
// lands on the safe answer by DEFAULT, so a code sessionstore adds later is
// never advertised as retryable by accident.
func TestADirectoryFailureIsAnOperationalAnswerNotAnInternalOne(t *testing.T) {
	t.Parallel()

	for name, probe := range map[string]struct {
		err       error
		status    int
		code      sessionwire.ErrorCode
		retryable bool
	}{
		"a backend outage": {
			err:    &sessionstore.HostTargetError{Code: sessionstore.HostTargetErrorBackend, Field: "list"},
			status: http.StatusServiceUnavailable, code: ErrorCodeUnavailable, retryable: true,
		},
		"an unknown store condition": {
			err:    &sessionstore.HostTargetError{Code: sessionstore.HostTargetErrorUnknown},
			status: http.StatusServiceUnavailable, code: ErrorCodeUnavailable, retryable: true,
		},
		"a request Factory built wrongly": {
			err:    &sessionstore.HostTargetError{Code: sessionstore.HostTargetErrorInvalid, Field: "limit"},
			status: http.StatusInternalServerError, code: ErrorCodeInternal,
		},
		"a cursor Factory never sent": {
			err:    &sessionstore.HostTargetError{Code: sessionstore.HostTargetErrorCursor},
			status: http.StatusInternalServerError, code: ErrorCodeInternal,
		},
		"an error of no known shape": {
			err:    errors.New("something else"),
			status: http.StatusInternalServerError, code: ErrorCodeInternal,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t, withDepartment(pooledTemplate), withDirectoryFailure(probe.err))
			recorder := f.get("/v1/agents")
			if recorder.Code != probe.status {
				t.Fatalf("status = %d, want %d; body %q", recorder.Code, probe.status, recorder.Body)
			}
			envelope := decodeEnvelope(t, recorder)
			if envelope.Error.Code != probe.code {
				t.Errorf("code = %q, want %q", envelope.Error.Code, probe.code)
			}
			if envelope.Error.Retryable != probe.retryable {
				t.Errorf("retryable = %t, want %t", envelope.Error.Retryable, probe.retryable)
			}
			if strings.Contains(recorder.Body.String(), "sessionstore") {
				t.Errorf("the failure body quotes the dependency: %q", recorder.Body)
			}
			if recorder.Header().Get("ETag") != "" {
				t.Errorf("a failed agent read carried a validator %q", recorder.Header().Get("ETag"))
			}
		})
	}
}

// TestTheAgentReadIsBoundedByTheRequestDeadline. /v1/agents does not stream, so
// a wedged directory must end at the deadline this router imposes rather than
// hold the handler goroutine.
func TestTheAgentReadIsBoundedByTheRequestDeadline(t *testing.T) {
	t.Parallel()

	f := newFixture(t,
		withDepartment(pooledTemplate),
		withBlockingDirectory(),
		withLimits(RouteLimits{MaxRequestBytes: 1 << 20, RequestTimeout: 50 * time.Millisecond}))
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- f.get("/v1/agents") }()
	select {
	case recorder := <-done:
		if recorder.Code != http.StatusGatewayTimeout {
			t.Errorf("status = %d, want 504; body %q", recorder.Code, recorder.Body)
		}
		if code := decodeEnvelope(t, recorder).Error.Code; code != ErrorCodeTimeout {
			t.Errorf("code = %q, want %q", code, ErrorCodeTimeout)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the handler was still inside the directory long after the deadline")
	}
}

// TestNewRouterRefusesAMalformedLaunchTemplate keeps a bad template a
// COMPOSITION failure.
//
// A template that reached the handler could only be dropped silently or fail
// every request; refusing to start is the answer that reaches an operator, and
// it is what lets serveAgents treat every configured key as buildable.
func TestNewRouterRefusesAMalformedLaunchTemplate(t *testing.T) {
	t.Parallel()

	valid := pooledTemplate
	for name, template := range map[string]LaunchTemplate{
		"no agent": {Key: sessionstore.HostTargetKey{
			RuntimeCompatibilityID: "runtime-1", Placement: sessionwire.HostPlacementPooled}},
		"an oversized agent": {Key: sessionstore.HostTargetKey{
			AgentID:                sessionwire.AgentID(strings.Repeat("a", sessionwire.MaxIDBytes+1)),
			RuntimeCompatibilityID: "runtime-1", Placement: sessionwire.HostPlacementPooled}},
		"no compatibility boundary": {Key: sessionstore.HostTargetKey{
			AgentID: "agent-x", Placement: sessionwire.HostPlacementPooled}},
		"no placement": {Key: sessionstore.HostTargetKey{
			AgentID: "agent-x", RuntimeCompatibilityID: "runtime-1"}},
		"a placement nothing serves": {Key: sessionstore.HostTargetKey{
			AgentID: "agent-x", RuntimeCompatibilityID: "runtime-1", Placement: "elsewhere"}},
		"an empty capability": {Key: valid.Key, Capabilities: []string{"gates", ""}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if err := template.Validate(); !errors.Is(err, ErrInvalidRouterConfig) {
				t.Errorf("Validate = %v, want ErrInvalidRouterConfig", err)
			}
		})
	}
	// The control: the valid template must pass, or the refusals above would
	// be explicable by a Validate that refuses everything.
	if err := valid.Validate(); err != nil {
		t.Errorf("a valid template was refused: %v", err)
	}
	if err := dedicatedTemplate.Validate(); err != nil {
		t.Errorf("a valid dedicated template was refused: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Step 4: the validator, and what carries one.
// ---------------------------------------------------------------------------

// TestTheAgentListCarriesAValidatorAndTheSessionPageDoesNot is the cache
// decision, both halves in one place because they are one decision.
//
// The agent list is not tenant data and is identical for every principal, so a
// strong validator over it discloses nothing and lets a polling client skip a
// re-download. A tenant's session page is private, and a validator over it is a
// stable fingerprint of that tenant's state which outlives the body in proxy
// and browser logs -- and it would almost never hit in any case, because every
// accepted command moves a session's LastActiveAt.
//
// Cache-Control stays no-store on BOTH: the validator here is for a client
// holding it in its own application state, which is not an HTTP cache, and
// weakening the header the API's threat model rests on to buy a revalidation
// would be the wrong trade.
func TestTheAgentListCarriesAValidatorAndTheSessionPageDoesNot(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withDepartment(dedicatedTemplate),
		withTenantPage(fixtureTenant, sessionwire.SessionPage{
			Sessions: []sessionwire.SessionSummary{summaryAt(fixtureSession, "agent-a", 0)},
		}))
	agents := f.get("/v1/agents")
	if agents.Header().Get("ETag") == "" {
		t.Error("the agent list carries no validator, so a polling client re-downloads it every time")
	}
	if got := agents.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("the agent list Cache-Control = %q, want no-store", got)
	}
	sessions := f.get("/v1/sessions")
	if got := sessions.Header().Get("ETag"); got != "" {
		t.Errorf("a tenant's private session page carries the validator %q, which fingerprints that tenant's state", got)
	}
	if got := sessions.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("the session page Cache-Control = %q, want no-store", got)
	}
}

// TestTheValidatorIsOverTheAnswerAndNothingElse.
//
// A validator that did not move when the answer moved would tell a client to
// keep a stale agent list -- which for this response means keeping an agent
// that has been withdrawn from the deployment. So both directions are driven:
// the same answer keeps its tag across independent routers, and each way the
// answer can change moves it.
func TestTheValidatorIsOverTheAnswerAndNothingElse(t *testing.T) {
	t.Parallel()

	base := func(options ...fixtureOption) string {
		options = append(options, withDepartment(pooledTemplate, dedicatedTemplate))
		recorder := newFixture(t, options...).get("/v1/agents")
		if recorder.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", recorder.Code)
		}
		tag := recorder.Header().Get("ETag")
		if !strings.HasPrefix(tag, `"`) || !strings.HasSuffix(tag, `"`) {
			t.Errorf("the validator %q is not a quoted entity tag", tag)
		}
		if strings.HasPrefix(tag, `W/`) {
			t.Errorf("the validator %q is weak; this package can only establish byte equality", tag)
		}
		return tag
	}
	advertised := base(withAdvertised(pooledTemplate.Key))
	if again := base(withAdvertised(pooledTemplate.Key)); again != advertised {
		t.Errorf("two routers with the same answer issued %q and %q", advertised, again)
	}
	// Withdrawing the advertisement removes an agent, which is the change a
	// stale validator would hide.
	if withdrawn := base(); withdrawn == advertised {
		t.Errorf("withdrawing an advertisement left the validator at %q", advertised)
	}
	// A different tenant is NOT a change: the aggregate is not tenant-scoped,
	// and a tag that moved per principal would be a per-principal fingerprint.
	if other := base(withAdvertised(pooledTemplate.Key), withVerifierTenant(otherTenant)); other != advertised {
		t.Errorf("the validator differs per tenant: %q against %q", other, advertised)
	}
}

// TestAMatchingValidatorIsAnsweredNotModified drives the exchange end to end,
// and drives the negative case with it.
func TestAMatchingValidatorIsAnsweredNotModified(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withDepartment(dedicatedTemplate))
	first := f.get("/v1/agents")
	tag := first.Header().Get("ETag")
	if tag == "" {
		t.Fatal("no validator was issued, so the exchange cannot be driven")
	}

	for name, probe := range map[string]struct {
		presented []string
		status    int
	}{
		"the tag itself":            {presented: []string{tag}, status: http.StatusNotModified},
		"the tag among others":      {presented: []string{`"other", ` + tag}, status: http.StatusNotModified},
		"the tag in a second field": {presented: []string{`"other"`, tag}, status: http.StatusNotModified},
		"the weak spelling of it":   {presented: []string{"W/" + tag}, status: http.StatusNotModified},
		"a wildcard":                {presented: []string{"*"}, status: http.StatusNotModified},
		"somebody else's tag":       {presented: []string{`"not-this-one"`}, status: http.StatusOK},
		"nothing":                   {presented: nil, status: http.StatusOK},
		"an empty header":           {presented: []string{""}, status: http.StatusOK},
		"a prefix of the tag":       {presented: []string{tag[:len(tag)-3] + `"`}, status: http.StatusOK},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			r := request(http.MethodGet, "/v1/agents", nil)
			for _, value := range probe.presented {
				r.Header.Add("If-None-Match", value)
			}
			recorder := f.serve(r)
			if recorder.Code != probe.status {
				t.Fatalf("status = %d, want %d; body %q", recorder.Code, probe.status, recorder.Body)
			}
			if recorder.Header().Get("ETag") != tag {
				t.Errorf("the answer carries validator %q, want %q; a 304 must still identify what matched",
					recorder.Header().Get("ETag"), tag)
			}
			if probe.status != http.StatusNotModified {
				return
			}
			if recorder.Body.Len() != 0 {
				t.Errorf("the 304 carries a body: %q", recorder.Body)
			}
			// The security headers are the middleware's, so a 304 that skips
			// the body must not skip them.
			for name, value := range apiResponseHeaders() {
				if got := recorder.Header().Get(name); got != value {
					t.Errorf("the 304 carries %s = %q, want %q", name, got, value)
				}
			}
		})
	}
}

// TestAConditionalReadIsStillAuthenticatedAndAuthorized. A 304 is a response
// about the caller's own copy, so it must not be reachable without the
// decisions the 200 needs -- otherwise If-None-Match would be an oracle for
// "has the deployment's agent set changed" available to anyone.
func TestAConditionalReadIsStillAuthenticatedAndAuthorized(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withDepartment(dedicatedTemplate))
	tag := f.get("/v1/agents").Header().Get("ETag")
	if tag == "" {
		t.Fatal("no validator was issued")
	}
	r := httptest.NewRequest(http.MethodGet, fixtureOrigin+"/v1/agents", nil)
	r.Host = fixtureHost
	r.Header.Set("If-None-Match", tag)
	recorder := f.serve(r)
	if recorder.Code != http.StatusUnauthorized {
		t.Errorf("an unauthenticated conditional read answered %d, want 401", recorder.Code)
	}
	if recorder.Header().Get("ETag") != "" {
		t.Errorf("a refused request carried the validator %q", recorder.Header().Get("ETag"))
	}
}

// TestMatchesEntityTagReadsEveryFormItClaims is the unit under the exchange
// above, driven in both directions so the header parsing is not established
// only by the cases the end-to-end test happens to send.
func TestMatchesEntityTagReadsEveryFormItClaims(t *testing.T) {
	t.Parallel()

	const tag = `"abc"`
	for name, probe := range map[string]struct {
		fields []string
		want   bool
	}{
		"the tag":                {fields: []string{tag}, want: true},
		"a list":                 {fields: []string{`"x", "abc", "y"`}, want: true},
		"repeated fields":        {fields: []string{`"x"`, tag}, want: true},
		"leading whitespace":     {fields: []string{`  "abc"`}, want: true},
		"the weak spelling":      {fields: []string{`W/"abc"`}, want: true},
		"a wildcard":             {fields: []string{"*"}, want: true},
		"a wildcard in a list":   {fields: []string{`"x", *`}, want: true},
		"nothing":                {fields: nil, want: false},
		"an empty field":         {fields: []string{""}, want: false},
		"another tag":            {fields: []string{`"abd"`}, want: false},
		"the tag unquoted":       {fields: []string{"abc"}, want: false},
		"a prefix":               {fields: []string{`"ab"`}, want: false},
		"the tag as a substring": {fields: []string{`"xabcx"`}, want: false},
	} {
		if got := matchesEntityTag(probe.fields, tag); got != probe.want {
			t.Errorf("%s: matchesEntityTag(%q, %q) = %t, want %t", name, probe.fields, tag, got, probe.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Step 2: the tenant's recent-first session page.
// ---------------------------------------------------------------------------

// TestTheSessionListIsTheTenantsOwnDurablePage.
func TestTheSessionListIsTheTenantsOwnDurablePage(t *testing.T) {
	t.Parallel()

	page := sessionwire.SessionPage{
		Sessions: []sessionwire.SessionSummary{
			summaryAt("session-newest", "agent-a", 10),
			summaryAt("session-middle", "agent-b", 5),
			summaryAt("session-oldest", "agent-a", 0),
		},
		NextCursor: "cursor-2",
	}
	f := newFixture(t, withTenantPage(fixtureTenant, page))
	got := decodeSessionPage(t, f.get("/v1/sessions"))
	want := []sessionwire.SessionID{"session-newest", "session-middle", "session-oldest"}
	ids := make([]sessionwire.SessionID, 0, len(got.Sessions))
	for _, summary := range got.Sessions {
		ids = append(ids, summary.SessionID)
	}
	if !slices.Equal(ids, want) {
		t.Errorf("the page lists %v, want the store's recent-first order %v", ids, want)
	}
	if got.NextCursor != "cursor-2" {
		t.Errorf("next_cursor = %q, want the store's continuation", got.NextCursor)
	}
	if len(f.targets.requests) != 0 {
		t.Error("the session list consulted the target directory")
	}
}

// TestAPageOutOfRecentFirstOrderIsRefusedRatherThanPublished.
//
// Recent-first is the ORDER the route promises, and Factory forwards Core's own
// type rather than re-projecting it -- so what enforces the promise is Core's
// SessionPage.Validate, called on the way out by its MarshalJSON. This is the
// reader for that: a store answering out of order produces a fault here rather
// than a page a client would render as recency.
func TestAPageOutOfRecentFirstOrderIsRefusedRatherThanPublished(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withTenantPage(fixtureTenant, sessionwire.SessionPage{
		Sessions: []sessionwire.SessionSummary{
			summaryAt("session-older", "agent-a", 0),
			summaryAt("session-newer", "agent-a", 10),
		},
	}))
	recorder := f.get("/v1/sessions")
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body %q", recorder.Code, recorder.Body)
	}
	if code := decodeEnvelope(t, recorder).Error.Code; code != ErrorCodeInternal {
		t.Errorf("code = %q, want %q", code, ErrorCodeInternal)
	}
	if strings.Contains(recorder.Body.String(), "session-older") {
		t.Errorf("the refusal body names a session: %q", recorder.Body)
	}
}

// TestEqualRecencyKeepsTheStoresOrder is the tie-break half.
//
// Core's own rule is that equal timestamps are "left in the provider's stable
// cursor order", so Factory must forward that order rather than impose one --
// a handler that sorted the page would break the cursor walk, because the
// continuation resumes at the provider's position and not at Factory's.
func TestEqualRecencyKeepsTheStoresOrder(t *testing.T) {
	t.Parallel()

	first := []sessionwire.SessionID{"session-c", "session-a", "session-b"}
	summaries := make([]sessionwire.SessionSummary, 0, len(first))
	for _, id := range first {
		summaries = append(summaries, summaryAt(id, "agent-a", 3))
	}
	f := newFixture(t, withTenantPage(fixtureTenant, sessionwire.SessionPage{Sessions: summaries}))
	got := decodeSessionPage(t, f.get("/v1/sessions"))
	ids := make([]sessionwire.SessionID, 0, len(got.Sessions))
	for _, summary := range got.Sessions {
		ids = append(ids, summary.SessionID)
	}
	if !slices.Equal(ids, first) {
		t.Errorf("equally recent sessions were reordered to %v, want the store's %v", ids, first)
	}
}

// TestTheSessionListAsksForTheBoundedTenantPageExactlyOnce is step 2's counting
// assertion.
//
// Once, because a handler that paged internally to fill a client's request
// would turn one authenticated GET into an unbounded walk of the tenant's whole
// catalogue -- the amplification a bounded page exists to prevent. The page is
// the STORE's to bound; Factory's job is to ask once and forward what it gets,
// including the continuation.
func TestTheSessionListAsksForTheBoundedTenantPageExactlyOnce(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withTenantPage(fixtureTenant, sessionwire.SessionPage{
		Sessions:   []sessionwire.SessionSummary{summaryAt(fixtureSession, "agent-a", 0)},
		NextCursor: "cursor-2",
	}))
	f.get("/v1/sessions")
	requests := f.reads.listSnapshot()
	if len(requests) != 1 {
		t.Fatalf("the tenant page was asked for %d times, want exactly once: %+v", len(requests), requests)
	}
	if requests[0].TenantID != fixtureTenant {
		t.Errorf("the page was scoped to %q, want the principal's %q", requests[0].TenantID, fixtureTenant)
	}
	// No other durable read was made either: the list is a page, not a page
	// plus a per-session lookup, which would be a read amplification of the
	// same kind one level down.
	if catalog, _ := f.reads.snapshot(); len(catalog) != 0 {
		t.Errorf("the session list also made %d catalog reads: %+v", len(catalog), catalog)
	}
}

// TestTheReadPlaneCanNameNoStoragePrimitive is the structural half of "never
// KV.Keys".
//
// A count of calls can only say that the handler did not enumerate a keyspace
// on the ONE path a test drove. What makes it true of every path is that the
// handlers cannot reach a keyspace at all: SessionReader and Directory are
// SessionStore DOMAIN interfaces, so no method hands a handler a
// github.com/looprig/storage value it could call Keys on, and no method IS a
// primitive by name.
//
// Both rules are proved in both directions on the real subject rather than
// asserted. The package rule is re-run against the module the seams DO name, so
// a walker that found nothing would be reported; the name rule is run against a
// probe interface that declares the method.
//
// What this does NOT establish, said rather than implied: it says nothing about
// what sessionstore does inside ListSessions. That is the aggregate's business,
// and no test in this module can see it.
func TestTheReadPlaneCanNameNoStoragePrimitive(t *testing.T) {
	t.Parallel()

	subjects := map[string]reflect.Type{
		"SessionReader": reflect.TypeOf((*SessionReader)(nil)).Elem(),
		"Directory":     reflect.TypeOf((*Directory)(nil)).Elem(),
	}
	for name, subject := range subjects {
		if subject.NumMethod() == 0 {
			t.Fatalf("%s has no methods, so examining it proves nothing", name)
		}
		if named := packagesNamedBy(subject, "github.com/looprig/storage"); len(named) != 0 {
			t.Errorf("%s names the Storage primitives %v, so a handler could enumerate a keyspace", name, named)
		}
		if methods := primitiveMethodsOf(subject); len(methods) != 0 {
			t.Errorf("%s declares %v, which is a storage primitive by name", name, methods)
		}
	}
	// The package rule, driven at a module the seams really do name. Without
	// this the zero above is equally explicable by a walker that reaches
	// nothing.
	if named := packagesNamedBy(subjects["SessionReader"], "github.com/looprig/sessionstore"); len(named) == 0 {
		t.Error("packagesNamedBy found no SessionStore type in SessionReader, so its zero for Storage means nothing")
	}
	// The name rule, driven at a subject that declares the method. Without
	// this the empty result above is equally explicable by a rule that reports
	// nothing.
	probe := reflect.TypeOf((*keyspaceEnumerator)(nil)).Elem()
	if methods := primitiveMethodsOf(probe); !slices.Contains(methods, "Keys") {
		t.Errorf("primitiveMethodsOf reported %v for an interface that declares Keys", methods)
	}
}

// keyspaceEnumerator is the shape the rule above exists to forbid: a seam that
// hands a handler the keyspace walk itself. It is declared here and implemented
// nowhere, because its only purpose is to be reported.
type keyspaceEnumerator interface {
	Keys(ctx context.Context, prefix string) ([]string, error)
}

// packagesNamedBy returns the named types in an interface's signatures that
// come from a module. It is the walk server_test.go's namedTypesIn does,
// restated here because that one is in another package.
func packagesNamedBy(subject reflect.Type, module string) []string {
	var out []string
	for _, named := range namedTypesReachableFrom(subject) {
		path := named.PkgPath()
		if path == module || strings.HasPrefix(path, module+"/") {
			out = append(out, path+"."+named.Name())
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// primitiveMethodsOf reports the storage primitive operations an interface
// declares by name. Keys is the one step 2 names; the others are here because a
// seam offering any of them offers the same escape.
func primitiveMethodsOf(subject reflect.Type) []string {
	primitives := map[string]bool{"Keys": true, "Scan": true, "List": true, "Range": true, "Iterate": true}
	var out []string
	for i := range subject.NumMethod() {
		if primitives[subject.Method(i).Name] {
			out = append(out, subject.Method(i).Name)
		}
	}
	slices.Sort(out)
	return out
}

func namedTypesReachableFrom(subject reflect.Type) []reflect.Type {
	var out []reflect.Type
	seen := map[reflect.Type]bool{}
	var walk func(reflect.Type)
	walk = func(typ reflect.Type) {
		if typ == nil || seen[typ] {
			return
		}
		seen[typ] = true
		if typ.PkgPath() != "" {
			out = append(out, typ)
			return
		}
		switch typ.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Array, reflect.Chan:
			walk(typ.Elem())
		case reflect.Map:
			walk(typ.Key())
			walk(typ.Elem())
		case reflect.Func:
			for i := range typ.NumIn() {
				walk(typ.In(i))
			}
			for i := range typ.NumOut() {
				walk(typ.Out(i))
			}
		case reflect.Interface:
			for i := range typ.NumMethod() {
				walk(typ.Method(i).Type)
			}
		case reflect.Struct:
			for i := range typ.NumField() {
				walk(typ.Field(i).Type)
			}
		}
	}
	for i := range subject.NumMethod() {
		walk(subject.Method(i).Type)
	}
	return out
}

// TestTwoTenantsSeeOnlyTheirOwnSessions.
//
// The scope carries the authenticated principal's tenant, so the store answers
// each caller from its own catalogue. It is driven with the SAME store holding
// both tenants' pages, because a fixture where each tenant had its own store
// could not tell a scoped query from an unscoped one.
func TestTwoTenantsSeeOnlyTheirOwnSessions(t *testing.T) {
	t.Parallel()

	pages := []fixtureOption{
		withTenantPage(fixtureTenant, sessionwire.SessionPage{
			Sessions: []sessionwire.SessionSummary{summaryAt("session-of-a", "agent-a", 0)},
		}),
		withTenantPage(otherTenant, sessionwire.SessionPage{
			Sessions: []sessionwire.SessionSummary{summaryAt("session-of-b", "agent-b", 0)},
		}),
	}
	for tenant, want := range map[sessionwire.TenantID]sessionwire.SessionID{
		fixtureTenant: "session-of-a",
		otherTenant:   "session-of-b",
	} {
		f := newFixture(t, append(slices.Clone(pages), withVerifierTenant(tenant))...)
		page := decodeSessionPage(t, f.get("/v1/sessions"))
		if len(page.Sessions) != 1 || page.Sessions[0].SessionID != want {
			t.Fatalf("tenant %q was shown %+v, want exactly %q", tenant, page.Sessions, want)
		}
		requests := f.reads.listSnapshot()
		if len(requests) != 1 || requests[0].TenantID != tenant {
			t.Errorf("tenant %q's list produced %+v, want one request scoped to it", tenant, requests)
		}
		// The other tenant's session is not merely absent from the list; its
		// identifier appears nowhere in the response at all.
		other := "session-of-a"
		if tenant == fixtureTenant {
			other = "session-of-b"
		}
		if strings.Contains(f.get("/v1/sessions").Body.String(), other) {
			t.Errorf("tenant %q's page names %q", tenant, other)
		}
	}
}

// TestAnEmptyTenantListIsAnEmptyArray. A tenant with no sessions is an ordinary
// answer, and it must be an empty JSON array rather than null: a client
// iterating the member should not have to branch on which the server meant.
func TestAnEmptyTenantListIsAnEmptyArray(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	recorder := f.get("/v1/sessions")
	page := decodeSessionPage(t, recorder)
	if len(page.Sessions) != 0 {
		t.Errorf("an empty tenant listed %+v", page.Sessions)
	}
	if !strings.Contains(recorder.Body.String(), `"sessions":[]`) {
		t.Errorf("the empty page is %q, want an empty array rather than null", recorder.Body)
	}
	if page.NextCursor != "" {
		t.Errorf("an empty page issued the continuation %q", page.NextCursor)
	}
	if requests := f.reads.listSnapshot(); len(requests) != 1 {
		t.Errorf("an empty tenant produced %d page reads, want 1", len(requests))
	}
}

// TestTheCursorAndLimitAreForwardedAndBounded sweeps the page controls.
//
// The limit space is derived from the mechanism rather than sampled: a limit is
// absent, or it parses and is inside the ceiling, or it parses and is outside
// it, or it does not parse as a usable page size. Each class gets its own
// answer, and the boundary itself is driven from both sides because an
// off-by-one at a ceiling is the defect a middle value cannot see.
func TestTheCursorAndLimitAreForwardedAndBounded(t *testing.T) {
	t.Parallel()

	for name, probe := range map[string]struct {
		query  string
		want   int
		status int
	}{
		"absent":                  {query: "", want: 0, status: http.StatusOK},
		"one":                     {query: "?limit=1", want: 1, status: http.StatusOK},
		"just inside the ceiling": {query: "?limit=" + strconv.Itoa(maxSessionPageLimit-1), want: maxSessionPageLimit - 1, status: http.StatusOK},
		"the ceiling itself":      {query: "?limit=" + strconv.Itoa(maxSessionPageLimit), want: maxSessionPageLimit, status: http.StatusOK},
		"just outside it":         {query: "?limit=" + strconv.Itoa(maxSessionPageLimit+1), want: maxSessionPageLimit, status: http.StatusOK},
		"far outside it":          {query: "?limit=100000000", want: maxSessionPageLimit, status: http.StatusOK},
		"zero":                    {query: "?limit=0", status: http.StatusBadRequest},
		"negative":                {query: "?limit=-1", status: http.StatusBadRequest},
		"not a number":            {query: "?limit=many", status: http.StatusBadRequest},
		"empty":                   {query: "?limit=", status: http.StatusBadRequest},
		"given twice":             {query: "?limit=1&limit=500", status: http.StatusBadRequest},
		"overflowing":             {query: "?limit=99999999999999999999", status: http.StatusBadRequest},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			recorder := f.get("/v1/sessions" + probe.query)
			if recorder.Code != probe.status {
				t.Fatalf("status = %d, want %d; body %q", recorder.Code, probe.status, recorder.Body)
			}
			requests := f.reads.listSnapshot()
			if probe.status != http.StatusOK {
				if code := decodeEnvelope(t, recorder).Error.Code; code != sessionwire.ErrorCodeInvalidRequest {
					t.Errorf("code = %q, want %q", code, sessionwire.ErrorCodeInvalidRequest)
				}
				// A limit this surface refuses never becomes a durable query.
				if len(requests) != 0 {
					t.Errorf("a refused limit still reached the store as %+v", requests)
				}
				return
			}
			if len(requests) != 1 {
				t.Fatalf("%d page reads, want 1", len(requests))
			}
			if requests[0].Limit != probe.want {
				t.Errorf("the store was asked for limit %d, want %d", requests[0].Limit, probe.want)
			}
		})
	}

	// A cursor is opaque and is forwarded verbatim: parsing one here would be
	// this package deriving ordering from a token whose whole contract is that
	// nobody outside the store may.
	f := newFixture(t)
	f.get("/v1/sessions?cursor=" + "LRCP-opaque-token")
	requests := f.reads.listSnapshot()
	if len(requests) != 1 || requests[0].Cursor != "LRCP-opaque-token" {
		t.Fatalf("the cursor reached the store as %+v, want it forwarded verbatim", requests)
	}
}

// TestACursorTheStoreRefusesRestartsTheWalk. The store's own contract is that a
// foreign cursor means "restart from the first page" rather than that anything
// is wrong, so the answer is the caller's to fix and is not a fault.
func TestACursorTheStoreRefusesRestartsTheWalk(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.reads.fail = &sessionstore.CatalogError{Code: sessionstore.CatalogErrorCursor, Field: "cursor"}
	recorder := f.get("/v1/sessions?cursor=foreign")
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body %q", recorder.Code, recorder.Body)
	}
	if code := decodeEnvelope(t, recorder).Error.Code; code != sessionwire.ErrorCodeInvalidRequest {
		t.Errorf("code = %q, want %q", code, sessionwire.ErrorCodeInvalidRequest)
	}
}

// TestAnExpiredTargetIsIrrelevantToTheSessionList is step 2's "expired target
// irrelevance", proved by the mechanism rather than by one comparison.
//
// A session's place in the list is a fact about SessionStore's catalog. The
// target directory is a placement hint with an expiry, and the list must not
// consult it -- so the response is compared WHOLE across three directory
// states, and the directory is asserted to have been asked nothing. The second
// half is what makes the first non-vacuous: three identical responses would
// also be produced by a handler that consulted the directory and ignored it,
// which would still make the list's cost depend on the fleet.
func TestAnExpiredTargetIsIrrelevantToTheSessionList(t *testing.T) {
	t.Parallel()

	page := withTenantPage(fixtureTenant, sessionwire.SessionPage{
		Sessions: []sessionwire.SessionSummary{summaryAt(fixtureSession, "agent-pooled", 0)},
	})
	states := map[string][]fixtureOption{
		"a live advertisement": {withDepartment(pooledTemplate), withAdvertised(pooledTemplate.Key)},
		// The store drops a lapsed row from the page it publishes, so an
		// expired advertisement is exactly an empty directory answer.
		"an expired advertisement": {withDepartment(pooledTemplate)},
		"no directory rows at all": {},
		// Even a directory that cannot be read at all: the list must not
		// acquire a dependency on it.
		"a broken directory": {withDepartment(pooledTemplate), withDirectoryFailure(errors.New("directory down"))},
	}
	var reference *httptest.ResponseRecorder
	for name, options := range states {
		f := newFixture(t, append(slices.Clone(options), page)...)
		recorder := f.get("/v1/sessions")
		if recorder.Code != http.StatusOK {
			t.Fatalf("%s: status = %d, want 200; body %q", name, recorder.Code, recorder.Body)
		}
		if requests, _ := f.targets.snapshot(); len(requests) != 0 {
			t.Errorf("%s: the session list asked the directory %+v", name, requests)
		}
		if reference == nil {
			reference = recorder
			continue
		}
		if diff := responseDifference(reference, recorder); diff != "" {
			t.Errorf("%s changed the session list: %s", name, diff)
		}
	}
	if reference == nil {
		t.Fatal("no directory state was driven")
	}
	// The control: the directory IS reachable from this router, so the zeros
	// above are the session list's doing rather than a fixture whose directory
	// nothing could call.
	f := newFixture(t, withDepartment(pooledTemplate), withAdvertised(pooledTemplate.Key))
	f.get("/v1/agents")
	if requests, _ := f.targets.snapshot(); len(requests) == 0 {
		t.Error("no route reaches the directory in this fixture, so asserting the list does not is vacuous")
	}
}

// TestTheSessionListIsAnsweredBeforeTheStoreWhenTheDecisionIsDenied. A caller
// who may not list this tenant must not be able to make Factory read it.
func TestTheSessionListIsAnsweredBeforeTheStoreWhenTheDecisionIsDenied(t *testing.T) {
	t.Parallel()

	authorizer := &recordingAuthorizer{deny: true}
	f := newFixture(t, withAuthorizer(authorizer))
	recorder := f.get("/v1/sessions")
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403; body %q", recorder.Code, recorder.Body)
	}
	if requests := f.reads.listSnapshot(); len(requests) != 0 {
		t.Errorf("a denied list still read the store: %+v", requests)
	}
	calls := authorizer.snapshot()
	if len(calls) != 1 || calls[0].operation != "list" {
		t.Errorf("the decisions asked for were %+v, want exactly the session list", calls)
	}
}

// TestASessionListFailureIsRedacted. The store's error text names a dependency
// and belongs in an operator's log, not in a caller's body.
func TestASessionListFailureIsRedacted(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.reads.fail = errors.New("dial tcp 10.0.0.5:5432: connection refused")
	recorder := f.get("/v1/sessions")
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", recorder.Code)
	}
	if strings.Contains(recorder.Body.String(), "10.0.0.5") {
		t.Errorf("the failure body carries the dependency address: %q", recorder.Body)
	}
}

// TestTheReadRoutesAreServedUnderHeadAsWellAsGet. HEAD is GET's rule exactly,
// and net/http suppresses the body -- so what this checks is that the status
// and the headers a client conditionally requests with survive it.
func TestTheReadRoutesAreServedUnderHeadAsWellAsGet(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withDepartment(dedicatedTemplate))
	for _, target := range []string{"/v1/agents", "/v1/capabilities", "/v1/sessions"} {
		get := f.serve(request(http.MethodGet, target, nil))
		head := f.serve(request(http.MethodHead, target, nil))
		if head.Code != http.StatusOK {
			t.Errorf("HEAD %s = %d, want 200", target, head.Code)
		}
		for name := range maps.Keys(apiResponseHeaders()) {
			if got, want := head.Header().Get(name), get.Header().Get(name); got != want {
				t.Errorf("HEAD %s: %s = %q, want GET's %q", target, name, got, want)
			}
		}
		if got, want := head.Header().Get("ETag"), get.Header().Get("ETag"); got != want {
			t.Errorf("HEAD %s: ETag = %q, want GET's %q", target, got, want)
		}
	}
}

// TestTheAuthenticatedFallbackIsNotReachableThroughTheChain records what the
// two handlers' unauthenticated branches are for.
//
// Both fail closed rather than trusting that serveRoute already refused, and
// neither is reachable through the composed router -- so the branch is driven
// DIRECTLY, which is the only way it can have a reader at all. Without this it
// would be a line no mutation could kill.
func TestTheAuthenticatedFallbackIsNotReachableThroughTheChain(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withDepartment(dedicatedTemplate))
	for name, handler := range map[string]http.Handler{
		"the agent list":   f.router.serveAgents(),
		"the session list": f.router.serveSessionList(),
	} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/anything", nil))
		if recorder.Code != http.StatusUnauthorized {
			t.Errorf("%s served an unauthenticated request %d, want 401", name, recorder.Code)
		}
		if code := decodeEnvelope(t, recorder).Error.Code; code != ErrorCodeUnauthenticated {
			t.Errorf("%s: code = %q, want %q", name, code, ErrorCodeUnauthenticated)
		}
	}
	// The control: through the composed router the same routes are reached
	// WITH a principal, so the branch above is a floor rather than the
	// ordinary path.
	if got := f.get("/v1/sessions").Code; got != http.StatusOK {
		t.Errorf("the composed session list answered %d, want 200", got)
	}
}
