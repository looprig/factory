package factory_test

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory"
	"github.com/looprig/sessionstore"
)

// These cases hold B5's composition: the placement sweep is DRIVEN when the
// durable query it reads is supplied and absent when it is not, and a session
// it finds reaches the HostLink pool's dialer through the composed reconciler.

// pendingProbe answers the disposition due view with one live pending command
// for tenant-a/session-a, counts the reads, and offers one pooled candidate
// whose endpoint is a recording HTTP server.
type pendingProbe struct {
	*probe

	mu sync.Mutex
	// pages counts the PLACEMENT sweep's reads and expiryReads the
	// disposition DEADLINE sweep's. Both page the same view through the same
	// object, so they are told apart by their bound: placement reads ahead to
	// now + the apply deadline, while the deadline sweep reads only what is
	// already due, bounded at now.
	pages       int
	expiryReads int
	candidate   sessionwire.InternalEndpoint
}

func (p *pendingProbe) ListDueDispositionCommands(ctx context.Context, req sessionstore.ListDueDispositionCommandsRequest) (sessionstore.DispositionDueCommandPage, error) {
	if err := ctx.Err(); err != nil {
		return sessionstore.DispositionDueCommandPage{}, err
	}
	p.mu.Lock()
	if req.Cursor == "" && !req.DueAtOrBefore.After(time.Now()) {
		p.expiryReads++
	} else {
		p.pages++
	}
	p.mu.Unlock()
	return sessionstore.DispositionDueCommandPage{
		Commands: []sessionstore.DispositionInboxEntry{{Record: sessionstore.DispositionInboxRecord{
			Descriptor: sessionstore.DispositionCommandDescriptor{
				TenantID: "tenant-a", SessionID: "session-a", CommandID: "create-1",
			},
			// Far in the future, so the wall-clock sweep always reads it live.
			ApplyDeadline: time.Now().Add(time.Hour),
			State:         sessionstore.InboxStatePending,
		}}},
		Examined: 1,
		Limit:    req.Limit,
	}, nil
}

func (p *pendingProbe) Candidates(context.Context, sessionstore.ListCompatibleHostsRequest) (sessionstore.HostTargetPage, error) {
	p.mu.Lock()
	endpoint := p.candidate
	p.mu.Unlock()
	if endpoint == "" {
		return sessionstore.HostTargetPage{}, nil
	}
	return sessionstore.HostTargetPage{Hosts: []sessionwire.HostLinkCapacityReport{{
		Version: sessionwire.CurrentWireVersion, HostID: "host-a", HostGeneration: 4,
		AgentID: "agent-a", RuntimeCompatibilityID: "runtime-v1", Placement: sessionwire.HostPlacementPooled,
		InternalEndpoint: endpoint, IsolationClass: sessionwire.HostIsolationClassCrossTenantIsolated,
		Accepting: true, AvailableCapacity: 4,
		ObservedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour),
	}}}, nil
}

func (p *pendingProbe) pageReads() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pages
}

func (p *pendingProbe) deadlineReads() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.expiryReads
}

func composedWithPlacement(t *testing.T, p *pendingProbe, pending bool) *factory.Server {
	t.Helper()

	limits := factory.DefaultReconcileLimits()
	limits.Interval = 5 * time.Millisecond
	limits.ClaimTTL = 50 * time.Millisecond
	limits.ApplyDeadline = 500 * time.Millisecond
	options := append(factory.RequiredOptionsExcept(
		"WithCommands", "WithGates", "WithHostTargets", "WithSessionReader",
		"WithReplicaID", "WithCatalog", "WithDirectory",
	),
		factory.WithReplicaID("replica-under-test"),
		factory.WithCommands(p),
		factory.WithGates(p),
		factory.WithHostTargets(p),
		factory.WithSessionReader(p),
		factory.WithReconcileLimits(limits),
		factory.WithCatalog(p),
		factory.WithDirectory(p),
		factory.WithDepartment(probeTemplate()),
	)
	if pending {
		options = append(options, factory.WithPendingCommands(p))
	}
	server, err := factory.New(options...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Stop(ctx); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	return server
}

// TestThePlacementSweepIsDrivenOnlyWhenItsQueryIsComposed is both halves: a
// composition with WithPendingCommands pages the disposition due view, and one
// without it -- every composition before this release -- never does, while its
// other sweeps run.
func TestThePlacementSweepIsDrivenOnlyWhenItsQueryIsComposed(t *testing.T) {
	t.Parallel()

	with := &pendingProbe{probe: &probe{}}
	if err := composedWithPlacement(t, with, true).Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	eventually(t, "the placement sweep read the disposition due view", func() bool { return with.pageReads() > 0 })

	without := &pendingProbe{probe: &probe{}}
	if err := composedWithPlacement(t, without, false).Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	eventually(t, "the other sweeps ran three times", func() bool {
		commands, gates, targets := without.counts()
		return commands >= 3 && gates >= 3 && targets >= 3
	})
	if got := without.pageReads(); got != 0 {
		t.Fatalf("a composition without WithPendingCommands read the due view ahead of now %d times", got)
	}
}

// TestTheDispositionDeadlineSweepIsDrivenInEveryComposition is the half of
// Gap 2 the composition owns: the sweep that rejects an expired disposition
// command runs whether or not the placement trigger is composed, because every
// command this replica admits is a disposition command and a Host leaves the
// deadline to Factory.
func TestTheDispositionDeadlineSweepIsDrivenInEveryComposition(t *testing.T) {
	t.Parallel()

	for _, pending := range []bool{false, true} {
		p := &pendingProbe{probe: &probe{}}
		if err := composedWithPlacement(t, p, pending).Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		eventually(t, "the deadline sweep read the disposition due view", func() bool { return p.deadlineReads() >= 3 })
	}
}

// TestAPendingSessionReachesTheHostLinkDialer is the composed path end to end
// on Factory's side: the sweep finds the session, the reconciler claims it
// under the replica's identifier, finds the candidate, and asks the HostLink
// pool to attach -- which DIALS the tenant's address DERIVED from the
// candidate's advertised base (Gap 1: base + /hostlink/<tenant>, never the base
// verbatim, which a v0.3.0 Host answers 404) with the JSON subprotocol a Host
// requires. The recording server refuses the upgrade, so
// the attach never completes; what is measured is that the attach was
// attempted at the candidate's address.
func TestAPendingSessionReachesTheHostLinkDialer(t *testing.T) {
	t.Parallel()

	var (
		mu    sync.Mutex
		paths []string
		proto []string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		proto = append(proto, r.Header.Get("Sec-WebSocket-Protocol"))
		mu.Unlock()
		http.Error(w, "no", http.StatusBadRequest)
	}))
	defer server.Close()

	p := &pendingProbe{probe: &probe{}}
	p.candidate = sessionwire.InternalEndpoint("ws" + strings.TrimPrefix(server.URL, "http"))
	if err := composedWithPlacement(t, p, true).Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	eventually(t, "the placement attach dialled the candidate", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(paths) > 0
	})
	mu.Lock()
	defer mu.Unlock()
	if paths[0] != "/hostlink/tenant-a" || proto[0] != "centrifuge-json" {
		t.Errorf("dial = path %q protocol %q, want the candidate's /hostlink/tenant-a with centrifuge-json", paths[0], proto[0])
	}
	p.probe.mu.Lock()
	defer p.probe.mu.Unlock()
	if len(p.probe.sweepHolders) == 0 {
		t.Fatal("the placement took no claim before dialling")
	}
	for _, holder := range p.probe.sweepHolders {
		if holder != "replica-under-test" {
			t.Errorf("a claim names %q, want the composed replica identifier", holder)
		}
	}
}

// TestAPlacementControllerIsNoLongerRequiredAndStillAccepted is the
// compatibility half of dropping the requirement: a composition that omits it
// composes (RequiredOptions no longer carries it), and one that still supplies
// it composes too.
func TestAPlacementControllerIsNoLongerRequiredAndStillAccepted(t *testing.T) {
	t.Parallel()

	if _, err := factory.New(factory.RequiredOptions()...); err != nil {
		t.Fatalf("New without WithPlacementController = %v, want it to compose", err)
	}
	if _, err := factory.New(append(factory.RequiredOptions(), factory.WithPlacementController(factory.FakeSeams{}))...); err != nil {
		t.Fatalf("New with WithPlacementController = %v, want it still accepted", err)
	}
}

// truncatingProbe answers the disposition due view with a continuation that
// never ends, so every pass of every sweep reading it is truncated.
type truncatingProbe struct {
	*pendingProbe
}

func (p *truncatingProbe) ListDueDispositionCommands(ctx context.Context, req sessionstore.ListDueDispositionCommandsRequest) (sessionstore.DispositionDueCommandPage, error) {
	if err := ctx.Err(); err != nil {
		return sessionstore.DispositionDueCommandPage{}, err
	}
	return sessionstore.DispositionDueCommandPage{Limit: req.Limit, NextCursor: "more"}, nil
}

// syncBuffer is a log sink safe for the sweep goroutines.
type syncBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestATruncatedPassIsLogged holds the one signal that a shard's due view is
// outgrowing a pass: it was computed and discarded, which made starvation
// silent. Both sweeps reading the disposition view are held, and a truncated
// pass is the only thing that may produce the line.
func TestATruncatedPassIsLogged(t *testing.T) {
	t.Parallel()

	logs := &syncBuffer{}
	p := &truncatingProbe{pendingProbe: &pendingProbe{probe: &probe{}}}
	limits := factory.DefaultReconcileLimits()
	limits.Interval = 5 * time.Millisecond
	limits.ClaimTTL = 50 * time.Millisecond
	limits.MaxConcurrent = 1
	options := append(factory.RequiredOptionsExcept(
		"WithCommands", "WithGates", "WithHostTargets", "WithSessionReader",
		"WithReplicaID", "WithCatalog", "WithDirectory",
	),
		factory.WithReplicaID("replica-under-test"),
		factory.WithCommands(p),
		factory.WithGates(p),
		factory.WithHostTargets(p),
		factory.WithSessionReader(p),
		factory.WithReconcileLimits(limits),
		factory.WithCatalog(p),
		factory.WithDirectory(p),
		factory.WithDepartment(probeTemplate()),
		factory.WithPendingCommands(p),
		factory.WithLogger(slog.New(slog.NewJSONHandler(logs, nil))),
	)
	server, err := factory.New(options...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Stop(ctx)
	})
	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	for _, sweep := range []string{"dispositions", "placement"} {
		eventually(t, "a truncated "+sweep+" pass was logged", func() bool {
			return strings.Contains(logs.String(), `"level":"WARN","msg":"sweep: a pass ended with the shard's due backlog unread","sweep":"`+sweep+`"`)
		})
	}
	// The control: the legacy view answers empty pages, so its sweep is never
	// truncated and must never produce the line.
	if strings.Contains(logs.String(), `"sweep":"commands"`) {
		t.Fatalf("an untruncated pass was logged as truncated: %s", logs.String())
	}
}
