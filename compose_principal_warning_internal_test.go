package factory

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	"github.com/looprig/factory/internal/realtime/hostlink"
	"github.com/looprig/sessionstore"
)

type pagedWarningDirectory struct {
	FakeSeams
	calls int
	done  chan struct{}
	once  sync.Once
}

type deadlineWarningDirectory struct{ FakeSeams }

func (d deadlineWarningDirectory) Candidates(ctx context.Context, _ sessionstore.ListCompatibleHostsRequest) (sessionstore.HostTargetPage, error) {
	<-ctx.Done()
	// A provider may return the page it finished concurrently with the
	// deadline. Its continuation still means later registered Hosts were not
	// checked, even though this call itself returned no error.
	return sessionstore.HostTargetPage{NextCursor: "more"}, nil
}

func TestPrincipalStartupProbeWarnsWhenItsDeadlineLeavesPagesUnchecked(t *testing.T) {
	logs := &bytes.Buffer{}
	s := &Server{cfg: config{
		stampPrincipal: true,
		directory:      deadlineWarningDirectory{},
		department:     []LaunchTemplate{{Key: sessionstore.HostTargetKey{AgentID: "agent-a", RuntimeCompatibilityID: "runtime-a", Placement: sessionwire.HostPlacementPooled}}},
		logger:         slog.New(slog.NewJSONHandler(logs, nil)),
	}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	s.warnIncapableHosts(ctx)
	if got := logs.String(); !strings.Contains(got, "some registered Hosts were not checked") {
		t.Fatalf("deadline stopped a paged probe without an incomplete-scan warning: %s", got)
	}
}

type endlessWarningDirectory struct {
	FakeSeams
	calls int
}

func (d *endlessWarningDirectory) Candidates(context.Context, sessionstore.ListCompatibleHostsRequest) (sessionstore.HostTargetPage, error) {
	d.calls++
	return sessionstore.HostTargetPage{NextCursor: "more"}, nil
}

func TestPrincipalStartupProbeWarnsAtItsPageBound(t *testing.T) {
	directory := &endlessWarningDirectory{}
	logs := &bytes.Buffer{}
	s := &Server{cfg: config{
		stampPrincipal: true,
		directory:      directory,
		department:     []LaunchTemplate{{Key: sessionstore.HostTargetKey{AgentID: "agent-a", RuntimeCompatibilityID: "runtime-a", Placement: sessionwire.HostPlacementPooled}}},
		logger:         slog.New(slog.NewJSONHandler(logs, nil)),
	}}
	s.warnIncapableHosts(context.Background())
	if directory.calls != principalProbeMaxPages || !strings.Contains(logs.String(), "page bound") {
		t.Fatalf("pages=%d, warnings=%s", directory.calls, logs.String())
	}
}

func TestPrincipalStartupProbeShutdownCancellationIsSilent(t *testing.T) {
	logs := &bytes.Buffer{}
	s := &Server{cfg: config{
		stampPrincipal: true,
		directory:      deadlineWarningDirectory{},
		department:     []LaunchTemplate{{Key: sessionstore.HostTargetKey{AgentID: "agent-a", RuntimeCompatibilityID: "runtime-a", Placement: sessionwire.HostPlacementPooled}}},
		logger:         slog.New(slog.NewJSONHandler(logs, nil)),
	}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.warnIncapableHosts(ctx)
	if got := logs.String(); got != "" {
		t.Fatalf("shutdown cancellation logged a rollout warning: %s", got)
	}
}

func (d *pagedWarningDirectory) Candidates(_ context.Context, req sessionstore.ListCompatibleHostsRequest) (sessionstore.HostTargetPage, error) {
	d.calls++
	if req.Cursor == "" {
		return sessionstore.HostTargetPage{Hosts: []sessionwire.HostLinkCapacityReport{{HostID: "host-old", HostGeneration: 1, InternalEndpoint: "ws://host-old.internal"}}, NextCursor: "next"}, nil
	}
	if d.done != nil {
		d.once.Do(func() { close(d.done) })
	}
	return sessionstore.HostTargetPage{Hosts: []sessionwire.HostLinkCapacityReport{{HostID: "host-new", HostGeneration: 1, InternalEndpoint: "ws://host-new.internal"}}}, nil
}

func TestPrincipalStartupProbePagesAndWarnsOnlyForIncapableHost(t *testing.T) {
	directory := &pagedWarningDirectory{}
	dialer := &advertisingDialer{methods: map[sessionwire.HostID][]string{"host-old": {sessionwire.HostLinkMethodBind}, "host-new": {sessionwire.HostLinkCapabilityAttributionPrincipal}}}
	pool, err := hostlink.NewPool(hostlink.Config{Dialer: dialer})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pool.Close(context.Background()) })
	service, _ := identity.NewPrincipal("tenant-a", "factory", identity.KindService)
	logs := &bytes.Buffer{}
	s := &Server{cfg: config{stampPrincipal: true, directory: directory, service: service, department: []LaunchTemplate{{Key: sessionstore.HostTargetKey{AgentID: "agent-a", RuntimeCompatibilityID: "runtime-a", Placement: sessionwire.HostPlacementPooled}}}, logger: slog.New(slog.NewJSONHandler(logs, nil))}, components: &components{pool: pool}}
	s.warnIncapableHosts(context.Background())
	if directory.calls != 2 {
		t.Fatalf("read %d candidate pages, want 2", directory.calls)
	}
	if got := logs.String(); strings.Count(got, "host-old") != 1 || strings.Contains(got, "host-new") {
		t.Fatalf("warnings = %s", got)
	}

	directory.calls = 0
	s.cfg.stampPrincipal = false
	s.warnIncapableHosts(context.Background())
	if directory.calls != 0 {
		t.Fatal("probe ran with stamping disabled")
	}
}

func TestStartRunsPrincipalWarningProbeWhenStampingEnabled(t *testing.T) {
	directory := &pagedWarningDirectory{done: make(chan struct{})}
	dialer := &advertisingDialer{methods: map[sessionwire.HostID][]string{
		"host-old": {sessionwire.HostLinkMethodBind},
		"host-new": {sessionwire.HostLinkCapabilityAttributionPrincipal},
	}}
	logs := &bytes.Buffer{}
	options := append(RequiredOptionsExcept("WithDirectory"),
		WithDirectory(directory), WithPrincipalStamping(),
		WithDepartment(LaunchTemplate{Key: sessionstore.HostTargetKey{AgentID: "agent-a", RuntimeCompatibilityID: "runtime-a", Placement: sessionwire.HostPlacementPooled}}),
		WithLogger(slog.New(slog.NewJSONHandler(logs, nil))))
	server, err := New(options...)
	if err != nil {
		t.Fatal(err)
	}
	oldPool := server.components.pool
	pool, err := hostlink.NewPool(hostlink.Config{Dialer: dialer})
	if err != nil {
		t.Fatal(err)
	}
	server.components.pool = pool
	_ = oldPool.Close(context.Background())
	if err := server.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-directory.done:
	case <-time.After(5 * time.Second):
		t.Fatal("startup probe did not page to completion")
	}
	if err := server.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if directory.calls != 2 || strings.Count(logs.String(), "host-old") != 1 {
		t.Fatalf("pages=%d, warnings=%s", directory.calls, logs.String())
	}
}
