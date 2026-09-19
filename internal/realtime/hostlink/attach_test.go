package hostlink_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	centrifuge "github.com/centrifugal/centrifuge"
	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/hostlink"
)

// These cases hold hostlink.attach, the one reserved method whose accepted
// reply carries a body. Two properties are the point and each has a case that
// fails without it:
//
//   - A Host that did not advertise attach is NEVER sent one. A Host that
//     predates the method resolves it as a channel and answers
//     runtime_unavailable, byte-identical to a genuine refusal, so the gate is
//     the only way a caller learns "cannot" rather than "will not".
//   - The reply is exactly one of Core's two records, and an empty body is not
//     success: an accepted attach must name the epoch a bind will carry.

// attachRequest is an absolute-literal attach fenced to hostOne generation 7.
func attachRequest(host sessionwire.HostID, session sessionwire.SessionID) sessionwire.HostLinkAttachRequest {
	return sessionwire.HostLinkAttachRequest{
		Version:                sessionwire.CurrentWireVersion,
		TenantID:               tenant,
		SessionID:              session,
		HostID:                 host,
		HostGeneration:         7,
		AgentID:                "agent-1",
		RuntimeCompatibilityID: "runtime-1",
		Mode:                   sessionwire.HostLinkAttachModeCreate,
		ActorID:                "factory-service",
		IdempotencyKey:         "attach-" + string(session),
	}
}

// observationFor is the observation a Host that accepted req at epoch would
// answer with.
func observationFor(req sessionwire.HostLinkAttachRequest, epoch uint64) sessionwire.HostLinkRegistryObservation {
	observed := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	return sessionwire.HostLinkRegistryObservation{
		Version:                sessionwire.CurrentWireVersion,
		TenantID:               req.TenantID,
		SessionID:              req.SessionID,
		HostID:                 req.HostID,
		HostGeneration:         req.HostGeneration,
		AgentID:                req.AgentID,
		RuntimeCompatibilityID: req.RuntimeCompatibilityID,
		Placement:              sessionwire.HostPlacementPooled,
		InternalEndpoint:       "wss://host-1.internal:8443/hostlink",
		Residency:              sessionwire.SessionResidencyResident,
		Accepting:              true,
		LeaseEpoch:             epoch,
		ObservedAt:             observed,
		ExpiresAt:              observed.Add(time.Minute),
	}
}

func attachMethods() []string {
	return []string{sessionwire.HostLinkMethodBind, sessionwire.HostLinkMethodUnbind, sessionwire.HostLinkMethodAttach}
}

func attachCalls(host *hostServer) []rpcCall {
	var attaches []rpcCall
	for _, call := range host.calls() {
		if call.method == "hostlink.attach" {
			attaches = append(attaches, call)
		}
	}
	return attaches
}

// TestAttachIsNeverSentToAHostThatDidNotAdvertiseIt is the capability gate.
// The stand-in advertises bind and unbind only -- the shape of a Host that
// predates attach -- and the case requires the refusal to be LOCAL: typed as
// unsupported, naming the method, and with no RPC of any kind reaching the
// Host.
func TestAttachIsNeverSentToAHostThatDidNotAdvertiseIt(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{
		methods: []string{sessionwire.HostLinkMethodBind, sessionwire.HostLinkMethodUnbind},
		rpc: func(string, []byte) ([]byte, error) {
			// What a pre-attach Host would say: the not-bound channel branch.
			return json.Marshal(sessionwire.HostLinkError{Code: sessionwire.HostLinkErrorRuntimeUnavailable})
		},
	})
	link := mustDial(t, host)

	_, err := link.Attach(context.Background(), attachRequest(hostOne, "s-1"))
	var unsupported *hostlink.UnsupportedMethodError
	if !errors.As(err, &unsupported) {
		t.Fatalf("Attach to a Host that did not advertise it = %v, want *UnsupportedMethodError", err)
	}
	if unsupported.Method != "hostlink.attach" {
		t.Errorf("UnsupportedMethodError.Method = %q, want %q", unsupported.Method, "hostlink.attach")
	}
	var refusal *hostlink.HostRefusal
	if errors.As(err, &refusal) {
		t.Errorf("the local refusal was reported as a Host refusal: %v", err)
	}
	if calls := host.calls(); len(calls) != 0 {
		t.Fatalf("the Host received %d RPCs (%v); an unadvertised attach must send none", len(calls), calls)
	}
}

// TestAnAcceptedAttachReturnsTheObservationTheHostAnswered drives the accepted
// shape over a real socket: the method is Core's literal, the body is Core's
// record for the request, and the answer is the Host's observation with its
// epoch intact.
func TestAnAcceptedAttachReturnsTheObservationTheHostAnswered(t *testing.T) {
	t.Parallel()

	req := attachRequest(hostOne, "s-1")
	host := newHostServer(t, hostOptions{
		methods: attachMethods(),
		rpc: func(method string, data []byte) ([]byte, error) {
			var got sessionwire.HostLinkAttachRequest
			if err := json.Unmarshal(data, &got); err != nil {
				return nil, centrifuge.ErrorBadRequest
			}
			return json.Marshal(observationFor(got, 41))
		},
	})
	link := mustDial(t, host)

	observation, err := link.Attach(context.Background(), req)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if observation.LeaseEpoch != 41 {
		t.Errorf("observation.LeaseEpoch = %d, want the Host's 41", observation.LeaseEpoch)
	}
	if observation.HostID != hostOne || observation.HostGeneration != 7 || observation.SessionID != "s-1" {
		t.Errorf("observation = %+v, want the Host's answer for host-1/7 s-1", observation)
	}
	calls := attachCalls(host)
	if len(calls) != 1 {
		t.Fatalf("the Host received %d attach RPCs, want 1 (all calls: %v)", len(calls), host.calls())
	}
	var sent sessionwire.HostLinkAttachRequest
	if err := json.Unmarshal(calls[0].data, &sent); err != nil {
		t.Fatalf("the attach body is not Core's record: %v", err)
	}
	if sent != req {
		t.Errorf("the Host received %+v, want %+v", sent, req)
	}
}

// TestEveryAttachRefusalCodeArrivesTyped holds the mapping from each code Core
// allows an attach to answer onto a *HostRefusal carrying that code and its
// detail. Each row is a separate refusal the placement caller branches on, so
// one row collapsing into another -- or into a transport error -- is a caller
// doing the wrong thing.
func TestEveryAttachRefusalCodeArrivesTyped(t *testing.T) {
	t.Parallel()

	for _, want := range []sessionwire.HostLinkError{
		{Code: sessionwire.HostLinkErrorEpochMismatch, CurrentLeaseEpoch: 93},
		{Code: sessionwire.HostLinkErrorRuntimeMismatch, RuntimeCompatibilityID: "runtime-other"},
		{Code: sessionwire.HostLinkErrorNoCapacity},
		{Code: sessionwire.HostLinkErrorNotAdmitting},
		{Code: sessionwire.HostLinkErrorRuntimeUnavailable},
		{Code: sessionwire.HostLinkErrorReleasing},
	} {
		t.Run(string(want.Code), func(t *testing.T) {
			t.Parallel()
			host := newHostServer(t, hostOptions{
				methods: attachMethods(),
				rpc:     func(string, []byte) ([]byte, error) { return json.Marshal(want) },
			})
			link := mustDial(t, host)

			observation, err := link.Attach(context.Background(), attachRequest(hostOne, "s-1"))
			var refusal *hostlink.HostRefusal
			if !errors.As(err, &refusal) {
				t.Fatalf("Attach = (%+v, %v), want a *HostRefusal", observation, err)
			}
			if refusal.HostLinkError != want {
				t.Errorf("refusal = %+v, want %+v", refusal.HostLinkError, want)
			}
			if observation != (sessionwire.HostLinkRegistryObservation{}) {
				t.Errorf("a refused attach returned an observation %+v; a caller could bind with its epoch", observation)
			}
		})
	}
}

// TestAnEmptyAttachReplyIsMalformedNotSuccess is the one shape attach does not
// share with bind: an empty body is bind's success and attach's defect.
func TestAnEmptyAttachReplyIsMalformedNotSuccess(t *testing.T) {
	t.Parallel()

	for name, body := range map[string][]byte{
		"empty":          nil,
		"empty object":   []byte(`{}`),
		"bind-shaped ok": []byte(`{"ok":true}`),
		"not a record":   []byte(`[1,2,3]`),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			host := newHostServer(t, hostOptions{
				methods: attachMethods(),
				rpc:     func(string, []byte) ([]byte, error) { return body, nil },
			})
			link := mustDial(t, host)

			observation, err := link.Attach(context.Background(), attachRequest(hostOne, "s-1"))
			if !errors.Is(err, hostlink.ErrMalformedAttachReply) {
				t.Fatalf("Attach = (%+v, %v), want ErrMalformedAttachReply", observation, err)
			}
			var refusal *hostlink.HostRefusal
			if errors.As(err, &refusal) {
				t.Errorf("a malformed reply was reported as a Host refusal: %v", err)
			}
		})
	}
}

// TestAnAttachFailureWithNoCodeIsNotAHostRefusal is Core's "a failure that is
// not a placement outcome carries no code": the Host answers at the TRANSPORT,
// and reporting that as a refusal would send a caller to re-place a session
// onto the next Host running the same broken build.
func TestAnAttachFailureWithNoCodeIsNotAHostRefusal(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{
		methods: attachMethods(),
		rpc:     func(string, []byte) ([]byte, error) { return nil, centrifuge.ErrorInternal },
	})
	link := mustDial(t, host)

	_, err := link.Attach(context.Background(), attachRequest(hostOne, "s-1"))
	if err == nil {
		t.Fatal("Attach hid a transport failure")
	}
	var refusal *hostlink.HostRefusal
	if errors.As(err, &refusal) {
		t.Errorf("a code-less transport failure was reported as a Host refusal: %v", err)
	}
	if errors.Is(err, hostlink.ErrMalformedAttachReply) {
		t.Errorf("a transport failure was reported as a malformed reply: %v", err)
	}
	// It IS the Host's answer, and says so: placement moves on from it rather
	// than aborting (B5 quality gate Q1).
	var failure *hostlink.HostFailure
	if !errors.As(err, &failure) || !errors.Is(err, hostlink.ErrHostFailed) || failure.Code != 100 || failure.Method != sessionwire.HostLinkMethodAttach {
		t.Errorf("a Host's code-less answer = %#v, want *HostFailure code 100 on hostlink.attach", err)
	}
}

// TestAnUnansweredAttachIsNotAHostFailure is the control for the row above: an
// attach the Host never answered -- the caller gave up -- may have reached a
// Host that acted, and must not be reported as the Host's final word.
func TestAnUnansweredAttachIsNotAHostFailure(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	defer close(release)
	host := newHostServer(t, hostOptions{
		methods: attachMethods(),
		rpc: func(string, []byte) ([]byte, error) {
			<-release
			return nil, nil
		},
	})
	link := mustDial(t, host)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := link.Attach(ctx, attachRequest(hostOne, "s-1"))
	if err == nil || errors.Is(err, hostlink.ErrHostFailed) {
		t.Fatalf("an unanswered attach = %v, want a failure that is not ErrHostFailed", err)
	}
}

// TestAPooledAttachValidatesBeforeAnyDial holds Pool.Attach's documented
// "validated before anything is dialled" (quality gate QM5/QM6) for the
// target and for the request.
func TestAPooledAttachValidatesBeforeAnyDial(t *testing.T) {
	t.Parallel()

	for name, call := range map[string]func(*hostlink.Pool) error{
		"target with no endpoint": func(p *hostlink.Pool) error {
			_, err := p.Attach(context.Background(), target(hostOne, ""), attachRequest(hostOne, "s-1"))
			return err
		},
		"request with no session": func(p *hostlink.Pool) error {
			req := attachRequest(hostOne, "s-1")
			req.SessionID = ""
			_, err := p.Attach(context.Background(), target(hostOne, endpoint1), req)
			return err
		},
		"request with no host fence": func(p *hostlink.Pool) error {
			req := attachRequest(hostOne, "s-1")
			req.HostGeneration = 0
			_, err := p.Attach(context.Background(), target(hostOne, endpoint1), req)
			return err
		},
	} {
		dialer := newRecordingDialer()
		pool := newPool(t, dialer, hostlink.Limits{})
		if err := call(pool); err == nil {
			t.Errorf("%s: Attach accepted it", name)
		}
		if got := dialer.dials(); got != 0 {
			t.Errorf("%s: dials = %d, want 0", name, got)
		}
	}
}

// TestATerminalLinkIsEvictedSoTheNextAttachDialsAfresh is quality gate Q2's
// pool half: a link made terminal -- another wire version, or a close the
// transport will not reconnect from -- refuses every later call before
// sending, so the pool must not keep handing it out. A non-terminal failure
// keeps the link, and a route to the evicted Host goes with it.
func TestATerminalLinkIsEvictedSoTheNextAttachDialsAfresh(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		err   error
		evict bool
	}{
		"wire version":        {fmt.Errorf("%w: host selected version 2", hostlink.ErrUnsupportedProtocol), true},
		"terminal disconnect": {&hostlink.HostDisconnect{Host: hostOne, Code: 3500}, true},
		"transient failure":   {errors.New("hostlink: hostlink.attach: context deadline exceeded"), false},
		"host failure":        {&hostlink.HostFailure{Method: sessionwire.HostLinkMethodAttach, Code: 100}, false},
	} {
		dialer := newRecordingDialer()
		dialer.onDial = func(link *fakeLink) {
			link.attachReply = func(sessionwire.HostLinkAttachRequest) (sessionwire.HostLinkRegistryObservation, error) {
				return sessionwire.HostLinkRegistryObservation{}, test.err
			}
		}
		pool := newPool(t, dialer, hostlink.Limits{})
		mustBind(t, pool, target(hostOne, endpoint1), bindRequest(hostOne, "s-viewer"))
		first := dialer.link(hostOne)
		if _, err := pool.Attach(context.Background(), target(hostOne, endpoint1), attachRequest(hostOne, "s-1")); !errors.Is(err, test.err) {
			t.Fatalf("%s: Attach = %v, want the link's error", name, err)
		}
		_, routed := pool.RouteFor(tenant, "s-viewer")
		_, _ = pool.Attach(context.Background(), target(hostOne, endpoint1), attachRequest(hostOne, "s-1"))
		if test.evict {
			if dialer.dials() != 2 || first.closes() != 1 || routed {
				t.Errorf("%s: dials=%d closes=%d viewer routed=%t, want the dead link evicted, closed, its route dropped and a fresh dial", name, dialer.dials(), first.closes(), routed)
			}
		} else if dialer.dials() != 1 || first.closes() != 0 || !routed {
			t.Errorf("%s: dials=%d closes=%d viewer routed=%t, want the live link kept", name, dialer.dials(), first.closes(), routed)
		}
	}
}

// TestALinkReapedDuringAnInFlightAttachFailsOnlyThatAttach is the quality
// gate's I-1 over the pool: an attach holds no binding, so the reaper may
// collect its link while the Host is working. The fake used to hold its lock
// across the reply, which made this interleaving a deadlock rather than a case.
func TestALinkReapedDuringAnInFlightAttachFailsOnlyThatAttach(t *testing.T) {
	t.Parallel()

	entered := make(chan struct{})
	release := make(chan struct{})
	dialer := newRecordingDialer()
	dialer.onDial = func(link *fakeLink) {
		link.attachReply = func(req sessionwire.HostLinkAttachRequest) (sessionwire.HostLinkRegistryObservation, error) {
			close(entered)
			<-release
			return sessionwire.HostLinkRegistryObservation{}, context.Canceled
		}
	}
	clock := &fakeClock{now: time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)}
	limits := defaultLimits()
	limits.IdleTimeout = time.Minute
	pool := newPoolWithClock(t, dialer, limits, clock)
	done := make(chan error, 1)
	go func() {
		_, err := pool.Attach(context.Background(), target(hostOne, endpoint1), attachRequest(hostOne, "s-1"))
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the attach never reached the link")
	}
	clock.advance(time.Minute)
	reaped := make(chan int, 1)
	go func() { reaped <- pool.ReapIdle() }()
	select {
	case n := <-reaped:
		if n != 1 {
			t.Fatalf("ReapIdle during the attach = %d, want 1", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ReapIdle deadlocked against an in-flight attach")
	}
	close(release)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("the reaped attach = %v, want its transport failure", err)
	}
	if got := dialer.link(hostOne).closes(); got != 1 {
		t.Fatalf("closes = %d, want the reaped link closed once", got)
	}
}

// TestAPooledAttachReusesTheHostsLinkAndReturnsTheObservation holds the pool
// half: the attach rides the same one-per-Host link a later bind uses, and the
// observation comes back unaltered.
func TestAPooledAttachReusesTheHostsLinkAndReturnsTheObservation(t *testing.T) {
	t.Parallel()

	dialer := newRecordingDialer()
	pool := newPool(t, dialer, hostlink.Limits{})

	req := attachRequest(hostOne, "s-1")
	observation, err := pool.Attach(context.Background(), target(hostOne, endpoint1), req)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if observation != observationFor(req, 7) {
		t.Errorf("observation = %+v, want the link's answer", observation)
	}
	mustBind(t, pool, target(hostOne, endpoint1), bindRequest(hostOne, "s-1"))
	if got := dialer.dials(); got != 1 {
		t.Errorf("dials = %d, want 1: the bind must reuse the link the attach opened", got)
	}
	if got := dialer.link(hostOne).attaches(); len(got) != 1 || got[0] != req {
		t.Errorf("attaches = %+v, want exactly the request", got)
	}
}

// TestAPooledAttachNamingAnotherHostIsRefusedBeforeAnyDial is Bind's rule
// applied to attach: the request's host fence must be the link's Host.
func TestAPooledAttachNamingAnotherHostIsRefusedBeforeAnyDial(t *testing.T) {
	t.Parallel()

	dialer := newRecordingDialer()
	pool := newPool(t, dialer, hostlink.Limits{})

	_, err := pool.Attach(context.Background(), target(hostOne, endpoint1), attachRequest(hostTwo, "s-1"))
	if !errors.Is(err, hostlink.ErrTargetMismatch) {
		t.Fatalf("Attach naming another Host = %v, want ErrTargetMismatch", err)
	}
	if got := dialer.dials(); got != 0 {
		t.Errorf("dials = %d, want 0", got)
	}
}

// TestAnObservationAboutSomethingElseIsRefused holds attachAnswers row by row.
// The generation row is the host fence's second job: an observation from an
// incarnation the caller did not place on names an epoch nobody may bind with.
func TestAnObservationAboutSomethingElseIsRefused(t *testing.T) {
	t.Parallel()

	for name, distort := range map[string]func(*sessionwire.HostLinkRegistryObservation){
		"another session":     func(o *sessionwire.HostLinkRegistryObservation) { o.SessionID = "s-2" },
		"another tenant":      func(o *sessionwire.HostLinkRegistryObservation) { o.TenantID = "tenant-b" },
		"another host":        func(o *sessionwire.HostLinkRegistryObservation) { o.HostID = hostTwo },
		"another incarnation": func(o *sessionwire.HostLinkRegistryObservation) { o.HostGeneration = 8 },
		"another agent":       func(o *sessionwire.HostLinkRegistryObservation) { o.AgentID = "agent-2" },
		"another runtime":     func(o *sessionwire.HostLinkRegistryObservation) { o.RuntimeCompatibilityID = "runtime-2" },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dialer := newRecordingDialer()
			dialer.onDial = func(link *fakeLink) {
				link.answerAttach(func(req sessionwire.HostLinkAttachRequest) (sessionwire.HostLinkRegistryObservation, error) {
					observation := observationFor(req, 7)
					distort(&observation)
					return observation, nil
				})
			}
			pool := newPool(t, dialer, hostlink.Limits{})

			observation, err := pool.Attach(context.Background(), target(hostOne, endpoint1), attachRequest(hostOne, "s-1"))
			if !errors.Is(err, hostlink.ErrAttachMismatch) {
				t.Fatalf("Attach = (%+v, %v), want ErrAttachMismatch", observation, err)
			}
			if observation != (sessionwire.HostLinkRegistryObservation{}) {
				t.Errorf("a refused observation leaked to the caller: %+v", observation)
			}
		})
	}
}

// TestAPooledAttachDoesNotHoldThePoolAcrossTheRPC is the one way Attach differs
// from Bind. An attach can launch a runtime; while one is parked inside the
// Host, a bind to ANOTHER Host must complete.
func TestAPooledAttachDoesNotHoldThePoolAcrossTheRPC(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	entered := make(chan struct{})
	dialer := newRecordingDialer()
	dialer.onDial = func(link *fakeLink) {
		if link.host != hostOne {
			return
		}
		link.attachReply = func(req sessionwire.HostLinkAttachRequest) (sessionwire.HostLinkRegistryObservation, error) {
			close(entered)
			<-release
			return observationFor(req, 7), nil
		}
	}
	pool := newPool(t, dialer, hostlink.Limits{})
	defer close(release)

	// attachReply runs under the fake link's own mutex, which is the fake's
	// business; the pool's lock is what is under test.
	done := make(chan error, 1)
	go func() {
		_, err := pool.Attach(context.Background(), target(hostOne, endpoint1), attachRequest(hostOne, "s-1"))
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the attach never reached the link")
	}

	bound := make(chan error, 1)
	go func() {
		bound <- pool.Bind(context.Background(), target(hostTwo, endpoint2), bindRequest(hostTwo, "s-9"))
	}()
	select {
	case err := <-bound:
		if err != nil {
			t.Fatalf("Bind to another Host during an attach: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a bind to another Host waited for an attach in flight; the pool is held across the attach RPC")
	}
}

// TestAnAttachOnAClosedPoolIsRefused keeps Attach inside the pool's lifetime.
func TestAnAttachOnAClosedPoolIsRefused(t *testing.T) {
	t.Parallel()

	dialer := newRecordingDialer()
	pool := newPool(t, dialer, hostlink.Limits{})
	if err := pool.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := pool.Attach(context.Background(), target(hostOne, endpoint1), attachRequest(hostOne, "s-1")); !errors.Is(err, hostlink.ErrPoolClosed) {
		t.Fatalf("Attach after Close = %v, want ErrPoolClosed", err)
	}
	if got := dialer.dials(); got != 0 {
		t.Errorf("dials = %d, want 0", got)
	}
}

// TestALateEvictionDoesNotDropTheLinkThatReplacedTheDeadOne is Pool.evict's
// guard (B5 v0.3.0 quality gate QH3b). Two attaches are in flight on a link
// that turns out terminal; the first evicts it and a third attach dials a
// fresh link and a viewer binds on it; THEN the slow second attach reports the
// same terminal error. Its eviction names the OLD link and must be a no-op:
// dropping whatever link is current would close the fresh one, strand the
// viewer's route, and force yet another dial.
func TestALateEvictionDoesNotDropTheLinkThatReplacedTheDeadOne(t *testing.T) {
	t.Parallel()

	dead := fmt.Errorf("%w: host selected version 2", hostlink.ErrUnsupportedProtocol)
	entered := make(chan struct{})
	release := make(chan struct{})
	dials := 0
	dialer := newRecordingDialer()
	dialer.onDial = func(link *fakeLink) {
		dials++
		if dials > 1 {
			return // the replacement accepts
		}
		link.attachReply = func(req sessionwire.HostLinkAttachRequest) (sessionwire.HostLinkRegistryObservation, error) {
			if req.SessionID == "s-slow" {
				close(entered)
				<-release
			}
			return sessionwire.HostLinkRegistryObservation{}, dead
		}
	}
	pool := newPool(t, dialer, hostlink.Limits{})
	slow := make(chan error, 1)
	go func() {
		_, err := pool.Attach(context.Background(), target(hostOne, endpoint1), attachRequest(hostOne, "s-slow"))
		slow <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the slow attach never reached the link")
	}
	old := dialer.link(hostOne)
	if _, err := pool.Attach(context.Background(), target(hostOne, endpoint1), attachRequest(hostOne, "s-fast")); !errors.Is(err, dead) {
		t.Fatalf("the fast attach = %v, want the terminal error", err)
	}
	if _, err := pool.Attach(context.Background(), target(hostOne, endpoint1), attachRequest(hostOne, "s-new")); err != nil {
		t.Fatalf("the attach after eviction = %v, want the fresh link to accept", err)
	}
	fresh := dialer.link(hostOne)
	if fresh == old {
		t.Fatal("the premise is a fresh link; the dialer handed back the old one")
	}
	mustBind(t, pool, target(hostOne, endpoint1), bindRequest(hostOne, "s-viewer"))

	close(release)
	if err := <-slow; !errors.Is(err, dead) {
		t.Fatalf("the slow attach = %v, want the terminal error", err)
	}
	if _, routed := pool.RouteFor(tenant, "s-viewer"); !routed || fresh.closes() != 0 || old.closes() != 1 {
		t.Fatalf("after the late eviction: viewer routed=%t, fresh closes=%d, old closes=%d; want the fresh link and its route untouched",
			routed, fresh.closes(), old.closes())
	}
	if _, err := pool.Attach(context.Background(), target(hostOne, endpoint1), attachRequest(hostOne, "s-again")); err != nil || dialer.dials() != 2 {
		t.Fatalf("the next attach = %v after %d dials, want the fresh link reused", err, dialer.dials())
	}
}
