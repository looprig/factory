package factory

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"
)

// Lifecycle errors. Serve reports these; Stop reports only what shutting a
// component down reported.
var (
	// ErrServerStopped reports a Serve on a Server that has been stopped. A
	// stopped Server is not restartable: its composition is still valid, but
	// the components Stop shut down are not restarted, so a caller that wants
	// to serve again composes a new Server.
	ErrServerStopped = errors.New("factory: server is stopped")

	// ErrAlreadyServing reports a second concurrent Serve. One Server owns at
	// most one listener, because Stop states an ORDER over the components it
	// shuts down and two listeners would leave that order unstated for one of
	// them.
	ErrAlreadyServing = errors.New("factory: server is already serving")

	// ErrAlreadyStarted reports a second Start. Background components are
	// started once, because starting them twice would run two sweep loops per
	// pass and two ClientLink nodes on one composition.
	ErrAlreadyStarted = errors.New("factory: server is already started")
	// ErrServerQuiesced reports a Start or Serve after public admission closed.
	ErrServerQuiesced = errors.New("factory: server is quiesced")
)

// HTTPLimits bounds the connections Serve accepts.
//
// These are the http.Server bounds internal/httpapi's RouteLimits
// documentation defers to the task that builds the server: RouteLimits bounds
// the WORK a request may do, as a deadline on its context, and nothing there
// can bound a peer that never finishes sending a header or never reads a
// response.
//
// They apply to Serve only. A library embedding uses Handler and supplies its
// own http.Server, and these values say nothing about it.
type HTTPLimits struct {
	// ReadHeaderTimeout bounds the time a peer may take to send a complete
	// header block. It is the slow-header bound, and it is the one field here
	// with no alternative: nothing above the socket can observe a request that
	// has not finished arriving.
	ReadHeaderTimeout time.Duration

	// IdleTimeout is how long a kept-alive connection may sit between
	// requests. It is set explicitly rather than left to default to
	// ReadTimeout, which is zero here, because that default is "forever".
	IdleTimeout time.Duration

	// MaxHeaderBytes bounds a request's headers. internal/identity bounds the
	// one header it parses on its own, deliberately, "because Factory does not
	// own the http.Server"; under Serve it does, and this is that bound.
	MaxHeaderBytes int
}

// DefaultHTTPLimits is what a composition naming no HTTP limits receives.
//
// There is deliberately NO ReadTimeout and NO WriteTimeout, and their absence
// is a decision rather than an omission. Both are absolute per-connection
// deadlines that net/http applies without regard to what a handler is doing,
// and this surface has two shapes they would break: an authorized object body
// is streamed, and /v1/realtime is a WebSocket upgrade a later task fills in,
// which by construction outlives any write deadline. What bounds a handler's
// work instead is httpapi.RouteLimits.RequestTimeout, a deadline on the request
// context that every dependency call takes.
//
// The residue is stated rather than hidden: a peer that completes its headers
// and then sends its BODY slowly is bounded by the handler's context deadline
// and by MaxRequestBytes, not by a socket deadline, so it holds a connection
// for as long as that deadline allows.
func DefaultHTTPLimits() HTTPLimits {
	return HTTPLimits{
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
}

// minHeaderBytes is the floor MaxHeaderBytes is held above.
//
// It is a floor rather than "at least 1" because a ceiling below one realistic
// header block rejects every request rather than bounding an abusive one, and a
// composition that cannot serve anybody should fail where an operator is
// watching. Four kibibytes is smaller than any browser's request and larger
// than a bare API call's.
const minHeaderBytes = 4 << 10

// Validate reports why these limits may not be used.
func (l HTTPLimits) Validate() error {
	if err := positive("HTTPLimits.ReadHeaderTimeout", l.ReadHeaderTimeout); err != nil {
		return err
	}
	if err := positive("HTTPLimits.IdleTimeout", l.IdleTimeout); err != nil {
		return err
	}
	if l.MaxHeaderBytes < minHeaderBytes {
		return fmt.Errorf("%w: HTTPLimits.MaxHeaderBytes is %d, want at least %d",
			ErrInvalidLimits, l.MaxHeaderBytes, minHeaderBytes)
	}
	// There is deliberately no rule relating these two fields. A header
	// deadline bounds one request's arrival and an idle window bounds the gap
	// BETWEEN requests, so no ordering between them describes a broken
	// deployment, and inventing one would be a rule with nothing behind it.
	return nil
}

// HTTPLimits returns the composed HTTP connection limits.
func (s *Server) HTTPLimits() HTTPLimits { return s.cfg.http }

// serverState is the lifecycle. It moves forward only: a stopped Server is not
// restartable, so there is no edge back to idle.
type serverState int

const (
	stateIdle serverState = iota
	stateServing
	stateStopped
)

// Serve accepts connections on ln until Stop, and owns the http.Server it
// builds -- but not the listener, which the caller opens and which Stop closes
// by shutting that server down.
//
// The split is deliberate. Opening the socket is where a deployment's real
// decisions live -- which address, which network, whether systemd or a
// supervisor passed the descriptor in, whether TLS is terminated here or ahead
// of here -- and none of them belong to this module. What DOES belong here is
// the set of bounds a wire-level peer is held to, which is HTTPLimits.
//
// A deliberate Stop is reported as success rather than as
// http.ErrServerClosed: this method's contract is "serve until stopped", and a
// caller should not have to classify the ordinary ending. Any other failure is
// returned as it was.
//
// Serve is optional. An embedder that owns its own http.Server uses Handler and
// never calls this.
// # Why the state claim is the FIRST thing this method does
//
// It has to be, and the cost of getting it wrong is a leaked socket rather
// than a confusing error. Everything Stop can close about the public surface
// it reaches through s.http, so a Stop that lands while s.http is still nil
// skips Shutdown entirely. If this method can then return WITHOUT having
// handed the listener to net/http, nobody closes it: the caller's socket stays
// open and unserved, which is exactly the failure "Shutdown stops public
// admission/listener" exists to prevent.
//
// An earlier draft of this stage called Start BEFORE the claim, on the reading
// that background planes should exist before the network is admitted -- which
// is right, and the PLACEMENT was wrong. A node boot and three goroutine
// launches sat between entering Serve and claiming the state, and a Stop in
// that window made Serve return ErrServerStopped with the listener orphaned.
// It reproduced roughly one run in three under load.
//
// The general shape, worth carrying: MOVING WORK AHEAD OF A LOCK OR STATE
// CLAIM WIDENS A WINDOW THAT USED TO BE ZERO. The claim is cheap and it is
// what makes every later step safe to interleave, so it goes first and the
// work goes after it.
func (s *Server) Serve(ln net.Listener) error {
	// (1) THE CLAIM. State and the http.Server together, under one lock hold,
	// before any other work. From here on a concurrent Stop finds a non-nil
	// server and its Shutdown is what closes the listener below.
	s.mu.Lock()
	switch s.state {
	case stateServing:
		s.mu.Unlock()
		return ErrAlreadyServing
	case stateStopped:
		s.mu.Unlock()
		return ErrServerStopped
	}
	if s.quiescing {
		s.mu.Unlock()
		return ErrServerQuiesced
	}
	s.state = stateServing
	s.http = &http.Server{
		Handler:           s.router,
		ReadHeaderTimeout: s.cfg.http.ReadHeaderTimeout,
		IdleTimeout:       s.cfg.http.IdleTimeout,
		MaxHeaderBytes:    s.cfg.http.MaxHeaderBytes,
	}
	server := s.http
	s.mu.Unlock()

	// (2) THE BACKGROUND PLANES, still before the listener is accepted on,
	// which is the property Start's placement here exists for: nothing may be
	// admitted from the network until the planes that carry an admitted
	// command exist. A caller that already started them explicitly gets
	// ErrAlreadyStarted, which is not a failure of Serve.
	//
	// A Start that FAILS closes the listener rather than returning it open.
	// Past the claim above this call owns the socket, and a caller handed back
	// an error has no way to know whether it was ever accepted on.
	switch err := s.Start(context.Background()); {
	case err == nil, errors.Is(err, ErrAlreadyStarted):
	case errors.Is(err, ErrServerStopped), errors.Is(err, ErrServerQuiesced):
		// A Stop overtook this call and has already torn everything down.
		// That is the ordinary ending, reported as success for the reason
		// stated above -- and the listener is closed HERE rather than left to
		// net/http, because this path never reaches server.Serve.
		_ = ln.Close()
		return nil
	default:
		_ = ln.Close()
		return err
	}

	// (3) THE LISTENER. A Stop that arrived at any point above is not a lost
	// shutdown: Shutdown marks the server closed, and net/http's Serve returns
	// ErrServerClosed immediately for a server already in that state -- AFTER
	// its own deferred l.Close(), which is what closes the caller's socket on
	// that path. So every route out of this method past the claim either
	// serves the listener or closes it.
	if err := server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Start runs this replica's background components.
//
// It exists as a separate method from Serve because a LIBRARY embedding owns
// its own http.Server and reaches the surface through Handler; without Start
// such a deployment would compose every reconciler and run none of them, and
// the symptom -- commands that are accepted and never settled -- would appear
// nowhere near the composition that caused it. Serve calls it, so a deployment
// that owns the socket through this module does not have to.
//
// The order inside it is stated rather than incidental:
//
//  1. The ClientLink node, because clientlink.NewHandler RUNS it and a
//     connection arriving the instant the listener opens must find a node, not
//     a half-built one. Until this succeeds /v1/realtime answers 503.
//  2. The periodic sweeps, each on its own goroutine and its own timer, so one
//     slow pass delays only its own sweep.
//
// The HostLink pool and the routing table are NOT started: both are demand
// driven, hold no goroutine until a session is bound, and are stopped by Stop
// whether or not they ever were.
//
// Start is not restartable. A stopped Server refuses it, for Serve's reason:
// the components Stop shut down are not restarted, so a caller that wants to
// run again composes a new Server.
func (s *Server) Start(ctx context.Context) error {
	// Start and Stop are SERIALIZED against each other, and the reason is the
	// same one that made Serve's claim go first. Start's work is not atomic --
	// it boots a node, then publishes the loop context, then launches the
	// sweeps -- so a Stop interleaved with it would tear down the half that
	// existed when it looked and leave the half that arrived afterwards
	// running for the lifetime of the process. The mutex is separate from
	// s.mu because Stop WAITS for the sweeps to return, which s.mu may not be
	// held across.
	s.lifecycle.Lock()
	defer s.lifecycle.Unlock()

	s.mu.Lock()
	if s.state == stateStopped {
		s.mu.Unlock()
		return ErrServerStopped
	}
	if s.quiescing {
		s.mu.Unlock()
		return ErrServerQuiesced
	}
	if s.started {
		s.mu.Unlock()
		return ErrAlreadyStarted
	}
	s.started = true
	s.mu.Unlock()

	if err := s.components.startRealtime(s.cfg, s.credentials); err != nil {
		return err
	}

	// The sweeps take their own context, derived from Background rather than
	// from ctx. A caller's context bounds the START, not the lifetime: a Start
	// made under a request context would stop every reconciler when that
	// request ended, which is a replica that silently stops reconciling.
	loopCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	done := make(chan struct{})
	s.mu.Lock()
	s.stop = cancel
	s.done = done
	s.mu.Unlock()

	warnPlacementUncomposed(ctx, s.cfg)
	sweeps := s.components.sweeps(s.cfg)
	var wg sync.WaitGroup
	for _, pass := range sweeps {
		wg.Add(1)
		go func(pass sweep) {
			defer wg.Done()
			s.runSweep(loopCtx, pass)
		}(pass)
	}
	go func() {
		wg.Wait()
		close(done)
	}()
	return nil
}

// runSweep drives one periodic pass until the loop context is cancelled.
//
// The interval is a GAP between passes rather than a period: the next timer is
// armed after the previous pass returns, so a pass slower than the interval
// delays the next one instead of overlapping with it. Overlapping passes of
// one sweeper on one replica would contend for their own claims, which is work
// spent proving a replica is not another replica.
//
// A pass FAILURE is not fatal and is not retried faster. Every sweep here is
// periodic by construction: whatever it could not do this pass is still due on
// the next one, and a tighter retry against a store that is already failing is
// how a control plane turns an outage into a stampede.
func (s *Server) runSweep(ctx context.Context, pass sweep) {
	for {
		s.runOnce(ctx, pass)
		timer := time.NewTimer(s.cfg.reconcile.Interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// runOnce bounds one pass and separates a cancelled pass from a failed one.
//
// The deadline is the sweep INTERVAL and not a separate limit, because a pass
// that has not finished by the time the next one is due has already lost the
// cadence; giving it longer would let one slow shard hold the loop.
func (s *Server) runOnce(ctx context.Context, pass sweep) {
	passCtx, cancel := context.WithTimeout(ctx, s.cfg.reconcile.Interval)
	defer cancel()
	_ = pass.run(passCtx)
	s.components.reapIdle()
}

// Stop shuts the Server down in a fixed order and does not return until each
// component it stopped has stopped.
//
// Quiesce first fences durable command admission and closes ClientLinks while
// leaving sweeps and HostLinks live. Stop then closes the listener and the
// remaining background components.
//
// There are THREE phases and four components, and each phase is labelled at
// the code below rather than only here. (This comment said "today there is
// exactly one component" until A9.1 stage 2 composed the rest; it is stated in
// full now because a shutdown order that is documented wrongly is worse than
// one that is not documented at all.)
//
// Stop never touches a Host runtime. Factory is not their supervisor: a session
// outlives every Factory replica, and a Stop that reached into placement would
// end sessions because a deployment restarted a front end.
//
// It is idempotent, because a signal handler and a deferred stop reach it
// together, and it is safe to call on a Server that never served.
func (s *Server) Stop(ctx context.Context) error {
	quiesceErr := s.Quiesce(ctx)
	if quiesceErr != nil {
		s.mu.Lock()
		done := s.quiesceDone
		s.mu.Unlock()
		select {
		case <-done:
			// The admission boundary completed; retain a ClientLink shutdown
			// diagnostic while still closing the remaining owned planes.
			s.mu.Lock()
			completedErr := s.quiesceErr
			s.mu.Unlock()
			if !errors.Is(quiesceErr, completedErr) && completedErr != nil {
				quiesceErr = errors.Join(quiesceErr, completedErr)
			}
		default:
			return quiesceErr
		}
	}
	// See Start for why these two are serialized. A Stop that arrives while a
	// Start is half done waits for it and then tears down everything, rather
	// than tearing down what happened to exist when it looked.
	s.lifecycle.Lock()
	defer s.lifecycle.Unlock()

	s.mu.Lock()
	if s.state == stateStopped {
		s.mu.Unlock()
		return nil
	}
	s.state = stateStopped
	server := s.http
	cancel := s.stop
	done := s.done
	s.mu.Unlock()

	// A Server that never served has no public surface to close, and that is
	// the ordinary case for a library embedding: it holds Handler and owns its
	// own http.Server. Marking the state above is what such a Stop is FOR --
	// it refuses a later Serve.
	// (1) THE PUBLIC LISTENER. Quiesce already fenced command admission and
	// stopped the ClientLink node. An embedder owns its own listener and must
	// close it separately; Handler still refuses new requests.
	firstErr := quiesceErr
	if server != nil {
		if err := server.Shutdown(ctx); err != nil {
			firstErr = errors.Join(firstErr, err)
		}
	}
	if err := s.components.stopRealtime(ctx); err != nil {
		firstErr = errors.Join(firstErr, err)
	}
	// The Host wakes admitted commands scheduled after their acknowledgement.
	// After the listener, so no handler is left to schedule one, and before
	// phase (3), because a wake calls into the routing table that closes
	// there. Cancelled and WAITED for, bounded by the caller's context, so a
	// returned Stop leaves no wake goroutine behind.
	if err := s.router.StopWakes(ctx); err != nil {
		firstErr = errors.Join(firstErr, err)
	}

	// (2) THE PERIODIC SWEEPS. Cancelled and then WAITED for, so a returned
	// Stop means no sweep is still writing to a store. The wait is bounded by
	// the caller's context: a sweep wedged in a dependency must not make Stop
	// unkillable.
	if cancel != nil {
		cancel()
	}
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			firstErr = errors.Join(firstErr, ctx.Err())
		}
	}

	// (3) THE LOCAL ROUTING STATE, then the links it was routing over. Demand
	// first, because it holds the polls that would otherwise rebind a session
	// whose link is being closed underneath it.
	//
	// None of this touches a Host RUNTIME. A session outlives every Factory
	// replica, so a Stop that reached into placement would end sessions
	// because a deployment restarted a front end. What closes here is this
	// replica's connections and its local table, and the Host sees a peer go
	// away -- which is the same thing it sees when a replica crashes.
	if err := s.components.demand.Close(ctx); err != nil {
		firstErr = errors.Join(firstErr, err)
	}
	if err := s.components.bindings.Close(ctx); err != nil {
		firstErr = errors.Join(firstErr, err)
	}
	// The live-tail plane after the routing state that fed it and before the
	// links its tails ran over: every tail was stopped by the unbinds above,
	// so what is left is its drainers, which it waits for.
	if err := s.components.live.Close(ctx); err != nil {
		firstErr = errors.Join(firstErr, err)
	}
	if err := s.components.pool.Close(ctx); err != nil {
		firstErr = errors.Join(firstErr, err)
	}
	return firstErr
}

// Quiesce permanently closes public admission and waits for every command
// admission already inside the shared service to return. Sweeps and HostLinks
// continue running until Stop. Canceling ctx only ends this caller's wait.
func (s *Server) Quiesce(ctx context.Context) error {
	s.mu.Lock()
	if !s.quiescing {
		s.quiescing = true
		s.quiesceDone = make(chan struct{})
		s.router.Quiesce()
		go s.finishQuiesce(context.WithoutCancel(ctx))
	}
	done := s.quiesceDone
	s.mu.Unlock()
	// Prefer the stable completed result if ctx is canceled at the same time.
	select {
	case <-done:
		s.mu.Lock()
		err := s.quiesceErr
		s.mu.Unlock()
		return err
	default:
	}
	select {
	case <-done:
		s.mu.Lock()
		err := s.quiesceErr
		s.mu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) finishQuiesce(ctx context.Context) {
	// A concurrent Start may already be constructing its ClientLink node.
	// Wait for its publication, then fence admission under the same lifecycle
	// lock Start uses. Release it before waiting for active calls or shutdown.
	s.lifecycle.Lock()
	drained := s.components.admissions.FenceAdmissions()
	s.lifecycle.Unlock()
	var err error
	select {
	case <-drained:
	case <-ctx.Done():
		err = ctx.Err()
	}
	if err == nil {
		// Node shutdown can wait for an in-flight RPC, then demand release can
		// wait for routing. Bound the whole owned attempt independently of the
		// Quiesce caller, whose cancellation ends only that caller's wait.
		shutdownCtx, cancel := context.WithTimeout(ctx, quiesceShutdownBound(s.cfg.client))
		err = s.components.stopRealtime(shutdownCtx)
		cancel()
	}
	s.mu.Lock()
	s.quiesceErr = err
	close(s.quiesceDone)
	s.mu.Unlock()
}

func quiesceShutdownBound(l ClientLinkLimits) time.Duration {
	const maxDuration = time.Duration(1<<63 - 1)
	if l.CommandTimeout > maxDuration-l.DemandTimeout {
		return maxDuration
	}
	return l.CommandTimeout + l.DemandTimeout
}

// The two startup warnings a composition without WithPendingCommands logs.
// They are constants so a test can hold the exact line.
const (
	warnNoPlacement            = "factory: WithPendingCommands is not composed, so this replica places no session on a Host"
	warnNoPlacementWithCreates = "factory: WithPublicCreates is composed but WithPendingCommands is not, so this replica admits creates it will never place on a Host"
)

// warnPlacementUncomposed says, once at Start, that this replica will place
// nothing (B5 spec gate F1).
//
// WithPendingCommands is what composes the placement sweep, and it is
// optional -- so a composition that omits it starts, serves, admits and
// sweeps, and never attaches a single session, with nothing anywhere saying
// so. That is a supported configuration (a read-only replica, a replica
// another one places for) and so it is a warning rather than a refusal; but it
// is the one silent failure a deployment that meant to place could not see.
// It is STRONGER when WithPublicCreates is composed, because that replica
// admits disposition creates -- durable promises that a session will exist --
// and will not itself make one resident. Requiring the option there, or
// deriving it from a PublicCreates value that also implements
// PendingCommands, is booked for a later minor: either changes what a
// composition that compiles today does.
func warnPlacementUncomposed(ctx context.Context, cfg config) {
	if cfg.pending != nil {
		return
	}
	if cfg.publicCreates != nil {
		logger(cfg).ErrorContext(ctx, warnNoPlacementWithCreates)
		return
	}
	logger(cfg).WarnContext(ctx, warnNoPlacement)
}
