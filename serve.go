package factory

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
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
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	switch s.state {
	case stateServing:
		s.mu.Unlock()
		return ErrAlreadyServing
	case stateStopped:
		s.mu.Unlock()
		return ErrServerStopped
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

	// A Stop that arrives between the unlock and this call is not a lost
	// shutdown: Shutdown marks the server closed, and net/http's Serve returns
	// ErrServerClosed immediately for a server already in that state.
	if err := server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Stop shuts the Server down in a fixed order and does not return until each
// component it stopped has stopped.
//
// The order is the one the runbook states: PUBLIC ADMISSION first -- the
// listener and the in-flight requests it accepted -- and then the background
// components, so nothing new is admitted while they are being torn down. Today
// there is exactly one component, because the reconcilers and the ClientLink
// and HostLink engines are later tasks; when they exist they stop after the
// call below, not before it.
//
// Stop never touches a Host runtime. Factory is not their supervisor: a session
// outlives every Factory replica, and a Stop that reached into placement would
// end sessions because a deployment restarted a front end.
//
// It is idempotent, because a signal handler and a deferred stop reach it
// together, and it is safe to call on a Server that never served.
func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	if s.state == stateStopped {
		s.mu.Unlock()
		return nil
	}
	s.state = stateStopped
	server := s.http
	s.mu.Unlock()

	// A Server that never served has no public surface to close, and that is
	// the ordinary case for a library embedding: it holds Handler and owns its
	// own http.Server. Marking the state above is what such a Stop is FOR --
	// it refuses a later Serve.
	if server != nil {
		if err := server.Shutdown(ctx); err != nil {
			return err
		}
	}
	return nil
}
