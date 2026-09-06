package factory_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/looprig/factory"
)

// This file drives the OPTIONAL serving shape.
//
// Server.Handler is what a library embedding uses, and an embedder that owns
// its own http.Server needs nothing here. Serve exists for the deployment that
// wants Factory to own the server -- the socket bounds internal/httpapi's
// RouteLimits documentation assigns to this task live on it -- and Stop is the
// ordered shutdown that pairs with it.
//
// What Stop orders TODAY is one thing, public serving, because that is the only
// component this composition has. The reconcilers and the ClientLink and
// HostLink engines are later tasks and are stopped after it when they exist.

// listen opens a loopback listener on a port the operating system chooses, so
// two tests never collide and no test needs a fixed port.
func listen(t *testing.T) net.Listener {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	return ln
}

// serveWait is how long a case waits for Serve to return. It is generous
// because this suite runs on a machine whose load it does not control; nothing
// asserts that a return is FAST, only that it happens.
const serveWait = 30 * time.Second

// served starts Serve in the background and returns a channel carrying its
// result.
//
// Serve is always started in a goroutine, never in the foreground, and the
// reason is what a WRONG lifecycle does rather than what a right one does: a
// Serve that should have been refused SERVES, and serving does not return. In
// the foreground that is a hung test binary with no attribution; here it is a
// bounded wait in the case that owns the claim.
func served(t *testing.T, server *factory.Server, ln net.Listener) <-chan error {
	t.Helper()

	done := make(chan error, 1)
	go func() { done <- server.Serve(ln) }()
	return done
}

// awaitServe returns what Serve returned, or fails the case naming what it was
// waiting for.
func awaitServe(t *testing.T, done <-chan error, waitingFor string) error {
	t.Helper()

	select {
	case err := <-done:
		return err
	case <-time.After(serveWait):
		t.Fatalf("Serve() had not returned after %v; it was expected to %s", serveWait, waitingFor)
		return nil
	}
}

// stopWhenDone stops the Server at the end of the case and waits for Serve.
func stopWhenDone(t *testing.T, server *factory.Server, done <-chan error) {
	t.Helper()

	t.Cleanup(func() {
		if err := server.Stop(context.Background()); err != nil {
			t.Errorf("Stop() = %v", err)
		}
		awaitServe(t, done, "return because of the deferred Stop")
	})
}

// waitServing blocks until ln answers a request, which is the observable form
// of "this Server has taken the listener". It retries rather than sleeping, so
// it costs what the machine costs and asserts nothing about how long that is.
func waitServing(t *testing.T, ln net.Listener) {
	t.Helper()

	deadline := time.Now().Add(60 * time.Second)
	for {
		// /app.js is outside the API segment and no UI is mounted, so the
		// composed router answers it itself without authenticating anything.
		response, err := http.Get("http://" + ln.Addr().String() + "/app.js")
		if err == nil {
			_ = response.Body.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the listener never answered: %v", err)
		}
		// A pause between attempts, so a case that is waiting on a loaded
		// machine does not compete with the server it is waiting for.
		time.Sleep(5 * time.Millisecond)
	}
}

// TestServeAnswersOverAListenerAndStopEndsIt is the whole lifecycle in one
// case: a real socket, a real authenticated request answered by the composed
// router, an ordered stop, and a Serve that returns because of it.
func TestServeAnswersOverAListenerAndStopEndsIt(t *testing.T) {
	t.Parallel()

	server, err := factory.New(factory.RequiredOptions()...)
	if err != nil {
		t.Fatalf("New() = %v", err)
	}
	ln := listen(t)
	done := served(t, server, ln)

	request, err := http.NewRequest(http.MethodGet, "http://"+ln.Addr().String()+"/v1/bootstrap", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	// The guard checks the host the request was ADDRESSED to against the
	// deployment's trusted origins, which a loopback address is not one of.
	request.Host = "app.example.com"
	request.Header.Set("Authorization", "Bearer "+factory.FakeCredentialValue)

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("the served listener did not answer: %v", err)
	}
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/bootstrap over the listener = %d, want 200; body %q", response.StatusCode, body)
	}

	if err := server.Stop(context.Background()); err != nil {
		t.Fatalf("Stop() = %v, want no error", err)
	}
	if err := awaitServe(t, done, "return because of the deliberate Stop"); err != nil {
		t.Errorf("Serve() = %v after a deliberate Stop, want no error", err)
	}

	// The listener is closed by the shutdown, so a further request cannot be
	// answered by this server.
	if _, err := http.DefaultClient.Do(request); err == nil {
		t.Error("a request succeeded after Stop()")
	}
}

// TestStopBeforeServeRefusesToServe is what makes the stop deterministic rather
// than a race: a Server stopped before it started must not start.
func TestStopBeforeServeRefusesToServe(t *testing.T) {
	t.Parallel()

	server, err := factory.New(factory.RequiredOptions()...)
	if err != nil {
		t.Fatalf("New() = %v", err)
	}
	if err := server.Stop(context.Background()); err != nil {
		t.Fatalf("Stop() before Serve() = %v, want no error", err)
	}
	done := served(t, server, listen(t))
	if err := awaitServe(t, done, "refuse to serve a stopped Server"); !errors.Is(err, factory.ErrServerStopped) {
		t.Fatalf("Serve() after Stop() = %v, want ErrServerStopped", err)
	}
}

// TestServeRefusesASecondListener keeps one Server from owning two sockets,
// which would leave Stop with an ordering it cannot state.
func TestServeRefusesASecondListener(t *testing.T) {
	t.Parallel()

	server, err := factory.New(factory.RequiredOptions()...)
	if err != nil {
		t.Fatalf("New() = %v", err)
	}
	ln := listen(t)
	first := served(t, server, ln)
	stopWhenDone(t, server, first)
	// The second call is made only once the first has demonstrably taken the
	// Server. Racing it instead would assert against whichever call happened to
	// arrive second.
	waitServing(t, ln)

	second := served(t, server, listen(t))
	if err := awaitServe(t, second, "refuse a second listener"); !errors.Is(err, factory.ErrAlreadyServing) {
		t.Fatalf("a second Serve() = %v, want ErrAlreadyServing", err)
	}
}

// TestStopIsIdempotent covers the shutdown path a signal handler and a deferred
// stop reach together.
func TestStopIsIdempotent(t *testing.T) {
	t.Parallel()

	server, err := factory.New(factory.RequiredOptions()...)
	if err != nil {
		t.Fatalf("New() = %v", err)
	}
	ln := listen(t)
	done := served(t, server, ln)
	// The three Stops are made against a Server that is demonstrably SERVING.
	// Without this the first Stop can win the race with Serve, which then
	// reports ErrServerStopped rather than serving -- correct behaviour, and a
	// different case from the one this asserts.
	waitServing(t, ln)

	for i := range 3 {
		if err := server.Stop(context.Background()); err != nil {
			t.Fatalf("Stop() call %d = %v, want no error", i+1, err)
		}
	}
	if err := awaitServe(t, done, "return after the first of three Stops"); err != nil {
		t.Errorf("Serve() = %v, want no error", err)
	}
}

// TestServeAppliesTheComposedHeaderReadTimeout is the socket bound the router's
// RouteLimits documentation assigns to this task, held by the one property that
// does not depend on how fast this machine is: a peer that sends a request line
// and never finishes its headers has its connection CLOSED by the server. The
// case waits as long as it takes; what it asserts is that the close happens at
// all, which is false for a server composed with no header timeout.
func TestServeAppliesTheComposedHeaderReadTimeout(t *testing.T) {
	t.Parallel()

	limits := factory.DefaultHTTPLimits()
	limits.ReadHeaderTimeout = 100 * time.Millisecond
	server, err := factory.New(append(factory.RequiredOptions(), factory.WithHTTPLimits(limits))...)
	if err != nil {
		t.Fatalf("New() = %v", err)
	}
	ln := listen(t)
	stopWhenDone(t, server, served(t, server, ln))

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("net.Dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// A request line and one header, and then nothing: the blank line that ends
	// the header block never arrives.
	if _, err := io.WriteString(conn, "GET /v1/bootstrap HTTP/1.1\r\nHost: app.example.com\r\n"); err != nil {
		t.Fatalf("writing the partial request: %v", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(60 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	if _, err := io.ReadAll(conn); err != nil {
		// A read deadline is this test's own bound and says nothing about the
		// server; anything else means the server ended the connection.
		if errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatal("the server never ended a connection whose headers never finished")
		}
	}
}

// TestServeReportsAListenerFailure is the other half of Serve's contract, and
// the half a passing suite is otherwise blind to.
//
// Serve reports a deliberate Stop as success, deliberately, so that a caller
// does not have to classify the ordinary ending. Everything else it must
// report AS IT WAS. Without this case a Serve that swallowed every listener
// error and returned nil would pass the whole suite, and a supervisor would
// read a server that never accepted a connection as a clean shutdown.
//
// A listener closed before Serve takes it is the cheapest real failure: the
// first Accept fails and there is nothing to tear down afterwards.
func TestServeReportsAListenerFailure(t *testing.T) {
	t.Parallel()

	server, err := factory.New(factory.RequiredOptions()...)
	if err != nil {
		t.Fatalf("New() = %v", err)
	}
	ln := listen(t)
	if err := ln.Close(); err != nil {
		t.Fatalf("closing the listener before Serve: %v", err)
	}

	err = awaitServe(t, served(t, server, ln), "report the listener's failure")
	if err == nil {
		t.Fatal("Serve() over a closed listener = nil, want the listener's error reported")
	}
	// The two lifecycle refusals are the errors this Server could return
	// WITHOUT ever reaching the listener, so a case that accepted either would
	// pass on a Serve that never tried to accept anything.
	if errors.Is(err, factory.ErrServerStopped) || errors.Is(err, factory.ErrAlreadyServing) {
		t.Fatalf("Serve() = %v, which is a lifecycle refusal rather than the listener's failure", err)
	}
	if !errors.Is(err, net.ErrClosed) {
		t.Errorf("Serve() = %v, want the closed listener's error (net.ErrClosed)", err)
	}
}
