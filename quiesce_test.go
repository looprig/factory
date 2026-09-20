package factory_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/looprig/factory"
	internalidentity "github.com/looprig/factory/internal/identity"
)

func TestQuiesceStopsPublicAdmissionWithoutStoppingBackgroundLifecycle(t *testing.T) {
	s, err := factory.New(factory.RequiredOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.Quiesce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.Quiesce(context.Background()); err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/bootstrap", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("handler after Quiesce = %d, want 503", w.Code)
	}
	if err := s.Start(context.Background()); !errors.Is(err, factory.ErrServerQuiesced) {
		t.Fatalf("Start after Quiesce = %v", err)
	}
	if err := s.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestQuiesceBeforeStartRefusesServe(t *testing.T) {
	s, err := factory.New(factory.RequiredOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Quiesce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.Serve(listen(t)); !errors.Is(err, factory.ErrServerQuiesced) {
		t.Fatalf("Serve after Quiesce = %v", err)
	}
	if err := s.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestQuiesceLeavesAnExistingReadHandlerAndListenerUntilStop(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	ui := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		w.WriteHeader(http.StatusOK)
	})
	s, err := factory.New(append(factory.RequiredOptions(), factory.WithUIHandler(ui))...)
	if err != nil {
		t.Fatal(err)
	}
	ln := listen(t)
	served := served(t, s, ln)
	requestDone := make(chan error, 1)
	go func() {
		response, err := http.Get("http://" + ln.Addr().String() + "/index.html")
		if err == nil {
			_ = response.Body.Close()
		}
		requestDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(serveWait):
		t.Fatal("read handler did not enter")
	}
	ctx, cancel := context.WithTimeout(context.Background(), serveWait)
	defer cancel()
	if err := s.Quiesce(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-served:
		t.Fatalf("Serve ended during Quiesce: %v", err)
	default:
	}
	response, err := http.Get("http://" + ln.Addr().String() + "/new")
	if err != nil {
		t.Fatalf("listener closed during Quiesce: %v", err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("new request = %d", response.StatusCode)
	}
	close(release)
	if err := <-requestDone; err != nil {
		t.Fatal(err)
	}
	if err := s.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := awaitServe(t, served, "stop after quiescence"); err != nil {
		t.Fatal(err)
	}
}

func TestQuiesceClosesEstablishedClientLinkAndRefusesNewUpgrade(t *testing.T) {
	s := composed(t, &probe{})
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	public := httptest.NewServer(s.Handler())
	defer public.Close()
	address := public.Listener.Addr().String()
	cookie := internalidentity.DefaultCookieName + "=" + factoryFakeCredential
	conn, status := upgradeRealtime(t, address, cookie)
	defer conn.Close()
	if status != http.StatusSwitchingProtocols {
		t.Fatalf("initial upgrade = %d", status)
	}
	writeTextFrame(t, conn, connectFrame(t, ""))
	if opcode, _ := readFrame(t, conn); opcode != 0x1 {
		t.Fatalf("connect reply opcode = %x", opcode)
	}
	if err := s.Quiesce(context.Background()); err != nil {
		t.Fatal(err)
	}
	newConn, status := upgradeRealtime(t, address, cookie)
	_ = newConn.Close()
	if status != http.StatusServiceUnavailable {
		t.Fatalf("upgrade after Quiesce = %d", status)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	for {
		var b [1]byte
		_, err := conn.Read(b[:])
		if err != nil {
			if ne, ok := err.(interface{ Timeout() bool }); ok && ne.Timeout() {
				t.Fatal("established ClientLink remained open after Quiesce")
			}
			break
		}
		if b[0]&0x0f == 0x8 {
			break
		}
	}
	if err := s.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}
