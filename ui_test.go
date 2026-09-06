package factory_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/fstest"

	"github.com/looprig/factory"
)

// TestLibraryCompositionWithoutAUIIsValid is the no-UI path, driven rather than
// asserted about. It is not enough that New tolerates the absence of a UI
// option: the composition must be COMPLETE without one, so this reads the
// configuration a serving Factory would read and requires it to be the same as
// a UI-bearing composition's in every respect but the UI.
func TestLibraryCompositionWithoutAUIIsValid(t *testing.T) {
	t.Parallel()

	server, err := factory.New(factory.RequiredOptions()...)
	if err != nil {
		t.Fatalf("New() with no UI option = %v, want no error", err)
	}
	handler, ok := server.UI()
	if ok {
		t.Errorf("UI() reported a UI in a composition that supplied none")
	}
	if handler != nil {
		t.Errorf("UI() = %T, want nil when there is no UI", handler)
	}
	if got := server.CSRF().TrustedOrigins; len(got) == 0 {
		t.Error("a UI-less composition has no trusted origins, so it is not fully composed")
	}
	if server.ReconcileLimits() != factory.DefaultReconcileLimits() {
		t.Error("a UI-less composition did not receive the default reconciliation limits")
	}
}

// TestUIPresenceIsTheOnlyDifference finds the readers of the with-UI and
// no-UI compositions and requires every one of them but UI() to agree. A
// "no UI" implemented by skipping later composition steps would fail here.
func TestUIPresenceIsTheOnlyDifference(t *testing.T) {
	t.Parallel()

	without, err := factory.New(factory.RequiredOptions()...)
	if err != nil {
		t.Fatalf("New() without a UI = %v", err)
	}
	with, err := factory.New(append(factory.RequiredOptions(), factory.WithUIFS(uiBundle()))...)
	if err != nil {
		t.Fatalf("New() with a UI = %v", err)
	}

	if without.HTTPLimits() != with.HTTPLimits() {
		t.Errorf("HTTP limits differ: %+v vs %+v", without.HTTPLimits(), with.HTTPLimits())
	}
	if without.ReconcileLimits() != with.ReconcileLimits() {
		t.Errorf("reconciliation limits differ: %+v vs %+v", without.ReconcileLimits(), with.ReconcileLimits())
	}
	if without.ClientLinkLimits() != with.ClientLinkLimits() {
		t.Errorf("ClientLink limits differ: %+v vs %+v", without.ClientLinkLimits(), with.ClientLinkLimits())
	}
	if without.HostLinkLimits() != with.HostLinkLimits() {
		t.Errorf("HostLink limits differ: %+v vs %+v", without.HostLinkLimits(), with.HostLinkLimits())
	}
	if string(without.CSRF().SharedKey) != string(with.CSRF().SharedKey) {
		t.Error("CSRF keys differ between the two compositions")
	}

	_, withoutUI := without.UI()
	_, withUI := with.UI()
	if withoutUI == withUI {
		t.Fatalf("UI() reported %t for both compositions, so nothing reads the difference", withUI)
	}
}

// TestUIHandlerIsMountedUnchanged asserts the handler a caller supplied is the
// handler UI() returns, by driving it. Identity by pointer would be defeated by
// a wrapper that dropped the body; a marker response is not.
func TestUIHandlerIsMountedUnchanged(t *testing.T) {
	t.Parallel()

	const marker = "supplied-by-the-caller"
	supplied := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, marker)
	})
	server, err := factory.New(append(factory.RequiredOptions(), factory.WithUIHandler(supplied))...)
	if err != nil {
		t.Fatalf("New() = %v, want no error", err)
	}
	handler, ok := server.UI()
	if !ok {
		t.Fatal("UI() reported no UI after WithUIHandler")
	}
	if body := serve(t, handler, "/anything"); body != marker {
		t.Fatalf("the mounted handler served %q, want %q", body, marker)
	}
}

// TestUIFSIsServed drives the second UI shape all the way to a response body.
// Storing the fs.FS and never turning it into a handler would satisfy a
// non-nil check and serve nothing.
func TestUIFSIsServed(t *testing.T) {
	t.Parallel()

	server, err := factory.New(append(factory.RequiredOptions(), factory.WithUIFS(uiBundle()))...)
	if err != nil {
		t.Fatalf("New() = %v, want no error", err)
	}
	handler, ok := server.UI()
	if !ok {
		t.Fatal("UI() reported no UI after WithUIFS")
	}
	if body := serve(t, handler, "/app.js"); body != "console.log(1)" {
		t.Errorf("the bundle served %q for /app.js", body)
	}
	if body := serve(t, handler, "/"); body != "<!doctype html>" {
		t.Errorf("the bundle served %q for /, want its index", body)
	}
}

func uiBundle() fstest.MapFS {
	return fstest.MapFS{
		"index.html": &fstest.MapFile{Data: []byte("<!doctype html>")},
		"app.js":     &fstest.MapFile{Data: []byte("console.log(1)")},
	}
}

func serve(t *testing.T, handler http.Handler, target string) string {
	t.Helper()

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, target, nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET %s = %d, want 200", target, recorder.Code)
	}
	return recorder.Body.String()
}
