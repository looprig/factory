package factory

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/looprig/factory/identity"
)

// TestRequiredOptionsCompose is the base assertion every negative case below
// depends on. A negative row proves the mutation was rejected only if the
// unmutated composition is accepted, so this runs first and asserts not merely
// that New returned no error but that every seam and every default is present.
func TestRequiredOptionsCompose(t *testing.T) {
	t.Parallel()

	server, err := New(RequiredOptions()...)
	if err != nil {
		t.Fatalf("New(RequiredOptions()...) = %v, want no error", err)
	}
	if server == nil {
		t.Fatal("New() returned a nil Server and no error")
	}
	present := map[string]bool{
		"authenticator": server.cfg.authenticator != nil,
		"authorizer":    server.cfg.authorizer != nil,
		"reads":         server.cfg.reads != nil,
		"commands":      server.cfg.commands != nil,
		"directory":     server.cfg.directory != nil,
		"placement":     server.cfg.placement != nil,
		"clock":         server.cfg.clock != nil,
		"uuids":         server.cfg.uuids != nil,
	}
	for name, ok := range present {
		if !ok {
			t.Errorf("composed Server has no %s", name)
		}
	}
	if got := server.ReconcileLimits(); got != DefaultReconcileLimits() {
		t.Errorf("ReconcileLimits() = %+v, want the default %+v", got, DefaultReconcileLimits())
	}
	if got := server.ClientLinkLimits(); got != DefaultClientLinkLimits() {
		t.Errorf("ClientLinkLimits() = %+v, want the default %+v", got, DefaultClientLinkLimits())
	}
	if got := server.HostLinkLimits(); got != DefaultHostLinkLimits() {
		t.Errorf("HostLinkLimits() = %+v, want the default %+v", got, DefaultHostLinkLimits())
	}
}

// TestNewRejectsAMissingSeam drops exactly one required option at a time. The
// table is derived from RequiredOptions rather than written out, so a seam that
// becomes required later is covered without anyone remembering to add a row.
func TestNewRejectsAMissingSeam(t *testing.T) {
	t.Parallel()

	base := RequiredOptions()
	if len(base) == 0 {
		t.Fatal("no options are required, so this test is vacuous")
	}
	for i, dropped := range base {
		t.Run(dropped.name, func(t *testing.T) {
			t.Parallel()

			kept := slices.Concat(base[:i:i], base[i+1:])
			server, err := New(kept...)
			if err == nil {
				t.Fatalf("New() without %s = %+v, want an error", dropped.name, server)
			}
			if server != nil {
				t.Errorf("New() returned a Server alongside its error")
			}
			if !errors.Is(err, ErrMissingDependency) {
				t.Errorf("error %v does not wrap ErrMissingDependency", err)
			}
			if !strings.Contains(err.Error(), dropped.name) {
				t.Errorf("error %q does not name the missing option %q", err, dropped.name)
			}
		})
	}
}

// TestNewRejectsADuplicateOption supplies each required option twice. Last-wins
// is the alternative and it is worse: two WithCSRF calls with different keys
// would compose silently and the running replica would sign with whichever the
// caller happened to list second.
func TestNewRejectsADuplicateOption(t *testing.T) {
	t.Parallel()

	base := RequiredOptions()
	for _, repeated := range base {
		t.Run(repeated.name, func(t *testing.T) {
			t.Parallel()

			server, err := New(append(slices.Clone(base), repeated)...)
			if err == nil {
				t.Fatalf("New() with a repeated %s = %+v, want an error", repeated.name, server)
			}
			if server != nil {
				t.Errorf("New() returned a Server alongside its error")
			}
			if !errors.Is(err, ErrDuplicateOption) {
				t.Errorf("error %v does not wrap ErrDuplicateOption", err)
			}
			if !strings.Contains(err.Error(), repeated.name) {
				t.Errorf("error %q does not name the repeated option %q", err, repeated.name)
			}
		})
	}
}

func TestNewRejectsANilOption(t *testing.T) {
	t.Parallel()

	server, err := New(append(RequiredOptions(), Option{})...)
	if err == nil {
		t.Fatalf("New() with a zero Option = %+v, want an error", server)
	}
	if server != nil {
		t.Errorf("New() returned a Server alongside its error")
	}
	if !errors.Is(err, ErrNilOption) {
		t.Errorf("error %v does not wrap ErrNilOption", err)
	}
}

// TestNewRejectsANilDependency covers every option that carries an interface or
// a handler. A nil there is not a request for the default: the defaulted seams
// are defaulted by their ABSENCE, so an explicit nil is a caller passing a
// dependency it failed to build.
func TestNewRejectsANilDependency(t *testing.T) {
	t.Parallel()

	tests := []Option{
		WithAuthenticator(nil),
		WithAuthorizer(nil),
		WithSessionReader(nil),
		WithCommands(nil),
		WithDirectory(nil),
		WithPlacementController(nil),
		WithClock(nil),
		WithUUIDSource(nil),
		WithUIHandler(nil),
		WithUIFS(nil),
	}
	for _, opt := range tests {
		t.Run(opt.name, func(t *testing.T) {
			t.Parallel()

			options := slices.Clone(RequiredOptions())
			options = slices.DeleteFunc(options, func(o Option) bool { return o.name == opt.name })
			server, err := New(append(options, opt)...)
			if err == nil {
				t.Fatalf("New() with %s(nil) = %+v, want an error", opt.name, server)
			}
			if server != nil {
				t.Errorf("New() returned a Server alongside its error")
			}
			if !errors.Is(err, ErrNilDependency) {
				t.Errorf("error %v does not wrap ErrNilDependency", err)
			}
			if !strings.Contains(err.Error(), opt.name) {
				t.Errorf("error %q does not name the option %q", err, opt.name)
			}
		})
	}
}

func TestNewRejectsBothUIOptionsAtOnce(t *testing.T) {
	t.Parallel()

	options := append(RequiredOptions(),
		WithUIHandler(stubHandler()),
		WithUIFS(stubFS()),
	)
	server, err := New(options...)
	if err == nil {
		t.Fatalf("New() with both UI options = %+v, want an error", server)
	}
	if !errors.Is(err, ErrConflictingUI) {
		t.Errorf("error %v does not wrap ErrConflictingUI", err)
	}
	for _, name := range []string{"WithUIHandler", "WithUIFS"} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("error %q does not name %q", err, name)
		}
	}
}

func TestNewRejectsAnInvalidCSRFConfiguration(t *testing.T) {
	t.Parallel()

	options := slices.DeleteFunc(slices.Clone(RequiredOptions()), func(o Option) bool { return o.name == "WithCSRF" })
	short := ValidCSRF()
	short.SharedKey = short.SharedKey[:1]
	server, err := New(append(options, WithCSRF(short))...)
	if err == nil {
		t.Fatalf("New() with a one-byte CSRF key = %+v, want an error", server)
	}
	if !errors.Is(err, identity.ErrInvalidCSRFConfig) {
		t.Errorf("error %v does not wrap identity.ErrInvalidCSRFConfig", err)
	}
	if !strings.Contains(err.Error(), "WithCSRF") {
		t.Errorf("error %q does not name WithCSRF", err)
	}
}

func TestNewRejectsInvalidLimits(t *testing.T) {
	t.Parallel()

	tests := []Option{
		WithReconcileLimits(ReconcileLimits{}),
		WithClientLinkLimits(ClientLinkLimits{}),
		WithHostLinkLimits(HostLinkLimits{}),
	}
	for _, opt := range tests {
		t.Run(opt.name, func(t *testing.T) {
			t.Parallel()

			server, err := New(append(RequiredOptions(), opt)...)
			if err == nil {
				t.Fatalf("New() with zero limits from %s = %+v, want an error", opt.name, server)
			}
			if !errors.Is(err, ErrInvalidLimits) {
				t.Errorf("error %v does not wrap ErrInvalidLimits", err)
			}
			if !strings.Contains(err.Error(), opt.name) {
				t.Errorf("error %q does not name %q", err, opt.name)
			}
		})
	}
}

// TestExplicitLimitsReplaceTheDefaults is the reader that separates "the option
// was accepted" from "the option was applied". Without it, an apply function
// that dropped its argument would pass every test above.
func TestExplicitLimitsReplaceTheDefaults(t *testing.T) {
	t.Parallel()

	reconcile := DefaultReconcileLimits()
	reconcile.MaxDuePerSweep = 7
	client := DefaultClientLinkLimits()
	client.PerConnectionQueue = 11
	host := DefaultHostLinkLimits()
	host.MaxLinks = 13

	server, err := New(append(RequiredOptions(),
		WithReconcileLimits(reconcile),
		WithClientLinkLimits(client),
		WithHostLinkLimits(host),
	)...)
	if err != nil {
		t.Fatalf("New() = %v, want no error", err)
	}
	if got := server.ReconcileLimits(); got != reconcile {
		t.Errorf("ReconcileLimits() = %+v, want %+v", got, reconcile)
	}
	if got := server.ClientLinkLimits(); got != client {
		t.Errorf("ClientLinkLimits() = %+v, want %+v", got, client)
	}
	if got := server.HostLinkLimits(); got != host {
		t.Errorf("HostLinkLimits() = %+v, want %+v", got, host)
	}
}

// TestExplicitClockAndUUIDSourceReplaceTheDefaults is the same reader for the
// two seams whose defaults are constructed rather than copied.
func TestExplicitClockAndUUIDSourceReplaceTheDefaults(t *testing.T) {
	t.Parallel()

	server, err := New(append(RequiredOptions(), WithClock(FakeClock{}), WithUUIDSource(FakeUUIDs{}))...)
	if err != nil {
		t.Fatalf("New() = %v, want no error", err)
	}
	if _, ok := server.cfg.clock.(FakeClock); !ok {
		t.Errorf("clock is %T, want FakeClock", server.cfg.clock)
	}
	if _, ok := server.cfg.uuids.(FakeUUIDs); !ok {
		t.Errorf("uuid source is %T, want FakeUUIDs", server.cfg.uuids)
	}
	if got := server.cfg.clock.Now(); !got.Equal(time.Unix(0, 0).UTC()) {
		t.Errorf("clock.Now() = %v, want the fake's fixed instant", got)
	}
}

// TestNewCopiesTheCallersCSRFConfiguration finds the reader of the difference
// between storing the caller's struct and storing a copy that owns its bytes:
// the key a later replica-shared signature is computed from.
func TestNewCopiesTheCallersCSRFConfiguration(t *testing.T) {
	t.Parallel()

	supplied := ValidCSRF()
	options := slices.DeleteFunc(slices.Clone(RequiredOptions()), func(o Option) bool { return o.name == "WithCSRF" })
	server, err := New(append(options, WithCSRF(supplied))...)
	if err != nil {
		t.Fatalf("New() = %v, want no error", err)
	}
	want := string(supplied.SharedKey)
	wantOrigin := supplied.TrustedOrigins[0]

	for i := range supplied.SharedKey {
		supplied.SharedKey[i] = 0
	}
	supplied.TrustedOrigins[0] = "https://evil.example.com"

	if got := string(server.CSRF().SharedKey); got != want {
		t.Errorf("server key became %q after the caller zeroed its buffer, want %q", got, want)
	}
	if got := server.CSRF().TrustedOrigins[0]; got != wantOrigin {
		t.Errorf("server origin became %q after the caller overwrote its slice, want %q", got, wantOrigin)
	}

	// And the accessor must not hand out the server's own memory either.
	handed := server.CSRF()
	for i := range handed.SharedKey {
		handed.SharedKey[i] = 0
	}
	handed.TrustedOrigins[0] = "https://evil.example.com"
	if got := string(server.CSRF().SharedKey); got != want {
		t.Errorf("server key became %q after a caller mutated an accessor result, want %q", got, want)
	}
	if got := server.CSRF().TrustedOrigins[0]; got != wantOrigin {
		t.Errorf("server origin became %q after a caller mutated an accessor result, want %q", got, wantOrigin)
	}
}
