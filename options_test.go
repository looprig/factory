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

// TestNewNamesEverySeamThatIsMissing is what makes the order the seams are
// checked in unobservable. Reporting the first missing seam would have made
// that order a contract -- reordering the check list would change which name a
// caller with two defects is told about -- while telling the caller less.
//
// The second row is the one that holds the SORT rather than merely passing a
// value through it. Its two seams are checked in the opposite order to the one
// they sort into, so deleting slices.Sort fails here; the first row's pair is
// already ascending in both, so it establishes completeness and says nothing
// about ordering. A row of the first kind alone left the sort with no reader at
// all, and the "the check order acquires no reader" claim resting on a line no
// test held.
func TestNewNamesEverySeamThatIsMissing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		drop []string
		want []string
	}{
		{
			name: "a pair whose check order is already its sorted order",
			drop: []string{"WithAuthorizer", "WithSessionReader"},
			want: []string{"WithAuthorizer", "WithSessionReader"},
		},
		{
			// WithCommands is checked fourth and WithCSRF last, but 'S' (0x53)
			// sorts before 'o' (0x6f), so the sorted answer inverts the pair.
			name: "a pair whose check order is the reverse of its sorted order",
			drop: []string{"WithCommands", "WithCSRF"},
			want: []string{"WithCSRF", "WithCommands"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			base := RequiredOptions()
			kept := slices.DeleteFunc(slices.Clone(base), func(o Option) bool {
				return slices.Contains(tt.drop, o.name)
			})
			if len(kept) != len(base)-len(tt.drop) {
				t.Fatalf("the seams this case drops (%v) are not all in RequiredOptions()", tt.drop)
			}
			server, err := New(kept...)
			if err == nil {
				t.Fatalf("New() without %v = %+v, want an error", tt.drop, server)
			}
			if !errors.Is(err, ErrMissingDependency) {
				t.Fatalf("error %v does not wrap ErrMissingDependency", err)
			}
			var missing *MissingSeamsError
			if !errors.As(err, &missing) {
				t.Fatalf("error %v is not a *MissingSeamsError", err)
			}
			if !slices.Equal(missing.Options, tt.want) {
				t.Errorf("MissingSeamsError.Options = %v, want %v", missing.Options, tt.want)
			}
			for _, name := range tt.want {
				if !strings.Contains(err.Error(), name) {
					t.Errorf("error %q does not name %q", err, name)
				}
			}
		})
	}
}

// TestMissingSeamsAreReportedBeforeAConflictingUI pins the check order New's
// own doc comment states. Both defects are present, so exactly one of the two
// answers is returned and the order is what decides which: a caller that
// supplied no seams at all is told about the seams, not about its UI.
func TestMissingSeamsAreReportedBeforeAConflictingUI(t *testing.T) {
	t.Parallel()

	server, err := New(WithUIHandler(stubHandler()), WithUIFS(stubFS()))
	if err == nil {
		t.Fatalf("New() with two UI options and no seams = %+v, want an error", server)
	}
	if errors.Is(err, ErrConflictingUI) {
		t.Fatalf("New() reported the UI conflict %v while every seam was also missing", err)
	}
	var missing *MissingSeamsError
	if !errors.As(err, &missing) {
		t.Fatalf("error %v is not a *MissingSeamsError", err)
	}
	if len(missing.Options) != len(RequiredOptions()) {
		t.Errorf("MissingSeamsError.Options = %v, want all %d required seams", missing.Options, len(RequiredOptions()))
	}
}

// TestMissingSeamsAreAlwaysReportedInSortedOrder is the property the two rows
// above sample. It exists because sampling was not enough: a COMPOUND mutation
// that deleted slices.Sort and simultaneously reordered the check list so that
// those particular two seams already came out in sorted order survived both
// rows. Any fixed set of pairs can be defeated the same way.
//
// So this drives every non-empty subset of the required seams -- 2^n-1 of them,
// which is 127 today -- and requires the report to be exactly the dropped set
// in sorted order. Deleting the sort now survives only if the check list is
// itself reordered into full sorted order, and that program is equivalent: the
// output is the sorted set for every input, which is the whole claim.
func TestMissingSeamsAreAlwaysReportedInSortedOrder(t *testing.T) {
	t.Parallel()

	base := RequiredOptions()
	if len(base) == 0 {
		t.Fatal("no seams are required, so this sweep is vacuous")
	}
	if len(base) > 16 {
		t.Fatalf("%d required seams is too many for an exhaustive sweep; make this a sampled property", len(base))
	}
	for mask := 1; mask < 1<<len(base); mask++ {
		var dropped []string
		var kept []Option
		for i, opt := range base {
			if mask&(1<<i) != 0 {
				dropped = append(dropped, opt.name)
			} else {
				kept = append(kept, opt)
			}
		}
		server, err := New(kept...)
		if err == nil {
			t.Fatalf("dropping %v: New() = %+v, want an error", dropped, server)
		}
		var missing *MissingSeamsError
		if !errors.As(err, &missing) {
			t.Fatalf("dropping %v: New() = %v, want a *MissingSeamsError", dropped, err)
		}
		// dropped is built in CHECK order; want is sorted here, independently.
		want := slices.Clone(dropped)
		slices.Sort(want)
		if !slices.Equal(missing.Options, want) {
			t.Fatalf("dropping %v: Options = %v, want %v", dropped, missing.Options, want)
		}
	}
}
