package factory

import (
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"strings"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
)

// Composition errors. Every failure New reports wraps one of these sentinels,
// so errors.Is classifies any of them.
//
// The CONCRETE type varies and a caller that switches on it must handle three
// shapes: an *OptionError naming one option, a *MissingSeamsError naming every
// seam that was not supplied -- the most common failure, and NOT an
// *OptionError -- and ErrConflictingUI returned bare, since it belongs to two
// options rather than one.
var (
	// ErrNilOption reports a nil entry in the option slice.
	ErrNilOption = errors.New("factory: nil option")
	// ErrDuplicateOption reports an option supplied more than once.
	ErrDuplicateOption = errors.New("factory: option supplied more than once")
	// ErrNilDependency reports an option carrying a nil interface value.
	ErrNilDependency = errors.New("factory: nil dependency")
	// ErrMissingDependency reports a required seam nothing supplied.
	ErrMissingDependency = errors.New("factory: required seam is missing")
	// ErrConflictingUI reports both UI options supplied at once.
	ErrConflictingUI = errors.New("factory: WithUIHandler and WithUIFS are mutually exclusive")
	// ErrInvalidLimits reports a limit value or relationship this composition
	// refuses.
	ErrInvalidLimits = errors.New("factory: invalid limits")
)

// OptionError names the option a composition failure belongs to.
type OptionError struct {
	// Option is the constructor's name, or the name of the option that had to
	// be supplied and was not.
	Option string
	Err    error
}

func (e *OptionError) Error() string { return e.Option + ": " + e.Err.Error() }
func (e *OptionError) Unwrap() error { return e.Err }

// MissingSeamsError names every seam a composition failed to supply. It is not
// an *OptionError, because it belongs to a set of options rather than to one.
//
// All of them are reported at once, and SORTED, for two reasons. A caller with
// three missing seams is not sent round the build-and-fail loop three times;
// and the order the seams are checked in acquires no reader, so it stays what
// it is -- an ordering chosen for readability -- rather than becoming a
// contract nobody meant to make. The sort is held by a case whose check order
// and sorted order DIFFER; without one, deleting the sort would change nothing
// observable and that second claim would quietly stop being true.
type MissingSeamsError struct {
	// Options are the option constructors that had to be supplied and were not.
	Options []string
}

func (e *MissingSeamsError) Error() string {
	return "factory: required seams are missing: " + strings.Join(e.Options, ", ")
}

func (e *MissingSeamsError) Unwrap() error { return ErrMissingDependency }

// Option configures a Server. An option is applied at most once; supplying one
// twice is an error rather than a silent last-wins.
type Option struct {
	name  string
	apply func(*config) error
}

type config struct {
	verifier      identity.Verifier
	cookieName    string
	defaultTenant sessionwire.TenantID
	authorizer    Authorizer
	reads         SessionReader
	commands      Commands
	directory     Directory
	placement     PlacementController

	clock Clock
	uuids UUIDSource

	csrf      identity.CSRFConfig
	csrfSet   bool
	http      HTTPLimits
	reconcile ReconcileLimits
	client    ClientLinkLimits
	host      HostLinkLimits

	ui   http.Handler
	uiFS fs.FS
}

func option(name string, apply func(*config) error) Option {
	return Option{name: name, apply: apply}
}

// nilDependency is what every option returns for an explicit nil. A nil is not
// a request for the default: the defaulted seams are defaulted by their
// ABSENCE, so a nil here is a caller passing a dependency it failed to build.
func nilDependency(name string) error {
	return &OptionError{Option: name, Err: ErrNilDependency}
}

// WithCredentialVerifier supplies the credential verifier.
//
// This is the authentication seam, and it is deliberately NOT an authenticator.
// Which carrier a credential is read from, which one wins when a request
// presents two, and what the operation context records about the one that
// authenticated are decided in exactly one place, because a second answer to
// "which credential authenticated this request" is an origin guard that skips
// its CSRF rules silently -- internal/httpapi.GuardConfig.Credentials states
// that at length. What a deployment genuinely owns is what makes a presented
// credential valid and what it asserts, which is this interface.
func WithCredentialVerifier(v identity.Verifier) Option {
	return option("WithCredentialVerifier", func(c *config) error {
		if v == nil {
			return nilDependency("WithCredentialVerifier")
		}
		c.verifier = v
		return nil
	})
}

// WithSessionCookieName replaces the browser session cookie a request is read
// from when it carries no Authorization header.
//
// A name that is not a valid cookie name is refused by New rather than at the
// first request: net/http DROPS such a cookie when it writes one, so the
// composition would look configured and never authenticate anybody.
func WithSessionCookieName(name string) Option {
	return option("WithSessionCookieName", func(c *config) error { c.cookieName = name; return nil })
}

// WithDefaultTenant scopes an actor credential that names no tenant.
//
// Leaving it unset requires every credential to name its own tenant, which is
// the cloud deployment's shape; a single-tenant local composition sets it. It
// applies to an ACTOR only -- a service credential that named no tenant would
// otherwise acquire one by configuration.
func WithDefaultTenant(tenant sessionwire.TenantID) Option {
	return option("WithDefaultTenant", func(c *config) error { c.defaultTenant = tenant; return nil })
}

// WithAuthorizer supplies the authorizer for every public operation.
func WithAuthorizer(a Authorizer) Option {
	return option("WithAuthorizer", func(c *config) error {
		if a == nil {
			return nilDependency("WithAuthorizer")
		}
		c.authorizer = a
		return nil
	})
}

// WithSessionReader supplies the durable read plane.
func WithSessionReader(r SessionReader) Option {
	return option("WithSessionReader", func(c *config) error {
		if r == nil {
			return nilDependency("WithSessionReader")
		}
		c.reads = r
		return nil
	})
}

// WithCommands supplies the durable command plane.
func WithCommands(cmds Commands) Option {
	return option("WithCommands", func(c *config) error {
		if cmds == nil {
			return nilDependency("WithCommands")
		}
		c.commands = cmds
		return nil
	})
}

// WithDirectory supplies the observed target directory.
func WithDirectory(d Directory) Option {
	return option("WithDirectory", func(c *config) error {
		if d == nil {
			return nilDependency("WithDirectory")
		}
		c.directory = d
		return nil
	})
}

// WithPlacementController supplies the placement controller.
func WithPlacementController(p PlacementController) Option {
	return option("WithPlacementController", func(c *config) error {
		if p == nil {
			return nilDependency("WithPlacementController")
		}
		c.placement = p
		return nil
	})
}

// WithClock replaces the system clock.
func WithClock(clock Clock) Option {
	return option("WithClock", func(c *config) error {
		if clock == nil {
			return nilDependency("WithClock")
		}
		c.clock = clock
		return nil
	})
}

// WithUUIDSource replaces the random identifier source.
func WithUUIDSource(u UUIDSource) Option {
	return option("WithUUIDSource", func(c *config) error {
		if u == nil {
			return nilDependency("WithUUIDSource")
		}
		c.uuids = u
		return nil
	})
}

// WithCSRF supplies the origin and CSRF configuration. It has no default.
func WithCSRF(cfg identity.CSRFConfig) Option {
	return option("WithCSRF", func(c *config) error { c.csrf = cfg; c.csrfSet = true; return nil })
}

// WithHTTPLimits replaces the connection limits Serve applies. They bound the
// socket, so they say nothing about a library embedding that uses Handler and
// supplies its own http.Server.
func WithHTTPLimits(l HTTPLimits) Option {
	return option("WithHTTPLimits", func(c *config) error { c.http = l; return nil })
}

// WithReconcileLimits replaces the reconciliation limits.
func WithReconcileLimits(l ReconcileLimits) Option {
	return option("WithReconcileLimits", func(c *config) error { c.reconcile = l; return nil })
}

// WithClientLinkLimits replaces the local ClientLink and fan-out queue limits.
func WithClientLinkLimits(l ClientLinkLimits) Option {
	return option("WithClientLinkLimits", func(c *config) error { c.client = l; return nil })
}

// WithHostLinkLimits replaces the HostLink pool limits.
func WithHostLinkLimits(l HostLinkLimits) Option {
	return option("WithHostLinkLimits", func(c *config) error { c.host = l; return nil })
}

// WithUIHandler mounts a caller-supplied user interface handler.
func WithUIHandler(h http.Handler) Option {
	return option("WithUIHandler", func(c *config) error {
		if h == nil {
			return nilDependency("WithUIHandler")
		}
		c.ui = h
		return nil
	})
}

// WithUIFS mounts a static user interface bundle.
func WithUIFS(dir fs.FS) Option {
	return option("WithUIFS", func(c *config) error {
		if dir == nil {
			return nilDependency("WithUIFS")
		}
		c.uiFS = dir
		return nil
	})
}

// ReconcileLimits bounds Factory's periodic reconciliation of pending commands
// with no live owner.
type ReconcileLimits struct {
	// Interval is the sweep cadence.
	Interval time.Duration
	// ClaimTTL is how long a reconciliation claim suppresses duplicate work.
	ClaimTTL time.Duration
	// ApplyDeadline is how long an accepted command may stay unapplied before
	// it becomes rejected/runtime_unavailable.
	ApplyDeadline time.Duration
	// MaxDuePerSweep bounds the due records one sweep withdraws.
	MaxDuePerSweep int
	// MaxConcurrent bounds concurrent reconciliations in this replica.
	MaxConcurrent int
}

// DefaultReconcileLimits is the configuration a composition gets if it names
// none.
func DefaultReconcileLimits() ReconcileLimits {
	return ReconcileLimits{
		Interval:       5 * time.Second,
		ClaimTTL:       30 * time.Second,
		ApplyDeadline:  5 * time.Minute,
		MaxDuePerSweep: 256,
		MaxConcurrent:  8,
	}
}

// Validate reports why these limits may not be used.
func (l ReconcileLimits) Validate() error {
	if err := positive("ReconcileLimits.Interval", l.Interval); err != nil {
		return err
	}
	if err := positive("ReconcileLimits.ClaimTTL", l.ClaimTTL); err != nil {
		return err
	}
	if err := positive("ReconcileLimits.ApplyDeadline", l.ApplyDeadline); err != nil {
		return err
	}
	if err := atLeastOne("ReconcileLimits.MaxDuePerSweep", l.MaxDuePerSweep); err != nil {
		return err
	}
	if err := atLeastOne("ReconcileLimits.MaxConcurrent", l.MaxConcurrent); err != nil {
		return err
	}
	// A sweep cadence at or beyond the claim TTL means every claim this replica
	// took has expired by the time the next sweep looks at it, so claims stop
	// suppressing duplicate placement work.
	if l.Interval >= l.ClaimTTL {
		return fmt.Errorf("%w: ReconcileLimits.Interval (%v) must be shorter than ClaimTTL (%v)",
			ErrInvalidLimits, l.Interval, l.ClaimTTL)
	}
	// A claim never extends a command's apply deadline, so a TTL at or beyond
	// the deadline describes a claim that outlives the command it was taken for.
	if l.ClaimTTL >= l.ApplyDeadline {
		return fmt.Errorf("%w: ReconcileLimits.ClaimTTL (%v) must be shorter than ApplyDeadline (%v)",
			ErrInvalidLimits, l.ClaimTTL, l.ApplyDeadline)
	}
	return nil
}

// ClientLinkLimits bounds this replica's local connections and fan-out queues.
type ClientLinkLimits struct {
	// MaxConnections bounds concurrent ClientLinks on this replica.
	MaxConnections int
	// PerConnectionQueue bounds one connection's outbound queue in messages.
	PerConnectionQueue int
	// WriteTimeout bounds one outbound write.
	WriteTimeout time.Duration
	// PingInterval is how often the server pings an idle connection.
	PingInterval time.Duration
	// PongTimeout is how long a ping may go unanswered.
	PongTimeout time.Duration
}

// DefaultClientLinkLimits is the configuration a composition gets if it names
// none. MaxConnections is sized for the 1,000-5,000 connection scale a Factory
// replica is expected to hold.
func DefaultClientLinkLimits() ClientLinkLimits {
	return ClientLinkLimits{
		MaxConnections:     5000,
		PerConnectionQueue: 256,
		WriteTimeout:       5 * time.Second,
		PingInterval:       25 * time.Second,
		PongTimeout:        10 * time.Second,
	}
}

// Validate reports why these limits may not be used.
func (l ClientLinkLimits) Validate() error {
	if err := atLeastOne("ClientLinkLimits.MaxConnections", l.MaxConnections); err != nil {
		return err
	}
	if err := atLeastOne("ClientLinkLimits.PerConnectionQueue", l.PerConnectionQueue); err != nil {
		return err
	}
	if err := positive("ClientLinkLimits.WriteTimeout", l.WriteTimeout); err != nil {
		return err
	}
	if err := positive("ClientLinkLimits.PingInterval", l.PingInterval); err != nil {
		return err
	}
	if err := positive("ClientLinkLimits.PongTimeout", l.PongTimeout); err != nil {
		return err
	}
	// A pong deadline at or beyond the ping cadence never separates a slow peer
	// from a dead one: the next ping is sent before the previous one's deadline
	// has been reached.
	if l.PongTimeout >= l.PingInterval {
		return fmt.Errorf("%w: ClientLinkLimits.PongTimeout (%v) must be shorter than PingInterval (%v)",
			ErrInvalidLimits, l.PongTimeout, l.PingInterval)
	}
	// A write allowed to block past the liveness deadline holds the connection
	// that deadline exists to reclaim.
	if l.WriteTimeout > l.PongTimeout {
		return fmt.Errorf("%w: ClientLinkLimits.WriteTimeout (%v) must not exceed PongTimeout (%v)",
			ErrInvalidLimits, l.WriteTimeout, l.PongTimeout)
	}
	return nil
}

// HostLinkLimits bounds this replica's demand-driven HostLink pool.
type HostLinkLimits struct {
	// MaxLinks bounds concurrent HostLinks. One link multiplexes every session
	// binding to one Host, so this bounds Hosts, not sessions.
	MaxLinks int
	// DialTimeout bounds one dial.
	DialTimeout time.Duration
	// IdleTimeout is how long a link with no local demand is kept.
	IdleTimeout time.Duration
	// ReconnectMin and ReconnectMax bound the reconnect backoff.
	ReconnectMin time.Duration
	ReconnectMax time.Duration
}

// DefaultHostLinkLimits is the configuration a composition gets if it names
// none.
func DefaultHostLinkLimits() HostLinkLimits {
	return HostLinkLimits{
		MaxLinks:     256,
		DialTimeout:  5 * time.Second,
		IdleTimeout:  60 * time.Second,
		ReconnectMin: 250 * time.Millisecond,
		ReconnectMax: 10 * time.Second,
	}
}

// Validate reports why these limits may not be used.
func (l HostLinkLimits) Validate() error {
	if err := atLeastOne("HostLinkLimits.MaxLinks", l.MaxLinks); err != nil {
		return err
	}
	if err := positive("HostLinkLimits.DialTimeout", l.DialTimeout); err != nil {
		return err
	}
	if err := positive("HostLinkLimits.IdleTimeout", l.IdleTimeout); err != nil {
		return err
	}
	if err := positive("HostLinkLimits.ReconnectMin", l.ReconnectMin); err != nil {
		return err
	}
	if err := positive("HostLinkLimits.ReconnectMax", l.ReconnectMax); err != nil {
		return err
	}
	if l.ReconnectMin > l.ReconnectMax {
		return fmt.Errorf("%w: HostLinkLimits.ReconnectMin (%v) must not exceed ReconnectMax (%v)",
			ErrInvalidLimits, l.ReconnectMin, l.ReconnectMax)
	}
	// A link is opened on local subscription demand. If a dial may take longer
	// than the idle window, a link can become reapable before it has served the
	// demand that opened it.
	if l.DialTimeout > l.IdleTimeout {
		return fmt.Errorf("%w: HostLinkLimits.DialTimeout (%v) must not exceed IdleTimeout (%v)",
			ErrInvalidLimits, l.DialTimeout, l.IdleTimeout)
	}
	return nil
}

func positive(name string, d time.Duration) error {
	if d <= 0 {
		return fmt.Errorf("%w: %s is %v, want a positive duration", ErrInvalidLimits, name, d)
	}
	return nil
}

func atLeastOne(name string, n int) error {
	if n < 1 {
		return fmt.Errorf("%w: %s is %d, want at least 1", ErrInvalidLimits, name, n)
	}
	return nil
}

// CSRF returns the composed origin and CSRF configuration. The result shares no
// memory with the Server's copy, so a caller that inspects and then reuses the
// returned key buffer cannot change what a running Factory signs with.
func (s *Server) CSRF() identity.CSRFConfig { return s.cfg.csrf.Clone() }

// ReconcileLimits returns the composed reconciliation limits.
func (s *Server) ReconcileLimits() ReconcileLimits { return s.cfg.reconcile }

// ClientLinkLimits returns the composed ClientLink limits.
func (s *Server) ClientLinkLimits() ClientLinkLimits { return s.cfg.client }

// HostLinkLimits returns the composed HostLink pool limits.
func (s *Server) HostLinkLimits() HostLinkLimits { return s.cfg.host }
