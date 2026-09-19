package routing

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
)

// ErrDemandClosed reports work asked of a closed demand plane.
var ErrDemandClosed = errors.New("routing: demand plane is closed")

// TipReader is the durable journal read this package uses to answer "how far
// has this session got", and NOTHING else.
//
// It is the SessionStore domain method, declared here as the narrow interface
// THIS package calls, for internal/httpapi's SessionReader's reason. Only
// JournalPage.CapturedTip is read: the events a page carries are not looked at,
// are not forwarded, and are not authorized here -- a hint says how far the
// journal goes, and the client reads the range it is missing through the
// durable query plane, which authorizes it in its own right.
type TipReader interface {
	ReadPublicJournal(ctx context.Context, req sessionstore.ReadPublicJournalRequest) (sessionwire.JournalPage, error)
}

// Hinter publishes one repair hint to this replica's LOCAL subscribers.
//
// Local is the whole of it. Specification section 10.5 gives version one no
// cross-node publication path, so a hint reaches the clients of this replica
// and no other; a second replica watching the same session polls and publishes
// its own. That is why the hint has to be repeatable rather than sequenced --
// see Demand.hintLocked.
type Hinter interface {
	PublishJournalTip(ctx context.Context, hint sessionwire.JournalTip) error
}

// Clock is the time seam, and this package reads exactly one thing from it:
// when to run the next ownership poll.
//
// It declares AfterFunc and not Now, for clientlink.Clock's reason: nothing
// here reads a wall clock, and a Now no caller called would be a method every
// composition supplies identically and no test could distinguish. factory.Clock
// is assignable to it without an adapter.
type Clock interface {
	// AfterFunc runs f after d and returns a stop function reporting whether
	// it prevented the call.
	AfterFunc(d time.Duration, f func()) (stop func() bool)
}

// DemandLimits bounds the background work one watched session may cost.
//
// Both members are REQUIRED and positive. A zero poll interval is a busy loop
// against the registry and a zero timeout is an unbounded store call on a
// goroutine with no caller to abandon it, and neither is a sane default to
// infer for a composition that forgot to say.
type DemandLimits struct {
	// OwnershipPollInterval is how long after one poll finishes the next is
	// armed. It is a gap between polls rather than a period, so a poll slower
	// than the interval delays the next one instead of overlapping with it.
	OwnershipPollInterval time.Duration

	// PollTimeout bounds ONE whole poll -- the registry read, the bind it may
	// provoke, the tip read and the publish together -- rather than each call
	// inside it. A poll runs on the clock's goroutine with no caller and no
	// request context to inherit, so this is the entire bound on it.
	PollTimeout time.Duration
}

// DefaultDemandLimits returns bounded defaults for demand-driven binding.
//
// Neither number is measured, and saying so is the point. The interval is the
// bounded delay specification section 10.5 promises a viewer on Factory A of
// work started through Factory B, and five seconds is a judgement about what a
// person watching a page will tolerate, not a figure derived from anything. The
// timeout is larger than the interval on purpose: a poll that is merely slow
// must not be abandoned in favour of a fresh one that will be just as slow.
func DefaultDemandLimits() DemandLimits {
	return DemandLimits{
		OwnershipPollInterval: 5 * time.Second,
		PollTimeout:           10 * time.Second,
	}
}

// Validate reports why these limits may not bound a demand plane.
func (l DemandLimits) Validate() error {
	if l.OwnershipPollInterval <= 0 {
		return fmt.Errorf("%w: OwnershipPollInterval must be positive", ErrInvalidConfig)
	}
	if l.PollTimeout <= 0 {
		return fmt.Errorf("%w: PollTimeout must be positive", ErrInvalidConfig)
	}
	return nil
}

// tipReadLimit is the page size of a tip read.
//
// The tip is a property of the scan PLAN -- SessionStore captures the ledger
// tip before it walks anything -- so a tip read wants the smallest page the
// store will build, not a useful one. Tail keeps the walk at the end of the
// journal instead of from sequence one, and ScanLimit bounds the records
// examined including the private ones a public page withholds. Both are set:
// Tail already bounds this walk to one record, and ScanLimit is the bound that
// still holds if a later store release changes what Tail means.
const tipReadLimit = 1

// Demand is this replica's LOCAL subscriber demand, and the binding and
// polling that demand drives.
//
// It sits directly on *Bindings rather than on an interface, and takes the
// resolver from it rather than being handed one. That is the single-authority
// property, by construction: the registry read a poll makes to decide whether a
// route is still current is THE SAME reader the table binds through, so there
// is no second ownership authority here that could disagree with the routing
// table about who owns a session.
//
// # What Acquire does NOT do, and why it is the point
//
// A first subscriber that finds no owner is left UNBOUND and is not refused.
// Viewing is not a reason to make a cold session resident: a restore is a
// durable COMMAND, admitted through internal/admission by a caller who asked
// for one, and a viewer asked only to watch. Nothing on this path can admit
// anything -- the seams above are a routing table, a journal READ and a local
// publish -- and the module-wide guard in the root package holds that for the
// composition, where the seams alone cannot.
//
// # Locking
//
// One mutex, held across the store and transport I/O a poll and an Acquire
// perform, which is Bindings' own trade at its own mu and is stated here for
// the same reason: it makes many subscribers arriving at once for one session
// cost one registry read, at the price of a slow read delaying work on an
// unrelated session. The lock order is Demand.mu then Bindings.mu and never the
// reverse -- nothing in Bindings names this type.
type Demand struct {
	bindings *Bindings
	tips     TipReader
	hints    Hinter
	clock    Clock
	limits   DemandLimits
	// watcher is told when a session starts and stops being watched; it is
	// discardWatcher unless SetWatcher composed one.
	watcher Watcher

	mu       sync.Mutex
	closed   bool
	sessions map[sessionKey]*demandSession
}

// demandSession is one watched session's local state.
//
// held is whether THIS type holds one unit of the routing table's demand, and
// it is not the same question as whether the table holds a binding: an
// observation pushed by a Host can invalidate the route while the demand
// remains, which is exactly the state a poll rebinds from.
//
// THE SUPERSESSION MECHANISM IS THE ENTRY POINTER, and there is deliberately
// only one. A stop that returns false is not enough on its own -- a timer may
// already be running its callback when it is stopped -- so the callback
// compares the entry it was armed with against the one the table now holds.
// clientlink's demand table carries a generation counter beside its stop for
// exactly this, and this one does not, because the situations differ:
// cancelRelease there LEAVES the entry in place, so an entry can be superseded
// without changing identity, while teardownLocked here DELETES it and a session
// watched again gets a fresh one. A generation counter was written here first
// and measured redundant -- deleting the comparison from the guard left the
// whole suite green -- so it is gone rather than kept as a second mechanism for
// one job, which is the decision routing.Bindings made about its own Close fast
// path and the HostLink pool made about idleSince.
type demandSession struct {
	subscribers int
	held        bool
	stop        func() bool
}

// NewDemand validates the composition before any call, so a missing seam is a
// composition error rather than a nil dereference on the first subscriber.
func NewDemand(bindings *Bindings, tips TipReader, hints Hinter, clock Clock, limits DemandLimits) (*Demand, error) {
	if bindings == nil {
		return nil, fmt.Errorf("%w: Bindings must not be nil", ErrInvalidConfig)
	}
	if tips == nil {
		return nil, fmt.Errorf("%w: TipReader must not be nil", ErrInvalidConfig)
	}
	if hints == nil {
		return nil, fmt.Errorf("%w: Hinter must not be nil", ErrInvalidConfig)
	}
	if clock == nil {
		return nil, fmt.Errorf("%w: Clock must not be nil", ErrInvalidConfig)
	}
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	return &Demand{
		bindings: bindings,
		tips:     tips,
		hints:    hints,
		clock:    clock,
		limits:   limits,
		watcher:  discardWatcher{},
		sessions: map[sessionKey]*demandSession{},
	}, nil
}

// Watcher is told when this replica starts and stops watching a session.
//
// It exists for Gap 3's live tail, which needs to know one thing only this
// type can say: whether a tail went live INSIDE the first subscriber's own
// Acquire. A tail started there is gapless for every viewer -- the first
// viewer's subscribe has not been acknowledged yet, so its durable read comes
// after -- while a tail started by any later poll or rebind may have missed
// records every current viewer was relying on it for, and is owed a reset.
//
// EVERY METHOD IS CALLED WITH THIS TYPE'S LOCK HELD, so an implementation must
// not block and must not call back into this package. The composition's is a
// few map writes under a leaf mutex.
type Watcher interface {
	// Watching is called for a session's FIRST local subscriber, before the
	// bind that subscriber provokes.
	Watching(tenant sessionwire.TenantID, session sessionwire.SessionID)
	// Served is called once that first serve has returned, bound or not.
	Served(tenant sessionwire.TenantID, session sessionwire.SessionID)
	// Unwatched is called when the session's last subscriber is gone, or the
	// plane closed.
	Unwatched(tenant sessionwire.TenantID, session sessionwire.SessionID)
}

type discardWatcher struct{}

func (discardWatcher) Watching(sessionwire.TenantID, sessionwire.SessionID)  {}
func (discardWatcher) Served(sessionwire.TenantID, sessionwire.SessionID)    {}
func (discardWatcher) Unwatched(sessionwire.TenantID, sessionwire.SessionID) {}

// SetWatcher composes the watcher. It is a composition-time call, made before
// the first Acquire; a nil watcher restores the default, which does nothing.
func (d *Demand) SetWatcher(w Watcher) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if w == nil {
		w = discardWatcher{}
	}
	d.watcher = w
}

// Acquire records one local subscriber for a session and, if it is the first,
// binds to the session's owner and starts polling.
//
// It answers a LOCAL fact -- this replica now has a subscriber -- and that fact
// cannot fail. A session with no owner, an owner Core will not carry, a refused
// bind and a registry outage are all reported by returning nil with the session
// left unbound, because each of them is a routing outcome the poll retries and
// none of them is a reason to refuse a viewer. The one error is a closed plane,
// where the answer would be a lie.
//
// A first subscriber that cannot be bound is served IMMEDIATELY with the tip
// hint, on the same path a poll uses, rather than waiting an interval for the
// first poll to notice. That is why serveLocked exists as one body: a viewer of
// a cold session and a viewer whose session went cold get the same treatment
// because they run the same statements.
//
// The debounce that keeps a reconnecting browser from costing a rebind is the
// ClientLink edge's, and is deliberately not restated here: this plane is told
// about the first and the last subscriber, and A6.3 owns what "last" means.
func (d *Demand) Acquire(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) error {
	key := sessionKey{tenant: tenant, session: session}

	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrDemandClosed
	}
	if entry := d.sessions[key]; entry != nil {
		entry.subscribers++
		return nil
	}
	entry := &demandSession{subscribers: 1}
	d.sessions[key] = entry

	// Bounded from the CALLER's context here, and from context.Background in
	// poll. A subscriber that went away should not leave a registry read
	// running, and a poll has no caller to inherit from.
	//
	// WithTimeout DERIVES rather than replaces, which is the whole claim: a
	// caller's cancellation and a caller's shorter deadline both still apply.
	// TestAcquireInheritsTheSubscribersOwnBound is what says so -- without it
	// context.WithoutCancel here, or context.Background, is indistinguishable.
	pollCtx, cancel := context.WithTimeout(ctx, d.limits.PollTimeout)
	defer cancel()
	d.watcher.Watching(tenant, session)
	d.serveLocked(pollCtx, key, entry)
	d.watcher.Served(tenant, session)
	d.scheduleLocked(key, entry)
	return nil
}

// Release gives back one subscriber. The LAST one stops the poll and gives the
// route back, because the route exists for the demand.
//
// The unbind's failure IS reported here, unlike everywhere else in this file,
// and the difference is that this one has a caller. Bindings.Release drops the
// local route either way; what the error says is that a Host may still be
// holding a route this replica has forgotten, which is an operational fact
// somebody can act on.
func (d *Demand) Release(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) error {
	key := sessionKey{tenant: tenant, session: session}

	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrDemandClosed
	}
	entry := d.sessions[key]
	if entry == nil || entry.subscribers == 0 {
		return ErrNoDemand
	}
	entry.subscribers--
	if entry.subscribers > 0 {
		return nil
	}
	return d.teardownLocked(ctx, key, entry)
}

// Rebind gives a watched session's route back and takes a fresh one
// IMMEDIATELY, without changing how many subscribers this replica holds.
//
// It exists for internal/routing's repair plane, which learns from below that a
// route is unusable -- a physical HostLink closed, a route queue overflowed --
// and may not wait an ownership poll to find out. It is deliberately the SAME
// two statements a poll runs, so a repair and a poll cannot repair differently.
//
// # THE INVARIANT THIS METHOD IS THE POINT OF: one demand holder per session
//
// Bindings.Release only unbinds on the LAST release, because it COUNTS. This
// method is a release followed by an acquire, so it is fresh only while this
// type is the sole holder of the routing table's demand for a session: with a
// second holder the release would merely decrement, route.bound would stay
// true, and Bindings.routeLocked would hand the acquire back THE SAME STALE
// ROUTE without reading the registry at all -- which is precisely the state a
// repair exists to leave.
//
// So the answer to "should Release become owner-aware" is no, and the reason is
// that owner-awareness would not fix it. Two holders means the route
// legitimately outlives one holder's release, so the stale-adoption window is a
// property of there being two, not of the counting. The repair plane therefore
// takes NO demand and asks here instead.
// TestASecondHolderOfTheRoutingTablesDemandMakesARebindStale is the reader for
// the hazard, and TestTheRoutingTablesDemandIsHeldOnlyByTheDemandPlane is the
// structural guard that keeps a second holder from appearing.
func (d *Demand) Rebind(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) error {
	key := sessionKey{tenant: tenant, session: session}

	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrDemandClosed
	}
	entry := d.sessions[key]
	if entry == nil {
		// Nothing watches this session, so there is no route to repair and
		// taking one would open a HostLink for nobody -- the failure
		// Bindings.Deliver refuses for the same reason.
		return ErrNoDemand
	}
	if entry.held {
		// The failure is reported for Demand.Release's reason: it says a Host
		// may still hold a route this replica has forgotten. It does not stop
		// the rebind, because the local route is gone either way.
		err := d.bindings.Release(ctx, key.tenant, key.session)
		entry.held = false
		d.serveLocked(ctx, key, entry)
		return err
	}
	d.serveLocked(ctx, key, entry)
	return nil
}

// Close stops every poll, gives every route back and refuses later work.
//
// Every session is attempted and every failure joined rather than returning at
// the first, for Bindings.Close's reason: one Host's failure must not strand
// the routes on the others.
func (d *Demand) Close(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true

	var failures []error
	for key, entry := range d.sessions {
		if err := d.teardownLocked(ctx, key, entry); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

// Watching reports how many local subscribers this replica holds for a session.
func (d *Demand) Watching(tenant sessionwire.TenantID, session sessionwire.SessionID) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	entry := d.sessions[sessionKey{tenant: tenant, session: session}]
	if entry == nil {
		return 0
	}
	return entry.subscribers
}

// Len reports how many sessions this replica is watching.
func (d *Demand) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.sessions)
}

// teardownLocked ends one session's demand: the poll is superseded, the entry
// removed, and the routing table's demand given back if this type holds any.
func (d *Demand) teardownLocked(ctx context.Context, key sessionKey, entry *demandSession) error {
	entry.cancelPoll()
	delete(d.sessions, key)
	d.watcher.Unwatched(key.tenant, key.session)
	if !entry.held {
		return nil
	}
	// There is deliberately no `entry.held = false` here. The entry has just
	// been removed from the table and nothing reads it again -- the only
	// reference left is a stale poll's closure, which returns on the identity
	// comparison before touching a field. It is the third instance of the
	// family this file already removed twice, for the reason routing.Bindings
	// gives at its own Close: a second mechanism for one job survives every
	// mutation, because what does the work is emptying the table.
	return d.bindings.Release(ctx, key.tenant, key.session)
}

// poll is the scheduled ownership recheck.
//
// It is refresh, then serve, then arm the next one -- three sequential
// conditions rather than a branch, which is what makes an owner arriving
// between two polls bind AND stop hinting in the same tick, and an owner
// disappearing drop AND start hinting in the same tick.
func (d *Demand) poll(key sessionKey, scheduled *demandSession) {
	d.mu.Lock()
	defer d.mu.Unlock()

	entry := d.sessions[key]
	if entry != scheduled {
		// Superseded: the demand was released, the plane was closed, or this
		// session was watched again after being forgotten. Stop is not enough
		// on its own -- a timer may already be running its callback when it is
		// stopped -- so this is what makes a cancelled poll stay cancelled.
		//
		// There is deliberately no `d.closed ||` beside it. A closed plane has
		// no entries, so this comparison already answers for one, and a second
		// guard here would be a second mechanism for one job: the decision
		// routing.Bindings took at its own Close, on the same evidence -- a
		// mutant deleting the redundant half survives, because what does the
		// work is emptying the table.
		return
	}
	// context.Background, because there is no caller and no request context in
	// existence to inherit a deadline from: PollTimeout is the whole bound.
	ctx, cancel := context.WithTimeout(context.Background(), d.limits.PollTimeout)
	defer cancel()

	if entry.held {
		d.refreshLocked(ctx, key, entry)
	}
	d.serveLocked(ctx, key, entry)
	d.scheduleLocked(key, entry)
}

// serveLocked is what a watched session gets: a route if one can be had, and
// the repair hint while one cannot.
//
// The two conditions are separate reads of entry.held, not an if/else, because
// binding is what changes it: a session that binds here must not also hint, and
// a session that fails to must.
//
// The FIRST condition is load-bearing beyond avoiding a wasted call, and a
// mutation deleting it survived the suite before its reader was written.
// Bindings.Acquire COUNTS demand, so binding a session this type already holds
// a route for would add a second unit of the routing table's demand on every
// poll -- and the last subscriber's release, which gives back one, would then
// never unbind. A watched session would hold a HostLink route for nobody, for
// the life of the replica.
func (d *Demand) serveLocked(ctx context.Context, key sessionKey, entry *demandSession) {
	if !entry.held {
		d.bindLocked(ctx, key, entry)
	}
	if !entry.held {
		d.hintLocked(ctx, key)
	}
}

// scheduleLocked arms the next poll.
//
// TWO CLAIMS, TWO DIFFERENT MECHANISMS, because an earlier version of this
// comment credited both to the arming order and only one of them is its.
//
// NON-OVERLAP is d.mu's. Two polls of one session cannot run at the same time
// whatever the arming order is, because poll holds the mutex for its whole
// body; a successor armed at the START of a poll would simply block on it.
// TestOnePollOfASessionExcludesEveryOther drives that directly.
//
// THE GAP is this call site's. Arming the successor at the END is what makes
// OwnershipPollInterval a delay measured from the previous poll FINISHING
// rather than from its starting, so a store slower than the interval delays the
// next poll instead of queueing one behind the mutex.
// TestThePollIntervalIsAGapAndNotAPeriod asks the clock, from inside a seam
// call, whether a successor is already armed.
func (d *Demand) scheduleLocked(key sessionKey, entry *demandSession) {
	entry.stop = d.clock.AfterFunc(d.limits.OwnershipPollInterval, func() {
		d.poll(key, entry)
	})
}

// cancelPoll supersedes the scheduled poll. The stop is what usually prevents
// the callback; what makes it harmless when the stop lost its race is that the
// caller then removes the entry, which is the comparison poll makes.
func (entry *demandSession) cancelPoll() {
	if entry.stop == nil {
		return
	}
	entry.stop()
	entry.stop = nil
}

// bindLocked binds to the session's current owner, and does nothing at all if
// there is not one.
//
// The error is deliberately discarded rather than distinguished. No owner, an
// owner Core cannot carry, a Host that refused the bind and a registry that
// could not be read produce the same LOCAL state -- unbound, still wanted, try
// again next interval -- and there is no caller on this path who could act on
// the difference. What a viewer gets meanwhile is the journal_tip hint, which
// is the degraded mode the whole poll exists to provide.
//
// This is also where "do not restore a cold session merely for viewing" lives,
// and it lives here by being ABSENT: there is no branch for a session with no
// owner because there is nothing this type may do about one. Placement is
// internal/placement's, and a restore is a durable command internal/admission
// admits for a caller who asked for one.
func (d *Demand) bindLocked(ctx context.Context, key sessionKey, entry *demandSession) {
	if _, err := d.bindings.Acquire(ctx, key.tenant, key.session); err != nil {
		return
	}
	entry.held = true
}

// refreshLocked rechecks a held route against the registry and gives it back if
// it is no longer the current one.
//
// The staleness rule is not restated here: Observe is the single authority for
// "does this observation contradict what the table holds", including its
// refusal to act on an older lease epoch, and this asks the table what it holds
// AFTERWARDS rather than deciding for itself. A route can also have been
// invalidated by an observation a Host pushed, which this sees as the same
// fact and repairs the same way.
//
// A registry read that FAILS keeps the route. This table's binding is a routing
// hint the owning Host fences against its own durable lease, so keeping one
// through an outage risks a refusal, while dropping one turns every registry
// blip into a rebind storm that has delivered nothing.
func (d *Demand) refreshLocked(ctx context.Context, key sessionKey, entry *demandSession) {
	observed, found, err := d.bindings.resolver.Owner(ctx, key.tenant, key.session)
	if err != nil {
		return
	}
	if found {
		d.bindings.Observe(ctx, observed)
	}
	if _, bound := d.bindings.Binding(key.tenant, key.session); bound && found {
		return
	}
	// The owner is gone, or the observation contradicted the route, or
	// something else invalidated it. Give the demand back so serveLocked takes
	// a fresh one: Bindings.Acquire COUNTS, so rebinding without releasing
	// would leave this replica holding two units of demand for one subscriber
	// and the last release would then never unbind. The error has nowhere to
	// go and changes nothing -- Bindings.Release drops the local route whether
	// the unbind succeeded or not, for its own stated reason.
	_ = d.bindings.Release(ctx, key.tenant, key.session)
	entry.held = false
}

// hintLocked publishes the session's current durable journal tip.
//
// It runs on every unbound poll, including when the tip has not moved and
// including when the tip is zero, and that is what "repeatable" means: the hint
// carries absolute state and no sequence, no nonce and no attempt count, so two
// hints with the same tip are the same bytes and a client that missed one loses
// nothing by taking the next. Publishing only on a CHANGE would make the hint a
// delta in everything but name -- a client that joined between two changes
// would wait for the next write to learn where the journal already was.
//
// A tip that could not be READ publishes nothing, and that is the fail-closed
// direction: a hint is a claim about the durable journal, and this replica has
// no other source for one. A publish that FAILS is dropped for bindLocked's
// reason; the next poll republishes, which is the property.
func (d *Demand) hintLocked(ctx context.Context, key sessionKey) {
	page, err := d.tips.ReadPublicJournal(ctx, tipRequest(key))
	if err != nil {
		return
	}
	hint := sessionwire.JournalTip{TenantID: key.tenant, SessionID: key.session, Tip: page.CapturedTip}
	if err := hint.Validate(); err != nil {
		// Unreachable through a ClientLink, whose channel grammar validated
		// both identities before any demand was taken, and reachable through
		// this package's exported surface, which any composition may call. A
		// hint that cannot be marshalled is not a hint: publishing it would
		// move the failure into the transport, naming nothing.
		return
	}
	_ = d.hints.PublishJournalTip(ctx, hint)
}

// tipRequest is the bounded read a tip poll makes. Only CapturedTip is read
// from the answer; the request asks for the smallest page the store will build.
func tipRequest(key sessionKey) sessionstore.ReadPublicJournalRequest {
	return sessionstore.ReadPublicJournalRequest{
		TenantID:  key.tenant,
		SessionID: key.session,
		Tail:      true,
		Limit:     tipReadLimit,
		ScanLimit: tipReadLimit,
	}
}
