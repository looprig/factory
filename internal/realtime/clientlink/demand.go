package clientlink

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
)

// ErrUnroutableChannel reports a channel the Authorizer allowed and this build
// cannot name a session in.
//
// It is a FAULT in the composition, not a decision about the caller, and
// keeping the two apart is the whole reason it exists. The Authorizer is a
// SEAM: an implementation deciding subscriptions under a grammar this package
// does not share could authorize a channel whose session cannot be derived, and
// there are exactly two wrong answers to that. Taking demand for something else
// -- the principal's tenant with whatever the string happened to contain -- is
// the dangerous one. Reporting it as a DENIAL is the quiet one: a denial is
// terminal for the principal, so a browser entitled to the channel would stop
// asking for a condition that has nothing to do with its permissions.
//
// FuzzTheDemandGrammarAgreesWithTheAuthorizers holds the production pair to one
// answer, so this is unreachable through Factory's own authorizer; the arm has
// a reader because the seam is an interface and a test can implement it.
var ErrUnroutableChannel = errors.New("clientlink: authorized channel names no session")

// channelPrefix is the one channel namespace this surface serves.
//
// It is spelled here as an absolute literal and in internal/identity as part of
// a regular expression, which is two statements of one grammar. That is a cost,
// and it is paid deliberately rather than removed by exporting a parser: the
// Authorizer is a seam whose implementation is a deployment's to choose, so
// this package cannot derive a session from "whatever the authorizer parsed"
// without the authorizer telling it -- and widening the seam's signature to
// return a parse is a change to the authorization contract A2.1 owns. What
// keeps the two from drifting is the differential fuzz target, which drives
// both surfaces over one channel string and requires them to agree in both
// directions.
const channelPrefix = "session:"

// demandKey identifies one session's local delivery demand.
//
// The tenant is the PRINCIPAL's, never the channel's, and that is not the same
// statement as "the authorizer compared them". A tenant read out of a string
// the client sent is a tenant chosen by the client; a tenant taken from the
// handshake is authority. They can only differ if the Authorizer allowed a
// channel naming another tenant, which is precisely the case demandKeyOf
// refuses rather than resolves.
type demandKey struct {
	tenant  sessionwire.TenantID
	session sessionwire.SessionID
}

// demandEntry is one session's local delivery demand.
//
// bindings is a count of DeliveryBindings, not of connections: two browsers
// watching one session are two bindings, and one of them going away is not the
// last removal. generation is what makes a cancelled release stay cancelled --
// see releaseBinding.
type demandEntry struct {
	bindings   int
	generation uint64
	// stop cancels the scheduled release, and its presence is also the record
	// that one is scheduled. It is nil exactly when this session's demand is
	// held rather than draining.
	//
	// THE INVARIANT, because two guards below are redundant under it and a
	// reader should know which: stop is non-nil only while bindings is zero.
	// releaseBinding arms it only after the last binding goes, and acquire
	// cancels it -- clearing stop -- on the first binding that comes back. So
	// `stop != nil` and `bindings == 0` are one fact, and a mutation deleting
	// either `entry.bindings != 0` in drain or `entry.bindings > 0` in
	// ReleaseIdleDemand SURVIVES the suite. Both are kept as fail-closed
	// conjuncts rather than removed, because each is the one that states the
	// property its own function actually depends on: drain must not release
	// demand somebody holds, and a flush must not release demand a live
	// connection holds. Neither is a second mechanism for the same job -- the
	// distinction that had the redundant `closed` fast paths deleted from
	// routing.Bindings and the HostLink pool -- and if a later edit arms a
	// release without going through releaseBinding, these are what stop it.
	stop func() bool
}

// Bind authorizes one subscription and records the DeliveryBinding it creates.
//
// It returns the binding's RELEASE, and that shape is the contract rather than
// a convenience. A caller cannot release a binding it did not take, cannot
// release one twice, and cannot release someone else's: there is no key to get
// wrong. The transport needs exactly that, because it learns a subscription has
// ended in three different places -- an unsubscribe, a disconnect, and a
// subscribe the library refused after the callback -- and all three must give
// back the same binding exactly once.
//
// Authorization and demand are ONE entry point, deliberately. A6.1 had
// AuthorizeSubscribe as the Engine's whole answer to a subscribe, and adding a
// second call beside it would mean the channel that was authorized and the
// session whose demand was taken came from two decisions that could disagree --
// which is exactly the defect A6.2 removed from the RPC path, where the
// authorized session and the admitted session came from two decoders.
//
// The order is authorize, then derive, then notify. Nothing about the session
// is looked up before the authorization decision, so a refusal discloses
// nothing; and the demand plane is not told about a session the principal was
// not allowed to watch.
func (e *Engine) Bind(ctx context.Context, principal identity.Principal, channel string) (release func(), err error) {
	if err := e.cfg.Authorizer.AuthorizeSubscribe(ctx, principal, channel); err != nil {
		return nil, err
	}
	key, err := demandKeyOf(principal, channel)
	if err != nil {
		return nil, err
	}
	if err := e.acquire(ctx, key); err != nil {
		return nil, err
	}
	// sync.Once rather than a boolean: the three transport paths that can end a
	// binding do not run on one goroutine, and a binding released twice would
	// take another subscriber's demand away.
	var once sync.Once
	return func() { once.Do(func() { e.releaseBinding(key) }) }, nil
}

// acquire records one DeliveryBinding, notifying the demand manager if it is
// this session's first.
//
// The two ways a session can already have an entry are NOT the same, and the
// difference is the whole of step 2's first sentence. A session with live
// bindings needs no notification: demand is already held. A session that is
// DRAINING -- last binding gone, release scheduled -- also needs none, because
// the demand was never given back; the scheduled release is cancelled and the
// demand it would have released is retained. A browser reconnecting inside the
// debounce therefore costs nothing on the demand plane, which is what the
// debounce is for.
func (e *Engine) acquire(ctx context.Context, key demandKey) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if entry := e.demand[key]; entry != nil {
		entry.bindings++
		entry.cancelRelease()
		return nil
	}
	// Bounded for the reason Limits.DemandTimeout states, and the deadline is
	// applied here rather than at the transport adapter because it is policy
	// read from the composed Limits -- the same decision, for the same reason,
	// that Engine.Admit makes for a command.
	ctx, cancel := context.WithTimeout(ctx, e.cfg.Limits.DemandTimeout)
	defer cancel()
	if err := e.cfg.Demand.Acquire(ctx, key.tenant, key.session); err != nil {
		// Nothing is recorded, so a retry is a first binding again. A table
		// entry left behind here would be demand this replica believes it holds
		// and the routing plane has never heard of.
		return err
	}
	e.demand[key] = &demandEntry{bindings: 1}
	return nil
}

// releaseBinding gives back one DeliveryBinding, scheduling the demand's
// release when it was the last.
//
// The release is SCHEDULED rather than performed, always -- including at a zero
// debounce -- so that "the last binding went away" and "the demand was given
// back" are two events in one order rather than one event. A caller that wants
// them collapsed calls ReleaseIdleDemand.
func (e *Engine) releaseBinding(key demandKey) {
	e.mu.Lock()
	defer e.mu.Unlock()

	entry := e.demand[key]
	if entry == nil || entry.bindings == 0 {
		// Unreachable through Bind: a release is handed out once per binding
		// and each binding incremented the count, so the count cannot reach
		// zero while a release is outstanding. It is kept because the
		// alternative to a guard here is a negative count -- a session whose
		// demand can never be released -- and because ReleaseIdleDemand deletes
		// entries on a path this one does not coordinate with. No test drives
		// it; deleting it kills nothing today.
		return
	}
	entry.bindings--
	if entry.bindings > 0 {
		return
	}
	// The generation is captured BEFORE the timer is armed and compared when it
	// fires. A stop that returns false is not enough on its own: a timer may
	// already be running its callback when it is stopped, so the callback has
	// to be able to recognise that the world moved under it.
	generation := entry.generation
	entry.stop = e.cfg.Clock.AfterFunc(e.cfg.Limits.DemandReleaseDebounce, func() {
		e.drain(key, entry, generation)
	})
}

// cancelRelease supersedes a scheduled release. Both halves are needed: the
// stop is what usually prevents the callback, and the generation bump is what
// makes it harmless when the stop lost the race.
func (entry *demandEntry) cancelRelease() {
	if entry.stop == nil {
		return
	}
	entry.stop()
	entry.stop = nil
	entry.generation++
}

// drain is the scheduled release, and everything it checks is a way the world
// may have moved since it was scheduled.
//
// It runs on the clock's goroutine and takes the same lock every other path
// takes, so a release can never interleave with the acquire of the demand it is
// giving back.
func (e *Engine) drain(key demandKey, scheduled *demandEntry, generation uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	entry := e.demand[key]
	if entry != scheduled || entry.generation != generation || entry.bindings != 0 {
		return
	}
	delete(e.demand, key)
	// The error is DROPPED, and there is nowhere for it to go: this runs on the
	// clock's goroutine with no caller to answer, and it is routing.Bindings'
	// own decision at Observe, for its reason. The local record is gone either
	// way -- keeping it would mean this replica believing it still holds demand
	// the routing plane may already have released, and a new subscriber would
	// then take a binding without notifying anything. Nothing above retries a
	// demand release; ReleaseIdleDemand is the only other path and it reports
	// its failures because it HAS a caller.
	_ = e.releaseDemandLocked(context.Background(), key)
}

// ReleaseIdleDemand gives back every demand whose release is merely scheduled,
// now, and reports what could not be given back.
//
// It is the drain: Handler.Shutdown calls it after the node has closed every
// link, when every DeliveryBinding is gone and every session is sitting behind
// its debounce. A replica that stopped at the node would leave the routing
// plane holding demand for sessions no connection remains to serve.
//
// A session whose bindings are still held is left alone. That is not a
// half-measure: a live binding means a connection the node did not close, and
// releasing its demand would be this edge deciding a subscription had ended
// when the transport says it has not.
//
// Every failure is joined rather than the first returned, for routing.Bindings'
// reason at its own Close: one session's failure must not hide the demand this
// replica did not give back on the others.
func (e *Engine) ReleaseIdleDemand(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	var failures []error
	for key, entry := range e.demand {
		if entry.bindings > 0 || entry.stop == nil {
			continue
		}
		// Superseded, so the timer that is already armed -- or already running
		// and waiting for this lock -- finds the entry gone and does nothing.
		entry.cancelRelease()
		delete(e.demand, key)
		if err := e.releaseDemandLocked(ctx, key); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

// releaseDemandLocked is the ONE call into the demand plane's Release.
//
// The context is the caller's, and for the debounced path there is no caller:
// drain hands it context.Background, because the connection whose subscription
// ended is typically gone and there is no request context left to inherit.
// DemandTimeout is therefore the whole of the bound, which is why it is applied
// here rather than at either call site.
func (e *Engine) releaseDemandLocked(ctx context.Context, key demandKey) error {
	ctx, cancel := context.WithTimeout(ctx, e.cfg.Limits.DemandTimeout)
	defer cancel()
	return e.cfg.Demand.Release(ctx, key.tenant, key.session)
}

// Demand reports how many local DeliveryBindings this replica holds for a
// session, and whether its demand is still held.
//
// The second result is what a count alone cannot say: a session draining behind
// the debounce has zero bindings and demand the routing plane still holds.
func (e *Engine) Demand(tenant sessionwire.TenantID, session sessionwire.SessionID) (bindings int, held bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	entry := e.demand[demandKey{tenant: tenant, session: session}]
	if entry == nil {
		return 0, false
	}
	return entry.bindings, true
}

// demandKeyOf derives the session whose delivery demand a channel represents.
//
// The grammar is `session:{tenant}:{session}` with exactly three segments and
// no colon anywhere else, both segments non-empty and both valid Core
// identifiers, and the tenant segment equal to the principal's. Every clause is
// a way the derivation could otherwise name something the caller did not
// subscribe to; none of them is an authorization decision, which has already
// been made, and a failure here is reported as a fault for the reason
// ErrUnroutableChannel gives.
//
// The channel is never returned in the error, and neither is the tenant. A
// message quoting the channel would put a client-controlled string into an
// operator's log, and the channel is the one thing the caller already knows.
func demandKeyOf(principal identity.Principal, channel string) (demandKey, error) {
	rest, found := strings.CutPrefix(channel, channelPrefix)
	if !found {
		return demandKey{}, fmt.Errorf("%w: not a session channel", ErrUnroutableChannel)
	}
	tenant, session, found := strings.Cut(rest, ":")
	if !found {
		return demandKey{}, fmt.Errorf("%w: a session channel has three segments", ErrUnroutableChannel)
	}
	// Cut splits at the FIRST colon, so a fourth segment is still in session.
	if strings.Contains(session, ":") {
		return demandKey{}, fmt.Errorf("%w: a session channel has three segments", ErrUnroutableChannel)
	}
	if err := sessionwire.TenantID(tenant).Validate(); err != nil {
		return demandKey{}, fmt.Errorf("%w: the tenant segment is not an identity Core carries", ErrUnroutableChannel)
	}
	if err := sessionwire.SessionID(session).Validate(); err != nil {
		return demandKey{}, fmt.Errorf("%w: the session segment is not an identity Core carries", ErrUnroutableChannel)
	}
	if sessionwire.TenantID(tenant) != principal.Tenant() {
		return demandKey{}, fmt.Errorf("%w: the channel names another tenant", ErrUnroutableChannel)
	}
	return demandKey{tenant: principal.Tenant(), session: sessionwire.SessionID(session)}, nil
}
