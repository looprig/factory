package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"
)

// ---------------------------------------------------------------------------
// The durable fixtures: a journal with private records in it, and a store whose
// "no such session" answer depends on the keyspace layout.
// ---------------------------------------------------------------------------

// privateJournalMarker is the byte string every PRIVATE record's body carries.
//
// It exists so a leak has a name. The private-material assertions search the
// whole response -- every header value and the body -- for it, so any path that
// published a withheld record's bytes is reported wherever those bytes came out
// rather than only where a test happened to look.
const privateJournalMarker = "PRIVATE-RUNTIME-MATERIAL-8f3c"

// gateAnswerMarker is the same device for a gate's submitted answer, which the
// store holds and the public projection must never carry.
const gateAnswerMarker = "SUBMITTED-GATE-ANSWER-41ab"

// fakeStorePageSize is the page size the fake uses for a limit of zero, which
// is what SessionStore's Store.pageLimit does with one. It is deliberately
// SMALL and not a round number, so a page that came back at this size because
// Factory sent no limit is distinguishable from one bounded by a limit Factory
// chose.
const fakeStorePageSize = 7

// fakeJournalRecord is one durable position in a session's journal.
//
// Public and private records occupy the same sequence space, which is the whole
// point: the public read must step over a private position while still
// advancing the watermark across it, and a fixture holding only public records
// could not tell a reader that does from one that does not.
type fakeJournalRecord struct {
	public  bool
	eventID sessionwire.EventID
	body    json.RawMessage
}

// publicRecord builds one public event whose body is canonical public JSON.
//
// The body is deliberately NOT what encoding/json would produce from a decoded
// value: its members are out of alphabetical order and it carries a \u escape.
// Core's canonical rule is json.Marshal(RawMessage) == body, which is a compact
// check and preserves both, so this body is canonical AND is changed by any
// round trip through a Go value. That is what makes "forwarded without decoding
// or re-encoding" observable.
func publicRecord(seq uint64) fakeJournalRecord {
	return fakeJournalRecord{
		public:  true,
		eventID: sessionwire.EventID(fmt.Sprintf("event-%04d", seq)),
		body:    json.RawMessage(fmt.Sprintf(`{"z":%d,"a":"café","kind":"public"}`, seq)),
	}
}

// privateRecord builds one record the public projection withholds entirely.
func privateRecord(seq uint64) fakeJournalRecord {
	return fakeJournalRecord{
		public:  false,
		eventID: sessionwire.EventID(fmt.Sprintf("private-%04d", seq)),
		body:    json.RawMessage(fmt.Sprintf(`{"marker":%q,"seq":%d}`, privateJournalMarker, seq)),
	}
}

// storeLayout is one of SessionStore's keyspace layouts, together with the
// error a read of a session it holds no binding for produces.
//
// The two layouts answer the same question with DIFFERENT typed errors, and
// that is not a detail: sessionstore's own noSuchSession (gates.go, released
// v0.1.0) reads "there is no such session" as CatalogErrorNotFound OR
// CatalogErrorDeleted OR KeyspaceBindingNotFound, because readCatalogEntry
// calls verifySessionScope BEFORE it reads the record and, outside the legacy
// single-tenant layout, an unbound session fails there with a KeyspaceError.
// A mapping that knew only the catalog codes would answer 500 for every
// nonexistent session in a multi-tenant deployment.
type storeLayout struct {
	name    string
	absence func() error
}

var (
	// layoutMultiTenant is the default layout, where the session's collision
	// witnesses are verified before any record read.
	layoutMultiTenant = storeLayout{
		name:    "multi-tenant witness binding",
		absence: func() error { return &sessionstore.KeyspaceError{Code: sessionstore.KeyspaceBindingNotFound} },
	}
	// layoutLegacySingleTenant is the layout harness opens the store with,
	// where verifySessionScope returns early and absence is the catalog's.
	layoutLegacySingleTenant = storeLayout{
		name: "legacy single tenant",
		absence: func() error {
			return &sessionstore.CatalogError{Code: sessionstore.CatalogErrorNotFound, Field: "record"}
		},
	}
	// layoutLegacyIdentityMismatch is the same layout's CROSS-TENANT answer.
	// Under it the catalog key is not tenant-prefixed, so the stored record is
	// found and then refused on identity; that refusal is the only thing
	// between a cross-tenant read and a hit.
	layoutLegacyIdentityMismatch = storeLayout{
		name: "legacy single tenant, cross-tenant identity refusal",
		absence: func() error {
			return &sessionstore.CatalogError{Code: sessionstore.CatalogErrorIdentity, Field: "tenant"}
		},
	}
	// layoutDeletedRecord is the fourth answer sessionstore's own noSuchSession
	// admits: a record that was real and is not any more.
	layoutDeletedRecord = storeLayout{
		name: "deleted record",
		absence: func() error {
			return &sessionstore.CatalogError{Code: sessionstore.CatalogErrorDeleted, Field: "record"}
		},
	}
)

// absenceLayouts is the whole space of "there is no such session" answers the
// released store produces, in sessionstore's own noSuchSession terms plus the
// legacy identity refusal A2.1 turns on. Every one of them must produce the
// SAME response, or a caller learns which of them it met.
func absenceLayouts() []storeLayout {
	return []storeLayout{
		layoutMultiTenant,
		layoutLegacySingleTenant,
		layoutLegacyIdentityMismatch,
		layoutDeletedRecord,
	}
}

// ---------------------------------------------------------------------------
// The fake's journal and gate reads.
// ---------------------------------------------------------------------------

// ReadPublicJournal walks the fake's journal the way the released store walks a
// real one.
//
// It restates the positioning rules of sessionstore v0.3.0 because each one
// is a rule Factory's paging depends on and a fake looser than any of them
// would leave the corresponding production line unread:
//
//   - a limit out of range is a typed refusal, not a smaller page (Store.pageLimit);
//   - a limit of zero means the store's own page size, so Factory learns nothing
//     about how large the page was;
//   - a start ABOVE the captured tip returns no events at all and a watermark at
//     the tip, which is walkJournal's first statement;
//   - Tail derives the last Limit sequence positions from the captured tip;
//   - ScanLimit bounds examined records, including private ones, and a budget
//     stop issues a cursor after the last covered sequence;
//   - a private record contributes nothing to the page and still advances
//     CoveredThrough without consuming a public-event slot, while scan budget remains.
func (f *fakeReader) ReadPublicJournal(ctx context.Context, req sessionstore.ReadPublicJournalRequest) (sessionwire.JournalPage, error) {
	f.mu.Lock()
	f.journalRequests = append(f.journalRequests, req)
	block, fail, panics, leak := f.block, f.fail, f.panics, f.leak
	if fail == nil {
		fail = f.journalFail
	}
	held := f.sessions[storedSession{tenant: req.TenantID, session: req.SessionID}]
	records := slices.Clone(f.journals[storedSession{tenant: req.TenantID, session: req.SessionID}])
	f.mu.Unlock()

	if refusePageLimit(req.Limit) {
		return sessionwire.JournalPage{}, &sessionstore.JournalError{Code: sessionstore.JournalErrorInvalid, Field: "limit"}
	}
	if refusePageLimit(req.ScanLimit) {
		return sessionwire.JournalPage{}, &sessionstore.JournalError{Code: sessionstore.JournalErrorInvalid, Field: "scan_limit"}
	}
	if req.Cursor != "" && req.FromSeq != 0 {
		return sessionwire.JournalPage{}, &sessionstore.JournalError{Code: sessionstore.JournalErrorInvalid, Field: "cursor"}
	}
	if req.Tail && (req.Cursor != "" || req.FromSeq != 0) {
		return sessionwire.JournalPage{}, &sessionstore.JournalError{Code: sessionstore.JournalErrorInvalid, Field: "tail"}
	}
	if panics != nil {
		panic(panics)
	}
	if block {
		<-ctx.Done()
		return sessionwire.JournalPage{}, ctx.Err()
	}
	if fail != nil {
		return sessionwire.JournalPage{}, fail
	}
	if !held {
		return sessionwire.JournalPage{}, f.absence()
	}

	limit := req.Limit
	if limit == 0 {
		limit = fakeStorePageSize
	}
	tip := uint64(len(records))
	from, capturedTip := req.FromSeq, tip
	if req.Tail && tip > uint64(limit) {
		from = tip - uint64(limit) + 1
	}
	if req.Cursor != "" {
		position, ok := decodeFakeJournalCursor(string(req.Cursor), req.TenantID, req.SessionID)
		if !ok || position.capturedTip > tip {
			return sessionwire.JournalPage{}, &sessionstore.JournalError{Code: sessionstore.JournalErrorCursor, Field: "cursor"}
		}
		from, capturedTip = position.nextSeq, position.capturedTip
	}
	if from < 1 {
		from = 1
	}
	covered := min(from-1, capturedTip)

	page := sessionwire.JournalPage{CapturedTip: capturedTip}
	truncatedAt := uint64(0)
	for seq := from; seq <= capturedTip; seq++ {
		if req.ScanLimit > 0 && seq-from >= uint64(req.ScanLimit) {
			truncatedAt = seq
			break
		}
		record := records[seq-1]
		if record.public || leak {
			if uint64(len(page.Events)) >= uint64(limit) {
				truncatedAt = seq
				break
			}
			page.Events = append(page.Events, sessionwire.JournalEvent{
				EventID:    record.eventID,
				JournalSeq: seq,
				Body:       record.body,
			})
		}
		covered = seq
	}
	page.CoveredThrough = covered
	if truncatedAt != 0 {
		page.NextCursor = sessionwire.Cursor(encodeFakeJournalCursor(fakeJournalPosition{
			nextSeq: truncatedAt, capturedTip: capturedTip,
		}, req.TenantID, req.SessionID))
	}
	return page, nil
}

type fakeJournalPosition struct {
	nextSeq     uint64
	capturedTip uint64
}

// encodeFakeJournalCursor binds a position to the session that issued it, which
// is the property of the real cursor Factory relies on: a token is not
// authority and cannot be moved between sessions.
func encodeFakeJournalCursor(position fakeJournalPosition, tenant sessionwire.TenantID, session sessionwire.SessionID) string {
	return fmt.Sprintf("fakejournal|%s|%s|%d|%d", tenant, session, position.nextSeq, position.capturedTip)
}

func decodeFakeJournalCursor(cursor string, tenant sessionwire.TenantID, session sessionwire.SessionID) (fakeJournalPosition, bool) {
	parts := strings.Split(cursor, "|")
	if len(parts) != 5 || parts[0] != "fakejournal" {
		return fakeJournalPosition{}, false
	}
	if parts[1] != string(tenant) || parts[2] != string(session) {
		return fakeJournalPosition{}, false
	}
	next, err := strconv.ParseUint(parts[3], 10, 64)
	if err != nil {
		return fakeJournalPosition{}, false
	}
	tip, err := strconv.ParseUint(parts[4], 10, 64)
	if err != nil {
		return fakeJournalPosition{}, false
	}
	return fakeJournalPosition{nextSeq: next, capturedTip: tip}, true
}

// ReadGates answers the session's open public gates from its catalog record,
// which is what the released store does: one record read, no cursor and no
// limit, because the catalog holds a bounded open-gate set.
func (f *fakeReader) ReadGates(ctx context.Context, req sessionstore.ReadGatesRequest) (sessionwire.GatePage, error) {
	f.mu.Lock()
	f.gateRequests = append(f.gateRequests, req)
	block, fail, panics, leak := f.block, f.fail, f.panics, f.leak
	if fail == nil {
		fail = f.gateFail
	}
	key := storedSession{tenant: req.TenantID, session: req.SessionID}
	held, record := f.sessions[key], f.records[key]
	f.mu.Unlock()

	if panics != nil {
		panic(panics)
	}
	if block {
		<-ctx.Done()
		return sessionwire.GatePage{}, ctx.Err()
	}
	if fail != nil {
		return sessionwire.GatePage{}, fail
	}
	if !held {
		return sessionwire.GatePage{}, f.absence()
	}
	gates := slices.Clone(record.OpenGates)
	if leak {
		// The control: a store that published the submitted answer would do it
		// HERE, in the projection, because that is the only member of the page
		// a body could travel in.
		for i := range gates {
			gates[i].Prompt.Body = gateAnswerMarker
		}
	}
	page := sessionwire.GatePage{
		JournalTip:    record.LastJournalSeq,
		OpenGateCount: uint64(len(gates)),
		Gates:         gates,
	}
	return page, nil
}

// journalSnapshot and gateSnapshot read through the fixture, so a test names
// the router it drove rather than reaching past it into the double.
func (f *fixture) journalSnapshot() []sessionstore.ReadPublicJournalRequest {
	return f.reads.journalSnapshot()
}

func (f *fixture) gateSnapshot() []sessionstore.ReadGatesRequest { return f.reads.gateSnapshot() }

func (f *fakeReader) journalSnapshot() []sessionstore.ReadPublicJournalRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.journalRequests)
}

func (f *fakeReader) gateSnapshot() []sessionstore.ReadGatesRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.gateRequests)
}

// ---------------------------------------------------------------------------
// Fixture options for the durable session state.
// ---------------------------------------------------------------------------

// withRecord registers a session together with the whole catalog record it
// projects, so a status read is driven from a record rather than from a stub.
func withRecord(tenant sessionwire.TenantID, session sessionwire.SessionID, record sessionstore.CatalogRecord) fixtureOption {
	return func(_ *RouterConfig, f *fixture) {
		record.TenantID, record.SessionID = tenant, session
		key := storedSession{tenant: tenant, session: session}
		f.reads.sessions[key] = true
		f.reads.records[key] = record
	}
}

// withSuccessorRecord makes the store answer a DIFFERENT record from its second
// catalog read onward, which is what lets a test tell a handler that carries
// the resolved record from one that reads the catalog again.
func withSuccessorRecord(tenant sessionwire.TenantID, session sessionwire.SessionID, record sessionstore.CatalogRecord) fixtureOption {
	return func(_ *RouterConfig, f *fixture) {
		record.TenantID, record.SessionID = tenant, session
		key := storedSession{tenant: tenant, session: session}
		f.reads.sessions[key] = true
		f.reads.nextRecords[key] = record
	}
}

// withJournal gives a session a durable journal. The records are numbered from
// one in the order given, which is the store's own dense sequence.
func withJournal(tenant sessionwire.TenantID, session sessionwire.SessionID, records ...fakeJournalRecord) fixtureOption {
	return func(_ *RouterConfig, f *fixture) {
		key := storedSession{tenant: tenant, session: session}
		f.reads.sessions[key] = true
		f.reads.journals[key] = records
		record, described := f.reads.records[key]
		if !described {
			// A journal belongs to a session, and a session has durable state.
			// Seeding a projectable record here keeps a test that cares only
			// about the journal from having to describe one.
			record = coldRecord(sessionwire.SessionStateIdle, sessionwire.SessionResidencyCold)
		}
		record.TenantID, record.SessionID = tenant, session
		record.LastJournalSeq = uint64(len(records))
		f.reads.records[key] = record
	}
}

// withStoreLayout makes the fake answer absence the way the named layout does.
func withStoreLayout(layout storeLayout) fixtureOption {
	return func(_ *RouterConfig, f *fixture) { f.reads.absent = layout.absence }
}

// withLeakingStore turns on the fake's disclosure of private material. Nothing
// in production can reach it; it exists so the assertions that private material
// never appears can be shown to observe one when it does.
func withLeakingStore() fixtureOption {
	return func(_ *RouterConfig, f *fixture) { f.reads.leak = true }
}

// coldRecord is a session that exists durably and is on no Host.
func coldRecord(state sessionwire.SessionState, residency sessionwire.SessionResidency) sessionstore.CatalogRecord {
	return sessionstore.CatalogRecord{
		AgentID:                "agent-pooled",
		RuntimeCompatibilityID: "runtime-1",
		CreatedAt:              time.Date(2026, 9, 4, 11, 0, 0, 0, time.UTC),
		LastActiveAt:           time.Date(2026, 9, 4, 11, 30, 0, 0, time.UTC),
		State:                  state,
		Residency:              residency,
		DesiredPlacement:       sessionwire.HostPlacementPooled,
		// A catalog record is created with a desired state, so the released
		// store refuses to canonicalize one whose generation is zero. A
		// fixture that skipped it would make every status read a 500 and would
		// be a fake LOOSER than the module it stands in for.
		DesiredGeneration: 4,
	}
}

// openGate builds one public gate projection opened at seq.
func openGate(id sessionwire.GateID, seq uint64) sessionwire.GateProjection {
	return sessionwire.GateProjection{
		GateID:           id,
		Kind:             "approval",
		Prompt:           sessionwire.GatePrompt{Title: "approve " + string(id), Body: "a presentation-safe prompt"},
		OpenedEventID:    sessionwire.EventID(fmt.Sprintf("event-%04d", seq)),
		OpenedJournalSeq: seq,
		Deadline:         time.Date(2026, 9, 4, 13, 0, 0, 0, time.UTC),
		Answerability:    sessionwire.GateAnswerabilitySuspended,
	}
}

func statusTarget(session sessionwire.SessionID) string {
	return "/v1/sessions/" + string(session) + "/status"
}

func journalTarget(session sessionwire.SessionID) string {
	return "/v1/sessions/" + string(session) + "/journal"
}

func gatesTarget(session sessionwire.SessionID) string {
	return "/v1/sessions/" + string(session) + "/gates"
}

// coldReadTargets is the three routes this task implements, which is the set
// every rule stated over "status, journal and gates" is swept across.
func coldReadTargets(session sessionwire.SessionID) map[string]string {
	return map[string]string{
		"status":  statusTarget(session),
		"journal": journalTarget(session),
		"gates":   gatesTarget(session),
	}
}

func decodeStatus(t *testing.T, recorder *httptest.ResponseRecorder) sessionwire.SessionStatus {
	t.Helper()

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body was %q", recorder.Code, recorder.Body)
	}
	var status sessionwire.SessionStatus
	if err := json.Unmarshal(recorder.Body.Bytes(), &status); err != nil {
		t.Fatalf("body %q is not a Core SessionStatus: %v", recorder.Body, err)
	}
	return status
}

func decodeJournalPage(t *testing.T, recorder *httptest.ResponseRecorder) sessionwire.JournalPage {
	t.Helper()

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body was %q", recorder.Code, recorder.Body)
	}
	var page sessionwire.JournalPage
	if err := json.Unmarshal(recorder.Body.Bytes(), &page); err != nil {
		t.Fatalf("body %q is not a Core JournalPage: %v", recorder.Body, err)
	}
	return page
}

func decodeGatePage(t *testing.T, recorder *httptest.ResponseRecorder) sessionwire.GatePage {
	t.Helper()

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body was %q", recorder.Code, recorder.Body)
	}
	var page sessionwire.GatePage
	if err := json.Unmarshal(recorder.Body.Bytes(), &page); err != nil {
		t.Fatalf("body %q is not a Core GatePage: %v", recorder.Body, err)
	}
	return page
}

func eventSequences(page sessionwire.JournalPage) []uint64 {
	out := make([]uint64, 0, len(page.Events))
	for _, event := range page.Events {
		out = append(out, event.JournalSeq)
	}
	return out
}

// ---------------------------------------------------------------------------
// Step 1: the durable status, at every state and residency, with no Host.
// ---------------------------------------------------------------------------

// coreSessionStates and coreSessionResidencies are Core v0.7.0's declared
// constants, named through Core's own identifiers so a rename upstream is a
// compile failure here.
//
// The LIMIT is stated rather than implied: Go cannot enumerate a package's
// constants at run time, so a state a later Core adds is invisible to this
// list. That gap is closed from the other side by
// TestTheStatusForwardsAStateThisBuildHasNeverHeardOf, which drives values that
// are in no list at all -- the handler does not branch on the value, so the
// property is over every non-empty string rather than over these twelve.
func coreSessionStates() []sessionwire.SessionState {
	return []sessionwire.SessionState{
		sessionwire.SessionStateRunning,
		sessionwire.SessionStateWaitingOnGate,
		sessionwire.SessionStateSuspended,
		sessionwire.SessionStateRestoring,
		sessionwire.SessionStateIdle,
		sessionwire.SessionStateFailed,
		sessionwire.SessionStateInterrupted,
		sessionwire.SessionStateStopped,
	}
}

func coreSessionResidencies() []sessionwire.SessionResidency {
	return []sessionwire.SessionResidency{
		sessionwire.SessionResidencyCold,
		sessionwire.SessionResidencyAttaching,
		sessionwire.SessionResidencyResident,
		sessionwire.SessionResidencyReleasing,
	}
}

// TestTheStatusIsTheDurableProjectionAtEveryStateAndResidency drives the whole
// grid the specification's "cold, running, releasing, failed" names four points
// of, with ZERO Hosts in the deployment.
//
// The two dimensions are independent -- Core says so in SessionResidency's own
// doc -- so testing the four named points would leave the other twenty-eight
// cells to a handler that could branch on either. Nothing is stopped or
// started: the fixture's directory advertises nothing and is asserted never to
// be consulted, which is what "readable while every Host is stopped" means at
// this layer.
func TestTheStatusIsTheDurableProjectionAtEveryStateAndResidency(t *testing.T) {
	t.Parallel()

	for _, state := range coreSessionStates() {
		for _, residency := range coreSessionResidencies() {
			t.Run(string(state)+"/"+string(residency), func(t *testing.T) {
				t.Parallel()

				record := coldRecord(state, residency)
				record.LastJournalSeq = 41
				f := newFixture(t, withSessions(), withRecord(fixtureTenant, fixtureSession, record))
				status := decodeStatus(t, f.get(statusTarget(fixtureSession)))

				if status.SessionID != fixtureSession {
					t.Errorf("session_id = %q, want %q", status.SessionID, fixtureSession)
				}
				if status.AgentID != record.AgentID {
					t.Errorf("agent_id = %q, want %q", status.AgentID, record.AgentID)
				}
				if status.State != state {
					t.Errorf("state = %q, want %q", status.State, state)
				}
				if status.Residency != residency {
					t.Errorf("residency = %q, want %q", status.Residency, residency)
				}
				if status.JournalTip != 41 {
					t.Errorf("journal_tip = %d, want 41", status.JournalTip)
				}
				if !status.UpdatedAt.Equal(record.LastActiveAt) {
					t.Errorf("updated_at = %s, want %s", status.UpdatedAt, record.LastActiveAt)
				}
				// No Host was consulted, and none could have been: the
				// directory is the only seam in this package that names one.
				if requests, _ := f.targets.snapshot(); len(requests) != 0 {
					t.Errorf("a status read consulted the Host directory: %+v", requests)
				}
			})
		}
	}
}

// TestTheStatusForwardsAStateThisBuildHasNeverHeardOf is the other half of the
// enumeration above, and the one that makes the property hold "for all X".
//
// Core's SessionState doc requires a consumer to treat an unfamiliar non-empty
// value as a state it cannot control rather than as a missing session, and
// SessionStatus.Validate enforces only non-emptiness. So the handler must not
// branch on the value at all, and a value in no list is the way to see that.
func TestTheStatusForwardsAStateThisBuildHasNeverHeardOf(t *testing.T) {
	t.Parallel()

	for _, unknown := range []string{"quiescing", "state-from-a-later-wire-version", "x"} {
		record := coldRecord(sessionwire.SessionState(unknown), sessionwire.SessionResidency(unknown+"-residency"))
		f := newFixture(t, withSessions(), withRecord(fixtureTenant, fixtureSession, record))
		status := decodeStatus(t, f.get(statusTarget(fixtureSession)))
		if string(status.State) != unknown {
			t.Errorf("state = %q, want the record's %q", status.State, unknown)
		}
		if string(status.Residency) != unknown+"-residency" {
			t.Errorf("residency = %q, want the record's %q", status.Residency, unknown+"-residency")
		}
	}
}

// TestTheStatusNamesTheFirstOpenGateAndNoOther pins the one derived member.
//
// WaitingGateID is Core's, and it is the FIRST gate in the record's canonical
// (opened_seq, gate_id) order rather than any open gate -- CatalogRecord.Status
// says so, and two readers of one record must name the same gate. The gate list
// itself is the gates route's answer; the status carries an identifier only.
func TestTheStatusNamesTheFirstOpenGateAndNoOther(t *testing.T) {
	t.Parallel()

	record := coldRecord(sessionwire.SessionStateWaitingOnGate, sessionwire.SessionResidencyCold)
	record.LastJournalSeq = 9
	record.OpenGates = []sessionwire.GateProjection{openGate("gate-first", 4), openGate("gate-second", 7)}
	f := newFixture(t, withSessions(), withRecord(fixtureTenant, fixtureSession, record))
	recorder := f.get(statusTarget(fixtureSession))
	status := decodeStatus(t, recorder)

	if status.WaitingGateID != "gate-first" {
		t.Errorf("waiting_gate_id = %q, want the first open gate", status.WaitingGateID)
	}
	if strings.Contains(recorder.Body.String(), "gate-second") {
		t.Errorf("the status body carries a second gate: %q", recorder.Body)
	}
	// A session with no open gate omits the member rather than carrying an
	// empty one, which is Core's rule and is what a client branches on.
	quiet := newFixture(t, withSessions(),
		withRecord(fixtureTenant, fixtureSession, coldRecord(sessionwire.SessionStateIdle, sessionwire.SessionResidencyCold)))
	if body := quiet.get(statusTarget(fixtureSession)).Body.String(); strings.Contains(body, "waiting_gate_id") {
		t.Errorf("a session with no open gate still names one: %q", body)
	}
}

// TestARecordCoreRefusesToProjectIsAFaultNotAnAnswer gives the status handler's
// projection failure a reader.
//
// A stored record the released store cannot canonicalize -- here one with no
// desired generation, which sessionstore refuses because every catalog record is
// created with a desired state -- is durable state disagreeing with itself. It
// is not something the caller did and must not be reported as a missing session,
// which would tell a client to stop asking about a session that is there.
func TestARecordCoreRefusesToProjectIsAFaultNotAnAnswer(t *testing.T) {
	t.Parallel()

	unprojectable := coldRecord(sessionwire.SessionStateIdle, sessionwire.SessionResidencyCold)
	unprojectable.DesiredGeneration = 0
	f := newFixture(t, withSessions(), withRecord(fixtureTenant, fixtureSession, unprojectable))
	recorder := f.get(statusTarget(fixtureSession))
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body was %q", recorder.Code, recorder.Body)
	}
	if code := decodeEnvelope(t, recorder).Error.Code; code != ErrorCodeInternal {
		t.Errorf("code = %q, want %q", code, ErrorCodeInternal)
	}
	// The control: the same record with the member the store requires projects
	// cleanly, so the 500 above is about that member and not about the fixture.
	projectable := unprojectable
	projectable.DesiredGeneration = 4
	ok := newFixture(t, withSessions(), withRecord(fixtureTenant, fixtureSession, projectable))
	if recorder := ok.get(statusTarget(fixtureSession)); recorder.Code != http.StatusOK {
		t.Fatalf("the control answered %d; body was %q", recorder.Code, recorder.Body)
	}
}

// ---------------------------------------------------------------------------
// Steps 2 and 3: the journal's tail-first initial read and its forward pages.
// ---------------------------------------------------------------------------

// longJournal builds n positions, every third one private.
func longJournal(n int) []fakeJournalRecord {
	records := make([]fakeJournalRecord, 0, n)
	for seq := uint64(1); seq <= uint64(n); seq++ {
		if seq%3 == 0 {
			records = append(records, privateRecord(seq))
			continue
		}
		records = append(records, publicRecord(seq))
	}
	return records
}

// TestTheInitialJournalViewIsABoundedTailAtTheCapturedTip is step 3's rule.
//
// A view with no cursor and no position receives the END of the journal, not
// its beginning. The fixture is long enough that the difference is unmissable:
// three thousand positions, of which the answer must carry the last few.
func TestTheInitialJournalViewIsABoundedTailAtTheCapturedTip(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withSessions(), withJournal(fixtureTenant, fixtureSession, longJournal(3000)...))
	page := decodeJournalPage(t, f.get(journalTarget(fixtureSession)))

	if page.CapturedTip != 3000 {
		t.Fatalf("journal_tip = %d, want 3000", page.CapturedTip)
	}
	if len(page.Events) == 0 {
		t.Fatal("the initial view carried no events at all")
	}
	// The tail reaches the tip's neighbourhood: the last public record is at
	// 2999, because 3000 is private.
	if last := page.Events[len(page.Events)-1].JournalSeq; last != 2999 {
		t.Errorf("the last event is at %d, want the journal's last public record at 2999", last)
	}
	if page.CoveredThrough != 3000 {
		t.Errorf("covered_through = %d, want the captured tip 3000", page.CoveredThrough)
	}
	// The first event is far from the beginning. This is the assertion that
	// fails for a replay from sequence one.
	if first := page.Events[0].JournalSeq; first < 2900 {
		t.Errorf("the initial view begins at sequence %d, which is not a tail of a 3000-record journal", first)
	}
}

// TestTheInitialJournalViewNeverReplaysFromTheFirstSequence is the same rule
// read from the REQUEST side, and it is a negative assertion, so what matters
// is that the probe can see the thing it forbids.
//
// It can see three different failures, each with its own reader: a request
// lacking Tail positioning, a page that begins at sequence
// one (the events), and a handler that walks the whole journal by following its
// own cursor (the call count). A replay from zero fails all three.
func TestTheInitialJournalViewNeverReplaysFromTheFirstSequence(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withSessions(), withJournal(fixtureTenant, fixtureSession, longJournal(3000)...))
	page := decodeJournalPage(t, f.get(journalTarget(fixtureSession)))

	requests := f.journalSnapshot()
	if len(requests) == 0 {
		t.Fatal("the journal was never read, so this probe proves nothing")
	}
	// Tail selects the window at the store's captured tip. An ordinary
	// unpositioned forward read would return the journal's oldest events.
	reading := requests[len(requests)-1]
	if !reading.Tail || reading.FromSeq != 0 || reading.Cursor != "" {
		t.Errorf("initial request did not select only Tail: %+v", reading)
	}
	if reading.Limit <= 0 {
		t.Errorf("the reading request carried limit %d, which hands the bound to the store", reading.Limit)
	}
	if uint64(len(page.Events)) > uint64(reading.Limit) {
		t.Errorf("the page carried %d events under a limit of %d", len(page.Events), reading.Limit)
	}
	// No walk. A handler that followed its own NextCursor to exhaustion would
	// read this journal hundreds of times and is bounded by nothing.
	if len(requests) != 1 {
		t.Errorf("the initial view read the journal %d times; want one Tail request", len(requests))
	}
}

// TestTheInitialJournalViewRequestsOneTail keeps window selection in the store.
func TestTheInitialJournalViewRequestsOneTail(t *testing.T) {
	t.Parallel()
	f := newFixture(t, withSessions(), withJournal(fixtureTenant, fixtureSession, longJournal(500)...))
	f.get(journalTarget(fixtureSession))
	requests := f.journalSnapshot()
	if len(requests) != 1 {
		t.Fatalf("initial view made %d journal reads, want one Tail request", len(requests))
	}
	req := requests[0]
	if !req.Tail || req.FromSeq != 0 || req.Cursor != "" || req.Limit != 64 || req.ScanLimit != 64 {
		t.Errorf("initial request = %+v, want Tail with event/scan limits 64 and no other position", req)
	}
}

// TestAJournalPageIdentifiesItsTipCoverageAndCursor is step 2's response
// contract, asserted on the BYTES rather than on a decoded value: a client
// reads members, and a member Core omits is a member no client can branch on.
func TestAJournalPageIdentifiesItsTipCoverageAndCursor(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withSessions(), withJournal(fixtureTenant, fixtureSession, longJournal(60)...))
	recorder := f.get(journalTarget(fixtureSession) + "?from_seq=1&limit=5")
	body := recorder.Body.String()
	for _, member := range []string{`"journal_tip"`, `"covered_through"`, `"next_cursor"`, `"events"`} {
		if !strings.Contains(body, member) {
			t.Errorf("the page omits %s: %q", member, body)
		}
	}
	page := decodeJournalPage(t, recorder)
	if page.CapturedTip != 60 {
		t.Errorf("journal_tip = %d, want 60", page.CapturedTip)
	}
	if page.NextCursor == "" {
		t.Fatal("a truncated page issued no cursor")
	}
	if page.CoveredThrough >= page.CapturedTip {
		t.Errorf("covered_through = %d on a truncated page whose tip is %d", page.CoveredThrough, page.CapturedTip)
	}
	// The exhausted page omits the cursor, which is how a client knows the walk
	// is over. Without this the member above would be satisfied by a page that
	// always carried one.
	last := decodeJournalPage(t, f.get(journalTarget(fixtureSession)+"?from_seq=59&limit=5"))
	if last.NextCursor != "" {
		t.Errorf("an exhausted page issued cursor %q", last.NextCursor)
	}
}

// TestAForwardCursorIsForwardedVerbatimAndPositionsTheNextPage is the paging
// walk. The cursor is the store's own opaque token: Factory hands it out and
// hands it back unread.
func TestAForwardCursorIsForwardedVerbatimAndPositionsTheNextPage(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withSessions(), withJournal(fixtureTenant, fixtureSession, longJournal(60)...))
	first := decodeJournalPage(t, f.get(journalTarget(fixtureSession)+"?from_seq=1&limit=4"))
	if first.NextCursor == "" {
		t.Fatal("the first page issued no cursor")
	}
	second := decodeJournalPage(t, f.get(journalTarget(fixtureSession)+"?cursor="+string(first.NextCursor)+"&limit=4"))

	forwarded := f.journalSnapshot()
	last := forwarded[len(forwarded)-1]
	if last.Cursor != first.NextCursor {
		t.Errorf("the store received cursor %q, want the one it issued, %q", last.Cursor, first.NextCursor)
	}
	if last.FromSeq != 0 {
		t.Errorf("a cursor request also carried from_seq %d, which the store refuses", last.FromSeq)
	}
	firstSeqs, secondSeqs := eventSequences(first), eventSequences(second)
	if len(secondSeqs) == 0 {
		t.Fatal("the second page was empty")
	}
	if secondSeqs[0] <= firstSeqs[len(firstSeqs)-1] {
		t.Errorf("the second page begins at %d, which is not after the first page's %d", secondSeqs[0], firstSeqs[len(firstSeqs)-1])
	}
	if second.CapturedTip != first.CapturedTip {
		t.Errorf("the walk's captured tip moved from %d to %d", first.CapturedTip, second.CapturedTip)
	}
}

// TestAnOlderPageIsAddressedByItsOwnPosition is step 3's second sentence. The
// initial view is a tail; everything older is a separate, equally bounded read.
func TestAnOlderPageIsAddressedByItsOwnPosition(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withSessions(), withJournal(fixtureTenant, fixtureSession, longJournal(3000)...))
	tail := decodeJournalPage(t, f.get(journalTarget(fixtureSession)))
	oldest := eventSequences(tail)[0]

	older := decodeJournalPage(t, f.get(journalTarget(fixtureSession)+"?from_seq=1&limit=10"))
	seqs := eventSequences(older)
	if !slices.Equal(seqs, []uint64{1, 2, 4, 5, 7, 8, 10}) || older.CoveredThrough != 10 || older.NextCursor == "" {
		t.Fatalf("older page sequences=%v covered=%d cursor=%q, want public subset of positions 1..10 and continuation", seqs, older.CoveredThrough, older.NextCursor)
	}
	if seqs[0] != 1 {
		t.Errorf("an explicitly positioned page began at %d, want 1", seqs[0])
	}
	if seqs[len(seqs)-1] >= oldest {
		t.Errorf("the older page reaches %d, which is not older than the tail's %d", seqs[len(seqs)-1], oldest)
	}
}

// TestAnExplicitFirstPositionIsNotTheSameRequestAsNamingNone is the reader for
// the distinction journalPosition.positioned exists to make.
//
// from_seq=0 is a POSITION -- SessionStore reads zero as "the first record" --
// and it is the spelling a client uses to ask for the oldest page. Naming no
// position at all is the tail. Reading the first as the second would silently
// answer "show me the beginning" with the end, and the two are otherwise easy
// to conflate because the zero value of the member carrying the position is the
// same zero.
func TestAnExplicitFirstPositionIsNotTheSameRequestAsNamingNone(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withSessions(), withJournal(fixtureTenant, fixtureSession, longJournal(3000)...))
	explicit := decodeJournalPage(t, f.get(journalTarget(fixtureSession)+"?from_seq=0"))
	seqs := eventSequences(explicit)
	if len(seqs) == 0 {
		t.Fatal("an explicit first position returned nothing")
	}
	if seqs[0] != 1 {
		t.Errorf("from_seq=0 began at %d, want the journal's first record", seqs[0])
	}
	// A named position requires one forward request with Tail disabled.
	if requests := f.journalSnapshot(); len(requests) != 1 {
		t.Errorf("an explicitly positioned read made %d journal reads, want 1", len(requests))
	}
	// The control: the SAME fixture with no position answers with the tail, so
	// the assertion above is about the parameter and not about the journal.
	tail := decodeJournalPage(t, newFixture(t, withSessions(),
		withJournal(fixtureTenant, fixtureSession, longJournal(3000)...)).get(journalTarget(fixtureSession)))
	if tailSeqs := eventSequences(tail); len(tailSeqs) == 0 || tailSeqs[0] < 2900 {
		t.Fatalf("the unpositioned control began at %v, so it is not a tail", tailSeqs)
	}
}

// TestAShortJournalTailBeginsAtTheFirstRecord is the boundary the tail
// arithmetic has to get right: a journal shorter than the window has no older
// page, so the tail is the whole journal and the walk starts at the beginning.
func TestAShortJournalTailBeginsAtTheFirstRecord(t *testing.T) {
	t.Parallel()

	for _, size := range []int{0, 1, 2, 5} {
		f := newFixture(t, withSessions(), withJournal(fixtureTenant, fixtureSession, longJournal(size)...))
		page := decodeJournalPage(t, f.get(journalTarget(fixtureSession)))
		if page.CapturedTip != uint64(size) {
			t.Errorf("size %d: journal_tip = %d", size, page.CapturedTip)
		}
		if page.CoveredThrough != uint64(size) {
			t.Errorf("size %d: covered_through = %d, want the whole journal", size, page.CoveredThrough)
		}
		seqs := eventSequences(page)
		if size >= 1 && (len(seqs) == 0 || seqs[0] != 1) {
			t.Errorf("size %d: the tail began at %v, want the first record", size, seqs)
		}
		if size == 0 && len(seqs) != 0 {
			t.Errorf("an empty journal produced %v", seqs)
		}
		if page.NextCursor != "" {
			t.Errorf("size %d: a complete tail issued cursor %q", size, page.NextCursor)
		}
	}
}

// TestARefusedJournalCursorRestartsTheWalk keeps a token the store will not
// accept a caller's problem rather than a fault, and says which.
func TestARefusedJournalCursorRestartsTheWalk(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withSessions(), withJournal(fixtureTenant, fixtureSession, longJournal(20)...))
	// A cursor bound to ANOTHER session, which is exactly what the real store's
	// scope-bound token refuses.
	foreign := encodeFakeJournalCursor(fakeJournalPosition{nextSeq: 2, capturedTip: 20}, fixtureTenant, "session-elsewhere")
	recorder := f.get(journalTarget(fixtureSession) + "?cursor=" + foreign)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body was %q", recorder.Code, recorder.Body)
	}
	if code := decodeEnvelope(t, recorder).Error.Code; code != sessionwire.ErrorCodeInvalidRequest {
		t.Errorf("code = %q, want %q", code, sessionwire.ErrorCodeInvalidRequest)
	}
}

// ---------------------------------------------------------------------------
// Step 6: public bodies travel through, private records do not.
// ---------------------------------------------------------------------------

// TestPrivateJournalRecordsOnlyAdvanceTheWatermark is step 6's second sentence.
//
// The fixture holds public and private records in one sequence space. The
// answer must carry the public ones, must carry none of the private ones' bytes
// anywhere in the response, and must close the private positions through
// covered_through alone -- which is the only member that can, since a client
// otherwise sees a gap it cannot distinguish from a lost record.
func TestPrivateJournalRecordsOnlyAdvanceTheWatermark(t *testing.T) {
	t.Parallel()

	records := []fakeJournalRecord{
		publicRecord(1), privateRecord(2), privateRecord(3), publicRecord(4), privateRecord(5),
	}
	f := newFixture(t, withSessions(), withJournal(fixtureTenant, fixtureSession, records...))
	recorder := f.get(journalTarget(fixtureSession))
	page := decodeJournalPage(t, recorder)

	if got := eventSequences(page); !slices.Equal(got, []uint64{1, 4}) {
		t.Errorf("events at %v, want only the public positions 1 and 4", got)
	}
	if page.CoveredThrough != 5 {
		t.Errorf("covered_through = %d, want 5: the private positions are closed by the watermark", page.CoveredThrough)
	}
	if leak := privateMaterialIn(recorder, privateJournalMarker); leak != "" {
		t.Errorf("private material reached the caller: %s", leak)
	}
}

// TestThePrivateMaterialProbeSeesALeak is the control for the assertion above.
//
// A negative assertion is worth exactly what its probe can observe, so the
// probe is driven against a store that DOES publish the withheld bytes. If this
// fails, the test above is asserting nothing.
func TestThePrivateMaterialProbeSeesALeak(t *testing.T) {
	t.Parallel()

	records := []fakeJournalRecord{publicRecord(1), privateRecord(2)}
	f := newFixture(t, withSessions(), withLeakingStore(),
		withJournal(fixtureTenant, fixtureSession, records...))
	recorder := f.get(journalTarget(fixtureSession))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body was %q", recorder.Code, recorder.Body)
	}
	if leak := privateMaterialIn(recorder, privateJournalMarker); leak == "" {
		t.Fatalf("the probe reported no leak from a store that published one: %q", recorder.Body)
	}
	// And the header half of the probe is exercised too, so a leak that
	// travelled in a header rather than a body would be seen.
	header := httptest.NewRecorder()
	header.Header().Set("X-Diagnostic", "prefix "+privateJournalMarker)
	if leak := privateMaterialIn(header, privateJournalMarker); leak == "" {
		t.Error("the probe does not read header values")
	}
}

// privateMaterialIn reports where a marker appears in a response, searching the
// body and every header value. It names the place rather than answering a
// boolean so a failure says how the material escaped.
func privateMaterialIn(recorder *httptest.ResponseRecorder, marker string) string {
	if strings.Contains(recorder.Body.String(), marker) {
		return "in the response body"
	}
	for name, values := range recorder.Header() {
		for _, value := range values {
			if strings.Contains(value, marker) {
				return "in header " + name
			}
		}
	}
	return ""
}

// TestAPublicBodyIsForwardedByteForByte is step 6's first sentence.
//
// The stored body is canonical AND is not what a Go round trip produces: its
// members are out of alphabetical order and it carries a \u escape, both of
// which encoding/json changes when a value is decoded and re-encoded. So byte
// equality here is a reader for "not decoded or re-encoded" rather than merely
// for "the same JSON".
func TestAPublicBodyIsForwardedByteForByte(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withSessions(), withJournal(fixtureTenant, fixtureSession, publicRecord(1)))
	recorder := f.get(journalTarget(fixtureSession))
	stored := string(publicRecord(1).body)

	if !strings.Contains(recorder.Body.String(), stored) {
		t.Errorf("the response does not carry the stored body verbatim.\nstored:   %s\nresponse: %s", stored, recorder.Body)
	}
	// The control: the round trip this is meant to detect really does change
	// these bytes, so byte equality is not trivially satisfied.
	var value any
	if err := json.Unmarshal([]byte(stored), &value); err != nil {
		t.Fatalf("the fixture body is not JSON: %v", err)
	}
	roundTripped, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	if string(roundTripped) == stored {
		t.Fatal("the fixture body survives a decode and re-encode unchanged, so byte equality proves nothing")
	}
}

// ---------------------------------------------------------------------------
// Step 4: the gate page.
// ---------------------------------------------------------------------------

// TestTheGatePageCarriesZeroOrManyOrderedProjections drives the cardinalities
// step 4 names -- none, one, several -- rather than the one a fixture happens
// to hold.
func TestTheGatePageCarriesZeroOrManyOrderedProjections(t *testing.T) {
	t.Parallel()

	for name, gates := range map[string][]sessionwire.GateProjection{
		"none":    nil,
		"one":     {openGate("gate-a", 3)},
		"several": {openGate("gate-a", 3), openGate("gate-b", 5), openGate("gate-c", 11)},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			record := coldRecord(sessionwire.SessionStateWaitingOnGate, sessionwire.SessionResidencyCold)
			record.LastJournalSeq = 40
			record.OpenGates = gates
			f := newFixture(t, withSessions(), withRecord(fixtureTenant, fixtureSession, record))
			recorder := f.get(gatesTarget(fixtureSession))
			page := decodeGatePage(t, recorder)

			if len(page.Gates) != len(gates) {
				t.Fatalf("the page carried %d gates, want %d", len(page.Gates), len(gates))
			}
			if page.OpenGateCount != uint64(len(gates)) {
				t.Errorf("open_gate_count = %d, want %d", page.OpenGateCount, len(gates))
			}
			if page.JournalTip != 40 {
				t.Errorf("journal_tip = %d, want 40", page.JournalTip)
			}
			var previous uint64
			for _, gate := range page.Gates {
				if gate.OpenedJournalSeq <= previous {
					t.Errorf("gate %q opens at %d, which does not follow %d", gate.GateID, gate.OpenedJournalSeq, previous)
				}
				previous = gate.OpenedJournalSeq
			}
			// An empty set is an empty ARRAY, not a null: a client that
			// branched on null would treat "no gates" as "no answer".
			if len(gates) == 0 && !strings.Contains(recorder.Body.String(), `"gates":[]`) {
				t.Errorf("no gates was rendered as %q", recorder.Body)
			}
		})
	}
}

// TestTheGatePageCarriesOnlyTheMembersCoreDeclares is step 4's "no raw answer
// or private prepared payload", derived from the mechanism rather than from a
// list of fields somebody thought of.
//
// The mechanism is that the public projection is Core's GateProjection, whose
// schema says in its own words that it "intentionally contains presentation-safe
// prompt data only, never a submitted answer, private prepared payload,
// credential, or signed URL". So the space to check is not four field names: it
// is EVERY member name the response's gate objects carry, against the set Core's
// type declares. Anything else -- an answer, a prepared payload, a signed URL,
// or something nobody has thought of yet -- is a member Core does not declare.
func TestTheGatePageCarriesOnlyTheMembersCoreDeclares(t *testing.T) {
	t.Parallel()

	declared := declaredJSONMembers(reflect.TypeOf(sessionwire.GateProjection{}))
	if len(declared) < 7 {
		t.Fatalf("the derivation found only %v members of GateProjection, so it is not reading the type", declared)
	}
	for _, required := range []string{"gate_id", "opened_journal_seq", "answerability", "prompt"} {
		if !slices.Contains(declared, required) {
			t.Fatalf("the derived member set %v omits %q, so it is not GateProjection's", declared, required)
		}
	}

	record := coldRecord(sessionwire.SessionStateWaitingOnGate, sessionwire.SessionResidencyCold)
	record.LastJournalSeq = 40
	gate := openGate("gate-a", 3)
	gate.Prompt.Controls = []sessionwire.GateControl{{Action: "approve", Label: "Approve"}}
	gate.Prompt.Schema = sessionwire.GatePromptSchema{Fields: []sessionwire.GatePromptField{{
		Name: "reason", Label: "Reason", Kind: sessionwire.GateFieldKindText, Required: true,
	}}}
	record.OpenGates = []sessionwire.GateProjection{gate}
	f := newFixture(t, withSessions(), withRecord(fixtureTenant, fixtureSession, record))

	var page struct {
		Gates []map[string]json.RawMessage `json:"gates"`
	}
	recorder := f.get(gatesTarget(fixtureSession))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; body was %q", recorder.Code, recorder.Body)
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(page.Gates) != 1 {
		t.Fatalf("the page carried %d gates, want 1", len(page.Gates))
	}
	for member := range page.Gates[0] {
		if !slices.Contains(declared, member) {
			t.Errorf("the gate carries member %q, which Core's GateProjection does not declare", member)
		}
	}
	// The check reads the members it is given, which is what makes the sweep
	// above a measurement rather than an empty loop.
	if len(page.Gates[0]) != len(declared) {
		t.Errorf("the gate carries %d members and Core declares %d; the projection is not being forwarded whole",
			len(page.Gates[0]), len(declared))
	}
}

// declaredJSONMembers reports the JSON member names a struct type declares,
// read from its own tags. It is the derivation the gate check rests on, so it
// reads the released type rather than a restatement of it.
func declaredJSONMembers(subject reflect.Type) []string {
	var out []string
	for i := range subject.NumField() {
		field := subject.Field(i)
		tag, ok := field.Tag.Lookup("json")
		if !ok {
			continue
		}
		name, _, _ := strings.Cut(tag, ",")
		if name == "" || name == "-" {
			continue
		}
		out = append(out, name)
	}
	slices.Sort(out)
	return out
}

// TestNoSubmittedGateAnswerReachesTheGatePage is the behavioural half, with its
// own control: a store that published an answer in the one member a body could
// travel in must be seen doing it.
func TestNoSubmittedGateAnswerReachesTheGatePage(t *testing.T) {
	t.Parallel()

	record := coldRecord(sessionwire.SessionStateWaitingOnGate, sessionwire.SessionResidencyCold)
	record.LastJournalSeq = 40
	record.OpenGates = []sessionwire.GateProjection{openGate("gate-a", 3)}

	clean := newFixture(t, withSessions(), withRecord(fixtureTenant, fixtureSession, record))
	if leak := privateMaterialIn(clean.get(gatesTarget(fixtureSession)), gateAnswerMarker); leak != "" {
		t.Errorf("a submitted answer reached the caller: %s", leak)
	}
	leaking := newFixture(t, withSessions(), withLeakingStore(), withRecord(fixtureTenant, fixtureSession, record))
	if leak := privateMaterialIn(leaking.get(gatesTarget(fixtureSession)), gateAnswerMarker); leak == "" {
		t.Fatal("the probe reported no answer from a store that published one, so the assertion above is vacuous")
	}
}

// ---------------------------------------------------------------------------
// Step 5: what is refused before the store, and what cannot be.
// ---------------------------------------------------------------------------

// TestEveryColdReadRefusesWhatItCanBeforeTheStore sweeps the three routes
// against every refusal this surface can make without a durable read.
//
// The assertion is the CALL COUNT, not the status: a handler that answered 400
// after asking the store would satisfy a status check and would still have made
// a durable query on behalf of a request it was going to refuse.
func TestEveryColdReadRefusesWhatItCanBeforeTheStore(t *testing.T) {
	t.Parallel()

	oversized := strings.Repeat("s", sessionwire.MaxIDBytes+1)
	for route, target := range coldReadTargets(fixtureSession) {
		for name, probe := range map[string]struct {
			target string
			deny   bool
			status int
		}{
			"an invalid session identifier": {
				target: strings.Replace(target, string(fixtureSession), oversized, 1),
				status: http.StatusBadRequest,
			},
			"a principal the authorizer refuses": {target: target, deny: true, status: http.StatusForbidden},
		} {
			t.Run(route+"/"+name, func(t *testing.T) {
				t.Parallel()

				authorizer := &recordingAuthorizer{deny: probe.deny}
				f := newFixture(t, withAuthorizer(authorizer), withSessions(),
					withJournal(fixtureTenant, fixtureSession, longJournal(20)...))
				recorder := f.get(probe.target)
				if recorder.Code != probe.status {
					t.Fatalf("status = %d, want %d; body was %q", recorder.Code, probe.status, recorder.Body)
				}
				catalog, _ := f.reads.snapshot()
				if len(catalog) != 0 || len(f.journalSnapshot()) != 0 || len(f.gateSnapshot()) != 0 {
					t.Errorf("the refusal reached the store: %d catalog, %d journal, %d gate reads",
						len(catalog), len(f.journalSnapshot()), len(f.gateSnapshot()))
				}
			})
		}
	}
}

// TestTheJournalPositionIsValidatedBeforeTheStore covers the refusals only the
// journal has, for the same reason and with the same reader.
func TestTheJournalPositionIsValidatedBeforeTheStore(t *testing.T) {
	t.Parallel()

	for name, query := range map[string]string{
		"a cursor and a position together": "?cursor=x&from_seq=4",
		"a limit that is not a number":     "?limit=many",
		"a limit of zero":                  "?limit=0",
		"a negative limit":                 "?limit=-1",
		"two limits":                       "?limit=1&limit=5",
		"two positions":                    "?from_seq=1&from_seq=5",
		"a position that is not a number":  "?from_seq=early",
		"a negative position":              "?from_seq=-1",
		"two cursors":                      "?cursor=a&cursor=b",
		// The empty-value cases. The table had nine rows and not one of them
		// sent a parameter with no value, which is how "?cursor=" reached the
		// store as no cursor at all and turned the tail into a replay.
		"an empty cursor":   "?cursor=",
		"an empty position": "?from_seq=",
		"an empty limit":    "?limit=",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t, withSessions(), withJournal(fixtureTenant, fixtureSession, longJournal(20)...))
			recorder := f.get(journalTarget(fixtureSession) + query)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body was %q", recorder.Code, recorder.Body)
			}
			if code := decodeEnvelope(t, recorder).Error.Code; code != sessionwire.ErrorCodeInvalidRequest {
				t.Errorf("code = %q, want %q", code, sessionwire.ErrorCodeInvalidRequest)
			}
			if reads := f.journalSnapshot(); len(reads) != 0 {
				t.Errorf("a malformed position reached the store as %+v", reads)
			}
		})
	}
}

// TestTheJournalLimitIsClampedRatherThanRefused keeps the well-formed question
// answerable, narrowly, and keeps the clamped value inside what the store
// accepts.
func TestTheJournalLimitIsClampedRatherThanRefused(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withSessions(), withJournal(fixtureTenant, fixtureSession, longJournal(400)...))
	recorder := f.get(journalTarget(fixtureSession) + "?from_seq=1&limit=100000")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body was %q", recorder.Code, recorder.Body)
	}
	requests := f.journalSnapshot()
	sent := requests[len(requests)-1].Limit
	if sent > storePageCeiling {
		t.Errorf("the store was asked for a page of %d, which it refuses outright", sent)
	}
	if sent != maxJournalPageLimit {
		t.Errorf("the clamped limit was %d, want this surface's ceiling %d", sent, maxJournalPageLimit)
	}
	// Against a LITERAL as well as against the constant, because asserting a
	// clamp against the constant that produced it cannot see the constant
	// moving. This one is the journal's own number and no other surface's.
	if sent != 100 {
		t.Errorf("the journal's ceiling is now %d; it was 100, and the tenant list's is 200", sent)
	}
}

// TestTheTwoPageCeilingsAreNotTheSameNumber gives boundedPageLimit's ceiling
// parameter a reader.
//
// "The ceiling is the caller's, so the two routes keep their own" is what
// boundedPageLimit's doc claims, and while maxJournalPageLimit and
// maxSessionPageLimit were BOTH 200 nothing could observe it: the parameter
// received the identical value from both call sites, so swapping one constant
// for the other at either site was measured surviving the whole suite -- a
// parameter every call site passes identically is untested by construction.
//
// The numbers differ now for a reason derived from what a row costs, not for
// the sake of differing: a session summary is bounded by Core's vocabulary and
// a journal event carries an arbitrary stored body. This is the assertion that
// keeps them apart, so a later edit collapsing them onto one number is reported
// here rather than silently re-blinding the clamp tests.
func TestTheTwoPageCeilingsAreNotTheSameNumber(t *testing.T) {
	t.Parallel()

	if maxJournalPageLimit == maxSessionPageLimit {
		t.Fatalf("both ceilings are %d, so nothing can observe boundedPageLimit keeping the two routes' bounds apart",
			maxJournalPageLimit)
	}
	// And each surface really is clamped to its OWN: the journal's clamp is
	// asserted above, so this is the tenant list's, driven at a limit above
	// both ceilings so the two answers are distinguishable.
	f := newFixture(t)
	if recorder := f.get("/v1/sessions?limit=100000"); recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body was %q", recorder.Code, recorder.Body)
	}
	requests := f.reads.listSnapshot()
	if len(requests) != 1 {
		t.Fatalf("%d page reads, want 1", len(requests))
	}
	if requests[0].Limit != 200 {
		t.Errorf("the tenant list clamped to %d, want 200 -- the journal's ceiling is 100", requests[0].Limit)
	}
}

// TestACrossTenantColdReadIsIndistinguishableFromAbsence is A2.1's obligation
// extended to the three routes that now serve a body, and driven across every
// way the released store says "there is no such session".
//
// The whole response is compared -- status, body bytes, every header but the
// minted identifier -- because an identical error code is the usual mask for a
// difference elsewhere in the answer. The layouts matter: outside the legacy
// single-tenant layout the store verifies the session's collision witnesses
// before it reads a record, so absence arrives as a KeyspaceError and not as a
// catalog code at all.
func TestACrossTenantColdReadIsIndistinguishableFromAbsence(t *testing.T) {
	t.Parallel()

	for route, target := range coldReadTargets(fixtureSession) {
		for _, layout := range absenceLayouts() {
			t.Run(route+"/"+layout.name, func(t *testing.T) {
				t.Parallel()

				// withSessions builds a fresh store, so the layout is applied
				// AFTER it or the option would be discarded.
				crossTenant := newFixture(t,
					withSessions(storedSession{tenant: otherTenant, session: fixtureSession}),
					withStoreLayout(layout))
				absent := newFixture(t, withSessions(), withStoreLayout(layout))

				first, second := crossTenant.get(target), absent.get(target)
				if first.Code != http.StatusNotFound {
					t.Fatalf("the cross-tenant read answered %d, want 404; body was %q", first.Code, first.Body)
				}
				if code := decodeEnvelope(t, first).Error.Code; code != sessionwire.ErrorCodeSessionNotFound {
					t.Errorf("code = %q, want %q", code, sessionwire.ErrorCodeSessionNotFound)
				}
				if diff := responseDifference(first, second); diff != "" {
					t.Errorf("a session in another tenant is distinguishable from one that does not exist: %s", diff)
				}
				for _, secret := range []string{string(fixtureSession), string(fixtureTenant), string(otherTenant)} {
					if strings.Contains(first.Body.String(), secret) {
						t.Errorf("the 404 body %q names %q", first.Body, secret)
					}
				}
			})
		}
	}
}

// TestTheColdReadsAskTheStoreForThePrincipalsOwnTenant is the behavioural half
// of the scope: the tenant in every request the three routes build is the one
// the VERIFIER issued, not one the caller could name.
func TestTheColdReadsAskTheStoreForThePrincipalsOwnTenant(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withVerifierTenant(otherTenant), withSessions(),
		withJournal(otherTenant, fixtureSession, longJournal(9)...))
	for route, target := range coldReadTargets(fixtureSession) {
		if recorder := f.get(target); recorder.Code != http.StatusOK {
			t.Fatalf("%s answered %d; body was %q", route, recorder.Code, recorder.Body)
		}
	}
	catalog, _ := f.reads.snapshot()
	if len(catalog) == 0 {
		t.Fatal("no catalog read was made, so this sweep proves nothing")
	}
	for _, req := range catalog {
		if req.TenantID != otherTenant {
			t.Errorf("a catalog read was scoped to %q, want the verifier's %q", req.TenantID, otherTenant)
		}
	}
	journals := f.journalSnapshot()
	if len(journals) == 0 {
		t.Fatal("no journal read was made")
	}
	for _, req := range journals {
		if req.TenantID != otherTenant || req.SessionID != fixtureSession {
			t.Errorf("a journal read named (%q, %q)", req.TenantID, req.SessionID)
		}
	}
	gates := f.gateSnapshot()
	if len(gates) == 0 {
		t.Fatal("no gate read was made")
	}
	for _, req := range gates {
		if req.TenantID != otherTenant || req.SessionID != fixtureSession {
			t.Errorf("a gate read named (%q, %q)", req.TenantID, req.SessionID)
		}
	}
}

// ---------------------------------------------------------------------------
// The seam between the route chain and the handlers.
// ---------------------------------------------------------------------------

// TestTheResolvedRecordIsReadOnceAndReusedByTheStatus is about the WIRING, not
// about either layer.
//
// serveRoute resolves the session to establish that it exists within the
// principal's tenant; the status is a projection of exactly that record. Reading
// it twice would cost a second durable round trip AND would let the two reads
// disagree -- the existence decision made against one record and the answer
// rendered from another.
func TestTheResolvedRecordIsReadOnceAndReusedByTheStatus(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withSessions(),
		withRecord(fixtureTenant, fixtureSession, coldRecord(sessionwire.SessionStateIdle, sessionwire.SessionResidencyCold)))
	decodeStatus(t, f.get(statusTarget(fixtureSession)))
	if requests, _ := f.reads.snapshot(); len(requests) != 1 {
		t.Errorf("a status read made %d catalog reads, want 1", len(requests))
	}
}

// TestTheResolvedRecordIsCarriedToTheHandlerNotRebuilt shows the seam carries
// the record the chain read, rather than the handler re-deriving one.
//
// It is driven by making the store answer a DIFFERENT record from its second
// call onward, so the two behaviours produce different bodies. Without that
// divergence the test asserted only that the status matches the one record the
// fake held -- which is one of the thirty-two cells the state grid already
// covers, and which a handler that discarded the carried entry and re-read the
// catalog PASSES. Measured: that mutation ran this test alone, green.
//
// The divergence is not a contrivance. The two reads are separate round trips
// with nothing holding a lock across them, so a session whose state moves
// between them is ordinary; the point of carrying the record is that the
// existence decision and the body describe the same instant.
func TestTheResolvedRecordIsCarriedToTheHandlerNotRebuilt(t *testing.T) {
	t.Parallel()

	resolved := coldRecord(sessionwire.SessionStateRunning, sessionwire.SessionResidencyResident)
	successor := coldRecord(sessionwire.SessionStateFailed, sessionwire.SessionResidencyCold)
	f := newFixture(t, withSessions(),
		withRecord(fixtureTenant, fixtureSession, resolved),
		withSuccessorRecord(fixtureTenant, fixtureSession, successor))

	status := decodeStatus(t, f.get(statusTarget(fixtureSession)))
	if status.State != sessionwire.SessionStateRunning {
		t.Errorf("state = %q, want the RESOLVED record's %q; %q is the record a second read would return",
			status.State, sessionwire.SessionStateRunning, sessionwire.SessionStateFailed)
	}
	if status.Residency != sessionwire.SessionResidencyResident {
		t.Errorf("residency = %q, want the resolved record's %q", status.Residency, sessionwire.SessionResidencyResident)
	}
	// The control: the fake really does answer differently the second time, so
	// the assertion above is about which record was rendered rather than about
	// a store that could only ever give one answer.
	second := newFixture(t, withSessions(),
		withRecord(fixtureTenant, fixtureSession, resolved),
		withSuccessorRecord(fixtureTenant, fixtureSession, successor))
	if _, err := second.reads.GetCatalogEntry(context.Background(), sessionstore.GetCatalogEntryRequest{
		TenantID: fixtureTenant, SessionID: fixtureSession,
	}); err != nil {
		t.Fatalf("the control's first read failed: %v", err)
	}
	entry, err := second.reads.GetCatalogEntry(context.Background(), sessionstore.GetCatalogEntryRequest{
		TenantID: fixtureTenant, SessionID: fixtureSession,
	})
	if err != nil {
		t.Fatalf("the control's second read failed: %v", err)
	}
	if entry.Record.State != sessionwire.SessionStateFailed {
		t.Fatalf("the fake answered %q on its second call, so the divergence this test rests on does not exist",
			entry.Record.State)
	}
}

// TestTheResolvedRecordFallbackIsNotReachableThroughTheChain is the reader for
// the fail-closed branch in each handler.
//
// Every handler here refuses rather than serving a zero record when the chain
// did not resolve one. That branch is unreachable through the composed router,
// which is the point: it is asserted to be unreachable, and it is driven
// directly so it is not a line nothing executes.
//
// What it establishes differs by handler, and the difference is measured.
// Deleting the JOURNAL's guard and deleting the GATES' guard are both killed
// here, because both go on to make a durable read with an empty session
// identifier and this test requires none. Deleting the STATUS handler's guard
// is NOT killed and cannot be: Status() canonicalizes the record and refuses a
// zero one, so the answer is the same 500 internal_error either way, with no
// store read either way. That guard is defence against a dependency changing
// its validation, and it is recorded as such beside it rather than dressed up
// as something a test here can see.
func TestTheResolvedRecordFallbackIsNotReachableThroughTheChain(t *testing.T) {
	t.Parallel()

	bare := map[string]func(*Router) http.Handler{
		"status":  (*Router).serveSessionStatus,
		"journal": (*Router).serveSessionJournal,
		"gates":   (*Router).serveSessionGates,
	}
	for name, build := range bare {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// A fixture of its own, so the store-read assertion below is about
			// THIS request and cannot be satisfied or spoiled by a sibling.
			f := newFixture(t, withSessions(),
				withRecord(fixtureTenant, fixtureSession, coldRecord(sessionwire.SessionStateIdle, sessionwire.SessionResidencyCold)))
			recorder := httptest.NewRecorder()
			// The bare handler, with no chain in front of it: no operation
			// context and no resolved record.
			build(f.router).ServeHTTP(recorder,
				httptest.NewRequest(http.MethodGet, "/v1/sessions/"+string(fixtureSession)+"/"+name, nil))
			// The refusal is the internal fault, named rather than merely
			// "some 4xx or 5xx": a handler that answered 404 would be telling
			// a caller their session is gone when what happened is that the
			// chain did not run.
			if recorder.Code != http.StatusInternalServerError {
				t.Fatalf("the unresolved handler answered %d, want 500; body was %q", recorder.Code, recorder.Body)
			}
			if code := decodeEnvelope(t, recorder).Error.Code; code != ErrorCodeInternal {
				t.Errorf("code = %q, want %q", code, ErrorCodeInternal)
			}
			if requests, _ := f.reads.snapshot(); len(requests) != 0 {
				t.Errorf("an unresolved handler read the store: %+v", requests)
			}
			if len(f.journalSnapshot()) != 0 || len(f.gateSnapshot()) != 0 {
				t.Errorf("an unresolved handler made a durable read")
			}
		})
	}
	// The control: through the chain, the same handlers serve.
	f := newFixture(t, withSessions(),
		withRecord(fixtureTenant, fixtureSession, coldRecord(sessionwire.SessionStateIdle, sessionwire.SessionResidencyCold)))
	for route, target := range coldReadTargets(fixtureSession) {
		if recorder := f.get(target); recorder.Code != http.StatusOK {
			t.Errorf("%s answered %d through the chain; body was %q", route, recorder.Code, recorder.Body)
		}
	}
}

// TestACancelledColdReadIsTheCallersOwnAnswer gives the cancellation branch of
// each failure mapping a reader.
//
// It was written because catalogFailure and directoryFailure both carry one and
// neither had one: a context that ended because the CALLER went away is not a
// server fault and must not be counted as one.
func TestACancelledColdReadIsTheCallersOwnAnswer(t *testing.T) {
	t.Parallel()

	for route, target := range coldReadTargets(fixtureSession) {
		t.Run(route, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t, withSessions(), withJournal(fixtureTenant, fixtureSession, longJournal(20)...))
			f.reads.fail = context.Canceled
			recorder := f.get(target)
			if recorder.Code != statusClientClosedRequest {
				t.Fatalf("status = %d, want %d", recorder.Code, statusClientClosedRequest)
			}
			if envelope := decodeEnvelope(t, recorder); envelope.Error.Code != ErrorCodeTimeout {
				t.Errorf("code = %q", envelope.Error.Code)
			}
		})
	}
	// The agent list reaches the same branch through the DIRECTORY's mapping,
	// which is a separate function with the same shape.
	f := newFixture(t, withDepartment(pooledTemplate), withDirectoryFailure(context.Canceled))
	if recorder := f.get("/v1/agents"); recorder.Code != statusClientClosedRequest {
		t.Errorf("the agent list answered %d for a cancelled read, want %d", recorder.Code, statusClientClosedRequest)
	}
}

// TestAColdReadFailureIsRedacted keeps a dependency's diagnostic out of a
// caller's response for the three routes this task adds.
func TestAColdReadFailureIsRedacted(t *testing.T) {
	t.Parallel()

	const secret = "postgres://user:hunter2@db.internal:5432/sessions"
	for route, target := range coldReadTargets(fixtureSession) {
		t.Run(route, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t, withSessions(), withJournal(fixtureTenant, fixtureSession, longJournal(20)...))
			f.reads.fail = &sessionstore.CatalogError{Code: sessionstore.CatalogErrorBackend, Field: secret}
			recorder := f.get(target)
			if recorder.Code != http.StatusInternalServerError {
				t.Fatalf("status = %d, want 500", recorder.Code)
			}
			if strings.Contains(recorder.Body.String(), "hunter2") || strings.Contains(recorder.Body.String(), "db.internal") {
				t.Errorf("the body carries a dependency's diagnostic: %q", recorder.Body)
			}
		})
	}
}

// TestTheColdReadsAreServedUnderHeadAsWellAsGet keeps the RFC 9110 obligation
// true for the routes this task fills in.
func TestTheColdReadsAreServedUnderHeadAsWellAsGet(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withSessions(), withJournal(fixtureTenant, fixtureSession, longJournal(9)...))
	for route, target := range coldReadTargets(fixtureSession) {
		recorder := f.serve(request(http.MethodHead, target, nil))
		if recorder.Code != http.StatusOK {
			t.Errorf("HEAD %s answered %d; body was %q", route, recorder.Code, recorder.Body)
		}
		if got := recorder.Header().Get("Content-Type"); got != "application/json" {
			t.Errorf("HEAD %s Content-Type = %q", route, got)
		}
	}
}

// TestAColdReadCarriesNoValidatorAndIsNotStored is the same decision the
// tenant's session list made, for the same reason: a strong validator over a
// tenant's private state is a stable fingerprint of it, and it survives in
// proxy and browser logs where the body does not.
func TestAColdReadCarriesNoValidatorAndIsNotStored(t *testing.T) {
	t.Parallel()

	f := newFixture(t, withSessions(), withJournal(fixtureTenant, fixtureSession, longJournal(9)...))
	for route, target := range coldReadTargets(fixtureSession) {
		recorder := f.get(target)
		if etag := recorder.Header().Get("ETag"); etag != "" {
			t.Errorf("%s carries the validator %q", route, etag)
		}
		if control := recorder.Header().Get("Cache-Control"); control != "no-store" {
			t.Errorf("%s Cache-Control = %q, want no-store", route, control)
		}
	}
}

// ---------------------------------------------------------------------------
// The structural guards.
// ---------------------------------------------------------------------------

// TestEveryPageLimitIsCheckedAgainstTheStoreCeiling holds the compile-time
// checks to the set of limits that exist, rather than to the set somebody
// remembered.
//
// The checks themselves are the mechanism: uint(storePageCeiling - n) does not
// compile when n exceeds the ceiling, so a limit past what SessionStore accepts
// cannot be built rather than merely failing a test. What no constant
// expression can do is notice a limit that HAS no check, which is exactly what
// a later task adding one would produce. This is the reader for that, and it is
// derived: the limits are found by parsing the package's own production files,
// enumerated from the directory.
func TestEveryPageLimitIsCheckedAgainstTheStoreCeiling(t *testing.T) {
	t.Parallel()

	files := productionSources(t)
	if len(files) == 0 {
		t.Fatal("no production files were found, so this scan proves nothing")
	}
	limits, checked, constants, used := map[string]bool{}, map[string]bool{}, map[string]bool{}, map[string]bool{}
	for name, source := range files {
		report, err := scanPageLimits(name, source)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, limit := range report.limits {
			limits[limit] = true
		}
		for _, check := range report.checks {
			checked[check] = true
		}
		for _, constant := range report.constants {
			constants[constant] = true
		}
		for _, use := range report.limitUses {
			used[use] = true
		}
	}
	if len(limits) == 0 {
		t.Fatal("the production files declare no page limit, so this scan proves nothing")
	}
	// The second derivation, and the one that does not depend on a name. Every
	// CONSTANT this package hands to SessionStore as a page bound must be
	// checked, whatever it is called. Without it the subject was the naming
	// convention rather than the behaviour: a limit named anything else was
	// invisible in both directions, and a `const probeObjectChunkSize = 5000`
	// passed as a Limit: value sailed through at five times the real ceiling.
	if len(used) == 0 {
		t.Fatal("no identifier is passed to SessionStore as a page limit, so the use-derived rule reads nothing")
	}
	for name := range used {
		if !constants[name] {
			// A parameter or a local carries a value from somewhere else; the
			// bound on it is at whatever assigned it, and this rule is about
			// constants written into a request.
			continue
		}
		if !checked[name] {
			t.Errorf("%s is passed to SessionStore as a page limit and has no ceiling check; add const _ = uint(storePageCeiling - %s)", name, name)
		}
	}
	// The use rule's REACH, stated rather than implied: every sessionstore
	// request literal on this surface is a scope helper in routes.go, and three
	// of the four limits arrive at one through a PARAMETER, which the skip
	// above passes over. So exactly one constant is covered by use today, and
	// the other three rest on the suffix convention. Asserting that one is here
	// so the rule cannot quietly reach nothing; extending the scan to follow a
	// parameter would be inter-procedural value tracking, and a scan that
	// guessed at it would be a guard whose own reach nobody could state.
	if !used["agentProbePageLimit"] {
		t.Errorf("the use-derived scan found %v, which does not include the one constant written into a request literal", slices.Sorted(maps.Keys(used)))
	}
	for limit := range limits {
		if !checked[limit] {
			t.Errorf("%s is a page limit with no ceiling check; add const _ = uint(storePageCeiling - %s)", limit, limit)
		}
	}
	for check := range checked {
		if !limits[check] {
			t.Errorf("a ceiling check names %s, which is not a page limit this package declares", check)
		}
	}
	// The set really is the one in the tree, named so a rename is reported
	// rather than silently shrinking the subject.
	for _, want := range []string{
		"agentProbePageLimit", "maxSessionPageLimit",
		"maxJournalPageLimit", "defaultJournalPageLimit",
	} {
		if !limits[want] {
			t.Errorf("the scan did not find %s; it found %v", want, slices.Sorted(maps.Keys(limits)))
		}
	}
}

// TestThePageLimitScanReportsALimitWithNoCheck is the guard's other direction.
// Without it, "every limit is checked" would be equally explicable by a scan
// that finds no limits at all or by one that reports every name as checked.
func TestThePageLimitScanReportsALimitWithNoCheck(t *testing.T) {
	t.Parallel()

	const unchecked = "package httpapi\n\nconst newlyAddedPageLimit = 5000\n"
	report, err := scanPageLimits("unchecked.go", []byte(unchecked))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !slices.Contains(report.limits, "newlyAddedPageLimit") {
		t.Errorf("the scan found limits %v, want newlyAddedPageLimit", report.limits)
	}
	if len(report.checks) != 0 {
		t.Errorf("the scan found checks %v in a source that declares none", report.checks)
	}

	const checkedSource = "package httpapi\n\nconst newlyAddedPageLimit = 5000\n\nconst _ = uint(storePageCeiling - newlyAddedPageLimit)\n"
	report, err = scanPageLimits("checked.go", []byte(checkedSource))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !slices.Contains(report.checks, "newlyAddedPageLimit") {
		t.Errorf("the scan found checks %v, want newlyAddedPageLimit", report.checks)
	}
	// A subtraction that is not the ceiling check is not a check. Without this
	// the rule would be satisfied by any arithmetic mentioning the name.
	const wrongCeiling = "package httpapi\n\nconst newlyAddedPageLimit = 5000\n\nconst _ = uint(64 - newlyAddedPageLimit)\n"
	report, err = scanPageLimits("wrong.go", []byte(wrongCeiling))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(report.checks) != 0 {
		t.Errorf("a subtraction from something other than storePageCeiling was read as a check: %v", report.checks)
	}

	// The use-derived rule, at the construct the suffix rule cannot see: a
	// constant named nothing like a page limit, written into a SessionStore
	// request as one. This is the shape that passed cleanly at five times the
	// real ceiling before the rule was derived from use.
	const misnamed = "package httpapi\n\nconst probeObjectChunkSize = 5000\n\n" +
		"func (s scope) objects() sessionstore.ListObjectsRequest {\n" +
		"\treturn sessionstore.ListObjectsRequest{Limit: probeObjectChunkSize}\n}\n"
	report, err = scanPageLimits("misnamed.go", []byte(misnamed))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(report.limits) != 0 {
		t.Errorf("the suffix rule claimed %v for a constant named nothing like a page limit", report.limits)
	}
	if !slices.Contains(report.constants, "probeObjectChunkSize") {
		t.Errorf("the scan did not record probeObjectChunkSize as a constant; it found %v", report.constants)
	}
	if !slices.Contains(report.limitUses, "probeObjectChunkSize") {
		t.Errorf("the scan did not see probeObjectChunkSize passed as a page limit; it saw %v", report.limitUses)
	}
	// A Limit: on something that is not a SessionStore request is not this
	// rule's business, or the scan would report every local struct with a
	// bound in it.
	const foreign = "package httpapi\n\nconst somethingElse = 5000\n\n" +
		"func f() any { return elsewhere.Request{Limit: somethingElse} }\n"
	report, err = scanPageLimits("foreign.go", []byte(foreign))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(report.limitUses) != 0 {
		t.Errorf("a Limit: on a foreign type was read as a SessionStore page limit: %v", report.limitUses)
	}
}

// pageLimitReport is what one file says about page limits.
type pageLimitReport struct {
	// limits are constants NAMED as page limits: the suffix convention.
	limits []string
	// checks are the ceiling checks the file carries.
	checks []string
	// constants is every constant the file declares, which is what tells a
	// misnamed limit from a function parameter at a use site.
	constants []string
	// limitUses are the identifiers the file passes to SessionStore as a page
	// bound, which is the subject derived from USE rather than from spelling.
	limitUses []string
}

// scanPageLimits reports the page limits a file declares, the ceiling checks it
// carries, and the identifiers it hands SessionStore as a bound.
//
// It derives its subject two ways, and the second exists because the first is a
// CONVENTION. The suffix rule -- a constant whose name ends in PageLimit -- is
// this package's naming rule and is load-bearing only while everybody follows
// it; a limit named otherwise is invisible to it in both directions, and a
// const probeObjectChunkSize = 5000 written into a request was measured passing
// at five times the store's real ceiling. So the scan also collects every
// identifier appearing as the Limit member of a sessionstore request literal,
// which is what a page bound IS rather than what it is called.
//
// A check is exactly the constant expression uint(storePageCeiling - <name>),
// read from the parsed tree rather than matched in text, because a text match
// cannot tell an expression from a comment quoting one. The request literal is
// recognised the same way: by its type expression naming the sessionstore
// package, so a Limit member on some other type is not this rule's business.
func scanPageLimits(name string, source []byte) (pageLimitReport, error) {
	file, err := parser.ParseFile(token.NewFileSet(), name, source, 0)
	if err != nil {
		return pageLimitReport{}, err
	}
	var report pageLimitReport
	for _, declaration := range file.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok || general.Tok != token.CONST {
			continue
		}
		for _, spec := range general.Specs {
			value, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for _, ident := range value.Names {
				if ident.Name == "_" {
					continue
				}
				report.constants = append(report.constants, ident.Name)
				if strings.HasSuffix(ident.Name, "PageLimit") {
					report.limits = append(report.limits, ident.Name)
				}
			}
			for _, expression := range value.Values {
				if checked, ok := ceilingCheckSubject(expression); ok {
					report.checks = append(report.checks, checked)
				}
			}
		}
	}
	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.CompositeLit)
		if !ok || !isSessionStoreType(literal.Type) {
			return true
		}
		for _, element := range literal.Elts {
			keyed, ok := element.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			key, ok := keyed.Key.(*ast.Ident)
			if !ok || key.Name != "Limit" {
				continue
			}
			if ident, ok := keyed.Value.(*ast.Ident); ok {
				report.limitUses = append(report.limitUses, ident.Name)
			}
		}
		return true
	})
	return report, nil
}

// isSessionStoreType reports whether a composite literal's type is one of
// SessionStore's, which is what makes a Limit member a page bound rather than
// some local struct's field of the same name.
func isSessionStoreType(expression ast.Expr) bool {
	selector, ok := expression.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := selector.X.(*ast.Ident)
	return ok && pkg.Name == "sessionstore"
}

// ceilingCheckSubject reports the limit a uint(storePageCeiling - x) expression
// checks.
func ceilingCheckSubject(expression ast.Expr) (string, bool) {
	call, ok := expression.(*ast.CallExpr)
	if !ok || len(call.Args) != 1 {
		return "", false
	}
	converted, ok := call.Fun.(*ast.Ident)
	if !ok || converted.Name != "uint" {
		return "", false
	}
	binary, ok := call.Args[0].(*ast.BinaryExpr)
	if !ok || binary.Op != token.SUB {
		return "", false
	}
	left, ok := binary.X.(*ast.Ident)
	if !ok || left.Name != "storePageCeiling" {
		return "", false
	}
	right, ok := binary.Y.(*ast.Ident)
	if !ok {
		return "", false
	}
	return right.Name, true
}

// TestTheReadPlaneNamesNoPrivateBearingStoreMethod is step 6's structural half:
// Factory forwards stored canonical PUBLIC bodies, and the reason it cannot
// forward anything else is that it cannot NAME the read that would return it.
//
// The set of private-bearing methods is DERIVED rather than listed. SessionStore
// carries a session's private runtime material as a stored Envelope -- the raw
// record, public and private alike -- so a method whose RESULTS can reach an
// Envelope is a method that can hand a caller private bytes. The rule is that
// walk, applied to every method of the released *sessionstore.Store, and the
// requirement is that SessionReader declares none of them.
//
// Results only. A method that TAKES an Envelope is a writer, and Factory holding
// one would be a different defect with a different guard.
func TestTheReadPlaneNamesNoPrivateBearingStoreMethod(t *testing.T) {
	t.Parallel()

	envelope := reflect.TypeOf(sessionstore.Envelope{})
	store := reflect.TypeOf(&sessionstore.Store{})
	bearing := map[string]bool{}
	for i := range store.NumMethod() {
		method := store.Method(i)
		if resultsReach(method.Type, envelope) {
			bearing[method.Name] = true
		}
	}
	if len(bearing) == 0 {
		t.Fatal("no method of *sessionstore.Store was found to carry an Envelope, so this derivation reads nothing")
	}
	if !bearing["ReadRuntimeJournal"] {
		t.Errorf("the derivation did not find ReadRuntimeJournal, which returns raw stored records; it found %v",
			slices.Sorted(maps.Keys(bearing)))
	}
	// The public projection is NOT private-bearing, or the rule would forbid
	// the read this task is built on and would be reporting the wrong thing.
	if bearing["ReadPublicJournal"] {
		t.Error("the derivation calls ReadPublicJournal private-bearing, so it is not distinguishing the two projections")
	}
	subject := reflect.TypeOf((*SessionReader)(nil)).Elem()
	if subject.NumMethod() == 0 {
		t.Fatal("SessionReader declares no methods, so examining it proves nothing")
	}
	for i := range subject.NumMethod() {
		if name := subject.Method(i).Name; bearing[name] {
			t.Errorf("SessionReader declares %s, which can return a stored Envelope; the read plane would be able to publish private records", name)
		}
	}
}

// resultsReach reports whether a function type's results can transitively hold
// a value of target. It descends INTO named struct and interface types, which
// is the difference from the shallow walk the storage-primitive guard uses: an
// Envelope is never a result on its own, it is a member of one.
func resultsReach(signature reflect.Type, target reflect.Type) bool {
	seen := map[reflect.Type]bool{}
	var walk func(reflect.Type) bool
	walk = func(typ reflect.Type) bool {
		if typ == nil || seen[typ] {
			return false
		}
		seen[typ] = true
		if typ == target {
			return true
		}
		switch typ.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Array, reflect.Chan:
			return walk(typ.Elem())
		case reflect.Map:
			return walk(typ.Key()) || walk(typ.Elem())
		case reflect.Struct:
			for i := range typ.NumField() {
				if walk(typ.Field(i).Type) {
					return true
				}
			}
		case reflect.Interface:
			for i := range typ.NumMethod() {
				if walk(typ.Method(i).Type) {
					return true
				}
			}
		case reflect.Func:
			for i := range typ.NumIn() {
				if walk(typ.In(i)) {
					return true
				}
			}
			for i := range typ.NumOut() {
				if walk(typ.Out(i)) {
					return true
				}
			}
		}
		return false
	}
	for i := range signature.NumOut() {
		if walk(signature.Out(i)) {
			return true
		}
	}
	return false
}

// TestTheReadPlaneNamesOnlyCoreAndSessionStore is the other half of step 6's
// "without importing Harness".
//
// The module-level rule is absolute and lives in import_boundary_test.go: no
// file in this module, production or test, may import github.com/looprig/harness
// at all. That is the real enforcement and it is proved there in both
// directions. What THIS adds is the seam's own statement of it, in the form the
// seam can carry: the read plane's signatures name Looprig types from core,
// sessionstore and factory and from nowhere else, so there is no third
// vocabulary a body could be decoded into or re-encoded from.
func TestTheReadPlaneNamesOnlyCoreAndSessionStore(t *testing.T) {
	t.Parallel()

	permitted := []string{
		"github.com/looprig/core",
		"github.com/looprig/sessionstore",
		"github.com/looprig/factory",
	}
	insidePermitted := func(path string) bool {
		for _, module := range permitted {
			if path == module || strings.HasPrefix(path, module+"/") {
				return true
			}
		}
		return false
	}
	subjects := map[string]reflect.Type{
		"SessionReader": reflect.TypeOf((*SessionReader)(nil)).Elem(),
		"Directory":     reflect.TypeOf((*Directory)(nil)).Elem(),
	}
	looprig := 0
	for name, subject := range subjects {
		for _, named := range namedTypesReachableFrom(subject) {
			path := named.PkgPath()
			if !strings.HasPrefix(path, "github.com/looprig/") {
				continue
			}
			looprig++
			if !insidePermitted(path) {
				t.Errorf("%s names %s.%s, which is outside Core, SessionStore and Factory", name, path, named.Name())
			}
		}
	}
	if looprig == 0 {
		t.Fatal("the seams name no Looprig type at all, so this rule read nothing")
	}
	// The rule fires for a module that is genuinely outside the set, driven at
	// a probe rather than assumed. Harness itself cannot be the probe: this
	// module may not import it, which is the point.
	probe := reflect.TypeOf((*foreignModuleSeam)(nil)).Elem()
	outside := 0
	for _, named := range namedTypesReachableFrom(probe) {
		if path := named.PkgPath(); strings.HasPrefix(path, "github.com/looprig/") && !insidePermitted(path) {
			outside++
		}
	}
	if outside == 0 {
		t.Error("the rule reported nothing for a seam naming a type outside the permitted set")
	}
}

// foreignModuleSeam is a seam naming a Looprig type from outside the permitted
// set. It is declared here and implemented nowhere; its only purpose is to be
// reported.
type foreignModuleSeam interface {
	Due(ctx context.Context) (storage.Due, error)
}

// TestAKeyspaceFaultIsNotAMissingSession is the other direction of the absence
// mapping, and it is what keeps the new branch narrow.
//
// sessionstore's KeyspaceError carries ten codes AT v0.9.0, and exactly one of
// them -- binding_not_found -- means "there is no such session". The count and
// the list below are hand-written and do not maintain themselves; what fails
// when the vocabulary grows is internal/command's
// TestTheStoreErrorVocabularyHasNotGrownSinceTheAbsenceSetWasDerived, which is
// the tripwire for this list and the two others like it. The rest are the
// deployment disagreeing with itself: a witness that does not match its key, a
// marker that cannot be read, a store opened under the wrong layout. Reporting
// any of those as a 404 would tell a client its session is gone when the truth
// is that Factory cannot read the keyspace at all.
func TestAKeyspaceFaultIsNotAMissingSession(t *testing.T) {
	t.Parallel()

	for _, code := range []sessionstore.KeyspaceErrorCode{
		sessionstore.KeyspaceBackend,
		sessionstore.KeyspaceMarkerMalformed,
		sessionstore.KeyspaceLayoutMismatch,
		sessionstore.KeyspaceMarkerAmbiguous,
		sessionstore.KeyspaceBindingAmbiguous,
		sessionstore.KeyspaceScopeInvalid,
		sessionstore.KeyspaceHashCollision,
		sessionstore.KeyspaceLegacyTenant,
		sessionstore.KeyspaceLegacySession,
	} {
		f := newFixture(t, withSessions())
		f.reads.absent = func() error { return &sessionstore.KeyspaceError{Code: code} }
		recorder := f.get(statusTarget(fixtureSession))
		if recorder.Code != http.StatusInternalServerError {
			t.Errorf("keyspace %q answered %d, want 500; body was %q", code, recorder.Code, recorder.Body)
		}
	}
	// The control: the one code that DOES mean absence still answers 404, so
	// the sweep above is separating the codes rather than answering 500 for
	// every keyspace error.
	f := newFixture(t, withSessions())
	f.reads.absent = layoutMultiTenant.absence
	if recorder := f.get(statusTarget(fixtureSession)); recorder.Code != http.StatusNotFound {
		t.Errorf("binding_not_found answered %d, want 404", recorder.Code)
	}
}

// withStaleCatalogTip makes the catalog record's durable journal sequence
// DISAGREE with the journal, which is an ordinary state rather than a corrupt
// one: a Host commits a journal record and updates the catalog in two separate
// durable writes, so between them the record is behind.
func withStaleCatalogTip(tenant sessionwire.TenantID, session sessionwire.SessionID, seq uint64) fixtureOption {
	return func(_ *RouterConfig, f *fixture) {
		key := storedSession{tenant: tenant, session: session}
		record := f.reads.records[key]
		record.LastJournalSeq = seq
		f.reads.records[key] = record
	}
}

// TestTheTailIsAnchoredOnTheStoresTipAndNotTheCatalogsRecord ensures Tail does
// not use the catalog's independently updated journal summary.
//
// The catalog record is right there in the resolved entry and carries a
// LastJournalSeq, so anchoring the tail on it would cost no extra read at all.
// The reason not to is that it is a SEPARATE durable write, and a fixture whose
// two tips always agree cannot see the difference -- which is exactly the shape
// of an argument written in a comment with nothing reading it. So the two are
// made to disagree, in both directions.
func TestTheTailIsAnchoredOnTheStoresTipAndNotTheCatalogsRecord(t *testing.T) {
	t.Parallel()

	t.Run("a catalog that is behind still yields the newest events", func(t *testing.T) {
		t.Parallel()

		// The Host has committed 3000 records and updated the catalog through
		// 40. An anchor on the record would place the window at 1..40 and
		// report the session's oldest history as its newest.
		f := newFixture(t, withSessions(),
			withJournal(fixtureTenant, fixtureSession, longJournal(3000)...),
			withStaleCatalogTip(fixtureTenant, fixtureSession, 40))
		page := decodeJournalPage(t, f.get(journalTarget(fixtureSession)))
		if page.CapturedTip != 3000 {
			t.Fatalf("journal_tip = %d, want the store's 3000", page.CapturedTip)
		}
		seqs := eventSequences(page)
		if len(seqs) == 0 {
			t.Fatal("the tail was empty")
		}
		if seqs[len(seqs)-1] != 2999 {
			t.Errorf("the tail ends at %d, want the journal's last public record 2999", seqs[len(seqs)-1])
		}
		if seqs[0] < 2900 {
			t.Errorf("the tail begins at %d, which is anchored on the stale catalog rather than the store's tip", seqs[0])
		}
	})

	t.Run("a catalog that is ahead still yields a non-empty tail", func(t *testing.T) {
		t.Parallel()

		// The other direction, and the worse one: an anchor above the tip puts
		// the whole window past the end of the journal, and the store answers
		// a walk starting past its captured tip with nothing at all. A live
		// session would report an empty history.
		f := newFixture(t, withSessions(),
			withJournal(fixtureTenant, fixtureSession, longJournal(120)...),
			withStaleCatalogTip(fixtureTenant, fixtureSession, 9_000_000))
		page := decodeJournalPage(t, f.get(journalTarget(fixtureSession)))
		if page.CapturedTip != 120 {
			t.Fatalf("journal_tip = %d, want the store's 120", page.CapturedTip)
		}
		if len(page.Events) == 0 {
			t.Fatal("a session with 120 durable records answered with no history at all")
		}
		if page.CoveredThrough != 120 {
			t.Errorf("covered_through = %d, want 120", page.CoveredThrough)
		}
	})
}

// TestAJournalFailureMapsToOneStablePublicAnswer drives journalFailure over its
// whole input space, as a unit.
//
// It is a unit test rather than an HTTP one because two of its branches are not
// reachable through the composed chain today: serveRoute resolves the session
// before the handler runs, so a journal read meeting a session that is not
// there requires the session to be deleted BETWEEN the two reads. That race is
// real -- it is two durable reads and nothing holds a lock across them -- and it
// is the reason the absence answer is delegated to catalogFailure rather than
// collapsed into a fault. Delegation with no reader is a line free to be wrong,
// which is what this is.
//
// The space is derived: every JournalErrorCode the released store declares,
// every way it says "there is no such session", both ways a context ends, and
// an error of no type at all.
func TestAJournalFailureMapsToOneStablePublicAnswer(t *testing.T) {
	t.Parallel()

	// Every code sessionstore v0.1.0 declares. Only the cursor is the caller's;
	// the rest describe the journal or this package's own request, and a
	// caller can do nothing with either.
	journalCodes := map[sessionstore.JournalErrorCode]int{
		sessionstore.JournalErrorInvalid:   http.StatusInternalServerError,
		sessionstore.JournalErrorLeaseHeld: http.StatusInternalServerError,
		sessionstore.JournalErrorLeaseLost: http.StatusInternalServerError,
		sessionstore.JournalErrorFenced:    http.StatusInternalServerError,
		sessionstore.JournalErrorUnknown:   http.StatusInternalServerError,
		sessionstore.JournalErrorClosed:    http.StatusInternalServerError,
		sessionstore.JournalErrorBackend:   http.StatusInternalServerError,
		sessionstore.JournalErrorIntegrity: http.StatusInternalServerError,
		sessionstore.JournalErrorTooLarge:  http.StatusInternalServerError,
		sessionstore.JournalErrorCursor:    http.StatusBadRequest,
	}
	refused := 0
	for code, want := range journalCodes {
		got := journalFailure(&sessionstore.JournalError{Code: code, Field: "probe"})
		if got.status != want {
			t.Errorf("journal %q mapped to %d, want %d", code, got.status, want)
		}
		if want == http.StatusBadRequest {
			refused++
			if got.code != sessionwire.ErrorCodeInvalidRequest {
				t.Errorf("journal %q carried code %q", code, got.code)
			}
		}
	}
	if refused == 0 {
		t.Fatal("no journal code maps to a caller's refusal, so the split is vacuous")
	}
	// Absence, delegated. This is the branch the delegation exists for, and
	// without it a session deleted between the catalog read and the journal
	// read is reported as a Factory fault.
	for _, layout := range absenceLayouts() {
		got := journalFailure(layout.absence())
		if got != sessionNotFound() {
			t.Errorf("%s mapped to %+v, want the one sessionNotFound construction", layout.name, got)
		}
	}
	// A closing store reaches the retryable answer THROUGH the delegation.
	// There is no storeUnavailable arm in journalFailure, deliberately, so
	// this is the reader for the fact that the delegation is what carries it.
	closing := journalFailure(&sessionstore.StoreClosedError{})
	if closing.status != http.StatusServiceUnavailable || closing.code != ErrorCodeUnavailable {
		t.Errorf("a closing store mapped to %+v, want the retryable 503", closing)
	}
	// A keyspace fault is still a fault, so the delegation did not widen the
	// 404 on its way through.
	if got := journalFailure(&sessionstore.KeyspaceError{Code: sessionstore.KeyspaceHashCollision}); got != internalFailure() {
		t.Errorf("a hash collision mapped to %+v, want the internal fault", got)
	}
	// A context that ended keeps its own answer, ahead of every typed error.
	if got := journalFailure(context.Canceled); got.status != statusClientClosedRequest {
		t.Errorf("a cancelled read mapped to %d, want %d", got.status, statusClientClosedRequest)
	}
	if got := journalFailure(context.DeadlineExceeded); got.status != http.StatusGatewayTimeout {
		t.Errorf("an expired deadline mapped to %d, want 504", got.status)
	}
	// An error of no type this package knows is a fault by default, not a 404.
	if got := journalFailure(errors.New("the backend is confused")); got != internalFailure() {
		t.Errorf("an untyped error mapped to %+v, want the internal fault", got)
	}
}

// TestAnEmptyCursorDoesNotBecomeAReplayFromTheFirstRecord is M1's own reader,
// separate from the position table because what matters is not only the status
// but WHICH history an accepted empty cursor would have served.
//
// A table row asserting 400 dies if the refusal is removed, but it says nothing
// about the failure that made the refusal necessary. This drives the three
// shapes side by side, so the record is what the answers actually were.
func TestAnEmptyCursorDoesNotBecomeAReplayFromTheFirstRecord(t *testing.T) {
	t.Parallel()

	journal := func() fixtureOption {
		return withJournal(fixtureTenant, fixtureSession, longJournal(3000)...)
	}
	// The refusal.
	empty := newFixture(t, withSessions(), journal()).get(journalTarget(fixtureSession) + "?cursor=")
	if empty.Code != http.StatusBadRequest {
		t.Fatalf("an empty cursor answered %d, want 400; body was %q", empty.Code, empty.Body)
	}
	if code := decodeEnvelope(t, empty).Error.Code; code != sessionwire.ErrorCodeInvalidRequest {
		t.Errorf("code = %q, want %q", code, sessionwire.ErrorCodeInvalidRequest)
	}
	// It is refused BEFORE the store, like every other malformed position.
	refused := newFixture(t, withSessions(), journal())
	refused.get(journalTarget(fixtureSession) + "?cursor=")
	if reads := refused.journalSnapshot(); len(reads) != 0 {
		t.Errorf("an empty cursor reached the store as %+v", reads)
	}
	// And the answer it is NOT: naming no cursor at all is the tail. This is
	// the comparison the refusal exists to prevent collapsing -- an accepted
	// empty cursor served this route's oldest events under a parameter that
	// means "continue from where I was".
	tail := decodeJournalPage(t, newFixture(t, withSessions(), journal()).get(journalTarget(fixtureSession)))
	seqs := eventSequences(tail)
	if len(seqs) == 0 || seqs[0] < 2900 {
		t.Fatalf("the unpositioned control began at %v, so it is not a tail", seqs)
	}
	// "Every parameter answers an empty value the same way" is NOT asserted
	// here any more, and the reason is worth the paragraph. It used to be a
	// hard-coded {"cursor", "from_seq", "limit"} driven at this route plus one
	// "?limit=" at the tenant list: four of the surface's five (route,
	// parameter) pairs, and the pair it omitted -- the tenant list's cursor --
	// was precisely the one that was not refused at all. A fixed fixture cannot
	// defend a "for all X" property; the enumeration has to come from the
	// mechanism. TestEveryParameterOnEveryRouteRefusesAnEmptyValue derives the
	// pairs by parsing the production files and asserts the guard's own MESSAGE
	// on each, which is what a status-only probe could not do for the two
	// parameters an empty value fails downstream anyway.
}

// TestADrainingStoreIsRetryableRatherThanAFault is M2's reader.
//
// SessionStore refuses every public read with *StoreClosedError once Close has
// begun. It is a bare struct that wraps nothing and carries no code, so it
// matches none of the typed arms and used to fall to the internal fault: every
// read on a draining replica answered 500 with retryable:false, which tells a
// client not to retry at exactly the moment another replica would serve it.
func TestADrainingStoreIsRetryableRatherThanAFault(t *testing.T) {
	t.Parallel()

	for route, target := range coldReadTargets(fixtureSession) {
		t.Run(route, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t, withSessions(), withJournal(fixtureTenant, fixtureSession, longJournal(20)...))
			f.reads.fail = &sessionstore.StoreClosedError{}
			recorder := f.get(target)
			if recorder.Code != http.StatusServiceUnavailable {
				t.Fatalf("a draining store answered %d, want 503; body was %q", recorder.Code, recorder.Body)
			}
			envelope := decodeEnvelope(t, recorder)
			if envelope.Error.Code != ErrorCodeUnavailable {
				t.Errorf("code = %q, want %q", envelope.Error.Code, ErrorCodeUnavailable)
			}
			if !envelope.Error.Retryable {
				t.Error("a draining store was reported as not retryable, so a client is told to stop asking")
			}
		})
	}
	// The tenant list and the agent list reach it through their own mappings,
	// which are separate functions: the rule is the surface's, not one route's.
	t.Run("the tenant session list", func(t *testing.T) {
		t.Parallel()

		f := newFixture(t)
		f.reads.fail = &sessionstore.StoreClosedError{}
		if recorder := f.get("/v1/sessions"); recorder.Code != http.StatusServiceUnavailable {
			t.Errorf("the session list answered %d, want 503", recorder.Code)
		}
	})
	t.Run("the agent list", func(t *testing.T) {
		t.Parallel()

		f := newFixture(t, withDepartment(pooledTemplate), withDirectoryFailure(&sessionstore.StoreClosedError{}))
		if recorder := f.get("/v1/agents"); recorder.Code != http.StatusServiceUnavailable {
			t.Errorf("the agent list answered %d, want 503", recorder.Code)
		}
	})
	// The journal error follows a successful catalog lookup.
	t.Run("the journal tail after resolving the session", func(t *testing.T) {
		t.Parallel()

		f := newFixture(t, withSessions(), withJournal(fixtureTenant, fixtureSession, longJournal(20)...))
		f.reads.journalFail = &sessionstore.StoreClosedError{}
		recorder := f.get(journalTarget(fixtureSession))
		if recorder.Code != http.StatusServiceUnavailable {
			t.Fatalf("the journal tail answered %d, want 503; body was %q", recorder.Code, recorder.Body)
		}
		if reads := f.journalSnapshot(); len(reads) != 1 {
			t.Errorf("the tail failure was reached after %d journal reads, want 1", len(reads))
		}
	})
	// The control: a draining store is separated from a fault rather than
	// every failure becoming 503.
	f := newFixture(t, withSessions())
	f.reads.fail = &sessionstore.CatalogError{Code: sessionstore.CatalogErrorBackend, Field: "get"}
	if recorder := f.get(statusTarget(fixtureSession)); recorder.Code != http.StatusInternalServerError {
		t.Errorf("a backend fault answered %d, want 500", recorder.Code)
	}
}

// TestTheTailReadFailureIsAnsweredRatherThanCarriedForward checks both the
// error mapping and that a failed Tail request is not retried as another read.
func TestTheTailReadFailureIsAnsweredRatherThanCarriedForward(t *testing.T) {
	t.Parallel()
	f := newFixture(t, withSessions(), withJournal(fixtureTenant, fixtureSession, longJournal(20)...))
	f.reads.journalFail = &sessionstore.StoreClosedError{}
	recorder := f.get(journalTarget(fixtureSession))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("failed tail answered %d, want 503; body=%q", recorder.Code, recorder.Body)
	}
	if code := decodeEnvelope(t, recorder).Error.Code; code != ErrorCodeUnavailable {
		t.Errorf("code = %q, want %q", code, ErrorCodeUnavailable)
	}
	if reads := f.journalSnapshot(); len(reads) != 1 {
		t.Errorf("request made %d journal reads, want 1", len(reads))
	}
	cursor := newFixture(t, withSessions(), withJournal(fixtureTenant, fixtureSession, longJournal(20)...))
	cursor.reads.journalFail = &sessionstore.JournalError{Code: sessionstore.JournalErrorCursor, Field: "cursor"}
	if got := cursor.get(journalTarget(fixtureSession)); got.Code != http.StatusBadRequest {
		t.Errorf("refused cursor answered %d, want 400", got.Code)
	}
}

// TestAGateReadFailureIsAnsweredRatherThanSwallowed closes the branch two
// mutations survived.
//
// serveSessionGates makes the SECOND durable read of its request, and the fake
// used to carry one global failure lever that resolveSession's catalog read
// consumed first -- so nothing could fail the gate read, and both "map it
// through internalFailure instead of catalogFailure" and "ignore the error and
// answer 200 with a zero page" survived the whole suite. The second is the one
// that matters: a session blocked on a gate whose gate read faults would report
// no open gates at all, which a client cannot tell from a session that has
// none.
func TestAGateReadFailureIsAnsweredRatherThanSwallowed(t *testing.T) {
	t.Parallel()

	const secret = "postgres://user:hunter2@db.internal:5432/sessions"
	record := coldRecord(sessionwire.SessionStateWaitingOnGate, sessionwire.SessionResidencyCold)
	record.LastJournalSeq = 40
	record.OpenGates = []sessionwire.GateProjection{openGate("gate-a", 3)}

	t.Run("a backend fault is a fault, redacted", func(t *testing.T) {
		t.Parallel()

		f := newFixture(t, withSessions(), withRecord(fixtureTenant, fixtureSession, record))
		f.reads.gateFail = &sessionstore.CatalogError{Code: sessionstore.CatalogErrorBackend, Field: secret}
		recorder := f.get(gatesTarget(fixtureSession))
		if recorder.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500; body was %q", recorder.Code, recorder.Body)
		}
		// The swallowing mutation's signature: a 200 whose body says there are
		// no gates. Asserting the status alone would not separate them if the
		// mapping ever moved.
		if strings.Contains(recorder.Body.String(), `"gates"`) {
			t.Errorf("a failed gate read answered with a gate page: %q", recorder.Body)
		}
		if strings.Contains(recorder.Body.String(), "hunter2") || strings.Contains(recorder.Body.String(), "db.internal") {
			t.Errorf("the body carries a dependency's diagnostic: %q", recorder.Body)
		}
	})

	t.Run("absence is the one 404 construction", func(t *testing.T) {
		t.Parallel()

		// The session resolves -- the catalog read succeeds -- and the GATE
		// read then reports it gone, which is the deleted-between-two-reads
		// race. Every way the store says so must produce the same answer as a
		// session that was never there.
		for _, layout := range absenceLayouts() {
			f := newFixture(t, withSessions(), withRecord(fixtureTenant, fixtureSession, record))
			f.reads.gateFail = layout.absence()
			got := f.get(gatesTarget(fixtureSession))

			absent := newFixture(t, withSessions(), withStoreLayout(layout))
			want := absent.get(gatesTarget(fixtureSession))
			if got.Code != http.StatusNotFound {
				t.Fatalf("%s: the gate read answered %d, want 404; body was %q", layout.name, got.Code, got.Body)
			}
			if diff := responseDifference(got, want); diff != "" {
				t.Errorf("%s: a session that vanished between the two reads is distinguishable from one that never existed: %s",
					layout.name, diff)
			}
		}
	})

	t.Run("a draining store is still retryable here", func(t *testing.T) {
		t.Parallel()

		f := newFixture(t, withSessions(), withRecord(fixtureTenant, fixtureSession, record))
		f.reads.gateFail = &sessionstore.StoreClosedError{}
		if recorder := f.get(gatesTarget(fixtureSession)); recorder.Code != http.StatusServiceUnavailable {
			t.Errorf("status = %d, want 503", recorder.Code)
		}
	})

	// The control: with no lever set the same fixture serves the gate, so the
	// refusals above are caused by the lever rather than by the fixture.
	f := newFixture(t, withSessions(), withRecord(fixtureTenant, fixtureSession, record))
	page := decodeGatePage(t, f.get(gatesTarget(fixtureSession)))
	if len(page.Gates) != 1 {
		t.Fatalf("the control served %d gates, want 1", len(page.Gates))
	}
}
