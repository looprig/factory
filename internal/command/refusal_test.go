package command_test

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/command"
	"github.com/looprig/sessionstore"
)

// refusalStatuses is the restatement the authority is measured against.
//
// It is written as ABSOLUTE LITERALS for the reason TestTheKindsAreTheDurableSpellings
// gives: every other reader names the function, so a table whose values moved
// would move every reader with it and nothing would fail. These are what a
// client's own retry logic branches on.
func refusalStatuses() map[sessionwire.ErrorCode]int {
	return map[sessionwire.ErrorCode]int{
		sessionwire.ErrorCodeInvalidRequest:     400,
		sessionwire.ErrorCodeSessionNotFound:    404,
		sessionwire.ErrorCodeCommandRejected:    409,
		sessionwire.ErrorCodeGateResolved:       409,
		sessionwire.ErrorCodeGateNotResumable:   409,
		sessionwire.ErrorCodeGateExpired:        409,
		sessionwire.ErrorCodeRuntimeUnavailable: 422,
	}
}

// TestEveryClassifiedRefusalHasTheStatusTheRestatementRequires pins the table
// in BOTH directions: a code the authority classifies that the restatement does
// not expect is as much a divergence as one it fails to classify.
func TestEveryClassifiedRefusalHasTheStatusTheRestatementRequires(t *testing.T) {
	t.Parallel()

	want := refusalStatuses()
	if len(want) == 0 {
		t.Fatal("nothing is expected, so this comparison is vacuous")
	}
	for code, status := range want {
		got, ok := command.RefusalStatus(code)
		if !ok {
			t.Errorf("%q is not classified by the refusal authority", code)
			continue
		}
		if got != status {
			t.Errorf("%q answers %d, want %d", code, got, status)
		}
	}
	for _, code := range command.RefusalCodes() {
		if _, expected := want[code]; !expected {
			t.Errorf("the authority classifies %q, which the restatement does not expect", code)
		}
	}
}

// TestNoClassifiedRefusalIsAdvertisedRetryable is the reader for the
// A3.3-retryable open item.
//
// retryable:true is a promise that repeating the IDENTICAL bytes could succeed
// with nothing else changing. No refusal admission mints is one -- and
// runtime_unavailable is the one that had to be settled rather than assumed,
// because the obvious 503 mapping is retryable and would tell a client to
// hammer a misconfigured deployment. The ClientLink answers every one of these
// retryable:false, so a status in the retryable set here would ALSO make the
// two edges disagree about the identical refusal.
//
// It is a for-all over the authority's own code set rather than over a list, so
// a code added there without a status is reported by the test above and a code
// added WITH a retryable status is reported here.
func TestNoClassifiedRefusalIsAdvertisedRetryable(t *testing.T) {
	t.Parallel()

	codes := command.RefusalCodes()
	if len(codes) == 0 {
		t.Fatal("the authority classifies nothing, so this sweep is vacuous")
	}
	for _, code := range codes {
		status, ok := command.RefusalStatus(code)
		if !ok {
			t.Errorf("%q is listed by RefusalCodes and not classified by RefusalStatus", code)
			continue
		}
		if command.RetryableStatus(status) {
			t.Errorf("%q answers %d, which is advertised retryable; repeating the identical request cannot change this answer", code, status)
		}
		if status < 400 || status > 499 {
			t.Errorf("%q answers %d; a classified refusal is a decision about the caller's command and belongs in 4xx", code, status)
		}
	}
}

// TestRuntimeUnavailableIsNotTheRetryable503 names the specific mapping the
// open item warned about, so the sweep above cannot be satisfied by a table
// that merely happens to avoid it today.
func TestRuntimeUnavailableIsNotTheRetryable503(t *testing.T) {
	t.Parallel()

	status, ok := command.RefusalStatus(sessionwire.ErrorCodeRuntimeUnavailable)
	if !ok {
		t.Fatal("runtime_unavailable is not classified")
	}
	if status == http.StatusServiceUnavailable {
		t.Fatal("runtime_unavailable maps to 503: admission mints it for an unresolvable target, " +
			"an oversized payload and a missing create reservation as well as for a transient " +
			"directory read, so the code cannot distinguish its one transient cause from its permanent ones")
	}
}

// TestTheRetryableRuleCanSayTrue is the positive control for both sweeps above.
//
// Both are negative claims -- "no refusal is retryable" -- and a RetryableStatus
// that returned false for everything would satisfy them while telling every
// caller of the REST plane not to retry a dependency outage. The three statuses
// here are the ones that name a condition the REQUEST did not cause.
func TestTheRetryableRuleCanSayTrue(t *testing.T) {
	t.Parallel()

	for _, status := range []int{http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout} {
		if !command.RetryableStatus(status) {
			t.Errorf("RetryableStatus(%d) = false; a dependency condition the caller did not cause is retryable", status)
		}
	}
	for _, status := range []int{200, 400, 404, 409, 422, 499, 500} {
		if command.RetryableStatus(status) {
			t.Errorf("RetryableStatus(%d) = true", status)
		}
	}
}

// TestACodeTheAuthorityDoesNotClassifyFailsClosed holds the second return.
// An edge that read the status alone would answer a code it has no ruling for
// with whatever the zero value happened to be.
func TestACodeTheAuthorityDoesNotClassifyFailsClosed(t *testing.T) {
	t.Parallel()

	for _, code := range []sessionwire.ErrorCode{
		"", "not_a_code", sessionwire.ErrorCodeGateResponseInvalid, sessionwire.ErrorCodeUnsupportedVersion,
	} {
		if _, ok := command.RefusalStatus(code); ok {
			t.Errorf("%q is classified; admission mints no such refusal, so a status for it is a ruling nobody made", code)
		}
	}
}

// ---------------------------------------------------------------------------
// The absence authority.
// ---------------------------------------------------------------------------

// TestEveryStoreSpellingOfAbsenceIsOneAnswer is the reader for the
// A9.1-notfound carry-forward.
//
// The released store has FOUR ways of saying "there is no such session here" AT
// v0.8.0, and they must be one public fact: a session in another tenant, a
// deleted one and one that never existed have to be indistinguishable, or a
// refusal discloses that an identifier the caller may not read is real
// somewhere else.
//
// Read the name narrowly: "every" is every spelling the pinned store HAS, not
// every spelling it may ever have. The four rows are hand-written, so a fifth
// arriving in a later release joins this test on the day somebody lists it --
// which is the honest formulation the module's own reflective guards already
// use. TestTheStoreErrorVocabularyHasNotGrownSinceTheAbsenceSetWasDerived is
// what makes that day arrive loudly instead of silently.
func TestEveryStoreSpellingOfAbsenceIsOneAnswer(t *testing.T) {
	t.Parallel()

	for name, err := range map[string]error{
		"the catalog's not-found":          &sessionstore.CatalogError{Code: sessionstore.CatalogErrorNotFound},
		"the catalog's deleted":            &sessionstore.CatalogError{Code: sessionstore.CatalogErrorDeleted},
		"the catalog's identity mismatch":  &sessionstore.CatalogError{Code: sessionstore.CatalogErrorIdentity},
		"the keyspace's binding-not-found": &sessionstore.KeyspaceError{Code: sessionstore.KeyspaceBindingNotFound},
		"a wrapped catalog not-found":      fmt.Errorf("read the entry: %w", &sessionstore.CatalogError{Code: sessionstore.CatalogErrorNotFound}),
	} {
		if !command.SessionAbsent(err) {
			t.Errorf("%s is not read as absence", name)
		}
	}
}

// TestAConditionThatIsNotAbsenceStaysAFault is the other direction, and it is
// what stops the predicate from being satisfied by "return true".
//
// Every row here is a condition the deployment has to hear about: a hash
// collision or a layout mismatch is the deployment disagreeing with itself, and
// answering it 404 would report a session absent because the store misbehaved.
func TestAConditionThatIsNotAbsenceStaysAFault(t *testing.T) {
	t.Parallel()

	for name, err := range map[string]error{
		"no error at all":                     nil,
		"an unclassified error":               errors.New("the backend is on fire"),
		"a cursor this session did not issue": &sessionstore.CatalogError{Code: sessionstore.CatalogErrorCursor},
		"a catalog backend outage":            &sessionstore.CatalogError{Code: sessionstore.CatalogErrorBackend},
		"a stale catalog epoch":               &sessionstore.CatalogError{Code: sessionstore.CatalogErrorEpoch},
		"a keyspace hash collision":           &sessionstore.KeyspaceError{Code: sessionstore.KeyspaceHashCollision},
		"an ambiguous binding":                &sessionstore.KeyspaceError{Code: sessionstore.KeyspaceBindingAmbiguous},
		"a keyspace backend outage":           &sessionstore.KeyspaceError{Code: sessionstore.KeyspaceBackend},
		"an inbox record not found":           &sessionstore.InboxError{Code: sessionstore.InboxErrorNotFound},
	} {
		if command.SessionAbsent(err) {
			t.Errorf("%s is read as an absent session", name)
		}
	}
}

// ---------------------------------------------------------------------------
// The record projection.
// ---------------------------------------------------------------------------

// TestEveryDurableInboxStateProjectsToOnePublicState drives ALL FIVE states the
// released inbox declares, because the two edges must not be able to describe
// one durable record differently.
func TestEveryDurableInboxStateProjectsToOnePublicState(t *testing.T) {
	t.Parallel()

	rejection := &sessionwire.ErrorDetail{Code: sessionwire.ErrorCodeCommandRejected, Message: "the host refused it"}
	for _, probe := range []struct {
		state sessionstore.InboxState
		want  sessionwire.CommandState
	}{
		{sessionstore.InboxStatePending, sessionwire.CommandStateAccepted},
		{sessionstore.InboxStateClaimed, sessionwire.CommandStateAccepted},
		{sessionstore.InboxStateApplying, sessionwire.CommandStateAccepted},
		{sessionstore.InboxStateApplied, sessionwire.CommandStateApplied},
		{sessionstore.InboxStateRejected, sessionwire.CommandStateRejected},
	} {
		record := sessionstore.InboxRecord{CommandID: "command-a", State: probe.state}
		if probe.state == sessionstore.InboxStateRejected {
			record.Rejection = rejection
		}
		status, ok := command.StatusFor(sessionstore.InboxEntry{Record: record, AcceptedOrder: 7})
		if !ok {
			t.Errorf("%q is not projected at all", probe.state)
			continue
		}
		if status.State != probe.want {
			t.Errorf("%q projects to %q, want %q", probe.state, status.State, probe.want)
		}
		if status.CommandID != "command-a" {
			t.Errorf("%q projects command id %q, want %q", probe.state, status.CommandID, "command-a")
		}
		if status.AcceptedOrder != 7 {
			t.Errorf("%q projects accepted order %d, want 7", probe.state, status.AcceptedOrder)
		}
		wantDetail := probe.state == sessionstore.InboxStateRejected
		if (status.Error != nil) != wantDetail {
			t.Errorf("%q projects error detail %v, want present = %t", probe.state, status.Error, wantDetail)
		}
	}
}

// TestAnUnknownDurableStateIsAFaultRatherThanAnAcceptance is the one case that
// must never be reported optimistically: a record whose state this build does
// not know is a store disagreeing with this build.
func TestAnUnknownDurableStateIsAFaultRatherThanAnAcceptance(t *testing.T) {
	t.Parallel()

	for _, state := range []sessionstore.InboxState{"", "settled", "PENDING"} {
		if _, ok := command.StatusFor(sessionstore.InboxEntry{
			Record: sessionstore.InboxRecord{CommandID: "command-a", State: state},
		}); ok {
			t.Errorf("state %q was projected; an unrecognised durable state must be a fault", state)
		}
	}
}

// ---------------------------------------------------------------------------
// The vocabulary tripwire.
// ---------------------------------------------------------------------------

// The pinned sessionstore, and the size of the two vocabularies SessionAbsent
// is an enumeration over.
//
// These are the numbers the absence set was derived against. They are stated as
// absolute literals for the reason every pinned-subject constant in this
// repository is: the whole point is to notice when the dependency's own answer
// moves.
const (
	pinnedSessionstoreModule  = "github.com/looprig/sessionstore"
	pinnedSessionstoreVersion = "v0.10.0"

	pinnedCatalogErrorCodes  = 14
	pinnedKeyspaceErrorCodes = 10
)

// TestTheStoreErrorVocabularyHasNotGrownSinceTheAbsenceSetWasDerived is the
// tripwire three hand-written lists needed and none of them had.
//
// # What it replaces, and why the replaced claim was false
//
// Three tests enumerate parts of sessionstore's error vocabulary by hand:
// TestEveryStoreSpellingOfAbsenceIsOneAnswer here, and internal/httpapi's
// catalog and keyspace mapping sweeps. One of them said its list was "derived
// from the constants ... so a code sessionstore adds and this mapping forgets
// lands on the DEFAULT". The premise is true and the CONSEQUENT is not: a code
// a later sessionstore adds does not join a hand-written slice, the sweep never
// drives it, and the suite stays green having observed nothing. Those tests
// cannot see vocabulary growth AT ALL.
//
// The lists themselves are fine -- a hand enumeration over a dependency's
// closed vocabulary is a reasonable choice, and reflecting over exported
// constants is worse -- so what is added here is the one thing they were
// missing: something that FAILS when the vocabulary grows. It is ONE tripwire
// for all three rather than one per package, because three parses of one fact
// would be three authorities for it.
//
// # What it can and cannot see
//
// It counts CONST DECLARATIONS of the two named types in the pinned module's
// production sources, parsed rather than grepped, so a comment mentioning a code
// is not a code and a reformatting does not move the count. It sees a code
// ADDED or REMOVED. It does not see one RENAMED or given a different string
// value at an unchanged count -- that is what the per-code rows in the three
// sweeps are for, and this is deliberately their complement rather than their
// replacement.
//
// If this fails, the instruction is: re-read all three lists against the new
// vocabulary and rule each added code, then move the constant. It is not to
// move the constant.
func TestTheStoreErrorVocabularyHasNotGrownSinceTheAbsenceSetWasDerived(t *testing.T) {
	t.Parallel()

	dir := pinnedSessionstoreDir(t)
	counts := map[string]int{}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	fset := token.NewFileSet()
	files := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, parseErr := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if parseErr != nil {
			t.Fatalf("parse %s: %v", name, parseErr)
		}
		files++
		for _, decl := range file.Decls {
			general, ok := decl.(*ast.GenDecl)
			if !ok || general.Tok != token.CONST {
				continue
			}
			// A const block states its type once, on the first spec, and the
			// rest inherit it -- so the declared type is carried forward
			// rather than read per spec, which is how a grep-shaped reader
			// undercounts a grouped declaration.
			declared := ""
			for _, spec := range general.Specs {
				value, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				if ident, ok := value.Type.(*ast.Ident); ok {
					declared = ident.Name
				} else if value.Type != nil {
					declared = ""
				}
				if declared == "CatalogErrorCode" || declared == "KeyspaceErrorCode" {
					counts[declared] += len(value.Names)
				}
			}
		}
	}
	if files == 0 {
		t.Fatal("no production file was parsed, so this count proves nothing")
	}
	for _, probe := range []struct {
		name string
		want int
	}{
		{"CatalogErrorCode", pinnedCatalogErrorCodes},
		{"KeyspaceErrorCode", pinnedKeyspaceErrorCodes},
	} {
		if counts[probe.name] != probe.want {
			t.Errorf("%s at %s declares %d codes, and the absence set plus internal/httpapi's two "+
				"mapping sweeps were written against %d. RE-READ ALL THREE LISTS and rule every added "+
				"code before moving this constant: a code nobody ruled lands on the fault default, "+
				"which is safe, and a code that should have meant absence would answer 500 for a "+
				"session that is not there",
				probe.name, pinnedSessionstoreVersion, counts[probe.name], probe.want)
		}
	}
}

// pinnedSessionstoreDir locates the pinned module's sources, and refuses to
// scan a version other than the one this file's constants were written
// against. A scan of the wrong copy of a dependency is worse than no scan.
func pinnedSessionstoreDir(t *testing.T) string {
	t.Helper()

	// internal/command -> module root. A test's working directory is its own
	// package directory, which go test guarantees.
	gomod, err := os.ReadFile(filepath.Join("..", "..", "go.mod"))
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	required := ""
	for _, line := range strings.Split(string(gomod), "\n") {
		if idx := strings.Index(line, "//"); idx >= 0 {
			line = line[:idx]
		}
		fields := strings.Fields(line)
		if len(fields) > 0 && fields[0] == "require" {
			fields = fields[1:]
		}
		if len(fields) == 2 && fields[0] == pinnedSessionstoreModule {
			required = fields[1]
			break
		}
	}
	if required != pinnedSessionstoreVersion {
		t.Fatalf("go.mod requires %s %q and this file's counts were taken at %q; recheck the vocabulary "+
			"at the version now pinned before moving the constants", pinnedSessionstoreModule, required, pinnedSessionstoreVersion)
	}
	out, err := exec.Command("go", "env", "GOMODCACHE").Output()
	if err != nil {
		t.Fatalf("go env GOMODCACHE: %v", err)
	}
	cache := strings.TrimSpace(string(out))
	if cache == "" {
		t.Fatal("go env GOMODCACHE is empty, so the pinned module cannot be located")
	}
	dir := filepath.Join(cache, pinnedSessionstoreModule+"@"+required)
	if entries, err := os.ReadDir(dir); err != nil || len(entries) == 0 {
		t.Fatalf("pinned sessionstore at %s is unreadable or empty (%v); a scan over nothing proves nothing", dir, err)
	}
	return dir
}
