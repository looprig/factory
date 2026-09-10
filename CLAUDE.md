# CLAUDE.md — factory

`factory` is the public-facing orchestration service: HTTP and WebSocket surface,
authentication and authorization, durable reads, command admission, target
selection, HostLink clients, multi-replica reconciliation, local realtime
fan-out, and optional UI composition.

## Dependency boundary

The boundary is enforced by `import_boundary_test.go`, not by convention. Five
rules, applied to every Go file in the module, production and test alike:

<!-- boundary-rules:begin -->
- `github.com/looprig/host` — forbidden **everywhere**.
- `github.com/looprig/harness` — forbidden **everywhere**.
- `github.com/centrifugal/...` — only under `internal/realtime`.
- `github.com/looprig/wui` — only under `cmd/factory`.
- `k8s.io/...` and `sigs.k8s.io/...` — only under `internal/placement/kubernetes`.
<!-- boundary-rules:end -->

Factory talks to Host **through Core and SessionStore records**. That is the
positive half of the first two rules and the reason they are absolute: there is
no Host type Factory could hold, because Factory cannot name one. `contract.go`
states it in code and `TestCrossServiceContractTypes` pins that every member of
the contract is a Core wire type.

Use exact released Core, Storage and SessionStore versions. No `replace`, no
vendoring. Verify standalone with `GOWORK=off go test ./...`; the workspace
`go.work` will otherwise mask a missing dependency.

`go.mod`'s Core and SessionStore requirements are held by real production
imports in `contract.go`, deliberately: a dependency named only in a guard's
string table is one `go mod tidy` away from being dropped.

## How the guard is written, and why

- **Rules are functions over parsed import paths.** Nothing matches against
  source text. A substring ban over source is defeated by a line break, a
  rename or an alias, and cannot tell an import from a comment mentioning one.
- **Containment compares whole segments.** `pathHasPrefix` is used for both
  import paths and directory scopes, so `internal/realtimefanout` is not inside
  `internal/realtime`, `github.com/looprig/hostage` is not inside
  `github.com/looprig/host`, and `cmd/factoryctl` does not inherit
  `cmd/factory`'s grant. It is differentially fuzzed against an independent
  segment-splitting oracle.
- **Scopes are subtrees, not lists.** A second package under
  `internal/realtime/` is inside the grant by design; a second binary beside
  `cmd/factory` is not. There is no hand-maintained package list to drift.
- **The scan fails loudly at zero files.** A guard that walks nothing is
  indistinguishable from a clean tree.
- **Scopes that do not exist yet are still proven.** None of the three scoped
  directories is built at scaffold time, so `TestScanReachesEveryRuleScope`
  constructs a fixture module containing a file at each scope, one directly
  outside it, and one at the root, drives the live enumerator and the live scan
  over it, and requires the in-scope files to pass and the others to be
  reported. Each probe carries an import the rule actually matches, so a pass
  is never explicable by the payload.
- **Both directions.** The case table sweeps ordinary Factory code — the
  standard library, Core, SessionStore, Storage, a router, a websocket library
  outside `internal/realtime` — as hard as the illegal shapes.
  `TestBoundaryCaseTableCoversEveryRule` floors on the set of RULES covered,
  requiring each to be driven to a rejection and each scoped rule to a
  permitted in-scope answer and a rejected out-of-scope one.

When you add a rule, add its cases, its `docToken`, and its entry in
`sampleImports`; the coverage assertion, the fuzz target and
`documented_rules_test.go` each fail if you do not. That last one holds the
`boundary-rules` blocks in this file and in `README.md` to the declared list, so
a sixth rule cannot leave the prose describing five. A rule scoped to
`""` must also have an entry in `forbiddenModules`, or
`TestEveryForbiddenModuleIsAnEverywhereImportRule` fails: go.mod may not
require what no file may import.

## The guard's subject is the MODULE, not the tree

`modfiles` stops descending at any directory holding `go.mod` or `.git`. The
skip is correct — a nested repository is not this module's content — but the
consequence is that **all five rules silently stop applying inside such a
subtree**. A reviewer proved it: `internal/httpapi/go.mod` beside a file
importing `github.com/looprig/host` left the import scan passing with the
offending file simply absent from its count. This
workspace already ships nested modules (`flow/store`, `pluto/cmd/pluto`), so a
later `factory/internal/testkit/go.mod` is not hypothetical.

`TestModuleHasNoUndeclaredNestedBoundary` closes it: every directory below the
root holding `go.mod`, `.git` or named `vendor` must appear in
`allowedNestedBoundaries` with a reason. **An entry there is not a hole** — an
allowed boundary is scanned in its own right by the same rules, so declaring
one buys an extra scan rather than an exemption. The root's own `.git` is
excluded, and that exclusion has its own fixture case so widening it to swallow
nested repositories fails.

**Nothing here restates `modfiles`. The detector consults it.**
`modfiles` decides visibility on five axes — which directories it refuses to
enter, which files it refuses to read, the boundary marker set, the `.go`
suffix, and a fail-closed symlink check — and anything hidden by any of them is
outside all five import rules *and* outside `make fmt-check`, which pipes the
same enumerator. The first version of this guard restated two axes as its own
literals and did not cover the other two, and it failed in both directions:

- **Too narrow.** Appending `|| name == "generated"` to the directory rule —
  one word, in an `internal/` helper nobody reviews as "the product" — hid
  every file under any `generated/` directory from all five rules and from
  `gofmt`, while this test stayed silent. Unlike `_x.go` or `testdata/`, a
  directory named `generated` **is compiled by Go**, so the hidden import was a
  real dependency of a real build.
- **Too wide.** It descended into `testdata/` and dot-prefixed directories that
  `modfiles` never enters, so it reported `.worktrees/feature-x` and
  `internal/modfiles/testdata/fixturemod` as boundaries. Both reports were
  false, and the prescribed remedy was harmful: declaring a `testdata` fixture
  module makes the guard scan a fixture whose whole purpose is to contain a
  forbidden import. `AGENTS.md` names `.worktrees/` as a structural path and
  this program's own workflow creates them, so that one would have broken
  `make check` on `main` the first time anyone added a worktree.

So `modfiles` exports `IgnoredDirectory`, `IgnoredFile` and `BoundaryMarkers`,
each returning a **reason**, and the detector stops descending exactly where
`modfiles` stops. Adding a sixth reason there cannot leave the detector behind.

**Sharing removes the disagreement; it does not narrow the shared answer** — a
widened shared predicate is a hole in both walks at once. So the sanctioned
answer is pinned separately, by an independent restatement written from the
justification rather than from the code
(`sanctionedIgnoredDirectory`/`sanctionedIgnoredFile`, deliberately spelled with
`strings.HasPrefix` rather than an index so a copy-paste cannot pass for
agreement). A table names the specific hazards a mutator would not invent —
`generated`, `zz_generated.go`, `node_modules`, `bazel-out` — and
`FuzzModfilesDecisionSurfaceMatchesTheSanctionedSet` generalises it. The
sanction is: `vendor`, `testdata`, and dot- or underscore-prefixed names, all of
which Go itself declines to build. **Nothing Go would compile may be hidden.**

`TestModuleFileSetMatchesAnIndependentEnumeration` compares `modfiles.Files`
against a re-derivation from the sanctioned rules, over the real tree and over a
fixture exercising every axis. It is the only assertion covering the `.go`
suffix axis: narrowing that test to exclude `_test.go` would hide every test
from the import rules, and nothing else would say so.

**Both markers are tested with `os.Lstat`, which is the same shape
`modfiles.nestedBoundary` uses, and that agreement is now structural.** `Lstat`
succeeds for a **file** as well as a directory, and both `git submodule add`
and `git worktree add` write `.git` as a file holding a `gitdir:` pointer. A
detector recognising only the directory form disagreed with the enumerator it
polices: a submodule under `factory/` made its whole subtree invisible to all
five rules with nothing failing — likelier in practice than the nested-`go.mod`
case. Do not reintroduce a second
implementation of "what is a boundary"; change `modfiles` and let the detector
follow, and widen the sanctioned restatement only with a justification of the
form "Go does not build this". A directory that is both a nested module and a
nested repository is reported twice, because removing one marker does not
restore the walk.

The `vendor` arm is **live, not fixture-only**. An *inconsistent* vendor tree
is refused by the toolchain first (`inconsistent vendoring`), but
`GOWORK=off go mod vendor` produces a **consistent** one: `go build ./...`
then succeeds and this test is what fails. That is exactly the workspace's
stated hazard — a real vendor tree silently consulted under `GOWORK=off`,
which is the one mode meant to verify a module against its true pins — so do
not weaken this arm on the belief that Go catches it for you.

## go.mod's shape is a test, not prose

`go mod verify` checks content hashes, not version shape: nothing in `make
check` would otherwise notice a `replace` directive or a pin moved to a
pseudo-version. `module_pin_test.go` parses go.mod's grammar — both the
`require x v1` and `require ( … )` spellings, **and with every token passed
through `strconv.Unquote`**, since a guard understanding only one spelling is
defeated by reformatting, the same failure mode as matching source text for
imports — and asserts:

- no `replace` directive, in either spelling;
- the module path is `github.com/looprig/factory`;
- every `github.com/looprig/*` requirement names its exact released version
  from `releasedLooprigVersions`;
- nothing in `forbiddenModules` is required at ANY version.

**`forbiddenModules` is consulted before `releasedLooprigVersions`, and that
order is the point.** The sibling `host` module rejects `factory` today because
there is no released version of it to name — so the ban is an accident of
absence that expires the day factory ships.
`TestForbiddenModulesAreRejectedBeforeTheVersionMap` drives the situation that
exposes it — Host present in the released map at the version named — and
asserts the message, not merely that something was reported.

The quoting case is not hypothetical decoration. go.mod's grammar permits
quoted tokens and the toolchain resolves them normally, so before the fix a
go.mod holding one ordinary Looprig require and a quoted
`github.com/looprig/host` passed this guard **entirely** — the requirement was
neither forbidden-checked, nor version-checked, nor counted — while `go build`
resolved Host as usual. `go mod tidy` normalises quotes away, but tidy is not
in `make check`, and "nobody would write that" is the premise this guard exists
not to rely on.

When a Looprig dependency moves, update `releasedLooprigVersions` in the same
change, and move the pin with `go get`, never `go mod tidy`.

## Where an interface goes

A narrow interface belongs to the package that CALLS it. `internal/httpapi`,
`internal/admission`, `internal/realtime/clientlink` and
`internal/realtime/hostlink` each declare theirs; there is no shared dependency
package, and adding one would make widening a dependency invisible at the place
that acquired it.

One constraint overrides that, and it is the language, not a preference: a
deployer implements `identity.Verifier` and `Authorizer` from OUTSIDE this module
and must be able to NAME every type in their signatures, so a public seam may
not mention a type from `internal/`. That is the whole reason `Principal` and
`CSRFConfig` live in the public `identity` package rather than in
`internal/identity` as runbook 05's file list anticipated; A1's derivation code
belongs beside them.

`internal/identity` is where that derivation code went: the `Authenticator` and
`OperationContext` have no external implementer, so neither is vocabulary a
deployer has to learn. `Credential`, `Claims`, `Source` and `Verifier` DO have
one -- A9.1 found that the router and the guard require the concrete internal
authenticator, so the seam a deployment implements is the verifier -- and they
moved to the public `identity` package, with aliases left behind so there is one
declaration rather than two. What A1.1 added to the PUBLIC package is
`ErrUnauthenticated` and `ErrCredentialExpired`. The expired sentinel wraps the
other, so an edge matching one classification still catches both; it is separate
because it is the one rejection a browser acts on without a human.

`server.go`'s public seams are therefore the UNION of the narrow ones, and
`TestPublicSeamsSatisfyTheirConsumers` plus
`TestPublicSeamsAreExactlyTheUnionOfTheirConsumers` hold them equal in both
directions. They are assignable with no adapter only because every method names
just standard-library, Core, SessionStore or `identity` types -- which is why
`Clock` is spelled with `AfterFunc` returning an unnamed `func() bool` rather
than a named `Timer`: a named type declared in each package would make the two
`Clock`s different interfaces across the `internal/` boundary.
`TestNoSeamNamesAStoragePrimitive` is the "SessionStore domain interface, not
raw Storage" rule made checkable, and it is driven against a probe so it cannot
pass by finding nothing.

`internal/realtime/hostlink.Dialer` is the one seam NOT on the public surface.
Its `Dial` returns a `hostlink.Link`, which no external package may name; that
is correct rather than an oversight, because the HostLink protocol is internal
and the boundary rules already confine its engine to `internal/realtime`. What
composition configures from outside is `HostLinkLimits`.

## Identity derivation reads no tenant carrier, and holds no state

Two properties of `internal/identity` are load-bearing, and both are held
structurally rather than by review.

**A tenant comes only from verified claims or from configuration.** The
derivation names none of `URL`, `RequestURI`, `Body`, `Form`, `PathValue`,
`Host` or their relatives, so there is nothing to "reject": a conflicting tenant
in a request is never read. `TestTenantComesOnlyFromVerifiedClaims` sweeps every
carrier a request offers, and `TestTheDerivationNamesNoTenantCarrier` scans the
PARSED file for the selectors themselves -- the behavioural sweep can only cover
the carriers somebody thought of, and the structural one is what survives a
carrier nobody did.

The structural scan enumerates the package's production files with
`os.ReadDir`, and that is not a stylistic choice. Pinned to `http.go`, it passed
unchanged when a second file in the package called `r.PathValue("tenant")` and
`r.URL.Query().Get("tenant")`; nothing else would have reported it, since
`import_boundary_test.go` walks imports rather than selectors. A1.2 and A1.3
both add files here. **A guard naming its own subject cannot fail for a subject
that did not exist when it was written.**

The default tenant is applied to an ACTOR only. A service credential is minted
by deployment configuration and can name its own scope, so one that names none
is misconfigured; inheriting the local default would hand an unscoped service
credential authority over whichever tenant a deployment happens to default to,
and a service principal is the identity authorized for the cross-tenant sweep.

**Which credential authenticated an operation is derived, never supplied.**
`Authenticator.NewOperationContext` is a method so the source comes from the
same `presented` the authentication used. It was a free function taking the
source as a parameter, and mutating that constructor to ignore its argument and
record `SourceBearer` for everything **survived the whole suite**, because every
call site happened to pass `SourceBearer`. That mutant is the CSRF-bypass bug:
an ambient cookie credential recorded as a bearer, and A1.3's guard skips --
silently, and in the direction that looks safe.
`TestTheOperationContextRecordsTheCredentialAuthenticationUsed` now compares the
recorded source against the credential the VERIFIER received, so the two ends of
the derivation are checked against each other rather than each against the same
expectation. `NewOperationContextWithSource` still exists for a caller that
authenticated by an unmodelled route; its doc says what the method prevents.
**A parameter is a place a caller can be wrong; a derivation is not.**

**An Authenticator holds no per-process state.** That is spec section 10 --
authentication state is stateless or shared durably, never held in one Factory
process -- and it is why the seam is a `Verifier` rather than a session table.
`TestAuthenticatorDeclaresNoPerProcessState` is the structural half, and it is
the half a behavioural test cannot supply: a per-process cache is invisible to a
two-replica test in every case where the cache misses. What it establishes is
bounded -- state reachable THROUGH an injected seam is the deployer's.

**No auth material reaches an error or a log, with two stated limits.** The
single place foreign text enters is the verifier's error: its TEXT is kept and
its VALUE dropped, so no unwrapping reaches a message that did not go through
`redactSecrets`. But `redactSecrets` is string equality, so it removes a
VERBATIM credential and nothing else -- a verifier embedding
`c.Value()[:16]` puts sixty-four bits of the token into an error the caller will
log, and no scrubber working on a finished string could catch it. That is
`TestScrubbingCoversOnlyAVerbatimCredential`, asserted from BOTH sides so the
limit is measured rather than promised. The alternative is discarding the
verifier's message, which costs the operator "bad signature" against "issuer
unreachable".

`Credential` redacts under `%v`/`%s`/`%q`, `%#v`, `slog` and `encoding/json`.
These are **not four independent mechanisms**, and counting them meant deleting
each and watching the outcome: `GoString` and `MarshalJSON` are each read by one
rendering, `String` by the rest, and **`LogValue` is read by none of them** --
`slog.TextHandler`'s `KindAny` path falls back to `fmt` and reaches `String`, so
deleting `LogValue` changed no output. It is kept because `slog.LogValuer` is
part of the contract and a reflecting handler would differ, and it is held by a
RUNTIME interface assertion; a compile-time `var _ slog.LogValuer` would report
its removal as a build failure, which is not an assertion kill. **A fallback
path silently carries a mechanism you thought you had counted.**

The nonce sweep in `TestNoAuthMaterialReachesErrorsLogsOrContexts` uses a fresh
random credential for the reason `pgstore` uses one for a DSN: a fixed token is
indistinguishable from ordinary message text.

## The public error shape, and the tenant boundary

**Every public failure is one Core `ErrorEnvelope`, and there is no second body
shape.** The origin and CSRF guard used to answer `{"code":…,"message":…}` of
its own; A2.1 nested that Reason as the envelope's stable code instead, so a
client decodes once and branches on one member — including the one rejection it
can act on without a human, `csrf_token_expired`. No Core `ErrorCode` **constant**
names an unauthenticated caller, a denied operation, an unknown route, a refused
method, an oversized body, an unserved route or a fault inside Factory, so those
are declared in `internal/httpapi/errors.go` as further `sessionwire.ErrorCode`
values. Note what is *not* claimed: Core's `invalid_request` is general enough
for a malformed request at any layer and A2.1 uses it for four pre-session
conditions, so "Core's codes are all about a session or a command" is false and
this package is the counterexample. `TestACodeCoreNamesIsSpelledWithCoresConstant`
holds the two sets disjoint and states its own limit: it cannot see a code a
later Core adds.

**An API failure never falls through to the SPA.** The split is by path segment,
before authentication, because the bundle's assets are public and the routes are
not. An *unclean* API path is refused outright rather than handed to the mux,
which would answer a 301 to the cleaned path — for `/v1/../assets/app.js` that
is a redirect out of the API and into the SPA, with an HTML body.

**A tenant reaches SessionStore only through `scope`.** A1.2's `Authorizer`
never reads its resource parameters, so "a cross-tenant identifier is
indistinguishable from a nonexistent one" was true there only *vacuously*:
existence was never consulted. `internal/httpapi`'s `scope` is where it stops
being vacuous — the catalog query carries the authenticated principal's tenant,
so the store reports another tenant's row as missing rather than as forbidden,
and one `sessionNotFound()` construction serves both cases so the two responses
cannot drift apart. `TestNoProductionFileBuildsAStoreRequestOutsideTheScope`
scans every production file of the package, **enumerated from the directory**,
and refuses a `sessionstore` composite literal, a keyed `TenantID` member and an
assignment to a `TenantID` field anywhere but a method on `scope`. A2.2 to A2.4
add their request builders there or that scan fails. What it establishes is
bounded: it cannot show the principal a scope was built from is the
authenticated one, because both are values of the same type — that half is
behavioural, and its reader compares the tenant the store received against the
one the *verifier* issued.

**`HEAD` is served wherever `GET` is; `OPTIONS` is refused with 405.** Both come
from RFC 9110 §9.1 — a general-purpose server MUST support GET and HEAD, and
every other method is optional. HEAD costs nothing: `stateChanging` already
classifies it as safe, so it is CSRF-exempt exactly as GET is, and `net/http`
suppresses the body. OPTIONS is refused because answering a preflight is the
first half of approving the CORS grant `CSRFHeaderName`'s defence depends on
never existing.

**Clearing a method's `owner` is not a one-word edit.** Every method in
`httpapi.routeTable` names the task that fills its body in and answers 501 until
then, so nothing stopped a later task shipping a handler on whatever
authorization rule the placeholder inherited. `sanctionedImplementedMethods` now
has to name the `METHOD /path` and say why its rule is adequate, and
`expectedRoutes` is an independent restatement of every `(method, route)`'s
rule, command kind, body, session, streaming and readiness columns — written
from the operation's shape, not from the table. Both object routes now require
`AuthorizeObjectRead` before catalog lookup, then a configured committed-reference
policy before metadata or bytes. Missing policy fails closed with 503.

**`owner` and `handle` are per METHOD, for the reason `auth` is.** A2.1 moved
authorization onto `methodRule` because `/v1/sessions` is a list and a create
and one rule for both had to be the weaker. A2.2 moved readiness for the same
reason: the list is served and the create is A3.1's, and a route-level column
could only call that route implemented — leaving the create's 501 unmeasured —
or pending, failing on the list it does serve. `TestARouteMayBePartlyImplemented`
is the anti-vacuity check: if no route were mixed, the finer column would buy
nothing.

## `/v1/bootstrap` reports identity; it does not select it

`GET /v1/bootstrap` is an `authAuthenticated` read whose complete response is
`{"tenant_id":"…"}`. The tenant is the bounded opaque `sessionwire.TenantID`
from `OperationContext.Principal`; it is never read from a header, query, path
or body, and the response contains no credential, subject or principal kind.
The response is a Factory-owned browser DTO rather than a Core session-wire
contract because it describes the authenticated HTTP caller, not a session or
runtime exchange.

The route accepts no query parameters, performs no durable or runtime read and
remains available while SessionStore is draining. A browser may call it with
an ambient cookie without a CSRF token because GET is safe; authentication,
the ordinary request deadline, error envelope and API security headers still
come from the same route chain as every other endpoint. In particular,
`Cache-Control: no-store` applies to both its successful identity document and
all authentication failures.

## `/v1/agents` is a deployment description, not tenant data

`/v1/agents` (and its migration spelling `/v1/capabilities`) is
`authAuthenticated` and **deliberately not tenant-scoped**, and that is the same
decision that lets it carry an `ETag`. Its two inputs have no tenant dimension:
the configured `Department` is deployment configuration, and SessionStore's Host
target directory is deliberately *not* partitioned by tenant — a pooled target
may serve several tenants, so a row carries an isolation class and
`ListCompatibleHostsRequest` has no `TenantID` member for a scope to fill. The
response is therefore a pure function of configuration and directory state and
is **byte-identical for every authenticated principal**, which is asserted
rather than assumed. The accepted cost is written down rather than left to be
discovered: a deployment whose tenants may launch different agents cannot
express that here, and every authenticated principal learns every configured
`AgentID`.

**Membership is configured; launchability is advertised.** SessionStore files a
target's rows under an ordering scope derived by **digest** from the
`(AgentID, RuntimeCompatibilityID, Placement)` triple, so a reader can ask
"which Hosts serve *this* target" and cannot ask "which targets exist" —
measured against the released `sessionstore v0.1.0`, whose only listing entry
points are `ListCompatibleHosts` (needs a whole key) and `ReconcileHostTargets`
(walks the *due* index, i.e. exactly the lapsed rows). So the deployment
supplies the keys and the directory supplies the liveness. That is what keeps it
from being the competing static catalogue the specification forbids: a
configured **pooled** template no Host advertises is **not listed**, while a
**dedicated** one is listed unconditionally because placement creates its
workload on demand. The probe is one bounded page per pooled template with an
explicit limit and no continuation; `agentProbePageLimit` states the exact cost
of that bound and which task removes the condition that produces it.

**Both page-limit constants are floored, ceilinged and unanchored in between,
and the ceiling is the half that matters.** `Store.pageLimit` refuses any limit
above `storage.MaxOrderedPageLimit` (**1000**, inclusive) with a typed error, so
raising `agentProbePageLimit` or `maxSessionPageLimit` past it would put a live
`internal_error` on a public authenticated route — for the probe, on *every*
`/v1/agents` request in a deployment with a pooled template. **Both fakes
therefore enforce the store's rule** (`refusePageLimit`), which is what gives
the constants a reader at all; a fake looser than the module on the dimension a
constant feeds is §5 class 4, and it let `32 → 5000` and `200 → 5000` survive
116 green tests. The floor is derived too (the probe must exceed one lapsed row,
and must not be zero). The value **between** floor and ceiling is a judgement
with no mechanical reader — `32 → 16` and `32 → 2` survive, correctly, because
nothing in this module can observe the cost of a larger page. Do not invent an
assertion for it.

**The `ETag` saves the client, not the deployment.** The tag is a digest of the
body, so a conditional request pays every directory read and the whole marshal
and skips only the write: a 304 costs what a 200 costs. `/v1/agents` is
O(pooled templates) serial store round-trips per request, uncached and bounded
by configuration rather than by the fleet. Coalescing or caching the aggregate
is a composition decision and belongs to A9.1.

**The validator, and what carries one.** `/v1/agents` carries a strong `ETag`
over the exact response bytes and honours `If-None-Match`; `/v1/sessions` does
not, because a validator over a tenant's private page is a stable fingerprint of
that tenant's state which outlives the body in logs, and it would almost never
hit anyway since every accepted command moves a session's `LastActiveAt`.
`Cache-Control: no-store` stays on **both**: the validator is for a client
holding it in its own application state, which is not an HTTP cache, and
weakening the header the API's threat model rests on to buy a revalidation would
be the wrong trade.

**`/v1/sessions` never consults the directory.** A session's place in the list is
a fact about SessionStore's catalog, so an expiring advertisement cannot change
it — asserted by comparing the *whole* response across four directory states
*and* by requiring the directory to have been asked nothing, since three
identical responses would also be produced by a handler that read it and ignored
it. Recent-first order is enforced by Core's own `SessionPage.Validate` on the
way out, because Factory forwards Core's type rather than re-projecting it.

## The cold session reads

**Status, journal and gates are answerable while every Host is stopped**, and
that is the property, not a side effect: every member each of them answers is a
SessionStore record member, so none of them consults the target directory and
none of them can. The three are asserted together wherever a rule is stated over
"the cold reads", because a rule proved on one of them is a rule the other two
are free to break.

**The journal's initial view is the END of the journal.** A request naming no
position gets the last `defaultJournalPageLimit` *sequences* below the tip the
store captured; older pages are separate, equally bounded reads addressed by
`from_seq`, and a forward `cursor` is SessionStore's own opaque token, handed out
and handed back unread. Nothing here follows a cursor on the caller's behalf, so
no credential buys an unbounded read. The window is a span of **sequences**, not
a count of events, which is what makes it safe: a span of n positions holds at
most n records, so the page limit can never cut it short by itself, and a tail
that is mostly private records comes back short rather than walking further back
to fill itself.

**The tail uses one SessionStore `Tail` request, available since v0.2.0.**
SessionStore captures one tip and derives the sequence window from it. The former
probe-then-read design could capture two different tips: with 100 records at the
probe and 1,000 private appends before the page read, a 64-position request read
1,064 sequences and reported the newer tip. `TestHTTPJournalTailUsesOneTipAcrossPrivateAppends`
reproduces that interleaving through HTTP and the real store, then requires one
tip capture, 64 cursor advances, and the original tip. A later all-private tail
still costs 64 advances. Including session resolution, the initial journal
request now makes two store calls: one catalog lookup and one journal read.
The catalog's `LastJournalSeq` remains unsuitable for positioning because it is
updated separately from journal appends.

**A byte-limited tail may continue with an ordinary cursor request.** Factory
forwards the cursor unchanged with `Tail=false`; explicit `from_seq`, including
zero, also disables Tail. `TestHTTPJournalTailByteBudgetCursorPreservesCapturedTip`
uses bodies exceeding the real store's page budget, appends another record,
then checks that cursor pages finish the original window at its original tip.

**Every journal request also bounds scanned records, including private ones.**
SessionStore v0.3.0's `ScanLimit` is set to the chosen `limit` in the shared
scope helper for Tail, explicit `from_seq`, and cursor reads: default 64,
maximum 100. An event limit alone did not establish that bound: a forward
`limit=1` over an opening fence and 10,000 private records made 10,001 cursor
advances. `TestHTTPJournalForwardPagesBoundPrivateScanWork` reproduces that
case and a real cursor positioned before the same private suffix. It requires
one advance for `limit=1`, then drives default/clamped cursor continuations to
completion at the original captured tip despite a later public append.
Private-only pages may be empty with a nonempty cursor; clients continue from
the cursor and coverage, not from the number of returned events. The scan
budget is applied again on each request because it is not encoded in the cursor.

**Private records only advance the watermark.** SessionStore withholds every
non-public record from the public projection and closes its sequence position
through `covered_through` alone; Factory forwards Core's `JournalPage` whole, so
`JournalEvent.Body` reaches the client as the **bytes the store holds**. Decoding
and re-encoding would reorder members and rewrite escapes — a body no longer
matching the one its writer canonicalized — and would need a vocabulary only
Harness defines, which this module may not name.
`TestTheReadPlaneNamesNoPrivateBearingStoreMethod` derives the forbidden set
rather than listing it: a method of the released `*sessionstore.Store` whose
**results** can transitively reach a stored `Envelope` is one that can hand back
private bytes, and `SessionReader` declares none of them.

**Absence is not always a catalog code.** Outside the legacy single-tenant
layout SessionStore verifies a session's collision witnesses *before* it reads a
record, so a session that is not there fails with `*KeyspaceError` /
`binding_not_found` and never reaches a `CatalogError` at all — sessionstore's
own `noSuchSession` names exactly that set. `catalogFailure` read only the
catalog codes until A2.3, which meant **every nonexistent and every cross-tenant
session answered 500 in a multi-tenant deployment**, on the very path the
byte-identical 404 exists to serve. The cross-tenant/absent comparison is now
driven across all four ways the store says "there is no such session".

**`resolveSession` carries the record it read.** It used to discard the entry,
because A2.1 owned only the existence decision. The status is a projection of
exactly that record, so reading it again would be a second durable round trip on
the most polled route on the surface *and* a second instant — the existence
decision made against one record and the body rendered from another. The gates
route still pays a second catalog read, because `ReadGates` re-reads the record
it projects and restating that projection here is how two readers of one record
come to disagree; coalescing durable reads across a request belongs to A9.1.

**A query parameter that is present must carry a value.** `singleValue` refuses
`?x=` and a repeat for every parameter this surface reads, and that is a rule
about the
surface rather than about parsing: `?x=` is a parameter the caller *sent*, and
no position or bound here means the empty string. The concrete failure it
prevents is `?cursor=`, which SessionStore reads as **no** cursor while Factory
had already treated the request as positioned — selecting a forward read and
walking from sequence one. Measured on a 3000-record journal: `?cursor=`
returned events 1–95 where naming no cursor returned 2938–2999. The client that
reaches it is doing the obvious thing: Core declares `next_cursor` `omitempty`,
so a client written as `cursor=${page.next_cursor ?? ""}` sends an empty cursor
exactly on reaching the tip, is thrown back to the head, and walks the journal
forward again — an unbounded read loop on an authenticated route. Refusing is
chosen over treating it as absent because both possible answers are guesses
about what the caller meant and one of them is the loop.

**"Every parameter" is a claim about call sites, and it is held by a derived
guard rather than by `singleValue`.** It was *false* as first written: the
tenant list read its own cursor as `sessionwire.Cursor(query.Get("cursor"))`,
bypassing the reader entirely, so `?cursor=` was forwarded as **no** cursor and
`?cursor=a&cursor=b` served the first of two positions — the same client bug as
the journal's, on the sibling route, milder only because `limit` bounds each
page into a non-terminating poll rather than a full-history walk.
`TestEveryQueryParameterIsReadThroughTheGuard` parses the production files and
reports `Get` and index bypasses for its recognized syntactic sources:
`url.Values` parameters and `Query()` results, assigned or used inline, with
parentheses ignored. A header `Get` is not one of those sources. This is not
type or data-flow analysis: arbitrary aliases and helpers returning `url.Values`
are outside its reach. `TestQueryReadScanRecognizesInlineReceivers` exercises
both inline read forms, dynamic names and guarded sibling reads, with header
and guarded-read controls. `TestEveryParameterOnEveryRouteRefusesAnEmptyValue`
then drives the derived **(route, parameter)** pairs — five, not three — and asserts the guard's
own *message*. That last part is not decoration: for `limit` and `from_seq` an
empty value fails downstream anyway in `Atoi`/`ParseUint` with the same 400 and
the same `invalid_request`, so a status-and-code probe cannot see the guard for
them at all, and scoping it to `cursor` alone was measured leaving the package
green. The old reader was a hard-coded `{"cursor", "from_seq", "limit"}` at one
route: four of the five pairs, omitting exactly the broken one.

**A draining store is 503 and retryable, not a fault.** `admitForeground`
refuses every read with `*StoreClosedError` once `Close` begins; it is a bare
struct that wraps nothing and matches no typed arm, so it fell to
`internalFailure` and made an ordinary graceful shutdown answer every read with
`500 retryable:false` — telling clients to stop retrying at the moment another
replica would serve them. `storeUnavailable` is consulted by **two** of the
three durable mappings, `catalogFailure` and `directoryFailure`;
`journalFailure` reaches it by *delegating* to `catalogFailure`, and adding a
copy there was measured equivalent, because `*StoreClosedError` is not a
`*JournalError` and falls past the typed arm to the delegation. Its stated
limit: a read already *in flight* when `Close` begins is
cancelled through the returned context and arrives as `context.Canceled`, which
is indistinguishable here from the caller going away.

**Every page limit is checked against `storage.MaxOrderedPageLimit` at compile
time.** `storePageCeiling` names the storage constant directly rather than
restating 1000, so a release that moved it is a build fact rather than a live
500, and `const _ = uint(storePageCeiling - n)` beside each limit fails the
*build* rather than a test. `TestEveryPageLimitIsCheckedAgainstTheStoreCeiling`
parses the package's own production files and requires each to have one, because
no constant expression can notice a limit that has no check. It derives its
subject **twice**: constants named `*PageLimit` (a convention, load-bearing only
while it is followed) and — the rule that does not depend on a name — every
identifier written as the `Limit` member of a `sessionstore` request literal. The
second exists because the first missed a measured case: `const
probeObjectChunkSize = 5000` passed as a `Limit:` value at five times the real
ceiling, invisible to the suffix rule in both directions.

**State the use rule's reach plainly: it covers one of the four limits.** Every
`sessionstore` request literal on this surface lives in `routes.go`'s `scope`
helpers, and three of the four constants reach one through a **parameter** —
which the scan skips, deliberately, since a bound on a parameter is at whatever
assigned it. Exactly one, `agentProbePageLimit`, is written into a literal and
therefore covered by the use-derived rule; the other three are held by the suffix
convention alone. The scan is not extended to follow the parameter because that
is inter-procedural value tracking, and a scan that guessed at it would be a
guard whose *own* reach nobody could state. What the use rule buys is the
construct the convention cannot see at all — a page bound named nothing like
one — and the test asserts `agentProbePageLimit` is in its result so the rule
cannot quietly reach nothing.

**The two ceilings are different numbers, and that is what gives
`boundedPageLimit`'s `ceiling` parameter a reader.** `maxJournalPageLimit` is
**100** and `maxSessionPageLimit` is **200**; while both were 200 the parameter
received the identical value from both call sites, so swapping one constant for
the other survived the whole suite — *a parameter every call site passes
identically is untested by construction*, and asserting the clamp against the
constant that produced it cannot see the constant move either. The difference is
derived from what one row costs: a session summary is a fixed set of members
Core's vocabulary bounds, while a `JournalEvent.Body` is arbitrary stored JSON,
so a page of each differs in byte cost by orders of magnitude. Each clamp is now
asserted against a **literal**, and `TestTheTwoPageCeilingsAreNotTheSameNumber`
fails if a later edit collapses them back onto one number.

**Do not add an HTTP method override.** The CSRF guard's subject is `r.Method`.
A route or middleware honouring `X-HTTP-Method-Override` or a `_method` field
would let a cross-site page present a POST as a GET: exempted as safe, then
executed as a write. The note lives beside the allowlist in `guard.go`, where a
route author will read it.

## The ClientLink

A6.1 built `internal/realtime/clientlink`: the browser-facing duplex connection,
split into an `Engine` that decides and a Centrifuge adapter that carries. Every
decision — protocol version, credential, channel, RPC method — is the Engine's,
so the policy is drivable without a socket and the adapter has nothing to
choose. `factory.New` does **not** compose it yet; `/v1/realtime` still answers
501 and now names A9.1 as its owner.

**Two properties of the transport are load-bearing and were measured, not
assumed.**

`ClientLinkLimits.PingInterval` is **rejected below one second** rather than
floored. The connect reply carries the cadence as a whole number of seconds
(`centrifuge@v0.38.0/client.go:2466`), so 500ms arrives as `0`; the Go client
assigns `c.sendPong = res.Pong` *inside* `if res.Ping > 0`
(`centrifuge-go@v0.12.0/client.go:1467-1474`), so a client told `0` never pongs
and the server closes **healthy** connections with `DisconnectNoPong` every pong
timeout. The deployment reads that as a network fault. Truncation *above* the
floor is allowed and the constant says why.

`ClientLinkLimits.PerConnectionQueueBytes` is in **bytes**, and the name now
carries the unit because the field previously said "in messages" with a default
of 256 — which would have configured a 256-**byte** budget and closed
essentially every connection on its first event. The only thing that enforces it
is `centrifuge.Config.ClientQueueMaxSize`, which is bytes
(`centrifuge@v0.38.0/config.go:58-61`). It is measured by execution, both ways:
**1,024** publications of 4 KiB kill a stalled consumer at a 4 KiB budget with
3008 and do not at 16 MiB. The number is 1,024 and not 64 because 64 (256 KiB)
was measured *surviving* — a stalled consumer's socket and read buffers absorb a
few hundred kibibytes before the server's queue grows at all — and the load was
raised rather than the assertion loosened.

**The measurement depends on releasing the consumer BEFORE taking the verdict,
and the first version did not.** The server's close is issued from the publish
path but cannot unwind while the transport's write is blocked on a socket
nobody is draining, so a helper that waited for the disconnect event first
reported "survived" in 8 of 14 clean runs with the hub holding *zero*
connections. The order is: publish the whole load, release the consumer, then
decide — and a "survived" verdict is refused unless a second witness agrees.

**That witness may not be the hub's population, and the first version's was.**
`DisconnectSlow` is 3008, which is inside centrifuge-go's reconnect band
(`transport_websocket.go:29`), so a client closed for the queue budget
reconnects well inside the helper's 30s wait and the hub reads 1 again: a
harness that missed the close was *corroborated* by its own second witness
(measured, with the disconnect plumbing sabotaged: `connects=2
lastDisconnectCode=3008 connections=1`). The witness is now the number of times
the server ACCEPTED a connection, which a reconnect cannot restore — a survivor
is accepted exactly once — so the harness cannot report a survival it did not
see. This bounds a false RED only; a true survivor always has a live
connection.

`MaxChannelsPerConnection` exists because the transport defaults
`ClientChannelLimit` to **128 silently** (`node.go:135-136`), and one browser
link multiplexes every session its user is watching. A ceiling nobody chose is a
ceiling discovered by a user hitting it.

**Close codes are the library's, and no custom code is minted.** A5.1 measured
the boundary and `centrifuge-go@v0.12.0/transport_websocket.go:29` states it:

```go
reconnect := code < 3500 || code >= 5000 || (code >= 4000 && code < 4500)
```

So a reconnect-band close **never reaches `OnDisconnected`** — it arrives at
`OnConnecting`. That is why the handshake refusals are classified into four
distinct answers rather than one: an unknown credential is 3500 (terminal,
back to login), an expired one is 3005 (terminal for that token, refresh and
reconnect), an unsupported protocol is 3506 (terminal), and a credential
verifier that could not be reached is 3004 **in the reconnect band**, because an
outage of the credential service must not sign every live user out. A test that
watched only `OnDisconnected` would time out on the last one and read as a hang.

**A successful handshake authorizes nothing.** The principal is stored in the
connection's context by `ConnectReply.Context`, and every subscribe and every
RPC is still decided by the `Authorizer`. Two tenants are driven against one
handler at once, because one connection cannot separate "the principal came from
this handshake" from "the handler holds one principal".

**Client publication is disabled by the absence of an `OnPublish` handler**,
which is what enables it in this library, so there is no check to skip.
Protobuf framing is refused by `ServeHTTP` **before** the upgrade — the
websocket handler selects it from `?format=`, `?cf_protocol=` or the
`centrifuge-protobuf` subprotocol and offers no way to turn it off — with a
control asserting the otherwise identical handshake IS upgraded.

**Origin is not decided here.** `WebsocketConfig.CheckOrigin` admits everything
on purpose: `internal/httpapi`'s guard owns the trusted-origin list, the
forwarded-header trust option and the rule that a handshake carrying a browser
credential must send an `Origin`, and it runs before this handler in the
composed chain. A second answer here is a second place for that rule to be
stated wrongly.

**That ordering is now measured, not deferred.**
`TestTheGuardDecidesOriginBeforeTheClientLinkUpgrade` wraps the real
`clientlink.Handler` in the real `httpapi.Guard` and writes three WebSocket
handshakes by hand over a socket, because the answer is the STATUS LINE and
because importing a websocket client would promote `gorilla/websocket` from an
indirect requirement to a direct one. The deployment's own origin is **upgraded
(101)** — the control, without which a chain refusing everything would pass —
while another site's origin and an ambient-credential upgrade carrying no
`Origin` are both **403 before the handler is entered**, naming
`origin_not_trusted` and `websocket_origin_missing`. Removing `guard.Wrap` from
the chain answers **101** to the cross-origin handshake and the request reaches
the link, so the guard is what decides. A6.1 recorded this as blocked on A9.1;
it was not. `NewGuard`, `Wrap` and `NewHandler` are exported, and a composition
seam is not the same thing as composition code.

**`internal/command` is a vocabulary, not a seam.** The five
`sessionstore.CommandKind` values are shared by `internal/httpapi` and
`internal/realtime/clientlink` because §8.1 makes the REST controls and the
ClientLink RPCs two spellings of one admission contract. Two private copies
would satisfy every test either package could write while letting an RPC be
admitted under a kind no route serves. The strings are pinned once, as absolute
literals, in that package's own test — every other reader names the constant, so
nothing else could notice a value change.

**`internal/admission` was the second copy, and the claim above was false until
A6.1's gate found it.** It declared all five as its own absolute literals while
being precisely the package both the REST controls and the ClientLink RPCs are
routed into — the exact hazard the vocabulary package was created to remove,
sitting in its most important consumer. It now holds aliases, as
`internal/httpapi` already did.
`TestTheCommandVocabularyIsSpelledInExactlyOnePlace` is what makes the claim
checkable rather than reviewed: it enumerates every **production** file with
`modfiles`, parses it, and reports any `CommandKind`-typed constant, variable or
conversion whose value is a string literal outside `internal/command/kind.go`.
A guard naming the packages it knew about could not have caught this one, so it
derives its subject from the module; it is driven against a fixture carrying
both shapes and two controls, because on a clean tree it passes whether it works
or not. Test files are outside its subject on purpose — `kind_test.go` must
state all five absolutely, since nothing else could notice a value changing.

### Commands are routed into the admission service (A6.2)

A6.2 gave the RPC path its durable half. An authorized RPC decodes into Core's
own request type, goes to `internal/admission`, and is answered from the
authoritative SessionInbox record. `Engine.Admit` is the whole of it; the
transport adapter still decides nothing.

**A refusal is a reply BODY, not a transport error, and a fault is the
opposite.** `Admit` returns bytes for every outcome admission classified and an
error only where no admission decision exists — an unknown method, a denied
authorization, a fault. The refusal body is a Core `ErrorEnvelope` carrying
admission's own `sessionwire.ErrorCode` unchanged, which is what makes the RPC's
answer and the REST route's answer the same string rather than two vocabularies
a client must switch on separately; the consumer is already written for it
(`wui`'s protocol package resolves an RPC whose data has `error` and no
`command_id` into its Core error hierarchy). **An error `admission` did not
itself classify** is never dressed as one of the nine public codes: a client
told its command was *rejected* stops retrying, where the truth is that nobody
knows whether it landed — which is the unknown outcome the durable CommandID
exists for. `centrifuge.ErrorInternal` is `Temporary`, which is the correct
advertisement for those.

Read that sentence's subject carefully, because the wider version of it — "a
provider outage is never dressed as a public code" — **was false when it was
first written here**, and the layer that had the reader was `admission`, not
this one. `existingCompatible` folded a `Targets.IsKnown` **failure** into
`runtime_unavailable`, `ResolveAgent`'s failure into the same, and a
`Directory.Owner` failure into `gate_not_resumable`. Every target and directory
read has three outcomes — yes, no, and *could not ask* — and the third was
spelled as the second, so a transient outage reached a browser as a classified,
non-retryable decision about its command. `internal/admission` now returns a
dependency fault **as itself**, wrapped for an operator's log and carrying no
public code; the negative *answer* is unchanged. No new vocabulary was needed to
do it, which is why it was correctable rather than owed to A9.1's shared
classification authority: each edge already has a fault channel.

One fold is knowingly left: `admissiblePayload` maps a `canonicalCommand` encode
failure — a Go-level fault about a request that has already passed `Validate` —
onto `invalid_request`. It is very nearly unreachable, since the input is a Core
type that just validated, and it is named here rather than fixed so the narrowed
claim above stays exactly true rather than nearly true. None of the
four call sites (three distinct dependency methods, `ResolveAgent` twice) had a
test.

**And a table of named sites was the wrong reader for it.** The property is
service-wide, so a fixed list defends it only where somebody thought to look —
which is how two more folds of the identical shape sat unread in the two
functions this fix rewrote, one of them telling a *retried* command it had been
permanently rejected. `TestNoDependencyFaultBecomesAPublicCode` derives its
subject instead — dependencies from the interface-kind **fields of `Config`**,
fault sites from the methods of those interfaces whose last result is an
`error`, entry points from the exported methods of **`*Service`** — and drives
the cross-product, asserting that **if the failing method was called, the answer
carries no public code**.

**What each axis is pinned by is not the same, and the difference matters.**
Deleting a *field* from `Config` is a compile error in `service.go`, so that axis
cannot shrink quietly. Deleting a *method* from one of those interfaces compiles
everywhere — a gate did it and the sweep went from 60 pairs to 54, green — so
the method axis is pinned only by the bidirectional check on
`unreachedDependencyMethods`: a record naming a method that is no longer derived
fails, which is what makes the disappearance visible. Do not restate the
compile-pinning claim over both axes; it is true of one.

Reflection cannot see a *call site*, so a third layer parses the sources and
requires every function calling `refusal()` — the sole constructor of a
classified code — to be reachable from a driven entry point. Its first version
**granted** coverage where it claimed to demand it: an orphan named `Validate`
was "reached" because `req.Validate()` was recorded as an edge to that name. The
graph now records only calls it can attribute — a bare identifier, or a selector
on the enclosing function's own receiver — and a duplicate declared name is a
hard failure, because a name-keyed graph cannot distinguish two of them.

`TestADependencyFaultIsNotADecisionAboutTheCommand` remains beside it for the
half a fault sweep cannot check — that the negative *answer* is still a refusal,
which is site-specific.

**The refusal carries no message and `retryable` is false.** Core makes
`message` optional and `code` the member a client branches on, so a message here
would be a second prose vocabulary A3.3 would have to reproduce word for word.
No refusal admission mints today is repeatable without changing the request:
`runtime_unavailable` is minted for an unresolvable target, an oversized
payload, the missing V1 create reservation **and** a directory read failure, so
the code cannot distinguish its one transient cause from three permanent ones,
and advertising it retryable would tell a client to hammer a misconfiguration.

**Decoding precedes authorization, because the session must have one reader.**
There is no path segment to read a session from, so the session an
authorization decision is made about is the one the decoded request names. A6.1
read `session_id` through a private envelope struct — correct while there was
nothing else to do with the data, and a hazard the moment there is: a body whose
strict decode and a lenient one disagree (a duplicate member, which Core refuses
and `encoding/json` resolves to the last occurrence) would be authorized under
one session and admitted under another. A body Core's decoder refuses is
therefore refused as `invalid_request` before any authorization decision, which
discloses nothing: the answer is derived from the caller's own bytes.

**The reply describes the RECORD, never the call.** Admission's "this call
accepted it" boolean is deliberately unread, so an original acceptance and a
retry that found the stored record are the same bytes — if they differed a
client could learn whether its earlier attempt landed. SessionStore's five inbox
states map onto Core's four: pending, claimed and applying are all `accepted`
(Core defines it as "the inbox commit succeeded", not "a Host applied it"),
applied is `applied`, rejected is `rejected` and carries the record's own
`ErrorDetail`. Core's `pending` is never minted — no durable record means it —
and a state this build does not recognise is a **fault**, not an optimistic
acceptance.

**The seam is derived from the service, not listed.** `clientlink.Admitter` is
audited against `*admission.Service` in both directions by reflection, so an
admission task adding a sixth V1 command fails here instead of silently leaving
the ClientLink unable to serve it. `AdmitLegacyCreate` is excluded BY NAME: it
mints identities server-side and keeps the legacy unknown-outcome limitation,
which is exactly what a ClientLink command may not have.

**The retry contract is measured against the released store, not a fake.** Two
admission services and two ClientLink handlers over one `sessionstore.Open` —
different clocks, different proposed runtime identities — with the accepting
replica shut down before the retries are sent. The identical retry returns the
original bytes; the same identity carrying different input, and the same
identity under a different kind, are `command_rejected`; an uncreated session is
`session_not_found`; a racing pair of replicas are told one order and one
mapping. The apply deadline is written once and a retry does not extend it. The
positive control is a *different* CommandID on the same session: without it
"nothing moved" would also be the output of a Factory that had stopped admitting
anything.

**`ClientLinkLimits.CommandTimeout` is the ONLY bound a durable admission has,
and before it there was none.** This was documented as "bounded by the link's
lifetime", which is false, and the truth is worse than the claim.
centrifuge@v0.38.0 dispatches an RPC **synchronously on the connection's read
loop** (`client.go:1385` → `2259`), and the connection context is cancelled by
the websocket handler's `defer close(ctxCh)` (`handler_websocket.go:218-222`) —
that is, when the read loop **returns**. An in-flight admission is the very
thing keeping it from returning, so nothing about the connection can cancel one:
a blocked admission's context was measured surviving `client.Close()`,
`node.Shutdown` **and** `server.Close()`. Two operational consequences followed,
and neither was a slow command: a wedged store or directory **hung one link with
no bound at all**, and because dispatch is serial it **head-of-line blocked every
other frame on that link**, so a replica could not be drained while one admission
was stuck.

The bound is a deadline `Engine.Admit` puts on the context it hands the
authorizer and the admitter, sized by the deployment and defaulting to
`httpapi`'s own 30s so a command admitted over REST and the identical one
admitted over a ClientLink get the same patience. It is worth exactly what a
context is worth, which is the same limit `httpapi.RouteLimits` states for its
edge: **a dependency that honours its context returns at the deadline, and one
that ignores it is bounded by nothing here.** `TestAStuckAdmissionDoesNotWedgeTheLink`
measures the property that matters over a real socket — not that the stuck
command fails, but that the link is **still serving** afterwards and that
`Shutdown` drains — and it is honest about that limit.

Because the connection context can never be cancelled while an RPC is in flight,
`client.Context()` versus `context.Background()` at the dispatch site is an
**equivalent mutant**, and both gates probed it: there is no reader because there
can be no reader. It is still `client.Context()`, and the reason is narrower than
either of the two this document gave before. The context is **not** valueless:
`principalOf` reads the operation context off it, and it descends from the
`http.Request`'s, which `net/http` loads with `LocalAddrContextKey`
(`net/http/server.go:1933`) and `ServerContextKey` (`net/http/server.go:3549`).
What makes the mutant equivalent is the pair of facts above plus one about this
composition: **no consumer downstream of `Admit` reads a value from the context
it is handed** — `internal/identity`'s `Authorizer` takes it as
`_ context.Context` (`internal/identity/authorize.go:50`) and admission forwards
it to SessionStore, which knows none of these keys. A consumer that read one
would make it a live difference. The same applies one layer in, to
`Engine.Admit`'s own parent.

**One limit this task did not close.** The V1 `session.create` RPC reaches
admission and is refused `runtime_unavailable`, because `AdmitCreate` still
returns `ErrCreateIdentityProtocolUnavailable`; the create path is A3.1's
remaining work, and the durable cases here use the legacy create to establish a
session.

**A constraint on A3.3, recorded where A3.3 will read it.** `httpapi`'s
`retryableStatus` returns **true** for 502/503/504, so the obvious
`runtime_unavailable` → 503 mapping would make the identical refusal
`retryable:true` over REST and `retryable:false` here. No divergence exists today
— the control routes answer 501 — but it is the default outcome unless A3.3
stops it. Whichever way it is settled, the answer must be **one** authority
consulted by both edges, not two literals; this is the third instance of the
shape `A9.1-notfound` already names.

### Subscriptions are tracked as delivery demand (A6.3)

A6.3 gave the subscribe path its routing half. `Engine.Bind` authorizes one
channel, derives the session it names, records a **DeliveryBinding**, and
returns that binding's release. There is no `AuthorizeSubscribe` on the Engine
any more: authorization and demand are one entry point, so the channel that was
authorized and the session whose demand was taken cannot come from two
decisions that disagree — the same defect A6.2 removed from the RPC path, where
the authorized session and the admitted session came from two decoders.

**A DeliveryBinding is a (connection, channel) pair, not a channel.** Three
browsers watching one session are three bindings and **one** demand, which is
what makes both edges edge-triggered: the demand plane is notified on the FIRST
binding and released after the LAST one has been gone for the configured
debounce. Neither is a state check — a count of bindings that happens to be one
is not the same event as a count that just became one.

**A binding arriving inside the debounce retains the demand; it does not
re-acquire it.** That is what the debounce is for: a page navigation, a
component remount and a reconnect after a lost link each drop a subscription and
take it again within a moment, and a replica that released on the first of those
pays a registry read and a rebind on the second. Cancellation does not rest on
the timer's `stop` having won its race — `time.Timer.Stop` explicitly may lose
one — so the entry carries a **generation** the callback compares, and
`fireEvenStopped` drives the losing side deliberately.

**`Bind`'s answer is the release, and there is no key.** A caller cannot release
a binding it did not take, release one twice, or release someone else's. The
transport needs exactly that, because it learns a subscription has ended in
**three** places and all three must give back one binding once: an unsubscribe,
a disconnect (`Client.close` unsubscribes every channel before reporting the
disconnect, `client.go:1075-1081`), and a subscribe the library refused AFTER
the callback returned — `onSubscribeError` deletes the channel and fires no
unsubscribe event at all (`client.go:1721-1733`, `1773-1789`, `3672-3681`), so the handler
consults `IsSubscribed` once the callback returns. The `OnDisconnect` arm is a
backstop with a mechanism rather than a worry: `unsubscribe` returns early when
`node.removeSubscription` fails (`client.go:3667-3670`), before the handler is
reached.

**The tenant is the principal's; only the session is parsed.** A tenant read out
of a string the client sent is a tenant the client chose. A channel whose tenant
segment is not the principal's, or whose segments Core will not carry, is a
**fault** (`ErrUnroutableChannel`, answered 100/temporary) and not a denial:
the Authorizer is a seam, so an implementation with a different grammar could
authorize a channel this build cannot name a session in, and answering that
with 103 would tell a browser entitled to the channel to stop asking forever.
The two grammars — `internal/identity`'s regular expression and this package's
`session:` prefix — are compared over `(channel, tenant)` pairs by
`FuzzTheDemandGrammarAgreesWithTheAuthorizers`. **Read what holds that
property, because the first version's answer was wrong.** The target drives the
engine through a **permissive** authorizer, and it has to: `Bind` consults the
authorizer first and returns before reaching `demandKeyOf`, so a target built on
the real authorizer reports "this edge cannot route it either" for every channel
that grammar refuses, whether or not it is true — a gate measured deleting the
fourth-segment check, a strictly laxer grammar, leaving all eleven seeds green,
including the one written for that case. The seeds now discriminate a laxer
grammar **without** `-fuzz`, which matters because `-fuzz` is a mode only the
Makefile's `FUZZ_TARGETS` invokes; that variable enumerated the root package
alone until A6.3, so the module's first out-of-root target was never fuzzed at
all. `TestEveryFuzzTargetInTheModuleIsFuzzed` runs the Makefile's own
enumeration and compares it with the targets `modfiles` finds in the sources.
The clause-by-clause behaviour — that an authorized channel this edge cannot
name is a fault and takes no demand — is held by
`TestAChannelTheAuthorizerAllowedButThisEdgeCannotNameIsAFault`, which builds
the same permissive composition.

**Both demand calls are bounded, and the release is the half that has nothing
else.** `DemandTimeout` bounds an acquire for `CommandTimeout`'s reason and
against the same mechanism; it bounds the debounced release because that release
runs on a timer after the connection is gone, so there is no request context in
existence to inherit a deadline from and `context.Background` alone would be
bounded by nothing. The fake records whether the context it received carried a
deadline **and** whether it was already cancelled, because a release handed a
dead context does nothing and looks exactly like one that worked.

**`Handler.Shutdown` gives the demand back.** `Node.Shutdown` closes every link,
so every binding is gone and every session is sitting behind its debounce; a
replica that stopped there would leave the routing plane holding demand for
sessions no connection remains to serve. `ReleaseIdleDemand` fires those
releases now and joins their failures. A session whose bindings are still held
is left alone — a live binding means a connection the node did not close.

**Host session ownership is not touched, and cannot be.** Nothing on this path
names a lease, an epoch or a Host: the demand seam takes a tenant and a session
and returns an error. Whether a session has an owner, and whether demand causes
one to be found, is A7.2's, and its own step says a first subscriber must not
restore a cold session merely for viewing — which is why an error from the
demand plane is specified as a FAULT and never as "no owner".

**Factory does not retain the browser's durable journal cursor (step 3), and
that is true at three layers.** The transport never hands one to this package:
`SubscribeEvent` carries a channel, a token, opaque data and three booleans
(`centrifuge@v0.38.0/events.go:168-183`) and no offset or epoch. The reply is
the zero `SubscribeOptions`, so `EnableRecovery` and `EnablePositioning` are
false, and those two are the only members that make centrifuge keep a stream
position for the connection at all (`client.go:3224`, `3248-3249`) — a
browser asking for a positioned recoverable subscription is told **no** to both,
with no stream position in the reply. And the demand seam carries the session
identity and nothing else, asserted as a for-all: two subscriptions differing in
recovery flags and subscription data reach it as indistinguishable calls, with a
different SESSION as the positive control.

**Per-binding repair is not delegated to the transport, and A6.1 does not
assume it is.** One `messageWriter` per `Client` and no per-channel queue
*bound* anywhere in the library means a stalled consumer loses its whole
connection, by the queue budget (3008) or the write deadline (3009). A7.3 owns
the repair, above the transport.

## The HostLink

A7.1 built `internal/realtime/hostlink`: Factory's client side of the
Factory-Host connection, split the same way the ClientLink is. `Pool` decides
and `CentrifugeDialer` carries, so the pooling invariant is drivable without a
socket and the transport has nothing to choose. `factory.New` does **not**
compose it; A9.1 owns that, along with the reaper's cadence.

The invariant is one sentence: **a session binding never costs a connection, and
a connection is never shared between Hosts.** The route table is keyed by
tenant AND session, because a session id is unique only within its tenant. Four
decisions protect that sentence, and each is a decision rather than an
implementation detail:

- **A bind record must name the Host its link goes to.** A bind carrying another
  Host's tuple would be refused by the receiving Host for a reason that reads as
  a lease problem rather than as a Factory routing bug.
- **Re-binding elsewhere is refused, not moved.** A silent move leaves the first
  Host holding a route the pool no longer tracks and can therefore never unbind.
  Re-binding to the *same* Host resends; that is the lease-epoch refresh.
- **A failed unbind still releases the local route.** A bind is Factory-local
  routing state that the Host validates against its own durable lease, so a
  route kept after a failure is kept forever — there is no retry, and the next
  bind would be refused as a conflict against a route nobody wants.
- **A refused bind keeps the connection and drops the binding.** The Host
  answered, so the socket is good; discarding it turns one lease disagreement
  into a dial storm.

`MaxLinks` bounds **Hosts**, and the check is reached only for a Host with no
link. That distinction needs a case where the two counts differ — with a ceiling
of one, the reuse path never reaches the check, so a pool counting sessions
passes it. `TestSessionsAlreadyBoundDoNotConsumeTheLinkCeiling` is that case.

**Runbook A7.1 step 3 — a failed command RPC leaves the committed inbox record
pending — is two claims and both are held.** `ErrCommandUndelivered` is returned
only for a failure to hand the record over; a Host's own answer arrives as
`*HostRefusal` carrying Core's typed `HostLinkError` and is deliberately **not**
wrapped in it, so a caller tells "never seen" from "answered" without reading
message text. And nothing here could mark the record anything else: a
parsed-import test holds that no production file in the package imports
`sessionstore`, and fails as vacuous at zero files. It proves what this package
cannot do, not what the admission service does.

**Step 2 is a decision, not an omission.** Several Factory replicas each hold
their own link to one Host; there is no broker and no leader to lose, and
closing one replica's pool leaves the others' connections and routes untouched.
Two cases measure it, one over fakes and one over real sockets.

**Three transport properties were measured and two of them corrected a claim
written from memory.**

`centrifuge.DisconnectInvalidToken` is **3500**, not 3501
(`centrifuge@v0.38.0/disconnect.go:122`); 3501 is `DisconnectBadRequest`, at its
own declaration `disconnect.go:127`. Both line numbers are at the **pinned**
v0.38.0 and are not interchangeable: at the unpinned v0.39.0, `:122` is
`DisconnectStateInvalidated`. The code is carried in a structured
`HostDisconnect` field so a caller and a test branch on the value rather than
matching text, and a terminal-band close on a LIVE link — not only at dial
time — is what stops the link answering.

**The embedded server validates a push body**, so `Client.Send` refuses invalid
JSON outright. An "unreadable push" case must therefore send well-formed JSON
this build still cannot read, which is also the case that exists in production.
An unreadable push is **dropped**, not fatal: a capacity report is an
advertisement and a registry observation is a routing hint, and neither is worth
the sessions multiplexed over the connection.

**`Config.Token` is left empty and `GetToken` is the only credential path.**
Setting both looks like belt and braces and is not: the client consults
`GetToken` only when the token is empty
(`centrifuge-go@v0.12.0/client.go:1183`), so a populated `Token` means the
callback is never reached on a first connect. An up-front `ServiceToken` fetch
written here first **survived** a mutation for exactly that reason — the library
fell back to the callback and produced the same refusal by a different mechanism
— so the redundant path is gone. Two other guards went the same way: a `closed`
fast path in `Pool.Close`, whose idempotence actually comes from emptying the
link table, and an `idleSince` reset on bind, unreachable because `idleSince` is
read only when the binding set is empty and only `Unbind` empties it.

The version is verified on **every** connect, not just the first, and reconnection
itself stays the transport's with the backoff taken from the pool's limits. A
Host restarted at a version this build does not speak would otherwise be
reconnected to indefinitely while every control record failed to decode; instead
the link is marked terminal and stops answering.

**A declared gap, not a seam.** Core v0.7.0 defines the HostLink record *bodies*
and their strict JSON, and defines **no transport framing** for them: no method
names, no push discriminator, no channel vocabulary. `MethodBind`,
`MethodUnbind`, `MethodCommand` and the `{type, data}` push envelope are
Factory's half of a protocol whose Host half does not exist in this repository.
The Host writer must implement this mirror, or one of the two must move. Do not
read the tests here as agreement with Host; the node they run against is a
stand-in that implements exactly this proposal.

**Where the framing should live is unresolved, and two homes are ruled out.**
`factory/internal/` is not it — Host cannot import a Factory-internal package,
so one half of a two-repo contract sits where the other half cannot see it. And
Core's `sessionwire/v1` is not it either: a method name and a push discriminator
are transport-*shaped*, they exist because the transport is centrifuge, and
`sessionwire/v1` is transport-neutral today. Putting `hostlink.bind` into a
tier-0 module would make a future transport change a tier-0 breaking release.
The answer is a shared, explicitly transport-scoped home; root books it, and
nothing in this repository should be read as that decision having been taken.

Because those strings are the artifact Host mirrors, they are pinned as
**absolute literals** — a test that compared `MethodBind` to itself pinned
nothing, since a rename moves both sides together.

**One decision in the proposal is contestable and is recorded as such.** Pushing
a capacity report as an async message rather than publishing it to a channel is
right for a *registry observation*, which is per-session and per-route, and
weaker for a *capacity report*, which every Factory replica wants: a channel
would let the broker fan one publication out instead of the Host calling
`Client.Send` per connected replica, and two replicas on one Host is a measured
case. It is not taken now because there is no agreed channel namespace — the
same gap as the method names — and the `Observer` seam absorbs a later change.

**The route table and a link's binding set are two maps, and only one of them
was observable.** `Bindings()` counts a link's binding set; `RouteFor()` reports
the route table, which is the pool's actual routing authority. Both unbind cases
asserted on the first while claiming the second, so `Unbind`'s `delete` of the
route survived deletion — and the consequence was not a wrong answer: `ReapIdle`
would still collect the link, whose binding set really is empty, and the next
`DeliverCommand` dereferenced nil under `p.mu`, leaking the lock and wedging
`Close` behind it. Both lookups of `p.links` by a routed Host now fail closed
with `ErrUnknownBinding`, and `Unbind` drops an orphaned route rather than
keeping one that would refuse every later bind as a conflict.

The pool carries the **control** plane only. Session event data, per-binding
queues, backpressure repair and the live tail are A7.3's, and nothing in this
package may be read as having solved them. Choosing *which* Host a session
belongs to is A7.2's demand-driven binding, which calls `Bind` and `Unbind`.
`ReapIdle` is a method rather than a goroutine because A9.1 owns the start/stop
ordering that would give it a lifetime.

## Placement

`internal/placement` is split into a pure half and an I/O half, and the split is
its main structural claim. `policy.go` is a total function of a catalog record,
an optional registry observation and one capacity page — no clock of its own, no
store call, nothing it can create — so every rule a reviewer checks is driven
directly rather than through a store. `reconciler.go` holds the claim, the
desired-state write and the controller call, and its only decision is which of
`Decide`'s answers it acted on.

**An owner is preferred before any claim is taken.** An owned session is the
common case, and putting a durable compare-and-swap in front of it would
serialize routing behind a record whose purpose is coordinating *scaling*.
Losing the claim is neither a failure nor simply a deferral: specification
section 15 step 4 makes a replica that observes an existing claim re-read
observed state, so the loser re-reads the registry and reports an owner the
winner has just produced. The case that holds this needs the owner to appear
BETWEEN the two reads — written as "put an owner, then reconcile" it passed
against a reconciler with the re-read deleted, because the first read already
saw the owner and no claim was ever attempted.

**`ReusableOwner` is the one statement of "is this the session's own live
owner".** `internal/admission` calls it; its own identical copy was removed. The
two must not be able to differ, because an owner placement would re-place while
admission still delivered to it is a command handed to a Host the router is
about to abandon. Placement is compared along with agent and runtime, which
reads as strict until you ask what the alternative does: section 14 defers
migration and never live-migrates a running session, so a mismatch means "not
reusable", never "move it".

**Desired state is idempotent through two mechanisms guarding different
things.** The content comparison keeps a replica from rewriting an intent
already stored, which is what holds the desired *generation* still — and the
generation is exactly what tells a controller its work is stale, so a reconciler
that rewrote the same intent would invalidate every controller's completed work
on every pass. The derived idempotency key keeps a *second* replica from writing
it a second time: two Factories deriving one intent derive one key, SessionStore
checks the key before the revision, and the loser's write is absorbed as a
replay. Both racers are driven. `Result.DesiredWrites` counts calls rather than
claiming "this replica applied it": with a derived key the two replicas are
indistinguishable in the record they leave, which is the point.

The key is length-prefixed per field before hashing, and that is driven too:
concatenated, version `v1` with payload `2x` and version `v12` with payload `x`
share a key, and SessionStore treats a reused key for a NEW intent as a replay
that succeeds without applying anything.

### What H5 decided, and where each part of it shows up

The Kubernetes adapter is **internal**, built as **two binaries from the one
`factory` module**, with **no leader election**.

- `WorkloadController` therefore names no platform type.
  `sessionstore.PlacementIntent` is the whole currency: Factory-authored desire
  and nothing else — no lease epoch, no HostID, no residency. It carries the
  generation a controller records against the workload it created, and that part
  is **not** an H5 clause: H5's recorded text says nothing about a generation.
  The requirement is A4.2 step 1's idempotent desired generation, and
  `sessionstore` supplies the field.
- It has **exactly one method**. Section 13's deletion ordering — request drain,
  wait for an epoch-fenced checkpoint and an observed `cold` release, then
  delete — has none of its steps in this module, and a `Delete` declared today
  would be a seam with no implementation, no caller and no test. **D1.1 step 1**
  is the widener — it specifies ensure, observe, request-drain and delete — and
  D2.2 supplies the drain protocol those depend on.
- A **nil controller is a valid configuration**, because `cmd/factory` holds no
  workload create/delete RBAC and composes none. A dedicated session reaching
  that replica is refused by name with `ErrNoWorkloadController`; reporting it as
  "no capacity" would send a caller into a retry loop waiting for an autoscaler
  that never runs.
- Replicas are safe with no leader through the derived key and the content
  comparison above, not through the claim, which suppresses duplicate scaling
  only.

`factory.PlacementController`'s `EnsurePlacement(ctx, sessionstore.DesiredWorkload)`
predates that answer and cannot identify a workload — a `DesiredWorkload` is a
payload and a version label with no tenant, session or generation — so nothing
here implements it. Reconciling the public option surface with H5 (most likely
by deleting the option, since the adapter is internal) is A9.1/D1.1 composition
work and is deliberately not done here.

### Two gaps, declared rather than papered over

**The pinned wire cannot carry an attachment.** A4.2 step 2 has the selected
candidate asked to acquire or attach. At the pinned `core v0.7.0` there is no
request that could. Three of its records concern one session — bind, unbind and
drain — and only a **bind** could establish a route; it refuses a zero
`LeaseEpoch`, so a bind names an ownership tuple that a session with no owner,
which is the only kind that reaches placement, does not have. Unbind refuses a
zero epoch too, and drain asks a Host to *give up* a session it already holds.
Core's own bind decoder fails closed so that "unknown members cannot become a
future attach/create workflow", which is the same gap seen from the other side.
`OutcomeAttachPooled` therefore names a Host and stops; the caller performs no
attachment because none exists to perform. The same premise records that **Core
names no `lease_held` refusal**: step 2's `LeaseHeld` is spelled
`HostLinkErrorEpochMismatch`, whose `CurrentLeaseEpoch` is the whole answer.

**The marker is executable, and its third assertion is derived rather than
named.** `TestThePinnedWireCannotCarryAnAttachment` fails on each of the three
ways Core could close this: bind relaxing the zero-epoch rule, a `lease_held`
code appearing, and — the likeliest, and the one the first version was blind to
— a **new record type** declared beside an unchanged bind. A test cannot name a
type that does not exist yet and no reflect call can enumerate a package, so the
HostLink vocabulary is parsed out of the pinned `sessionwire/v1` source and
compared against an absolute list of the ten names it holds today. The version
is read from `go.mod` and required to be the pin before anything is parsed, so
the premise cannot be checked against a different copy of core than the build
resolves. The whole `HostLink` prefix is watched rather than the `*Request`
suffix, because naming is exactly what a future Core is free to choose; the
accepted cost is that any growth of that vocabulary fails the test and asks a
human to recheck the premise. `TestTheHostLinkVocabularyScanSeesANewType` is the
scan's own positive control, because against a clean pinned core the assertion
reports the same list whether the scan works or is stuck.

**Tenant-exclusive pooled capacity is refused, not admitted.** Section 12 makes
Factory placement the enforcer of tenant exclusivity for a pooled Host without
the isolating capacity class, and Factory cannot see which tenants a Host is
serving: SessionStore files targets under an `(AgentID, RuntimeCompatibilityID,
Placement)` scope with no tenant dimension, so no query answers "is this
exclusive Host already this tenant's". The only enforcement available is to
refuse the class, so a `tenant_exclusive` **pooled** advertisement is never
selected. The cost is stated rather than left to be found: such Hosts give no
pooled placement at all until the directory carries a tenant dimension, which is
H8's per-tenant Department and a specification section 7 change that is not this
repository's to book.

Not built here: the attachment of A4.2 step 2,
drain-before-delete of D2.2, and the authorship of a dedicated workload's
payload — `Desired` is an INPUT, because what a launch template should contain
is a composition question A9.1 owns.

## The local binding table

A4.3 added `routing.Bindings`: which Host each session this replica is serving
is bound to, and how many local subscribers want it. It is **local state only**
— nothing is written durably, nothing is coordinated with another replica, and
a restart is a new value that knows nothing. There is deliberately no
reconstruction path to write, because reconstruction is what the ordinary path
already does: the registry answers who owns the session and a subscriber's
`Acquire` supplies the demand.

**Identity is the whole ownership tuple, not the session.** `BindingKey` is
`{TenantID, SessionID, HostID, HostGeneration, LeaseEpoch}`, and each member
names a way a route becomes wrong without the session changing: a session id is
unique only within a tenant (the HostLink pool's route table learned this), a
session that moved is served by a different link, a restarted Host has none of
the state the route assumed, and a superseded epoch is exactly what the owning
Host refuses. So "invalidate" and "the key changed" are one fact, and there is
no second staleness rule that could disagree with the key.

**Reuse costs no registry read, which is what makes invalidation a push.** The
route is not re-derived on use, so `Observe` is how it learns a tuple is no
longer current — and the assertion for that is a COUNT of registry reads, since
a table that re-read every time would return an identical binding and pass any
comparison of the two. `Observe` **drops** the route rather than rebinding from
the observation: a push is a hint, the registry is the authority, and what the
push is trusted for is only "what you hold is stale", which needs no trust at
all. Demand survives, so the next `Acquire` or `Deliver` rebinds. An older
epoch is stale and ignored; the same epoch naming a different Host or
generation is a contradiction and invalidates, because keeping a route the
registry contradicts is how a command reaches a Host that is not the owner.
The stated cost: a lease that changes with nobody pushing leaves a stale route
until the owning Host refuses what it carries. That is a refusal, not a repair;
repair is A7.3's.

**Demand is the only lifetime rule.** `Deliver` requires demand and does not
open a route, so a caller delivering to an unwatched session brackets it with
`Acquire`/`Release` and the pool's idle window absorbs the cost. A delivery
whose binding was invalidated is rebound and the command is **redelivered** to
the new owner — the delivery carries only the retry-stable public CommandID,
and the Host's lease and SessionStore's idempotency are what make repeating it
safe. A delivery FAILURE changes nothing: this table cannot tell a lost
connection from a refusal, and dropping the route on either is a rebind storm
that still has not delivered anything.

**A failed unbind still drops the local route, and `Close` joins rather than
returns.** The first is the HostLink pool's rule one level up and for its
reason: nothing above retries, so a route kept after a failure is kept forever
and the next bind is refused as a conflict against a route nobody wants. The
second is so one Host's failure cannot strand the routes on the others, which
are the ones a restarted replica would then conflict with. `Close` has no
`closed` fast path: a mutant deleting one survived the whole suite, because
idempotence comes from emptying the table — the same evidence on which the
HostLink pool removed its own.

**The bind key is derived and framed.** Core wants a retry-stable idempotency
key; deriving it from the tuple is what makes a repeated bind a repeat without
coordination, and the unbind carries the same key so a Host's two records of
one route cannot name two things. The tuple is length-framed before hashing —
an identifier is arbitrary UTF-8 up to `MaxIDBytes`, so tenant `a` with session
`bc` and tenant `ab` with session `c` are one string under concatenation. That
is `internal/placement`'s desired-key lesson, asserted here in its own right
rather than shared, because the two keys frame different fields; no integer
conversion appears in either.

**The seam names Core, not the transport.** `Binder` is the HostLink pool's
control surface stated in Core's vocabulary, with the endpoint passed beside
the request instead of the pool's `Target` struct. A binding is routing state
that outlives any particular transport, and naming a `hostlink` type here would
put centrifuge in this package's graph where the boundary rules deliberately do
not have it. The cost is one adapter at composition, which is A9.1's.

**`Directory.Owner` did not report every absence, and A4.3's integration case
is what found it.** Outside the legacy single-tenant layout SessionStore
verifies a session's collision witnesses before it reads a record, so a session
that was never created fails with `*KeyspaceError` / `binding_not_found` and
never reaches the registry — `internal/httpapi` recorded exactly this, and
`routing` had the same defect: every absent session was reported as a store
FAILURE, which a caller answers by placing a session that has no record to
place. The classification table could not have caught it, because a fake
answers whatever error a case hands it and the cases were written from the
registry's vocabulary; the case that found it drives the real store, and it
also pins the store's answer so a later SessionStore that stopped answering
that way would say so here.

## Demand-driven binding

A7.2 added `routing.Demand`: the local subscriber demand that drives `Bindings`,
and the poll that keeps it current. It is the implementation of the ClientLink's
`DemandManager` seam, which A6.3 declared and left unimplemented; nothing in
production composes the two yet, and that adapter is A9.1's along with the rest.

**It sits on `*Bindings` directly and takes the resolver FROM it.** That is the
single-authority property by construction: the registry read a poll makes to
decide whether a route is still current is the same reader the table binds
through, so there is no second ownership authority here that could disagree with
the routing table about who owns a session. Passing a `Resolver` in beside the
table would have made that a configuration a composition could get wrong.

**`Acquire` answers a local fact, and a local fact cannot fail.** A session with
no owner, an owner Core will not carry, a bind the Host refused and a registry
outage all return nil with the session left UNBOUND and watched. Every one of
them is a routing outcome the poll retries, and none is a reason to refuse a
viewer; the only error is a closed plane, where the answer would be a lie. That
is also why `DemandManager`'s doc specifies an error as a FAULT: an
implementation answering "no owner" as an error would refuse every viewer of an
idle session.

**A first subscriber does not restore a cold session, and that is now something
a test READS.** A6.3's spec gate demonstrated the hazard by adding an
`AdmitRestore` on every subscribe: the whole suite passed, exit 0. The property
held by construction of the seams — neither package can express a command — but
a property of the seams is not a property of the COMPOSITION, which is where the
restore would go and which does not exist yet.
`TestNothingOnTheViewingPathAdmitsACommand` closes it structurally and
module-wide: it derives the viewing path's roots (every method of `Demand`,
`Engine.Bind`, and every function literal registered for a subscription
lifecycle event), follows calls through every production file `modfiles`
enumerates, and reports any admission or placement entry point it can reach. Its
subject is wider than "restore" on purpose — a subscribe that admitted an input
is the same defect, and a guard naming only the runbook's instance passes on
every neighbour.

**The unit of analysis is the module's own declarations**, reached through bare
calls, through method and field calls on values — every seam here is dispatched
through an interface, so a graph following only calls it could resolve to a
concrete receiver would follow none of them — and through calls qualified by a
package **of this module**. A call qualified by a package of a FOREIGN module is
the one shape skipped, and the skip is keyed on the import PATH being outside
`github.com/looprig/factory`, never on the identifier being an import name.

**Both halves of that were measured, and the first version had the key wrong.**
Following foreign package-qualified calls as edges to every same-named
declaration (a dozen `Validate`s, one `Error`) reaches **229 of the module's
362** declarations, including `internal/httpapi`'s router, whose control routes
are *supposed* to admit commands — the ban would have had to be deleted the
first time a later task implemented the REST restore route. (**229, not the 248
this document first said.** That figure was real but measured a different graph:
an even earlier version treating every identifier a body *mentions* as an edge,
not only the ones it calls. Re-measured at this head: 91 with the skip, 229
without, 248 with mentions-as-edges.) But keying the skip
on "is an import name" dropped **22 real intra-module edges**, including
`admission -> placement.ReusableOwner`, `server.go -> httpapi.NewRouter` and
`principalOf -> internalidentity.OperationContextFrom`, which is *on the reached
set*. It was defeatable with no shadowing at all: an identical helper called
bare from `internal/routing` was a finding, and the same helper moved to
`internal/placement` and called as `placement.ReachRestore()` was green — the
shape production already uses 22 times and the shape A9.1's composition will
have. Keyed on the module path the reach is **91**.

**What it still cannot see is stated at the function, and that list is meant to
be complete**: a call through a func value in a struct field, map or slice; a
foreign module calling back into Factory through a callback (which is not
hypothetical — it is how centrifuge invokes the subscribe handler, and it is why
the roots include the lifecycle literals rather than relying on an edge into
them); which of several same-named declarations a call really reaches, since it
reports all of them; a **local identifier shadowing a foreign import's name**,
since the skip set is per file rather than per scope; and a **generic call with
explicit type arguments**, whose callee is an `*ast.IndexExpr` and matches
neither arm of the type switch. The last two are false NEGATIVES rather than
conservatisms, and neither has an instance in the module today.

**The skip's key is `module_pin_test.go`'s `factoryModulePath`, aliased rather
than copied.** That constant is checked against the real `go.mod`. A second
unanchored literal was a guard naming its own subject on the exact key this
guard has now been corrected for twice, and it was unkillable: the analyzer's
fixture writes its own `go.mod` from the same constant, so pointing the pair at
another module path moved both sides together and reverted the reach to 89 in
silence.
It DOES walk files no build configuration compiles, which is conservative for a
ban. Three fixtures test the analyzer rather than the module: one plants a
restore at each root category and two hops away with two controls; one plants
one across an interface seam; one drives a module-qualified call and a
foreign-qualified one in the same graph, with the foreign target carrying an
admission entry point so a skip that stopped working reports rather than
silently widening.

**The poll is refresh, then serve, then arm the next one.** Three sequential
conditions rather than a branch, which is what makes an owner arriving between
two polls bind AND stop hinting in the same tick, and an owner disappearing drop
AND start hinting in the same tick. `Acquire` runs the same `serveLocked` body,
so a viewer of a cold session gets its first hint immediately rather than an
interval later.

**A rebind is a release and then an acquire, in that order.**
`Bindings.Acquire` COUNTS, so rebinding without releasing would leave this
replica holding two units of demand for one subscriber and the last release
would never unbind. The staleness rule is not restated: `refreshLocked` pushes
the observation through `Bindings.Observe` — the single authority for "does this
contradict what the table holds", including its refusal to act on an older lease
epoch — and then asks the table what it still holds. A route invalidated by an
observation a HOST pushed is the same fact and is repaired the same way.

**A registry read that fails keeps a held route and does not refuse a new one.**
The binding is a routing hint the owning Host fences against its own durable
lease, so keeping one through an outage risks a refusal, while dropping one
turns every registry blip into a rebind storm that has delivered nothing.

**`OwnershipPollInterval` is a GAP, not a period — and non-overlap is a
different claim with a different mechanism.** This document credited both to the
arming order and only one of them is its. **`d.mu` is what makes two polls of
one session unable to run at once**: `poll` holds the mutex for its whole body,
so a successor armed at the START would simply block on it.
`TestOnePollOfASessionExcludesEveryOther` asks the lock directly with `TryLock`
from inside a seam call, rather than timing a goroutine — "another goroutine did
not finish within 50ms" is a timing non-event that passes for a plane holding no
lock at all. **The arming order is what makes the interval a gap**: measured from
the previous poll FINISHING, so a store slower than the interval delays the next
poll instead of queueing one behind the mutex.
`TestThePollIntervalIsAGapAndNotAPeriod` asks the clock, from inside the tip
read, whether a successor is already armed — the only place the question can be
asked, since the two arming orders are indistinguishable once the poll returns.

`PollTimeout` bounds one WHOLE poll rather than each call inside it, because a
poll runs on the clock's goroutine with no caller and no request context to
inherit a deadline from. **`Acquire` is the opposite half**: it derives its
bound from the CALLER's context, so a subscriber's shorter deadline and a
subscriber's cancellation both still apply, and
`TestAcquireInheritsTheSubscribersOwnBound` drives both — without the second,
`context.WithoutCancel` there is indistinguishable. Every case in
`demand_test.go` drives off-default limits — a bound every call site passes
identically is untested by construction — and the interval is asserted against
an absolute literal, with the defaults pinned separately by their own case.

**The `journal_tip` hint is published on EVERY unbound poll**, including when
the tip has not moved and including when it is zero. That is what repeatable
means: the hint carries absolute state and no sequence, no nonce and no attempt
count, so two hints for one tip are the same BYTES and a client that missed one
loses nothing by taking the next. Publishing only on a change would make it a
delta in everything but name — a client joining between two changes would wait
for the next write to learn where the journal already was. The claim is a
for-all, so a table of tip sequences is not its only reader:
`FuzzTheJournalTipHintIsAFunctionOfTheTipAlone` derives the space, and its seeds
discriminate a delta, a suppressed repeat and a numbered hint without `-fuzz`.
A tip that cannot be READ publishes nothing, which is the fail-closed direction;
a publish that fails is retried by the next poll, which is the property.

**The tip read asks for the tip and nothing else.** `Tail`, `Limit: 1` and
`ScanLimit: 1`: the tip is a property of SessionStore's scan PLAN, captured
before it walks anything, so a tip read wants the smallest page the store will
build. Without `Tail` the walk starts at sequence one. The request is asserted
whole, because a read that produced the right hint at the wrong cost would pass
every assertion about the hint.

**Two guards this file does NOT have, and one assertion it gained, all from the
same mutation batch.** A generation counter beside the poll's `stop` was written
first and measured redundant: `teardownLocked` deletes the entry, so comparing
the entry the timer was armed with against the one the table now holds already
answers every supersession, and deleting the generation comparison left the
suite green. `clientlink`'s demand table keeps its generation for a reason that
does not apply here — `cancelRelease` there leaves the entry in place. A
`d.closed` beside that comparison went the same way and for `routing.Bindings`'
own reason at its `Close`: a closed plane has no entries. What did NOT survive
scrutiny was the missing assertion in the opposite direction — deleting
`serveLocked`'s first `!entry.held` so that every poll of a bound session called
`Bindings.Acquire` again survived the whole file, and the consequence is not a
wasted call but a routing table gaining a unit of demand per poll, whose last
subscriber's release then never unbinds.
`TestASteadyPollNeitherRebindsNorAccumulatesDemand` is that reader, and it ends
by releasing so the count becomes an observable outcome. A third member of the
same family was found later and removed: `teardownLocked`'s `entry.held = false`
is a dead store, since the entry has just left the table and the only reference
left is a stale poll's closure, which returns on the identity comparison before
touching a field.

**How the scenario set was derived, since the runbook's seven are a floor.**
Three axes, each enumerated from the code: what the registry can say at a poll
(seven answers, read off `Directory.Owner` and `Bindings.bindLocked`), what the
table holds when that answer arrives (unbound, bound, invalidated-by-push), and
the demand transition (eight, including a release nobody holds, work after
`Close`, and `Close` with demand outstanding). The unreachable cells are named
rather than silently skipped, and the three properties that are for-alls rather
than cells are stated as such.

## Not implemented yet

A2.4 implements `/objects/{oid}` and `/objects/{oid}/metadata` in the internal
router. The metadata response is Core's immutable index projection, not proof
that the blob still exists. Byte GET accepts one explicit inclusive Range and
returns exact 206/Content-Range only after whole-object EOF, digest/size checks
and Close succeed. Full GET is supported only up to one page. Defaults and hard
ceilings are 1 MiB retained page and 64 MiB verification, using a 32 KiB buffer
and the existing 30-second request deadline. Range requests reread the entire
object; larger objects require another protocol. Invalid/multipart/open/suffix
ranges are 400; an end outside the object is 416 without clipping.

`ObjectPolicy` must establish committed-reference permission and trusted kind;
index existence and the tenant-only default Authorizer cannot do that. The
resolver consumes the exact catalog binding, including runtime identity and
protocol mode. Only a canonical unbound legacy record may use the existing
reader; all explicit bindings require a resolver. Requests retain authenticated
tenant and canonical session scope; runtime namespace translation is the trusted
adapter's responsibility. Tests exercise two real independent stores, but A9
still owes production policy, frozen configuration resolution and public server
composition. This does not activate independent-store Host execution.

Composition seams (A0.2), identity derivation (A1.1), the route and error
foundation (A2.1), the agent and session reads (A2.2), the cold session
reads — status, journal and gates (A2.3) — and the browser identity bootstrap
are done; the remaining read,
control, admission, routing, placement and realtime handlers are later tasks in
runbook 05. Every method in `httpapi.routeTable` carries the runbook
task that fills its body in, and answers 501 until it does;
`TestTheUnimplementedMethodsAreExactlyTheOnesLaterTasksOwn` holds the two sets
equal. `httpapi.Directory` has no production implementation yet — **A4.1 owns
it**, and until then `/v1/agents` reports every pooled target as unadvertised
under any composition that supplies a directory answering empty pages. `factory.New` composes the authenticator, the guard and the `Router`, and
`Server.Handler` serves them; A9.1 stage 1 did that over the seams that exist.
`Serve`/`Stop` are optional and own an `http.Server` but not the listener, which
is where `httpapi.RouteLimits`' deferred socket bounds landed as `HTTPLimits`.
What it does NOT compose is ClientLink, HostLink, placement or the reconcilers,
and the router it builds carries an empty launch `Department`, a nil
`ObjectPolicy` and no object-store resolver -- each fails closed. There is no
default verifier option, because a deployment supplies the `Verifier` and there
is no credible default for one. `internal/realtime` holds the ClientLink engine
(A6.1), the HostLink pool and dialer (A7.1) and the pinned transport spike
(A5.1); none of the three is composed by `factory.New`. `cmd/factory` and `internal/placement/kubernetes` do not
exist; their exemptions grant nothing today and `TestBoundaryScopesAreNotStale`
will fail if one of those directories appears without a Go file in it. Do not
add a placeholder Go file to satisfy it: that would permanently satisfy a live
tripwire, trading a guard that fires the day a directory appears unearned for a
directory that is always "earned" by a file that means nothing.
