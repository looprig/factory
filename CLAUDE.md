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
