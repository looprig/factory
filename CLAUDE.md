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
- `github.com/looprig/wui` — forbidden **everywhere**.
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
  `github.com/looprig/host`, and `internal/placement/kubernetesx` does not
  inherit `internal/placement/kubernetes`'s grant. It is differentially fuzzed
  against an independent segment-splitting oracle.
- **Scopes are subtrees, not lists.** A second package under
  `internal/realtime/` is inside the grant by design; a second package beside
  `internal/placement/kubernetes` is not. There is no hand-maintained package
  list to drift.
- **The scan fails loudly at zero files.** A guard that walks nothing is
  indistinguishable from a clean tree.
- **Scopes that do not exist are still proven.** Of the two scoped rules, only
  `internal/realtime/` is built; `internal/placement/kubernetes` is not, and the
  Kubernetes adapter ships in the separate `looprig/controller` repository
  instead. So `TestScanReachesEveryRuleScope`
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
choose. `factory.New` composes it at `Start` (A9.1): `/v1/realtime` answers
503 before `Start` and after `Stop`, and since v0.4.0 the node also carries a
watched session's live output (see "The live tail").

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

**A cookie-authenticated upgrade needs no connect token (B7), and the reuse is
narrow.** The embedded WUI sends no connect token — it holds a session cookie
the browser attaches to the `/v1/realtime` upgrade — and the handshake refused
an empty credential before any verifier ran, so REST worked and realtime never
did. `Engine.Authenticate` now reuses the principal the ROUTER verified at the
upgrade when, and only when, the client presents **no token**, the upgrade was
authenticated by a **cookie**, and the context carries a constructed principal.
The adapter reads the upgrade's operation context off the connection context
(centrifuge builds it from the HTTP request, `handler_websocket.go:218-225`) and
hands it to the Engine as `ConnectRequest.Upgrade`/`Upgraded`; the Engine
decides. A non-empty token is verified exactly as before whatever rode the
upgrade, a **bearer** upgrade is not reused (a browser cannot set one, so it is
not the WUI's request), and no cookie is read, no verifier called and **no
credential minted** anywhere on the path. A cross-site page cannot reach it: the
origin guard refuses an ambient-credential upgrade with no or another site's
`Origin` before the handler is entered. The decision table is
`TestACookieAuthenticatedUpgradeNeedsNoConnectToken` plus three single-clause
readers, each asserting the exact close code 3500; a reused principal is per
connection because the context is (`TestAnUpgradePrincipalIsNeverReusedOnAnotherConnection`).

**The composed `/v1/realtime` could not upgrade at all, and the end-to-end
cookie case is what found it.** `recordingWriter` exposed the real writer through
`Unwrap` and its doc called that "the whole fix"; gorilla/websocket@v1.5.3's
`Upgrader` asserts `w.(http.Hijacker)` directly (`server.go:175`) and never
consults `http.ResponseController`, so every upgrade through the router
answered **500 "response does not implement http.Hijacker"**. Nothing measured
it: the composed case sent a plain GET (400 before the hijack) and the guard
case wrapped the ClientLink handler with no router in front.
`recordingWriter.Hijack` now delegates through the ResponseController, and
`TestAComposedFactoryAdmitsACookieUpgradeWithNoConnectToken` in the root package
writes the handshake and one Centrifuge connect frame by hand over the composed
`Server.Handler()` — 101 then a connect result for the cookie, 3500 for a bogus
token, 401 with no credential.

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
the ClientLink unable to serve it. `AdmitLegacyCreate` is excluded BY NAME: admission
refuses every legacy create with `runtime_unavailable` before any durable write,
and no edge routes to it.

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

**The V1 `session.create` RPC is served by composition, not by build.**
`AdmitCreate` admits into the durable public-create plane when the composition
supplies both `WithSessionBinding` and `WithPublicCreates`, and refuses
`runtime_unavailable` when it does not; there is no
`ErrCreateIdentityProtocolUnavailable` any more, and the legacy create is refused
before any durable write and routed to by no edge. The durable cases in
`durable_test.go` seed a **disposition-bound** session through the store's
public-create plane: every command is admitted into the disposition family
(Gap 2, below), and a command for a legacy-bound session is refused
`runtime_unavailable`.

**`A3.3-retryable` is settled, and the answer is 422.** `runtime_unavailable`
does **not** map to 503. The set was **enumerated at the pin**, and re-enumerated
at v0.3.0: five `runtime_unavailable` `refusal()` sites in
`internal/admission/service.go` carrying **five distinct conditions** — a
create's `AgentID` naming no configured target (`:202`), the create binding
this composition was not configured with (`:205`), an **existing** session's
pinned target no longer being configured (`:306`), a session bound to the
**legacy** protocol (`:384`, `ErrLegacySessionUnsupported` in `commandRefusal`
— since Gap 2, which replaced the oversized-payload refusal because the
disposition family stores a large payload by reference), and the refused legacy
create (`:418`, `ErrLegacyCreateUnsupported`). **Every one is permanent** until the
deployment's configuration or the request itself changes, and that — not any
inability to tell a transient member apart — is the argument: `retryable:true`
promises that repeating the identical bytes could succeed, and none of the five
can. A failed target-directory read is **not** in the set; it is returned as a
plain wrapped fault carrying no public code (`resolveTargetFault` at
`service.go:95`, called at `:199`, and the `IsKnown` fault at `:303`), a fold
an earlier task already removed. `reconciler.go:522` (`expiredCommandRejection`)
mints the same code into a durable `Record.Rejection`, which reaches a client
through `StatusFor` inside a **2xx** body, never through this table. 503 would
also make the identical refusal `retryable:true` over REST and `false` on the
ClientLink. Every classified refusal is a **4xx**: 400
`invalid_request`, 404 `session_not_found`, 409 for the four state-conflict
codes (the legacy surface's own answer for `gate_not_ready`), 422
`runtime_unavailable`. It is **one authority, not two literals**:
`internal/command.RefusalStatus` holds the table, `RetryableStatus` holds the
502/503/504 rule that `httpapi.retryableStatus` now delegates to, and this
edge's `refusalBody` reads `retryable` from it rather than writing `false`.

### The controls are one contract, and it is now measured (A3.3)

**Four of the five control routes are served; the create is not.** `input`,
`interrupt`, `restore` and `gates/{gid}` decode Core's own request type, admit
through `internal/admission` and answer from the authoritative inbox record.
The create stays **501 and names its blocker** — a V1 create files a
reservation carrying an immutable `SessionBinding` whose `StorageBindingID`,
`BindingVersion` and `RuntimeSessionID` this composition has no source for, and
a binding is immutable after create — rather than naming a task tag.
`methodRule.reason` carries that sentence into the response body, because an
owner column answers nothing a caller asked and `owner: "A3.1"` outlived the
task it named. Runbook step 2's legacy create decoder goes with it: admission
exposes exactly one legacy entry point and it is a create.

**Four decisions are shared with the ClientLink and live in `internal/command`**
— the refusal status, the retryable rule, the four store spellings of "no such
session" (`SessionAbsent`), and the five-state projection into Core's
`CommandStatus` (`StatusFor`). `parity_test.go` drives both edges over one
admitter and requires **byte-identical** refusal envelopes and acceptance
bodies; the create is excluded there by an assertion that fails the day it is
served.

**The body's `session_id` is compared against the path's `{sid}`** (and
`gate_id` against `{gid}`), and a disagreement is refused rather than rewritten:
`serveRoute` authorizes the PATH's session and the service admits the BODY's, so
without the comparison one session is authorized and another admitted — the
defect A6.2 removed from the RPC path, which closes it by having one reader.

**The success status is the legacy surface's, per route**: 200 for input,
interrupt and restore, **202** for the gate response, measured in
`harness/pkg/serve`. It does **not** vary by durable state — a rejected command
is still a command that was durably admitted, and Core's `status` member is what
a client branches on.

**The Host wake runs after the answer, off the request (v0.7.1).** A control
route's local delivery (`CommandDelivery`, runbook A3.3 step 3) used to run on
the request's context *before* the answer, so against a Host that dropped the
delivery reply every admitted POST waited out the HostLink RPC bound — I1.2
measured 5.004–5.006 s — for a command already committed. The answer never
depended on it (it is built from the durable record, and the wake's error is
discarded), so there is no `applied` fast path to preserve and no short bounded
wait was kept. `internal/httpapi`'s `wakes` owns the attempt: the route's own
`RequestTimeout` bounds each one, at most `maxConcurrentWakes` (64) run at once
and a further one is **dropped** (the sweeps own eventual application), a panic
in the seam is contained and logged at ERROR through the composition's logger, and **`Server.Stop` — not `Quiesce`, which leaves
HostLinks running — cancels them and waits**, before the routing table they call
into closes. A caller hanging up no longer cancels its command's wake. The
ClientLink RPC path makes no delivery attempt and the create is served by the
same handler, so neither had the latency. **`routing.Bindings.Deliver` no longer
holds the table's mutex across the Host RPC** (v0.7.1 gate S1): it resolves (and,
if invalidated, rebinds) the route under the lock, captures the tenant and
session, unlocks, then calls the Binder. Held across the RPC, one silent Host
stalled every `Acquire`/`Release`/`Observe`/delivery on the replica — ClientLink
subscribes included — for about `RequestTimeout`, since queued wakes each waited
out the lock. A route that moves or a table that closes after the unlock costs
only the hint: the delivery names no Host or epoch, the pool re-resolves the
route under its own lock at call time, and the Host fences on its lease.

**`A9.1-notfound` is settled too.** `internal/admission`'s `catalogNotFound` read
two of the store's four spellings of absence while `internal/httpapi` read four,
so a deleted or identity-mismatched session answered a READ 404 and a COMMAND
with a bare fault. Both now call `command.SessionAbsent`.

**`CatalogErrorIdentity` is in that set by a deliberate ruling, not because it
is the cross-tenant arm.** It is the store's **stable-key collision guard** —
`GetCatalogEntry` → `readCatalogEntry` → `catalogEntry` (`catalog.go:730`) mints
it when a decoded record's own identity disagrees with the one asked for — and
it is on the **live read path**, not hypothetical. The cross-tenant case arrives
one layer earlier as `binding_not_found`. It is answered 404 because the record
the store declined to vouch for **may be another tenant's**, so any answer that
distinguishes it from absence risks confirming that some other session occupies
the id, and because `httpapi` has answered it 404 since before A3.3 — a fault
here would make a command and a read disagree about one session. The cost is
stated rather than discovered: an operator loses the 500 on the control path,
exactly as they already had on the read path. `KeyspaceHashCollision` and
`CatalogErrorIdentity` are **two different detectors** and are ruled
differently on purpose.

**The three hand-written vocabulary lists have one tripwire.**
`TestTheStoreErrorVocabularyHasNotGrownSinceTheAbsenceSetWasDerived` parses the
**pinned** sessionstore's sources and counts its two error vocabularies (14 and
10 at v0.9.0, unchanged from v0.8.0). The lists themselves are fine; what was wrong was claiming they
were self-maintaining. A value added to a vocabulary joins those tests **on the
day somebody lists it**, and this is what makes that day arrive loudly.

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
connection, by the queue budget (3008) or the write deadline (3009). A7.3 built
the repair above the transport: see "Bounded delivery and backpressure repair".

## The HostLink

A7.1 built `internal/realtime/hostlink`: Factory's client side of the
Factory-Host connection, split the same way the ClientLink is. `Pool` decides
and `CentrifugeDialer` carries, so the pooling invariant is drivable without a
socket and the transport has nothing to choose. `factory.New` does **not**
compose it; A9.1 owns that, along with the reaper's cadence.

The invariant is one sentence: **a session binding never costs a connection, and
a connection is never shared between Hosts or between tenants.** The route
table is keyed by tenant AND session, because a session id is unique only
within its tenant, and since v0.5.0 the LINK table is keyed by (HostID,
TenantID) — see "Gap 1" below. Four
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

`MaxLinks` bounds **(Host, tenant) links** (Hosts before v0.5.0), and the
check is reached only for a pair with no link. That distinction needs a case where the two counts differ — with a ceiling
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

**The framing is Core's since v0.8.0, and B6 is what it closed.** Core v0.7.0
defined the HostLink record *bodies* and no transport framing, so this package
declared `MethodBind`, `MethodUnbind` and `MethodCommand` as its own half of a
protocol whose Host half it could not see — and the two halves disagreed. Host
reserved only `hostlink.bind`, `hostlink.unbind` and the two drain methods and
resolved **every other method as a channel name**, so Factory's
`hostlink.command` would have been refused `runtime_unavailable` on every
delivery. `sessionwire/v1/hostlink_framing.go` now names the reserved methods
(`HostLinkMethodBind`, `Unbind`, `Attach`, `Drain`, `DrainStatus`), the
`hostlink.v1.` prefix and `HostLinkChannel(tenant, session)`, byte-identical to
Host's derivation, and this package declares no method constant of its own.
**Command delivery's RPC method IS the session's channel**, which is why
`Link.DeliverCommand` takes the tenant and session beside the record: the body
names no session. The `{type, data}` push envelope is still Factory's half —
Core names no push discriminator — and the stand-in node in the tests
implements exactly it.

**B8 was three defects, not one, and no Factory before the v0.2.0 release ever held a link
to any Host.** Each was invisible to this suite for the same reason: a stand-in
that accepted what a Host refuses. Read them together.

- **Connect framing.** Host decoded the connect Data as a bare
  `VersionNegotiationRequest`; Factory sent `{"version_negotiation":{…}}`, and
  Host's strict decoder disconnected **4501**. Core v0.9.0 owns the codecs now
  (`EncodeHostLinkConnectRequest`/`DecodeHostLinkConnectReply`, which refuse the
  wrapped shape), and `563f15e` adopted them. The former wrapped request is
  still refused 4501 by `host v0.2.1`, measured, so the fix is not vacuous.
- **Refusal decode — a defect `563f15e` fixed silently, recorded here.** Before
  it, an RPC reply was decoded into `controlReply{Error *HostLinkError}`
  expecting `{"error":{…}}`. Host v0.1.0 and v0.2.1 both send the **bare**
  `HostLinkError`, so a genuine refusal decoded to `Error == nil` and was
  **returned as success**: an `epoch_mismatch` read as a delivered command. The
  reply is now decoded bare, `{}` and any unknown body fail closed, and
  `TestBareHostRefusalJSONIsValidatedByCore` is the fixture control.
- **The upgrade itself.** A Host's HTTP handler answers **400** unless the
  client names the JSON protocol (`selectsJSONProtocol`, unchanged since Host
  v0.1.0). centrifuge-go's JSON client sends no `Sec-WebSocket-Protocol`, and
  the other route Host accepts — `?format=json` — is closed by Core's
  `InternalEndpoint.Validate`. So with framing and refusals both right, the
  dialer still could not connect. `Dial` now sends
  `Sec-WebSocket-Protocol: centrifuge-json`; driven against the released
  `host v0.2.1` (`host.Compose` over memstore, no shim), the dial gets 101, the
  link retains `[bind unbind attach drain drain_status]`, and Bind and
  DeliverCommand round-trip to Core-valid refusals. That harness cannot live
  here — `host` is forbidden everywhere in this module — and is booked for
  `tests/internal/orchestrationtest`.

**The rule those three teach: a stand-in must refuse what the real peer
refuses.** Every stand-in now sits behind `RequireJSONSubprotocol`, a copy of
Host's gate; with the header absent every dial in the package fails, and
`TestTheStandInRefusesAnUpgradeThatDoesNotNameTheJSONProtocol` pins the gate so
it cannot be quietly weakened.

**The capability gate, and the one transient it must not misreport.** Bind,
unbind and attach are admitted against the `hostlink_methods` the Host advertised in its
connect reply, snapshotted under `mu`, and refused locally with
`*UnsupportedMethodError` otherwise. Between a dropped connection and the next
reply there is no set to admit against, and that window is refused with
**`ErrLinkReconnecting`**, a distinct sentinel: the earlier answer was
`UnsupportedMethodError` — "host did not advertise bind" — during a 250ms
reconnect, reaching `routing/bindings.go` undifferentiated from a genuine
capability refusal. A stale bind must not be queued against the old set either;
`TestABindInTheConnectingWindowIsRefusedAsReconnectingNotUnsupported` drives a
bind 20ms into a 300ms reconnect toward a Host that will advertise nothing and
requires zero RPCs to arrive. Delivery is not gated and IS queued by the
transport — and is **emitted before the new reply is verified**, because
centrifuge-go resolves connect futures (`client.go:1346`) before it runs
`OnConnected` (`:1354`). That does not matter for delivery and a version
mismatch marks the link terminal anyway, but the gate is a snapshot, not a
promise that every emitted RPC sits behind the latest reply.

**Nothing is held across `client.RPC`, and the numbers say why.** `563f15e`
added a second mutex held across the RPC so "check then send" was one
operation. Measured: a 100ms-deadline delivery for one session waited **702ms**
behind another session's slow reply, because a caller deadline cannot preempt a
mutex wait. Worse, centrifuge-go v0.12.0 can run an RPC's completion callback
**twice** — `clearConnectedState` fails every pending request on a new goroutine
(`client.go:745`) while the caller's own send has already registered the request
and then fails (`:2187`, `:443`) — and the second callback blocks forever on
`RPC`'s capacity-1 result channel (`:373`). Landing on the caller's goroutine,
that wedges the caller before it reaches `RPC`'s `select`, and no context frees
a channel send. With the lock, one wedge froze every later call on the link, and
through `Pool.Bind`'s `p.mu` the whole pool; reproduced 1 in 4 reconnect stress
runs. The lock is gone — the snapshot under `mu` plus the generation binding
already give the atomicity it claimed, and the mutant that dropped it survived
the whole suite — and `client.RPC` runs on its own goroutine with the caller
selecting against the bound context, so a wedge costs one leaked goroutine and
the caller is released by the same reconnect's generation cancel. The 60-round,
4-sender stress (`TestReconnectStressNeverWedgesACaller`) is opt-in under
`-race` because it trips centrifuge-go's **own** data race (`client.go:2187`
reads `c.transport` without `c.mu`), a third-party report that says nothing
about this module. `Pool.Bind`/`Unbind` still hold `p.mu` across the link call,
deliberately: that lock is what orders a Bind and an Unbind for one session
across two Hosts, and narrowing it is a per-session design change, booked.

**What the generation registry is and is not proven to do.** The registry
cancels a superseded generation's bound contexts **synchronously** — when
`onConnecting` returns, every bound context is done, and
`TestRPCGenerationCancelIsSynchronous` pins that at the unit against a
`context.AfterFunc` bridge, which cancels on a new goroutine. Whether the
transport could ever emit a stale RPC through that goroutine-wide window is
**reasoned, not measured**: the former bridge passes every transport-level test,
including the queued-bind case, because a reconnect delay is milliseconds and
the asynchronous cancel lands in microseconds. What the queued-bind case does
pin is `bind`'s re-check after `context.WithCancel`; dropping it lets a stale
bind reach the Host (assertion kill). Its fixture is bounded everywhere and
releases its gate by `defer`, because an earlier version could not fail, only
hang: a `Bind` that returned before consulting its context parked the package
on an unbounded receive, and a `Bind` parked inside the gate pinned `Cleanup`.

The strings are still pinned as **absolute literals**
(`TestTheWireVocabularyIsPinnedToItsLiterals`), now against Core's constants and
against the spelled-out channel `hostlink.v1.dGVuYW50LWE.cy0x` for
`("tenant-a", "s-1")`, because a test that compares a constant to itself pins
nothing. `TestACommandDeliveryIsSentOnTheSessionsChannelAsItsMethod` measures
the derivation over a real socket: two deliveries differing only in tenant reach
the Host as two methods.

**One decision in the push envelope is contestable and is recorded as such.**
Pushing a capacity report as an async message rather than publishing it to a
channel is right for a *registry observation*, which is per-session and
per-route, and weaker for a *capacity report*, which every Factory replica
wants: a channel would let the broker fan one publication out instead of the
Host calling `Client.Send` per connected replica, and two replicas on one Host
is a measured case. It is not taken now because `HostLinkChannel` is per
session and Core names no Host-level channel, and the `Observer` seam absorbs a
later change.

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

### Gap 1 (v0.5.0): one link per (Host, tenant), at Core's derived address

A Host advertises a BASE endpoint and serves each tenant's HostLink at
`sessionwire.HostLinkEndpoint(base, tenant)`; a verbatim dial of the base is
404. **Derivation happens in exactly one place, `tenantTarget` in `link.go`**,
from the Target's endpoint (always the ADVERTISED BASE — nothing above the pool
holds a derived address) and the request's tenant; the Dialer is handed the
derived one. Bind, attach and the gate-response capability read derive; unbind,
delivery and subscribe resolve the route's own tenant's link. So **R-1 is
structural**: every lookup of a link by a route is `routeKey.link(host)`, which
carries the route's tenant, and `TestEveryOperationTravelsOverItsOwnTenantsLink`
reads every operation's link. Everything per-link (negotiated methods,
reconnect state, subscriptions, `regMu`) is per (Host, tenant) because it
lives inside the link. A terminal eviction drops only that tenant's link and
its routes; `Unsubscribe` reaches only the tenant's links; the reaper collects
per pair.

**A derivation refusal is per tenant**: `*EndpointError` wraps both
`ErrNoTenantEndpoint` and Core's `*HostLinkEndpointError` (read the code with
`errors.As`), and is returned before any dial. `classifyAttach` maps it to
`placement.ErrTenantUnaddressable`: placement records the candidate in
`Result.Unaddressable`, logs a WARN with Core's code, and asks the next — the
Host is not abandoned for any other tenant. `livetail.Plane.Bind` logs the same
refusal on the viewing path. A base that is not bare (`base_names_tenant`,
`base_not_bare`) refuses EVERY tenant: that is the compatibility window with a
Host still advertising `…/hostlink/<tenant>`.

### The gate_response capability gate (v0.5.0)

**`hostlink.GateResponseCapable` is the ONE predicate** over a Host's connect
reply that says "this Host can apply a gate_response command": it is
`reply.Supports(sessionwire.HostLinkCapabilityGateResponse)`, the token
`hostlink.command.gate_response` (core v0.11.0, advertised by host ≥ v0.4.0 in
`hostlink_methods` — "reserved methods plus capability tokens"; a token is
never dispatched, never equals a method and never starts with `hostlink.v1.`).
**Never read `CatalogRecord.LeaseEpoch` as a capability** (owner ruling
2026-09-19: a per-session, caller-asserted value cannot answer a per-process
question). `TestTheGateResponseCapabilityIsExactlyCoresToken` spells the token
as a literal and refuses a substring and the prefixed spelling.
`Pool.AcceptsGateResponses` asks it over the owner's link FOR THE SESSION'S
TENANT (the `Negotiator` capability reads the link's current reply: a
reconnecting link is a transient error, a terminal one is evicted, a link that
cannot report answers false). `Config.GateResponses` injects another predicate
so the accepting half is drivable. Two callers, one adapter
(`gateResponders` in `compose.go`): **admission** refuses a gate response
`gate_not_resumable` (`ErrGateResponseUnsupported`) BEFORE writing it — admitting
IS delivering, since the owner reads the durable stream — and a nil
`GateResponders` refuses too; a failure to ask is a fault with no public code.
**Placement** withholds a gate-response wake (`Request.GateResponses`, filled by
the pending sweep from the durable kind) from a Host that cannot apply it and
counts it in `WithheldGateResponses`. **Placement** places a session with a
pending gate response only on a candidate carrying the token (`appliesGateResponses`,
`Result.Incapable`; one aggregated WARN per pass, at most once per session per
5 minutes; a candidate whose base cannot address the tenant is Unaddressable
with Core's code, not Incapable). With none, **the whole session waits** —
inputs and interrupts too — for at most the pending answer's `ApplyDeadline`
(default 5 m), after which the expiry sweep rejects the answer and placement
resumes; a command admitted behind it can expire in that window. The filter is
**pooled only**: a dedicated placement hands the record to the controller
unfiltered. A transient failure to reach the owner is
`admission.ErrGateResponderUnavailable` (classified in `compose.go`), answered
503 retryable at the edge. The capability read acquires its link with
`acquireUnlocked` — the dial runs outside `Pool.mu`, since a public request can
trigger it — and its answer can be one reply stale (it is not fenced to the
owner's generation). **The mixed-fleet
residue**: an answer admitted under a v0.4.0 owner that lands on a v0.3.0 Host
after re-placement (or was claimed there) sits `applying` until a v0.4.0
successor settles it `not_applied`; the filter keeps a *pending* one off an
incapable Host but cannot recall one in flight. Do not run v0.3.0 and v0.4.0
Hosts serving gating agents together.

**A gate response is never admitted by reference** (host v0.4.0 spec gate C1):
above `sessionstore.MaxInboxPayloadBytes` (64 KiB, measured on the same
canonical ENCODED payload `admit` stores, not on the bytes sent) a NEW gate response is refused
`invalid_request` with `ErrGateResponseTooLarge`, after the retry read and
before any write. A Host blocks the session's command stream behind a
by-reference gate response until its apply deadline.
`TestAnOversizedGateResponseIsRefusedAtTheInlineBound` holds both sides of the
exact bound; every other kind is still stored by reference above it.

The pool carries the control plane AND, since v0.4.0 (Gap 3), a session's
**live tail**: `Pool.Subscribe` subscribes the session's `HostLinkChannel` on
the link that holds its bind (see "The live tail" below). The per-binding
queues and backpressure repair sit *above* this package, in
`internal/realtime/delivery` and `routing.Relay`, and are composed by
`internal/realtime/livetail`. Choosing *which* Host a session
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

H5 decided the Kubernetes adapter would be internal, built as two binaries from
the one `factory` module, with **no leader election**. **The packaging half is
superseded** (owner ruling 2026-09-18): the adapter and its executable ship as
the separate **`looprig/controller`** repository, Factory ships **no binary**,
and no Kubernetes library enters this module's graph. The no-leader-election
half still holds.

- `WorkloadController` therefore names no platform type.
  `sessionstore.PlacementIntent` is the whole currency: Factory-authored desire
  and nothing else — no lease epoch, no HostID, no residency. It carries the
  generation a controller records against the workload it created, and that part
  is **not** an H5 clause: H5's recorded text says nothing about a generation.
  The requirement is A4.2 step 1's idempotent desired generation, and
  `sessionstore` supplies the field.
- It exposes the four domain lifecycle operations `EnsureWorkload`,
  `ObserveWorkload`, `RequestDrain`, and `DeleteWorkload`. Section 13's deletion
  ordering — request drain, wait for an epoch-fenced checkpoint and an observed
  `cold` release, then delete — stays in the caller's ordering; no platform type
  crosses this seam. This module is seam-only and has no scheduled reconciliation
  driver. Widening the exported method set is a source-compatibility break for
  external implementations; **while this module is pre-1.0 it ships as a MINOR
  bump** (owner ruling 2026-09-18), and becomes a major release only after
  `v1.0.0`. D2.2 supplies the drain protocol those operations depend on.
- A **nil controller is a valid configuration**, because Factory ships no binary
  and grants itself no workload create/delete RBAC. A dedicated session reaching
  a replica composed without one is refused by name with
  `ErrNoWorkloadController`; reporting it as "no capacity" would send a caller
  into a retry loop waiting for an autoscaler that never runs.
- Replicas are safe with no leader through the derived key and the content
  comparison above, not through the claim, which suppresses duplicate scaling
  only.

`factory.PlacementController`'s `EnsurePlacement(ctx, sessionstore.DesiredWorkload)`
predates that answer and cannot identify a workload — a `DesiredWorkload` is a
payload and a version label with no tenant, session or generation — so nothing
here implements it. B5 dropped its requirement (see below); the option is still
accepted.

### One gap closed by Core, one still declared

**The wire carries an attachment since `core v0.8.0`, and B5 sends it.** The current pin is `core v0.11.0`. A4.2 step 2 has the
selected candidate asked to acquire or attach. At `core v0.7.0` no request
could carry that — bind and unbind refuse a zero `LeaseEpoch`, drain asks a Host
to *give up* a session —
and `TestThePinnedWireCannotCarryAnAttachment` held that premise by failing on
any growth of the `HostLink*` vocabulary. Core v0.8.0 grew it by exactly the
record that closes the gap, `HostLinkAttachRequest` (sent as
`HostLinkMethodAttach`, carrying **no lease epoch** and a **required host fence**
`host_id`/`host_generation`, answered with a `HostLinkRegistryObservation`
whose `lease_epoch` a bind then names), so that test failed on the bump by
design and `TestThePinnedWireCarriesAnAttachment` replaces it in the positive
direction: attach validates without an epoch and refuses without the fence,
bind still refuses a zero epoch, `epoch_mismatch` still carries the **other
holder's** epoch and Core still names no `lease_held`, and the vocabulary remains
the twelve names introduced by v0.8.0. Core v0.9.1 additionally puts the
optional `hostlink_methods` capability signal on the negotiation response; the
HostLink transport retains it and gates its reserved bind, unbind and attach
operations with `Supports`. The scan is unchanged — parsed from the pinned source, refused
if `go.mod` has moved off `pinnedCoreVersion`, with
`TestTheHostLinkVocabularyScanSeesANewType` as its positive control — so the
next growth asks a human again.

### Gap 2: every command is a disposition command

**A session created on a Host can now be talked to.** Until this change only
the create was admitted into the DISPOSITION family; input, interrupt, restore
and gate response went to the LEGACY inbox through `AdmitCommand`, which a Host
can never reach (it takes residency through `AcquireResidency`, which pins
disposition), and which the store refuses for a disposition session anyway. So a
session could be created on a Host and never spoken to again.

- **Admission.** `internal/admission` admits all five kinds with
  `AdmitDispositionCommand`, under the binding the catalog holds (or, on a
  retry, the winner's own descriptor binding — the store requires equality and
  nothing here constructs one). The retry read is `GetDispositionCommand`. An
  oversized payload of any kind is uploaded with `PutCommandPayload` and
  admitted by reference (A3.1 step 5), because the disposition inbox compares a
  retry on digest and size rather than on the object reference;
  `ErrPayloadProtocolUnavailable` is gone. `commandRefusal` is the one
  classifier: a content mismatch is `command_rejected`, the store's
  `binding.protocol_mode` refusal (invalid or conflict — never backend) is
  `runtime_unavailable` wrapping `ErrLegacySessionUnsupported`, everything else
  a fault. **No mixed-family admission remains**: `CommandStore` has no legacy
  method, and `clientlink`'s seam-shape rule refuses a method returning a
  legacy `InboxEntry`.
- **Superseded in v0.5.0** (sessionstore pinned at v0.12.0, and the gate is now
  the capability predicate — see "The gate_response capability gate"). What
  follows is the v0.3.0/v0.4.0 state, kept for the record. **Gate responses to
  Host-resident sessions answered `409 gate_resolved` until
  sessionstore ≥ v0.12.0 and host ≥ v0.4.0** (B5 v0.3.0 spec gate M1). The
  reason is the store, not this module: sessionstore v0.10.0 (the pin) and
  v0.11.0 refuse `OpenGate` -- every Host-owned catalog write -- on a
  disposition-bound session (`hostEpochFence`, `catalog invalid
  (binding.protocol_mode)`), so no disposition session can carry a gate
  projection, A3.1 step 4's projection check refuses first, and its
  fresh-owner check (`gate_not_resumable`) is unreachable in production.
  Until those releases, **a Host-resident agent that opens a gate stalls until
  its gate deadline.** The admission path is already disposition, so nothing
  here changes when they ship.
- **The deadline sweep.** A Host never rejects on its own clock — it declares no
  seam for `RejectDispositionCommand` and leaves the deadline to Factory — so
  `admission.DispositionReconciler` (`dispositions.go`, the "dispositions"
  sweep, composed unconditionally) pages one control shard per pass bounded at
  **now**, and rejects with a zero residency only a command that is `pending`,
  or `claimed` under a lapsed claim, past its deadline and carrying **no
  attempt**. It claims under its OWN holder, `<replica id>/dispositions`
  (`dispositionHolder`): under the shared replica id the store treated its
  acquire as an EXTENSION of placement's live claim and its release freed
  that claim mid-attach for another replica (B5 v0.3.0 quality gate N1;
  `TestTheDispositionSweepDefersToThisReplicasPlacementClaim`). Its rotor and
  kept positions are the `rotor` type the legacy sweep uses, one copy.
  An `applying` command is never asked about: there is no
  caller-authored rejection once an attempt exists. The legacy `Reconciler` still
  runs, for legacy rows a store may already hold.
- **Neither sweep starves (the B5 spec gate's F3).** The due view is
  deadline-ordered from the head, and the head is where rows a sweep skips pile
  up — applying commands, live claims, and (for placement) expired commands. A
  pass that always re-read from the head would never again reach a row behind
  more than `MaxDuePerSweep*MaxConcurrent` of them. So `DispositionReconciler`
  and `placement.PendingSweeper` keep the store's continuation per shard, as the
  gate sweeper already did: a truncated pass's successor over that shard resumes
  from it, a pass that reaches the end re-arms at the head against a fresh
  bound, a continuation the store refuses (`InboxErrorCursor`) is dropped, and a
  transient fault keeps it. Retirement keeps the head short; the positions are
  what make each sweep correct when it does not. A resumed pass carries the
  bound its cycle started with, so a command admitted mid-cycle is met on the
  next cycle. `TestALiveCreateBehindMoreThanAPassOfExpiredRowsIsStillPlaced` is
  the gate's probe, committed. Every truncated pass of the commands,
  dispositions, gates and placement sweeps is logged at WARN (`warnTruncated`);
  it was computed and discarded.
- **What a rejection says.** The disposition record has no reason member, so
  `command.StatusForDisposition` describes an **attemptless** rejection — whose
  only producer in this fleet is that sweep — as `rejected /
  runtime_unavailable`, as the legacy sweep did. A rejection carrying an attempt
  (the store's not_applied settlement) stays unreadable.
- **Seam.** `factory.Commands` gained `AdmitDispositionCommand`,
  `GetDispositionCommand`, `PutCommandPayload`, `RejectDispositionCommand` and
  `ListDueDispositionCommands`, and lost `AdmitCommand` and `GetCommand`. A
  `*sessionstore.Store` satisfies it. Pre-1.0, a minor bump.

### B5: the attach caller, and what triggers it

**`OutcomeAttachPooled` is now acted on.** With `Config.Links` composed (the
root composes it over the HostLink pool as `placementLinks`), `Reconcile`'s
pooled arm runs `placePooled` under the claim: re-read the catalog and the
owner, page the candidates, ask each ranked admissible one to attach, and bind
with the epoch the **attach returned**. Without `Links` it still only names the
candidate, which is what every pre-B5 test drives.

- **The host fence is the candidate's capacity report**, and `attachRequest` is
  the only builder. `actor_id` is the sweep's service identity, `mode` comes
  from the catalog (`attachMode`: no journal progress and no checkpoint is
  `create`), and the idempotency key names the intent and **not the Host**,
  because section 15 step 5 retries "through the same key" and the retry may
  reach another Host.
- **Every answer has one meaning, in `attachAnswer`:** accepted → bind and wake;
  `epoch_mismatch` → the registry was stale, abandon the page, back off, re-run
  from the owner read (bounded by `ReplaceAttempts`, `ErrRegistryStale` after);
  any other code, `runtime_unavailable` included → this candidate refused, try
  the next; `ErrAttachUnsupported` → exclude and **log by Host id**;
  `ErrHostUnreachable` → skip; `ErrAttachFailed` → this candidate failed, log
  it and try the next; anything else → **abort**, because the request may have
  reached the Host. `current_lease_epoch` is never read.
- **Classification lives in the composition's adapter** (`classifyAttach` in
  `compose.go`), because this package names no transport type for routing's
  reason. Failures before the request left the process -- including
  `hostlink.ErrUnsupportedProtocol`, a link made terminal by a wire-version
  change -- become `ErrHostUnreachable`. A Host's own code-less ANSWER, which
  `hostlink` now surfaces as `*HostFailure` (centrifuge-go returns `*Error`
  only for a reply the server sent), becomes `ErrAttachFailed`, and placement
  tries the next candidate. That reply is NOT a placement outcome, and NOT a promise the Host rolled back:
  host v0.2.1 also sends it after an incomplete rollback, and when the session
  IS resident but its observation could not be published. Moving on is safe
  because the session LEASE guards residency: while that Host holds it, the
  next candidate refuses `epoch_mismatch` and placement converges on the
  owner, so no second residency forms (B5 v0.3.0 gates N2/C1). Aborting on it
  let one Host whose launches always fail --
  never losing capacity, so ranked first every pass -- block placement for its
  whole agent and runtime (B5 quality gate Q1). No per-candidate backoff is
  kept: the reconciler holds no state across passes, a failed attach is one
  fast RPC, and a Host that cannot launch is the Host's to stop advertising.
- **The pool evicts a terminal link.** `Pool.Attach` drops a link whose error
  is `ErrUnsupportedProtocol` or a terminal `*HostDisconnect`, with every route
  naming its Host, and closes it, so the next caller dials afresh instead of
  being refused by a dead link until the reaper -- which a viewer route could
  pin forever -- collected it (B5 quality gate Q2).
- **The record is re-read under the claim in `Reconcile` too.** The dedicated
  arm handed `EnsureWorkload` the intent from the PRE-claim read, so a racer's
  newer desired generation could be answered with an obsolete workload (B5
  spec gate F2); `TestTheDedicatedArmDecidesFromTheRecordReadUnderTheClaim` is
  the gate's probe. `placePooled` still re-reads for its own window
  (`TestAPlacementChangedDuringThePooledArmIsUndecided`).
- **Backoff:** defaults 3 attempts from 200ms, doubling, each wait capped at 5s
  (the old shift overflowed negative near 35 attempts), `ReplaceAttempts` at
  most 10, and the default wait sleeps uniformly in `[d/2, d]`. The `Wait`
  seam receives the nominal duration.
- **Placement runs over the real pool in a test.** The scripted links'
  `RouteFor` now follows binds and unbinds and refuses a cross-Host rebind as
  the pool does; `TestPlacementOverTheRealPoolGivesBackItsTransientRoute`
  drives `hostlink.Pool` itself, because a flag-answered `RouteFor` let a
  route-leaking reorder survive the whole package (quality gate QM24).
- **A replica that places nothing says so.** `Start` logs a WARN when
  `WithPendingCommands` is absent, ERROR when `WithPublicCreates` is composed
  too (B5 spec gate F1); requiring or deriving the option is a later minor.
- **The owner and the record are re-read under the claim.** The pre-claim read
  is what lets an owned session skip the claim; acting on it after the claim
  would widen the window between the read and the claim into the whole
  acquisition. `TestTheOwnerIsRereadUnderTheClaim` is the reader.
- **The route is transient.** `bindAndDeliver` binds, delivers the wake, and
  unbinds a route it created; a route that already existed (a viewer's) is left
  alone. Leaving placement's routes in the pool would pin links against the
  reaper and make the next placement of the same session to another Host a
  binding conflict.
- **An owned session with a wake is bound with the REGISTRY's epoch** (section
  15 step 1) and takes no claim; with nothing to wake it touches no Host.

**The trigger is `PendingSweeper`, a sweep over the disposition inbox**
(`pending.go`), composed only with `WithPendingCommands`. The inbox files every
open disposition command at its apply deadline, so one page at
`now + ApplyDeadline` is every command still open. A session is placed when it
has a pending or claimed command inside its deadline, or an **applying** one at
any age (only a successor runtime settles it); the wake is its live pending
commands. It is a sweep and not admission because I1.2 case 3 kills the
admitting replica, and section 10.4 lets any replica reconcile due work; an
admission-time kick would be a latency optimisation on top, and is not built.

**`WithPlacementController` is no longer required.** Nothing ever read it but
the required-seams table; `EnsurePlacement(DesiredWorkload)` cannot identify a
workload. The option and type remain (removal is a break) and are marked
`Deprecated`.

**Closed in v0.5.0 (Gap 1):** the gap stated here -- a pooled Host reachable
only for the tenant its advertised endpoint named -- is gone. Core v0.10.0 names
the convention (`HostLinkEndpoint`) and the pool derives per tenant; see "Gap 1"
under The HostLink.

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

### A released dedicated session is placed again (D3.1 F2)

Deletion desire is a dedicated placement naming **no** workload. Before this, a
later command for such a session was admitted and never placed: the dedicated
arm handed the controller the released intent every pass, and controller
v0.2.0's adapter refuses an empty payload (`unsupported workload payload
version`). Now `Request.OpenWork` (set by `PendingSweeper` for every session it
reconciles) licenses `reviveReleased`: the record is re-expressed as a **new
desired generation** carrying `CatalogRecord.PublicCreate.InitialWorkload` —
the template the session was **created** with, SessionStore's immutable
provenance — and only then ensured. Rules:

- It runs **before** the `Workloads == nil` check: desire is Factory-authored
  and the controller driver never writes it, so a controller-less replica (H5's
  split) must be the one that re-expresses it.
- **Only a release is replaced.** The write is a revision CAS; a conflict
  re-reads and re-decides, so a racer's non-empty workload is never
  overwritten, and a racing re-release is answered with another generation.
- The key (`reviveKey`) is the intent **plus the released generation**:
  replicas re-expressing one release collide and are absorbed, while each later
  release gets a key of its own (SessionStore compares only the current key).
- No provenance (a `CreateCatalogEntry` session) or an empty `InitialWorkload`
  is `ErrNoLaunchTemplate`, by name, with nothing written. The configured
  `LaunchTemplate` is deliberately **not** consulted: a returning session's
  workload must not depend on configuration drift since it was created.
- **Open work wins over a release.** A command still open when a product writes
  deletion desire re-expresses the workload; a product that means "gone for
  good" must let its open commands settle or expire first (the reviver does
  not read session state; the controller driver ignores a `stopped` session).

Not built here: drain-before-delete of D2.2, and the authorship of a dedicated workload's
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
the repair is `routing.Relay`'s, which learns from below that a route is
unusable and asks `Demand.Rebind` rather than waiting for an ownership poll.

**Demand is the only lifetime rule.** `Deliver` requires demand and does not
open a route, so a caller delivering to an unwatched session must take demand
first — **through `Demand`, never through `Bindings` directly**, which is the
`A7.2-sole-demand-holder` decision below; this paragraph used to say "brackets it
with `Acquire`/`Release`", which is an instruction to become a second holder of
this table's demand and is the hazard that decision settled. The pool's idle
window absorbs the cost either way. A delivery
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

## Bounded delivery and backpressure repair

A7.3 added `internal/realtime/delivery` (the bounded queue, above the transport)
and `routing.Relay` (the repair that an overflow owes). Since v0.4.0 both are
composed: `internal/realtime/livetail` feeds the Relay from the HostLink
subscription and implements its `Publisher` over the ClientLink (see "The live
tail").

**The invariant is one sentence: an enduring record is never discarded
silently.** Every bound either evicts a record carrying no durable content, or
reports an overflow that is repaired by telling the affected consumer — and only
the affected consumer — where to read from. That is `wui`'s U2.2 inversion
ported to Factory's side of the same stream: dropping the oldest enduring frame
and reporting it is silent durable loss *with a receipt attached*, because
nothing redelivers the frame and the reconnect cursor walks past it as soon as a
later one is applied.

**`Enqueue` has three outcomes, not two.** `nil`; `ErrDropped`, an arriving
*ephemeral* record a full durable queue had no room for, which lost nothing and
owes no repair; and `ErrOverflow`, which leaves the queue **unchanged** and owes
one. Collapsing the first two failure classes would make every busy token stream
repair a queue that is intact.

**The eviction policy is `wui`'s `selectFrameToDrop`, including its ordering.**
The busiest declared coalesce key loses its oldest member; with no keyed
ephemeral present, the oldest ephemeral goes; otherwise there is **no victim**.
Ties between two equally busy keys are broken by the age of their oldest member,
so the answer is a function of the buffer and not of map iteration order. The
coalesce key is **Factory-local and never on the wire**: Core gives an ephemeral
publication no identity at all, so coalescing without a declared key would be
guessing that two opaque bodies are interchangeable.

**The two queue bounds are two numbers and must stay two.** A HostBinding's
route queue holds one session's inbound tail; a DeliveryBinding's queue holds one
client's outbound copy, and a session with thirty subscribers holds thirty of the
second and one of the first. `TestEachBoundIsAppliedAtItsOwnSite` drives 5 and 3
to two absolute answers, so a relay that passed one constant to both sites fails
— the defect `internal/httpapi` measured on its two page ceilings.

**The bytes are forwarded, never re-encoded.** `ParseEnduring` reads the routing
and sequence envelope through Core's own decoder, which keeps the body as opaque
`RawMessage`, and the record queued is the caller's own slice. Both byte
assertions use a fixture written in a member order Core's marshaller does not
produce, so a re-encoding path fails where a canonically ordered fixture would
pass either way.

**The watermark is a bounds PAIR against two different quantities.** *Below its
own event sequence* is **Core's** rule — `EnduringPublication.Validate` requires
`covered_through` to equal `journal_seq`, because a live publication covers
exactly what it committed — and this package classifies that refusal rather than
restating the comparison, keying on the typed error's `Field` so it cannot be
confused with an unrelated decode failure (`ErrMalformed` is the control).
*Above the Host's committed append sequence* is this package's, and
`Frame.CommittedAppendSeq` is an **input**: a record cannot vouch for itself.
Both bounds are driven at their exact values in both directions.

**`CommittedAppendSeq` is a declared gap, and the gap is narrower than it first
reads.** Core v0.9.1 **does** define the session-channel record bodies and the
HostLink framing for their per-session channel —
`enduring_publication`, `ephemeral_publication`, `journal_tip`, `session.reset` —
and `Relay.classify` dispatches on exactly that discriminator. The transport
framing gap is closed, and since v0.4.0 this module subscribes to the session
channel, but there is still no **member on any record** from which a Host's
committed append sequence could be read. That makes the upper watermark bound
unimplementable from the wire, so `livetail` passes the publication's own
`covered_through` -- a DECLARED VACUOUS fence, since Core requires
`covered_through == journal_seq`; an additive Core member can tighten it. The field
is the seam the Host half will fill; until it exists the honest reading is "what
the producer declares", and the relay fences against it.
**For the same reason there is no ephemeral producer in this module**, so the
ephemeral policy is driven at `Receive` and at the queue's own contract rather
than through a stream that carries nothing. The dispatch is on Core's own
discriminator, so a caller cannot mislabel an unsequenced delta as durable data,
and a repair **control arriving from a Host is refused**: a `session.reset` names
what *this replica* forwarded in order, which a Host is in no position to know.

**A `session.reset` names what was DELIVERED, not what was queued.** Everything a
repair discards was never applied, so a reset built from the queue would tell a
client it holds records it never saw — a hole with a cursor asserting the hole is
covered. "Contiguous" is enforced rather than assumed: a gap **sticks**, because
a client told it has everything through a sequence it does not have will never
read the hole. Zero is the honest answer for a binding that has vouched for
nothing, which Core allows and which is required here because **Factory does not
retain the browser's own durable cursor** (A6.3 step 3).

**Repeatability is a property of the encoding, not of a function.** The record
carries two absolute sequences and no nonce, attempt count or sequence of its
own, so two resets for one `(last, tip)` are the same bytes;
`FuzzTheSessionResetIsAFunctionOfItsSequencePairAlone` derives that space rather
than sampling it, and a second `Repair` supersedes a pending control rather than
queueing beside it — which falls out of clearing the buffer rather than being a
second rule.

**A HostBinding repair captures ONE tip for every binding.** Two reads could
capture two tips, so two clients of one session would be told two different
places the journal had reached. Each binding's reset still differs, because
`LastContiguous` is that binding's own. The order is stop, capture, reset each
binding, rebind, resume after the tip, and it is **asserted as an order**: a
resume before the rebind restarts a tail on a route being replaced.

**Fail closed, four ways, each closing only what it must.** A tip that cannot be
read **during a HostBinding repair** closes every affected ClientLink and leaves
the tail stopped, because a reset naming no tip is not a repair instruction. A
tip that cannot be read **during a DeliveryBinding overflow** closes that one
link, for the same reason one level down. A reset Core refuses as incoherent
closes that one link. A publish failure — as distinct from `ErrWouldBlock`,
which leaves the record queued — closes that one link. Every step of a host
repair is attempted and every failure joined, so a tail that would not stop does
not leave the clients unreset.

**The second of those four was the last unread one, and it is the one that
mattered most.** Three mutations of `fanOutLocked`'s tip-read branch survived the
whole module — close every binding, close none, and *return nil and keep
streaming*. The third does exactly what the branch's own comment forbids: a
binding that overflowed and cannot be repaired goes on being published to, and
the client's cursor walks past the hole. It is the sibling of the step-4 arm one
call site up, which was read and had just been strengthened; the sweep that
strengthened it covered "only that one" claims and did not descend to the
fail-closed tip read beneath them. All three die at the error assertion and the
closed set; the case's final stream arm is a **hedge** against a future edit that
swallows the failure *and* closes something, and it says so — an earlier version
of this paragraph claimed the mutations died on that arm, and stripping the arm
was measured leaving all three still dying.

### The checklist that would have caught all five at once

Five findings across this task were one class: **a claim about what a
state-destroying action did not touch, sampled before anything pumped the
observable.** Asking two questions of every `Close`/`Clear`/`Repair`/reset call
site is cheaper than the rounds it took to find them one at a time:

1. **Is it sampled at all — PER STATEMENT?** `fanOutLocked`'s fail-closed arm
   failed this outright: nothing asserted about it in any direction. And
   `Queue.Close` failed it *at a leaf*: it has two statements, `q.closed = true`
   and `q.buf = nil`, and every refusal a test drove was answered by the first
   alone, so deleting the second survived the whole module. **Sign off a
   function, not a statement, and you have signed off half of it.**
2. **What is the positive observable that would differ if the action touched
   something it should not have?** A peer's *queue* is not one unless the peer's
   queue is non-empty at that instant — a fixture that pumps after every record
   makes every peer's queue empty, so only the *reset* half is testable.
3. **Is anything between the action and the observable NOT SYNCHRONOUS with the
   call?** A reset is **queued, not published**; a "no reset for the peer" check
   read before that peer's own next pump cannot be non-zero whatever the relay
   did.

**Question 3 is keyed on asynchrony, not on object identity, and the difference
is a correction.** An earlier version of this note said a "leaf" action — one
whose observable is the object acted on — needs no pump question. That is sound
in the positive direction and unsound in the negative: it is true here only
because every leaf in this package is a mutex-guarded synchronous buffer, and it
will get the first asynchronous leaf wrong. Worse in practice, the *label*
waived question 1 as well, which is how `Queue.Close`'s second statement went
unread. **A leaf earns a shorter answer to question 3 and no relief at all from
question 1.**

**Every "only that one" claim is asserted against an observable**, not against an
absence: a wrongly repaired peer has a `session.reset` in its stream, a wrongly
repaired session has a tail stop and a rebind naming it. Each case carries the
positive half in the same function, so the repair is visible as a difference
rather than as nothing having happened.

**Three things about HOW those cases are built, each of which was a surviving
mutant first.** A peer's queue must be **non-empty at the instant the victim
overflows**, or the "clear only that queue" half is untested — a fixture that
pumps after every record leaves every peer's queue empty and can only see the
*reset* half. A reset is **queued, not published**, so a "no reset for the peer"
check must be sampled **after that peer's own next pump**; read before it, the
observable cannot be non-zero whatever the relay did. And a reader for
`host.queue.Clear()` needs **exactly one** backlog record: with three, the
uncleared backlog re-overflows the delivery queue and a second repair masks the
defect behind a plausible-looking reset.

**A sequence is not a count, and a fixture must make them different numbers.**
Every contiguity assertion here uses sequences well above the number of records
delivered (11 vs 2, 14/13/12 vs 4/3/2, 65 vs 5), because a mutant naming
`forwarded` instead of `lastContiguous` is invisible in any fixture where the two
coincide. Four readers had that collision and all four were changed.

**`Unsubscribe` is the third "only that one" claim** and it is read in its own
right: a mutant closing every binding of the session passed the whole module
while the only case calling it held a single binding.

### `A7.2-sole-demand-holder`: one demand holder per session, and it is `Demand`

`Bindings.Release` only unbinds on the **last** release, because it counts. A
second holder therefore makes a release decrement without unbinding,
`route.bound` stays true, and `routeLocked` hands the next caller a route nobody
re-read from the registry — precisely the state a repair exists to leave behind.

The decision is that the invariant is **one holder**, not that `Release` becomes
owner-aware, and the reason is that owner-awareness **would not fix it**: two
holders means the route legitimately outlives one holder's release, so the
stale-adoption window is a property of there being two, not of the counting.
Owner-awareness would also require `Bindings` to carry a second identity concept
beside `BindingKey` and to take an owner token on a seam A4.3 deliberately kept
in Core's vocabulary.

So the repair plane takes **no** demand. `Demand.Rebind` is the seam instead: a
release then an acquire, the same two statements a poll runs, leaving the
subscriber count unchanged. `Bindings.Deliver`'s doc, which used to instruct a
caller to bracket a delivery with `Acquire`/`Release`, is **corrected** in place
— that instruction is the hazard.

Two readers. `TestASecondHolderOfTheRoutingTablesDemandMakesARebindStale`
records the hazard by asserting the defective outcome, with a message telling a
later reader to re-read the argument before deleting it.
`TestTheRoutingTablesDemandIsHeldOnlyByTheDemandPlane` is the structural half,
and its subject is the **module**, in two arms: inside `internal/routing` every
demand-taking reference must sit in a method on `*Demand`, and any *other*
production file that imports the package may not name `Acquire` or `Release` at
all.

**The second arm exists because the first version's scope claim was false**, and
a gate proved it. `internal/` bars other *modules*, not other packages of this
one, and `Bindings`, `NewBindings`, `Acquire` and `Release` are all **exported**
— so a second holder one package over compiled and passed everything, and that
is precisely the shape A9.1's composition has, since the composition root is what
hands a `*Bindings` to anything. The second arm is deliberately wider than the
hazard; wider is the safe direction for a ban, and if A9.1 legitimately needs one
of those names it fails loudly and a human re-reads the argument.

**The unit is the SELECTOR, not the call.** `take := b.Acquire` is a method
*value* with no call expression to match, and it was the cheapest spelling of the
hazard and escaped a call-keyed scan. Matching every `.Acquire`/`.Release`
selector covers calls, method values and method expressions in one rule. The
import path is **derived end to end and then checked against the compiler**: the
module half from `go.mod`, the package half from `filepath.Rel`, and the result
against `reflect.TypeOf(Demand{}).PkgPath()`. Deriving alone was not enough, and
the difference is worth keeping: the module arm reaches zero files today, so a
*wrong* import path is invisible in its result — "0 elsewhere importing it" is
the expected value either way — and the arm's own fixture control is built from
the same function, so both sides move together. `PkgPath` is the import path the
linked binary was built with, so requiring the two derivations to agree turns a
corruption of either into a present-tense failure instead of a silently vacuous
arm. **This is the third correction of that class in this repository.** Three
anti-vacuity arms, a seven-case fixture control and an import-detection control;
the module arm reaches zero files today and the test **logs** that too. What it still cannot see is a call
through a func value reached from a field, map or slice, and a wrapper spelled
some other name.

## The live tail (Gap 3, v0.4.0)

**A watched session's live output reaches the viewers watching it.** Host v0.2.1
publishes one `EnduringPublication` per committed public event on
`HostLinkChannel(tenant, session)`, **with no history**; before v0.4.0 Factory
never subscribed, so everything a Host published went nowhere.
`internal/realtime/livetail.Plane` composes what already existed --
`routing.Relay` (complete since A7.3, constructed in `composeLive` for the first
time), `routing.Demand`, the HostLink pool -- and adds three edges:

- **A HostLink subscribe AFTER a successful bind, on the same connection.**
  `Plane.Bind` is the routing table's `Binder`: `pool.Bind`, then
  `pool.Subscribe`; a subscribe that fails undoes the bind and fails the call,
  so a route this replica holds always carries a tail. A Host accepts a
  subscribe only on the connection holding the bind (`MaySubscribe`), and the
  pool refuses a subscribe for a session it holds no route for. A subscribe is a
  transport operation, not a HostLink method, so it needs no `hostlink_methods`
  entry. Placement's transient binds go through `placementLinks` and do NOT
  subscribe.
- **A ClientLink publish** to `session:{tenant}:{session}`
  (`clientlink.SessionChannel`, the demand grammar's inverse and the channel wui
  subscribes to), **channel-wide, once per record**. The Relay holds ONE
  DeliveryBinding per session (`channelLink`), so its reset is the channel's;
  the per-viewer bound is the transport's byte queue plus `DisconnectSlow`
  (3008), and a disconnected viewer repairs on reconnect (wui U2.1).
  `CloseLink` becomes `Handler.CloseSession`: every viewer of that session is
  unsubscribed server-side with 2000 (below the 2500 resubscribe band), off the
  calling goroutine because the OnUnsubscribe it triggers takes the Engine's lock.
- **A per-session drainer** that feeds `Relay.Receive` + `Pump` from the
  subscription's sink, in order. The sink runs on centrifuge-go's callback
  goroutine -- which for a publication is the goroutine READING the connection
  -- so it never blocks: it appends to a bounded mailbox (the Relay's
  HostBinding queue size). A mailbox at its bound is a LOST tail (repaired
  below), never unbounded growth.

**The rule: a Host keeps no history, so every tail START is followed by a
session.reset, sent AFTER the tail is live -- except the one start inside the
first viewer's own subscribe.** That start is gapless: `Engine.Bind` calls
`Demand.Acquire` before the viewer's subscribe is acknowledged, so its durable
read comes later. Any other start (a poll binding a session that had no owner
when the viewer arrived, a re-bind after a repair, an owner change) may have
missed records every current viewer relied on, and wui's join trusts live
publications without gap detection (private records make public sequences
sparse), so a missed record would be SILENT. `routing.Watcher` is how the plane
tells the two apart (`Watching` before the first serve, `Served` after), and
`Relay.Resync` is the reset: one tip, each binding's own last contiguous, no
tail touched. It is queued behind the live marker in the mailbox, so it is
published before anything the new tail carries.

**Every tail that STOPS without being asked is `Relay.HostLinkClosed`**: stop,
one tip, reset every binding, `Demand.Rebind` (unbind + bind, and the bind
re-subscribes), resume. A refused record (the Relay would not classify it) is a
hole and takes the same repair. Every OWNER-initiated stop -- `Plane.Unbind`,
which `Bindings.Release`/`Observe` reach when the last viewer leaves or the
route drops, and `Tail.Stop` -- unsubscribes, because **a Host never
unsubscribes a Factory**, not on unbind and not on `InvalidateSession`.
`Pool.Unsubscribe` reaches every link OF THE SESSION'S TENANT, not only the routed one, because the route may
already be gone. The plane gives each subscription a generation, so a stopped
or replaced tail's late publication never reaches the Relay.

**Reconnect ordering is owned here, and centrifuge-go's own resubscribe never
runs.** centrifuge-go moves every subscription back to subscribing on a
transport close and resubscribes it the moment the next connection is up
(`client.go:1476`, `:1531`) -- before anything is re-bound there, so a Host
refuses it, or, if a queued bind raced ahead, ACCEPTS it and the tail restarts
over a hole. So: `onConnecting` ends every tail (`endAllSubscriptions`),
synchronously removing the entries so every old callback is ignored, and tells
each sink `Ended`; the transport-side withdrawal runs on its own goroutine
(normally inside the reconnect delay, when an Unsubscribe is local-only) because
`Client.Close` holds the client lock while it drains this callback queue. The
repair's re-bind fails fast (`ErrLinkReconnecting`), leaving the session unbound;
`onConnected`, once the new reply is verified, tells each orphaned sink
`Restored`, and the plane re-binds -- which subscribes -- and that new tail's
start sends the reset. **Re-bind, then subscribe, then reset.** An owner
`Unsubscribe` does NOT cancel the orphan: the repair stops the tail through that
very call before its re-bind fails, and Restored is then the only thing left
that re-binds (measured; `TestAnOwnersUnsubscribeDuringTheReconnectStillHearsRestored`).
A server-side unsubscribe with a resubscribe code (>= 2500) is ended the same
way rather than tolerated. No callback takes a lock anything calling the
transport holds: `Plane.mu` and the link's `mu` are leaves, and the Relay is
called only from a drainer goroutine. **What IS held across a Host round trip,
stated because v0.4.0's first cut claimed nothing was:** `routing.Demand`'s one
mutex across a bind (`Acquire`, a poll, `Rebind` -- and `Plane.Bind` waits for
the subscribe's answer too), and the pool's across a bind RPC, as before. So a
slow Host delays other sessions' binds and polls. It no longer delays their
DELIVERY: the Relay's one mutex used to be held across a repair's stop, tip read,
rebind and resume, and one session's 3s bind stalled an unrelated session's
record 2.9s (quality gate F3); `Relay.repair` now does that I/O with the lock
released, re-locking only to apply the resets.

**Every transport registry change is serialised under the link's `regMu`, and a
withdrawal only acts on its own subscription** (spec gate F2). centrifuge-go
keys its subscription registry, and the unsubscribe it sends a Host, by CHANNEL
NAME, so a late discard of an old subscription -- the second one every owner
unsubscribe used to provoke through `OnUnsubscribed`, or a late
`endAllSubscriptions` withdrawal -- unsubscribed a NEWER tail at the Host while
the link believed it live. `regMu` is never taken on a callback goroutine
(`Client.Close` holds the client lock while draining callbacks; the gate's
inline-discard mutants hang the suite). Likewise a mailbox overflow no longer
starts its own unsubscribe: the repair's `Tail.Stop` withdraws the tail in
order, and an unordered withdrawal landing after the re-subscribe removed the
new tail (both gates' F1). **Since v0.5.0 the subscribe itself is sent under
`regMu` after re-checking the entry is current** (regate N4): a withdrawal
between registration and send used to leave an orphan subscription at the Host.
Both regMu rules are held structurally by `regmu_structure_test.go`, whose
anti-vacuity half applies the regate's mutants X2d/X2e/X2f/D1/D2/D3 verbatim to
the live source.

**`routing.Relay` requires calls for one session to be serialised by its
caller** (regate N1). It releases its mutex across a repair's I/O, so two
concurrent repairs of one session can publish a reset whose tip goes backwards;
composition meets the precondition because the plane calls the Relay only from
the session's single drainer. A new caller must do the same.

**A tip read that fails during a repair closes every affected viewer and STILL
re-binds** (quality gate F1). It used to leave the tail stopped with the route
held, and nothing -- no poll, no restored link -- ever re-bound it: a session
silent for as long as it was watched.
**A bind that meets a terminal link now evicts it**, as attach did since B5: a
viewer's route otherwise pins a dead link and the re-bind never reaches the Host.

**Known limits, booked rather than hidden.** (1) A private record between two
public ones makes the Relay's `lastContiguous` stick (Core requires
`covered_through == journal_seq` on a live publication), so a reset names an
older sequence than it could and viewers over-repair; advancing it needs a
journal read over the gap, not the tip read, and is not cheap. (2) Host silence
still looks like idle while BOUND: the `journal_tip` hint (now published to the
same channel instead of the old `unpublishedHints` refusal; `ErrNoHintPublisher`
is kept, Deprecated) runs only for an UNBOUND watched session, and wui ignores a
hint in its live phase. (3) `Frame.CommittedAppendSeq` is a vacuous fence
(above). (4) A placement transient unbind racing a viewer's bind of the same
session can drop the viewer's pool route (routes are not reference-counted);
the tail keeps flowing -- a Host publishes regardless of bind -- and since the
fix round the next ownership poll notices the missing pool route
(`routing.RouteReporter`, reported by the plane) and re-binds. Before, the poll
trusted the routing table's own binding and never did (spec gate C1). (5)
`Demand`'s mutex across a bind means one slow Host delays other sessions' binds
and polls, not their delivery (above).

## Current composition boundary

`factory.New` composes the public router, admission, ClientLink, HostLink pool,
placement sweeps, disposition reconciliation and live tail. `Start` runs their
background work. `WithPendingCommands` is required for pooled placement; without
it the replica reports that it will place no sessions. Factory ships no UI and
no binary; a product mounts its own UI through `WithUIHandler`, `WithUIFS` and
`WithUIRoutes`. `internal/placement/kubernetes` is absent from this root
module.

The service exports no Prometheus metrics handler. Do not invent backlog,
resident-wait, queue, reconciliation or drain series in an operations example.
The placement seam does not implement dedicated drain-before-delete; that is
owned by the workload controller. A cold AskUser answer/resume remains
unsupported. SessionStore's legacy object-first `PutObject` does not imply a
Host disposition `SessionObjectStore`.
