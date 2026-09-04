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
deployer implements `Authenticator` and `Authorizer` from OUTSIDE this module
and must be able to NAME every type in their signatures, so a public seam may
not mention a type from `internal/`. That is the whole reason `Principal` and
`CSRFConfig` live in the public `identity` package rather than in
`internal/identity` as runbook 05's file list anticipated; A1's derivation code
belongs beside them.

`internal/identity` is where that derivation code went: `Credential`, `Claims`,
`Verifier`, the `Authenticator` and `OperationContext` have no external
implementer, so none of them is vocabulary a deployer has to learn. What A1.1
added to the PUBLIC package is only what an implementer must NAME:
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
from the operation's shape, not from the table. The objects route carries
`awaitsObjectAuthorization`, which fails the suite the day it serves a body
without an object-level decision; `AuthorizeObjectRead` has no production caller
today.

**`owner` and `handle` are per METHOD, for the reason `auth` is.** A2.1 moved
authorization onto `methodRule` because `/v1/sessions` is a list and a create
and one rule for both had to be the weaker. A2.2 moved readiness for the same
reason: the list is served and the create is A3.1's, and a route-level column
could only call that route implemented — leaving the create's 501 unmeasured —
or pending, failing on the list it does serve. `TestARouteMayBePartlyImplemented`
is the anti-vacuity check: if no route were mixed, the finer column would buy
nothing.

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

**Do not add an HTTP method override.** The CSRF guard's subject is `r.Method`.
A route or middleware honouring `X-HTTP-Method-Override` or a `_method` field
would let a cross-site page present a POST as a GET: exempted as safe, then
executed as a write. The note lives beside the allowlist in `guard.go`, where a
route author will read it.

## Not implemented yet

Composition seams (A0.2), identity derivation (A1.1), the route and error
foundation (A2.1) and the agent and session reads (A2.2) are done; the remaining
read, control, admission, routing, placement and realtime handlers are later
tasks in runbook 05. Every method in `httpapi.routeTable` carries the runbook
task that fills its body in, and answers 501 until it does;
`TestTheUnimplementedMethodsAreExactlyTheOnesLaterTasksOwn` holds the two sets
equal. `httpapi.Directory` has no production implementation yet — **A4.1 owns
it**, and until then `/v1/agents` reports every pooled target as unadvertised
under any composition that supplies a directory answering empty pages. Nothing composes a `Router` into `factory.New` yet — server composition
is A9. Nothing wires the `Authenticator` into `factory.New` yet -- there is
no default authenticator option, because a deployment supplies the `Verifier`
and there is no credible default for one. `internal/realtime` now exists and holds the ClientLink
and HostLink seams. `cmd/factory` and `internal/placement/kubernetes` do not
exist; their exemptions grant nothing today and `TestBoundaryScopesAreNotStale`
will fail if one of those directories appears without a Go file in it. Do not
add a placeholder Go file to satisfy it: that would permanently satisfy a live
tripwire, trading a guard that fires the day a directory appears unearned for a
directory that is always "earned" by a file that means nothing.
