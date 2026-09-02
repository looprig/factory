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

## Not implemented yet

This module is a scaffold. Composition seams (A0.2), identity, the HTTP API,
admission, routing, placement and realtime are later tasks in runbook 05.
`cmd/factory`, `internal/realtime` and `internal/placement/kubernetes` do not
exist; their exemptions grant nothing today and
`TestBoundaryScopesAreNotStale` will fail if one of those directories appears
without a Go file in it. Do not add a placeholder Go file to satisfy it: that
would permanently satisfy a live tripwire, trading a guard that fires the day a
directory appears unearned for a directory that is always "earned" by a file
that means nothing.
