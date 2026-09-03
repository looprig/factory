# factory

`factory` is Looprig's public-facing orchestration service. It owns the HTTP and
WebSocket surface, authentication and authorization, durable reads, command
admission, target selection, Host links, multi-replica reconciliation, local
realtime fan-out, and optional UI composition.

Factory imports neither Host nor Harness. Everything the two services exchange
is a record from
[`github.com/looprig/core/sessionwire/v1`](https://github.com/looprig/core) or
[`github.com/looprig/sessionstore`](https://github.com/looprig/sessionstore),
and that is the whole of the contract between them.

Factory is deliberately in the data path for active clients at the expected
1,000–5,000 connection scale, and its required state is disposable: reconnects
and reads recover from SessionStore.

## The import boundary is a test

`import_boundary_test.go` parses every Go file's import block — production and
test — and applies five rules:

| Dependency | Permitted in |
|---|---|
<!-- boundary-rules:begin -->
| `github.com/looprig/host` | nowhere |
| `github.com/looprig/harness` | nowhere |
| `github.com/centrifugal/...` | `internal/realtime/...` |
| `github.com/looprig/wui` | `cmd/factory/...` |
| `k8s.io/...`, `sigs.k8s.io/...` | `internal/placement/kubernetes/...` |
<!-- boundary-rules:end -->

The rules are functions over PARSED import paths; nothing matches against source
text, and containment is compared by whole path segments, so
`internal/realtimefanout` is not inside `internal/realtime` and a second binary
beside `cmd/factory` does not inherit its exemption. The scan fails as vacuous
if it finds no files, and `TestScanReachesEveryRuleScope` builds a fixture module
containing each scope so an exemption whose directory does not exist yet is still
proven reachable and still proven to reject the location just outside it.

Two companion guards keep that scan honest. `TestModuleHasNoUndeclaredNestedBoundary`
fails on any nested `go.mod`, `.git` or `vendor` below the root — including the
`.git` **file** that a submodule or worktree writes — because such a directory
stops the module-owned walk and makes all five rules silently stop applying to
its whole subtree; a declared boundary is scanned in its own right rather than
exempted. And `module_pin_test.go` parses `go.mod`'s grammar to
assert there is no `replace` directive in either spelling — and with tokens
unquoted, since go.mod permits `require "github.com/looprig/host" v1.0.0` and a
split-only parser would skip it silently — and that every Looprig requirement
names its exact released version — `go mod verify` checks
content hashes, not version shape, so nothing else in `make check` would
notice. Modules Factory may never import are rejected there before the
released-version map is consulted, so the ban does not expire the day one of
them ships.

`go.mod` names exact released versions and contains no `replace` directive;
nothing here is vendored, so `GOWORK=off go test ./...` verifies the module
against the versions it actually pins.

## Composition

`factory.New` takes functional options and returns a validated `*Server`. An
option is applied at most once -- a repeat is an error, not a last-wins -- and
an explicit `nil` dependency is an error rather than a request for the default,
because the seams that have defaults are defaulted by their ABSENCE.

The seams a deployer supplies are `Authenticator`, `Authorizer`,
`SessionReader`, `Commands`, `Directory`, `PlacementController`, the CSRF
configuration, and optionally a `Clock`, a `UUIDSource`, limits and a UI. The
UI is optional in both shapes: `WithUIHandler` for a handler and `WithUIFS` for
a static bundle, mutually exclusive, and **a composition with neither is valid
and is exercised as such** -- the default binary mounts the Vite bundle, and an
embedder that mounts its own or none composes the same `Server`.

Each public seam is the union of the narrow interfaces the packages that CALL
it declare for themselves, in `internal/httpapi`, `internal/admission`,
`internal/identity` and `internal/realtime/clientlink`. There is no shared dependency package. The two
sets are held together by tests rather than by discipline, and the two
directions fail differently: widening a CONSUMER's interface still compiles and
fails `TestPublicSeamsSatisfyTheirConsumers`, while widening a PUBLIC seam past
its consumers fails the build outright, because the test fake that implements
every seam no longer does.

The public seams name only standard-library, Core, SessionStore and
`factory/identity` types, since a deployer outside this module cannot import
`github.com/looprig/factory/internal/...` to name anything else -- which is why
`Principal` and the CSRF configuration are in the public `identity` package in
the first place. `internal/realtime/hostlink`'s dialer is deliberately NOT on
that surface: the HostLink protocol is internal, and what composition configures
is the pool's limits.

## Identity

`internal/identity` derives a principal from an inbound credential: an
`Authorization: Bearer` header or the session cookie for REST, a connect token
for a ClientLink. Two properties are the reason it is a package rather than a
handler:

- **A tenant comes only from verified claims or from configuration.** Nothing
  in the derivation reads the URL, the path, a path value, the body, a form or
  the `Host` header, so a conflicting tenant in a request is not weighed and
  discarded -- it is never read. `TestTheDerivationNamesNoTenantCarrier` holds
  that over the PARSED file, because a behavioural sweep can only cover the
  carriers somebody thought of.
- **An `Authenticator` holds no per-process state.** Every decision is a
  function of the credential, the injected `Verifier` and the injected `Clock`,
  so a browser whose WebSocket reconnects to another Factory replica is
  authenticated there with no handoff. That is section 10 of the spec, and it is
  the same requirement that makes `WithCSRF` mandatory.

An `OperationContext` carries exactly a `Principal`, which credential
authenticated the operation, and a W3C trace identifier; the field set is
enumerated by test, so a header, a cookie or a raw token cannot be added to it
quietly. The credential source is there because CSRF applies to an ambient
credential and not to a bearer one, and `Authenticator.CredentialSource` is the
single reader of that precedence rule -- a CSRF guard that re-derived it could
disagree, and would do so by skipping CSRF rather than by failing.

For the same reason the operation context is built by a **method**,
`Authenticator.NewOperationContext`, which derives the source rather than
accepting it. Taking it as a parameter moved "which credential authenticated
this" back into the edge's hands one call further out, and a caller that wrote
`SourceBearer` for a cookie-authenticated operation would make the CSRF guard
skip. `NewOperationContextWithSource` remains for a caller that authenticated by
a route this package does not model, and is documented as the exception rather
than the default.

A `Credential` redacts under `%v`, `%s`, `%q`, `%#v`, `log/slog` and
`encoding/json`, because the likeliest way a token reaches a log is that
somebody formatted the value they were handed. Those are **not** independent
mechanisms, and the difference was measured: deleting `GoString` or
`MarshalJSON` changes what `%#v` and `encoding/json` produce, while deleting
`LogValue` changes nothing a `slog.TextHandler` prints, because its `KindAny`
path falls back to `fmt` and reaches `String`. `LogValue` is kept for the
handlers that reflect instead, and is held by a runtime `slog.LogValuer`
assertion rather than by a rendering.

Scrubbing a verifier's error text is byte-for-byte replacement, so it removes a
**verbatim** credential and nothing else. A verifier that embeds a prefix, a
digest or a re-encoding leaks that derivative, and
`TestScrubbingCoversOnlyAVerbatimCredential` pins the boundary from both sides
rather than leaving it to a caveat.

## Status

Seams and identity derivation. `contract.go` states the cross-service contract,
`server.go` / `options.go` state the composition, and `internal/identity`
derives principals; the HTTP API, admission, routing, placement and the realtime
engines are the subject of later tasks in runbook 05.
