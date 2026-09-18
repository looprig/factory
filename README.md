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

The seams a deployer supplies are `identity.Verifier`, `Authorizer`,
`SessionReader`, `Commands`, `Directory`, the CSRF configuration, and
optionally a `Clock`, a `UUIDSource`, a session cookie name, a default tenant,
limits, a UI, a `*slog.Logger` (`WithLogger`) and the `PendingCommands` query
that triggers pooled placement (`WithPendingCommands`). `PlacementController`
is no longer required and nothing reads it; the option is still accepted so an
existing composition keeps composing. Authentication is supplied as a VERIFIER and
not as an authenticator: Factory composes exactly one authenticator, because
which credential authenticated a request must have one answer and a second one
is an origin guard that skips its CSRF rules silently. The
UI is optional in both shapes: `WithUIHandler` for a handler and `WithUIFS` for
a static bundle, mutually exclusive, and **a composition with neither is valid
and is exercised as such** -- the default binary mounts the Vite bundle, and an
embedder that mounts its own or none composes the same `Server`.

## Serving

`Server.Handler` is Factory's public HTTP surface: the API under `/v1`, and the
injected user interface everywhere else. The injected UI is handed to the router
as its fallback rather than mounted above or beside it, which is what makes
**API routes take precedence over the SPA**: the router splits by path before
authentication, so `/v1/unknown` is a JSON `route_not_found`, never
`index.html`.

`Serve` and `Stop` are the OPTIONAL half. An embedder that owns its own
`http.Server` uses `Handler` and never calls them. A deployment that wants
Factory to own the server passes a listener it opened itself -- which address,
which network, whether a supervisor passed the descriptor in, whether TLS is
terminated here -- and gets the wire-level bounds `HTTPLimits` carries:
`ReadHeaderTimeout`, `IdleTimeout` and `MaxHeaderBytes`. There is deliberately
no `ReadTimeout` and no `WriteTimeout`; both are absolute per-connection
deadlines, and this surface streams object bodies and will carry a WebSocket.
`Stop` is idempotent, does not return until what it stopped has stopped, and
stops **public admission first** so nothing new is admitted while the background
components -- when they exist -- are torn down. It never touches a Host runtime:
a session outlives every Factory replica.

What `New` composes today is that surface and nothing else -- the authenticator
built from the deployer's verifier, the origin and CSRF guard, the router and
the optional UI. Command admission, placement, the ClientLink and HostLink
engines and the multi-replica reconcilers are separate tasks; the seams they
will use are validated at composition and then held unread. The router is built
with an empty launch `Department`, a nil object policy and no object-store
resolver, each of which fails closed.

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

## Realtime transport

The realtime transport is pinned exactly. The pins below, the constants in
`internal/realtime/transport/doc.go` and `go.mod` are held equal to one another
by tests in that package -- including this table, which is read from this file,
so it cannot drift into a fourth unguarded copy of the version triple:

| Role | Module / package | Version |
| --- | --- | --- |
| Embedded server, for ClientLink | `github.com/centrifugal/centrifuge` | `v0.38.0` |
| Go client, for HostLink | `github.com/centrifugal/centrifuge-go` | `v0.12.0` |
| Wire library both depend on | `github.com/centrifugal/protocol` | `v0.19.2` |
| Browser client | npm `centrifuge` | `5.7.2` |

**The Go client is deliberately not the latest, and cannot be.** Factory embeds
the server for ClientLink and dials with the client for HostLink in one binary,
so both resolve ONE `centrifugal/protocol` version. The compatibility test is
executable, and every version below was built and run rather than inferred from
a `go.mod` require line -- a require is a floor, not a statement about which
symbols a package exports:

| `centrifuge-go` | resolved `protocol` | `go build ./...` |
| --- | --- | --- |
| `v0.10.12` | `v0.17.0` | exit 0 |
| `v0.11.0` | `v0.19.2` | exit 0 |
| `v0.12.0` | `v0.19.2` | exit 0 (**pinned**) |
| `v0.12.1` | `v0.21.0` | exit 1 -- `undefined: protocol.GetStreamCommandDecoder` |

`protocol` v0.19.2 still exports `GetStreamCommandDecoder` alongside its
replacement; only v0.21.0 drops it. So the break is at `centrifuge-go` v0.12.1,
and `v0.12.0` is the newest client that coexists with the pinned server.
Reaching v0.12.1 means moving the embedded server to a release built against
`protocol` v0.21.0+, and it is that server move -- not the client move -- that
invalidates the compatibility measurement taken against the pinned browser
client and requires the spike be redone.

Everything the transport is trusted to do is measured against a real embedded
node over a real loopback WebSocket in `internal/realtime/transport`, including
the answers that are inconvenient: a stalled consumer loses its whole
connection, not one subscription, by either the connection queue bound or the
connection write deadline; and the connect reply carries the ping interval in
whole SECONDS, so a sub-second ping interval is silently not negotiated and the
server then closes healthy connections for a missing pong. The 5,000-connection
transport-only case is behind the `transportscale` build tag and makes no
durability or correctness claim.

History, recovery, presence, join/leave, client publication and WebSocket
compression are each **asserted** off rather than merely left unset. Compression
is asserted by speaking the RFC 6455 opening handshake directly from the
standard library and reading `Sec-WebSocket-Extensions` off the 101 response,
because the Go client surfaces neither the response nor the negotiated
extension; this deliberately adds no direct `gorilla/websocket` dependency. The
Redis and NATS brokers and standalone Centrifugo are **absent rather than
asserted-refused** -- the node keeps its in-process memory broker and no
external process is started. The suite exercises the JSON protocol only.

## The HostLink

`internal/realtime/hostlink` is Factory's client side of the Factory-Host
connection, split like the ClientLink: a `Pool` that decides and a
`CentrifugeDialer` that carries. The invariant is one sentence — a session
binding never costs a connection, and a connection is never shared between
Hosts — and the route table is keyed by tenant AND session, because a session id
is unique only within its tenant.

A failed command RPC is reported as `ErrCommandUndelivered`, which leaves the
already committed inbox record pending. A Host's own answer arrives as
`*HostRefusal` carrying Core's typed `HostLinkError` and is deliberately not in
that class, so a caller tells "never seen" from "answered" without reading
message text. The package imports no store and a parsed-import test holds that,
so it could not record anything about the record either way.

Several Factory replicas may hold their own connection to one Host. There is no
broker and no leader: closing one replica's pool leaves the others' connections
and routes untouched, measured over fakes and over real sockets.

**Core owns the HostLink framing contract, not only the record bodies.** Core
v0.9.1's `sessionwire/v1` defines the bare connect codecs, reserved method
names, and injective `HostLinkChannel` derivation that Factory and Host must
share. The asynchronous `{type, data}` push envelope is the only framing still
local to Factory; it is not a Core record or a HostLink RPC method. The Host half
does not exist in this repository, so these tests still run against a stand-in
node implementing the proposal. They pin Factory's side, but are not proof of a
live cross-module Host implementation.

**Three defects kept every Factory before the v0.2.0 release from ever holding a link to a
Host, and the stand-ins hid all three.** Host decoded the connect Data as a
bare `VersionNegotiationRequest` while Factory wrapped it (B8; Core v0.9.0 now
owns the connect codecs); a Host's bare refusal body was decoded through a
wrapped `{"error":…}` shape, so it decoded to `Error == nil` and a genuine
refusal was returned as **success** — fixed silently in `563f15e` and recorded
here; and the WebSocket upgrade named no subprotocol, which a Host answers HTTP
400 before reading a frame (`Sec-WebSocket-Protocol: centrifuge-json` is the
only route open, since Core's `InternalEndpoint` refuses the `?format=json`
query). Every stand-in now gates the upgrade as a Host does, and the dialer was
driven against the released `host v0.2.1` — dial, Bind and DeliverCommand
round-trip — in a gate-side harness that cannot live in this suite because
`host` may not be a Factory dependency.

Bind and unbind are gated by the capability set the Host advertised in its
connect reply and refused locally with `*UnsupportedMethodError` otherwise;
during a reconnect, before the new reply has landed, they are refused with the
distinct transient `ErrLinkReconnecting` instead. Command delivery is not gated
and is queued by the transport across a reconnect — and emitted before the new
reply is verified, because centrifuge-go resolves connect futures before it runs
`OnConnected`. RPCs on a link are concurrent: nothing is held across the RPC,
which runs on its own goroutine so that a centrifuge-go v0.12.0 double-callback
wedge costs one leaked goroutine rather than a caller.

The pool carries the control plane only. A7.3 moved the per-binding queues and
the backpressure repair *above* it, into `internal/realtime/delivery` and
`routing.Relay`; the live publication consumer itself remains absent. Core's
HostLink framing now defines the session channel and command-delivery shape, but
Factory has not yet consumed the live publication stream. Choosing which Host a
session belongs to is A7.2.
`factory.New` composes neither the pool nor the reaper's cadence; A9.1 owns that.

## Placement

`internal/placement` decides where a session runs. `Decide` is pure — a catalog
record, an optional registry observation and one capacity page in, one of five
outcomes out — and the reconciler around it takes the session's short-lived
reconciliation claim, stores desired state idempotently and hands a dedicated
session's intent to a `WorkloadController`.

A live owner is preferred before any claim is taken, and a replica that loses
the claim re-reads the registry rather than answering "busy": the winner may
have finished placing in between. `ReusableOwner` is the module's one statement
of "is this the session's own live owner"; `internal/admission` calls it.

Desired state is idempotent twice over. A content comparison keeps a replica
from rewriting an intent already stored, which is what holds the desired
generation still, and a key derived from the intent keeps a second replica from
applying it again — two Factories deriving one intent derive one key, and
SessionStore checks the key before the revision.

`WorkloadController` names no platform type and exposes the four domain
lifecycle operations `EnsureWorkload`, `ObserveWorkload`, `RequestDrain`, and
`DeleteWorkload`, all over Core and SessionStore records. The seam is
composition-only here: no scheduled reconciliation driver is built in this
module. H5 keeps the Kubernetes adapter out of this module entirely: it lives in
the separate repository `looprig/controller` (owner ruling 2026-09-18), which
consumes Factory only as a published module, so no Factory consumer inherits the
Kubernetes client graph;
`cmd/factory` supplies no workload create/delete RBAC, so a nil controller is a
supported configuration and a dedicated session reaching the tenant-facing
replica is refused by name. Widening this exported method set is a source
compatibility break for external implementations. **While this module is pre-1.0
(owner ruling 2026-09-18), such a widening ships as a MINOR bump** — the `v0.x`
contract every module in this workspace is released under — and the widening that
added `ObserveWorkload`, `RequestDrain`, and `DeleteWorkload` as `EnsureWorkload`'s
companions rides `v0.2.0`. Once `v1.0.0` is cut, widening it becomes a major
release.

**Pooled placement attaches (B5).** With `WithPendingCommands`, a
"placement" sweep pages one control shard of the disposition inbox per
interval and, for each session with an open command and no live owner, asks
the first ranked admissible candidate to `hostlink.attach` -- fenced by that
candidate's `host_id`/`host_generation` from its capacity report, with the
sweep's service identity as `actor_id` -- then binds with the lease epoch the
Host **answered** and delivers the pending commands as a wake. A candidate that
did not advertise `hostlink.attach` in its connect reply is never sent one: it
is excluded and logged by Host id, because an older Host routes the method as
a channel and answers `runtime_unavailable`, indistinguishable from a genuine
refusal. `epoch_mismatch` means the registry was stale: placement re-reads it
and retries, bounded with backoff, and never binds with the refusal's
`current_lease_epoch` (the other holder's). Every other coded refusal moves on
to the next candidate; a failure after the request may have reached the Host
aborts the attempt so no second attach is put in flight. A `tenant_exclusive`
pooled advertisement is still never selected, because Factory cannot see which
tenants a Host serves and refusing is the only enforcement of specification
section 12 available to it.

**One gap is declared rather than worked around.** The released Host serves
HostLink per tenant, at `/hostlink/<tenant>`, and its capacity report
advertises ONE `internal_endpoint`; Core names no convention for deriving a
tenant's HostLink address from a Host's. Factory dials the advertised endpoint
as-is and keys its links by Host, so a pooled Host is reachable here for the
tenant its advertised endpoint names, and an attach for any other tenant is
refused by that Host (`runtime_unavailable`, foreign tenant) and placed
elsewhere. Cross-tenant pooled placement onto one Host needs that convention in
Core first.

## Status

Seams and identity derivation. `contract.go` states the cross-service contract,
`server.go` / `options.go` state the composition, and `internal/identity`
derives principals; the HTTP API, admission, routing and placement are the
subject of later tasks in runbook 05. The realtime transport is pinned and
measured (A5.1), the ClientLink engine is built (A6.1) and the HostLink pool and
dialer are built (A7.1); `factory.New` composes none of the three. Target
discovery (A4.1), the placement policy and reconciler (A4.2) and the local
HostBindings table (A4.3) are built and likewise uncomposed.
