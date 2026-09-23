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
| `github.com/looprig/wui` | nowhere |
| `k8s.io/...`, `sigs.k8s.io/...` | `internal/placement/kubernetes/...` |
<!-- boundary-rules:end -->

The rules are functions over PARSED import paths; nothing matches against source
text, and containment is compared by whole path segments, so
`internal/realtimefanout` is not inside `internal/realtime` and a second package
beside `internal/placement/kubernetes` does not inherit its exemption. The scan
fails as vacuous if it finds no files, and `TestScanReachesEveryRuleScope` builds
a fixture module containing each scope so an exemption whose directory does not
exist yet is still proven reachable and still proven to reject the location just
outside it.

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
and is exercised as such** -- Factory ships no UI of its own and no `cmd/factory`
binary exists or is planned. Users bring their own UI: a product supplies its
bundle or handler through these seams, and Factory mounts it behind its own
auth, origin and CSRF guards. Carbon, for example, supplies `wui.Assets()` to
`WithUIHandler`; an embedder that mounts its own bundle or none composes the
same `Server`.
Application-owned `/ui/` routes can be mounted with `WithUIRoutes`. Factory
authenticates each request, applies its origin and CSRF guard, and calls the
required route authorizer before the handler. These routes are independent of
the public bundle, so a deployment can provide either, both, or neither.
The protected handler reads the same verified principal passed to authorization
through `UIRoutePrincipal(request)`.

### A Host-owned session's journal: `WithSessionJournalResolver` (v0.10.0)

**A journal resolver is required with `WithPublicCreates` or
`WithPendingCommands`; `New` refuses either without one
(`ErrHostSessionsWithoutJournalResolver`, as an `*OptionError` naming
`WithSessionJournalResolver`).** Use `WithSessionJournalResolver`; the v0.9.0
`WithJournalResolver` still works and satisfies the requirement, but is
**deprecated**, and supplying both is refused (`ErrConflictingJournalResolvers`,
as an `*OptionError` naming `WithSessionJournalResolver`) rather than one
silently shadowing the other. A session a Host runs is
DISPOSITION-bound, and its journal is **not** in the SessionStore you hand
`WithSessionReader`: the runtime keeps it on its own backend under the
binding's `RuntimeSessionID`, and SessionStore never writes a public journal
for a disposition session. Read through `WithSessionReader` alone, every Host
session's journal is empty at tip 0, so `/journal` shows no history and every
live-tail repair after a delivered record UNSUBSCRIBES the session's viewers
(Core refuses a `session.reset` whose `last_contiguous` is above its
`journal_tip`) instead of resetting them. That was the stock composition up
to v0.8.x.

With a resolver, every journal read Factory makes -- `/v1/sessions/{sid}/journal`
(and its `journal_tip`), the `journal_tip` hint for an unbound watched session,
and the tip every live-tail repair resets to -- reads the catalog binding for
the public id and, for a disposition-bound session, reads
`resolver(ctx, tenant, session, binding)` under `binding.RuntimeSessionID`.
`session` is the PUBLIC session id the read is for -- the authorized
`/journal` route's session, or the watched session a hint, repair or gap probe
is for; the id the catalog entry and so `binding` were read by, never a value
taken from a cursor or the runtime journal. It is the only place that id
exists: the request the reader is then asked names the runtime id, a one-way
derivation of the public one. Cursors are
wrapped to the public session and its binding. A legacy-bound session is still
read from `WithSessionReader`. A resolver error is an error, never an empty
journal; return SessionStore's typed errors (or wrap them) so `/journal`
classifies them.

**Project the runtime journal to public identities with Host's
`NewPublicJournals`.** A Host's runtime journal bodies carry runtime identities
(the binding's `RuntimeSessionID`, runtime command ids) that must never reach a
client. `host` (>= v0.10.0) exports the projection; Factory cannot import
`host`, so the composition (Carbon, the tests lane) applies it. Share one
`*host.PublicJournals` across calls -- it caches each session's mapping (LRU,
`0` takes the default 1024):

```go
publicJournals := host.NewPublicJournals(0)

factory.WithSessionJournalResolver(func(ctx context.Context, tenant sessionwire.TenantID,
	session sessionwire.SessionID, binding sessionstore.SessionBinding) (factory.JournalReader, error) {
	if binding.StorageBindingID != myBindingID || binding.BindingVersion != myVersion {
		return nil, fmt.Errorf("unknown journal binding %q/%q", binding.StorageBindingID, binding.BindingVersion)
	}
	// One store per tenant, opened once and cached; closed at shutdown.
	store, err := journals.forTenant(ctx, tenant, func(ctx context.Context) (*sessionstore.Store, error) {
		return sessionstore.Open(ctx, runtimeBackend(tenant), sessionstore.WithLegacySingleTenant(tenant))
	})
	if err != nil {
		return nil, err
	}
	// Refuses (host.ErrPublicJournalScope) any request not for this tenant and
	// binding.RuntimeSessionID; bodies are byte-identical to the live tail's.
	return publicJournals.Reader(store, tenant, session, binding)
})
```

Without the projection (the deprecated `WithJournalResolver`, or a
`WithSessionJournalResolver` that returns the store directly) `/journal`
serves the runtime journal's bodies as written, runtime ids included.

**The unprojected composition (v0.9.0 shape) needs no adapter.** Harness
writes its journal in SessionStore's own envelope format on its legacy
single-tenant layout, so a `*sessionstore.Store` opened over the **runtime's
backend** the way Harness opens it is the `JournalReader`:

```go
factory.WithJournalResolver(func(ctx context.Context, tenant sessionwire.TenantID,
	binding sessionstore.SessionBinding) (factory.JournalReader, error) {
	if binding.StorageBindingID != myBindingID || binding.BindingVersion != myVersion {
		return nil, fmt.Errorf("unknown journal binding %q/%q", binding.StorageBindingID, binding.BindingVersion)
	}
	// One store per tenant, opened once and cached; closed at shutdown.
	return journals.forTenant(ctx, tenant, func(ctx context.Context) (*sessionstore.Store, error) {
		return sessionstore.Open(ctx, runtimeBackend(tenant), sessionstore.WithLegacySingleTenant(tenant))
	})
})
```

`runtimeBackend(tenant)` is the backend the Host's Harness writes that tenant's
journals to (its `storage.Composite`, e.g. NATS or Postgres ledger plus an
`s3store` Blobs provider). SessionStore's `Open` refuses a Blobs provider
without the bounded reader lifecycle (`fsstore`), so a filesystem deployment
wraps its Blobs as Carbon does. Refuse any binding you do not know: a resolver
answering a default serves one deployment's journal under another's
configuration.
SessionStore's legacy layout marker admits **one tenant per backend**: `Open`
persists the tenant in the backend's layout marker and refuses another, so
`runtimeBackend(tenant)` must be exactly the backend the Host's Harness uses for
that tenant -- not a shared one -- and the session is addressed by
`binding.RuntimeSessionID`, which must be a canonical UUID (the rendering Harness
files a journal under).

## Serving

`Server.Handler` is Factory's public HTTP surface: the API under `/v1`, protected
application routes under `/ui/` when supplied, and the injected user interface
on other paths. The injected UI is handed to the router
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
deadlines, and this surface streams object bodies and carries a WebSocket.
`Stop` is idempotent, does not return until what it stopped has stopped, and
stops **public admission first** so nothing new is admitted while the background
components are torn down. It never touches a Host runtime:
a session outlives every Factory replica.

For a shared Factory and Host process, shut down in this order:
`Server.Quiesce(ctx)` fences new command admission, waits for admissions already
inside to return, and closes ClientLinks. Requests already past the router,
including durable reads, may finish; new public requests receive 503. The HTTP
listener, sweeps and HostLinks remain live. Observe bounded durable command
settlement directly in the store, then drain Host while those links are live.
Call `Server.Stop(ctx)`
before closing the shared storage backend. Canceling a Quiesce caller's context
ends only that caller's wait; the quiescence attempt continues. A Factory-only
replica may call `Stop` directly, which invokes Quiesce itself.

`New` also composes command admission, the ClientLink node, the HostLink pool,
the routing and live-tail planes, and the placement and disposition sweeps.
`Start` runs the background components. Pooled placement requires
`WithPendingCommands`; without it the replica logs a warning and places no
session (an error-level log if public creates are also composed). Either option
also requires `WithSessionJournalResolver` (above).

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

## Deployment and recovery

Factory's listener is public; the caller supplies it to `Serve` and owns TLS
termination. `Handler` embedders must configure their own HTTP server limits.
`WithCSRF` is required and has no default trusted origin or shared key.
Factory verifies bearer, cookie or ClientLink credentials with the deployer's
verifier; authorization and the origin/CSRF guard apply at the public edge.

Keep one application-scoped ClientLink per browser and subscribe to session
channels on it (`MaxChannelsPerConnection` defaults to 256). After a lost tail
or `session.reset`, read the durable session journal through the HTTP read
plane. Centrifuge history and recovery are disabled; a reconnect may land on
another replica. This composition needs no sticky routing, notifier, cache,
Redis or NATS broker. NATS may separately be selected as a Storage backend.

Pooled and dedicated describe placement, not durability. Durable recovery
requires a shared SessionStore backend, its leases and the Harness journal;
object bytes require the selected object store. A Host session's tool-result
captures live in the runtime's store under the binding's `RuntimeSessionID`;
`WithSessionObjectStoreResolver` resolves that store from the principal's
tenant, the public session and the catalog binding, and Factory addresses the
read to the runtime id. The object route serves only with an injected
`ObjectPolicy` (503 without one), serves only `tool-result` objects, answers a
policy denial with the same 404 as an absent object, and verifies at most
64 MiB per object. A dedicated workload is controlled by its
workload controller, including drain and termination; Factory does not issue a
HostLink drain or implement drain-before-delete in its placement seam.

Factory exports no Prometheus `/metrics` handler or `factory_` series. It
publishes no direct backlog, resident wait, delivery queue, reconciliation or
drain metric. Durable records and the owning Host/controller provide only
partial operational visibility until Factory metrics are implemented. `Stop` stops public admission and background work;
it does not stop a resident Host runtime.

Gate responses require a Host that advertises
`hostlink.command.gate_response` (host v0.4.0 or later), with Factory pinned to
sessionstore v0.12.0 or later before any Host publishes a gate. Keep Factory
v0.5.0 or later paired with Host v0.3.0 or later and a bare advertised HostLink
base. A cold AskUser answer/resume is unsupported; the gate path is for a
resident session.

Dedicated placement's first-attach path additionally requires Factory ≥ v0.6.0
paired with a workload controller implementing `WorkloadEndpointDiscovery`
(e.g. `looprig/controller` ≥ v0.2.0), which lets Factory discover a ready
dedicated Host's pre-attach endpoint before any resident session exists.
Factory v0.5.0 remains storage-compatible but cannot discover that endpoint, so
it cannot place a new dedicated session against a controller of that shape.

## The HostLink

`internal/realtime/hostlink` is Factory's client side of the Factory-Host
connection, split like the ClientLink: a `Pool` that decides and a
`CentrifugeDialer` that carries. The invariant is one sentence — a session
binding never costs a connection, and a connection is never shared between
Hosts or between tenants — and the route table is keyed by tenant AND session,
because a session id is unique only within its tenant.

**One pooled Host serves several tenants at once (Gap 1, v0.5.0).** A Host
advertises a BASE internal endpoint and serves each tenant's HostLink at Core's
`sessionwire.HostLinkEndpoint(base, tenant)` (`base/hostlink/<tenant>`, core
v0.10.0, host v0.3.0); a verbatim dial of the base is answered 404. The pool
derives every dial address in one place from the advertised base and the
request's tenant, and keys its links by **(HostID, TenantID)**, so bind,
attach, delivery, the live tail and placement all reach a tenant's own
connection and nothing of one tenant's crosses another tenant's (R-1). Every
per-link structure — capabilities, reconnect state, subscriptions — is per
(Host, tenant). `HostLinkLimits.MaxLinks` therefore bounds (Host, tenant)
pairs. A tenant a Host's base cannot address (`too_long`,
`unroutable_tenant`, or a base that is not bare) is refused before any dial as
`hostlink.ErrNoTenantEndpoint`; placement skips that candidate for that tenant
and logs a WARN with Core's code, and a routing bind to such an owner is logged
and left unbound. Factory makes no drain RPC: drain is the workload
controller's, which must derive the same address.

**Compatibility window.** A Host advertising a non-bare endpoint (a v0.2.x
configuration naming `…/hostlink/<tenant>`) is refused as `base_names_tenant`
for every tenant, so factory v0.5.0 cannot reach it until it is reconfigured to
a bare base; a v0.3.0 Host is reachable only from factory ≥ v0.5.0.

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
v0.9.1's (pinned: v0.11.0) `sessionwire/v1` defines the bare connect codecs, reserved method
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

The pool carries the control plane and, since v0.4.0, a session's live tail:
a subscribe to the session's `HostLinkChannel` made after a successful bind on
the same connection. The per-binding queues and the backpressure repair sit
*above* it, in `internal/realtime/delivery` and `routing.Relay`. Choosing which
Host a session belongs to is A7.2.

## Live output (Gap 3, v0.4.0)

A watched session's live output reaches the viewers watching it.
`internal/realtime/livetail` composes `routing.Relay` between the HostLink
subscription and the ClientLink: each record a Host publishes is published once
to `session:{tenant}:{session}`, the channel wui subscribes to, in order. A Host
keeps no history, so every time a session's tail starts (other than inside the
first viewer's own subscribe) or stops unasked, the viewers receive a
`session.reset` -- sent after the new tail is live -- and repair from the
durable journal. On a HostLink reconnect Factory withdraws every tail itself
(centrifuge-go's automatic resubscribe would reach a Host before the re-bind),
re-binds on the new connection, then subscribes, then resets. Limits: a private
record between public ones makes resets name an older sequence than necessary
(over-repair), and Host silence while bound still looks like idle.
Since v0.8.1 a Host record whose own tenant or session is not the tail's it
arrived on is refused (`routing.ErrForeignRecord`, a `delivery.ErrMalformed`),
logged at ERROR, counted (`Relay.ForeignRecords`) and repaired like any refused
record: it never reaches a viewer, and the viewers are reset.
`factory.New` composes the pool, and `Start` drives the reaper on the sweep
cadence.

**A record the live tail never carried is not skipped silently (v0.9.0, I3.1
D2).** A Host relays public records only, and it can commit one it never
relays -- a warm release commits `SessionResidencyReleased` after its tail
stopped -- before a re-placed session's new tail arrives on the same
subscription. Once the tail's position is known (anchored at a first viewer's
tail start, or at the tip a reset sent every viewer to), a record that skips
positions is checked against the journal with one bounded read: if a PUBLIC
record lies in the gap, every viewer is sent a `session.reset` to the journal's
tip in front of it; a gap of private records needs none. A tip that cannot
describe the gap closes the viewers instead. This needs a journal resolver
(`WithSessionJournalResolver`) for Host sessions -- the check reads the runtime journal.

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
supplemented by optional `WorkloadEndpointDiscovery` for the first attach:
`ObserveWorkload` reads a post-attach registry observation and cannot discover
the first Host address. After ensuring a dedicated workload, Factory asks this
method for the ready Host's identity, generation and bare internal endpoint,
then attaches through HostLink and binds from the Host's successful residency
reply. An older controller lacking discovery yields the internal diagnostic
`placement.ErrWorkloadEndpointUnsupported`; the separate controller module must
implement discovery before it can place new dedicated sessions. The endpoint
alone proves neither capacity nor residency.

The workload seam remains composition-only here: no scheduled reconciliation
driver is built in this module. H5 keeps the Kubernetes adapter out of this
module entirely: it lives in the separate repository `looprig/controller`
(owner ruling 2026-09-18), which
consumes Factory only as a published module, so no Factory consumer inherits the
Kubernetes client graph;
Factory ships no binary and grants itself no workload create/delete RBAC, so a
nil controller is a supported configuration and a dedicated session reaching a
replica composed without one is refused by name. Widening this exported method
set is a source compatibility break for external implementations. **While this
module is pre-1.0
(owner ruling 2026-09-18), such a widening ships as a MINOR bump** — the `v0.x`
contract every module in this workspace is released under — and the widening that
added `ObserveWorkload`, `RequestDrain`, and `DeleteWorkload` as `EnsureWorkload`'s
companions rides `v0.2.0`. Once `v1.0.0` is cut, widening it becomes a major
release.

**A Host restarted under its HostID is followed promptly (v0.9.0, I3.1 D1).**
A pooled Host that restarts the way a pod does keeps its HostID and comes back
at a higher generation on a new address. The HostLink pool now replaces a
cached (Host, tenant) link whose Host advertises a new address under a HIGHER
generation -- on the next bind, attach or gate-capability read -- closing the
old link and dropping its routes (a viewer's route is re-bound by the routing
plane's repair). Before, the replica kept redialling the dead address until the
60 s idle reaper collected the link, or never while a viewer pinned it. An
older or same-generation observation of another address does not move a link.

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
to the next candidate, and so does a Host's own code-less failure reply (a
transport error answer such as centrifuge's `ErrorInternal`) -- so one Host
whose launches always fail cannot block placement on the healthy ones. That
reply is not a placement outcome, and it does not mean the Host rolled back (host v0.2.1 also sends it after an
incomplete rollback, or when the session is resident but its observation
could not be published); moving on is safe because the session lease makes
any other candidate refuse `epoch_mismatch` while that Host still holds it,
so no second residency can form. A candidate that
cannot be asked at all -- a failed dial, a link between connections, a pool at
its link ceiling, or a link made terminal by a wire-version change, which the
pool also evicts -- is skipped. Only an AMBIGUOUS failure aborts the attempt: a
cancelled or timed-out request, a lost connection, a malformed or mismatched
reply, or a closed pool, where the request may have reached the Host and a
second attach would be put in flight for nothing; the next sweep retries under
the same idempotency key. Re-placement waits are capped at 5s each and
jittered, and `ReplaceAttempts` is capped at 10. A `tenant_exclusive`
pooled advertisement is still never selected, because Factory cannot see which
tenants a Host serves and refusing is the only enforcement of specification
section 12 available to it.

**A replica that places nothing says so at Start.** Without
`WithPendingCommands` no session is ever attached to a Host, so `Start` logs a
WARN naming that -- an ERROR when `WithPublicCreates` is also composed, because
that replica admits creates it will never place. Requiring the option there,
or deriving it from the create plane, is booked for a later minor.

**Every command is a disposition command (Gap 2).** Input, interrupt,
restore and gate response are admitted into the same DISPOSITION inbox a create
is, under the session's own immutable binding, so a Host that holds the session
consumes and applies them; before this change they went to the legacy inbox,
which no Host can reach. An oversized payload of any kind is stored by
reference. A command for a session bound to the legacy protocol is refused
`runtime_unavailable`. A "dispositions" sweep, composed in every composition,
rejects a disposition command no Host applied before its apply deadline --
only while it is `pending` or `claimed` under a lapsed claim, never once an
attempt exists -- and a retry of it then answers `rejected /
runtime_unavailable`. The deadline and placement sweeps each keep a position
per shard across passes, so a live command behind more than a pass of rows
they must skip is still reached, and a pass that ends with a shard's backlog
unread is logged at WARN. `Commands` accordingly names the disposition admission,
retry read, payload upload, rejection and due query in place of `AdmitCommand`
and `GetCommand`; a `*sessionstore.Store` satisfies it.

**The gate rollout rule is met: sessionstore is pinned at v0.12.0**, whose
readers accept a gate page a Host wrote on a disposition session (older readers
refuse it). `TestAHostPublishedDispositionGateIsReadByThePinnedStore` writes a
gate the way a Host does and reads it through both of Factory's gate readers.

**A gate response is delivered only to a Host that can apply one: a Host whose
connect reply lists Core's token `hostlink.command.gate_response`**
(`sessionwire.HostLinkCapabilityGateResponse`, core v0.11.0; advertised by
host ≥ v0.4.0 in `hostlink_methods`). The answer comes from ONE predicate,
`hostlink.GateResponseCapable` — exact-name `Supports`, so a substring or a
`hostlink.v1.`-prefixed spelling is not the capability, and the catalog's
`LeaseEpoch` is never read as one. Three places ask it, over the owner's (or
candidate's) link for the session's tenant:

- **Admission.** Admitting a gate response IS delivering it — the owner reads
  it from the durable stream — so a gate response for an owner without the
  token is refused `409 gate_not_resumable` (`ErrGateResponseUnsupported`)
  before anything is written.
- **The gates read (v0.9.0).** `GET /v1/sessions/{sid}/gates` reports a gate
  stored `resident` as `unavailable` whenever an answer would be refused
  `gate_not_resumable` -- no fresh matching owner (its Host released the
  session crash-equivalently, is releasing it, or crashed and lapsed), or an
  owner without the token -- and also when the owner could not be asked (the
  write's 503). It is the admission service's own check
  (`GateResponsesAnswerable`, the one `AdmitGateResponse` makes), so the read
  and the write agree. The gate itself is still listed; a successor restores
  and re-publishes it. Other stored values are left as they are.
- **The wake.** A gate-response wake is withheld from a bound Host without the
  token and counted (`WithheldGateResponses`).
- **Placement.** A session with a pending gate response attaches only to a Host
  with the token. Pooled placement skips incapable candidates and reports them
  in one WARN per pass, at most once per session every 5 minutes. Dedicated
  placement may ensure its workload first, then probes the ready endpoint's
  capability before attach; an incapable or unreachable endpoint stays
  unattached. **With no capable Host, the WHOLE session waits** — its inputs
  and interrupts included — for at most the pending answer's apply deadline
  (`ReconcileLimits.ApplyDeadline`, default 5 minutes). The expiry sweep then
  rejects the answer and the session places normally; a command admitted behind
  it may expire in that window.
- **Staleness.** A capability read uses the link's latest reply and is not
  fenced to the owner's generation, so it can be one reply stale across a Host
  restart the link has not yet noticed.
- **Transients.** A gate response whose owner cannot be reached (its link
  reconnecting, the link ceiling full, a failed dial) is answered `503
  unavailable`, retryable, with nothing written. The capability read dials, if
  it must, outside the pool's lock.

**A gate response is never stored by reference.** Every other command larger
than the inbox's inline bound (`sessionstore.MaxInboxPayloadBytes`, 64 KiB of
canonical command payload — the ENCODED size, where JSON escaping can make it
several times the body sent) is uploaded and admitted by reference; a NEW gate
response over it is refused `400 invalid_request` (`ErrGateResponseTooLarge`)
before anything is written, because host v0.4.0 blocks a session's whole
command stream behind a by-reference gate response until its apply deadline,
and every command admitted behind it expires with it. A retry of a gate
response already stored still answers from its record.

**Mixed fleets: do not run v0.3.0 and v0.4.0 Hosts that serve gating agents
together.** An answer admitted under a v0.4.0 owner that is then re-placed onto
a v0.3.0 Host — or that was already claimed there — sits `applying` until a
v0.4.0 successor settles it `not_applied`. The placement filter keeps a
*pending* answer off an incapable Host, but it cannot recall one already in
flight.

## Status

`factory.New` composes the whole service: identity, the HTTP API, admission,
the ClientLink node (started by `Start`), the HostLink pool, the routing table
and demand plane, placement and its sweeps, and -- since v0.4.0 -- the live
tail (`routing.Relay` between the HostLink subscription and the ClientLink).
Since v0.5.0 one pooled Host serves several tenants (Gap 1), and a gate
response reaches a Host that advertises `hostlink.command.gate_response`
(host ≥ v0.4.0). Factory ships no UI and no binary; a product mounts its own
UI through `WithUIHandler`, `WithUIFS` and `WithUIRoutes`.
