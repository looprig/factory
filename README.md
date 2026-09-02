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
| `github.com/looprig/host` | nowhere |
| `github.com/looprig/harness` | nowhere |
| `github.com/centrifugal/...` | `internal/realtime/...` |
| `github.com/looprig/wui` | `cmd/factory/...` |
| `k8s.io/...`, `sigs.k8s.io/...` | `internal/placement/kubernetes/...` |

The rules are functions over PARSED import paths; nothing matches against source
text, and containment is compared by whole path segments, so
`internal/realtimefanout` is not inside `internal/realtime` and a second binary
beside `cmd/factory` does not inherit its exemption. The scan fails as vacuous
if it finds no files, and `TestScanReachesEveryRuleScope` builds a fixture module
containing each scope so an exemption whose directory does not exist yet is still
proven reachable and still proven to reject the location just outside it.

`go.mod` names exact released versions and contains no `replace` directive;
nothing here is vendored, so `GOWORK=off go test ./...` verifies the module
against the versions it actually pins.

## Status

Scaffold. `contract.go` states the cross-service contract; composition seams,
identity, the HTTP API, admission, routing, placement and realtime are the
subject of later tasks in runbook 05.
