# Contributing to looprig/factory

Thanks for contributing. `factory` is Looprig's public-facing orchestration
service over released Core wire records and SessionStore's durable aggregate.

## Before writing code

Read [`CLAUDE.md`](CLAUDE.md). Open an issue for non-trivial public API,
authorization, or wire-surface changes so compatibility can be reviewed first.

Factory must not import Host or Harness, anywhere, including tests. Centrifuge
is confined to `internal/realtime`, the web UI bundle to `cmd/factory`, and the
Kubernetes SDK to `internal/placement/kubernetes`. Do not add local `replace`
directives or vendored dependencies. If you need to move a pinned dependency,
use `go get`, update `releasedLooprigVersions` in `module_pin_test.go` in the
same change, and check that `go mod tidy` leaves `go.mod` unchanged afterwards.

Do not add a nested `go.mod` or a nested repository inside this module without
declaring it in `allowedNestedBoundaries`. Such a directory stops the
module-owned walk, and every import rule silently stops applying inside it.

## Build and test

Run these before pushing:

```sh
GOWORK=off go mod tidy
GOWORK=off go test -race ./...
GOWORK=off make check
```

Add tests before implementation and keep errors typed. When you change the
import boundary, mutation-test it: add the forbidden import in the forbidden
location, watch the specific named test fail, and revert.
