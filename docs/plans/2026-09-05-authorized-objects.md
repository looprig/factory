# Authorized object paging implementation plan

**Goal:** Serve metadata and verified bounded pages of authorized session objects.

**Architecture:** The router makes an object authorization decision before catalog lookup. A separate trusted reference policy establishes committed-reference permission and expected kind. The canonical catalog binding selects a narrow object reader; only an unbound version-one legacy record may use the existing reader. There is no production policy or public server composition yet (A9).

**Tech stack:** Go HTTP, Core wire types, released SessionStore v0.4.0, Storage memstore tests.

1. Add failing HTTP tests in `internal/httpapi/objects_test.go` for missing policy, tenant/session/object denial and metadata/ranges.
2. Add `ObjectPolicy`, `ObjectReader`, binding resolver configuration and bounded object limits in `objects.go`; extend SessionReader with metadata lookup. Pin released SessionStore v0.4.0.
3. Authorize both routes before resolving the catalog; use trusted policy before metadata or body access. Reject unknown, partial or new-mode bindings without a resolver. Test with independent real stores.
4. Accept only explicit single inclusive ranges. Retain at most 1 MiB while draining through a 32 KiB buffer to verified EOF. Reject objects over 64 MiB before body I/O and apply the existing request deadline. Full GET is limited to one page. Emit bytes and integrity headers only after successful EOF and Close.
5. Drive invalid ranges, size limits, deletion, corruption outside the requested page, backend failures, cancellation and blocked Read/Close. Keep query and route declaration guards accurate.
6. Run targeted tests after each change, then `GOWORK=off make check` (five 30-second fuzz targets, race tests, security checks and build). Run isolated mutation probes after source edits stop. Commit repository-local files only; independent review remains owed.

Metadata describes an authorized immutable index entry, not current blob presence. Repeated paging rereads the whole object; objects above the verification ceiling need another protocol. Production reference evidence and immutable configuration routing remain composition obligations; these endpoints do not activate Host independent-store execution.
