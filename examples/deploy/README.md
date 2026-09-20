# Cloud pooled Factory composition example

This is a **product composition template**, not a runnable Factory command. The
root `factory` module exports `factory.New` and typed options; it does not ship
a stock binary or parse environment variables. Replace the image, probes,
authentication and secret loading in the product before applying the manifest.
The public Service is `ClusterIP`; terminate external TLS at a product-owned
ingress and enforce Factory authentication, allowed origins and CSRF policy.
Do not publish a HostLink listener through this Service or any public ingress.

Run two Factory replicas against the **same** PostgreSQL structured store and
S3 object namespace. Both use the same SessionStore layout and product tenant
policy. Each Factory discovers Host targets from shared durable registration.
The Host-owned pooled Deployment/HPA must advertise a **unique, bare internal
base endpoint per Host** (for example, `wss://host-7.hosts.svc.cluster.local:8443`,
with no path); Factory derives each tenant's HostLink URL with Core's
`HostLinkEndpoint(base, tenant)`. Keep those endpoints on a private network,
authenticate HostLinks and allow traffic only from Factory. Do not use a
load-balanced Service address as a Host's identity. No session affinity is
needed at the public Factory Service: reconnects and durable reads work through
the shared store. The Host repository owns its Deployment/HPA, listener and
network policy; the controller repository owns dedicated placements and drain
timing. This example makes no claim that those separate manifests are here.

## Per-replica admission and memory profile

Set the typed `factory.ClientLinkLimits` in the product composition root. The
compiled `wiring` package below provides `ClientLimits(5000)` and validates it.
For a conservative launch, use 1,000 links per replica until load tests justify
5,000. Use `MaxChannelsPerConnection: 256` and
`PerConnectionQueueBytes: 1 << 20`; the latter is an actual byte ceiling on
each Centrifuge client queue, and slow peers are disconnected. Also bound
HostLink count per `(Host, tenant)` and the relay's host/delivery queues using
Factory's typed defaults or lower measured limits.

At 5,000 links, `5000 * 1 MiB = 5,000 MiB = 4.883 GiB` of **possible queued
payload per replica**. At 1,000 links the ceiling is 0.977 GiB. These are
worst-case queue allowances, not reserved memory or an RSS prediction. The
manifest's 8 GiB limit is a candidate budget, not a proven safe capacity:
measure connection baseline, fan-out, S3 transfer buffers (up to 512 MiB
accounted by s3store), Go heap and peak RSS under slow-reader load. Lower
`MaxConnections` or the per-connection queue budget, or raise the measured
resource limit, before production if the margin is inadequate. `MaxConnections`
is local to each replica, so two fully loaded replicas can admit 10,000 links.

## Structured and object storage

`wiring.Open` composes PostgreSQL's Ledger, Leaser, KV and OrderedIndex with
S3's Blobs and opens SessionStore. Supply a TLS-verifying PostgreSQL DSN (for
example `sslmode=verify-full` with a trusted CA) and an HTTPS S3 endpoint.
Use `MigrationValidate` on both Factory replicas; run migrations once as a
separate owner before rollout. The product injects credentials from a scoped
provider; never put them in this YAML or log DSNs. `EncryptionKMS` plus
`RequireConfirmedEncryption` forces an explicit server-side KMS request; supply
the actual KMS key ID through the product's secret/config provider. Confirm
bucket policy, IAM, key access and backup/restore separately against the live
provider. `DeploymentPrefix` is a canonical shared deployment namespace (for
example `deployments/production`), **not** a per-replica or per-tenant path.
Factory and Host must agree on that namespace and the SessionStore layout.
Pass a bounded startup context to `wiring.Open`; the returned SessionStore
has its own lifetime and remains usable after that startup context ends.
After Factory and Host stop, close SessionStore and then the PostgreSQL pool.

Run `python3 examples/deploy/validate.py` (requires PyYAML) and
`cd examples/deploy/wiring && GOWORK=off go test ./...` before publishing a
product adaptation. The validator checks these templates; review product
rendered manifests and network policy as part of deployment.
