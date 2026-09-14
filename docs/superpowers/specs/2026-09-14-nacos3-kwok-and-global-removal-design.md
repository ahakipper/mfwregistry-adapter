# Spotter Nacos 3, Kwok Scale, and Legacy Global Removal Design

**Status:** Approved for implementation by the user on 2026-09-14

**Scope:** Move the supported Nacos target from the historical 2.1 scratch
shape to Nacos 3.2.4, prove the real K8s informer/cache/Sink path with kwok at
large Pod counts, remove the runtime cluster-admin health-check coupling, and
delete the obsolete package-global logger/config/notice compatibility tree.

## Goal

Spotter must have one production-shaped path for Nacos 3: persistent instance
state is written through an official SDK gRPC-capable facade, full snapshots
are processed in application-scoped batches of at most 100 items, and real
kwok observation proves bounded latency, convergence, deletion correctness,
retry behavior, and absence of stale Nacos entries. The active Go graph must
use explicit dependencies only; the old package globals and wrappers are
removed after repository callers are migrated.

## Decisions

### Nacos target and SDK

Nacos 3.2.4-slim on `linux/arm64` is the only current release target. Nacos
2.1.0 remains historical evidence and is not a release gate. The adapter must
not rely on the Nacos 1.x/2.x HTTP OpenAPI or a compatibility plugin.

The Nacos client boundary must expose the operations Spotter needs without
leaking vendor types:

```go
type NamingClient interface {
    RegisterPersistent(context.Context, PersistentInstance) error
    DeregisterPersistent(context.Context, PersistentInstance) error
    SelectAll(context.Context, ServiceScope) ([]PersistentInstance, error)
    ListServices(context.Context, ServiceQuery) (ServicePage, error)
    Subscribe(context.Context, Subscription) error
    Unsubscribe(context.Context, Subscription) error
    Close(context.Context) error
}
```

The implementation must use an official Nacos Go SDK release or an explicitly
pinned official SDK development commit whose module path and checksum are
recorded. If the high-level SDK routes persistent instances over legacy HTTP,
the facade must use the SDK's official gRPC naming surface or fail the Nacos 3
gate; it must never construct a hidden raw HTTP client for business writes.

Protocol-level `BatchRegisterInstance` is not assumed to be additive. The
current SDK development branch documents batch publication as a replacement
of one service publication for one connection. Spotter therefore keeps its
safe logical batch contract: one application scope, at most 100 items per
work unit, bounded execution, and persistent single-item gRPC calls unless a
Nacos 3 integration test proves a protocol batch has additive semantics for
the exact scope and chunking used. A 201-instance test must end with all 201
instances present; a final-count loss is a release failure.

### Health-check management boundary

`healthChecker=NONE` is a Nacos service/cluster management setting. It is not
required to invoke the naming SDK's register, deregister, query, or subscribe
methods. Spotter must not make an unavailable Admin/Maintainer SDK a hidden
precondition for all runtime writes.

The Nacos deployment contract will provision the intended health-check policy
before Spotter starts. Spotter's runtime owns naming data only. An optional
Admin/Maintainer capability may be injected later for an explicit preflight,
but its absence must not cause the naming client to call raw HTTP or silently
change instance lifecycle semantics.

Persistent instances remain mandatory (`Ephemeral=false`). The health-check
policy and the ephemeral/persistent lifecycle flag are independent concerns.

### Kwok reliability gate

The real scale vehicle is `kwokctl` plus a fake Kubernetes node and the real
client-go informer/watch path. Fake providers and in-memory mocks remain unit
test tools only. The definitive gate creates at least 1,000 Pods across at
least 20 applications/services (`OBS_SERVICES=20`, with one application mapped
to one Nacos service scope) and runs for at least two hours. It performs
continuous source-to-Sink comparison and records every observation tick.

Each tick compares:

1. the authoritative kwok/Kubernetes Pod snapshot;
2. Spotter's internal observed/cache projection, exposed through a test-only
   read port or observer callback; and
3. Nacos's complete SDK view, including disabled/unhealthy persistent entries.

The gate must exercise create-before-delete churn, burst deletes, application
batch boundaries (`100 + 100 + 1`), retry after an injected failure, and
reconciliation after a missed or delayed watch event. The fixed observation
bound is `OBS_BOUND = max(10 seconds, configured full-push interval)`; an entry
younger than that bound may be marked `inflight`, while an older missing or
extra composite identity is a divergence. Acceptance requires no dropped
events, zero divergences older than `OBS_BOUND`, exact final cardinality, and
verified cleanup of Pods, containers, temporary files, and Nacos registrations.

### Legacy package removal

The active composition root already passes explicit logger, notifier, metrics,
and config values. The remaining `pkg/log`, `pkg/notice`, local
`appcenternotice` shim, mutable `config/config.go`, and
`internal/infra/legacycompat` exist only for deprecated constructors and
legacy tests. This design intentionally removes that source-compatibility
surface. Deployment assets under `config/certs/` and `config/kubeconfigs/`
remain.

The final static gate must find no production or test import of the deleted
packages, no writes (`=`, `+=`, `-=`, `++`, `--`) to package globals, and no
deprecated constructor files. Equality comparisons and short declarations are
not assignment violations.

## Error and consistency rules

- A Nacos write error is retryable unless the SDK classifies it as a permanent
  client/authorization error.
- A failed application batch never triggers prune for that full snapshot.
- Register/update and deregister work units are never mixed.
- Per-application work units preserve input order; independent application
  scopes may overlap under a global Nacos item-call limit of 8.
- A successful write is not considered complete until the subsequent SDK
  read/reconcile observes the expected composite identity and fields.
- A confirmed source deletion must eventually remove the corresponding Nacos
  registration; a transient source read failure must not be interpreted as an
  empty snapshot.

## Verification and evidence

Every implementation stage has focused unit tests and a fresh independent
review. The release matrix includes:

```text
git diff --check
go vet ./...
go test ./... -count=1
go test -race ./... -count=1
go test -tags=observe ./tests/observe/... -count=1
go test -tags=nacos_real ./tests/e2e/... -count=1
OBS_SCALE=1000 OBS_DURATION=2h make test-observe
```

Skipped external gates must say `NOT VERIFIED` with the exact missing
environment. A Nacos 2 result, a fake-provider result, or a short observe
smoke cannot be promoted to the Nacos 3/kwok release verdict.
