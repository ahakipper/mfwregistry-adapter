# Nacos SDK Provenance and Lifecycle Gate

The active repository pins the official
`github.com/nacos-group/nacos-sdk-go/v3` module to commit
`93a93504cc2fc450c702e60c82d7f81acffe5f28` through pseudo-version
`v3.0.0-20260831100852-93a93504cc2f`.

Module checksums:

- module: `h1:Y6ibzGDlzqX8RTWGCiMU/l+f5+3N36wtGDmrQ3hdtNA=`
- go.mod: `h1:iHP3Mf7pqqoDdInUNr+R62VbEEHYDDx+4lVNCTwYM+o=`

Spotter uses the SDK's Nacos 3 gRPC infrastructure and a narrow adapter-owned
`PersistentInstanceRequest` seam for persistent register/deregister. It does
not route production business writes through `/v1/ns` HTTP. Persistent
application batches remain groups of at most 100 item requests; the SDK's
protocol batch is ephemeral/complete-publication semantics and is not used for
Spotter persistent state.

This is still a development pseudo-version rather than a stable tagged v3
release. Review the pin before 2026-12-31; replace it with the first compatible
official tag or renew the exception with fresh Nacos 3 lifecycle, race, and
wire evidence.

Cluster health-check administration is not a naming prerequisite. The default
policy is `deployment-owned`; register, deregister, query, Subscribe, batch,
readiness, and reconcile do not require an Admin/Maintainer facade. Optional
`admin-managed` mode fails closed if its separately injected facade is absent
and never falls back to raw HTTP.

CI gate: run `go test -race ./pkg/nacos ./pkg/worker ./internal/...`, the tagged
E2E matrix, and the guarded Nacos 3 lifecycle/restart tests on every SDK or
lifecycle change. A race report, persistent request incompatibility, lost
canonical field, failed cleanup, or failed reconnect verification blocks the
data-plane release.
