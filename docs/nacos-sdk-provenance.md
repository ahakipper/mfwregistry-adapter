# Nacos SDK Provenance and Lifecycle Gate

The repository pins `github.com/nacos-group/nacos-sdk-go/v2` to upstream
commit `002486583df5ad370ab809cd19dfd97e71b2ef6d` (pseudo-version
`v2.3.6-0.20260902123754-002486583df5`). This is a temporary vendor race fix:
the commit makes `RpcClient.currentConnection` atomic and closes the observed
restart/reconnect data race. It is not a released `v2.3.6`.

This dependency is a release candidate only. Production PASS is prohibited
until Nacos publishes a tagged release containing the fix, or this exact pin
completes the approved scratch HA/restart soak and compatibility gates. The
pin must be reviewed before 2026-12-31; replace it with the first compatible
tag or renew the exception with fresh evidence.

CI gate: run `go test -race -vet=off ./pkg/nacos ./internal/...` and the guarded
`nacos_restart` lifecycle test on every SDK or lifecycle change. A race report,
API incompatibility, or failed reconnect verification blocks release.
