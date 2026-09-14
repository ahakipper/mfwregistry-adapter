# Nacos 3 / KWOK target evidence (2026-09-14)

The approved active target is Nacos 3.2.4-slim on ARM64.  No released
`github.com/nacos-group/nacos-sdk-go/v3` version is advertised by the module
proxy (`go list -m -versions` returned no tagged versions), so this change pins
the official `v3.x-dev` source at pseudo-version
`v3.0.0-20260831100852-93a93504cc2f`, resolving to commit
`93a93504cc2fc450c702e60c82d7f81acffe5f28`.

Persistent naming operations are intentionally exposed through a
Spotter-owned vendor seam (`RegisterPersistent`, `DeregisterPersistent`,
`SelectAll`, `ListServices`, `Subscribe`, `Unsubscribe`, `Close`).  The seam is
the contract for the follow-up adapter that must emit Nacos gRPC request
messages with `Ephemeral=false`; raw HTTP business operations remain outside
the production path.
