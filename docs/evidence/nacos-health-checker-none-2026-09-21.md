# Nacos health-checker evidence (2026-09-21)

## Conclusion

Registering a persistent instance does not, by itself, disable Nacos server-side health checks. The registration request carries instance state (`ip`, `port`, `healthy`, `enabled`, `ephemeral`, and metadata); the active checker is a property of the `(namespace, group, service, cluster)` object.

This distinction explains the `limactl -> 10.0.x.x:7096` warnings. `limactl usernet` is the Colima forwarding process. Nacos is the component that initiates TCP checks; the forwarding process only carries those connections into the host's network/TUN path.

## Reproduction against historical Nacos 2.1.0

The local `nacos/nacos-server:v2.1.0-slim` image was started under the existing Colima/amd64 emulation path. A persistent instance was registered through the v1 naming API:

```text
serviceName=health-v2
clusterName=k8s
ip=10.0.0.1
port=7096
ephemeral=false
```

The service metadata endpoint returned:

```json
{"healthChecker":{"type":"TCP"}}
```

While the instance remained registered, the Nacos container's `/proc/net/tcp6` contained many `SYN_SENT` connections to `10.0.0.1:7096`. This is the same mechanism as the screenshot's repeated `dial tcp 10.0.x.x:7096` timeouts. The instance may still be reported as `healthy=true` while connection attempts are in flight; that does not mean that probing is disabled.

Nacos's current health documentation describes persistent instances as being maintained by server-side active health checks and lists `TCP`, `HTTP`, `MySQL`, and `NONE` as the checker choices: [Health, Weight, And Metadata](https://www.nacos.io/en/docs/next/manual/user/naming/health-and-metadata/).

## Observe harness protection

The Nacos 3 Observe harness now performs an explicit, test/deployment-control-plane preflight for every synthetic `obs-app-*` service:

1. Create the persistent service shell through the explicitly named Nacos 3 Admin compatibility adapter.
2. Register a loopback sentinel through the official Nacos Go naming SDK so the `k8s` cluster exists.
3. Set `healthChecker={"type":"none"}` through the Nacos 3 Admin compatibility adapter.
4. Deregister the sentinel through the official SDK.
5. Start Spotter and send all business register, deregister, query, and subscribe operations through the official SDK.

The Admin compatibility calls are not the Spotter data path. They exist only because the pinned official Go naming SDK does not expose the Nacos Maintainer/Admin cluster-metadata operation. If that control-plane preflight cannot be completed, the harness fails closed instead of running with an unknown probe policy.

The Nacos 3 container also sets `NACOS_AUTH_ADMIN_ENABLE=false` for this isolated local fixture. This is a test-only startup setting; production authentication and deployment policy remain outside the Spotter scope.

## Verification run

Run: `20260921-0023` (Nacos 3.2.4 ARM64, KWOK, 20 Pods, 2 services, 2-minute window, SDK data path).

Evidence:

- `Nacos healthChecker=NONE provisioned for 2 k8s service clusters` was emitted before Spotter startup.
- 13/13 observation ticks were exactly consistent; 0 divergent and 0 observation-error ticks.
- 2/2 K8s mutations were correlated to Nacos observations; missing=0.
- K8s Watch -> Nacos latency: create P90/P95/P99 `0.568s`; delete P90/P95/P99 `0.584s`.
- Final post-quiescence three-plane snapshot was exact.
- Nacos and KWOK teardown completed with no residual resources.

The detailed report is [20260921-0023-summary.md](../../tests/observe/results/20260921-0023-summary.md); the machine-readable record is [20260921-0023-summary.json](../../tests/observe/results/20260921-0023-summary.json).

## Scope boundary

This evidence closes the local probe-storm cause and the Observe-fixture guard. It does not claim that a production Nacos deployment has the same health policy: production Admin authentication, TLS, namespace/group authorization, and deployment ownership still need to be configured by the deployment owner. Those deployment gates are intentionally outside the current Spotter scope.
