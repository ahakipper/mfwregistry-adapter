# spotter Testing Guide

This document is the test-coverage audit and the Makefile test-matrix design for
**spotter**, the discovery-center adapter. It records the current state of the
test suite (measured on this repository), maps the existing tests to the four
required tiers (white-box unit, black-box, smoke, e2e), lists the coverage gaps
per tier, and specifies the exact Makefile targets that will implement the
matrix. It is the implementation brief for the test work that follows; it does
not change any code by itself.

Companion documents: [architecture.md](architecture.md) (design),
[data-model.md](data-model.md) (the `Instance` model),
[operations.md](operations.md) (runbook). See [README.md](README.md) for the
documentation index.

## 1. Tier definitions

| Tier | Definition | Runs | Network |
| --- | --- | --- | --- |
| White-box unit | In-package tests of internal functions and unexported branches, using fakes and the manual clock. | `go test` per package, race enabled. | None; loopback only. |
| Black-box | Behavioral tests of public contracts (`Client`, `DiscoveryCenter`, `DefaultWorker`, `ClientFactorySimple`, `consulMonitor`) driven only through their public API with test doubles. | `go test`, selected via the `TestBlackbox` name prefix. | None; loopback only (bufconn / httptest). |
| Smoke | Binary-level checks of the built executable: help output, invalid-env / no-provider / invalid-provider error paths, exit codes. | Make target that builds the binary and runs each case under a timeout. | None; the offline cases fail fast on etcd dial timeout. |
| E2E | Full pipeline against in-process mocks: provider sources → worker → discovery-center mock, asserting the gRPC calls received. | `go test -tags=e2e ./tests/e2e/...`. | None; loopback only. |

All tiers must run fully offline: no corporate etcd, Consul, or gRPC endpoints
(the default `--grpc-addr` value points at a production address and must never
be dialed by a test). The only listeners allowed are `127.0.0.1` sockets
created by the testkit mocks.

## 2. Current state (audit results)

Commands run on this repository (Go toolchain in use builds the module without
errors):

- `go test ./...` — **exit 0**. 14 packages report `ok`, 19 packages report
  `[no test files]`, none fail. 116 test functions exist in total.
- `go test -race -count=1` over the same 14 tested packages — **exit 0**, no
  data races.
- `go test -cover ./...` — **exit 1**, with `FAIL spotter/tools/cache [build
  failed]`. Root cause: a `go vet` printf diagnostic at
  `tools/cache/cache.go:52` (`fmt.Sprintf call has arguments but no formatting
  directives`; the call passes `table.cleanupInterval` to a format string with
  no verb). The plain `go test ./...` run does not build the test-free
  `tools/cache` package, so it passes; the `-cover` run instruments it, which
  builds (and vets) it and fails. Fixing that one line is a prerequisite for
  any `./...`-wide coverage target; until then the matrix uses an explicit
  package allowlist (see section 5).

### 2.1 Coverage per package

| Package | Coverage | Test files | Tests | Notes |
| --- | --- | --- | --- | --- |
| `spotter/internal` | 46.0% | 1 | 4 | Server lifecycle; `Run`, `NewServerFromDeps`, `InitializeProviders` at 0%. |
| `spotter/internal/composition` | 90.5% | 1 | 5 | Only `Build` exists; one branch uncovered. |
| `spotter/internal/domain/instance` | 95.9% | 1 | 9 | Rules and observation policies. |
| `spotter/internal/infra/config` | 100.0% | 1 | 18 | `Load`, presets, tri-state flags fully covered. |
| `spotter/internal/infra/logging` | 89.4% | 1 | 8 | `Warnf` 0%, `Close` 80%. |
| `spotter/internal/infra/metrics` | 96.9% | 1 | 5 | `StartHTTP` error branch 83.3%. |
| `spotter/internal/infra/notice` | 85.2% | 1 | 4 | `Notify` 83.3%, `getLocalIP` 76.9%. |
| `spotter/internal/ports` | — | 0 | 0 | Interfaces + `NopLogger` only. |
| `spotter/internal/testkit/consulmock` | 89.8% | 1 | 4 | Mock itself; `mustMarshal` 75%. |
| `spotter/internal/testkit/discoverymock` | 81.1% | 1 | 2 | `Codec`/`Name`/`String` 0%; handlers 44.4% (interceptor branch unexercised). |
| `spotter/internal/testkit/fakes` | 87.3% | 1 | 10 | `FakeInstanceSink.PushAll`/`PushAllCalls` and `FakeInstanceSource.Run` at 0%. |
| `spotter/pkg/discoverycenter` | 83.9% | 2 | 14 | `Client.Dial` 36.4%, `DiscoveryCenter.GetAll` 0%, `PushAll` 66.7%. |
| `spotter/pkg/providers/consul` | 33.0% | 2 | 27 | Monitor and client-factory well covered; `consul.go` and `convertion.go` entirely 0%. |
| `spotter/pkg/providers/k8s` | 29.1% | 1 | 1 | Only `formatInstance` conversion table test; all of `k8s.go` is 0%. |
| `spotter/pkg/worker` | 42.4% | 1 | 5 | `DefaultWorker` + `UnsyncedService` partially covered; all of `elector.go` and `worker_fack.go` is 0%. |

Packages with **no test files** (0.0% of statements): `spotter` (root, `main`),
`spotter/cmd`, `spotter/config`, `spotter/pkg/beehive/service/v2`,
`spotter/pkg/distribute/discovery`, `spotter/pkg/distribute/election`,
`spotter/pkg/etcd`, `spotter/pkg/k8srobot`, `spotter/pkg/log`,
`spotter/pkg/metrics`, `spotter/pkg/notice`,
`spotter/pkg/notice/appcenternotice`, `spotter/pkg/providers`,
`spotter/pkg/providers/aggregate`, `spotter/tools`,
`spotter/tools/cache`, `spotter/tools/gpool`, `spotter/tools/net`,
`spotter/tools/unit`.

Measured runtime: the full race-enabled suite sums to roughly 130 seconds of
package test time on a dev machine (largest packages: infra/config 11.5s,
infra/metrics 12.7s, composition 10.9s); `go test` runs packages in parallel,
so wall clock is lower. The non-race cached run completes in well under a
minute. The whole matrix budget (section 5.4) stays inside three minutes.

### 2.2 Test assets available today

- **Fakes** (`internal/testkit/fakes`): `FakeLogger`, `FakeNotifier`,
  `FakeClock` (manual, deterministic), `FakeInstanceSink` (records
  `Push`/`PushAll`/`GetAll`), `FakeInstanceSource` (scripted watch channel),
  `FakeLeaderElector`, `FakeMetricsRecorder`, `FakeEventQueue`. All satisfy
  `internal/ports` interfaces via compile-time assertions.
- **Consul mock** (`internal/testkit/consulmock`): loopback HTTP server
  implementing `/v1/status/leader`, `/v1/catalog/services`,
  `/v1/health/service/<name>` (with tag and passing filters) and
  `/v1/health/state/any` with `X-Consul-Index` control; records every request.
- **Discovery-center mock** (`internal/testkit/discoverymock`): in-memory gRPC
  server over `bufconn` with the legacy JSON codec, implementing
  `SynInstance`, `SynAllInstance`, `GetAllInstance`; records calls, can script
  response codes and served instances.
- **Domain policies** (`internal/domain/instance`): pure functions
  (`StateOf`, `StatusOf`, `EnvTypeOf`, `DiffNewerReversion`,
  `DiffEqualReversion`, `CompareThreeWay`) that mirror the provider conversion
  rules and are already used as the reference for k8s conversion tests.

## 3. Tier mapping of the existing tests

| Area | Files | Tier | What is exercised |
| --- | --- | --- | --- |
| Domain rules/observation | `internal/domain/instance/rules_test.go` | White-box | Filters, diff policies, three-way compare, nil-receiver helpers. |
| Composition root | `internal/composition/root_test.go` | White-box | `Build` defaults, injected overrides, log-closer handoff. |
| Infra adapters | `internal/infra/{config,logging,metrics,notice}/*_test.go` | White-box | `Load` flag resolution and presets; logger rotation setup; metrics HTTP start/stop; notice send failure paths. |
| Server lifecycle | `internal/server_test.go` | White-box | `startProviders` cancel-on-leader-loss, `Stop` during dial, generation serialization, dial retry cadence (3 attempts, 5s waits). |
| Worker | `pkg/worker/worker_test.go` | Black-box | `DefaultWorker` handler delegation, failed-push queueing, `PushAll`/`GetAll` delegation, queue-depth metric. |
| Discovery center client/registry | `pkg/discoverycenter/*_test.go` | Black-box | `Client.Sync/SyncAll/GetAll` against a fake service, metrics recording, 10s deadline, nonzero response codes; `DiscoveryCenter` push error/success notify, `disablePush` short-circuit, `Close`. |
| Consul client factory | `pkg/providers/consul/client_factory_test.go` | Black-box | Address normalization, first-valid selection, failover from dead address, cache reuse and eviction, aggregate error unwrap, concurrency. |
| Consul monitor | `pkg/providers/consul/monitor_test.go` | White-box + mocks | Constructor nil rejection, factory failure notification + 5s wait, health-state wait-index behavior, 50ms debounce (single and reset), handler error notification, GetServices/GetServiceEntries semantics via consulmock. |
| K8s conversion | `pkg/providers/k8s/k8s_test.go` | White-box | `formatInstance` pod → instance table test. |
| Testkit | `internal/testkit/**/**_test.go` | White-box | The mocks' own contracts (index advance, call recording, codec round trip, fake clock advance). |

## 4. Gap analysis

### 4.1 White-box gaps (function level, from `go tool cover -func`)

**`spotter/internal` (46.0%)**

| Function | Coverage | Gap |
| --- | --- | --- |
| `Server.Run` | 0% | The whole leader-transition loop: fake-leader startup (`EnableLeaderElection=false` writes `true` to `leaderChCh`), leadership loss (notifier + `stopProviders`), regain (restart), metrics server start/stop, `stop` channel shutdown. |
| `NewServerFromDeps` | 0% | Nil-runtime rejection, empty-providers rejection, elector construction failure path (notify + error). |
| `startMetricsServer` / `stopMetricsServer` | 0% | Metrics HTTP lifecycle inside `Run`; failure logging. |
| `dialDiscoveryClient` | 0% | Real `discoverycenter.Dial` invocation with 5s timeout (needs a loopback listener, not the production default address). |
| `InitializeProviders` / `initializeProvidersFromConfig` | 0% | Provider construction dispatch: k8s happy path, ecs with no Consul address error, unknown provider name error, empty providers error. |
| `startProviders` | 73.7% | Worker construction failure and provider-initialization failure cleanup branches. |
| `stopAndStartProviders` | 84.6% | `stopProviders` error propagation branch. |

**`spotter/pkg/providers/k8s` (29.1%)** — all of `k8s.go` is 0%:

| Function | Coverage | Gap |
| --- | --- | --- |
| `CompareAndFlush` | 0% | The core reconciliation: empty remote list → push all; per-instance diff push; remote-only instances marked offline (status 3, state terminated); `config.PushAppCodes` filtering; all three inconsistency notices. |
| `pod2Instance` | 0% | Add/update path via `robot.GetByKey` + `formatInstance` + filter + cache diff; delete path via cache fallback (`obj2InstanceId`, offline transition). |
| `hasInstanceDiff` | 0% | Offline-equal no-diff, newer-reversion diff, field-compare diff (mirrors domain `DiffNewerReversion`). |
| `monitor` / `Run` / `ProcessIntervalFullPush` | 0% | Sync-wait loop, event pop loop, `Finish` acknowledgement, ctx cancellation; interval ticker (uses real `time` — needs a short interval). |
| `GetAll` / `flushInstances` / `ProcessCache` / `eventSync` / `buildAndSendEvent` / `VerifyInstance` / `NewK8SProvider` | 0% | Supporting paths; `NewK8SProvider` needs a `Robot` seam (see 4.4). |

`conversion.go` partial gaps: `formatStatus` 23.8%, `formatContainerEnabled`
20.0%, `formatState` 25.9%, `formatCpuSize` 33.3%, `formatIDC` 58.3%,
`formatAppPort` 71.4% — the container-status, restart-reason, resource and IDC
label branches need table tests.

**`spotter/pkg/providers/consul` (33.0%)** — `consul.go` and `convertion.go`
are entirely 0%:

| Function | Gap |
| --- | --- |
| `NewConsulProvider` | Parameter validation (nil ctx/worker, empty addrs), monitor wiring, handler registration. |
| `Run` | Full-push goroutine + `CompareAndFlush` goroutine + `monitor.Start` blocking; error notification on monitor stop. |
| `syncInstance` / `InstanceChanged` / `ServiceChanged` | Cache swap, empty-endpoints guard, diff extraction, event flush. |
| `extractDiff` | Add / update (higher `Reversion`) / delete classification with filter rejections; the domain `CompareThreeWay` is the reference behavior. |
| `CompareAndFlush` | Same three reconciliation cases as k8s, plus the `worker.GetAll` error return and empty-remote full push. |
| `GetAll` / `toInstance` | Services → entries → instances walk; conversion error skip. |
| `convertion.go` (all functions 0%) | `convertInstance` and the per-field converters (`convertPort` including the `scheme` compatibility field, `convertEnv`, `convertAppcode`, `convertLabels`, `convertState`, `convertSatus`, `convertInstanceId`, `convertVersion`), including every error branch. |
| `Monitor.Start` | The errgroup composition of `watchConsul` + `updateRecord` (both are covered individually). |

**`spotter/pkg/worker` (42.4%)**

| Function | Coverage | Gap |
| --- | --- | --- |
| `elector.go` (all 0%) | `NewElectorWithDeps`, `ElectWait` campaign/wait loop, `Stop`, `setLeaderChangeNotifyCall`, `syncStoppedState`. Requires etcd — needs a `Candidate`/client seam or is covered only at the server tier via `FakeLeaderElector`. |
| `UnsyncedService.Add` (52.9%) | Same-ID newer-`Reversion` replacement and stale-`Reversion` skip branches. |
| `UnsyncedService.Sync` (83.3%) | 5s ticker loop with ctx cancel; `syncOnce` retry-then-delete is 88.9% (both push outcomes hit). |
| `worker_fack.go` (all 0%) | The fake worker used for manual runs; either test minimally or exclude from coverage scope. |

**Infra and testkit residuals**

- `internal/infra/logging`: `Warnf` 0% (trivial), `Close` 80%, `New` 88.9%
  (encoding fallback branches).
- `internal/infra/metrics`: `StartHTTP` invalid-address branch 83.3%.
- `internal/infra/notice`: `Notify` send-error branch 83.3%; `getLocalIP`
  interface-error branch 76.9%.
- `internal/composition`: `Build` logger-construction failure branch
  (unwritable log path).
- `internal/testkit/fakes`: `FakeInstanceSink.PushAll`, `PushAllCalls`,
  `FakeInstanceSource.Run` at 0% — these are prerequisites for e2e assertions.
- `internal/testkit/discoverymock`: `Codec`, `jsonCodec.Name`,
  `legacyCodec.String` 0%; the gRPC interceptor branch of the three handlers
  44.4%.

### 4.2 Black-box gaps (public contract behavior)

- **`InstanceSink` push semantics through the real chain**:
  `discoverycenter.Client.Sync` → `DiscoveryCenter.Push` — success, nonzero
  response code (error with code and message), transport error (notify with
  exact title), and the `DisablePushWorker` fake-push short-circuit. The
  registry tests cover most of this today; the uncovered part is `Dial`
  (36.4%): connecting a real `Client` through a loopback listener
  (`discoverymock.DialContext` with `ForceCodec`) and the dial-deadline error
  path. `DiscoveryCenter.GetAll` passthrough is 0%.
- **`EventQueue` retry semantics**: `UnsyncedService` retry cadence is 5s of
  real time in `Sync`/`syncOnce`; `Add`'s highest-`Reversion`-wins
  replacement is only half covered. A black-box test of "failed push is
  queued, retried, removed on success, kept on failure" exists
  (`TestWorkerHandlersDelegateAndQueueFailedSync`,
  `TestUnsyncedServiceRecordsQueueDepthBeforeRetry`); missing are the
  same-ID revision-dedup branches and the 5s-cadence + ctx-cancel behavior of
  `Sync`.
- **`ConsulClientFactory` failover**: well covered (first-valid, failover,
  cache reuse, warm-cache eviction, aggregate error). Residual: `Unwrap`
  empty-failures branch 66.7%.
- **Monitor debounce**: covered deterministically with `FakeClock`
  (50ms debounce, reset on repeated changes, single handler run). Residual
  black-box gap: `Monitor.Start` returning when the context is canceled
  (the errgroup wrapper itself).
- **`config.Load`**: 100% — no gap.
- **`InstanceSource` contract** (`FakeInstanceSource` exists but no
  production source is driven through it yet): the k8s/consul providers
  still consume their legacy sources (`k8srobot.Robot`, `Monitor`), so the
  ports-level source contract has no production black-box test. This is an
  architecture follow-up, not a test-only fix.

### 4.3 Smoke gaps (binary level)

Currently the smoke checks are manual (`spotter -h`, run with flags by hand).
Measured behavior of the current binary that the automated suite must assert:

| # | Command | Exit | Output to assert | Time |
| --- | --- | --- | --- | --- |
| 1 | `spotter -h` | 0 | `Usage:`, `Available Commands`, `adapter` | <1s |
| 2 | `spotter adapter -h` | 0 | `Run the instance adapter`, `--providers`, `--leader-elect` | <1s |
| 3 | `spotter adapter -e badenv` | 1 | `starting adapter`, `invalid env param` | <1s |
| 4 | `spotter adapter -e test` (no `--providers`) | 1 | `no providers configured` | <1s |
| 5 | `spotter adapter -e test -r badprovider -t=false` | 0 | `connect to etcd server failed` (offline: the elector dials the preset etcd endpoints and fails after the 5s member-list deadline; `NewServerFromDeps` returns the error, the command prints it and returns) | ≤10s |

Notes for the implementation:

- Case 5 asserts the message, not a failure exit code: the elector-construction
  error path in `cmd/adapter.go` prints and returns (exit 0). This documents
  current behavior; if the exit code is later fixed to 1, the assertion should
  follow.
- The adapter writes `./logfiles/` and an `app.log` into its working directory
  on startup (an `app.log` is already committed under
  `pkg/providers/consul/` from manual runs). The smoke runner must execute the
  binary inside a temporary directory and must not leave artifacts in the
  repository.
- A "fake leader startup with disable-worker" smoke case (`-t=false -w`) is
  **not feasible offline today**: `NewServerFromDeps` always constructs the
  etcd elector, even when election is disabled, so the process never reaches
  provider startup without reachable etcd endpoints. This requires the
  elector seam described in 4.4; until then it stays a manual check.

### 4.4 E2E gaps and required seams

The target e2e shape (all offline, loopback only):

```
consulmock (httptest) ──> NewConsulProvider ──> DefaultWorker ──> DiscoveryCenter
                                                                    │
discoverymock (bufconn) <──────────── assert SynInstance/SynAllInstance calls <┘
```

1. **Consul pipeline — feasible today with exported API only.**
   `consul.NewConsulProvider(ctx, worker, interval, []string{mock.URL()})`
   accepts mock addresses; `discoverymock.Start()` + `DialContext` gives a
   `*grpc.ClientConn` whose JSON codec matches the wire format; the e2e test
   wraps the conn in a small `v2.InstanceServiceClient` adapter (same
   `conn.Invoke` pattern as the unexported `grpcServiceClient`) and builds
   `discoverycenter.NewClient` → `NewDiscoveryCenter` →
   `worker.NewResourceWorker`. Feed services/entries into the consulmock,
   advance the index, and poll `discoverymock.Calls()` for the expected
   `SynInstance` payloads. Monitor timings (50ms debounce) make the test fast.
2. **Server-level e2e — blocked by the elector.**
   `NewServerFromDeps` constructs the etcd elector internally
   (`worker.NewElectorWithDeps`), so the `Server` cannot be created offline.
   Required seam (minimal, no behavior change): let `Server` accept an
   optional elector/elector-factory override (the struct already takes
   injected `dialDiscovery`, `initializeProviders` and `lifecycleLocker` —
   the same pattern applies), then drive `Run` with
   `fakes.FakeLeaderElector` transitions and assert provider start/stop.
   Alternatively the e2e lives in package `internal` behind the `e2e` build
   tag and assembles the `Server` struct directly, as `server_test.go` already
   does.
3. **K8s pipeline — feasible with a `Robot` seam.**
   `NewK8SProvider` builds `k8srobot.NewRobot` from kubeconfig file paths, so
   it cannot run against a fake cluster. The module already depends on
   `k8s.io/client-go v0.22.4`, which ships `k8s.io/client-go/kubernetes/fake`
   and the informer factories that `pkg/k8srobot` itself uses per cluster.
   Required seams: (a) `NewK8SProviderWithRobot` (or an options struct)
   accepting an injected `k8srobot.Robot` — the `Robot` interface already
   exists and the `k8s` struct field is private; and (b) a
   `k8srobot` constructor from `[]kubernetes.Interface` (for example
   `NewRobotForClientsets`) that builds the same informers the kubeconfig
   path does. With `fake.NewSimpleClientset(pods...)` the informers emit
   add/update/delete events, `GetByKey` serves live pods, and deletion
   exercises the cache-fallback path in `pod2Instance`. This is the highest
   effort but also the highest fidelity item.
4. **Testkit completion first.** `FakeInstanceSink.PushAll`/`PushAllCalls`
   and `FakeInstanceSource.Run` are untested; e2e assertions depend on them.

## 5. Makefile test matrix (design for implementation)

The current Makefile only has `build` (with `OS ?= linux` and
`GOOS=$(OS) go build -o spotter`). That target stays unchanged. The `OS`
variable cross-compiles: the default `linux` suits the Docker image; on a
dev machine use `make build OS=darwin` (or `OS=$(go env GOOS)`). Test targets
never set `GOOS`; they always test on the host toolchain.

Rules for every target: deterministic pass/fail (non-zero exit on any failure
or, for smoke, any output/exit-code mismatch), offline, no corporate
endpoints, race-clean, and each case bounded by a timeout.

```make
# --- Test matrix -----------------------------------------------------------

# Packages under test. Explicit allowlist: untested legacy packages cannot
# break the matrix, and spotter/tools/cache currently fails vet under -cover
# (fmt.Sprintf with arguments but no formatting directives, cache.go:52).
TEST_PKGS := ./internal/... \
	./pkg/discoverycenter \
	./pkg/worker \
	./pkg/providers/consul \
	./pkg/providers/k8s

# Packages hosting black-box suites (tests named TestBlackbox*).
BLACKBOX_PKGS := ./pkg/discoverycenter ./pkg/worker ./pkg/providers/consul

# White-box tier: every allowlisted package, race detector on, cache off.
test-unit:
	go test -race -count=1 $(TEST_PKGS)

# Coverage variant: merged profile, text total, optional HTML report.
test-unit-coverage:
	go test -race -count=1 -coverprofile=coverage.out $(TEST_PKGS)
	go tool cover -func=coverage.out | tail -n 1
	go tool cover -html=coverage.out -o coverage.html

# Fast inner loop: no race detector, package cache allowed.
test-fast:
	go test -count=1 $(TEST_PKGS)

# Black-box tier: behavioral suites selected by test-name prefix.
test-blackbox:
	go test -race -count=1 -run '^TestBlackbox' -count=1 $(BLACKBOX_PKGS)

# Smoke tier: build once, then run the offline binary checks.
SMOKE_BIN := ./build/spotter

test-smoke:
	go build -o $(SMOKE_BIN) .
	./scripts/smoke.sh $(SMOKE_BIN)

# E2E tier: full pipeline against in-process mocks, no network.
test-e2e:
	go test -race -count=1 -tags=e2e ./tests/e2e/...

# Aggregate: everything, in tier order. Budget ~3 min on a dev machine.
test-all: test-unit test-blackbox test-smoke test-e2e

.PHONY: test-unit test-unit-coverage test-fast test-blackbox test-smoke test-e2e test-all
```

`scripts/smoke.sh` contract (portable `sh`): for each row of the table in
4.3, run `timeout 30 <bin> <args>` inside a fresh temporary directory
(`mktemp -d`, cleaned on exit), capture combined output, and fail (exit 1,
printing case number, expected and actual) when the exit code or a required
`grep -q` pattern does not match. If `timeout` is unavailable (macOS without
coreutils), use the equivalent Go runner instead (`go run ./tests/smoke
-bin <path>` executing cases with `exec.CommandContext` deadlines); keep the
same case table and assertions either way. `build/` and `coverage.out`/
`coverage.html` are build artifacts — add them to `.gitignore`.

### 5.1 Target semantics

| Target | What it proves | Fail condition | Budget |
| --- | --- | --- | --- |
| `test-unit` | All allowlisted packages pass with the race detector; cache defeated (`-count=1`). | Any test failure or race report. | ~2 min (race cost included). |
| `test-unit-coverage` | Same, plus a merged `coverage.out`, a one-line total and `coverage.html`. | As above; also fails if the profile cannot be written. | ~2 min. |
| `test-fast` | Quick regression loop without race. | Any test failure. | <1 min. |
| `test-blackbox` | Public-contract behavior suites only (`TestBlackbox*`). | Any failure or zero matching tests (guard: at least one test must match per package — otherwise the target silently passes; verify during implementation). | <1 min. |
| `test-smoke` | The built binary answers help flags and fails cleanly on invalid env / missing / invalid providers. | Build failure, timeout, wrong exit code, missing output pattern. | <30 s (dominated by the ~10 s etcd-deadline case). |
| `test-e2e` | The full offline pipeline pushes the expected instances to the discovery mock. | Any failure or race. | <1 min. |
| `test-all` | Everything above in sequence. | Any constituent failure (make stops on first failure). | ≤3 min. |

### 5.2 CI placement

- **Per-PR pipeline**: `test-unit`, `test-smoke`, `test-e2e` (fast, race
  enabled). `test-fast` is a dev-machine convenience, not CI.
- **Main branch / nightly**: `test-unit-coverage` (publish `coverage.out`
  total and `coverage.html` as artifacts) plus `test-blackbox`.
- Total CI budget: the unit tier dominates (~2 min); the whole matrix stays
  under ~3 min on a dev machine and comfortably under typical CI timeouts.
- The `OS` variable only affects `build`; CI test jobs run on the host
  toolchain. Keep `make build OS=linux` for the image build stage.

## 6. Prioritized backlog (top 10)

Highest-value missing tests, in order. "Fake Robot"/"fake Monitor" are
same-package test doubles; production seams are only needed where noted.

| # | File | Test name | Scenario | Doubles needed | Tier |
| --- | --- | --- | --- | --- | --- |
| 1 | `pkg/providers/k8s/k8s.go` | `TestK8SCompareAndFlushCases` | Three-way reconcile: empty remote list pushes all; newer instance pushed; remote-only instance pushed offline (status 3, terminated); `PushAppCodes` filter; the three inconsistency notices. | In-package fake `k8srobot.Robot` (serves `List`), recording fake `worker.Worker`. | White-box |
| 2 | `pkg/providers/consul/consul.go` | `TestConsulCompareAndFlushCases` / `TestSyncInstanceSwapsCache` | Same reconciliation for ecs; `worker.GetAll` error return; empty-remote full push; empty-consul guard; cache swap in `syncInstance`. | Fake `Monitor` (in-package interface), recording fake worker. | White-box |
| 3 | `pkg/providers/consul/convertion.go` | `TestConvertInstanceTable` / `TestConvertPortSchemeCompatibility` | Endpoint → instance field mapping, `scheme` vs `protocol` port compatibility, every error branch (nil endpoint, bad ports meta, missing appcode/env). | Pure functions; consulmock fixtures. | White-box |
| 4 | `internal/server.go` | `TestRunFakeLeaderLifecycle` | `Run` with election disabled (fake leader), leadership lost (notification + providers stopped), regained (restart), then `Stop` shuts down cleanly and idempotently. | `fakes.FakeLeaderElector`, injected `dialDiscovery` (discoverymock), `initializeProviders` returning a blocking provider, `FakeNotifier`. | White-box |
| 5 | `internal/server.go` | `TestInitializeProvidersErrorPaths` | Empty providers, unknown provider name, ecs without Consul addresses, k8s constructor error; assert exact messages. | None (pure dispatch). | White-box |
| 6 | `pkg/discoverycenter/client.go` + `registry.go` | `TestBlackboxDialOverLoopback` / `TestBlackboxDiscoveryCenterGetAll` | `Dial` succeeds against `discoverymock.DialContext` (JSON codec) and fails on deadline; `DiscoveryCenter.GetAll` passthrough; `PushAll` nonzero-code error path. | discoverymock, `FakeLogger`/`FakeNotifier`. | Black-box |
| 7 | `pkg/providers/k8s/k8s.go` + `conversion.go` | `TestPod2InstanceDeleteFromCache` / `TestHasInstanceDiffCases` / conversion table completion | Delete event falls back to cache and pushes offline; offline-equal no-diff; revision and field diffs; complete `formatStatus`/`formatState`/`formatContainerEnabled`/`formatCpuSize`/`formatIDC` branches. | Fake `Robot`, in-package cache. | White-box |
| 8 | `pkg/worker/unsynced_service.go` | `TestBlackboxUnsyncedReversionWinsAndRetryCadence` | `Add` replaces same-ID events only on higher `Reversion`; retry removes entries on success, keeps them on failure; `Sync` honors ctx cancel. | Recording fake `Pusher`, `FakeMetricsRecorder`, short ctx (ticker is 5 s real time — cancel before first tick, test `syncOnce` directly). | Black-box |
| 9 | `tests/e2e/consul_pipeline_test.go` (new, tag `e2e`) | `TestE2EConsulPipeline` | consulmock catalog + entries → `NewConsulProvider` → `DefaultWorker` → `DiscoveryCenter` → discoverymock; assert `SynInstance` calls contain the converted instances (id, ports, env code, appcode). | consulmock, discoverymock, `v2.InstanceServiceClient` wrapper over the bufconn. | E2E |
| 10 | `tests/e2e/k8s_pipeline_test.go` (new, tag `e2e`, needs the seam) | `TestE2EK8sPipeline` | Fake clientset with pods → `NewK8SProviderWithRobot` → worker → discoverymock; add, update and delete a pod; assert the corresponding pushes (delete pushes the cached instance offline). | `k8s.io/client-go/kubernetes/fake` + informers behind the `Robot` seam (4.4 item 3). | E2E |

Supporting work (not tests, prerequisite): fix
`tools/cache/cache.go:52` (`fmt.Sprintf` without a verb) so
`go test -cover ./...` can pass; complete `testkit/fakes` coverage of
`PushAll`/`PushAllCalls`/`Run`; add `scripts/smoke.sh`; create the
`tests/e2e` package; add the two e2e seams (server elector override, k8s
`Robot` injection).

## 7. Risks and constraints

- **Offline guarantee**: every tier must avoid the production presets —
  etcd endpoints, Consul addresses and the default `--grpc-addr`
  (`172.16.130.71:50051`). Tests inject `dialDiscovery` or use the testkit
  mocks; the only exception is smoke case 5, which deliberately hits the etcd
  dial timeout and asserts the resulting message.
- **`tools/cache` vet failure** currently breaks any `./...` coverage run;
  the matrix allowlists packages, and the one-line printf fix removes the
  constraint for future `./...` adoption.
- **Legacy globals**: the consul and k8s providers log through `pkg/log` and
  notify through `pkg/notice` globals; tests that run `Run()` or
  `CompareAndFlush` must either initialize the globals in setup
  (`notice.InitNoticeClient`, `log.LoggerInit` with a temp log dir) or run in
  a temporary working directory — otherwise `app.log` and `logfiles/` appear
  in the repository (one already has).
- **Real-time waits**: `UnsyncedService.Sync` (5 s ticker),
  `ProcessIntervalFullPush` (interval configurable, so tests can pass 1–2 s)
  and the etcd member-list probe (5 s) are the only slow paths; tests must
  assert behavior without sleeping beyond them, and smoke wraps everything in
  `timeout`.
- **Elector coverage**: `pkg/worker/elector.go` cannot be tested offline
  without a `Candidate`/etcd-client seam; server-tier tests already use
  `FakeLeaderElector`, and e2e needs the seam described in 4.4. Treat direct
  elector tests as blocked-by-seam, not as forgotten.
- **Black-box selection risk**: `test-blackbox` matches by test-name prefix;
  an empty match silently passes. The implementation must assert that at
  least one `TestBlackbox*` test ran per package (for example by checking the
  test count in the output or by keeping the suites in dedicated `_test.go`
  files).
- **Budget**: measured race-suite time is ~130 s of package time; the
  aggregate `test-all` (unit + blackbox + smoke + e2e) is designed to stay
  under ~3 minutes wall clock on a dev machine.
