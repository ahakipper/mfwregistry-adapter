# Test Reliability Audit — Execution Plan

**Status:** IN PROGRESS
**Date:** 2026-09-10
**Trigger:** F7 (empty-IP offline shell spun the nacos retry queue 9624× per instance for ~13h in the live demo) shipped through a green test matrix of 308 tests at 68.2% coverage. The suite did not catch it because the scenario (a pod dying before ever receiving an IP, then a permanent 4xx answer) was never modeled anywhere — not in unit tests, not in blackbox tests, not in e2e, not in the 1h soak. If that class of defect escaped, others likely did.

## Objective

Audit every test in the repository for:
1. **Reliability** — does the test actually pin the behavior it claims? (mutation-survivability mindset: if the code under test regressed, would the test fail?)
2. **Boundary coverage** — are edge cases tested (empty/nil inputs, zero values, error paths, permanent vs retriable failures, hidden-view vs served-view, idempotency, race windows)?
3. **Global perspective** — do the tests verify the system the way it runs in production (provider → worker → fanout → sink chain, leader election, retry queue), or only isolated units with mocks that encode the same assumptions the production code makes?

## Ground rules (binding, from the user)

- Problem discovery: multiple agents in PARALLEL.
- Bug fixing: strictly SERIALIZED through agent-1, reviewed by agent-2 per fix batch (avoid cross-fix interference).
- Every flagged issue must be confirmed by FULL code reading — no flagging from test names or vibes; the auditor must read both the test and the production code it exercises.
- Every verification claim must be actually executed (tests run, output captured) — no "should pass".
- Findings land in `docs/test-reliability-audit.md`; fixes happen only after the findings document is complete, in a unified pass.

## Audit domain split (4 parallel auditors)

| Auditor | Scope (read EVERYTHING, test + production) | Focus |
|---|---|---|
| A | pkg/worker (7 test files, 47 tests) + pkg/beehive + pkg/providers/aggregate | retry queue semantics, fanout contract, elector wiring, the F7-adjacent surfaces |
| B | pkg/nacos + internal/testkit/nacosmock + pkg/discoverycenter + internal/testkit/discoverymock | sink policy, prune blind spot (F8: instance/list hides IP-DISABLED), mock fidelity vs real nacos 2.1.0 wire behavior |
| C | pkg/providers/{k8s,consul,common,cache} + internal/testkit/{consulmock,fakes} + pkg/k8srobot | conversion boundaries (empty IP, pending, terminated), cache semantics, consul check-matching, the F7-adjacent source guards |
| D | internal/{composition,server,domain/instance,infra/*} + tests/e2e + tests/soak + pkg/etcd + pkg/distribute/election + pkg/distribute | composition wiring, config, e2e scenario realism, soak assertion power, election |

Each auditor delivers: findings table (ID, severity, file:line, claim, evidence), each finding re-verified by running the relevant test or a targeted experiment.

## Fix protocol (after findings doc)

1. agent-1 fixes findings in priority order (P0 → P1 → P2), one batch per commit, tests-first where applicable.
2. agent-2 reviews each batch adversarially; P0/P1 blockers loop back to agent-1.
3. Lead runs the full matrix (`go test ./... -count=1`, plus `make test-blackbox`/`test-smoke`/`test-e2e` where relevant) after every batch.
4. Detailed English commit + push after each batch.

## Known real bugs already found (feed into the audit's fix phase)

- **F8 prune blind spot** (pkg/nacos/nacos.go prune): the prune listing uses `v1/ns/instance/list`, which hides IP-DISABLED instances on nacos 2.1.0 — an out-of-date remote instance that fails health probes can never be pruned. Candidate fix: list via `v1/ns/catalog/instances` (sees the datum store; live-verified params: serviceName, clusterName, groupName, pageSize, pageNo, namespaceId=public, hasIpCount=false).
