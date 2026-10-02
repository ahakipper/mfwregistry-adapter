all: build

#usage
#

# Predefined variables
ENV ?= dev
OS ?= linux
DOCKER_VERSION ?= latest

build:
	GOOS=$(OS) go build -o spotter

# --- Test matrix -----------------------------------------------------------

# Packages under test. Explicit allowlist: untested legacy packages cannot
# break the matrix. The historical tools/cache vet failure was cleared in
# eb6bf0c; future coverage expansion should add legacy packages deliberately,
# not hide new diagnostics behind this list.
TEST_PKGS := ./internal/... \
	./pkg/discoverycenter \
	./pkg/nacos \
	./pkg/worker \
	./pkg/providers/consul \
	./pkg/providers/k8s

# Packages hosting black-box suites (tests named TestBlackbox*).
BLACKBOX_PKGS := ./pkg/discoverycenter ./pkg/worker ./pkg/providers/consul ./pkg/nacos

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
# The loop is the empty-match guard from docs/testing.md section 5.1:
# `go test -list` prints "ok <pkg>" followed by one line per matching test,
# so a package with zero TestBlackbox* tests yields only the "ok" line and
# the guard fails the target instead of silently passing.
test-blackbox:
	@for pkg in $(BLACKBOX_PKGS); do \
		matched=$$(go test -list '^TestBlackbox' $$pkg 2>/dev/null | grep -c '^TestBlackbox' || true); \
		if [ "$$matched" -eq 0 ]; then \
			echo "FAIL test-blackbox: no TestBlackbox* tests matched in $$pkg" >&2; \
			exit 1; \
		fi; \
		echo "test-blackbox: $$matched TestBlackbox* test(s) in $$pkg"; \
	done
	go test -race -count=1 -run '^TestBlackbox' $(BLACKBOX_PKGS)

# Smoke tier: build once, then run the offline binary checks.
SMOKE_BIN := ./build/spotter

test-smoke:
	go build -o $(SMOKE_BIN) .
	./scripts/smoke.sh $(SMOKE_BIN)

# E2E tier: full pipeline against in-process loopback mocks/embedded etcd;
# no external network or shared service is used.
test-e2e:
	go test -race -count=1 -tags=e2e ./internal ./pkg/providers/k8s ./tests/e2e/...

# Soak tier (plan docs/nacos-sink-plan.md §8, decision D5): the local
# full-stack e2e + 1h soak. NOT part of test-all — it needs the colima
# docker engine (6 vCPU / ~10 GiB floor), pulls ~1.5 GiB of images and
# takes 1h wall clock plus stack boot.
#
#   make test-soak                # the real 1-hour run
#   SOAK_DURATION=90s make test-soak   # the harness's smoke shakedown
#                                       (minutes, compressed cadences)
#
# The stack (nacos 18848, consul 18500, k3s 6443) comes up via
# scripts/soak-up.sh with health-wait and kubeconfig extraction; the
# tagged test below owns everything dynamic (embedded etcd, the Atlas
# stand-in, the spotter child, churn, assertions, scenarios) and tears it
# down itself; compose goes down afterwards either way.
SOAK_DURATION ?= 1h
SOAK_TIMEOUT ?= 90m

test-soak:
	./scripts/soak-up.sh
	@status=0; \
	go build -o build/spotter . || status=$$?; \
	if [ $$status -eq 0 ]; then \
		SOAK_DURATION=$(SOAK_DURATION) SPOTTER_BIN=build/spotter KUBECONFIG=build/soak/kubeconfig \
			go test -tags=soak -run '^TestSoakLocalStack$$' -timeout $(SOAK_TIMEOUT) -v ./tests/soak/... || status=$$?; \
	fi; \
	./scripts/soak-down.sh; \
	exit $$status

.PHONY: test-soak

# Observe tier (docs/dsca-4-observation.md §4/§4.6, batch Fix-D): the
# sustained large-scale consistency observation. Owns a THROWAWAY stack
# (nacos docker container on scratch port 28848, in-process Atlas
# stand-in on 19997, embedded etcd, own spotter child on scratch metrics
# port 19998) against a kwok cluster (default: the dsca1 cluster's
# kubeconfig at ~/.kwok/clusters/dsca1/kubeconfig.yaml). The demo stack
# (18848/18500/6443/12379/19999/19848/19849/18090) is never touched.
#
#   make test-observe                                  # the definitive 2h/1000 sustained run
#   OBS_DURATION=30m OBS_SCALE=100 make test-observe   # the 30m/100 rehearsal (bursts ON)
#   OBS_DURATION=25m make test-observe                 # the 25m/1000 burst-augmented window
#   OBS_DURATION=1m OBS_SCALE=10 OBS_BURSTS=false make test-observe  # minutes-scale micro-run
#
# Knobs: OBS_DURATION (2h), OBS_SCALE (1000), OBS_SERVICES (20),
# OBS_TICK (10s), OBS_CHURN_EVERY (20s), OBS_CHURN_RATE (5 %/min),
# OBS_BURSTS (default: on for windows <= 30m — the §4.2 burst schedule;
# false keeps the sustained-churn-only shape),
# OBS_KUBECONFIG, OBS_NACOS_ADDR (127.0.0.1:28848),
# OBS_ATLAS_PORT (19997), OBS_METRICS_PORT (19998).
# Prerequisites: docker (the harness pulls the pinned Nacos 3 ARM64 image), a
# running kwok cluster, and the kwok node's capacity patched per
# docs/dsca-1-scale.md. NOT part of test-all (hours of wall clock).
OBS_DURATION ?= 2h
OBS_TIMEOUT ?= 180m
OBS_SCALE ?= 1000
OBS_SERVICES ?= 20
OBS_TICK ?= 10s
OBS_CHURN_EVERY ?= 20s
OBS_CHURN_RATE ?= 5
OBS_BURSTS ?= false

test-observe:
	@if [ -n "$(OBS_KUBECONFIG)" ]; then echo 'OBS_KUBECONFIG external mode: running read-only unit gates'; go test -race -tags=observe -run '^TestObserveUnit' -v ./tests/observe/...; exit $$?; fi; \
	status=0; trap './scripts/observe-down.sh' 0 2 15; \
	./scripts/observe-up.sh || status=$$?; \
	if [ $$status -eq 0 ]; then mkdir -p build/observe && go build -o build/observe/spotter . || status=$$?; fi; \
	if [ $$status -ne 0 ]; then exit $$status; fi; \
	OBS_DURATION=$(OBS_DURATION) OBS_SCALE=$(OBS_SCALE) OBS_SERVICES=$(OBS_SERVICES) \
	OBS_TICK=$(OBS_TICK) OBS_CHURN_EVERY=$(OBS_CHURN_EVERY) OBS_CHURN_RATE=$(OBS_CHURN_RATE) \
	OBS_BURSTS=$(OBS_BURSTS) \
	SPOTTER_BIN=build/observe/spotter \
		go test -tags=observe -run '^TestObserveConsistency$$|^TestObserveUnit' -timeout $(OBS_TIMEOUT) -v ./tests/observe/... || status=$$?; \
	./scripts/observe-down.sh; \
	exit $$status

.PHONY: test-observe

# Scale-ladder tier for the KWork reliability report. This is intentionally
# separate from the sustained two-hour gate so P90/P95/P99 can be attributed
# to exact create/delete batch sizes and the CrashLoopBackOff transition.
# The target owns the same throwaway kwok/Nacos stack and writes a dedicated
# tests/observe/results/<stamp>-scale-ladder-summary.{json,md} report.
OBS_LADDER_TIMEOUT ?= 90m

test-observe-ladder:
	@if [ -n "$(OBS_KUBECONFIG)" ]; then echo 'OBS_KUBECONFIG external mode is read-only; scale ladder requires an owned writable stack'; exit 2; fi; \
	status=0; trap './scripts/observe-down.sh' 0 2 15; \
	./scripts/observe-up.sh || status=$$?; \
	if [ $$status -eq 0 ]; then mkdir -p build/observe && go build -o build/observe/spotter . || status=$$?; fi; \
	if [ $$status -eq 0 ]; then \
		OBS_NODE_POD_CAPACITY=$${OBS_NODE_POD_CAPACITY:-1200} SPOTTER_BIN=build/observe/spotter \
			go test -tags=observe -run '^TestObserveScaleLadder$$' -timeout $(OBS_LADDER_TIMEOUT) -v ./tests/observe/... || status=$$?; \
	fi; \
	./scripts/observe-down.sh; \
	exit $$status

.PHONY: test-observe-ladder

# Guarded local Consul -> Spotter -> Nacos 3 qualification. This target owns
# only two disposable linux/arm64 containers, pins both image digests, refuses
# preconfigured remote endpoints and occupied ports, and leaves the JSON
# report/logs under build/consul-real/<run-id> for later comparison. It is
# intentionally separate from test-all because it performs real writes and
# takes up to the configured startup timeout plus the E2E run.
#
#   make test-consul-real-local
#   CONSUL_REAL_LOCAL_SAMPLES=5 make test-consul-real-local
#   make test-consul-real-config
test-consul-real-local:
	./scripts/consul-real-local.sh

# Consul scale-ladder E2E: one shared application with 1, 100, 1,000 and 10,000
# instances. Each instance contributes a mutation-start -> catalog and
# mutation-start -> official SDK Subscribe sample; the report contains P80,
# P90 and P99 per scale plus four stage summaries. CONSUL_SCALE_PUSH_CONCURRENCY
# allows a controlled Nacos-capacity A/B without changing the Consul Watch
# freshness path. The disposable Consul fixture explicitly sets
# limits.http_max_conns_per_client=10000; production Consul limits are not
# changed. The observation deadline is fixed at 5s by the E2E parser. This is
# a real-write, potentially long-running gate. CONSUL_SCALE_QUERY_GATE is off
# by default because the Nacos gRPC Query cache defect is tracked separately;
# Subscribe remains the service-discovery acceptance plane. The target is
# deliberately excluded from test-all.
CONSUL_SCALE_TIMEOUT ?= 30m
CONSUL_SCALE_LIST ?= 1,100,1000,10000
CONSUL_SCALE_RUNS ?= 1
CONSUL_SCALE_OBSERVE_TIMEOUT ?= 5s
CONSUL_SCALE_RPC_TIMEOUT ?= 30s
CONSUL_SCALE_PUSH_CONCURRENCY ?= 8
CONSUL_SCALE_QUERY_GATE ?= off

test-consul-real-scale:
	CONSUL_REAL_LOCAL_TEST_TAGS='consul_real,consul_scale_real' \
	CONSUL_REAL_LOCAL_RUN='^TestConsulRealScaleQualification$$' \
	CONSUL_REAL_LOCAL_REPORT_MARKER='CONSUL_REAL_SCALE' \
	CONSUL_REAL_LOCAL_REPORT_MODE=scale \
	CONSUL_REAL_LOCAL_SCALE_LIST=$(CONSUL_SCALE_LIST) \
	CONSUL_REAL_LOCAL_SCALE_RUNS=$(CONSUL_SCALE_RUNS) \
	CONSUL_REAL_LOCAL_SCALE_OBSERVE_TIMEOUT=$(CONSUL_SCALE_OBSERVE_TIMEOUT) \
	CONSUL_REAL_LOCAL_SCALE_PUSH_CONCURRENCY=$(CONSUL_SCALE_PUSH_CONCURRENCY) \
	CONSUL_REAL_LOCAL_SCALE_QUERY_GATE=$(CONSUL_SCALE_QUERY_GATE) \
	CONSUL_REAL_LOCAL_RPC_TIMEOUT=$(CONSUL_SCALE_RPC_TIMEOUT) \
	CONSUL_REAL_LOCAL_TIMEOUT=$(CONSUL_SCALE_TIMEOUT) \
	./scripts/consul-real-local.sh

# Offline parser/percentile and lifecycle-contract checks. No Docker daemon,
# Consul, Nacos, or network write is contacted by this target.
test-consul-real-config:
	go test -tags=consul_real ./tests/e2e -run '^Test(ParseConsulRealConfig|ParseNacosRealConfig|Percentile)' -count=1
	bash -n scripts/consul-real-local.sh scripts/consul-real-fake.sh scripts/consul-real-lifecycle-test.sh
	grep -q 'http_max_conns_per_client = 10000' scripts/consul-real-limits.hcl
	bash scripts/consul-real-lifecycle-test.sh

.PHONY: test-consul-real-local test-consul-real-scale test-consul-real-config

# Aggregate: everything, in tier order. Budget ~3 min on a dev machine.
test-all: test-unit test-blackbox test-smoke test-e2e

.PHONY: test-unit test-unit-coverage test-fast test-blackbox test-smoke test-e2e test-all
