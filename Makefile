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
# break the matrix, and spotter/tools/cache currently fails vet under -cover
# (fmt.Sprintf with arguments but no formatting directives, cache.go:52).
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

# E2E tier: full pipeline against in-process mocks, no network.
test-e2e:
	go test -race -count=1 -tags=e2e ./tests/e2e/...

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
# Prerequisites: docker (the nacos image is the soak stack's tag), a
# running kwok cluster, and the kwok node's capacity patched per
# docs/dsca-1-scale.md. NOT part of test-all (hours of wall clock).
OBS_DURATION ?= 2h
OBS_TIMEOUT ?= 180m
OBS_SCALE ?= 1000
OBS_SERVICES ?= 20
OBS_TICK ?= 10s
OBS_CHURN_EVERY ?= 20s
OBS_CHURN_RATE ?= 5

test-observe:
	@mkdir -p build/observe
	go build -o build/observe/spotter .
	@status=0; \
	OBS_DURATION=$(OBS_DURATION) OBS_SCALE=$(OBS_SCALE) OBS_SERVICES=$(OBS_SERVICES) \
	OBS_TICK=$(OBS_TICK) OBS_CHURN_EVERY=$(OBS_CHURN_EVERY) OBS_CHURN_RATE=$(OBS_CHURN_RATE) \
	SPOTTER_BIN=build/observe/spotter \
		go test -tags=observe -run '^TestObserveConsistency$$|^TestObserveUnit' -timeout $(OBS_TIMEOUT) -v ./tests/observe/... || status=$$?; \
	exit $$status

.PHONY: test-observe

# Aggregate: everything, in tier order. Budget ~3 min on a dev machine.
test-all: test-unit test-blackbox test-smoke test-e2e

.PHONY: test-unit test-unit-coverage test-fast test-blackbox test-smoke test-e2e test-all