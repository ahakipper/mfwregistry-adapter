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