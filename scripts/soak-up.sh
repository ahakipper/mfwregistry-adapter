#!/usr/bin/env bash
# soak-up.sh brings up the local soak stack of docs/nacos-sink-plan.md §8:
# nacos (18848), consul (18500) and k3s (6443) via docker compose, then
# health-waits every component and extracts the k3s kubeconfig for the
# harness (host kubectl speaks to 127.0.0.1:6443 — F0-verified).
#
# Usage: scripts/soak-up.sh [WORKDIR]
#   WORKDIR (default: build/soak) receives kubeconfig.
#
# Environment:
#   SOAK_STACK_WAIT_SECS  overall health-wait budget (default 300)
#
# Exits nonzero with a pointed message on any failed prerequisite
# (docker/colima not running, kubectl missing, host ports occupied).

set -euo pipefail

STACK_FILE="$(cd "$(dirname "$0")" && pwd)/soak-stack.yml"
WORKDIR=${1:-$(cd "$(dirname "$0")/.." && pwd)/build/soak}
WAIT_BUDGET=${SOAK_STACK_WAIT_SECS:-300}

# --- prerequisites ----------------------------------------------------------

if ! command -v docker >/dev/null 2>&1; then
    echo "soak-up: docker is not installed or not on PATH" >&2
    exit 1
fi
if ! docker info >/dev/null 2>&1; then
    echo "soak-up: the docker daemon is unreachable (is colima started? 'colima start --cpu 6 --memory 10 --vz-rosetta')" >&2
    exit 1
fi
if ! command -v kubectl >/dev/null 2>&1; then
    echo "soak-up: kubectl is required (kubeconfig extraction + churn driver)" >&2
    exit 1
fi
if ! command -v docker-compose >/dev/null 2>&1 && ! docker compose version >/dev/null 2>&1; then
    echo "soak-up: docker compose is required (v2 plugin or docker-compose)" >&2
    exit 1
fi

compose() {
    if docker compose version >/dev/null 2>&1; then
        docker compose -f "$STACK_FILE" "$@"
    else
        docker-compose -f "$STACK_FILE" "$@"
    fi
}

# Port pre-flight: the 8848 trap of plan §8.2 — host 8848 is bound by a
# colima SSH tunnel, so nacos HTTP is mapped to 18848. Any listener already
# sitting on the remapped ports fails fast with the port table. Docker's
# userland proxy releases published ports a few seconds AFTER `compose
# down` returns, so a short retry window absorbs that teardown race.
ports_free() {
    for port in 18848 19848 19849 18500 6443; do
        if lsof -nP -i :"$port" >/dev/null 2>&1; then
            return 1
        fi
    done
    return 0
}
for _ in $(seq 1 10); do
    if ports_free; then
        break
    fi
    sleep 3
done
if ! ports_free; then
    echo "soak-up: one of the soak host ports is already in use (plan §8.2 port table: nacos 18848/19848/19849, consul 18500, k3s 6443)" >&2
    lsof -nP -i :18848 -i :19848 -i :19849 -i :18500 -i :6443 >&2 || true
    exit 1
fi

# --- bring the stack up -----------------------------------------------------

mkdir -p "$WORKDIR"
echo "soak-up: starting compose stack ($STACK_FILE)"
compose up -d

# --- health-wait ------------------------------------------------------------

deadline=$((SECONDS + WAIT_BUDGET))

wait_healthy() {
    local service=$1
    while true; do
        if [ "$(docker inspect -f '{{.State.Health.Status}}' soak-"$service" 2>/dev/null || echo starting)" = "healthy" ]; then
            echo "soak-up: $service is healthy"
            return 0
        fi
        if [ "$SECONDS" -ge "$deadline" ]; then
            echo "soak-up: $service did not become healthy within ${WAIT_BUDGET}s" >&2
            docker logs soak-"$service" 2>&1 | tail -n 30 >&2 || true
            return 1
        fi
        sleep 5
    done
}

# nacos readiness: the JVM takes ~2-3 min on the 6 vCPU floor (plan §8.2).
wait_healthy nacos
# curl the readiness endpoint from the host as well — the container
# healthcheck and the host mapping must both work. THREE consecutive
# successes are required: on the ARM/rosetta JVM the console can answer
# readiness briefly while the application context is still loading, and a
# single passing probe releases the harness into a half-booted nacos.
nacos_ready_streak=0
for _ in $(seq 1 90); do
    if curl -sf "http://127.0.0.1:18848/nacos/v1/console/health/readiness" >/dev/null 2>&1; then
        nacos_ready_streak=$((nacos_ready_streak + 1))
        if [ "$nacos_ready_streak" -ge 3 ]; then
            echo "soak-up: nacos readiness endpoint stable on 127.0.0.1:18848 (3 consecutive probes)"
            break
        fi
    else
        nacos_ready_streak=0
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
        echo "soak-up: nacos readiness endpoint never answered stably on the host mapping" >&2
        exit 1
    fi
    sleep 3
done

# consul: leader via the host mapping.
while true; do
    if curl -sf "http://127.0.0.1:18500/v1/status/leader" 2>/dev/null | grep -q ':'; then
        echo "soak-up: consul leader is elected (127.0.0.1:18500)"
        break
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
        echo "soak-up: consul did not elect a leader within ${WAIT_BUDGET}s" >&2
        exit 1
    fi
    sleep 3
done

# k3s: node ready via kubectl against the extracted kubeconfig. Extraction
# and verification are ONE retry cycle: k3s can answer readyz with a
# kubeconfig it then rotates (fresh volumes rotate serving certs during
# boot), so a config extracted before the rotation fails `get nodes` — the
# loop re-extracts until the CURRENT file both satisfies readyz and lists
# nodes. A second verification 8s later guards against a rotation landing
# between the first verify and the harness's first use.
KUBECONFIG_OUT="$WORKDIR/kubeconfig"
k3s_ready=0
for _ in $(seq 1 "$((WAIT_BUDGET / 5 + 1))"); do
    if docker exec soak-k3s cat /etc/rancher/k3s/k3s.yaml >"$KUBECONFIG_OUT" 2>/dev/null; then
        chmod 600 "$KUBECONFIG_OUT"
        if KUBECONFIG="$KUBECONFIG_OUT" kubectl get --raw=/readyz >/dev/null 2>&1 &&
            KUBECONFIG="$KUBECONFIG_OUT" kubectl get nodes >/dev/null 2>&1; then
            sleep 8
            docker exec soak-k3s cat /etc/rancher/k3s/k3s.yaml >"$KUBECONFIG_OUT" 2>/dev/null || true
            chmod 600 "$KUBECONFIG_OUT"
            if KUBECONFIG="$KUBECONFIG_OUT" kubectl get nodes >/dev/null 2>&1; then
                echo "soak-up: k3s readyz answers and nodes list (kubeconfig stable); written to $KUBECONFIG_OUT"
                k3s_ready=1
                break
            fi
        fi
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
        echo "soak-up: k3s did not become ready within ${WAIT_BUDGET}s" >&2
        exit 1
    fi
    sleep 5
done

if [ "$k3s_ready" -ne 1 ] || [ ! -s "$KUBECONFIG_OUT" ]; then
    echo "soak-up: kubeconfig extraction/verification failed" >&2
    exit 1
fi

# Pre-pull the churn image: scenario (h) scales to 100 pods at once, and
# letting k3s pull busybox concurrently 100 times turns the batch bound
# into an image-pull measurement. One pre-pull keeps the bound about the
# event pipeline (plan §8.5 (h)).
if ! KUBECONFIG="$KUBECONFIG_OUT" kubectl run soak-image-prepull --image=busybox:1.36 --restart=Never --command -- true >/dev/null 2>&1; then
    echo "soak-up: WARNING: busybox pre-pull failed (scenario (h) may be slow)" >&2
else
    KUBECONFIG="$KUBECONFIG_OUT" kubectl wait --for=condition=Ready pod/soak-image-prepull --timeout="${WAIT_BUDGET}s" >/dev/null 2>&1 || true
    KUBECONFIG="$KUBECONFIG_OUT" kubectl delete pod soak-image-prepull --ignore-not-found=true >/dev/null 2>&1 || true
    echo "soak-up: busybox:1.36 pre-pulled into k3s"
fi

echo "soak-up: stack is up (nacos 18848, consul 18500, k3s 6443, kubeconfig $KUBECONFIG_OUT)"
