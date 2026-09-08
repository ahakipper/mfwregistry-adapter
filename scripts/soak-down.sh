#!/usr/bin/env bash
# soak-down.sh tears the local soak stack down: compose down (containers and
# the named volumes — the k3s state and the nacos derby store are run
# artifacts, not state worth keeping) and a report of anything left behind.
#
# Usage: scripts/soak-down.sh
#
# The dynamic pieces the harness owned (embedded etcd, Atlas stand-in,
# spotter child) are cleaned by the test's own defers + signal handling.

set -euo pipefail

STACK_FILE="$(cd "$(dirname "$0")" && pwd)/soak-stack.yml"

if ! docker info >/dev/null 2>&1; then
    echo "soak-down: the docker daemon is unreachable; nothing to do" >&2
    exit 0
fi

if docker compose version >/dev/null 2>&1; then
    docker compose -f "$STACK_FILE" down -v
else
    docker-compose -f "$STACK_FILE" down -v
fi

leftovers=$(docker ps -a --filter "name=soak-" --format '{{.Names}}' 2>/dev/null || true)
if [ -n "$leftovers" ]; then
    echo "soak-down: WARNING: containers left behind: $leftovers" >&2
    docker ps -a --filter "name=soak-" --format '{{.Names}} {{.Status}}' >&2 || true
    exit 1
fi

echo "soak-down: stack is down"
