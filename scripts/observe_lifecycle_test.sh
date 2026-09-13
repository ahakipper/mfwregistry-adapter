#!/usr/bin/env bash
set -euo pipefail
grep -q 'OBS_KUBECONFIG external mode' Makefile
grep -q 'external kubeconfig mode: no deletion' scripts/observe-down.sh
bash -n scripts/observe-up.sh scripts/observe-down.sh
echo 'observe lifecycle contract: PASS'
