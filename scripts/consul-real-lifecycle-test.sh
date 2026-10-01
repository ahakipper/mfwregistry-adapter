#!/usr/bin/env bash
# Failure-path qualification without pulling images or contacting services.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
tmp=$(mktemp -d "${TMPDIR:-/tmp}/consul-real-life.XXXXXX")
trap 'rm -rf "$tmp"' EXIT
mkdir "$tmp/bin"
for tool in docker curl go nc; do ln -s "$root/scripts/consul-real-fake.sh" "$tmp/bin/$tool"; done

run_case() {
  local mode=$1 expected=$2
  shift 2
  local state="$tmp/$mode" rc
  mkdir "$state"
  set +e
  env PATH="$tmp/bin:$PATH" FAKE_STATE="$state" FAKE_MODE="$mode" \
    CONSUL_REAL_LOCAL_STARTUP_ATTEMPTS=1 CONSUL_REAL_LOCAL_SAMPLES=2 \
    "$@" /bin/bash "$root/scripts/consul-real-local.sh" >"$state/output" 2>&1
  rc=$?
  set -e
  [[ "$rc" == "$expected" ]] || { echo "FAIL $mode: exit $rc, expected $expected"; sed -n '1,120p' "$state/output"; exit 1; }
  local out
  out=$(sed -n 's/^CONSUL_REAL_LOCAL_ARTIFACTS=//p' "$state/output")
  [[ -d "$out" && -s "$out/exit-metadata" ]] || { echo "FAIL $mode: missing final metadata"; exit 1; }
  grep -qx "final_exit_code=$expected" "$out/exit-metadata"
  if [[ "$expected" == 0 ]]; then
    grep -qx 'qualification_status=passed' "$out/exit-metadata"
    grep -qx 'cleanup_status=passed' "$out/exit-metadata"
    [[ -s "$out/report.json" && -s "$out/consul-container.log" && -s "$out/nacos-container.log" ]]
    grep -q -- '-p 127.0.0.1:18500:8500' "$state/commands"
    grep -q -- '-p 127.0.0.1:28848:8848 -p 127.0.0.1:29848:9848 -p 127.0.0.1:29849:9849' "$state/commands"
  else
    grep -qx 'qualification_status=failed' "$out/exit-metadata"
  fi
  if [[ "$mode" == cleanupfail || "$mode" == cleanupcheckfail || "$mode" == lostownership ]]; then
    grep -qx 'cleanup_status=failed' "$out/exit-metadata"
    grep -qx 'residual_unknown=true' "$out/exit-metadata"
  else
    grep -qx 'cleanup_status=passed' "$out/exit-metadata"
    [[ ! -f "$state/$(printf '%064d' 1)" && ! -f "$state/$(printf '%064d' 2)" ]]
  fi
  # A mismatching owner label must never be deleted, even during teardown.
  if [[ "$mode" == lostownership ]]; then ! grep -q '^docker rm ' "$state/commands"; fi
  echo "consul-real lifecycle: $mode PASS"
}

run_case pass 0
run_case gotestfail 17
run_case startfail 9
for mode in skip missingreport incompletesample reportcleanupfail duplicatereport cleanupfail cleanupcheckfail lostownership; do run_case "$mode" 1; done
for mode in nameconflict portconflict wrongarch; do run_case "$mode" 2; done
run_case readinessfail 3
run_case remoteendpoint 2 CONSUL_SERVER=http://consul.production.invalid:8500
run_case dockerhost 2 DOCKER_HOST=tcp://10.0.0.9:2375
run_case contextremote 2
run_case badport 2 CONSUL_REAL_LOCAL_NACOS_PORT=65000
run_case badportzero 2 CONSUL_REAL_LOCAL_NACOS_PORT=0 CONSUL_REAL_LOCAL_NACOS_GRPC_PORT=1000 CONSUL_REAL_LOCAL_NACOS_RAFT_PORT=1001
run_case badmapping 2 CONSUL_REAL_LOCAL_NACOS_GRPC_PORT=29847
run_case badsample 2 CONSUL_REAL_LOCAL_SAMPLES=0
echo 'consul-real offline lifecycle shell tests: PASS'
