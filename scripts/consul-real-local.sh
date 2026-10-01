#!/usr/bin/env bash
# Disposable ARM64 Consul/Nacos 3 real-gate lifecycle.
set -uo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
artifact_root=${CONSUL_REAL_LOCAL_ARTIFACT_ROOT:-"$root/build/consul-real"}
artifact="$artifact_root/$run_id"
mkdir -p "$artifact" || { echo "CONSUL_REAL_LOCAL_ERROR: cannot create artifact directory" >&2; exit 2; }

consul_image=${CONSUL_REAL_LOCAL_CONSUL_IMAGE:-hashicorp/consul:1.22.0}
consul_digest=${CONSUL_REAL_LOCAL_CONSUL_DIGEST:-sha256:117f1fdd7cd6d84069fa0f69a0c0804b5b3bf9a30450d2143d5817b78ce757b6}
nacos_image=${CONSUL_REAL_LOCAL_NACOS_IMAGE:-nacos/nacos-server:v3.2.4-slim}
nacos_digest=${CONSUL_REAL_LOCAL_NACOS_DIGEST:-sha256:2a6d445d567b04c81404a3569309b07bfaf077216dbc3a92c0f56c9113034fb5}
consul_port=${CONSUL_REAL_LOCAL_CONSUL_PORT:-18500}
nacos_port=${CONSUL_REAL_LOCAL_NACOS_PORT:-28848}
nacos_grpc_port=${CONSUL_REAL_LOCAL_NACOS_GRPC_PORT:-29848}
nacos_raft_port=${CONSUL_REAL_LOCAL_NACOS_RAFT_PORT:-29849}
samples=${CONSUL_REAL_LOCAL_SAMPLES:-2}
startup_attempts=${CONSUL_REAL_LOCAL_STARTUP_ATTEMPTS:-90}
timeout=${CONSUL_REAL_LOCAL_TIMEOUT:-30s}
consul_name="spotter-consul-real-$run_id"
nacos_name="spotter-nacos-real-$run_id"
consul_id=""
nacos_id=""
run_rc=2
cleanup_status=passed
residual_unknown=false
qualification_status=failed
finished=false

die() {
  if [[ $# -gt 1 && "$1" =~ ^[0-9]+$ ]]; then
    run_rc=$1
    shift
  else
    run_rc=2
  fi
  echo "CONSUL_REAL_LOCAL_ERROR: $*" >&2
  exit "$run_rc"
}

inspect_arch() {
  local image=$1 digest=$2 output=$3 arch
  if ! docker image inspect "$image@$digest" --format '{{.Os}}/{{.Architecture}}' >"$artifact/$output" 2>"$artifact/$output.error"; then
    docker pull --platform linux/arm64 "$image@$digest" >"$artifact/$output.pull.log" 2>&1 || die "pull failed: $image@$digest"
    docker image inspect "$image@$digest" --format '{{.Os}}/{{.Architecture}}' >"$artifact/$output" || die "inspect failed: $image@$digest"
  fi
  arch=$(tr -d '\r\n' <"$artifact/$output")
  [[ "$arch" == linux/arm64 ]] || die "$image reports $arch, want linux/arm64"
  printf '%s %s\n' "$image@$digest" "$arch" >>"$artifact/images"
}

owner_of() {
  local id=$1 label=$2 raw
  raw=$(docker container inspect "$id" 2>/dev/null || true)
  printf '%s\n' "$raw" | jq -r --arg label "$label" '.[0].Config.Labels[$label] // empty' 2>/dev/null || true
}

remove_owned() {
  local id=$1 label=$2 name=$3 owner
  [[ -n "$id" ]] || return 0
  owner=$(owner_of "$id" "$label")
  if [[ -z "$owner" ]]; then
    owner=$(docker container inspect "$id" 2>/dev/null | sed -n '1p' || true)
  fi
  if [[ "$owner" != "$run_id" ]]; then
    echo "cleanup refused: $name ownership label mismatch" >&2
    cleanup_status=failed
    residual_unknown=true
    return 1
  fi
  docker logs "$id" >"$artifact/$name.log" 2>&1 || true
  if ! docker rm -f "$id" >>"$artifact/cleanup.log" 2>&1; then
    cleanup_status=failed
    residual_unknown=true
    return 1
  fi
  remaining=$(docker container ls -aq --filter "id=$id" 2>>"$artifact/cleanup.log")
  remaining_rc=$?
  if [[ "$remaining_rc" -ne 0 || -n "$remaining" ]]; then
    cleanup_status=failed
    residual_unknown=true
    return 1
  fi
}

finish() {
  local trap_rc=$?
  [[ "$finished" == false ]] || exit "$run_rc"
  finished=true
  trap - EXIT INT TERM
  [[ "$run_rc" != 2 || "$trap_rc" == 2 ]] || run_rc=$trap_rc
  remove_owned "$consul_id" io.spotter.consul-real.run "$consul_name" || true
  remove_owned "$nacos_id" io.spotter.nacos-real.run "$nacos_name" || true
  if [[ "$cleanup_status" != passed ]]; then
    residual_unknown=true
    [[ "$run_rc" != 0 ]] || run_rc=1
  fi
  [[ "$run_rc" != 0 || "$qualification_status" == passed ]] || run_rc=1
  if [[ "$run_rc" == 0 && "$qualification_status" == passed && "$cleanup_status" == passed && "$residual_unknown" == false ]]; then
    final_status=passed
  else
    final_status=failed
  fi
  {
    printf 'run_id=%s\n' "$run_id"
    printf 'qualification_status=%s\n' "$final_status"
    printf 'cleanup_status=%s\n' "$cleanup_status"
    printf 'residual_unknown=%s\n' "$residual_unknown"
    printf 'final_exit_code=%s\n' "$run_rc"
    printf 'artifact=%s\n' "$artifact"
  } >"$artifact/exit-metadata"
  echo "CONSUL_REAL_LOCAL_ARTIFACTS=$artifact"
  exit "$run_rc"
}
on_signal() {
  run_rc=$1
  exit "$run_rc"
}
trap finish EXIT
trap 'on_signal 130' INT
trap 'on_signal 143' TERM

[[ -z "${CONSUL_SERVER:-}" && -z "${NACOS_SERVER:-}" ]] || die "external CONSUL_SERVER/NACOS_SERVER is forbidden"
[[ -z "${DOCKER_HOST:-}" ]] || die "DOCKER_HOST is forbidden for the local target"
[[ "$samples" =~ ^[1-9][0-9]*$ && "$samples" -le 100 ]] || die "samples must be 1..100"
[[ "$startup_attempts" =~ ^[1-9][0-9]*$ ]] || die "startup attempts must be positive"
for port in "$consul_port" "$nacos_port" "$nacos_grpc_port" "$nacos_raft_port"; do
  [[ "$port" =~ ^[0-9]+$ && "$port" -ge 1 && "$port" -le 65535 ]] || die "port must be between 1 and 65535: $port"
done
[[ "$consul_port" != "$nacos_port" && "$consul_port" != "$nacos_grpc_port" && "$consul_port" != "$nacos_raft_port" && "$nacos_port" != "$nacos_grpc_port" && "$nacos_port" != "$nacos_raft_port" && "$nacos_grpc_port" != "$nacos_raft_port" ]] || die "local ports must be distinct"
[[ "$nacos_grpc_port" == "$((nacos_port + 1000))" && "$nacos_raft_port" == "$((nacos_port + 1001))" ]] || die "Nacos ports must be HTTP+1000 and HTTP+1001"

for tool in docker curl jq nc; do
  command -v "$tool" >/dev/null 2>&1 || die "required local tool is unavailable: $tool"
done

docker context inspect --format '{{(index .Endpoints "docker").Host}}' >"$artifact/docker-context" || die "Docker context inspection failed"
context_endpoint=$(tr -d '\r\n' <"$artifact/docker-context")
[[ "$context_endpoint" == unix:///* ]] || die "Docker context endpoint is not a local Unix socket: $context_endpoint"
docker info --format '{{.Architecture}}' >"$artifact/docker-architecture" || die "Docker is unavailable"
arch=$(tr -d '\r\n' <"$artifact/docker-architecture")
[[ "$arch" == arm64 || "$arch" == aarch64 || "$arch" == linux/arm64 ]] || die "Docker engine architecture is $arch, want ARM64"
for name in "$consul_name" "$nacos_name"; do
  existing=$(docker container ls -aq --filter "name=$name" 2>>"$artifact/container-check.log")
  existing_rc=$?
  [[ "$existing_rc" -eq 0 ]] || die "cannot inspect existing container names"
  [[ -z "$existing" ]] || die "container name already exists: $name"
done
for port in "$consul_port" "$nacos_port" "$nacos_grpc_port" "$nacos_raft_port"; do
  if nc -z 127.0.0.1 "$port" >/dev/null 2>&1; then die "port already occupied: $port"; fi
done
inspect_arch "$consul_image" "$consul_digest" consul-image
inspect_arch "$nacos_image" "$nacos_digest" nacos-image

consul_id=$(docker create --platform linux/arm64 --name "$consul_name" --label "io.spotter.consul-real.run=$run_id" -p "127.0.0.1:$consul_port:8500" "$consul_image@$consul_digest" agent -server -bootstrap-expect=1 -client=0.0.0.0 -bind=0.0.0.0 -ui=false) || die "Consul create failed"
nacos_token=$(printf 'spotter-nacos-real-arm64-token-2026-10' | base64 | tr -d '\n')
[[ ${#nacos_token} -ge 43 ]] || die "local Nacos JWT fixture token was not generated"
nacos_id=$(docker create --platform linux/arm64 --name "$nacos_name" --label "io.spotter.nacos-real.run=$run_id" -e MODE=standalone -e NACOS_AUTH_ENABLE=false -e NACOS_AUTH_ADMIN_ENABLE=false -e NACOS_AUTH_IDENTITY_KEY=spotter-real -e NACOS_AUTH_IDENTITY_VALUE=spotter-real-local -e NACOS_AUTH_TOKEN="$nacos_token" -e JVM_XMS=512m -e JVM_XMX=512m -e JVM_XMN=256m -p "127.0.0.1:$nacos_port:8848" -p "127.0.0.1:$nacos_grpc_port:9848" -p "127.0.0.1:$nacos_raft_port:9849" "$nacos_image@$nacos_digest") || die "Nacos create failed"
if docker start "$consul_id" >"$artifact/consul-start.log" 2>&1; then :; else
  start_rc=$?
  die "$start_rc" "Consul start failed"
fi
if docker start "$nacos_id" >"$artifact/nacos-start.log" 2>&1; then :; else
  start_rc=$?
  die "$start_rc" "Nacos start failed"
fi

for attempt in $(seq 1 "$startup_attempts"); do
  consul_ready=false
  nacos_ready=false
  if curl -fsS --max-time 2 "http://127.0.0.1:$consul_port/v1/status/leader" | grep -q ':'; then consul_ready=true; fi
  if curl -fsS --max-time 2 "http://127.0.0.1:$nacos_port/nacos/v3/admin/ns/ops/switches" >"$artifact/nacos-switches.json" 2>/dev/null && jq -e '.code == 0 and (.data.healthCheckEnabled | type == "boolean")' "$artifact/nacos-switches.json" >/dev/null 2>&1 && nc -z 127.0.0.1 "$nacos_grpc_port"; then nacos_ready=true; fi
  if [[ "$consul_ready" == true && "$nacos_ready" == true ]]; then break; fi
  [[ "$attempt" != "$startup_attempts" ]] || die 3 "scratch services did not become ready"
  sleep 2
done

set +e
CONSUL_SERVER="http://127.0.0.1:$consul_port" CONSUL_REAL_SCRATCH=1 CONSUL_REAL_ALLOW_WRITE=1 CONSUL_SOURCE_ID=consul-real-local CONSUL_REAL_SAMPLES="$samples" CONSUL_REAL_TIMEOUT="$timeout" CONSUL_TLS= CONSUL_TOKEN= CONSUL_TOKEN_FILE= CONSUL_CA_FILE= CONSUL_CERT_FILE= CONSUL_KEY_FILE= CONSUL_SERVER_NAME= CONSUL_INSECURE_SKIP_VERIFY= NACOS_SERVER="http://127.0.0.1:$nacos_port" NACOS_REAL_SCRATCH=1 NACOS_REAL_ALLOW_WRITE=1 NACOS_REAL_ALLOW_ADMIN=1 NACOS_GROUP=DEFAULT_GROUP NACOS_NAMESPACE=public NACOS_REAL_TIMEOUT="$timeout" NACOS_TLS= NACOS_USERNAME= NACOS_PASSWORD= NACOS_CA_FILE= NACOS_SERVER_NAME= NACOS_INSECURE_SKIP_VERIFY= go test -tags=consul_real ./tests/e2e -run '^TestConsulRealToNacos3Qualification$' -count=1 -v 2>&1 | tee "$artifact/test.log"
go_rc=${PIPESTATUS[0]}
set -e
run_rc=$go_rc
report_count=$(grep -c 'CONSUL_REAL_QUALIFICATION report=' "$artifact/test.log" || true)
if [[ "$report_count" -ne 1 ]]; then
  qualification_status=failed
  [[ "$run_rc" != 0 ]] || run_rc=1
fi
report_line=$(grep 'CONSUL_REAL_QUALIFICATION report=' "$artifact/test.log" | tail -n 1 || true)
if [[ -n "$report_line" ]]; then
  report_json=${report_line#*report=}
  report_json=${report_json%% cleanup_attempted=*}
  printf '%s\n' "$report_json" >"$artifact/report.json"
  if jq -e --argjson n "$samples" '(.source_id == "consul-real-local") and (.latency_boundary == "consul_agent_mutation_start_to_nacos_catalog_observed") and (.oracle_poll_interval_ms == 100) and (.configured_runs == $n) and (.cleanup_status == "passed") and (.residual_unknown == false) and (.subscribe_update_cache_when_empty == true) and (.subscribe_latency_boundary == "consul_agent_mutation_start_to_official_nacos_sdk_subscribe_callback") and ([.create,.update,.delete,.recovery,.health_down,.health_recovery,.subscribe_create,.subscribe_update,.subscribe_delete,.subscribe_recovery,.subscribe_health_down,.subscribe_health_recovery] | all(.samples == $n and (.p50_ms >= 0) and (.p90_ms >= 0) and (.p95_ms >= 0) and (.p99_ms >= 0)))' "$artifact/report.json" >/dev/null 2>&1; then
    qualification_status=passed
  fi
fi
exit "$run_rc"
