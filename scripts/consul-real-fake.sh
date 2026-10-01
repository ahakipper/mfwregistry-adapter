#!/usr/bin/env bash
# Offline lifecycle fixture; never delegates to a real Docker/curl/go command.
set -euo pipefail
tool=${0##*/}
mode=${FAKE_MODE:-pass}
state=${FAKE_STATE:?FAKE_STATE is required}
printf '%s %s\n' "$tool" "$*" >>"$state/commands"
case "$tool" in
  docker)
    case "$1 $2" in
      'info --format') printf 'linux/arm64\n';;
      'context inspect')
        if [[ "$mode" == contextremote ]]; then printf 'tcp://10.0.0.9:2375\n'; else printf 'unix:///fake/docker.sock\n'; fi;;
      'pull --platform') exit 0;;
      'image inspect')
        if [[ "$mode" == wrongarch ]]; then printf 'linux/amd64\n'; else printf 'linux/arm64\n'; fi;;
      'container ls')
        filter=${*: -1}
        if [[ "$filter" == name=* && "$mode" == nameconflict ]]; then printf '%064d\n' 9
        elif [[ "$filter" == id=* && "$mode" == cleanupcheckfail ]]; then exit 9
        elif [[ "$filter" == id=* ]]; then
          id=${filter#id=}; [[ ! -f "$state/$id" ]] || printf '%s\n' "$id"
        fi;;
      'container inspect')
        id=${*: -1}; [[ -f "$state/$id" ]] || exit 1
        if [[ "$mode" == lostownership ]]; then printf 'not-this-run\n'; else sed -n '1p' "$state/$id"; fi;;
      *)
        case "$1" in
          create)
            name= label=
            while (($#)); do
              case "$1" in --name) name=$2; shift;; --label) label=$2; shift;; esac
              shift
            done
            [[ "$name" == spotter-consul-real-* ]] && id=$(printf '%064d' 1) || id=$(printf '%064d' 2)
            printf '%s\n%s\n' "${label#*=}" "$name" >"$state/$id"
            printf '%s\n' "$id";;
          start) [[ "$mode" != startfail || "$2" != "$(printf '%064d' 2)" ]] || exit 9;;
          logs) printf 'fixture container log\n';;
          rm) [[ "$mode" != cleanupfail ]] || exit 1; rm -f "$state/${*: -1}";;
          *) echo "unsupported fake docker command: $*" >&2; exit 70;;
        esac;;
    esac;;
  curl)
    [[ "$mode" != readinessfail ]] || exit 7
    if [[ "${*: -1}" == *'/v1/status/leader' ]]; then printf '"127.0.0.1:8300"\n'
    else printf '{"code":0,"data":{"healthCheckEnabled":false}}\n'; fi;;
  nc)
    [[ "$mode" != portconflict ]] || exit 0
    [[ -f "$state/$(printf '%064d' 2)" ]] && exit 0
    exit 1;;
  go)
    if [[ "$*" == *'^TestConsulRealScaleQualification$'* ]]; then
      [[ "${CONSUL_REAL_SCALE_LIST:-}" == "100,1000,10000" ]] || exit 70
      report='{"latency_boundary":"fixture","push_concurrency":8,"ledger_complete":true,"canonical_equality":"wire_predicate_passed","cleanup_status":"passed","residual_unknown":false,"scales":['
      first=true
      IFS=',' read -r -a scale_values <<< "${CONSUL_REAL_SCALE_LIST}"
      for scale in "${scale_values[@]}"; do
        [[ "$first" == true ]] || report="$report,"
        first=false
        report="$report{\"instances\":$scale,\"runs\":1,\"sync_all_events\":1,\"watch_to_provider_sync\":{\"samples\":$scale,\"p80_ms\":1,\"p90_ms\":1,\"p99_ms\":1},\"provider_sync_to_nacos_ack\":{\"samples\":$scale,\"p80_ms\":1,\"p90_ms\":1,\"p99_ms\":1},\"nacos_ack_to_catalog\":{\"samples\":$scale,\"p80_ms\":1,\"p90_ms\":1,\"p99_ms\":1},\"nacos_ack_to_subscribe\":{\"samples\":$scale,\"p80_ms\":1,\"p90_ms\":1,\"p99_ms\":1},\"catalog\":{\"samples\":$scale,\"p80_ms\":1,\"p90_ms\":1,\"p99_ms\":1},\"subscribe\":{\"samples\":$scale,\"p80_ms\":1,\"p90_ms\":1,\"p99_ms\":1},\"stage_samples_complete\":true}"
      done
      report="$report]}"
      printf '    consul_scale_real_test.go:1: CONSUL_REAL_SCALE report=%s\n' "$report"
      echo '--- PASS: TestConsulRealScaleQualification'; echo PASS
      exit 0
    fi
    [[ "$*" == *'^TestConsulRealToNacos3Qualification$'* ]] || exit 70
    [[ "${CONSUL_SERVER:-}" == http://127.0.0.1:* && "${NACOS_SERVER:-}" == http://127.0.0.1:* ]] || exit 70
    [[ "${NACOS_REAL_ALLOW_ADMIN:-}" == 1 ]] || exit 70
    [[ "${CONSUL_TOKEN:-}" == '' && "${NACOS_USERNAME:-}" == '' ]] || exit 70
    [[ "$mode" != gotestfail ]] || { echo 'fixture go failure'; exit 17; }
    [[ "$mode" != skip ]] || { echo 'NOT VERIFIED: fixture missing guard'; echo '--- SKIP: TestConsulRealToNacos3Qualification'; exit 0; }
    [[ "$mode" != missingreport ]] || { echo 'PASS'; exit 0; }
    samples=${CONSUL_REAL_SAMPLES:-2}
    count=$samples; [[ "$mode" != incompletesample ]] || count=0
    cleanup=passed; [[ "$mode" != reportcleanupfail ]] || cleanup=failed
    report="{\"source_id\":\"consul-real-local\",\"latency_boundary\":\"consul_agent_mutation_start_to_nacos_catalog_observed\",\"oracle_poll_interval_ms\":100,\"configured_runs\":$samples,\"cleanup_status\":\"$cleanup\",\"residual_unknown\":false"
    for operation in create update delete recovery health_down health_recovery; do
      report="$report,\"$operation\":{\"samples\":$count,\"p50_ms\":1,\"p90_ms\":1,\"p95_ms\":1,\"p99_ms\":1}"
    done
    for operation in subscribe_create subscribe_update subscribe_delete subscribe_recovery subscribe_health_down subscribe_health_recovery; do
      report="$report,\"$operation\":{\"samples\":$count,\"p50_ms\":1,\"p90_ms\":1,\"p95_ms\":1,\"p99_ms\":1,\"events\":$count,\"callback_errors\":0}"
    done
    report="$report,\"subscribe_update_cache_when_empty\":true,\"subscribe_latency_boundary\":\"consul_agent_mutation_start_to_official_nacos_sdk_subscribe_callback\""
    report="$report}"
    printf '    consul_real_test.go:1: CONSUL_REAL_QUALIFICATION report=%s cleanup_attempted=true cleanup_errors=0\n' "$report"
    [[ "$mode" != duplicatereport ]] || printf 'CONSUL_REAL_QUALIFICATION report=%s cleanup_attempted=true cleanup_errors=0\n' "$report"
    echo '--- PASS: TestConsulRealToNacos3Qualification'; echo PASS;;
  *) echo "unsupported fixture tool: $tool" >&2; exit 70;;
esac
