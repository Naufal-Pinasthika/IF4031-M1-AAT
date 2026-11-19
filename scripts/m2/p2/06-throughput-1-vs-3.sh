#!/usr/bin/env bash
set -euo pipefail
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)/lib/demo-common.sh"
require_command kubectl
require_command k6
require_command jq
: "${AGGREGATOR_API_KEY:?AGGREGATOR_API_KEY is required}"

one_result="$(mktemp)"
three_result="$(mktemp)"
cleanup() {
  kubectl -n "${NAMESPACE}" scale deployment/aggregator --replicas=3 >/dev/null || true
  rm -f -- "${one_result}" "${three_result}"
}
trap cleanup EXIT

run_case() {
  local replicas="$1" output="$2"
  heading "P2.6 measuring throughput with ${replicas} Aggregator instance(s)"
  kubectl -n "${NAMESPACE}" scale deployment/aggregator --replicas="${replicas}"
  kubectl -n "${NAMESPACE}" rollout status deployment/aggregator --timeout=180s
  sleep 10
  k6 run --quiet --summary-export "${output}" \
    -e AGGREGATOR_BASE_URL="${AGGREGATOR_BASE_URL:-http://127.0.0.1:8083}" \
    -e AGGREGATOR_API_KEY="${AGGREGATOR_API_KEY}" \
    "${REPO_ROOT}/scripts/m2/p2/throughput.js"
}

run_case 1 "${one_result}"
run_case 3 "${three_result}"
one_rate="$(jq -er '.metrics.successful_requests | (.values.rate // .rate)' "${one_result}")"
three_rate="$(jq -er '.metrics.successful_requests | (.values.rate // .rate)' "${three_result}")"
ratio="$(awk -v one="${one_rate}" -v three="${three_rate}" 'BEGIN { if (one <= 0) exit 1; printf "%.3f", three / one }')"
printf 'one_instance_req_s=%s\nthree_instance_req_s=%s\nthroughput_ratio=%s\n' "${one_rate}" "${three_rate}" "${ratio}"
awk -v ratio="${ratio}" 'BEGIN { exit !(ratio >= 2.0) }' || {
  echo "error: three-instance throughput is below 2.0x" >&2
  exit 1
}
