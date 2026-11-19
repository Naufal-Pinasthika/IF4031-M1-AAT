#!/usr/bin/env bash
set -euo pipefail
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)/lib/demo-common.sh"
require_command curl

base_url="${AGGREGATOR_BASE_URL:-http://127.0.0.1:8083}"
heading "P5.2 observing fast status and circuit recovery"
for _ in $(seq 1 30); do
  curl_timed -H "X-Aggregator-Key: ${AGGREGATOR_API_KEY:?AGGREGATOR_API_KEY is required}" \
    "${base_url}/internal/source-status"
  curl --silent --show-error "${base_url}/metrics" | grep -E 'coastal_circuit|coastal_invalid|coastal_critical'
  sleep 1
done
