#!/usr/bin/env bash
set -euo pipefail
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)/lib/demo-common.sh"
require_command curl

base_url="${SENSOR_BASE_URL:-http://127.0.0.1:8090}"
: "${PANTAI_AUTH_TOKEN:?PANTAI_AUTH_TOKEN is required}"
heading "P5.1 Sensor Pantai health and contract"
curl_timed "${base_url}/healthz"
curl_timed -H "Authorization: Bearer ${PANTAI_AUTH_TOKEN}" "${base_url}/v1/stations"
curl_timed -H "Authorization: Bearer ${PANTAI_AUTH_TOKEN}" "${base_url}/v1/observations?limit=10"
