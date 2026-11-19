#!/usr/bin/env bash
set -euo pipefail
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)/lib/demo-common.sh"
require_command curl

base_url="${AGGREGATOR_BASE_URL:-http://127.0.0.1:8083}"
: "${P1_AUDIT_TOKEN:?P1_AUDIT_TOKEN is required}"
heading "P5.3 invalid Sensor Pantai payloads are quarantined"
curl_timed -H "Authorization: Bearer ${P1_AUDIT_TOKEN}" \
  "${base_url}/internal/audit/unprocessed?source=SENSOR_PANTAI&limit=100"
