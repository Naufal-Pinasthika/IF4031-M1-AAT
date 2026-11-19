#!/usr/bin/env bash
set -euo pipefail
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)/lib/demo-common.sh"
require_command curl

base_url="${NOTIFIER_BASE_URL:-http://127.0.0.1:8086}"
heading "P5.4 critical delivery counts and p95 latency"
curl_timed "${base_url}/stats"
curl_timed "${base_url}/metrics"
