#!/usr/bin/env bash
set -euo pipefail
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)/lib/demo-common.sh"
require_command kubectl
require_command curl

base_url="${AGGREGATOR_BASE_URL:-http://127.0.0.1:8083}"
heading "P2.1 topology, readiness, and leader gauges"
kubectl -n "${NAMESPACE}" get deployment/aggregator pod -l app=aggregator -o wide
kubectl -n "${NAMESPACE}" get pdb/aggregator
curl_timed "${base_url}/health"
curl_timed "${base_url}/ready"
curl_timed "${base_url}/metrics" | grep -E 'time_total|http_code|aggregator_source_leader'
