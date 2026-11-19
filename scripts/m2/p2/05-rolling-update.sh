#!/usr/bin/env bash
set -euo pipefail
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)/lib/demo-common.sh"
require_command kubectl

image="${1:-}"
[[ -n "${image}" ]] || { echo "Usage: $0 <aggregator-image:tag>" >&2; exit 2; }
heading "P2.5 rolling update to ${image}"
kubectl -n "${NAMESPACE}" set image deployment/aggregator aggregator="${image}"
kubectl -n "${NAMESPACE}" rollout status deployment/aggregator --timeout=180s
kubectl -n "${NAMESPACE}" get pods -l app=aggregator -o wide
