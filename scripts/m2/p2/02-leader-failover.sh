#!/usr/bin/env bash
set -euo pipefail
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)/lib/demo-common.sh"
require_command kubectl

source_name="${1:-BMKG}"
leader=""
for pod in $(kubectl -n "${NAMESPACE}" get pods -l app=aggregator -o name); do
  if kubectl -n "${NAMESPACE}" exec "${pod}" -- wget -qO- http://127.0.0.1:8083/metrics \
    | grep -F "aggregator_source_leader{source=\"${source_name}\"} 1" >/dev/null; then
    leader="${pod#pod/}"
    break
  fi
done
[[ -n "${leader}" ]] || { echo "error: no ${source_name} leader found" >&2; exit 1; }

heading "P2.2 deleting ${source_name} leader ${leader}"
started="$(date +%s)"
kubectl -n "${NAMESPACE}" delete pod "${leader}" --wait=false
while (( $(date +%s) - started <= 60 )); do
  for pod in $(kubectl -n "${NAMESPACE}" get pods -l app=aggregator -o name); do
    [[ "${pod#pod/}" == "${leader}" ]] && continue
    if kubectl -n "${NAMESPACE}" exec "${pod}" -- wget -qO- http://127.0.0.1:8083/metrics 2>/dev/null \
      | grep -F "aggregator_source_leader{source=\"${source_name}\"} 1" >/dev/null; then
      echo "new_leader=${pod#pod/} failover_seconds=$(( $(date +%s) - started ))"
      exit 0
    fi
  done
  sleep 1
done
echo "error: failover exceeded 60 seconds" >&2
exit 1
