#!/usr/bin/env bash
set -euo pipefail
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)/lib/demo-common.sh"
require_command kubectl

hold_seconds="${HOLD_SECONDS:-40}"
for replicas in 3 5 2; do
  heading "P2.4 scaling Aggregator to ${replicas} replicas"
  kubectl -n "${NAMESPACE}" scale deployment/aggregator --replicas="${replicas}"
  kubectl -n "${NAMESPACE}" rollout status deployment/aggregator --timeout=120s
  kubectl -n "${NAMESPACE}" get pods -l app=aggregator -o wide
  sleep "${hold_seconds}"
done
