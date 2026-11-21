#!/usr/bin/env bash
set -euo pipefail
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)/lib/demo-common.sh"
require_command kubectl

scenario="${1:-}"
if [[ -z "${scenario}" ]]; then
  echo "Usage: $0 <practice-scenario-name>" >&2
  kubectl exec -n tester-agent daemonset/tester-agent -- /app list
  exit 2
fi
heading "P2.3 Tester Agent scenario ${scenario}"
kubectl exec -n tester-agent daemonset/tester-agent -- /app run practice "${scenario}"
kubectl logs -n tester-agent daemonset/tester-agent --since=10m
