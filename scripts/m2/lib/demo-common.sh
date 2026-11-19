#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../../.." && pwd)"
NAMESPACE="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"

require_command() {
  command -v "$1" >/dev/null 2>&1 || { echo "error: $1 is required" >&2; exit 1; }
}

curl_timed() {
  curl --silent --show-error "$@" \
    --write-out $'\nhttp_code=%{http_code}\ntime_total=%{time_total}s\n'
}

heading() {
  printf '\n[%s] %s\n' "$(date -u +%FT%TZ)" "$*"
}
