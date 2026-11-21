#!/usr/bin/env bash
set -euo pipefail
REPO="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd)"
mkdir -p "${REPO}/.e2e"
tag="${IMAGE_TAG:-m2-e2e-$(date -u +%Y%m%d%H%M%S)}"
for service in aggregator auth bmkg client-api dashboard-consumer notifier-consumer pvmbg; do
  echo "Building ${service} with shared Go caches"
  if ! docker build --network=host --tag "aat/${service}:${tag}" "${REPO}/services/${service}" > "${REPO}/.e2e/build-${service}.log" 2>&1; then
    tail -60 "${REPO}/.e2e/build-${service}.log"
    exit 1
  fi
done
printf '%s\n' "${tag}" > "${REPO}/.e2e/image-tag"
echo "Built image tag ${tag}; deploy.sh will use this exact tag."
