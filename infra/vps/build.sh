#!/usr/bin/env bash
set -euo pipefail
umask 077
cd /opt/aat/repo
tag="$1"
build_options=(--build-arg "BUILD_VERSION=${tag}")
if [[ "${2:-}" == --no-cache ]]; then
  build_options+=(--no-cache --pull)
elif [[ -n "${2:-}" ]]; then
  echo 'error: expected --no-cache as the optional second argument' >&2
  exit 2
fi
mkdir -p .vps /opt/aat/images
images=()
for service in aggregator auth bmkg client-api dashboard-consumer notifier-consumer pvmbg observability; do
  echo "Building $service on VPS A"
  if ! DOCKER_BUILDKIT=1 docker build "${build_options[@]}" --tag "aat/$service:$tag" "services/$service" > ".vps/build-$service.log" 2>&1; then
    tail -60 ".vps/build-$service.log"
    exit 1
  fi
  images+=("aat/$service:$tag")
done
docker save -o "/opt/aat/images/$tag.tar" "${images[@]}"
k3s ctr images import "/opt/aat/images/$tag.tar"
printf '%s\n' "$tag" > .vps/image-tag
