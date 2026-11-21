#!/usr/bin/env bash
set -euo pipefail
umask 077
cd /opt/aat/repo
tag="$1"
mkdir -p .vps /opt/aat/images
images=()
for service in aggregator auth bmkg client-api dashboard-consumer notifier-consumer pvmbg observability; do
  echo "Building $service on VPS A"
  if ! DOCKER_BUILDKIT=1 docker build --tag "aat/$service:$tag" "services/$service" > ".vps/build-$service.log" 2>&1; then
    tail -60 ".vps/build-$service.log"
    exit 1
  fi
  images+=("aat/$service:$tag")
done
docker save -o "/opt/aat/images/$tag.tar" "${images[@]}"
k3s ctr images import "/opt/aat/images/$tag.tar"
printf '%s\n' "$tag" > .vps/image-tag
