#!/usr/bin/env bash
# No Kubernetes writes and no connection to application storage.
set -euo pipefail
directory="$1"
repeat="${2:-1}"
[[ "$directory" =~ ^/opt/aat/regression/p2p5-[0-9TZ]+$ ]] || exit 2
[[ "$repeat" =~ ^[0-9]+$ ]] && ((repeat>=1 && repeat<=50)) || exit 2
cd "$directory"
name="aat-design-regression-${directory##*/}"
if docker inspect "$name" >/dev/null 2>&1; then
  echo 'Unexpected existing regression container; refusing to replace it.' >&2
  exit 1
fi
created=false
cleanup() {
  if [[ "$created" == true ]]; then docker rm -f "$name" >/dev/null; fi
}
trap cleanup EXIT
docker run -d --name "$name" --label aat.purpose=design-regression \
  --memory=512m --cpus=1 --tmpfs /var/lib/postgresql/data \
  -p 127.0.0.1::5432 -e POSTGRES_PASSWORD=local-regression-only \
  -e POSTGRES_DB=regression postgres:16-alpine >/dev/null
created=true
for attempt in {1..60}; do
  if docker exec "$name" pg_isready -U postgres -d regression >/dev/null; then break; fi
  sleep 1
done
port="$(docker inspect --format '{{(index (index .NetworkSettings.Ports "5432/tcp") 0).HostPort}}' "$name")"
[[ "$port" =~ ^[0-9]+$ ]] || exit 2
mkdir -p /opt/aat/go-test-cache /opt/aat/go-test-mod
docker run --rm --network host --memory=3g --cpus=4 \
  -v "$directory:/work" -w /work \
  -v /opt/aat/go-test-cache:/root/.cache/go-build -v /opt/aat/go-test-mod:/go/pkg/mod \
  -e AAT_FOCUSED_REPEAT="$repeat" \
  -e P2_TEST_DATABASE_URL="postgres://postgres:local-regression-only@127.0.0.1:$port/regression?sslmode=disable" \
  golang:1.25-alpine sh -ec '
    apk add --no-cache build-base >/dev/null
    for service in aggregator client-api notifier-consumer; do
      echo "Testing $service (race detector and vet)"
      cd "/work/$service"
      go test -race -count=1 -timeout=180s ./...
      go vet ./...
      if [ "$service" = aggregator ]; then
        echo "Repeating gateway admission/drain regression $AAT_FOCUSED_REPEAT times"
        go test -race -count="$AAT_FOCUSED_REPEAT" -timeout=180s ./internal/gateway ./cmd/gateway
        echo "Repeating bounded health/outage/bulkhead regression $AAT_FOCUSED_REPEAT times"
        go test -race -count="$AAT_FOCUSED_REPEAT" -timeout=180s \
          -run "^(TestRawNetwork|TestDefinitiveIO|TestProbeCannot|TestProbeLoop|TestReadWork|TestPostgresNetworkOutage|TestPostgresMaintenanceBudget|TestPostgresRecoveryDoesNotRecopy)" ./internal/store
      fi
    done
  '
