#!/usr/bin/env bash
# Isolated regression database; never targets application PostgreSQL or PVCs.
set -euo pipefail
cd /opt/aat/repo/services/aggregator
name=aat-vps-regression-pg
if docker inspect "$name" >/dev/null 2>&1; then
  echo 'Regression container already exists; inspect it before running again.' >&2
  exit 1
fi
trap 'docker rm -f "$name" >/dev/null 2>&1 || true' EXIT
docker run -d --name "$name" --label aat.purpose=regression --tmpfs /var/lib/postgresql/data \
  -p 127.0.0.1:55439:5432 -e POSTGRES_PASSWORD=local-regression-only \
  -e POSTGRES_DB=regression postgres:16-alpine >/dev/null
for attempt in {1..60}; do
  if docker exec "$name" pg_isready -U postgres -d regression >/dev/null; then break; fi
  sleep 1
done
mkdir -p /opt/aat/go-test-cache /opt/aat/go-test-mod
docker run --rm --network host -v "$PWD:/src" -w /src \
  -v /opt/aat/go-test-cache:/root/.cache/go-build -v /opt/aat/go-test-mod:/go/pkg/mod \
  -e P2_TEST_DATABASE_URL=postgres://postgres:local-regression-only@127.0.0.1:55439/regression?sslmode=disable \
  golang:1.25-alpine go test "$@"
