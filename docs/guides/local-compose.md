# Local Compose development

Run all commands from the repository root in Bash/WSL. This is the local
Compose workflow, not the three-node VPS deployment or a capacity benchmark.
For VPS operation, use the [VPS runbook](../../infra/vps/README.md).

## Quick Start

### Requirements

- Docker Engine with Docker Compose v2, or Docker Desktop through WSL
- Go 1.25 for optional local checks
- OpenSSL for generating local example secrets
- `curl` for API calls
- `jq` for the demonstration commands
- Optional: `hey` for the Problem 2 load test

All shell commands below are written for Bash and can be run from WSL.

### 1. Create the local environment

```bash
bash ./scripts/setup-env.sh
```

The script copies `.env.example` to `.env`, replaces every `replace-with-*`
placeholder with a unique random value, and generates the RSA private key used
to sign access tokens. It refuses to overwrite an existing `.env`; use
`bash ./scripts/setup-env.sh --force` only when the current local configuration
may be replaced.

> [!IMPORTANT]
> `.env.example` is documentation, not a ready-to-use credential file. Keep
> `.env` local and never commit it. BMKG, PVMBG, Aggregator, database, and all
> downstream-client secrets must remain different.

### 2. Start the complete system

For a repeatable evaluation from an empty state, remove data left by an older
run before starting:

```bash
docker compose down -v --remove-orphans
```

> [!CAUTION]
> This reset permanently removes this Compose project's database, Kafka, and
> consumer volumes. Skip it when existing local data must be preserved. If
> database passwords were changed after volumes were created, a fresh reset is
> required because PostgreSQL does not reapply initialization credentials to an
> existing data directory.

Start all containers with one orchestration command:

```bash
docker compose config --quiet
docker compose up -d --build
docker compose ps
docker compose ps -a kafka-init
```

Wait until all application containers report `healthy`. `kafka-init` must
report `Exited (0)` because it creates the event topic before the producer and
consumers start. Follow structured logs with:

```bash
docker compose logs -f \
  bmkg pvmbg aggregator auth client-api \
  dashboard-consumer notifier-consumer
```

### 3. Check service health

```bash
curl -fsS http://localhost:8080/health
curl -fsS http://localhost:8081/health
curl -fsS http://localhost:8082/health
curl -fsS http://localhost:8084/health
curl -fsS http://localhost:8085/health
curl -fsS http://localhost:8086/health

# Aggregator is intentionally exposed only inside the Compose network.
docker compose exec -T aggregator \
  wget -qO- http://localhost:8083/health
```

### 4. Load local credentials for the examples

The `sed` expression strips possible Windows carriage returns before Bash
loads the file:

```bash
set -a
source <(sed 's/\r$//' .env)
set +a
```

Issue a Tim Lapangan token and read canonical events:

```bash
FIELD_TOKEN_RESPONSE="$(
  curl -fsS -X POST \
    -u "${FIELD_CLIENT_ID}:${FIELD_CLIENT_SECRET}" \
    http://localhost:8084/v1/token
)"

FIELD_ACCESS_TOKEN="$(jq -r '.access_token' <<<"$FIELD_TOKEN_RESPONSE")"
FIELD_REFRESH_TOKEN="$(jq -r '.refresh_token' <<<"$FIELD_TOKEN_RESPONSE")"

curl -fsS \
  -H "Authorization: Bearer ${FIELD_ACCESS_TOKEN}" \
  -H "X-Correlation-ID: quick-start-field-read" \
  'http://localhost:8080/events?limit=5' | jq
```

> [!NOTE]
> Access tokens expire after 60 seconds by default. Issue a new pair or use the
> [refresh flow](../runbooks/m1-demo.md#problem-3-trust-domains-scopes-and-refresh) if a demonstration command returns HTTP 401.

## Observability

Every application service exposes `/health` and writes JSON logs. HTTP
correlation IDs use the `X-Correlation-ID` header; a service generates one when
the caller omits it. The same ID is propagated through Client API → Aggregator,
Aggregator → BMKG/PVMBG, PostgreSQL query logs, Kafka headers/envelopes, and
consumer processing logs.

Useful commands:

```bash
docker compose logs --since 5m aggregator \
  | grep -E 'outbound_http|postgres_query|hazard_event_published'

docker compose logs --since 5m auth client-api \
  | grep -E 'correlation_id|latency_ms'
```

## Development Checks

Run all module checks from the repository root:

```bash
test -z "$(gofmt -l $(find services -type f -name '*.go'))"

go test ./services/aggregator/... \
  ./services/auth/... \
  ./services/bmkg/... \
  ./services/client-api/... \
  ./services/dashboard-consumer/... \
  ./services/notifier-consumer/... \
  ./services/pvmbg/...

docker compose config --quiet
docker compose build
```

## Shutdown and Reset

Stop containers while preserving named volumes:

```bash
docker compose down
```

Remove containers and all project data:

```bash
docker compose down -v
```

> [!CAUTION]
> `docker compose down -v` permanently deletes canonical events, Auth refresh
> sessions, Kafka data and offsets, dashboard state, and notifier state.
