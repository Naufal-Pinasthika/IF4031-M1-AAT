# Hazard Coordination Platform

> Distributed hazard ingestion and coordination platform for IF4031

## VPS deployment (M2)

The three-node Biznet deployment, existing-server provisioning, encrypted node
network, one-command deployment, access tunnel and verification are documented
in [infra/vps/README.md](infra/vps/README.md). Builds run on VPS A, not the laptop.
The VPS deployment starts fresh; old local test data was explicitly discarded.

The source-to-notifier measurement boundary and private operations dashboard are
documented in [P5 observability](docs/P5-observability.md). HA has been deferred:
coordinator/Kafka/notifier use the pre-replication singleton topology.

<p align="center">
  A distributed hazard-ingestion and coordination platform connecting BMKG,
  PVMBG, BNPB clients, and independent event consumers.
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Go-1.25-00ADD8?logo=go&logoColor=white" alt="Go 1.25" />
  <img src="https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white" alt="PostgreSQL 16" />
  <img src="https://img.shields.io/badge/Apache%20Kafka-4.0-231F20?logo=apachekafka&logoColor=white" alt="Apache Kafka 4.0" />
  <img src="https://img.shields.io/badge/Docker%20Compose-Enabled-2496ED?logo=docker&logoColor=white" alt="Docker Compose" />
</p>

<p align="center">
  <a href="#quick-start">Quick Start</a>
  &middot;
  <a href="#service-and-api-reference">API Reference</a>
  &middot;
  <a href="#configuration">Configuration</a>
  &middot;
  <a href="#demonstration-runbook">Demonstration Runbook</a>
</p>

## Overview

Hazard Coordination Platform is a distributed system for ingesting,
normalizing, securing, storing, and distributing seismic and volcanic hazard
information from independently managed institutions.

## Authors

<div align="center">
  <table>
    <tr>
      <th>NIM</th>
      <th>Name</th>
      <th>GitHub</th>
    </tr>
    <tr align="center">
      <td>13523090</td>
      <td>Nayaka Ghana Subrata</td>
      <td>
        <a href="https://github.com/Nayekah">
          <img src="https://github.com/Nayekah.png" width="48" alt="Nayekah" /><br/>
          <sub><b>@Nayekah</b></sub>
        </a>
      </td>
    </tr>
    <tr align="center">
      <td>13524013</td>
      <td>Anindya Naufal Pinasthika</td>
      <td>
        <a href="https://github.com/Naufal-Pinasthika/">
          <img src="https://github.com/Naufal-Pinasthika.png" width="48" alt="Naufal-Pinasthika" /><br/>
          <sub><b>@Naufal-Pinasthika</b></sub>
        </a>
      </td>
    </tr>
    <tr align="center">
      <td>13524003</td>
      <td>Faiq Azzam Nafidz</td>
      <td>
        <a href="https://github.com/Shui7/">
          <img src="https://github.com/Shui7.png" width="48" alt="Shui7" /><br/>
          <sub><b>@Shui7</b></sub>
        </a>
      </td>
    </tr>
  </table>
</div>

## About the Application

BMKG and PVMBG are represented as independent mock institutions with different
credentials, response characteristics, and schemas. The Aggregator polls both
sources, maps their records into the canonical `HazardEvent` format, stores
them in PostgreSQL, and publishes every upsert through Kafka.

Downstream clients obtain short-lived RS256 access tokens from a dedicated Auth
Service. The Client API validates those tokens through JWKS and applies
field-level authorization. Media clients receive summary fields, while field
and internal-operations clients can access raw fields. Two independent Kafka
consumer groups update dashboard state and field notifications without direct
coupling to the Aggregator.

```text
BMKG :8081  ──REST polling──┐
                            ├──> Aggregator :8083 ──> Canonical PostgreSQL
PVMBG :8082 ─REST polling───┘             │
                                          └──Kafka──┬──> Dashboard Consumer
                                                    └──> Notifier Consumer

Client ──HTTP Basic──> Auth :8084 ──> Auth PostgreSQL
  │                         │
  │                         └──JWKS──> Client API :8080
  └────Bearer access token───────────> Client API ──REST──> Aggregator
```

The Canonical PostgreSQL network is private to the Aggregator. The Auth
PostgreSQL network is private to the Auth Service. Client-facing reads always
pass through the Client API and Aggregator APIs.

## Technology Stack

- Go 1.25 for all application services and mocks
- PostgreSQL 16 with JSONB for canonical hazard attributes
- A separate PostgreSQL 16 database for clients and refresh sessions
- Apache Kafka 4.0 in KRaft mode for durable fan-out distribution
- bbolt for consumer-owned dashboard and notification state
- RS256 JWT access tokens, opaque rotating refresh tokens, and JWKS
- Docker Compose for container orchestration and network isolation
- JSON structured logs with propagated correlation IDs and outbound latency

## Features

- Independent BMKG and PVMBG polling loops
- Additive-only JSON schema evolution for unknown source fields
- Static BNPB volcano reference mapping for canonical names and coordinates
- Source-specific credentials and downstream client identities
- Short-lived scoped access tokens with refresh-token rotation
- Server-side summary/raw field authorization
- Concurrency limits with controlled HTTP 429 rejection
- Cached canonical data with `UP`, `STALE`, `UNAVAILABLE`, and `UNKNOWN` source states
- PostgreSQL JSONB storage without migrations for new source attributes
- Kafka fan-out to two independent, idempotent consumers
- Consumer catch-up after temporary downtime
- Health endpoints, structured logs, correlation IDs, and outbound latency logs

## Repository Layout

```text
services/
  aggregator/           Polling, canonical mapping, storage ownership, Kafka producer
  auth/                 Client authentication, token issuance, refresh, and JWKS
  bmkg/                 Low-latency seismic and tsunami mock service
  client-api/           Downstream API and field-level authorization
  dashboard-consumer/   Kafka consumer and dashboard-owned bbolt state
  database/             Canonical PostgreSQL initialization
  notifier-consumer/    Kafka consumer and notifier-owned bbolt state
  pvmbg/                Delayed volcanic-report mock with schema/outage controls
scripts/
  setup-env.sh          Generate local secrets and the Auth RSA signing key
docker-compose.yml       Full-system orchestration
.env.example             Complete configuration template with placeholder secrets
go.work                  Go workspace for all seven modules
```

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
> refresh flow below if a demonstration command returns HTTP 401.

## Service and API Reference

### Services and ports

| Service | Address from host | Responsibility | Storage |
|---|---:|---|---|
| Client API | `localhost:8080` | Downstream reads and field authorization | None |
| BMKG | `localhost:8081` | Seismic events and tsunami warnings | In-memory mock data |
| PVMBG | `localhost:8082` | Volcanic reports and failure controls | In-memory mock data |
| Aggregator | `aggregator:8083` internal | Polling, mapping, canonical API, Kafka publish | Canonical PostgreSQL |
| Auth | `localhost:8084` | Client credentials, access/refresh tokens, JWKS | Auth PostgreSQL |
| Dashboard Consumer | `localhost:8085` | Materialized dashboard state | Dedicated bbolt volume |
| Notifier Consumer | `localhost:8086` | Idempotent field notifications | Dedicated bbolt volume |
| Kafka | `kafka:9092` internal | Retained hazard-event stream | Dedicated Kafka volume |

### HTTP endpoints

| Service | Method and path | Authentication | Purpose |
|---|---|---|---|
| BMKG | `GET /health` | None | Health check |
| BMKG | `GET /seismic-events?since=<RFC3339>` | `X-BMKG-Key` | Seismic records |
| BMKG | `GET /tsunami-warnings?since=<RFC3339>` | `X-BMKG-Key` | Tsunami warnings |
| PVMBG | `GET /health` | None | Health check |
| PVMBG | `GET /volcanic-reports?since=<RFC3339>` | Bearer PVMBG token | Volcanic reports |
| PVMBG | `POST /admin/schema-version` | Bearer PVMBG token | Toggle `confidence_level` |
| PVMBG | `POST /admin/outage` | Bearer PVMBG token | Toggle simulated outage |
| Aggregator | `GET /health` | None, internal network | Health check |
| Aggregator | `GET /internal/events` | `X-Aggregator-Key` | Canonical event collection |
| Aggregator | `GET /internal/events/{hazard_id}` | `X-Aggregator-Key` | One canonical event |
| Aggregator | `GET /internal/source-status` | `X-Aggregator-Key` | Source freshness |
| Auth | `POST /v1/token` | HTTP Basic client credentials | Issue token pair |
| Auth | `POST /v1/token/refresh` | HTTP Basic plus refresh token | Rotate token pair |
| Auth | `GET /.well-known/jwks.json` | None | RS256 public key |
| Auth | `GET /health` | None | Auth/database health |
| Client API | `GET /events` | Bearer access token | Filtered event collection |
| Client API | `GET /events/{hazard_id}` | Bearer access token | One canonical event |
| Client API | `GET /source-status` | Bearer access token | BMKG/PVMBG freshness |
| Client API | `GET /health` | None | Health check |
| Dashboard | `GET /health` | None | Consumer/Kafka health |
| Dashboard | `GET /events`, `GET /stats` | None, demo endpoint | Consumer state |
| Notifier | `GET /health` | None | Consumer/Kafka health |
| Notifier | `GET /notifications`, `GET /stats` | None, demo endpoint | Consumer state |

`GET /notifications` returns at most 500 entries by default (`limit=1..1000`).
Follow `next_cursor` using `?limit=1000&cursor=...` until it is absent to inspect
the full history. Pages follow the persistent domain-key order, not newest-first
order. This is a live traversal, not a cross-request snapshot: concurrent inserts
before a cursor are visible on the next fresh traversal. No historical data is
deleted. `/stats` still scans the full history and is not a bounded-cost endpoint.

`GET /events` accepts `source`, `hazard_type`, `severity`, `from`, `to`,
`limit`, `offset`, and `fields`. The `fields` parameter is authorized by the
Client API; requesting a raw field without `hazards:read:raw` returns HTTP 403.

## Configuration

Compose reads `.env` automatically. The table below mirrors `.env.example`.
Time values ending in `_MS` use milliseconds; TTL values use seconds.

### Source mocks and Aggregator

| Variable | Example/default | Purpose |
|---|---|---|
| `BMKG_API_KEY` | secret, required | Credential accepted only by BMKG |
| `PVMBG_TOKEN` | secret, required | Bearer credential accepted only by PVMBG |
| `BMKG_EVENT_INTERVAL_MS` | `10000` | BMKG new-event interval |
| `PVMBG_EVENT_INTERVAL_MS` | `10000` | PVMBG new-report interval |
| `PVMBG_DELAY_MIN_MS` | `500` | Minimum PVMBG response delay |
| `PVMBG_DELAY_MAX_MS` | `3000` | Maximum PVMBG response delay |
| `BMKG_URL` | `http://bmkg:8081` | Aggregator-to-BMKG URL |
| `PVMBG_URL` | `http://pvmbg:8082` | Aggregator-to-PVMBG URL |
| `AGGREGATOR_API_KEY` | secret, required | Client API-to-Aggregator credential |
| `AGGREGATOR_LISTEN_ADDRESS` | `:8083` | Aggregator listener |
| `AGGREGATOR_URL` | `http://aggregator:8083` | Client API-to-Aggregator URL |
| `AGGREGATOR_POLL_INTERVAL_MS` | `5000` | Independent source polling interval |
| `AGGREGATOR_HTTP_TIMEOUT_MS` | `4000` | Source-call timeout |
| `AGGREGATOR_MAX_IN_FLIGHT` | `64` | Concurrent Aggregator API limit |
| `AGGREGATOR_START_SINCE` | `1970-01-01T00:00:00Z` | Initial polling cursor |
| `CLIENT_API_MAX_IN_FLIGHT` | `64` | Concurrent authorized Client API limit |

### Canonical and Auth databases

| Variable | Example/default | Purpose |
|---|---|---|
| `POSTGRES_ADMIN_PASSWORD` | secret, required | Canonical PostgreSQL bootstrap password |
| `POSTGRES_APP_USER` | `aggregator_app` | Least-privilege Aggregator database user |
| `POSTGRES_APP_PASSWORD` | secret, required | Aggregator database password |
| `DATABASE_URL` | PostgreSQL URL | Aggregator-owned Canonical Store connection |
| `AUTH_POSTGRES_PASSWORD` | secret, required | Auth PostgreSQL password |
| `AUTH_DATABASE_URL` | PostgreSQL URL | Auth-owned database connection |

`POSTGRES_APP_PASSWORD` must match the password embedded in `DATABASE_URL`.
`AUTH_POSTGRES_PASSWORD` must match the password embedded in
`AUTH_DATABASE_URL`.

### Auth and downstream clients

| Variable | Example/default | Purpose |
|---|---|---|
| `AUTH_LISTEN_ADDRESS` | `:8084` | Auth listener |
| `AUTH_JWKS_URL` | `http://auth:8084/.well-known/jwks.json` | Client API JWKS URL |
| `AUTH_ACCESS_TOKEN_TTL_SECONDS` | `60` | Short-lived access-token TTL |
| `AUTH_REFRESH_TOKEN_TTL_SECONDS` | `86400` | Refresh-token TTL |
| `AUTH_MAX_IN_FLIGHT` | `32` | Concurrent Auth request limit |
| `AUTH_JWT_ISSUER` | `bnpb-auth-service` | Required JWT issuer |
| `AUTH_JWT_AUDIENCE` | `bnpb-client-api` | Required JWT audience |
| `AUTH_JWT_KEY_ID` | `bnpb-auth-2026-01` | JWT/JWKS key identifier |
| `AUTH_JWT_PRIVATE_KEY_B64` | base64 PKCS#8 PEM | RS256 private signing key |
| `MEDIA_CLIENT_ID` | `media-client` | Media identity |
| `MEDIA_CLIENT_SECRET` | unique secret | Media client credential |
| `FIELD_CLIENT_ID` | `field-client` | Tim Lapangan identity |
| `FIELD_CLIENT_SECRET` | unique secret | Tim Lapangan client credential |
| `OPS_CLIENT_ID` | `ops-client` | BNPB internal-operations identity |
| `OPS_CLIENT_SECRET` | unique secret | Internal-operations credential |

### Kafka and consumers

| Variable | Example/default | Purpose |
|---|---|---|
| `KAFKA_BROKERS` | `kafka:9092` | Broker addresses |
| `KAFKA_TOPIC` | `hazard.events.v1` | Hazard-event topic |
| `DASHBOARD_CONSUMER_GROUP` | `dashboard-updater-v1` | Dashboard consumer group |
| `DASHBOARD_LISTEN_ADDRESS` | `:8085` | Dashboard demo API listener |
| `DASHBOARD_STORE_PATH` | `/data/dashboard.db` | Dashboard bbolt file |
| `NOTIFIER_CONSUMER_GROUP` | `field-notifier-v1` | Notifier consumer group |
| `NOTIFIER_LISTEN_ADDRESS` | `:8086` | Notifier demo API listener |
| `NOTIFIER_STORE_PATH` | `/data/notifier.db` | Notifier bbolt file |

## Authentication and Authorization

The three downstream identities are seeded independently:

| Client | Scopes | Default response fields |
|---|---|---|
| Media | `hazards:read:summary` | Summary fields only |
| Tim Lapangan | `hazards:read:summary hazards:read:raw` | All fields |
| BNPB Internal Ops | `hazards:read:summary hazards:read:raw` | All fields |

Summary fields are `hazard_id`, `source`, `hazard_type`, `severity`,
`area_name`, `occurred_at`, and `ingested_at`. Raw fields are `source_ref_id`,
`latitude`, `longitude`, and `attributes`.

Access tokens are RS256 JWTs validated against a configured issuer, audience,
expiry, type, key ID, and JWKS key. Refresh tokens are opaque, stored only as
hashes, rotated on use, and protected against reuse.

## Demonstration Runbook

Run the Quick Start first, load `.env` into the shell, and issue a fresh Tim
Lapangan token before running these scenarios.

### Problem 1: Additive schema evolution

> [!NOTE]
> For deterministic before-and-after evidence, begin this scenario immediately
> after the empty-state reset in Quick Start. This prevents records from an
> earlier schema-v2 demonstration from appearing in the initial sample.

Record the Aggregator start time, then reset only PVMBG so the source starts
with schema v1. Wait for at least one polling cycle before reading canonical
events:

```bash
AGGREGATOR_STARTED_BEFORE="$(
  docker inspect "$(docker compose ps -q aggregator)" \
    --format '{{.State.StartedAt}}'
)"

docker compose restart pvmbg
until curl -fsS http://localhost:8082/health >/dev/null; do sleep 1; done
sleep 7

curl -fsS \
  -H "Authorization: Bearer ${PVMBG_TOKEN}" \
  'http://localhost:8082/volcanic-reports?since=1970-01-01T00:00:00Z' \
  | jq '.[0] | {report_id, volcano_id, confidence_level_present: has("confidence_level")}'
```

Issue a fresh Tim Lapangan token:

```bash
FIELD_TOKEN_RESPONSE="$(
  curl -fsS -X POST \
    -u "${FIELD_CLIENT_ID}:${FIELD_CLIENT_SECRET}" \
    http://localhost:8084/v1/token
)"
FIELD_ACCESS_TOKEN="$(jq -r '.access_token' <<<"$FIELD_TOKEN_RESPONSE")"
```

Show that both source schemas have already been mapped to the canonical
`HazardEvent` contract:

```bash
curl -fsS \
  -H "Authorization: Bearer ${FIELD_ACCESS_TOKEN}" \
  'http://localhost:8080/events?source=BMKG&limit=1' \
  | jq '.events[0] | {
      hazard_id, source, source_ref_id, hazard_type, severity,
      area_name, latitude, longitude, occurred_at, attributes
    }'

curl -fsS \
  -H "Authorization: Bearer ${FIELD_ACCESS_TOKEN}" \
  'http://localhost:8080/events?source=PVMBG&limit=1' \
  | jq '.events[0] | {
      hazard_id, source, source_ref_id, hazard_type, severity,
      area_name, latitude, longitude, occurred_at, attributes
    }'
```

The BMKG event must use `SEISMIC`, the BMKG region and coordinates, and the
required seismic attributes. The PVMBG event must use `VOLCANIC`, the mapped
volcano name and coordinates, and the required volcanic attributes.

Trigger the additive schema change without restarting any service:

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer ${PVMBG_TOKEN}" \
  -H 'X-Correlation-ID: demo-problem-1-schema-change' \
  http://localhost:8082/admin/schema-version | jq

sleep 20

curl -fsS \
  -H "Authorization: Bearer ${FIELD_ACCESS_TOKEN}" \
  -H 'X-Correlation-ID: demo-problem-1-read' \
  'http://localhost:8080/events?source=PVMBG&limit=100' \
  | jq '[.events[] | select(.attributes.confidence_level != null)][0]'

AGGREGATOR_STARTED_AFTER="$(
  docker inspect "$(docker compose ps -q aggregator)" \
    --format '{{.State.StartedAt}}'
)"

[ "${AGGREGATOR_STARTED_BEFORE}" = "${AGGREGATOR_STARTED_AFTER}" ] \
  && echo PASS || echo FAIL
```

The resulting event must retain its canonical name and coordinates while
placing `confidence_level` in `attributes`. The final comparison must print
`PASS`, proving that the Aggregator handled the additive change without a
restart or redeploy. The declared policy is additive-only: unknown fields are
preserved in `attributes`. Removal of required fields, incompatible type
changes, and semantic changes to existing fields are not supported.

> [!WARNING]
> `/admin/schema-version` is a toggle. Calling it a second time disables schema
> v2. Read `schema_v2_enabled` from the response before continuing.

### Problem 2: Concurrency and graceful degradation

Set PVMBG to a fixed three-second delay. A longer token TTL prevents access
token expiry from contaminating a 60-second load-test result:

```bash
sed -i 's/^PVMBG_DELAY_MIN_MS=.*/PVMBG_DELAY_MIN_MS=3000/' .env
sed -i 's/^PVMBG_DELAY_MAX_MS=.*/PVMBG_DELAY_MAX_MS=3000/' .env
sed -i 's/^AUTH_ACCESS_TOKEN_TTL_SECONDS=.*/AUTH_ACCESS_TOKEN_TTL_SECONDS=120/' .env

set -a
source <(sed 's/\r$//' .env)
set +a

docker compose up -d --force-recreate pvmbg auth

until curl -fsS http://localhost:8082/health >/dev/null; do sleep 1; done
until curl -fsS http://localhost:8084/health >/dev/null; do sleep 1; done
```

Issue a fresh Tim Lapangan token, then use any sustained-load tool. The example
below assumes `hey` is installed:

```bash
FIELD_TOKEN_RESPONSE="$(
  curl -fsS -X POST \
    -u "${FIELD_CLIENT_ID}:${FIELD_CLIENT_SECRET}" \
    http://localhost:8084/v1/token
)"
FIELD_ACCESS_TOKEN="$(jq -r '.access_token' <<<"$FIELD_TOKEN_RESPONSE")"
```

Run BMKG-only and PVMBG-only traffic concurrently. The two processes create a
total of 50 sustained connections for 60 seconds:

```bash
hey -z 60s -c 25 \
  -H "Authorization: Bearer ${FIELD_ACCESS_TOKEN}" \
  'http://localhost:8080/events?source=BMKG&limit=20' > /tmp/bmkg-load.txt &
BMKG_LOAD_PID=$!

hey -z 60s -c 25 \
  -H "Authorization: Bearer ${FIELD_ACCESS_TOKEN}" \
  'http://localhost:8080/events?source=PVMBG&limit=20' > /tmp/pvmbg-load.txt &
PVMBG_LOAD_PID=$!

wait "$BMKG_LOAD_PID" "$PVMBG_LOAD_PID"
```

Print throughput, p50/p95/p99, and the HTTP status distribution from both
results:

```bash
for result in /tmp/bmkg-load.txt /tmp/pvmbg-load.txt; do
  echo "=== ${result} ==="
  grep -E 'Requests/sec:|50%+ in|95%+ in|99%+ in|^[[:space:]]+\[[0-9]{3}\]' "${result}"
done

BMKG_P95_SECONDS="$(
  awk '$1 ~ /^95%+$/ && $2 == "in" {print $3; exit}' /tmp/bmkg-load.txt
)"

awk -v p95="${BMKG_P95_SECONDS}" 'BEGIN {
  printf "BMKG p95: %.6f seconds, %s\n", p95, (p95 < 0.300 ? "PASS" : "FAIL")
  exit !(p95 < 0.300)
}'
```

The last command must print `PASS`. Calculate the combined uncontrolled error
rate across both result files. HTTP 429 is counted separately as a controlled
rejection; every other non-200 response and transport error is uncontrolled:

```bash
read -r TOTAL_ATTEMPTS CONTROLLED_429 UNCONTROLLED_ERRORS < <(
  awk '
    /^Status code distribution:/ {section = "status"; next}
    /^Error distribution:/       {section = "error"; next}

    section == "status" && $1 ~ /^\[[0-9][0-9][0-9]\]$/ {
      code = $1
      gsub(/[\[\]]/, "", code)
      total += $2
      if (code == 429) controlled += $2
      else if (code != 200) uncontrolled += $2
    }

    section == "error" && $1 ~ /^\[[0-9]+\]$/ {
      count = $1
      gsub(/[\[\]]/, "", count)
      total += count
      uncontrolled += count
    }

    END {print total + 0, controlled + 0, uncontrolled + 0}
  ' /tmp/bmkg-load.txt /tmp/pvmbg-load.txt
)

awk \
  -v total="${TOTAL_ATTEMPTS}" \
  -v rejected="${CONTROLLED_429}" \
  -v error_count="${UNCONTROLLED_ERRORS}" \
  'BEGIN {
    rate = 100
    if (total > 0) rate = error_count * 100 / total
    printf "attempts=%d controlled_429=%d uncontrolled_errors=%d error_rate=%.4f%% %s\n",
      total, rejected, error_count, rate, (rate < 1 ? "PASS" : "FAIL")
    exit !(rate < 1)
  }'
```

The combined uncontrolled error rate must be below one percent. Issue another
fresh token before testing outage recovery:

```bash
FIELD_TOKEN_RESPONSE="$(
  curl -fsS -X POST \
    -u "${FIELD_CLIENT_ID}:${FIELD_CLIENT_SECRET}" \
    http://localhost:8084/v1/token
)"
FIELD_ACCESS_TOKEN="$(jq -r '.access_token' <<<"$FIELD_TOKEN_RESPONSE")"
```

Trigger PVMBG outage, wait for the next poll, and compare source states:

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer ${PVMBG_TOKEN}" \
  http://localhost:8082/admin/outage | jq

sleep 7

curl -fsS \
  -H "Authorization: Bearer ${FIELD_ACCESS_TOKEN}" \
  'http://localhost:8080/events?source=BMKG&limit=3' | jq '.sources'

curl -fsS \
  -H "Authorization: Bearer ${FIELD_ACCESS_TOKEN}" \
  'http://localhost:8080/events?source=PVMBG&limit=3' \
  | jq '{sources, event_count: (.events | length)}'
```

BMKG must remain `UP`; PVMBG must return cached events with `STALE` and
`stale_since`. Toggle outage off and wait for automatic recovery:

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer ${PVMBG_TOKEN}" \
  http://localhost:8082/admin/outage | jq

sleep 7

curl -fsS \
  -H "Authorization: Bearer ${FIELD_ACCESS_TOKEN}" \
  http://localhost:8080/source-status | jq
```

> [!WARNING]
> `/admin/outage` is also a toggle. Ensure the first response says
> `outage_enabled: true` and the recovery response says `false`.

Restore the normal delay range and the default access-token TTL after load
testing:

```bash
sed -i 's/^PVMBG_DELAY_MIN_MS=.*/PVMBG_DELAY_MIN_MS=500/' .env
sed -i 's/^PVMBG_DELAY_MAX_MS=.*/PVMBG_DELAY_MAX_MS=3000/' .env
sed -i 's/^AUTH_ACCESS_TOKEN_TTL_SECONDS=.*/AUTH_ACCESS_TOKEN_TTL_SECONDS=60/' .env

set -a
source <(sed 's/\r$//' .env)
set +a

docker compose up -d --force-recreate pvmbg auth
until curl -fsS http://localhost:8082/health >/dev/null; do sleep 1; done
until curl -fsS http://localhost:8084/health >/dev/null; do sleep 1; done
```

### Problem 3: Trust domains, scopes, and refresh

Start with positive controls to prove that each institutional credential works
in its own trust domain:

```bash
curl -sS -o /dev/null -w 'BMKG key at BMKG: HTTP %{http_code}\n' \
  -H "X-BMKG-Key: ${BMKG_API_KEY}" \
  'http://localhost:8081/seismic-events?since=1970-01-01T00:00:00Z'

curl -sS -o /dev/null -w 'PVMBG token at PVMBG: HTTP %{http_code}\n' \
  -H "Authorization: Bearer ${PVMBG_TOKEN}" \
  'http://localhost:8082/volcanic-reports?since=1970-01-01T00:00:00Z'
```

Both controls must return HTTP 200. Then exchange the credentials between the
institutions:

```bash
curl -sS -o /dev/null -w 'BMKG key at PVMBG: HTTP %{http_code}\n' \
  -H "Authorization: Bearer ${BMKG_API_KEY}" \
  'http://localhost:8082/volcanic-reports?since=1970-01-01T00:00:00Z'

curl -sS -o /dev/null -w 'PVMBG token at BMKG: HTTP %{http_code}\n' \
  -H "X-BMKG-Key: ${PVMBG_TOKEN}" \
  'http://localhost:8081/seismic-events?since=1970-01-01T00:00:00Z'
```

Both cross-domain requests must return HTTP 401 or 403. Next, issue a Media
token and inspect the server-assigned scope:

```bash
MEDIA_TOKEN_RESPONSE="$(
  curl -fsS -X POST \
    -u "${MEDIA_CLIENT_ID}:${MEDIA_CLIENT_SECRET}" \
    http://localhost:8084/v1/token
)"
MEDIA_ACCESS_TOKEN="$(jq -r '.access_token' <<<"$MEDIA_TOKEN_RESPONSE")"

jq '{scope, expires_in}' <<<"$MEDIA_TOKEN_RESPONSE"
```

The scope must be `hazards:read:summary`. Verify that a normal request contains
only summary fields and that an explicit raw-field request is rejected by the
server:

```bash
curl -fsS \
  -H "Authorization: Bearer ${MEDIA_ACCESS_TOKEN}" \
  'http://localhost:8080/events?limit=1' \
  | jq '.events[0]'

curl -sS -o /dev/null -w 'Media raw-field request: HTTP %{http_code}\n' \
  -H "Authorization: Bearer ${MEDIA_ACCESS_TOKEN}" \
  'http://localhost:8080/events?limit=1&fields=hazard_id,latitude'
```

The first response contains only summary fields; the explicit raw-field request
must return HTTP 403.

For natural expiry and refresh, first restore the default TTL and recreate Auth:

```bash
sed -i 's/^AUTH_ACCESS_TOKEN_TTL_SECONDS=.*/AUTH_ACCESS_TOKEN_TTL_SECONDS=60/' .env

set -a
source <(sed 's/\r$//' .env)
set +a

docker compose up -d --force-recreate auth
until curl -fsS http://localhost:8084/health >/dev/null; do sleep 1; done

FIELD_TOKEN_RESPONSE="$(
  curl -fsS -X POST \
    -u "${FIELD_CLIENT_ID}:${FIELD_CLIENT_SECRET}" \
    http://localhost:8084/v1/token
)"
OLD_ACCESS_TOKEN="$(jq -r '.access_token' <<<"$FIELD_TOKEN_RESPONSE")"
FIELD_REFRESH_TOKEN="$(jq -r '.refresh_token' <<<"$FIELD_TOKEN_RESPONSE")"

sleep 65

curl -sS -o /dev/null -w 'Expired access token: HTTP %{http_code}\n' \
  -H "Authorization: Bearer ${OLD_ACCESS_TOKEN}" \
  'http://localhost:8080/events?limit=1'

REFRESH_RESPONSE="$(
  jq -n --arg refresh_token "$FIELD_REFRESH_TOKEN" \
    '{refresh_token: $refresh_token}' \
  | curl -fsS -X POST \
      -u "${FIELD_CLIENT_ID}:${FIELD_CLIENT_SECRET}" \
      -H 'Content-Type: application/json' \
      --data-binary @- \
      http://localhost:8084/v1/token/refresh
)"

NEW_ACCESS_TOKEN="$(jq -r '.access_token' <<<"$REFRESH_RESPONSE")"
NEW_REFRESH_TOKEN="$(jq -r '.refresh_token' <<<"$REFRESH_RESPONSE")"

curl -sS -o /dev/null -w 'Refreshed access token: HTTP %{http_code}\n' \
  -H "Authorization: Bearer ${NEW_ACCESS_TOKEN}" \
  'http://localhost:8080/events?limit=1'

curl -sS -o /dev/null -w 'Old access token after refresh: HTTP %{http_code}\n' \
  -H "Authorization: Bearer ${OLD_ACCESS_TOKEN}" \
  'http://localhost:8080/events?limit=1'
```

The expired token must return 401 and the refreshed token must return 200. The
refresh request still authenticates the machine client, but it does not require
an end user to log in again. `NEW_REFRESH_TOKEN` replaces the old refresh token
because refresh sessions are rotated on every use.

Finally, verify the repository secret policy without printing any credential:

```bash
git check-ignore -q .env && echo PASS || echo FAIL
test -f .env.example && git ls-files --error-unmatch .env.example >/dev/null \
  && echo PASS || echo FAIL
test -z "$(git log main --format='%H' -- .env)" \
  && echo PASS || echo FAIL
```

The three commands must print `PASS`: `.env` is ignored, `.env.example` is
tracked, and `.env` does not appear in the reachable history of `main`. The
commands check repository state without displaying local secret values.

### Problem 4: Independent deployment and flexible storage

Issue a fresh Tim Lapangan token:

```bash
FIELD_TOKEN_RESPONSE="$(
  curl -fsS -X POST \
    -u "${FIELD_CLIENT_ID}:${FIELD_CLIENT_SECRET}" \
    http://localhost:8084/v1/token
)"
FIELD_ACCESS_TOKEN="$(jq -r '.access_token' <<<"$FIELD_TOKEN_RESPONSE")"
```

Record the start time of every long-running container except the Notifier:

```bash
P4_UNRELATED_SERVICES=(
  postgres auth-postgres bmkg pvmbg kafka aggregator auth
  client-api dashboard-consumer
)

record_started_at() {
  for service in "${P4_UNRELATED_SERVICES[@]}"; do
    printf '%s ' "${service}"
    docker inspect "$(docker compose ps -q "${service}")" \
      --format '{{.State.StartedAt}}'
  done
}

record_started_at > /tmp/p4-started-before.txt
```

Stop and rebuild only the Notifier Consumer while an unrelated Client API read
continues to succeed:

```bash
docker compose stop notifier-consumer

curl -fsS \
  -H "Authorization: Bearer ${FIELD_ACCESS_TOKEN}" \
  'http://localhost:8080/events?source=BMKG&limit=1' | jq

docker compose build notifier-consumer
docker compose up -d notifier-consumer
until curl -fsS http://localhost:8086/health >/dev/null; do sleep 1; done

record_started_at > /tmp/p4-started-after.txt
diff -u /tmp/p4-started-before.txt /tmp/p4-started-after.txt \
  && echo PASS || echo FAIL
```

The final command must print `PASS`, proving that the unrelated containers were
not restarted. Issue a fresh token in case the rebuild consumed the previous
token's 60-second TTL, then inspect and assert storage-network ownership:

```bash
FIELD_TOKEN_RESPONSE="$(
  curl -fsS -X POST \
    -u "${FIELD_CLIENT_ID}:${FIELD_CLIENT_SECRET}" \
    http://localhost:8084/v1/token
)"
FIELD_ACCESS_TOKEN="$(jq -r '.access_token' <<<"$FIELD_TOKEN_RESPONSE")"

DATABASE_NETWORK="$(
  docker inspect "$(docker compose ps -q postgres)" \
    --format '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{"\n"}}{{end}}' \
    | grep '_database$'
)"

AGGREGATOR_NETWORKS="$(
  docker inspect "$(docker compose ps -q aggregator)" \
    --format '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{"\n"}}{{end}}'
)"

CLIENT_API_NETWORKS="$(
  docker inspect "$(docker compose ps -q client-api)" \
    --format '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{"\n"}}{{end}}'
)"

grep -qx "${DATABASE_NETWORK}" <<<"${AGGREGATOR_NETWORKS}" \
  && echo 'PASS: Aggregator can access the Canonical Store' \
  || echo 'FAIL: Aggregator is outside the database network'

if grep -qx "${DATABASE_NETWORK}" <<<"${CLIENT_API_NETWORKS}"; then
  echo 'FAIL: Client API can access the database network'
else
  echo 'PASS: Client API is isolated from the database network'
fi

curl -fsS \
  -H "Authorization: Bearer ${FIELD_ACCESS_TOKEN}" \
  'http://localhost:8080/events?limit=1' | jq '.events[0]'
```

PostgreSQL and Aggregator share the private `database` network; Client API does
not. The successful Client API request shows that the read passes through the
Aggregator API rather than through a shared database connection. To show
flexible JSONB storage independently of earlier scenarios, restart PVMBG in
schema v1 and wait for a new v1 record to be stored:

```bash
docker compose restart pvmbg
until curl -fsS http://localhost:8082/health >/dev/null; do sleep 1; done
sleep 20

FIELD_TOKEN_RESPONSE="$(
  curl -fsS -X POST \
    -u "${FIELD_CLIENT_ID}:${FIELD_CLIENT_SECRET}" \
    http://localhost:8084/v1/token
)"
FIELD_ACCESS_TOKEN="$(jq -r '.access_token' <<<"$FIELD_TOKEN_RESPONSE")"

PVMBG_V1_RESPONSE="$(
  curl -fsS \
    -H "Authorization: Bearer ${FIELD_ACCESS_TOKEN}" \
    'http://localhost:8080/events?source=PVMBG&limit=100'
)"

jq -e \
  '[.events[] | select(.attributes.confidence_level == null)] | length > 0' \
  >/dev/null <<<"${PVMBG_V1_RESPONSE}" \
  && echo PASS || echo FAIL
```

The check must print `PASS`. Enable schema v2 without restarting the
Aggregator, wait for a new v2 record, and issue a fresh token before reading
both versions:

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer ${PVMBG_TOKEN}" \
  http://localhost:8082/admin/schema-version \
  | jq -e '.schema_v2_enabled == true'

sleep 20

FIELD_TOKEN_RESPONSE="$(
  curl -fsS -X POST \
    -u "${FIELD_CLIENT_ID}:${FIELD_CLIENT_SECRET}" \
    http://localhost:8084/v1/token
)"
FIELD_ACCESS_TOKEN="$(jq -r '.access_token' <<<"$FIELD_TOKEN_RESPONSE")"

PVMBG_SCHEMA_RESPONSE="$(
  curl -fsS \
    -H "Authorization: Bearer ${FIELD_ACCESS_TOKEN}" \
    'http://localhost:8080/events?source=PVMBG&limit=100'
)"

jq '{
    before: ([.events[] | select(.attributes.confidence_level == null)][0]),
    after:  ([.events[] | select(.attributes.confidence_level != null)][0])
  }' <<<"${PVMBG_SCHEMA_RESPONSE}"

jq -e '
    ([.events[] | select(.attributes.confidence_level == null)] | length > 0)
    and
    ([.events[] | select(.attributes.confidence_level != null)] | length > 0)
  ' >/dev/null <<<"${PVMBG_SCHEMA_RESPONSE}" \
  && echo PASS || echo FAIL
```

The final command must print `PASS`. Both records are served through the same
API and from the same table without a schema migration.

### Problem 5: Kafka fan-out and consumer catch-up

Confirm that topic provisioning completed before checking the event stream:

```bash
docker compose ps -a kafka-init
```

`kafka-init` must report `Exited (0)`. Read one event directly from the Kafka
topic to prove that the Aggregator publishes through the broker:

```bash
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic "${KAFKA_TOPIC}" \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 30000
```

The payload must contain `event_type`, `schema_version`, `correlation_id`, and
the canonical `HazardEvent` in `data`.

Check that both independent consumer groups have processed events:

```bash
curl -fsS http://localhost:8085/stats | jq
curl -fsS http://localhost:8086/stats | jq
```

Both `processed_count` values must be greater than zero. The following commands
select one recent correlation ID and show the matching producer, Dashboard, and
Notifier records:

```bash
CORRELATION_ID="$(
  docker compose logs --since 2m --no-log-prefix aggregator \
    | jq -r 'select(.msg == "hazard_event_published") | .correlation_id' \
    | tail -n 1
)"

sleep 1

docker compose logs --since 2m --no-log-prefix \
  aggregator dashboard-consumer notifier-consumer \
  | jq -c --arg cid "$CORRELATION_ID" '
      select(
        .correlation_id == $cid and
        (
          .msg == "hazard_event_published" or
          .msg == "dashboard_event_applied" or
          .msg == "notification_delivered"
        )
      )
    '
```

The output must contain the same correlation ID for all three components. Stop
Notifier and record the outage start time while Dashboard and Aggregator
continue:

```bash
curl -fsS http://localhost:8085/stats | jq
curl -fsS http://localhost:8086/stats | jq

OUTAGE_STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
docker compose stop notifier-consumer
sleep 25

curl -fsS http://localhost:8085/stats | jq

docker compose logs --since "$OUTAGE_STARTED_AT" aggregator dashboard-consumer \
  | grep -E 'hazard_event_published|dashboard_event_applied'

docker compose exec -T kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --group "${NOTIFIER_CONSUMER_GROUP}"
```

The Aggregator and Dashboard logs must continue while the Notifier group shows
positive `LAG`. Restart Notifier and observe its retained-event catch-up:

```bash
docker compose start notifier-consumer
sleep 20

curl -fsS http://localhost:8086/health | jq
curl -fsS http://localhost:8086/stats | jq

docker compose logs --since "$OUTAGE_STARTED_AT" notifier-consumer \
  | grep -E 'notification_delivered|kafka_commit_completed'

docker compose exec -T kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --group "${NOTIFIER_CONSUMER_GROUP}"
```

The Notifier processes retained events and its lag eventually returns to zero.
This proves that events published during the outage are caught up rather than
lost.

Add a third consumer that remains active without changing, rebuilding, or
restarting the Aggregator:

```bash
AGGREGATOR_STARTED_BEFORE="$(
  docker inspect "$(docker compose ps -q aggregator)" \
    --format '{{.State.StartedAt}}'
)"

THIRD_CONSUMER_GROUP="demo-third-consumer-$(date +%s)"
THIRD_CONTAINER="${THIRD_CONSUMER_GROUP}"

docker compose run -d --rm --no-deps \
  --name "${THIRD_CONTAINER}" \
  --entrypoint /opt/kafka/bin/kafka-console-consumer.sh \
  kafka \
  --bootstrap-server kafka:9092 \
  --topic "${KAFKA_TOPIC}" \
  --group "${THIRD_CONSUMER_GROUP}"

sleep 15

docker compose exec -T kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --group "${THIRD_CONSUMER_GROUP}" \
  --members \
  --verbose

docker logs --tail 1 "${THIRD_CONTAINER}" | jq
```

The group description must show one active member with assignments for
`hazard.events.v1`. Verify that the Aggregator process and source code remained
unchanged:

```bash
AGGREGATOR_STARTED_AFTER="$(
  docker inspect "$(docker compose ps -q aggregator)" \
    --format '{{.State.StartedAt}}'
)"

[ "${AGGREGATOR_STARTED_BEFORE}" = "${AGGREGATOR_STARTED_AFTER}" ] \
  && echo PASS || echo FAIL

git diff --quiet --ignore-space-at-eol HEAD -- services/aggregator \
  && echo PASS || echo FAIL
```

Both checks must print `PASS`. Stop the demonstration consumer afterward; it
is removed automatically because it was created with `--rm`:

```bash
docker stop "${THIRD_CONTAINER}"
```

Kafka delivery is at-least-once. Consumers persist their state before manually
committing offsets. Each consumer records `topic:partition:offset` in the same
bbolt transaction as its processing result, so a redelivered message does not
apply the same domain change twice.

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

## Milestone 2 — Problem 2 dan Problem 5

Bagian ini mendokumentasikan implementasi P2 (*Dua Nakhoda, Satu Kapal*) dan
P5 (*Pelampung yang Berbohong*). Keduanya tetap memakai penyimpanan
terdistribusi P1, Audit API, Kafka, autentikasi M1, dan kontrak sumber lain.

Analisis ulang seluruh spesifikasi, perubahan desain terbaru, trade-off, hasil
regresi, serta kriteria yang belum terbukti tersedia di
[M2 P2/P5 design review](docs/M2-P2-P5-design-review.md).
The [published verification status](docs/README.md) records the current build,
retained failures and remaining evidence gaps. Dated operator reports and raw
run artifacts stay local rather than cluttering the remote repository.

Durable polling ledger, schema-v2 migration/rollback constraints and the
upstream-log checker are documented in the
[P2 polling guide](docs/P2-polling-ledger.md).

### Desain P2: Aggregator Aktif-Tunggu dengan Fencing

Deployment menjalankan tiga replika Aggregator di belakang satu Kubernetes
Service. `readinessProbe` memakai `/ready`, sehingga Service berhenti
mengarahkan permintaan ke pod yang kehilangan seluruh penyimpanan kanonik.
Gangguan coordinator menghentikan penerimaan data baru, sementara pod yang
sudah berjalan tetap dapat melayani pembacaan data kanonik. `PodDisruptionBudget`, *topology spread*, dan strategi
`RollingUpdate` menjaga ketersediaan ketika replika diubah atau versi diganti.

GET daftar event biasa memakai read-side dengan penggabungan request bersamaan
dan cache terbatas, dengan TTL bawaan 100 ms sejak pembacaan dimulai. Seluruh
filter publik menjadi bagian dari kunci cache; respons parsial/error tidak
disimpan. Audit API tetap membaca langsung. Konsekuensinya, pembacaan biasa
dapat tertinggal hingga TTL tersebut, sedangkan audit tidak memakai cache ini.

Skema coordinator mempunyai penanda versi yang disimpan atomik bersama migrasi.
Pod baru pada database yang sudah mutakhir cukup membaca penanda, tanpa
mengulang DDL atau menunggu lock migrasi. Database lama tanpa penanda tetap
dimigrasikan; versi skema yang tidak didukung ditolak, bukan diturunkan.
Kegagalan koneksi sementara saat startup dicoba ulang secara terbatas.

Setiap sumber (`BMKG`, `PVMBG`, dan `SENSOR_PANTAI`) mempunyai lease tersendiri.
Lease disimpan di PostgreSQL `aggregator-coordinator` dan seluruh keputusan
kedaluwarsa memakai `clock_timestamp()` database, bukan jam pod. Pergantian
pemilik atau perolehan ulang lease yang sudah habis menaikkan *fencing token*.
Pembaruan lease berjalan setiap TTL/3 (bawaan lima detik) selama batch diproses.
Pembaruan harus mencocokkan holder, token, dan lease yang masih berlaku;
kegagalan membatalkan konteks polling dan pemrosesan batch.

Jalur penulisan memakai **admission transaksional dan outbox**:

Schema v3 update (build `10-replication`): delivery acknowledgements, retries,
and per-identity ordering are now independent for Kafka and **each assigned
canonical replica**. The payload is cleared only after all targets acknowledge.
Reads reconcile both replicas; ready-store anti-entropy repairs historical gaps.
See [the schema/cutover guide](docs/P2-polling-ledger.md) and
[verification status](docs/README.md).

Build 11 follow-up: read completeness now uses a bounded prefix-coverage proof;
reachable replica lag is reported separately as `replication_degraded`. Failed
reads or uncertified filtered pages remain partial. See the
[P2.7/P2.8 verification status and retained failures](docs/README.md).

1. `AcceptEvent` mengunci baris lease dan memeriksa holder, token, serta waktu
   database, kemudian menyimpan intent event ke `ingestion_outbox` dalam
   transaksi PostgreSQL yang sama. Poller tidak menulis langsung ke P1/Kafka.
2. Transaksi yang ditinggalkan klien dibatasi `statement_timeout` dan
   `idle_in_transaction_session_timeout` pada server (bawaan dua detik).
   Klien yang bangun dengan transaksi yang sudah dibatalkan tidak dapat commit.
3. Worker mengirim intent yang sudah sah ke shard P1 dan Kafka. Setiap intent
   memiliki `ingestion_version` yang persisten. UPSERT P1 dan dashboard consumer
   mengabaikan versi yang lebih lama atau sudah diterapkan.
4. Acknowledgement disimpan setelah pengiriman berhasil. Kegagalan di tengah
   pengiriman mengulang versi yang sama, dengan backoff 100 ms sampai lima detik.
   Payload outbox dibersihkan setelah acknowledgement; metadata dipertahankan.

Validasi lease dan penerimaan intent mempunyai satu titik keputusan atomik;
tidak ada celah antara pemeriksaan lease di aplikasi dan penulisan event baru.
Penguncian baris mengikuti [kontrak transaksi PostgreSQL](https://www.postgresql.org/docs/16/explicit-locking.html#LOCKING-ROWS).
Worker yang melanjutkan pengiriman intent lama hanya mengulang pekerjaan yang
sudah diterima secara sah, bukan menerima input baru menggunakan lease lama.
Tidak ada klaim transaksi atomik lintas PostgreSQL dan Kafka: pengiriman outbox
bersifat *at-least-once*, dengan penanganan idempoten di tujuan.

Cursor sumber disimpan hanya setelah seluruh item batch diterima secara
persisten ke outbox. Pengiriman ke P1/Kafka dapat menyusul. Batch yang dibaca
ulang dideduplikasi menggunakan identitas event dan fingerprint isi; kegagalan
query cursor menghasilkan error, bukan dianggap cursor kosong. Cursor waktu
tidak dapat bergerak mundur dan perubahan cursor dikunci terhadap takeover.
Pemisahan lease dan worker pengiriman per sumber mengisolasi sumber yang lambat.

Nilai bawaan lease adalah 15 detik. Nilai ini memberi ruang lebih dari dua kali
timeout HTTP Aggregator, tetapi masih memungkinkan pengambilalihan jauh di bawah
batas 60 detik dalam kondisi coordinator/jaringan yang tersedia. Database
koordinasi ditempatkan pada control plane. Pembatasan gangguan hanya pada worker
non-control-plane berlaku pada topologi dua node; pada tiga VPS, node A tidak
boleh diasumsikan bebas gangguan. Kehilangan disk coordinator belum ditangani.
Coordinator saat ini **belum HA**; pending intent juga bergantung pada disk
tersebut. HA coordinator memerlukan rancangan replikasi/failover tersendiri.

Setiap HTTP request upstream memvalidasi lease lagi, termasuk request kedua
BMKG. Hal ini mencegah request yang sudah diketahui stale. Namun, proses masih
dapat freeze setelah validasi dan sebelum paket HTTP dikirim. Tanpa dukungan
fencing atau idempotency pada upstream, sistem tidak dapat menjamin bahwa
request HTTP fisik sama sekali tidak pernah terulang pada semua kemungkinan
freeze/jaringan. Log upstream dan pengujian gangguan tetap diperlukan untuk
menilai kriteria P2.2; deduplikasi penyimpanan tidak membuktikan syarat tersebut.

### Desain P5: Isolasi, Circuit Breaker, dan Quarantine

Sensor Pantai memakai image dan digest resmi berikut tanpa modifikasi:

```text
sustuna/sensor-pantai@sha256:51fbd90d3891ad6e9f5768ad330ffb65205644cb29e50da65c7538f2e0b4397d
```

Manifest mengikuti [contoh resmi Sensor Pantai](https://raw.githubusercontent.com/susTuna/if4031-sensor-pantai/772f2c6a58b5eab786c718ead5d7b1541760f517/manifest.yaml).
Token berada di Secret `pantai-token`; profil, seed, dan kecepatan berada di
ConfigMap. Secret opsional `pantai-key` dapat dibuat untuk profil ujian.

Adapter Sensor Pantai mempunyai HTTP client, timeout, dan goroutine terpisah
dari BMKG/PVMBG. Respons dibatasi 4 MiB dan setiap stasiun serta observasi
divalidasi. Item yang melanggar kontrak tidak menjadi `HazardEvent`; payload
aslinya disimpan di `unprocessed_messages` bersama alasan, jumlah percobaan, dan
status. Data ini tersedia melalui Audit API dan endpoint khusus Ops.

Tiga kegagalan berurutan membuka *circuit breaker* selama dua detik. Saat
terbuka, tidak ada permintaan baru ke sensor. Setelah masa tunggu, hanya satu
probe *half-open* yang diizinkan. Jam simulasi Sensor Pantai berbeda dari jam
host. Karena itu, watermark `generated_at` dan waktu terakhir kemajuannya
disimpan di database koordinasi. Timestamp yang mundur atau tidak maju selama
30 detik dianggap tidak sehat, meskipun HTTP menjawab 200. Durasi stagnasi
memakai jam database, bukan jam pod. Offset absolut pada sampel pertama tidak
dapat diketahui dari kontrak ini saja; sampel pertama membentuk baseline.

Observasi valid dipetakan menjadi `HazardEvent` dengan `source=SENSOR_PANTAI`
dan `hazard_type=COASTAL`. Observasi kritis memakai severity `AWAS`, langsung
dikirim melalui outbox ke Kafka, dan dicatat secara idempoten oleh notifier.
Satu halaman diterima dalam satu transaksi batch berpagar lease. Pengiriman
event kritis mendahului antrean normal, dengan urutan versi per identitas tetap
terjaga. Endpoint `/stats` dan `/metrics` notifier melaporkan jumlah event kritis,
p95 publikasi-ke-notifier, serta p95 admission-ke-notifier untuk alert pantai.
`source_latency_ms` bernilai `null` untuk Sensor Pantai: mengurangi timestamp
simulasi dari jam host akan menghasilkan angka yang tidak bermakna.

Client API mencadangkan kapasitas khusus Tim Lapangan. Media hanya dapat memakai
kapasitas bersama, sedangkan Tim Lapangan dapat memakai kapasitas bersama dan
cadangan. Saat jenuh, Media menerima 429 dengan cepat tanpa menghabiskan slot
Tim Lapangan.

Prioritas kini diteruskan dari identitas JWT yang terverifikasi ke Aggregator,
bukan dari header client. Aggregator mencadangkan slot HTTP, slot pengisian
cache, serta dua koneksi baca per unit storage untuk Tim Lapangan. Pengiriman
canonical dan Kafka berjalan independen; outbox baru diakui setelah keduanya
berhasil. Notifier mengulang pesan/commit yang gagal sebelum mengambil offset
berikutnya agar kegagalan tidak terlewati oleh commit yang lebih baru.

Target keandalan jalur peringatan Tim Lapangan adalah:

- 100% observasi kritis valid diterima notifier;
- p95 ketersediaan alert di sumber sampai notifier paling lama 5 detik; dan
- tidak ada duplikat notifikasi untuk identitas event dan severity yang sama.

### Konfigurasi P2/P5

| Variabel | Bawaan | Fungsi |
|---|---:|---|
| `COORDINATION_DATABASE_URL` | wajib | Database lease, cursor, outbox, dan quarantine |
| `AGGREGATOR_LEASE_TTL_MS` | `15000` | Masa berlaku lease berdasarkan waktu database |
| `COORDINATION_TIMEOUT_MS` | `2000` | Batas operasi database koordinasi |
| `AGGREGATOR_FIELD_RESERVED_SLOTS` | `16` | Cadangan Tim Lapangan dari total 64 slot Aggregator; 0 menonaktifkan cadangan |
| `AGGREGATOR_READ_CACHE_TTL_MS` | `100` | TTL read-side GET biasa, rentang 0–1000 ms; 0 menonaktifkan cache; Audit API tidak memakai cache |
| `COASTAL_URL` | `http://sensor-pantai:8080` | Alamat Sensor Pantai |
| `PANTAI_AUTH_TOKEN` | wajib | Bearer token Sensor Pantai |
| `COASTAL_POLL_INTERVAL_MS` | `500` | Interval pengambilan observasi |
| `COASTAL_HTTP_TIMEOUT_MS` | `1000` | Batas satu permintaan sensor |
| `COASTAL_MAX_STALENESS_MS` | `30000` | Batas durasi tanpa kemajuan jam sumber |
| `COASTAL_CIRCUIT_FAILURES` | `3` | Kegagalan sebelum circuit terbuka |
| `COASTAL_CIRCUIT_OPEN_MS` | `2000` | Durasi circuit terbuka |
| `CLIENT_API_FIELD_RESERVED_SLOTS` | `32` | Slot khusus Tim Lapangan dari total 64 |

### Menjalankan dan Membuktikan

Siapkan Secret dan deploy seluruh sistem dari kondisi awal:

```bash
bash scripts/setup-env.sh
bash scripts/deploy-k8s.sh --k3s --build --push \
  --image-prefix docker.io/<pengguna>
```

Jika `.env` dari M1/P1 masih dipakai, tambahkan hanya variabel yang belum ada
tanpa mengganti kredensial lama:

```bash
bash scripts/setup-env.sh --merge
```

Untuk profil ujian Sensor Pantai, ubah `PANTAI_PROFILE=exam`, buat Secret kunci,
kemudian deploy ulang:

```bash
kubectl -n sistem-informasi-kebencanaan-bnpb create secret generic pantai-key \
  --from-file=key=/lokasi/berkas-key
```

Untuk smoke manual, jalankan port-forward pada terminal terpisah. Port-forward
Service memilih satu pod: jangan gunakan jalur ini sebagai bukti distribusi
beban 1-vs-3 atau scaling. Untuk load test VPS, gunakan common Service melalui
WireGuard sesuai [panduan VPS](infra/vps/README.md).

```bash
kubectl -n sistem-informasi-kebencanaan-bnpb port-forward service/aggregator 8083:8083
kubectl -n sistem-informasi-kebencanaan-bnpb port-forward service/client-api 8080:8080
kubectl -n sistem-informasi-kebencanaan-bnpb port-forward service/notifier-consumer 8086:8086
kubectl -n sistem-informasi-kebencanaan-bnpb port-forward service/sensor-pantai 8090:8080
```

Skrip demonstrasi sudah dipisahkan dan diberi nomor:

```bash
bash scripts/m2/p2/01-topology-and-health.sh
bash scripts/m2/p2/02-leader-failover.sh BMKG
bash scripts/m2/p2/03-tester-scenario.sh <nama-skenario-practice>
bash scripts/m2/p2/04-scale-3-5-2.sh
bash scripts/m2/p2/05-rolling-update.sh <image-aggregator:tag>
bash scripts/m2/p2/06-throughput-1-vs-3.sh

bash scripts/m2/p5/01-sensor-contract.sh
bash scripts/m2/p5/02-circuit-recovery.sh
bash scripts/m2/p5/03-audit-unprocessed.sh
bash scripts/m2/p5/04-notifier-latency.sh
```

Setiap permintaan cURL pada skrip mencetak `http_code` dan `time_total`.
Load test P2 dijalankan dari luar pod aplikasi dan dipertahankan pada 10.000 VU
selama 120 detik:

```bash
k6 run -e AGGREGATOR_BASE_URL=http://127.0.0.1:8083 \
  -e AGGREGATOR_API_KEY="$AGGREGATOR_API_KEY" scripts/m2/p2/load.js
```

Jalankan `04-scale-3-5-2.sh` selama k6 berlangsung. Uji prioritas P5 memakai
access token Media dan Tim Lapangan:

```bash
k6 run -e MEDIA_ACCESS_TOKEN="$MEDIA_ACCESS_TOKEN" \
  -e FIELD_ACCESS_TOKEN="$FIELD_ACCESS_TOKEN" scripts/m2/p5/05-priority-load.js
```

Bukti akhir tetap harus diambil pada cluster demo minimal dua node dengan kernel
terpisah. Unit test dan manifest tidak menggantikan hasil Tester Agent, log
upstream, tangkapan dashboard, checksum Audit API, atau hasil load test aktual.

Pengujian regresi PostgreSQL tersedia untuk pemilihan serentak, pemilik lama yang
freeze setelah validasi, perolehan ulang epoch, kegagalan cursor, retry outbox,
transaksi yang ditinggalkan, dan pengiriman versi yang terbalik. Gunakan database
pengujian yang dapat dijangkau dari proses Go; setiap test memakai schema acak
dan menghapus hanya schema miliknya saat selesai:

```bash
P2_TEST_DATABASE_URL='postgres://postgres:<password>@127.0.0.1:5432/lease_test?sslmode=disable' \
  go test -count=1 -v ./services/aggregator/internal/store
go test ./services/aggregator/internal/aggregator ./services/dashboard-consumer/internal/store
```

Tanpa `P2_TEST_DATABASE_URL`, test integrasi PostgreSQL ditandai *skip*.

Validasi perbaikan pada 18 November 2026: delapan test integrasi tersebut lulus
pada PostgreSQL 16.16, termasuk fault injection timeout di tengah admission.
Seluruh test service dan `go vet` Aggregator/dashboard consumer juga lulus.
Hasil ini belum menggantikan beban 10.000 VU dan seluruh skenario ujian.
Catatan pengujian tiga VM, hasil aktual, serta batas validasinya tersedia pada
[`scripts/m2/e2e/README.md`](scripts/m2/e2e/README.md).

---

<div align="center">
  IF4031 &middot; Arsitektur Aplikasi Terdistribusi
</div>
