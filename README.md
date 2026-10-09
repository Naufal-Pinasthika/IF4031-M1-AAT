# Hazard Coordination Platform

> Distributed hazard ingestion and coordination platform for IF4031

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
docker-compose.yml       Full-system orchestration
.env.example             Complete configuration template with placeholder secrets
go.work                  Go workspace for all seven modules
```

## Quick Start

### Requirements

- Docker Engine with Docker Compose v2, or Docker Desktop through WSL
- Go 1.25 for generating the JWT signing key and local checks
- OpenSSL for generating local example secrets
- `curl` for API calls
- `jq` for the demonstration commands
- Optional: `hey` for the Problem 2 load test

All shell commands below are written for Bash and can be run from WSL.

### 1. Create the local environment

```bash
cp .env.example .env
```

Values beginning with `replace-with-` are placeholders. Replace them with
unique values before starting the system. This Bash helper generates safe
random values while preserving repeated database-password references:

```bash
replace_secret() {
  local placeholder="$1"
  local value
  value="$(openssl rand -hex 24)"
  sed -i "s|${placeholder}|${value}|g" .env
}

for placeholder in \
  replace-with-bmkg-api-key \
  replace-with-pvmbg-token \
  replace-with-aggregator-api-key \
  replace-with-postgres-admin-password \
  replace-with-aggregator-app-password \
  replace-with-auth-postgres-password \
  replace-with-unique-media-secret \
  replace-with-unique-field-secret \
  replace-with-unique-ops-secret
do
  replace_secret "$placeholder"
done

JWT_KEY="$(go run ./services/auth/cmd/keygen)"
sed -i "s|replace-with-base64-pem-rsa-private-key|${JWT_KEY}|" .env
unset JWT_KEY
```

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
```

Wait until all application containers report `healthy`. Follow structured logs
with:

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

Reset PVMBG before the scenario so it starts with schema v1:

```bash
docker compose restart pvmbg
until curl -fsS http://localhost:8082/health >/dev/null; do sleep 1; done

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
```

The resulting event must retain its canonical name and coordinates while
placing `confidence_level` in `attributes`.

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

Record throughput, p50/p95/p99, non-429 errors, and HTTP 429 responses from
`/tmp/bmkg-load.txt` and `/tmp/pvmbg-load.txt`. Issue another fresh token
before testing outage recovery:

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

Restore `AUTH_ACCESS_TOKEN_TTL_SECONDS=60` after load testing.

### Problem 3: Trust domains, scopes, and refresh

Verify that credentials are not accepted across institutions:

```bash
curl -sS -o /dev/null -w 'BMKG key at PVMBG: HTTP %{http_code}\n' \
  -H "Authorization: Bearer ${BMKG_API_KEY}" \
  'http://localhost:8082/volcanic-reports?since=1970-01-01T00:00:00Z'

curl -sS -o /dev/null -w 'PVMBG token at BMKG: HTTP %{http_code}\n' \
  -H "X-BMKG-Key: ${PVMBG_TOKEN}" \
  'http://localhost:8081/seismic-events?since=1970-01-01T00:00:00Z'
```

Both requests must return HTTP 401 or 403. Verify Media field enforcement:

```bash
MEDIA_TOKEN_RESPONSE="$(
  curl -fsS -X POST \
    -u "${MEDIA_CLIENT_ID}:${MEDIA_CLIENT_SECRET}" \
    http://localhost:8084/v1/token
)"
MEDIA_ACCESS_TOKEN="$(jq -r '.access_token' <<<"$MEDIA_TOKEN_RESPONSE")"

curl -fsS \
  -H "Authorization: Bearer ${MEDIA_ACCESS_TOKEN}" \
  'http://localhost:8080/events?limit=1' | jq '.events[0]'

curl -i -sS \
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

curl -sS -o /dev/null -w 'Refreshed access token: HTTP %{http_code}\n' \
  -H "Authorization: Bearer ${NEW_ACCESS_TOKEN}" \
  'http://localhost:8080/events?limit=1'

curl -sS -o /dev/null -w 'Old access token after refresh: HTTP %{http_code}\n' \
  -H "Authorization: Bearer ${OLD_ACCESS_TOKEN}" \
  'http://localhost:8080/events?limit=1'
```

The expired token must return 401 and the refreshed token must return 200.

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

Stop and rebuild only the Notifier Consumer while unrelated reads continue:

```bash
docker compose stop notifier-consumer

curl -fsS \
  -H "Authorization: Bearer ${FIELD_ACCESS_TOKEN}" \
  'http://localhost:8080/events?source=BMKG&limit=1' | jq

docker compose build notifier-consumer
docker compose up -d notifier-consumer
docker compose ps
```

Inspect storage-network ownership:

```bash
docker inspect "$(docker compose ps -q postgres)" \
  --format '{{json .NetworkSettings.Networks}}' | jq

docker inspect "$(docker compose ps -q aggregator)" \
  --format '{{json .NetworkSettings.Networks}}' | jq

docker inspect "$(docker compose ps -q client-api)" \
  --format '{{json .NetworkSettings.Networks}}' | jq
```

PostgreSQL and Aggregator share the private `database` network; Client API does
not. To show flexible JSONB storage, query up to 100 PVMBG events after the
Problem 1 schema change:

```bash
curl -fsS \
  -H "Authorization: Bearer ${FIELD_ACCESS_TOKEN}" \
  'http://localhost:8080/events?source=PVMBG&limit=100' \
  | jq '{
      before: ([.events[] | select(.attributes.confidence_level == null)][0]),
      after:  ([.events[] | select(.attributes.confidence_level != null)][0])
    }'
```

Both records are served from the same table without a schema migration.

### Problem 5: Kafka fan-out and consumer catch-up

Check that both independent consumer groups receive events:

```bash
curl -fsS http://localhost:8085/stats | jq
curl -fsS http://localhost:8086/stats | jq

docker compose logs --since 2m \
  aggregator dashboard-consumer notifier-consumer \
  | grep correlation_id
```

Stop Notifier while Dashboard and Aggregator continue:

```bash
docker compose stop notifier-consumer
sleep 25

curl -fsS http://localhost:8085/stats | jq

docker compose exec -T kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --group "${NOTIFIER_CONSUMER_GROUP}"
```

Restart Notifier and observe its retained-event catch-up:

```bash
docker compose start notifier-consumer
sleep 15

curl -fsS http://localhost:8086/stats | jq

docker compose exec -T kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --group "${NOTIFIER_CONSUMER_GROUP}"
```

Add a third consumer without changing or rebuilding the Aggregator:

```bash
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic "${KAFKA_TOPIC}" \
  --group demo-third-consumer \
  --from-beginning \
  --max-messages 1
```

Kafka delivery is at-least-once. Consumers persist their state before manually
committing offsets and apply idempotency checks so redelivery does not create a
duplicate domain effect.

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

---

<div align="center">
  IF4031 &middot; Arsitektur Aplikasi Terdistribusi
</div>
