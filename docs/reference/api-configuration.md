# Service, API, and configuration reference

This reference describes the Compose/M1 interfaces and defaults. For M2 polling,
Sensor Pantai, gateway, and read-path additions, also consult the
[M2 guide](../guides/m2.md) and [design index](../README.md#design-and-contracts).
Examples use Compose addresses, not VPS NodePorts. Keep actual credentials in
`.env`; `.env.example` documents application configuration.

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
