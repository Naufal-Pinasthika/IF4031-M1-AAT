# M1 demonstration runbook

Problem numbers in this document refer to **M1**, not M2. Run commands from the
repository root in Bash/WSL after the [local Compose setup](../guides/local-compose.md),
including loading `.env` and issuing the client tokens. The examples can change
mock behavior, rebuild services, or stop consumers; use a disposable local lab.
They are not an instruction to fault the VPS deployment.

## Cases

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
