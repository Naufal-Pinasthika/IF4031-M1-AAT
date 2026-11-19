# P2: durable polling ownership and evidence

Status: implemented and regression-tested on 19 November 2026; deployed in
builds `m2-vps-20261119-03` / `04`. This strengthens P2.2 without changing source
contracts, official images, resource limits or the load-test workload. It does
not implement coordinator/Kafka HA or establish a full P2/P5 acceptance pass.

## Ownership and shared cadence

All Aggregators serve reads. Independently, each source has one current lease
holder and monotonically increasing fencing token. The holder must now also
commit a **poll attempt** before making any upstream HTTP request.

Previously the poll interval was a process-local timer: a replacement owner
could poll immediately after its predecessor finished. The new database-backed
`source_poll_heads.next_poll_at` survives restarts, scaling and handovers.
Admission, cursor updates and poll-state transitions serialize on the same
lease row. Authorization and scheduling use the coordinator's database clock;
local monotonic timers are only wake-up hints and cannot override the gate.

| State | Meaning | Next action |
| --- | --- | --- |
| `in_flight` | Begin committed; HTTP and durable admission may be underway | Only the same current owner/epoch/attempt can validate a request or finish it. |
| `completed` | Fetch/processing callback and completion committed | Next cycle waits until at least the previous start plus the configured interval. Slow cycles do not accumulate catch-up ticks. |
| `uncertain` | Failure, lost response or abandoned epoch; not proof of no upstream effect | Keep the durable checkpoint and replay after one shared retry interval. Never label the unknown result successful. |

An ownership change first marks an abandoned attempt uncertain and establishes
its cooldown, without sending HTTP in that reconciliation call. A same-epoch
unfinished attempt is not silently reused. Ambiguous Begin/Finish failures
retire the epoch; if release itself fails, expiration remains the fallback.
Otherwise a process could renew an abandoned attempt forever.

Each attempt records its ID, source, owner, epoch, previous attempt, start/end,
status/error and a snapshot of the persisted **input checkpoint**. An empty
checkpoint object means there was no persisted cursor at Begin; the existing
configured bootstrap fallback still applies. This is evidence metadata, not a
copy of source payloads or the specification's canonical Audit API.

Marking an attempt uncertain does not itself advance a cursor. However, an
admission/cursor commit can have succeeded before a later completion response
is lost. Therefore uncertainty must never rewind a successfully persisted
cursor; recovery always reads the current durable checkpoint. Existing stable
hazard identities, fenced admission and idempotent projections still handle
overlapping input/replays.

## HTTP guard and trace identity

Every actual source RoundTrip, including both BMKG endpoints, validates the
current attempt and epoch. Cancellation is checked again after that query.
Headers carry `X-Poll-ID`, `X-Poll-Holder`, `X-Poll-Epoch` and correlation ID
`poll-<SOURCE>-<ID>`. Existing BMKG/PVMBG request logs already preserve the
correlation header; no mock implementation or contract was modified.

Only the low-rate source clients disable HTTP connection reuse, HTTP/2 and
automatic redirects. Go's transport can otherwise retry idempotent requests
on a previously successful connection. A failed source operation instead
becomes an explicitly recorded later cycle. Client read/load-test connections
are unaffected. Trade-off: extra TCP/TLS setup and redirect responses become
visible failures rather than transparently following another URL.
[Go Transport retry behavior](https://pkg.go.dev/net/http#Transport)

**This is not absolute exactly-once physical HTTP.** A process can still pause
after authorization and before sending, or lose the response after the source
processed a request. A retry uses a new attempt ID and may overlap the prior
input range. These headers identify requests; they do not make an upstream
enforce fencing or idempotency. Strict handling of that boundary needs upstream
cooperation, which is not assumed or fabricated for the official components.
The shared gate prevents ordinary handover-driven extra polling and makes
uncertainty inspectable; actual upstream evidence is still required for P2.2.

## Inspecting and checking evidence after deployment

- Internal route: `GET /internal/polling?source=BMKG&limit=1000`, using the
  existing `X-Aggregator-Key` authentication.
- Client API route: `GET /polling?...`, using a verified token with `ops` scope.
  Field/Media tokens do not authorize this endpoint.
- Default limit is 100; maximum 1000, ordered by descending poll ID. Omit source
  to return all sources. This endpoint does not replace or extend Appendix A.
- Retain at least the latest 10,000 attempts per source, pruning metadata every
  128 new attempts. Historical `previous_poll_id` may point beyond retention.
  Export periodically: a single API response only contains its requested limit.

Example from an authorized operator machine on the private VPS mesh, with
credentials already set in the environment (do not commit their values):

```bash
mkdir -p .e2e/polling
curl --fail --silent --show-error \
  -H "X-Aggregator-Key: $AGGREGATOR_API_KEY" \
  'http://10.77.0.1:30083/internal/polling?source=BMKG&limit=1000' \
  --output .e2e/polling/bmkg-ledger.json \
  --write-out 'http_code=%{http_code} time_total=%{time_total}s\n'
# Repeat with source=PVMBG and output pvmbg-ledger.json.

NS=sistem-informasi-kebencanaan-bnpb
kubectl -n "$NS" logs -l app=bmkg --all-containers=true --prefix=false --tail=-1 \
  > .e2e/polling/bmkg.jsonl
kubectl -n "$NS" logs -l app=pvmbg --all-containers=true --prefix=false --tail=-1 \
  > .e2e/polling/pvmbg.jsonl

python3 scripts/m2/p2/check_polling.py \
  --ledger .e2e/polling/bmkg-ledger.json .e2e/polling/pvmbg-ledger.json \
  --upstream .e2e/polling/bmkg.jsonl .e2e/polling/pvmbg.jsonl \
  --output .e2e/polling/result.json
```

Preserve all upstream replicas' logs, including previous/deleted pods using a
log collector during the experiment. Do not concatenate overlapping exports
of the **same** raw log: the checker intentionally treats repeated lines as
potential duplicate HTTP. Export final metadata after attempts settle; if
supplying successive snapshots, put the most recent last.

The checker fails on repeated `(source, poll_id, endpoint)`, unattributed
business requests, missing/mismatched metadata, missing/non-2xx endpoint logs
for **any exported completed cycle**, missing required source cycles and
non-JSON input. Thus exporting a ledger whose cycles predate retained upstream
logs correctly fails with incomplete evidence; do not weaken that check to
claim a pass. Preserve matching coverage from the beginning of the experiment.

By default it requires BMKG and PVMBG, not Sensor Pantai. Optional
`--require-source SENSOR_PANTAI` only works if the genuine official image emits
the supported correlated JSON request-log format; that is not established by
these tests. Never alter that image to satisfy this checker.

Passing means **only the supplied bounded evidence** passed these checks. It
does not prove cross-attempt request uniqueness, no data loss, complete log
coverage, clock/freeze safety, failover time or all P2 criteria. A new-image E2E
run must still collect upstream rate/ownership evidence across 3→5→2 scaling,
freeze/wake and failover, plus canonical identity/checksum comparisons.

## Migration and verification

Coordinator schema v1/no-marker databases migrate atomically to **v2**, adding
polling tables and preserving existing leases/cursors/outbox. Current v2 startup
only reads the marker; future versions are rejected. Old binaries that only
accept v1 are **not a supported automatic rollback** after migration. Plan a
compatible rollback image and preserve the database; do not lower the marker
or drop tables to force old startup. During mixed-version rollout old pollers
do not obey the new cadence, so claim shared-gate evidence only after every
poller uses this build. The v1-to-v2 live migration was performed by build 03;
existing PVCs/cursors were retained.

Regression includes concurrent Begin winners, stale request/finish rejection,
abandoned-attempt reconciliation, cadence across handover, uncertain checkpoint
preservation and v1 migration; real HTTP tests check both BMKG endpoints,
request identity, cancellation and redirects. See the full
[P2/P5 review](M2-P2-P5-design-review.md) for test results and open risks.
Coordinator/Kafka HA, coordinator wall-clock jumps and node-A disk loss remain
separate design gaps; this ledger shares the coordinator's failure domain.

### Schema v3 canonical handoff follow-up

Build `m2-vps-20261119-10-replication` upgrades v1/v2 to v3 with a per-target
projection queue. Completed legacy payloads cannot be reconstructed from the
outbox; ready-store anti-entropy repairs them from their surviving P1 peers.
Pending legacy payloads gain separate `broker` and strict-all-replica `canonical`
tasks. New admissions atomically create one broker task and the two assigned
canonical tasks (one in explicit single-store development mode).

The v2→v3 transition requires stopping old Aggregator workers before starting
new ones: v2 workers may acknowledge after only one replica write. This one-off
cutover is NOT a P2.8 rolling-update pass. Do not run schema-v2 rollback images
against v3. See [the correction and evidence summary](../status/README.md).
