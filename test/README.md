# Report-facing system tests

`p1/`, `p3/` and `p4/` contain numbered report-facing E2E scenarios, not unit tests.
Related assertions share a workflow: schema replay includes ordering and access
control; delivery includes idempotency and hard crashes; stream recovery includes
clustering, mutations, burst catch-up and baseline/fault equivalence.

Unit and PostgreSQL integration tests live beside their services:

```sh
go test ./services/aggregator/... ./services/client-api/...
node --test services/client-api/cmd/server/dashboard_ui.test.cjs
```

The dashboard's dependency-free Node tests cover DOM rendering, UTC date
controls, partial/lagging/stale states and inert source text. They do not
substitute for manual browser layout verification.

Database integration tests require `P2_TEST_DATABASE_URL` pointing to a disposable
PostgreSQL database with schema-creation permission. Without it, those tests skip.
Never use the live coordinator or canonical database. The Go overlay runner and
date-specific remote regression-container runner have been removed.

## Prerequisites

- Run on the laptop with Python 3, cURL and an already deployed lab.
- Keep credentials in the ignored repository `.env`, or use `--env-file`.
- In WSL/Linux, `--vps` uses private `infra/vps/inventory.json`, opens its own SSH
  HTTP tunnel and uses remote kubectl. It closes only its own tunnel afterward.
- Without `--vps`, provide reachable URLs and local kubectl access through
  `--kubeconfig` or the normal kubectl environment. Override URLs with
  `--client-url`, `--auth-url`, `--aggregator-url`, `--mock-url`, and `--feed-url`.
- Audit reads use the existing local development `P1_AUDIT_TOKEN`, not the
  assistant's future special audit credentials.
- Run mutating scenarios sequentially in a dedicated development/practice window,
  never concurrently with another operator, an exam, or an official fault run.

Commands below assume the repository root. Scripts resolve configuration and
evidence paths independently of the current working directory. Every script has
`--help`. Shared options include `--repeat` (default 3), `--timeout` (default 90s),
and `--output` (a **new**, private evidence directory). `--image-tag` selects an
already imported Aggregator candidate for isolated tests; otherwise those tests
use the currently deployed image. It does not rebuild or deploy images.
`p4/02-recovery.py` uses a 300-second overall wait budget; its separate
60-second post-burst catch-up assertion is unchanged by that budget.

## Scenarios

| File | Responsibility | Criteria | Side effects |
| --- | --- | --- | --- |
| [p1/00-preflight.py](p1/00-preflight.py) | Storage topology, node-local disks and memory limits | Preparation | Read-only |
| [p1/01-sharding.py](p1/01-sharding.py) | Physical placement, two copies and geographically skewed load | P1.1–2 practice | Three temporary node-local storage volumes plus isolated application/database/topic |
| [p1/02-queries.py](p1/02-queries.py) | Global pagination and analytical totals/time filters against Audit API | P1.3, P1.7 | Same isolated setup; no original data changes |
| [p1/03-recovery.py](p1/03-recovery.py) | Repeated empty-volume replacement, live traffic and restored replication | P1.4–7 practice | Deletes only run-owned storage PVCs; optional `--node-fault` interrupts worker B for 70s |
| [p1/04-live-recovery.py](p1/04-live-recovery.py) | Actual canonical-volume loss, worker return and physical per-record recovery | P1.4–7 live development lab | Permanently deletes one explicitly selected original canonical PVC on a worker; stops its k3s/containers for 70s; retains unrelated PVCs |
| [p1/05-analytics.py](p1/05-analytics.py) | Repeated live all-history/time-range analytics, request budget and aggregate integrity | P1.7 | Read-only; does not replace the isolated Audit comparison or manual chart inspection |
| [p3/00-preflight.py](p3/00-preflight.py) | API/configuration/topology prerequisites | Preparation | Read-only |
| [p3/01-schema.py](p3/01-schema.py) | Declared schema-acceptance policy | P3.1 | Temporarily selects adapter v1 and PVMBG modes |
| [p3/02-poison.py](p3/02-poison.py) | Poison isolation and intact retention | P3.2–3 | Repeats four malformed PVMBG modes |
| [p3/03-ops-access.py](p3/03-ops-access.py) | Ops-only failure access | P3.5 | Unauthorized replay requests must be rejected |
| [p3/04-replay.py](p3/04-replay.py) | Ordered schema recovery, isolation and Ops-only access | P3.1, P3.4–5 | Temporary VPS application and databases; no live mock/config changes |
| [p3/05-delivery.py](p3/05-delivery.py) | Fast durable acknowledgements, retry classification, crash recovery and idempotency | P3.2, P3.6 | Blocks only temporary result storage, kills all three temporary receivers |
| [p4/00-preflight.py](p4/00-preflight.py) | Configured feed prerequisites | Preparation | Reads session; never starts/resets it |
| [p4/01-live-stream.py](p4/01-live-stream.py) | Live progress, lag and latency observation | P4.4, P4.7 | Read-only |
| [p4/02-recovery.py](p4/02-recovery.py) | Full-input oracle, updates/retractions, catch-up, reconnect and baseline/fault equivalence | P4.1–5, P4.8 | Temporary VPS applications/databases/topic; kills only their stream owner |
| [p4/03-audit.py](p4/03-audit.py) | Canonical wildfire snapshot/invariants | P4.1 | Read-only; not a reference comparison |
| [p4/04-dashboard.py](p4/04-dashboard.py) | Real dashboard HTML and measured Prometheus panel data | P4.4 | Private loopback port-forward; read-only |

### P1 sequence

```sh
python3 test/p1/00-preflight.py --vps
python3 test/p1/01-sharding.py --vps --allow-changes --repeat 3 --records 300
python3 test/p1/02-queries.py --vps --allow-changes --repeat 3
python3 test/p1/03-recovery.py --vps --allow-changes --repeat 3
python3 test/p1/05-analytics.py --vps --repeat 5
```

The P1 sandbox clones the deployed PostgreSQL StatefulSet templates into three
new, run-labelled local PVCs on A/B/C. Application images and memory limits are
unchanged. Original canonical PVCs are never fault targets. A private two-replica
gateway fronts three Aggregators; the temporary Client API uses that gateway.
The coordinator gets only a temporary database. Cleanup permanently removes the
temporary data, after checking ownership labels and exact claim names.

`01-sharding` measures CPU/memory during import and physical primary/replica
counts before/after the catalog and concentrated regional burst. Its input is
explicitly labelled practice data, not the later assistant catalog. Physical
copies, audit locations, checksums and aggregates are independently checked.
Short-run Kubernetes CPU samples have metrics-server sampling resolution; they
are not a capacity benchmark. `02-queries` checks every page, including time ties,
and all five hazard types plus severity, area and UTC daily totals/range filters.

For an actual worker interruption, obtain a dedicated maintenance window first:

```sh
python3 test/p1/03-recovery.py --vps --allow-changes --node-fault --repeat 3
```

This stops k3s and its containers on **worker B**, not the control plane, with a
systemd timer that restarts k3s-agent after 70 seconds. Other applications on B
are affected temporarily, including the official Tester Agent as a consequence
of the node outage; its manifest, permissions and image are not modified.
Only the temporary canonical volume is erased. No whole VPS disk or original
application volume is erased. The ordinary mode instead alternates temporary
disk loss on B and C, without stopping either node. Report these fault scopes
precisely; neither run establishes control-plane/coordinator failure tolerance.

Reads and analytical requests are exercised across fault injection and empty-disk
bootstrap. They must return 200 within the declared five-second test bound;
partial responses must name unavailable units. The importer acknowledges only
fully replicated writes: while one destination is missing it may return 503
with accepted/failed counts, not falsely acknowledge full replication. Such
ambiguous writes are retried after repair with the same identities, and the
final audit/physical comparison must recover two copies of every input record.
No SQL repair, manual data copy, or Aggregator restart is permitted to make the
replacement recover. Kubernetes recreates the volume/pod; application recovery
must restore its contents while traffic continues.

Worker return has a separate 120-second test bound, with the transient timer's
`AccuracySec=1s` explicitly set; node Ready/NotReady transitions are retained.
Existing event identity and complete contents must match before any write retry.
Only new/ambiguous fault-window writes are retried afterward; old acknowledged
data is never re-imported to make a recovery assertion pass.

### Original VPS database recovery

`04-live-recovery` is destructive and must run only with explicit authorization
in a dedicated maintenance window. Inspect the actual pod placement first:
`postgres-a` is a unit name, not a guarantee that it runs on node A. The selected
claim must currently belong to the selected **worker**, never the control plane.

```sh
python3 test/p1/04-live-recovery.py --vps --allow-changes \
  --worker aat-vps-b --claim data-postgres-a-0
```

The script captures a physical manifest for every canonical row, including
tombstones, ingestion version, revision and a full-row MD5 fingerprint. Before
deletion it requires two intact/equal-or-newer copies of every baseline event.
The manifest has a SHA-256 summary; it is not the assistant's reference checksum
and is not a backup of database contents. Comparison allows only legitimately
newer versions while the real feeds continue to run.
The three captures are not an atomic distributed snapshot. For versions that
changed during capture, a separate 120-second safety gate rechecks only the
affected identities against the **unchanged** baseline version and fingerprint.
It never lowers a baseline version or deletes a volume with an unverified row.

One live PVC is deleted through Kubernetes and the exact old node-local data
directory must disappear. A systemd timer restarts the worker's k3s-agent after
70 seconds. OS, SSH, unrelated data volumes and Tester Agent configuration are
retained. The test checks surviving copies, recreates the StatefulSet replica on
an empty PVC and observes automatic application repair without SQL data repair
or re-importing old events. New, clearly labelled practice records are written
during recovery; only those new ambiguous inputs are retried afterward.

The default repair observation budget is 1,200 seconds for the existing live
dataset, distinct from the worker's 70-second automatic restart and the sampled
five-second API response bound. The test preserves API failures independently
of data recovery and fails overall if any sampled API does not meet that bound.
Its SQLite comparison ledger performs index I/O in a temporary local database,
then writes an evidence copy; it does not create tables in application storage.
An independent observer uses its own SSH tunnel, authentication state and result
directory. It continuously samples event, Audit and analytical reads throughout
volume deletion, worker interruption, manifest comparison and empty-disk repair;
blocking administrative commands do not suspend that sampling. Each sample
retains UTC start/end times, correlation ID, latency and complete/partial status.
Physical COPY captures use independent, bounded SSH channels rather than leaving
an abandoned capture attached to a persistent SSH master.
Write observation continues after a rejected or ambiguous POST. HTTP 200 counts
as an acknowledgement only with `accepted=1, failed=0`. A 503 with per-item
failure counts or the gateway's `backend_unavailable` error is not an
acknowledgement: the input may already have committed on one destination. Other
responses are retained as failures, but do not stop read observation or worker
restoration. Only new fault-window identities are retried after the original
baseline has regained two physical copies.
No original application volume is deleted unless all pre-destruction safety
checks pass. An interrupted preflight is not a successful fault/recovery run.

If local evidence collection was interrupted after the volume was replaced,
verify the retained baseline without repeating a destructive action:

```sh
python3 test/p1/04-live-recovery.py --vps \
  --worker aat-vps-b --claim data-postgres-a-0 \
  --verify-run .e2e/report-tests/p1/04-live-recovery/<original-run>
```

This mode reconstructs the original baseline and requires its count and SHA-256
to match the original manifest summary before comparing all current physical
copies. It also verifies the new PVC identity and preservation of unrelated
volumes and Tester Agent identity. It performs no cluster mutation. It cannot
retroactively prove every response during a missing observation window; the
original window remains inconclusive even when final data recovery is proved.
Allow sufficient free disk space for manifests, the SQLite comparison and its
evidence copy; the live scenario requires at least 4 GiB free before proceeding.
A larger dataset may require more. A local disk I/O failure is an observer failure, not evidence of
application data loss or a passing recovery window.

The assistant catalog/checksum comparison stays separate until those resources
are released. Practice success is not labelled an assistant-checksum pass.

After every campaign has cleaned up, run `p1/00-preflight.py --vps --baseline
<earlier-baseline.json>` to compare the original PVC UIDs and Tester Agent UID,
and verify no temporary applications or volume claims remain. The baseline is
created by the initial preflight under its private evidence directory. The
client test harness renews tokens using `expires_in` before they expire; raw
unexpected 401 responses and the subsequent reauthentication remain recorded.

### P3 sequence

```sh
python3 test/p3/00-preflight.py --vps
python3 test/p3/01-schema.py --vps --allow-changes
python3 test/p3/02-poison.py --vps --allow-changes
python3 test/p3/03-ops-access.py --vps
python3 test/p3/04-replay.py --vps --allow-changes
python3 test/p3/05-delivery.py --vps --allow-changes --count 20 --repeat 3
```

`01-schema.py` demonstrates the real deployed PVMBG mock. `04-replay.py` creates
its own incompatible revisions, so it does not require pre-existing failures.
`02-poison.py` supplies a parked raw message for inspection in `03-ops-access.py`.
Every poison round also retains new upstream request logs, so an old archived
body alone cannot establish that the repeated exposure occurred. Identical
poison bodies intentionally share one durable message identity.
Missing prerequisites produce an inconclusive result, not fabricated samples.
Modes return to the documented `legacy` baseline in `finally`; the original
adapter setting is restored even after a failed check. Reports and archived
messages remain as evidence, not deleted as cleanup. An interruption preventing
cleanup (such as SIGKILL) may require manual mock/configuration restoration.

### P4 sequence

```sh
python3 test/p4/00-preflight.py --vps
python3 test/p4/01-live-stream.py --vps --seconds 180 --interval 5
python3 test/p4/02-recovery.py --vps --allow-changes --repeat 3
python3 test/p4/03-audit.py --vps
python3 test/p4/04-dashboard.py --vps --repeat 3
```

The official feed remains a black box: live tests never select its seed, IDs,
offsets or profile. The separate recovery scenario uses our explicitly labeled
controlled source and independent Python oracle, not an assistant reference.
Application images are unchanged between the baseline and fault runs and have
no fixture-specific logic. URL/token remain replaceable configuration.
No scenario starts an exam, resets the official session, modifies Tester Agent,
deletes a PVC, or changes node networking/clocks.

### Isolated VPS E2E workflows

`p3/04`, `p3/05` and `p4/02` require `--vps --allow-changes`. The laptop drives
HTTP requests through SSH; application processing runs on three VPS nodes.
Each workflow creates three Aggregator replicas, one Client API, one controlled
source, four uniquely named temporary databases in the existing owner PostgreSQL
servers, and a dedicated Kafka topic. Real Auth supplies signed role tokens.
Resource memory limits are unchanged. A narrowly scoped, temporary NetworkPolicy
allows only these receivers to access storage; their labels never match the
production Aggregator Service. Production databases, PVCs and feed state are not
reset, copied or edited. Cleanup removes only this workflow's labeled resources,
verified temporary database names and topic. Failed results retain pod logs.

Run **one isolated campaign at a time**. Sharing database servers means concurrent
copies can exhaust their connection ceiling; the harness rejects an existing
campaign instead of increasing resource limits. An external SIGKILL or laptop
shutdown may require cleanup of the recorded `aat-e2e-<id>` resources.

`p3/05` holds an exclusive lock on its own result table while requests still
receive 202 in under two seconds. It verifies retryable reasons, force-kills all
three receivers, releases the lock, reconciles every acknowledged ID, retries
the same bodies and rejects conflicting bodies. Default: 60 reports/three crashes.

`p4/02` defaults to three paired runs of 1,772 frames each. Inputs include
cross-satellite transitive clusters, duplicate detections, in-window out-of-order
arrivals, too-late records, low confidence, singletons, updates, zero FRP,
retractions and sequenced heartbeats. It validates every final component, count,
key, centroid, event time, confidence, FRP and severity against an independently
computed oracle, then compares all canonical fields/identities between runs.
Only arrival-derived `ingested_at` is excluded from baseline/fault comparison.

The controlled burst compresses a 60 ms base emission interval by 60 to 1 ms.
Its production clock advances independently of socket reads and reconnects;
the 60-second catch-up timer starts when the final input becomes **available**,
not when a slow receiver finally reads it. Fault runs sever SSE every 137 frames
and kill the lease owner during recognition. No frame-count/rate reduction is
used to turn a failed run into a pass. This stress test supplements, rather than
renames, the official practice-feed observation.

The application p95 includes the bounded receiver queue and canonical target
acknowledgements. Controlled-source runs also report availability-to-canonical
p95 separately, derived from the producer schedule and actual canonical target
acknowledgement times. This second measurement includes source backlog; neither
measurement is a source-to-notifier P5 result.

## Evidence and interpretation

Each run creates `.e2e/report-tests/<problem>/<case>/<UTC-run-id>/` containing
`result.json`, deployment images/placement, HTTP response artifacts and scenario
observations. Requests use cURL `-w`/`--write-out` with `%{time_total}` and retain
HTTP status, elapsed seconds and correlation ID. Authorization headers and token
responses are not retained. Failed-message artifacts may contain sensitive input:
keep this ignored directory private and redact report excerpts.

Exit codes: **0** = checks completed (inspect `passed` versus `observed` in the
result), **1** = failure, **2** = inconclusive. Keep failed evidence too. A criteria
list identifies relevance, not a claim that an entire criterion passed.

The strict isolated workflows assert ordering, loss/duplicate counts, full-state
equivalence and the catch-up bound directly. Live observation records real
backlog separately; checkpoint progress or processing p95 alone never substitutes
for those assertions. `04-dashboard` verifies the same query used by the live
dashboard, rather than merely checking that a metrics endpoint exists.

## After the deadline

Assistant reference datasets, exam decryption keys, special audit credentials and
final exam/reference comparisons are **deferred to the post-deadline window**.
They are not prerequisites or current blockers for this practice suite. Do not
invent replacement assistant data or reverse-engineer hidden scenarios. When the
resources arrive, use their documented contract and capture the real comparison
separately; neither these scripts nor the application may depend on the practice
feed's particular data pattern.
