# Report-facing system tests

`p1/` through `p5/` contain numbered report-facing E2E scenarios, not unit tests.
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

### Tester Agent practice

Run from the laptop against the VPS cluster, sequentially:

```sh
python3 test/tester-agent/00-preflight.py --vps
python3 test/tester-agent/01-kill-leader.py --vps --allow-changes --repeat 3
python3 test/tester-agent/02-polling-failover.py --vps --allow-changes --repeat 3
python3 test/tester-agent/03-schema.py --vps --allow-changes --repeat 3
python3 test/tester-agent/04-kill-store.py --vps --allow-changes --repeat 3
python3 test/tester-agent/05-clock-skew.py --vps --allow-changes --repeat 3
python3 test/tester-agent/06-partition.py --vps --allow-changes --repeat 3
```

Preflight verifies the official image digest, one Ready Agent per node and
practice discovery. `01-kill-leader` submits the official practice request,
waits for an actual Aggregator pod identity change, records Agent logs and
entrypoint timings, then waits for all Aggregator replicas to become Ready.
The CLI only queues work: exit zero is not evidence that a fault happened.
No manual repair occurs during the recovery observation window.

`02-polling-failover` supplements the official scenario: it resolves the actual
BMKG (or `--source PVMBG`) polling owner and force-deletes that exact Aggregator
pod. It is **not** labelled an Agent-issued fault. For every affected mock it
requires a completed poll by a new, higher-fenced owner within 60 seconds,
independently enumerates real source records generated in the window, compares
recent complete Audit API pages, checks one admission per immutable record in
that window, and reconciles upstream request logs with the polling ledger and
configured cadence. Field reads must also remain successful. Source mock IDs
can be reused across mock restarts: historical versions from an earlier mock
lifetime are not duplicate admissions in the current fault window.

`03-schema` submits the official schema practice and verifies an actual admin
call, new processed reports in the active compatible schema, and no application
restart. It establishes a legacy mock baseline before the fault and restores
the prior mode after the campaign, not during the recovery window. These are
practice-integration assertions, not the entire incompatible-schema/replay P3
acceptance suite. Unknown exam profiles remain deferred.

PVMBG exposes a separate control listener only when
`PVMBG_ADMIN_LISTEN_ADDRESS` is set. Kubernetes configuration enables `:8089`
and advertises `pvmbg-admin` with `role=mock` for Agent discovery. Its
NetworkPolicy admits Tester Agent namespace traffic to that port, but not
ordinary application pods; node/cluster administrators retain their inherent
privileges. The normal `pvmbg:8082` data and admin APIs still require their
independent Bearer credential. BMKG credentials remain invalid there. Only
PVMBG provides this schema-control API, so BMKG's data Service is not advertised
as an administrative target. No Agent image, privileges or behavior is changed.
Explicit schema `version` values `1`, `2`, `3` (also string forms and `v1`–`v3`)
select legacy, additive and incompatible rename modes idempotently. Existing
`mode` controls and the empty-body toggle remain supported; ambiguous or unknown
versions are rejected.

The `01` case proves Agent execution and pod/readiness recovery only. It does not prove
that the killed pod owned a particular source, the ingestion takeover SLO,
exactly-once polling, or data-loss resistance. Those require separate assertions.
Cases `04`–`06` observe actual Agent actions and cleanup, independent node
clocks/firewall snapshots, and complete or explicitly partial API reads until
previously Ready applications and complete pages recover stably. Clock tests
do not by themselves prove every P2 fencing requirement.

`wipe-node` permanently deletes node-local application data. It requires explicit
operator approval, a separate practice window and a verified fresh backup:

```sh
python3 test/tester-agent/07-backup.py --vps --allow-changes
python3 test/tester-agent/08-wipe-node.py --vps --allow-changes --backup-manifest .e2e/report-tests/tester-agent/07-backup/<run>/backup-manifest.json
```

Backup streams go directly through SSH to the external generator, outside all
application nodes. PostgreSQL dumps use the observed Ready container through
the owning node's local `crictl exec`, avoiding long data streams through the
API-server/kubelet WebSocket proxy. Archives are logical transaction-consistent
per database, not a global atomic snapshot. Metadata includes DaemonSets as
well as Deployments, StatefulSets and volume definitions. An interrupted or
incomplete manifest is never accepted for `wipe-node`.

Kafka and bbolt volumes are archived while their owning StatefulSet is stopped,
then restored to its prior replica count. This maintenance is **before** the
fault, not a recovery intervention. Kubernetes metadata, database globals and
credentials are private backup material, never repository content. The backup
stays private. `08` rechecks every full-file
SHA-256, volume identity and backup age before submitting the official scenario.
It does not manually delete OS disks or restore backup data during observation.
Known canonical identities are compared before/after; live SATELIT records are
excluded from that specific preservation comparison because fire retractions
are legitimate. This is not the later assistant-reference checksum test.

The shared practice runner removes only its own exact persisted practice request
after the measurement window, using resource-version and value checks. For
`wipe-node`, the destructive stimulus ends when the target powers off: the runner
retires its request then, before planned operator reintroduction. Keep the VPS
off until the runner names it and confirms trigger removal; press **Start**, not
Rebuild. Application reads continue to be observed across shutdown and reboot.
Provider power-on is a planned P1 reintroduction, not automatic VM recovery or
proof of P5 combined-fault acceptance. This
prevents a completed `wipe-node` trigger replaying when a node boots. The private
pre-cleanup queue snapshot is retained; Agent specifications, privileges and
other operators' requests are never edited. Do not start another campaign while
a request is still being observed.

Read-path recovery and rolling availability have independent workflows:

```sh
python3 test/tester-agent/09-gateway-failover.py --vps --allow-changes --client-image-tag <imported-client-tag> --repeat 3
python3 test/tester-agent/10-audit-recovery.py --vps --baseline <private-before-identities.txt.gz> --repeat 3
python3 test/tester-agent/11-api-rollout.py --vps --allow-changes --repeat 3
```

Source-owner fencing has a separate isolated VPS workflow. Cross-compile the
service-local Go store tests to a fresh Linux/amd64 binary, then run:

```sh
python3 test/tester-agent/12-fencing.py --vps --allow-changes --repeat 3 --store-test-binary .e2e/fencing-store.test
```

It pauses the actual run-owned polling process beyond its lease lifetime,
observes takeover and fresh admissions while that process remains frozen, then
resumes the same process. A worker clock is also held at +45/-30 seconds with
NTP disabled and restored afterward. Private upstream request records, durable
admissions and service-local negative fencing regressions are checked together.
Availability failures remain failures even when ownership safety is demonstrated.
The isolated read path mirrors the production per-node gateway and
`PreferSameNode` Service rather than bypassing the L7 router. An independent
Field observer runs throughout the complete window, including takeover and
resume waits; its requests are not retried into a passing measurement.
All temporary databases, workloads and source fixtures are run-owned; neither the
vendor Sensor Pantai nor official Agent is modified. This freezes a process, not
the whole node kernel: P2.5's node/VM suspend-resume demonstration remains a
separate requirement. Powering off and booting a new process does not substitute
for resuming the old process after a VM freeze.

For an overlapping official partition and priority-load window:

```sh
python3 test/tester-agent/13-partition-load.py --vps --allow-changes --repeat 3
```

The existing laptop k6 workload runs for 120 seconds while the real Agent
partitions peers. Read errors and the workload's original role/error thresholds
are retained and fail the run. This is concurrent network/load evidence, not a
substitute for critical-alert delivery measurements or the encrypted exam.

`09` freezes a run-owned gateway's existing keep-alive connection and listener,
then reads real canonical data through a candidate Client API and the healthy
peer. Temporary workloads are removed; production gateways, databases and
Sensor Pantai are unchanged. Endpoint convergence after an intentional temporary
API restart is checked between rounds, never retried inside the fault request.
`10` is read-only: every page must be complete, pagination must terminate, and
every supplied pre-fault identity must remain visible. Failed/partial pages are
retained; the test does not retry them into a passing result. `11` rolls only the
production Client API while independently sampling Field reads without retrying
transport failures. It does not delete data or change replica counts.

Exam scenarios/keys are not inspected or required here.

P5 critical-path verification has resumed. The two staged database clusters already exist
in `sistem-informasi-kebencanaan-bnpb`, but production applications have not
been switched to them. Kafka expansion was stopped at one Ready broker;
the completed Kafka 4.1.2/dynamic-quorum upgrade was retained, not downgraded.
Legacy volumes and offsets were not deleted or reset. This intermediate state
is not a completed HA rollout and should be recorded with fault-test evidence.

### General prerequisites

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

### P2 capacity and polling

For the provisioned external generator, use the same controller with:

```sh
python3 test/p2/01-capacity.py --vps --generator-vps --allow-changes --mode scale --vus 10000 --seconds 120 --repeat 1 --max-rss-mib 6144
python3 test/p2/01-capacity.py --vps --generator-vps --allow-changes --mode throughput --vus 1000 --seconds 60 --repeat 2 --max-rss-mib 6144
python3 test/p2/02-network.py --vps --allow-changes --repeat 3
python3 test/p2/03-profile.py --vps --allow-changes --repeat 1
python3 test/p2/04-generator.py --vps --allow-changes --repeat 1
```

These run k6 as the separate VPS's SSH user, outside Kubernetes, directly over
WireGuard. Linux-only generator monitoring replaces the laptop memory guard;
the laptop still controls scaling, collects upstream evidence and restores the
original replica count. The API key travels over SSH stdin and is not put in
command arguments, copied `.env` files or result artifacts. Each remote worker
has its own deadline and private working directory. This option supersedes the
SSH-forwarded laptop path below for high-concurrency verification.
`--reverse-first --repeat 1` measures the counterbalancing 3→1 pair separately;
completed case summaries and the initial polling boundary are saved immediately
so an interrupted controller does not erase earlier evidence.

`03-profile` captures a 20-second CPU profile from k6 and each Ready gateway and
Aggregator during the unchanged 1,000-VU, 60-second workload. App profiling is
temporarily enabled on pod loopback only and restored in `finally`; k6 profiling
binds only the external generator's loopback. This requires two scoped app
rollouts to enable/restore profiling, but does not change images, budgets, data
or replica counts. Profiles are private diagnostic evidence, not acceptance
measurements. Inspect them with `go tool pprof -top <profile>`.
The external worker defaults to `--generator-procs 6 --generator-gogc 100`.
Runtime experiments may set these explicitly (for example, `8` and `300` on
the 8-vCPU generator), identically for both replica counts. The settings and
peak RSS are retained; memory safety stops and full JSON validation still
apply. Label runtime experiments separately from application design changes.

`04-generator` cross-builds a small Linux HTTP fixture using Windows Go (run
the Python controller in WSL), snapshots the exact complete 20-event response,
and serves that snapshot from a temporary authenticated listener on A's private
WireGuard IP. It uses the same k6 workload and full-body checks for 60 seconds.
The fixture exits after at most 95 seconds, is stopped in `finally` when its
process identity still matches, and never replaces a Kubernetes service.
This calibrates the generator plus private network/fixture path; it does not
isolate generator CPU from network capacity and is **not application throughput
or P2 acceptance**. No credentials are written into the fixture or its files.

`02-network` measures an eight-second single-TCP-flow transfer from A to the
generator over WireGuard, only when no k6 is active. Its temporary listener binds
only A's private address, closes after each round and has a 25-second watchdog;
no public listener or persistent firewall change is created. This is transport
diagnosis, not a P2 pass or proof of a provider's bandwidth cap. Run it separately
from application load testing.

Run sequentially from the laptop against the VPS lab:

```sh
python3 test/tester-agent/02-polling-failover.py --vps --allow-changes --repeat 3
python3 test/p2/01-capacity.py --vps --allow-changes --mode throughput --vus 1000 --seconds 60 --repeat 2
python3 test/p2/01-capacity.py --vps --allow-changes --mode scale --vus 10000 --seconds 120 --repeat 1
```

Capacity uses the `scripts/m2/p2/` k6 workloads through four private SSH tunnels
to the **same** gateway NodePort (`--tunnels` changes only the transport count).
`AGGREGATOR_BASE_URLS` assigns VUs evenly across these local tunnel endpoints;
it does not route directly to pods or change requests, pacing or thresholds.
This distributes channels across processes, but does not guarantee SSH can
carry 10,000 VUs. A tunnel exit invalidates the campaign; use a direct private
VPN route for a conclusive high-concurrency measurement if SSH saturates.
Throughput alternates 1→3 and 3→1, with 10 seconds of
stabilization and identical VUs, payload and duration. Only complete responses
count toward the ≥2× ratio; 429, partial and malformed results do not. Scaling
holds 10,000 VUs for 120 seconds, requests 3→5→2 replicas and requires both changed
counts to become Ready during load. The original replica count is restored
afterward, including on failure. No volume, source state or image is reset.

The runner retains k6 summaries, server/node resource samples, replica timelines
and correlated mock polling evidence. Laptop memory safety uses the existing
Linux/Windows guard; a safety stop is a failed/incomplete campaign, never a pass.
Its own file-descriptor limit is raised to 65,536 before opening the SSH tunnel;
this does not change machine-wide limits, application budgets or the workload.
SSH transport, laptop capacity and WAN remain part of this measurement path;
this is not an isolated server-capacity benchmark. These tests do not change
node clocks, freeze VMs or prove a new-version rolling update, and cannot alone
establish all P2 criteria.

### P5 sequence

Run from the laptop (WSL for VPS access), with the lab already deployed:

```sh
python3 test/p5/00-health.py --vps --repeat 3 --seconds 60
python3 test/p5/01-critical.py --vps --repeat 3 --seconds 60
python3 test/p5/02-load.py --vps --mode isolation --repeat 3
python3 test/p5/02-load.py --vps --mode priority --repeat 3
python3 test/p5/03-recovery.py --vps --allow-changes --repeat 3
python3 test/p5/04-circuit.py --vps --repeat 3 --seconds 30
python3 test/p5/05-observability.py --vps --repeat 3
python3 test/p5/05-observability.py --vps --repeat 3 --under-load
```

- `00-health`: concurrent health, statistics and metrics requests; requires a
  Ready notifier before the window and rejects restarts or unbounded responses.
- `01-critical`: genuine Sensor Pantai observation from the current persisted
  cursor, per-ID notifier durability confirmation, and same-observer monotonic
  conservative latency bounds. Only an empty unfiltered page establishes the
  prior availability bound. No new critical samples or incomplete source
  coverage produces **inconclusive**, not a fabricated pass. Historical alerts
  without an availability bound are not latency samples.
- `02-load`: k6 executes on the laptop. Isolation uses 25 BMKG and 25 PVMBG VUs
  for 60 seconds, requires complete nonempty source-filtered responses, less
  than 1% errors and BMKG p95 below 300 ms. Priority uses 300 Media VUs plus
  10 Field requests/second, requiring zero Field failures, some controlled
  Media 429s and no dropped iterations. These are P5 checks, not a replacement
  for P2's 10,000-VU capacity benchmark. Credentials remain in process
  environment; per-round summaries and logs stay in the private evidence folder.
- `03-recovery`: clones the deployed notifier into one run-owned StatefulSet,
  topic and PVC. Publishes duplicated identities, kills only that notifier's
  PID 1 and reconciles every identity after autonomous restart. Original volumes,
  consumer group and messages are retained. Cleanup permanently removes only
  the temporary topic/PVC. This proves the Kafka-to-notifier component, **not**
  source-availability latency or a full multi-component game day.
- `04-circuit`: reads actual Aggregator upstream-request logs and samples Field
  reads while the genuine source is unhealthy. Counts are compared with the
  configured cooldown/polling bound. It does not manufacture source recovery;
  outage-phase success alone does not prove automatic recovery after a real
  source return.
- `05-observability`: verifies actual dashboard HTML, live Prometheus series,
  notifier scrape health and a correlation ID propagated across Client API and
  gateway/Aggregator. `--under-load` starts the unchanged 120-second priority k6
  workload on the laptop, waits for it to become active and retains both trace
  and load evidence in one campaign. Each trace lookup must overlap the active
  load; both trace coverage and workload thresholds must pass. This checks
  data/traces, not a new visual-browser inspection. The explorer remains a
  bounded log-tail query, not durable distributed-trace retention.

Each script runs all requested rounds before reporting failed assertions where
safe. Exit status 0 means that scenario passed, 1 failed, and 2 inconclusive.
None starts the encrypted exam, resets source state, changes the official image
or relaxes resource/latency limits. Check source condition and current image tags
in the recorded deployment snapshots when comparing campaigns.

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

### P5 candidate resilience

Run these from the repository root on the laptop/WSL. Candidate images must be
imported on all three VPS nodes first. These cases create run-owned workloads,
topics and isolated databases, then remove only those temporary resources.
The production Sensor Pantai image, state and cursor are never reset.

```bash
python3 test/p5/06-resilience.py --vps --allow-changes \
  --image-tag <aggregator-tag> --notifier-image-tag <notifier-tag> --repeat 3
python3 test/p5/07-source-age.py --vps --allow-changes \
  --image-tag <aggregator-tag> --notifier-image-tag <notifier-tag> --repeat 3 --seconds 60
python3 test/p5/08-critical-lane.py --vps --allow-changes \
  --image-tag <aggregator-tag> --notifier-image-tag <notifier-tag> --repeat 3
python3 test/p5/09-notifier-failover.py --vps --allow-changes \
  --image-tag <aggregator-tag> --notifier-image-tag <notifier-tag> --repeat 3
python3 test/p5/10-contract.py --vps --allow-changes \
  --image-tag <aggregator-tag> --notifier-image-tag <notifier-tag> --repeat 3
python3 test/p5/03-recovery.py --vps --allow-changes \
  --notifier-image-tag <notifier-tag> --repeat 3
```

`06-resilience` varies fault order, injects critical observations during faults,
and exercises old/future source timestamp origins, including clock regression.
Timestamp origin is not equivalent to aging the official sensor process.
It measures each phase separately from fixture availability to durable notifier
delivery, including recovery waiting time. Seed selects test fault scheduling
only; application behavior must never depend on it.

`07-source-age` starts an unchanged fresh official practice image and observes
the existing long-running official source read-only. It does not write synthetic
clock state, assume a source speed, decrypt exam data, or reset production. If
critical coverage/availability timing is insufficient, the result is inconclusive.
The independent observer drains at most three successful pages per sample
until it sees an actual empty page; a short page is never assumed to be the
tail. It stops on source errors rather than retrying them. This establishes
an availability bound without assuming sensor speed or subtracting virtual
timestamps from wall time. A latency upper bound above five seconds alone is
inconclusive, not proof that the actual latency exceeded five seconds.

`08-critical-lane` requires a measured ordinary Kafka backlog to remain when
critical delivery completes. A fast response without an actual backlog is not
proof of isolation. `03-recovery` checks crash durability; its elapsed recovery
and latency statistics must still be compared against the five-second SLO.

`09-notifier-failover` requires the prepared `notifier-ha` database cluster.
It creates a private run-owned database, three stable notifier replicas on
different nodes and a matching owner-only network policy. Each replica has an
independent critical consumer group; all share one durable notification store.
The test warms every reader, removes one notifier pod per round, and checks
new critical deliveries, exact durable row counts and unchanged survivors.
Replacement readers resume their committed offsets; the warm-up check uses
the latest crash-window records rather than demanding replay of old committed
priming records. Only temporary test workloads/databases/topics are removed.
This does not switch the production notifier from bbolt to PostgreSQL, and
does not establish Kafka/coordinator or node-loss HA.

`10-contract` mixes seven invalid field/identity/time variants with valid
neighbors per round. It compares the complete canonical Audit API against
independently expected valid IDs, then verifies every invalid payload remains
intact with reason, time and a non-retryable parked status. It does not infer
canonical validation from notification counts alone.

Both Kafka lanes preserve the general event stream. Critical events additionally
use a separately acknowledged projection and topic (`KAFKA_CRITICAL_TOPIC`) with
an independent consumer group. Notifier domain deduplication spans both topics.

VPS load runs retain `round-<n>-field-path.log` while k6 is active, filtered to
Field correlation IDs (including Auth issuance/refresh) and gateway
discovery/probe failures. This avoids waiting
until a high-volume container log has rotated before diagnosing a Field failure.
Non-200 client diagnostics include the response error, never authorization
headers. Field authentication attempts also retain their own correlation ID,
status and elapsed time. Each selected pod stream must subscribe before k6
starts; its initial old tail record is discarded. The stream drains briefly
after k6 finishes, is
read-only and time-bounded; business thresholds remain
unchanged.

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

## External fault observation

For official fault cases, `--vps --ingress-vps` originates API TCP connections
on the inventory's separate `load_generator` VPS, then forwards responses to
the local runner. The application target remains A's same NodePorts; kubectl,
mock administration and fault control remain separate SSH control channels.
Without `--ingress-vps`, API connections originate on A itself. Record these
as different network paths: Kubernetes host-originated NodePort routing can
select remote endpoints even with `externalTrafficPolicy: Local`. Do not erase
earlier failures or present one path's pass as proof for the other.

```sh
python3 test/tester-agent/06-partition.py --vps --ingress-vps --allow-changes --repeat 3
```

## After the deadline

Assistant reference datasets, exam decryption keys, special audit credentials and
final exam/reference comparisons are **deferred to the post-deadline window**.
They are not prerequisites or current blockers for this practice suite. Do not
invent replacement assistant data or reverse-engineer hidden scenarios. When the
resources arrive, use their documented contract and capture the real comparison
separately; neither these scripts nor the application may depend on the practice
feed's particular data pattern.
