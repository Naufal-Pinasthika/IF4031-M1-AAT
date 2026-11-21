# P1 practice verification

Branch `test-p1`, based on main `e430b2d`. The initial runs used the P3/P4 r8
candidate recorded in [status](../status/README.md). A later bounded canonical
write retry with server-side deadlines is tested as `m2-p1-e430b2d-20261121-r2`, built from this branch's
uncommitted worktree, tested in isolation, then rolled out to all three main
Aggregators after the tests passed. Other workload images were unchanged. New
report-facing scripts are in [`test/p1`](../../test/p1); unit/integration tests
remain with their services. This is practice evidence, not an assistant-dataset
or assistant-checksum result.

## Test boundary

The laptop drives cURL through private SSH tunnels into the VPS cluster. Each
isolated campaign creates three node-local PostgreSQL units using the deployed
StatefulSet image, bootstrap script and 512Mi/1Gi memory settings. Three
Aggregators use 256Mi/512Mi and run on different nodes. A private two-replica
gateway supplies one entrypoint, and Client API forwards to that gateway.
During isolated tests, original canonical volumes, source configuration and
application images are not replaced. The coordinator receives only a temporary database; Kafka receives a
temporary topic. Temporary resources have run-specific names and ownership
labels, and are removed after the campaign.

Disk-loss tests delete a verified, run-owned PVC and recreate it with a new UID.
The application must repopulate that empty disk without a manual SQL copy or
Aggregator restart. During replacement, cURL continues reading, querying analytics
and writing new canonical events. Previously acknowledged data is checked in full
before any retry: re-importing the old dataset is prohibited. New ambiguous
writes may be retried with unchanged identities after recovery.

The importer returns success only for fully replicated writes. A missing
destination can produce HTTP 503 plus accepted/failed counts for imports; the
surviving destination may already have committed. That is not a successful
acknowledgement, nor a lost acknowledged record. Read and analytics requests must
remain HTTP 200 within the declared five-second test bound and identify partial
results. The five-second bound is a test assertion, not a new P1 specification
requirement.

## Recorded results

| Scenario | Result | Evidence directory under `.e2e/report-tests/` |
| --- | --- | --- |
| P1 preflight | Three ready node-local units on separate nodes; storage resource settings matched | `p1/00-preflight/20261121T145728Z-442b247d` |
| Initial sharding smoke | 180 practice events; primary counts 68/57/55 after a concentrated regional burst; two copies of each identity; checksums and locations matched | `p1/01-sharding/20261121T145731Z-17805513` |
| Global query/analytics | Three repetitions over 600 events; every 50-item page globally ordered; hazard/severity/area/daily totals and UTC range filtering matched independent Audit API counting | `p1/02-queries/20261121T145949Z-c424762c` |
| Repeated empty-disk replacement | B → C → B; 600 initial records and 200 concurrent new records; every round restored two physical copies, full contents and checksums; old data was not re-imported | `p1/03-recovery/20261121T151032Z-d01658ba` |
| Worker interruption plus empty disk | Worker B reached Ready → Unknown → Ready without manual restart; 600 initial plus 126 new records; all original contents and two copies restored before retrying new writes | `p1/03-recovery/20261121T151600Z-1a46bf78` |
| Corrected r2 sharding/load | Three catalog/burst pairs, 1,800 records, primary counts 605/597/598, exactly two copies of each record; all 119 HTTP requests returned 200, maximum 0.660 seconds | `p1/01-sharding/20261121T153317Z-a31e4ee3` |
| Corrected r2 queries/analytics | Three complete pagination/aggregate/range-filter comparisons over 600 events behind the isolated L7 gateway passed | `p1/02-queries/20261121T153523Z-673513b2` |
| Corrected r2 repeated recovery | B → C → B; original records restored before retries; 661/710/769 final records across rounds, all with two copies; 276 read/analytics requests returned 200, maximum 0.576s | `p1/03-recovery/20261121T153719Z-f5ce6016` |
| Post-rollout retention/preflight | Three original storage units ready, all original PVC UIDs and Tester Agent UID retained; no temporary applications or claims remained | `p1/00-preflight/20261121T154254Z-4617e57a` |

The initial sharding and query runs preceded adding the isolated L7 gateway;
their common entrypoint was a Kubernetes Service. Later recovery uses the L7
gateway. Do not present them as the same topology or claim a later script
revision was already run. The corrected 1,800-record run and final query run
used the isolated L7 gateway and are recorded separately from the initial runs.

The successful three-round disk run exercised 40, 28 and 36 read/analytics
requests in its explicitly counted observation/replacement windows, plus reads
during graceful storage shutdown and audit/checksum verification. All operations
succeeded; the raw log includes three HTTP 401 responses on expiring client
tokens, followed by normal reauthentication and successful retries. Across the
290 raw read/analytics requests, the maximum response time was 0.566 seconds;
none returned 500. The harness now renews using the token's declared lifetime
before expiration, while retaining its fallback for unexpected 401 responses.
It restored 680, 730 and 800 input records respectively. It proves repeated
temporary-unit disk loss, not deletion of an entire VPS disk.

The worker run had 208 raw read/analytics requests, maximum 0.564 seconds, with
two expected token-expiry 401 responses followed by successful reauthentication.
Its explicitly counted observation/replacement window had 140 read/analytics
operations. The guest OS and SSH remained running: the injected fault stopped
k3s-agent and killed its containers, rather than powering off the VM. Only its
run-owned canonical volume was erased. Original application PVCs were retained.

## Canonical write correction

The larger r8 sharding run failed after the first 300 records, at
`p1/01-sharding/20261121T151959Z-bf0757be`. The failing batch returned
`accepted=19, failed=1`: one store-c transaction exceeded its unchanged 500 ms
I/O deadline. Correlated logs show the write batch sent successfully and the
following database round trip timed out; the failed acknowledgement therefore
cannot be assumed to mean the data was not committed. CPU samples were low
(about 12%/4%/6% across A/B/C in the preceding sample), not evidence of uniformly
saturated application nodes.

The corrected single-record canonical writer retries a transient failure once,
after a 20 ms delay, preserving the identical identity, ingestion time, revision
and admitted version. The existing SQL version guard prevents an ambiguous
first commit from becoming an additional update on retry. Each attempt keeps
the existing storage I/O deadline; caller cancellation terminates the retry.
Constraint/schema/permission errors are not retried. Destination acknowledgement
still requires an actual successful result; missing replicas are not counted as
successes. The bulk repair loop and read-path budgets are unchanged.

`write_retry_integration_test.go` reproduces the missing retry with a temporary
row lock and verifies that both replicas retain exactly one row at revision 2.
The unchanged failing assertion is retained in `.e2e/p1-write-before.log`. The
corrected case and caller-cancellation case passed five times each in
`.e2e/p1-write-after.log`. Classification tests live beside the store service.
The E2E importer does not retry healthy-run failed batches to hide this failure;
the same larger sharding command was rerun against the corrected image.

The intermediate r1 candidate still failed the same healthy-run assertion at
`p1/01-sharding/20261121T152715Z-276a4f0d`; retry alone was insufficient. Logs
showed the second insert waiting on an index tuple after a previous transaction's
commit acknowledgement timed out. This is consistent with locks outliving the
client-side timeout, not proof of CPU saturation or a missing replica. The r2
fix also sets PostgreSQL transaction-local `lock_timeout` to 75% and
`statement_timeout` to 90% of the existing per-attempt I/O budget, plus
`idle_in_transaction_session_timeout` equal to that budget. A disconnected or
suspended client therefore cannot indefinitely retain its transaction locks.
The write retry also recognizes server lock/statement timeouts. No global
database settings or application memory limits were changed.

The server-deadline regression installs a test-only slow trigger in a disposable
schema, verifies that lock and idle deadlines are nonzero, and requires a
PostgreSQL `57014` error before client expiry with zero partial effects. It and
the transient-lock/caller-cancellation cases passed three times each in
`.e2e/p1-write-server-deadlines.log`. The final full suite passed 70 top-level
tests plus 12 classification subtests (82 passing results), zero failures and
zero skips, in `.e2e/p1-service-regression-r2-fixed.log`.

## Failed attempts retained

- `p1/03-recovery/20261121T150318Z-80f96d48`: the scheduled 70-second worker
  restoration was checked after 85 seconds, but the default systemd timer
  accuracy delayed its trigger until approximately 95 seconds. Journal evidence
  confirms automatic restart; the failed campaign remains failed. The injector
  now declares `AccuracySec=1s`, captures node-condition history and checks a
  separate 120-second worker-return bound. Its failure was not repaired by
  changing application state or manually starting the worker.
- `p1/03-recovery/20261121T150755Z-ac3eec28`: an assertion compared checksum
  counts with the pre-fault count despite deliberately concurrent new writes.
  It failed on additional records, not proof of missing old records. The test
  now checks every old identity and complete payload before retry, rather than
  treating a changing total as a fixed snapshot.
- `.e2e/p1-service-regression.log`: first disposable-database run had 64 passing
  tests and two failures, no skips. The TCP fault-proxy test requires an explicit
  port in its upstream URL; the initial operator URL omitted `:5432`, so the
  proxy could not connect. A coalescing check also failed once under concurrent
  activity; five focused reruns passed it without a source/test change. With the
  explicit database port, the complete unchanged store suite passed **66 tests,
  zero failures, zero skips**, recorded in `.e2e/p1-service-regression-r2.log`.
  The isolated regression container and anonymous volume were removed after
  verifying their ownership label. A passing rerun does not erase the earlier
  timing-sensitive coalescing failure or establish an unconditional coalescing
  guarantee; the cache deliberately allows canonical fallback after its budget.

## Original VPS canonical-volume loss

The later authorized run used the original development databases, not the
temporary storage sandbox. Original run:
`.e2e/report-tests/p1/04-live-recovery/20261121T161439Z-1bcb9a94/`.
The target was `data-postgres-a-0` on worker **aat-vps-b**; storage unit names do
not imply the same node letter. Before deletion, a frozen manifest of **558,001**
distinct canonical identities, including tombstones, was checked against two
intact/equal-or-newer copies. Sequential captures initially saw twelve changing
versions only once; a read-only follow-up confirmed their two copies. The test
therefore rechecks changing identities against the unchanged frozen baseline
within 120 seconds, rather than lowering the expected version or ignoring rows.

The original PVC `91d16174-2412-44ff-9952-4623279beeba` and its exact node-local
directory were deleted. Worker k3s/containers were interrupted with a scheduled
70-second automatic restart; the guest OS and SSH were retained. Kubernetes
recorded `NodeNotReady` at **16:17:14 UTC** and `NodeReady` at **16:17:40 UTC**.
The replacement PVC is `01cf950c-b3b5-4b62-8129-7658b8a7b36d`. Application repair
reported `ready` with 372,147 copied rows at the later observation. There was no
manual SQL data repair or re-import of the old baseline. Unrelated application
PVC identities and the official Tester Agent DaemonSet identity were retained.

The original observation window is **inconclusive**, not a passing P1.4/P1.5
request-window result. Its local evidence process failed while laptop C: was
effectively full (about 329 MiB free); a later read-only comparison also raised
`disk I/O error`. Earlier aborted preflights and an indexed-lookup fix are
retained as observer failures, not application data-loss evidence. No additional
fault was injected after disk exhaustion. The injector now uses a bounded,
non-multiplexed SSH call and observes traffic/node state concurrently; this
instrumentation change is not claimed to have had a new destructive passing run.

After the user freed disk space, read-only verification completed the physical
comparison in
`.e2e/report-tests/p1/04-live-recovery/20261121T162853Z-49b6f4fd/`:

- Reconstructed original manifest count and SHA-256 matched the pre-erasure
  summary exactly. Same-version rows require the same full-row MD5 fingerprint;
  only higher ingestion versions/revisions may supersede them during live feed
  progress. This is not the assistant's reference checksum.
- **Zero** baseline identities missing/regressed, and **zero** lacking two
  intact/equal-or-newer physical copies. The three current physical captures
  contained 372,206 / 371,884 / 372,145 rows, including legitimate later writes.
- Three storage pods and three Aggregators were Ready on three distinct nodes.
  Unrelated PVC identities and Tester Agent identity were unchanged.
- Three post-recovery event reads returned 200 in **0.277 / 0.151 / 0.064 s**.
- Global analytics still returned **503 in 5.445 s** (`backend_unavailable`).
  Before the fault, it had already returned 503 in 4.888 s in
  `.e2e/report-tests/p1/live-preflight/20261121T155943Z-7292ee74/`.
  Successful physical recovery does not make this dashboard failure pass.

Read-only gateway diagnosis at
`.e2e/report-tests/p1/analytics-diagnosis/20261121T163555Z-e83571f3/` matched the
analytics request correlation ID to `gateway_upstream_failed` with
`net/http: timeout awaiting response headers`. The gateway's response-header
budget is five seconds. Analytics currently enumerates the whole logical dataset
through the audit-style paginated merge on every request. The observed failure
is therefore an analytics request-budget failure, not evidence that the restored
canonical rows were lost. A scalable aggregate/read model still needs design and
validation; simply increasing the timeout would not establish the required
bounded, correct complete/partial analytical responses.

The readiness diagnosis is separate. At
`.e2e/report-tests/p1/live-inspect/20261121T155748Z-502089ca/`, Sensor Pantai and
notifier both had `OOMKilled`/exit 137 under their 256 MiB limits. Sensor Pantai
continued to crash-loop, reaching restart count 28 in the final health snapshot
`.e2e/report-tests/p1/final-health/20261121T163110Z-e38285bc/`. Notifier temporarily
became Ready, then NotReady again; its health logs show Kafka DNS cancellation
and request timeouts. The statistics/metrics implementation scans and decodes
all stored notifications and sorts percentile arrays per request, a plausible
allocation-pressure contributor; no allocation profile proves it the sole OOM
cause. A transient DNS cancellation alone does not prove the DNS server failed.
Neither service's image, configuration nor memory limit was changed here.

## Analytics request-path correction

The correction keeps canonical storage and the existing authorized Client API;
it does not add Grafana, a Kafka analytics consumer, a TTL cache, or a centralized
copy of all canonical history. Those are separate architectural choices, not a
remedy for the observed five-second gateway timeout by themselves.

The analytical reader now streams only identity, ingestion version, revision,
hazard, severity, area, occurrence time and a boolean retraction flag. Three
partitions are processed concurrently. Each partition merges its two ordered
replicas, selects the latest version once, then applies retraction/time filters
and aggregates. A newer tombstone must suppress an older live replica; an older
in-range occurrence must not override a newer out-of-range occurrence. Only JSON
boolean `true`, not the string `"true"`, is a tombstone, matching Audit semantics.

Read-only repeatable-read server cursors avoid re-sorting the remaining history
on every keyset page when statistics lag after bulk insertion. FETCH batches
contain at most 2,048 narrow records. The existing read slots/pools and operation
deadline still apply; cancellation joins workers, rolls back cursor transactions
and releases admissions. Two covering partial indexes separate live records and
tombstones. Planner preferences are transaction-local (`cursor_tuple_fraction`
and `enable_sort`); ordinary request plans are not changed. A server-side 57014
query cancellation is now classified as query-budget exhaustion, not disk loss.

This is still an O(N) on-demand aggregation, not an incrementally maintained
aggregate read model. Counts merge snapshots observed by separate replica
transactions; they are not a globally linearizable snapshot across independent
databases during concurrent updates. Stable-input Audit comparison and live
latency/response integrity are measured separately. Unavailable/failed replicas
remain explicitly partial; changing the UI must not hide that status.

Earlier candidate failures are retained in `.e2e/analytics-after.log`,
`analytics-repeated.log`, `analytics-bounded.log`, `analytics-cursor.log` and
`analytics-streamed.log`. A first successful run did not establish repeatability:
some subsequent requests had correct totals but were partial, or exhausted their
query budget. The ordered-cursor candidate then passed three 120,000-event,
8-KiB-payload runs in 0.728 / 0.673 / 0.665 seconds, including cancellation and
admission-saturation checks (`.e2e/analytics-ordered.log`). Later boolean-predicate
and server-cancellation changes require their own final candidate verification.

Concurrent index preparation initially hit a two-second lock timeout on
postgres-c. The blocker was our abandoned physical-capture COPY query, waiting
on `ClientWrite` for over 50 minutes after the earlier observer interruption.
Only that exact recorded PID/query was canceled, then terminated when cancellation
could not drain its stalled client. No application connection was targeted.
The unusable index left by the canceled build was rebuilt concurrently. The
final boolean-aware index preparation/definition verification is in
`.e2e/analytics-indexes-20261121T171109Z/` and `20261121T171258Z/`; all canonical rows and PVC identities
were retained. Only the two additive analytics indexes were rebuilt, not data.

### Final analytics candidate and live requests

`aat/aggregator:m2-p1-analytics-e430b2d-20261122-r6` was staged on all three
nodes, then rolled out only to Aggregator. Three replicas became Ready on A/B/C
without a schema cutover, database reset, or another workload image change.
Deployment evidence: `.e2e/replication-update-20261121T171401Z/`.

The final Linux store test binary SHA-256 is
`2f7cc031d0835f3240fe6f7742f942580e212dc82e13024cc42b2a932dea396b`.
All **74 top-level tests and 12 subtests** passed against real disposable
PostgreSQL, without skips (`.e2e/analytics-full-suite.log`). The final analytics
tests also passed two additional runs (`.e2e/analytics-final-repeated.log`):
120,000 records with 8-KiB attributes aggregated in **0.627 / 0.638 / 0.859 s**
across the three final-candidate measurements. Actual cursor plans used Merge
Append and both covering Index Only Scans. Tests additionally cover divergent
versions, tombstones versus string attributes, UTC boundaries, explicit partial
status, caller cancellation, saturation and admission cleanup. Native service
tests and store `go vet` passed separately; native runs without the database
environment are not substituted for the real PostgreSQL run.

The candidate r5 query E2E passed three Audit comparisons on the stable isolated
600-record, five-hazard dataset:
`.e2e/report-tests/p1/02-queries/20261121T170611Z-986906e9/`. Its later r6 changes
correct boolean tombstones and server-cancellation classification, with separate
final-candidate tests above.

After the r6 rollout, read-only live scenario `05-analytics --repeat 5` passed:
`.e2e/report-tests/p1/05-analytics/20261121T171500Z-4db4486f/`. All ten analytics
requests returned **200, partial=false**, in **1.682–3.607 s**, within the unchanged
five-second gateway budget. All-history totals were **558,448–558,449** as the
live feed advanced. Five requests used all history; five used January 2025,
which currently contains no events. Nonempty filter correctness is established
by the controlled Audit/oracle tests, not that empty live interval. Aggregate
dimensions and UTC daily sums matched each live response's total. This is not
a full live-history Audit snapshot comparison or a multi-user capacity test.
Dashboard HTML returned 200; visual chart inspection remains with the operator.

The final r6 isolated recovery test also passed **two** volume-loss cycles,
units B then C: `.e2e/report-tests/p1/03-recovery/20261121T171221Z-03f11ea3/`.
All 182 event-read/analytics responses returned 200, maximum **0.587 s**; degraded
counts matched available Audit rows and carried explicit partial/unit labels.
Both replacement volumes recovered automatically, restoring two copies of all
**720** final events without re-importing previously acknowledged events. This
run erased only its temporary PVCs, not original VPS data or a worker OS disk.
Cleanup removed its labelled applications, topic, coordinator test database
and three temporary storage PVCs; those test contents are not recoverable.

Final preflights at `20261121T171713Z-81fc96a3` and
`20261121T171735Z-6803cc31` found all three canonical storage units ready on
distinct nodes with unchanged specified memory requests/limits. All eight
original application PVC UIDs matched the analytics-preparation baseline;
the official Tester Agent UID matched the earlier pre-P1 baseline. No temporary
E2E deployments or storage claims remained. These storage checks do not claim
that the separate notifier/Sensor Pantai readiness problems were fixed.

The disposable, owner-labelled PostgreSQL regression container and its own test
volume were removed after verification. Its contents are not recoverable;
application PVCs were not deletion targets.

## Continuous live-volume retest

The attempt at `p1/04-live-recovery/20261121T180038Z-386ae6fc/` erased the
original development claim `data-postgres-a-0` on worker B again, after verifying
two intact/equal-or-newer copies of all **558,844** frozen baseline identities.
The old node-local directory was confirmed absent; unrelated claims were not
deleted. Its first fault-window POST returned gateway HTTP 503
`backend_unavailable`, not per-item accepted/failed counts. The observer wrongly
required those counts on every 503 and aborted. This run remains failed; a
rejected/ambiguous POST is not an acknowledged write, and this response alone
does not prove lost data or a defective read path.

The test now continues observing after a rejected or unexpected POST, retains
unexpected responses as failures, and requires exact acknowledgement counts for
HTTP 200. It still requires successful, bounded event/Audit/analytics reads;
neither that assertion nor the application request budgets were relaxed.

The separate, read-only follow-up at
`p1/repair-followup/20261121T180501Z-687ba002/` observed the existing repair until
it reported `ready`, with **372,734** copied rows. No baseline events were
re-imported and no application restart or SQL data repair was used. Its sampled
**330** read responses remained HTTP 200: 110 event reads (maximum 0.149 s),
110 Audit reads (0.071 s) and 110 analytics reads (3.367 s). The copied counter restarted several times
before completion: `recoverUnit` restarts a scan after an unsuccessful cycle,
rather than persisting a resumable partition cursor. Existing logs do not
establish which individual operation aborted each cycle. This is a recovery
efficiency/diagnostic concern, not evidence that the repeated rows are duplicate
canonical identities. The follow-up cannot fill the earlier observation gap or
turn the failed run into a pass.

### Completed uninterrupted retest

`p1/04-live-recovery/20261121T181053Z-9256ccdd/` **passed** on the unchanged r6
Aggregator and dashboard r2 images. The laptop drove the test from 18:10:53 to
18:19:49 UTC. Before deletion, all **558,921** frozen identities had two
intact/equal-or-newer copies. Eleven identities changing during the sequential
capture were rechecked against that unchanged baseline before proceeding.

Only the original development canonical claim `data-postgres-a-0` on worker B
was deleted. Its old UID was `09876cf7-1205-43cb-ab95-fbc9e151c62a`; the new
empty-volume UID is `0c62e3c2-7862-4e6c-be06-ef55f86958db`. The exact old data
directory was proven absent. Worker B became NotReady and returned automatically
at the 79.26-second observation, following its scheduled 70-second restart.
The guest OS, SSH and every unrelated application volume were retained. This is
an actual application-volume loss plus k3s/container outage, not whole-OS-disk
destruction or VM power-off.

- **Zero** frozen baseline identities were missing/regressed on surviving
  storage while the original volume was absent.
- Automatic empty-disk repair reached `ready` at 372,812 copied rows. No old
  baseline event was re-imported, manually copied with SQL, or restored from a
  backup. This run's observed copied progress did not restart before completion.
- Final physical captures contained 372,842 / 372,519 / 372,770 rows in a/b/c.
  **Zero** of the 558,921 baseline identities lacked two intact/equal-or-newer
  copies. Equal versions require an identical full-row fingerprint; later live
  versions are permitted, not silently substituted with weaker checks.
- The independent observer retained **408** reads, 136 each: event maximum
  **0.662 s**, Audit **0.577 s**, analytics **2.535 s**. Every response was HTTP
  200 and bounded; all three endpoints explicitly reported partial/unavailable
  units during degradation. No read failure was hidden by a retry.
- There were **86** new fault-window import attempts: **58** exactly acknowledged
  with `accepted=1, failed=0`, and **28** explicitly unavailable HTTP 503
  responses. A 503 was not counted as a successful write. Only these 86 new
  identities were retried after baseline two-copy recovery was proved; every
  retry received the exact success acknowledgement. Old baseline data was never
  re-imported to make the result pass.
- All seven unrelated application PVC UIDs and the official Tester Agent
  DaemonSet UID were retained. The observer and parent test both closed their
  own SSH tunnels; no temporary E2E deployment or PVC remained.

Post-recovery `05-analytics --repeat 3` also passed at
`p1/05-analytics/20261121T182011Z-df87a8a9/`: six HTTP 200, explicitly complete
responses in **1.699–2.486 s**. All-history totals were 559,045–559,046 active
events while the feed progressed. The January 2025 filter was nonempty,
containing **87** events on each repetition; UTC daily and dimensional sums
reconciled. Exact controlled-input Audit comparisons remain the separate
`02-queries` evidence, not a claim of an atomic live-history comparison.

Final preflights `20261121T181955Z-662db64d` and
`20261121T182045Z-ee1ed5e4` found three Ready canonical pods on separate nodes,
unchanged specified storage memory settings, retained post-replacement PVCs and
Tester Agent identity, and no temporary E2E applications/claims. The three main
Aggregators were Ready on A/B/C. Notifier was Ready in that snapshot; Sensor
Pantai remained NotReady with 56 restarts. Those P5 health conditions were not
changed by this P1 test. The nine service-local dashboard DOM tests were rerun
and passed; they do not replace manual browser checks.

### Browser UI verification

The subsequent UI campaign used the existing Chrome tab at
`http://localhost:30080/dashboard#distribution`, controlled through native
Windows Computer Use because the direct browser connector was unavailable.
This was actual rendering and interaction, not only an HTML fetch or DOM stub.
No dashboard source, deployment, database data or resource limit was changed.
Local evidence is under
`.e2e/report-tests/p1/ui-browser-20261121T1829Z/`, including the final screenshot,
sanitized accessibility text and an explicit observation record.

- Scrolling exposed both lower panels. The severity table displayed NORMAL,
  WASPADA, AWAS and SIAGA counts, and the area table displayed names, counts and
  horizontal bars without visible overlap in the maximized browser.
- Setting the actual date controls to **1–31 January 2025** and clicking Refresh
  produced **87** total events, all SEISMIC/NORMAL, one area named
  `P1 live recovery practice`, AWAS zero and the other four hazard totals zero.
  The chart visibly changed to the January data point; the displayed request
  duration was **1.80 s**. These 87 records match the earlier live API test.
- An expired access token produced an explicit **HTTP 401 / Data lama** state.
  Prior figures remained visible but dimmed and labelled as not current. Normal
  Auth-issued temporary tokens restored requests; no credentials were printed
  or saved in the report, and each temporary token handoff file was removed.
- Live observations visibly distinguished `Replika tertinggal` warnings for
  store-a/store-b from the green `Data lengkap` state. These are UI observations,
  not a new proof about the duration or cause of replica divergence.
- Clearing both date controls using their calendar Clear buttons and refreshing
  restored all history: **559,113** active events, **8,923 AWAS**, **47** areas,
  all five legend entries and a green complete state. The displayed request
  duration was **2.11 s**. The browser was left in this restored all-history view.

This closes the previously pending severity/area-panel and nonempty date-filter
visual checks. It does not repeat the node-loss test, establish concurrent-user
UI capacity, or substitute for the later assistant catalog/checksum comparison.

## P1 report answers and design trade-offs

### Problem and mechanism

The relevant concepts are horizontal partitioning, skew/hotspots, replication
across failure domains, scatter/gather reads, replica reconciliation and
anti-entropy repair. Partitioning distributes ownership; replication preserves
another copy when a storage node disappears. They are separate responsibilities.

`partition.go` encodes the identity as JSON `[source, source_ref_id]`, hashes it
with FNV-1a, and selects `hash % 3`. SHA-256 of the same identity determines the
canonical hazard ID. Logical partition `p` has its primary in slot `p` and its
replica in slot `(p+1)%3`. A physical database therefore holds two of the three
partitions, not the whole dataset. The deployment places these slots on separate
nodes. Unit names must not be inferred from node letters: in this VPS layout,
store-a is on B, store-b on A, and store-c on C.

Geography is not the partition key, so many distinct events in one region need
not all target one primary. This does not guarantee equal bytes, CPU, or update
rates: one repeatedly updated identity still has the same two destinations.
The sharding test measures actual counts and node CPU/memory rather than
inferring load balance from the hash function.

Membership is deliberately a fixed three-slot configuration. Removing a node
does not change the modulo or remap every identity. Reads consult available
copies and mark degradation. Kubernetes may recreate the missing slot on an
empty local disk; an elected repair worker copies the assigned partitions from
surviving peers, preserving versions and tombstones. A version guard prevents
an old copy from overwriting a newer write. The rebuilt unit does not serve as
ready until activation; ongoing reconciliation closes replica gaps afterward.
Replacement of an existing slot is supported and tested. Automatic addition of
a fourth shard with online repartitioning is **not** implemented or claimed.

### Alternatives considered

| Approach | Effect on regional skew | Trade-off against this implementation |
| --- | --- | --- |
| Geographic/range partitioning | A concentrated earthquake region can overload one owner | Good locality and efficient range pruning, but requires hotspot splitting/movement |
| Fixed identity hash, chosen here | Distinct regional identities spread over three primary slots | Simple deterministic placement and replacement; global queries fan out and changing the shard count needs a migration |
| Consistent/rendezvous hashing with virtual partitions | Spreads many independent keys and moves fewer assignments when membership changes | Better dynamic membership, but requires ownership epochs, migration/catch-up and safe cutover; hot individual keys still need separate treatment |

The Dynamo paper describes consistent hashing, versioning and replica repair
as complementary mechanisms. It is a literature comparison, not a claim that
this modulo-three PostgreSQL implementation is Dynamo or implements its quorum
protocol. [DeCandia et al., SOSP 2007](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf).

### Endpoint consistency

These guarantees describe the P1 data endpoints, not every operational endpoint
of other problems. No endpoint below establishes a globally linearizable
transaction across three independent PostgreSQL databases. Ordinary SQL reads
see committed statement snapshots; analytics uses a repeatable-read snapshot
within each replica transaction, not one shared snapshot across databases.
[PostgreSQL 16 transaction isolation](https://www.postgresql.org/docs/16/transaction-iso.html).

| Endpoint | Normal operation | During a missing/rebuilding unit |
| --- | --- | --- |
| `GET /events` and `/internal/events` | Latest observed replica versions are merged and globally ordered; complete pages may use the configured bounded-TTL read cache/projection | Canonical fills use surviving copies and explicit partial metadata; failed/partial pages are not cached. A previously complete cached page can remain until its original TTL expires, so failure detection is not instantaneous |
| `GET /events/{id}` and `/internal/events/{id}` | Direct replica lookup/reconciliation, bypassing the page cache | Surviving result and explicit degradation; this is not a promise that an isolated copy has the globally newest version |
| `GET /internal/audit/events` | Bypasses page cache, merges observed versions, orders by ingestion time then ID; cursor is not a frozen multi-page transaction | Surviving records plus conservative partial/unavailable metadata. Concurrent ingestion can change later pages; use stable input for exact reference comparisons |
| `GET /internal/audit/checksum` | SHA-256 over sorted source reference IDs from the uncached Audit traversal | Retains partial metadata seen on any page; a partial digest must not be reported as complete or compared as a full reference snapshot |
| `GET /internal/audit/location` | Reports actual observed physical row locations and primary/replica roles | Reports reachable observed locations and degradation; existence is not proof of a complete replica or an atomic view of all locations |
| `GET /analytics` and `/internal/analytics` | Uncached, per-partition latest-version merge, then aggregation from replica-local repeatable-read snapshots | Still returns available aggregates with explicit partial/unit metadata; no stale full-history cache substitutes for a missing replica |
| `GET /internal/storage-stats` | Physical counts and local/observed readiness; diagnostic rather than a distributed snapshot | Down/recovering state remains visible; counts sampled at different moments must not be interpreted as an atomic logical total |
| `POST /internal/import/events` | A per-item success requires both assigned destinations to acknowledge. A whole-batch 200 requires every item to succeed; there is no cross-item/distributed transaction | Missing acknowledgement yields 503, possibly after one copy committed. The caller must not treat it as success or assume no effect; retries preserve source identity. The gateway can reject with `backend_unavailable` before a per-item response exists |
| `GET /internal/audit/unprocessed` | Reads durable ingress state in the coordinator database, not the canonical page cache | Survives the tested worker B canonical-volume fault because its owner database is on A; coordinator-disk/control-plane loss is a separate failure scope |

The five-second API limit in this campaign is an explicit test bound, not a
P1-specification quote or proof of capacity under concurrent analytical users.
The one-worker campaign does not establish arbitrary simultaneous failures,
coordinator/Kafka HA, or linearizability during network partitions.

### Analytical calculation and presentation

Each partition merges its two ordered, narrow replica streams by hazard ID and
selects one newest ingestion-version/revision record. Filtering is applied after
that selection, so an older in-range copy cannot override a newer out-of-range
copy, and an old live copy cannot resurrect a newer boolean tombstone. The
surviving record contributes once to hazard, severity, area and UTC-day counts.
Partition totals are then combined; replica row counts are never simply added.
Partial, unavailable, lagging and stale states are presented separately in the
dashboard rather than silently labelling every successful HTTP response complete.

Controlled, stable-input tests compare all dimensions and nonempty time ranges
with independent Audit counting. Live-feed requests test latency, response
integrity and internal sum reconciliation separately; changing live totals are
not a fixed assistant checksum. The operator's screenshots show the five-series
chart rendered and a later complete state after a transient store-a lag warning.
The later native Chrome UI campaign above additionally verified the severity
and area panels, a nonempty date-range interaction, stale-token handling and
restoration of the all-history view.

## Acceptance still requiring separate evidence

The scoped main rollout is recorded in `.e2e/p1-rollout-r2/`: three r2
Aggregators Ready on A/B/C, with other workload images unchanged. Follow-up P1,
P3 and P4 preflights passed at `20261121T154254Z-4617e57a`,
`20261121T154258Z-e0ad8808` and `20261121T154300Z-d4b88f72` respectively.
The practice satellite feed was progressing with reported offset lag zero;
that is not a new full P4 acceptance campaign. No temporary test pods, PVCs or
labelled regression containers remained. Original application PVC identities
and the official Tester Agent DaemonSet identity matched the initial baseline.

The earlier P1 rollout snapshot also recorded notifier and Sensor Pantai
pods Running but not Ready. Notifier was already not Ready in the initial
inventory. Their images/configuration were not changed by this P1 update, and
their readiness is not claimed as a P1 test pass or as a newly resolved P5 issue.

The assistant catalog and checksum are post-deadline resources and are not
substituted with generated data. Geographic skew is explicitly present in the
practice fixture, but its distribution is not claimed to be the assistant's
natural catalog distribution. Kubernetes CPU/memory sampling is retained during
loading; short samples are not a capacity benchmark or exclusive attribution of
all node CPU to this test.

P1.7 browser rendering must be distinguished from HTML availability and verified
analytics JSON; the latter alone is not a screenshot of correctly rendered
charts. Earlier browser-control initialization failed twice, including after a
reset; no automated visual pass was established. The operator later supplied
screenshots confirming the five-series chart and its complete/lagging states.
The later native Chrome campaign completed the severity/area-panel and date-range
interaction checks, with its own screenshot and observation record above.
Control-plane/coordinator loss and other problems' acceptance criteria are not
established by this campaign.
