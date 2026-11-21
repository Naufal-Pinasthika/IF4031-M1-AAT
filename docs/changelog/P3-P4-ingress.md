# P3/P4 durable ingress and stream processing

Implementation and verification note, 21 November 2026. The complete updated M2
specification was reviewed before these changes. Pre-deadline evidence below
is kept separate from the later assistant-reference and exam comparisons.

## P3: preserve first, process asynchronously

PVMBG responses are spooled and stored in a PostgreSQL durable inbox before the
polling cursor advances. Raw bytes are stored in 64 KiB chunks, including invalid
UTF-8 and oversized bodies. Parsing is bounded to 1 MiB and 64 nesting levels.
Unparseable messages are parked with their source, receive time and reason;
database failures roll back, retain a `downstream_retry` reason, and use bounded
exponential backoff without parking the message. A parked identity blocks later
messages for that identity, not unrelated reports.

The default adapter accepts extra fields but rejects renamed/missing required
fields and incompatible types. Adapter version 2 explicitly accepts `report_ref`
and integer-valued string `eruption_count_24h`. Missing mandatory information is not
invented. Deploying a compatible adapter and invoking replay reuses the original
bytes; a genuinely truncated message remains parked until it can be corrected.

`POST /field-reports` returns 202 only after durable commit, independently of
downstream processing. Idempotency is scoped to the authenticated client and key;
reusing a key with another body returns 409. Processing and completion use one
transaction, while per-identity ordering and canonical deduplication prevent
duplicate effects. This is at-least-once retry with idempotent effects, not a
promise of exactly-once HTTP delivery.

Only signed Internal Ops identities may inspect raw failed messages, replay them,
or inspect satellite status. Client-supplied role headers are overwritten. The
audit-token API remains separate, as required by the specification.

## P4: one fenced stream, atomic state and offset

The lease owner consumes SSE; other Aggregators remain available for reads and
takeover. Stream offset, detections, clustering changes, and durable projection
intents commit together in the coordinator database. Offset zero is valid; resume
uses the next offset because the observed feed's `from` parameter is inclusive.
Sequenced heartbeats advance the durable checkpoint without creating a fire;
unsequenced transport keepalives do not. Live inspection found a heartbeat at
offset 2187 between detections 2186 and 2188, exposing and correcting the earlier
incorrect assumption that every heartbeat was out of band. Failed frame
processing cannot advance the checkpoint. Idle/disconnected streams reconnect
with bounded backoff.

Clustering uses satellite detection time, a six-hour lateness allowance, and the
specified spatial/temporal connectivity. Late detections are retained separately
and counted. Updates include the live feed's `changes` envelope and preserve zero
values. Retractions produce versioned canonical tombstones, preventing an old
replica or replay from resurrecting a removed fire. Dashboard consumers remove
the current fire; historical notification records remain historical records.

The operations dashboard exposes checkpoint lag, pending projections, late and
singleton counts, sample count, and rolling p95 handler-ingress-to-canonical-ack
latency. This latency is not source-to-notifier latency. Unknown measurements are
not rendered as a fabricated zero.

The stream mutation path loads an existing connected component once, rather than
re-probing every unchanged member's neighbors. Cluster membership changes use
set-based updates instead of one SQL update per detection. Canonical admission
combines fingerprint deduplication, durable outbox insertion and destination
intents into one SQL statement in the original transaction. These changes reduce
database round trips without skipping feed offsets or intermediate mutations.

The receiver uses a bounded 16-frame queue and flushes batches of at most 16
frames after at most 5 ms of collection. All intermediate updates/retractions
are still applied in order. State, projection intents and the batch's final
checkpoint share one transaction; an error anywhere rolls back the entire batch.
Processing failure cancels the speculative reader, and the next ownership cycle
reloads the durable database offset. Queued but uncommitted frames are replayed
after process loss. Latency timestamps originate before queueing, so batching
does not hide queue wait from the reported p95. TCP connection establishment is
also bounded independently of the lifetime of a healthy SSE stream.

Repeated VPS runs also exposed internal DNS timeouts, including an approximately
27-second reconnect gap while the lease holder remained unchanged. The conntrack
table was not full, and CoreDNS itself had no restart. The exact packet-loss
cause is not established by those facts. Aggregator's declared Pod DNS policy
now uses TCP (`use-vc`) with bounded resolver retries; the same policy is applied
to isolated E2E receivers, not only to the benchmark. Go explicitly supports
this option in its [resolver implementation](https://go.dev/src/net/dnsconfig_unix.go).
[Kubernetes DNS guidance](https://kubernetes.io/docs/tasks/administer-cluster/nodelocaldns/)
also describes UDP-related timeout tail latency and the use of TCP upstreams.
This change does not pin source IPs, bypass service discovery, change the input
stream, or replicate Kafka/coordinator infrastructure.

Read-projection initialization has a separate two-second transactional DDL
budget; runtime cache queries retain their original 100 ms bound. A real
PostgreSQL regression exposed the previous accidental use of the runtime budget
for initialization. The failing run is retained, followed by a 64-test pass.

## Earlier verification: initial r2 campaign

The results in this section belong to the initial r2 campaign, not the later
candidate. See [test tooling](../../test/README.md) for current reproducible
commands.
The isolated suite exercises five crash/replay/retraction rounds, eight rounds
of 1,000 SSE frames, exact raw-byte retention, malformed payloads, receiver
recreation after acknowledgment, key conflicts, and authorization boundaries.
Generated logs stay ignored under `.e2e/`.

The final isolated run passed 62 store, 22 aggregation, 19 handler and four
Client API tests (107 total). The per-identity replay test explicitly verifies
that a parked older revision blocks its successor while another identity is
processed, then checks ordered recovery and no additional effects on replay.

Two laptop-origin live campaigns accepted 40 reports with 120 stable retries
and 40 rejected key/body conflicts. All 40 results were unique after receiver
rollouts. POST response p95 was 71.48 ms and 60.71 ms respectively. Adapter-v2
Ops replay recovered the rename and string-integer messages; malformed and
missing-required-field inputs remained parked. Three Aggregators and the PVMBG
mock had no container restarts in the observed campaign. All eight PVC UIDs and
the Tester Agent UID were unchanged.

Candidate `m2-p3p4-5bafd81-20261121T1200Z-r2` contains the heartbeat fix. The initial
eight-image build used no cache; the corrected Aggregator was rebuilt without
cache, while unchanged fresh image contents were reused for the other services.
Feed checkpoint progression after restart and exported dashboard metrics were
observed, but a large pre-existing feed history remains to be consumed. Therefore
this is not a 60-second post-burst catch-up pass. The earlier stalled-checkpoint
observation is retained in raw evidence rather than reclassified as a pass.

After the corrected deployment and another receiver rollout, a 230-second live
window observed the committed offset advance from 3,322 to 5,724. The final
rolling ingress-to-canonical p95 was 408.44 ms over 3,599 samples, with one
pending projection and 182,986 offsets of historical lag. These are different
measurements: a short processing latency does not establish that the stream is
current. The observation used the existing practice session at 60x; it did not
reset the official feed or choose a more favorable profile.

Live deployment, restart observations, and 60x catch-up must be reported against
the actual image tag and observation window. A feed token is a credential, not
the assistant's reference output. Under the updated demonstration schedule, that
comparison is deferred to the post-deadline window, not treated as a blocker for
the current implementation or practice testing.
Coordinator and Kafka remain singleton infrastructure. Acknowledged reports
survive receiver-process loss, not permanent loss of the coordinator disk.

The notifier's Kafka fetch queue is bounded and its Go memory target is reduced
within the unchanged 256 MiB resource limit. No notifier OOM/restart was observed
in the bounded post-rollout window; long-duration stability is not established.

## Repeated VPS recovery: r7 and TCP DNS

The report-facing scenarios run from the laptop against the three VPS nodes,
using the application images, signed Auth tokens, Kubernetes limits and real
PostgreSQL/Kafka infrastructure. Temporary application identities, databases and
topic isolate destructive tests from production state. The scenario names and
commands are in [test/README.md](../../test/README.md); service-local regressions
remain with their services.

`p4/02-recovery` passed three baseline/fault pairs on candidate
`m2-p3p4-5bafd81-20261121T1200Z-r7` with the declared TCP DNS policy. Each run
contained 1,772 frames, 1,608 unique detections and 81 final fires. All canonical
fields and stable identities matched the independent oracle and the paired
baseline, except the explicitly arrival-derived `ingested_at`. Fault runs also
severed the stream every 137 frames and killed its owner while processing.

| Pair | Baseline catch-up | Fault catch-up | Fault stream connections | Fault ingress-to-canonical p95 | Fault availability-to-canonical p95 |
| --- | --- | --- | --- | --- | --- |
| 1 | 34.28 s | 32.25 s | 14 | 883.39 ms | 29.56 s |
| 2 | 27.50 s | 35.70 s | 13 | 1,004.67 ms | 33.84 s |
| 3 | 24.03 s | 44.39 s | 13 | 1,386.80 ms | 41.50 s |

Catch-up is an upper bound measured from the independent producer's final
availability time until the last offset and all projection destinations are
complete. It is not measured from a delayed socket read. Source availability
latency includes backlog; the application measurement includes its bounded
receiver queue. Neither is a P5 source-to-notifier latency claim.

Evidence: `.e2e/report-tests/p4/02-recovery/20261121T133429Z-f9633e05/`.
Earlier failed burst runs remain in adjacent directories. In particular, the
previous r7 run passed one pair but failed the second with a roughly 27-second
internal DNS reconnect gap. Its failure was not erased or reclassified. The
input count, 60x production rate and 60-second bound were retained during fixes.

The r7 service regression rerun passed all 65 store-package tests with PostgreSQL
enabled and no skips. Before that,
one interrupted-admission test hit its two-second database timeout under load;
that failure is retained. The same unchanged test then passed five repetitions,
followed by the complete 65-test suite. Evidence is under
`.e2e/report-tests/service-regression-r7-repeat/`. The native service tests and
Aggregator consumer race tests also passed. PostgreSQL tests require a disposable
database; a native run without its DSN does not establish their result.

The production r7 P3 rerun then passed all three schema-policy rounds, twelve
poison exposures and three Ops-access rounds. Fresh PVMBG request logs prove
each repeated poison exposure occurred; identical bodies intentionally retain
one durable inbox identity. Raw preservation includes the entire 2,097,156-byte
oversized body. No relevant pod was replaced or restarted during this campaign.

Three isolated replay rounds recovered both incompatible revisions per identity
in order, without blocking an unrelated additive report or generating duplicate
effects on repeated replay. The durable-delivery scenario acknowledged 60 reports
while result storage was exclusively locked, force-killed all three temporary
receivers in each of three rounds, and reconciled 60 unique completed results.
There were zero lost acknowledgements or duplicate results. Initial POST p95 was
47.36 ms and maximum 257.12 ms; all were below the test's two-second bound.

Retained r7 evidence under `.e2e/report-tests/`:

- `p3/01-schema/20261121T134322Z-1fdecee8/`
- `p3/02-poison/20261121T134646Z-e5d01fc7/`
- `p3/03-ops-access/20261121T134941Z-d637fbfa/`
- `p3/04-replay/20261121T134944Z-685dd9f8/`
- `p3/05-delivery/20261121T135134Z-3811c5d4/`

## Follow-up: complete audit pagination and consumer runtime

The final r7 production audit was inconclusive, not a pass:
`p4/03-audit/20261121T135239Z-32656d25/`. Structured logs identified
`coverage_unproven` on otherwise reachable replicas. Tombstone rows consumed the
bounded query prefix before reconciliation removed them; a 1,001-row audit
prefix could not refill past that history. A new disposable-database regression
reproduced the same empty, partial page with 1,100 tombstones and 30 live fires.

The candidate query now excludes already-retracted rows from each replica's
initial prefix. Latest-version candidate lookups remain **unfiltered**, so a
newer peer tombstone still suppresses an old visible replica. This fixes the
healthy pagination path without dropping retained tombstones, weakening the
coverage proof, or changing the HTTP partial marker. The new pagination test
and stale-replica retraction test passed three repetitions, then all 66 store
tests passed with real PostgreSQL and no skips. Before/after and full logs are
under `.e2e/report-tests/service-regression-r8/`.

The runtime check also found the notifier still running the initial image,
not the later bounded-fetch source change, with Kafka DNS timeouts and an older
OOM restart. Its Pod DNS policy now uses the same bounded TCP resolver settings
as Aggregator. The existing 128 MiB request, 256 MiB limit and 96 MiB Go memory
target are unchanged. Queue capacity is two and Kafka fetch size is 1 MiB.
All eight application images are rebuilt without layer cache for r8 rather than
assigning an old runtime the new source's verification results.

The initial r8 rollout also lost its multiplexed SSH connection while waiting
for the old, unready notifier revision. Its Pod still differed from the new
StatefulSet template. Only that obsolete Pod was replaced (normal deletion,
PVC retained); deployment was then rerun over independent SSH connections.
All eight images reached r8, all application pods became Ready, and the Tester
Agent remained unchanged. This was deployment repair **before** fault tests,
not an intervention inside their recovery windows. Kubernetes documents the
possible stuck `OrderedReady` update state and Pod replacement in its
[StatefulSet recovery guidance](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/#forced-rollback).

The formerly inconclusive production audit then completed all pages in three
successive snapshots: 3,976 fires with identical identity checksum
`a28d5b01977d6f6338fa10f9988eb674dc24c8a99604c35b8f173d18d7411408`.
The real dashboard's six satellite series were also present in three scrapes,
including measured p95 and a positive sample count. Evidence is under
`p4/03-audit/20261121T140924Z-256cf116/` and
`p4/04-dashboard/20261121T140934Z-dd386baa/`.

## Final r8 repeated stream verification

The full three-pair recovery campaign was rerun against the freshly built r8
images, not assigned the earlier r7 result. All six runs passed the same full
oracle and every baseline/fault comparison was identical. Each run retained
1,772 frames, 1,608 unique detections and 81 final fires; fault connections were
cut every 137 frames and the active owner was killed during recognition.

| Pair | Baseline catch-up | Fault catch-up | Fault connections | Fault ingress-to-canonical p95 | Fault availability-to-canonical p95 |
| --- | --- | --- | --- | --- | --- |
| 1 | 18.65 s | 38.05 s | 13 | 1,285.28 ms | 35.77 s |
| 2 | 34.36 s | 43.85 s | 13 | 1,226.62 ms | 41.75 s |
| 3 | 24.45 s | 31.62 s | 13 | 862.56 ms | 30.16 s |

Evidence: `p4/02-recovery/20261121T140955Z-34169049/` under the ignored
`.e2e/report-tests/` directory. The simultaneous read-only official-feed window
is separate evidence at `p4/01-live-stream/20261121T140954Z-a5eca89a/`.
Neither campaign reset or selected a pattern in the official practice feed.
Across nineteen samples in that 180-second window, the official checkpoint
advanced from 253,466 to 253,901 and fires increased from 3,976 to 3,982.
Observed lag stayed at zero offsets; final rolling ingress-to-canonical p95
was 742.21 ms. That live-window latency is not a source-to-notifier measurement
and does not replace the separate controlled burst/fault assertions above.

The r8 P3 rerun also passed three ordered replay rounds and three downstream-lock
plus all-receiver-crash rounds. Sixty initial acknowledgements reconciled to
sixty unique completed results, with no losses or duplicate effects. POST p95
was 48.84 ms and maximum 721.75 ms, below the unchanged two-second assertion.
Repeated identical requests retained their IDs; conflicting bodies returned 409.
Evidence: `p3/04-replay/20261121T141753Z-c4ed2d00/` and
`p3/05-delivery/20261121T141932Z-1c31d506/`.

The final runtime inventory, `.e2e/report-tests/runtime-r8-final/runtime.json`,
verified twelve fresh application pods Ready with zero container restarts,
three Aggregators on distinct nodes, all eight original PVC UIDs retained,
and the original Tester Agent UID unchanged. Temporary E2E applications,
databases, Kafka topics and dashboard forwards were absent. Cleanup removed
only disposable test state; real application data and failed-run evidence remain.
