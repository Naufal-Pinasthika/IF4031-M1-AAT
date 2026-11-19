# P2/P5 design and verification status

Snapshot: 19 November 2026. Implementation is not a claim that every acceptance
criterion passes. Dated operator journals and raw evidence stay local and are
ignored by Git; this page summarizes their relevant results and limitations.

## Maintained guides

- [Whole-system design review](M2-P2-P5-design-review.md): dependencies,
  alternatives, trade-offs and the historical acceptance matrix.
- [P2 polling ledger](P2-polling-ledger.md): ownership, fencing, request evidence
  and coordinator schema/rollback constraints.
- [P5 observability](P5-observability.md): source-to-notifier measurement and the
  private operations dashboard.

## Current implementation

Three Aggregators serve reads behind one Service, with independent source
leases. Coordinator schema v3 atomically admits each event and independent
projection jobs for Kafka and each assigned canonical replica. Payload cleanup
waits for every target acknowledgement; retries and ready-store anti-entropy
repair missing copies. This is not exactly-once physical upstream HTTP.

Read completeness reconciles latest versions across replicas and checks bounded
page coverage. `partial` means completeness cannot be certified;
`replication_degraded` can describe a lagging reachable copy even when the union
is complete. Failed reads and unproven pages are never relabelled complete.

The last deployed Aggregator is `m2-vps-20261119-12-buildinfo`. Its compatible
rolling candidate is `m2-vps-20261119-11-coverage`, not a pre-v3 binary. A v2-to-v3
worker cutover is not a rolling-update pass. Coordinator, Kafka and notifier
remain singleton infrastructure; HA was explicitly deferred.

## Evidence and open criteria

| Area | Observed result | Limitation |
| --- | --- | --- |
| Regression | Real-PostgreSQL race/vet suites passed for Aggregator, Client API and notifier. Four focused coverage/replication tests passed ten repetitions, including 650 complete-page oracle comparisons. | Bounded fixtures do not prove all possible failures. |
| Canonical durability | Three 35-second canonical-store process pauses recovered automatically; independent broker delivery continued and queued projections drained. Prior four missing assigned copies were repaired. | Process pause is not whole-VM freeze or disk-loss proof. |
| P2.6 | Build 10 sustained 10,000 VUs for 127.7 seconds, observed Ready 3-to-5-to-2 scaling and passed the existing script thresholds. | About 73% of requests were controlled 429 responses and some 2xx were partial. This predates the build-11 read correction and needs repetition on current code. |
| P2.7 | Build 11 passed four unchanged 1,000-VU/60-second cases, ordered 1-to-3-to-3-to-1. Complete-response throughput ratios were 51.87 and 46.85, both above 2. | Cache/admission regimes explain superlinear observed capacity; this is not a general scaling law. Single-instance cases rejected about 98% of requests. Generator C shared application resources. |
| P2.8 | First strict 50-VU/120-second round served 54,086 requests without HTTP failures and observed both distinct binaries fully Ready. | **Failed:** 11 HTTP-200 responses were partial, before the first image swap. Exact cause remains unproven; near-500ms durations only suggest the store timeout. Two planned further rounds were not run. |
| P5 | Nine singleton load rounds and three identity catch-up rounds passed their checks; seven-service metrics and an asynchronous consumer trace were observed. | No fresh critical source events were available to prove source-to-notifier p95 at most 5 seconds. Old 422ms admission-to-notifier timing is not that SLO. |

Exam-profile testing remains unavailable until its scheduled release. Broad
combined-fault/node-loss acceptance and coordinator/Kafka HA remain open.

## Reproduce and retain evidence

See [VPS provisioning/access](../infra/vps/README.md). Review fault-injection
scripts before executing them. Tests create local `.e2e/` and `.vps/` artifacts;
retain failures as well as passes. Never commit inventory, credentials,
kubeconfig, database files or raw run logs. Dated journals remain available on
the operator's current machine, not as links expected to resolve in a clone.
