# Source and verification status

Snapshot: 22 November 2026 (WIB). Results below distinguish repeated practice
E2E evidence from historical results and the later assistant/exam comparison.

## Source versus runtime

- Integration base: merge `5bafd81` (main `aa87526` plus `feat/wildfire`).
  P3/P4 runtime fixes are recorded in `c34106b`, with deployment, acceptance-test
  and documentation changes in separate commits on `feat/integration`.
- Original P3/P4 candidate: `m2-p3p4-5bafd81-20261121T1200Z-r8`. All eight application
  images were rebuilt without layer cache and imported on all three nodes.
  This candidate was built from the pre-commit worktree; its tag identifies the
  integration base, not the exact final source revision. Committing and merging
  the source does not itself rebuild or redeploy the VPS.
  Earlier named candidates and their failures remain separate evidence.
- Earlier P1 update: the three Aggregators ran
  `m2-p1-e430b2d-20261121-r2`, built from the uncommitted `test-p1` worktree.
  The scoped rollout retained original PVCs and left other workload images
  unchanged. All three new Aggregators are Ready on A/B/C. Evidence is in
  `.e2e/p1-rollout-r2/` and the P1 verification note below; this is not a new
  10,000-VU rolling-update proof or a fresh-database rebuild.
- Three Aggregators run across three nodes. All eight application PVC UIDs and
  the official Tester Agent UID were retained across the image deployment.
  A later, explicitly authorized P1 disk-loss test deleted the original
  `data-postgres-a-0` volume on worker B. Its replacement recovered all 558,001
  baseline identities to two intact/equal-or-newer copies; unrelated PVCs and
  Tester Agent identity remained unchanged. The interrupted observation window
  is inconclusive, and global analytics failed with 503 at that time. This is a data
  recovery result, not an overall P1 pass; see the P1 verification note.
- Latest analytics update: three Aggregators now run
  `m2-p1-analytics-e430b2d-20261122-r6`. The scoped rollout retained data and did
  not change other workload images or memory limits. All 74 store tests plus
  12 subtests passed on real disposable PostgreSQL. Repeated live analytics
  returned ten 200/complete responses in 1.682–3.607 seconds over approximately
  558,449 active events; dimensional/daily totals reconciled. Controlled Audit
  comparisons are separate from that live latency check. Charts still need
  manual visual verification; this is not a full P1 or capacity pass. See the
  [P1 verification note](../changelog/P1-verification.md) for exact evidence,
  earlier candidate failures, snapshot semantics and the on-demand O(N) design.
  The final r6 temporary-volume recovery test passed two cycles B → C with
  182 read/analytics 200 responses (maximum 0.587s) and two recovered copies of
  all 720 final records. Original PVCs were not fault targets in that campaign;
  all eight retained their UIDs and cleanup left no temporary E2E PVC/deployment.
- Dashboard presentation update: Client API now runs
  `m2-dashboard-20261122-r2`, with a compact dark, Grafana-inspired layout (not
  an installed Grafana service). Nine dependency-free DOM behavior tests,
  Client API Go tests and `go vet` passed. The deployed HTML matched the source
  byte-for-byte; unauthenticated analytics returned 401 and authorized complete
  analytics returned 200 in 2.255s. Other workload images and every PVC UID were
  retained. Evidence: `.e2e/dashboard-rollout-20261121T173432Z/` and
  `.e2e/report-tests/p1/dashboard-smoke/20261121T173505Z-0ddf49b7/`.
  Browser layout inspection is explicitly left to the operator.
- Latest original-volume retest passed:
  `.e2e/report-tests/p1/04-live-recovery/20261121T181053Z-9256ccdd/`.
  Worker B's original development canonical volume was deleted and replaced;
  all **558,921** frozen identities automatically regained two intact/newer
  physical copies, without re-importing old data. All **408** continuously
  sampled event/Audit/analytics reads returned 200 (maximum **2.535 s**) with
  explicit partial metadata during degradation. Of 86 new fault-window imports,
  58 were fully acknowledged and 28 returned explicit 503; these rejections were
  not counted as successful writes. Only new inputs were retried after baseline
  recovery. Unrelated PVCs and Tester Agent identity were retained. This stopped
  k3s/containers and erased one canonical PVC, not the guest OS/whole VPS disk.
  The earlier failed and interrupted runs remain failed/inconclusive.
  Six post-recovery analytics responses were 200/complete in **1.699–2.486 s**;
  the nonempty January 2025 range contained 87 records. Final preflights found
  three Ready stores and three Ready Aggregators on distinct nodes, with no
  temporary E2E applications/claims. Notifier was Ready in that snapshot; Sensor
  Pantai remained NotReady. The operator's screenshots confirm the five-series
  chart and the transition from replica-lag warning to complete. Severity/area
  panels and date-range interaction were subsequently verified through native
  Chrome control, as recorded below.
  The [P1 note](../changelog/P1-verification.md) now includes the five report
  answers, alternatives and per-endpoint consistency guarantees.
- Browser UI verification completed using the existing Chrome tab (the direct
  browser connector was unavailable). The severity and area panels rendered
  visibly, January 1–31, 2025 produced the expected 87 SEISMIC/NORMAL events in
  one area, expired tokens visibly marked old figures stale, and clearing the
  dates restored the five-series all-history chart. The final view was green
  complete: 559,113 active events, 8,923 AWAS and 47 areas, request 2.11 seconds.
  Evidence: `.e2e/report-tests/p1/ui-browser-20261121T1829Z/`.
  No application code or VPS deployment was changed for this UI campaign.
- The private local environment and deployed Secret enable the official satellite
  feed; PVMBG adapter version 2 enables the explicitly supported schema changes.
- Schema changes are additive. A Git rollback does not undo database schema or
  Kafka partition changes. Inspect compatibility before any future rollback.
- An older unrelated P1–P3 experiment remains in a local stash, not applied here.

## Acceptance boundaries

| Area | What the retained report establishes | What remains unproved |
| --- | --- | --- |
| P1 practice | Earlier sharding/skew and stable-input Audit/analytics comparisons passed. Latest r6 original worker-B canonical-volume loss restored two copies of all 558,921 baseline identities; all 408 continuously sampled reads were 200, maximum 2.535s. Native Chrome checks passed for the chart, severity/area panels, a nonempty date filter and explicit stale/lagging/complete states. Exact evidence is recorded above and in the P1 note. | Assistant catalog/checksum comparison is deferred. The actual canonical PVC was erased, but the OS and unrelated application disks were retained; this is not whole-VM disk destruction. Earlier failed runs remain in the P1 note. |
| P2 throughput | The last comparable 1–3–3–1 comparison in the baseline report failed the required 2x ratio. | No new throughput pass. Balanced requests alone do not prove scaling. |
| P2 scaling/rolling | Named candidates passed some bounded scale and rolling checks; earlier failures are retained. | Not every candidate, workload, or failure scenario passed. |
| P2 stale ownership | Bounded lease, clock, and process-pause evidence exists. | Whole-VM freeze/stale-process-resume is not established by those runs. |
| P5 delivery | Singleton load/catch-up checks exist. | Fresh critical source-to-notifier p95 at most 5 seconds remains unproved in the baseline report. |
| Infrastructure | Coordinator, Kafka, and notifier use singleton infrastructure in the documented baseline. | Infrastructure HA is deferred, not implied by three Aggregators. |
| Latest VPS health | Three canonical stores and three Aggregators are Ready after the authorized original-volume replacement. | Sensor Pantai continues OOM/crash-looping; notifier readiness fluctuates, with an earlier OOM and Kafka probe timeouts. No P5 readiness fix is claimed. |
| P3 durable ingress | Repeated VPS schema/poison/access checks passed. The r8 rerun passed three ordered replay rounds and three blocked-downstream/all-receiver-crash rounds: 60 acknowledgements, zero lost or duplicate results, POST p95 48.84 ms. | Permanent malformed or missing-required-field messages remain parked by policy, not silently repaired. Receiver-process durability is not permanent coordinator-disk-loss durability. |
| P4 stream | Three r8 baseline/fault pairs of 1,772 frames matched the full oracle and each other; catch-up was 18.65–43.85 seconds. The audit-pagination fix passed 66 store-package tests with PostgreSQL enabled and no skips, followed by three complete production snapshots of 3,976 fires. Live feed and the real dashboard were observed separately. | Assistant-reference comparison P4.6 and exam validation belong to the post-deadline window. Controlled-source equality is not assistant-reference equality. |

Read the [historical verification report](../changelog/verification-2026-11-19.md) for exact
candidate tags, workloads, errors, and evidence paths. Those observations must
not be silently reassigned to a different build.

The [P3/P4 implementation and verification note](../changelog/P3-P4-ingress.md)
records the new fixes, failed attempts, measured bounds and retained evidence
paths. Current reproducible commands are in [test/README.md](../../test/README.md).
The [P1 practice verification note](../changelog/P1-verification.md) records the
later tests, correction and failed attempts separately from the r8 snapshot.
The final runtime check found all application pods Ready with zero container
restarts in the fresh r8 pods, three Aggregators on distinct nodes, all eight
original PVC UIDs and the original Tester Agent UID unchanged. No temporary
E2E application, database, topic or dashboard port-forward remained.

## Evidence policy

Keep successful **and failed** raw runs locally under `.e2e/` and `.vps/`.
Dated local journals are ignored, while reusable guides and this explicitly
archived report are versioned. Never commit inventories, credentials, private
keys, kubeconfig, database files, or raw logs. Paths to ignored evidence in the
historical report are operator references, not files promised in a fresh clone.

## Test tooling cleanup

The former test, load, fault-injection, diagnosis, and evidence-collection files
in `infra/vps/` were deleted, not moved. Future standalone test tooling belongs
in [`test/`](../../test/README.md), split into named P3 and P4 report scenarios.
Unit and PostgreSQL integration regressions stay with their services, not in the
report-test directory. These scenarios do not replace every deleted runner. Historical
commands referring to the removed runners cannot be executed from this checkout.
The 34 offline tooling tests reported before deletion are historical results,
not an available test command now. Existing service-local Go tests and
`scripts/` demonstrations remain available. Reorganization alone proves no criterion;
the candidate-specific E2E results above establish the new evidence.

Assistant reference data, exam keys and final special credentials are scheduled
for the post-deadline testing window. Their absence is not a current practice-test
blocker. Full exam/reference validation is deferred, not fabricated.
