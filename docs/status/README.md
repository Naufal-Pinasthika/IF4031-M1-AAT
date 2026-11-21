# Source and verification status

Snapshot: 21 November 2026. Results below distinguish repeated pre-deadline
E2E evidence from historical results and the later assistant/exam comparison.

## Source versus runtime

- Integration base: merge `5bafd81` (main `aa87526` plus `feat/wildfire`).
  P3/P4 runtime fixes are recorded in `c34106b`, with deployment, acceptance-test
  and documentation changes in separate commits on `feat/integration`.
- Deployed candidate: `m2-p3p4-5bafd81-20261121T1200Z-r8`. All eight application
  images were rebuilt without layer cache and imported on all three nodes.
  This candidate was built from the pre-commit worktree; its tag identifies the
  integration base, not the exact final source revision. Committing and merging
  the source does not itself rebuild or redeploy the VPS.
  Earlier named candidates and their failures remain separate evidence.
- Three Aggregators run across three nodes. All eight application PVC UIDs and
  the official Tester Agent UID were retained across deployment.
- The private local environment and deployed Secret enable the official satellite
  feed; PVMBG adapter version 2 enables the explicitly supported schema changes.
- Schema changes are additive. A Git rollback does not undo database schema or
  Kafka partition changes. Inspect compatibility before any future rollback.
- An older unrelated P1–P3 experiment remains in a local stash, not applied here.

## Acceptance boundaries

| Area | What the retained report establishes | What remains unproved |
| --- | --- | --- |
| P2 throughput | The last comparable 1–3–3–1 comparison in the baseline report failed the required 2x ratio. | No new throughput pass. Balanced requests alone do not prove scaling. |
| P2 scaling/rolling | Named candidates passed some bounded scale and rolling checks; earlier failures are retained. | Not every candidate, workload, or failure scenario passed. |
| P2 stale ownership | Bounded lease, clock, and process-pause evidence exists. | Whole-VM freeze/stale-process-resume is not established by those runs. |
| P5 delivery | Singleton load/catch-up checks exist. | Fresh critical source-to-notifier p95 at most 5 seconds remains unproved in the baseline report. |
| Infrastructure | Coordinator, Kafka, and notifier use singleton infrastructure in the documented baseline. | Infrastructure HA is deferred, not implied by three Aggregators. |
| P3 durable ingress | Repeated VPS schema/poison/access checks passed. The r8 rerun passed three ordered replay rounds and three blocked-downstream/all-receiver-crash rounds: 60 acknowledgements, zero lost or duplicate results, POST p95 48.84 ms. | Permanent malformed or missing-required-field messages remain parked by policy, not silently repaired. Receiver-process durability is not permanent coordinator-disk-loss durability. |
| P4 stream | Three r8 baseline/fault pairs of 1,772 frames matched the full oracle and each other; catch-up was 18.65–43.85 seconds. The audit-pagination fix passed 66 store-package tests with PostgreSQL enabled and no skips, followed by three complete production snapshots of 3,976 fires. Live feed and the real dashboard were observed separately. | Assistant-reference comparison P4.6 and exam validation belong to the post-deadline window. Controlled-source equality is not assistant-reference equality. |

Read the [historical verification report](../changelog/verification-2026-11-19.md) for exact
candidate tags, workloads, errors, and evidence paths. Those observations must
not be silently reassigned to a different build.

The [P3/P4 implementation and verification note](../changelog/P3-P4-ingress.md)
records the new fixes, failed attempts, measured bounds and retained evidence
paths. Current reproducible commands are in [test/README.md](../../test/README.md).
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
