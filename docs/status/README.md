# Source and verification status

Snapshot: 20 November 2026. Documentation cleanup is not a new acceptance test.

## Source versus runtime

- Source baseline: `47d7494` (the main commit restored before this cleanup).
- Uncommitted P1–P3 changes were saved in a local Git stash, not retained in
  this baseline. The stash and raw evidence are not part of a fresh clone.
- VPS deployment was **not rolled back** with the source. It still runs later
  candidates; do not assume local manifests describe the live deployment.
- Before a new rollout, inspect live images, configuration, and database schema
  compatibility. A Git rollback does not undo schema or Kafka partition changes.
- No provisioning, remote deployment, fault injection, or load run is performed
  by this documentation/tooling cleanup.

## Acceptance boundaries

| Area | What the retained report establishes | What remains unproved |
| --- | --- | --- |
| P2 throughput | The last comparable 1–3–3–1 comparison in the baseline report failed the required 2x ratio. | No new throughput pass. Balanced requests alone do not prove scaling. |
| P2 scaling/rolling | Named candidates passed some bounded scale and rolling checks; earlier failures are retained. | Not every candidate, workload, or failure scenario passed. |
| P2 stale ownership | Bounded lease, clock, and process-pause evidence exists. | Whole-VM freeze/stale-process-resume is not established by those runs. |
| P5 delivery | Singleton load/catch-up checks exist. | Fresh critical source-to-notifier p95 at most 5 seconds remains unproved in the baseline report. |
| Infrastructure | Coordinator, Kafka, and notifier use singleton infrastructure in the documented baseline. | Infrastructure HA is deferred, not implied by three Aggregators. |

Read the [historical verification report](../changelog/verification-2026-11-19.md) for exact
candidate tags, workloads, errors, and evidence paths. Those observations must
not be silently reassigned to a different build.

## Evidence policy

Keep successful **and failed** raw runs locally under `.e2e/` and `.vps/`.
Dated local journals are ignored, while reusable guides and this explicitly
archived report are versioned. Never commit inventories, credentials, private
keys, kubeconfig, database files, or raw logs. Paths to ignored evidence in the
historical report are operator references, not files promised in a fresh clone.

## Test tooling cleanup

The former test, load, fault-injection, diagnosis, and evidence-collection files
in `infra/vps/` were deleted, not moved. Future standalone test tooling belongs
in [`test/`](../../test/README.md), which has no replacement suite yet. Historical
commands referring to the removed runners cannot be executed from this checkout.
The 34 offline tooling tests reported before deletion are historical results,
not an available test command now. Existing service-local Go tests and
`scripts/` demonstrations remain unchanged. No new acceptance pass is claimed.
