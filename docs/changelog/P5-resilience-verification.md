# P5 resilience fixes and repeated VPS verification

Snapshot: 22 November 2026 WIB. **This is not an overall P5 pass.**
Exam and assistant reference resources remain deferred to their official release.

This is the earlier singleton checkpoint, retained with its failures. Later
critical-lane, redundant-notifier sandbox, contract, source-observation and
gateway/trace results are recorded in the [current status](../status/README.md).
The candidate tags and remaining-work table below describe this checkpoint,
not the latest deployed images.

## Deployed scope

Branch: `fix/p5-resilience`, based on main `c6bc838`. The candidates were built
from this uncommitted worktree; their tags do not identify a committed revision.

| Component | Final candidate | State |
| --- | --- | --- |
| Aggregator | `m2-p5-source-index-20261122-r1` | Three Ready instances on A/B/C |
| Notifier | `m2-p5-notifier-20261122-r5` | One Ready instance on A; original bbolt volume retained |
| Auth | `m2-p5-auth-priority-20261122-r2` | Ready; original credentials and sessions retained |
| Sensor Pantai | Original pinned official digest | Same pod/state/config; still OOMKilled/CrashLoopBackOff |
| Kafka/coordinator | Original singleton topology | No HA rollout, reset, topic-offset reset or original data deletion |

All eight original application PVC identities and the official Tester Agent UID
were retained. The temporary notifier topics/PVCs and isolated sensor namespaces
were removed after testing; their disposable data is not a backup. Source image,
practice profile, seed, speed and resource-table limits were not changed.

## Causes and surgical fixes

The Karpathy-guidelines scope was followed: reproduce defects, change the
responsible paths, retain failed evidence and avoid speculative infrastructure
replication or relaxed acceptance thresholds.

1. **Notifier monitoring amplified work and memory with history size.** Each
   scrape decoded the entire notification database, allocated latency arrays and
   sorted copies. The live file was approximately 758 MiB; overlapping scrapes
   timed out and the earlier notifier was OOMKilled at its 256 MiB cgroup limit.
   This is a demonstrated amplification mechanism, not a captured heap profile
   proving that it was the exclusive allocation source.
   Statistics now update atomically with delivery/deduplication. Three persistent
   ordered latency-count indexes maintain exact nearest-rank p95 values; a scrape
   reads one small summary. Adding a sample moves its percentile rank by at most
   one sorted value. There is no unbounded latency slice or approximate percentile.
   Legacy backfill commits bounded 4,096-row batches plus a restart cursor before
   consumption starts. An older writer is detected from the processed counter;
   only derived statistics are rebuilt. Original delivery/dedup records remain.
   `InitialMmapSize` reserves virtual address space, not an extra resident dataset.

2. **Kafka health timeout did not bound metadata reads.** A fake broker that
   accepted TCP but never answered reproduced a 50 ms context remaining blocked
   beyond 200 ms, repeatedly. Metadata connections now receive the deadline and
   close on cancellation. HTTP health callers share one in-flight dependency
   probe, each with a bounded wait; an uninterruptible store operation cannot
   accumulate workers. An earlier busy-gate candidate falsely returned 503 when
   Kubernetes and HTTP probes overlapped; the final candidate coalesces those
   callers instead. Real dependency failures still return 503 and name the phase.

3. **Source-filtered reads scanned unrelated coastal history.** PostgreSQL plans
   discarded about 158,545 rows before returning 11 BMKG rows on partition 0.
   BMKG/PVMBG query executions were 214–319 ms even before request overhead.
   The additive `p1_source_occurred(partition_id,source,occurred_at,hazard_id)`
   partial index matches the existing live-row predicate. Actual plans switched
   to source-index range scans, approximately 0.24–0.50 ms, with no query-budget,
   filtering, consistency or resource-limit change. Existing databases were
   prepared concurrently; new empty schemas create the same index normally.

4. **Auth admission had no Field reserve.** Earlier overload rounds had 2/601
   and 3/601 Field failures before a business request was made. The shared Auth
   limiter allowed regular issuance to occupy its whole budget; a unit test
   reproduces starvation and verifies the new boundary. Token issuance/refresh
   now reserves capacity for configured Field credentials. Both client ID and a
   constant-time secret-hash comparison are required for classification; headers
   cannot claim priority. Database authentication, enabled-client checks, scopes
   and refresh rotation remain authoritative. Total admission capacity is not
   increased. `AUTH_FIELD_RESERVED_SLOTS` defaults to one quarter of
   `AUTH_MAX_IN_FLIGHT`; the live 32-slot configuration therefore reserves eight.

## Repeated evidence

All report runners execute from the laptop/WSL against VPSs. k6 runs on the
laptop, not inside an application pod. Raw data remains ignored under `.e2e/`.

| Scenario | Result and scope | Evidence suffix under `.e2e/report-tests/p5/` |
| --- | --- | --- |
| Final notifier health/scrapes, 3 × 60 s | All HTTP responses 200, no process restart; round p95 36/98/110 ms, including concurrent overload activity | `00-health/20261121T191344Z-28b4f55c` |
| Genuine sensor critical observation, 3 × 60 s | Inconclusive: no successful observation pages/new latency samples; no zero-loss or five-second claim | `01-critical/20261121T185652Z-041ac4a0` |
| Isolation, 50 VUs, 3 × 60 s | Passed: complete-response errors 0.0160% / 0.0166% / 0.0088%; BMKG p95 47.1 / 45.4 / 48.8 ms | `02-load/20261121T190753Z-1860e7ad` |
| Final priority, 300 Media VUs + 10 Field requests/s, 3 × 60 s | Passed: 601 + 601 + 600 Field requests, zero Field failures/dropped iterations; 263,346 / 240,606 / 268,067 controlled Media 429s | `02-load/20261121T192423Z-44b90a55` |
| Run-owned notifier SIGKILL, 3 rounds | Passed identity/durability check: 1,080 records, 540 unique notifications, 270 critical, no missing/duplicate identities; recovery 37.4–39.2 s | `03-recovery/20261121T191638Z-50d7730c` |
| Final injector verification | One further round verifies exit 137 explicitly, 360 records → 180 unique notifications including 90 critical; 38.5 s recovery | `03-recovery/20261121T192831Z-4f5bdd8a` |
| Genuine outage/breaker, 3 × 30 s | Passed outage-phase bound: 10 upstream requests/window, configured bound 42; last-data Field read p95 107 / 54 / 62 ms | `04-circuit/20261121T191258Z-84f7ad74` |
| Operations dashboard, 3 quiet observations | Passed live series and Client API → gateway → Aggregator correlation lookup | `05-observability/20261121T193010Z-79f44b79` |
| Operations trace during overload | Failed: bounded log lookup retained Aggregator/gateway steps but missed Client API steps; quiet success does not erase this | `05-observability/20261121T192507Z-51131a94` |

The 50-VU run used the existing practice source settings, not a newly forced
PVMBG fixed-three-second configuration. It is not a replacement for every M1
latency scenario or for the P2 10,000-VU scale-out proof. Report total HTTP rate,
successful business rate and controlled rejections separately.

Final independent snapshot: `.e2e/p5-final-20261121T193203Z/`. Notifier working-set
memory was about **17 MiB**, Go heap **5.6 MB**, goroutines **15**, Prometheus
target Up and scrape duration **2.31 ms**. File-cache charges are not Go heap or
working-set measurements. Sensor had **70** restarts and its latest termination
was still `OOMKilled`; this external process is not declared repaired.

The three-minute health/durability measurements above used notifier r4. Final
r5 changes only startup logging to use the service JSON logger and a startup
correlation ID; it does not change delivery, statistics or probe logic. Its
scoped rollout retained all PVCs. Deployment evidence:
`.e2e/notifier-consumer-update-20261121T193506Z/`.
Final r5 smoke also passed three ten-second windows: all 90 concurrent
health/statistics/metrics responses were 200, p95 at most 41 ms, no restart.
Evidence: `00-health/20261121T193714Z-0df168ea` under the report-test root above.

Earlier failures remain failures: startup/backfill health test, busy-gate health
candidate, pre-index isolation campaign, two pre-reserve priority campaigns and
the first crash injector (in-namespace SIGKILL did not actually kill PID 1).
The corrected injector resolves the runtime container identity to a host PID
and verifies exit 137. It kills only the run-owned container, not the host or
the production notifier.

## Remaining acceptance gates and error budget

| P5 criterion | Current boundary |
| --- | --- |
| 1: zero critical loss, p95 ≤5 s | Not passed as a whole. Official-source coverage is unavailable. The crash test proves no lost generated identities, but its critical ingress p95 is about **36.7–37.0 s**, not five seconds. |
| 2: invalid observations quarantined | Validation/quarantine unit regressions pass; no fresh official-source invalid-input E2E sample in this window. |
| 3: other sources retain M1 latency | Repeated practice isolation passes; fixed-three-second PVMBG and later exam combinations remain separate gates. |
| 4: bounded outage calls, fast reads, automatic source return | Outage phase passes. Genuine source return was not observed; unit breaker recovery is not a substitute for that E2E phase. |
| 5: priority under overload | Final repeated campaign passes, including token acquisition. Prior failed campaigns are retained. |
| 6: combined autonomous recovery | Notifier-only hard-crash recovery passes without operator repair. No new whole-system combined-fault acceptance run; singleton dependencies remain deliberate boundaries. |
| 7: explain from measured evidence | Live panels and quiet correlation lookup pass; high-load trace retention remains deficient. |
| 8: reliability target/budget | Target: no missing critical IDs, and at least 95% durably delivered within five seconds, including fault windows. Loss budget is zero; late budget is `N - ceil(0.95*N)`. No official-source samples means no calculable source-level latency budget, not 100% success. Component crash latency already warns that the singleton path cannot promise the target. |

The final production consumer still has **195,606** records of general-topic
backlog (63,410 / 65,481 / 66,715 per partition). Readiness is not catch-up.
New critical events can wait behind that backlog; adding Kafka brokers alone
does not create consumer priority or remove the per-record serial delivery path.
Next design work is an independently scheduled durable critical lane plus a
deliberate receiver/rebalance availability design, not an offset reset or ignored
messages. HA remains deferred as requested. High-load tracing needs bounded
indexed/streamed retention rather than an ever larger synchronous log tail.

The official image/config/state was not reset or reverse-engineered to turn an
inconclusive observation into a pass. Fresh isolated instances stayed healthy
for several minutes with contract traffic, including the opaque production
cursor. That does not reproduce 40-hour state or establish the internal OOM
allocation cause. Raw black-box evidence is in `.e2e/p5-sensor-184903/`,
`.e2e/p5-sensor-185250/` and `.e2e/p5-diagnosis-20261121T183713Z/`.

Local service tests, repeated notifier/Auth regressions, `go vet`, Python syntax
checks and actual PostgreSQL before/after plans passed. PostgreSQL integration
tests without their disposable-database environment still skip; this note does
not re-label them as newly executed database tests. Use the executable cases in
[`test/README.md`](../../test/README.md) to reproduce the explicitly scoped gates.
