# M2 whole-system review and P2/P5 improvements

For the current build and acceptance results, see the [verification status](../status/README.md).
This review retains historical design context; dated run journals remain local.

Review date: 19 November 2026. The entire M2 specification was reread, including
implementation rules, all P1–P5 criteria, report/demo requirements and appendices
A–C, before selecting these changes. Inherited M1 implementation rules and
the BMKG-only latency target were also checked.

This document separates **implementation**, **regression evidence**, and
**unproven acceptance criteria**. Update: these changes were deployed as
`m2-vps-20261119-03`, followed by the source-clock/valid-critical correction in
`m2-vps-20261119-04`, then fleet-wide cached source health in build `05`.
See [repeated P5 testing status](../status/README.md).
Follow-up: [durable measurement and operations dashboard](P5-observability.md).
The operator deferred HA and requested restoration of the original singleton
coordinator/Kafka/notifier topology. Experimental replication is not active.
The earlier 10k result used image `m2-vps-20261119-02`, not these changes.

Follow-up: [canonical replica consistency correction status](../status/README.md).
Build `10-replication` uses coordinator schema v3 and durable per-target
acknowledgements. Earlier build-09 load results do not validate this change.

## 1. Whole-system constraints and dependencies

| Area | Required behavior | Consequence for P2/P5 |
| --- | --- | --- |
| Platform | Kubernetes, separate kernels, declared topology, one-command deployment, fixed memory requests/limits, Secret/ConfigMap | Do not increase memory limits, share host kernels for demo, or publish storage to make a test pass. CPU limits are not a substitute for better request handling. |
| Storage ownership | Only the owning service can connect; platform enforcement | Field priority uses additional connections owned by Aggregator, never direct Client API access to canonical PostgreSQL. |
| P1 | Partitioned/replicated data; correct cross-store ordering, explicit partial responses, disk-loss repair and restored redundancy | Polling ownership is not storage durability. Recovery shares database/network resources with reads and ingest; notification delivery must not wait for every canonical replica. Audit must bypass read caches. |
| P2 | Distributed reads, single source owner, fenced admission, takeover, scaling, throughput and rolling updates | HTTP serving is active-active; source ownership is independent per source. A lease alone does not prove exactly-once HTTP requests. |
| P3 | Raw failed messages, retry vs parked, replay ordering, Ops-only inspection, durable 202 Field reports | A cursor advances only after durable handoff. Field reports need their own durable inbox/outbox; reserving HTTP capacity does not implement that endpoint. Do not acknowledge before persistence. |
| P4 | Event-time connected components, stable fire IDs, updates/retractions, checkpoints, late data and 60x catch-up | SSE offsets and intermediate fire state need atomic durable transitions and the same fencing discipline. Do not use the M1 timestamp poll cursor for SSE or treat the presence of a SATELIT enum as a working integration. |
| P5 | Critical no-loss/5s, validation, source isolation, circuit breaker, Field priority, autonomous combined-fault recovery, SLO accounting | Protect every bottleneck, including cache fills, DB connections and Kafka commits—not just Client API concurrency. Distinguish sensor virtual time, admission time and actual source availability. |
| Observability / audit | Per-service dashboard, end-to-end durations, structured correlation logs; exact Audit API formats | Bound metric labels. Correlation logs alone are not a complete tracing UI or a unified dashboard. Keep incomplete/unknown measurements visible. |
| Official components | Unmodified Sensor Pantai and Tester Agent; no blocking Tester Agent; node-local store data | No profile/seed/ID-specific branches, no fake sensor replacement, and no moving store data to external network storage to avoid faults. |

Demo evidence must also include the actual assistant resources and Tester Agent
logs, the report's per-criterion evidence and the asynchronous video. Unit tests
and a successful smoke are not substitutes for those deliverables. Exam keys,
reference datasets and satellite credentials must not be fabricated.

M1 P2 permits controlled rejection such as 429 when counts are reported. M2 P2.6
retains an error threshold outside controlled rejection. This **does not** mean
429 is complete successful service, or that the separate rolling-update
criterion (no failed requests) automatically exempts it.

## 2. Chosen invariants

1. One committed source epoch authorizes new input. Lease validation and durable
   admission share the same PostgreSQL transaction/lease-row lock.
2. A cursor is never evidence of downstream delivery; it is a checkpoint after
   durable admission. A crash may replay input, but cannot discard an admitted
   intent or assign it a new delivery version on retry.
3. Canonical and broker delivery are independent, replayable projections of an
   already admitted event. Clear an outbox payload only when Kafka and **every
   assigned canonical replica** have acknowledged it (schema v3).
4. A notifier record is durable before its Kafka offset is committed. Never
   fetch ahead across a failed delivery or unresolved commit.
5. Field classification comes from verified identity, not a caller-controlled
   priority header. Regular traffic cannot consume the Field reserve.
6. Every queue/concurrency pool is bounded; clients do not call the sensor on
   their read path. No unbounded per-hazard metric labels.
7. Termination waits for active HTTP requests and background workers before
   closing their resources. Graceful lease release cannot revoke a newer epoch.

## 3. Implemented improvements

### P2: lifecycle, epochs and bounded observability

- **Durable source polling gate:** commit an identified attempt before HTTP;
  enforce one shared cadence across restarts/scaling/handover, explicitly record
  uncertain results and checkpoint snapshots, and validate each RoundTrip.
  Source-only clients disable implicit reused-connection retry and redirects.
  Ops can inspect the bounded ledger; the new upstream evidence checker rejects
  duplicates and missing completed-cycle logs. See
  [polling design, migration and evidence procedure](P2-polling-ledger.md).
  This does not claim absolute physical HTTP uniqueness or coordinator HA.
- **Actual graceful HTTP drain:** `cmd/server/lifecycle.go` waits for
  `Server.Shutdown` to finish. Previously `ListenAndServe` could return and main
  could exit while the shutdown goroutine was still draining active requests.
  Pollers/delivery workers are joined before resource closure. Read/write/idle
  HTTP deadlines are finite; existing Kubernetes preStop/rolling strategy remain.
  Go explicitly requires waiting for Shutdown after ListenAndServe returns.
  [Go HTTP lifecycle documentation](https://pkg.go.dev/net/http#Server.Shutdown)
- **Conditional graceful release:** expire only the current holder/token row,
  never delete it or reset its token. A rolling owner can hand over early;
  a stale release cannot invalidate a replacement's lease. Crash recovery still
  uses lease expiration, not this cooperative optimization.
- **Bounded metric dimensions:** use registered route patterns instead of raw
  `/events/<id>` paths, normalize unmatched routes and uncommon HTTP methods.
  IDs remain in correlation logs. This prevents an arbitrary-URL workload from
  growing the in-memory metrics map without bound.
- Existing TTL 15s, renewal TTL/3, poll 5s and coordination timeout 2s are retained.
  Do not present their nominal sum as a measured failover bound during arbitrary
  coordinator/network failure. SQL row fencing proves admission safety under
  tested stale-owner cases, not permanent coordinator availability.

Kubernetes termination marks old endpoints not Ready, but existing connections
still need application draining. Readiness cannot compensate for main exiting
before the drain finishes.
[Kubernetes endpoint termination behavior](https://kubernetes.io/docs/tutorials/services/pods-and-endpoint-termination-flow/)

### P5: close the Kafka offset-loss window

Previously a failed `Deliver` led directly to the next `FetchMessage`. Committing
a later message in that partition could acknowledge the failed one as well.
Kafka tracks one committed offset per partition, so this was a real loss window,
not merely a retry performance issue.
[kafka-go commit semantics](https://pkg.go.dev/github.com/segmentio/kafka-go#Reader.CommitMessages)

The notifier now retries the **same record**, using cancelable exponential
backoff 100ms→2s. After durable delivery, it retries the same offset commit before
fetching another message. Shutdown with unresolved delivery leaves the offset
uncommitted. A crash after local persistence but before commit is still safe
because existing bbolt transport/domain deduplication is transactional.

Trade-off: the serial consumer stops fetching all partitions while one record
is unresolved. This favors no-silent-loss over throughput. A later partitioned
worker design must retain a contiguous commit watermark per partition; simply
adding goroutines would reintroduce the bug. Internal poison-envelope handling
still needs a durable quarantine policy if untrusted writers are ever allowed
on the broker; this change does not claim that P3 feature is complete.

### P5: independent canonical and broker projections

Previously canonical write failure prevented Kafka publication. A storage
outage could therefore suppress a critical alert even when Kafka/notifier were
healthy. Schema v3 gives Kafka and each assigned canonical replica independent
workers, persisted acknowledgements and bounded exponential retries. The intent
remains pending until ALL assigned targets acknowledge it. Ordering is enforced
per identity **and destination**, so a failed canonical replica cannot prevent
Kafka from delivering the next alert for that identity.

Trade-off: a notification can precede canonical query visibility. This is an
explicit eventual-consistency boundary, not an atomic PostgreSQL/Kafka commit.
A crash/ambiguous remote acknowledgement can still cause a replay, so existing
version checks and notifier domain deduplication remain mandatory. An already
acknowledged Kafka destination is not replayed merely because a canonical
replica is unavailable. Ready-store anti-entropy repairs historical omissions;
reads reconcile both replicas and explicitly mark observed inconsistency.

### P5: preserve Field priority across the read path

- Client API derives `X-Request-Priority: field` from verified `ClientID`, ignoring
  any incoming priority header. Internal Aggregator authentication runs before
  the priority limiter; possession of the internal key remains a trust boundary.
- Aggregator keeps the same total admission budget. New configuration
  `AGGREGATOR_FIELD_RESERVED_SLOTS` defaults to one quarter of the total (16 of
  64); regular traffic cannot consume that reserve. Zero explicitly disables
  it; configuration rejects negative/reserve≥total.
- Cache hits remain shared because content/filter semantics are identical.
  Cold Field fills do not join a potentially stalled regular fill. Of 64 total
  cache flights, at most 48 are regular, preserving 16 for Field; total entries
  and bytes remain bounded. Concurrent fills replacing one cache entry correctly
  account for its size.
- Each canonical unit has a separate Field read connection pool, max 2,
  min 0, under the **same Aggregator storage owner**. Saturating ordinary
  read/audit connections cannot consume those connections. This adds at most
  two PostgreSQL connections per unit per Aggregator; include those in capacity
  planning (up to 10 per database with five Aggregators).

Trade-off: regular traffic loses some admission capacity by design. Field still
has a finite budget; no system can guarantee serving unbounded Field traffic or
data from an entirely unavailable storage tier. This is class-based admission,
not per-user anti-spam rate limiting and not per-client identity fairness.

### P5: circuit cooldown measures actual failure time

The breaker now starts cooldown when a request fails, not before the request
started. Previously a slow attempt could consume the entire open interval and
allow another request immediately after failure. The failure threshold, cooldown
value and single half-open-probe policy are unchanged. No sensor-ID/profile
heuristics were introduced.

### P5: unhealthy clocks do not suppress valid critical records

A page-level generated_at stall/regression previously hid every parsed item
from admission. Source-clock failures now preserve the parsed page: quarantine
the health evidence, admit schema-valid records through the existing fenced
durable path, and only then advance its opaque checkpoint. Keep the poll
uncertain and breaker unhealthy rather than falsely reporting freshness.
Quarantine/admission failures still prevent checkpoint advancement. Removed an
invented five-minute observed_at/generated_at skew bound that was not present
in the base contract. Detailed component and E2E evidence/limits are in the
[P5 verification status](../status/README.md).

### M1/P5: consistent source freshness on read-only replicas

Per-process STANDBY was a polling role, not a valid health summary for cached
source data. Build 05 refreshes a shared-health snapshot from the poll ledger
once per second in a bounded background worker. No client read queries/waits
for the coordinator. Every read replica can expose the same latest-success and
first-failure `stale_since`, including after owner expiry; failed/aged metadata
refresh explicitly marks the cached health uncertain instead of leaving UP
forever. Actual polling roles and breaker metrics remain per process. Snapshot
propagation is bounded/eventual, not instantaneous consistency. Real-PG and
race tests cover outage/recovery, expired owners and coordinator-refresh failure.

## 4. Acceptance criteria: remaining evidence and risks

The following matrix is a design-review snapshot. Latest deployed-build P5
evidence is summarized in the [verification status](../status/README.md): nine load
rounds and three identity catch-up rounds passed; seven-service metrics and one
asynchronous trace were checked. Fresh critical latency and broad node-loss /
game-day acceptance remain open. Do not read older dashboard/testing gaps below
as meaning that no dashboard or repeated test exists today.

| Criterion | Design / current evidence | Still required |
| --- | --- | --- |
| P2.1 | Three VPS kernels/common Service; readiness, spread and drain code | New-image rolling endpoint/connection evidence; a Service port-forward targets a selected pod and is not a load-balancer benchmark. |
| P2.2 | Durable shared poll cadence/attempt ledger, per-request guard, DB admission fencing, replay versions and evidence checker | New-build upstream logs across scaling/freeze/failover; cross-attempt behavior/rate. Dedupe is not exactly-once physical HTTP. |
| P2.3–5 | 25 real-PG integration tests include takeover/frozen-owner admission, abandoned transactions, poll reconciliation/cadence, shared health and conditional release | Actual node/pod failures, clock shifts and freeze/wake; measure ≤60s and compare full identities/checksums. |
| P2.6 | Earlier tag02 run observed 10k VUs for 129.3s and 3→5→2; scripted thresholds passed, 63.98% 429 | Repeat on this code and confirm upstream criterion during load. Raw negative k6 durations invalidate a clean latency claim until investigated. |
| P2.7 | No ratio established by a single high-VU run | Identical 1-vs-3 test measuring complete successful RPS, excluding 429 and partial responses. |
| P2.8 | Drain regression passes, zero-unavailable rollout configuration retained | Genuine version change under load, complete response validation and no failed requests; no relaxed exception for the run's 429s. |
| P5.1 | Durable admitted intents, independent publication, no offset skip, domain dedupe | Full reference critical-ID reconciliation and validated source-availability→notifier timestamps; ingress-only 422ms smoke is insufficient. |
| P5.2 | Bounds, required/type/value validation, quarantine of invalid observations | Generic malformed/deep/oversize/UTF-8 tests with mixed valid critical records; count invalid critical records explicitly rather than silently excluding them from the no-loss denominator. |
| P5.3–4 | Source workers/timeouts isolated, async reads, breaker and DB-clock progress detector | BMKG-only p95 **<300ms** under sensor failure/PVMBG 3s delay; measure sensor request rate and autonomous reuse after recovery. |
| P5.5 | Field reserve now spans API, Aggregator admission, cache fill and DB reads | Mixed authenticated Media saturation + Field scenario on deployed image, reporting complete success separately from 429 and dropped iterations. |
| P5.6 | Retry/fencing mechanisms improved | Unannounced combined-fault game day, no manual restart/scale/edit inside recovery window, official Tester logs. |
| P5.7–8 | Correlation logs, bounded Aggregator metrics, notifier stats | Unified per-service dashboard/traces and a completed-window error-budget/post-mortem report; not inferred from a smoke. |

### Hard limitations that must not be hidden

1. **Single coordinator / single Kafka / local notifier state:** placing these on
   node A is not HA. Losing A's disk can lose pending intents or broker history;
   replication of P1 canonical records does not replicate these other stores.
   The spec's special non-control-plane fault restriction applies to the
   two-node topology; the three-VPS topology must not assume A is exempt.
   A complete node-A-loss design needs replicated coordinator/broker state and
   a tested notifier rebuild/delivery strategy, not merely more Aggregators.
2. **Upstream exactly-once HTTP:** a process can pause after client-side lease
   validation and before sending. Without upstream cooperation, a finite lease
   cannot exclude every delayed physical request. Do not modify official/mock
   contracts, add fake pattern-specific behavior, or call dedupe proof of this
   requirement. Keep the admission guarantee and the transport limit separate.
3. **Clock authority:** DB wall time removes dependence on each Aggregator's
   wall clock, but is not a monotonic/quorum clock. A backwards shift on the
   coordinator host can extend expiration and break a nominal takeover bound.
   The SQL epoch/row lock still serializes admission; time-based availability
   under that combined fault needs further design and explicit testing.
4. **Sensor trust / rebootstrap:** a fixed absolute offset at bootstrap is
   unknowable from this contract alone. The current persistent high-water mark
   detects stalls/regressions but can need a general rebaseline policy after a
   legitimate source clock reset or a poisoned future timestamp. Never implement
   a seed/date/profile special case. Do not claim HTTP 200 means fresh data.
5. **P3/P4 integration:** durable Field-report intake, full schema replay and
   satellite connected-component/retraction processing are separate work.
   Existing delivery workers enumerate BMKG/PVMBG/Sensor; a satellite producer
   must add its dispatcher/checkpoint integration, not only submit SATELIT rows.

Recommended next design investment is the failure domain of the coordinator and
broker, with an explicit quorum/failover protocol and monotonic/session-based
timeouts; automatic restart of a single lost disk is not data replication.
Per-destination outbox acknowledgements and partitioned notification workers
come after correctness tests, not before them.

## 5. Reliability target and game-day accounting

Keep **zero critical losses** as a hard invariant; its loss budget is zero.
For the required p95≤5s objective, report total reference critical alerts N,
delivered IDs, missing IDs, late IDs and invalid/unverifiable timing separately.
At most `N - ceil(0.95*N)` alerts may exceed five seconds for that percentile
criterion, but **any missing alert still fails no-loss**. Report consumed late
budget and remaining budget; for small N the integer allowance can be zero.

Only close the accounting window after the declared drain deadline; include
alerts observed during faults, not just successful deliveries. Use the assistant
reference/validated availability timestamp domain. Sensor `observed_at` can be
virtual; admission time excludes earlier backlog, and negative cross-host
durations must not be silently clamped to zero as proof of meeting the SLO.
The existing notifier stats alone cannot establish this full accounting.
No completed game-day budget consumption is claimed in this review.

## 6. Verification of these changes

- Windows `go test ./...` passed for Aggregator, Client API and notifier.
- Linux `go test -race -count=1 -timeout=180s ./...` and `go vet ./...` passed for
  all three services. Aggregator's **25 PostgreSQL integration tests** ran against
  a new isolated PostgreSQL 16 container, not production data. Regression coverage
  includes Field reads under saturated regular connections, conditional epoch
  release, projection failure/latency isolation, cache reserve/coalescing,
  verified priority forwarding, bounded labels, full HTTP drain, failed local
  delivery and failed Kafka commit without fetching ahead.
- The slow-failure cooldown regression also passed; six provisioning tests,
  Python compilation and shell syntax validation passed.
- Seven Python polling-evidence tests pass, including an entirely missing
  completed cycle (not only a missing endpoint) and unattributed requests.
- Latest remote regression log: `.vps/design-tests-20261119T061916Z.log`.
  Source snapshot: `/opt/aat/regression/p2p5-20261119T061916Z/` on A. Temporary
  regression PostgreSQL was removed; no application pod, PVC, Tester Agent,
  source profile, application resource limit or load script was modified by tests.

The infrastructure regression runner used for those checks was removed during
cleanup. The results above are historical; future standalone runners belong in
[`test/`](../../test/README.md). No replacement suite or new acceptance result is supplied.

Build 05 is deployed, with repeated scoped P5 E2E documented separately; it is
not a full acceptance pass. Preserve PVCs/credentials and collect evidence for
the remaining criteria. Do not reuse tag02's 10k summary for this build.
