# M2 P2/P5 design proposal

Status: **design review, not implemented or accepted**. Prepared on 19 November
2026 after rereading the complete M2 specification, Appendices A-C, inherited M1
implementation rules and the M1 latency requirement. Focus remains P2/P5; P1,
P3 and P4 interfaces are dependencies, not permission to replace teammates' work.
No application, infrastructure or test threshold was changed for this review.

Current capacity follow-up: [P2 read-capacity design](P2-read-capacity-design.md)
uses the later build-17/2-CPU failure and measured per-pod routing. The older
rolling incident below is retained as historical context, not the latest result.

## 1. Requirements before mechanisms

| Contract | Consequence for this design |
| --- | --- |
| Kubernetes, separate node kernels, exact Appendix B budgets, platform-enforced storage ownership | Keep the three-VPS topology, service-owned stores and NetworkPolicy. Do not put a complete canonical copy on every Aggregator or use external network storage to evade node loss. |
| P1: partitioning, replicas, globally ordered pagination, node/disk-loss repair, analytical dashboard | Read semantics and replica repair must be agreed with P1. Analytics and Audit must use compatible data/version rules; an operational metrics dashboard is not the required analytical dashboard. |
| P2: active reads, one source owner, clock/freeze safety, takeover within 60s, 10k VU for at least 120s while scaling 3-to-5-to-2, throughput ratio and rolling update | Separate ownership safety, read capacity and process lifecycle. A working election or more pods alone does not establish all eight criteria. |
| P3: raw quarantine, retrying versus parked, ordered replay, durable Field-report 202 | Use a common durable admission interface. Do not advance a source checkpoint before every item has a durable valid-or-quarantined disposition. Field priority is not durable Field-report intake. |
| P4: event-time connected components, stable fire identities, updates/retractions, durable SSE offset/state, 60x catch-up | Reserve ingest/repair capacity against bursts. Agree stable identity, revision and deletion/retraction semantics; a monotonic poll timestamp is not an SSE checkpoint. |
| P5: zero critical loss, p95 at most 5s, validation, source isolation, Field preference, unattended combined recovery | Critical delivery must not wait for all canonical projections. Every durable acknowledgement and every queue is part of the failure model. |
| Audit Appendix A | Preserve exact field names, order, cursor and checksum definition. Unreachable storage requires explicit partial metadata, not an invented complete response or a hanging request. |
| Official components and assessment | Do not alter/block Sensor Pantai or Tester Agent; do not specialize by seed, ID or practice pattern. Keep per-criterion evidence, topology, one-command deploy, report and video obligations separate from unit-test success. |

Memory requests/limits per instance remain: Aggregator 256Mi/512Mi; store and
broker/stream processor 512Mi/1Gi; consumer, API/Auth and other 128Mi/256Mi;
BMKG/PVMBG mock 64Mi/128Mi. Load generators stay outside application pods.
Sensor uses the pinned official example, not a self-authored replacement;
its single-instance Recreate/emptyDir behavior is part of the environment.
[Official sensor manifest](https://raw.githubusercontent.com/susTuna/if4031-sensor-pantai/772f2c6a58b5eab786c718ead5d7b1541760f517/manifest.yaml).

## 2. What the latest failure establishes

The last rolling run returned 54,086 HTTP responses, with no HTTP failures or
429s. Eleven HTTP-200 responses were partial, in one burst before the first
version change, with durations around 491-525ms. Therefore the strict
complete-response check failed; a rolling lifecycle fault is not established.

Local evidence: `.e2e/p2-rolling-20261119T094710Z/`. Old-pod logs were not retained
well enough to identify the exact cause. The 500ms store timeout is a hypothesis,
not a diagnosis. Shared cache fills could account for multiple affected clients.

There are three separate reasons a current read can become partial: failed or
unavailable I/O, insufficient page-coverage proof, and conservative store-health
metadata merged into the result. Each needs its own diagnostic reason. Replica
lag alone is a separate `replication_degraded` condition, not proof of data loss.

## 3. Preserve the useful architecture; clarify the boundaries

The proposed baseline keeps three active HTTP readers, one independently elected
owner per source, transactional fenced admission, and independent projections to
Kafka and each assigned P1 replica. Client reads never synchronously fetch the
sensor or require a live coordinator query. The notifier persists before offset
commit. These are logical responsibilities, not a proposal for more services.

### 3.1 Ownership: election is not fencing

Retain the SQL coordinator for the near-term baseline. Source ownership, epoch
validation and admission must serialize on the same authority. A stale owner
cannot admit new data merely because its process remembers being leader. Keep
bounded server-side transaction timeouts, cancellation on renewal failure,
conditional release and the durable upstream-attempt/cadence ledger.

This follows the fencing principle: the protected resource must reject stale
epochs, not just trust a client-side lock check.
[Kleppmann, distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html).
Kubernetes' election client explicitly does not itself guarantee fencing;
replacing the SQL lease with a Kubernetes Lease is not a complete fix.
[client-go leader election](https://pkg.go.dev/k8s.io/client-go/tools/leaderelection).

Keep the initial 15s lease / 5s renewal settings as a baseline, not a measured
60s guarantee. Measure failure detection, acquisition, first successful fetch
and first admitted event separately. Do not hold an admission lease-row lock
across external HTTP. A replay of an already admitted immutable intent is
different from accepting fresh input under an expired owner.

Two limitations remain explicit design gates:

- Fencing storage does not make physical upstream HTTP exactly-once. A pause
  between validation and transmission remains possible without upstream
  cooperation. Audit real upstream logs, and do not substitute deduplication
  for that requirement or promise arbitrary-failure HTTP uniqueness.
- SQL database wall time is independent of an Aggregator's shifted wall clock,
  but not of a clock jump on the coordinator host. Epoch serialization protects
  admission while expiry/takeover timing can change. Coordinator-clock testing
  and a clock-authority decision remain necessary for a broader claim. A future
  consensus/session-based authority needs a separately reviewed atomic bridge
  to admission; introducing etcd by name does not solve that bridge.

### 3.2 Durable ingestion and delivery

Retain schema-v3 per-target acknowledgements and immutable ingestion versions.
Persist valid events and quarantine dispositions before moving the checkpoint;
an interruption may replay the same page. Kafka and canonical workers proceed
independently, preserving per-identity/per-destination order. Clear payload only
after all required acknowledgements; repair historical canonical omissions from
surviving peers. Source/stage lag and oldest pending critical age must be visible.

The outbox pattern addresses inconsistent dual writes, but duplicate delivery
is still possible and consumers must be idempotent.
[AWS transactional outbox](https://docs.aws.amazon.com/prescriptive-guidance/latest/cloud-design-patterns/transactional-outbox.html).
This is an **at-least-once transport with idempotent/versioned effects** design,
not a global PostgreSQL/Kafka transaction or an immutable full-history event store.

Preserve notifier persist-before-commit and do not acknowledge a later offset
past a failed record. If serial consumption is a demonstrated latency bottleneck,
use bounded per-partition processing and contiguous commit progress, with
rebalance/crash tests; never add unordered goroutines as a throughput shortcut.
Malformed broker envelopes also need durable disposition before commit rather
than relying only on a log line. Admission and recipient confirmation are
different stages; an alert may reach the notifier before canonical visibility.

### 3.3 Read contract: complete, fresh and redundant are different

Proposed ordinary-read contract: bounded-staleness cache over an explicitly
eventually consistent canonical view. `partial=false` certifies the requested
page within the declared read-view contract, not visibility of every newly
admitted intent, a linearizable latest value or a global cross-database snapshot.
Replica redundancy and source freshness are reported separately.

Retain both-replica reconciliation initially, but make its work/proof explicit:

1. Fetch only bounded sorted candidate prefixes from each assigned replica.
2. Resolve the latest version of every candidate before applying mutable filters;
   do not resurrect an old severity/time match. Include agreed retraction rules.
3. Separate **requested result size** from **internal fetch size**. After refill,
   prove the requested prefix plus the cursor lookahead, not the entire enlarged
   fetch. Keep the requested boundary fixed while expanding candidates.
4. Bound refill rounds, total candidates, concurrent SQL operations and total
   deadline. Failure to prove coverage remains explicit, never silently complete.
5. Establish stable per-store views for multi-query reconciliation. Evaluate
   short read-only Repeatable Read transactions covering that store's relevant
   prefixes/lookups. Do not hold snapshots across HTTP pagination or idle clients.
6. A per-store snapshot is not a shared snapshot across stores. Define ordinary
   pagination under concurrent updates and run live differential tests; a static
   oracle does not prove concurrent completeness. If stable historical pagination
   is required, jointly design version history plus an as-of cursor with P1/P4.
   A token containing `max(version)` alone cannot recreate overwritten versions.

PostgreSQL Read Committed can expose different snapshots to successive queries;
Repeatable Read stabilizes one database transaction, not independent servers.
[PostgreSQL isolation](https://www.postgresql.org/docs/16/transaction-iso.html).
This motivates the proposal, not a claim that a local isolation change solves
distributed consistency.

Audit bypasses the response cache. Preserve Appendix A's partial behavior for
unreachable units even if a reachable peer happens to contain all observed IDs.
Ordinary P1 recovery also keeps explicit degraded responses. Do not loosen the
normal rolling test to hide transient partial pages; label fault/recovery tests
separately and validate whatever completeness is actually claimed.

| Read alternative | Benefit | Reason for decision |
| --- | --- | --- |
| Bounded dual-replica union with clear coverage/view semantics | Preserves current P1 placement and repairs; limited migration | Recommended first, gated by concurrency/correctness tests and measured cost. |
| Certified preferred replica using applied progress | Could remove unnecessary peer reads | Defer: reachable/ready or maximum version is not a no-gap certificate; parallel projections can leave holes. Needs per-partition contiguous coverage and recovery epochs. |
| Per-shard primary with synchronous replication | More conventional authoritative shard reads | Valid alternative, but requires a joint P1 ownership/failover redesign; two-copy synchronous acknowledgement trades write availability for durability. Not a small P2 patch. |
| New full-copy read database/cache on every node | Simple local reads | Reject as a default: full canonical copies conflict with P1 placement and memory constraints; also creates another consistency system. |

### 3.4 Resource isolation and deadlines

Protect distinct work classes: authenticated Field reads, ordinary reads, new
ingest/projection writes, repair/backfill, and control/health operations. Preserve
Field reserves through Client API, Aggregator, cache fills and DB acquisition.
Keep identity-derived priority; do not trust caller-supplied priority headers.

Use bounded pools/semaphores and short queues, not unbounded new connections.
For each database, budget the maximum simultaneous rollout/scaling population:
sum over all overlapping pods of (Field + ordinary + ingest + repair + probe
connections), plus migration/admin headroom, below its connection/memory budget.
Five desired pods can temporarily coexist with surge and terminating pods.
Independent pools still share database CPU, disk and locks, so cap background
repair IOPS/rows/time too. Reserve coordinator capacity for renewal/admission
instead of letting slow delivery I/O occupy every control connection.

The basis is dependency isolation rather than timeout tuning alone.
[AWS dependency isolation](https://builder.aws.com/content/3EuxuD6bWtQ6gEp9FaKQfd3Z2AM/using-dependency-isolation-to-contain-concurrency-overload).
Reject excess ordinary work early with reported 429s instead of consuming the
Field reserve. Keep successful, rejected, partial and timed-out counts separate.
[Google SRE handling overload](https://sre.google/sre-book/handling-overload/).

Measure pool-acquisition time separately from SQL time, connect time, lock wait,
merge time and JSON time. Preserve end-to-end deadlines; sub-budgets must fit
inside them. Derive timeout changes from observed tails and the M1 BMKG-only
p95 <300ms requirement, not from the desire to make a test pass. Retry only safe
operations, in one bounded layer per operation, with capped backoff/jitter and a
retry budget. Warm representative dependency connections before routing traffic.
[AWS timeouts/retries](https://aws.amazon.com/builders-library/timeouts-retries-and-backoff-with-jitter/).
Do not extend cache TTL, serve expired data without metadata or introduce
stale-while-revalidate solely to hide a failure.

### 3.5 Startup, degraded readiness and termination

Distinguish startup readiness, ability to serve degraded data, source health and
process liveness. A single successful independent pool ping is not a full read-
path check; conversely, withdrawing every Aggregator when one shared replica
fails would defeat P1's degraded-service requirement.

Proposal: validate schema compatibility, initialize usable unit state, warm
representative read connections and complete a bounded representative read before
first traffic. In an actual storage outage, allow explicitly degraded readiness
when useful reads remain possible; export that mode. Subsequent probes should
use inexpensive cached state with hysteresis, not cause a query storm or flap
because of one busy query. Do not require current sensor/coordinator availability
for already-running canonical readers.

For normal version changes: schema-compatible expand/contract releases,
`maxUnavailable=0`, bounded surge, a measured readiness stabilization interval,
and enough physical headroom. Drain routed/existing connections before resource
closure, cancel/join workers, then release resources. Cooperative source release
must not invalidate the replacement epoch. PDB does not guarantee survival of an
involuntary node failure, and pod counts alone do not prove drained connections.
[Kubernetes termination behavior](https://kubernetes.io/docs/tutorials/services/pods-and-endpoint-termination-flow/),
[Deployment readiness stabilization](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#min-ready-seconds).

### 3.6 Sensor resilience and critical-alert accounting

Keep per-source isolation: independent workers, deadlines, limits and resumable
checkpoints. Client reads use stored data/status, never wait on sensor polling.
Transport success is not freshness. Treat source virtual time, station mapping,
schema validity, cursor progress and observed availability as distinct evidence.
Do not reject schema-valid critical events solely because page-level time is
stalled/regressing; quarantine the health evidence separately.

Breaker/backoff must retain a system-wide source request budget across ownership
changes, not reset into a probe storm when a new pod becomes owner. Design durable
cooldown/probe ownership with the polling gate; one bounded half-open probe,
capped jittered retries and no per-client retries against the source. Define a
generic rebaseline policy for legitimate source resets; no seed/date/ID branches.
An opaque cursor must not be parsed as a numeric observation ID.

The critical path receives admission/dispatcher reserves without violating
per-identity order. SQL critical-first ordering cannot bypass an existing Kafka
backlog; measure publish-to-fetch lag before deciding on partition concurrency
or a separate priority lane. More replicas are not automatically that lane.

Use separate measures: source availability-to-recipient persistence, admission-
to-publish, publish-to-fetch, and fetch-to-persistence. Use the assistant's
validated availability/reference domain when provided. A monotonic observer can
give an interval/upper bound only when source coverage/freshness assumptions are
verified; a possibly stale empty page is not an unquestionable availability
oracle. `observed_at` or admission time alone cannot establish the full 5s SLO.

Reconcile all reference critical IDs against durable recipient records. Report
missing, duplicate effects, late, invalid-critical and timing-unknown cases
separately. Missing is a failure, not a sample dropped from the p95. No fresh
samples means unproven, not zero latency. The loss budget is zero; the required
5s objective is evaluated over the declared full window, with late allowance
`N - ceil(0.95*N)` under the declared nearest-rank convention, never by averaging
per-stage p95s. Synthetic fixtures exercise generic cases but are not official
sensor acceptance evidence. [Google SRE SLO methodology](https://sre.google/workbook/implementing-slos/).

### 3.7 Kafka and failure domains

Do not scale Kafka as the first response to partial PostgreSQL reads. Kafka
replication protects broker history/availability; partitions and consumer work
allocation govern a different dimension of capacity. Kafka transactions also do
not make an external bbolt effect automatically exactly-once.
[Kafka delivery/replication design](https://kafka.apache.org/40/design/design/).

HA stays deferred, per the operator's decision. A future design would need more
than `replicas: 3`: unique broker/controller identities, quorum, topic and offset
replication, appropriate ISR/ack policy, local volumes, placement and failover
tests. It must also address coordinator durability and notifier replay/dedup
state. Replicated Kafka cannot restore an intent lost before publication or
prevent duplicate effects after losing the recipient's deduplication state.

Therefore **arbitrary node/disk-loss no-loss and autonomous recovery are not
closed by this baseline**. Three nodes do not make A exempt. These are P1/P2/P5
cross-cutting dependencies, not solely a P1 matter. Do not change the requested
fault scope or silently claim full P5.6 while singleton failure domains remain.

## 4. Evidence plan before calling this design ready

| Gate | Required evidence |
| --- | --- |
| Explain the 11 partial responses | Per-fill ID linked to every waiting correlation ID; partial reason, unit, partition, phase, timing and relevant health transition. Retain old/new pod logs during the run. |
| Read correctness | Independent latest-data oracle; stale mutable filters, missing copies, retractions, ties, cursor/offset/refill combinations; concurrent writes/repairs. Every complete claim must equal its declared oracle/view. |
| Resource design | Pool acquire/SQL/lock/merge timings, DB connections/waits, CPU throttling, heap/GC, queue age and repair activity, measured for 1/3/5 pods plus rollout overlap. Do not guess new pool sizes. |
| P2.1-5 | Actual routed endpoint removal; upstream logs without multiplied polling; first successor data within 60s; +45/-30 worker clock changes; actual VM freeze/wake, not just process pause. Test the coordinator clock separately. |
| P2.6 | Unchanged 10k sustained workload for at least 120s on the new build, Ready 3-to-5-to-2, all response categories, upstream audit and post-window identity reconciliation. |
| P2.7 | Identical complete-response throughput workload for 1 and 3 replicas, repeat 1-3-3-1 and report generator contention, cache state and rejection/partial counts. No 429 counted as throughput. |
| P2.8 | At least three retained strict rolling rounds on two genuinely different compatible binaries, actual serving version/endpoint/connection evidence, no failed complete-response checks. Then combine rollout with the declared required load profile; do not imply a 50-VU round proves 10k behavior. |
| P5.1-2 | Repeated official-source coverage windows; full critical identity reconciliation, independently valid timing, malformed mixed pages and quarantine-before-checkpoint crashes. Do not stop after a favorable window. |
| P5.3-5 | Sensor/PVMBG faults while measuring BMKG-only p95 <300ms; no upstream probe storm; verified Media saturation plus complete Field service and autonomous recovery. |
| P5.6-8 | Declared unattended combined-fault window, Tester logs, durable-state reconciliation, end-to-end traces, SLO/error-budget report and post-mortem. Exam is deferred, not simulated as passed. |

Diagnostic labels must be bounded (`query_timeout`, `unit_unavailable`,
`coverage_unproven`, `metadata_degraded`, for example); correlation IDs belong in
logs/traces, not metric labels. Record a shared-fill leader ID and waiter count
so a burst is not mistaken for many independent DB failures. Do not log secrets
or raw sensitive payloads into the public dashboard.

## 5. Decisions and open questions

Recommended decisions: keep SQL fencing/outbox; retain P1 placement; first fix
and prove the bounded read contract; isolate work budgets; make startup/drain
representative; keep critical publication independent; defer Kafka replication
until its own failure-domain decision. No Redis, new broker, full-copy read
store, wholesale rewrite or higher memory limit is justified by the 11 failures.

Before implementation, resolve these gates explicitly:

1. Diagnose timeout versus coverage versus metadata in the retained failure;
   reproduction instrumentation is proposed, not a completed diagnosis.
2. Agree ordinary/Audit pagination and concurrent-update/retraction semantics
   with P1/P4. Decide whether stable historical snapshots are actually required.
3. Measure connection/repair budgets rather than select new numbers by intuition.
4. Keep coordinator-clock behavior and the deferred durable-state HA scope as
   visible limitations until a separate design closes them.
5. Confirm the official critical-availability oracle when exam resources arrive;
   do not redefine the source-to-recipient SLO around convenient timestamps.

This proposal is ready for discussion and targeted design validation, **not a
statement that P2/P5 now pass**. See [current implementation/results](../status/README.md)
for what has actually been deployed and measured.
