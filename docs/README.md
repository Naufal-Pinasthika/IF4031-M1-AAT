# P2/P5 design and verification status

Snapshot: 19 November 2026. Implementation is not a claim that every acceptance
criterion passes. Dated operator journals and raw evidence stay local and are
ignored by Git; this page summarizes their relevant results and limitations.

## Maintained guides

- [P2 read-capacity redesign](P2-read-capacity-design.md): current failed-run
  routing evidence, generator/transport/admission boundaries and gated next design.
- [P2 read-path implementation](P2-read-path-implementation.md): bounded query
  projection, separate fill/HTTP/compression budgets, L7 gateway and CQRS decision.
- [P2/P5 design proposal](M2-P2-P5-design-proposal.md): whole-M2 contract review,
  primary-source research, proposed decisions and validation gates; see the
  current implementation below rather than treating every proposal as deployed.
- [Whole-system design review](M2-P2-P5-design-review.md): dependencies,
  alternatives, trade-offs and the historical acceptance matrix.
- [P2 polling ledger](P2-polling-ledger.md): ownership, fencing, request evidence
  and coordinator schema/rollback constraints.
- [P5 observability](P5-observability.md): source-to-notifier measurement and the
  private operations dashboard.

## Current implementation

Latest deployed candidate: `m2-vps-20261119T163045Z-read-capacity`. The gateway
follow-up section distinguishes its checks from the earlier `160708Z` throughput
comparison and `145321Z` capacity results. Build-17 evidence below is retained
historical context, not automatically a pass for these newer candidates.

Three Aggregators serve reads behind one Service, with independent source
leases. Coordinator schema v3 atomically admits each event and independent
projection jobs for Kafka and each assigned canonical replica. Payload cleanup
waits for every target acknowledgement; retries and ready-store anti-entropy
repair missing copies. This is not exactly-once physical upstream HTTP.

Read completeness reconciles latest versions across replicas and checks bounded
page coverage. `partial` means completeness cannot be certified;
`replication_degraded` can describe a lagging reachable copy even when the union
is complete. Failed reads and unproven pages are never relabelled complete.

The historical baseline Aggregator is `m2-vps-20261119-17-recovery-state`. Its compatible
rolling candidate is `m2-vps-20261119-15-page-encoding`, not a pre-v3 binary.
Build 13 added bounded slow-query/partial/shared-fill diagnostics. Build 14 added
opt-in loopback-only CPU profiling; the runtime flag is now removed. Build 15
reuses immutable encoded page fields inside the existing bounded read cache,
while computing source health per response. Initial code-only comparisons kept
the 750m CPU quota. After that quota still caused overload, the deployed/tested
CPU limit was explicitly raised to 2 per Aggregator, using existing VPS capacity.
Memory remains 256Mi request/512Mi limit; cache TTL, database deadlines, workload
and acceptance thresholds were not increased. The GOMAXPROCS=1 experiment was
not a sufficient fix and its override was removed. Capacity results below are
**2-CPU results**, not a claim that code changes alone solved the 750m case.
Builds 16/17 recognize raw network timeouts, separate probe and repair loops,
generation-guard health observations, reset interrupted idle connections on
recovery and partition the existing connection budget for maintenance/live work.
Ordinary replica SQL admission is bounded without increasing total connections.
Build 17 additionally requires durable metadata to request a copy: a local
connectivity flag alone cannot turn a ready volume into `recovering`.
These mechanisms are not a claim that every overload case is solved. A v2-to-v3
worker cutover is not a rolling-update pass. Coordinator, Kafka and notifier
remain singleton infrastructure; HA was explicitly deferred.

## Evidence and open criteria

| Area | Observed result | Limitation |
| --- | --- | --- |
| Regression | Real-PostgreSQL race/vet suites passed for Aggregator, Client API and notifier. Four focused coverage/replication tests passed ten repetitions, including 650 complete-page oracle comparisons. | Bounded fixtures do not prove all possible failures. |
| Canonical durability | Three 35-second canonical-store process pauses recovered automatically; independent broker delivery continued and queued projections drained. Prior four missing assigned copies were repaired. | Process pause is not whole-VM freeze or disk-loss proof. |
| P2 control retest | Two rounds on build 17/current 2-CPU allocation exercised worker clocks on B and C at +45/-30 seconds, with automatic restoration and zero observed admission epoch-order violations. Six per-source owner-pod deletions completed successor polls in 2.508-9.357 seconds. Initial/final topology and ready Service targets matched three pods on three nodes. | Observed bounded failover/admission evidence, not an official payload/checksum oracle or proof for every pause point. Immediate pod deletion does not itself confirm the exact instant the old process stopped. |
| P2.5 pause evidence | Two 28-second Aggregator process pauses removed the old ready Service target, observed zero old-epoch admissions and completed successor polls in 18.632/27.270 seconds. | **Still unproven.** The second round checked original container identity/restarts and found a liveness-triggered restart, so its stricter stale-resume check failed. The first round lacked this identity check. Neither is whole-VM suspend/resume; do not claim an old process safely resumed based on these results. |
| P2.6 | Build 17 with 2 CPU/512Mi passed two unchanged scale runs: observed 10k VUs 123.418s and 126.544s, Ready 3-to-5-to-2, at least two Ready in every sampled snapshot. Complete/partial/429/other counts were 773,897/1/87,930/0 and 750,949/503/101,336/140. Even counting partial as failure, rates excluding 429 were 0.0001292% and 0.08555%. | These are explicitly higher-CPU results, not code-only improvements. Rejections/partials/errors remain, and generator C shares application resources. The 750m build-16 repeat had 210,155 partials; build 17 at 750m and the GOMAXPROCS=1 trial also retained partials and zero-Ready snapshots. Earlier failed/incomplete runs remain retained; no full P2 pass is claimed. |
| P2.7 | **Failed the current retest.** Build 17/2 CPU, unchanged 1,000 VU/60 seconds per case, order 1-to-3-to-3-to-1: complete throughput 4,828.963 / 3,808.151 / 3,928.272 / 4,684.482 responses/s. Paired ratios 0.788606 and 0.838571, below 2.0. All four cases had zero partial/malformed/other responses; 429 counts were 85,720 / 7,910 / 5,367 / 106,961. | Individual error-rate passes do not override the failed throughput ratio. Generator C shares resources; this retest does not establish the bottleneck. Historical build-11 ratios 51.87/46.85 used the previous allocation and do not prove this build passes. No workload/threshold/resource change was made to force a pass. |
| P2.8 | Build 17-to-15-to-17 at the current 2-CPU allocation passed strict 50-VU/120-second rolling: 54,173 requests, zero HTTP failures/partial checks, p95 16.33ms, both distinct binaries fully Ready. Earlier build-15 and build-13 rolling rounds are retained. | This is one current-build rolling round, not 10k rolling proof. The historical build-12/build-11 run failed with 11 partial responses; exact cause remains unproven. |
| P5 | Nine singleton load rounds and three identity catch-up rounds passed their checks; seven-service metrics and an asynchronous consumer trace were observed. | No fresh critical source events were available to prove source-to-notifier p95 at most 5 seconds. Old 422ms admission-to-notifier timing is not that SLO. |

Exam-profile testing remains unavailable until its scheduled release. Broad
combined-fault/node-loss acceptance and coordinator/Kafka HA remain open.

## Read-diagnostics findings

The build-13 scale run reproduced partials from prefix-query timeouts, including
one fill that served the same partial result to 48 waiters. Captured logs separate
acquire/connect time from query/scan time. Some failures occurred after immediate
acquisition; others exhausted the deadline acquiring a connection. No observed
coverage-proof failure was needed to explain these new timeout examples.

Read-only CPU snapshots showed the unchanged 750m quota and cumulative throttled
period ratios around 66% on two serving pods, despite host headroom. This is a
demonstrated bottleneck and a strong contributor, not a timestamp-aligned causal
proof for every timeout or the old rolling failure. PostgreSQL query/scan timing
also includes network/client scheduling; it is not SQL-server execution alone.
Do not blindly enlarge pools, CPU limits or timeouts on that basis.

Follow-up CPU profiles now verify GOMAXPROCS=2 with the same 750m quota on all
three pods. HTTP processing dominated the sampled CPU, including repetitive
page encoding and socket writes; logging was approximately 6-10%, not the sole
cause. Build 15's immutable encoding reuse reduced that cost, but repeated scale
tests still showed timeout/partial bursts, particularly in the two-replica phase.
Retained logs showed raw socket timeouts and independent probe failures followed
by unavailable metadata. Builds 16/17 correct timeout classification and separate
health/repair work; a real outage fixture also exposed stale pooled connections,
which now reset on recovery. The first loop split exposed a repair/probe race;
build 17 checks durable readiness before starting a copy. Regressions include a
fixture trigger that rejects unnecessary copies of ready data. Code-only 750m
and GOMAXPROCS=1 experiments still showed overload; the later 2-CPU runs are
reported separately above. Failed reads remain honest partial/error results.

The later throughput retest and two control rounds were followed by bounded
source/Audit comparisons, both without missing/duplicate identities. The final
comparison covered BMKG 2,393 reference IDs, PVMBG 1,789 and Sensor Pantai 126,066.
All 184 supplied mock upstream requests passed correlation checks; polling cadence
passed and no same-epoch multiple holders were admitted. The preceding throughput
comparison covered 249 mock HTTP requests and also passed. Canonical targets
accepted before the earlier two 2-CPU scale runs had no pending projections at
that earlier final inspection. Official full-payload checksum,
physical HTTP exactly-once and fresh critical-alert latency were not established.
P5 work/testing remains on hold; no P5 fault/load campaign was resumed.

Local Go test/vet and nine Python tooling tests passed. Real-PostgreSQL race/vet
regression passed on the second run; the first run's unchanged 100ms locked-copy
fixture returned storage-busy unexpectedly and remains retained as an intermittent
failure. Dated detailed notes stay local in `P2-read-diagnostics-2026-11-19.md`.
The later serialization/profiling regression also passed real-PostgreSQL race/vet
for all three services, local Go test/vet, ten focused cache/contract repetitions
and eight Python harness tests. `P2-cpu-profile-2026-11-19.md` retains the measured
comparison and its failures locally; raw profiles are ignored/private.
The later health/recovery correction passed full PostgreSQL/race/vet and ten
focused repetitions, including a TCP outage, pool isolation and a trigger that
rejects unnecessary repair. Six infrastructure tests passed with the 2-CPU/512Mi
manifest. The retained narrative and all failed/intermediate experiments are in
local `P2-health-bulkhead-2026-11-19.md`; they were not rewritten as successful.

Final runtime after the control retests: three replacement build-17 pods Ready on
distinct nodes, each currently at zero restarts, same 2-CPU/512Mi limits. A tested
earlier container did restart during pause; that is not hidden by this final
snapshot. All nodes have NTP enabled and no remaining load/log-follow helpers.
P5 remains on hold. Twelve Python tooling tests and six infrastructure tests
passed. Detailed current retest evidence, including the failed ratio and
non-exercised stale-resume case, stays in local `P2-retest-2026-11-19.md`.
P2.7 now has a demonstrated failed retest requiring diagnosis/correction; actual
VM-freeze/node-loss evidence remains open before a whole-P2 claim.

Read-only historical routing analysis subsequently found near-even request
shares during those failed three-pod cases: A/B/C 33.75/34.31/31.94% and
35.87/30.17/33.96%. HTTP-200 shares were also distributed, so a severe sustained
single-pod hotspot is not supported by these windows. Mean outer business
middleware time was about 1.18-2.59ms per pod versus whole-case k6 HTTP means
99-101ms; these have different boundaries/populations and do not establish
network delay or the root cause. See the read-capacity design for the evidence
method and generator/transport isolation before choosing a production change.
No deployment or load workload was changed for that review.

### Laptop-generator attempts

The unchanged 1,000-VU/60s 1-3-3-1 campaign was subsequently attempted from the
operator laptop with stock k6 v2.4.0, matching the VPS generator's Linux binary
hash. The WSL/WireGuard run completed but is **invalid timing/capacity evidence**:
all four summaries contain negative HTTP durations/receiving times. Its reported
ratios 0.995667/1.010520 and receive-rate plateau must not be used as valid
capacity measurements. A separate integrity review preserves the original raw
files instead of clamping or deleting negative samples.

A read-only 45-second local probe subsequently observed three backwards guest
wall-time corrections of at least 1.19-1.21 seconds while monotonic time never
regressed in its samples. WSL NTP was active and Windows reported an unsynchronized
Local CMOS time source. This is consistent with k6's documented wall-clock
timing weakness, but does not establish the exact host/guest correction policy.
No clock, NTP, clocksource or WSL restart setting was changed.

Native Windows k6 avoided negative timing in the observed fallback attempts,
but its SSH transports failed at the first case: the WSL-localhost relay had
mostly request timeouts, and direct native Windows OpenSSH logged
`accept: Too many open files`. Both are incomplete/failed generator-path attempts,
not application capacity proof or a completed four-case comparison. No VU,
timeout, payload validation or threshold was relaxed. Memory protection never
stopped the campaigns; the WSL attempt's peak generator RSS was 463MiB with
more than 3.6GiB Windows physical headroom observed.

The WSL attempt's bounded source audit found no missing/duplicate identities:
BMKG 2,558, PVMBG 1,921 and Sensor Pantai 131,341 reference IDs; all 213 supplied
mock request/correlation checks passed. Audit identity evidence is distinct from
the invalid latency/rate measurements. Source images and P5 were not changed.
Three build-17 Aggregators were restored Ready on distinct nodes with unchanged
2-CPU/512Mi limits. Owned test processes, listeners, WireGuard resources and the
private temporary native SSH key copy were cleaned up; the original key remains.
Fifteen Python tooling tests passed. Detailed local evidence is retained in
`P2-laptop-2026-11-19.md`. The earlier valid C-host P2.7 failure remains open;
these laptop attempts do not establish either a pass or the SUT's scaling ceiling.

### Latest laptop rerun: stable clock, throughput ratio still failed

A reversible guest-NTP experiment subsequently completed the identical
1,000-VU/60s four-case campaign over WireGuard. Guest NTP was temporarily
stopped, not disabled; the original active/enabled state was restored afterward.
The clock probe observed zero backward corrections during both the 45-second
preflight and the 329.54-second campaign observation (136,417 samples). All four
k6 summaries passed the finite/nonnegative native-timing gate. This removes the
previous timing-integrity blocker for **this run**, not permanently for WSL.

| Replicas, execution order | Complete responses/s | Controlled 429 count | Other count |
| --- | ---: | ---: | ---: |
| 1, first | 3,324.590 | 8,142 | 2 |
| 3, second | 3,198.621 | 2,994 | 9 |
| 3, third | 3,384.660 | 1,097 | 3 |
| 1, last | 3,383.573 | 17,698 | 0 |

Both required complete-goodput ratios still fail: **0.962110 and 1.000321**,
versus >=2. No partial or malformed responses were counted, and unexpected
failures were below 1% in every case. Passing those case-level checks does not
turn the failed scaling comparison into a pass. This was a P2.7 comparison,
not a new 10k-VU/120s scaling test.

Historical traffic shares during the two three-replica windows were A/B/C
36.04/32.18/31.78% and 37.86/32.49/29.65%; severe sustained single-pod routing
skew is still not supported. Received application data was about 265-280Mbps;
this plateau is measured, but an ISP/entry/generator ceiling is not yet isolated.
Peak k6 RSS was 471.91MiB, with at least 3,097.63MiB Windows physical and
6,368.93MiB WSL available memory observed. No memory safety stop occurred.

The bounded post-run audit found zero missing/duplicate reference identities
for BMKG 2,828, PVMBG 2,137 and Sensor Pantai 139,979 IDs. All 212 supplied
mock request/correlation checks and source cadence checks passed, with zero
same-epoch multiple holders. These remain scoped identity/log checks, not full
payload or absolute physical HTTP exactly-once proof. Three build-17 pods were
restored Ready on distinct nodes, zero current restarts; temporary WireGuard
resources and restoration timers were removed. Eighteen Python tooling tests
passed. No application, resource limit, official sensor or P5 change was made.
Raw current evidence is retained locally under
`.e2e/p2-laptop-throughput-20261119T134224Z/`,
`.e2e/p2-laptop-evidence-20261119T134224Z/` and
`.e2e/p2-ntp-experiment-20261119T134135Z.json`.

### Subsequent read-capacity candidate (not an acceptance pass)

The scoped upgrade initially deployed `m2-vps-20261119T143453Z-read-capacity` on three
Aggregators and two bounded L7 gateways. It includes separate HTTP/fill budgets,
100ms bounded shared query projections, negotiated gzip and per-backend gateway
backpressure. Canonical storage, official source/tester images and the acceptance
workload remain unchanged. Gateway pods are additional resources and must be
included in capacity reporting. Kafka/coordinator HA and P5 remain deferred.

The earlier candidate's uncontended standard four-case campaign completed at
2,310.575 / 3,531.708 / 3,519.337 / 2,135.026 complete responses/s. Ratios
1.528497 / 1.648381 still failed >=2. Its lower single-replica baseline must not
be hidden by celebrating a larger ratio. A still earlier attempt overlapped
isolated CPU regression work and is explicitly annotated as contended, not a
clean capacity comparison. Raw failures remain retained.

The next candidate rerun stopped after its first one-replica case: 201,441
complete responses (3,307.440/s), 26,471 unexpected responses (11.61%), k6 exit
99. The unchanged <1% threshold failed; no four-case scaling ratio exists for
this attempt. Retained Prometheus counters localize the observed 503s to one
gateway's no-backend/admission path, with zero proxied-upstream 5xx increments
in the diagnostic 45-second window; successful canonical responses were 200.
That identifies a routing/discovery/health failure boundary, not yet the exact
reason the gateway considered its backend unavailable. It is not evidence that
adding Kafka replicas or a full CQRS read database would fix the failure.

HTTP contract checks passed separately: complete responses, wrong-key 401 and
negotiated gzip (~10KB decoded to ~2.1KB wire). Gateway Prometheus targets are up;
the observability image/configuration was updated without changing Prometheus
pod UID or restarting its current history. Contract/metrics success is not a
load/HA/P5 pass. The failed load run restored three Aggregators and the original
guest-NTP active/enabled state; its clock observation saw no backward step.

Current evidence: `.e2e/p2-capacity-deploy-20261119T143453Z/`,
`.e2e/p2-obs-20261119T144648Z/`,
`.e2e/p2-capacity-contract-20261119T144930Z.json`,
`.e2e/p2-laptop-throughput-20261119T144811Z/` and
`.e2e/p2-ntp-experiment-20261119T144722Z.json` (local, ignored).

The current candidate is `m2-vps-20261119T145321Z-read-capacity`. It adds bounded
last-good DNS handling without renewing the original 3-second discovery lifetime,
active probe checks during lookup failures, and distinct discovery/probe/no-backend
diagnostics. Local tests/vet passed; the isolated PostgreSQL/race/vet regression
passed across Aggregator, Client API and notifier, with repeated store health tests.

Its complete standard 1-3-3-1 comparison returned 3,570.291 / 3,789.411 /
3,743.121 / 2,967.616 complete responses/s. Every case passed response and native
timing checks, but the ratios **1.061373 / 1.261323 still fail >=2**. All four
cases had zero unexpected, partial or malformed responses. Historical 45-second gateway counters show zero
upstream-5xx and admission-rejection increments in all four diagnostic windows.
The earlier 11.61% failure is retained; the exact old unavailability trigger is
not retroactively proved by a successful rerun.

The clock monitor covered the whole campaign with zero backward steps. Original
NTP state and three Aggregators were restored. Raw evidence:
`.e2e/p2-laptop-throughput-20261119T145513Z/`,
`.e2e/p2-ntp-experiment-20261119T145423Z.json` and
`.vps/design-tests-20261119T145204Z.log` (local, ignored).

The separately labelled gzip campaign completed at 2,985.007 / 4,571.853 /
4,428.792 / 3,081.782 complete responses/s. Its ratios 1.531605 / 1.437088 also
fail >=2; compression was not silently added to the standard script. In the first
three-replica case, incoming application data was ~87.85Mbps, versus ~315.38Mbps
in the standard case, but compression/decompression also changes CPU demand.
Peak generator CPU reached 5.56 logical-core equivalents out of GOMAXPROCS=6.
This is not an isolated proof of an ISP limit or of the server's maximum capacity.
Optional Kubernetes-exec CPU-accounting samples ran during the later gzip cases;
that added diagnostic overhead is recorded separately, not presented as a clean
unobserved comparison. The retained matching-pod samples showed no Aggregator
CPU throttling and only 2 additional throttled gateway periods in that interval.

In the standard run's 45-second windows, about 130k-174k successful request-counter
increments required only 342-456 actual canonical fills (mean fill ~5.2-8.6ms).
These are sampled counter increases, not exact k6 counts or SQL-tail percentiles.
They support deferring a full event-driven CQRS read-store redesign for this
tested hot-page workload, not a promise about untested high-cardinality queries.
See the implementation guide's consistency/recovery requirements before adopting
such a store. Gzip evidence: `.e2e/p2-laptop-gzip-throughput-20261119T150133Z/`
and `.e2e/p2-ntp-experiment-20261119T150045Z.json` (local, ignored).

The candidate-to-build-17-to-candidate strict rolling regression passed at
50 VUs / 120 seconds: **52,856 complete HTTP 200s, zero HTTP/check failures**,
p95 27.96ms, and both distinct image digests observed fully Ready. This is not
10k rolling evidence. A separate unchanged 10k scale round reached Ready
3-to-5-to-2, with 644,903 complete / 178 partial / 95,657 controlled 429 / 837
other responses; HTTP unexpected rate 0.112868% and all-response p95 1,670.26ms.
It is **not a P2.6 pass**: only 117.521 consecutive seconds at 10k were observed,
below the required 120. The generator was C-host colocated, with peak RSS
3,821.64MiB and no memory safety stop. The duration gate was not lowered and the
failed verdict remains retained. Evidence: `.e2e/p2-scale-20261119T150713Z/`
and `.e2e/p2-rolling-20261119T151142Z/` (local, ignored).

An identical second scale run passed its existing gates, observing **126.636s at
10k VUs** with Ready 3-to-5-to-2: 675,541 complete / 384 partial / 163,003
controlled 429 / zero other or malformed responses, all-response p95 1,544.34ms.
Counting partials as failures gives 0.04577% of all requests (0.05681% of
non-429 requests), still below 1%; partials are not counted as complete goodput.
Peak generator RSS was 3,832.80MiB with no safety stop. This is a bounded
colocated C-host scaling observation, not a dedicated-generator capacity result
or proof of all 10k requests succeeding. The first duration-gate failure remains.
Evidence: `.e2e/p2-scale-20261119T151409Z/` (local, ignored).

The final bounded source audit found zero missing or duplicate reference
identities: BMKG 3,501, PVMBG 2,675 and Sensor Pantai 161,520 reference IDs.
All 813 supplied mock upstream request/correlation checks passed; source start
cadence passed and no same-epoch multiple holder was observed. This remains
identity/log evidence, not a payload/checksum oracle, absolute physical HTTP
exactly-once or actual VM-freeze/resume proof. See
`.e2e/p2-laptop-evidence-20261119T145513Z/` (local, ignored).

Final contract/observability checks passed: gateway metrics targets up and a
fresh bounded correlation-log trace includes both gateway and Aggregator. The
first direct node-host dashboard probe was correctly denied by the private
NetworkPolicy; the verification was corrected to authenticated Kubernetes
exec/loopback without weakening that policy. Raw failed diagnostic evidence is
retained. This is not a full distributed-span trace. Evidence:
`.e2e/p2-capacity-contract-20261119T151852Z.json` (local, ignored).
P2.7 remains failed; actual P2.5 VM-freeze/stale-process-resume evidence remains
open. P5 and infrastructure HA were not retested or expanded in this round.

## Gateway admission/drain follow-up

The follow-up adds class-specific bounded backend queues (one waiter per active
slot, 100ms maximum queue wait), occupancy-aware selection, explicit local 429s
that do not mark healthy backends failed, queue/header/body-lifetime metrics,
and one gateway-wide copy-buffer pool with at most 8MiB retained. Shutdown now
awaits active responses, with 5s Kubernetes endpoint propagation before a 15s
HTTP drain. See the implementation guide for the fixed-gateway-count and body
timing limitations. Cache TTL, SQL deadlines, resource limits and the standard
load script were not changed. No Kafka topology or full CQRS migration was made.

The `160708Z` candidate's unchanged 1,000-VU/60s 1-3-3-1 comparison returned
**3,460.880 / 3,687.854 / 3,679.720 / 3,576.603 complete responses/s**. Paired
ratios **1.065583 / 1.028831 still fail >=2**. All four cases had zero partial,
malformed or unexpected responses; controlled 429 counts were
51,505 / 111 / 93 / 27,827. These rejections are retained, not successful
throughput. Earlier zero-rejection runs remain above; bounded admission is not
presented as a capacity gain. Received application data remained about
290 / 307 / 306 / 298Mbps. This does not isolate an ISP bandwidth limit.

Historical 45s window estimates showed the three Aggregators receiving about
33-34% each in both cases. Three-replica mean gateway queue wait was
roughly 0.04-0.44ms, header phase 10-11ms and body lifetime 0.4-0.5ms. Those
observations do not support blaming a large sustained three-replica gateway
queue; tail waits and controlled rejections still occurred. They are not exact
k6 populations or an isolated measurement of network/copy CPU. The clock was
stable over the full campaign, NTP restored and temporary WireGuard removed.
Evidence: `.e2e/p2-laptop-throughput-20261119T161105Z/` and
`.e2e/p2-ntp-experiment-20261119T161016Z.json` (local, ignored).

An explicitly instrumented 100-VU/45s C-colocated profile produced 186,725
complete / 20 no-backend / zero partial, malformed or 429 responses. The 20
failures are retained, not retroactively diagnosed or erased. Gateway CPU was
about 1.18-1.22 cores with zero observed throttled periods; sampled cgroup memory
peaked at about 13.5-15.3MiB. Logging represented about 6% sampled CPU, while
socket I/O/copy paths dominated. This is not a clean capacity comparison or
proof that the gateway could never saturate. Profiling opt-in was removed and
the original Deployment spec restored. Raw profiles and local allocation
microbenchmarks remain ignored under `.e2e/p2-gateway-profile-20261119T160842Z/`
and `.e2e/gateway-admission-profile/`.

The final `161828Z` rebuild additionally makes canceled-client responses
explicitly unsuccessful in gateway telemetry, without poisoning backend health
or counting them as genuine upstream failures. Success-path/queue/drain policy
is otherwise unchanged from `160708Z`; the four-case comparison above is still
labelled with its actual tested image, not silently reassigned to the rebuild.
Local Aggregator tests/vet and the real-PostgreSQL race/vet suite passed for the
final source across Aggregator, Client API and notifier. Gateway admission/drain
and store health tests passed three repetitions. All 20 P2 tool tests and seven
provisioning tests passed. Final-source regression log:
`.vps/design-tests-20261119T161644Z.log` (local, ignored).

Gateway-specific current-to-`160708Z`-to-current rolling passed unchanged
50-VU/120s strict checks: **50,758 complete HTTP 200s, zero HTTP/check failures**,
p95 32.25ms. Both distinct gateway image IDs were observed fully Ready and the
final gateway image was restored. This is gateway replacement evidence, not
10k rolling or a substitute for the Aggregator-version criterion. Evidence:
`.e2e/p2-rolling-gateway-20261119T161946Z/` (local, ignored).

## Gateway memory-budget follow-up

The first `161828Z` 10k follow-up **failed**: 111.560 consecutive observed seconds
at 10k (below 120), 351,872 complete / 238 partial / 114,801 controlled 429 /
840,877 other responses. Unexpected failures were 64.30%; all-response p95 was
1,165.84ms and generator RSS peaked at 4,086.32MiB. Both gateways were OOMKilled
at the unchanged 256Mi limit (restart counts 5 and 4), while three restored
Aggregators had zero restarts. Historical gateway metrics reset/disappeared
after the kills; missing samples are not zero-error evidence. This failure is
retained under `.e2e/p2-scale-20261119T162208Z/` (local, ignored).

The subsequent bounded source/Audit audit still found no missing reference
identities or duplicate canonical identities: BMKG 4,040, PVMBG 3,106 and
Sensor Pantai 178,753 reference IDs; 288 mock request/correlation checks and
source cadence checks passed. This does not override the failed load result
or prove payload/checksum, whole-VM freeze or physical HTTP exactly-once.
Evidence: `.e2e/p2-source-evidence-20261119T162208Z/` (local, ignored).

The memory-budget follow-up reserves runtime headroom with `GOMEMLIMIT=160MiB`
inside the **same** 256Mi gateway limit, adds connection/runtime-memory metrics
and a no-new-gateway-restarts scale gate. It is an evaluated mitigation, not an
assumed OOM cure; see the implementation guide. Local tests/vet and isolated
PostgreSQL race/vet plus three focused repetitions passed on the new source:
`.vps/design-tests-20261119T162855Z.log`. P2 tool tests now total 22 passing.

On `163045Z`, the unchanged first memory-budget retest passed its existing
scale gates: **124.025s at 10k VUs**, Ready 3-to-5-to-2, 480,573 complete /
259 partial / 66,995 controlled 429 / 557 other / zero malformed responses.
Unexpected failures were 0.10157% of all requests; conservatively counting
partials and excluding 429s from the denominator gives about 0.170% failure.
All-response p95 was **1,996.74ms**, close to the unchanged 2,000ms script gate,
so this is not generous latency headroom. Both gateway identities/restarts
remained unchanged (zero restarts). Sampled working sets peaked near
161.34/163.20MiB; observed open client connections were about 4,900-5,100 per
gateway, and exported runtime limits matched 160MiB. Generator C remained
colocated (peak RSS 3,713.45MiB). These observations support the memory mitigation
but do not prove universal OOM safety or complete request success. Raw evidence:
`.e2e/p2-scale-20261119T163207Z/` (local, ignored).

An identical second memory-budget scale run also passed: **129.220s at 10k**,
Ready 3-to-5-to-2, 477,060 complete / zero partial / 30,729 controlled 429 /
249 other / zero malformed responses, p95 1,658.27ms. Both gateway identities
again had zero restarts. Unexpected HTTP failure was 0.04901% of all requests;
excluding 429s from the denominator gives 0.05217% failure. Generator peak RSS
was 3,685.21MiB, with no safety stop. This was the same colocated C-host generator
and unchanged load script, not a dedicated-generator capacity proof.
Separate tightening-only post-reviews of both runs passed the conservative
non-429 semantic error calculation and gateway stability gate, preserving the
original raw results. Evidence: `.e2e/p2-scale-20261119T163626Z/` and each run's
`semantic-review.json` (local, ignored).

Final API/auth/negotiation checks and bounded gateway-to-Aggregator correlation
trace passed. Three Aggregators were restored Ready on three nodes and both
gateways Ready, all with zero restarts on `163045Z`. No k6 process remained on
A/B/C, profiling was disabled, WSL NTP active, and the original Prometheus pod
UID/history was preserved. Evidence:
`.e2e/p2-capacity-contract-20261119T164121Z.json` and
`.e2e/p2-gateway-final-20261119T164138Z.json` (local, ignored).
These are bounded observations, not all-P2 acceptance: the last comparable
throughput result still failed 2x, and actual stale-process VM freeze/resume
remains unproved. P5 and coordinator/Kafka HA remain deferred.

The closing source audit spanning the memory-budget runs passed: zero missing
reference IDs and zero duplicate canonical identities for BMKG 4,127, PVMBG
3,176 and Sensor Pantai 181,574 reference IDs; all 347 supplied mock upstream
request/correlation checks passed, source cadence passed, and no same-epoch
multiple holder was observed. Extra canonical IDs can arrive between source
and Audit snapshots. This remains bounded identity/log evidence, not a full
payload/checksum or physical HTTP exactly-once proof. Evidence:
`.e2e/p2-source-evidence-20261119T163207Z/` (local, ignored).

## Reproduce and retain evidence

See [VPS provisioning/access](../infra/vps/README.md). Review fault-injection
scripts before executing them. Tests create local `.e2e/` and `.vps/` artifacts;
retain failures as well as passes. Never commit inventory, credentials,
kubeconfig, database files or raw run logs. Dated journals remain available on
the operator's current machine, not as links expected to resolve in a clone.
