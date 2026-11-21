# P2 read-capacity redesign: balance is not the measured failure

Status: historical design/diagnosis, not an acceptance pass. Reviewed
19 November 2026 against the whole M2 specification and current build 17.
P5 and infrastructure HA remain deferred. Application code, manifests, CPU,
memory, database state and acceptance workloads were not changed by this review.

The subsequent candidate implementation and its CQRS boundary are documented in
[P2 read-path implementation](P2-read-path-implementation.md). The measurements
below describe the earlier build-17 diagnosis; they are not results for that
candidate. See [current evidence index](../status/README.md) for later test outcomes.

## Evidence that changes the design decision

The four retained 1,000-VU/60-second cases still fail P2.7: complete-response
3/1 ratios are 0.788606 and 0.838571. The failure is not reclassified as a pass.

Historical Prometheus queries over the same two three-replica cases show:

| Case | A request share | B request share | C request share |
| --- | ---: | ---: | ---: |
| First 3-replica run | 33.75% | 34.31% | 31.94% |
| Second 3-replica run | 35.87% | 30.17% | 33.96% |

These count `GET /internal/events` responses including controlled rejections,
not health/metrics calls. HTTP 200 shares are also distributed: A/B/C
34.00/34.02/31.99% and 35.76/30.54/33.70%. k6 reported no partial or malformed
responses in these cases. All three scrape targets were up throughout each
queried window. Thus the evidence does not support a severe single-pod traffic
hotspot as the primary explanation for these two runs. It does not rule out
short bursts, unequal in-flight work, skew with other connection patterns, or
an entry-node bottleneck.

Method: retained counter increases, 45-second windows from case start+5s to
start+50s, with 5-second scrapes. Counts are extrapolated Prometheus estimates,
not exact full-run k6 totals. Raw queries/results are retained in ignored
`.e2e/p2-throughput-20261119T123818Z/historical-routing.json`.
The first collector invocation saved valid data but then failed printing a
relative path; the path-reporting bug was corrected and collection rerun.

Live `aggregator-vps` Service settings are Cluster traffic policies and
`sessionAffinity: None`, with no topology preference. One private URL on A is
the entry point. Kubernetes Service proxying selects a backend for a connection;
it is not application-aware per-request load scheduling. Persistent connections
can matter, but they did not produce severe observed RPS skew here.
[Kubernetes virtual IPs](https://kubernetes.io/docs/reference/networking/virtual-ips/).

There is a larger unexplained timing gap:

| Measurement | First single replica | First three replicas |
| --- | ---: | ---: |
| k6 mean HTTP duration | 70.43 ms | 100.64 ms |
| k6 mean iteration duration | 156.32 ms | 247.49 ms |
| k6 mean waiting/TTFB | 67.60 ms | 95.25 ms |
| k6 mean connection acquisition/blocking | 0.23 ms | 0.30 ms |
| Mean inner handler, per pod | 1.30 ms | 1.62-2.23 ms |
| Mean outer business middleware, per pod | 1.58 ms | 1.90-2.59 ms |

Server means use the declared 45-second windows; k6 means use whole cases. The
inner histogram includes all routes and statuses; outer middleware excludes
probes and includes synchronous application logging, but neither measures all
kernel/TCP output completion or client scheduling. Rejections affect averages.
These numbers must NOT be subtracted to claim a measured 98ms network delay,
nor does their mean prove that every SQL tail is fast. They prioritize measuring
generator scheduling, response handling and transport/entry queuing alongside
server work. Synchronous logging is not established as the dominant mean cost.

`constant-vus` is a closed model: an iteration must finish before that VU starts
another. The script parses/validates each complete response. At three replicas,
the fraction of full bodies increased substantially while cheap 429s decreased.
The iteration-minus-HTTP gap can include JavaScript/validation, scheduling and
metrics overhead; it is not pure measured JSON CPU time. Generator contention
is therefore a hypothesis to test, not a demonstrated excuse for the failed
ratio. [k6 execution models](https://grafana.com/docs/k6/latest/using-k6/scenarios/concepts/open-vs-closed/),
[metric boundaries](https://grafana.com/docs/k6/latest/using-k6/metrics/reference/).

## Design decisions

Laptop follow-up: an identical 1-3-3-1 attempt outside the cluster was made, but
the WSL/WireGuard timing is invalid (negative native durations and independently
observed backwards guest realtime). Native Windows fallbacks reached SSH path
limits, including native OpenSSH descriptor exhaustion. This is **not completed
generator isolation proof** and does not justify treating the original ratio as
passed or attributing the SUT limit to an ISP plan. See [current status](../status/README.md).
The next prerequisite is a trustworthy clock with a high-concurrency private
path; neither lower VUs nor relaxed timeouts are substitutes.

### 1. Preserve the read/ingestion boundary

All Aggregators remain active readers with one independently leased poller per
source. Retain transactional fenced admission, per-target outbox/projections,
idempotent writes and the current P1 partition/replica layout. An ordinary read
does not synchronously poll a source, publish Kafka or query lease ownership.
Increasing Kafka replicas cannot directly remove work from this GET path.

Do not create a complete canonical copy on every node. Keep Audit uncached,
global pagination/version reconciliation and explicit partial/degraded metadata.
No certificate of completeness may be replaced by a successful network ping.
M2 P1/P3/P4 correctness and P5 Field protection remain design constraints even
while this investigation focuses on P2.

### 2. Keep the existing Service as the initial baseline

There is no measured justification to deploy an additional HTTP proxy merely
to make the request shares look closer to 33.33%. Keep one common entry and
observe both its forwarding cost and backend work. More endpoints do not add
capacity to the single entry node's NIC/kernel/WireGuard path.

A future L7 proxy is conditional: use endpoint discovery and per-request
least-active-request selection if persistent/multiplexed connections or
heterogeneous service times demonstrably create overload on a subset of pods.
For example, Envoy's least-request policy compares active requests, rather than
assuming equal TCP connection counts mean equal work.
[Envoy least-request policy](https://www.envoyproxy.io/docs/envoy/v1.37.7/intro/arch_overview/upstream/load_balancing/load_balancers).

If introduced, it must discover ready Pod endpoints directly; proxying all
requests through one pooled connection to the existing Service would retain
hidden L4 backend selection. Preserve readiness removal, endpoint updates,
connection draining, authenticated priority and correlation IDs. Bound queues;
do not retry 429s automatically or replay writes to hide overload. Any BNPB
proxy runs in Kubernetes with declared other-class 128Mi/256Mi memory budgets,
and its own CPU/latency/throughput must be measured. It is not free capacity or
an HA solution for the singleton entry/control-plane node.

### 3. Separate cheap response work from expensive fills, if measured

Current admission runs before the local cache and holds an ordinary slot through
cache waiting and response writing. A cache hit and a slow cache-miss waiter
therefore compete for the same 48 ordinary HTTP slots (64 total, 16 reserved
for Field). A slow reader can also hold a slot during output. This is a design
coupling worth measuring, not yet the proven cause of this ratio.

Proposed two-level budget, without an unbounded cache fast path:

1. Authenticate/authorize and derive trusted Field priority before serving data.
2. Apply a bounded response/egress budget, including Field reserves, to hits and
   misses alike. Cap memory and slow-client lifetime; no unlimited connections.
3. On an unexpired complete cache hit, reuse immutable encoded page fields and
   compute current source health. Do not consume a database-fill slot.
4. On a miss, use separately bounded single-flight fills and SQL budgets. Limit
   waiters and total deadline; maintain separate Field fill/connection reserves.
5. Retain TTL from read initiation, no stale page relabelled fresh, no cached
   partial/error, and explicit response categories on rejection/failure.

Current dedicated Field/maintenance/probe pools and read-slot isolation are
retained. Before changing admission sizes, record active hits, fills, waiters,
egress slots, hit/miss age, acquire/SQL/merge/encode/write time and CPU/GC per pod.
Choose budgets from those measurements and 512Mi limits, not the required ratio.
Implement this change only if fill/egress contention is observed after generator
and transport isolation.

### 4. Do not amplify database work blindly

A cold list read fans out across three partitions and two assigned replicas,
then performs bounded cross-lookups/reconciliation. Every pod has its own cache;
more pods can increase aggregate misses and connections against the same three
stores. Measure fill rate, SQL calls per fill, acquisition/lock waits and repair
load, not just total API RPS.

Keep current bounded SQL work initially. A preferred-replica read optimization
requires a real per-partition no-gap/applied-progress certificate plus recovery
epochs and version/update/retraction rules agreed with P1/P4. Reachable/ready or
maximum observed version alone is not that certificate. Do not remove peer
reconciliation or weaken partial semantics as a local throughput shortcut.

## Validation sequence and decision gates

Latest laptop isolation evidence (unchanged workload/build/resources): a
temporary guest-NTP stop eliminated observed backwards corrections over the
45-second preflight and 329.54-second campaign, and all HTTP timing minima were
valid. The complete-goodput 1-3-3-1 rates were
3324.590/3198.621/3384.660/3383.573 RPS: ratios 0.962110/1.000321, still failed.
Memory protection did not stop the campaign (peak k6 RSS 471.91MiB). Historical
three-pod shares were 36.04/32.18/31.78% and 37.86/32.49/29.65%. The 265-280Mbps
received-data plateau and k6 peak CPU of 4.91 logical-core equivalents warrant
aligned generator/transport measurement, not an unproven claim of an ISP limit.
NTP and three replicas were restored; no production design was changed.
The earlier VPS-host failure and invalid unmitigated laptop run remain retained
separately. This comparison does not replace the 10k sustained or VM-freeze tests.

| Stage | What to do | Decision |
| --- | --- | --- |
| Historical balance | Query the actual failed windows, separating 200/429 and scrape coverage. | Completed: no severe sustained per-pod RPS imbalance observed. Keep current LB baseline. |
| Isolate generator | Run the identical script from an existing machine outside A/B/C, with CPU/RAM/NIC headroom and guarded limits. Keep all requests through the common entry. | Do not buy another server or increase app CPU before establishing whether the generator/path limits the run. |
| Align resource evidence | Sample generator CPU/quota/GC and response phases; entry-node NIC/WireGuard/conntrack/TCP retransmissions/softirq; per-pod active work and CPU/quotas; DB waits in the same declared windows. | A shared transport ceiling, generator limit and application limit need different fixes. |
| Isolate path | Bounded direct-Pod diagnostics can distinguish backend cost from the common entry, with comparable payload/work and unchanged authorization. | Diagnostics only. Direct-Pod or multiple-URL results do not replace the single-entry P2 acceptance run. |
| Determine saturation | Supplement with a small VU ladder or fixed-arrival diagnostic test; report achieved arrival, complete goodput, rejection, partial and error rates. | Extra diagnostics do not replace/relax the existing constant-VU test or its failed verdict. |
| Change one measured bottleneck | Generator contention: isolate generator. Entry transport limit: fix forwarding/packet cost. Admission contention: split work budgets. SQL contention: bound/certify reads jointly with P1. Proven request skew: evaluate L7 routing. | No simultaneous rewrite that prevents attribution. No arbitrary larger timeout/TTL/memory budget. |
| Revalidate | Same build/settings for 1 and 3, repeat 1-3-3-1, retain failures and require both complete-goodput ratios >=2. Then rerun unchanged 10k/120s 3-5-2, source audit and strict rolling. | Still failed until measured passes exist. Do not count 429/partial as successful throughput or artificially degrade the single-replica baseline. |

If a generator needs multiple hosts, synchronize execution segments and total
VUs, retain per-generator outcomes and aggregate counts over matching windows;
never average p95s or claim 10k from two non-overlapping 5k intervals.
[k6 generator headroom/distributed execution](https://grafana.com/docs/k6/latest/testing-guides/running-large-tests/).
Do not remove correctness checks/response bodies or add sleeps to raise the
reported ratio. CPU stays 2 and memory 512Mi per Aggregator during comparisons.

## Separate open safety issue

P2.5 still needs actual VM freeze/resume and evidence from the original stale
process. A liveness-triggered replacement process is not that experiment. Keep
the SQL epoch/admission guard; do not disable liveness or shorten a pause just
to obtain a passing-looking result. Provider/hypervisor control is a separate
prerequisite. This transport/capacity design does not close P2.5, P1 disk loss,
singleton coordinator/Kafka HA, P5 critical latency or encrypted exam evidence.

## Historical analysis and future tooling

The former infrastructure routing-analysis runner was removed during cleanup.
Its historical conclusions above remain bounded observations, not fresh proof.
Future standalone analysis and test runners belong in [`test/`](../../test/README.md).

Historical Prometheus queries and raw exports can expire or be incomplete.
Missing history is not evidence of balanced routing or a passing throughput ratio.
