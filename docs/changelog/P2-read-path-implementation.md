# P2 read-path implementation

Implemented 19 November 2026. The acceptance criteria, data contract, 100ms
read-cache TTL and per-Aggregator 2 CPU / 256Mi request / 512Mi limit remain
unchanged. A deployed feature is not itself proof of the >=2x scaling criterion.

## Read and write responsibilities

The source-lease / fenced admission / independent Kafka and canonical projection
write path remains unchanged. Ordinary reads never poll sources. Audit and
analytics retain the existing canonical reconciliation and completeness rules.

The serving path now separates three budgets:

1. Authenticated HTTP admission: at most 128 active responses per Aggregator,
   with one quarter reserved for trusted Field requests.
2. Actual database fills: the existing 64 / 16 Field-reserve configuration,
   applied inside the cache. A cache hit never consumes a database-fill slot.
   Per-unit SQL, probe, maintenance and Field pools remain independently bounded.
3. Compression: at most 16 gzip encoders per Aggregator. On exhaustion the
   same JSON is sent uncompressed, not discarded or changed. Negotiation honors
   `Accept-Encoding`, explicit q=0 and `Vary`; small responses remain uncompressed.

Cache misses coalesce within each process. Immutable page encoding is reused;
source health is still computed independently on every response. Neither
partial results nor errors are cached. The local L1 cache retains its 128-key,
8MiB encoded-byte bounds. Decoded object overhead and active response memory
still require runtime measurement; an encoded-byte bound is not total RSS.

## Bounded shared query projection

`read_page_projection` in the Aggregator-owned coordinator database is a
disposable materialized query view, not a new canonical copy or an event-sourced
full-dataset read replica. Thirty-two hash slots hold at most one 1MiB page each.
All filters/cursors affect the key; there is no benchmark-route special case.
Collisions evict a page and reduce hit rate but cannot return another query's
result. No dataset preload or background full-data copy is performed.

Each pod uses a separate two-connection ordinary / one-connection Field pool.
This avoids consuming lease/admission/delivery connections. Transaction-scoped
advisory locks coalesce concurrent fills across pods, with a bounded 20ms join
wait and database-enforced transaction/statement timeouts. Field fills have a
separate lock namespace so they never wait behind an ordinary fill. Concurrent
publication retains the later expiry rather than overwriting it with an older
read. Existing ingestion/schema v3 and its rollback compatibility are unchanged.

Expiry begins before the canonical read, using the coordinator's clock for the
shared row. Remaining age is conservatively translated into a local monotonic
deadline; L1 never grants another full TTL to another pod's result. Slow fills
can answer their callers but cannot publish a newly fresh cache entry. Projection
loss, lock contention, pool exhaustion, or malformed cached data falls back to
the existing certified canonical read, not to stale or falsely complete data.

This is deliberately a limited CQRS/materialized-query step. It does not replace
P1 replica-union/coverage proofs with a reachability check or a maximum-version
heuristic, and does not create a full HazardEvent dataset on each node. A full
event-driven read-store redesign would additionally need mutation/retraction,
replay and no-gap progress contracts; that larger redesign is not claimed here.

The coordinator is still singleton infrastructure. Cache loss is tolerated;
coordinator loss still affects the existing write path. New cache load may
compete for its CPU/I/O despite connection isolation, and must be measured.
`AGGREGATOR_SHARED_READ_PROJECTION=false` disables this optimization.

## Common-entry L7 gateway

Two `role=other` gateway pods use the same image's `/app/gateway` executable,
128Mi request / 256Mi limit and explicit 2-CPU limit each. These are additional
resources, not free Aggregator capacity; report them when comparing results.
Both `aggregator` and the private VPS NodePort select these pods. A headless
`aggregator-backends` Service discovers ready Aggregator endpoints directly.

The gateway health-checks backends, expires discovery, and selects a healthy
least-active backend on every request with randomized ties. It does not choose
one pooled connection to the old Service VIP and assume per-request routing.
Backend DNS health is sampled, not instantaneous; readiness/draining and actual
rolling-failure measurements remain essential. Forwarding failure marks a
backend unhealthy and returns an explicit 503; it is not silently retried at the
application layer. Existing 429s, response data, credentials and correlation IDs
pass through. Successful/failed proxy requests have structured duration logs.
Admission saturation returns 429 with Retry-After; missing/failed backends return
503. The load test's error classifications are not changed to hide either.

A transient DNS error does not mean an authoritative empty endpoint set. The
gateway retains the last successful snapshot only within its original 3-second
lifetime and continues active health checks; neither a failed lookup nor a
successful health probe renews that lifetime. A successful empty discovery
removes endpoints immediately. Discovery, failed-probe and no-backend counters
distinguish routing unavailability from proxied application failures.

Connection pools and active responses are bounded. One quarter of 1024 gateway
response slots is protected from ordinary traffic; only a matching internal API
key can activate Field priority. The gateway receives only that Secret key, not
database credentials, and has no Kubernetes API token or database ingress access.
Metrics expose proxied requests, rejections, upstream failures and duration.

A separate per-backend semantic budget is held until the upstream response body
closes, not merely until response headers arrive. With the fixed two gateways
and 128 backend response slots, each gateway allows 48 ordinary / 16 Field
upstream responses per backend. Each backend additionally admits at most one
waiter per active slot in the corresponding class. A free slot takes a fast path;
otherwise queue wait expires after 100ms, well inside the 5s upstream/client
deadline. Overflow/expiry returns explicit 429 + Retry-After without marking a
healthy backend failed. Cancellation releases pending capacity. Selection compares
class-specific active-plus-pending occupancy, so ordinary congestion does not
divert Field traffic from its protected budget. The global response caps remain
outer safety bounds, not a target queue depth. Health probes use an independent
transport. `GATEWAY_REPLICAS` must
match the Deployment replica count when this fixed topology is changed; the
partition is conservative when a gateway is absent, and does not implement a
distributed/global semaphore. Application calls bypassing the gateway can still
consume backend capacity. During a third-gateway rolling surge, the fixed
two-gateway partition is not a global capacity guarantee; backend admission is
still the final safety guard. A canceled client does not by itself mark a backend
unhealthy. Common `aat_http_*` telemetry also covers the gateway for dashboard
queries; logs remain bounded correlation-log evidence, not distributed spans.

Gateway replicas do not add bandwidth to node A's physical/private entry or
solve coordinator/Kafka HA. Extra routing and logging can themselves become a
bottleneck. This is an evaluated design candidate, not a guaranteed speedup.

### Gateway lifecycle and measurement

SIGTERM withdraws gateway readiness and initiates finite HTTP shutdown. Main now
waits for shutdown completion, rather than exiting when the listener closes.
Kubernetes preStop allows 5s for terminating-endpoint propagation, followed by
up to 15s HTTP drain inside a 25s termination grace period. These are propagation
budgets, not a mathematical guarantee about every external load balancer.
Real-TCP regression tests verify that an in-flight response finishes after
listener shutdown and that an exceeded drain deadline closes blocked clients.

Fixed-cardinality metrics separate `gateway_queue_wait_seconds`,
`gateway_upstream_headers_seconds` and `gateway_body_lifetime_seconds`, each by
ordinary/Field class, with active/pending gauges and queue full/expired/canceled
counters. Header timing includes transport connection/dial/response-header work;
body lifetime includes upstream reads and downstream backpressure until Close.
It is not isolated copy CPU time, and sums from different populations must not
be subtracted to infer exact network delay. Full per-request correlation logs
are retained; no sampling was silently introduced for a benchmark.

One gateway-wide nonblocking copy-buffer pool retains at most 256 x 32KiB (8MiB).
Pool misses allocate transient buffers rather than queuing; oversized buffers
are not retained. A local Windows, four-worker, 10KiB real-upstream/discard-client
microbenchmark reduced allocation from about 45KB/97 allocations to 12KB/96
allocations per response. Its combined allocation profile attributed 54% of
allocated bytes to ReverseProxy.copyBuffer. This supports buffer reuse, not a
claim that the same percentage of production CPU or end-to-end latency vanished.
The benchmark excludes real client JSON validation, network path and access logs.

Opt-in `GATEWAY_PROFILE_ENABLED=1` provides the same bounded 127.0.0.1:6060 CPU
and runtime diagnostic surface as Aggregator profiling. Runtime summaries now
include cumulative allocations and GC counters, not heap/environment dumps.
The public/business mux never exposes profiling. Capture via authenticated pod
exec and remove the opt-in after the explicitly instrumented experiment.

## Deployment and verification

The normal Kubernetes deployment includes the gateway. The retained scoped
entry point is `python3 infra/vps/deploy-p2-capacity.py`; review live schema
compatibility and obtain fresh regression evidence before executing it.

An interrupted scoped deployment can use `--resume p2-capacity-deploy-<UTC stamp>`.
The deployment validates retained state and the current image before reusing
that image and original rollback target. It refuses completed records and
unrelated live image changes. It retains previous image/selectors/config values,
waits for readiness, and preserves PVCs and official sensor/tester images.

Former infrastructure test, clock-experiment, negotiation, and diagnosis runners
were removed. Their earlier workload descriptions and results are historical,
not runnable commands or a new-build pass. New standalone runners belong under
[`test/`](../../test/README.md). Re-establish throughput, scaling, rolling,
ownership/source audits, and stale-process-resume evidence before acceptance.
Do not run builds or fault/regression campaigns alongside clean capacity tests.

## CQRS decision

Read/write separation does not require a second database, Event Sourcing or a
broker migration. The present implementation separates read execution budgets
from source polling/admission and provides a disposable bounded query projection.
It is an incremental read-model optimization, not a claim that a complete CQRS
architecture has been implemented. Canonical writes and reconciliation remain
the authority for correctness.

Introduce a separately maintained event-driven read store only if concurrent
measurements show canonical fan-out/reconciliation dominates miss latency or
database CPU/I/O after cache coalescing. A gateway, client CPU or network ceiling
is not fixed by another read database. Before adopting the larger design, define
partition placement (including P1's prohibition of a full dataset per node),
durable mutation/retraction propagation, idempotent replay, atomic publication,
per-partition no-gap progress and bounded staleness. A delayed projection must
never silently answer an Audit request as complete. Those consistency and recovery
costs are why the larger change is deferred, not a claim it could never help.

Source patterns: [AWS caching](https://aws.amazon.com/builders-library/caching-challenges-and-strategies/),
[AWS overload protection](https://aws.amazon.com/builders-library/using-load-shedding-to-avoid-overload/),
[Microsoft CQRS](https://learn.microsoft.com/en-us/azure/architecture/patterns/cqrs),
[Envoy least-request](https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/upstream/load_balancing/load_balancers).
The small Go gateway implements the relevant policy without adding Envoy's
deployment/API dependencies; it is not Envoy and inherits no claim of Envoy's
full feature coverage or validation.

## Gateway memory budget

The first 10k follow-up exposed a real failure: both gateways were OOMKilled
under the unchanged 256Mi container limit. A bounded active-request semaphore
does not bound persistent connection buffers/stacks, garbage between GC cycles,
or memory charged to sockets by the kernel. Three Aggregators staying Ready
does not establish a healthy common entry.

The gateway manifest now sets `GOMEMLIMIT=160MiB`, leaving 96MiB of the existing
256Mi limit for transient and non-Go/cgroup overhead. This is a **soft Go runtime
target**, not an RSS ceiling, additional RAM, or a proof against OOM. Default
GOGC, client deadlines, VUs and container limits are unchanged. It may increase
GC CPU; if live memory cannot fit, connection/memory design still needs work.
This choice must be validated under the original load, not by reducing VUs or
increasing RAM. See the [Go GC guide](https://go.dev/doc/gc-guide) for the meaning
and limitations of a runtime memory limit.

`gateway_connections_open` counts the lifetime of real client connections,
not request concurrency. Common telemetry exposes Go-accounted memory and the
configured runtime limit alongside heap/goroutines. Scale evidence now captures
both gateway pod identities, readiness, restart counts and last termination
reasons. New gateway restarts/replacements fail the scale check even when an
aggregate error percentage might otherwise hide them. Generator RSS tracking
and interrupts are restricted to the run's own systemd scope.

The scale runner additionally checks `(partial + malformed + other) /
(complete + partial + malformed + other) < 1%`. This excludes controlled 429s
from the denominator as well as the numerator, so rejections cannot dilute
semantic failure. A run containing only rejections fails this extra check.
Existing workload/threshold checks remain; a separate post-review can only
tighten a retained result, never erase its original failed verdict.
