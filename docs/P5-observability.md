# P5 measurement and observability (singleton infrastructure)

Latest [singleton retest summary](README.md) additionally observed
and strictly checked an asynchronous Aggregator → both-consumers trace, including
publish/fetch/delivery/commit durations. This is sampled bounded-log evidence,
not guaranteed coverage of every event. Fresh critical SLO samples remain absent.

On 19 November 2026 the operator explicitly deferred HA and requested the
pre-replication topology: one coordinator, one Kafka broker and one notifier
using its node-local bbolt inbox. The experimental HA manifests, Patroni/DCS,
router, notifier PostgreSQL backend and HA deployment flag have been removed
from the active implementation. Canonical P1 storage changes are not reverted.
Historical failed HA tests are not evidence for the current topology.

## Critical-alert measurement

`scripts/m2/p5/06-reliability.py` measures a conservative upper bound with one
observer's monotonic clock: the request start of the last empty unfiltered
source cursor page, followed by first observation of a new critical identity,
then completion of `POST /notifications/confirm`. The latter reads a committed
bbolt transaction. Source availability is interval-censored, not an exact vendor
timestamp. Initial historical IDs count for observed delivery comparison, not
new latency samples. Neither source virtual timestamps nor notifier wall-clock
timestamps are subtracted to claim the full SLO.

No new critical samples means **unproven**, not p95=0 or a pass. Missed source
coverage, absent confirmations and invalid critical records remain explicit.
Three observer tests enforce these boundaries, including contradictory source /
notifier wall clocks. The prior 422ms ingress result is still not source-to-
notifier latency. Exam resources remain deferred.

## Private operations dashboard

Prometheus scrapes all seven application services every 5 seconds. Bounded
process-local counters and histograms report HTTP throughput, 5xx, controlled
429, p95 estimates, heap and goroutines. Arbitrary paths and correlation IDs are
not metric labels. Notifier event counts remain separate from API reads.

The read-only dashboard combines metrics, source breaker state, pod placement /
readiness and correlation-log traces with per-step durations. Traces cover the
last 15 minutes / 5,000 lines per pod; incomplete retention and unavailable pods
are shown explicitly. It is not a guaranteed full-retention OpenTelemetry
backend. Official sensor/broker internals remain black boxes.

RBAC permits only namespace-local pod discovery and log reads: no Secrets, pod
exec, workload mutation, privileged mounts or SQL credentials. Monitoring has no
public HTTP entry point. Prometheus monitoring data is disposable, retained for
6h / 256MB under the Appendix B memory budget.

```bash
# From the repository root in WSL; keep this terminal open.
python3 infra/vps/access.py observe
# Open http://localhost:8090 in your browser.
python3 infra/vps/test-observability.py
python3 scripts/m2/p5/test_reliability.py
```

Functional evidence before rollback: `.e2e/p5-observability-20261119T071030Z.json`
scraped all seven service types and reconstructed 11 log records from a real
Auth → Client API → Aggregator workflow. It did not prove the asynchronous
event trace complete. Automated browser access was unavailable, so visual QA
is not claimed. See the [singleton verification summary](README.md)
for verification of the restored build, including post-rebuild dashboard checks.
