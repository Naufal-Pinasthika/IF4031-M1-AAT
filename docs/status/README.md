# Source and verification status

Snapshot: 22 November 2026 (WIB). Results below distinguish repeated practice
E2E evidence from historical results and the later assistant/exam comparison.

## Latest P2 external-generator update

A separate 8-vCPU / approximately 8-GB VPS now runs k6 1.4.2 outside Kubernetes.
It reaches the existing common gateway over WireGuard; SSH carries control and
evidence only. Production images, volumes, budgets and the original Sensor
Pantai state were not changed by the P2 campaign.

The first completed direct-VPN scaling test held 10,000 VUs for 120 seconds and
observed 3→5→2 Ready Aggregators. It completed 891,648 requests: 688,716 complete
2xx responses and 202,932 controlled 429s, with zero unexpected failures and
p95 1.513 seconds. Eighty BMKG/PVMBG polling attempts reconciled with upstream
logs without multiplied endpoints, cadence violations or repeated admissions.
The replica count was restored to three. This is a bounded workload/scaling
pass, **not an overall P2 pass**; comparison, clock/freeze and rolling evidence
must be evaluated separately.

The external-generator throughput comparisons subsequently measured **1.525×**
and **1.290×**, below P2.7's ≥2× target, despite zero unexpected errors in each
case. Single-instance complete throughput was 3,415–4,043/s; three-instance
complete throughput was 5,208–5,216/s. A separate idle-path WireGuard diagnostic
carried 833–849 Mbps, so a hard 500-Mbps path cap is not established. Subsequent
profiling found significant generator JSON parsing/GC and gateway network-copy
cost, with no added cgroup CPU throttling in the sampled pods. Using the same
workload and validation with an explicitly recorded generator runtime (8 Go
processors, GOGC 300) measured **4,509 → 6,344 complete RPS (1.407×)**, still
below the target. Response coalescing, internal gzip and three gateway replicas
did not improve the result and were rolled back; original application code,
images and the two-gateway topology were restored. The completed
counterbalancing campaign reconciled 64 further mock polls without duplication
or cadence violations and restored three Aggregators.

Two separately labelled generator/path calibrations served an exact real
20-event response snapshot from a temporary authenticated private listener,
without the database or gateway. They reached **6,800–6,862 complete RPS**
with the original k6 parsing and validation. These are not application results
or P2 passes; they show insufficient measurement headroom for proving about
9,018 RPS (twice the 4,509-RPS baseline) with this setup. Additional independent
generator capacity should be measured before attributing the remaining ratio
to Kafka or database scale-out.

Three prior actual-owner crash rounds also passed the mock-window coverage and
deduplication checks, taking 3.389–7.209 seconds to complete takeover. Failed
SSH-forwarded laptop campaigns and invalid negative timing samples remain
recorded, rather than being reclassified as passes. See the
[P2 verification report](../changelog/P2-capacity-verification.md) and
[reproducible commands](../../test/README.md#p2-capacity-and-polling).

## Latest P5 critical-path update

P5 work has resumed. **Overall P5 is still not passed.** Aggregator now runs
`m2-p5-source-logs-20261122-r2`; notifier runs `m2-p5-critical-20261122-r1`,
with an additional `hazard.critical.v1`
topic and independently acknowledged outbox projection/consumer lane. The
general event stream, original PVCs, credentials, consumer offsets and Sensor
Pantai spec/state were retained. Gateway subsequently moved to
`m2-p5-discovery-grace-20261122-r1` for the scoped routing fix below.
Auth now runs `m2-p5-auth-queue-20261122-r3` for bounded Field burst admission.
Production remains three
Aggregators and one bbolt-backed notifier; redundant notifier cutover and
Kafka/coordinator HA are not completed by this rollout.

Repeated laptop-driven VPS evidence:

| Test | Result | Private evidence under `.e2e/report-tests/p5/` |
| --- | --- | --- |
| Ordinary backlog isolation, 3 rounds | 90 critical alerts; p95 0.730 / 0.905 / 0.791 s while 9,825 / 19,445 / 29,032 ordinary records remained pending | `08-critical-lane/20261121T214026Z-c91fd6a1` |
| Transport/payload/source-clock/polling-owner faults, 3 rounds with two timestamp origins each | 1,116 unique notifications, 396 critical, no missing identities; six window p95 values 3.113-3.792 s | `06-resilience/20261121T214215Z-6dcf59fc` |
| Redundant notifier pod crashes, 3 rounds | 90 new critical alerts plus 3 priming alerts; exact 93 durable rows, no duplicates; crash-window p95 0.771 / 1.501 / 0.800 s | `09-notifier-failover/20261121T214955Z-6a5b4f38` |
| Coastal contract validation, 3 rounds | Complete canonical audit contains exactly 21 valid identities; all 21 invalid payloads remain parked with reason/time and their contents intact | `10-contract/20261121T215627Z-7dbf0838` |
| Genuine production-source outage, 3 x 30 s | 10 / 9 / 9 logged outbound attempts, each below bound 42; last-data Field read p95 0.118 / 0.283 / 0.064 s | `04-circuit/20261121T220522Z-cc95bd25` |
| Immutable official-source observation, 3 x 90 s | All 3 observed critical identities durably confirmed; conservative latency upper bounds 1.297 / 1.130 / 1.097 s; no new critical sample in the third window; old production source remains unavailable | `07-source-age/20261121T220451Z-98029404` |
| Priority overload, 3 x 60 s from the laptop | 601 / 600 / 600 Field requests, zero Field errors or dropped iterations; 350,351 / 337,023 / 343,581 controlled Media 429 responses | `02-load/20261121T221131Z-64c9bbf6` |
| Source HTTP-error logging under the priority campaign | Two additional timestamp-origin windows: 372 unique notifications, 132 critical, zero missing identities; p95 3.755 / 3.668 s, actual source 503 logs retained | `06-resilience/20261121T221217Z-72ee29b5` |
| Combined priority overload and tracing after the gateway fix, 3 x 120 s | 3,603 Field requests, zero Field failures or dropped iterations; all 9 sampled traces include Client API, gateway and Aggregator; BMKG successful-read p95 127.908 / 114.539 / 104.337 ms | `05-observability/20261121T223047Z-7a08b2cc`, `20261121T223247Z-3d5c62f6`, `20261121T223445Z-23397bb4` |
| Concurrent notifier health during the last combined run, 3 x 10 s | 87 health/stats/metrics responses, all HTTP 200; maximum window p95 69.750 ms; same process, zero restarts | `00-health/20261121T223513Z-784472c2` |
| Priority overload after the Auth queue fix, 3 x 60 s | 1,803 Field requests, zero Field failures or dropped iterations; all 65 Field token issue/refresh attempts returned HTTP 200 | `02-load/20261121T224620Z-2928c8f2` |
| Final combined load/trace with the Auth queue, 120 s | 1,200 Field requests, zero Field failures/dropped iterations, 42 successful Field token attempts, all 3 cross-service traces present | `05-observability/20261121T224921Z-e744d097` |
| Final log-subscription verification under the same priority load, 60 s | 601 successful Field requests and 40 successful Field token attempts; server-stream counts match both exactly | `02-load/20261121T225213Z-238e9b91` |

These use a controlled HTTP contract source, real candidate services, Kafka and
VPS databases. They do not replace the immutable official Sensor Pantai or
establish official-source zero loss/latency. Timestamp origins test clock
independence, not the age of the official process. The notifier crash experiment
uses three independent critical consumer groups and a shared notifier-owned
database, with two surviving readers; its production equivalent is not yet
enabled. Kafka/coordinator crashes and node loss are separate gates.

In the 396-alert campaign, **4** alerts exceeded five seconds (maximum **8.690 s**),
against a 19-alert late budget for this sample size. Loss budget is zero.
Passing aggregate/window p95 does not mean every alert met five seconds.
The longest sample followed a controlled connection-reset phase; failed
experiments and the original singleton's roughly 38-second crash result remain
historical evidence, not erased by these new results.

Service-local PostgreSQL regressions were actually run on a VPS: critical
projection progress despite general-target failure, same-version replay,
persisted retry cadence across owner changes, timestamp admission precision,
concurrent notifier deduplication and idempotent legacy import passed. Their
private logs are `.e2e/p5-aggregator-store-1795297596765060373/` and
`.e2e/p5-notifier-store-1795297641861801530/`. Ordinary local Go tests/vet also pass;
other environment-gated integration tests are not claimed as newly executed.

Harness corrections are distinct from service fixes: Python 3.10 fractional
RFC3339 parsing, stable-reader replacement checks against current rather than
already-committed priming messages, and PostgreSQL regression search-path
isolation. Earlier harness failures were retained. Four old regression-only
tables containing exactly two test notifications and three processed test IDs
were verified and removed from the coordinator public schema; production
notification/canonical data was untouched. Future regression runs assert their
temporary schema and clean it up. Build transfer fixes separate per-service
archives and binary SSH streams; no application data was deleted.

Rollout evidence: `.e2e/critical-lane-rollout-20261121T215119Z/` and
`.e2e/aggregator-update-20261121T221017Z/`. The r2 delta records one outbound
attempt log for HTTP errors and incomplete response bodies as well as network
failures/successes, including status, correlation ID and duration without tokens.
Previously HTTP 503/body failures could be absent from the attempted-request
count. Three repeated unit regressions for HTTP 200, HTTP 503 and truncated
response bodies pass; full Aggregator/notifier Go tests and vet pass.

Official-source observation uses only public contract endpoints and opaque
cursors, never internal state or a selected virtual age. Its bounded independent
observer now reaches actual empty pages; the earlier one-request-per-sample
observer continually trailed live arrivals and could not establish availability
bounds. The earlier inconclusive run remains retained. No source image, memory
limit, production pod/state or cursor was reset. The new run is still
**inconclusive as a whole**: only three bounded critical samples, an empty
critical-sample window and no production-source recovery do not establish
general P5.1/P5.4 acceptance. The fresh official source had zero restarts.
The three 60-second priority rounds rejected Media traffic explicitly; their
request totals are not successful business throughput. BMKG successful-response
p95 was 103.64 / 94.43 / 97.82 ms. Unexpected-error rates were 0.0639 / 0.0847 /
0.4819 percent, including the startup Media authentication failures in the
error accounting. None of these rounds lost a Field request.

A subsequent **120-second combined load/trace run failed**, even though all
three sampled traces contained Client API, gateway and Aggregator. One of
1,200 Field requests returned HTTP 503 (`p5-1-FIELD-94-56`, 27.665 ms); the zero
Field-error threshold was not relaxed. Private evidence is
`05-observability/20261121T221755Z-afb14300`. The original failed request's
server logs had rotated before retrieval. Historical Prometheus counters in
the same minute show a DNS discovery failure and a `gateway_no_backend_total`
increase on B, with zero probe failures, upstream errors or read failovers.
This supports a routing/discovery failure rather than a measured backend
failure, but is not an exact per-request trace. Diagnostic evidence:
`diagnose-503/20261121T222628Z-e661dd53`. Three repeat 120-second runs before
the gateway fix passed, each with 1,201 Field requests and all three sampled
cross-service traces; that intermittent success does not erase the earlier
failure. Repetition now captures the Field request path live, and failure
diagnostics include the response error without credentials.

A deterministic service-local regression reproduced HTTP 503 when the last
successful DNS answer was four seconds old despite a newly successful backend
probe. Discovery and probe validity had shared an overly short three-second
budget. Gateway `m2-p5-discovery-grace-20261122-r1` separates their lifetimes:
known endpoint identity expires after 30 seconds without successful discovery,
but routing still requires a successful probe within three seconds. Failed
probes withdraw eligibility immediately; authoritative empty DNS results remove
endpoints immediately. A DNS failure never renews the 30-second lifetime.
No retry of application responses or writes was added. Unroutable decisions
now retain correlation ID, HTTP status and reason in logs.

Both new failure regressions failed on the earlier code and pass after the
change; expired probe, expired discovery and authoritative-removal cases also
pass. Gateway tests ran three times; all Aggregator Go tests/vet pass. The
scoped two-gateway rollout retained the other protected pods and all original
PVC identities: `.e2e/gateway-discovery-rollout-20261121T223009Z/`.

After that rollout, all three combined load/trace campaigns passed their
unchanged thresholds. Media received 740,959 / 693,661 / 741,141 explicit HTTP
429 responses; unexpected-error rates were 0.0442 / 0.0410 / 0.0289 percent.
These are not counts of successful business requests. Live Field-path logs
were captured before rotation; they contain 1,201 / 1,196 / 1,196 Client API
HTTP 200 completions versus k6's 1,201 in each round. The two shorter captures
each contain only 15 of the 20 first-iteration completions: the log subscription
had started too late, not a measured business failure. The runner now waits for
every selected pod stream to deliver its initial tail record before k6 starts,
discards those old tail records, and drains for two seconds after k6 finishes.
Full durable log retention is not claimed.

Post-run counters on both current gateways were zero for discovery failures,
probe failures, upstream errors and no-ready-backend rejections. Thus live
campaigns verify workload behavior, while the deterministic local regression
is the evidence for routing through a transient DNS failure; a real DNS outage
was not injected into the production cluster. The read-only gateway-checkpoint snapshot is
`post-discovery-snapshot/20261121T223731Z-149f13cc`. It confirms unchanged Sensor
Pantai pod identity (still OOMKilled/CrashLoopBackOff, 105 restarts), three Ready
Aggregators, two Ready gateways, Ready notifier/Kafka/coordinator, and the
original official Tester Agent DaemonSet with three Ready replicas. An earlier
snapshot check incorrectly compared that DaemonSet UID to pod UIDs; its failure
is retained separately, and the corrected check compares the same resource type.

### Later Auth burst failure and fix

A further cold-start priority round **failed** after the three successful
combined runs: 14/600 Field iterations were rejected while acquiring a token,
before any business request reached the gateway. All 586 issued Field business
requests returned HTTP 200. The failure remains in
`02-load/20261121T223805Z-79190a34`; its 1.296 percent unexpected-error rate also
crossed the unchanged one-percent threshold. The existing Field reserve was
enabled (32 active slots, default reserve eight), and runtime/test credentials
matched; no credential values were printed during that comparison.

The reserve protected Field from Media-only occupancy, but immediate rejection
still dropped a legitimate Field burst when the shared active budget was full.
A deterministic burst regression failed with HTTP 429 on the prior limiter.
The limiter now retains the same active budget and regular-traffic ceiling,
but valid Field token issuance/refresh can wait **at most one second** in a
waiting room capped at the configured active-budget size (32 on this VPS).
Only Field callers wait; regular traffic still sheds immediately. Semaphore
handoff admits queued Field work as active requests finish. Context cancellation
and a full waiting room release/reject promptly rather than accumulating work.
Client ID, constant-time secret check, method/path, database authentication,
client enablement, scopes and refresh-token rotation remain authoritative;
spoofed priority headers cannot access this queue.

Burst, spoofing, active-budget and waiter/cancellation regressions pass, as do
all Auth Go tests/vet. Auth was rolled independently, retaining original PVCs
and other workloads (`.e2e/auth-update-20261121T224526Z/`). A first build failed
on a Docker Hub TLS handshake before any rollout; retry built the unchanged
candidate successfully. Later load evidence must be evaluated separately from
the preceding intermittent successes.

The first post-queue three-round campaign passed without lowering load or
thresholds: 601 Field requests per round, zero Field failures/dropped iterations,
and all 60 token issuances plus five refreshes returned HTTP 200 (maximum
observed Field Auth response 503.396 ms). BMKG successful-read p95 was
119.848 / 178.907 / 120.334 ms. Media received 335,040 / 344,801 / 327,873
controlled HTTP 429 responses; unexpected-error rates, still counting failed
Media authentication attempts, were 0.0757 / 0.0523 / 0.1204 percent.

The subsequent combined 120-second run also passed: 1,200 Field business reads,
42 successful Field issuance/refresh attempts, three complete sampled
Client API/gateway/Aggregator correlations, no Field errors or dropped
iterations, and BMKG successful-read p95 141.396 ms. Media received 680,950
controlled HTTP 429 responses; unexpected-error rate was 0.0355 percent.
The final 60-second stream-readiness check passed with 601 Field business reads
and 40 successful Field token attempts (20 issuance, 20 refresh). Its live
server log counts match both exactly after the subscription-handshake fix;
BMKG successful-read p95 was 107.551 ms, Media 429 count 344,657 and
unexpected-error rate 0.0675 percent.

Across these five post-Auth-queue load rounds, **3,604** Field business requests
and **147** Field token attempts succeeded, with zero Field failures or dropped
iterations. This is bounded repeated workload evidence, not a guarantee for
unlimited Field traffic or proof of full P5. Fresh official-source coverage,
automatic recovery of the old production source, production redundant-notifier
cutover and Kafka/coordinator/node-loss combined-fault acceptance remain open.
The final read-only pod snapshot is `.e2e/p5-closeout-20261121T225407Z/`;
Auth, both gateways and notifier are Ready without restarts, while the unchanged
production sensor remains in CrashLoopBackOff after OOM (108 restarts).

Observability image `p2-obs-20261121T221605Z` prioritizes ingress log retrieval
before slower downstream tails, fixing a reproducible trace-eviction race.
Both service-local Python regressions pass, and the ingress-race regression
fails on the old implementation. Prometheus retained its pod/history during
the rollout (`.e2e/p2-obs-20261121T221605Z/`). Correlation lookup remains an
explicitly bounded log search, not durable full-span retention. Combined-fault
recovery and production redundancy also remain open until measured.

## Latest Tester Agent / polling update

### Full-practice follow-up, 22 November

The prior P2 work was committed in three pieces, pushed and merged into `main`
as `37fe9d3`. Further fault work is on `test/tester-agent-full`, not merged back.
New repeated preflight, pod-kill, actual-owner failover and compatible-schema
checks passed. Three strengthened official store-kill rounds observed 54/56/60
valid reads and stable recovery in 67.9/71.5/70.4 seconds. Three high-frequency
clock rounds observed actual 300-second jumps with successful API reads and
clock recovery. The displaced clock was corrected after roughly 0.2 seconds;
this does not prove a sustained 30-second skew or the entire P2 fencing gate.

Partition practice initially failed because Ubuntu's absolute `iptables`
alternatives link escaped the Agent's host bind mount. Provisioning now retains
the selected distro binary through a relative symlink, saving the original link
and verifying unchanged firewall rules/version. Subsequent Agent steps completed,
but the final independent TCP/kernel-event observation **did not establish the
intended network partition**. Earlier passes based on isolated TCP failures are
insufficient and are superseded by this stricter result. Do not claim partition
resilience passed. No Agent image, permissions or scenario was modified.

The operator approved `wipe-node` after backup. A completed private off-cluster
backup contains 26 files / 628,875,856 bytes; full-file SHA-256 checks passed
immediately before submission. The baseline includes 553,167 non-SATELIT
canonical identities. After official `wipe-node` submission, C became unreachable
on both SSH and kubelet; A/B remained Ready. The initial runner stopped on an
Agent-log proxy error, so it did not establish uninterrupted availability through
the whole wipe. A supplemental 120-second observer recorded 172 read samples, including
one Field transport timeout at about 20 seconds; later observed reads succeeded.
That timeout is a real open availability issue, not a controlled overload pass.

C subsequently powered off again after operator starts. Previous Agent logs
confirm that its persistent practice request `001be28a410f` replayed on startup:
`wipe-node` began again at 04:21:39 UTC and `wipe_and_shutdown` at 04:21:44 UTC.
The exact own practice trigger was privately backed up and removed using a
resource-version/test-and-remove patch. The Agent image, specification, privileges
and identity were retained; no other operator request or exam definition was
changed. C then stayed online after the operator started it. All three nodes and
official Agents returned Ready, without a VPS rebuild or backup restore.

The canonical store rebuilt C automatically. A direct physical SQL comparison
found **zero missing baseline identities and zero with fewer than two copies**
among the 553,167 non-SATELIT identities. This establishes physical preservation
and restored redundancy, not uninterrupted API availability during the wipe.
A subsequent full Audit enumeration still encountered an explicit partial page.
Reproduction at that same cursor identified `coverage_unproven`, not a database
outage: a 1,000-row Audit page could not enlarge its candidate prefix, and refills
incorrectly tried to certify the enlarged internal page rather than the caller's
requested prefix. The correction retains three bounded rounds and real partial
responses when coverage is unproved. A PostgreSQL regression fails on the old
code and passes three times after the fix; the separate latest-version oracle
and unavailable-replica regressions also pass three times.

Client API now bounds ordinary gateway response-header waiting, reconnects once
for bodyless GET/HEAD transport failures, and uses headless gateway discovery.
Writes, HTTP error responses and partial response bodies are never replayed by
this wrapper. Three isolated VPS fault rounds returned real canonical Field data
in **2.188 / 2.199 / 2.225 seconds**, with the exact pooled gateway frozen and
the other gateway healthy. This is not proof for every arbitrary network fault.
The first scoped production rollout exposed two refused Client API connections;
that failed observation is retained. A follow-up adds termination drain and
endpoint-withdrawal delay. The deployed versions are
`aat/aggregator:m2-audit-prefix-20261122-r1` and
`aat/client-api:m2-client-read-failover-20261122-r2`. Three subsequent Client API
rolling restarts returned **175/175 successful Field reads** (58/58/59 samples).
Three full post-wipe Audit enumerations each completed **564 pages**, all
complete, and retained **all 553,167 baseline non-SATELIT identities**, with zero
missing. No backup restore or manual data copy occurred. These close the observed
read-path defects; they do not retrospectively turn the interrupted wipe window
into an uninterrupted-availability pass or establish every combined-fault SLO.
After these fixes, three further official `kill-store` rounds passed with
**118 / 106 / 102 valid read samples**, zero invalid reads and stable automatic
recovery within 72 seconds in every round. The independent observer stayed
active through fault and recovery. Each exact own trigger was removed only after
the window; the original Agent and volume identities were retained.

Still open: an independently demonstrated network partition, sustained skew /
all fencing assertions and combined-fault acceptance. The staged coordinator and
notifier database clusters each retain one unready C replica after their local
data was wiped; production does not use those staged clusters. The immutable
long-running Sensor Pantai also remains unready. The production bbolt notifier
is Ready. These observations do not justify an overall Tester Agent/P5 pass.

Before another destructive campaign, the runner keeps API observation independent
of log retrieval, bounds/tolerates unavailable logs, checks node readiness and
removes only its own exact persisted practice trigger after the observation
window. No new wipe has been submitted. Private evidence:

- `tester-agent/07-backup/20261122T010226Z-bc473262`
- `tester-agent/04-kill-store/20261122T010559Z-9bc318d1`
- `tester-agent/05-clock-skew/20261122T011050Z-45116ebe` and `20261122T011214Z-f683c553`
- `tester-agent/06-partition/20261122T012214Z-a2213715`
- `tester-agent/08-wipe-node/20261122T012341Z-62bb25e3`
- `tester-agent/wipe-observer/20261122T012637Z-c279af96`
- `.e2e/tester-practice-cleanup-20261122T042334Z`
- `.e2e/post-wipe-physical-audit-20261122T043202Z`
- `tester-agent/audit-diagnosis/20261122T044102Z-6e8a50e8`
- `tester-agent/09-gateway-failover/20261122T044401Z-ec9585c7`
- `tester-agent/read-recovery-rollout/20261122T044837Z-f9f66f23`
- `tester-agent/10-audit-recovery/20261122T045000Z-c8282067`
- `tester-agent/11-api-rollout/20261122T045305Z-1ca2dd7d`
- `tester-agent/00-preflight/20261122T045435Z-54c1c25e`
- `tester-agent/04-kill-store/20261122T045502Z-a3203fff`

### Earlier checkpoint

At that checkpoint P5/HA work was paused. Official Agent discovery and three `kill-leader` practice
rounds passed, but those faults hit a standby Aggregator on C while BMKG/PVMBG
polling belonged to A. They did not prove polling takeover.

The supplemental targeted-owner campaign found a real precision bug: warning
fingerprints changed when a parent timestamp was read back from PostgreSQL at
microsecond precision. A real PostgreSQL regression failed for both single and
batch admission before normalization and passed afterward, including a check
that genuine microsecond-level changes still create a new version.
An earlier counting-oracle failure was separate: mock IDs had been reused on
earlier days, so lifetime historical versions were not duplicate admissions in
the current test window. Both failures remain in private evidence.

Three final targeted-owner crash rounds passed after the fix and a bounded
gateway connection-establishment failover: completed polling resumed under a
new fencing epoch in **3.44–7.12 seconds**, all **13** independently enumerated
window events appeared in complete canonical audit pages, current-window
admissions were unique, and upstream request logs reconciled with poll IDs and
cadence without multiplied requests. All **18** sampled Field reads returned
200 (maximum **0.133 seconds**). Two actual `gateway_read_failover` log entries
showed a refused dead-pod connection retried on a different healthy backend.
The earlier campaign's one `backend_unavailable` 503 is retained, not counted as
controlled overload or erased. Writes, application 503/429 responses and partial
response bodies are not replayed by this fix.

Runtime: Aggregator `m2-p2-precision-20261122-r1` (three Ready replicas), gateway
`m2-p2-read-failover-20261122-r2` (two replicas). These were built from staged
main-baseline sources plus the scoped fixes, not the entire dirty P5 worktree.
Data volumes, offsets and credentials were retained. Evidence:
`.e2e/report-tests/tester-agent/02-polling-failover/20261121T211937Z-10813ab2/`.

Official schema practice exposed missing mock Service discovery, a 401 on
the authenticated data/admin listener, and the unsupported `version` request
field. After adding a separate platform-restricted control listener and an
idempotent schema-version API, **three official schema practice rounds passed**:
the Agent received 200, new additive-schema inbox messages reached `done`, and
PVMBG/Aggregator pods did not restart. Missing Bearer and BMKG credentials still
returned 401 on the data port. A normal application pod could reach the data
port but not the control port; positive controls confirmed that the latter was
live. Node/cluster administrators retain their inherent privileges. The Agent
manifest/image were unchanged. Runtime PVMBG is
`m2-agent-admin-20261122-r3`; evidence is
`.e2e/report-tests/tester-agent/03-schema/20261121T212655Z-be33e6b2/`.
This is not an overall P3 pass. `kill-store`, `clock-skew`, `partition`, `wipe-node`, freeze,
10k-VU sustained/scaling and throughput acceptance were not newly verified by
this campaign. No overall P2/P5 pass is claimed. Reproduce with the numbered
cases in [`test/tester-agent/`](../../test/tester-agent/).

## Previous P5 update (before critical-lane rollout)

See [P5 resilience verification](../changelog/P5-resilience-verification.md).
Notifier statistics/probes, source-filtered database indexing and Field Auth
admission were fixed and deployed without deleting original data or increasing
memory limits. Repeated health, practice isolation, final priority and component
crash/dedup checks pass. **Overall P5 is not passed:** genuine Sensor Pantai still
OOMs, source-to-notifier latency has no new official-source samples, singleton
crash latency is around 37 seconds, general-topic consumer backlog remains, and
correlation-log retention failed under overload. Quiet dashboard checks do not
erase that failure. Encrypted exam resources remain deferred.

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
