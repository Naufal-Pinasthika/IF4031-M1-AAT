# P2 capacity verification

Campaign date: 22 November 2026 (WIB). This is a new test of the deployed
P5-era builds, not a redeployment or an overall P2 acceptance pass.

## Polling owner crashes

`test/tester-agent/02-polling-failover.py --vps --allow-changes --repeat 3`
passed all three rounds. Each round killed the actual recorded BMKG owner;
PVMBG was also owned by that pod and was checked independently.

| Round | BMKG completed takeover | PVMBG completed takeover | Newly generated BMKG / PVMBG records |
| --- | --- | --- | --- |
| 1 | 3.389 s | 6.150 s | 2 / 2 |
| 2 | 6.089 s | 6.089 s | 3 / 3 |
| 3 | 4.574 s | 7.209 s | 4 / 2 |

All observed Field reads succeeded. Every independently enumerated source-window
record reached a complete canonical audit page, identities were unique and each
record had one admission. Correlated mock logs matched one request per completed
poll endpoint, with no multiplied logical polling cadence.

This is evidence for the normal-clock mock crash window, not a proof of
arbitrary-failure exactly-once HTTP, clock skew, whole-VM resume, satellite
takeover or availability of the currently unhealthy coastal source.

Private evidence:
`.e2e/report-tests/tester-agent/02-polling-failover/20261121T230312Z-211f8572/`.

## Initial throughput comparison: below target

Same endpoint `/internal/events?limit=20`, 1,000 VUs, 60 seconds per case,
10 seconds stabilization, order 1→3→3→1. Only complete valid responses count.

| Pair | One instance complete RPS | Three instances complete RPS | Ratio |
| --- | --- | --- | --- |
| 1 | 1,341.957 | 1,853.228 | 1.381× |
| 2 | 1,924.669 | 1,805.530 | 0.938× |

Neither pair meets P2.7's ≥2× requirement. All four original k6 error thresholds
returned exit zero; the first case nevertheless had 297 unexpected responses
(0.356%). The other three had none. Controlled 429s were excluded from complete
throughput, as were partial and malformed results.

The final single-instance case also reported an impossible negative minimum
HTTP duration (-1,574.894 ms). Its latency output is not valid acceptance
evidence. The cause has not been established. Linux recorded a loopback SYN
backlog warning on the SSH forward. Received k6 data ranged from approximately
112 to 160 Mbps; sampled VPS CPU was not saturated. These facts do not isolate
WAN, SSH, generator and application limits from one another.

Private evidence:
`.e2e/report-tests/p2/01-capacity/20261121T230457Z-b4c4c044/`.

## Sustained scaling: transport failures, not a pass

The unchanged 10,000-VU / 120-second hold workload was attempted through SSH:

1. `20261121T231028Z-5aa20570`: local SSH exhausted file descriptors; interrupted
   after retaining `Too many open files` evidence.
2. `20261121T231152Z-18680dce`: after raising only test-process descriptor limits,
   OpenSSH exited with `channels_alloc 16390 too big`; interrupted.
3. `20261121T231457Z-0ec982de`: four independent tunnels all targeted the same
   private gateway NodePort. Five and later two Ready Aggregators were observed,
   but one tunnel again exhausted its internal channel capacity. The runner
   detected its exit and stopped. Generator peak RSS was about 2,921 MiB with no
   memory safety stop; this is not a completed 120-second acceptance hold.

The replica count was restored after each attempt. No original volume, sensor
state, application image, resource budget, public firewall rule or node clock
was changed. The existing unhealthy Sensor Pantai was not reset.

These attempts do not prove P2.6's error budget or upstream guarantees throughout
a completed sustained run. Retain failures; do not label scaling readiness alone
as a pass. A direct private VPN path is the next transport to validate; no public
application/database ports were opened by this campaign.

## External-generator scaling result

`20261121T233519Z-b38d3e82` completed the original 30-second ramp, 120-second
10,000-VU hold and 30-second ramp down over direct WireGuard. The controller
observed 3→5→2 Ready replicas while the workload ran, then restored three.

| Measurement | Result |
| --- | --- |
| Requests | 891,648 |
| Complete HTTP 2xx | 688,716 (3,825.403/s over the k6 campaign) |
| Controlled HTTP 429 | 202,932; excluded from successful throughput |
| Unexpected failures | 0 |
| HTTP p95 | 1,512.884 ms, below the unchanged 2,000-ms threshold |
| Generator peak RSS | 3,971.664 MiB |
| Lowest generator available RAM | 3,362.117 MiB; no swap configured |
| Generator peak sampled CPU | 5.943 cores on the 8-vCPU machine |
| Mock logical polls reconciled | 80 |
| Missing upstream log entries / duplicated poll endpoints | 0 / 0 |
| Polling cadence violations / repeated mock admissions | 0 / 0 |

Both original k6 thresholds passed, response timing was nonnegative, all 2xx
responses had complete parsed payloads, and there was no generator safety stop.
This passes the workload/scaling and observed BMKG/PVMBG polling checks for this
campaign. It does not count 429 as successful service or establish all P2
criteria. Source-window completeness during owner crashes is evidenced by the
separate three-round failover campaign, not inferred from the k6 counter.

The source Sensor Pantai remained unhealthy and was not reset. Clock-skew,
whole-VM freeze/resume and a genuine new-version rolling update were not part
of this run.

## Other comparisons and remaining acceptance

### External-generator throughput remains below 2×

The completed first pair in `20261121T233910Z-19117f64` measured
3,415.310→5,207.500 complete responses/s: **1.525×**, below 2×. The controller
was interrupted during its second pair, so that campaign has no final overall
result; its completed individual summaries are retained. No unfinished case is
claimed as a pass.

The missing counterbalancing 3→1 pair was run separately with `--reverse-first`
in `20261121T234613Z-7411313e`:

| Order | One-instance complete RPS | Three-instance complete RPS | Ratio |
| --- | --- | --- | --- |
| 1→3 | 3,415.310 | 5,207.500 | 1.525× |
| 3→1 | 4,043.144 | 5,216.128 | 1.290× |

Both used 1,000 VUs, 60 seconds per case, 10 seconds of stabilization, k6 1.4.2,
the same uncompressed request/validation and common gateway, and the same
deployed Aggregator image. In both single-instance cases the surviving pod was
on node B. All four k6 case thresholds passed with zero unexpected failures and
valid nonnegative timings. Controlled 429s are not successful throughput. The
second campaign correctly exits failed because the ratio is below the unchanged
target, even though its per-case error thresholds pass.

That completed counterbalancing campaign reconciled 64 mock polling attempts:
zero missing log entries, duplicate endpoints, cadence violations or repeated
admissions. Three Aggregators were restored afterward. No application image or
resource budget was changed to obtain these results.

### Transport diagnosis, not a bandwidth-cap claim

An eight-second sample during the earlier application run recorded generator
WireGuard RX around 504 Mbps (physical interface RX about 524 Mbps), and node A
physical TX about 625 Mbps including inter-node traffic. This alone does not
prove a provider bandwidth cap.

After all k6 stopped, `test/p2/02-network.py` measured a single TCP flow over
the same private WireGuard path three times: **848.558 / 832.632 / 842.340 Mbps**.
The listener bound only A's private address, closed after each round and had a
25-second watchdog. No public port or persistent firewall rule was added.
Evidence: `.e2e/report-tests/p2/02-network/20261121T234913Z-7b8f3b90/`.

The path demonstrably carried more than 500 Mbps during this diagnostic. The
application path adds proxying, inter-node traffic and request/payload
processing; these were not isolated by the raw transfer. Generator CPU samples
also reached roughly 5.0–5.2 cores, with `GOMAXPROCS=6` on the 8-vCPU generator.
Profiling the generator and gateway is the next diagnostic step before deciding
which design component to change. Neither more Aggregator replicas nor a larger
generator is asserted to solve the remaining ratio without that evidence.

### Historical laptop attempts

The four-tunnel comparison (`20261121T231825Z-2827c307`) also missed the target:
2,037.476→2,801.275 complete RPS (1.375×), then
2,982.713→3,064.522 complete RPS (1.027×). One case again had negative minimum
duration. Its final polling reconciliation hit a controller timestamp-parser
error on a five-digit PostgreSQL fractional second; this is not evidence that
polling failed or passed. The parser now normalizes fractions to six digits,
without changing any application or acceptance threshold.

An external generator is now provisioned separately from Kubernetes, with 8
vCPUs, about 8 GB RAM, k6 1.4.2 and a direct WireGuard route to the same gateway.
Its private identity is in the ignored inventory. No operator SSH private key
or entire application `.env` was copied onto it. The external campaign is a
separate measurement path, not a reinterpretation of failed laptop campaigns.

Clock-skew, whole-VM freeze/resume and a genuine new-version rolling update
were not executed by this campaign.

## CPU profiling and bounded gateway candidate

Branch: `fix/p2-throughput`. The workload, complete-response validation, VU
count, case duration, acceptance ratio and application resource budgets remain
unchanged. Profiling is opt-in on pod loopback and was removed after capture.
No database, volume, source clock or Sensor Pantai state was reset.

Private profiles: `.e2e/report-tests/p2/03-profile/20261122T000955Z-26fa2e8a/`.
The automated collector initially used pod UIDs where names were required; the
run is marked failed, not passed. Pod profiles were subsequently collected
during the still-running workload using the correct names. Their capture
window differs from the generator's window, so percentages must not be treated
as synchronized end-to-end time decomposition. The reproducible collector now
uses pod names.

- k6: 95.42 CPU-seconds in 20.16 wall-seconds. `Response.JSON` accounted for
  33.89% cumulative CPU samples; heap scanning was also prominent. These are
  cumulative stacks, not independent additive percentages.
- Gateways: 18.94 and 18.41 CPU-seconds over about 15.13 seconds, with roughly
  54–56% flat syscall samples and response-copy/write paths prominent.
- Aggregators: roughly 0.50–0.65 CPU cores in those capture windows.
- All five pod runtime snapshots showed zero additional throttled microseconds.

This supports investigating request processing and generator overhead rather
than asserting that database throughput or Kafka partitions are the hot path.
It does not yet prove a single exclusive bottleneck.

A small-response coalescing candidate was unit-tested and deployed only to the
gateway as `m2-p2-coalesce-20261122-r1`. It fills the existing proxy copy buffer
for finite responses at most 32 KiB; unknown-size, larger and SSE bodies retain
streaming. It introduces no cache or payload changes. The first unchanged
comparison measured 4,102.322 → 5,206.584 complete RPS (**1.269×**, failed), with
zero unexpected failures. This does not establish a useful improvement over
the previous approximately 5,216-RPS three-instance result.
The reverse pair measured 5,051.488 versus 3,924.546 complete RPS (**1.287×**,
failed). Both pairs reconciled mock polling without duplication. The candidate
was therefore removed from the worktree and the original gateway image was
restored; no speculative application optimization remains deployed.
Evidence: `.e2e/report-tests/p2/01-capacity/20261122T001520Z-f22ecad2/`.

The external worker now records its Go parallelism and GC target. Optional
`--generator-procs` and `--generator-gogc` apply identically to both sides of
a comparison; their historical defaults remain 6 and 100. Changing these is a
separately labelled generator experiment, not an application scalability fix.

With the original gateway restored and generator parallelism 8 / GOGC 300,
the unchanged 1,000-VU pair measured **4,509.023 → 6,344.410 complete RPS
(1.407×, failed)**. Peak generator RSS was below 996 MiB and average active
process CPU was approximately 5.35–5.68 cores, rather than the earlier roughly
5 cores. There were no unexpected failures, malformed/partial complete results
or duplicated mock polls. All three Aggregators were restored. This is evidence
that generator runtime overhead affects the measurement, not evidence that
the application has met P2.7.
Evidence: `.e2e/report-tests/p2/01-capacity/20261122T002107Z-0624d446/`.

An internal gzip negotiation candidate, tested without changing any client
headers or checks, measured **2,213.279 → 3,695.736 complete RPS (1.670×)** with
the same generator runtime. Both absolute throughput and the ratio failed to
justify this change. It was removed from the worktree and rolled back on the
VPS. The client-side HTTP contract passed unit tests, but correctness alone did
not make this a successful performance optimization.
Evidence: `.e2e/report-tests/p2/01-capacity/20261122T002555Z-6e52dd8e/`.

Increasing gateway replicas from two to three, spread across all three nodes,
while dividing the **same total per-backend concurrency budget** across the
three gateways, measured **4,259.594 → 6,142.379 complete RPS (1.442×)**.
Unexpected failures remained zero. This did not outperform the original two
gateways at 6,344 RPS with the same generator runtime, so replica count and
gateway budget configuration were restored. No manifest change was retained.
Evidence: `.e2e/report-tests/p2/01-capacity/20261122T002939Z-47121cf8/`.

## Generator/path calibration, not application acceptance

`test/p2/04-generator.py` captured an exact real complete page (20 events,
approximately 10.1 KiB) and served it from a temporary authenticated Go HTTP
fixture on A's private WireGuard IP. The fixture does no database work, source
polling or gateway routing. k6 still executes the original request loop, full
JSON parsing, hazard-ID checks, counters and failure rules: 1,000 VUs for 60
seconds, runtime 8 / 300. No public port or application service is replaced.

Two independent snapshots measured **6,862.376 and 6,800.108 complete RPS**,
with no unexpected failures and peak k6 RSS approximately 1,038 / 1,007 MiB.
The temporary listeners were stopped after each run. An earlier attempt built
a Windows executable through WSL and failed before serving traffic; it remains
failed evidence. The controller now propagates the cross-build environment and
asserts an ELF header before upload.

Private evidence:

- `.e2e/report-tests/p2/04-generator/20261122T003327Z-796ad8fd/`
- `.e2e/report-tests/p2/04-generator/20261122T003525Z-089cb22a/`

The unchanged three-Aggregator result (6,344 RPS) is already about 92–93% of
this measured fixture/path rate. Twice the corresponding one-instance baseline
would require about 9,018 complete RPS. **This is a measurement bottleneck
warning, not a proof that the application can reach 9,018 RPS or that the
fixture is a universal hardware ceiling.** The fixture still includes its own
HTTP server and the private network. These measurements do not fully isolate
CPU, kernel/network processing and fixture-server capacity.

The next useful capacity experiment is additional independent load-generator
capacity with the same total VUs, response validation and entrypoint, followed
by the same counterbalanced one-versus-three comparison. More Kafka partitions
or database shards are not justified by these read-path profiles. Do not slow
the one-instance case, discard response bodies, count 429s as successful work,
or replace the application with the fixture to manufacture a P2.7 pass.

## Synchronized follow-up profile

The repaired collector captured all five pod CPU profiles plus k6 during the
same middle 20-second workload window, with generator runtime 8 / 300.
Evidence: `.e2e/report-tests/p2/03-profile/20261122T003706Z-868d53fc/`.
The instrumented run reached 6,390 complete RPS with zero unexpected failures;
it is diagnostic, not a new uninstrumented acceptance comparison.

- k6 used 108 CPU-seconds over 20.17 seconds. `Response.JSON` accounted for
  41.42% cumulative samples. Its [version-pinned implementation](https://github.com/grafana/k6/blob/v1.4.2/js/modules/k6/http/response.go)
  parses the complete body into Go JSON values before conversion to JavaScript.
- The three Aggregators sampled approximately 0.59 / 0.69 / 0.79 CPU cores.
- The two gateways sampled approximately 1.32 / 1.38 CPU cores, with syscall
  samples accounting for about 51–56% of their CPU profiles.
- Runtime snapshots showed **zero additional throttled periods or
  microseconds** across all five pods. This is bounded-window evidence, not a
  claim that every node or network subsystem has unlimited capacity.

Application CPU limits, response budgets, cache lifetime and body validation
were not reduced to manufacture a better ratio. The retained changes are
reproducible diagnostics, explicitly labelled generator runtime controls and
evidence updates, not an unproven application redesign.

Final verification observed **3/3 Ready Aggregators and 2/2 Ready gateways**,
both on their original images, with profiling flags removed. No k6 or fixture
process remained. The snapshot is `final-verification.json` in the last profile
directory. `go test ./...` in `services/aggregator`, Python compilation of the
changed test scripts and `git diff --check` passed. The changed-text credential
scan found only the public JWT key identifier, not credential values. All
database volumes and the original Sensor Pantai state were retained.
