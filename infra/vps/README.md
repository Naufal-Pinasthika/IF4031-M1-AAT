# Existing VPS deployment

This provisions **existing** Ubuntu Server 24.04 x86-64 VPSs. It does not order
instances, require a cloud API key, or create billable cloud resources. Python's
standard library plus SSH/rsync is used instead of Terraform remote-exec: the
servers already exist, and no Terraform state containing join tokens is needed.

## Topology

| Node | Type | vCPU | RAM | Role |
| --- | --- | ---: | ---: | --- |
| aat-vps-a | Biznet KVM VM | 16 | 16 GB | k3s server + worker, platform services, Aggregator, one canonical unit |
| aat-vps-b | Biznet KVM VM | 8 | 8 GB | k3s agent, Aggregator, one canonical unit |
| aat-vps-c | Biznet KVM VM | 8 | 8 GB | k3s agent, Aggregator, one canonical unit |

Each VPS has its own guest kernel. Physical-host placement and physical-host
CPU/RAM are **unknown**; do not claim the separate-physical-host bonus without
provider evidence. All three initially report `6.8.0-111-generic`; matching
kernel versions do not mean a shared running kernel.

WireGuard `aatwg0` provides an encrypted node mesh (`10.77.0.1` through `.3`).
K3s uses those node addresses and VXLAN over that interface. Its NetworkPolicy
controller remains enabled. UFW permits SSH, peer-only UDP 51820, and internal
pod/node traffic. No database or Kubernetes API port is opened publicly.
NodePorts are restricted to `10.77.0.0/24` by kube-proxy on **every** node.
The official Tester Agent manifest/image/RBAC is applied unchanged.

This is **not full HA**: control plane, Kafka and coordination database remain
singletons on A. The intended worker-failure tests do not establish A-failure
tolerance. A single provider also does not guarantee independent physical hosts.

## Prerequisites

Run from Linux or WSL with Python 3, OpenSSH client and rsync. No local Docker,
Kubernetes cluster, or Terraform installation is required.

1. Copy `inventory.example.json` to `inventory.json` and fill existing IPs,
   usernames, and the **local Linux path** to the SSH private key. This file is
   gitignored. A Windows `\\wsl.localhost\...` path must become `/home/...` in WSL.
2. Verify/accept each SSH host key once using the provider console fingerprint.
   Automation uses `StrictHostKeyChecking=yes` and passwordless `sudo -n`.
3. Create `.env` with `bash scripts/setup-env.sh`, or keep the existing valid
   file. Do not commit it. Deployment transfers it over SSH to root-only storage.
4. These nodes must be dedicated to this cluster. Provisioning changes their
   firewall and installs packages; it does not uninstall an existing cluster.

## One-command deployment

From the repository root:

```bash
python3 infra/vps/deploy.py --provision --tag m2-vps-20261119-01
```

For later application-only updates, omit `--provision` and choose a new tag:

```bash
python3 infra/vps/deploy.py --tag m2-vps-NEXT
```

Images are built on A, then streamed over SSH through the operator machine into
the agents' containerd image stores. The laptop does not build or run them.
No image or source is published to a public registry. The seven Go services use
the same tag. PostgreSQL, Kafka, Sensor Pantai and Tester Agent keep their
manifest images; the official sensor/tester digests are not replaced.

Provisioning resolves the stable k3s version once per run for all nodes. To pin
it explicitly, run `provision.py provision --version vX.Y.Z+k3sN` followed by
`deploy.py` without `--provision`. Initial deployment used `v1.37.2+k3s1`.
Re-running the application deployment retains PVCs and existing credentials.
Old images/build cache on A are not automatically pruned, to keep rollback
possible; monitor its 60 GB root disk.

If the build succeeded but deployment was interrupted, reuse the exact tag:

```bash
python3 infra/vps/deploy.py --skip-build --tag m2-vps-20261119-01
```

## Access and verification

Repeated P5 tests and current deployment evidence are summarized in
[the verification status](../../docs/README.md). The Linux load
generator can run on B's host outside application pods with explicit CPU/memory
bounds; that shares B's resources and is not an independent capacity benchmark.
Do not count empty critical-alert windows as a latency pass.

```bash
python3 infra/vps/access.py status
python3 infra/vps/access.py smoke
python3 infra/vps/verify.py
python3 infra/vps/access.py tunnel
```

Leave `tunnel` running in a separate terminal. Its local listeners bind only to
`127.0.0.1`; endpoints are:

| Local port | Service |
| ---: | --- |
| 30080 | Client API |
| 30083 | Aggregator common Service |
| 30084 | Auth |
| 30086 | Notifier |
| 30090 | Official Sensor Pantai |
| 16443 | Kubernetes API (still requires an authorized kubeconfig) |

```bash
curl -sS -w '\nHTTP %{http_code}; total %{time_total}s\n' http://127.0.0.1:30083/ready
python3 infra/vps/access.py kube -- get nodes -o wide
```

Authenticated requests use the credentials in `.env`; never paste them into
logs. Run k6 outside the application cluster. An SSH tunnel adds its own CPU and
network bottleneck: smoke checks via a tunnel are not 10,000-VU capacity proof.
Measure that path, or configure a dedicated operator WireGuard peer before the
full load test. Do not expose internal NodePorts publicly to work around this.

### Guarded 10,000-VU test from the operator laptop

The regular OpenSSH tunnel hit its channel-allocation limit in a real 10k
attempt. Use a temporary WireGuard client instead. These commands run from
Windows PowerShell, from the repository root, with Ubuntu-22.04 WSL available:

```powershell
$repo = '/mnt/c/Users/Ghana/Documents/aat/IF4031-M1-AAT'
wsl -d Ubuntu-22.04 -u root --exec python3 "$repo/infra/vps/load-network.py" up
if ($LASTEXITCODE -ne 0) { throw 'Private test network setup failed' }
try {
    wsl -d Ubuntu-22.04 --exec python3 "$repo/infra/vps/load.py" --base-url http://10.77.0.1:30083
    if ($LASTEXITCODE -ne 0) { throw 'Load test failed; inspect retained evidence' }
} finally {
    wsl -d Ubuntu-22.04 -u root --exec python3 "$repo/infra/vps/load-network.py" down
}
```

Adjust the operator repository path and `load.py --k6` path for another laptop.
The network helper requires existing WireGuard tools/kernel support and uses
the SSH key's sibling `known_hosts`, whose host fingerprints must already be
verified. It adds only an operator peer and a source-IP-scoped UDP rule on A,
routes only `10.77.0.1/32`, and does not restart the node mesh or publish HTTP.
`down` removes its peer, local interface/key/route and newly owned firewall rule.
If the terminal is interrupted, run `down` explicitly; runtime state is in
WSL `/run/aat-k6-network.json`.

The runner preserves the HTTP request, no-sleep pacing, 5-second request timeout
and existing thresholds. It adds separate complete/partial/429 counters,
observes actual VUs via the local k6 REST API, holds the target for 130 seconds,
requests replicas 3→5→2 and restores 3 in `finally`. Passing requires an observed
full-VU interval of at least 120 seconds and both scaling targets Ready.
Independent memory guards stop k6 if Windows physical headroom falls below
768 MiB, commit headroom below 512 MiB, Linux available memory below 512 MiB,
or k6 RSS exceeds 4,608 MiB. A safety stop is a failed/incomplete test, not a pass.

Evidence is retained under `.e2e/vps-linux-scale-10000-*`; reruns archive earlier
artifacts in `.e2e/runs/`. The Windows attempt is retained separately in `.vps/`.
See [the verification status](../../docs/README.md); the earlier laptop rerun
completed its duration/scaling and passed scripted thresholds, but still has a
timing anomaly and substantial HTTP 429 rejection. It is not a full P2/P5 pass.

Smoke verification checks authentication boundaries, complete reads, unique
canonical identities, delivery of admitted critical coastal events and three
Ready Aggregators on distinct nodes. Its ingress latency is **not** the full
source-to-notifier SLO. It does not establish all P1-P5 criteria or the 10k test.

Local infrastructure tests (PyYAML is needed only for these tests):

```bash
python3 infra/vps/test_provision.py
```

Bounded service regressions use a **separate disposable PostgreSQL fixture**,
not the live cluster. To repeat the network-outage, health-loop and maintenance
bulkhead cases ten times after the full race/vet suite:

```bash
python3 infra/vps/test-design.py --focused-repeat 10
```

The test-owned TCP proxy interrupts only fixture connections. Test evidence,
including failures, stays in ignored `.vps/`. This does not prove whole-VM freeze,
disk-loss recovery or P5 end-to-end latency. Do not run build/regression containers
concurrently with capacity measurements on the same VPS.

### P2 throughput and control-path retests

From WSL/Linux at the repository root, run sequentially:

```bash
python3 infra/vps/test-p2-load.py throughput
python3 infra/vps/test-p2-control.py
```

Throughput runs the unchanged 1,000-VU/60-second workload in the order
1 -> 3 -> 3 -> 1, counts only complete responses and requires both 3/1 ratios
to reach 2.0. It restores three replicas. The current build-17/2-CPU retest
**failed** at 0.788606/0.838571; see [current status](../../docs/README.md).

Control tests deliberately delete owner pods, change a worker's wall clock
+45/-30 seconds (with automatic time/NTP restoration), and pause an Aggregator
task for 28 seconds (with automatic resume). Do not run concurrently with other
fault/load tests. Both runners take the same exclusive local lock. If all leases
are on A, the controller records an additional normal owner-pod loss so a worker
can acquire ownership; it never assigns lease rows manually.

The pause check retains pre/post container identity and restart count. If
kubelet restarts the task, successful availability checks are not sufficient:
the stale-process-resume case fails as unexercised. This happened in the current
retest. This tool does **not** suspend the whole VM and does not close P2.5.
Raw evidence, including failures, stays under ignored `.e2e/`.

### Laptop throughput and clock integrity

Laptop throughput tooling is available as `test-p2-laptop.py` (WSL/WireGuard) and
`test-p2-native.ps1` (native Windows k6/SSH), both using the unchanged four-case
workload and restoring three replicas. The original WSL path reported negative
HTTP timing, while native SSH exhausted descriptors at 1,000 VUs. The reversible
NTP experiment below subsequently produced clean timing over WireGuard, but
the throughput ratios still failed. Do not treat an ordinary unmitigated WSL
run or the native SSH fallback as a validated capacity path. Native mode uses a new user-only temporary key copy
and deletes it in `finally`; the original credential and clock policy are not
changed. See [current status](../../docs/README.md).

To review retained WSL timing without running load or modifying the cluster:

```bash
python3 infra/vps/test-p2-laptop.py --review-existing .e2e/p2-laptop-throughput-20261119T130631Z
python3 infra/vps/probe-clock.py --seconds 45
```

The first command exits nonzero on invalid/missing timing evidence and writes
a separate integrity report without overwriting raw summaries. The second is a
bounded read-only clock observation, not a time/NTP repair.

For a reversible competing-time-authority experiment (WSL root only):

```bash
sudo python3 infra/vps/test-p2-clock.py --benchmark
```

This temporarily stops an originally active `systemd-timesyncd`, installs a
15-minute automatic restart timer, and requires a clean 45-second clock probe
before opening the temporary WireGuard path. The benchmark runs as the SSH key's
non-root owner, using existing strict host verification. It observes the clock
throughout the benchmark, then removes its owned network resources and restores
the original NTP active/enabled state. It never sets a clock or disables NTP
persistently. Inspect both the clock evidence and all four benchmark cases;
absence of a negative HTTP minimum alone does not establish clock stability.
Omit `--benchmark` for the bounded clock-only experiment.

### Scoped P2 read-path rollout

The candidate read-path upgrade and its trade-offs are described in
[P2 read-path implementation](../../docs/P2-read-path-implementation.md).
From WSL at the repository root:

```bash
python3 infra/vps/test-design.py --focused-repeat 3
python3 infra/vps/deploy-p2-capacity.py
python3 infra/vps/check-p2-capacity.py
python3 infra/vps/update-p2-observability.py
sudo python3 infra/vps/test-p2-clock.py --benchmark
```

The scoped deploy changes only Aggregator/gateway and their common-entry
selectors/configuration, retaining PVCs and source/tester images. An interrupted
unfinished record can be resumed with `--resume p2-capacity-deploy-<UTC stamp>`;
the original image/selector rollback targets remain intact. The observability
upgrade adds gateway trace/metrics coverage and reloads Prometheus without
restarting its pod, preserving its current in-pod history.
After load tests, `python3 infra/vps/check-p2-capacity.py --trace` also verifies
the gateway-to-Aggregator bounded correlation-log trace through authenticated
Kubernetes exec/loopback. The private dashboard's NetworkPolicy stays unchanged.

The default benchmark is unchanged. A **separate** protocol experiment is
`sudo python3 infra/vps/test-p2-clock.py --benchmark --workload gzip`. Stock k6
does not negotiate gzip by default in the probed binary; report both results
separately. Do not use compressed results to relabel an uncompressed failure.
Mutating deployment/regression/load tools use the same exclusive local lock;
do not run heavy local compilation during laptop capacity tests either.
Read-only samples can be collected alongside a campaign with
`python3 infra/vps/observe-p2-capacity.py .e2e/<run-directory> --seconds 360`.
For an explicitly instrumented diagnosis, add `--cpu-accounting` to sample
pod CPU-quota counters through bounded Kubernetes exec. This has extra observer
overhead and writes a separate `capacity-cpu-accounting.json`; compare deltas
only for the same pod/container lifetime. Do not call it an unobserved benchmark.

### Private Aggregator CPU profiling

Diagnostic builds support `AGGREGATOR_PROFILE_ENABLED=1`. This starts a separate
**127.0.0.1:6060** listener inside each pod, not a business route or Service.
Only `/runtime` (Go concurrency and cgroup counters) and `/cpu?seconds=1..30`
are exposed; heap, environment, command-line and goroutine dumps are not.
Keep the flag absent for normal operation. Enabling/removing it rolls the pods.

With the flag explicitly enabled on a compatible image, run:

```bash
python3 infra/vps/test-p2-load.py scale --cpu-profile
```

The unchanged scale workload starts one bounded 30-second CPU capture on each
serving pod at elapsed 35 seconds, through authenticated Kubernetes exec over
SSH. Timestamped runtime samples accompany the profiles in ignored `.e2e/`.
This adds diagnostic overhead; do not present it as an uninstrumented benchmark.
The runner restores the image/three replicas, but does **not** change the profiling
flag: remove that explicit Deployment env entry and await rollout after collecting.
No public firewall or Kubernetes port is opened. Keep raw profiles private.

## Common-entry gateway regression

The gateway image now supports the same opt-in private diagnostic interface
under `GATEWAY_PROFILE_ENABLED=1`. Leave it absent in normal operation and remove
it after profiling. Profiling is additional overhead, not a clean capacity run.
`p2_profiles.py` recognizes Aggregator and gateway container names.

To verify actual gateway image replacement (not only Aggregator replacement),
using the unchanged strict 50-VU/120s rolling workload:

```bash
python3 infra/vps/test-p2-load.py rolling --rolling-target aggregator-gateway \
  --previous-image aat/aggregator:<schema-compatible-prior-tag>
```

The runner records both Ready gateway binaries and image IDs, preserves failure
results, and restores the starting gateway image. Aggregators remain at three
replicas. A passing gateway rollout does not prove 10k rolling or whole-node
failure tolerance. Gateway queues, headers and body-lifetime metrics distinguish
local waiting from upstream work; see the [implementation guide](../../docs/P2-read-path-implementation.md).

The normal gateway memory limit remains 256Mi; `GOMEMLIMIT=160MiB` is a soft
runtime target inside it, not an increase in resources. Scale verification now
records gateway pod identities/restarts/termination reasons and fails on new
gateway restarts or replacements. Its additional semantic-error gate excludes
429s from the denominator and counts partial/malformed responses as failures;
all-rejection runs cannot pass it. Existing k6 workload and thresholds remain.

## Local migration policy

The operator selected a **fresh database** on VPS and subsequently explicitly
requested deletion of old local test data **without backup**. The dedicated
local lab distro and Docker resources are removed. Deletion of temporary archive
files on drive D was denied by the local tool policy; those inert files require
manual removal and are not a verified backup.
Source, `.env`, VPS SSH keys and test evidence are retained. Restoration of old
test data is not guaranteed. Never use a remote kubeconfig for local
cleanup. Future cleanup of VPS data requires a separate explicit request.

Actual migration results and remaining manual archive cleanup are retained in
the local-only operator journal `migration-2026-11-19.md` (ignored by Git).
# Private operations dashboard

HA has been deferred at the operator's request. Coordinator, Kafka and notifier
use the original singleton topology; notifier uses bbolt. See
[measurement and observability](../../docs/P5-observability.md).

Run `python3 infra/vps/access.py observe` (from WSL, repository root), then open
`http://localhost:8090`. The dashboard is forwarded over SSH; no public HTTP port
was opened. Keep that terminal running and stop it with Ctrl+C when finished.
