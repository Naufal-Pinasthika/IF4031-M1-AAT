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
