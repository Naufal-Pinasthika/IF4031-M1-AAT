# Existing VPS deployment

## External load generator

The optional `load_generator` object in the private inventory describes a
fourth VPS, not a Kubernetes node. It uses the same operator SSH key and its
own WireGuard identity, generated on that host and never downloaded.

```sh
python3 infra/vps/provision-load.py
python3 infra/vps/provision-load.py --apply
```

Provisioning pins k6 1.4.2 for comparison with the laptop, using Grafana's signed
APT repository. It enables a dedicated WireGuard link to A at `10.77.0.10`,
allows only the relevant peer's public UDP traffic, preserves existing firewall
rules and does not restart A's VPN. A protected pre-change configuration copy
remains on A. The generator routes only A's private address through this peer;
application/database public ports remain closed. No Kubernetes, Docker or
database is installed on the generator.

Run report-facing tests from [`test/`](../../test/README.md) with
`--generator-vps`. SSH carries control and evidence; k6 sends HTTP directly over
WireGuard to the common gateway NodePort, never to individual Aggregator pods.
Generator jobs run as the SSH user, receive only the needed API key over SSH
stdin, enforce a bounded runtime/memory guard and keep private per-run files.
Normal load jobs must not copy the operator private SSH key, cluster secrets or
the entire `.env` onto the generator. Separately approved destructive-test
backups use a private `~/.local/state/aat-backups/<run>` directory there; those
archives contain sensitive cluster metadata and must remain outside Git and
outside load-job directories. Provisioning alone is not a P2 acceptance pass.

For script selection and side effects, see the [script catalog](SCRIPTS.md).

The Aggregator front door is a per-node gateway DaemonSet. Its NodePort and
internal `aggregator` Service use `trafficDistribution: PreferSameNode` with
Cluster traffic policies; Client API reaches this Service before L7 backend
selection. This requires Kubernetes supporting PreferSameNode (verified on
the VPS k3s v1.37 cluster). Every participating application node should have a Ready
gateway. `GATEWAY_REPLICAS=3` divides the backend budget for this three-node lab;
adjust that startup value when changing the gateway node count. Locality is a
preference, allowing remote endpoints when the local gateway is absent, not a
claim of control-plane or whole-node HA.

Host-originated NodePort traffic is a Kubernetes exception to strict Local
external policies. The selected topology hints also prefer the local endpoint
on that path, unlike externalTrafficPolicy alone. Use `--ingress-vps` on
report tests to observe the same NodePorts from the separate inventory generator.
Keep both paths' evidence separately; an external-ingress pass alone does not
prove that the SSH/host-originated path survived.
For the restored source versus live-runtime boundary, see the
[status page](../../docs/status/README.md). Documentation cleanup does not
redeploy the VPSs; inspect schema compatibility before rolling back binaries.

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

Provisioning enables `ufw.service` at boot as well as loading the rules now.
Check both `ufw status` and `systemctl is-enabled ufw`: active kernel rules do
not by themselves prove that host filtering will survive the next reboot.

Ubuntu's absolute `/usr/sbin/iptables` alternatives symlink escapes the Agent's
`/host` bind mount. A relative link alone is insufficient: the dynamically linked
distro binary also needs an interpreter absent from the Agent container.
After k3s installation, `tester-tools` installs k3s's bundled static xtables
binary under `/usr/local/libexec/` and uses a relative iptables link. It verifies
the same kernel firewall backend and unchanged filter rules; the userspace
version can change. The original executable selection is saved privately in
`/etc/aat-vps/iptables-exec-before-tester.json`. No rules are flushed and the
Agent is unchanged. Preflight executes `/host/usr/sbin/iptables --version`
inside every original Agent. Re-run this step after package/alternatives changes;
an Agent step marked successful is still not proof of a real network fault.

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

Read [the verification status](../../docs/status/README.md) before interpreting
historical evidence. The infrastructure test runners have been deleted, not
moved. [`test/`](../../test/README.md) is reserved for their future replacements.
The retained `access.py smoke` action invokes the existing demonstration under
`scripts/m2/e2e/`; that demonstration remains outside this infrastructure cleanup.

```bash
python3 infra/vps/access.py status
python3 infra/vps/access.py smoke
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

## Scoped application updates

For official Tester Agent admin calls, build a PVMBG candidate with
`update-service.py --service pvmbg --tag TAG --build-only`, then run
`python3 infra/vps/enable-tester-admin.py --image-tag TAG --apply`.
Without `--apply` the second command only prints its plan. It installs the
admin-port NetworkPolicy before enabling the `:8089` control listener and
advertises `pvmbg-admin` to the Agent. Normal `pvmbg:8082` still requires its
Bearer token. The official Agent manifest/image are not changed.

`update-service.py --source-dir PATH` can build an explicitly staged service
tree. The polling hotfix used main `c6bc838`, the already deployed source index,
and only the timestamp-precision/read-failover fixes, excluding paused P5/HA
worktree changes. Candidate tags identify build artifacts, not Git commits.

These deployment entry points remain in infrastructure tooling:

- `update-service.py --service notifier-consumer --tag UNIQUE`: builds/imports only notifier and performs
  a scoped StatefulSet update. Existing notification/dedup rows, Kafka offsets,
  credentials and PVCs are retained. The first upgraded start backfills exact
  incremental statistics in restartable batches before consumption/HTTP starts;
  later starts read only the persisted summary. It is a maintenance upgrade,
  not evidence of zero-downtime notification delivery. `--build-only` stages the
  candidate without changing the workload.
  `--service auth` selects only Auth instead, without resetting clients or sessions.
- `prepare-source-index.py`: plan by default; `--apply` concurrently creates the
  source-filtered pagination index on each canonical unit. Run it before the
  corresponding Aggregator rollout on populated databases. It preserves rows,
  memory limits and query timeouts. Fresh schemas create the index normally;
  existing schema fast-path startup deliberately does not build it under the
  short readiness/query budget.

- `deploy-p2-capacity.py`: scoped read-path/gateway rollout with retained resume
  state and selector/image rollback safeguards.
- `update-aggregator.py`: Aggregator-only update; an explicitly requested schema
  cutover may stop old workers and is not a zero-downtime rolling-update proof.
- `update-p2-observability.py`: observability integration update.
- `prepare-analytics.py`: plan by default; `--apply` prepares two additive,
  covering partial indexes on each canonical unit before the new analytics
  reader is rolled out. Builds run concurrently with application traffic, one
  unit at a time, with bounded lock/statement waits. A previously canceled,
  invalid build of either named index is dropped concurrently and rebuilt;
  canonical rows and PVCs are never deleted. Review any failure before retrying.
  `--rebuild` explicitly rebuilds only these two indexes when their definition
  changes. The boolean JSON retraction predicate must match the reader; a string
  `"true"` is not a canonical boolean tombstone.

For existing databases, run `python3 infra/vps/prepare-analytics.py --apply`
before the scoped Aggregator update. Fresh database schemas create the same
indexes automatically. Preserve the ignored `.e2e/analytics-indexes-*` evidence.

Review compatibility and obtain fresh regression evidence before a rollout.
Old test and diagnostic runners in `infra/vps/` have been removed; replacement
standalone tooling belongs in [`test/`](../../test/README.md). Do not execute
historical test commands or treat historical results as validation of a new build.

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
## Private operations dashboard

HA has been deferred at the operator's request. Coordinator, Kafka and notifier
use the original singleton topology; notifier uses bbolt. See
[measurement and observability](../../docs/changelog/P5-observability.md).

Run `python3 infra/vps/access.py observe` (from WSL, repository root), then open
`http://localhost:8090`. The dashboard is forwarded over SSH; no public HTTP port
was opened. Keep that terminal running and stop it with Ctrl+C when finished.

## Private analytical dashboard

This is the P1 business-data dashboard, separate from the operations dashboard.
From WSL at the repository root, keep this command running:

```sh
python3 infra/vps/access.py tunnel
```

Open `http://localhost:30080/dashboard` manually. Enter a current Client API
access token with `hazards:read:summary` (for example the Ops client's token
obtained from `http://localhost:30084/v1/token` using its configured credentials).
Credentials remain in the ignored `.env`; they are not embedded in the page.
Inspect the five hazard series, UTC range controls, severity/area tables and
complete/partial warning. Backend JSON/HTML tests do not replace this visual
inspection. No Grafana analytics dashboard has been introduced by this change.
