# Infrastructure script catalog

Run commands from the repository root in Linux/WSL. This directory contains
provisioning, deployment, build, and operational access tooling, not test suites.

The former load, fault-injection, diagnosis, profiling, evidence-collection,
and tooling-test runners were deleted. They were **not** moved. Future standalone
test tooling belongs in [`test/`](../../test/README.md). Numbered P3/P4
report-facing E2E scenarios now run from the laptop against the VPS lab; they
do not replace every deleted runner. Unit and PostgreSQL integration tests
remain beside their services, and `scripts/` demonstrations remain available.

## Provisioning and deployment

| File | Purpose | Effects |
| --- | --- | --- |
| [provision.py](provision.py) | Configure existing VPSs | Changes host packages, firewall, WireGuard, and Kubernetes. Does not purchase servers. |
| [host.sh](host.sh) | Privileged provisioning helper | Changes host configuration for the selected action. |
| [deploy.py](deploy.py) | Full build/import/application deployment | Syncs sources and configuration, builds/imports images, and changes Kubernetes resources. `--provision` also configures hosts. |
| [build.sh](build.sh) | Build/export/import images on A | Consumes remote CPU/disk and writes image archives. |
| [deploy-p2-capacity.py](deploy-p2-capacity.py) | Scoped read-path/gateway deployment | Changes selected resources with resume/rollback state; retains PVCs. |
| [update-aggregator.py](update-aggregator.py) | Aggregator-only update/schema cutover | Can stop old workers for an incompatible schema generation; not automatically zero-downtime. |
| [update-service.py](update-service.py) | Scoped notifier, Auth, Aggregator or PVMBG build/import/rollout | Retains PVCs, credentials and offsets; `--source-dir` selects a staged service source without shipping unrelated worktree changes. |
| [enable-tester-admin.py](enable-tester-admin.py) | PVMBG control-listener deployment | Plan by default; applies the restrictive NetworkPolicy before enabling the listener, advertises its admin Service, and rolls only PVMBG. Does not change Tester Agent. |
| [prepare-source-index.py](prepare-source-index.py) | Add source-filtered pagination index | Plan by default; `--apply` builds concurrently, preserving canonical rows and query budgets. |
| [deploy-critical-lane.py](deploy-critical-lane.py) | Enable the additional critical event path | Plan by default; rolls only staged Aggregator/notifier images, retains the general topic/offsets/PVCs and does not reset Sensor Pantai. Does not enable notifier or broker HA. |
| [update-p2-observability.py](update-p2-observability.py) | Observability integration update | Builds/updates selected monitoring resources; preserves Prometheus history. |

These entry points have different compatibility and rollback requirements; do
not substitute one for another without reviewing their implementation. Source
rollback does not reverse database schemas or Kafka partition changes.
Scoped service archives include the service name as well as the tag. Binary
image streams use independent SSH connections and a bounded import wait so
bulk transfers do not block operational commands on a shared SSH connection.

## Access and shared configuration

| File | Responsibility |
| --- | --- |
| [access.py](access.py) | Status, private tunnels/dashboard, and kubectl dispatch. `kube` can mutate resources. `smoke` delegates to the retained `scripts/m2/e2e/` demonstration. |
| [vps_config.py](vps_config.py) | Shared non-secret namespace/NodePort defaults; imports no inventory or network connections. |
| [inventory.example.json](inventory.example.json) | Template for private machine addresses and SSH key paths. |

Application configuration remains in `.env`, ConfigMaps, Secrets, and
`k8s/base`. Shared operator defaults do not replace application manifests.
Use the ignored `inventory.json` for real machine details; never commit it.

## Evidence and historical commands

Historical reports retain results and failures, but their removed runner commands
are no longer executable. Keep raw evidence locally under ignored `.e2e/` and
`.vps/`. This cleanup does not delete that evidence, change VPS state, or supply
a new acceptance result. See the [status page](../../docs/status/README.md) and
[VPS runbook](README.md).
