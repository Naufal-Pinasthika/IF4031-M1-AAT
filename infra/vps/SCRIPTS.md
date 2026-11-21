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
| [update-p2-observability.py](update-p2-observability.py) | Observability integration update | Builds/updates selected monitoring resources; preserves Prometheus history. |

These entry points have different compatibility and rollback requirements; do
not substitute one for another without reviewing their implementation. Source
rollback does not reverse database schemas or Kafka partition changes.

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
