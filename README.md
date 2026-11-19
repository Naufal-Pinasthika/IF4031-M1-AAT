# Hazard Coordination Platform

Distributed hazard ingestion and coordination platform for IF4031.

BMKG and PVMBG mocks, the official Sensor Pantai, Aggregator, Auth, Client API,
and independent Kafka consumers form the M2 platform. Canonical storage,
polling ownership, read-side projections, and consumer state have separate
responsibilities; see the [design guides](docs/README.md#design-and-contracts).

## Start here

| Goal | Read |
| --- | --- |
| Run locally with Docker Compose | [Local setup](docs/guides/local-compose.md) |
| Use the existing three-node VPS deployment | [VPS provisioning/access](infra/vps/README.md) |
| Choose a deployment or testing script | [Script catalog](infra/vps/SCRIPTS.md) |
| Understand APIs and configuration | [Reference](docs/reference/api-configuration.md) |
| Check results and limitations | [Verification status](docs/status/README.md) |
| Browse all guides | [Documentation index](docs/README.md) |

## Source and deployment status

This cleanup starts from main commit `47d7494`. Uncommitted P1–P3 work was
saved in a local stash. **The Git rollback did not roll back the VPS deployment.**
Before deploying this source, inspect live images/configuration and database
schema compatibility; do not assume source, historical reports, and runtime match.

P2/P5 are not declared fully passed. The retained throughput comparison failed
the 2x target, and fresh critical source-to-notifier latency evidence remains
open. See the [status page](docs/status/README.md) and its linked historical report.

## Quick Start

Run from the repository root in Bash/WSL, with Docker Compose available:

```bash
bash scripts/setup-env.sh
docker compose config --quiet
docker compose up -d --build
docker compose ps
```

`setup-env.sh` refuses to overwrite an existing `.env`. Keep existing credentials
when reusing volumes. See the [complete local guide](docs/guides/local-compose.md)
for prerequisites, health checks, tokens, and explicit destructive-reset warnings.
Local startup is not a multi-node acceptance test.

## Repository Layout

```text
services/       Go application services, source mocks, and observability
k8s/base/       Kubernetes workloads, platform, jobs, and network policy
scripts/        Environment setup, manifest generation, demos, and local lab
infra/vps/      Existing-server provisioning, deployment, and access
test/           Reserved for future standalone test tooling
docs/           Usage guides, references, runbooks, and status
docs/changelog/ Design notes, historical changes, and test reports
docker-compose.yml
.env.example    Application configuration template; never put real secrets here
go.work         Go workspace
```

## Service and API Reference

See [service ports and HTTP endpoints](docs/reference/api-configuration.md#service-and-api-reference).
Compose ports and VPS NodePorts are different; use the appropriate runbook.

## Configuration

Use [`.env.example`](.env.example) and the [configuration reference](docs/reference/api-configuration.md#configuration).
Private VPS inventory, credentials, keys, and runtime evidence remain ignored.
Shared operator namespace/NodePorts live in [`vps_config.py`](infra/vps/vps_config.py);
Kubernetes manifests remain the source of application topology.

## Authentication and Authorization

See the [auth reference](docs/reference/api-configuration.md#authentication-and-authorization)
for roles, JWT/JWKS validation, and rotating refresh tokens.

## Demonstration Runbook

The [M1 cases](docs/runbooks/m1-demo.md) keep the original numbered examples.
M1 and M2 problem numbers are not interchangeable.

## Milestone 2 — Problem 2 dan Problem 5

Use the [M2 guide](docs/guides/m2.md), [design index](docs/README.md#design-and-contracts),
and [script catalog](infra/vps/SCRIPTS.md). Read the effects and recovery
requirements before running scale, rolling-update, or fault-injection tests.

## Observability

See [P5 measurement/dashboard](docs/changelog/P5-observability.md) and
[local logs](docs/guides/local-compose.md#observability). Coordinator, Kafka,
and notifier HA remains deferred in the documented baseline.

## Development Checks

See [local checks](docs/guides/local-compose.md#development-checks).
Old infrastructure test/diagnostic runners have been deleted, not moved.
Future standalone test tooling belongs in [`test/`](test/README.md).
Service-local Go tests and existing `scripts/` demonstrations remain unchanged.

## Shutdown and Reset

See [local stop/reset](docs/guides/local-compose.md#shutdown-and-reset).
`docker compose down` preserves volumes; adding `-v` destroys project data.
VPS data cleanup requires separate explicit authorization.

## Authors

<div align="center">
  <table>
    <tr>
      <th>NIM</th>
      <th>Name</th>
      <th>GitHub</th>
    </tr>
    <tr align="center">
      <td>13523090</td>
      <td>Nayaka Ghana Subrata</td>
      <td>
        <a href="https://github.com/Nayekah">
          <img src="https://github.com/Nayekah.png" width="48" alt="Nayekah" /><br/>
          <sub><b>@Nayekah</b></sub>
        </a>
      </td>
    </tr>
    <tr align="center">
      <td>13524013</td>
      <td>Anindya Naufal Pinasthika</td>
      <td>
        <a href="https://github.com/Naufal-Pinasthika/">
          <img src="https://github.com/Naufal-Pinasthika.png" width="48" alt="Naufal-Pinasthika" /><br/>
          <sub><b>@Naufal-Pinasthika</b></sub>
        </a>
      </td>
    </tr>
    <tr align="center">
      <td>13524003</td>
      <td>Faiq Azzam Nafidz</td>
      <td>
        <a href="https://github.com/Shui7/">
          <img src="https://github.com/Shui7.png" width="48" alt="Shui7" /><br/>
          <sub><b>@Shui7</b></sub>
        </a>
      </td>
    </tr>
  </table>
</div>
