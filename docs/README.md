# Documentation

Start with the [project overview](../README.md). Commands in these guides run
from the repository root unless stated otherwise.

## Setup and operation

| Task | Guide |
| --- | --- |
| Run the local Compose lab | [Local development](guides/local-compose.md) |
| Provision, deploy, or access existing VPSs | [VPS runbook](../infra/vps/README.md) |
| Choose a script and understand its effects | [Script catalog](../infra/vps/SCRIPTS.md) |
| Look up Compose APIs, credentials, and defaults | [API/configuration reference](reference/api-configuration.md) |
| Demonstrate M1 cases | [M1 runbook](runbooks/m1-demo.md) |
| Understand M2/manual demonstrations | [M2 guide](guides/m2.md) |
| Run report-facing P3/P4 E2E tests | [Test directory](../test/README.md) |
| Check what is actually proven | [Source/runtime and verification status](status/README.md) |

## Design and contracts

Design proposals, reviews, and implementation notes are collected in
`changelog/`. Usage guides and references remain separate.

- [P2 polling ledger](changelog/P2-polling-ledger.md): ownership, fencing, durable admission,
  and schema/rollback constraints.
- [P2 read-path implementation](changelog/P2-read-path-implementation.md): bounded read
  projections, gateway admission, and memory budgets.
- [P2 read-capacity design](changelog/P2-read-capacity-design.md): measured bottlenecks and
  validation gates; not an acceptance pass.
- [P5 observability](changelog/P5-observability.md): measurement boundaries and private dashboard.
- [Whole-system review](changelog/M2-P2-P5-design-review.md): dependencies, trade-offs, and
  historical acceptance matrix.
- [Design proposal](changelog/M2-P2-P5-design-proposal.md): proposals and alternatives;
  not every proposed mechanism is deployed.

## Historical evidence

Design notes, historical reports, and journals belong in [changelog/](changelog/README.md).
Active guides and references remain in their separate directories above.

The [19 November verification report](changelog/verification-2026-11-19.md)
preserves the previous documentation index's full experimental history.
Its named candidates and failures are historical, not a live deployment inventory.
Dated journals and raw run artifacts stay local; see the [evidence policy](status/README.md#evidence-policy).

## Configuration ownership

Application defaults and secret placeholders: [`.env.example`](../.env.example).
Kubernetes application topology/policy: [`k8s/base`](../k8s/base/kustomization.yaml).
Operator-side namespace and NodePort mapping: [`vps_config.py`](../infra/vps/vps_config.py).
Machine addresses and SSH key paths: local ignored `infra/vps/inventory.json`,
following the [example](../infra/vps/inventory.example.json).
