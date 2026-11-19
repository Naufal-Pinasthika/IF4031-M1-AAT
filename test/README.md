# Test tooling

This repository-root directory is reserved for future standalone test tooling.
The former load, fault-injection, diagnostic, evidence-collection, and tooling
test files in `infra/vps/` were deleted, not moved here. No replacement suite
or acceptance result is supplied yet.

Infrastructure provisioning, builds, deployment, inventory, and access belong
in `infra/`. New standalone test runners and helpers belong under `test/`, with
explicit prerequisites, side effects, cleanup, and evidence requirements.

Existing service-local Go tests and demonstrations in `scripts/` are unchanged.
Historical reports may refer to removed infrastructure runners; those commands
are no longer runnable. Tracked deleted files remain recoverable from Git.

Raw evidence remains local and ignored under `.e2e/` and `.vps/`. Do not commit
credentials, machine inventory, kubeconfig, or generated reports here.

See the [documentation index](../docs/README.md) and
[source/verification status](../docs/status/README.md).
