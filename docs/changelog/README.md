# Historical changes and test reports

This directory contains design proposals, design reviews, implementation notes,
historical test reports, and operator journals. Usage guides, references, and
runbooks remain outside it; start with the [documentation index](../README.md).

## Design and implementation notes

- [P2/P5 design proposal](M2-P2-P5-design-proposal.md)
- [Whole-system design review](M2-P2-P5-design-review.md)
- [P2 polling ledger](P2-polling-ledger.md)
- [P2 read-capacity design](P2-read-capacity-design.md)
- [P2 read-path implementation](P2-read-path-implementation.md)
- [P5 measurement and observability](P5-observability.md)
- [P3/P4 durable ingress and stream processing](P3-P4-ingress.md)
- [P1 practice verification](P1-verification.md)

These notes retain their original scope and verification limits; placing them
here does not establish a new acceptance pass.

## Verification reports

The [19 November verification report](verification-2026-11-19.md) is versioned
and preserves measured results, failures, and limitations for its named builds.
It is not a live deployment inventory or a new acceptance pass. See the
[current source and verification status](../status/README.md).

Dated operator journals in this directory remain local and ignored. They may
be absent in a fresh clone. Raw evidence stays under ignored `.e2e/` and `.vps/`.
