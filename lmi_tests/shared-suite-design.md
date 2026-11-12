# Shared LMI conformance adapter

## Problem

Python's LMI regression harness and the shared conformance runner currently use
independent drivers, evidence formats and acceptance reports. Merging both test
PRs does not make the Python fixtures satisfy shared coverage or ownership rules.

## Cause

The SDK harness owns the actual public-API functions, deployment, S3 ledger and
service calls. The shared runner owns the multi-invocation scenario matrix,
resource leases, validators and strict reports. The existing ledger does not yet
record every operation, export and lifecycle boundary the shared schema requires.

## Options considered

1. Keep invoking pytest from the shared adapter. This reuses the old command but
   does not expose placement, collection and retirement as separate phases, or
   bind every native invocation to the shared ownership journal.
2. Copy the shared validators into this repository. This preserves two competing
   acceptance implementations and makes version/replay comparisons unreliable.
3. Adapt the existing Cloud/fixture implementation and add the missing public-API
   observations, while delegating scheduling and acceptance to a pinned shared
   runner. This reuses the actual SDK behavior and eliminates the independent CI
   acceptance path without fabricating missing evidence.

## Chosen design

Use option 3, following the requested adoption of conformance PR #186. Pin the
shared runner revision. Generate its `python-lifecycle` and `export-lifecycle`
matrices for the two persistent c1/c2 functions: 76 mapped lifecycle/OTel cells
plus 24 exporter-mode cells. Keep exporter mode independent of language and choose
immutable provider variants per invocation. The shared conformance package runs
on the driver only, not inside the deployed function bundle.

The SDK-owned adapter exposes prepare/start/place/execute/collect/release/retire
and finish. It keeps a distinct Cloud/controller state for every case, journals
all direct launches and observed service resumes, and records exact artifact,
configuration and original request identities. Public plugin hooks and the
existing forwarding Lambda client provide actual operation/checkpoint metadata.
Missing observations remain incomplete; no timestamps, completion events or SDK
capabilities are synthesized to force shared validation to pass.

Lifecycle and telemetry records preserve actual boundary timestamps separately
from later diagnostic I/O. Normalization retains raw source evidence. Synchronous
exports settle before wrapper return; asynchronous exports and flush work retain
ownership after a bounded wait until actual completion. Case cleanup releases only
its own controls and executions. Persistent ownership uses conditional S3 writes
outside the expiring run prefix and is retained after unproven retirement. S3
stays private and uses native SigV4/IAM authentication.

CI retains the existing opt-in cloud policy and SDK-local unit/regression tests,
but invokes the shared runner for cloud acceptance. Reports and all attempted
case evidence are retained. The implementation and tests use the real public SDK
handler/plugin paths against the local service where practical; real-LMI
placement/recovery and deployed acceptance remain explicitly unverified until a
cloud run is recorded.

## Removed or superseded work

The old pytest cloud command is no longer the primary CI acceptance path. Its
SDK-specific fixture/regression tests remain available during migration. Shared
requirements and validators are not duplicated here, and this change does not
claim to fix the open runtime deadline/cleanup defects tracked by Python #741.

## Reviewable decisions and limits

- This PR targets current main, reuses the Python #743 harness, and does not modify that PR's branch.
- Conformance is pinned to PR #186 revision `6f899679c7eb6ede836c737acb9816e6fd7072fd`.
- The default target is the Python-specific 76-cell matrix plus the portable
  24-cell exporter matrix; it does not imply ordinary SDK/OTel suite acceptance
  on LMI or acceptance against the separate standard profile.
- Fixture budgets are declared before a run and kept distinct from product SLAs.
- SDK bugs stay failures. No cloud success or bounded arbitrary user-I/O
  termination is inferred from local harness tests.

## Observation boundaries

The public SDK does not emit a root user-function hook. The fixture observes its
actual root entry/finally using the root operation ID supplied by public invocation
metadata. Operation names may be null. A Workflow span may start at the real
service execution start, before the current native invocation begins; history
retains that timestamp separately from invocation admission.

The source ledger reserves capture order at each actual SDK boundary independently
of later S3 publication. Export release receipts record when the SDK observed the
control; driver release intent and acknowledgement remain separate observations.
Native timeout and wrapper return are independent outcomes. Deadline measurements
are frozen before cleanup releases the held step; cleanup cannot repair failed
behavior assertions. The shared error expectations explicitly enumerate allowed
service statuses where a measurement may precede execution termination.
