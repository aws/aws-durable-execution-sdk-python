# Python LMI lifecycle conformance

This PR adapts the Python SDK to the seven shared lifecycle requirements in
[conformance #186](https://github.com/aws/aws-durable-execution-conformance-tests/pull/186).
The exact dependency is pinned in `shared_contract.py` and the workflow. Each
shared requirement declares one scalar `scenario`; the Python adapter reads the
shared matrix directly rather than maintaining a second requirement catalog.

| Requirement | Scenario | Target | Invocation roles |
| --- | --- | --- | --- |
| `lmi-lifecycle-1` | `native-overlap` | c2 | victim, peer |
| `lmi-lifecycle-2` | `normal-return` | c1 | victim |
| `lmi-lifecycle-3` | `deadline-during-step` | c1 | victim |
| `lmi-lifecycle-4` | `peer-recovery` | c2 | victim, peer, recovery |
| `lmi-lifecycle-5` | `suspension-retry` | c1 | victim, resume |
| `lmi-lifecycle-6` | `warm-reuse` | c1 | victim, warm |
| `lmi-lifecycle-7` | `normal-failure` | c1 | victim |

The shared runner owns assertions, strict reports, placement identity, per-case
scheduling, launch journals and independent retirement auditing. This repository owns the
real public SDK handlers, AWS transport, source observations and deployment.
OTel/exporter tests and the previous Python-specific matrix are deferred.
There is no local conformance profile or validator that replaces a shared rule.

## Local validation

Install Hatch and Python 3.14. Clone the shared repository into `.conformance`
and check out the exact `CONFORMANCE_REVISION` from `shared_contract.py`:

```bash
git clone https://github.com/aws/aws-durable-execution-conformance-tests.git .conformance
git -C .conformance checkout --detach cc1eae152d9d0a49ecb777db140339f290b43567
hatch run lmi:shared-setup
hatch run lmi:lint
hatch run lmi:shared-tests
hatch run lmi:build
```

`shared-setup` verifies the commit and rejects edited shared package sources.
It installs only the shared core runner. The LMI environment contains the checked
out core SDK and testing library; the Lambda bundle contains the core SDK and
its dependencies plus fixture/observation modules. It does not include the OTel
SDK, an OTel plugin, local catalogs, validators or the shared runner itself.

Unit tests cover manifest completeness, case ownership, cancellation, source
normalization, driver ordering and cleanup. Local e2e tests use the actual public
SDK and its in-process test service, replacing only AWS transport and evidence
storage. They verify checkpoint/replay and preserve genuine admission failures.
These controls do not establish real LMI placement or deployed acceptance.

## Cloud execution

Use a dedicated test account with the configured LMI capacity provider and Lambda
execution role. The workflow validates the account before deployment. The
persistent functions use Python 3.14, arm64, 2048 MiB and native concurrency 1 and
2, with one environment each. SDK artifact and configuration digests are retained
in every manifest and checked before invoking either target.

Run cloud tests through the workflow's `workflow_dispatch` action or a trusted
same-repository PR. Its fixed `lmi-e2e-shared-capacity-provider` concurrency group
serializes cloud jobs across PRs and refs from deployment through final cleanup,
with `cancel-in-progress: false`. GitHub Actions is the only cross-run concurrency
control. Direct CLI calls do not acquire a lock and are not serialized with CI.

`shared plan` writes the seven-case manifest without invoking functions. `shared
run` writes under `lmi_tests/artifacts/shared/standard/`: the manifest, coverage,
JSON/JUnit reports, launch journal and retained case evidence. Each invocation
uses a case-owned control/evidence prefix. A service resume is journaled under
the same durable execution; it is not another driver Invoke.

Trusted same-repository pull requests and manual workflow runs exercise the cloud
suite. Fork and Dependabot pull requests run only the unprivileged local harness.
There is no S3 lease, fence token or persistent quarantine record. Existing S3
ownership objects are not read and cannot block a later workflow run.

## Actual observations and failure handling

`fixture.py` uses public durable APIs for the scenarios. Its instrumentation
observer captures actual operation IDs, parents, stored outcomes and user-task
lifetimes. The service-client wrapper forwards every SDK request and response;
it never supplies synthetic checkpoints or changes SDK cancellation flags.
The instrumentation observer is a measurement tool, not a plugin conformance test.

For normal return/failure, the adapter records the actual durable wrapper boundary
and probes the original context after that boundary. The operation probe uses
`DurableContext.step`; the checkpoint probe calls the SDK's
`context.state.create_checkpoint(is_sync=False)` admission path without issuing a
raw Lambda request. Only an actual `OrphanedChildException` is recorded as
rejection. An accepted checkpoint, another error, or a still-pending operation
remains visible. Probe tasks are tracked separately; a timeout cannot make them
quiescent. Current SDK gaps are failures, not expected passes or exclusions.

The deadline fixture holds real I/O across the runtime deadline and cooperatively
exits after it. A runtime clock alone never supplies a `platform_timeout` event:
that event requires the original request's service `InvocationCompleted` history.
The recovery driver keeps the original healthy peer held while an independent
recovery request establishes capacity in the original environment. Releasing a
victim fault cannot establish the preceding cleanup or recovery assertion.

The warm fixture creates invocation-owned late work inside a step. A later
invocation must find it in the same process; it observes the old callback using
the original request identity. Failure to reuse that process is a placement or
collection failure, not permission to select a replacement victim.

`shared_evidence.py` normalizes retained observations into the shared schema.
Service payload wrappers are decoded consistently with checkpoint snapshots;
truncated results are rejected. Replay compares a real resume's initial
checkpoints to the final service history and counts actual user-function entries.
Missing observations are never filled from expected answers.

The shared behavior schema asks for one scenario-owned `task_exit`. Other user
and probe tasks remain in the same evidence as `observed_task_start/exit`, with
their original `sourceKind`. The independent retirement snapshot restores all
original task kinds, so a pending task cannot disappear from the audit.
Coordination barriers retain their complete `controlRecords` journal separately
from injected victim `faultsCreated`; releasing a healthy peer is not victim
fault recovery.

Cleanup releases only the current run's controls and requests stops only for its
identified executions. It validates run, case and function identity before those
requests. It records `NO_CASES`, `CLEANUP_REQUESTED` or `FAILED` in `cleanup.json`;
completed cleanup requests do not claim that every task retired. Assertion and
retirement failures remain in the shared reports. The shared runner can prevent
reuse of an unretired target within the current run, but that state is not stored
as a lock for future workflow runs.
