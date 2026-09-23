# Python LMI end-to-end tests

This suite implements [#742](https://github.com/aws/aws-durable-execution-sdk-python/issues/742)
and asserts the invocation-lifetime behavior requested by
[#741](https://github.com/aws/aws-durable-execution-sdk-python/issues/741).
It follows the separate cloud-suite approach in
[Java PR #728](https://github.com/aws/aws-durable-execution-sdk-java/pull/728).
It changes no production SDK code. The deadline and early-completion regressions
are **expected to fail on the current SDK**. They are ordinary failing assertions,
with no `xfail`, swallowed failure, or expected-failure success status. CI runs the
cloud suite automatically on every trusted PR update and main push. Local cloud
runs and local red regressions require explicit commands; the ordinary local test
suite remains usable while the fix is designed.

## Run locally

Use Python 3.13 or 3.14 and Hatch from the repository root:

```sh
hatch run lmi:unit
hatch run lmi:lint
hatch run lmi:regressions --junitxml=lmi_tests/artifacts/regressions.xml
```

The last command asserts four fixed behaviors against the real decorator/step/map/
parallel APIs with an in-memory checkpoint service: an expired invocation starts no
step, and an early result does not pin a parallel, map, or nested-map wrapper past
its invocation budget. These local tests have final releases and bounded fixture
waits. They establish the SDK defects, not real LMI worker recovery.

The green harness tests exercise negative evidence controls, process-shared marker
initialization, real public-API replay/callback/retry with the local runner,
checkpoint response/error forwarding, deployment readback, and ownership/retirement.
They also exercise the real parallel/map/nested fixtures with controlled I/O and
reject an early-completion pass when the losing branch failed before the winner.

## Cloud prerequisites and configuration

Use a **test account** and region supporting durable functions on LMI with Python
3.13/3.14 and arm64. Provision an existing test-owned capacity provider with an
explicit 2–128 vCPU maximum. Its `VpcConfig` supplies subnet/security-group
configuration; change networking on that provider before the run. Workers need
access to Lambda's checkpoint API, S3, and CloudWatch Logs through NAT or endpoints.
The suite saves the provider configuration, but never creates, updates, or deletes
the provider. Its owner remains responsible for its capacity costs and lifecycle.

Set:

| Variable | Purpose |
| --- | --- |
| `AWS_REGION` | Region matching the provider |
| `TEST_ACCOUNT_ID` | Required authenticated account check |
| `CAPACITY_PROVIDER_ARN` | Existing test provider with bounded capacity |
| `TEST_LAMBDA_EXECUTION_ROLE_ARN` | Lambda role with LMI/durable execution and logging permissions |

The deployment identity needs CloudFormation stack operations, S3 bucket/policy/
object operations, Lambda deployment/configuration/invoke/history/stop/delete,
`iam:PassRole` for the supplied role, CloudWatch Logs read/create/delete, and
read access to the provider. The stack's private bucket policy grants that function
role access **only** to this run's event and control objects. The SDK and dependencies
come from a wheel built from the checked-out core package; no published SDK wheel
is substituted. The artifact hash, SDK wheel version, and commit are retained.

```sh
hatch run lmi:build
hatch run lmi:python -m lmi_tests.deploy reconcile
hatch run lmi:python -m lmi_tests.deploy deploy \
  --run-id local-20260923-a --runtime python3.13 --concurrency 2
hatch run lmi:cloud --junitxml=lmi_tests/artifacts/cloud.xml
# Run both commands below even when a scenario fails:
hatch run lmi:python -m lmi_tests.deploy collect
hatch run lmi:python -m lmi_tests.deploy cleanup
python -m lmi_tests.summary
```

For unattended local runs, install a shell `EXIT`/`INT`/`TERM` trap that calls
`collect` followed by `cleanup`; the workflow already has unconditional steps.
Use a fresh run ID for each deployment. Run all four combinations of runtime
`python3.13`/`python3.14` and environment concurrency `1`/`2`, as CI does.
The independent SDK branch-concurrency settings include 2 and 3.

Each combination creates a unique stack, private bucket, log groups, and two
functions (`normal`, `deadline`). Both functions declare native scaling limits of
exactly one environment and use 2 GiB / 1 vCPU. LMI automatically publishes
`$LATEST.PUBLISHED`; every invocation uses that qualified target. Creating an extra
numbered version would allocate another set of environments. Before assertions,
the suite checks Active state, runtime/architecture, durability, timeout, provider,
process concurrency, applied scaling limits, commit/run identity, and code hash.
Unsupported configurations, insufficient permissions, and unavailable capacity fail
provisioning. There is no fallback to standard Lambda and no passing cloud skip.

The normal handler mapping is saved in `artifacts/function-name-map.json`, using
the existing `PYTEST_FUNCTION_NAME_MAP` convention. The test driver reuses
`DurableFunctionCloudTestRunner` for async invocation, callback completion, and
waiting for results, with finite boto transport timeouts.

## Evidence and regression contracts

| Scenario | Evidence required |
| --- | --- |
| Replay/callback/retry | Real service histories; one body execution for checkpointed success and failure; stable failure type/message; retry attempts 1 and 2; nested child/map, parallel, wait, callback completion; PENDING and resumed requests |
| Environment concurrency | Held external barriers overlap in one shared `/tmp` environment marker, with distinct request IDs, execution ARNs, process UUIDs, and PIDs; c1 is the baseline, c2 must prove two processes |
| Warm cleanup | Repeated success, failure, and suspension/resume; explicit reuse of the same environment/process; no residual SDK threads after wrapper return; bounded current RSS and FD growth after warming |
| Early completion (#741) | `first_successful` parallel, `min_successful=1` map, and nested pools; loser blocked inside a step; separate winner selection, user result, user exit, wrapper exit; no late effects after grace |
| Checkpoint settlement | Real synchronous service checkpoint is acknowledged but response delivery to the SDK waiter is externally held; wrapper cannot return before settlement; releasing the response must unblock branch cleanup |
| Invocation deadline (#741) | A real short function timeout, service invocation-completed error for the exact request, external side effects, full original-environment worker capacity recovery, and healthy concurrent work on another process for c2 |
| Timeout/retry | A second service invocation of the **same** durable execution, completed step skipped, original attempt effects cease; repeated interrupted at-least-once work is recorded separately and is not called exactly-once |

The current SDK has no public cooperative cancellation API. The fault fixture
therefore deliberately remains blocked until interrupted/retired by the eventual
SDK/runtime policy or released externally. Its 75-second emergency limit always
invalidates a regression pass. It does not implement deadline cancellation in test
code, use `shutdown(wait=False)` as a fix, kill the process itself, or manufacture
successful checkpoints. Selecting a bounded retirement strategy belongs to #741.

The initial **test acceptance budget** is five seconds after invocation deadline
or early winner selection. This is not a published SDK cancellation contract;
review/update it together with the #741 fix. A platform-supported replacement of
an affected Python worker in the same environment is allowed. Whole-environment
replacement is reported as a placement failure until a supported policy and
corresponding evidence are agreed; scaling elsewhere cannot hide a pinned worker.
The local regressions use a one-second invocation budget and two-second observation.

All trace records include run, commit, marker, execution ARN, request, environment,
process initialization UUID, PID, sequence, timestamp, and phase. Body/effect records
include operation/attempt where relevant. A file lock safely initializes the shared
environment UUID across Python processes. Test-only wrapper/client diagnostics do
not control durable branches. All barrier and side-effect I/O in user code lives
inside steps; operation names stay static. Infrastructure diagnostics can repeat on
replay and are explicitly separate from the body/effect ledger.

S3 records remain readable when a wrapper is stuck. CloudWatch logs and real history
provide independent runtime/service evidence. Recovery probes are queued at the
deadline, before waiting for eventually consistent history. A client timeout or a
logical execution's `TIMED_OUT` status alone cannot satisfy invocation-timeout
assertions. Missing service retry evidence is a collection/precondition failure,
never a passing retry test. No explicit second logical execution is mislabeled as
a service retry.

Control objects are created with `hold` before invocation and updated to `release`
explicitly. Fixtures read their content with `GetObject`; missing objects, 403s,
and invalid states emit `CONTROL_ERROR` and fail as test infrastructure errors.
This avoids relying on `HeadObject` returning 404 for missing keys when the
function role has no `s3:ListBucket` permission. The winner is released only after
the losing step has validated its controls and emitted an actual side-effect
record. A loser that exits before `WINNER_READY` cannot satisfy the regression.

Lifecycle objects are partitioned by case marker and request ID. Live polling
reads only the current case's prefixes with a bounded pool of readers; final
collection retrieves the complete run. Earlier cases therefore do not consume a
later case's short observation budget. Checkpoint holds and returns are correlated
to the same request, and stale-attempt effects are checked before waiting for a
retry that might itself be unable to start on a pinned worker.

## Independent budgets and results

| Budget | Default |
| --- | --- |
| Lambda invocation | Normal 60 s; deadline fixture 10 s |
| Durable logical execution | 240 s |
| Driver result polling | 120 s |
| Cleanup acceptance grace | 5 s |
| Fault-fixture emergency I/O release | 75 s |
| Workflow cloud assertions | 12 min |
| Matrix job including provisioning/retirement | 55 min |

Artifacts contain JUnit, configuration/provider readbacks, qualified targets,
commit/wheel/code hash, invocation inputs/ARNs, all lifecycle and side-effect records,
execution histories/results, logs, setup/collection errors, and cleanup confirmation.
Tokens are omitted from client tracing and redacted from structured histories.
JUnit `lmi_outcome` properties and the Actions summary distinguish
`ProvisioningError`, `PlacementError`, `CollectionError`, and `RegressionAssertion`.
An unexecuted scenario is never summarized as passing.

## CI and resource ownership

The dedicated workflow runs the harness and full LMI cloud matrix automatically
when a same-repository PR is opened, updated, or reopened (including Draft PRs),
and on every push to `main`, including merged changes. There are no path filters,
label requirements, or ready-for-review requirements. `workflow_dispatch` remains
available for manual reruns; there are no scheduled jobs.

Each workflow run has its own resources and runs its four matrix entries
sequentially. Runs do not share a GitHub concurrency group, so a new PR update or
main push cannot replace another commit's pending cloud job.

Privileged cloud jobs retain the repository's existing restrictions for forked PRs
and Dependabot; the harness still runs for those PRs. Cloud jobs reuse
`TEST_ROLE_ARN`, `TEST_ACCOUNT_ID`, and `TEST_LAMBDA_EXECUTION_ROLE_ARN` with OIDC.
The #741 regression assertions remain visibly failing until its fix lands; they
are not skipped or converted into expected-success results to keep CI green.

The workflow collects evidence before teardown, even after test failure or ordinary
cancellation. Cleanup sends external releases, stops running durable executions,
deletes the run's functions to retire their environments, empties the bucket, and
waits for stack deletion. Stopping an execution alone is not evidence that Python
code stopped. Only suite/run-tagged resources are eligible for deletion. A failed
create/name collision cannot cause another run's stack to be deleted.

Forced runner termination may prevent an `always()` step. Each stack has a four-hour
expiry tag; `reconcile` runs before the next cloud deployment and can also be invoked
manually. It retires only expired stacks owned by this suite. S3 objects additionally
expire after two days; object expiry does not retire compute. Inspect cleanup errors
and run reconciliation if `cleanup.json` does not confirm deletion. The shared
capacity provider remains under its owner's explicit lifecycle policy.
