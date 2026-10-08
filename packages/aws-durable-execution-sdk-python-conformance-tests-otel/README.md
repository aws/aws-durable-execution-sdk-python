# Durable Execution Python SDK - OpenTelemetry Conformance Tests

OpenTelemetry **conformance test handlers** for the Durable Execution Python SDK
and its OpenTelemetry plugin:

- [`aws-durable-execution-sdk-python`](https://pypi.org/project/aws-durable-execution-sdk-python/)
- [`aws-durable-execution-sdk-python-otel`](https://pypi.org/project/aws-durable-execution-sdk-python-otel/)

The handlers deploy as AWS Lambda functions and are exercised by the
language-agnostic OTel conformance runner in
[`aws/aws-durable-execution-conformance-tests`](https://github.com/aws/aws-durable-execution-conformance-tests),
which invokes each function, collects its spans from the configured backend, and
asserts they match the shared requirement specification. That repository owns the
runner, the requirement specifications, and the orchestration (backend matrix,
ADOT layer resolution, collector build, long-running cycle); this package owns
the Python handlers and SAM templates.

This mirrors the non-OTel
[`aws-durable-execution-sdk-python-conformance-tests`](../aws-durable-execution-sdk-python-conformance-tests)
package: handlers live next to the SDK so a PR runs them against its own commit.

## Layout

```
src/
  common.py                 # plugin selection and input validation
  otel_<n>_<name>.py        # one module per invocation/execution scenario
  otel_long_running_<n>_<name>.py
  Makefile                  # SAM makefile build for every function
  requirements.txt          # SDK + OTel plugin, resolved from PYTHON_SDK_REF
template.yaml               # otel-invocation and otel-execution suites
template-long-running.yaml  # otel-long-running suite
tests/                      # contract tests for the templates and handlers
```

The invocation and execution requirements reuse the same scenario
handlers; the view is selected per function through the `OTEL_PLUGIN_MODE`
environment variable, which `common.otel_plugin()` reads to pick
`InvocationOtelPlugin` or `ExecutionOtelPlugin`. `template.yaml` deploys only the
view named by its `OtelSuite` parameter.

## Scenarios

| Requirement | Handler | Behavior |
|---|---|---|
| `otel-invocation-1` | `otel_1_success.handler` | Verifies every successful step and attempt span. |
| `otel-invocation-2` | `otel_2_wait_resume.handler` | Verifies every wait, resume, and post-resume step span. |
| `otel-invocation-3` | `otel_3_retry.handler` | Verifies failed and successful retry attempts across invocations. |
| `otel-invocation-4` | `otel_4_terminal_failure.handler` | Verifies complete telemetry for a terminal execution failure. |
| `otel-invocation-5` | `otel_5_child_context.handler` | Verifies every child-context and nested-step span. |
| `otel-invocation-6` | `otel_6_parallel.handler` | Verifies every parallel context, branch, step, and attempt span. |
| `otel-invocation-7` | `otel_7_map.handler` | Verifies every map context, iteration, step, and attempt span. |
| `otel-invocation-8` | `otel_8_handled_failure.handler` | Verifies complete failed-step and recovery telemetry. |
| `otel-invocation-9` | `otel_9_wait_for_condition.handler` | Verifies every condition polling attempt and continuation. |
| `otel-invocation-10` | `otel_10_wait_for_callback.handler` | Verifies callback context, callback, and submitter spans. |
| `otel-invocation-11` | `otel_11_chained_invoke.handler` | Verifies chained-invoke continuation spans. |
| `otel-invocation-12` | `otel_12_child_context_failure.handler` | Verifies a failed child-context span. |
| `otel-invocation-13` | `otel_13_parallel_failure.handler` | Verifies failed parallel-branch telemetry. |
| `otel-invocation-14` | `otel_14_map_failure.handler` | Verifies failed map-iteration telemetry. |
| `otel-invocation-15` | `otel_15_wait_interrupted.handler` | Verifies an interrupted wait when execution times out. |
| `otel-invocation-16` | `otel_16_wait_for_condition_failure.handler` | Verifies failed condition-check telemetry. |
| `otel-invocation-17` | `otel_17_wait_for_callback_failure.handler` | Verifies external callback-failure telemetry. |
| `otel-invocation-18` | `otel_18_chained_invoke_failure.handler` | Verifies failed chained-invoke telemetry. |
| `otel-invocation-19` | `otel_19_execution_failure.handler` | Verifies telemetry for a direct handler failure. |
| `otel-invocation-20` | `otel_20_virtual_context.handler` | Verifies a virtual child-context span without context checkpoints. |
| `otel-invocation-21` | `otel_21_completed_step_replay.handler` | Replays a completed step after a successful wait/resume; each step body runs once. |
| `otel-invocation-22` | `otel_22_user_function_context.handler` | Creates ordinary user spans under the active handler, attempt, child, branch, and iteration contexts. |
| `otel-invocation-23` | `otel_23_callback_function_context.handler` | Verifies retry/check attempts, callback submitter, wrapped retry helper, and virtual-child callback parents. |
| `otel-invocation-24` | `otel_24_invocation_retry_status.handler` | Raises a retryable invocation error after a completed step, then resumes with its saved result. |
| `otel-invocation-25` | `otel_17_wait_for_callback_failure.handler` | Targets a failed callback without error details; service history must satisfy the explicit no-error-details precondition. |
| `otel-invocation-26` | `otel_26_external_callback_completion_replay.handler` | Completes a root callback after suspension, saves its result in a step, and replays it through two callback barriers. |
| `otel-execution-1` | `otel_1_success.handler` | Verifies the execution-view workflow, step, and attempt hierarchy. |
| `otel-execution-2` | `otel_2_wait_resume.handler` | Verifies the execution view across a resumed invocation. |
| `otel-execution-3` | `otel_3_retry.handler` | Verifies the execution view across retry attempts. |
| `otel-execution-4` | `otel_4_terminal_failure.handler` | Verifies the failed workflow, step, and attempt hierarchy. |
| `otel-execution-5` | `otel_5_child_context.handler` | Verifies child-context and nested-step parentage. |
| `otel-execution-6` | `otel_6_parallel.handler` | Verifies parallel context, branch, step, and attempt parentage. |
| `otel-execution-7` | `otel_7_map.handler` | Verifies map context, iteration, step, and attempt parentage. |
| `otel-execution-8` | `otel_8_handled_failure.handler` | Verifies failed and recovery operations under a successful workflow. |
| `otel-execution-9` | `otel_9_wait_for_condition.handler` | Verifies condition polling attempts across invocations. |
| `otel-execution-10` | `otel_10_wait_for_callback.handler` | Verifies callback, submitter, and attempt parentage. |
| `otel-execution-11` | `otel_11_chained_invoke.handler` | Verifies source and target workflow roots for a chained invoke. |
| `otel-execution-12` | `otel_12_child_context_failure.handler` | Verifies a failed child context under a failed workflow. |
| `otel-execution-13` | `otel_13_parallel_failure.handler` | Verifies a failed parallel branch under its operation. |
| `otel-execution-14` | `otel_14_map_failure.handler` | Verifies a failed map iteration under its operation. |
| `otel-execution-15` | `otel_15_wait_interrupted.handler` | Verifies a pending invocation when workflow spans do not complete. |
| `otel-execution-16` | `otel_16_wait_for_condition_failure.handler` | Verifies a failed condition operation and attempt. |
| `otel-execution-17` | `otel_17_wait_for_callback_failure.handler` | Verifies failed callback telemetry under one workflow. |
| `otel-execution-18` | `otel_18_chained_invoke_failure.handler` | Verifies source and target failed workflow roots. |
| `otel-execution-19` | `otel_19_execution_failure.handler` | Verifies a failed invocation without a completed workflow. |
| `otel-execution-20` | `otel_20_virtual_context.handler` | Verifies a virtual child-context span under the workflow root. |
| `otel-execution-21` | `otel_21_completed_step_replay.handler` | Verifies completed-operation spans are exported once across normal successful replay. |
| `otel-execution-22` | `otel_22_user_function_context.handler` | Observes active execution-view callback contexts without supplying or repairing parents. |
| `otel-execution-23` | `otel_23_callback_function_context.handler` | Verifies the same SDK-owned callback lifecycle parents in the execution view. |
| `otel-execution-24` | `otel_24_invocation_retry_status.handler` | Verifies invocation retry status independently of step retry and recovery re-exports. |
| `otel-execution-25` | `otel_17_wait_for_callback_failure.handler` | Targets the same errorless failed callback and its `UNSET` leaf in the execution view. |
| `otel-execution-26` | `otel_26_external_callback_completion_replay.handler` | Requires one terminal root-callback export at first completion and no duplicate exports on two later replays. |
| `otel-long-running-1` | `otel_long_running_1_wait.handler` | Verifies wait and resume telemetry across a long durable suspension. |
| `otel-long-running-2` | `otel_long_running_2_retry.handler` | Verifies retry telemetry across a long durable backoff. |
| `otel-long-running-3` | `otel_long_running_3_callback.handler` | Verifies callback telemetry when completion arrives after a long delay. |
| `otel-long-running-4` | `otel_long_running_4_chained_invoke.handler` | Verifies chained-invoke telemetry while the target stays suspended. |

The runner discovers each mapping from `TestingMetadata.TestDescription` on the
functions in the templates.

## External completion and status coverage

The local testing library includes callback success, failure and timeout in the
next invocation's `UpdatedOperationIds`. The core delivers each terminal update
notification once per invocation, including when a resumed operation and a later
checkpoint response carry the same completion. It tracks actual notifications,
preserves first delivery when the update-ID metadata is absent, and clears that
tracking at invocation boundaries. Public runner regressions cover memory and
file stores, stored step results, failure payloads and two subsequent replays.
An exactly empty serialized callback error is represented as absent in both
stores, matching the SDK's service parser. Present fields remain intact,
including empty messages, types, data and stack lists; the enclosing failed
callback future still raises the same error as the file-store baseline.
Detailed callback-failure history retains the service's empty error `Payload`
object with `Truncated: false`, while SDK-facing state still has no error details.
Metadata-only history and nonempty errors retain their existing representation.

Case 24 covers invocation `RETRY` becoming `RETRYING`/`UNSET`. Case 25 targets the
separate rule that a failed operation without error details remains `UNSET`.
Its cloud coverage requires both views to pass the raw service-history no-error-details
precondition and the telemetry assertions. Local file-store results establish
SDK behavior; that AWS service precondition still needs independent validation.
`CANCELLED`, `TIMED_OUT` and `STOPPED` without error details have explicit unit
coverage in both views; their corresponding cloud paths are not established.
The existing success/`OK` and detailed-error/`ERROR` controls remain in the suites.

Case 26 revisits a root-level public callback on every replay. The driver waits
for `InvocationCompleted` before completing the target and each barrier. The
target's terminal span must precede the observed step and must not be exported
again during the two later resumes. Invocation view also retains its initial
pending callback segment. Cloud validation must count the raw S3 export records
without deduplicating equal span IDs; local runner checks alone do not establish
cloud coverage.

## Callback coverage boundary

Cases 22 and 23 create normal user spans from the active context, without an
explicit parent or a copied execution ARN. They cover handler context, step and
condition attempts, child and branch bodies, callback submission, wrapped
`with_retry` body/strategy callbacks, and a virtual child. Case 23 places the
retry helper and virtual child after callback completion so ordinary successful
replay does not repeat their probes.

This does not promise an operation/attempt parent for every arbitrary callback.
General retry/wait policies, serializers, summary generators, and item naming
have phase-specific caller scopes outside the wrapped user-function lifecycle;
these cases do not impose a new ownership policy on them. Instrumentation
extensions and user-created threads are outside this business-callback contract.

## How a handler maps to a requirement

```yaml
Otel1Success:
  Type: AWS::Serverless::Function
  Condition: DeployInvocationView
  Metadata:
    BuildMethod: makefile
  TestingMetadata:
    TestDescription:
      - otel-invocation-1
  Properties:
    CodeUri: src/
    Handler: otel_1_success.handler
    FunctionName: !Sub "${AWS::StackName}-otel-invocation-1"
    Role: !Ref LambdaExecutionRoleArn
```

## Building

`src/requirements.txt` installs both SDK packages from the single commit in
`PYTHON_SDK_REF`, so every function in a run uses the same core and plugin
revision. The explicit `src/Makefile` build avoids SAM's package metadata
inspection, which does not support Git monorepo subdirectory dependencies, and
resolves binary dependencies for Lambda's `manylinux2014_x86_64` platform when
building from macOS.

```bash
cd packages/aws-durable-execution-sdk-python-conformance-tests-otel
export PYTHON_SDK_REF=$(git rev-parse HEAD)   # must be pushed to the SDK remote
sam build --template-file template.yaml
```

## Running a suite

Prerequisites: the AWS SAM CLI, AWS credentials for an account where Durable
Execution is available, and an execution role allowing Durable Execution, logs,
and X-Ray.

```bash
pip install \
  aws-durable-execution-conformance-tests \
  aws-durable-execution-conformance-tests-otel

durable-execution-conformance \
  --template packages/aws-durable-execution-sdk-python-conformance-tests-otel/template.yaml \
  --language python \
  --suite otel-invocation \
  --parameter-overrides \
    LambdaExecutionRoleArn=arn:aws:iam::123456789012:role/example \
    OtelSuite=otel-invocation \
  --otel-exporter adot \
  --otel-layer-arn "$ADOT_LAYER_ARN" \
  --otel-service-name durable-execution-conformance \
  --otel-backend xray
```

Set `ADOT_LAYER_ARN` to the current regional ARN from the
[ADOT Python release](https://github.com/aws-observability/aws-otel-python-instrumentation/releases/latest).
The runner supplies the remaining OTel SAM parameters.

To assert against official OTLP payloads instead of X-Ray, the runner can target
a collector extension that writes OTLP objects to S3:

```bash
durable-execution-conformance \
  --template packages/aws-durable-execution-sdk-python-conformance-tests-otel/template.yaml \
  --language python \
  --suite otel-invocation \
  --parameter-overrides \
    LambdaExecutionRoleArn=arn:aws:iam::123456789012:role/example \
    OtelSuite=otel-invocation \
    OtelCollectorLayerArn="$COLLECTOR_LAYER_ARN" \
    OtelCollectorBucket="$OTEL_S3_BUCKET" \
    OtelCollectorPrefix=traces \
  --otel-exporter community \
  --otel-endpoint http://localhost:4318 \
  --otel-backend collector \
  --otel-backend-endpoint "s3://$OTEL_S3_BUCKET/traces"
```

The collector layer is built by the conformance repository's
`collector/build-lambda-layer.sh` and packages its config at
`/opt/collector-config/config-s3.yaml`. The function role needs prefix-scoped S3
write access; the runner identity needs list, read, and cleanup access.

## Authoring a new scenario

1. Find or add the requirement in the conformance repository under
   `test-requirements/<suite>/<id>.yaml`. New requirement IDs must be registered
   there first.
2. Add `src/otel_<n>_<name>.py` exporting `handler`. Select the plugin with
   `common.otel_plugin()` and guard the input with `common.require_scenario()`.
   Use the SDK's real API; never hand-roll behavior to force an expected result.
3. Register the function in `template.yaml` (or `template-long-running.yaml`)
   with `Handler: <module>.handler` and `TestDescription: ["<id>"]`, and add a
   `build-<LogicalId>` target to `src/Makefile`.
4. Update `tests/test_otel_examples.py`, which pins the template-to-requirement
   mapping.

## CI

`.github/workflows/opentelemetry-conformance-tests.yml` calls the shared
orchestrator in the conformance repository and points it at this package with
`examples_dir`, so orchestration stays centralized while the handlers run from
the commit under test. Pull requests run the invocation and execution suites plus
a short (60-second) long-running cycle; the full multi-hour long-running cycle is
driven by `workflow_dispatch` with `phase: launch` and `phase: check`.
