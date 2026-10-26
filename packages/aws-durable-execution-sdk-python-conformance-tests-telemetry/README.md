# Durable Execution Python SDK: Durable Execution Telemetry Conformance Tests

This package answers one question.
Does [`aws-durable-execution-sdk-python-telemetry-exporter`](../aws-durable-execution-sdk-python-telemetry-exporter), with its `CloudWatchLogsExporter`, deliver a complete `DurableExecutionTelemetry` record to CloudWatch Logs for a real Lambda execution?

Unit tests can assert that the plugin builds a record.
They cannot assert that the record reaches the configured log group on a real invocation.
They cannot assert that it carries the right identity fields for the deployed function.
Those are properties of a deployed function.
So the scenario here is a Lambda function.
The runner deploys it with the plugin configured and invokes it once.
It then fetches the record from CloudWatch Logs and checks it against the requirement.

The requirement and the runner live in [`aws/aws-durable-execution-conformance-tests`](https://github.com/aws/aws-durable-execution-conformance-tests), package `aws-durable-execution-conformance-tests-telemetry`.
This package holds the SDK-specific part: the handler and the SAM template that maps it to the requirement ID.

## Coverage

| Suite       | Requirements  | What it pins down                                                                                                                              |
| ----------- | ------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `telemetry` | `telemetry-1` | One successful single-step execution emits one `SUCCEEDED` record with `input`, `output`, ARN-derived identity fields, and `operationsByName`. |

## Layout

```
src/
  telemetry_1_success.py     # single step "increment"; plugin + CloudWatchLogsExporter
  Makefile                   # build-Telemetry1Success target used by `sam build`
  requirements.txt           # core SDK + telemetry exporter, pinned to ${PYTHON_SDK_REF}
template.yaml                # declares TelemetryLogGroupName and creates the group
tests/
  test_telemetry_examples.py # guards template -> handler wiring and the parameter contract
```

The handler uses the real SDK API only.
It constructs no record and calls no exporter method.
A missing or wrong record is the plugin's defect and is the signal the suite measures.

## Template contract

The runner passes `TelemetryLogGroupName` from the extension.
Extensions have no pre-deploy hook.
So the template creates the group with an `AWS::Logs::LogGroup` resource, and the function depends on it.
The group name reaches the handler through `TELEMETRY_LOG_GROUP_NAME`.
The handler raises at import if that variable is unset.

The `LambdaExecutionRoleArn` role must allow `logs:CreateLogStream` and `logs:PutLogEvents` on the group.
`AWSLambdaBasicDurableExecutionRolePolicy` grants both on `*`.

## Building locally

Prerequisites: Python 3.13 and the AWS SAM CLI.

`src/requirements.txt` installs both SDK packages from git at `${PYTHON_SDK_REF}`.
The ref must be a pushed commit that contains the packages.

```bash
export PYTHON_SDK_REF=$(git rev-parse HEAD)   # must already be pushed
sam build --template packages/aws-durable-execution-sdk-python-conformance-tests-telemetry/template.yaml
```

Run the wiring tests from the repository root:

```bash
hatch run test:all packages/aws-durable-execution-sdk-python-conformance-tests-telemetry/tests
```

## Running the suite

Run from a checkout of the conformance repository with the telemetry package installed:

```bash
durable-execution-conformance \
  --template <this-repo>/packages/aws-durable-execution-sdk-python-conformance-tests-telemetry/template.yaml \
  --language python --region us-west-2 --suite telemetry \
  --report console json \
  --parameter-overrides LambdaExecutionRoleArn=<role-arn>
```

## CI

[`.github/workflows/telemetry-conformance-tests.yml:5–63`](../../.github/workflows/telemetry-conformance-tests.yml#L5-L63)
runs on pull requests and pushes to `main` that touch the SDK, telemetry
exporter, this package, or the workflow file. It also supports manual dispatch.

It calls the conformance repository's reusable telemetry workflow at a pinned
SHA. `sdk_ref` selects the PR head commit, and `examples_dir` points to this
package. CI builds the handlers with `sam build`, which installs the SDK and
exporter from git at that commit. It then deploys the template and validates
the `telemetry` suite. Reports and execution histories are uploaded as
artifacts.

CI assumes AWS credentials through OIDC using the repository's existing test
secrets. Fork and Dependabot pull requests skip the cloud job.
