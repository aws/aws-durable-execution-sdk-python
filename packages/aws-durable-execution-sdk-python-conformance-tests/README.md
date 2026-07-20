# Durable Execution Python SDK — Conformance Tests

Cross-SDK **conformance test handlers** for the Durable Execution Python SDK.
These handlers deploy as AWS Lambda functions and are exercised by the
language-agnostic conformance runner
[`aws-durable-execution-conformance-tests`](https://pypi.org/project/aws-durable-execution-conformance-tests/),
which invokes each function, pulls its execution history, and asserts it matches
the shared requirement specification.

The runner and the requirement specifications (the `test-requirements/` YAML
files) are **not** in this repo — they live in
[`aws/aws-durable-execution-conformance-tests`](https://github.com/aws/aws-durable-execution-conformance-tests).
This package owns only the **Python handlers** and the **SAM templates** that
wire them to requirement IDs.

## Layout

```
handlers/
  <suite>/                  # one .py handler per conformance scenario
template_<suite>.yaml       # maps suite handlers to requirement IDs
scripts/
  build_examples.py         # assembles lambda-build/ from the local monorepo SDK
  inject_execution_role.py  # CI: point functions at a pre-existing role
tests/                      # unit tests for the scripts
```

Each `template_<suite>.yaml` is a self-contained deployment for one conformance
suite. The checked-in templates and the CI workflow matrix are the source of
truth for supported suites; this README intentionally does not duplicate that
list.

## How a handler maps to a requirement

The link is the `TestingMetadata.TestDescription` field on each function in the
SAM template — a list of requirement IDs the handler satisfies:

```yaml
RequirementCase:
  Type: AWS::Serverless::Function
  TestingMetadata:
    TestDescription: ["<requirement-id>"]
  Properties:
    CodeUri: lambda-build/
    Handler: <suite>.<handler_module>.handler
    Role: !GetAtt DurableFunctionRole.Arn
    DurableConfig:
      RetentionPeriodInDays: 7
      ExecutionTimeout: 300
```

The runner invokes the function once per requirement ID using that requirement's
`Input`, then diffs the resulting execution history against the requirement's
`ExpectedExecutionHistory`.

## Building locally

`scripts/build_examples.py` assembles the Lambda deployment package under
`lambda-build/` (git-ignored) from the **local monorepo SDK source** — not a PyPI
release — so the handlers exercise exactly the code in this checkout. `boto3`
(the SDK's only runtime dependency) is provided by the Lambda Python runtime and
is intentionally not vendored.

```bash
cd packages/aws-durable-execution-sdk-python-conformance-tests
python3 scripts/build_examples.py     # local SDK auto-located from the monorepo
```

This produces:

```
lambda-build/
  aws_durable_execution_sdk_python/   # copied from the local SDK package
  <suite>/                            # copied from handlers/<suite>
```

## Running a suite

Prerequisites: Python ≥ 3.14, the AWS SAM CLI, and AWS credentials for an
account where the Durable Execution service is available.

```bash
cd packages/aws-durable-execution-sdk-python-conformance-tests
SUITE=<suite>

# 1. Assemble lambda-build/ from the local SDK
python3 scripts/build_examples.py

# 2. Install the pinned conformance runner
pip install aws-durable-execution-conformance-tests==0.1.0

# 3. Deploy + invoke + validate the selected suite
python -m aws_durable_execution_conformance_tests.app \
  --template "template_${SUITE}.yaml" \
  --language python \
  --suite "$SUITE" \
  --name "conformance-python-${SUITE}-local" \
  --region us-west-2 \
  --history-dir "history-${SUITE}" \
  --report junit --report-file "report-${SUITE}"
```

The runner deploys the template via SAM, invokes each function once per
requirement ID, and reports `PASSED` / `FAILED` / `UNCOVERED` per requirement. A
non-zero exit means at least one requirement failed. Stacks are cleaned up by
the runner's default `--cleanup` behavior.

## Authoring a new test case

1. **Find (or add) the requirement** in the conformance repo under
   `test-requirements/<suite>/<id>.yaml`.
2. **Write the handler** at `handlers/<suite>/<descriptive_name>.py` exporting a
   `handler`. Use the SDK's **real API** — never hand-roll logic to force the
   expected result. A handler that fails because the SDK is non-compliant is a
   valid, valuable outcome; that failure is the signal. Naming convention:
   descriptive `snake_case`, no numeric IDs in the filename (IDs live in
   `TestDescription`).
3. **Register it** in `template_<suite>.yaml` with `Handler: <suite>.<name>.handler`
   and the matching `TestDescription: ["<id>"]`.
4. **Rebuild and run** (above).

## CI

`.github/workflows/conformance-tests.yml` runs the same flow on pull requests
that touch this package and on manual dispatch, one parallel job per suite (a
build matrix). It assumes AWS credentials via OIDC using the repository's
existing integration secrets (`TEST_ROLE_ARN` for the SAM-capable deploy role).

Before deploying, CI runs `scripts/inject_execution_role.py` to point every
function at the pre-existing execution role
(`TEST_LAMBDA_EXECUTION_ROLE_ARN`) and drop the template's self-created
`DurableFunctionRole` — so CI deploys don't create IAM roles. This rewrites only
CI's checkout; the checked-in template stays self-contained for local runs.
