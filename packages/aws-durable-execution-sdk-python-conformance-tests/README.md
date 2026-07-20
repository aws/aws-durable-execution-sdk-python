# Durable Execution Python SDK — Conformance Tests

Cross-SDK **conformance test handlers** for the Durable Execution Python SDK.
These handlers deploy as AWS Lambda functions and are exercised by the
language-agnostic conformance runner
[`aws-durable-execution-conformance-tests`](https://pypi.org/project/aws-durable-execution-conformance-tests/),
which invokes each function, pulls its execution history, and asserts it matches
the shared requirement specification.

The runner and requirement specifications are maintained in
[`aws/aws-durable-execution-conformance-tests`](https://github.com/aws/aws-durable-execution-conformance-tests).
This package owns the Python handlers and SAM templates that wire them to
requirement IDs.

## Layout

```
handlers/
  <suite>/                  # one .py handler per conformance scenario
template_<suite>.yaml       # maps suite handlers to requirement IDs
scripts/
  discover_suites.py        # derives supported suites from templates
  build_examples.py         # assembles lambda-build/ from the local monorepo SDK
  inject_execution_role.py  # CI: point functions at a pre-existing role
tests/                      # unit tests for the scripts
```

Each `template_<suite>.yaml` is a self-contained deployment for one conformance
suite. Templates are the single source of truth: the local build and GitHub
Actions matrix discover suites from them automatically.

## How a handler maps to a requirement

The link is the `TestingMetadata.TestDescription` field on each function in the
SAM template:

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

The runner invokes the function once per requirement ID using the requirement's
`Input`, then compares the execution history with `ExpectedExecutionHistory`.

## Building locally

`scripts/build_examples.py` assembles `lambda-build/` from the local monorepo
SDK source, not a PyPI release. `boto3` is provided by the Lambda runtime and is
not vendored.

```bash
cd packages/aws-durable-execution-sdk-python-conformance-tests
python3 scripts/build_examples.py
```

The script discovers every `template_<suite>.yaml`, verifies a matching
`handlers/<suite>/` directory exists, and copies all discovered suites:

```
lambda-build/
  aws_durable_execution_sdk_python/
  <suite>/
```

## Running a suite

Prerequisites: Python ≥ 3.14, the AWS SAM CLI, and AWS credentials for an
account where Durable Execution is available.

```bash
cd packages/aws-durable-execution-sdk-python-conformance-tests
SUITE=<suite>

python3 scripts/build_examples.py
pip install aws-durable-execution-conformance-tests==0.1.0

python -m aws_durable_execution_conformance_tests.app \
  --template "template_${SUITE}.yaml" \
  --language python \
  --suite "$SUITE" \
  --name "conformance-python-${SUITE}-local" \
  --region us-west-2 \
  --history-dir "history-${SUITE}" \
  --report junit --report-file "report-${SUITE}"
```

The runner deploys the template, invokes each function, validates its result and
history, and cleans up the stack by default.

## Authoring a new test case

1. Find or add the requirement in the conformance repository under
   `test-requirements/<suite>/<id>.yaml`.
2. Add `handlers/<suite>/<descriptive_name>.py` exporting `handler`. Use the
   SDK's real API; never hand-roll behavior to force an expected result.
3. Register it in `template_<suite>.yaml` with
   `Handler: <suite>.<name>.handler` and `TestDescription: ["<id>"]`.
4. Rebuild and run the suite.

For a new suite, adding its template and handler directory is sufficient; the
build and CI matrix discover it automatically.

## CI

`.github/workflows/conformance-tests.yml` first discovers all suites from
`template_<suite>.yaml`, then runs one parallel matrix job per suite. CI assumes
AWS credentials through the repository's existing OIDC secrets.

Before deployment, `scripts/inject_execution_role.py` points every function at
the pre-existing execution role (`TEST_LAMBDA_EXECUTION_ROLE_ARN`) and removes
the template's self-created `DurableFunctionRole`. The checked-in templates
remain self-contained for local runs.
