# Integration Tests for Python Durable Execution SDK

This directory contains integration tests for the Python Durable Execution SDK examples. Tests can run in two modes using pytest fixtures.

## Test Modes

### Local Mode (Default)
Tests run against the in-memory `DurableFunctionTestRunner`:
- ✅ Fast execution (seconds)
- ✅ No AWS credentials needed
- ✅ Perfect for development
- ✅ Validates local runner behavior

```bash
# Run all example tests locally (default, from repo root)
hatch run dev-examples:test

# Run with explicit mode flag
pytest --runner-mode=local -m example packages/aws-durable-execution-sdk-python-examples/test/

# Run specific test
pytest --runner-mode=local -k test_hello_world packages/aws-durable-execution-sdk-python-examples/test/
```

### Cloud Mode (Integration)
Tests run against actual AWS Lambda functions using `DurableFunctionCloudTestRunner`:
- ✅ Validates cloud deployment
- ✅ Tests real Lambda execution
- ✅ Verifies end-to-end behavior
- ⚠️ Requires deployed functions

```bash
# Build and deploy the example stack first (from repo root)
hatch run examples:build
hatch run examples:generate-sam-template
sam build --template-file packages/aws-durable-execution-sdk-python-examples/template.generated.json
sam deploy \
  --template-file .aws-sam/build/template.yaml \
  --stack-name python-examples-test \
  --resolve-s3 \
  --no-confirm-changeset \
  --parameter-overrides \
    PythonRuntime=python3.13 \
    FunctionNamePrefix=PythonTest- \
    LambdaEndpoint=https://lambda.us-west-2.amazonaws.com \
    LambdaExecutionRoleArn=arn:aws:iam::123456789012:role/example-lambda-role

# Optional: invoke a deployed example directly with SAM remote execution.
# The logical ID comes from template.generated.json; hello_world.handler maps to HelloWorld.
sam remote invoke \
  --stack-name python-examples-test \
  HelloWorld \
  --event '{"name":"Ada"}' \
  --parameter 'Qualifier=$LATEST' \
  --output json

# Optional: save a reusable remote test event and invoke with it.
sam remote test-event put \
  --stack-name python-examples-test \
  HelloWorld \
  --name hello-world \
  --file event.json \
  --force
sam remote invoke \
  --stack-name python-examples-test \
  HelloWorld \
  --test-event-name hello-world \
  --parameter 'Qualifier=$LATEST'

# Optional: inspect or stop a durable execution returned by an invocation.
sam remote execution get "$DURABLE_EXECUTION_ARN" --format json
sam remote execution history "$DURABLE_EXECUTION_ARN" --format table
sam remote execution stop "$DURABLE_EXECUTION_ARN" \
  --error-type UserCancellation \
  --error-message "Stopped during manual troubleshooting"

# Set environment variables for cloud testing
export AWS_REGION=us-west-2
export LAMBDA_ENDPOINT=https://lambda.us-west-2.amazonaws.com
export PYTEST_FUNCTION_NAME_MAP='{"Hello World":"PythonTest-HelloWorld:$LATEST"}'

# Run tests (from repo root)
pytest --runner-mode=cloud -k test_hello_world packages/aws-durable-execution-sdk-python-examples/test/

# Or using hatch (from repo root)
hatch run test:examples-integration -k test_hello_world
```

See the [SAM remote execution command reference](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/sam-cli-command-reference-sam-remote-execution.html) for more `sam remote invoke`, `sam remote execution`, and `sam remote test-event` options.

## Writing Tests

Use the `durable_runner` pytest fixture with the `@pytest.mark.durable_execution` marker:

```python
import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from examples.src import my_example


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=my_example.handler,
    lambda_function_name="my example",
)
def test_my_example(durable_runner):
    """Test my example in both local and cloud modes."""
    with durable_runner:
        result = durable_runner.run(input={"test": "data"}, timeout=10)
    
    # Assertions work in both modes
    assert result.status == InvocationStatus.SUCCEEDED
    assert result.result == "expected output"
    
    # Optional mode-specific validations
    if durable_runner.mode == "cloud":
        # Cloud-specific assertions
        pass
```

## Configuration

### Environment Variables (Cloud Mode)
- `AWS_REGION` - AWS region for Lambda invocation (default: us-west-2)
- `LAMBDA_ENDPOINT` - Optional Lambda endpoint URL for testing
- `PYTEST_FUNCTION_NAME_MAP` - JSON mapping of example names to qualified function names (required for cloud mode)

### CLI Options
- `--runner-mode` - Test mode: `local` (default) or `cloud`

### Pytest Markers
- `-m example` - Run only example tests
- `-k test_name` - Run tests matching pattern

## CI/CD Integration

Tests automatically run in CI/CD after deployment:

1. `cloud-tests.yml` deploys functions
2. Integration tests run against deployed functions
3. Results reported in GitHub Actions

See `.github/workflows/cloud-tests.yml` for details.

## Troubleshooting

### Timeout errors
**Problem**: `TimeoutError: Execution did not complete within 60s`

**Solution**: Increase timeout in test:
```python
result = runner.run(input="test", timeout=120)  # Increase to 120s
```

### Import errors
**Problem**: `ModuleNotFoundError: No module named 'aws_durable_execution_sdk_python_testing'`

**Solution**: Install dependencies:
```bash
hatch run dev-examples:test  # Installs dependencies automatically
