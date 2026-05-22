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
# Run all example tests locally (default)
hatch run test:examples

# Run with explicit mode flag
pytest --runner-mode=local -m example examples/test/

# Run specific test
pytest --runner-mode=local -k test_hello_world examples/test/
```

### Cloud Mode (Integration)
Tests run against actual AWS Lambda functions using `DurableFunctionCloudTestRunner`:
- ✅ Validates cloud deployment
- ✅ Tests real Lambda execution
- ✅ Verifies end-to-end behavior
- ⚠️ Requires deployed functions

```bash
# Deploy function first
hatch run examples:deploy "hello world" --function-name HelloWorld-Test

# Set environment variables for cloud testing
export AWS_REGION=us-west-2
export LAMBDA_ENDPOINT=https://lambda.us-west-2.amazonaws.com
export QUALIFIED_FUNCTION_NAME="HelloWorld-Test:\$LATEST"
export LAMBDA_FUNCTION_TEST_NAME="hello world"

# Run tests
pytest --runner-mode=cloud -k test_hello_world examples/test/

# Or using hatch
hatch run test:examples-integration -k test_hello_world
```

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
- `QUALIFIED_FUNCTION_NAME` - Deployed Lambda function ARN or qualified name (required for cloud mode)
- `LAMBDA_FUNCTION_TEST_NAME` - Lambda function name to match with test's `lambda_function_name` marker (required for cloud mode)

### CLI Options
- `--runner-mode` - Test mode: `local` (default) or `cloud`

### Pytest Markers
- `-m example` - Run only example tests
- `-k test_name` - Run tests matching pattern

## CI/CD Integration

Tests automatically run in CI/CD after deployment:

1. `deploy-examples.yml` deploys functions
2. Integration tests run against deployed functions
3. Results reported in GitHub Actions

See `.github/workflows/deploy-examples.yml` for details.

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
hatch run test:examples  # Installs dependencies automatically
