# Testing Modes: Local vs Cloud

## Table of Contents

- [Overview](#overview)
- [Terminology](#terminology)
- [Key features](#key-features)
- [Getting started](#getting-started)
- [Configuration](#configuration)
- [Deployment workflow](#deployment-workflow)
- [Running tests in different modes](#running-tests-in-different-modes)
- [Local vs cloud modes](#local-vs-cloud-modes)
- [Best practices](#best-practices)
- [Troubleshooting](#troubleshooting)
- [See also](#see-also)

[← Back to main index](../index.md)

## Overview

The [AWS Durable Execution SDK Testing Framework](https://github.com/aws/aws-durable-execution-sdk-python-testing) (`aws-durable-execution-sdk-python-testing`) supports two execution modes: local and cloud. Local mode runs tests in-memory for fast development, while cloud mode runs tests against actual AWS Lambda functions for integration validation.

**Local mode** uses `DurableFunctionTestRunner` to execute your function in-memory without AWS deployment. It's fast, requires no credentials, and perfect for development.

**Cloud mode** uses `DurableFunctionCloudTestRunner` to invoke deployed Lambda functions and poll for completion. It validates your function's behavior in a real AWS environment, including Lambda runtime behavior, IAM permissions, and service integrations.

[↑ Back to top](#table-of-contents)

## Terminology

**Cloud mode** - Test execution mode that runs tests against deployed Lambda functions in AWS.

**Local mode** - Test execution mode that runs tests in-memory without AWS deployment (default).

**DurableFunctionCloudTestRunner** - Test runner class that executes durable functions against AWS Lambda backend.

**Qualified function name** - Lambda function identifier including version or alias (e.g., `MyFunction:$LATEST`).

**Polling** - The process of repeatedly checking execution status until completion.

[↑ Back to top](#table-of-contents)

## Key features

- **Real AWS environment** - Tests run against actual Lambda functions
- **End-to-end validation** - Verifies deployment, IAM permissions, and service integrations
- **Same test interface** - Tests work in both local and cloud modes without changes
- **Automatic polling** - Waits for execution completion automatically
- **Execution history** - Retrieves full execution history for assertions

[↑ Back to top](#table-of-contents)

## Getting started

Here's a simple example of running tests in cloud mode:

```python
import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from examples.src import hello_world


@pytest.mark.durable_execution(
    handler=hello_world.handler,
    lambda_function_name="hello world",
)
def test_hello_world(durable_runner):
    """Test hello world in both local and cloud modes."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)
    
    assert result.status == InvocationStatus.SUCCEEDED
    assert result.result == "Hello World!"
```

Run the test in cloud mode:

```console
# Set environment variables
export AWS_REGION=us-west-2
export QUALIFIED_FUNCTION_NAME="HelloWorld:$LATEST"
export LAMBDA_FUNCTION_TEST_NAME="hello world"

# Run test
pytest --runner-mode=cloud -k test_hello_world
```

[↑ Back to top](#table-of-contents)

## Configuration

### Environment Variables

Cloud mode requires these environment variables:

**Required:**

- `QUALIFIED_FUNCTION_NAME` - The deployed Lambda function ARN or qualified name
  - Example: `MyFunction:$LATEST`
  - Example: `arn:aws:lambda:us-west-2:123456789012:function:MyFunction:$LATEST`

- `LAMBDA_FUNCTION_TEST_NAME` - The function name to match against test markers
  - Example: `hello world`
  - Must match the `lambda_function_name` parameter in `@pytest.mark.durable_execution`

**Optional:**

- `AWS_REGION` - AWS region for Lambda invocation (default: `us-west-2`)
  - Example: `us-east-1`

- `LAMBDA_ENDPOINT` - Custom Lambda endpoint URL for testing
  - Example: `https://lambda.us-west-2.amazonaws.com`
  - Useful for testing against local Lambda emulators

### CLI Options

- `--runner-mode` - Test execution mode
  - `local` (default) - Run tests in-memory
  - `cloud` - Run tests against deployed Lambda functions

### Test Markers

Use the `@pytest.mark.durable_execution` marker to configure tests:

```python
@pytest.mark.durable_execution(
    handler=my_function.handler,           # Required for local mode
    lambda_function_name="my function",    # Required for cloud mode
)
def test_my_function(durable_runner):
    # Test code here
    pass
```

**Parameters:**

- `handler` - The durable function handler (required for local mode)
- `lambda_function_name` - The function name for cloud mode matching (required for cloud mode)

[↑ Back to top](#table-of-contents)

## Deployment workflow

Follow these steps to deploy and test your durable functions in the cloud:

### 1. Deploy your function

Deploy your Lambda function to AWS using your preferred deployment tool (SAM, CDK, Terraform, etc.):

```console
# Example using SAM
sam build
sam deploy --stack-name my-durable-function
```

### 2. Get the function ARN

After deployment, get the qualified function name or ARN:

```console
# Get function ARN
aws lambda get-function --function-name MyFunction --query 'Configuration.FunctionArn'
```

### 3. Set environment variables

Configure the environment for cloud testing:

```console
export AWS_REGION=us-west-2
export QUALIFIED_FUNCTION_NAME="MyFunction:$LATEST"
export LAMBDA_FUNCTION_TEST_NAME="my function"
```

### 4. Run tests

Execute your tests in cloud mode:

```console
pytest --runner-mode=cloud -k test_my_function
```

[↑ Back to top](#table-of-contents)

## Running tests in different modes

### Run all tests in local mode (default)

```console
pytest examples/test/
```

### Run all tests in cloud mode

```console
pytest --runner-mode=cloud examples/test/
```

### Run specific test in cloud mode

```console
pytest --runner-mode=cloud -k test_hello_world examples/test/
```

### Run with custom timeout

Increase the timeout for long-running functions:

```python
def test_long_running(durable_runner):
    with durable_runner:
        result = durable_runner.run(input="test", timeout=300)  # 5 minutes
    
    assert result.status == InvocationStatus.SUCCEEDED
```

### Mode-specific assertions

Check the runner mode in your tests:

```python
def test_with_mode_check(durable_runner):
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)
    
    assert result.status == InvocationStatus.SUCCEEDED
    
    # Cloud-specific validation
    if durable_runner.mode == "cloud":
        # Additional assertions for cloud environment
        pass
```

[↑ Back to top](#table-of-contents)

## Local vs cloud modes

### Comparison

| Feature | Local Mode | Cloud Mode |
|---------|-----------|------------|
| **Execution** | In-memory | AWS Lambda |
| **Speed** | Fast (seconds) | Slower (network latency) |
| **AWS credentials** | Not required | Required |
| **Deployment** | Not required | Required |
| **IAM permissions** | Not validated | Validated |
| **Service integrations** | Mocked | Real |
| **Cost** | Free | Lambda invocation costs |
| **Use case** | Development, unit tests | Integration tests, validation |

### When to use local mode

Use local mode for:
- **Development** - Fast iteration during development
- **Unit tests** - Testing function logic without AWS dependencies
- **CI/CD** - Fast feedback in pull request checks
- **Debugging** - Easy debugging with local tools

### When to use cloud mode

Use cloud mode for:
- **Integration testing** - Validate real AWS service integrations
- **Deployment validation** - Verify deployed functions work correctly
- **IAM testing** - Ensure permissions are configured correctly
- **End-to-end testing** - Test complete workflows in production-like environment

### Writing mode-agnostic tests

Write tests that work in both modes:

```python
@pytest.mark.durable_execution(
    handler=my_function.handler,
    lambda_function_name="my function",
)
def test_my_function(durable_runner):
    """Test works in both local and cloud modes."""
    with durable_runner:
        result = durable_runner.run(input={"value": 42}, timeout=10)
    
    # These assertions work in both modes
    assert result.status == InvocationStatus.SUCCEEDED
    assert result.result == "expected output"
```

[↑ Back to top](#table-of-contents)

## Best practices

### Use local mode for development

Run tests locally during development for fast feedback:

```console
# Fast local testing
pytest examples/test/
```

### Use cloud mode for validation

Run cloud tests before merging or deploying:

```console
# Validate deployment
pytest --runner-mode=cloud examples/test/
```

### Set appropriate timeouts

Cloud tests need longer timeouts due to network latency:

```python
# Local mode: short timeout
result = runner.run(input="test", timeout=10)

# Cloud mode: longer timeout
result = runner.run(input="test", timeout=60)
```

### Use environment-specific configuration

Configure different settings for different environments:

```console
# Development
export AWS_REGION=us-west-2
export QUALIFIED_FUNCTION_NAME="MyFunction-Dev:$LATEST"

# Production
export AWS_REGION=us-east-1
export QUALIFIED_FUNCTION_NAME="MyFunction-Prod:$LATEST"
```

### Test one function at a time

When running cloud tests, test one function at a time to avoid confusion:

```console
# Test specific function
export LAMBDA_FUNCTION_TEST_NAME="hello world"
pytest --runner-mode=cloud -k test_hello_world
```

### Use CI/CD for automated cloud testing

Integrate cloud testing into your CI/CD pipeline:

```yaml
# Example GitHub Actions workflow
- name: Deploy function
  run: sam deploy --stack-name test-stack

- name: Run cloud tests
  env:
    AWS_REGION: us-west-2
    QUALIFIED_FUNCTION_NAME: ${{ steps.deploy.outputs.function_arn }}
    LAMBDA_FUNCTION_TEST_NAME: "hello world"
  run: pytest --runner-mode=cloud -k test_hello_world
```

[↑ Back to top](#table-of-contents)

## Troubleshooting

### TimeoutError: Execution did not complete

**Problem:** Test times out waiting for execution to complete.

**Cause:** The function takes longer than the timeout value, or the function is stuck.

**Solution:** Increase the timeout parameter:

```python
# Increase timeout to 120 seconds
result = runner.run(input="test", timeout=120)
```

Check the Lambda function logs to see if it's actually running:

```console
aws logs tail /aws/lambda/MyFunction --follow
```

### Environment variables not set

**Problem:** `Cloud mode requires both QUALIFIED_FUNCTION_NAME and LAMBDA_FUNCTION_TEST_NAME environment variables`

**Cause:** Required environment variables are missing.

**Solution:** Set both required environment variables:

```console
export QUALIFIED_FUNCTION_NAME="MyFunction:$LATEST"
export LAMBDA_FUNCTION_TEST_NAME="hello world"
```

### Test skipped: doesn't match LAMBDA_FUNCTION_TEST_NAME

**Problem:** Test is skipped with message about function name mismatch.

**Cause:** The test's `lambda_function_name` doesn't match `LAMBDA_FUNCTION_TEST_NAME`.

**Solution:** Either:
1. Update `LAMBDA_FUNCTION_TEST_NAME` to match the test:
   ```console
   export LAMBDA_FUNCTION_TEST_NAME="my function"
   ```

2. Or run only the matching test:
   ```console
   pytest --runner-mode=cloud -k test_hello_world
   ```

### Failed to invoke Lambda function

**Problem:** `Failed to invoke Lambda function MyFunction: ...`

**Cause:** AWS credentials are invalid, function doesn't exist, or IAM permissions are missing.

**Solution:** 

1. Verify AWS credentials:
   ```console
   aws sts get-caller-identity
   ```

2. Verify function exists:
   ```console
   aws lambda get-function --function-name MyFunction
   ```

3. Check IAM permissions - you need `lambda:InvokeFunction` permission:
   ```json
   {
     "Effect": "Allow",
     "Action": "lambda:InvokeFunction",
     "Resource": "arn:aws:lambda:*:*:function:*"
   }
   ```

### No DurableExecutionArn in response

**Problem:** `No DurableExecutionArn in response for function MyFunction`

**Cause:** The Lambda function is not a durable function or doesn't have durable execution enabled.

**Solution:** Ensure your function is decorated with `@durable_execution`:

```python
from aws_durable_execution_sdk_python import durable_execution, DurableContext

@durable_execution
def handler(event: dict, context: DurableContext):
    # Your durable function code
    pass
```

### Lambda function failed

**Problem:** `Lambda function failed: ...`

**Cause:** The function threw an unhandled exception.

**Solution:** Check the Lambda function logs:

```console
aws logs tail /aws/lambda/MyFunction --follow
```

Fix the error in your function code and redeploy.

### Failed to get execution status

**Problem:** `Failed to get execution status: ...`

**Cause:** The Lambda service API call failed.

**Solution:** 

1. Check AWS service health
2. Verify your AWS credentials have the required permissions
3. Check if you're using the correct region:
   ```console
   export AWS_REGION=us-west-2
   ```

[↑ Back to top](#table-of-contents)

## See also

- [Getting Started](../getting-started.md) - Set up your development environment
- [Testing patterns](../testing-patterns/basic-tests.md) - Practical pytest examples
- [Examples README](../../examples/test/README.md) - More examples and configuration details

[↑ Back to top](#table-of-contents)
