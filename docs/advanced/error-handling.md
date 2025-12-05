# Error Handling

## Table of Contents

- [Overview](#overview)
- [Terminology](#terminology)
- [Getting started](#getting-started)
- [Exception types](#exception-types)
- [Retry strategies](#retry-strategies)
- [Error response formats](#error-response-formats)
- [Common error scenarios](#common-error-scenarios)
- [Troubleshooting](#troubleshooting)
- [Best practices](#best-practices)
- [FAQ](#faq)
- [Testing](#testing)
- [See also](#see-also)

[← Back to main index](../index.md)

## Overview

Error handling in durable functions determines how your code responds to failures. The SDK provides typed exceptions, automatic retry with exponential backoff, and AWS-compliant error responses that help you build resilient workflows.

When errors occur, the SDK can:
- Retry transient failures automatically with configurable backoff
- Checkpoint failures with detailed error information
- Distinguish between recoverable and unrecoverable errors
- Provide clear termination reasons and stack traces for debugging

[↑ Back to top](#table-of-contents)

## Terminology

**Exception** - A Python error that interrupts normal execution flow. The SDK provides specific exception types for different failure scenarios.

**Retry strategy** - A function that determines whether to retry an operation after an exception and how long to wait before retrying.

**Termination reason** - A code indicating why a durable execution terminated, such as `UNHANDLED_ERROR` or `INVOCATION_ERROR`.

**Recoverable error** - An error that can be retried, such as transient network failures or rate limiting.

**Unrecoverable error** - An error that terminates execution immediately without retry, such as validation errors or non-deterministic execution.

**Backoff** - The delay between retry attempts, typically increasing exponentially to avoid overwhelming failing services.

[↑ Back to top](#table-of-contents)

## Getting started

Here's a simple example of handling errors in a durable function:

```python
from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
    durable_step,
    StepContext,
)

@durable_step
def process_order(step_context: StepContext, order_id: str) -> dict:
    """Process an order with validation."""
    if not order_id:
        raise ValueError("Order ID is required")
    
    # Process the order
    return {"order_id": order_id, "status": "processed"}

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    """Handle order processing with error handling."""
    try:
        order_id = event.get("order_id")
        result = context.step(process_order(order_id))
        return result
    except ValueError as e:
        # Handle validation errors from your code
        return {"error": "InvalidInput", "message": str(e)}
```

When this function runs:
1. If `order_id` is missing, `ValueError` is raised from your code
2. The exception is caught and handled gracefully
3. A structured error response is returned to the caller

[↑ Back to top](#table-of-contents)

## Exception types

The SDK provides several exception types for different failure scenarios.

### Exception summary

| Exception | Retryable | Behavior | Use case |
|-----------|-----------|----------|----------|
| `ValidationError` | No | Fails immediately | SDK detects invalid arguments |
| `ExecutionError` | No | Returns FAILED status | Permanent business logic failures |
| `InvocationError` | Yes (by Lambda) | Lambda retries invocation | Transient infrastructure issues |
| `CallbackError` | No | Returns FAILED status | Callback handling failures |
| `StepInterruptedError` | Yes (automatic) | Retries on next invocation | Step interrupted before checkpoint |
| `CheckpointError` | Depends | Retries if 4xx (except invalid token) | Failed to save execution state |
| `SerDesError` | No | Returns FAILED status | Serialization failures |

### Base exceptions

**DurableExecutionsError** - Base class for all SDK exceptions.

```python
from aws_durable_execution_sdk_python import DurableExecutionsError

try:
    # Your code here
    pass
except DurableExecutionsError as e:
    # Handle any SDK exception
    print(f"SDK error: {e}")
```

**UnrecoverableError** - Base class for errors that terminate execution. These errors include a `termination_reason` attribute.

```python
from aws_durable_execution_sdk_python import (
    ExecutionError,
    InvocationError,
)

try:
    # Your code here
    pass
except (ExecutionError, InvocationError) as e:
    # Access termination reason from unrecoverable errors
    print(f"Execution terminated: {e.termination_reason}")
```

### Validation errors

**ValidationError** - Raised by the SDK when you pass invalid arguments to SDK operations.

```python
from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
    ValidationError,
)
from aws_durable_execution_sdk_python.config import CallbackConfig

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    """Handle SDK validation errors."""
    try:
        # SDK raises ValidationError if timeout is invalid
        callback = context.create_callback(
            config=CallbackConfig(timeout_seconds=-1),  # Invalid!
            name="approval"
        )
        return {"callback_id": callback}
    except ValidationError as e:
        # SDK caught invalid configuration
        return {"error": "InvalidConfiguration", "message": str(e)}
```

The SDK raises `ValidationError` when:
- Operation arguments are invalid (negative timeouts, empty names)
- Required parameters are missing
- Configuration values are out of range

### Execution errors

**ExecutionError** - Raised when execution fails in a way that shouldn't be retried. Returns `FAILED` status without retry.

```python
from aws_durable_execution_sdk_python import ExecutionError

@durable_step
def process_data(step_context: StepContext, data: dict) -> dict:
    """Process data with business logic validation."""
    if not data.get("required_field"):
        raise ExecutionError("Required field missing")
    return {"processed": True}
```

Use `ExecutionError` for:
- Business logic failures
- Invalid data that won't be fixed by retry
- Permanent failures that should fail fast

### Invocation errors

**InvocationError** - Raised when Lambda should retry the entire invocation. Causes Lambda to retry by throwing from the handler.

```python
from aws_durable_execution_sdk_python import InvocationError

@durable_step
def call_external_service(step_context: StepContext) -> dict:
    """Call external service with retry."""
    try:
        # Call external service
        response = make_api_call()
        return response
    except ConnectionError:
        # Trigger Lambda retry
        raise InvocationError("Service unavailable")
```

Use `InvocationError` for:
- Service unavailability
- Network failures
- Transient infrastructure issues

### Callback errors

**CallbackError** - Raised when callback handling fails.

```python
from aws_durable_execution_sdk_python import CallbackError
from aws_durable_execution_sdk_python.config import CallbackConfig

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    """Handle callback with error handling."""
    try:
        callback = context.create_callback(
            config=CallbackConfig(timeout_seconds=3600),
            name="approval"
        )
        context.wait_for_callback(callback)
        return {"status": "approved"}
    except CallbackError as e:
        return {"error": "CallbackError", "callback_id": e.callback_id}
```

### Step interrupted errors

**StepInterruptedError** - Raised when a step is interrupted before checkpointing.

```python
from aws_durable_execution_sdk_python import StepInterruptedError

# This can happen if Lambda times out during step execution
@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    try:
        result = context.step(long_running_operation())
        return result
    except StepInterruptedError as e:
        # Step was interrupted, will retry on next invocation
        context.logger.warning(f"Step interrupted: {e.step_id}")
        raise
```

### Serialization errors

**SerDesError** - Raised when serialization or deserialization fails.

```python
from aws_durable_execution_sdk_python import SerDesError

@durable_step
def process_complex_data(step_context: StepContext, data: object) -> dict:
    """Process data that might not be serializable."""
    try:
        # Process data
        return {"result": data}
    except SerDesError as e:
        # Handle serialization failure
        return {"error": "Cannot serialize result"}
```

[↑ Back to top](#table-of-contents)

## Retry strategies

Configure retry behavior for steps using retry strategies.

### Creating retry strategies

Use `RetryStrategyConfig` to define retry behavior:

```python
from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
    durable_step,
    StepContext,
)
from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.retries import (
    RetryStrategyConfig,
    create_retry_strategy,
)

@durable_step
def unreliable_operation(step_context: StepContext) -> str:
    """Operation that might fail."""
    # Your code here
    return "success"

@durable_execution
def handler(event: dict, context: DurableContext) -> str:
    # Configure retry strategy
    retry_config = RetryStrategyConfig(
        max_attempts=3,
        initial_delay_seconds=1,
        max_delay_seconds=10,
        backoff_rate=2.0,
        retryable_error_types=[RuntimeError, ConnectionError],
    )
    
    # Create step config with retry
    step_config = StepConfig(
        retry_strategy=create_retry_strategy(retry_config)
    )
    
    # Execute with retry
    result = context.step(unreliable_operation(), config=step_config)
    return result
```

### RetryStrategyConfig parameters

**max_attempts** - Maximum number of attempts (including the initial attempt). Default: 3.

**initial_delay_seconds** - Initial delay before first retry in seconds. Default: 5.

**max_delay_seconds** - Maximum delay between retries in seconds. Default: 300 (5 minutes).

**backoff_rate** - Multiplier for exponential backoff. Default: 2.0.

**jitter_strategy** - Jitter strategy to add randomness to delays. Default: `JitterStrategy.FULL`.

**retryable_errors** - List of error message patterns to retry (strings or regex patterns). Default: matches all errors.

**retryable_error_types** - List of exception types to retry. Default: empty (retry all).

### Retry presets

The SDK provides preset retry strategies for common scenarios:

```python
from aws_durable_execution_sdk_python.retries import RetryPresets
from aws_durable_execution_sdk_python.config import StepConfig

# No retries
step_config = StepConfig(retry_strategy=RetryPresets.none())

# Default retries (6 attempts, 5s initial delay)
step_config = StepConfig(retry_strategy=RetryPresets.default())

# Quick retries for transient errors (3 attempts)
step_config = StepConfig(retry_strategy=RetryPresets.transient())

# Longer retries for resource availability (5 attempts, up to 5 minutes)
step_config = StepConfig(retry_strategy=RetryPresets.resource_availability())

# Aggressive retries for critical operations (10 attempts)
step_config = StepConfig(retry_strategy=RetryPresets.critical())
```

### Retrying specific exceptions

Only retry certain exception types:

```python
from random import random
from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
    durable_step,
    StepContext,
)
from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.retries import (
    RetryStrategyConfig,
    create_retry_strategy,
)

@durable_step
def call_api(step_context: StepContext) -> dict:
    """Call external API that might fail."""
    if random() > 0.5:
        raise ConnectionError("Network timeout")
    return {"status": "success"}

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    # Only retry ConnectionError, not other exceptions
    retry_config = RetryStrategyConfig(
        max_attempts=3,
        retryable_error_types=[ConnectionError],
    )
    
    result = context.step(
        call_api(),
        config=StepConfig(create_retry_strategy(retry_config)),
    )
    
    return result
```

### Exponential backoff

Configure exponential backoff to avoid overwhelming failing services:

```python
retry_config = RetryStrategyConfig(
    max_attempts=5,
    initial_delay_seconds=1,    # First retry after 1 second
    max_delay_seconds=60,        # Cap at 60 seconds
    backoff_rate=2.0,            # Double delay each time: 1s, 2s, 4s, 8s, 16s...
)
```

With this configuration:
- Attempt 1: Immediate
- Attempt 2: After 1 second
- Attempt 3: After 2 seconds
- Attempt 4: After 4 seconds
- Attempt 5: After 8 seconds

[↑ Back to top](#table-of-contents)

## Error response formats

The SDK follows AWS service conventions for error responses.

### Error response structure

When a durable function fails, the response includes:

```json
{
  "errorType": "ExecutionError",
  "errorMessage": "Order validation failed",
  "termination_reason": "EXECUTION_ERROR",
  "stackTrace": [
    "  File \"/var/task/handler.py\", line 42, in process_order",
    "    raise ExecutionError(\"Order validation failed\")"
  ]
}
```

### Termination reasons

**UNHANDLED_ERROR** - An unhandled exception occurred in user code.

**INVOCATION_ERROR** - Lambda should retry the invocation.

**EXECUTION_ERROR** - Execution failed and shouldn't be retried.

**CHECKPOINT_FAILED** - Failed to checkpoint execution state.

**NON_DETERMINISTIC_EXECUTION** - Execution produced different results on replay.

**STEP_INTERRUPTED** - A step was interrupted before completing.

**CALLBACK_ERROR** - Callback handling failed.

**SERIALIZATION_ERROR** - Failed to serialize or deserialize data.

### HTTP status codes

When calling durable functions via API Gateway or Lambda URLs:

- **200 OK** - Execution succeeded
- **400 Bad Request** - Validation error or invalid input
- **500 Internal Server Error** - Execution error or unhandled exception
- **503 Service Unavailable** - Invocation error (Lambda will retry)

[↑ Back to top](#table-of-contents)

## Common error scenarios

### Handling input validation

Validate input early and return clear error messages:

```python
from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
)

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    """Validate input and handle errors."""
    # Validate required fields
    if not event.get("user_id"):
        return {"error": "InvalidInput", "message": "user_id is required"}
    
    if not event.get("action"):
        return {"error": "InvalidInput", "message": "action is required"}
    
    # Process valid input
    user_id = event["user_id"]
    action = event["action"]
    
    result = context.step(
        lambda _: {"user_id": user_id, "action": action, "status": "completed"},
        name="process_action"
    )
    
    return result
```

### Handling transient failures

Retry transient failures automatically:

```python
from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
    durable_step,
    StepContext,
)
from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.retries import RetryPresets

@durable_step
def call_external_api(step_context: StepContext, endpoint: str) -> dict:
    """Call external API with retry."""
    # API call that might fail transiently
    response = make_http_request(endpoint)
    return response

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    """Handle API calls with automatic retry."""
    # Use transient preset for quick retries
    step_config = StepConfig(retry_strategy=RetryPresets.transient())
    
    try:
        result = context.step(
            call_external_api(event["endpoint"]),
            config=step_config,
        )
        return {"status": "success", "data": result}
    except Exception as e:
        # All retries exhausted
        return {"status": "failed", "error": str(e)}
```

### Handling permanent failures

Fail fast for permanent errors:

```python
from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
    durable_step,
    ExecutionError,
    StepContext,
)

@durable_step
def process_payment(step_context: StepContext, amount: float, card: str) -> dict:
    """Process payment with validation."""
    # Validate card
    if not is_valid_card(card):
        # Don't retry invalid cards
        raise ExecutionError("Invalid card number")
    
    # Process payment
    return {"transaction_id": "txn_123", "amount": amount}

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    """Handle payment with error handling."""
    try:
        result = context.step(
            process_payment(event["amount"], event["card"])
        )
        return {"status": "success", "transaction": result}
    except ExecutionError as e:
        # Permanent failure, don't retry
        return {"status": "failed", "error": str(e)}
```

### Handling multiple error types

Handle different error types appropriately:

```python
from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
    durable_step,
    ExecutionError,
    InvocationError,
    ValidationError,
    StepContext,
)

@durable_step
def complex_operation(step_context: StepContext, data: dict) -> dict:
    """Operation with multiple failure modes."""
    # Validate input
    if not data:
        raise ValueError("Data is required")
    
    # Check business rules
    if data.get("amount", 0) < 0:
        raise ExecutionError("Amount must be positive")
    
    # Call external service
    try:
        result = call_external_service(data)
        return result
    except ConnectionError:
        # Transient failure
        raise InvocationError("Service unavailable")

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    """Handle multiple error types."""
    try:
        result = context.step(complex_operation(event))
        return {"status": "success", "result": result}
    except ValueError as e:
        return {"status": "invalid", "error": str(e)}
    except ExecutionError as e:
        return {"status": "failed", "error": str(e)}
    except InvocationError as e:
        # Let Lambda retry
        raise
```

[↑ Back to top](#table-of-contents)

## Troubleshooting

### Step retries exhausted

**Problem:** Your step fails after exhausting all retry attempts.

**Cause:** The operation continues to fail, or the error isn't retryable.

**Solution:** Check your retry configuration and error types:

```python
# Ensure you're retrying the right errors
retry_config = RetryStrategyConfig(
    max_attempts=5,  # Increase attempts
    retryable_error_types=[ConnectionError, TimeoutError],  # Add error types
)
```

### Checkpoint failed errors

**Problem:** Execution fails with `CheckpointError`.

**Cause:** Failed to save execution state, possibly due to payload size limits or service issues.

**Solution:** Reduce checkpoint payload size or check service health:

```python
# Reduce payload size by returning only necessary data
@durable_step
def large_operation(step_context: StepContext) -> dict:
    # Process large data
    large_result = process_data()
    
    # Return only summary, not full data
    return {"summary": large_result["summary"], "count": len(large_result["items"])}
```

### Callback timeout

**Problem:** Callback times out before receiving a response.

**Cause:** External system didn't respond within the timeout period.

**Solution:** Increase callback timeout or implement retry logic:

```python
from aws_durable_execution_sdk_python.config import CallbackConfig

# Increase timeout
callback = context.create_callback(
    config=CallbackConfig(
        timeout_seconds=7200,  # 2 hours
        heartbeat_timeout_seconds=300,  # 5 minutes
    ),
    name="long_running_approval"
)
```

### Step interrupted errors

**Problem:** Steps are interrupted before completing.

**Cause:** Lambda timeout or memory limit reached during step execution.

**Solution:** Increase Lambda timeout or break large steps into smaller ones:

```python
# Break large operation into smaller steps
@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    # Process in chunks instead of all at once
    items = event["items"]
    chunk_size = 100
    
    results = []
    for i in range(0, len(items), chunk_size):
        chunk = items[i:i + chunk_size]
        result = context.step(
            lambda _, c=chunk: process_chunk(c),
            name=f"process_chunk_{i}"
        )
        results.extend(result)
    
    return {"processed": len(results)}
```

[↑ Back to top](#table-of-contents)

## Best practices

**Validate input early** - Check for invalid input at the start of your function and return clear error responses or raise appropriate exceptions like `ValueError`.

**Use appropriate exception types** - Choose the right exception type for each failure scenario. Use `ExecutionError` for permanent failures and `InvocationError` for transient issues.

**Configure retry for transient failures** - Use retry strategies for operations that might fail temporarily, such as network calls or rate limits.

**Fail fast for permanent errors** - Don't retry errors that won't be fixed by retrying, such as validation failures or business logic errors.

**Wrap non-deterministic code in steps** - All code that produces different results on replay must be wrapped in steps, including random values, timestamps, and external API calls.

**Handle errors explicitly** - Catch and handle exceptions in your code. Provide meaningful error messages to callers.

**Log errors with context** - Use `context.logger` to log errors with execution context for debugging.

**Keep error messages clear** - Write error messages that help users understand what went wrong and how to fix it.

**Test error scenarios** - Write tests for both success and failure cases to ensure your error handling works correctly.

**Monitor error rates** - Track error rates and termination reasons to identify issues in production.

[↑ Back to top](#table-of-contents)

## FAQ

**Q: What's the difference between ExecutionError and InvocationError?**

A: `ExecutionError` fails the execution without retry (returns FAILED status). `InvocationError` triggers Lambda to retry the entire invocation. Use `ExecutionError` for permanent failures and `InvocationError` for transient issues.

**Q: How do I retry only specific exceptions?**

A: Use `retryable_error_types` in `RetryStrategyConfig`:

```python
retry_config = RetryStrategyConfig(
    max_attempts=3,
    retryable_error_types=[ConnectionError, TimeoutError],
)
```

**Q: Can I customize the backoff strategy?**

A: Yes, configure `initial_delay_seconds`, `max_delay_seconds`, `backoff_rate`, and `jitter_strategy` in `RetryStrategyConfig`.

**Q: What happens when retries are exhausted?**

A: The step checkpoints the error and the exception propagates to your handler. You can catch and handle it there.

**Q: How do I prevent duplicate operations on retry?**

A: Use at-most-once semantics for operations with side effects:

```python
from aws_durable_execution_sdk_python.config import StepConfig, StepSemantics

step_config = StepConfig(
    step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY
)
```

**Q: Can I access error details in my code?**

A: Yes, catch the exception and access its attributes:

```python
try:
    result = context.step(operation())
except CallbackError as e:
    print(f"Callback failed: {e.callback_id}")
except NonDeterministicExecutionError as e:
    print(f"Non-deterministic step: {e.step_id}")
```

**Q: How do I handle errors in parallel operations?**

A: Wrap each parallel operation in a try-except block or let errors propagate to fail the entire execution:

```python
results = []
for item in items:
    try:
        result = context.step(lambda _, i=item: process(i), name=f"process_{item}")
        results.append(result)
    except Exception as e:
        results.append({"error": str(e)})
```

**Q: What's the maximum number of retry attempts?**

A: You can configure any number of attempts, but consider Lambda timeout limits. The default is 6 attempts.

[↑ Back to top](#table-of-contents)

## Testing

You can test error handling using the testing SDK. The test runner executes your function and lets you inspect errors.

### Testing successful execution

```python
import pytest
from aws_durable_execution_sdk_python_testing import InvocationStatus
from my_function import handler

@pytest.mark.durable_execution(
    handler=handler,
    lambda_function_name="my_function",
)
def test_success(durable_runner):
    """Test successful execution."""
    with durable_runner:
        result = durable_runner.run(input={"data": "test"}, timeout=10)
    
    assert result.status is InvocationStatus.SUCCEEDED
```

### Testing error conditions

Test that your function handles errors correctly:

```python
@pytest.mark.durable_execution(
    handler=handler_with_validation,
    lambda_function_name="validation_function",
)
def test_input_validation(durable_runner):
    """Test input validation handling."""
    with durable_runner:
        result = durable_runner.run(input={}, timeout=10)
    
    # Function should return error response for invalid input
    assert result.status is InvocationStatus.SUCCEEDED
    assert "error" in result.result
    assert result.result["error"] == "InvalidInput"
```

### Testing SDK validation errors

Test that the SDK catches invalid configuration:

```python
@pytest.mark.durable_execution(
    handler=handler_with_invalid_config,
    lambda_function_name="sdk_validation_function",
)
def test_sdk_validation_error(durable_runner):
    """Test SDK validation error handling."""
    with durable_runner:
        result = durable_runner.run(input={}, timeout=10)
    
    # SDK should catch invalid configuration
    assert result.status is InvocationStatus.FAILED
    assert "ValidationError" in str(result.error)
```

### Testing retry behavior

Test that steps retry correctly:

```python
@pytest.mark.durable_execution(
    handler=handler_with_retry,
    lambda_function_name="retry_function",
)
def test_retry_success(durable_runner):
    """Test that retries eventually succeed."""
    with durable_runner:
        result = durable_runner.run(input={}, timeout=30)
    
    # Should succeed after retries
    assert result.status is InvocationStatus.SUCCEEDED
```

### Testing retry exhaustion

Test that execution fails when retries are exhausted:

```python
@pytest.mark.durable_execution(
    handler=handler_always_fails,
    lambda_function_name="failing_function",
)
def test_retry_exhausted(durable_runner):
    """Test that execution fails after exhausting retries."""
    with durable_runner:
        result = durable_runner.run(input={}, timeout=30)
    
    # Should fail after all retries
    assert result.status is InvocationStatus.FAILED
    assert "RuntimeError" in str(result.error)
```

### Inspecting error details

Inspect error details in test results:

```python
@pytest.mark.durable_execution(
    handler=handler_with_error,
    lambda_function_name="error_function",
)
def test_error_details(durable_runner):
    """Test error details are captured."""
    with durable_runner:
        result = durable_runner.run(input={}, timeout=10)
    
    # Check error details
    assert result.status is InvocationStatus.FAILED
    assert result.error is not None
    assert "error_type" in result.error
    assert "message" in result.error
```

For more testing patterns, see:
- [Basic tests](../testing-patterns/basic-tests.md) - Simple test examples
- [Complex workflows](../testing-patterns/complex-workflows.md) - Multi-step workflow testing
- [Best practices](../testing-patterns/best-practices.md) - Testing recommendations

[↑ Back to top](#table-of-contents)

## See also

- [Steps](../core/steps.md) - Configure retry for steps
- [Callbacks](../core/callbacks.md) - Handle callback errors
- [Child contexts](../core/child-contexts.md) - Error handling in nested contexts
- [Retry strategies](../api-reference/config.md) - Retry configuration reference
- [Examples](https://github.com/awslabs/aws-durable-execution-sdk-python/tree/main/examples/src/step) - Error handling examples

[↑ Back to top](#table-of-contents)

## License

See the [LICENSE](../../LICENSE) file for our project's licensing.

[↑ Back to top](#table-of-contents)
