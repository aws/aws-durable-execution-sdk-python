# Invoke Operations

## Table of Contents

- [What are invoke operations?](#what-are-invoke-operations)
- [Terminology](#terminology)
- [Key features](#key-features)
- [Getting started](#getting-started)
- [Method signature](#method-signature)
- [Function composition patterns](#function-composition-patterns)
- [Configuration](#configuration)
- [Error handling](#error-handling)
- [Advanced patterns](#advanced-patterns)
- [Best practices](#best-practices)
- [FAQ](#faq)
- [Testing](#testing)
- [See also](#see-also)

[← Back to main index](../index.md)

## Terminology

**Invoke operation** - A durable operation that calls another durable function and waits for its result. Created using `context.invoke()`.

**Chained invocation** - The process of one durable function calling another durable function. The calling function suspends while the invoked function executes.

**Function composition** - Building complex workflows by combining multiple durable functions, where each function handles a specific part of the overall process.

**Payload** - The input data sent to the invoked function. Can be any JSON-serializable value or use custom serialization.

**Timeout** - The maximum time to wait for an invoked function to complete. If exceeded, the invoke operation fails with a timeout error.

[↑ Back to top](#table-of-contents)

## What are invoke operations?

Invoke operations let you call other Lambda functions from within your durable function. You can invoke both durable functions and regular on-demand Lambda functions. This enables function composition, where you break complex workflows into smaller, reusable functions. The calling function suspends while the invoked function executes, and resumes when the result is available.

Use invoke operations to:
- Decompose complex workflows into manageable functions
- Call existing Lambda functions (durable or on-demand) from your workflow
- Isolate different parts of your business logic
- Build hierarchical execution patterns
- Coordinate multiple Lambda functions durably
- Integrate with existing Lambda-based services

When you invoke a function, the SDK:
1. Checkpoints the invoke operation
2. Triggers the target function asynchronously
3. Suspends the calling function
4. Resumes the calling function when the result is ready
5. Returns the result or propagates any errors

[↑ Back to top](#table-of-contents)

## Key features

- **Automatic checkpointing** - Invoke operations are checkpointed before execution
- **Asynchronous execution** - Invoked functions run independently without blocking resources
- **Result handling** - Results are automatically deserialized and returned
- **Error propagation** - Errors from invoked functions propagate to the caller
- **Timeout support** - Configure maximum wait time for invoked functions
- **Custom serialization** - Control how payloads and results are serialized
- **Named operations** - Identify invoke operations by name for debugging

[↑ Back to top](#table-of-contents)

## Getting started

Here's a simple example of invoking another durable function:

```python
from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
)

@durable_execution
def process_order(event: dict, context: DurableContext) -> dict:
    """Process an order by validating and charging."""
    order_id = event["order_id"]
    amount = event["amount"]
    
    # Invoke validation function
    validation_result = context.invoke(
        function_name="validate-order",
        payload={"order_id": order_id},
        name="validate_order",
    )
    
    if not validation_result["valid"]:
        return {"status": "rejected", "reason": validation_result["reason"]}
    
    # Invoke payment function
    payment_result = context.invoke(
        function_name="process-payment",
        payload={"order_id": order_id, "amount": amount},
        name="process_payment",
    )
    
    return {
        "status": "completed",
        "order_id": order_id,
        "transaction_id": payment_result["transaction_id"],
    }
```

When this function runs:
1. It invokes the `validate-order` function and waits for the result
2. If validation succeeds, it invokes the `process-payment` function
3. Each invoke operation is checkpointed automatically
4. If the function is interrupted, it resumes from the last completed invoke

[↑ Back to top](#table-of-contents)

## Method signature

### context.invoke()

```python
def invoke(
    function_name: str,
    payload: P,
    name: str | None = None,
    config: InvokeConfig[P, R] | None = None,
) -> R
```

**Parameters:**

- `function_name` - The name of the Lambda function to invoke. This should be the function name, not the ARN.
- `payload` - The input data to send to the invoked function. Can be any JSON-serializable value.
- `name` (optional) - A name for the invoke operation, useful for debugging and testing.
- `config` (optional) - An `InvokeConfig` object to configure timeout and serialization.

**Returns:** The result returned by the invoked function.

**Raises:** 
- `CallableRuntimeError` - If the invoked function fails or times out
- `SuspendExecution` - When the operation is first started or still in progress (internal)

[↑ Back to top](#table-of-contents)

## Function composition patterns

### Sequential invocations

Call multiple functions in sequence, where each depends on the previous result:

```python
@durable_execution
def orchestrate_workflow(event: dict, context: DurableContext) -> dict:
    """Orchestrate a multi-step workflow."""
    user_id = event["user_id"]
    
    # Step 1: Fetch user data
    user = context.invoke(
        function_name="fetch-user",
        payload={"user_id": user_id},
        name="fetch_user",
    )
    
    # Step 2: Enrich user data
    enriched_user = context.invoke(
        function_name="enrich-user-data",
        payload=user,
        name="enrich_user",
    )
    
    # Step 3: Generate report
    report = context.invoke(
        function_name="generate-report",
        payload=enriched_user,
        name="generate_report",
    )
    
    return report
```

### Conditional invocations

Invoke different functions based on conditions:

```python
@durable_execution
def process_document(event: dict, context: DurableContext) -> dict:
    """Process a document based on its type."""
    document_type = event["document_type"]
    document_data = event["data"]
    
    if document_type == "pdf":
        result = context.invoke(
            function_name="process-pdf",
            payload=document_data,
            name="process_pdf",
        )
    elif document_type == "image":
        result = context.invoke(
            function_name="process-image",
            payload=document_data,
            name="process_image",
        )
    else:
        result = context.invoke(
            function_name="process-generic",
            payload=document_data,
            name="process_generic",
        )
    
    return result
```

### Hierarchical workflows

Build hierarchical workflows where parent functions coordinate child functions:

```python
@durable_execution
def parent_workflow(event: dict, context: DurableContext) -> dict:
    """Parent workflow that coordinates sub-workflows."""
    project_id = event["project_id"]
    
    # Invoke sub-workflow for data collection
    data = context.invoke(
        function_name="collect-data-workflow",
        payload={"project_id": project_id},
        name="collect_data",
    )
    
    # Invoke sub-workflow for data processing
    processed = context.invoke(
        function_name="process-data-workflow",
        payload=data,
        name="process_data",
    )
    
    # Invoke sub-workflow for reporting
    report = context.invoke(
        function_name="generate-report-workflow",
        payload=processed,
        name="generate_report",
    )
    
    return report
```

### Invoking on-demand functions

You can invoke regular Lambda functions (non-durable) from your durable workflow:

```python
@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    """Invoke a mix of durable and on-demand functions."""
    user_id = event["user_id"]
    
    # Invoke a regular Lambda function for data fetching
    user_data = context.invoke(
        function_name="fetch-user-data",  # Regular Lambda function
        payload={"user_id": user_id},
        name="fetch_user",
    )
    
    # Invoke a durable function for complex processing
    processed = context.invoke(
        function_name="process-user-workflow",  # Durable function
        payload=user_data,
        name="process_user",
    )
    
    # Invoke another regular Lambda for notifications
    notification = context.invoke(
        function_name="send-notification",  # Regular Lambda function
        payload={"user_id": user_id, "data": processed},
        name="send_notification",
    )
    
    return {
        "status": "completed",
        "notification_sent": notification["sent"],
    }
```

[↑ Back to top](#table-of-contents)

## Configuration

Configure invoke behavior using `InvokeConfig`:

```python
from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
)
from aws_durable_execution_sdk_python.config import Duration, InvokeConfig

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    # Configure invoke with timeout
    invoke_config = InvokeConfig(
        timeout=Duration.from_minutes(5),
    )
    
    result = context.invoke(
        function_name="long-running-function",
        payload=event,
        name="long_running",
        config=invoke_config,
    )
    
    return result
```

### InvokeConfig parameters

**timeout** - A `Duration` object specifying the maximum time to wait for the invoked function to complete. If the timeout is exceeded, the invoke operation fails. Default is no timeout.

**serdes_payload** - Custom serialization/deserialization for the payload sent to the invoked function. If not provided, uses JSON serialization.

**serdes_result** - Custom serialization/deserialization for the result returned by the invoked function. If not provided, uses JSON serialization.

### Setting timeouts

Use the `Duration` class to set timeouts:

```python
from aws_durable_execution_sdk_python.config import Duration, InvokeConfig

# Timeout after 30 seconds
config = InvokeConfig(timeout=Duration.from_seconds(30))

# Timeout after 5 minutes
config = InvokeConfig(timeout=Duration.from_minutes(5))

# Timeout after 2 hours
config = InvokeConfig(timeout=Duration.from_hours(2))
```

[↑ Back to top](#table-of-contents)

## Error handling

### Handling invocation errors

Errors from invoked functions propagate to the calling function. Catch and handle them as needed:

```python
from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
    CallableRuntimeError,
)

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    """Handle errors from invoked functions."""
    try:
        result = context.invoke(
            function_name="risky-function",
            payload=event,
            name="risky_operation",
        )
        return {"status": "success", "result": result}
    
    except CallableRuntimeError as e:
        # Handle the error from the invoked function
        context.logger.error(f"Invoked function failed: {e}")
        return {
            "status": "failed",
            "error": str(e),
        }
```

### Timeout handling

Handle timeout errors specifically:

```python
from aws_durable_execution_sdk_python.config import Duration, InvokeConfig

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    """Handle timeout errors."""
    config = InvokeConfig(timeout=Duration.from_seconds(30))
    
    try:
        result = context.invoke(
            function_name="slow-function",
            payload=event,
            config=config,
        )
        return {"status": "success", "result": result}
    
    except CallableRuntimeError as e:
        if "timed out" in str(e).lower():
            context.logger.warning("Function timed out, using fallback")
            return {"status": "timeout", "fallback": True}
        raise
```

### Retry patterns

Implement retry logic for failed invocations:

```python
@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    """Retry failed invocations."""
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            result = context.invoke(
                function_name="unreliable-function",
                payload=event,
                name=f"attempt_{attempt + 1}",
            )
            return {"status": "success", "result": result, "attempts": attempt + 1}
        
        except CallableRuntimeError as e:
            if attempt == max_retries - 1:
                # Last attempt failed
                return {
                    "status": "failed",
                    "error": str(e),
                    "attempts": max_retries,
                }
            # Wait before retrying
            context.wait(Duration.from_seconds(2 ** attempt))
    
    return {"status": "failed", "reason": "max_retries_exceeded"}
```

[↑ Back to top](#table-of-contents)

## Advanced patterns

### Custom serialization

Use custom serialization for complex data types:

```python
from aws_durable_execution_sdk_python.config import InvokeConfig
from aws_durable_execution_sdk_python.serdes import SerDes

class CustomSerDes(SerDes):
    """Custom serialization for complex objects."""
    
    def serialize(self, value):
        # Custom serialization logic
        return json.dumps({"custom": value})
    
    def deserialize(self, data: str):
        # Custom deserialization logic
        obj = json.loads(data)
        return obj["custom"]

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    """Use custom serialization."""
    config = InvokeConfig(
        serdes_payload=CustomSerDes(),
        serdes_result=CustomSerDes(),
    )
    
    result = context.invoke(
        function_name="custom-function",
        payload={"complex": "data"},
        config=config,
    )
    
    return result
```

### Fan-out pattern with parallel invocations

Invoke multiple functions in parallel using steps:

```python
from aws_durable_execution_sdk_python import durable_step, StepContext

@durable_step
def invoke_service(step_context: StepContext, service_name: str, data: dict) -> dict:
    """Invoke a service and return its result."""
    # Note: This is a simplified example. In practice, you'd need access to context
    # which isn't directly available in step functions.
    return {"service": service_name, "result": data}

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    """Fan out to multiple services."""
    services = ["service-a", "service-b", "service-c"]
    
    # Invoke each service sequentially
    results = []
    for service in services:
        result = context.invoke(
            function_name=service,
            payload=event,
            name=f"invoke_{service}",
        )
        results.append(result)
    
    return {"results": results}
```

### Passing context between invocations

Pass data between invoked functions:

```python
@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    """Pass context between invocations."""
    # First invocation creates context
    initial_context = context.invoke(
        function_name="initialize-context",
        payload=event,
        name="initialize",
    )
    
    # Second invocation uses the context
    processed = context.invoke(
        function_name="process-with-context",
        payload={
            "data": event["data"],
            "context": initial_context,
        },
        name="process",
    )
    
    # Third invocation finalizes
    final_result = context.invoke(
        function_name="finalize",
        payload={
            "processed": processed,
            "context": initial_context,
        },
        name="finalize",
    )
    
    return final_result
```

[↑ Back to top](#table-of-contents)

## Best practices

**Use descriptive function names** - Choose clear, descriptive names for the functions you invoke to make workflows easier to understand.

**Name invoke operations** - Use the `name` parameter to identify invoke operations in logs and tests.

**Set appropriate timeouts** - Configure timeouts based on expected execution time. Don't set them too short or too long.

**Handle errors explicitly** - Catch and handle errors from invoked functions. Don't let them propagate unexpectedly.

**Keep payloads small** - Large payloads increase serialization overhead. Consider passing references instead of large data.

**Design for idempotency** - Invoked functions should be idempotent since they might be retried.

**Use hierarchical composition** - Break complex workflows into layers of functions, where each layer handles a specific level of abstraction.

**Avoid deep nesting** - Don't create deeply nested invocation chains. Keep hierarchies shallow for better observability.

**Log invocation boundaries** - Log when invoking functions and when receiving results for better debugging.

**Consider cost implications** - Each invoke operation triggers a separate Lambda invocation, which has cost implications.

**Mix durable and on-demand functions** - You can invoke both durable and regular Lambda functions. Use durable functions for complex workflows and on-demand functions for simple operations.

[↑ Back to top](#table-of-contents)

## FAQ

**Q: What's the difference between invoke and step?**

A: `invoke()` calls another durable function (Lambda), while `step()` executes code within the current function. Use invoke for function composition, use step for checkpointing operations within a function.

**Q: Can I invoke non-durable functions?**

A: Yes, `context.invoke()` can call both durable functions and regular on-demand Lambda functions. The invoke operation works with any Lambda function that accepts and returns JSON-serializable data.

**Q: How do I pass the result from one invoke to another?**

A: Simply use the return value:

```python
result1 = context.invoke("function-1", payload1)
result2 = context.invoke("function-2", result1)
```

**Q: What happens if an invoked function fails?**

A: The error propagates to the calling function as a `CallableRuntimeError`. You can catch and handle it.

**Q: Can I invoke the same function multiple times?**

A: Yes, you can invoke the same function multiple times with different payloads or names.

**Q: How do I invoke a function in a different AWS account?**

A: The `function_name` parameter accepts function names in the same account. For cross-account invocations, you need appropriate IAM permissions and may need to use function ARNs (check AWS documentation for cross-account Lambda invocations).

**Q: What's the maximum timeout I can set?**

A: The timeout is limited by Lambda's maximum execution time (15 minutes). However, durable functions can run longer by suspending and resuming.

**Q: Can I invoke functions in parallel?**

A: Not directly with `context.invoke()`. For parallel execution, consider using `context.parallel()` with steps that perform invocations, or invoke multiple functions sequentially.

**Q: How do I debug invoke operations?**

A: Use the `name` parameter to identify operations in logs. Check CloudWatch logs for both the calling and invoked functions.

**Q: What happens if I don't set a timeout?**

A: The invoke operation waits indefinitely for the invoked function to complete. It's recommended to set timeouts for better error handling.

**Q: What's the difference between context.invoke() and using boto3's Lambda client to invoke functions?**

A: When you use `context.invoke()`, the SDK suspends your durable function's execution while waiting for the result. This means you don't pay for Lambda compute time while waiting. With boto3's Lambda client, your function stays active and consumes billable compute time while waiting for the response. Additionally, `context.invoke()` automatically checkpoints the operation, handles errors durably, and integrates with the durable execution lifecycle.

[↑ Back to top](#table-of-contents)

## Testing

You can test invoke operations using the testing SDK. The test runner executes your function and lets you inspect invoke operations.

### Basic invoke testing

```python
import pytest
from aws_durable_execution_sdk_python_testing import InvocationStatus
from my_function import handler

@pytest.mark.durable_execution(
    handler=handler,
    lambda_function_name="my_function",
)
def test_invoke(durable_runner):
    """Test a function with invoke operations."""
    with durable_runner:
        result = durable_runner.run(
            input={"order_id": "order-123", "amount": 100.0},
            timeout=30,
        )
    
    # Check overall status
    assert result.status is InvocationStatus.SUCCEEDED
    
    # Check final result
    assert result.result["status"] == "completed"
```

### Inspecting invoke operations

Use the result object to inspect invoke operations:

```python
@pytest.mark.durable_execution(
    handler=handler,
    lambda_function_name="my_function",
)
def test_invoke_operations(durable_runner):
    """Test and inspect invoke operations."""
    with durable_runner:
        result = durable_runner.run(input={"user_id": "user-123"}, timeout=30)
    
    # Get all operations
    operations = result.operations
    
    # Find invoke operations
    invoke_ops = [op for op in operations if op.operation_type == "CHAINED_INVOKE"]
    
    # Verify invoke operations were created
    assert len(invoke_ops) == 2
    
    # Check specific invoke operation
    validate_op = next(op for op in invoke_ops if op.name == "validate_order")
    assert validate_op.status is InvocationStatus.SUCCEEDED
```

### Testing error handling

Test that invoke errors are handled correctly:

```python
@pytest.mark.durable_execution(
    handler=handler_with_error_handling,
    lambda_function_name="error_handler_function",
)
def test_invoke_error_handling(durable_runner):
    """Test invoke error handling."""
    with durable_runner:
        result = durable_runner.run(input={}, timeout=30)
    
    # Function should handle the error gracefully
    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result["status"] == "failed"
    assert "error" in result.result
```

### Testing timeouts

Test that timeouts are handled correctly:

```python
from aws_durable_execution_sdk_python.config import Duration, InvokeConfig

@pytest.mark.durable_execution(
    handler=handler_with_timeout,
    lambda_function_name="timeout_function",
)
def test_invoke_timeout(durable_runner):
    """Test invoke timeout handling."""
    with durable_runner:
        result = durable_runner.run(input={}, timeout=60)
    
    # Check that timeout was handled
    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result["status"] == "timeout"
```

### Mocking invoked functions

When testing, you can mock the invoked functions to control their behavior:

```python
from unittest.mock import Mock, patch

@pytest.mark.durable_execution(
    handler=handler,
    lambda_function_name="my_function",
)
def test_invoke_with_mock(durable_runner):
    """Test invoke with mocked function."""
    # The testing framework handles invocations internally
    # You can test the orchestration logic without deploying all functions
    
    with durable_runner:
        result = durable_runner.run(
            input={"order_id": "order-123"},
            timeout=30,
        )
    
    # Verify the orchestration logic
    assert result.status is InvocationStatus.SUCCEEDED
```

For more testing patterns, see:
- [Basic tests](../testing-patterns/basic-tests.md) - Simple test examples
- [Complex workflows](../testing-patterns/complex-workflows.md) - Multi-step workflow testing
- [Best practices](../testing-patterns/best-practices.md) - Testing recommendations

[↑ Back to top](#table-of-contents)

## See also

- [Steps](steps.md) - Execute code with checkpointing
- [Child contexts](child-contexts.md) - Organize operations hierarchically
- [Parallel operations](parallel.md) - Execute multiple operations concurrently
- [Error handling](../advanced/error-handling.md) - Handle errors in durable functions
- [DurableContext API](../api-reference/context.md) - Complete context reference

[↑ Back to top](#table-of-contents)

## License

See the [LICENSE](../../LICENSE) file for our project's licensing.

[↑ Back to top](#table-of-contents)
