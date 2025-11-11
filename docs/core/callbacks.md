# Callbacks

## Table of Contents

- [Terminology](#terminology)
- [What are callbacks?](#what-are-callbacks)
- [Key features](#key-features)
- [Getting started](#getting-started)
- [Method signatures](#method-signatures)
- [Configuration](#configuration)
- [Waiting for callbacks](#waiting-for-callbacks)
- [Integration patterns](#integration-patterns)
- [Advanced patterns](#advanced-patterns)
- [Best practices](#best-practices)
- [FAQ](#faq)
- [Testing](#testing)
- [See also](#see-also)

[← Back to main index](../index.md)

## Terminology

**Callback** - A mechanism that pauses execution and waits for an external system to provide a result. Created using `context.create_callback()`.

**Callback ID** - A unique identifier for a callback that you send to external systems. The external system uses this ID to send the result back.

**Callback timeout** - The maximum time to wait for a callback response. If the timeout expires without a response, the callback fails.

**Heartbeat timeout** - The maximum time between heartbeat signals from the external system. Use this to detect when external systems stop responding.

**Wait for callback** - The operation that pauses execution until the callback receives a result. Created using `context.wait_for_callback()`.

[↑ Back to top](#table-of-contents)

## What are callbacks?

Callbacks let your durable function pause and wait for external systems to respond. When you create a callback, you get a unique callback ID that you can send to external systems like approval workflows, payment processors, or third-party APIs. Your function pauses until the external system calls back with a result.

Use callbacks to:
- Wait for human approvals in workflows
- Integrate with external payment systems
- Coordinate with third-party APIs
- Handle long-running external processes
- Implement request-response patterns with external systems

[↑ Back to top](#table-of-contents)

## Key features

- **External system integration** - Pause execution and wait for external responses
- **Unique callback IDs** - Each callback gets a unique identifier for routing
- **Configurable timeouts** - Set maximum wait times and heartbeat intervals
- **Type-safe results** - Callbacks are generic and preserve result types
- **Automatic checkpointing** - Callback results are saved automatically
- **Heartbeat monitoring** - Detect when external systems stop responding

[↑ Back to top](#table-of-contents)

## Getting started

Callbacks let you pause your durable function while waiting for an external system to respond. Think of it like this:

**Your durable function:**
1. Creates a callback and gets a unique `callback_id`
2. Sends the `callback_id` to an external system (payment processor, approval system, etc.)
3. Calls `callback.result()` - execution pauses here ⏸️
4. When the callback is notified, execution resumes ▶️

**Your notification handler** (separate Lambda or service):
1. Receives the result from the external system (via webhook, queue, etc.)
2. Calls AWS Lambda API `SendDurableExecutionCallbackSuccess` with the `callback_id`
3. This wakes up your durable function

The key insight: callbacks need two pieces working together - one that waits, and one that notifies.

### Basic example

Here's a simple example showing the durable function side:

```python
from typing import Any
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.config import CallbackConfig, Duration

@durable_execution
def handler(event: Any, context: DurableContext) -> dict:
    """Create a callback and wait for external system response."""
    # Step 1: Create the callback
    callback_config = CallbackConfig(
        timeout=Duration.from_minutes(2),
        heartbeat_timeout=Duration.from_seconds(60),
    )
    
    callback = context.create_callback(
        name="example_callback",
        config=callback_config,
    )
    
    # Step 2: Send callback ID to external system
    # In a real scenario, you'd send this to a third-party API,
    # message queue, or webhook endpoint
    send_to_external_system({
        "callback_id": callback.callback_id,
        "data": event.get("data"),
    })
    
    # Step 3: Wait for the result - execution suspends here
    result = callback.result()
    
    # Step 4: Execution resumes when result is received
    return {
        "status": "completed",
        "result": result,
    }
```

### Notifying the callback

When your external system finishes processing, you need to notify the callback using AWS Lambda APIs. You have three options:

**send_durable_execution_callback_success** - Notify success with a result:

```python
import boto3
import json

lambda_client = boto3.client('lambda')

# When external system succeeds
callback_id = "abc123-callback-id-from-durable-function"
result_data = json.dumps({'status': 'approved', 'amount': 1000}).encode('utf-8')

lambda_client.send_durable_execution_callback_success(
    CallbackId=callback_id,
    Result=result_data
)
```

**send_durable_execution_callback_failure** - Notify failure with an error:

```python
# When external system fails
callback_id = "abc123-callback-id-from-durable-function"

lambda_client.send_durable_execution_callback_failure(
    CallbackId=callback_id,
    Error={
        'ErrorType': 'PaymentDeclined',
        'ErrorMessage': 'Insufficient funds'
    }
)
```

**send_durable_execution_callback_heartbeat** - Send heartbeat to keep callback alive:

```python
# Send heartbeat for long-running operations
callback_id = "abc123-callback-id-from-durable-function"

lambda_client.send_durable_execution_callback_heartbeat(
    CallbackId=callback_id
)
```

### Complete example with message broker

Here's a complete example showing both sides of the callback flow:

```python
# Durable function side
@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    """Process payment with external payment processor."""
    # Create callback
    callback = context.create_callback(
        name="payment_callback",
        config=CallbackConfig(timeout=Duration.from_minutes(5)),
    )
    
    # Send to message broker (SQS, SNS, EventBridge, etc.)
    send_to_payment_queue({
        "callback_id": callback.callback_id,
        "amount": event["amount"],
        "customer_id": event["customer_id"],
    })
    
    # Wait for result - execution suspends here
    payment_result = callback.result()
    
    # Execution resumes here when callback is notified
    return {
        "payment_status": payment_result.get("status"),
        "transaction_id": payment_result.get("transaction_id"),
    }
```

```python
# Message processor side (separate Lambda or service)
import boto3
import json

lambda_client = boto3.client('lambda')

def process_payment_message(event: dict):
    """Process payment and notify callback."""
    callback_id = event["callback_id"]
    amount = event["amount"]
    customer_id = event["customer_id"]
    
    try:
        # Process payment with external system
        result = payment_processor.charge(customer_id, amount)
        
        # Notify success
        result_data = json.dumps({
            'status': 'completed',
            'transaction_id': result.transaction_id,
        }).encode('utf-8')
        
        lambda_client.send_durable_execution_callback_success(
            CallbackId=callback_id,
            Result=result_data
        )
    except PaymentError as e:
        # Notify failure
        lambda_client.send_durable_execution_callback_failure(
            CallbackId=callback_id,
            Error={
                'ErrorType': 'PaymentError',
                'ErrorMessage': f'{e.error_code}: {str(e)}'
            }
        )
```

### Key points

- **Callbacks require two parts**: Your durable function creates the callback, and a separate process notifies the result
- **Use Lambda APIs to notify**: `SendDurableExecutionCallbackSuccess`, `SendDurableExecutionCallbackFailure`, or `SendDurableExecutionCallbackHeartbeat`
- **Execution suspends at `callback.result()`**: Your function stops running and doesn't consume resources while waiting
- **Execution resumes when notified**: When you call the Lambda API with the callback ID, your function resumes from where it suspended
- **Heartbeats keep callbacks alive**: For long operations, send heartbeats to prevent timeout

[↑ Back to top](#table-of-contents)

## Method signatures

### context.create_callback()

```python
def create_callback(
    name: str | None = None,
    config: CallbackConfig | None = None,
) -> Callback[T]
```

**Parameters:**

- `name` (optional) - A name for the callback, useful for debugging and testing
- `config` (optional) - A `CallbackConfig` object to configure timeout behavior

**Returns:** A `Callback` object with a `callback_id` property

**Type parameter:** `T` - The type of result the callback will receive

### callback.callback_id

```python
callback_id: str
```

A unique identifier for this callback. Send this ID to external systems so they can return results.

### callback.result()

```python
def result() -> T | None
```

Returns the callback result. Blocks until the result is available or the callback times out.

[↑ Back to top](#table-of-contents)

## Configuration

Configure callback behavior using `CallbackConfig`:

```python
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.config import CallbackConfig, Duration

@durable_execution
def handler(event: dict, context: DurableContext) -> str:
    # Configure callback with custom timeouts
    config = CallbackConfig(
        timeout=Duration.from_seconds(60),
        heartbeat_timeout=Duration.from_seconds(30),
    )
    
    callback = context.create_callback(
        name="timeout_callback",
        config=config,
    )
    
    return f"Callback created with 60s timeout: {callback.callback_id}"
```

### CallbackConfig parameters

**timeout** - Maximum time to wait for the callback response. Use `Duration` helpers to specify:
- `Duration.from_seconds(60)` - 60 seconds
- `Duration.from_minutes(5)` - 5 minutes
- `Duration.from_hours(2)` - 2 hours
- `Duration.from_days(1)` - 1 day

**heartbeat_timeout** - Maximum time between heartbeat signals from the external system. If the external system doesn't send a heartbeat within this interval, the callback fails. Set to 0 or omit to disable heartbeat monitoring.

**serdes** (optional) - Custom serialization/deserialization for the callback result. If not provided, uses JSON serialization.

### Duration helpers

The `Duration` class provides convenient methods for specifying timeouts:

```python
from aws_durable_execution_sdk_python.config import Duration

# Various ways to specify duration
timeout_60s = Duration.from_seconds(60)
timeout_5m = Duration.from_minutes(5)
timeout_2h = Duration.from_hours(2)
timeout_1d = Duration.from_days(1)

# Use in CallbackConfig
config = CallbackConfig(
    timeout=Duration.from_hours(2),
    heartbeat_timeout=Duration.from_minutes(15),
)
```

[↑ Back to top](#table-of-contents)

## Waiting for callbacks

After creating a callback, you typically wait for its result. There are two ways to do this:

### Using callback.result()

Call `result()` on the callback object to wait for the response:

```python
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.config import CallbackConfig, Duration

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    # Create callback
    callback = context.create_callback(
        name="approval_callback",
        config=CallbackConfig(timeout=Duration.from_hours(24)),
    )
    
    # Send callback ID to approval system
    send_approval_request(callback.callback_id, event["request_details"])
    
    # Wait for approval response
    approval_result = callback.result()
    
    if approval_result and approval_result.get("approved"):
        return {"status": "approved", "details": approval_result}
    else:
        return {"status": "rejected"}
```

### Using context.wait_for_callback()

Alternatively, use `wait_for_callback()` to wait for a callback by its ID:

```python
@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    # Create callback
    callback = context.create_callback(name="payment_callback")
    
    # Send to payment processor
    initiate_payment(callback.callback_id, event["amount"])
    
    # Wait for payment result
    payment_result = context.wait_for_callback(
        callback.callback_id,
        config=CallbackConfig(timeout=Duration.from_minutes(5)),
    )
    
    return {"payment_status": payment_result}
```

[↑ Back to top](#table-of-contents)

## Integration patterns

### Human approval workflow

Use callbacks to pause execution while waiting for human approval:

```python
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.config import CallbackConfig, Duration

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    """Process an order that requires approval."""
    order_id = event["order_id"]
    
    # Create callback for approval
    approval_callback = context.create_callback(
        name="order_approval",
        config=CallbackConfig(
            timeout=Duration.from_hours(48),  # 48 hours to approve
            heartbeat_timeout=Duration.from_hours(12),  # Check every 12 hours
        ),
    )
    
    # Send approval request to approval system
    # The approval system will use callback.callback_id to respond
    send_to_approval_system({
        "callback_id": approval_callback.callback_id,
        "order_id": order_id,
        "details": event["order_details"],
    })
    
    # Wait for approval
    approval = approval_callback.result()
    
    if approval and approval.get("approved"):
        # Process approved order
        return process_order(order_id)
    else:
        # Handle rejection
        return {"status": "rejected", "reason": approval.get("reason")}
```

### Payment processing

Integrate with external payment processors:

```python
@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    """Process a payment with external processor."""
    amount = event["amount"]
    customer_id = event["customer_id"]
    
    # Create callback for payment result
    payment_callback = context.create_callback(
        name="payment_processing",
        config=CallbackConfig(
            timeout=Duration.from_minutes(5),
            heartbeat_timeout=Duration.from_seconds(30),
        ),
    )
    
    # Initiate payment with external processor
    initiate_payment_with_processor({
        "callback_id": payment_callback.callback_id,
        "amount": amount,
        "customer_id": customer_id,
        "callback_url": f"https://api.example.com/callbacks/{payment_callback.callback_id}",
    })
    
    # Wait for payment result
    payment_result = payment_callback.result()
    
    return {
        "transaction_id": payment_result.get("transaction_id"),
        "status": payment_result.get("status"),
        "amount": amount,
    }
```

### Third-party API integration

Wait for responses from third-party APIs:

```python
@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    """Integrate with third-party data enrichment API."""
    user_data = event["user_data"]
    
    # Create callback for enrichment result
    enrichment_callback = context.create_callback(
        name="data_enrichment",
        config=CallbackConfig(timeout=Duration.from_minutes(10)),
    )
    
    # Request data enrichment from third-party
    request_data_enrichment({
        "callback_id": enrichment_callback.callback_id,
        "user_data": user_data,
        "webhook_url": f"https://api.example.com/webhooks/{enrichment_callback.callback_id}",
    })
    
    # Wait for enriched data
    enriched_data = enrichment_callback.result()
    
    # Combine original and enriched data
    return {
        "original": user_data,
        "enriched": enriched_data,
        "timestamp": enriched_data.get("processed_at"),
    }
```

### Multiple callbacks

Handle multiple external systems in parallel:

```python
@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    """Wait for multiple external systems."""
    # Create callbacks for different systems
    credit_check = context.create_callback(
        name="credit_check",
        config=CallbackConfig(timeout=Duration.from_minutes(5)),
    )
    
    fraud_check = context.create_callback(
        name="fraud_check",
        config=CallbackConfig(timeout=Duration.from_minutes(3)),
    )
    
    # Send requests to external systems
    request_credit_check(credit_check.callback_id, event["customer_id"])
    request_fraud_check(fraud_check.callback_id, event["transaction_data"])
    
    # Wait for both results
    credit_result = credit_check.result()
    fraud_result = fraud_check.result()
    
    # Make decision based on both checks
    approved = (
        credit_result.get("score", 0) > 650 and
        fraud_result.get("risk_level") == "low"
    )
    
    return {
        "approved": approved,
        "credit_score": credit_result.get("score"),
        "fraud_risk": fraud_result.get("risk_level"),
    }
```

[↑ Back to top](#table-of-contents)

## Advanced patterns

### Callback with retry

Combine callbacks with retry logic for resilient integrations:

```python
from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
    durable_step,
    StepContext,
)
from aws_durable_execution_sdk_python.config import (
    CallbackConfig,
    Duration,
    StepConfig,
)
from aws_durable_execution_sdk_python.retries import (
    RetryStrategyConfig,
    create_retry_strategy,
)

@durable_step
def wait_for_external_system(
    step_context: StepContext,
    callback_id: str,
) -> dict:
    """Wait for external system with retry on timeout."""
    # This will retry if the callback times out
    result = context.wait_for_callback(
        callback_id,
        config=CallbackConfig(timeout=Duration.from_minutes(2)),
    )
    return result

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    # Create callback
    callback = context.create_callback(name="external_api")
    
    # Send request
    send_external_request(callback.callback_id)
    
    # Wait with retry
    retry_config = RetryStrategyConfig(
        max_attempts=3,
        initial_delay_seconds=5,
    )
    
    result = context.step(
        wait_for_external_system(callback.callback_id),
        config=StepConfig(retry_strategy=create_retry_strategy(retry_config)),
    )
    
    return result
```

### Conditional callback handling

Handle different callback results based on conditions:

```python
@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    """Handle callback results conditionally."""
    callback = context.create_callback(
        name="conditional_callback",
        config=CallbackConfig(timeout=Duration.from_minutes(10)),
    )
    
    # Send request
    send_request(callback.callback_id, event["request_type"])
    
    # Wait for result
    result = callback.result()
    
    # Handle different result types
    if result is None:
        return {"status": "timeout", "message": "No response received"}
    
    result_type = result.get("type")
    
    if result_type == "success":
        return process_success(result)
    elif result_type == "partial":
        return process_partial(result)
    else:
        return process_failure(result)
```

### Callback with fallback

Implement fallback logic when callbacks timeout:

```python
@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    """Use fallback when callback times out."""
    callback = context.create_callback(
        name="primary_service",
        config=CallbackConfig(timeout=Duration.from_seconds(30)),
    )
    
    # Try primary service
    send_to_primary_service(callback.callback_id, event["data"])
    
    result = callback.result()
    
    if result is None:
        # Primary service timed out, use fallback
        fallback_callback = context.create_callback(
            name="fallback_service",
            config=CallbackConfig(timeout=Duration.from_minutes(2)),
        )
        
        send_to_fallback_service(fallback_callback.callback_id, event["data"])
        result = fallback_callback.result()
    
    return {"result": result, "source": "primary" if result else "fallback"}
```

[↑ Back to top](#table-of-contents)

## Best practices

**Set appropriate timeouts** - Choose timeout values based on your external system's expected response time. Add buffer for network delays and processing time.

**Use heartbeat timeouts for long operations** - Enable heartbeat monitoring for callbacks that take more than a few minutes. This helps detect when external systems stop responding.

**Send callback IDs securely** - Treat callback IDs as sensitive data. Use HTTPS when sending them to external systems.

**Handle timeout scenarios** - Always handle the case where `callback.result()` returns `None` due to timeout. Implement fallback logic or error handling.

**Name callbacks for debugging** - Use descriptive names to identify callbacks in logs and tests.

**Don't reuse callback IDs** - Each callback gets a unique ID. Don't try to reuse IDs across different operations.

**Validate callback results** - Always validate the structure and content of callback results before using them.

**Use type hints** - Specify the expected result type when creating callbacks: `Callback[dict]`, `Callback[str]`, etc.

**Monitor callback metrics** - Track callback success rates, timeout rates, and response times to identify integration issues.

**Document callback contracts** - Clearly document what data external systems should send back and in what format.

[↑ Back to top](#table-of-contents)

## FAQ

**Q: What happens if a callback times out?**

A: If the timeout expires before receiving a result, `callback.result()` returns `None`. You should handle this case in your code.

**Q: Can I cancel a callback?**

A: No, callbacks can't be cancelled once created. They either receive a result or timeout.

**Q: How do external systems send results back?**

A: External systems use the callback ID to send results through your application's callback endpoint. You need to implement an endpoint that receives the callback ID and result, then forwards it to the durable execution service.

**Q: Can I create multiple callbacks in one function?**

A: Yes, you can create as many callbacks as needed. Each gets a unique callback ID.

**Q: What's the maximum timeout for a callback?**

A: You can set any timeout value using `Duration` helpers. For long-running operations (hours or days), use longer timeouts and enable heartbeat monitoring to detect if external systems stop responding.

**Q: Do I need to wait for a callback immediately after creating it?**

A: No, you can create a callback, send its ID to an external system, perform other operations, and wait for the result later in your function.

**Q: Can callbacks be used with steps?**

A: Yes, you can create and wait for callbacks inside step functions. However, `context.wait_for_callback()` is a convenience method that already wraps the callback in a step with retry logic for you.

**Q: What happens if the external system sends a result after the timeout?**

A: Late results are ignored. The callback has already failed due to timeout.

**Q: How do I test functions with callbacks?**

A: Use the testing SDK to simulate callback responses. See the Testing section below for examples.

**Q: Can I use callbacks in child contexts?**

A: Yes, callbacks work in child contexts just like in the main context.

**Q: What's the difference between timeout and heartbeat_timeout?**

A: `timeout` is the maximum total wait time. `heartbeat_timeout` is the maximum time between heartbeat signals. Use heartbeat timeout to detect when external systems stop responding before the main timeout expires.

[↑ Back to top](#table-of-contents)

## Testing

You can test callbacks using the testing SDK. The test runner lets you simulate callback responses and verify callback behavior.

### Basic callback testing

```python
import pytest
from aws_durable_execution_sdk_python_testing import InvocationStatus
from examples.src.callback import callback

@pytest.mark.durable_execution(
    handler=callback.handler,
    lambda_function_name="callback",
)
def test_callback(durable_runner):
    """Test callback creation."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)
    
    # Check overall status
    assert result.status is InvocationStatus.SUCCEEDED
    
    # Verify callback was created
    assert "Callback created with ID:" in result.result
```

### Inspecting callback operations

Use `result.operations` to inspect callback details:

```python
@pytest.mark.durable_execution(
    handler=callback.handler,
    lambda_function_name="callback",
)
def test_callback_operation(durable_runner):
    """Test and inspect callback operation."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)
    
    # Find callback operations
    callback_ops = [
        op for op in result.operations
        if op.operation_type.value == "CALLBACK"
    ]
    
    assert len(callback_ops) == 1
    callback_op = callback_ops[0]
    
    # Verify callback properties
    assert callback_op.name == "example_callback"
    assert callback_op.callback_id is not None
```

### Testing callback timeouts

Test that callbacks handle timeouts correctly:

```python
from examples.src.callback import callback_with_timeout

@pytest.mark.durable_execution(
    handler=callback_with_timeout.handler,
    lambda_function_name="callback_timeout",
)
def test_callback_timeout(durable_runner):
    """Test callback with custom timeout."""
    with durable_runner:
        result = durable_runner.run(input={}, timeout=10)
    
    assert result.status is InvocationStatus.SUCCEEDED
    assert "60s timeout" in result.result
```

### Testing callback integration patterns

Test complete integration workflows:

```python
@pytest.mark.durable_execution(
    handler=approval_workflow_handler,
    lambda_function_name="approval_workflow",
)
def test_approval_workflow(durable_runner):
    """Test approval workflow with callback."""
    with durable_runner:
        result = durable_runner.run(
            input={"order_id": "order-123", "amount": 1000},
            timeout=30,
        )
    
    # Verify workflow completed
    assert result.status is InvocationStatus.SUCCEEDED
    
    # Check callback was created
    callback_ops = [
        op for op in result.operations
        if op.operation_type.value == "CALLBACK"
    ]
    assert len(callback_ops) == 1
    assert callback_ops[0].name == "order_approval"
```

For more testing patterns, see:
- [Basic tests](../testing-patterns/basic-tests.md) - Simple test examples
- [Complex workflows](../testing-patterns/complex-workflows.md) - Multi-step workflow testing
- [Best practices](../testing-patterns/best-practices.md) - Testing recommendations

[↑ Back to top](#table-of-contents)

## See also

- [DurableContext API](../api-reference/context.md) - Complete context reference
- [CallbackConfig](../api-reference/config.md) - Configuration options
- [Duration helpers](../api-reference/config.md#duration) - Time duration utilities
- [Steps](steps.md) - Combine callbacks with steps for retry logic
- [Child contexts](child-contexts.md) - Use callbacks in nested contexts
- [Error handling](../advanced/error-handling.md) - Handle callback failures
- [Examples](https://github.com/awslabs/aws-durable-execution-sdk-python/tree/main/examples/src/callback) - More callback examples

[↑ Back to top](#table-of-contents)

## License

See the LICENSE file for our project's licensing.

[↑ Back to top](#table-of-contents)
