# Complex Workflow Testing

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Multi-step workflows](#multi-step-workflows)
- [Nested child contexts](#nested-child-contexts)
- [Parallel operations](#parallel-operations)
- [Error scenarios](#error-scenarios)
- [Timeout handling](#timeout-handling)
- [Polling patterns](#polling-patterns)
- [FAQ](#faq)
- [See also](#see-also)

[← Back to main index](../index.md)

## Overview

When your workflows involve multiple steps, nested contexts, or parallel operations, you need to verify more than just the final result. You'll want to check intermediate states, operation ordering, error handling, and timeout behavior.

This guide shows you how to test workflows that chain operations together, handle errors gracefully, and implement polling patterns.

[↑ Back to top](#table-of-contents)

## Prerequisites

You need both SDKs installed:

```console
pip install aws-durable-execution-sdk-python
pip install aws-durable-execution-sdk-python-testing
pip install pytest
```

If you're new to testing durable functions, start with [Basic test patterns](basic-tests.md) first.

[↑ Back to top](#table-of-contents)

## Multi-step workflows

### Sequential operations


Here's a workflow that processes an order through validation, payment, and fulfillment:

```python
from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
    durable_step,
    StepContext,
)

@durable_step
def validate_order(step_context: StepContext, order_id: str) -> dict:
    return {"order_id": order_id, "status": "validated"}

@durable_step
def process_payment(step_context: StepContext, order: dict) -> dict:
    return {**order, "payment_status": "completed"}

@durable_step
def fulfill_order(step_context: StepContext, order: dict) -> dict:
    return {**order, "fulfillment_status": "shipped"}

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    order_id = event["order_id"]
    
    validated = context.step(validate_order(order_id), name="validate")
    paid = context.step(process_payment(validated), name="payment")
    fulfilled = context.step(fulfill_order(paid), name="fulfillment")
    
    return fulfilled
```

Verify all steps execute in order:

```python
import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import OperationType
from test.conftest import deserialize_operation_payload

@pytest.mark.durable_execution(handler=handler, lambda_function_name="order_workflow")
def test_order_workflow(durable_runner):
    """Test order processing executes all steps."""
    with durable_runner:
        result = durable_runner.run(input={"order_id": "order-123"}, timeout=30)
    
    assert result.status is InvocationStatus.SUCCEEDED
    
    # Check final result
    final_result = deserialize_operation_payload(result.result)
    assert final_result["order_id"] == "order-123"
    assert final_result["payment_status"] == "completed"
    assert final_result["fulfillment_status"] == "shipped"
    
    # Verify all three steps ran
    step_ops = [op for op in result.operations if op.operation_type == OperationType.STEP]
    assert len(step_ops) == 3
    
    # Check step order
    step_names = [op.name for op in step_ops]
    assert step_names == ["validate", "payment", "fulfillment"]
```

[↑ Back to top](#table-of-contents)

### Conditional branching

Test different execution paths based on input:

```python
@durable_execution
def handler(event: dict, context: DurableContext) -> str:
    amount = event.get("amount", 0)
    
    context.step(lambda _: amount, name="validate_amount")
    
    if amount > 1000:
        context.step(lambda _: "Manager approval required", name="approval")
        context.wait(seconds=10, name="approval_wait")
        result = context.step(lambda _: "High-value order processed", name="process_high")
    else:
        result = context.step(lambda _: "Standard order processed", name="process_standard")
    
    return result
```

Test both paths separately:

```python
@pytest.mark.durable_execution(handler=handler, lambda_function_name="conditional_workflow")
def test_high_value_path(durable_runner):
    """Test high-value orders require approval."""
    with durable_runner:
        result = durable_runner.run(input={"amount": 1500}, timeout=30)
    
    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == "High-value order processed"
    
    # Verify approval step exists
    approval_step = result.get_step("approval")
    assert approval_step is not None

@pytest.mark.durable_execution(handler=handler, lambda_function_name="conditional_workflow")
def test_standard_path(durable_runner):
    """Test standard orders skip approval."""
    with durable_runner:
        result = durable_runner.run(input={"amount": 500}, timeout=30)
    
    assert result.status is InvocationStatus.SUCCEEDED
    
    # Verify no approval step
    step_names = [op.name for op in result.operations if op.operation_type == OperationType.STEP]
    assert "approval" not in step_names
```

[↑ Back to top](#table-of-contents)

## Nested child contexts


### Single child context

Child contexts isolate operations:

```python
from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
    durable_with_child_context,
)

@durable_with_child_context
def process_item(ctx: DurableContext, item_id: str) -> dict:
    ctx.step(lambda _: f"Validating {item_id}", name="validate")
    result = ctx.step(
        lambda _: {"item_id": item_id, "status": "processed"},
        name="process"
    )
    return result

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    item_id = event["item_id"]
    result = context.run_in_child_context(
        process_item(item_id),
        name="item_processing"
    )
    return result
```

Verify the child context executes:

```python
@pytest.mark.durable_execution(handler=handler, lambda_function_name="child_context_workflow")
def test_child_context(durable_runner):
    """Test child context execution."""
    with durable_runner:
        result = durable_runner.run(input={"item_id": "item-123"}, timeout=30)
    
    assert result.status is InvocationStatus.SUCCEEDED
    
    # Check child context ran
    context_ops = [op for op in result.operations if op.operation_type.value == "CONTEXT"]
    assert len(context_ops) == 1
    assert context_ops[0].name == "item_processing"
    
    # Check child context result
    child_result = result.get_context("item_processing")
    child_data = deserialize_operation_payload(child_result.result)
    assert child_data["item_id"] == "item-123"
```

[↑ Back to top](#table-of-contents)

### Multiple child contexts

Use multiple child contexts to organize operations:

```python
@durable_with_child_context
def validate_data(ctx: DurableContext, data: dict) -> dict:
    return ctx.step(lambda _: {**data, "validated": True}, name="validate")

@durable_with_child_context
def transform_data(ctx: DurableContext, data: dict) -> dict:
    return ctx.step(lambda _: {**data, "transformed": True}, name="transform")

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    data = event["data"]
    
    validated = context.run_in_child_context(validate_data(data), name="validation")
    transformed = context.run_in_child_context(transform_data(validated), name="transformation")
    
    return transformed
```

Verify both contexts execute:

```python
@pytest.mark.durable_execution(handler=handler, lambda_function_name="multiple_contexts")
def test_multiple_child_contexts(durable_runner):
    """Test multiple child contexts."""
    with durable_runner:
        result = durable_runner.run(input={"data": {"value": 42}}, timeout=30)
    
    assert result.status is InvocationStatus.SUCCEEDED
    
    final_result = deserialize_operation_payload(result.result)
    assert final_result["validated"] is True
    assert final_result["transformed"] is True
    
    # Verify both contexts ran
    context_ops = [op for op in result.operations if op.operation_type.value == "CONTEXT"]
    assert len(context_ops) == 2
```

[↑ Back to top](#table-of-contents)

## Parallel operations

### Basic parallel execution

Multiple operations execute concurrently:

```python
@durable_execution
def handler(event: dict, context: DurableContext) -> list[str]:
    task1 = context.step(lambda _: "Task 1 complete", name="task1")
    task2 = context.step(lambda _: "Task 2 complete", name="task2")
    task3 = context.step(lambda _: "Task 3 complete", name="task3")
    
    return [task1, task2, task3]
```

Verify all operations execute:

```python
@pytest.mark.durable_execution(handler=handler, lambda_function_name="parallel_ops")
def test_parallel_operations(durable_runner):
    """Test parallel execution."""
    with durable_runner:
        result = durable_runner.run(input={}, timeout=30)
    
    assert result.status is InvocationStatus.SUCCEEDED
    
    results = deserialize_operation_payload(result.result)
    assert len(results) == 3
    
    # Verify all steps ran
    step_ops = [op for op in result.operations if op.operation_type == OperationType.STEP]
    assert len(step_ops) == 3
    
    step_names = {op.name for op in step_ops}
    assert step_names == {"task1", "task2", "task3"}
```

[↑ Back to top](#table-of-contents)

### Processing collections


Process collection items in parallel:

```python
@durable_execution
def handler(event: dict, context: DurableContext) -> list[int]:
    numbers = event.get("numbers", [1, 2, 3, 4, 5])
    
    results = []
    for i, num in enumerate(numbers):
        result = context.step(lambda _, n=num: n * 2, name=f"square_{i}")
        results.append(result)
    
    return results
```

Verify collection processing:

```python
@pytest.mark.durable_execution(handler=handler, lambda_function_name="parallel_collection")
def test_collection_processing(durable_runner):
    """Test collection processing."""
    with durable_runner:
        result = durable_runner.run(input={"numbers": [1, 2, 3, 4, 5]}, timeout=30)
    
    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == [2, 4, 6, 8, 10]
    
    # Verify all steps ran
    step_ops = [op for op in result.operations if op.operation_type == OperationType.STEP]
    assert len(step_ops) == 5
```

[↑ Back to top](#table-of-contents)

## Error scenarios

### Expected failures

Test that your workflow fails correctly:

```python
@durable_step
def validate_input(step_context: StepContext, value: int) -> int:
    if value < 0:
        raise ValueError("Value must be non-negative")
    return value

@durable_execution
def handler(event: dict, context: DurableContext) -> int:
    value = event.get("value", 0)
    validated = context.step(validate_input(value), name="validate")
    return validated
```

Verify validation failures:

```python
@pytest.mark.durable_execution(handler=handler, lambda_function_name="validation_workflow")
def test_validation_failure(durable_runner):
    """Test validation fails with invalid input."""
    with durable_runner:
        result = durable_runner.run(input={"value": -5}, timeout=30)
    
    assert result.status is InvocationStatus.FAILED
    assert "Value must be non-negative" in str(result.error)
```

[↑ Back to top](#table-of-contents)

### Retry behavior

Test operations that retry on failure:

```python
from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.retries import (
    RetryStrategyConfig,
    create_retry_strategy,
)

attempt_count = 0

@durable_step
def unreliable_operation(step_context: StepContext) -> str:
    global attempt_count
    attempt_count += 1
    
    if attempt_count < 3:
        raise RuntimeError("Transient error")
    
    return "Operation succeeded"

@durable_execution
def handler(event: dict, context: DurableContext) -> str:
    retry_config = RetryStrategyConfig(
        max_attempts=5,
        retryable_error_types=[RuntimeError],
    )
    
    result = context.step(
        unreliable_operation(),
        config=StepConfig(create_retry_strategy(retry_config)),
        name="unreliable"
    )
    
    return result
```

Verify retry succeeds:

```python
@pytest.mark.durable_execution(handler=handler, lambda_function_name="retry_workflow")
def test_retry_behavior(durable_runner):
    """Test operation retries on failure."""
    global attempt_count
    attempt_count = 0
    
    with durable_runner:
        result = durable_runner.run(input={}, timeout=60)
    
    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == "Operation succeeded"
    assert attempt_count >= 3
```

[↑ Back to top](#table-of-contents)

### Partial failures

Test workflows where some operations succeed before failure:

```python
@durable_execution
def handler(event: dict, context: DurableContext) -> str:
    context.step(lambda _: "Step 1 complete", name="step1")
    context.step(lambda _: "Step 2 complete", name="step2")
    context.step(
        lambda _: (_ for _ in ()).throw(RuntimeError("Step 3 failed")),
        name="step3"
    )
    return "Should not reach here"
```

Verify partial execution:

```python
@pytest.mark.durable_execution(handler=handler, lambda_function_name="partial_failure")
def test_partial_failure(durable_runner):
    """Test workflow fails after some steps succeed."""
    with durable_runner:
        result = durable_runner.run(input={}, timeout=30)
    
    assert result.status is InvocationStatus.FAILED
    
    # First two steps succeeded
    step1 = result.get_step("step1")
    assert deserialize_operation_payload(step1.result) == "Step 1 complete"
    
    step2 = result.get_step("step2")
    assert deserialize_operation_payload(step2.result) == "Step 2 complete"
    
    assert "Step 3 failed" in str(result.error)
```

[↑ Back to top](#table-of-contents)

## Timeout handling

### Callback timeouts


Verify callback timeout configuration:

```python
from aws_durable_execution_sdk_python.config import CallbackConfig

@durable_execution
def handler(event: dict, context: DurableContext) -> str:
    config = CallbackConfig(timeout_seconds=60, heartbeat_timeout_seconds=30)
    callback = context.create_callback(name="approval_callback", config=config)
    return f"Callback created: {callback.callback_id}"
```

Test callback configuration:

```python
@pytest.mark.durable_execution(handler=handler, lambda_function_name="callback_timeout")
def test_callback_timeout(durable_runner):
    """Test callback timeout configuration."""
    with durable_runner:
        result = durable_runner.run(input={}, timeout=10)
    
    assert result.status is InvocationStatus.SUCCEEDED
    
    callback_ops = [op for op in result.operations if op.operation_type.value == "CALLBACK"]
    assert len(callback_ops) == 1
    assert callback_ops[0].name == "approval_callback"
```

[↑ Back to top](#table-of-contents)

### Long waits

For workflows with long waits, verify configuration without actually waiting:

```python
@durable_execution
def handler(event: dict, context: DurableContext) -> str:
    context.step(lambda _: "Starting", name="start")
    context.wait(seconds=3600, name="long_wait")  # 1 hour
    context.step(lambda _: "Continuing", name="continue")
    return "Complete"
```

Test completes quickly:

```python
@pytest.mark.durable_execution(handler=handler, lambda_function_name="long_wait")
def test_long_wait(durable_runner):
    """Test long wait configuration."""
    with durable_runner:
        result = durable_runner.run(input={}, timeout=10)
    
    assert result.status is InvocationStatus.SUCCEEDED
    
    # Verify wait exists
    wait_ops = [op for op in result.operations if op.operation_type.value == "WAIT"]
    assert len(wait_ops) == 1
    assert wait_ops[0].name == "long_wait"
```

[↑ Back to top](#table-of-contents)

## Polling patterns

### Wait-for-condition

Poll until a condition is met:

```python
@durable_execution
def handler(event: dict, context: DurableContext) -> int:
    state = 0
    attempt = 0
    max_attempts = 5
    
    while attempt < max_attempts:
        attempt += 1
        
        state = context.step(lambda _, s=state: s + 1, name=f"increment_{attempt}")
        
        if state >= 3:
            break
        
        context.wait(seconds=1, name=f"wait_{attempt}")
    
    return state
```

Verify polling behavior:

```python
@pytest.mark.durable_execution(handler=handler, lambda_function_name="polling")
def test_polling(durable_runner):
    """Test wait-for-condition pattern."""
    with durable_runner:
        result = durable_runner.run(input={}, timeout=30)
    
    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == 3
    
    # Should have 3 increment steps
    step_ops = [op for op in result.operations if op.operation_type == OperationType.STEP]
    assert len(step_ops) == 3
    
    # Should have 2 waits (before reaching state 3)
    wait_ops = [op for op in result.operations if op.operation_type.value == "WAIT"]
    assert len(wait_ops) == 2
```

[↑ Back to top](#table-of-contents)

### Maximum attempts

Test polling respects attempt limits:

```python
@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    target = event.get("target", 10)
    state = 0
    attempt = 0
    max_attempts = 5
    
    while attempt < max_attempts and state < target:
        attempt += 1
        state = context.step(lambda _, s=state: s + 1, name=f"attempt_{attempt}")
        
        if state < target:
            context.wait(seconds=1, name=f"wait_{attempt}")
    
    return {"state": state, "attempts": attempt, "reached_target": state >= target}
```

Test with unreachable target:

```python
@pytest.mark.durable_execution(handler=handler, lambda_function_name="max_attempts")
def test_max_attempts(durable_runner):
    """Test polling stops at max attempts."""
    with durable_runner:
        result = durable_runner.run(input={"target": 10}, timeout=30)
    
    assert result.status is InvocationStatus.SUCCEEDED
    
    final_result = deserialize_operation_payload(result.result)
    assert final_result["attempts"] == 5
    assert final_result["state"] == 5
    assert final_result["reached_target"] is False
```

[↑ Back to top](#table-of-contents)

## FAQ

**Q: How do I test workflows with long waits?**

A: The test runner doesn't actually wait. You can verify wait operations are configured correctly without waiting for them to complete.

**Q: Can I test workflows with external API calls?**

A: Yes, but mock external dependencies in your tests. The test runner executes your code locally, so standard Python mocking works.

**Q: What's the best way to test conditional logic?**

A: Write separate tests for each execution path. Use descriptive test names and verify the specific operations that should execute in each path.

**Q: How do I verify operation ordering?**

A: Iterate through `result.operations` and check the order. You can also use operation names to verify specific sequences.

**Q: What timeout should I use?**

A: Use a timeout slightly longer than expected execution time. For most tests, 30-60 seconds is sufficient.

**Q: How do I test error recovery?**

A: Test both the failure case (verify the error is raised) and the recovery case (verify retry succeeds). Use separate tests for each scenario.

[↑ Back to top](#table-of-contents)

## See also

- [Basic test patterns](basic-tests.md) - Simple testing patterns
- [Best practices](best-practices.md) - Testing recommendations
- [Steps](../core/steps.md) - Step operations
- [Wait operations](../core/wait.md) - Wait operations
- [Callbacks](../core/callbacks.md) - Callback operations
- [Child contexts](../core/child-contexts.md) - Child context operations
- [Parallel operations](../core/parallel.md) - Parallel execution

[↑ Back to top](#table-of-contents)

## License

See the [LICENSE](../../LICENSE) file for our project's licensing.

[↑ Back to top](#table-of-contents)
