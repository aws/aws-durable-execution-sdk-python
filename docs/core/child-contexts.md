# Child Contexts

## Table of Contents

- [Terminology](#terminology)
- [What are child contexts?](#what-are-child-contexts)
- [Key features](#key-features)
- [Getting started](#getting-started)
- [Method signatures](#method-signatures)
- [Using the @durable_with_child_context decorator](#using-the-durable_with_child_context-decorator)
- [Naming child contexts](#naming-child-contexts)
- [Use cases for isolation](#use-cases-for-isolation)
- [Advanced patterns](#advanced-patterns)
- [Best practices](#best-practices)
- [FAQ](#faq)
- [Testing](#testing)
- [See also](#see-also)

[← Back to main index](../index.md)

## Terminology

**Child context** - An isolated execution scope within a durable function. Created using `context.run_in_child_context()`.

**Parent context** - The main durable function context that creates child contexts.

**Context function** - A function decorated with `@durable_with_child_context` that receives a `DurableContext` and can execute operations.

**Context isolation** - Child contexts have their own operation namespace, preventing naming conflicts with the parent context.

**Context result** - The return value from a child context function, which is checkpointed as a single unit in the parent context.

[↑ Back to top](#table-of-contents)

## What are child contexts?

A child context creates a scope in which you can nest durable operations. It creates an isolated execution scope with its own set of operations, checkpoints, and state. This is often useful as a unit of concurrency that lets you run concurrent operations within your durable function. You can also use child contexts to wrap large chunks of durable logic into a single piece - once completed, that logic won't run or replay again.

Use child contexts to:
- Run concurrent operations (steps, waits, callbacks) in parallel
- Wrap large blocks of logic that should execute as a single unit
- Handle large data that exceeds individual step limits
- Isolate groups of related operations
- Create reusable components
- Improve code organization and maintainability

[↑ Back to top](#table-of-contents)

## Key features

- **Concurrency unit** - Run multiple operations concurrently within your function
- **Execution isolation** - Child contexts have their own operation namespace
- **Single-unit checkpointing** - Completed child contexts never replay
- **Large data handling** - Process data that exceeds individual step limits
- **Named contexts** - Identify contexts by name for debugging and testing

[↑ Back to top](#table-of-contents)

## Getting started

Here's an example showing why child contexts are useful - they let you group multiple operations that execute as a single unit:

```python
from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
    durable_step,
    durable_with_child_context,
    StepContext,
)

@durable_step
def validate_order(step_context: StepContext, order_id: str) -> dict:
    """Validate order details."""
    # Validation logic here
    return {"valid": True, "order_id": order_id}

@durable_step
def reserve_inventory(step_context: StepContext, order_id: str) -> dict:
    """Reserve inventory for order."""
    # Inventory logic here
    return {"reserved": True, "order_id": order_id}

@durable_step
def charge_payment(step_context: StepContext, order_id: str) -> dict:
    """Charge payment for order."""
    # Payment logic here
    return {"charged": True, "order_id": order_id}

@durable_step
def send_confirmation(step_context: StepContext, result: dict) -> dict:
    """Send order confirmation."""
    # Notification logic here
    return {"sent": True, "order_id": result["order_id"]}

@durable_with_child_context
def process_order(ctx: DurableContext, order_id: str) -> dict:
    """Process an order with multiple steps."""
    # These three steps execute as a single unit
    validation = ctx.step(validate_order(order_id))
    inventory = ctx.step(reserve_inventory(order_id))
    payment = ctx.step(charge_payment(order_id))
    
    return {"order_id": order_id, "status": "completed"}

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    """Process order using a child context."""
    # Once this completes, it never replays - even if the function continues
    result = context.run_in_child_context(
        process_order(event["order_id"]),
        name="order_processing"
    )
    
    # Additional operations here won't cause process_order to replay
    context.step(send_confirmation(result))
    
    return result
```

**Why use a child context here?**

Child contexts let you group related operations into a logical unit. Once `process_order` completes, its result is saved just like a step - everything inside won't replay even if the function continues or restarts. This provides organizational benefits and a small optimization by avoiding unnecessary replays.

**Key benefits:**

- **Organization**: Group related operations together for better code structure and readability
- **Reusability**: Call `process_order` multiple times in the same function, and each execution is tracked independently
- **Isolation**: Child contexts act like checkpointed functions - once done, they're done

[↑ Back to top](#table-of-contents)

## Method signatures

### context.run_in_child_context()

```python
def run_in_child_context(
    func: Callable[[DurableContext], T],
    name: str | None = None,
) -> T
```

**Parameters:**

- `func` - A callable that receives a `DurableContext` and returns a result. Use the `@durable_with_child_context` decorator to create context functions.
- `name` (optional) - A name for the child context, useful for debugging and testing

**Returns:** The result of executing the context function.

**Raises:** Any exception raised by the context function.

### @durable_with_child_context decorator

```python
@durable_with_child_context
def my_context_function(ctx: DurableContext, arg1: str, arg2: int) -> dict:
    # Your operations here
    return result
```

The decorator wraps your function so it can be called with arguments and passed to `context.run_in_child_context()`.

[↑ Back to top](#table-of-contents)

## Using the @durable_with_child_context decorator

The `@durable_with_child_context` decorator marks a function as a context function. Context functions receive a `DurableContext` as their first parameter and can execute any durable operations:

```python
from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
    durable_with_child_context,
)

@durable_with_child_context
def process_order(ctx: DurableContext, order_id: str, items: list) -> dict:
    """Process an order in a child context."""
    # Validate items
    validation = ctx.step(
        lambda _: validate_items(items),
        name="validate_items"
    )
    
    if not validation["valid"]:
        return {"status": "invalid", "errors": validation["errors"]}
    
    # Calculate total
    total = ctx.step(
        lambda _: calculate_total(items),
        name="calculate_total"
    )
    
    # Process payment
    payment = ctx.step(
        lambda _: process_payment(order_id, total),
        name="process_payment"
    )
    
    return {
        "order_id": order_id,
        "total": total,
        "payment_status": payment["status"],
    }

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    """Process an order using a child context."""
    order_id = event["order_id"]
    items = event["items"]
    
    # Execute order processing in child context
    result = context.run_in_child_context(
        process_order(order_id, items)
    )
    
    return result
```

**Why use @durable_with_child_context?**

The decorator wraps your function so it can be called with arguments and passed to `context.run_in_child_context()`. It provides a convenient way to define reusable workflow components.

[↑ Back to top](#table-of-contents)

## Naming child contexts

You can name child contexts explicitly using the `name` parameter. Named contexts are easier to identify in logs and tests:

```python
@durable_with_child_context
def data_processing(ctx: DurableContext, data: dict) -> dict:
    """Process data in a child context."""
    result = ctx.step(lambda _: transform_data(data), name="transform")
    return result

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    # Named child context
    result = context.run_in_child_context(
        data_processing(event["data"]),
        name="data_processor"
    )
    return result
```

**Naming best practices:**

- Use descriptive names that explain what the context does
- Keep names consistent across your codebase
- Use names when you need to inspect specific contexts in tests
- Names help with debugging and monitoring

[↑ Back to top](#table-of-contents)

## Use cases for isolation

### Organizing complex workflows

Use child contexts to organize complex workflows into logical units:

```python
@durable_with_child_context
def inventory_check(ctx: DurableContext, items: list) -> dict:
    """Check inventory for all items."""
    results = []
    for item in items:
        available = ctx.step(
            lambda _: check_item_availability(item),
            name=f"check_{item['id']}"
        )
        results.append({"item_id": item["id"], "available": available})
    
    return {"all_available": all(r["available"] for r in results)}

@durable_with_child_context
def payment_processing(ctx: DurableContext, order_total: float) -> dict:
    """Process payment in isolated context."""
    auth = ctx.step(
        lambda _: authorize_payment(order_total),
        name="authorize"
    )
    
    if auth["approved"]:
        capture = ctx.step(
            lambda _: capture_payment(auth["transaction_id"]),
            name="capture"
        )
        return {"status": "completed", "transaction_id": capture["id"]}
    
    return {"status": "declined"}

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    """Process order with organized child contexts."""
    # Check inventory
    inventory = context.run_in_child_context(
        inventory_check(event["items"]),
        name="inventory_check"
    )
    
    if not inventory["all_available"]:
        return {"status": "failed", "reason": "items_unavailable"}
    
    # Process payment
    payment = context.run_in_child_context(
        payment_processing(event["total"]),
        name="payment_processing"
    )
    
    if payment["status"] != "completed":
        return {"status": "failed", "reason": "payment_declined"}
    
    return {
        "status": "success",
        "transaction_id": payment["transaction_id"],
    }
```

### Creating reusable components

Child contexts make it easy to create reusable workflow components:

```python
@durable_with_child_context
def send_notifications(ctx: DurableContext, user_id: str, message: str) -> dict:
    """Send notifications through multiple channels."""
    email_sent = ctx.step(
        lambda _: send_email(user_id, message),
        name="send_email"
    )
    
    sms_sent = ctx.step(
        lambda _: send_sms(user_id, message),
        name="send_sms"
    )
    
    push_sent = ctx.step(
        lambda _: send_push_notification(user_id, message),
        name="send_push"
    )
    
    return {
        "email": email_sent,
        "sms": sms_sent,
        "push": push_sent,
    }

@durable_execution
def order_confirmation_handler(event: dict, context: DurableContext) -> dict:
    """Send order confirmation notifications."""
    notifications = context.run_in_child_context(
        send_notifications(
            event["user_id"],
            f"Order {event['order_id']} confirmed"
        ),
        name="order_notifications"
    )
    
    return {"notifications_sent": notifications}

@durable_execution
def shipment_handler(event: dict, context: DurableContext) -> dict:
    """Send shipment notifications."""
    notifications = context.run_in_child_context(
        send_notifications(
            event["user_id"],
            f"Order {event['order_id']} shipped"
        ),
        name="shipment_notifications"
    )
    
    return {"notifications_sent": notifications}
```

[↑ Back to top](#table-of-contents)

## Advanced patterns

### Conditional child contexts

Execute child contexts based on conditions:

```python
@durable_with_child_context
def standard_processing(ctx: DurableContext, data: dict) -> dict:
    """Standard data processing."""
    result = ctx.step(lambda _: process_standard(data), name="process")
    return {"type": "standard", "result": result}

@durable_with_child_context
def premium_processing(ctx: DurableContext, data: dict) -> dict:
    """Premium data processing with extra steps."""
    enhanced = ctx.step(lambda _: enhance_data(data), name="enhance")
    validated = ctx.step(lambda _: validate_premium(enhanced), name="validate")
    result = ctx.step(lambda _: process_premium(validated), name="process")
    return {"type": "premium", "result": result}

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    """Process data based on customer tier."""
    customer_tier = event.get("tier", "standard")
    
    if customer_tier == "premium":
        result = context.run_in_child_context(
            premium_processing(event["data"]),
            name="premium_processing"
        )
    else:
        result = context.run_in_child_context(
            standard_processing(event["data"]),
            name="standard_processing"
        )
    
    return result
```

### Error handling in child contexts

Handle errors within child contexts:

```python
@durable_with_child_context
def risky_operation(ctx: DurableContext, data: dict) -> dict:
    """Operation that might fail."""
    try:
        result = ctx.step(
            lambda _: potentially_failing_operation(data),
            name="risky_step"
        )
        return {"status": "success", "result": result}
    except Exception as e:
        # Handle error within child context
        fallback = ctx.step(
            lambda _: fallback_operation(data),
            name="fallback"
        )
        return {"status": "fallback", "result": fallback, "error": str(e)}

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    """Handle errors in child context."""
    result = context.run_in_child_context(
        risky_operation(event["data"]),
        name="risky_operation"
    )
    
    if result["status"] == "fallback":
        # Log or handle fallback scenario
        return {"warning": "Used fallback", "result": result["result"]}
    
    return result
```

### Sequential child contexts

Execute multiple child contexts sequentially:

```python
@durable_with_child_context
def process_region_a(ctx: DurableContext, data: dict) -> dict:
    """Process data for region A."""
    result = ctx.step(lambda _: process_for_region("A", data), name="process_a")
    return {"region": "A", "result": result}

@durable_with_child_context
def process_region_b(ctx: DurableContext, data: dict) -> dict:
    """Process data for region B."""
    result = ctx.step(lambda _: process_for_region("B", data), name="process_b")
    return {"region": "B", "result": result}

@durable_with_child_context
def process_region_c(ctx: DurableContext, data: dict) -> dict:
    """Process data for region C."""
    result = ctx.step(lambda _: process_for_region("C", data), name="process_c")
    return {"region": "C", "result": result}

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    """Process data for multiple regions sequentially."""
    data = event["data"]
    
    # Execute child contexts sequentially
    result_a = context.run_in_child_context(
        process_region_a(data),
        name="region_a"
    )
    
    result_b = context.run_in_child_context(
        process_region_b(data),
        name="region_b"
    )
    
    result_c = context.run_in_child_context(
        process_region_c(data),
        name="region_c"
    )
    
    return {
        "regions_processed": 3,
        "results": [result_a, result_b, result_c],
    }
```

For parallel execution, use `context.parallel()` instead. See [Parallel operations](parallel.md) for details.

[↑ Back to top](#table-of-contents)

## Best practices

**Use child contexts for logical grouping** - Group related operations together in a child context to improve code organization and readability.

**Name contexts descriptively** - Use clear names that explain what the context does. This helps with debugging and testing.

**Keep context functions focused** - Each context function should have a single, well-defined purpose. Don't create overly complex context functions.

**Use child contexts for large data** - When processing data that exceeds step size limits, break it into multiple steps within a child context.

**Create reusable components** - Design context functions that can be reused across different workflows.

**Handle errors appropriately** - Decide whether to handle errors within the child context or let them propagate to the parent.

**Pass data through parameters** - Pass data to child contexts through function parameters, not global variables.

**Document context functions** - Add docstrings explaining what the context does and what it returns.

**Test context functions independently** - Write tests for individual context functions to ensure they work correctly in isolation.

[↑ Back to top](#table-of-contents)

## FAQ

**Q: What's the difference between a child context and a step?**

A: A step is a single operation that checkpoints its result. A child context is a collection of operations (steps, waits, callbacks, etc.) that execute in an isolated scope. The entire child context result is checkpointed as a single unit in the parent context.

**Q: Can I use steps inside child contexts?**

A: Yes, child contexts can contain any durable operations: steps, waits, and callbacks.

**Q: When should I use a child context vs multiple steps?**

A: Use child contexts when you want to:
- Group related operations logically
- Create reusable workflow components
- Handle data larger than step size limits
- Isolate operations from the parent context

Use multiple steps when operations are independent and don't need isolation.

**Q: Can child contexts access the parent context?**

A: No, child contexts receive their own `DurableContext` instance. They can't access the parent context directly. Pass data through function parameters.

**Q: What happens if a child context fails?**

A: If an operation within a child context raises an exception, the exception propagates to the parent context unless you handle it within the child context.

**Q: Can I create multiple child contexts in one function?**

A: Yes, you can create as many child contexts as needed. They execute sequentially by default. For parallel execution, use `context.parallel()` instead.

**Q: Can I use callbacks in child contexts?**

A: Yes, child contexts support all durable operations including callbacks, waits, and steps.

**Q: Can I pass large data to child contexts?**

A: Yes, but be mindful of Lambda payload limits. If data is very large, consider storing it externally (S3, DynamoDB) and passing references.

**Q: Do child contexts share the same logger?**

A: Yes, the logger is inherited from the parent context, but you can access it through the child context's `ctx.logger`.

[↑ Back to top](#table-of-contents)

## Testing

You can test child contexts using the testing SDK. The test runner executes your function and lets you inspect child context results.

### Basic child context testing

```python
import pytest
from aws_durable_execution_sdk_python_testing import InvocationStatus
from examples.src.run_in_child_context import run_in_child_context

@pytest.mark.durable_execution(
    handler=run_in_child_context.handler,
    lambda_function_name="run in child context",
)
def test_run_in_child_context(durable_runner):
    """Test basic child context execution."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)
    
    # Check overall status
    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result == "Child context result: 10"
```

### Inspecting child context operations

Use `result.get_context()` to inspect child context results:

```python
@pytest.mark.durable_execution(
    handler=run_in_child_context.handler,
    lambda_function_name="run in child context",
)
def test_child_context_operations(durable_runner):
    """Test and inspect child context operations."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)
    
    # Verify child context operation exists
    context_ops = [
        op for op in result.operations
        if op.operation_type.value == "CONTEXT"
    ]
    assert len(context_ops) >= 1
    
    # Get child context by name (if named)
    child_result = result.get_context("child_operation")
    assert child_result is not None
```

### Testing large data handling

Test that child contexts handle large data correctly:

```python
from examples.src.run_in_child_context import run_in_child_context_large_data

@pytest.mark.durable_execution(
    handler=run_in_child_context_large_data.handler,
    lambda_function_name="run in child context large data",
)
def test_large_data_processing(durable_runner):
    """Test large data handling with child context."""
    with durable_runner:
        result = durable_runner.run(input=None, timeout=30)
    
    result_data = result.result
    
    # Verify execution succeeded
    assert result.status is InvocationStatus.SUCCEEDED
    assert result_data["success"] is True
    
    # Verify large data was processed
    assert result_data["summary"]["totalDataSize"] > 240  # ~250KB
    assert result_data["summary"]["stepsExecuted"] == 5
    
    # Verify data integrity across wait
    assert result_data["dataIntegrityCheck"] is True
```



### Testing error handling

Test that child contexts handle errors correctly:

```python
@pytest.mark.durable_execution(
    handler=error_handling_handler,
    lambda_function_name="error_handling",
)
def test_child_context_error_handling(durable_runner):
    """Test error handling in child context."""
    with durable_runner:
        result = durable_runner.run(input={"data": "invalid"}, timeout=10)
    
    # Function should handle error gracefully
    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result["status"] == "fallback"
    assert "error" in result.result
```

For more testing patterns, see:
- [Basic tests](../testing-patterns/basic-tests.md) - Simple test examples
- [Complex workflows](../testing-patterns/complex-workflows.md) - Multi-step workflow testing
- [Best practices](../testing-patterns/best-practices.md) - Testing recommendations

[↑ Back to top](#table-of-contents)

## See also

- [DurableContext API](../api-reference/context.md) - Complete context reference
- [Steps](steps.md) - Use steps within child contexts
- [Wait operations](wait.md) - Use waits within child contexts
- [Callbacks](callbacks.md) - Use callbacks within child contexts
- [Parallel operations](parallel.md) - Execute child contexts in parallel
- [Examples](https://github.com/awslabs/aws-durable-execution-sdk-python/tree/main/examples/src/run_in_child_context) - More child context examples

[↑ Back to top](#table-of-contents)

## License

See the LICENSE file for our project's licensing.

[↑ Back to top](#table-of-contents)
