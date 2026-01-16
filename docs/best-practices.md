# Best Practices

## Table of Contents

- [Overview](#overview)
- [Function design](#function-design)
- [Timeout configuration](#timeout-configuration)
- [Naming conventions](#naming-conventions)
- [Performance optimization](#performance-optimization)
- [Serialization](#serialization)
- [Common mistakes](#common-mistakes)
- [Code organization](#code-organization)
- [FAQ](#faq)
- [See also](#see-also)

[← Back to main index](index.md)

## Overview

This guide covers best practices for building reliable, maintainable durable functions. You'll learn how to design functions that are easy to test, debug, and maintain in production.

[↑ Back to top](#table-of-contents)

## Function design

### Keep functions focused

Each durable function should have a single, clear purpose. Focused functions are easier to test, debug, and maintain. They also make it simpler to understand execution flow and identify failures.

**Good:**

```python
@durable_execution
def process_order(event: dict, context: DurableContext) -> dict:
    """Process a single order through validation, payment, and fulfillment."""
    order_id = event["order_id"]
    
    validation = context.step(validate_order(order_id))
    payment = context.step(process_payment(order_id, event["amount"]))
    fulfillment = context.step(fulfill_order(order_id))
    
    return {"order_id": order_id, "status": "completed"}
```

**Avoid:**

```python
@durable_execution
def process_everything(event: dict, context: DurableContext) -> dict:
    """Process orders, update inventory, send emails, generate reports..."""
    # Too many responsibilities - hard to test and maintain
    # If one part fails, the entire function needs to retry
    pass
```

### Wrap non-deterministic code in steps

All non-deterministic operations must be wrapped in steps:

```python
@durable_step
def get_timestamp(step_context: StepContext) -> int:
    return int(time.time())

@durable_step
def generate_id(step_context: StepContext) -> str:
    return str(uuid.uuid4())

@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    timestamp = context.step(get_timestamp())
    request_id = context.step(generate_id())
    return {"timestamp": timestamp, "request_id": request_id}
```

**Why:** Non-deterministic code produces different values on replay, breaking state consistency.


### Use @durable_step for reusable functions

Decorate functions with `@durable_step` to get automatic naming, better code organization, and cleaner syntax. This makes your code more maintainable and easier to test.

**Good:**

```python
@durable_step
def validate_input(step_context: StepContext, data: dict) -> bool:
    return all(key in data for key in ["name", "email"])

@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    is_valid = context.step(validate_input(event))
    return {"valid": is_valid}
```

**Avoid:**

```python
@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    # Lambda functions require explicit names and are harder to test
    is_valid = context.step(
        lambda _: all(key in event for key in ["name", "email"]),
        name="validate_input"
    )
    return {"valid": is_valid}
```

### Don't share state between steps

Pass data through return values, not global variables or class attributes. Global state breaks on replay because steps return cached results, but global variables reset to their initial values.

**Good:**

```python
@durable_step
def fetch_user(step_context: StepContext, user_id: str) -> dict:
    return {"user_id": user_id, "name": "Jane Doe"}

@durable_step
def send_email(step_context: StepContext, user: dict) -> bool:
    send_to_address(user["name"], user.get("email"))
    return True

@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    user = context.step(fetch_user(event["user_id"]))
    sent = context.step(send_email(user))
    return {"sent": sent}
```

**Avoid:**

```python
# DON'T: Global state
current_user = None

@durable_step
def fetch_user(step_context: StepContext, user_id: str) -> dict:
    global current_user
    current_user = {"user_id": user_id, "name": "Jane Doe"}
    return current_user

@durable_step
def send_email(step_context: StepContext) -> bool:
    # On replay, current_user might be None!
    send_to_address(current_user["name"], current_user.get("email"))
    return True

@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    # First execution: works fine
    # On replay: fetch_user returns cached result but doesn't set global variable
    # send_email crashes because current_user is None
    user = context.step(fetch_user(event["user_id"]))
    sent = context.step(send_email())
    return {"sent": sent}
```

### Choose the right execution semantics

Use at-most-once semantics for operations with side effects (payments, emails, database writes) to prevent duplicate execution. Use at-least-once (default) for idempotent operations that are safe to retry.

**At-most-once for side effects:**

```python
from aws_durable_execution_sdk_python.config import StepConfig, StepSemantics

@durable_step
def charge_credit_card(step_context: StepContext, amount: float) -> dict:
    return {"transaction_id": "txn_123", "status": "completed"}

@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    # Prevent duplicate charges on retry
    payment = context.step(
        charge_credit_card(event["amount"]),
        config=StepConfig(step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY),
    )
    return payment
```

**At-least-once for idempotent operations:**

```python
@durable_step
def calculate_total(step_context: StepContext, items: list) -> float:
    return sum(item["price"] for item in items)

@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> float:
    # Safe to run multiple times - same input produces same output
    total = context.step(calculate_total(event["items"]))
    return total
```

### Handle errors explicitly

Catch and handle exceptions in your step functions. Distinguish between transient failures (network issues, rate limits) that should retry, and permanent failures (invalid input, not found) that shouldn't.

**Good:**

```python
@durable_step
def call_external_api(step_context: StepContext, url: str) -> dict:
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.Timeout:
        raise  # Let retry handle timeouts
    except requests.HTTPError as e:
        if e.response.status_code >= 500:
            raise  # Retry server errors
        # Don't retry client errors (400-499)
        return {"error": "client_error", "status": e.response.status_code}
```

**Avoid:**

```python
@durable_step
def call_external_api(step_context: StepContext, url: str) -> dict:
    # No error handling - all errors cause retry, even permanent ones
    response = requests.get(url)
    return response.json()
```

[↑ Back to top](#table-of-contents)

## Timeout configuration

### Set realistic timeouts

Choose timeout values based on expected execution time plus buffer for retries and network delays. Too short causes unnecessary failures; too long wastes resources waiting for operations that won't complete.

**Good:**

```python
from aws_durable_execution_sdk_python.config import CallbackConfig, Duration

@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    # Expected 2 minutes + 1 minute buffer = 3 minutes
    callback = context.create_callback(
        name="approval",
        config=CallbackConfig(timeout=Duration.from_minutes(3)),
    )
    return {"callback_id": callback.callback_id}
```

**Avoid:**

```python
@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    # Too short - will timeout before external system responds
    callback = context.create_callback(
        name="approval",
        config=CallbackConfig(timeout=Duration.from_seconds(5)),
    )
    return {"callback_id": callback.callback_id}
```

### Use heartbeat timeouts for long operations

Enable heartbeat monitoring for callbacks that take more than a few minutes. Heartbeats detect when external systems stop responding, preventing you from waiting the full timeout period.

```python
callback = context.create_callback(
    name="approval",
    config=CallbackConfig(
        timeout=Duration.from_hours(24),  # Maximum wait time
        heartbeat_timeout=Duration.from_hours(2),  # Fail if no heartbeat for 2 hours
    ),
)
```

Without heartbeat monitoring, you'd wait the full 24 hours even if the external system crashes after 10 minutes.

### Configure retry delays appropriately

```python
from aws_durable_execution_sdk_python.retries import RetryStrategyConfig

# Fast retry for transient network issues
fast_retry = RetryStrategyConfig(
    max_attempts=3,
    initial_delay_seconds=1,
    max_delay_seconds=5,
    backoff_rate=2.0,
)

# Slow retry for rate limiting
slow_retry = RetryStrategyConfig(
    max_attempts=5,
    initial_delay_seconds=10,
    max_delay_seconds=60,
    backoff_rate=2.0,
)
```

[↑ Back to top](#table-of-contents)

## Naming conventions

### Use descriptive operation names

Choose names that explain what the operation does, not how it does it. Good names make logs easier to read and help you identify which operation failed.

**Good:**

```python
@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    user = context.step(fetch_user(event["user_id"]), name="fetch_user")
    validated = context.step(validate_user(user), name="validate_user")
    notification = context.step(send_notification(user), name="send_notification")
    return {"status": "completed"}
```

**Avoid:**

```python
@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    # Generic names don't help with debugging
    user = context.step(fetch_user(event["user_id"]), name="step1")
    validated = context.step(validate_user(user), name="step2")
    notification = context.step(send_notification(user), name="step3")
    return {"status": "completed"}
```

### Use consistent naming patterns

```python
# Pattern: verb_noun for operations
context.step(validate_order(order_id), name="validate_order")
context.step(process_payment(amount), name="process_payment")

# Pattern: noun_action for callbacks
context.create_callback(name="payment_callback")
context.create_callback(name="approval_callback")

# Pattern: descriptive_wait for waits
context.wait(Duration.from_seconds(30), name="payment_confirmation_wait")
```

### Name dynamic operations with context

Include context when creating operations in loops:

```python
@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> list:
    results = []
    for i, item in enumerate(event["items"]):
        result = context.step(
            process_item(item),
            name=f"process_item_{i}_{item['id']}"
        )
        results.append(result)
    return results
```

[↑ Back to top](#table-of-contents)

## Performance optimization

### Minimize checkpoint size

Keep operation inputs and results small. Large payloads increase checkpoint overhead, slow down execution, and can hit size limits. Store large data in S3 and pass references instead.

**Good:**

```python
@durable_step
def process_large_dataset(step_context: StepContext, s3_key: str) -> str:
    data = download_from_s3(s3_key)
    result = process_data(data)
    result_key = upload_to_s3(result)
    return result_key  # Small checkpoint - just the S3 key

@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    result_key = context.step(process_large_dataset(event["s3_key"]))
    return {"result_key": result_key}
```

**Avoid:**

```python
@durable_step
def process_large_dataset(step_context: StepContext, data: list) -> list:
    return process_data(data)  # Large checkpoint!

@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    # Passing megabytes of data through checkpoints
    large_data = download_from_s3(event["s3_key"])
    result = context.step(process_large_dataset(large_data))
    return {"result": result}  # Another large checkpoint!
```

### Batch operations when possible

Group related operations to reduce checkpoint overhead. Each step creates a checkpoint, so batching reduces API calls and speeds up execution.

**Good:**

```python
@durable_step
def process_batch(step_context: StepContext, items: list) -> list:
    return [process_item(item) for item in items]

@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> list:
    items = event["items"]
    results = []
    
    # Process 10 items per step instead of 1
    for i in range(0, len(items), 10):
        batch = items[i:i+10]
        batch_results = context.step(
            process_batch(batch),
            name=f"process_batch_{i//10}"
        )
        results.extend(batch_results)
    
    return results
```

**Avoid:**

```python
@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> list:
    results = []
    # Creating a step for each item - too many checkpoints!
    for i, item in enumerate(event["items"]):
        result = context.step(
            lambda _, item=item: process_item(item),
            name=f"process_item_{i}"
        )
        results.append(result)
    return results
```

### Use parallel operations for independent work

Execute independent operations concurrently to reduce total execution time. Use `context.parallel()` to run multiple operations at the same time.

**Good:**

```python
@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    # Execute all three operations concurrently
    results = context.parallel(
        fetch_user_data(event["user_id"]),
        fetch_order_history(event["user_id"]),
        fetch_preferences(event["user_id"]),
    )
    
    return {
        "user": results[0],
        "orders": results[1],
        "preferences": results[2],
    }
```

**Avoid:**

```python
@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    # Sequential execution - each step waits for the previous one
    user_data = context.step(fetch_user_data(event["user_id"]))
    order_history = context.step(fetch_order_history(event["user_id"]))
    preferences = context.step(fetch_preferences(event["user_id"]))
    
    return {
        "user": user_data,
        "orders": order_history,
        "preferences": preferences,
    }
```

### Avoid unnecessary waits

Only use waits when you need to delay execution:

```python
@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    job_id = context.step(start_job(event["data"]))
    context.wait(Duration.from_seconds(30), name="job_processing_wait")  # Necessary
    result = context.step(check_job_status(job_id))
    return result
```

[↑ Back to top](#table-of-contents)

## Serialization

### Use JSON-serializable types

The SDK uses JSON serialization by default for checkpoints. Stick to JSON-compatible types (dict, list, str, int, float, bool, None) for operation inputs and results.

**Good:**

```python
@durable_step
def process_order(step_context: StepContext, order: dict) -> dict:
    return {
        "order_id": order["id"],
        "total": 99.99,
        "items": ["item1", "item2"],
        "processed": True,
    }
```

**Avoid:**

```python
from datetime import datetime
from decimal import Decimal

@durable_step
def process_order(step_context: StepContext, order: dict) -> dict:
    # datetime and Decimal aren't JSON-serializable by default
    return {
        "order_id": order["id"],
        "total": Decimal("99.99"),  # Won't serialize!
        "timestamp": datetime.now(),  # Won't serialize!
    }
```

### Convert non-serializable types

Convert complex types to JSON-compatible formats before returning from steps:

```python
from datetime import datetime
from decimal import Decimal

@durable_step
def process_order(step_context: StepContext, order: dict) -> dict:
    return {
        "order_id": order["id"],
        "total": float(Decimal("99.99")),  # Convert to float
        "timestamp": datetime.now().isoformat(),  # Convert to string
    }
```

### Use custom serialization for complex types

For complex objects, implement custom serialization or use the SDK's SerDes system:

```python
from dataclasses import dataclass, asdict

@dataclass
class Order:
    order_id: str
    total: float
    items: list

@durable_step
def process_order(step_context: StepContext, order_data: dict) -> dict:
    order = Order(**order_data)
    # Process order...
    return asdict(order)  # Convert dataclass to dict
```

[↑ Back to top](#table-of-contents)

## Common mistakes

### ⚠️ Modifying mutable objects between steps

**Wrong:**

```python
@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    data = {"count": 0}
    context.step(increment_count(data))
    data["count"] += 1  # DON'T: Mutation outside step
    return data
```

**Right:**

```python
@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    data = {"count": 0}
    data = context.step(increment_count(data))
    data = context.step(increment_count(data))
    return data
```

### ⚠️ Using context inside its own operations

**Wrong:**

```python
@durable_step
def process_with_wait(step_context: StepContext, context: DurableContext) -> str:
    # DON'T: Can't use context inside its own step operation
    context.wait(Duration.from_seconds(1))  # Error: using context inside step!
    result = context.step(nested_step(), name="step2")  # Error: nested context.step!
    return result

@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    # This will fail - context is being used inside its own step
    result = context.step(process_with_wait(context), name="step1")
    return {"result": result}
```

**Right:**

```python
@durable_step
def nested_step(step_context: StepContext) -> str:
    return "nested step"

@durable_with_child_context
def process_with_wait(child_ctx: DurableContext) -> str:
    # Use child context for nested operations
    child_ctx.wait(seconds=1)
    result = child_ctx.step(nested_step(), name="step2")
    return result

@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    # Use run_in_child_context for nested operations
    result = context.run_in_child_context(
        process_with_wait(),
        name="block1"
    )
    return {"result": result}
```

**Why:** You can't use a context object inside its own operations (like calling `context.step()` inside another `context.step()`). Use child contexts to create isolated execution scopes for nested operations.

### ⚠️ Forgetting to handle callback timeouts

**Wrong:**

```python
@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    callback = context.create_callback(name="approval")
    result = callback.result()
    return {"approved": result["approved"]}  # Crashes if timeout!
```

**Right:**

```python
@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    callback = context.create_callback(name="approval")
    result = callback.result()
    
    if result is None:
        return {"status": "timeout", "approved": False}
    
    return {"status": "completed", "approved": result.get("approved", False)}
```

### ⚠️ Creating too many small steps

**Wrong:**

```python
@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    a = context.step(lambda _: event["a"])
    b = context.step(lambda _: event["b"])
    sum_val = context.step(lambda _: a + b)
    return {"result": sum_val}
```

**Right:**

```python
@durable_step
def calculate_result(step_context: StepContext, a: int, b: int) -> int:
    return a + b

@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    result = context.step(calculate_result(event["a"], event["b"]))
    return {"result": result}
```

### ⚠️ Not using retry for transient failures

**Right:**

```python
from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.retries import (
    RetryStrategyConfig,
    create_retry_strategy,
)

@durable_step
def call_api(step_context: StepContext, url: str) -> dict:
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()

@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    retry_config = RetryStrategyConfig(
        max_attempts=3,
        retryable_error_types=[requests.Timeout, requests.ConnectionError],
    )
    
    result = context.step(
        call_api(event["url"]),
        config=StepConfig(retry_strategy=create_retry_strategy(retry_config)),
    )
    return result
```

[↑ Back to top](#table-of-contents)

## Code organization

### Separate business logic from orchestration

```python
# business_logic.py
@durable_step
def validate_order(step_context: StepContext, order: dict) -> dict:
    if not order.get("items"):
        raise ValueError("Order must have items")
    return {**order, "validated": True}

# handler.py
@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    order = event["order"]
    validated_order = context.step(validate_order(order))
    return {"status": "completed", "order_id": validated_order["order_id"]}
```

### Use child contexts for complex workflows

```python
@durable_with_child_context
def validate_and_enrich(ctx: DurableContext, data: dict) -> dict:
    validated = ctx.step(validate_data(data))
    enriched = ctx.step(enrich_data(validated))
    return enriched

@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    enriched = context.run_in_child_context(
        validate_and_enrich(event["data"]),
        name="validation_phase",
    )
    return enriched
```

### Group related configuration

```python
# config.py
from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.retries import (
    RetryStrategyConfig,
    create_retry_strategy,
)

FAST_RETRY = StepConfig(
    retry_strategy=create_retry_strategy(
        RetryStrategyConfig(
            max_attempts=3,
            initial_delay_seconds=1,
            max_delay_seconds=5,
            backoff_rate=2.0,
        )
    )
)

# handler.py
from config import FAST_RETRY

@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    data = context.step(fetch_data(event["id"]), config=FAST_RETRY)
    return data
```

[↑ Back to top](#table-of-contents)

## FAQ

**Q: How many steps should a durable function have?**

A: There's a limit of 3,000 operations per execution. Keep in mind that more steps mean more API operations and longer execution time. Balance granularity with performance - group related operations when it makes sense, but don't hesitate to break down complex logic into steps.

**Q: Should I create a step for every function call?**

A: No. Only create steps for operations that need checkpointing, retry logic, or isolation.

**Q: Can I use async/await in durable functions?**

A: Functions decorated with `@durable_step` must be synchronous. If you need to call async code, use `asyncio.run()` inside your step to execute it synchronously.

**Q: How do I handle secrets and credentials?**

A: Use AWS Secrets Manager or Parameter Store. Fetch secrets in a step at the beginning of your workflow.

**Q: What's the maximum execution time for a durable function?**

A: Durable functions can run for days or weeks using waits and callbacks. Each individual Lambda invocation is still subject to the 15-minute Lambda timeout.

**Q: How do I test durable functions locally?**

A: Use the testing SDK (`aws-durable-execution-sdk-python-testing`) to run functions locally without AWS credentials. See [Testing patterns](testing-patterns/basic-tests.md) for examples.

**Q: How do I monitor durable functions in production?**

A: Use CloudWatch Logs for execution logs, CloudWatch Metrics for performance metrics, and X-Ray for distributed tracing.

[↑ Back to top](#table-of-contents)

## See also

- [Getting started](getting-started.md) - Build your first durable function
- [Steps](core/steps.md) - Step operations
- [Error handling](advanced/error-handling.md) - Handle failures
- [Configuration](api-reference/config.md) - Configuration options
- [Testing patterns](testing-patterns/basic-tests.md) - How to test your functions

[↑ Back to top](#table-of-contents)

## License

See the [LICENSE](../LICENSE) file for our project's licensing.

[↑ Back to top](#table-of-contents)
