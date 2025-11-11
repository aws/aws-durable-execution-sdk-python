# Steps

## Table of Contents

- [What are steps?](#what-are-steps)
- [Terminology](#terminology)
- [Key features](#key-features)
- [Getting started](#getting-started)
- [Method signature](#method-signature)
- [Using the @durable_step decorator](#using-the-durable_step-decorator)
- [Naming steps](#naming-steps)
- [Configuration](#configuration)
- [Advanced patterns](#advanced-patterns)
- [Best practices](#best-practices)
- [FAQ](#faq)
- [Testing](#testing)
- [See also](#see-also)

[← Back to main index](../index.md)

## Terminology

**Step** - A durable operation that executes a function and checkpoints its result. Created using `context.step()`.

**Step function** - A function decorated with `@durable_step` that can be executed as a step. Receives a `StepContext` as its first parameter.

**Checkpoint** - A saved state of execution that allows your function to resume from a specific point. The SDK creates checkpoints automatically after each step completes.

**Replay** - The process of re-executing your function code when resuming from a checkpoint. Completed steps return their saved results instantly without re-executing.

**Step semantics** - Controls how many times a step executes per retry attempt. At-least-once (default) re-executes on retry. At-most-once executes only once per retry attempt.

**StepContext** - A context object passed to step functions containing metadata about the current execution.

[↑ Back to top](#table-of-contents)

## What are steps?

Steps are the fundamental building blocks of durable functions. A step is a unit of work that executes your code and automatically checkpoints the result. A completed step won't execute again, it returns its saved result instantly. If a step fails to complete, it automatically retries and saves the error after all retry attempts are exhausted.

Use steps to:
- Execute business logic with automatic checkpointing
- Retry operations that might fail
- Control execution semantics (at-most-once or at-least-once)
- Break complex workflows into manageable units

[↑ Back to top](#table-of-contents)

## Key features

- **Automatic checkpointing** - Results are saved automatically after execution
- **Configurable retry** - Define retry strategies with custom backoff
- **Execution semantics** - Choose at-most-once or at-least-once per retry
- **Named operations** - Identify steps by name for debugging and testing
- **Custom serialization** - Control how inputs and results are serialized
- **Instant replay** - Completed steps return saved results without re-executing

[↑ Back to top](#table-of-contents)

## Getting started

Here's a simple example of using steps:

```python
from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
    durable_step,
    StepContext,
)

@durable_step
def add_numbers(step_context: StepContext, a: int, b: int) -> int:
    """Add two numbers together."""
    return a + b

@durable_execution
def handler(event: dict, context: DurableContext) -> int:
    """Simple durable function with a step."""
    result = context.step(add_numbers(5, 3))
    return result
```

When this function runs:
1. `add_numbers(5, 3)` executes and returns 8
2. The result is checkpointed automatically
3. If the durable function replays, the step returns 8 instantly without re-executing the `add_numbers` function

[↑ Back to top](#table-of-contents)

## Method signature

### context.step()

```python
def step(
    func: Callable[[StepContext], T],
    name: str | None = None,
    config: StepConfig | None = None,
) -> T
```

**Parameters:**

- `func` - A callable that receives a `StepContext` and returns a result. Use the `@durable_step` decorator to create step functions.
- `name` (optional) - A name for the step, useful for debugging. If you decorate `func` with `@durable_step`, the SDK uses the function's name automatically.
- `config` (optional) - A `StepConfig` object to configure retry behavior, execution semantics, and serialization.

**Returns:** The result of executing the step function.

**Raises:** Any exception raised by the step function (after retries are exhausted if configured).

[↑ Back to top](#table-of-contents)

## Using the @durable_step decorator

The `@durable_step` decorator marks a function as a step function. Step functions receive a `StepContext` as their first parameter:

```python
from aws_durable_execution_sdk_python import durable_step, StepContext

@durable_step
def validate_order(step_context: StepContext, order_id: str) -> dict:
    """Validate an order."""
    # Your validation logic here
    return {"order_id": order_id, "valid": True}

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    order_id = event["order_id"]
    validation = context.step(validate_order(order_id))
    return validation
```

**Why use @durable_step?**

The decorator wraps your function so it can be called with arguments and passed to `context.step()`. It also automatically uses the wrapped function's name as the step's name. You can optionally use lambda functions instead:

```python
# With @durable_step (recommended)
result = context.step(validate_order(order_id))

# Optionally, use a lambda function
result = context.step(lambda _: validate_order_logic(order_id))
```

**StepContext parameter:**

The `StepContext` provides metadata about the current execution. While you must include it in your function signature, you typically don't need to use it unless you need execution metadata or custom logging.

[↑ Back to top](#table-of-contents)

## Naming steps

You can name steps explicitly using the `name` parameter. Named steps are easier to identify in logs and tests:

```python
@durable_execution
def handler(event: dict, context: DurableContext) -> str:
    # Explicit name
    result = context.step(
        lambda _: "Step with explicit name",
        name="custom_step"
    )
    return f"Result: {result}"
```

If you don't provide a name, the SDK uses the function's name automatically when using `@durable_step`:

```python
@durable_step
def process_payment(step_context: StepContext, amount: float) -> dict:
    return {"status": "completed", "amount": amount}

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    # Step is automatically named "process_payment"
    result = context.step(process_payment(100.0))
    return result
```

**Naming best practices:**

- Use descriptive names that explain what the step does
- Keep names consistent across your codebase
- Use names when you need to inspect specific steps in tests
- Let the SDK auto-name steps when using `@durable_step`

**Note:** Names don't need to be unique, but using distinct names improves observability when debugging or monitoring your workflows.

[↑ Back to top](#table-of-contents)

## Configuration

Configure step behavior using `StepConfig`:

```python
from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
    durable_step,
    StepContext,
)
from aws_durable_execution_sdk_python.config import StepConfig, StepSemantics
from aws_durable_execution_sdk_python.retries import (
    RetryStrategyConfig,
    create_retry_strategy,
)

@durable_step
def process_data(step_context: StepContext, data: str) -> dict:
    """Process data with potential for transient failures."""
    # Your processing logic here
    return {"processed": data, "status": "completed"}

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    # Create a retry strategy
    retry_config = RetryStrategyConfig(
        max_attempts=3,
        retryable_error_types=[RuntimeError, ValueError],
    )
    
    # Configure the step
    step_config = StepConfig(
        retry_strategy=create_retry_strategy(retry_config),
        step_semantics=StepSemantics.AT_LEAST_ONCE_PER_RETRY,
    )
    
    # Use the configuration
    result = context.step(process_data(event["data"]), config=step_config)
    return result
```

### StepConfig parameters

**retry_strategy** - A function that determines whether to retry after an exception. Use `create_retry_strategy()` to build one from `RetryStrategyConfig`.

**step_semantics** - Controls execution behavior on retry:
- `AT_LEAST_ONCE_PER_RETRY` (default) - Step re-executes on each retry attempt
- `AT_MOST_ONCE_PER_RETRY` - Step executes only once per retry attempt, even if the function is replayed

**serdes** - Custom serialization/deserialization for the step result. If not provided, uses JSON serialization.

[↑ Back to top](#table-of-contents)

## Advanced patterns

### Retry with exponential backoff

Configure steps to retry with exponential backoff when they fail:

```python
from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
)
from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.retries import (
    RetryStrategyConfig,
    create_retry_strategy,
)

@durable_execution
def handler(event: dict, context: DurableContext) -> str:
    # Configure exponential backoff
    retry_config = RetryStrategyConfig(
        max_attempts=3,
        initial_delay_seconds=1,
        max_delay_seconds=10,
        backoff_rate=2.0,
    )
    
    step_config = StepConfig(
        retry_strategy=create_retry_strategy(retry_config)
    )
    
    result = context.step(
        lambda _: "Step with exponential backoff",
        name="retry_step",
        config=step_config,
    )
    return f"Result: {result}"
```

This configuration:
- Retries up to 3 times
- Waits 1 second before the first retry
- Doubles the wait time for each subsequent retry (2s, 4s, 8s)
- Caps the wait time at 10 seconds

### Retry specific exceptions

Only retry certain types of errors:

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
def unreliable_operation(step_context: StepContext) -> str:
    """Operation that might fail."""
    if random() > 0.5:
        raise RuntimeError("Random error occurred")
    return "Operation succeeded"

@durable_execution
def handler(event: dict, context: DurableContext) -> str:
    # Only retry RuntimeError, not other exceptions
    retry_config = RetryStrategyConfig(
        max_attempts=3,
        retryable_error_types=[RuntimeError],
    )
    
    result = context.step(
        unreliable_operation(),
        config=StepConfig(create_retry_strategy(retry_config)),
    )
    
    return result
```

### At-most-once semantics

Use at-most-once semantics when your step has side effects that shouldn't be repeated:

```python
from aws_durable_execution_sdk_python.config import StepConfig, StepSemantics

@durable_step
def charge_credit_card(step_context: StepContext, amount: float) -> dict:
    """Charge a credit card - should only happen once."""
    # Payment processing logic
    return {"transaction_id": "txn_123", "status": "completed"}

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    # Use at-most-once to prevent duplicate charges
    step_config = StepConfig(
        step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY
    )
    
    payment = context.step(
        charge_credit_card(event["amount"]),
        config=step_config,
    )
    
    return payment
```

With at-most-once semantics:
- The step executes only once per retry attempt
- If the function replays due to Lambda recycling, the step returns the saved result
- Use this for operations with side effects like payments, emails, or database writes

### Multiple steps in sequence

Chain multiple steps together to build complex workflows:

```python
@durable_step
def fetch_user(step_context: StepContext, user_id: str) -> dict:
    """Fetch user data."""
    return {"user_id": user_id, "name": "Jane Doe", "email": "jane_doe@example.com"}

@durable_step
def validate_user(step_context: StepContext, user: dict) -> bool:
    """Validate user data."""
    return user.get("email") is not None

@durable_step
def send_notification(step_context: StepContext, user: dict) -> dict:
    """Send notification to user."""
    return {"sent": True, "email": user["email"]}

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    user_id = event["user_id"]
    
    # Step 1: Fetch user
    user = context.step(fetch_user(user_id))
    
    # Step 2: Validate user
    is_valid = context.step(validate_user(user))
    
    if not is_valid:
        return {"status": "failed", "reason": "invalid_user"}
    
    # Step 3: Send notification
    notification = context.step(send_notification(user))
    
    return {
        "status": "completed",
        "user_id": user_id,
        "notification_sent": notification["sent"],
    }
```

Each step is checkpointed independently. If the function is interrupted after step 1, it resumes at step 2 without re-fetching the user.

[↑ Back to top](#table-of-contents)

## Best practices

**Use @durable_step for reusable functions** - Decorate functions you'll use as steps to get automatic naming and convenient with succinct syntax.

**Name steps for debugging** - Use explicit names for steps you'll need to inspect in logs or tests.

**Keep steps focused** - Each step should do one thing. Break complex operations into multiple steps.

**Use retry for transient failures** - Configure retry strategies for operations that might fail temporarily (network calls, rate limits).

**Choose semantics carefully** - Use at-most-once for operations with side effects. Use at-least-once (default) for idempotent operations.

**Don't share state between steps** - Pass data between steps through return values, not global variables.

**Wrap non-deterministic code in steps** - All non-deterministic code, such as random values or timestamps, must be wrapped in a step. Once the step completes, the result won't change on replay.

**Handle errors explicitly** - Catch and handle exceptions in your step functions. Let retries handle transient failures.

[↑ Back to top](#table-of-contents)

## FAQ

**Q: What's the difference between a step and a regular function call?**

A: A step is checkpointed automatically. Completed steps return their saved results without re-executing. Regular function calls execute every time your function runs.

**Q: When should I use at-most-once vs at-least-once semantics?**

A: Use at-most-once for operations with side effects (payments, emails, database writes). Use at-least-once (default) for idempotent operations (calculations, data transformations).

**Q: Can I use async functions as steps?**

A: No, step functions must be synchronous. If you need to call async code, use `asyncio.run()` inside your step function.

**Q: How do I pass multiple arguments to a step?**

A: Use the `@durable_step` decorator and pass arguments when calling the function:

```python
@durable_step
def my_step(step_context: StepContext, arg1: str, arg2: int) -> str:
    return f"{arg1}: {arg2}"

result = context.step(my_step("value", 42))
```

**Q: Can I nest steps inside other steps?**

A: No, you can't call `context.step()` inside a step function. Steps are leaf operations. Use child contexts if you need nested operations.

**Q: What happens if a step raises an exception?**

A: If no retry strategy is configured, the exception propagates and fails the execution. If retry is configured, the SDK retries according to your strategy. After exhausting retries, the step checkpoints the error and the exception propagates.

**Q: How do I access the StepContext?**

A: The `StepContext` is passed as the first parameter to your step function. It contains metadata about the execution, though you typically don't need to use it.

**Q: Can I use lambda functions as steps?**

A: Yes, but they won't have automatic names:

```python
result = context.step(lambda _: "some value", name="my_step")
```

Use `@durable_step` for better ergonomics.

[↑ Back to top](#table-of-contents)

## Testing

You can test steps using the testing SDK. The test runner executes your function and lets you inspect step results.

### Basic step testing

```python
import pytest
from aws_durable_execution_sdk_python_testing import InvocationStatus
from my_function import handler

@pytest.mark.durable_execution(
    handler=handler,
    lambda_function_name="my_function",
)
def test_step(durable_runner):
    """Test a function with steps."""
    with durable_runner:
        result = durable_runner.run(input={"data": "test"}, timeout=10)
    
    # Check overall status
    assert result.status is InvocationStatus.SUCCEEDED
    
    # Check final result
    assert result.result == 8
```

### Inspecting step results

Use `result.get_step()` to inspect individual step results:

```python
@pytest.mark.durable_execution(
    handler=handler,
    lambda_function_name="my_function",
)
def test_step_result(durable_runner):
    """Test and inspect step results."""
    with durable_runner:
        result = durable_runner.run(input={"data": "test"}, timeout=10)
    
    # Get step by name
    step_result = result.get_step("add_numbers")
    assert step_result.result == 8
    
    # Check step status
    assert step_result.status is InvocationStatus.SUCCEEDED
```

### Testing retry behavior

Test that steps retry correctly on failure:

```python
@pytest.mark.durable_execution(
    handler=handler_with_retry,
    lambda_function_name="retry_function",
)
def test_step_retry(durable_runner):
    """Test step retry behavior."""
    with durable_runner:
        result = durable_runner.run(input={}, timeout=30)
    
    # Function should eventually succeed after retries
    assert result.status is InvocationStatus.SUCCEEDED
    
    # Inspect the step that retried
    step_result = result.get_step("unreliable_operation")
    assert step_result.status is InvocationStatus.SUCCEEDED
```

### Testing error handling

Test that steps fail correctly when errors occur:

```python
@pytest.mark.durable_execution(
    handler=handler_with_error,
    lambda_function_name="error_function",
)
def test_step_error(durable_runner):
    """Test step error handling."""
    with durable_runner:
        result = durable_runner.run(input={}, timeout=10)
    
    # Function should fail
    assert result.status is InvocationStatus.FAILED
    
    # Check the error
    assert "RuntimeError" in str(result.error)
```

For more testing patterns, see:
- [Basic tests](../testing-patterns/basic-tests.md) - Simple test examples
- [Complex workflows](../testing-patterns/complex-workflows.md) - Multi-step workflow testing
- [Best practices](../testing-patterns/best-practices.md) - Testing recommendations

[↑ Back to top](#table-of-contents)

## See also

- [DurableContext API](../api-reference/context.md) - Complete context reference
- [StepConfig](../api-reference/config.md) - Configuration options
- [Retry strategies](../advanced/error-handling.md) - Implementing retry logic
- [Wait operations](wait.md) - Pause execution between steps
- [Child contexts](child-contexts.md) - Organize complex workflows
- [Examples](https://github.com/awslabs/aws-durable-execution-sdk-python/tree/main/examples/src/step) - More step examples

[↑ Back to top](#table-of-contents)

## License

See the [LICENSE](../../LICENSE) file for our project's licensing.

[↑ Back to top](#table-of-contents)
