# Basic Test Patterns

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Project structure](#project-structure)
- [Getting started](#getting-started)
- [Status checking patterns](#status-checking-patterns)
- [Result verification patterns](#result-verification-patterns)
- [Operation-specific assertions](#operation-specific-assertions)
- [Test organization tips](#test-organization-tips)
- [FAQ](#faq)
- [See also](#see-also)

[← Back to main index](../index.md)

## Overview

When you test durable functions, you need to verify that your function executed successfully, returned the expected result, and that operations like steps or waits ran correctly. This document shows you common patterns for writing these tests with simple assertions using the testing SDK.

The testing SDK (`aws-durable-execution-sdk-python-testing`) provides tools to run and inspect durable functions locally without deploying to AWS. Use these patterns as building blocks for your own tests, whether you're checking a simple calculation or inspecting individual operations.

[↑ Back to top](#table-of-contents)

## Prerequisites

To test durable functions, you need both SDKs installed:

```console
# Install the core SDK (for writing durable functions)
pip install aws-durable-execution-sdk-python

# Install the testing SDK (for testing durable functions)
pip install aws-durable-execution-sdk-python-testing

# Install pytest (test framework)
pip install pytest
```

The core SDK provides the decorators and context for writing durable functions. The testing SDK provides the test runner and assertions for testing them.

[↑ Back to top](#table-of-contents)

## Project structure

Here's a typical project structure for testing durable functions:

```
my-project/
├── src/
│   ├── __init__.py
│   └── my_function.py          # Your durable function
├── test/
│   ├── __init__.py
│   ├── conftest.py             # Pytest configuration and fixtures
│   └── test_my_function.py     # Your tests
├── requirements.txt
└── pytest.ini
```

**Key files:**

- `src/my_function.py` - Contains your durable function with `@durable_execution` decorator
- `test/conftest.py` - Configures the `durable_runner` fixture for pytest
- `test/test_my_function.py` - Contains your test cases using the `durable_runner` fixture

**Example conftest.py:**

```python
import pytest
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner

@pytest.fixture
def durable_runner(request):
    """Pytest fixture that provides a test runner."""
    marker = request.node.get_closest_marker("durable_execution")
    if not marker:
        pytest.fail("Test must be marked with @pytest.mark.durable_execution")
    
    handler = marker.kwargs.get("handler")
    runner = DurableFunctionTestRunner(handler=handler)
    
    yield runner
```

[↑ Back to top](#table-of-contents)

## Getting started

Here's a simple durable function:

```python
from aws_durable_execution_sdk_python import DurableContext, durable_execution

@durable_execution
def handler(event: dict, context: DurableContext) -> str:
    """Simple hello world durable function."""
    return "Hello World!"
```

And here's how you test it:

```python
import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from test.conftest import deserialize_operation_payload

@pytest.mark.durable_execution(
    handler=handler,
    lambda_function_name="hello world",
)
def test_hello_world(durable_runner):
    """Test hello world example."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == "Hello World!"
```

This test:
1. Marks the test with `@pytest.mark.durable_execution` to configure the runner
2. Uses the `durable_runner` fixture to execute the function
3. Checks the execution status
4. Verifies the final result

[↑ Back to top](#table-of-contents)

## Status checking patterns

### Check for successful execution

The most basic pattern verifies that your function completed successfully:

```python
@pytest.mark.durable_execution(
    handler=my_handler,
    lambda_function_name="my_function",
)
def test_success(durable_runner):
    """Test successful execution."""
    with durable_runner:
        result = durable_runner.run(input={"data": "test"}, timeout=10)
    
    assert result.status is InvocationStatus.SUCCEEDED
```

### Check for expected failures

Test that your function fails correctly when given invalid input:

```python
@pytest.mark.durable_execution(
    handler=handler_with_validation,
    lambda_function_name="validation_function",
)
def test_validation_failure(durable_runner):
    """Test that invalid input causes failure."""
    with durable_runner:
        result = durable_runner.run(input={"invalid": "data"}, timeout=10)
    
    assert result.status is InvocationStatus.FAILED
    assert "ValidationError" in str(result.error)
```

### Check execution with timeout

Verify that your function completes within the expected time:

```python
@pytest.mark.durable_execution(
    handler=quick_handler,
    lambda_function_name="quick_function",
)
def test_completes_quickly(durable_runner):
    """Test that function completes within timeout."""
    with durable_runner:
        # Use a short timeout to verify quick execution
        result = durable_runner.run(input={}, timeout=5)
    
    assert result.status is InvocationStatus.SUCCEEDED
```

[↑ Back to top](#table-of-contents)

## Result verification patterns

### Verify simple return values

Check that your function returns the expected value:

```python
from test.conftest import deserialize_operation_payload

@pytest.mark.durable_execution(
    handler=calculator_handler,
    lambda_function_name="calculator",
)
def test_calculation_result(durable_runner):
    """Test calculation returns correct result."""
    with durable_runner:
        result = durable_runner.run(input={"a": 5, "b": 3}, timeout=10)
    
    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == 8
```

### Verify complex return values

Check specific fields in complex return values:

```python
@pytest.mark.durable_execution(
    handler=order_handler,
    lambda_function_name="order_processor",
)
def test_order_processing(durable_runner):
    """Test order processing returns correct structure."""
    with durable_runner:
        result = durable_runner.run(
            input={"order_id": "order-123", "amount": 100.0},
            timeout=10
        )
    
    assert result.status is InvocationStatus.SUCCEEDED
    
    order_result = deserialize_operation_payload(result.result)
    assert order_result["order_id"] == "order-123"
    assert order_result["status"] == "completed"
    assert order_result["amount"] == 100.0
```

### Verify list results

Check that your function returns the expected list of values:

```python
@pytest.mark.durable_execution(
    handler=parallel_handler,
    lambda_function_name="parallel_tasks",
)
def test_parallel_results(durable_runner):
    """Test parallel operations return all results."""
    with durable_runner:
        result = durable_runner.run(input={}, timeout=10)
    
    assert result.status is InvocationStatus.SUCCEEDED
    
    results = deserialize_operation_payload(result.result)
    assert len(results) == 3
    assert results == [
        "Task 1 complete",
        "Task 2 complete",
        "Task 3 complete",
    ]
```

[↑ Back to top](#table-of-contents)

## Operation-specific assertions

### Verify step operations

Here's a function with a step:

```python
from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
    durable_step,
    StepContext,
)

@durable_step
def add_numbers(step_context: StepContext, a: int, b: int) -> int:
    return a + b

@durable_execution
def handler(event: dict, context: DurableContext) -> int:
    result = context.step(add_numbers(5, 3))
    return result
```

Check that the step executed and produced the expected result:

```python
import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from test.conftest import deserialize_operation_payload

@pytest.mark.durable_execution(
    handler=handler,
    lambda_function_name="step_function",
)
def test_step_execution(durable_runner):
    """Test step executes correctly."""
    with durable_runner:
        result = durable_runner.run(input={}, timeout=10)
    
    assert result.status is InvocationStatus.SUCCEEDED
    
    # Get step by name
    step_result = result.get_step("add_numbers")
    assert deserialize_operation_payload(step_result.result) == 8
```

### Verify wait operations

Here's a function with a wait:

```python
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.config import Duration

@durable_execution
def handler(event: dict, context: DurableContext) -> str:
    context.wait(Duration.from_seconds(5))
    return "Wait completed"
```

Check that the wait operation was created with correct timing:

```python
@pytest.mark.durable_execution(
    handler=handler,
    lambda_function_name="wait_function",
)
def test_wait_operation(durable_runner):
    """Test wait operation is created."""
    with durable_runner:
        result = durable_runner.run(input={}, timeout=10)
    
    assert result.status is InvocationStatus.SUCCEEDED
    
    # Find wait operations
    wait_ops = [
        op for op in result.operations 
        if op.operation_type.value == "WAIT"
    ]
    assert len(wait_ops) == 1
    assert wait_ops[0].scheduled_end_timestamp is not None
```

### Verify callback operations

Here's a function that creates a callback:

```python
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.config import CallbackConfig

@durable_execution
def handler(event: dict, context: DurableContext) -> str:
    callback_config = CallbackConfig(
        timeout_seconds=120,
        heartbeat_timeout_seconds=60
    )
    
    callback = context.create_callback(
        name="example_callback",
        config=callback_config
    )
    
    return f"Callback created with ID: {callback.callback_id}"
```

Check that the callback was created with correct configuration:

```python
@pytest.mark.durable_execution(
    handler=handler,
    lambda_function_name="callback_function",
)
def test_callback_creation(durable_runner):
    """Test callback is created correctly."""
    with durable_runner:
        result = durable_runner.run(input={}, timeout=10)
    
    assert result.status is InvocationStatus.SUCCEEDED
    
    # Find callback operations
    callback_ops = [
        op for op in result.operations 
        if op.operation_type.value == "CALLBACK"
    ]
    assert len(callback_ops) == 1
    
    callback_op = callback_ops[0]
    assert callback_op.name == "example_callback"
    assert callback_op.callback_id is not None
```

### Verify child context operations

Here's a function with a child context:

```python
from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
    durable_with_child_context,
)

@durable_with_child_context
def child_operation(ctx: DurableContext, value: int) -> int:
    return ctx.step(lambda _: value * 2, name="multiply")

@durable_execution
def handler(event: dict, context: DurableContext) -> str:
    result = context.run_in_child_context(child_operation(5))
    return f"Child context result: {result}"
```

Check that the child context executed correctly:

```python
@pytest.mark.durable_execution(
    handler=handler,
    lambda_function_name="child_context_function",
)
def test_child_context(durable_runner):
    """Test child context executes."""
    with durable_runner:
        result = durable_runner.run(input={}, timeout=10)
    
    assert result.status is InvocationStatus.SUCCEEDED
    
    # Find child context operations
    context_ops = [
        op for op in result.operations 
        if op.operation_type.value == "CONTEXT"
    ]
    assert len(context_ops) >= 1
```

### Verify parallel operations

Here's a function with parallel operations:

```python
from aws_durable_execution_sdk_python import DurableContext, durable_execution

@durable_execution
def handler(event: dict, context: DurableContext) -> list[str]:
    # Execute multiple operations
    task1 = context.step(lambda _: "Task 1 complete", name="task1")
    task2 = context.step(lambda _: "Task 2 complete", name="task2")
    task3 = context.step(lambda _: "Task 3 complete", name="task3")
    
    # All tasks execute concurrently and results are collected
    return [task1, task2, task3]
```

Check that multiple operations executed in parallel:

```python
from aws_durable_execution_sdk_python.lambda_service import OperationType

@pytest.mark.durable_execution(
    handler=handler,
    lambda_function_name="parallel_function",
)
def test_parallel_operations(durable_runner):
    """Test parallel operations execute."""
    with durable_runner:
        result = durable_runner.run(input={}, timeout=10)
    
    assert result.status is InvocationStatus.SUCCEEDED
    
    # Find all step operations
    step_ops = [
        op for op in result.operations 
        if op.operation_type == OperationType.STEP
    ]
    assert len(step_ops) == 3
    
    # Verify step names
    step_names = {op.name for op in step_ops}
    assert step_names == {"task1", "task2", "task3"}
```

[↑ Back to top](#table-of-contents)

## Test organization tips

### Use descriptive test names

Name your tests to clearly describe what they verify:

```python
# Good - describes what is being tested
def test_order_processing_succeeds_with_valid_input(durable_runner):
    pass

def test_order_processing_fails_with_invalid_order_id(durable_runner):
    pass

# Avoid - vague or unclear
def test_order(durable_runner):
    pass

def test_case_1(durable_runner):
    pass
```

### Group related tests

Organize tests by feature or functionality:

```python
# tests/test_order_processing.py
class TestOrderValidation:
    """Tests for order validation."""
    
    @pytest.mark.durable_execution(handler=handler, lambda_function_name="orders")
    def test_valid_order(self, durable_runner):
        """Test valid order is accepted."""
        pass
    
    @pytest.mark.durable_execution(handler=handler, lambda_function_name="orders")
    def test_invalid_order_id(self, durable_runner):
        """Test invalid order ID is rejected."""
        pass

class TestOrderFulfillment:
    """Tests for order fulfillment."""
    
    @pytest.mark.durable_execution(handler=handler, lambda_function_name="orders")
    def test_fulfillment_success(self, durable_runner):
        """Test successful order fulfillment."""
        pass
```

### Use fixtures for common test data

Create fixtures for test data you use across multiple tests:

```python
# conftest.py
@pytest.fixture
def valid_order():
    """Provide valid order data."""
    return {
        "order_id": "order-123",
        "customer_id": "customer-456",
        "amount": 100.0,
        "items": [
            {"product_id": "prod-1", "quantity": 2},
            {"product_id": "prod-2", "quantity": 1},
        ],
    }

# test_orders.py
@pytest.mark.durable_execution(handler=handler, lambda_function_name="orders")
def test_order_processing(durable_runner, valid_order):
    """Test order processing with valid data."""
    with durable_runner:
        result = durable_runner.run(input=valid_order, timeout=10)
    
    assert result.status is InvocationStatus.SUCCEEDED
```

### Add docstrings to tests

Document what each test verifies:

```python
@pytest.mark.durable_execution(handler=handler, lambda_function_name="payment")
def test_payment_with_retry(durable_runner):
    """Test payment processing retries on transient failures.
    
    This test verifies that:
    1. Payment step retries on RuntimeError
    2. Function eventually succeeds after retries
    3. Final result includes transaction ID
    """
    with durable_runner:
        result = durable_runner.run(input={"amount": 50.0}, timeout=30)
    
    assert result.status is InvocationStatus.SUCCEEDED
```

### Use parametrized tests for similar cases

Test multiple inputs with the same logic using `pytest.mark.parametrize`:

```python
@pytest.mark.parametrize("a,b,expected", [
    (5, 3, 8),
    (10, 20, 30),
    (0, 0, 0),
    (-5, 5, 0),
])
@pytest.mark.durable_execution(handler=add_handler, lambda_function_name="calculator")
def test_addition(durable_runner, a, b, expected):
    """Test addition with various inputs."""
    with durable_runner:
        result = durable_runner.run(input={"a": a, "b": b}, timeout=10)
    
    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == expected
```

### Keep tests focused

Each test should verify one specific behavior:

```python
# Good - focused on one behavior
@pytest.mark.durable_execution(handler=handler, lambda_function_name="orders")
def test_order_validation_succeeds(durable_runner):
    """Test order validation with valid input."""
    with durable_runner:
        result = durable_runner.run(input={"order_id": "order-123"}, timeout=10)
    
    assert result.status is InvocationStatus.SUCCEEDED

@pytest.mark.durable_execution(handler=handler, lambda_function_name="orders")
def test_order_validation_fails_missing_id(durable_runner):
    """Test order validation fails without order ID."""
    with durable_runner:
        result = durable_runner.run(input={}, timeout=10)
    
    assert result.status is InvocationStatus.FAILED

# Avoid - testing multiple behaviors
@pytest.mark.durable_execution(handler=handler, lambda_function_name="orders")
def test_order_validation(durable_runner):
    """Test order validation."""
    # Test valid input
    result1 = durable_runner.run(input={"order_id": "order-123"}, timeout=10)
    assert result1.status is InvocationStatus.SUCCEEDED
    
    # Test invalid input
    result2 = durable_runner.run(input={}, timeout=10)
    assert result2.status is InvocationStatus.FAILED
```

[↑ Back to top](#table-of-contents)

## FAQ

**Q: Do I need to deploy my function to test it?**

A: No, the test runner executes your function locally. You only need to deploy for cloud testing mode.

**Q: How do I test functions with external dependencies?**

A: Mock external dependencies in your test setup. The test runner executes your function code as-is, so standard Python mocking works.

**Q: Can I test multiple functions in one test file?**

A: Yes, use different `@pytest.mark.durable_execution` markers for each function you want to test.

**Q: How do I access operation results?**

A: Use `result.get_step(name)` for steps, or iterate through `result.operations` to find specific operation types.

**Q: What's the difference between result.result and step.result?**

A: `result.result` is the final return value of your handler function. `step.result` is the return value of a specific step operation.

**Q: How do I test error scenarios?**

A: Check that `result.status is InvocationStatus.FAILED` and inspect `result.error` for the error message.

**Q: Can I run tests in parallel?**

A: Yes, use pytest-xdist: `pytest -n auto` to run tests in parallel.

**Q: How do I debug failing tests?**

A: Add print statements or use a debugger. The test runner executes your code locally, so standard debugging tools work.

**Q: What timeout should I use?**

A: Use a timeout slightly longer than your function's expected execution time. For most tests, 10-30 seconds is sufficient.

**Q: How do I test functions that use environment variables?**

A: Set environment variables in your test setup or use pytest fixtures to manage them.

[↑ Back to top](#table-of-contents)

## See also

- [Complex workflows](complex-workflows.md) - Testing multi-step workflows
- [Best practices](../best-practices.md) - Testing recommendations
- [Testing modes](../advanced/testing-modes.md) - Local and cloud test execution
- [Steps](../core/steps.md) - Testing step operations
- [Wait operations](../core/wait.md) - Testing wait operations
- [Callbacks](../core/callbacks.md) - Testing callback operations

[↑ Back to top](#table-of-contents)

## License

See the [LICENSE](../../LICENSE) file for our project's licensing.

[↑ Back to top](#table-of-contents)
