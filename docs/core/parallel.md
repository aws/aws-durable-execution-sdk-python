# Parallel Operations

## Table of Contents

- [What are parallel operations?](#what-are-parallel-operations)
- [Terminology](#terminology)
- [Key features](#key-features)
- [Getting started](#getting-started)
- [Method signature](#method-signature)
- [Basic usage](#basic-usage)
- [Collecting results](#collecting-results)
- [Configuration](#configuration)
- [Advanced patterns](#advanced-patterns)
- [Error handling](#error-handling)
- [Result ordering](#result-ordering)
- [Performance considerations](#performance-considerations)
- [Best practices](#best-practices)
- [FAQ](#faq)
- [Testing](#testing)
- [See also](#see-also)

[← Back to main index](../index.md)

## Terminology

**Parallel operation** - An operation that executes multiple functions concurrently using `context.parallel()`. Each function runs in its own child context.

**Branch** - An individual function within a parallel operation. Each branch executes independently and can succeed or fail without affecting other branches.

**BatchResult** - The result object returned by parallel operations, containing successful results, failed results, and execution metadata.

**Completion strategy** - Configuration that determines when a parallel operation completes (e.g., all successful, first successful, all completed).

**Concurrent execution** - Multiple operations executing at the same time. The SDK manages concurrency automatically, executing branches in parallel.

**Child context** - An isolated execution context created for each branch. Each branch has its own step counter and operation tracking.

[↑ Back to top](#table-of-contents)

## What are parallel operations?

Parallel operations let you execute multiple functions concurrently within a durable function. Each function runs in its own child context and can perform steps, waits, or other operations independently. The SDK manages the concurrent execution and collects results automatically.

Use parallel operations to:
- Execute independent tasks concurrently for better performance
- Process multiple items that don't depend on each other
- Implement fan-out patterns where one input triggers multiple operations
- Reduce total execution time by running operations simultaneously

[↑ Back to top](#table-of-contents)

## Key features

- **Automatic concurrency** - Functions execute concurrently without manual thread management
- **Independent execution** - Each branch runs in its own child context with isolated state
- **Flexible completion** - Configure when the operation completes (all successful, first successful, etc.)
- **Error isolation** - One branch failing doesn't automatically fail others
- **Result collection** - Automatic collection of successful and failed results
- **Concurrency control** - Limit maximum concurrent branches with `max_concurrency`
- **Checkpointing** - Results are checkpointed as branches complete

[↑ Back to top](#table-of-contents)

## Getting started

Here's a simple example of parallel operations:

```python
from aws_durable_execution_sdk_python import (
    BatchResult,
    DurableContext,
    durable_execution,
)

@durable_execution
def handler(event: dict, context: DurableContext) -> list[str]:
    """Execute three tasks in parallel."""
    # Define functions to execute in parallel
    task1 = lambda ctx: ctx.step(lambda _: "Task 1 complete", name="task1")
    task2 = lambda ctx: ctx.step(lambda _: "Task 2 complete", name="task2")
    task3 = lambda ctx: ctx.step(lambda _: "Task 3 complete", name="task3")
    
    # Execute all tasks concurrently
    result: BatchResult[str] = context.parallel([task1, task2, task3])
    
    # Return successful results
    return result.successful_results
```

When this function runs:
1. All three tasks execute concurrently
2. Each task runs in its own child context
3. Results are collected as tasks complete
4. The `BatchResult` contains all successful results

[↑ Back to top](#table-of-contents)

## Method signature

### context.parallel()

```python
def parallel(
    functions: Sequence[Callable[[DurableContext], T]],
    name: str | None = None,
    config: ParallelConfig | None = None,
) -> BatchResult[T]
```

**Parameters:**

- `functions` - A sequence of callables that each receive a `DurableContext` and return a result. Each function executes in its own child context.
- `name` (optional) - A name for the parallel operation, useful for debugging and testing.
- `config` (optional) - A `ParallelConfig` object to configure concurrency limits, completion criteria, and serialization.

**Returns:** A `BatchResult[T]` object containing:
- `successful_results` - List of results from branches that succeeded
- `failed_results` - List of results from branches that failed
- `total_count` - Total number of branches
- `success_count` - Number of successful branches
- `failure_count` - Number of failed branches
- `status` - Overall status of the parallel operation
- `completion_reason` - Why the operation completed

**Raises:** Exceptions are captured per branch and included in `failed_results`. The parallel operation itself doesn't raise unless all branches fail (depending on completion configuration).

[↑ Back to top](#table-of-contents)

## Basic usage

### Simple parallel execution

Execute multiple independent operations concurrently:

```python
from aws_durable_execution_sdk_python import (
    BatchResult,
    DurableContext,
    durable_execution,
)

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    """Process multiple services in parallel."""
    
    def check_inventory(ctx: DurableContext) -> dict:
        return ctx.step(lambda _: {"service": "inventory", "status": "ok"})
    
    def check_payment(ctx: DurableContext) -> dict:
        return ctx.step(lambda _: {"service": "payment", "status": "ok"})
    
    def check_shipping(ctx: DurableContext) -> dict:
        return ctx.step(lambda _: {"service": "shipping", "status": "ok"})
    
    # Execute all checks in parallel
    result: BatchResult[dict] = context.parallel([
        check_inventory,
        check_payment,
        check_shipping,
    ])
    
    return {
        "total": result.total_count,
        "successful": result.success_count,
        "results": result.successful_results,
    }
```

## Collecting results

The `BatchResult` object provides multiple ways to access results:

```python
from aws_durable_execution_sdk_python import (
    BatchResult,
    DurableContext,
    durable_execution,
)

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    """Demonstrate result collection."""
    
    functions = [
        lambda ctx: ctx.step(lambda _: f"Result {i}")
        for i in range(5)
    ]
    
    result: BatchResult[str] = context.parallel(functions)
    
    return {
        # Successful results only
        "successful": result.successful_results,
        
        # Failed results (if any)
        "failed": result.failed_results,
        
        # Counts
        "total_count": result.total_count,
        "success_count": result.success_count,
        "failure_count": result.failure_count,
        
        # Status information
        "status": result.status.value,
        "completion_reason": result.completion_reason.value,
    }
```

### Accessing individual results

Results are ordered by branch index:

```python
from aws_durable_execution_sdk_python import (
    BatchResult,
    DurableContext,
    durable_execution,
)

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    """Access individual results from parallel execution."""
    
    def task_a(ctx: DurableContext) -> str:
        return ctx.step(lambda _: "Result A")
    
    def task_b(ctx: DurableContext) -> str:
        return ctx.step(lambda _: "Result B")
    
    def task_c(ctx: DurableContext) -> str:
        return ctx.step(lambda _: "Result C")
    
    result: BatchResult[str] = context.parallel([task_a, task_b, task_c])
    
    # Access results by index
    first_result = result.successful_results[0]   # "Result A"
    second_result = result.successful_results[1]  # "Result B"
    third_result = result.successful_results[2]   # "Result C"
    
    return {
        "first": first_result,
        "second": second_result,
        "third": third_result,
        "all": result.successful_results,
    }
```

[↑ Back to top](#table-of-contents)

## Configuration

Configure parallel behavior using `ParallelConfig`:

```python
from aws_durable_execution_sdk_python import (
    BatchResult,
    DurableContext,
    durable_execution,
)
from aws_durable_execution_sdk_python.config import (
    CompletionConfig,
    ParallelConfig,
)

@durable_execution
def handler(event: dict, context: DurableContext) -> str:
    """Configure parallel execution."""
    
    # Configure to complete when first branch succeeds
    config = ParallelConfig(
        max_concurrency=3,  # Run at most 3 branches concurrently
        completion_config=CompletionConfig.first_successful(),
    )
    
    functions = [
        lambda ctx: ctx.step(lambda _: "Task 1", name="task1"),
        lambda ctx: ctx.step(lambda _: "Task 2", name="task2"),
        lambda ctx: ctx.step(lambda _: "Task 3", name="task3"),
    ]
    
    result: BatchResult[str] = context.parallel(functions, config=config)
    
    # Get the first successful result
    first_result = (
        result.successful_results[0]
        if result.successful_results
        else "None"
    )
    
    return f"First successful result: {first_result}"
```

### ParallelConfig parameters

**max_concurrency** - Maximum number of branches to execute concurrently. If `None` (default), all branches run concurrently. Use this to control resource usage:

```python
# Limit to 5 concurrent branches
config = ParallelConfig(max_concurrency=5)
```

**completion_config** - Defines when the parallel operation completes:

- `CompletionConfig.all_successful()` - Requires all branches to succeed (default)
- `CompletionConfig.first_successful()` - Completes when any branch succeeds
- `CompletionConfig.all_completed()` - Waits for all branches to complete regardless of success/failure
- Custom configuration with specific success/failure thresholds

```python
# Require at least 3 successes, tolerate up to 2 failures
config = ParallelConfig(
    completion_config=CompletionConfig(
        min_successful=3,
        tolerated_failure_count=2,
    )
)
```

**serdes** - Custom serialization for the `BatchResult` object. If not provided, uses JSON serialization.

**item_serdes** - Custom serialization for individual branch results. If not provided, uses JSON serialization.

[↑ Back to top](#table-of-contents)

## Advanced patterns

### First successful pattern

Execute multiple strategies and use the first one that succeeds:

```python
from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
)
from aws_durable_execution_sdk_python.config import (
    CompletionConfig,
    ParallelConfig,
)

@durable_execution
def handler(event: dict, context: DurableContext) -> str:
    """Try multiple data sources, use first successful."""
    
    def try_primary_db(ctx: DurableContext) -> dict:
        return ctx.step(lambda _: {"source": "primary", "data": "..."})
    
    def try_secondary_db(ctx: DurableContext) -> dict:
        return ctx.step(lambda _: {"source": "secondary", "data": "..."})
    
    def try_cache(ctx: DurableContext) -> dict:
        return ctx.step(lambda _: {"source": "cache", "data": "..."})
    
    # Complete as soon as any source succeeds
    config = ParallelConfig(
        completion_config=CompletionConfig.first_successful()
    )
    
    result: BatchResult[dict] = context.parallel(
        [try_primary_db, try_secondary_db, try_cache],
        config=config,
    )
    
    if result.successful_results:
        return result.successful_results[0]
    
    return {"error": "All sources failed"}
```

### Controlled concurrency

Limit concurrent execution to manage resource usage:

```python
from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
)
from aws_durable_execution_sdk_python.config import ParallelConfig

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    """Process many items with controlled concurrency."""
    items = event.get("items", [])
    
    # Create a function for each item
    functions = [
        lambda ctx, item=item: ctx.step(
            lambda _: f"Processed {item}",
            name=f"process_{item}"
        )
        for item in items
    ]
    
    # Process at most 10 items concurrently
    config = ParallelConfig(max_concurrency=10)
    
    result: BatchResult[str] = context.parallel(functions, config=config)
    
    return {
        "processed": result.success_count,
        "failed": result.failure_count,
        "results": result.successful_results,
    }
```

### Partial success handling

Handle scenarios where some branches can fail:

```python
from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
)
from aws_durable_execution_sdk_python.config import (
    CompletionConfig,
    ParallelConfig,
)

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    """Allow some branches to fail."""
    
    # Require at least 2 successes, tolerate up to 1 failure
    config = ParallelConfig(
        completion_config=CompletionConfig(
            min_successful=2,
            tolerated_failure_count=1,
        )
    )
    
    functions = [
        lambda ctx: ctx.step(lambda _: "Success 1"),
        lambda ctx: ctx.step(lambda _: "Success 2"),
        lambda ctx: ctx.step(lambda _: raise_error()),  # This might fail
    ]
    
    result: BatchResult[str] = context.parallel(functions, config=config)
    
    return {
        "status": "partial_success",
        "successful": result.successful_results,
        "failed_count": result.failure_count,
    }
```

### Nested parallel operations

Parallel operations can contain other parallel operations:

```python
from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
)

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    """Nested parallel execution."""
    
    def process_group_a(ctx: DurableContext) -> list:
        # Inner parallel operation for group A
        task1 = lambda c: c.step(lambda _: "group-a-item-1")
        task2 = lambda c: c.step(lambda _: "group-a-item-2")
        task3 = lambda c: c.step(lambda _: "group-a-item-3")
        
        inner_result = ctx.parallel([task1, task2, task3])
        return inner_result.successful_results
    
    def process_group_b(ctx: DurableContext) -> list:
        # Inner parallel operation for group B
        task1 = lambda c: c.step(lambda _: "group-b-item-1")
        task2 = lambda c: c.step(lambda _: "group-b-item-2")
        task3 = lambda c: c.step(lambda _: "group-b-item-3")
        
        inner_result = ctx.parallel([task1, task2, task3])
        return inner_result.successful_results
    
    # Outer parallel operation
    result: BatchResult[list] = context.parallel([process_group_a, process_group_b])
    
    return {
        "groups_processed": result.success_count,
        "results": result.successful_results,
    }
```

[↑ Back to top](#table-of-contents)

## Error handling

Parallel operations handle errors gracefully, isolating failures to individual branches:

### Individual branch failures

When a branch fails, other branches continue executing:

```python
from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
)
from aws_durable_execution_sdk_python.config import (
    CompletionConfig,
    ParallelConfig,
)

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    """Handle individual branch failures."""
    
    def successful_task(ctx: DurableContext) -> str:
        return ctx.step(lambda _: "Success")
    
    def failing_task(ctx: DurableContext) -> str:
        return ctx.step(lambda _: raise_error("Task failed"))
    
    functions = [successful_task, failing_task, successful_task]
    
    # Use all_completed to wait for all branches
    config = ParallelConfig(
        completion_config=CompletionConfig.all_completed()
    )
    
    result: BatchResult[str] = context.parallel(functions, config=config)
    
    return {
        "successful": result.successful_results,
        "failed_count": result.failure_count,
        "status": result.status.value,
    }
```

### Checking for failures

Inspect the `BatchResult` to detect and handle failures:

```python
from aws_durable_execution_sdk_python import BatchResult

result: BatchResult = context.parallel(functions)

if result.failure_count > 0:
    # Some branches failed
    return {
        "status": "partial_failure",
        "successful": result.successful_results,
        "failed_count": result.failure_count,
    }

# All branches succeeded
return {
    "status": "success",
    "results": result.successful_results,
}
```

### Completion strategies and errors

Different completion strategies handle errors differently:

**all_successful()** - Fails fast when any branch fails:
```python
config = ParallelConfig(
    completion_config=CompletionConfig.all_successful()
)
# Stops executing new branches after first failure
```

**first_successful()** - Continues until one branch succeeds:
```python
config = ParallelConfig(
    completion_config=CompletionConfig.first_successful()
)
# Ignores failures until at least one succeeds
```

**all_completed()** - Waits for all branches regardless of errors:
```python
config = ParallelConfig(
    completion_config=CompletionConfig.all_completed()
)
# All branches complete, collect both successes and failures
```

[↑ Back to top](#table-of-contents)

## Result ordering

Results in `successful_results` maintain the same order as the input functions:

```python
from aws_durable_execution_sdk_python import (
    BatchResult,
    DurableContext,
    durable_execution,
)

@durable_execution
def handler(event: dict, context: DurableContext) -> list[str]:
    """Demonstrate result ordering."""
    
    functions = [
        lambda ctx: ctx.step(lambda _: "First"),
        lambda ctx: ctx.step(lambda _: "Second"),
        lambda ctx: ctx.step(lambda _: "Third"),
    ]
    
    result = context.parallel(functions)
    
    # Results are in the same order as functions
    assert result.successful_results[0] == "First"
    assert result.successful_results[1] == "Second"
    assert result.successful_results[2] == "Third"
    
    return result.successful_results
```

**Important:** Even though branches execute concurrently and may complete in any order, the SDK preserves the original order in the results list. This makes it easy to correlate results with inputs.

### Handling partial results

When some branches fail, `successful_results` only contains results from successful branches, but the order is still preserved relative to the input:

```python
# If function at index 1 fails:
# Input:  [func0, func1, func2]
# Result: [result0, result2]  # result1 is missing, but order preserved
```

[↑ Back to top](#table-of-contents)

## Performance considerations

### Concurrency limits

Use `max_concurrency` to balance performance and resource usage:

```python
from aws_durable_execution_sdk_python import BatchResult
from aws_durable_execution_sdk_python.config import ParallelConfig

# Process 100 items, but only 10 at a time
config = ParallelConfig(max_concurrency=10)
result: BatchResult = context.parallel(functions, config=config)
```

**When to limit concurrency:**
- Processing many items (hundreds or thousands)
- Calling external APIs with rate limits
- Managing memory usage with large data
- Controlling database connection pools

**When to use unlimited concurrency:**
- Small number of branches (< 50)
- Independent operations with no shared resources
- When maximum speed is critical

### Completion strategies

Choose the right completion strategy for your use case:

**first_successful()** - Best for:
- Redundant operations (multiple data sources)
- Racing multiple strategies
- Minimizing latency

**all_successful()** - Best for:
- Operations that must all succeed
- Fail-fast behavior
- Strict consistency requirements

**all_completed()** - Best for:
- Best-effort operations
- Collecting partial results
- Logging or monitoring tasks

### Checkpointing overhead

Each branch creates checkpoints as it executes. For many small branches, consider:
- Batching items together
- Using map operations instead
- Grouping related operations

[↑ Back to top](#table-of-contents)

## Best practices

**Use parallel for independent operations** - Only parallelize operations that don't depend on each other's results.

**Limit concurrency for large workloads** - Use `max_concurrency` when processing many items to avoid overwhelming resources.

**Choose appropriate completion strategies** - Match the completion strategy to your business requirements (all must succeed vs. best effort).

**Handle partial failures gracefully** - Check `failure_count` and handle scenarios where some branches fail.

**Keep branches focused** - Each branch should be a cohesive unit of work. Don't make branches too granular.

**Use meaningful names** - Name your parallel operations for easier debugging and testing.

**Consider map operations for collections** - If you're processing a collection of similar items, use `context.map()` instead.

**Avoid shared state** - Each branch runs in its own context. Don't rely on shared variables or global state.

**Monitor resource usage** - Parallel operations can consume significant resources. Monitor memory and API rate limits.

**Test with realistic concurrency** - Test your parallel operations with realistic numbers of branches to catch resource issues.

[↑ Back to top](#table-of-contents)

## FAQ

**Q: What's the difference between parallel() and map()?**

A: `parallel()` executes a list of different functions, while `map()` executes the same function for each item in a collection. Use `parallel()` for heterogeneous operations and `map()` for homogeneous operations.

**Q: How many branches can I run in parallel?**

A: There's no hard limit, but consider resource constraints. For large numbers (> 100), use `max_concurrency` to limit concurrent execution.

**Q: Do branches execute in a specific order?**

A: Branches execute concurrently, so execution order is non-deterministic. However, results are returned in the same order as the input functions.

**Q: Can I use async functions in parallel operations?**

A: No, branch functions must be synchronous. If you need to call async code, use `asyncio.run()` inside your function.

**Q: What happens if all branches fail?**

A: The behavior depends on your completion configuration. With `all_successful()`, the operation fails. With `all_completed()`, you get a `BatchResult` with all failures in `failed_results`.

**Q: Can I cancel running branches?**

A: Not directly. The SDK doesn't provide branch cancellation. Use completion strategies like `first_successful()` to stop starting new branches early.

**Q: How do I pass different arguments to each branch?**

A: Use lambda functions with default arguments:

```python
functions = [
    lambda ctx, val=value: process(ctx, val)
    for value in values
]
```

**Q: Can branches communicate with each other?**

A: No, branches are isolated. They can't share state or communicate during execution. Pass data through the parent context or use the results after parallel execution completes.

**Q: What's the overhead of parallel operations?**

A: Each branch creates a child context and checkpoints its results. For very small operations, the overhead might outweigh the benefits. Profile your specific use case.

**Q: Can I nest parallel operations?**

A: Yes, you can call `context.parallel()` inside a branch function. Each nested parallel operation creates its own set of child contexts.

[↑ Back to top](#table-of-contents)

## Testing

You can test parallel operations using the testing SDK. The test runner executes your function and lets you inspect branch results.

### Basic parallel testing

```python
import pytest
from aws_durable_execution_sdk_python_testing import InvocationStatus
from my_function import handler

@pytest.mark.durable_execution(
    handler=handler,
    lambda_function_name="parallel_function",
)
def test_parallel(durable_runner):
    """Test parallel operations."""
    with durable_runner:
        result = durable_runner.run(input={"data": "test"}, timeout=10)
    
    # Check overall status
    assert result.status is InvocationStatus.SUCCEEDED
    
    # Check the result contains expected values
    assert len(result.result) == 3
    assert "Task 1 complete" in result.result
```

### Inspecting branch operations

Use the test result to inspect individual branch operations:

```python
from aws_durable_execution_sdk_python_testing import OperationType

@pytest.mark.durable_execution(
    handler=handler,
    lambda_function_name="parallel_function",
)
def test_parallel_branches(durable_runner):
    """Test and inspect parallel branches."""
    with durable_runner:
        result = durable_runner.run(input={}, timeout=10)
    
    # Verify all step operations exist
    step_ops = [
        op for op in result.operations
        if op.operation_type == OperationType.STEP
    ]
    assert len(step_ops) == 3
    
    # Check step names
    step_names = {op.name for op in step_ops}
    assert step_names == {"task1", "task2", "task3"}
```

### Testing completion strategies

Test that completion strategies work correctly:

```python
@pytest.mark.durable_execution(
    handler=handler_first_successful,
    lambda_function_name="first_successful_function",
)
def test_first_successful(durable_runner):
    """Test first successful completion strategy."""
    with durable_runner:
        result = durable_runner.run(input={}, timeout=10)
    
    # Should succeed with at least one result
    assert result.status is InvocationStatus.SUCCEEDED
    assert "First successful result:" in result.result
```

### Testing error handling

Test that parallel operations handle errors correctly:

```python
@pytest.mark.durable_execution(
    handler=handler_with_failures,
    lambda_function_name="parallel_with_failures",
)
def test_parallel_with_failures(durable_runner):
    """Test parallel operations with some failures."""
    with durable_runner:
        result = durable_runner.run(input={}, timeout=10)
    
    # Check that some branches succeeded
    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result["successful_count"] > 0
    assert result.result["failed_count"] > 0
```

### Testing concurrency limits

Test that concurrency limits are respected:

```python
@pytest.mark.durable_execution(
    handler=handler_with_concurrency_limit,
    lambda_function_name="limited_concurrency",
)
def test_concurrency_limit(durable_runner):
    """Test parallel operations with concurrency limit."""
    with durable_runner:
        result = durable_runner.run(input={"items": list(range(20))}, timeout=30)
    
    # All items should be processed
    assert result.status is InvocationStatus.SUCCEEDED
    assert len(result.result["results"]) == 20
```

For more testing patterns, see:
- [Basic tests](../testing-patterns/basic-tests.md) - Simple test examples
- [Complex workflows](../testing-patterns/complex-workflows.md) - Multi-step workflow testing
- [Best practices](../testing-patterns/best-practices.md) - Testing recommendations

[↑ Back to top](#table-of-contents)

## See also

- [Map operations](map.md) - Process collections with the same function
- [Child contexts](child-contexts.md) - Understand child context isolation
- [Steps](steps.md) - Use steps within parallel branches
- [Error handling](../advanced/error-handling.md) - Handle errors in durable functions
- [ParallelConfig](../api-reference/config.md) - Configuration options
- [BatchResult](../api-reference/result.md) - Result object reference
- [Examples](https://github.com/awslabs/aws-durable-execution-sdk-python/tree/main/examples/src/parallel) - More parallel examples

[↑ Back to top](#table-of-contents)

## License

See the [LICENSE](../../LICENSE) file for our project's licensing.

[↑ Back to top](#table-of-contents)
