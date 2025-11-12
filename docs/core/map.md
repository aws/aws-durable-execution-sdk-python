# Map Operations

## Table of Contents

- [What are map operations?](#what-are-map-operations)
- [Terminology](#terminology)
- [Key features](#key-features)
- [Getting started](#getting-started)
- [Method signature](#method-signature)
- [Map function signature](#map-function-signature)
- [Configuration](#configuration)
- [Advanced patterns](#advanced-patterns)
- [Best practices](#best-practices)
- [Performance tips](#performance-tips)
- [FAQ](#faq)
- [Testing](#testing)
- [See also](#see-also)

[← Back to main index](../index.md)

## Terminology

**Map operation** - A durable operation that processes a collection of items in parallel, where each item is processed independently and checkpointed. Created using `context.map()`.

**Map function** - A function that processes a single item from the collection. Receives the context, item, index, and full collection as parameters.

**BatchResult** - The result type returned by map operations, containing results from all processed items with success/failure status.

**Concurrency control** - Limiting how many items process simultaneously using `max_concurrency` in `MapConfig`.

**Item batching** - Grouping multiple items together for processing as a single unit to optimize efficiency.

**Completion criteria** - Rules that determine when a map operation succeeds or fails based on individual item results.

[↑ Back to top](#table-of-contents)

## What are map operations?

Map operations let you process collections durably by applying a function to each item in parallel. Each item's processing is checkpointed independently, so if your function is interrupted, completed items don't need to be reprocessed.

Use map operations to:
- Transform collections with automatic checkpointing
- Process lists of items in parallel
- Handle large datasets with resilience
- Control concurrency and batching behavior
- Define custom success/failure criteria

Map operations use `context.map()` to process collections efficiently. Each item becomes an independent operation that executes in parallel with other items.

[↑ Back to top](#table-of-contents)

## Key features

- **Parallel processing** - Items process concurrently by default
- **Independent checkpointing** - Each item's result is saved separately
- **Partial completion** - Completed items don't reprocess on replay
- **Concurrency control** - Limit simultaneous processing with `max_concurrency`
- **Batching support** - Group items for efficient processing
- **Flexible completion** - Define custom success/failure criteria
- **Result ordering** - Results maintain the same order as inputs

[↑ Back to top](#table-of-contents)

## Getting started

Here's a simple example of processing a collection:

```python
from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
    BatchResult,
)

def square(context: DurableContext, item: int, index: int, items: list[int]) -> int:
    """Square a number."""
    return item * item

@durable_execution
def handler(event: dict, context: DurableContext) -> BatchResult[int]:
    """Process a list of items using map operations."""
    items = [1, 2, 3, 4, 5]
    
    result = context.map(items, square)
    return result
```

When this function runs:
1. Each item is processed in parallel
2. The `square` function is called for each item
3. Each result is checkpointed independently
4. The function returns a `BatchResult` with results `[1, 4, 9, 16, 25]`

If the function is interrupted after processing items 0-2, it resumes at item 3 without reprocessing the first three items.

[↑ Back to top](#table-of-contents)

## Method signature

### context.map()

```python
def map(
    inputs: Sequence[U],
    func: Callable[[DurableContext, U | BatchedInput[Any, U], int, Sequence[U]], T],
    name: str | None = None,
    config: MapConfig | None = None,
) -> BatchResult[T]
```

**Parameters:**

- `inputs` - A sequence of items to process (list, tuple, or any sequence type).
- `func` - A callable that processes each item. See [Map function signature](#map-function-signature) for details.
- `name` (optional) - A name for the map operation, useful for debugging and testing.
- `config` (optional) - A `MapConfig` object to configure concurrency, batching, and completion criteria.

**Returns:** A `BatchResult[T]` containing the results from processing all items.

**Raises:** Exceptions based on the completion criteria defined in `MapConfig`.

[↑ Back to top](#table-of-contents)

## Map function signature

The map function receives four parameters:

```python
def process_item(
    context: DurableContext,
    item: U | BatchedInput[Any, U],
    index: int,
    items: Sequence[U]
) -> T:
    """Process a single item from the collection."""
    # Your processing logic here
    return result
```

**Parameters:**

- `context` - A `DurableContext` for the item's processing. Use this to call steps, waits, or other operations.
- `item` - The current item being processed. Can be a `BatchedInput` if batching is configured.
- `index` - The zero-based index of the item in the original collection.
- `items` - The full collection of items being processed.

**Returns:** The result of processing the item.

### Example

```python
def validate_email(
    context: DurableContext,
    item: str,
    index: int,
    items: list[str]
) -> dict:
    """Validate an email address."""
    is_valid = "@" in item and "." in item
    return {
        "email": item,
        "valid": is_valid,
        "position": index,
        "total": len(items)
    }

@durable_execution
def handler(event: dict, context: DurableContext) -> BatchResult[dict]:
    emails = ["jane_doe@example.com", "john_doe@example.org", "invalid"]
    result = context.map(emails, validate_email)
    return result
```

[↑ Back to top](#table-of-contents)

## Configuration

Configure map behavior using `MapConfig`:

```python
from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
    BatchResult,
)
from aws_durable_execution_sdk_python.config import (
    MapConfig,
    CompletionConfig,
    ItemBatcher,
)

def process_item(context: DurableContext, item: int, index: int, items: list[int]) -> dict:
    """Process a single item."""
    return {"item": item, "squared": item * item}

@durable_execution
def handler(event: dict, context: DurableContext) -> BatchResult[dict]:
    items = list(range(100))
    
    # Configure map operation
    config = MapConfig(
        max_concurrency=10,  # Process 10 items at a time
        item_batcher=ItemBatcher(max_items_per_batch=5),  # Batch 5 items together
        completion_config=CompletionConfig.all_successful(),  # Require all to succeed
    )
    
    result = context.map(items, process_item, name="process_numbers", config=config)
    return result
```

### MapConfig parameters

**max_concurrency** - Maximum number of items to process concurrently. If `None`, all items process in parallel. Use this to control resource usage.

**item_batcher** - Configuration for batching items together. Use `ItemBatcher(max_items_per_batch=N)` to group items.

**completion_config** - Defines when the map operation succeeds or fails:
- `CompletionConfig()` - Default, allows any number of failures
- `CompletionConfig.all_successful()` - Requires all items to succeed
- `CompletionConfig(min_successful=N)` - Requires at least N items to succeed
- `CompletionConfig(tolerated_failure_count=N)` - Fails after N failures
- `CompletionConfig(tolerated_failure_percentage=X)` - Fails if more than X% fail

**serdes** - Custom serialization for the entire `BatchResult`. If `None`, uses JSON serialization.

**item_serdes** - Custom serialization for individual item results. If `None`, uses JSON serialization.

**summary_generator** - Function to generate compact summaries for large results (>256KB).

[↑ Back to top](#table-of-contents)

## Advanced patterns

### Concurrency control

Limit how many items process simultaneously:

```python
from aws_durable_execution_sdk_python.config import MapConfig

def fetch_data(context: DurableContext, url: str, index: int, urls: list[str]) -> dict:
    """Fetch data from a URL."""
    # Network call that might be rate-limited
    return {"url": url, "data": "..."}

@durable_execution
def handler(event: dict, context: DurableContext) -> BatchResult[dict]:
    urls = [f"https://example.com/api/{i}" for i in range(100)]
    
    # Process only 5 URLs at a time
    config = MapConfig(max_concurrency=5)
    
    result = context.map(urls, fetch_data, config=config)
    return result
```

### Batching items

Group multiple items for efficient processing:

```python
from aws_durable_execution_sdk_python.config import MapConfig, ItemBatcher, BatchedInput

def process_batch(
    context: DurableContext,
    batch: BatchedInput[None, int],
    index: int,
    items: list[int]
) -> list[dict]:
    """Process a batch of items together."""
    # Process all items in the batch together
    return [{"item": item, "squared": item * item} for item in batch.items]

@durable_execution
def handler(event: dict, context: DurableContext) -> BatchResult[list[dict]]:
    items = list(range(100))
    
    # Process items in batches of 10
    config = MapConfig(
        item_batcher=ItemBatcher(max_items_per_batch=10)
    )
    
    result = context.map(items, process_batch, config=config)
    return result
```

### Custom completion criteria

Define when the map operation should succeed or fail:

```python
from aws_durable_execution_sdk_python.config import MapConfig, CompletionConfig

def process_item(context: DurableContext, item: int, index: int, items: list[int]) -> dict:
    """Process an item that might fail."""
    # Processing that might fail
    if item % 7 == 0:
        raise ValueError(f"Item {item} failed")
    return {"item": item, "processed": True}

@durable_execution
def handler(event: dict, context: DurableContext) -> BatchResult[dict]:
    items = list(range(20))
    
    # Succeed if at least 15 items succeed, fail after 5 failures
    config = MapConfig(
        completion_config=CompletionConfig(
            min_successful=15,
            tolerated_failure_count=5,
        )
    )
    
    result = context.map(items, process_item, config=config)
    return result
```

### Using context operations in map functions

Call steps, waits, or other operations inside map functions:

```python
from aws_durable_execution_sdk_python import durable_step, StepContext

@durable_step
def fetch_user_data(step_context: StepContext, user_id: str) -> dict:
    """Fetch user data from external service."""
    return {"user_id": user_id, "name": "Jane Doe", "email": "jane_doe@example.com"}

@durable_step
def send_notification(step_context: StepContext, user: dict) -> dict:
    """Send notification to user."""
    return {"sent": True, "email": user["email"]}

def process_user(
    context: DurableContext,
    user_id: str,
    index: int,
    user_ids: list[str]
) -> dict:
    """Process a user by fetching data and sending notification."""
    # Use steps within the map function
    user = context.step(fetch_user_data(user_id))
    notification = context.step(send_notification(user))
    return {"user_id": user_id, "notification_sent": notification["sent"]}

@durable_execution
def handler(event: dict, context: DurableContext) -> BatchResult[dict]:
    user_ids = ["user_1", "user_2", "user_3"]
    
    result = context.map(user_ids, process_user)
    return result
```

### Filtering and transforming results

Access individual results from the `BatchResult`:

```python
def check_inventory(
    context: DurableContext,
    product_id: str,
    index: int,
    products: list[str]
) -> dict:
    """Check if a product is in stock."""
    # Check if product is in stock
    return {"product_id": product_id, "in_stock": True, "quantity": 10}

@durable_execution
def handler(event: dict, context: DurableContext) -> list[str]:
    product_ids = ["prod_1", "prod_2", "prod_3", "prod_4"]
    
    # Get all inventory results
    batch_result = context.map(product_ids, check_inventory)
    
    # Filter to only in-stock products
    in_stock = [
        r.result["product_id"]
        for r in batch_result.results
        if r.result["in_stock"]
    ]
    
    return in_stock
```

[↑ Back to top](#table-of-contents)

## Best practices

**Use descriptive names** - Name your map operations for easier debugging: `context.map(items, process_item, name="process_orders")`.

**Control concurrency for external calls** - When calling external APIs, use `max_concurrency` to avoid rate limits.

**Batch for efficiency** - For small, fast operations, use `item_batcher` to reduce overhead.

**Define completion criteria** - Use `CompletionConfig` to specify when the operation should succeed or fail.

**Keep map functions focused** - Each map function should process one item. Don't mix collection iteration with item processing.

**Use context operations** - Call steps, waits, or other operations inside map functions for complex processing.

**Handle errors gracefully** - Wrap error-prone code in try-except blocks or use completion criteria to tolerate failures.

**Consider collection size** - For very large collections (10,000+ items), consider batching or processing in chunks.

**Monitor memory usage** - Large collections create many checkpoints. Monitor Lambda memory usage.

**Return only necessary data** - Large result objects increase checkpoint size. Return minimal data from map functions.

[↑ Back to top](#table-of-contents)

## Performance tips

**Parallel execution is automatic** - Items execute concurrently by default. Don't try to manually parallelize.

**Use max_concurrency wisely** - Too much concurrency can overwhelm external services or exhaust Lambda resources. Start conservative and increase as needed.

**Batch small operations** - If each item processes quickly (< 100ms), batching reduces overhead:

```python
config = MapConfig(
    item_batcher=ItemBatcher(max_items_per_batch=10)
)
```

**Optimize map functions** - Keep map functions lightweight. Move heavy computation into steps within the map function.

**Use appropriate completion criteria** - Fail fast with `tolerated_failure_count` to avoid processing remaining items when many fail.

**Monitor checkpoint size** - Large result objects increase checkpoint size and Lambda memory usage. Return only necessary data.

**Consider memory limits** - Processing thousands of items creates many checkpoints. Monitor Lambda memory and adjust batch size or concurrency.

**Profile your workload** - Test with representative data to find optimal concurrency and batch settings.

[↑ Back to top](#table-of-contents)

## FAQ

**Q: What's the difference between map and parallel operations?**

A: Map operations process a collection of similar items using the same function. Parallel operations execute different functions concurrently. Use map for collections, parallel for heterogeneous tasks.

**Q: How many items can I process?**

A: There's no hard limit, but consider Lambda's memory and timeout constraints. For very large collections (10,000+ items), use batching or process in chunks.

**Q: Do items process in order?**

A: Items execute in parallel, so processing order is non-deterministic. However, results maintain the same order as inputs in the `BatchResult`.

**Q: What happens if one item fails?**

A: By default, the map operation continues processing other items. Use `CompletionConfig` to define failure behavior (e.g., fail after N failures).

**Q: Can I use async functions in map operations?**

A: No, map functions must be synchronous. If you need async processing, use `asyncio.run()` inside your map function.

**Q: How do I access individual results?**

A: The `BatchResult` contains a `results` list with each item's result:

```python
batch_result = context.map(items, process_item)
for item_result in batch_result.results:
    print(item_result.result)
```

**Q: Can I nest map operations?**

A: Yes, you can call `context.map()` inside a map function to process nested collections.

**Q: How does batching work?**

A: When you configure `item_batcher`, multiple items are grouped together and passed as a `BatchedInput` to your map function. Process all items in `batch.items`.

**Q: What's the difference between serdes and item_serdes?**

A: `item_serdes` serializes individual item results as they complete. `serdes` serializes the entire `BatchResult` at the end. Use both for custom serialization at different levels.

**Q: How do I handle partial failures?**

A: Check the `BatchResult.results` list. Each result has a status indicating success or failure:

```python
batch_result = context.map(items, process_item)
successful = [r for r in batch_result.results if r.status == "SUCCEEDED"]
failed = [r for r in batch_result.results if r.status == "FAILED"]
```

**Q: Can I use map operations with steps?**

A: Yes, call `context.step()` inside your map function to execute steps for each item.

[↑ Back to top](#table-of-contents)

## Testing

You can test map operations using the testing SDK. The test runner executes your function and lets you inspect individual item results.

### Basic map testing

```python
import pytest
from aws_durable_execution_sdk_python_testing import InvocationStatus
from my_function import handler

@pytest.mark.durable_execution(
    handler=handler,
    lambda_function_name="map_operations",
)
def test_map_operations(durable_runner):
    """Test map operations."""
    with durable_runner:
        result = durable_runner.run(input={}, timeout=10)
    
    # Check overall status
    assert result.status is InvocationStatus.SUCCEEDED
    
    # Check the BatchResult
    batch_result = result.result
    assert batch_result.total_count == 5
    assert batch_result.success_count == 5
    assert batch_result.failure_count == 0
    
    # Check individual results
    assert batch_result.results[0].result == 1
    assert batch_result.results[1].result == 4
    assert batch_result.results[2].result == 9
```

### Inspecting individual items

Use `result.get_map()` to inspect the map operation:

```python
from aws_durable_execution_sdk_python.lambda_service import OperationType

@pytest.mark.durable_execution(
    handler=handler,
    lambda_function_name="map_operations",
)
def test_map_individual_items(durable_runner):
    """Test individual item processing."""
    with durable_runner:
        result = durable_runner.run(input={}, timeout=10)
    
    # Get the map operation
    map_op = result.get_map("square")
    assert map_op is not None
    
    # Verify all items were processed
    assert map_op.result.total_count == 5
    
    # Check specific items
    assert map_op.result.results[0].result == 1
    assert map_op.result.results[2].result == 9
```

### Testing error handling

Test that individual item failures are handled correctly:

```python
@pytest.mark.durable_execution(
    handler=handler_with_errors,
    lambda_function_name="map_with_errors",
)
def test_map_error_handling(durable_runner):
    """Test error handling in map operations."""
    with durable_runner:
        result = durable_runner.run(input={}, timeout=10)
    
    # Function should handle errors based on completion config
    assert result.status is InvocationStatus.SUCCEEDED
    
    batch_result = result.result
    
    # Check that some items succeeded
    successful = [r for r in batch_result.results if r.status == "SUCCEEDED"]
    assert len(successful) > 0
    
    # Check that some items failed
    failed = [r for r in batch_result.results if r.status == "FAILED"]
    assert len(failed) > 0
```

### Testing with configuration

Test map operations with custom configuration:

```python
from aws_durable_execution_sdk_python.config import MapConfig, CompletionConfig

@pytest.mark.durable_execution(
    handler=handler_with_config,
    lambda_function_name="map_with_config",
)
def test_map_with_config(durable_runner):
    """Test map operations with custom configuration."""
    with durable_runner:
        result = durable_runner.run(input={}, timeout=30)
    
    # Verify the map operation completed
    assert result.status is InvocationStatus.SUCCEEDED
    
    # Get the map operation
    map_op = result.get_map("process_items")
    
    # Verify configuration was applied
    assert map_op is not None
    assert map_op.result.total_count > 0
```

For more testing patterns, see:
- [Basic tests](../testing-patterns/basic-tests.md) - Simple test examples
- [Complex workflows](../testing-patterns/complex-workflows.md) - Multi-step workflow testing
- [Best practices](../testing-patterns/best-practices.md) - Testing recommendations

[↑ Back to top](#table-of-contents)

## See also

- [Parallel operations](parallel.md) - Execute different functions concurrently
- [Steps](steps.md) - Understanding step operations
- [Child contexts](child-contexts.md) - Organizing complex workflows
- [Configuration](../api-reference/config.md) - MapConfig and CompletionConfig details
- [BatchResult](../api-reference/result.md) - Working with batch results
- [Examples](https://github.com/awslabs/aws-durable-execution-sdk-python/tree/main/examples/src/map) - More map examples

[↑ Back to top](#table-of-contents)

## License

See the [LICENSE](../../LICENSE) file for our project's licensing.

[↑ Back to top](#table-of-contents)
