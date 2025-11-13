# Execution Stores

## Table of Contents

- [Overview](#overview)
- [Available stores](#available-stores)
- [In-memory store](#in-memory-store)
- [Filesystem store](#filesystem-store)
- [Choosing a store](#choosing-a-store)
- [Configuration](#configuration)
- [FAQ](#faq)
- [See also](#see-also)

[← Back to main index](../index.md)

## Overview

Execution stores manage how test execution data is persisted during testing. The testing SDK (`aws-durable-execution-sdk-python-testing`) provides different store implementations for different testing scenarios. By default, tests use an in-memory store that's fast and doesn't require cleanup. For scenarios where you need persistence across test runs or want to inspect execution history, you can use a filesystem store.

More store types will be added in future releases to support additional testing scenarios.

[↑ Back to top](#table-of-contents)

## Available stores

The SDK currently provides two store implementations:

- **In-memory store** - Fast, ephemeral storage for standard testing (default)
- **Filesystem store** - Persistent storage that saves executions to disk

Additional store types may be added in future releases.

[↑ Back to top](#table-of-contents)

## In-memory store

The in-memory store keeps execution data in memory during test runs. It's the default store and works well for most testing scenarios.

### Characteristics

- **Fast** - No disk I/O overhead
- **Ephemeral** - Data is lost when tests complete
- **Thread-safe** - Uses locks for concurrent access
- **No cleanup needed** - Memory is automatically freed

### When to use

Use the in-memory store when:
- Running standard unit tests
- You don't need to inspect executions after tests complete
- You want the fastest test execution
- You're running tests in CI/CD pipelines

### Example

The in-memory store is used by default:

```python
import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

@pytest.mark.durable_execution(
    handler=my_handler,
    lambda_function_name="my_function",
)
def test_with_memory_store(durable_runner):
    """Test uses in-memory store by default."""
    with durable_runner:
        result = durable_runner.run(input={"data": "test"}, timeout=10)
    
    assert result.status is InvocationStatus.SUCCEEDED
```

[↑ Back to top](#table-of-contents)

## Filesystem store

The filesystem store persists execution data to disk as JSON files. Each execution is saved in a separate file, making it easy to inspect execution history.

### Characteristics

- **Persistent** - Data survives test runs
- **Inspectable** - JSON files can be viewed and analyzed
- **Configurable location** - Choose where files are stored
- **Automatic directory creation** - Creates storage directory if needed

### When to use

Use the filesystem store when:
- Debugging complex test failures
- You need to inspect execution history
- Running integration tests that span multiple sessions
- Analyzing execution patterns over time

### Example

Configure the filesystem store using environment variables:

```console
# Set store type to filesystem
export AWS_DEX_STORE_TYPE=filesystem

# Optionally set custom storage directory (defaults to .durable_executions)
export AWS_DEX_STORE_PATH=./test-executions

# Run tests
pytest tests/
```

Or configure it programmatically when using the cloud test runner:

```python
from aws_durable_execution_sdk_python_testing.runner import (
    DurableFunctionCloudTestRunner,
    DurableFunctionCloudTestRunnerConfig,
)
from aws_durable_execution_sdk_python_testing.stores.base import StoreType

config = DurableFunctionCloudTestRunnerConfig(
    function_name="my-function",
    region="us-west-2",
    store_type=StoreType.FILESYSTEM,
    store_path="./my-test-executions",
)

runner = DurableFunctionCloudTestRunner(config=config)
```

### Storage format

Executions are stored as JSON files with sanitized ARN names:

```
.durable_executions/
├── arn_aws_states_us-west-2_123456789012_execution_my-function_abc123.json
├── arn_aws_states_us-west-2_123456789012_execution_my-function_def456.json
└── arn_aws_states_us-west-2_123456789012_execution_my-function_ghi789.json
```

Each file contains the complete execution state including operations, checkpoints, and results.

[↑ Back to top](#table-of-contents)

## Choosing a store

Use this guide to choose the right store for your needs:

| Scenario | Recommended Store | Reason |
|----------|------------------|---------|
| Unit tests | In-memory | Fast, no cleanup needed |
| CI/CD pipelines | In-memory | Fast, ephemeral |
| Debugging failures | Filesystem | Inspect execution history |
| Integration tests | Filesystem | Persist across sessions |
| Performance testing | In-memory | Minimize I/O overhead |
| Execution analysis | Filesystem | Analyze patterns over time |

[↑ Back to top](#table-of-contents)

## Configuration

### Environment variables

Configure stores using environment variables:

```console
# Store type (memory or filesystem)
export AWS_DEX_STORE_TYPE=filesystem

# Storage directory for filesystem store (optional, defaults to .durable_executions)
export AWS_DEX_STORE_PATH=./test-executions
```

### Programmatic configuration

Configure stores when creating a cloud test runner:

```python
from aws_durable_execution_sdk_python_testing.runner import (
    DurableFunctionCloudTestRunner,
    DurableFunctionCloudTestRunnerConfig,
)
from aws_durable_execution_sdk_python_testing.stores.base import StoreType

# In-memory store (default)
config = DurableFunctionCloudTestRunnerConfig(
    function_name="my-function",
    region="us-west-2",
    store_type=StoreType.MEMORY,
)

# Filesystem store
config = DurableFunctionCloudTestRunnerConfig(
    function_name="my-function",
    region="us-west-2",
    store_type=StoreType.FILESYSTEM,
    store_path="./my-executions",
)

runner = DurableFunctionCloudTestRunner(config=config)
```

### Default values

If not specified:
- Store type defaults to `MEMORY`
- Filesystem store path defaults to `.durable_executions`

[↑ Back to top](#table-of-contents)

## FAQ

**Q: Can I switch stores between test runs?**

A: Yes, you can change the store type at any time. However, executions stored in one store won't be available in another.

**Q: Does the filesystem store clean up old executions?**

A: No, the filesystem store doesn't automatically delete old executions. You need to manually clean up the storage directory when needed.

**Q: Can I use the filesystem store with the local test runner?**

A: The filesystem store is primarily designed for the cloud test runner. The local test runner uses an in-memory store by default.

**Q: Are execution files human-readable?**

A: Yes, execution files are stored as formatted JSON and can be opened in any text editor.

**Q: What happens if the storage directory doesn't exist?**

A: The filesystem store automatically creates the directory if it doesn't exist.

**Q: Can I use a custom store implementation?**

A: The SDK defines an `ExecutionStore` protocol that you can implement for custom storage backends. However, this is an advanced use case.

**Q: Will more store types be added?**

A: Yes, additional store types may be added in future releases to support more testing scenarios.

**Q: Does the in-memory store support concurrent tests?**

A: Yes, the in-memory store is thread-safe and supports concurrent test execution.

**Q: How much disk space does the filesystem store use?**

A: Each execution typically uses a few KB to a few MB depending on the number of operations and data size. Monitor your storage directory if running many tests.

[↑ Back to top](#table-of-contents)

## See also

- [Basic tests](basic-tests.md) - Simple test patterns
- [Cloud testing](../advanced/cloud-testing.md) - Testing with deployed functions
- [Test runner](../core/test-runner.md) - Test runner configuration
- [Best practices](best-practices.md) - Testing recommendations

[↑ Back to top](#table-of-contents)

## License

See the [LICENSE](../../LICENSE) file for our project's licensing.

[↑ Back to top](#table-of-contents)
