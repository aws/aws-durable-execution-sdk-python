# Logger integration

The Durable Execution SDK automatically enriches your logs with execution context, making it easy to trace operations across checkpoints and replays. You can use the built-in logger or integrate with Powertools for AWS Lambda (Python) for advanced structured logging.

## Table of contents

- [Key features](#key-features)
- [Terminology](#terminology)
- [Getting started](#getting-started)
- [Method signature](#method-signature)
- [Automatic context enrichment](#automatic-context-enrichment)
- [Adding custom metadata](#adding-custom-metadata)
- [Logger inheritance in child contexts](#logger-inheritance-in-child-contexts)
- [Integration with Powertools for AWS Lambda (Python)](#integration-with-powertools-for-aws-lambda-python)
- [Replay behavior and log deduplication](#replay-behavior-and-log-deduplication)
- [Best practices](#best-practices)
- [FAQ](#faq)
- [Testing logger integration](#testing-logger-integration)
- [See also](#see-also)

[← Back to main index](../index.md)

## Key features

- Automatic log deduplication during replays - logs from completed operations don't repeat
- Automatic enrichment with execution context (execution ARN, parent ID, operation name, attempt number)
- Logger inheritance in child contexts for hierarchical tracing
- Compatible with Python's standard logging and Powertools for AWS Lambda (Python)
- Support for custom metadata through the `extra` parameter
- All standard log levels: debug, info, warning, error, exception

[↑ Back to top](#table-of-contents)

## Terminology

**Log deduplication** - The SDK prevents duplicate logs during replays by tracking completed operations. When your function is checkpointed and resumed, logs from already-completed operations aren't emitted again, keeping your CloudWatch logs clean.

**Context enrichment** - The automatic addition of execution metadata (execution ARN, parent ID, operation name, attempt number) to log entries. The SDK handles this for you, so every log includes tracing information.

**Logger inheritance** - When you create a child context, it inherits the parent's logger and adds its own context information. This creates a hierarchical logging structure that mirrors your execution flow.

**Extra metadata** - Additional key-value pairs you can add to log entries using the `extra` parameter. These merge with the automatic context enrichment.

[↑ Back to top](#table-of-contents)

## Getting started

Access the logger through `context.logger` in your durable functions:

```python
from aws_durable_execution_sdk_python import DurableContext, durable_execution

@durable_execution
def handler(event: dict, context: DurableContext) -> str:
    # Log at the top level
    context.logger.info("Starting workflow", extra={"event_id": event.get("id")})
    
    # Execute a step
    result: str = context.step(
        lambda _: "processed",
        name="process_data",
    )
    
    context.logger.info("Workflow completed", extra={"result": result})
    return result
```

The logger automatically includes execution context in every log entry.

### Integration with Lambda Advanced Log Controls

Durable functions work with Lambda's Advanced Log Controls. You can configure your Lambda function to filter logs by level, which helps reduce CloudWatch Logs costs and noise. When you set a log level filter (like INFO or ERROR), logs below that level are automatically ignored.

For example, if you set your Lambda function's log level to INFO, debug logs won't appear in CloudWatch Logs:

```python
@durable_execution
def handler(event: dict, context: DurableContext) -> str:
    context.logger.debug("This won't appear if log level is INFO or higher")
    context.logger.info("This will appear")
    
    result: str = context.step(
        lambda _: "processed",
        name="process_data",
    )
    
    return result
```

Learn more about configuring log levels in the [Lambda Advanced Log Controls documentation](https://docs.aws.amazon.com/lambda/latest/dg/monitoring-cloudwatchlogs.html#monitoring-cloudwatchlogs-advanced).

[↑ Back to top](#table-of-contents)

## Method signature

The logger provides standard logging methods:

```python
context.logger.debug(msg, *args, extra=None)
context.logger.info(msg, *args, extra=None)
context.logger.warning(msg, *args, extra=None)
context.logger.error(msg, *args, extra=None)
context.logger.exception(msg, *args, extra=None)
```

**Parameters:**
- `msg` (object) - The log message. Can include format placeholders.
- `*args` (object) - Arguments for message formatting.
- `extra` (dict[str, object] | None) - Optional dictionary of additional fields to include in the log entry.

[↑ Back to top](#table-of-contents)

## Automatic context enrichment

The SDK automatically enriches logs with execution metadata:

```python
@durable_execution
def handler(event: dict, context: DurableContext) -> str:
    # This log includes: execution_arn
    context.logger.info("Top-level log")
    
    result: str = context.step(
        lambda _: "processed",
        name="process_data",
    )
    
    # This log includes: execution_arn, parent_id, name, attempt
    context.logger.info("Step completed")
    
    return result
```

**Enriched fields:**
- `execution_arn` - Always present, identifies the durable execution
- `parent_id` - Present in child contexts, identifies the parent operation
- `name` - Present when the operation has a name
- `attempt` - Present in steps, shows the retry attempt number

[↑ Back to top](#table-of-contents)

## Adding custom metadata

Use the `extra` parameter to add custom fields to your logs:

```python
@durable_execution
def handler(event: dict, context: DurableContext) -> str:
    order_id = event.get("order_id")
    
    context.logger.info(
        "Processing order",
        extra={
            "order_id": order_id,
            "customer_id": event.get("customer_id"),
            "priority": "high"
        }
    )
    
    result: str = context.step(
        lambda _: f"order-{order_id}-processed",
        name="process_order",
    )
    
    context.logger.info(
        "Order completed",
        extra={"order_id": order_id, "result": result}
    )
    
    return result
```

Custom fields merge with the automatic context enrichment, so your logs include both execution metadata and your custom data.

[↑ Back to top](#table-of-contents)

## Logger inheritance in child contexts

Child contexts inherit the parent's logger and add their own context:

```python
from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
    durable_with_child_context,
)

@durable_with_child_context
def child_workflow(ctx: DurableContext) -> str:
    # Logger includes parent_id for the child context
    ctx.logger.info("Running in child context")
    
    # Step in child context has nested parent_id
    child_result: str = ctx.step(
        lambda _: "child-processed",
        name="child_step",
    )
    
    ctx.logger.info("Child workflow completed", extra={"result": child_result})
    return child_result

@durable_execution
def handler(event: dict, context: DurableContext) -> str:
    # Top-level logger: only execution_arn
    context.logger.info("Starting workflow", extra={"event_id": event.get("id")})
    
    # Child context inherits logger and adds its own parent_id
    result: str = context.run_in_child_context(
        child_workflow(),
        name="child_workflow"
    )
    
    context.logger.info("Workflow completed", extra={"result": result})
    return result
```

This creates a hierarchical logging structure where you can trace operations from parent to child contexts.

[↑ Back to top](#table-of-contents)

## Integration with Powertools for AWS Lambda (Python)

The SDK is compatible with Powertools for AWS Lambda (Python), giving you structured logging with JSON output and additional features.

**Powertools for AWS Lambda (Python) benefits:**
- JSON structured logging for CloudWatch Logs Insights
- Automatic Lambda context injection (request ID, function name, etc.)
- Correlation IDs for distributed tracing
- Log sampling for cost optimization
- Integration with X-Ray tracing

### Using Powertools for AWS Lambda (Python) directly

You can use Powertools for AWS Lambda (Python) directly in your durable functions:

```python
from aws_lambda_powertools import Logger
from aws_durable_execution_sdk_python import DurableContext, durable_execution

logger = Logger(service="order-processing")

@durable_execution
def handler(event: dict, context: DurableContext) -> str:
    logger.info("Starting workflow")
    
    result: str = context.step(
        lambda _: "processed",
        name="process_data",
    )
    
    logger.info("Workflow completed", extra={"result": result})
    return result
```

This gives you all Powertools for AWS Lambda (Python) features like JSON logging and correlation IDs.

### Integrating with context.logger

For better integration with durable execution, set Powertools for AWS Lambda (Python) on the context:

```python
from aws_lambda_powertools import Logger
from aws_durable_execution_sdk_python import DurableContext, durable_execution

logger = Logger(service="order-processing")

@durable_execution
def handler(event: dict, context: DurableContext) -> str:
    # Set Powertools for AWS Lambda (Python) on the context
    context.set_logger(logger)
    
    # Now context.logger uses Powertools for AWS Lambda (Python) with automatic enrichment
    context.logger.info("Starting workflow", extra={"event_id": event.get("id")})
    
    result: str = context.step(
        lambda _: "processed",
        name="process_data",
    )
    
    context.logger.info("Workflow completed", extra={"result": result})
    return result
```

**Benefits of using context.logger:**
- All Powertools for AWS Lambda (Python) features (JSON logging, correlation IDs, etc.)
- Automatic SDK context enrichment (execution_arn, parent_id, name, attempt)
- Log deduplication during replays (see next section)

The SDK's context enrichment (execution_arn, parent_id, name, attempt) merges with Powertools for AWS Lambda (Python) fields (service, request_id, function_name, etc.) in the JSON output.

[↑ Back to top](#table-of-contents)

## Replay behavior and log deduplication

A critical feature of `context.logger` is that it prevents duplicate logs during replays. When your durable function is checkpointed and resumed, the SDK replays your code to reach the next operation, but logs from completed operations aren't emitted again.

### How context.logger prevents duplicate logs

When you use `context.logger`, the SDK tracks which operations have completed and suppresses logs during replay:

```python
from aws_durable_execution_sdk_python import DurableContext, durable_execution

@durable_execution
def handler(event: dict, context: DurableContext) -> str:
    # This log appears only once, even if the function is replayed
    context.logger.info("Starting workflow")
    
    # Step 1 - logs appear only once
    result1: str = context.step(
        lambda _: "step1-done",
        name="step_1",
    )
    context.logger.info("Step 1 completed", extra={"result": result1})
    
    # Step 2 - logs appear only once
    result2: str = context.step(
        lambda _: "step2-done",
        name="step_2",
    )
    context.logger.info("Step 2 completed", extra={"result": result2})
    
    return f"{result1}-{result2}"
```

**What happens during replay:**
1. First invocation: All logs appear (starting workflow, step 1 completed, step 2 completed)
2. After checkpoint and resume: Only new logs appear (step 2 completed if step 1 was checkpointed)
3. Your CloudWatch logs show each message only once, making them clean and easy to read

### Logging behavior with direct logger usage

When you use a logger directly (not through `context.logger`), logs will be emitted on every replay:

```python
from aws_lambda_powertools import Logger
from aws_durable_execution_sdk_python import DurableContext, durable_execution

logger = Logger(service="order-processing")

@durable_execution
def handler(event: dict, context: DurableContext) -> str:
    # This log appears on every replay
    logger.info("Starting workflow")
    
    result1: str = context.step(
        lambda _: "step1-done",
        name="step_1",
    )
    # This log appears on every replay after step 1
    logger.info("Step 1 completed")
    
    result2: str = context.step(
        lambda _: "step2-done",
        name="step_2",
    )
    # This log appears only once (no more replays after this)
    logger.info("Step 2 completed")
    
    return f"{result1}-{result2}"
```

**What happens during replay:**
1. First invocation: All logs appear once
2. After checkpoint and resume: "Starting workflow" and "Step 1 completed" appear again
3. Your CloudWatch logs show duplicate entries for replayed operations

### Using context.logger with Powertools for AWS Lambda (Python)

To get both log deduplication and Powertools for AWS Lambda (Python) features, set the Powertools Logger on the context:

```python
from aws_lambda_powertools import Logger
from aws_durable_execution_sdk_python import DurableContext, durable_execution

logger = Logger(service="order-processing")

@durable_execution
def handler(event: dict, context: DurableContext) -> str:
    # Set Powertools for AWS Lambda (Python) on the context
    context.set_logger(logger)
    
    # Now you get BOTH:
    # - Powertools for AWS Lambda (Python) features (JSON logging, correlation IDs, etc.)
    # - Log deduplication during replays
    context.logger.info("Starting workflow")
    
    result1: str = context.step(
        lambda _: "step1-done",
        name="step_1",
    )
    context.logger.info("Step 1 completed", extra={"result": result1})
    
    result2: str = context.step(
        lambda _: "step2-done",
        name="step_2",
    )
    context.logger.info("Step 2 completed", extra={"result": result2})
    
    return f"{result1}-{result2}"
```

**Benefits of this approach:**
- Clean logs without duplicates during replays
- JSON structured logging from Powertools for AWS Lambda (Python)
- Automatic context enrichment from the SDK (execution_arn, parent_id, name, attempt)
- Lambda context injection from Powertools for AWS Lambda (Python) (request_id, function_name, etc.)
- Correlation IDs and X-Ray integration from Powertools for AWS Lambda (Python)

### When you might see duplicate logs

You'll still see duplicate logs in these scenarios:
- Logs from operations that fail and retry (this is expected and helpful for debugging)
- Logs outside of durable execution context (before `@durable_execution` decorator runs)
- Logs from code that runs during replay before reaching a checkpoint

This is normal behavior and helps you understand the execution flow.

[↑ Back to top](#table-of-contents)

## Best practices

**Use structured logging with extra fields**

Add context-specific data through the `extra` parameter rather than embedding it in the message string:

```python
# Good - structured and queryable
context.logger.info("Order processed", extra={"order_id": order_id, "amount": 100})

# Avoid - harder to query
context.logger.info(f"Order {order_id} processed with amount 100")
```

**Log at appropriate levels**

- `debug` - Detailed diagnostic information for troubleshooting
- `info` - General informational messages about workflow progress
- `warning` - Unexpected situations that don't prevent execution
- `error` - Error conditions that may need attention
- `exception` - Exceptions with stack traces (use in except blocks)

**Include business context in logs**

Add identifiers that help you trace business operations:

```python
context.logger.info(
    "Processing payment",
    extra={
        "order_id": order_id,
        "customer_id": customer_id,
        "payment_method": "credit_card"
    }
)
```

**Use Powertools for AWS Lambda (Python) for production**

For production workloads, use Powertools for AWS Lambda (Python) to get JSON structured logging and CloudWatch Logs Insights integration:

```python
from aws_lambda_powertools import Logger

logger = Logger(service="my-service")

@durable_execution
def handler(event: dict, context: DurableContext) -> str:
    context.set_logger(logger)
    # Now you get JSON logs with all Powertools for AWS Lambda (Python) features
    context.logger.info("Processing started")
```

**Don't log sensitive data**

Avoid logging sensitive information like passwords, tokens, or personal data:

```python
# Good - log identifiers only
context.logger.info("User authenticated", extra={"user_id": user_id})

# Avoid - don't log sensitive data
context.logger.info("User authenticated", extra={"password": password})
```

[↑ Back to top](#table-of-contents)

## FAQ

**Q: Does logging work during replays?**

Yes, but `context.logger` prevents duplicate logs. When you use `context.logger`, the SDK tracks completed operations and suppresses their logs during replay. This keeps your CloudWatch logs clean and easy to read. If you use a logger directly (not through `context.logger`), you'll see duplicate log entries on every replay.

**Q: How do I filter logs by execution?**

Use the `execution_arn` field that's automatically added to every log entry. In CloudWatch Logs Insights:

```
fields @timestamp, @message, execution_arn
| filter execution_arn = "arn:aws:lambda:us-east-1:123456789012:function:my-function:execution-id"
| sort @timestamp asc
```

**Q: Can I use a custom logger?**

Yes. Any logger that implements the `LoggerInterface` protocol works with the SDK. Use `context.set_logger()` to set your custom logger.

The protocol is defined in `aws_durable_execution_sdk_python.types`:

```python
from typing import Protocol
from collections.abc import Mapping

class LoggerInterface(Protocol):
    def debug(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None: ...

    def info(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None: ...

    def warning(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None: ...

    def error(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None: ...

    def exception(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None: ...
```

Any logger with these methods (like Python's standard `logging.Logger` or Powertools Logger) is compatible.

**Q: What's the difference between the SDK logger and Powertools for AWS Lambda (Python)?**

The SDK provides a logger wrapper that adds execution context. Powertools for AWS Lambda (Python) provides structured JSON logging and Lambda-specific features. You can use them together - set the Powertools Logger on the context, and the SDK will enrich it with execution metadata.

**Q: Do child contexts get their own logger?**

Child contexts inherit the parent's logger and add their own `parent_id` to the context. This creates a hierarchical logging structure where you can trace operations from parent to child.

**Q: How do I change the log level?**

If using Python's standard logging, configure it before your handler:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

If using Powertools for AWS Lambda (Python), set the level when creating the logger:

```python
from aws_lambda_powertools import Logger
logger = Logger(service="my-service", level="DEBUG")
```

**Q: Can I access the underlying logger?**

Yes. Use `context.logger.get_logger()` to access the underlying logger instance if you need to call methods not in the `LoggerInterface`.

[↑ Back to top](#table-of-contents)

## Testing logger integration

You can verify that your durable functions log correctly by capturing log output in tests.

### Example test

```python
import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from src.logger_example import logger_example
from test.conftest import deserialize_operation_payload

@pytest.mark.durable_execution(
    handler=logger_example.handler,
    lambda_function_name="logger example",
)
def test_logger_example(durable_runner):
    """Test logger example."""
    with durable_runner:
        result = durable_runner.run(input={"id": "test-123"}, timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == "processed-child-processed"
```

### Verifying log output

To verify specific log messages, capture log output using Python's logging test utilities:

```python
import logging
import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

@pytest.mark.durable_execution(handler=my_handler)
def test_logging_output(durable_runner, caplog):
    """Test that expected log messages are emitted."""
    with caplog.at_level(logging.INFO):
        with durable_runner:
            result = durable_runner.run(input={"id": "test-123"}, timeout=10)
    
    assert result.status is InvocationStatus.SUCCEEDED
    
    # Verify log messages
    assert "Starting workflow" in caplog.text
    assert "Workflow completed" in caplog.text
```

### Testing with Powertools for AWS Lambda (Python)

When testing with Powertools for AWS Lambda (Python), you can verify structured log output:

```python
import json
import pytest
from aws_lambda_powertools import Logger

@pytest.mark.durable_execution(handler=my_handler)
def test_powertools_logging(durable_runner, caplog):
    """Test Powertools for AWS Lambda (Python) integration."""
    logger = Logger(service="test-service")
    
    with caplog.at_level(logging.INFO):
        with durable_runner:
            result = durable_runner.run(input={"id": "test-123"}, timeout=10)
    
    # Parse JSON log entries
    for record in caplog.records:
        if hasattr(record, 'msg'):
            try:
                log_entry = json.loads(record.msg)
                # Verify Powertools for AWS Lambda (Python) fields
                assert "service" in log_entry
                # Verify SDK enrichment fields
                assert "execution_arn" in log_entry
            except json.JSONDecodeError:
                pass  # Not a JSON log entry
```

[↑ Back to top](#table-of-contents)

## See also

- [Steps](steps.md) - Learn about step operations that use logger enrichment
- [Child contexts](child-contexts.md) - Understand logger inheritance in nested contexts
- [Getting started](../getting-started.md) - Basic durable function setup
- [Powertools for AWS Lambda (Python) - Logger](https://docs.powertools.aws.dev/lambda/python/latest/core/logger/) - Powertools Logger documentation

[↑ Back to top](#table-of-contents)

## License

See the LICENSE file for our project's licensing.

[↑ Back to top](#table-of-contents)
