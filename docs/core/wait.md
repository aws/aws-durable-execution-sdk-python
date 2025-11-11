# Wait Operations

## Table of Contents

- [What are wait operations?](#what-are-wait-operations)
- [When to use wait operations](#when-to-use-wait-operations)
- [Terminology](#terminology)
- [Key features](#key-features)
- [Getting started](#getting-started)
- [Method signature](#method-signature)
- [Duration helpers](#duration-helpers)
- [Naming wait operations](#naming-wait-operations)
- [Multiple sequential waits](#multiple-sequential-waits)
- [Understanding scheduled_end_timestamp](#understanding-scheduled_end_timestamp)
- [Best practices](#best-practices)
- [FAQ](#faq)
- [Alternatives to wait operations](#alternatives-to-wait-operations)
- [Testing](#testing)
- [See also](#see-also)

[← Back to main index](../index.md)

## Terminology

**Wait operation** - A durable operation that pauses execution for a specified duration. Created using `context.wait()`.

**Duration** - A time period specified in seconds, minutes, hours, or days using the `Duration` class.

**Scheduled end timestamp** - The Unix timestamp (in milliseconds) when the wait operation is scheduled to complete.

**Suspend** - The process of pausing execution and saving state. The Lambda function exits and resumes later.

**Resume** - The process of continuing execution after a wait completes. The SDK automatically invokes your function again.

[↑ Back to top](#table-of-contents)

## What are wait operations?

Wait operations pause execution for a specified time. Your function suspends, the Lambda exits, and the system automatically resumes execution when the wait completes.

Unlike `time.sleep()`, waits don't consume Lambda execution time. Your function checkpoints, exits cleanly, and resumes later, even if the wait lasts hours or days.

[↑ Back to top](#table-of-contents)

## When to use wait operations

Use `context.wait()` when you need a simple time-based delay.

**Choose a different method if you need:**
- **Wait for external system response** → Use [`context.wait_for_callback()`](callbacks.md)
- **Wait until a condition is met** → Use [`context.wait_for_condition()`](../advanced/wait-for-condition.md)
- **Wait for a step to complete** → Use [`context.step()`](steps.md)

[↑ Back to top](#table-of-contents)

## Key features

- **Durable pauses** - Execution suspends and resumes automatically
- **Flexible durations** - Specify time in seconds, minutes, hours, or days
- **Named operations** - Identify waits by name for debugging and testing
- **Automatic scheduling** - The SDK handles timing and resumption
- **Sequential waits** - Chain multiple waits together
- **No polling required** - The system invokes your function when ready

[↑ Back to top](#table-of-contents)

## Getting started

Here's a simple example of using a wait operation:

```python
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.config import Duration

@durable_execution
def handler(event: dict, context: DurableContext) -> str:
    """Simple durable function with a wait."""
    # Wait for 5 seconds
    context.wait(duration=Duration.from_seconds(5))
    return "Wait completed"
```

When this function runs:
1. The wait operation is checkpointed with a scheduled end time
2. The Lambda function exits (suspends)
3. After 5 seconds, the system automatically invokes your function again
4. Execution resumes after the wait and returns "Wait completed"

[↑ Back to top](#table-of-contents)

## Method signature

### context.wait()

```python
def wait(
    duration: Duration,
    name: str | None = None,
) -> None
```

**Parameters:**

- `duration` (Duration, required) - How long to wait. Must be at least 1 second. Use `Duration.from_seconds()`, `Duration.from_minutes()`, `Duration.from_hours()`, or `Duration.from_days()` to create a duration.
- `name` (str, optional) - A name for the wait operation. Useful for debugging and testing.

**Returns:** None

**Raises:**
- `ValidationError` - If duration is less than 1 second

[↑ Back to top](#table-of-contents)

## Duration helpers

The `Duration` class provides convenient methods to specify time periods:

```python
from aws_durable_execution_sdk_python.config import Duration

# Wait for 30 seconds
context.wait(duration=Duration.from_seconds(30))

# Wait for 5 minutes
context.wait(duration=Duration.from_minutes(5))

# Wait for 2 hours
context.wait(duration=Duration.from_hours(2))

# Wait for 1 day
context.wait(duration=Duration.from_days(1))
```

If using duration in seconds, you can also create a Duration directly:

```python
# Wait for 300 seconds (5 minutes)
context.wait(duration=Duration(seconds=300))
```

[↑ Back to top](#table-of-contents)

## Naming wait operations

You can name wait operations to make them easier to identify in logs and tests:

```python
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.config import Duration

@durable_execution
def handler(event: dict, context: DurableContext) -> str:
    """Durable function with a named wait."""
    # Wait with explicit name
    context.wait(duration=Duration.from_seconds(2), name="custom_wait")
    return "Wait with name completed"
```

Named waits are helpful when:
- You have multiple waits in your function
- You want to identify specific waits in test assertions
- You're debugging execution flow

[↑ Back to top](#table-of-contents)

## Understanding scheduled_end_timestamp

Each wait operation has a `scheduled_end_timestamp` attribute that indicates when the wait is scheduled to complete. This timestamp is in Unix milliseconds.

You can access this timestamp when inspecting operations in tests or logs. The SDK uses this timestamp to determine when to resume your function.

The scheduled end time is calculated when the wait operation is first checkpointed:
- Current time + wait duration = scheduled end timestamp

[↑ Back to top](#table-of-contents)

## Best practices

### Choose appropriate wait durations

When your function hits a wait, it terminates execution and doesn't incur compute charges during the wait period. The function resumes with a new invocation when the wait completes. Choose durations based on your workflow needs:

```python
# Short wait for rate limiting
context.wait(duration=Duration.from_seconds(30))

# Medium wait for polling intervals
context.wait(duration=Duration.from_minutes(5))

# Long wait for scheduled tasks
context.wait(duration=Duration.from_hours(24))
```

**Note:** If you have concurrent operations running (like parallel or map operations), those continue executing even when the main execution hits a wait. The function waits for all concurrent operations to complete before terminating.

### Use named waits for clarity

Name your waits when you have multiple waits or complex logic:

```python
# Good - clear purpose
context.wait(duration=Duration.from_seconds(60), name="rate_limit_cooldown")
context.wait(duration=Duration.from_minutes(5), name="polling_interval")

# Less clear - unnamed waits
context.wait(duration=Duration.from_seconds(60))
context.wait(duration=Duration.from_minutes(5))
```

### Combine waits with steps

Use waits between steps to implement delays in your workflow:

```python
@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    # Start a process
    job_id = context.step(start_job())
    
    # Wait before checking status
    context.wait(duration=Duration.from_seconds(30), name="initial_delay")
    
    # Check status
    status = context.step(check_job_status(job_id))
    
    return {"job_id": job_id, "status": status}
```

### Avoid very short waits

Waits must be at least 1 second. For very short delays, consider if you actually need a wait:

```python
# Avoid - too short, will raise ValidationError
context.wait(duration=Duration.from_seconds(0))

# Minimum - 1 second
context.wait(duration=Duration.from_seconds(1))

# Better - use meaningful durations
context.wait(duration=Duration.from_seconds(5))
```

[↑ Back to top](#table-of-contents)

## FAQ

### How long can a wait operation last?

There is an upper limit of 1 year - that's the maximum length of an execution.

The wait itself doesn't consume Lambda execution time, your function suspends and resumes later. However, consider cost implications of long-running executions.

### Can I cancel a wait operation?

No, once a wait operation is checkpointed, it will complete after the specified duration. Design your workflows with this in mind.

### Do waits execute in parallel?

No, waits execute sequentially in the order they appear in your code. If you need parallel operations, use `context.parallel()` or `context.map()` instead.

### How accurate are wait durations?

Wait durations are approximate. The actual resume time depends on:
- System scheduling
- Lambda cold start time
- Current system load

### Can I use waits for polling?

You can, but we recommend using `context.wait_for_condition()` instead. It simplifies polling by handling the loop logic for you:

```python
from aws_durable_execution_sdk_python.waits import WaitForConditionConfig, FixedWait

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    job_id = context.step(start_job())
    
    # wait_for_condition handles the polling loop
    def check_status(state, check_context):
        status = get_job_status(state["job_id"])
        return {"job_id": state["job_id"], "status": status}
    
    result = context.wait_for_condition(
        check=check_status,
        config=WaitForConditionConfig(
            initial_state={"job_id": job_id},
            condition=lambda state: state["status"] == "completed",
            wait_strategy=FixedWait(Duration.from_minutes(1))
        )
    )
    return result
```

See [Wait for Condition](../advanced/wait-for-condition.md) for more details.

[↑ Back to top](#table-of-contents)

## Alternatives to wait operations

### Using wait_for_callback for external responses

When you need to wait for an external system to respond, use `context.wait_for_callback()`:

```python
from aws_durable_execution_sdk_python import DurableContext, durable_execution

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    # Wait for external approval
    def submit_for_approval(callback_id: str):
        # Send callback_id to external approval system
        send_to_approval_system(callback_id)
    
    result = context.wait_for_callback(
        submitter=submit_for_approval,
        name="approval_wait"
    )
    return result
```

See [Callbacks](callbacks.md) for more details.

### Using wait_for_condition for polling

When you need to poll until a condition is met, use `context.wait_for_condition()`:

```python
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.waits import WaitForConditionConfig, ExponentialBackoff
from aws_durable_execution_sdk_python.config import Duration

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    # Poll until job completes
    def check_job_status(state, check_context):
        status = get_job_status(state["job_id"])
        return {
            "job_id": state["job_id"],
            "status": status,
            "done": status == "COMPLETED"
        }
    
    result = context.wait_for_condition(
        check=check_job_status,
        config=WaitForConditionConfig(
            initial_state={"job_id": "job-123", "done": False},
            condition=lambda state: state["done"],
            wait_strategy=ExponentialBackoff(
                initial_wait=Duration.from_seconds(5)
            )
        )
    )
    return result
```

See [Wait for Condition](../advanced/wait-for-condition.md) for more details.

[↑ Back to top](#table-of-contents)

## Testing

### Testing wait operations

You can verify wait operations in your tests by inspecting the operations list:

```python
import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from src.wait import wait

@pytest.mark.durable_execution(
    handler=wait.handler,
    lambda_function_name="Wait State",
)
def test_wait(durable_runner):
    """Test wait example."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    
    # Find the wait operation
    wait_ops = [op for op in result.operations if op.operation_type.value == "WAIT"]
    assert len(wait_ops) == 1
    
    # Verify the wait has a scheduled end timestamp
    wait_op = wait_ops[0]
    assert wait_op.scheduled_end_timestamp is not None
```

### Testing multiple waits

When testing functions with multiple waits, you can verify each wait individually:

```python
@pytest.mark.durable_execution(handler=multiple_wait.handler)
def test_multiple_waits(durable_runner):
    """Test multiple sequential waits."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=20)

    assert result.status is InvocationStatus.SUCCEEDED
    
    # Find all wait operations
    wait_ops = [op for op in result.operations if op.operation_type.value == "WAIT"]
    assert len(wait_ops) == 2
    
    # Verify both waits have names
    wait_names = [op.name for op in wait_ops]
    assert "wait-1" in wait_names
    assert "wait-2" in wait_names
```

### Testing named waits

Named waits are easier to identify in tests:

```python
@pytest.mark.durable_execution(handler=wait_with_name.handler)
def test_named_wait(durable_runner):
    """Test wait with custom name."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    
    # Find the named wait operation
    wait_ops = [op for op in result.operations 
                if op.operation_type.value == "WAIT" and op.name == "custom_wait"]
    assert len(wait_ops) == 1
```

[↑ Back to top](#table-of-contents)

## See also

- [Steps](steps.md) - Execute business logic with automatic checkpointing
- [Callbacks](callbacks.md) - Wait for external system responses
- [Wait for Condition](../advanced/wait-for-condition.md) - Poll until a condition is met
- [Getting Started](../getting-started.md) - Learn the basics of durable functions

[↑ Back to top](#table-of-contents)

## License

See the [LICENSE](../../LICENSE) file for our project's licensing.

[↑ Back to main index](../index.md)
