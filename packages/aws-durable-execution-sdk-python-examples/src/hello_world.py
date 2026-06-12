"""Simple durable Lambda handler example.

This example demonstrates:
- Step execution with logging
- Wait operations (pausing without consuming resources)
- Replay-aware logging
- Returning a response
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext, durable_step
from aws_durable_execution_sdk_python.execution import durable_execution

if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.types import StepContext


@durable_step
def step_1(step_context: StepContext) -> None:
    """First step that logs a message."""
    step_context.logger.info("Hello from step1")


@durable_step
def step_2(step_context: StepContext, status_code: int) -> str:
    """Second step that returns a message."""
    step_context.logger.info("Returning message with status code: %d", status_code)
    return f"Hello from Durable Lambda! (status: {status_code})"


@durable_execution
def handler(event: Any, context: DurableContext) -> dict[str, Any]:
    """Durable Lambda handler with steps, waits, and logging.

    Args:
        event: Lambda event input
        context: Durable execution context

    Returns:
        Response dictionary with statusCode and body
    """
    # Execute Step #1 - logs a message
    context.step(step_1())

    # Pause for 10 seconds without consuming CPU cycles or incurring usage charges
    # The execution will suspend here and resume after 10 seconds
    context.wait(Duration.from_seconds(10))

    context.logger.info("Waited for 10 seconds")

    # Execute Step #2 - returns a message with status code
    message = context.step(step_2(status_code=200))

    # Return response
    return {
        "statusCode": 200,
        "body": message,
    }
