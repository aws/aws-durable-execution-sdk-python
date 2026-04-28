"""Example demonstrating multiple sequential wait operations."""

from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.config import Duration


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Handler demonstrating multiple sequential wait operations."""
    context.wait(Duration.from_seconds(5), name="wait-1")
    context.wait(Duration.from_seconds(5), name="wait-2")

    return {
        "completedWaits": 2,
        "finalStep": "done",
    }
