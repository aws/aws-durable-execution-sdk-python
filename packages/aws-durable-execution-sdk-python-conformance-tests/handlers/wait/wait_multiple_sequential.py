"""2-3: Multiple sequential waits."""

from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict:
    context.wait(Duration.from_seconds(2), name="wait-1")
    context.wait(Duration.from_seconds(2), name="wait-2")
    return {"completedWaits": 2}
