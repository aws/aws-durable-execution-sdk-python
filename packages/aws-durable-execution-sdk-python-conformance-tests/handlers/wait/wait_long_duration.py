"""2-5: Wait with long duration (1 hour)."""

from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(_event: Any, context: DurableContext) -> str:
    context.wait(Duration.from_hours(1))
    return "Wait with hours completed"
