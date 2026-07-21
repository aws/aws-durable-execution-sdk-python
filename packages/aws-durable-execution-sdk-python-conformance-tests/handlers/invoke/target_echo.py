"""Target function that echoes back whatever it receives (with a short wait to ensure caller suspends)."""

from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(event: Any, context: DurableContext) -> Any:
    context.wait(Duration.from_seconds(1))
    return event
