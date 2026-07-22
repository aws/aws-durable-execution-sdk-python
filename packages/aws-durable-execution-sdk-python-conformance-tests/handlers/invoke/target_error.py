"""Target function that waits briefly then raises an exception."""

from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(_event: Any, context: DurableContext) -> str:
    context.wait(Duration.from_seconds(1))
    msg = "target function error"
    raise RuntimeError(msg)
