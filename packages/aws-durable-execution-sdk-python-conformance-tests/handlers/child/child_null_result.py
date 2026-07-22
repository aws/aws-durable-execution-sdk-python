"""3-16: Child context returning null."""

from typing import Any

from aws_durable_execution_sdk_python.context import (
    DurableContext,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_with_child_context
def null_child(_ctx: DurableContext) -> None:
    return None


@durable_execution
def handler(_event: Any, context: DurableContext) -> None:
    result = context.run_in_child_context(null_child(), name="null-child")
    return result
