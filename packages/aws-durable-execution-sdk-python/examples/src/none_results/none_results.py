"""Demonstrates handling of operations that return undefined values during replay."""

from typing import Any

from aws_durable_execution_sdk_python.context import (
    DurableContext,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.config import Duration


@durable_with_child_context
def parent_context(ctx: DurableContext) -> None:
    """Parent context that returns None."""
    return None


@durable_execution
def handler(_event: Any, context: DurableContext) -> str:
    """Handler demonstrating operations with undefined/None results."""
    context.step(
        lambda _: None,
        name="fetch-user",
    )

    context.run_in_child_context(parent_context(), name="parent")

    context.wait(Duration.from_seconds(1), name="wait")

    return "result"
