"""Demonstrates step execution tracking when no replay occurs."""

from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, bool]:
    """Handler demonstrating step execution without replay."""
    context.step(lambda _: "user-1", name="fetch-user-1")
    context.step(lambda _: "user-2", name="fetch-user-2")

    return {"completed": True}
