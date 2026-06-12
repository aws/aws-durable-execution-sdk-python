"""Example demonstrating parallel with wait operations."""

from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.config import Duration


@durable_execution
def handler(_event: Any, context: DurableContext) -> str:
    """Execute parallel waits."""

    # Call get_results() to extract data and avoid BatchResult serialization
    context.parallel(
        functions=[
            lambda ctx: ctx.wait(Duration.from_seconds(1), name="wait_1_second"),
            lambda ctx: ctx.wait(Duration.from_seconds(2), name="wait_2_seconds"),
            lambda ctx: ctx.wait(Duration.from_seconds(5), name="wait_5_seconds"),
        ],
        name="parallel_waits",
    ).get_results()

    return "Completed waits"
