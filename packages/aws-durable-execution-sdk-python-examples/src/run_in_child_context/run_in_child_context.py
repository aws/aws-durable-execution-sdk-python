from typing import Any

from aws_durable_execution_sdk_python.context import (
    DurableContext,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.execution import durable_execution


def multiply_by_two(value: int) -> int:
    return value * 2


@durable_with_child_context
def child_operation(ctx: DurableContext, value: int) -> int:
    return ctx.step(lambda _: multiply_by_two(value), name="multiply")


@durable_execution
def handler(_event: Any, context: DurableContext) -> str:
    result = context.run_in_child_context(child_operation(5))
    return f"Child context result: {result}"
