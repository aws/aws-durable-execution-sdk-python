"""3-6: Nested child contexts."""

from typing import Any

from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_step
def outer_step(_step_context: StepContext, value: str) -> str:
    return value


@durable_step
def inner_step(_step_context: StepContext, value: str) -> str:
    return value


@durable_with_child_context
def inner_child(ctx: DurableContext, value: str) -> str:
    return ctx.step(inner_step(value))


@durable_with_child_context
def outer_child(ctx: DurableContext, value: str) -> str:
    ctx.step(outer_step(value))
    inner_result: str = ctx.run_in_child_context(inner_child(value), name="inner")
    return inner_result


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    result: str = context.run_in_child_context(outer_child(str(event)), name="outer")
    return result
