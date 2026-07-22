"""3-3: Child context with multiple sequential steps."""

from typing import Any

from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_step
def first_step(_step_context: StepContext, value: str) -> str:
    return value


@durable_step
def second_step(_step_context: StepContext, value: str) -> str:
    return value


@durable_with_child_context
def multi_step_child(ctx: DurableContext, value: str) -> str:
    result1: str = ctx.step(first_step(value))
    result2: str = ctx.step(second_step(result1))
    return result2


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    result: str = context.run_in_child_context(
        multi_step_child(str(event)), name="multi-step"
    )
    return result
