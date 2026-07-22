"""3-1: Child context basic (single step inside)."""

from typing import Any

from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_step
def child_step(_step_context: StepContext, value: str) -> str:
    return value


@durable_with_child_context
def child_operation(ctx: DurableContext, value: str) -> str:
    return ctx.step(child_step(value))


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    result: str = context.run_in_child_context(child_operation(str(event)))
    return result
