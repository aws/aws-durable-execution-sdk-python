"""3-13: Child context with wait inside - verify replay."""

from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_with_child_context
def wait_child(ctx: DurableContext, value: str) -> str:
    ctx.wait(Duration.from_seconds(1))
    return value


@durable_step
def after_child_step(_step_context: StepContext, value: str) -> str:
    return value


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    context.run_in_child_context(wait_child(str(event)), name="wait-child")
    result: str = context.step(after_child_step(str(event)))
    return result
