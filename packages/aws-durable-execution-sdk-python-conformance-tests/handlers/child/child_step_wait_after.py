"""3-18: Child context with step and wait inside, step and wait after."""

from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_step
def inner_step(_step_context: StepContext, value: str) -> str:
    return value


@durable_with_child_context
def step_and_wait_child(ctx: DurableContext, value: str) -> str:
    ctx.step(inner_step(value))
    ctx.wait(Duration.from_seconds(2))
    return value


@durable_step
def outer_step(_step_context: StepContext, value: str) -> str:
    return value


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    context.run_in_child_context(
        step_and_wait_child(str(event)), name="step-wait-child"
    )
    result: str = context.step(outer_step(str(event)))
    context.wait(Duration.from_seconds(2))
    return result
