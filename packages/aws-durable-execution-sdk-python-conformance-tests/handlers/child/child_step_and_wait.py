"""3-10: Child context with step and wait inside."""

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
def compute_step(_step_context: StepContext, value: str) -> str:
    return value


@durable_with_child_context
def mixed_ops_child(ctx: DurableContext, value: str) -> str:
    ctx.step(compute_step(value))
    ctx.wait(Duration.from_seconds(2))
    return value


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    result: str = context.run_in_child_context(
        mixed_ops_child(str(event)), name="mixed-ops"
    )
    return result
