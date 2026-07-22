"""3-4: Child context error (step fails, execution fails)."""

from typing import Any

from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.retries import RetryPresets


@durable_step
def failing_step(_step_context: StepContext) -> str:
    msg = "Child step failed"
    raise RuntimeError(msg)


@durable_with_child_context
def failing_child(ctx: DurableContext) -> str:
    return ctx.step(
        failing_step(),
        config=StepConfig(retry_strategy=RetryPresets.none()),
    )


@durable_execution
def handler(_event: Any, context: DurableContext) -> str:
    result: str = context.run_in_child_context(failing_child(), name="failing-child")
    return result
