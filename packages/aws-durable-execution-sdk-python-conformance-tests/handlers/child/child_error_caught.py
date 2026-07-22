"""3-5: Child context error caught (try/catch, execution succeeds)."""

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


@durable_step
def recovery_step(_step_context: StepContext, value: str) -> str:
    return value


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    try:
        context.run_in_child_context(failing_child())
    except Exception:
        pass

    result: str = context.step(recovery_step(str(event)))
    return result
