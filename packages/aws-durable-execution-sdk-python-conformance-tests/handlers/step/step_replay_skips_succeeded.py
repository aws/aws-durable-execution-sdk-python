"""1-9: Replay skips succeeded step - step logs once, proving no re-execution on replay."""

from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_step
def compute_with_log(step_context: StepContext) -> str:
    step_context.logger.info("step executed")
    return "cached_value"


@durable_execution
def handler(_event: Any, context: DurableContext) -> str:
    result: str = context.step(compute_with_log())
    context.wait(Duration.from_seconds(1))
    return result
