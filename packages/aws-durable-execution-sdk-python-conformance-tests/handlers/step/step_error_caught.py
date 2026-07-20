"""1-20: Error caught and handled (try/catch) - step fails, error caught, execution continues."""

from typing import Any

from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.retries import RetryPresets


@durable_step
def failing_step(_step_context: StepContext) -> str:
    msg = "Something went wrong"
    raise RuntimeError(msg)


@durable_step
def fallback_step(_step_context: StepContext) -> str:
    return "fallback_result"


@durable_execution
def handler(_event: Any, context: DurableContext) -> str:
    try:
        context.step(
            failing_step(),
            config=StepConfig(retry_strategy=RetryPresets.none()),
        )
    except Exception:
        pass

    result: str = context.step(fallback_step())
    return result
