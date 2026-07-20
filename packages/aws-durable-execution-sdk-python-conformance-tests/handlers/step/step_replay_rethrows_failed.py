"""1-10: Replay re-throws failed step - step logs once, proving no re-execution on replay."""

from typing import Any

from aws_durable_execution_sdk_python.config import Duration, StepConfig
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.retries import RetryPresets


@durable_step
def failing_with_log(step_context: StepContext) -> str:
    step_context.logger.info("step executed")
    msg = "Something went wrong"
    raise RuntimeError(msg)


@durable_execution
def handler(_event: Any, context: DurableContext) -> str:
    try:
        context.step(
            failing_with_log(),
            config=StepConfig(retry_strategy=RetryPresets.none()),
        )
    except Exception as e:
        error_msg = str(e)

    context.wait(Duration.from_seconds(1))
    return f"caught: {error_msg}"
