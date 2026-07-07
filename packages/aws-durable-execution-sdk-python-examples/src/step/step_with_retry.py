from typing import Any

from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.retries import (
    RetryStrategyConfig,
    create_retry_strategy,
)


@durable_step
def unreliable_operation(
    step_context: StepContext,
) -> str:
    # Fail on the first durable step attempt, then let the retry succeed.
    if step_context.attempt < 2:
        msg = f"Attempt {step_context.attempt} failed"
        raise RuntimeError(msg)
    return "Operation succeeded"


@durable_execution
def handler(_event: Any, context: DurableContext) -> str:
    retry_config = RetryStrategyConfig(
        max_attempts=3,
        retryable_error_types=[RuntimeError],
    )

    result: str = context.step(
        unreliable_operation(),
        config=StepConfig(create_retry_strategy(retry_config)),
    )

    return result
