from itertools import count
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


# Counter for deterministic behavior across retries
_attempts = count(1)  # starts from 1


@durable_step
def unreliable_operation(
    _step_context: StepContext,
) -> str:
    # Use counter for deterministic behavior
    # Will fail on first attempt, succeed on second
    attempt = next(_attempts)
    if attempt < 2:
        msg = f"Attempt {attempt} failed"
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
