"""1-16: Retry specific exception (non-retryable fails) - TransientError not in retryable list."""

from typing import Any

from aws_durable_execution_sdk_python.config import Duration, StepConfig
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


class TransientError(Exception):
    """Custom error that is NOT in the retryable list."""


class ValidationError(Exception):
    """The only error type configured as retryable."""


@durable_step
def throw_transient(_step_context: StepContext) -> str:
    raise TransientError("transient failure")


@durable_execution
def handler(_event: Any, context: DurableContext) -> str:
    retry_config = RetryStrategyConfig(
        max_attempts=3,
        initial_delay=Duration.from_seconds(1),
        retryable_error_types=[ValidationError],
    )

    result: str = context.step(
        throw_transient(),
        config=StepConfig(retry_strategy=create_retry_strategy(retry_config)),
    )
    return result
