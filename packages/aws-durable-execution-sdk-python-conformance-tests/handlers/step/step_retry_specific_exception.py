"""1-15: Retry specific exception (uses the step context attempt number)."""

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
    """Custom transient error that should be retried."""


@durable_step
def transient_on_first(step_context: StepContext) -> str:
    # Fail on the first attempt, succeed on the second, using the SDK's
    # built-in durable attempt counter from the step context (1-based).
    if step_context.attempt < 2:
        raise TransientError("Temporary failure")
    return "recovered from transient"


@durable_execution
def handler(_event: Any, context: DurableContext) -> str:
    retry_config = RetryStrategyConfig(
        max_attempts=3,
        initial_delay=Duration.from_seconds(1),
        retryable_error_types=[TransientError],
    )

    result: str = context.step(
        transient_on_first(),
        config=StepConfig(retry_strategy=create_retry_strategy(retry_config)),
    )
    return result
