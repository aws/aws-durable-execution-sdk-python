"""Pause the execution after every third failed attempt of a step.

Experimental: StepConfig.pause_every may change or be removed in a future
release.

The function's execution role needs lambda:PauseDurableExecution on its own
durable executions. Without it, the step logs a warning and retries as usual.
An operator resumes a paused execution with ResumeDurableExecution.
"""

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


@durable_step
def call_partner_api(step_context: StepContext) -> str:
    # Fail on the first four attempts and succeed on attempt 5.
    if step_context.attempt < 5:
        msg = f"Partner API unavailable on attempt {step_context.attempt}"
        raise RuntimeError(msg)
    return "Partner API answered"


@durable_execution
def handler(_event: Any, context: DurableContext) -> str:
    config = StepConfig(
        retry_strategy=create_retry_strategy(
            RetryStrategyConfig(
                max_attempts=10,
                initial_delay=Duration.from_seconds(1),
                retryable_error_types=[RuntimeError],
            )
        ),
        # Pause after attempts 3, 6, 9. An operator can inspect the error
        # and resume the execution when the partner API is back.
        pause_every=3,
    )

    result: str = context.step(call_partner_api(), config=config)
    return result
