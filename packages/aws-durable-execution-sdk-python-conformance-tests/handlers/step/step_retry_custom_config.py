"""1-14: Retry with custom config (uses the step context attempt number)."""

from typing import Any

from aws_durable_execution_sdk_python.config import Duration, JitterStrategy, StepConfig
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
def flaky(step_context: StepContext) -> str:
    # Fail on the first two attempts, succeed on the third, using the SDK's
    # built-in durable attempt counter from the step context (1-based).
    if step_context.attempt < 3:
        msg = f"Attempt {step_context.attempt} failed"
        raise RuntimeError(msg)
    return "finally succeeded"


@durable_execution
def handler(_event: Any, context: DurableContext) -> str:
    retry_config = RetryStrategyConfig(
        max_attempts=5,
        initial_delay=Duration.from_seconds(2),
        backoff_rate=3,
        jitter_strategy=JitterStrategy.NONE,
    )

    result: str = context.step(
        flaky(),
        config=StepConfig(retry_strategy=create_retry_strategy(retry_config)),
    )
    return result
