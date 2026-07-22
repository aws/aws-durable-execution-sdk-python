"""3-7: Child context with step retry (fails then succeeds)."""

from typing import Any

from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.retries import (
    RetryStrategyConfig,
    create_retry_strategy,
)


@durable_step
def unreliable_step(step_context: StepContext, *, value: str) -> str:
    # Fail on the first attempt, succeed on the second, using the SDK's
    # built-in durable attempt counter from the step context (1-based).
    if step_context.attempt < 2:
        msg = f"Attempt {step_context.attempt} failed"
        raise RuntimeError(msg)
    return value


@durable_with_child_context
def retry_child(ctx: DurableContext, *, value: str) -> str:
    retry_config = RetryStrategyConfig(
        max_attempts=3,
        retryable_error_types=[RuntimeError],
    )

    return ctx.step(
        unreliable_step(value=value),
        config=StepConfig(retry_strategy=create_retry_strategy(retry_config)),
    )


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    result: str = context.run_in_child_context(
        retry_child(value=str(event)),
        name="retry-child",
    )
    return result
