"""3-8: Child context with step retry exhaustion (child fails)."""

from typing import Any

from aws_durable_execution_sdk_python.config import Duration, JitterStrategy, StepConfig
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
def always_fail(_step_context: StepContext) -> str:
    msg = "Always fails"
    raise RuntimeError(msg)


@durable_with_child_context
def exhaust_child(ctx: DurableContext) -> str:
    retry_config = RetryStrategyConfig(
        max_attempts=2,
        initial_delay=Duration.from_seconds(1),
        backoff_rate=1,
        jitter_strategy=JitterStrategy.NONE,
    )

    return ctx.step(
        always_fail(),
        config=StepConfig(retry_strategy=create_retry_strategy(retry_config)),
    )


@durable_execution
def handler(_event: Any, context: DurableContext) -> str:
    result: str = context.run_in_child_context(exhaust_child(), name="exhaust-child")
    return result
