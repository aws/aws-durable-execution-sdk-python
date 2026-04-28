from typing import Any

from aws_durable_execution_sdk_python.config import StepConfig, Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.retries import (
    RetryStrategyConfig,
    create_retry_strategy,
)


@durable_execution
def handler(_event: Any, context: DurableContext) -> str:
    # Step with exponential backoff retry strategy
    retry_config = RetryStrategyConfig(
        max_attempts=3,
        initial_delay=Duration.from_seconds(1),
        max_delay=Duration.from_seconds(10),
        backoff_rate=2.0,
    )

    step_config = StepConfig(retry_strategy=create_retry_strategy(retry_config))

    result = context.step(
        lambda _: "Step with exponential backoff", name="retry_step", config=step_config
    )
    return f"Result: {result}"
