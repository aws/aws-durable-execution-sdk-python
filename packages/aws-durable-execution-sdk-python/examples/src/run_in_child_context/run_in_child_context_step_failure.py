"""Demonstrates runInChildContext with a failing step followed by a successful wait."""

from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.config import StepConfig, Duration
from aws_durable_execution_sdk_python.retries import (
    RetryStrategyConfig,
    create_retry_strategy,
)


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, bool]:
    """Handler demonstrating runInChildContext with failing step."""

    def child_with_failure(ctx: DurableContext) -> None:
        """Child context with a failing step."""

        retry_config = RetryStrategyConfig(
            max_attempts=3,
            initial_delay=Duration.from_seconds(1),
            max_delay=Duration.from_seconds(10),
            backoff_rate=2.0,
        )
        step_config = StepConfig(retry_strategy=create_retry_strategy(retry_config))

        def failing_step(_: DurableContext) -> None:
            """Step that always fails."""
            raise Exception("Step failed in child context")

        ctx.step(
            failing_step,
            name="failing-step",
            config=step_config,
        )

    try:
        context.run_in_child_context(
            child_with_failure,
            name="child-with-failure",
        )
    except Exception as error:
        # Catch and ignore child context and step errors
        result = {"success": True, "error": str(error)}

    context.wait(Duration.from_seconds(1), name="wait-after-failure")

    return result
