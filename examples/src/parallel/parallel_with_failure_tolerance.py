"""Example demonstrating parallel with failure tolerance."""

from typing import Any

from aws_durable_execution_sdk_python.config import (
    CompletionConfig,
    ParallelConfig,
    StepConfig,
)
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.retries import RetryStrategyConfig


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Execute tasks with failure tolerance."""

    # Tolerate up to 2 failures
    config = ParallelConfig(
        completion_config=CompletionConfig(tolerated_failure_count=2)
    )

    # Disable retries so failures happen immediately
    step_config = StepConfig(retry_strategy=RetryStrategyConfig(max_attempts=1))

    results = context.parallel(
        functions=[
            lambda ctx: ctx.step(
                lambda _: "success 1", name="task1", config=step_config
            ),
            lambda ctx: ctx.step(
                lambda _: _failing_task(2), name="task2", config=step_config
            ),
            lambda ctx: ctx.step(
                lambda _: "success 3", name="task3", config=step_config
            ),
            lambda ctx: ctx.step(
                lambda _: _failing_task(4), name="task4", config=step_config
            ),
            lambda ctx: ctx.step(
                lambda _: "success 5", name="task5", config=step_config
            ),
        ],
        name="parallel_with_tolerance",
        config=config,
    )

    return {
        "success_count": results.success_count,
        "failure_count": results.failure_count,
        "succeeded": results.get_results(),
        "completion_reason": results.completion_reason.value,
    }


def _failing_task(task_num: int) -> str:
    """Task that always fails."""
    raise ValueError(f"Task {task_num} failed")
