"""Example demonstrating map with failure tolerance."""

from typing import Any

from aws_durable_execution_sdk_python.config import (
    CompletionConfig,
    MapConfig,
    StepConfig,
)
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.retries import RetryStrategyConfig


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Process items with failure tolerance."""
    items = list(range(1, 11))  # [1, 2, 3, ..., 10]

    # Tolerate up to 3 failures
    config = MapConfig(
        max_concurrency=5,
        completion_config=CompletionConfig(tolerated_failure_count=3),
    )

    # Disable retries so failures happen immediately
    step_config = StepConfig(retry_strategy=RetryStrategyConfig(max_attempts=1))

    results = context.map(
        inputs=items,
        func=lambda ctx, item, index, _: ctx.step(
            lambda _: _process_with_failures(item),
            name=f"item_{index}",
            config=step_config,
        ),
        name="map_with_tolerance",
        config=config,
    )

    return {
        "success_count": results.success_count,
        "failure_count": results.failure_count,
        "succeeded": [item.result for item in results.succeeded()],
        "failed_count": len(results.failed()),
        "completion_reason": results.completion_reason.value,
    }


def _process_with_failures(item: int) -> int:
    """Process item - fails for items 3, 6, 9."""
    if item % 3 == 0:
        raise ValueError(f"Item {item} failed")
    return item * 2
