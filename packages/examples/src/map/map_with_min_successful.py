"""Example demonstrating map with min_successful completion config."""

from typing import Any

from aws_durable_execution_sdk_python.config import CompletionConfig, MapConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Process items with min_successful threshold."""
    items = list(range(1, 11))  # [1, 2, 3, ..., 10]

    # Configure to complete when 6 items succeed
    config = MapConfig(
        max_concurrency=5,
        completion_config=CompletionConfig(min_successful=6),
    )

    results = context.map(
        inputs=items,
        func=lambda ctx, item, index, _: ctx.step(
            lambda _: _process_item(item), name=f"item_{index}"
        ),
        name="map_min_successful",
        config=config,
    )

    return {
        "success_count": results.success_count,
        "failure_count": results.failure_count,
        "total_count": results.total_count,
        "results": results.get_results(),
        "completion_reason": results.completion_reason.value,
    }


def _process_item(item: int) -> int:
    """Process item - fails for items 7, 8, 9."""
    if item in [7, 8, 9]:
        raise ValueError(f"Item {item} failed")
    return item * 2
