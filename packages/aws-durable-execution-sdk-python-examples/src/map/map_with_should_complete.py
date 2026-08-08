"""Example demonstrating map with a custom should_complete predicate.

The predicate gives full control over when a batch completes early,
beyond the threshold-based fields (min_successful, tolerated_failure_count,
tolerated_failure_percentage). Here we stop processing as soon as we
accumulate 3 successful results, regardless of how many items remain.
"""

from typing import Any

from aws_durable_execution_sdk_python.config import (
    CompletionConfig,
    CompletionStatus,
    MapConfig,
)
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


def _should_complete(status: CompletionStatus) -> bool:
    """Complete once 3 items have succeeded."""
    return status.success_count >= 3


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Process items with a custom completion predicate."""
    items: list[int] = list(range(1, 11))  # [1, 2, ..., 10]

    config: MapConfig = MapConfig(
        max_concurrency=2,
        completion_config=CompletionConfig(should_complete=_should_complete),
    )

    results = context.map(
        inputs=items,
        func=lambda ctx, item, index, _: ctx.step(
            lambda _: item * 10, name=f"process-{index}"
        ),
        name="map_should_complete",
        config=config,
    )

    return {
        "success_count": results.success_count,
        "failure_count": results.failure_count,
        "started_count": results.started_count,
        "completion_reason": results.completion_reason.value,
        "results": results.get_results(),
    }
