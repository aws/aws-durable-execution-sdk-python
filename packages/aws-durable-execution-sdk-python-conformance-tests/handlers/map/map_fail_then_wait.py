"""9-18: Suspension after a map that completed with a failure (replay skips the completed map)."""

from typing import Any

from aws_durable_execution_sdk_python.config import (
    CompletionConfig,
    Duration,
    MapConfig,
)
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


def map_fn(_ctx: DurableContext, item: str, _index: int, _items: Any) -> str:
    if item == "fail":
        raise RuntimeError("item failed")
    return item


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict:
    result = context.map(
        ["ok", "fail"],
        map_fn,
        name="fail-then-wait",
        config=MapConfig(
            max_concurrency=1,
            completion_config=CompletionConfig(tolerated_failure_count=1),
        ),
    )
    # Suspend after the map (which recorded a failure); on replay the completed map is skipped.
    context.wait(Duration.from_seconds(1))
    return {
        "completionReason": result.completion_reason.value,
        "status": result.status.value,
        "successCount": result.success_count,
        "failureCount": result.failure_count,
        "totalCount": result.total_count,
    }
