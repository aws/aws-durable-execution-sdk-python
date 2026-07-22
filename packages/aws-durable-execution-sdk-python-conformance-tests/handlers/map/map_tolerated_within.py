"""9-8: Map tolerated-failure-count within tolerance (all items complete)."""

from typing import Any

from aws_durable_execution_sdk_python.config import CompletionConfig, MapConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


def map_fn(_ctx: DurableContext, item: str, _index: int, _items: Any) -> str:
    if item == "fail":
        raise RuntimeError("item failed")
    return item


@durable_execution
def handler(event: Any, context: DurableContext) -> dict:
    result = context.map(
        ["s0", "fail", "s2"],
        map_fn,
        name="tolerated",
        config=MapConfig(
            max_concurrency=1,
            completion_config=CompletionConfig(tolerated_failure_count=1),
        ),
    )
    return {
        "completionReason": result.completion_reason.value,
        "status": result.status.value,
        "successCount": result.success_count,
        "failureCount": result.failure_count,
        "totalCount": result.total_count,
    }
