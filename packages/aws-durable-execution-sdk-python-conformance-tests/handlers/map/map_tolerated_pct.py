"""9-10: Map tolerated-failure-percentage exceeded (stops early)."""

from typing import Any

from aws_durable_execution_sdk_python.config import CompletionConfig, MapConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


def map_fn(_ctx: DurableContext, item: str, _index: int, _items: Any) -> str:
    if item != "never":
        raise RuntimeError("item failed")
    return item


@durable_execution
def handler(event: Any, context: DurableContext) -> dict:
    result = context.map(
        ["f0", "f1", "never", "never"],
        map_fn,
        name="tolerated-pct",
        config=MapConfig(
            max_concurrency=1,
            completion_config=CompletionConfig(tolerated_failure_percentage=25),
        ),
    )
    return {
        "completionReason": result.completion_reason.value,
        "successCount": result.success_count,
        "failureCount": result.failure_count,
        "totalCount": result.total_count,
    }
