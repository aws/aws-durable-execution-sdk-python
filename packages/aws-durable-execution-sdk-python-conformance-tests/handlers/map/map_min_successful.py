"""9-7: Map min-successful early completion."""

from typing import Any

from aws_durable_execution_sdk_python.config import CompletionConfig, MapConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


def map_fn(_ctx: DurableContext, item: str, _index: int, _items: Any) -> str:
    return item


@durable_execution
def handler(event: Any, context: DurableContext) -> dict:
    result = context.map(
        ["s0", "s1", "s2", "s3"],
        map_fn,
        name="min-successful",
        config=MapConfig(
            max_concurrency=1,
            completion_config=CompletionConfig(min_successful=2),
        ),
    )
    return {
        "completionReason": result.completion_reason.value,
        "successCount": result.success_count,
        "totalCount": result.total_count,
    }
