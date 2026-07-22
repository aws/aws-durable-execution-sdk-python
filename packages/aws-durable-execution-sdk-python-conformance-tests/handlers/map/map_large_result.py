"""9-16: Map with a large aggregate result (exceeds the checkpoint size threshold)."""

from typing import Any

from aws_durable_execution_sdk_python.config import MapConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


def map_fn(_ctx: DurableContext, _item: int, _index: int, _items: Any) -> str:
    # ~70KB per item; 4 items -> ~280KB aggregate, exceeding the 256KB threshold.
    return "x" * 70000


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict:
    result = context.map(
        [0, 1, 2, 3], map_fn, name="large", config=MapConfig(max_concurrency=1)
    )
    return {
        "successCount": result.success_count,
        "totalCount": result.total_count,
    }
