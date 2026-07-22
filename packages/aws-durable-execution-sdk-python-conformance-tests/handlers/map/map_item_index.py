"""9-3: Map function receives item and index."""

from typing import Any

from aws_durable_execution_sdk_python.config import MapConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


def map_fn(_ctx: DurableContext, item: int, index: int, _items: Any) -> int:
    return item + index


@durable_execution
def handler(event: Any, context: DurableContext) -> list:
    items = event if isinstance(event, list) else [10, 20, 30]
    result = context.map(
        items, map_fn, name="indexed", config=MapConfig(max_concurrency=1)
    )
    return result.get_results()
