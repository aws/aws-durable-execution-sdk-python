"""9-13: Map with a custom item namer."""

from typing import Any

from aws_durable_execution_sdk_python.config import MapConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


def map_fn(_ctx: DurableContext, item: int, _index: int, _items: Any) -> int:
    return item * 10


@durable_execution
def handler(event: Any, context: DurableContext) -> list:
    items = event if isinstance(event, list) else [1, 2]
    result = context.map(
        items,
        map_fn,
        name="named-items",
        config=MapConfig(
            max_concurrency=1,
            item_namer=lambda item, index: f"item-{item}",
        ),
    )
    return result.get_results()
