"""9-4: Map with an empty items list."""

from typing import Any

from aws_durable_execution_sdk_python.config import MapConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


def map_fn(_ctx: DurableContext, item: Any, _index: int, _items: Any) -> Any:
    return item


@durable_execution
def handler(event: Any, context: DurableContext) -> list:
    items = event if isinstance(event, list) else []
    result = context.map(items, map_fn, name="empty", config=MapConfig())
    return result.get_results()
