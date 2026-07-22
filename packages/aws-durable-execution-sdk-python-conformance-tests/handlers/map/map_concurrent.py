"""9-11: Map real concurrency (max-concurrency > 1) preserves index-ordered results."""

from typing import Any

from aws_durable_execution_sdk_python.config import MapConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


def map_fn(_ctx: DurableContext, item: str, _index: int, _items: Any) -> str:
    return item


@durable_execution
def handler(event: Any, context: DurableContext) -> list:
    result = context.map(
        ["r0", "r1", "r2"],
        map_fn,
        name="concurrent",
        config=MapConfig(max_concurrency=2),
    )
    return result.get_results()
