"""9-1: Map basic (one step per item, all succeed)."""

from typing import Any

from aws_durable_execution_sdk_python.config import MapConfig
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_step
def greet(_step_context: StepContext, item: str) -> str:
    return f"Hello, {item}!"


def map_fn(ctx: DurableContext, item: str, _index: int, _items: Any) -> str:
    return ctx.step(greet(item))


@durable_execution
def handler(event: Any, context: DurableContext) -> list:
    items = event if isinstance(event, list) else ["World", "Kiro"]
    result = context.map(items, map_fn, name="map", config=MapConfig(max_concurrency=1))
    return result.get_results()
