"""9-12: Map with FLAT nesting (virtual iteration contexts)."""

from typing import Any

from aws_durable_execution_sdk_python.config import MapConfig, NestingType
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_step
def return_value(_step_context: StepContext, value: str) -> str:
    return value


def map_fn(ctx: DurableContext, item: str, _index: int, _items: Any) -> str:
    return ctx.step(return_value(item))


@durable_execution
def handler(event: Any, context: DurableContext) -> list:
    result = context.map(
        ["fa", "fb"],
        map_fn,
        name="flat",
        config=MapConfig(max_concurrency=1, nesting_type=NestingType.FLAT),
    )
    return result.get_results()
