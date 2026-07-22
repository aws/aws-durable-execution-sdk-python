"""9-15: Map suspends inside an iteration; replay skips the completed iteration."""

from typing import Any

from aws_durable_execution_sdk_python.config import Duration, MapConfig
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_step
def return_value(_step_context: StepContext, value: str) -> str:
    return value


def map_fn(ctx: DurableContext, item: str, index: int, _items: Any) -> str:
    # Iteration 1 issues a durable wait before its step, suspending mid-map.
    if index == 1:
        ctx.wait(Duration.from_seconds(1))
    return ctx.step(return_value(item))


@durable_execution
def handler(_event: Any, context: DurableContext) -> list:
    result = context.map(
        ["r0", "r1"], map_fn, name="suspend", config=MapConfig(max_concurrency=1)
    )
    return result.get_results()
