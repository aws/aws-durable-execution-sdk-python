"""9-17: Suspension after a successful map (replay skips the completed map)."""

from typing import Any

from aws_durable_execution_sdk_python.config import Duration, MapConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


def map_fn(_ctx: DurableContext, item: str, _index: int, _items: Any) -> str:
    return item.upper()


@durable_execution
def handler(_event: Any, context: DurableContext) -> list:
    result = context.map(
        ["a", "b"], map_fn, name="then-wait", config=MapConfig(max_concurrency=1)
    )
    # Suspend after the map; on replay the completed map is skipped.
    context.wait(Duration.from_seconds(1))
    return result.get_results()
