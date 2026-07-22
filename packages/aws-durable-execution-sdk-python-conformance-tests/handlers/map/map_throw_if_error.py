"""9-6: Map throw-if-error propagates an item failure to the execution."""

from typing import Any

from aws_durable_execution_sdk_python.config import CompletionConfig, MapConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


def map_fn(_ctx: DurableContext, item: str, _index: int, _items: Any) -> str:
    if item == "fail":
        raise RuntimeError("item failed")
    return item


@durable_execution
def handler(event: Any, context: DurableContext) -> list:
    result = context.map(
        ["fail", "never"],
        map_fn,
        name="throwing",
        config=MapConfig(
            max_concurrency=1,
            completion_config=CompletionConfig(tolerated_failure_count=0),
        ),
    )
    # Rethrow the first item failure; uncaught -> the execution fails.
    result.throw_if_error()
    return result.get_results()
