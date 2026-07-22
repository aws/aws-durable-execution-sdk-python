"""9-14: Map with a custom per-item serdes."""

from typing import Any

from aws_durable_execution_sdk_python.config import MapConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.serdes import SerDes, SerDesContext


class WrapSerDes(SerDes[str]):
    """Real, non-identity serdes: wraps on serialize, unwraps on deserialize (round-trips)."""

    def serialize(self, value: str | None, _: SerDesContext) -> str | None:
        return value if value is None else f"wrapped:{value}"

    def deserialize(self, data: str | None, _: SerDesContext) -> str | None:
        return data if data is None else data.removeprefix("wrapped:")


def map_fn(_ctx: DurableContext, item: str, _index: int, _items: Any) -> str:
    return item.upper()


@durable_execution
def handler(_event: Any, context: DurableContext) -> list:
    result = context.map(
        ["x", "y"],
        map_fn,
        name="serdes",
        config=MapConfig(max_concurrency=1, item_serdes=WrapSerDes()),
    )
    return result.get_results()
