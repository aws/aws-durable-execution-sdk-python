"""8-15: Parallel with a custom per-branch serde (item_serdes) round-trips results."""

import json
from typing import Any

from aws_durable_execution_sdk_python.config import ParallelConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.serdes import SerDes, SerDesContext


class WrapSerDes(SerDes[str]):
    """Symmetric serde: wraps a value as {"wrapped": v} and unwraps it back to v."""

    def serialize(self, value: str, _: SerDesContext) -> str:
        return json.dumps({"wrapped": value})

    def deserialize(self, data: str, _: SerDesContext) -> str:
        return json.loads(data)["wrapped"]


def branch0(_ctx: DurableContext) -> str:
    return "x"


def branch1(_ctx: DurableContext) -> str:
    return "y"


@durable_execution
def handler(event: Any, context: DurableContext) -> list:
    result = context.parallel(
        [branch0, branch1],
        name="serde",
        config=ParallelConfig(max_concurrency=1, item_serdes=WrapSerDes()),
    )
    return result.get_results()
