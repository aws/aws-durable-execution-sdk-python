# 4-16: Callback with custom serdes (numeric)
import json
from typing import Any

from aws_durable_execution_sdk_python.config import CallbackConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.serdes import SerDes, SerDesContext


class NumericSerDes(SerDes[int]):
    def serialize(self, value: int | None, _: SerDesContext) -> str | None:
        if value is None:
            return None
        return json.dumps(value)

    def deserialize(self, payload: str | None, _: SerDesContext) -> int | None:
        if payload is None:
            return None
        return int(json.loads(payload))


@durable_execution
def handler(event: Any, context: DurableContext) -> dict[str, Any]:
    callback = context.create_callback(
        name=event,
        config=CallbackConfig(serdes=NumericSerDes()),
    )
    value = callback.result()
    return {"count": value, "doubled": value * 2}
