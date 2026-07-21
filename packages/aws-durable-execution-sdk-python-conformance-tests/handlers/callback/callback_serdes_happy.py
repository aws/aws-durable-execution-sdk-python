# 4-15: Callback with custom serdes (happy path - Date roundtrip)
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from aws_durable_execution_sdk_python.config import CallbackConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.serdes import SerDes, SerDesContext


@dataclass
class CustomData:
    id: int
    message: str
    timestamp: datetime


class CustomDataSerDes(SerDes[CustomData]):
    def serialize(self, value: CustomData | None, _: SerDesContext) -> str | None:
        if value is None:
            return None
        return json.dumps(
            {
                "id": value.id,
                "message": value.message,
                "timestamp": value.timestamp.isoformat(),
            }
        )

    def deserialize(self, payload: str | None, _: SerDesContext) -> CustomData | None:
        if payload is None:
            return None
        data = json.loads(payload)
        ts_str = data["timestamp"]
        if ts_str.endswith("Z"):
            ts_str = ts_str[:-1] + "+00:00"
        return CustomData(
            id=data["id"],
            message=data["message"],
            timestamp=datetime.fromisoformat(ts_str),
        )


@durable_execution
def handler(event: Any, context: DurableContext) -> dict[str, Any]:
    callback = context.create_callback(
        name=event,
        config=CallbackConfig(serdes=CustomDataSerDes()),
    )
    result: CustomData = callback.result()
    return {
        "received": {
            "id": result.id,
            "message": result.message,
            "timestamp": int(result.timestamp.timestamp()),
        },
    }
