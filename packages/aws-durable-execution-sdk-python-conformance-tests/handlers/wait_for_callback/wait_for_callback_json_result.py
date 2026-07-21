# 7-11: Wait-for-callback with structured (JSON) result deserialization
import json
from typing import Any

from aws_durable_execution_sdk_python.config import WaitForCallbackConfig
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    WaitForCallbackContext,
)
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.serdes import SerDes, SerDesContext


class JsonObjectSerDes(SerDes[dict]):
    """Deserializes the callback payload as a JSON object."""

    def serialize(self, value: dict | None, _: SerDesContext) -> str | None:
        if value is None:
            return None
        return json.dumps(value)

    def deserialize(self, payload: str | None, _: SerDesContext) -> dict | None:
        if payload is None:
            return None
        return json.loads(payload)


def submitter(_callback_id: str, _context: WaitForCallbackContext) -> None:
    """Submitter completes without side effects."""


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    config = WaitForCallbackConfig(serdes=JsonObjectSerDes())
    result = context.wait_for_callback(submitter, name=event, config=config)
    return result["status"]
