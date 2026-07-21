"""5-15: Invoke with custom payload serdes."""

import json
import os
from typing import Any

from aws_durable_execution_sdk_python.config import InvokeConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.serdes import SerDes, SerDesContext


class UppercasePayloadSerDes(SerDes[Any]):
    """Custom serdes that uppercases string values in the payload."""

    def serialize(self, value: Any, _: SerDesContext) -> str:
        if isinstance(value, str):
            return json.dumps(value.upper())
        return json.dumps(value)

    def deserialize(self, data: str, _: SerDesContext) -> Any:
        return json.loads(data)


@durable_execution
def handler(event: Any, context: DurableContext) -> Any:
    function_name = os.environ["TARGET_FUNCTION_NAME"]
    result = context.invoke(
        function_name,
        event,
        config=InvokeConfig(serdes_payload=UppercasePayloadSerDes()),
    )
    return result
