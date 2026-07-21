"""5-16: Invoke with custom result serdes."""

import os
from typing import Any

from aws_durable_execution_sdk_python.config import InvokeConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.serdes import SerDes, SerDesContext


class UppercaseResultSerDes(SerDes[str]):
    """Custom serdes that uppercases the result on deserialization."""

    def serialize(self, value: str, _: SerDesContext) -> str:
        return value

    def deserialize(self, data: str, _: SerDesContext) -> str:
        return data.upper()


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    function_name = os.environ["TARGET_FUNCTION_NAME"]
    result: str = context.invoke(
        function_name,
        event,
        config=InvokeConfig(serdes_result=UppercaseResultSerDes()),
    )
    return result
