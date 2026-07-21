"""5-7: Invoke large payload (payload near size limit)."""

import os
from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    function_name = os.environ["TARGET_FUNCTION_NAME"]
    large_payload = {"data": "x" * 200000}
    result: str = context.invoke(function_name, large_payload)
    return result
