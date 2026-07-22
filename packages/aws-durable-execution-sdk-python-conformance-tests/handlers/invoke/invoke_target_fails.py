"""5-5: Invoke target fails (execution fails with InvokeError)."""

import os
from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    function_name = os.environ["TARGET_FUNCTION_NAME"]
    result: str = context.invoke(function_name, event)
    return result
