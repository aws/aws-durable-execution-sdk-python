"""5-14: Multiple sequential invokes."""

import os
from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    function_name_1 = os.environ["TARGET_FUNCTION_NAME_1"]
    function_name_2 = os.environ["TARGET_FUNCTION_NAME_2"]
    result1: str = context.invoke(function_name_1, event)
    result2: str = context.invoke(function_name_2, result1)
    return result2
