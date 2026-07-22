"""5-2: Invoke with name (explicit name parameter from input)."""

import os
from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(event: Any, context: DurableContext) -> Any:
    function_name = os.environ["TARGET_FUNCTION_NAME"]
    return context.invoke(function_name, event["payload"], name=event["name"])
