"""5-6: Invoke target fails, caught (try/catch, execution succeeds)."""

import os
from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    function_name = os.environ["TARGET_FUNCTION_NAME"]
    try:
        context.invoke(function_name, event)
    except Exception:
        pass
    return "fallback"
