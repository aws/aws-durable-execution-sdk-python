"""5-10: Invoke replay re-throws (failed invoke error re-thrown from cache)."""

import os
from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    function_name = os.environ["TARGET_FUNCTION_NAME"]
    try:
        context.invoke(function_name, event)
    except Exception:
        pass
    context.wait(Duration.from_seconds(1))
    return "caught and continued"
