"""5-8: Invoke timeout, caught (timeout caught, execution continues)."""

import os
from typing import Any

from aws_durable_execution_sdk_python.config import Duration, InvokeConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    function_name = os.environ["TARGET_FUNCTION_NAME"]
    try:
        context.invoke(
            function_name, event, config=InvokeConfig(timeout=Duration.from_seconds(5))
        )
    except Exception:
        pass
    return "timeout fallback"
