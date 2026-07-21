# 4-2: Create callback with explicit name
from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(_event: Any, context: DurableContext) -> str:
    callback = context.create_callback(name="approval")
    return callback.result()
