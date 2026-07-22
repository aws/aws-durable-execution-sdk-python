# 4-9: Callback + Wait + success
from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    callback = context.create_callback(name=event)
    context.wait(Duration.from_seconds(5), name="delay")
    return callback.result()
