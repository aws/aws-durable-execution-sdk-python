# 4-11: Callback + Wait + timeout (callback timeout < wait duration)
from typing import Any

from aws_durable_execution_sdk_python.config import CallbackConfig, Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    callback = context.create_callback(
        name=event,
        config=CallbackConfig(timeout=Duration.from_seconds(3)),
    )
    context.wait(Duration.from_seconds(6), name="delay")
    return callback.result()
