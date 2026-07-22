# 4-7: Callback + Step + failure
from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    callback = context.create_callback(name=event)
    context.step(lambda _: "notified", name="notify-external")
    return callback.result()
