# 4-8: Callback + Step + timeout
from typing import Any

from aws_durable_execution_sdk_python.config import CallbackConfig, Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    callback = context.create_callback(
        name=event,
        config=CallbackConfig(timeout=Duration.from_seconds(5)),
    )
    context.step(lambda _: "notified", name="notify-external")
    return callback.result()
