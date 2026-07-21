# 4-12: Callback success → Wait → verify replay
from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    callback = context.create_callback(name=event)
    cb_result = callback.result()
    context.wait(Duration.from_seconds(2), name="after-cb")
    return cb_result
