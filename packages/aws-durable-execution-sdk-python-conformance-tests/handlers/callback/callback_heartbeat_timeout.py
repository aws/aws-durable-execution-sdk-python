# 4-4: Create callback heartbeat timeout (no heartbeat sent)
from typing import Any

from aws_durable_execution_sdk_python.config import CallbackConfig, Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    callback = context.create_callback(
        name=event,
        config=CallbackConfig(heartbeat_timeout=Duration.from_seconds(5)),
    )
    return callback.result()
