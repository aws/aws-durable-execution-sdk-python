# 4-6: Callback failure (external system reports failure)
from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    callback = context.create_callback(name=event)
    # Do not catch — let the exception propagate so the execution fails.
    return callback.result()
