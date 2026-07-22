# 7-3: Wait-for-callback with anonymous submitter (no name)
from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(_event: Any, context: DurableContext) -> str:
    return context.wait_for_callback(lambda _callback_id, _ctx: None)
