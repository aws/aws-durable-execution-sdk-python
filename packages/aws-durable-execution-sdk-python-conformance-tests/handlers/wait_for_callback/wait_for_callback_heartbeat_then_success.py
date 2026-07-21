# 7-13: Wait-for-callback with heartbeat then success
from typing import Any

from aws_durable_execution_sdk_python.config import Duration, WaitForCallbackConfig
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    WaitForCallbackContext,
)
from aws_durable_execution_sdk_python.execution import durable_execution


def submitter(_callback_id: str, _context: WaitForCallbackContext) -> None:
    """Submitter completes; external system sends heartbeat then success."""


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    config = WaitForCallbackConfig(heartbeat_timeout=Duration.from_seconds(10))
    return context.wait_for_callback(submitter, name=event, config=config)
