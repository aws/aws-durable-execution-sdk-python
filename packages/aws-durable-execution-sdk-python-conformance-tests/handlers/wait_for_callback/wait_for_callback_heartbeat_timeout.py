# 7-12: Wait-for-callback heartbeat timeout (no heartbeat sent)
from typing import Any

from aws_durable_execution_sdk_python.config import Duration, WaitForCallbackConfig
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    WaitForCallbackContext,
)
from aws_durable_execution_sdk_python.execution import durable_execution


def submitter(_callback_id: str, _context: WaitForCallbackContext) -> None:
    """Submitter completes; no heartbeat or terminal callback is ever sent."""


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    config = WaitForCallbackConfig(heartbeat_timeout=Duration.from_seconds(5))
    # Do not catch — heartbeat timeout propagates and execution fails.
    return context.wait_for_callback(submitter, name=event, config=config)
