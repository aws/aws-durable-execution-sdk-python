# 7-5: Wait-for-callback timeout (no external completion)
from typing import Any

from aws_durable_execution_sdk_python.config import Duration, WaitForCallbackConfig
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    WaitForCallbackContext,
)
from aws_durable_execution_sdk_python.execution import durable_execution


def submitter(_callback_id: str, _context: WaitForCallbackContext) -> None:
    """Submitter completes; no external system ever completes the callback."""


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    config = WaitForCallbackConfig(timeout=Duration.from_seconds(3))
    # Do not catch — timeout propagates and execution fails.
    return context.wait_for_callback(submitter, name=event, config=config)
