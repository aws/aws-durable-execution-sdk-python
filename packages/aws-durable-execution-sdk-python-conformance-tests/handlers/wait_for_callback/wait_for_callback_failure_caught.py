# 7-6: Wait-for-callback external failure caught (recovers)
from typing import Any

from aws_durable_execution_sdk_python.context import (
    DurableContext,
    WaitForCallbackContext,
)
from aws_durable_execution_sdk_python.execution import durable_execution


def submitter(_callback_id: str, _context: WaitForCallbackContext) -> None:
    """Submitter completes; external system will report failure."""


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    try:
        return context.wait_for_callback(submitter, name=event)
    except Exception:
        return "recovered"
