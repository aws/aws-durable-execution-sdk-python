# 7-4: Wait-for-callback external failure (uncaught)
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
    # Do not catch — let the external failure propagate so the execution fails.
    return context.wait_for_callback(submitter, name=event)
