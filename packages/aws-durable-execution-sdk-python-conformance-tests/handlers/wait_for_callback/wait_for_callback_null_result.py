# 7-15: Wait-for-callback success with empty (null) payload
from typing import Any

from aws_durable_execution_sdk_python.context import (
    DurableContext,
    WaitForCallbackContext,
)
from aws_durable_execution_sdk_python.execution import durable_execution


def submitter(_callback_id: str, _context: WaitForCallbackContext) -> None:
    """Submitter completes; external system sends success with no payload."""


@durable_execution
def handler(event: Any, context: DurableContext) -> Any:
    return context.wait_for_callback(submitter, name=event)
