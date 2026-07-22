# 7-1: Wait-for-callback basic (success via external callback)
from typing import Any

from aws_durable_execution_sdk_python.context import (
    DurableContext,
    WaitForCallbackContext,
)
from aws_durable_execution_sdk_python.execution import durable_execution


def submitter(_callback_id: str, _context: WaitForCallbackContext) -> None:
    """Submitter receives the callback id; does nothing durable."""


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    return context.wait_for_callback(submitter, name=event)
