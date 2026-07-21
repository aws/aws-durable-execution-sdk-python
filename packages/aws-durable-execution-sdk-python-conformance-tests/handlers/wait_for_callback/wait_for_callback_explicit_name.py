# 7-2: Wait-for-callback with explicit static name
from typing import Any

from aws_durable_execution_sdk_python.context import (
    DurableContext,
    WaitForCallbackContext,
)
from aws_durable_execution_sdk_python.execution import durable_execution


def submitter(_callback_id: str, _context: WaitForCallbackContext) -> None:
    """Submitter completes without side effects."""


@durable_execution
def handler(_event: Any, context: DurableContext) -> str:
    return context.wait_for_callback(submitter, name="approval")
