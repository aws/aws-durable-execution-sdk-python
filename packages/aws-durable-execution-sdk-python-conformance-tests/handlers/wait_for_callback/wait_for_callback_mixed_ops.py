# 7-10: Wait-for-callback mixed with wait and step
from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    WaitForCallbackContext,
)
from aws_durable_execution_sdk_python.execution import durable_execution


def submitter(_callback_id: str, _context: WaitForCallbackContext) -> None:
    """Submitter completes without side effects."""


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    context.wait(Duration.from_seconds(1))
    context.step(lambda _: "fixed-data")
    return context.wait_for_callback(submitter, name=event)
