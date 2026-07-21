# 7-8: Wait-for-callback inside a child context
from typing import Any

from aws_durable_execution_sdk_python.context import (
    DurableContext,
    WaitForCallbackContext,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.execution import durable_execution


def submitter(_callback_id: str, _context: WaitForCallbackContext) -> None:
    """Submitter completes without side effects."""


@durable_with_child_context
def wrapper(child_context: DurableContext, name: str) -> str:
    """Child context wrapping the wait_for_callback operation."""
    return child_context.wait_for_callback(submitter, name=name)


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    return context.run_in_child_context(wrapper(name=event), name="wrapper")
