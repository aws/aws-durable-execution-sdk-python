"""Demonstrates waitForCallback with anonymous (inline) submitter function."""

import time
from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Handler demonstrating waitForCallback with anonymous submitter."""
    result: str = context.wait_for_callback(
        lambda _callback_id, _context: time.sleep(1)
    )

    return {
        "callbackResult": result,
        "completed": True,
    }
