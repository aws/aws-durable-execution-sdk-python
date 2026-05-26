"""Demonstrates sending heartbeats during long-running callback processing."""

import time
from typing import Any

from aws_durable_execution_sdk_python.config import Duration, WaitForCallbackConfig
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    WaitForCallbackContext,
)
from aws_durable_execution_sdk_python.execution import durable_execution


def submitter(_callback_id: str, _context: WaitForCallbackContext) -> None:
    """Simulate long-running submitter function."""
    time.sleep(5)
    return None


@durable_execution
def handler(event: dict[str, Any], context: DurableContext) -> dict[str, Any]:
    """Handler demonstrating waitForCallback with heartbeat timeout."""

    config = WaitForCallbackConfig(
        timeout=Duration.from_seconds(120), heartbeat_timeout=Duration.from_seconds(15)
    )

    result: str = context.wait_for_callback(submitter, config=config)

    return {
        "callbackResult": result,
        "completed": True,
    }
