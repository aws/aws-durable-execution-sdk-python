"""Demonstrates waitForCallback timeout scenarios."""

from typing import Any

from aws_durable_execution_sdk_python.config import Duration, WaitForCallbackConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Handler demonstrating waitForCallback timeout."""

    config = WaitForCallbackConfig(
        timeout=Duration.from_seconds(1), heartbeat_timeout=Duration.from_seconds(2)
    )

    def submitter(_callback_id, _context) -> None:
        """Submitter succeeds but callback never completes."""
        return None

    try:
        result: str = context.wait_for_callback(
            submitter,
            config=config,
        )
        return {
            "callbackResult": result,
            "success": True,
        }
    except Exception as error:
        return {
            "success": False,
            "error": str(error),
        }
