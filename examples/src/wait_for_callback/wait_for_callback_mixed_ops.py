"""Demonstrates waitForCallback combined with steps, waits, and other operations."""

import time
from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Handler demonstrating waitForCallback mixed with other operations."""
    # Mix waitForCallback with other operation types
    context.wait(Duration.from_seconds(1), name="initial-wait")

    step_result: dict[str, Any] = context.step(
        lambda _: {"userId": 123, "name": "John Doe"},
        name="fetch-user-data",
    )

    def submitter(_callback_id, _context) -> None:
        """Submitter uses data from previous step."""
        time.sleep(0.1)
        return None

    callback_result: str = context.wait_for_callback(
        submitter,
        name="wait-for-callback",
    )

    context.wait(Duration.from_seconds(2), name="final-wait")

    final_step: dict[str, Any] = context.step(
        lambda _: {
            "status": "completed",
            "timestamp": int(time.time() * 1000),
        },
        name="finalize-processing",
    )

    return {
        "stepResult": step_result,
        "callbackResult": callback_result,
        "finalStep": final_step,
        "workflowCompleted": True,
    }
