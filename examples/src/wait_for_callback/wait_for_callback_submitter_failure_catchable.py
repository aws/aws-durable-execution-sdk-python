"""Demonstrates waitForCallback with submitter function that fails."""

import time
from typing import Any

from aws_durable_execution_sdk_python.config import Duration, WaitForCallbackConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.retries import (
    RetryStrategyConfig,
    create_retry_strategy,
)


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Handler demonstrating waitForCallback with failing submitter."""

    def submitter(_callback_id, _context) -> None:
        """Submitter function that fails after a delay."""
        time.sleep(0.5)
        # Submitter fails
        raise Exception("Submitter failed")

    config = WaitForCallbackConfig(
        timeout=Duration.from_seconds(10),
        heartbeat_timeout=Duration.from_seconds(20),
        retry_strategy=create_retry_strategy(
            config=RetryStrategyConfig(
                max_attempts=3,
                initial_delay=Duration.from_seconds(1),
                max_delay=Duration.from_seconds(1),
            )
        ),
    )

    try:
        result: str = context.wait_for_callback(
            submitter,
            name="failing-submitter-callback",
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
