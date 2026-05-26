"""Demonstrates createCallback mixed with steps, waits, and other operations."""

import time
from typing import Any

from aws_durable_execution_sdk_python.config import CallbackConfig, Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Handler demonstrating createCallback mixed with other operations."""

    step_result: dict[str, Any] = context.step(
        lambda _: {"userId": 123, "name": "John Doe"},
        name="fetch-data",
    )

    callback_config = CallbackConfig(timeout=Duration.from_minutes(1))
    callback = context.create_callback(
        name="process-user",
        config=callback_config,
    )

    # Mix callback with step and wait operations
    context.wait(Duration.from_seconds(1), name="initial-wait")

    callback_result = callback.result()

    return {
        "stepResult": step_result,
        "callbackResult": callback_result,
        "completed": True,
    }
