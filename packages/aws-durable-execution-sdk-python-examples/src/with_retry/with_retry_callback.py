"""Demonstrates with_retry wrapping a wait_for_callback operation.

The callback may fail multiple times before succeeding. The with_retry helper
retries the entire callback flow (including creating a new callback each attempt)
with exponential backoff between attempts.
"""

from typing import Any

from aws_durable_execution_sdk_python.config import Duration, WaitForCallbackConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.retries import (
    RetryStrategyConfig,
    WithRetryConfig,
    create_retry_strategy,
    with_retry,
)


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Handler demonstrating with_retry around a wait_for_callback.

    The external system may fail to process the callback multiple times.
    with_retry will re-create the callback and wait again on each retry,
    with exponential backoff between attempts.
    """

    def retryable_callback_flow(ctx: DurableContext, attempt: int) -> str:
        """The retryable block: create a callback and wait for the result."""

        def submitter(callback_id: str, _callback_ctx) -> None:
            """Submit the callback ID to an external system."""
            # In real usage, this would send the callback_id to an external
            # system (e.g., via API call, SQS message, etc.)
            pass

        config = WaitForCallbackConfig(
            timeout=Duration.from_seconds(30),
            heartbeat_timeout=Duration.from_seconds(60),
        )

        return ctx.wait_for_callback(
            submitter, name=f"external-call-attempt-{attempt}", config=config
        )

    retry_config = WithRetryConfig(
        retry_strategy=create_retry_strategy(
            RetryStrategyConfig(
                max_attempts=5,
                initial_delay=Duration.from_seconds(2),
                backoff_rate=1.0,
            )
        ),
    )

    result = with_retry(
        context,
        func=retryable_callback_flow,
        config=retry_config,
        name="callback-with-retry",
    )

    return {
        "success": True,
        "result": result,
    }
