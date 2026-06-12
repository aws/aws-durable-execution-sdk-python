"""Demonstrates multiple invocations tracking with waitForCallback operations across different invocations."""

from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Handler demonstrating multiple invocations with waitForCallback operations."""
    # First invocation - wait operation
    context.wait(Duration.from_seconds(1), name="wait-invocation-1")

    # First callback operation
    def first_submitter(callback_id: str, _context) -> None:
        """Submitter for first callback."""
        print(f"First callback submitted with ID: {callback_id}")
        return None

    callback_result_1: str = context.wait_for_callback(
        first_submitter,
        name="first-callback",
    )

    # Step operation between callbacks
    step_result: dict[str, Any] = context.step(
        lambda _: {"processed": True, "step": 1},
        name="process-callback-data",
    )

    # Second invocation - another wait operation
    context.wait(Duration.from_seconds(1), name="wait-invocation-2")

    # Second callback operation
    def second_submitter(callback_id: str, _context) -> None:
        """Submitter for second callback."""
        print(f"Second callback submitted with ID: {callback_id}")
        return None

    callback_result_2: str = context.wait_for_callback(
        second_submitter,
        name="second-callback",
    )

    # Final invocation returns complete result
    return {
        "firstCallback": callback_result_1,
        "secondCallback": callback_result_2,
        "stepResult": step_result,
        "invocationCount": "multiple",
    }
