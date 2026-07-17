"""Example demonstrating multiple steps with retry logic."""

from typing import Any

from aws_durable_execution_sdk_python.config import Duration, StepConfig
from aws_durable_execution_sdk_python.context import DurableContext, StepContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.retries import (
    RetryStrategyConfig,
    create_retry_strategy,
)


def simulated_get_item(
    step_context: StepContext, name: str, item_available: bool
) -> dict[str, Any] | None:
    """Simulate getting an item with deterministic attempt-based behavior."""
    if not item_available and step_context.attempt == 1:
        msg = "Random failure"
        raise RuntimeError(msg)

    if not item_available:
        return None

    return {"id": name, "data": "item data"}


@durable_execution
def handler(event: Any, context: DurableContext) -> dict[str, Any]:
    """Handler demonstrating polling with retry logic."""
    name = event.get("name", "test-item")

    # Retry configuration for steps
    retry_config = RetryStrategyConfig(
        max_attempts=5,
        retryable_error_types=[RuntimeError],
    )

    step_config = StepConfig(create_retry_strategy(retry_config))

    item = None
    poll_count = 0
    max_polls = 5

    try:
        while poll_count < max_polls:
            poll_count += 1

            # Each poll is a new step whose attempt starts at 1. Availability
            # models the external item state independently from retry attempts.
            item_available: bool = poll_count > 1

            # Try to get the item with retry
            get_response = context.step(
                lambda step_context,
                n=name,
                available=item_available: simulated_get_item(
                    step_context, n, available
                ),
                name=f"get_item_poll_{poll_count}",
                config=step_config,
            )

            # Did we find the item?
            if get_response:
                item = get_response
                break

            # Wait 1 second until next poll
            context.wait(Duration.from_seconds(1))

    except RuntimeError as e:
        # Retries exhausted
        return {"error": "DDB Retries Exhausted", "message": str(e)}

    if not item:
        return {"error": "Item Not Found"}

    # We found the item!
    return {"success": True, "item": item, "pollsRequired": poll_count}
