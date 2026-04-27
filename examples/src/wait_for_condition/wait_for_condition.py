"""Example demonstrating wait-for-condition pattern."""

from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.waits import (
    WaitForConditionConfig,
    WaitForConditionDecision,
)


@durable_execution
def handler(_event: Any, context: DurableContext) -> int:
    """Handler demonstrating wait-for-condition pattern."""

    def condition_function(state: int, _) -> int:
        """Increment state by 1."""
        return state + 1

    def wait_strategy(state: int, attempt: int) -> dict[str, Any]:
        """Wait strategy that continues until state reaches 3."""
        if state >= 3:
            return WaitForConditionDecision.stop_polling()
        return WaitForConditionDecision.continue_waiting(Duration.from_seconds(1))

    config = WaitForConditionConfig(wait_strategy=wait_strategy, initial_state=0)

    result = context.wait_for_condition(check=condition_function, config=config)

    return result
