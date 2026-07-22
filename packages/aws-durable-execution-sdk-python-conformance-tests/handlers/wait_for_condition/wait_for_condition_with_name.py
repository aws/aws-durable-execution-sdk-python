"""6-3: Wait-for-condition with explicit name."""

from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.types import WaitForConditionCheckContext
from aws_durable_execution_sdk_python.waits import (
    WaitForConditionConfig,
    WaitForConditionDecision,
)


@durable_execution
def handler(event: Any, context: DurableContext) -> int:
    threshold = int(event)

    def check(state: int, _ctx: WaitForConditionCheckContext) -> int:
        return state + 1

    def wait_strategy(state: int, _attempt: int) -> WaitForConditionDecision:
        if state >= threshold:
            return WaitForConditionDecision.stop_polling()
        return WaitForConditionDecision.continue_waiting(Duration.from_seconds(1))

    config = WaitForConditionConfig(wait_strategy=wait_strategy, initial_state=0)
    return context.wait_for_condition(check=check, config=config, name="poll-status")
