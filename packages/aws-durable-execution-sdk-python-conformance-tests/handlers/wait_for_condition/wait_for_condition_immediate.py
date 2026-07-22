"""6-2: Wait-for-condition immediate stop (condition already met on first check)."""

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
    # Convention shared with the JS/Java examples: the initial state comes from
    # the input and the threshold is a fixed constant (5). For input 5 the
    # condition (state >= 5) is already satisfied on the first check.
    initial_state = int(event)

    def check(state: int, _ctx: WaitForConditionCheckContext) -> int:
        return state

    def wait_strategy(state: int, _attempt: int) -> WaitForConditionDecision:
        if state >= 5:
            return WaitForConditionDecision.stop_polling()
        return WaitForConditionDecision.continue_waiting(Duration.from_seconds(1))

    config = WaitForConditionConfig(
        wait_strategy=wait_strategy, initial_state=initial_state
    )
    return context.wait_for_condition(check=check, config=config)
