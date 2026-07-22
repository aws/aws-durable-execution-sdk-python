"""6-13: Multiple sequential wait_for_condition operations."""

from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.types import WaitForConditionCheckContext
from aws_durable_execution_sdk_python.waits import (
    WaitForConditionConfig,
    WaitForConditionDecision,
)


def _make_wait_strategy(threshold: int):
    def wait_strategy(state: int, _attempt: int) -> WaitForConditionDecision:
        if state >= threshold:
            return WaitForConditionDecision.stop_polling()
        return WaitForConditionDecision.continue_waiting(Duration.from_seconds(1))

    return wait_strategy


def _check(state: int, _ctx: WaitForConditionCheckContext) -> int:
    return state + 1


@durable_execution
def handler(event: Any, context: DurableContext) -> int:
    config1 = WaitForConditionConfig(
        wait_strategy=_make_wait_strategy(2), initial_state=0
    )
    first_result: int = context.wait_for_condition(check=_check, config=config1)

    config2 = WaitForConditionConfig(
        wait_strategy=_make_wait_strategy(4), initial_state=first_result
    )
    second_result: int = context.wait_for_condition(check=_check, config=config2)

    return second_result
