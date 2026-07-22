"""6-9: Wait-for-condition with a complex object state."""

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
def handler(_event: Any, context: DurableContext) -> dict:
    def check(state: dict, _ctx: WaitForConditionCheckContext) -> dict:
        new_attempts = state["attempts"] + 1
        new_status = "DONE" if new_attempts >= 2 else "PENDING"
        return {"status": new_status, "attempts": new_attempts}

    def wait_strategy(state: dict, _attempt: int) -> WaitForConditionDecision:
        if state["status"] == "DONE":
            return WaitForConditionDecision.stop_polling()
        return WaitForConditionDecision.continue_waiting(Duration.from_seconds(1))

    initial_state = {"status": "PENDING", "attempts": 0}
    config = WaitForConditionConfig(
        wait_strategy=wait_strategy, initial_state=initial_state
    )
    return context.wait_for_condition(check=check, config=config)
