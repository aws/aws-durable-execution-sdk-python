"""6-8: Wait-for-condition check throws, caught by handler (recovers)."""

from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.types import WaitForConditionCheckContext
from aws_durable_execution_sdk_python.waits import (
    WaitForConditionConfig,
    WaitForConditionDecision,
)


@durable_execution
def handler(_event: Any, context: DurableContext) -> str:
    def check(_state: None, _ctx: WaitForConditionCheckContext) -> None:
        raise RuntimeError("check function failed")

    def wait_strategy(_state: None, _attempt: int) -> WaitForConditionDecision:
        return WaitForConditionDecision.stop_polling()

    config: WaitForConditionConfig[None] = WaitForConditionConfig(
        wait_strategy=wait_strategy, initial_state=None
    )
    try:
        context.wait_for_condition(check=check, config=config)
    except Exception:
        return "recovered"
