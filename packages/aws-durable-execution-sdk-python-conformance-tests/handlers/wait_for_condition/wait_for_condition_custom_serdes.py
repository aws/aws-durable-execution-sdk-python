"""6-11: Wait-for-condition with custom state serdes."""

from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.serdes import SerDes, SerDesContext
from aws_durable_execution_sdk_python.types import WaitForConditionCheckContext
from aws_durable_execution_sdk_python.waits import (
    WaitForConditionConfig,
    WaitForConditionDecision,
)


class AppendSerDes(SerDes[str]):
    """Custom serdes that encodes the string state for checkpointing.

    Applies a visible ``ENC:`` prefix on serialize (matching the JS example's
    ``"ENC:"`` prefix and the Java example's JSON-quote wrapping) so test 6-11
    actually exercises the custom serdes round-trip; an identity no-op would pass
    even if the SDK ignored the configured serdes.
    """

    def serialize(self, value: str, _: SerDesContext) -> str:
        return "ENC:" + value

    def deserialize(self, data: str, _: SerDesContext) -> str:
        return data[4:] if data.startswith("ENC:") else data


@durable_execution
def handler(_event: Any, context: DurableContext) -> str:
    def check(state: str, _ctx: WaitForConditionCheckContext) -> str:
        return state + "x"

    def wait_strategy(state: str, _attempt: int) -> WaitForConditionDecision:
        if len(state) >= 2:
            return WaitForConditionDecision.stop_polling()
        return WaitForConditionDecision.continue_waiting(Duration.from_seconds(1))

    config = WaitForConditionConfig(
        wait_strategy=wait_strategy,
        initial_state="",
        serdes=AppendSerDes(),
    )
    return context.wait_for_condition(check=check, config=config)
