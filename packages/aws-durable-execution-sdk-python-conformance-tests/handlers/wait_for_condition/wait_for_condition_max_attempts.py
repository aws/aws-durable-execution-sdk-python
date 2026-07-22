"""6-6: Wait-for-condition max attempts exceeded (failure)."""

from typing import Any

from aws_durable_execution_sdk_python.config import Duration, JitterStrategy
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.types import WaitForConditionCheckContext
from aws_durable_execution_sdk_python.waits import (
    WaitForConditionConfig,
    WaitStrategyConfig,
    create_wait_strategy,
)


@durable_execution
def handler(_event: Any, context: DurableContext) -> int:
    def check(state: int, _ctx: WaitForConditionCheckContext) -> int:
        return state + 1

    config = WaitForConditionConfig(
        wait_strategy=create_wait_strategy(
            WaitStrategyConfig(
                should_continue_polling=lambda _s: True,
                max_attempts=3,
                initial_delay=Duration.from_seconds(1),
                backoff_rate=1,
                jitter_strategy=JitterStrategy.NONE,
            )
        ),
        initial_state=0,
    )
    return context.wait_for_condition(check=check, config=config)
