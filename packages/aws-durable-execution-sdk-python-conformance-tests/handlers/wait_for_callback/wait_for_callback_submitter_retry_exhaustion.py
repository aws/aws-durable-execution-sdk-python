# 7-7: Wait-for-callback submitter retry exhaustion
from typing import Any

from aws_durable_execution_sdk_python.config import Duration, WaitForCallbackConfig
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    WaitForCallbackContext,
)
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.retries import (
    RetryStrategyConfig,
    create_retry_strategy,
)


def submitter(_callback_id: str, _context: WaitForCallbackContext) -> None:
    """Submitter always throws."""
    raise Exception("submitter failure")


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    config = WaitForCallbackConfig(
        retry_strategy=create_retry_strategy(
            config=RetryStrategyConfig(
                max_attempts=2,
                initial_delay=Duration.from_seconds(1),
                max_delay=Duration.from_seconds(1),
            )
        ),
    )
    # Do not catch — retry exhaustion propagates and execution fails.
    return context.wait_for_callback(submitter, name=event, config=config)
