"""1-18: AtMostOnce interrupted (with retry, uses the step context attempt number)."""

import os
import time
from typing import Any

from aws_durable_execution_sdk_python.config import Duration, StepConfig, StepSemantics
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.retries import (
    RetryStrategyConfig,
    create_retry_strategy,
)


@durable_step
def at_most_once_step(step_context: StepContext, *, input_1: str) -> str:
    # Print input to stdout each time step executes
    print(input_1, flush=True)
    time.sleep(1)  # Allow time for logs to flush to CloudWatch

    # The attempt number is the SDK's built-in durable counter from the step
    # context (1-based). Under AtMostOncePerRetry the interrupted first attempt
    # is consumed, so the retry re-executes as attempt 2.
    if step_context.attempt < 2:
        # First attempt: simulate Lambda crash
        os._exit(1)
    # Second attempt (retry): succeed
    return "succeeded on second attempt"


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    retry_config = RetryStrategyConfig(
        max_attempts=3,
        initial_delay=Duration.from_seconds(1),
    )

    result: str = context.step(
        at_most_once_step(input_1=str(event)),
        config=StepConfig(
            retry_strategy=create_retry_strategy(retry_config),
            step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY,
        ),
    )
    return result
