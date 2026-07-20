"""1-17: AtMostOnce interrupted (no retry) - Lambda crash, StepInterruptedError, fails permanently."""

import os
import time
from typing import Any

from aws_durable_execution_sdk_python.config import StepConfig, StepSemantics
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.retries import RetryPresets


@durable_step
def at_most_once_flaky_step(_step_context: StepContext, *, input_1: str) -> str:
    print(input_1, flush=True)
    time.sleep(1)  # Allow time for logs to flush to CloudWatch
    os._exit(1)  # Simulate Lambda crash
    return "unreachable"


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    result: str = context.step(
        at_most_once_flaky_step(input_1=str(event)),
        name="at_most_once_flaky_step",
        config=StepConfig(
            retry_strategy=RetryPresets.none(),
            step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY,
        ),
    )
    return result
