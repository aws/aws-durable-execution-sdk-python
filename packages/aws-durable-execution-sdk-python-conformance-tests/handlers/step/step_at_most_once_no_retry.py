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
def at_most_once_flaky_step(step_context: StepContext, *, input_1: str) -> str:
    # Log through the step context logger so the record carries the execution
    # ARN and can be correlated to this durable execution in CloudWatch.
    step_context.logger.info(input_1)
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
