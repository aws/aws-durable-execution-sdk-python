"""1-13: Default retry strategy (uses the step context attempt number)."""

from typing import Any

from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_step
def unreliable(step_context: StepContext) -> str:
    # Fail on the first two attempts, succeed on the third, using the SDK's
    # built-in durable attempt counter from the step context (1-based).
    if step_context.attempt < 3:
        msg = f"Attempt {step_context.attempt} failed"
        raise RuntimeError(msg)
    return "recovered"


@durable_execution
def handler(_event: Any, context: DurableContext) -> str:
    # Step with no explicit retry config — uses SDK default
    result: str = context.step(unreliable())
    return result
