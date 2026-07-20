from typing import Any

from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_step
def step_one(_step_context: StepContext) -> str:
    return "first"


@durable_step
def step_two(_step_context: StepContext, previous: str) -> str:
    return f"{previous}_second"


@durable_execution
def handler(_event: Any, context: DurableContext) -> str:
    result1: str = context.step(step_one())
    result2: str = context.step(step_two(result1))
    return result2
