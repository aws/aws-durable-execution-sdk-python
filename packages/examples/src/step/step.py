from typing import Any

from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_step
def add_numbers(_step_context: StepContext, a: int, b: int) -> int:
    return a + b


@durable_execution
def handler(_event: Any, context: DurableContext) -> int:
    result: int = context.step(add_numbers(5, 3))
    return result
