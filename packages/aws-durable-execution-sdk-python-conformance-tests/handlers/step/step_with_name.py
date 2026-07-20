from typing import Any

from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_step
def greet(_step_context: StepContext, name: str) -> str:
    return f"Hello, {name}!"


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    result: str = context.step(greet(event), name="custom_step_name")
    return result
