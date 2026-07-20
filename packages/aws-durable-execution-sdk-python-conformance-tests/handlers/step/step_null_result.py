"""1-5: Undefined/null result."""

from typing import Any

from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_step
def do_nothing(_step_context: StepContext) -> None:
    return None


@durable_execution
def handler(_event: Any, context: DurableContext) -> None:
    result = context.step(do_nothing())
    return result
