from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_step
def compute(_step_context: StepContext) -> str:
    return "computed"


@durable_execution
def handler(_event: Any, context: DurableContext) -> str:
    result: str = context.step(compute())
    context.wait(Duration.from_seconds(2))
    return result
