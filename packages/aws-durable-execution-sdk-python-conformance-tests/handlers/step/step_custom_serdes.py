"""1-6: Custom serdes (per-step) - transforms result to uppercase."""

from typing import Any

from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.serdes import SerDes, SerDesContext


class UppercaseSerDes(SerDes[str]):
    """Custom serdes that transforms strings to uppercase on serialization."""

    def serialize(self, value: str, _: SerDesContext) -> str:
        return value.upper()

    def deserialize(self, data: str, _: SerDesContext) -> str:
        return data


@durable_step
def return_input(_step_context: StepContext, value: str) -> str:
    return value


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    result: str = context.step(
        return_input(event),
        config=StepConfig(serdes=UppercaseSerDes()),
    )
    return result
