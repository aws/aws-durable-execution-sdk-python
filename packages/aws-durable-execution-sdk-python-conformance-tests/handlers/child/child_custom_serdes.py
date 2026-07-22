"""3-14: Child context with custom serdes (succeed)."""

from typing import Any

from aws_durable_execution_sdk_python.config import ChildConfig
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.serdes import SerDes, SerDesContext


class UppercaseSerDes(SerDes[str]):
    def serialize(self, value: str, _: SerDesContext) -> str:
        return value.upper()

    def deserialize(self, data: str, _: SerDesContext) -> str:
        return data


@durable_step
def return_input(_step_context: StepContext, value: str) -> str:
    return value


@durable_with_child_context
def serdes_child(ctx: DurableContext, value: str) -> str:
    return ctx.step(return_input(value))


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    result: str = context.run_in_child_context(
        serdes_child(event),
        name="serdes-child",
        config=ChildConfig(serdes=UppercaseSerDes()),
    )
    return result
