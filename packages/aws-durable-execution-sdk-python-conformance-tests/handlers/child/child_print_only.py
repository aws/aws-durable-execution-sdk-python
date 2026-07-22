"""3-17: Child context with print only (verify no re-execution on replay)."""

from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_with_child_context
def print_child(_ctx: DurableContext, *, input_1: str) -> str:
    print(input_1, flush=True)
    return input_1


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    result: str = context.run_in_child_context(
        print_child(input_1=str(event)), name="print-child"
    )
    context.wait(Duration.from_seconds(1))
    return result
