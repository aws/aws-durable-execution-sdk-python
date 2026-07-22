"""3-15: Child context error without step (error thrown directly in child body)."""

from typing import Any

from aws_durable_execution_sdk_python.context import (
    DurableContext,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_with_child_context
def direct_error(_ctx: DurableContext) -> str:
    msg = "direct error"
    raise RuntimeError(msg)


@durable_execution
def handler(_event: Any, context: DurableContext) -> str:
    result: str = context.run_in_child_context(direct_error(), name="direct-error")
    return result
