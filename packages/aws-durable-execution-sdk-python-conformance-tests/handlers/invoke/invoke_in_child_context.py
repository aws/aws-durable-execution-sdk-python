"""5-13: Invoke inside child context."""

import os
from typing import Any

from aws_durable_execution_sdk_python.context import (
    DurableContext,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_with_child_context
def invoke_in_child(ctx: DurableContext) -> str:
    function_name = os.environ["TARGET_FUNCTION_NAME"]
    return ctx.invoke(function_name, None)


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    result: str = context.run_in_child_context(invoke_in_child())
    return result
