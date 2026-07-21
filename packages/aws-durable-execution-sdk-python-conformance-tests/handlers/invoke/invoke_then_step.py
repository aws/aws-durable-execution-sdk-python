"""5-12: Invoke then step (invoke result used by subsequent step)."""

import os
from typing import Any

from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_step
def process_result(_step_context: StepContext, value: str) -> str:
    return f"processed: {value}"


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    function_name = os.environ["TARGET_FUNCTION_NAME"]
    invoke_result: str = context.invoke(function_name, event)
    result: str = context.step(process_result(invoke_result))
    return result
