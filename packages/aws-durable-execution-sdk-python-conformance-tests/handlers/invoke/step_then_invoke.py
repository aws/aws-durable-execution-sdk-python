"""5-11: Step then invoke (sequential operations)."""

import os
from typing import Any

from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_step
def compute_payload(_step_context: StepContext) -> str:
    return "step result"


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    function_name = os.environ["TARGET_FUNCTION_NAME"]
    step_result: str = context.step(compute_payload())
    result: str = context.invoke(function_name, step_result)
    return result
