"""Step handler that uses the SDK's step context logger.

Validates that the logger provided by StepContext correctly emits log
entries to CloudWatch during step execution.
"""

from typing import Any

from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_step
def greet(step_context: StepContext, name: str) -> str:
    step_context.logger.info(f"Greeting step started for: {name}")
    result = f"Hello, {name}!"
    step_context.logger.info(f"Greeting step completed with: {result}")
    return result


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    result: str = context.step(greet(event))
    return result
