import asyncio
from typing import Any

from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_step
async def add_numbers(_step_context: StepContext, a: int, b: int) -> int:
    await asyncio.sleep(0)
    return a + b


@durable_execution
async def handler(_event: Any, context: DurableContext) -> int:
    result: int = context.step(add_numbers(5, 3))
    return result
