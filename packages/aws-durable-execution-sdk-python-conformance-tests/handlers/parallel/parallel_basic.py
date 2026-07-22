"""8-1: Parallel basic (two branches, each a single step, all succeed)."""

from typing import Any

from aws_durable_execution_sdk_python.config import ParallelConfig
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_step
def return_value(_step_context: StepContext, value: str) -> str:
    return value


def branch0(ctx: DurableContext) -> str:
    return ctx.step(return_value("task-1"))


def branch1(ctx: DurableContext) -> str:
    return ctx.step(return_value("task-2"))


@durable_execution
def handler(event: Any, context: DurableContext) -> list:
    result = context.parallel(
        [branch0, branch1],
        name="parallel",
        config=ParallelConfig(max_concurrency=1),
    )
    return result.get_results()
