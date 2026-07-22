"""8-12: Parallel with FLAT nesting (virtual branch contexts)."""

from typing import Any

from aws_durable_execution_sdk_python.config import NestingType, ParallelConfig
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
    return ctx.step(return_value("fa"))


def branch1(ctx: DurableContext) -> str:
    return ctx.step(return_value("fb"))


@durable_execution
def handler(event: Any, context: DurableContext) -> list:
    result = context.parallel(
        [branch0, branch1],
        name="flat",
        config=ParallelConfig(
            max_concurrency=1,
            nesting_type=NestingType.FLAT,
        ),
    )
    return result.get_results()
