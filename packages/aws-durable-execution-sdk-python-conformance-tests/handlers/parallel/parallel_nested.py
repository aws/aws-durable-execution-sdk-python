"""8-21: Nested parallel (a parallel operation inside a parallel branch)."""

from typing import Any

from aws_durable_execution_sdk_python.config import ParallelConfig
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_step
def ret(_step_context: StepContext, value: str) -> str:
    return value


def inner0(ctx: DurableContext) -> str:
    return ctx.step(ret("i1"))


def inner1(ctx: DurableContext) -> str:
    return ctx.step(ret("i2"))


def branch_a(ctx: DurableContext) -> list:
    inner = ctx.parallel(
        [inner0, inner1],
        name="inner",
        config=ParallelConfig(max_concurrency=1),
    )
    return inner.get_results()


@durable_execution
def handler(event: Any, context: DurableContext) -> list:
    result = context.parallel(
        [branch_a],
        name="outer",
        config=ParallelConfig(max_concurrency=1),
    )
    return result.get_results()
