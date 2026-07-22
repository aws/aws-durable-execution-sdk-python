"""8-14: Parallel replay skips succeeded branches across a wait suspension."""

from typing import Any

from aws_durable_execution_sdk_python.config import Duration, ParallelConfig
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_step
def ret_b0(_step_context: StepContext, value: str) -> str:
    return value


def branch0(ctx: DurableContext) -> str:
    return ctx.step(ret_b0("b0"))


def branch1(ctx: DurableContext) -> str:
    ctx.wait(Duration.from_seconds(2))
    return "b1"


@durable_execution
def handler(event: Any, context: DurableContext) -> list:
    result = context.parallel(
        [branch0, branch1],
        name="replay",
        config=ParallelConfig(max_concurrency=1),
    )
    return result.get_results()
