"""8-11: Parallel real concurrency (max-concurrency > 1) preserves index-ordered results."""

from typing import Any

from aws_durable_execution_sdk_python.config import ParallelConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


def branch0(_ctx: DurableContext) -> str:
    return "r0"


def branch1(_ctx: DurableContext) -> str:
    return "r1"


def branch2(_ctx: DurableContext) -> str:
    return "r2"


@durable_execution
def handler(event: Any, context: DurableContext) -> list:
    result = context.parallel(
        [branch0, branch1, branch2],
        name="concurrent",
        config=ParallelConfig(max_concurrency=2),
    )
    # get_results() is ordered by branch index, not completion order.
    return result.get_results()
