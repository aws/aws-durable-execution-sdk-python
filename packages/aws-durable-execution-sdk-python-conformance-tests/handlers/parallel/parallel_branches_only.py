"""8-2: Parallel branches-only form (no operation name)."""

from typing import Any

from aws_durable_execution_sdk_python.config import ParallelConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


def branch0(_ctx: DurableContext) -> str:
    return "alpha"


def branch1(_ctx: DurableContext) -> str:
    return "beta"


@durable_execution
def handler(event: Any, context: DurableContext) -> list:
    result = context.parallel(
        [branch0, branch1],
        config=ParallelConfig(max_concurrency=1),
    )
    return result.get_results()
