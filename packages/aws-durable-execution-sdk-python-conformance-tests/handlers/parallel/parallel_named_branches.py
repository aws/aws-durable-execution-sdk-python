"""8-3: Parallel with named branch objects."""

from typing import Any

from aws_durable_execution_sdk_python.config import ParallelBranch, ParallelConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


def first_branch(_ctx: DurableContext) -> str:
    return "one"


def second_branch(_ctx: DurableContext) -> str:
    return "two"


@durable_execution
def handler(event: Any, context: DurableContext) -> list:
    result = context.parallel(
        [
            ParallelBranch(func=first_branch, name="first"),
            ParallelBranch(func=second_branch, name="second"),
        ],
        name="named",
        config=ParallelConfig(max_concurrency=1),
    )
    return result.get_results()
