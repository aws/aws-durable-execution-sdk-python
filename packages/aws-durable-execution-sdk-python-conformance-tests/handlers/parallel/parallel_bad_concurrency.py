"""8-19: Parallel with invalid max-concurrency raises a validation error."""

from typing import Any

from aws_durable_execution_sdk_python.config import ParallelConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


def branch_a(_ctx: DurableContext) -> str:
    return "a"


def branch_b(_ctx: DurableContext) -> str:
    return "b"


@durable_execution
def handler(event: Any, context: DurableContext) -> list:
    # max_concurrency=0 is invalid; the SDK should reject it before any branch runs.
    result = context.parallel(
        [branch_a, branch_b],
        name="bad-concurrency",
        config=ParallelConfig(max_concurrency=0),
    )
    return result.get_results()
