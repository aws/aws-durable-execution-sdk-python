"""8-4: Parallel with heterogeneous branch return types."""

from typing import Any

from aws_durable_execution_sdk_python.config import ParallelConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


def branch_string(_ctx: DurableContext) -> str:
    return "hello"


def branch_number(_ctx: DurableContext) -> int:
    return 42


def branch_object(_ctx: DurableContext) -> dict:
    return {"k": "v"}


@durable_execution
def handler(event: Any, context: DurableContext) -> list:
    result = context.parallel(
        [branch_string, branch_number, branch_object],
        name="hetero",
        config=ParallelConfig(max_concurrency=1),
    )
    return result.get_results()
