"""8-5: Parallel with an empty branches list."""

from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(event: Any, context: DurableContext) -> list:
    result = context.parallel([], name="empty")
    return result.get_results()
