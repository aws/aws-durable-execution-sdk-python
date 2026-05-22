from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(_event: Any, context: DurableContext) -> str:
    # Step without explicit name - should use function name
    result = context.step(lambda _: "Step without name")
    return f"Result: {result}"
