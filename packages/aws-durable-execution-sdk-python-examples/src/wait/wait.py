from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.config import Duration


@durable_execution
def handler(_event: Any, context: DurableContext) -> str:
    context.wait(Duration.from_seconds(5))
    return "Wait completed"
