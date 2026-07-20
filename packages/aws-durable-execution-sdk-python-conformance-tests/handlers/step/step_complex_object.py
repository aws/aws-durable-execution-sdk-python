"""1-4: Returning complex object."""

from typing import Any

from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_step
def build_response(_step_context: StepContext, name: str, tags: list[str]) -> dict:
    return {
        "user": {
            "name": name,
            "tags": tags,
        },
        "count": len(tags),
    }


@durable_execution
def handler(event: Any, context: DurableContext) -> dict:
    result: dict = context.step(build_response(event["name"], event["tags"]))
    return result
