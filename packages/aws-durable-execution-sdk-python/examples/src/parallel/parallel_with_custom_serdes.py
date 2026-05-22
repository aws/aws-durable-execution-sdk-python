"""Example demonstrating parallel with custom serdes."""

import json
from typing import Any

from aws_durable_execution_sdk_python.config import ParallelConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.serdes import SerDes, SerDesContext


class CustomItemSerDes(SerDes[dict[str, Any]]):
    """Custom serializer for individual items that adds metadata."""

    def serialize(self, value: dict[str, Any], _: SerDesContext) -> str:
        # Add custom metadata during serialization
        wrapped = {"data": value, "serialized_by": "CustomItemSerDes"}

        return json.dumps(wrapped)

    def deserialize(self, payload: str, _: SerDesContext) -> dict[str, Any]:
        wrapped = json.loads(payload)
        # Extract the original data
        return wrapped["data"]


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Execute parallel tasks with custom item serialization.

    This example demonstrates using item_serdes to customize serialization
    of individual function results, while using default serialization for the
    overall BatchResult.
    """

    # Use custom serdes for individual function results only
    # The BatchResult will use default JSON serialization
    config = ParallelConfig(item_serdes=CustomItemSerDes())

    results = context.parallel(
        functions=[
            lambda ctx: ctx.step(
                lambda _: {"task": "task1", "value": 100}, name="task1"
            ),
            lambda ctx: ctx.step(
                lambda _: {"task": "task2", "value": 200}, name="task2"
            ),
            lambda ctx: ctx.step(
                lambda _: {"task": "task3", "value": 300}, name="task3"
            ),
        ],
        name="parallel_with_custom_serdes",
        config=config,
    )

    return {
        "success_count": results.success_count,
        "results": results.get_results(),
        "total_value": sum(r["value"] for r in results.get_results()),
    }
