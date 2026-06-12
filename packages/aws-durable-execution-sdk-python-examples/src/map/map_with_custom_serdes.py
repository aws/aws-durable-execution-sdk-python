"""Example demonstrating map with custom serdes."""

import json
from typing import Any

from aws_durable_execution_sdk_python.config import MapConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.serdes import SerDes, SerDesContext


class CustomItemSerDes(SerDes[dict[str, Any]]):
    """Custom serializer for individual items that adds metadata."""

    def serialize(self, value: dict[str, Any], _: SerDesContext) -> str:
        # Add custom metadata during serialization
        wrapped = {"data": value, "serialized_by": "CustomItemSerDes", "version": "1.0"}

        return json.dumps(wrapped)

    def deserialize(self, payload: str, _: SerDesContext) -> dict[str, Any]:
        wrapped = json.loads(payload)
        # Extract the original data
        return wrapped["data"]


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Process items with custom item serialization.

    This example demonstrates using item_serdes to customize serialization
    of individual item results, while using default serialization for the
    overall BatchResult.
    """
    items = [
        {"id": 1, "name": "item1"},
        {"id": 2, "name": "item2"},
        {"id": 3, "name": "item3"},
    ]

    # Use custom serdes for individual items only
    # The BatchResult will use default JSON serialization
    config = MapConfig(item_serdes=CustomItemSerDes())

    results = context.map(
        inputs=items,
        func=lambda ctx, item, index, _: ctx.step(
            lambda _: {
                "processed": item["name"],
                "index": index,
                "doubled_id": item["id"] * 2,
            },
            name=f"process_{index}",
        ),
        name="map_with_custom_serdes",
        config=config,
    )

    return {
        "success_count": results.success_count,
        "results": results.get_results(),
        "processed_names": [r["processed"] for r in results.get_results()],
    }
