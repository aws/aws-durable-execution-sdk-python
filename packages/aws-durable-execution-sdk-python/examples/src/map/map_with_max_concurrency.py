"""Example demonstrating map with maxConcurrency limit."""

from typing import Any

from aws_durable_execution_sdk_python.config import MapConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(_event: Any, context: DurableContext) -> list[int]:
    """Process items with concurrency limit of 3."""
    items = list(range(1, 11))  # [1, 2, 3, ..., 10]

    # Extract results immediately to avoid BatchResult serialization
    return context.map(
        inputs=items,
        func=lambda ctx, item, index, _: ctx.step(
            lambda _: item * 3, name=f"process_{index}"
        ),
        name="map_with_concurrency",
        config=MapConfig(max_concurrency=3),
    ).get_results()
