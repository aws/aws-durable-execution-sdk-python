"""Example demonstrating map operations with custom iteration naming."""

from typing import Any

from aws_durable_execution_sdk_python.config import MapConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(_event: Any, context: DurableContext) -> list[str]:
    """Process orders using context.map() with custom iteration names."""
    orders = [
        {"id": "order-101", "amount": 25},
        {"id": "order-102", "amount": 50},
        {"id": "order-103", "amount": 75},
    ]

    return context.map(
        inputs=orders,
        func=lambda ctx, order, index, _: ctx.step(
            lambda _: f"processed-{order['id']}-${order['amount']}",
            name=f"process_{order['id']}",
        ),
        name="process_orders",
        config=MapConfig(
            max_concurrency=2,
            item_namer=lambda order, index: f"order-{order['id']}",
        ),
    ).get_results()
