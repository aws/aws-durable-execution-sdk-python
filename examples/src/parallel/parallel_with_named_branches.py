"""Example demonstrating parallel operations with named branches."""

from typing import Any

from aws_durable_execution_sdk_python.config import ParallelBranch, ParallelConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(_event: Any, context: DurableContext) -> list[str]:
    """Execute named parallel branches using ParallelBranch."""

    return context.parallel(
        functions=[
            ParallelBranch(
                func=lambda ctx: ctx.step(
                    lambda _: "user-data-loaded", name="load_user"
                ),
                name="fetch-user-data",
            ),
            ParallelBranch(
                func=lambda ctx: ctx.step(
                    lambda _: "orders-loaded", name="load_orders"
                ),
                name="fetch-order-history",
            ),
            ParallelBranch(
                func=lambda ctx: ctx.step(lambda _: "prefs-loaded", name="load_prefs"),
                name="fetch-preferences",
            ),
        ],
        name="load_all_data",
        config=ParallelConfig(max_concurrency=3),
    ).get_results()
