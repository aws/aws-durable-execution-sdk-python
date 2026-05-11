"""Example demonstrating all parallel branch patterns."""

from typing import Any

from aws_durable_execution_sdk_python.config import ParallelBranch, ParallelConfig
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    durable_parallel_branch,
)
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_parallel_branch(name="fetch-orders")
def fetch_orders(ctx: DurableContext) -> str:
    return ctx.step(lambda _: "orders-loaded", name="load_orders")


@durable_parallel_branch()
def fetch_preferences(ctx: DurableContext) -> str:
    return ctx.step(lambda _: "prefs-loaded", name="load_prefs")


@durable_execution
def handler(_event: Any, context: DurableContext) -> list[str]:
    """Execute parallel branches using all supported patterns."""

    return context.parallel(
        functions=[
            # 1. Named parallel branch with ParallelBranch
            ParallelBranch(
                func=lambda ctx: ctx.step(
                    lambda _: "user-data-loaded", name="load_user"
                ),
                name="fetch-user-data",
            ),
            # 2. Named parallel branch with decorator
            fetch_orders(),
            # 3. Unnamed parallel branch with decorator
            fetch_preferences(),
            # 4. Unnamed parallel branch with ParallelBranch
            ParallelBranch(
                func=lambda ctx: ctx.step(
                    lambda _: "metrics-loaded", name="load_metrics"
                ),
            ),
            # 5. No wrapper, just a raw callable
            lambda ctx: ctx.step(lambda _: "config-loaded", name="load_config"),
        ],
        name="load_all_data",
        config=ParallelConfig(max_concurrency=3),
    ).get_results()
