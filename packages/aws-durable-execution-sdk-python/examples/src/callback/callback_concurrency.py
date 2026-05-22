"""Demonstrates multiple concurrent createCallback operations using context.parallel."""

from typing import Any

from aws_durable_execution_sdk_python.config import CallbackConfig, Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Handler demonstrating multiple concurrent callback operations."""

    callback_config = CallbackConfig(timeout=Duration.from_seconds(30))

    def callback_branch_1(ctx: DurableContext) -> str:
        """First callback branch."""
        callback = ctx.create_callback(
            name="api-call-1",
            config=callback_config,
        )
        return callback.result()

    def callback_branch_2(ctx: DurableContext) -> str:
        """Second callback branch."""
        callback = ctx.create_callback(
            name="api-call-2",
            config=callback_config,
        )
        return callback.result()

    def callback_branch_3(ctx: DurableContext) -> str:
        """Third callback branch."""
        callback = ctx.create_callback(
            name="api-call-3",
            config=callback_config,
        )
        return callback.result()

    parallel_results = context.parallel(
        functions=[callback_branch_1, callback_branch_2, callback_branch_3],
        name="parallel_callbacks",
    )

    # Extract results from parallel execution
    results = parallel_results.get_results()

    return {
        "results": results,
        "allCompleted": True,
    }
