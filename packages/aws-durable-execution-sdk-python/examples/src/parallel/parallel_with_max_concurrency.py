"""Example demonstrating parallel with maxConcurrency limit."""

from typing import Any

from aws_durable_execution_sdk_python.config import ParallelConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(_event: Any, context: DurableContext) -> list[str]:
    """Execute 5 tasks with concurrency limit of 2."""

    # Extract results immediately to avoid BatchResult serialization
    return context.parallel(
        functions=[
            lambda ctx: ctx.step(lambda _: "task 1", name="task1"),
            lambda ctx: ctx.step(lambda _: "task 2", name="task2"),
            lambda ctx: ctx.step(lambda _: "task 3", name="task3"),
            lambda ctx: ctx.step(lambda _: "task 4", name="task4"),
            lambda ctx: ctx.step(lambda _: "task 5", name="task5"),
        ],
        name="parallel_with_concurrency",
        config=ParallelConfig(max_concurrency=2),
    ).get_results()
