from typing import Any

from aws_durable_execution_sdk_python.config import CompletionConfig, ParallelConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(_event: Any, context: DurableContext) -> str:
    # Parallel execution with first_successful completion strategy
    config = ParallelConfig(completion_config=CompletionConfig.first_successful())

    functions = [
        lambda ctx: ctx.step(lambda _: "Task 1", name="task1"),
        lambda ctx: ctx.step(lambda _: "Task 2", name="task2"),
        lambda ctx: ctx.step(lambda _: "Task 3", name="task3"),
    ]

    results = context.parallel(
        functions, name="first_successful_parallel", config=config
    )

    # Extract the first successful result
    first_result = (
        results.successful_results[0] if results.successful_results else "None"
    )
    return f"First successful result: {first_result}"
