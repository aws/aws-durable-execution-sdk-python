"""8-7: Parallel throw-if-error propagates a branch failure to the execution (fail-fast config)."""

from typing import Any

from aws_durable_execution_sdk_python.config import CompletionConfig, ParallelConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


def branch_fail(_ctx: DurableContext) -> str:
    raise RuntimeError("branch failed")


def branch_never(_ctx: DurableContext) -> str:
    return "never"


@durable_execution
def handler(event: Any, context: DurableContext) -> list:
    # Fail-fast: tolerated_failure_count=0 stops on the first failure (portable across SDKs).
    result = context.parallel(
        [branch_fail, branch_never],
        name="throwing",
        config=ParallelConfig(
            max_concurrency=1,
            completion_config=CompletionConfig(tolerated_failure_count=0),
        ),
    )
    # Rethrow the first branch failure; uncaught -> the execution fails.
    result.throw_if_error()
    return result.get_results()
