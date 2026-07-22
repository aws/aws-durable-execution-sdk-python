"""8-17: Parallel min-successful not reached (all branches run)."""

from typing import Any

from aws_durable_execution_sdk_python.config import CompletionConfig, ParallelConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


def ok0(_ctx: DurableContext) -> str:
    return "ok0"


def branch_fail(_ctx: DurableContext) -> str:
    raise RuntimeError("branch failed")


def ok2(_ctx: DurableContext) -> str:
    return "ok2"


@durable_execution
def handler(event: Any, context: DurableContext) -> dict:
    result = context.parallel(
        [ok0, branch_fail, ok2],
        name="min-not-reached",
        config=ParallelConfig(
            max_concurrency=1,
            completion_config=CompletionConfig(min_successful=3),
        ),
    )
    # totalCount = started branches (succeeded + failed); matches Java's succeeded()+failed().
    return {
        "completionReason": result.completion_reason.value,
        "status": result.status.value,
        "successCount": result.success_count,
        "failureCount": result.failure_count,
        "totalCount": result.total_count,
    }
