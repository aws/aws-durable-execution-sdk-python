"""8-16: Parallel where all branches fail (within tolerance, ALL_COMPLETED)."""

from typing import Any

from aws_durable_execution_sdk_python.config import CompletionConfig, ParallelConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


def branch_fail(_ctx: DurableContext) -> str:
    raise RuntimeError("branch failed")


@durable_execution
def handler(event: Any, context: DurableContext) -> dict:
    result = context.parallel(
        [branch_fail, branch_fail, branch_fail],
        name="all-fail",
        config=ParallelConfig(
            max_concurrency=1,
            completion_config=CompletionConfig(tolerated_failure_count=3),
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
