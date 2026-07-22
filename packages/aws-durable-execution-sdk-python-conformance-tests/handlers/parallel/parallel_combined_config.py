"""8-18: Parallel with combined completion config (min-successful + tolerated-failure-count)."""

from typing import Any

from aws_durable_execution_sdk_python.config import CompletionConfig, ParallelConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


def branch_fail(_ctx: DurableContext) -> str:
    raise RuntimeError("branch failed")


def ok2(_ctx: DurableContext) -> str:
    return "ok2"


def ok3(_ctx: DurableContext) -> str:
    return "ok3"


@durable_execution
def handler(event: Any, context: DurableContext) -> dict:
    result = context.parallel(
        [branch_fail, branch_fail, ok2, ok3],
        name="combined",
        config=ParallelConfig(
            max_concurrency=1,
            completion_config=CompletionConfig(
                min_successful=3, tolerated_failure_count=1
            ),
        ),
    )
    # totalCount = started branches (succeeded + failed); matches Java's succeeded()+failed().
    return {
        "completionReason": result.completion_reason.value,
        "successCount": result.success_count,
        "failureCount": result.failure_count,
        "totalCount": result.total_count,
    }
