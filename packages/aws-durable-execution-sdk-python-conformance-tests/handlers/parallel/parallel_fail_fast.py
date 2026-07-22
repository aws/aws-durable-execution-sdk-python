"""8-6: Parallel fail-fast (tolerated-failure-count=0) stops after first failure."""

from typing import Any

from aws_durable_execution_sdk_python.config import CompletionConfig, ParallelConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


def branch_ok(_ctx: DurableContext) -> str:
    return "ok"


def branch_fail(_ctx: DurableContext) -> str:
    raise RuntimeError("branch failed")


def branch_never(_ctx: DurableContext) -> str:
    return "never"


@durable_execution
def handler(event: Any, context: DurableContext) -> dict:
    # Fail-fast: tolerated_failure_count=0 stops on the first failure (portable across SDKs).
    result = context.parallel(
        [branch_ok, branch_fail, branch_never],
        name="failfast",
        config=ParallelConfig(
            max_concurrency=1,
            completion_config=CompletionConfig(tolerated_failure_count=0),
        ),
    )
    # totalCount = started branches (succeeded + failed); early-stopped
    # branches are not counted, matching Java's succeeded()+failed().
    return {
        "completionReason": result.completion_reason.value,
        "status": result.status.value,
        "successCount": result.success_count,
        "failureCount": result.failure_count,
        "totalCount": result.total_count,
    }
