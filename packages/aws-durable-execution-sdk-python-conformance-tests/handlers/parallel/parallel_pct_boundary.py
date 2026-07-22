"""8-22: Parallel tolerated-failure-percentage at the boundary (not exceeded, ALL_COMPLETED)."""

from typing import Any

from aws_durable_execution_sdk_python.config import CompletionConfig, ParallelConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


def branch_fail(_ctx: DurableContext) -> str:
    raise RuntimeError("branch failed")


def ok1(_ctx: DurableContext) -> str:
    return "ok1"


def ok2(_ctx: DurableContext) -> str:
    return "ok2"


def ok3(_ctx: DurableContext) -> str:
    return "ok3"


@durable_execution
def handler(event: Any, context: DurableContext) -> dict:
    result = context.parallel(
        [branch_fail, ok1, ok2, ok3],
        name="pct-boundary",
        config=ParallelConfig(
            max_concurrency=1,
            completion_config=CompletionConfig(tolerated_failure_percentage=25),
        ),
    )
    # totalCount = started branches (succeeded + failed).
    return {
        "completionReason": result.completion_reason.value,
        "status": result.status.value,
        "successCount": result.success_count,
        "failureCount": result.failure_count,
        "totalCount": result.total_count,
    }
