"""8-10: Parallel tolerated-failure-count exceeded (stops early)."""

from typing import Any

from aws_durable_execution_sdk_python.config import CompletionConfig, ParallelConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


def branch0(_ctx: DurableContext) -> str:
    raise RuntimeError("branch failed")


def branch1(_ctx: DurableContext) -> str:
    raise RuntimeError("branch failed")


def branch2(_ctx: DurableContext) -> str:
    return "never"


@durable_execution
def handler(event: Any, context: DurableContext) -> dict:
    result = context.parallel(
        [branch0, branch1, branch2],
        name="tolerated-exceeded",
        config=ParallelConfig(
            max_concurrency=1,
            completion_config=CompletionConfig(tolerated_failure_count=1),
        ),
    )
    # totalCount = started branches (succeeded + failed); early-stopped
    # branches are not counted, matching Java's succeeded()+failed().
    return {
        "completionReason": result.completion_reason.value,
        "successCount": result.success_count,
        "failureCount": result.failure_count,
        "totalCount": result.total_count,
    }
