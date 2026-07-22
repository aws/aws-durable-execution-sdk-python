"""8-20: Parallel BatchResult accessors (succeeded/failed/get_errors/has_failure)."""

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
        name="accessors",
        config=ParallelConfig(
            max_concurrency=1,
            completion_config=CompletionConfig(tolerated_failure_count=1),
        ),
    )
    # Exercise the accessor methods rather than the count properties.
    return {
        "hasFailure": result.has_failure,
        "successCount": len(result.succeeded()),
        "failureCount": len(result.failed()),
        "errorCount": len(result.get_errors()),
    }
