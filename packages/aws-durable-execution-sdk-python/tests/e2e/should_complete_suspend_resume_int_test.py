"""End-to-end test: should_complete predicate fires correctly across suspend/resume.

Branch 0 succeeds immediately. Branch 1 waits (causing suspension).
After resume, branch 1 succeeds and the predicate fires at 2 successes,
producing CUSTOM_COMPLETION_SUCCEEDED. Verifies the full cross-invocation
path including checkpoint restoration and predicate evaluation.
"""

from __future__ import annotations

import json
from typing import Any

from aws_durable_execution_sdk_python.config import (
    CompletionConfig,
    CompletionDecision,
    CompletionStatus,
    Duration,
    ParallelConfig,
    complete_batch,
    continue_batch,
)
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import (
    InvocationStatus,
    durable_execution,
)

from aws_durable_execution_sdk_python_testing.runner import (
    DurableFunctionTestResult,
    DurableFunctionTestRunner,
)


def _predicate(status: CompletionStatus) -> CompletionDecision:
    return complete_batch() if status.success_count >= 2 else continue_batch()


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Parallel with should_complete where one branch suspends."""
    config: ParallelConfig = ParallelConfig(
        max_concurrency=2,
        completion_config=CompletionConfig(should_complete=_predicate),
    )

    functions = [
        lambda ctx: ctx.step(lambda _: "branch_0_done", name="fast-branch"),
        lambda ctx: (
            ctx.wait(Duration.from_seconds(1), name="suspend-wait"),
            ctx.step(lambda _: "branch_1_done", name="after-wait"),
        )[1],
    ]

    results = context.parallel(functions, name="suspend-parallel", config=config)

    return {
        "success_count": results.success_count,
        "completion_reason": results.completion_reason.value,
    }


def test_should_complete_across_suspend_resume() -> None:
    """Predicate fires correctly after a branch suspends and resumes."""
    with DurableFunctionTestRunner(handler=handler) as runner:
        result: DurableFunctionTestResult = runner.run(input="{}", timeout=15)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result is not None

    result_data: dict = json.loads(result.result)

    assert result_data["success_count"] >= 2
    assert result_data["completion_reason"] == "CUSTOM_COMPLETION_SUCCEEDED"
